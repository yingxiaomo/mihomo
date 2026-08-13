package naive

import (
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"time"
)

// flushWriter is the minimal interface a padded HTTP/2 or HTTP/3 response needs.
// Both net/http and github.com/metacubex/http response writers satisfy it, so
// naiveH2Conn stays independent of any particular http package.
type flushWriter interface{ Flush() }

// paddingCount is the number of leading frames (in each direction) that carry
// NaïveProxy padding. After that the stream is passed through verbatim.
//
// The padding wire format matches the upstream NaïveProxy / sing-box
// implementation so mihomo can interoperate with any standard naive client
// (including the cronet-based client used by mihomo's own naive outbound):
//
//	[2 bytes big-endian original data length][1 byte padding length][data][padding zeros]
const paddingCount = 8

// generatePaddingHeader builds the random value sent in the "Padding" HTTP
// header, mimicking the upstream naive server behaviour.
func generatePaddingHeader() string {
	paddingLen := rand.Intn(32) + 30
	padding := make([]byte, paddingLen)
	bits := rand.Uint64()
	for i := 0; i < 16; i++ {
		padding[i] = "!#$()+<>?@[]^`{}"[bits&15]
		bits >>= 4
	}
	for i := 16; i < paddingLen; i++ {
		padding[i] = '~'
	}
	return string(padding)
}

// paddingConn holds the per-direction padding state shared by the read and
// write halves of a naive connection.
type paddingConn struct {
	readPadding      int
	writePadding     int
	readRemaining    int
	paddingRemaining int
}

func (p *paddingConn) readWithPadding(reader io.Reader, buffer []byte) (n int, err error) {
	if p.readRemaining > 0 {
		if len(buffer) > p.readRemaining {
			buffer = buffer[:p.readRemaining]
		}
		n, err = reader.Read(buffer)
		if err != nil {
			return
		}
		p.readRemaining -= n
		return
	}
	if p.paddingRemaining > 0 {
		err = skipN(reader, p.paddingRemaining)
		if err != nil {
			return
		}
		p.paddingRemaining = 0
	}
	if p.readPadding < paddingCount {
		paddingHeader := make([]byte, 3)
		_, err = io.ReadFull(reader, paddingHeader)
		if err != nil {
			return
		}
		originalDataSize := int(binary.BigEndian.Uint16(paddingHeader[:2]))
		paddingSize := int(paddingHeader[2])
		if len(buffer) > originalDataSize {
			buffer = buffer[:originalDataSize]
		}
		n, err = reader.Read(buffer)
		if err != nil {
			return
		}
		p.readPadding++
		p.readRemaining = originalDataSize - n
		p.paddingRemaining = paddingSize
		return
	}
	return reader.Read(buffer)
}

func (p *paddingConn) writeWithPadding(writer io.Writer, data []byte) (n int, err error) {
	for len(data) > 0 {
		var chunk []byte
		// A single padded frame carries at most 65535 bytes because the length
		// prefix is a uint16; split larger writes into multiple frames.
		if len(data) > 65535 {
			chunk = data[:65535]
			data = data[65535:]
		} else {
			chunk = data
			data = nil
		}
		if p.writePadding < paddingCount {
			paddingSize := rand.Intn(256)
			buffer := make([]byte, 3+len(chunk)+paddingSize)
			binary.BigEndian.PutUint16(buffer, uint16(len(chunk)))
			buffer[2] = byte(paddingSize)
			copy(buffer[3:], chunk)
			// the trailing paddingSize bytes are already zero
			if _, err = writer.Write(buffer); err != nil {
				return
			}
			p.writePadding++
			n += len(chunk)
		} else {
			var written int
			written, err = writer.Write(chunk)
			n += written
			if err != nil {
				return
			}
		}
	}
	return
}

// skipN discards exactly n bytes from reader. n never exceeds 255 (a single
// padding length byte), so a stack-sized buffer is fine.
func skipN(reader io.Reader, n int) error {
	buffer := make([]byte, n)
	_, err := io.ReadFull(reader, buffer)
	return err
}

// naiveH2Conn adapts an HTTP/2 or HTTP/3 CONNECT stream (request body as reader,
// response writer as writer) into a net.Conn with transparent naive padding, so
// it can be fed into mihomo's tunnel like any other inbound connection.
type naiveH2Conn struct {
	reader     io.Reader
	writer     io.Writer
	flusher    flushWriter
	localAddr  net.Addr
	remoteAddr net.Addr
	paddingConn
}

func (c *naiveH2Conn) Read(b []byte) (int, error) {
	return c.readWithPadding(c.reader, b)
}

func (c *naiveH2Conn) Write(b []byte) (n int, err error) {
	n, err = c.writeWithPadding(c.writer, b)
	if err == nil && c.flusher != nil {
		c.flusher.Flush()
	}
	return
}

func (c *naiveH2Conn) Close() error {
	if closer, ok := c.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (c *naiveH2Conn) LocalAddr() net.Addr  { return c.localAddr }
func (c *naiveH2Conn) RemoteAddr() net.Addr { return c.remoteAddr }

// Deadlines are not supported over an HTTP/2 stream; report success without
// enforcing them so mihomo's relay loop (which probes deadlines) keeps working.
func (c *naiveH2Conn) SetDeadline(t time.Time) error      { return nil }
func (c *naiveH2Conn) SetReadDeadline(t time.Time) error  { return nil }
func (c *naiveH2Conn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*naiveH2Conn)(nil)
