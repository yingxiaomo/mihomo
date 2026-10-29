package naive

import (
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/net/deadline"
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
	// HTTP bodies cannot be interrupted reversibly: Close cancels an H2/H3
	// stream permanently. Keep at most one pending decoded read, on owned
	// memory, across timeouts. In particular, a half-read padding header or
	// discarded padding must stay with that read rather than be parsed again.
	initOnce     sync.Once
	closeOnce    sync.Once
	readDeadline deadline.PipeDeadline
	closed       chan struct{}
	closeErr     error
	mu           sync.Mutex // serializes deadline changes with Close
	readMu       sync.Mutex // serializes readers and guards pending/cache
	writeMu      sync.Mutex // guards the write padding state and terminal error
	writeErr     error
	pending      chan naiveReadResult
	cache        naiveReadResult
}

type naiveReadResult struct {
	data []byte
	err  error
}

func (c *naiveH2Conn) initDeadline() {
	c.initOnce.Do(func() {
		c.readDeadline = deadline.MakePipeDeadline()
		c.closed = make(chan struct{})
	})
}

func (c *naiveH2Conn) Read(b []byte) (int, error) {
	c.initDeadline()
	c.readMu.Lock()
	defer c.readMu.Unlock()
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	select {
	case <-c.readDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	if len(b) == 0 {
		return 0, nil
	}
	if len(c.cache.data) == 0 && c.cache.err == nil {
		if c.pending == nil {
			c.pending = make(chan naiveReadResult, 1)
			// Only this worker touches the read padding state. The buffered
			// channel lets it exit even after Close, without touching b. A
			// timed-out caller reuses this pending read, never starts another.
			go c.readAsync(c.pending, min(len(b), 65535))
		}
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.readDeadline.Wait():
			return 0, os.ErrDeadlineExceeded
		case c.cache = <-c.pending:
			c.pending = nil
		}
	}
	// Close can release the body and make both select cases ready at once.
	// Preserve a ready result on expiry, but never report a body-close error
	// as a deadline or copy its bytes after observing Close.
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.readDeadline.Wait():
		return 0, os.ErrDeadlineExceeded
	default:
	}
	n := copy(b, c.cache.data)
	c.cache.data = c.cache.data[n:]
	if len(c.cache.data) > 0 {
		return n, nil
	}
	err := c.cache.err
	c.cache = naiveReadResult{}
	return n, err
}

func (c *naiveH2Conn) readAsync(result chan<- naiveReadResult, size int) {
	buffer := make([]byte, size)
	n, err := c.readWithPadding(c.reader, buffer)
	result <- naiveReadResult{data: buffer[:n], err: err}
}

func (c *naiveH2Conn) Write(b []byte) (n int, err error) {
	c.initDeadline()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	n, err = c.writeWithPadding(completeWriter{c.writer}, b)
	if err == nil && c.flusher != nil {
		if flusher, ok := c.flusher.(interface{ FlushError() error }); ok {
			err = flusher.FlushError()
		} else {
			c.flusher.Flush()
		}
	}
	// A failed framed write may have emitted part of its header or data.
	// Never resume with a new frame after clearing a native write deadline.
	c.writeErr = err
	return
}

type completeWriter struct{ io.Writer }

func (w completeWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (c *naiveH2Conn) Close() error {
	c.initDeadline()
	c.closeOnce.Do(func() {
		c.mu.Lock()
		close(c.closed)
		c.readDeadline.Set(time.Time{}) // release the timer, not the body
		c.mu.Unlock()
		// Actual request bodies are closable, so this also releases the
		// single outstanding worker. Arbitrary non-closable io.Readers
		// cannot be forcibly interrupted, but cannot accumulate workers.
		if closer, ok := c.reader.(io.Closer); ok {
			c.closeErr = closer.Close()
		}
	})
	return c.closeErr
}

func (c *naiveH2Conn) LocalAddr() net.Addr  { return c.localAddr }
func (c *naiveH2Conn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *naiveH2Conn) SetReadDeadline(t time.Time) error {
	c.initDeadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	c.readDeadline.Set(t)
	return nil
}

// Write deadlines use the response writer's native stream implementation, not
// asynchronous writes: returning while a padded frame is still being written
// would corrupt subsequent frames. H2 write expiry may reset that stream and
// is not recoverable. Writers without deadline support report it explicitly.
func (c *naiveH2Conn) SetWriteDeadline(t time.Time) error {
	c.initDeadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	if writer, ok := c.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return writer.SetWriteDeadline(t)
	}
	if t.IsZero() {
		return nil
	}
	return os.ErrNoDeadline
}

func (c *naiveH2Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

var _ net.Conn = (*naiveH2Conn)(nil)
