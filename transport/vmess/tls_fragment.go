package vmess

import (
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/metacubex/randv2"
)

// TLSFragmentConfig controls TLS write fragmentation.
// When PacketsFrom=0 and PacketsTo=1, only the first TLS ClientHello record is split.
type TLSFragmentConfig struct {
	PacketsFrom uint64
	PacketsTo   uint64
	LengthMin   uint64
	LengthMax   uint64
	IntervalMin uint64
	IntervalMax uint64
	MaxSplitMin uint64
	MaxSplitMax uint64
}

type tlsFragmentConn struct {
	net.Conn
	fragment *TLSFragmentConfig
	count    atomic.Uint64
}

const clientHelloFallbackSplitOffset = 40

func newTLSFragmentConn(conn net.Conn, fragment *TLSFragmentConfig) net.Conn {
	if conn == nil || fragment == nil {
		return conn
	}
	if fragment.LengthMin == 0 || fragment.LengthMax == 0 {
		return conn
	}
	return &tlsFragmentConn{Conn: conn, fragment: fragment}
}

func (c *tlsFragmentConn) Write(b []byte) (int, error) {
	count := c.count.Add(1)

	if c.fragment.PacketsFrom == 0 && c.fragment.PacketsTo == 1 {
		return c.writeTLSHelloFragment(count, b)
	}

	if c.fragment.PacketsFrom != 0 && (count < c.fragment.PacketsFrom || count > c.fragment.PacketsTo) {
		return c.Conn.Write(b)
	}

	return c.writeFragmentedChunks(b)
}

func (c *tlsFragmentConn) writeTLSHelloFragment(count uint64, b []byte) (int, error) {
	if count != 1 || len(b) <= 5 || b[0] != 22 {
		return c.Conn.Write(b)
	}

	recordLen := 5 + (int(b[3])<<8 | int(b[4]))
	if len(b) < recordLen {
		return c.Conn.Write(b)
	}

	data := b[5:recordLen]
	firstSplit := firstClientHelloSplitOffset(data)
	maxSplit := c.randBetween(c.fragment.MaxSplitMin, c.fragment.MaxSplitMax)

	var splitNum uint64
	var hello []byte

	for from := 0; ; {
		to := from + int(c.randBetween(c.fragment.LengthMin, c.fragment.LengthMax))
		// Clamp first fragment before server_name extension when possible.
		if from == 0 && firstSplit > 0 {
			if to > firstSplit {
				to = firstSplit
			}
			if to <= from {
				to = from + 1
			}
		}
		splitNum++
		if to > len(data) || (maxSplit > 0 && splitNum >= maxSplit) {
			to = len(data)
		}

		l := to - from
		fragment := make([]byte, 5+l)
		copy(fragment[:3], b[:3])
		fragment[3] = byte(l >> 8)
		fragment[4] = byte(l)
		copy(fragment[5:], data[from:to])
		from = to

		if c.fragment.IntervalMax == 0 {
			hello = append(hello, fragment...)
		} else {
			if err := writeAll(c.Conn, fragment); err != nil {
				return 0, err
			}
			c.sleepInterval()
		}

		if from == len(data) {
			if len(hello) > 0 {
				if err := writeAll(c.Conn, hello); err != nil {
					return 0, err
				}
			}
			if len(b) > recordLen {
				n, err := c.Conn.Write(b[recordLen:])
				if err != nil {
					return recordLen + n, err
				}
			}
			return len(b), nil
		}
	}
}

func (c *tlsFragmentConn) writeFragmentedChunks(b []byte) (int, error) {
	maxSplit := c.randBetween(c.fragment.MaxSplitMin, c.fragment.MaxSplitMax)
	var splitNum uint64

	written := 0
	for from := 0; ; {
		to := from + int(c.randBetween(c.fragment.LengthMin, c.fragment.LengthMax))
		splitNum++
		if to > len(b) || (maxSplit > 0 && splitNum >= maxSplit) {
			to = len(b)
		}

		n, err := c.Conn.Write(b[from:to])
		from += n
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
		if from >= len(b) {
			return written, nil
		}
		c.sleepInterval()
	}
}

func (c *tlsFragmentConn) sleepInterval() {
	if c.fragment.IntervalMax == 0 {
		return
	}
	interval := c.randBetween(c.fragment.IntervalMin, c.fragment.IntervalMax)
	if interval > 0 {
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
}

func (c *tlsFragmentConn) randBetween(min uint64, max uint64) uint64 {
	if max <= min {
		return min
	}
	return min + uint64(randv2.Int64N(int64(max-min+1)))
}

func firstClientHelloSplitOffset(data []byte) int {
	// Best effort: split right before server_name extension header.
	if sniOffset, ok := parseClientHelloServerNameOffset(data); ok && sniOffset > 0 {
		return sniOffset
	}
	// Fallback: split before any extension.
	if extStart, ok := parseClientHelloExtensionsOffset(data); ok && extStart > 0 {
		return extStart
	}
	// Conservative fallback for malformed/unexpected ClientHello layout.
	if len(data) > clientHelloFallbackSplitOffset && len(data) > 0 && data[0] == 1 {
		return clientHelloFallbackSplitOffset
	}
	return 0
}

// parseClientHelloExtensionsOffset returns the offset within data (which
// starts at the Handshake header) where the extensions block begins.
// It returns (offset, true) on success; otherwise (0, false).
func parseClientHelloExtensionsOffset(data []byte) (int, bool) {
	extStart, _, ok := parseClientHelloExtensionsBlock(data)
	if !ok {
		return 0, false
	}
	return extStart, true
}

func parseClientHelloServerNameOffset(data []byte) (int, bool) {
	extStart, extLen, ok := parseClientHelloExtensionsBlock(data)
	if !ok {
		return 0, false
	}
	extEnd := extStart + extLen
	for pos := extStart; pos+4 <= extEnd; {
		extType := int(data[pos])<<8 | int(data[pos+1])
		eLen := int(data[pos+2])<<8 | int(data[pos+3])
		next := pos + 4 + eLen
		if next > extEnd {
			return 0, false
		}
		if extType == 0 { // server_name
			return pos, true
		}
		pos = next
	}
	return 0, false
}

func parseClientHelloExtensionsBlock(data []byte) (int, int, bool) {
	if len(data) < 4+2+32+1+2+1+2 {
		return 0, 0, false
	}
	if data[0] != 1 { // ClientHello
		return 0, 0, false
	}

	helloLen := int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	helloEnd := 4 + helloLen
	if helloEnd > len(data) {
		return 0, 0, false
	}

	pos := 4
	if pos+2 > helloEnd {
		return 0, 0, false
	}
	pos += 2
	if pos+32 > helloEnd {
		return 0, 0, false
	}
	pos += 32

	if pos+1 > helloEnd {
		return 0, 0, false
	}
	sidLen := int(data[pos])
	pos++
	if pos+sidLen > helloEnd {
		return 0, 0, false
	}
	pos += sidLen

	if pos+2 > helloEnd {
		return 0, 0, false
	}
	csLen := int(data[pos])<<8 | int(data[pos+1])
	pos += 2
	if pos+csLen > helloEnd {
		return 0, 0, false
	}
	pos += csLen

	if pos+1 > helloEnd {
		return 0, 0, false
	}
	compLen := int(data[pos])
	pos++
	if pos+compLen > helloEnd {
		return 0, 0, false
	}
	pos += compLen

	if pos+2 > helloEnd {
		return 0, 0, false
	}
	extLen := int(data[pos])<<8 | int(data[pos+1])
	pos += 2
	if pos+extLen > helloEnd {
		return 0, 0, false
	}

	return pos, extLen, true
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
