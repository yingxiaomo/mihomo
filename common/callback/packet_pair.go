package callback

import (
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// packetPairCallBackConn passively measures the first two "packets" of a TCP
// flow for a Packet-Pair bandwidth-ceiling estimate:
//   - t0: wall-clock when the first byte of the response arrives (TTFR start)
//   - t64: wall-clock when cumulative received bytes cross PPProbeBytes (64KiB)
//
// The implied ceiling = PPProbeBytes / (t64 − t0). This is a passive probe: it
// never sends traffic of its own, it only stamps the read path of the real
// connection, so small web transfers get a meaningful upper-bound bandwidth
// signal without any active measurement.
type PacketPairCallBackConn struct {
	C.Conn

	// startNs is set by the first successful Read (response data starts flowing).
	startNs atomic.Int64
	// crossNs is set when cumulative bytes >= threshold.
	crossNs atomic.Int64
	// bytesRead accumulates received payload bytes.
	bytesRead atomic.Int64
}

// PPProbeBytes is the byte budget for the packet-pair bandwidth window.
// 64 KiB is small enough to be reached by most web responses quickly, large
// enough to average out per-segment scheduling jitter.
const PPProbeBytes = 64 * 1024

// PacketPairResult is the readout of one connection's packet-pair measurement.
type PacketPairResult struct {
	// FirstByteMs is ms from wrap time to first received byte (TTFR).
	FirstByteMs int64
	// BytesTo64KMs is ms from first byte to crossing 64KiB (0 if never reached).
	BytesTo64KMs int64
}

func (c *PacketPairCallBackConn) accountRead(n int, now time.Time) {
	if n <= 0 {
		return
	}
	c.startNs.CompareAndSwap(0, now.UnixNano())
	total := c.bytesRead.Add(int64(n))
	if c.crossNs.Load() == 0 && total >= PPProbeBytes {
		c.crossNs.Store(now.UnixNano())
	}
}

// Readout returns the packet-pair timing deltas measured so far.
func (c *PacketPairCallBackConn) Readout() PacketPairResult {
	start := c.startNs.Load()
	if start == 0 {
		return PacketPairResult{}
	}
	res := PacketPairResult{FirstByteMs: (time.Now().UnixNano() - start) / int64(time.Millisecond)}
	if cross := c.crossNs.Load(); cross != 0 {
		res.BytesTo64KMs = (cross - start) / int64(time.Millisecond)
	}
	return res
}

func (c *PacketPairCallBackConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.accountRead(n, time.Now())
	return n, err
}

func (c *PacketPairCallBackConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	err = c.Conn.ReadBuffer(buffer)
	if buffer != nil {
		c.accountRead(buffer.Len(), time.Now())
	}
	return err
}

func (c *PacketPairCallBackConn) Upstream() any {
	return c.Conn
}

func (c *PacketPairCallBackConn) WriterReplaceable() bool {
	return true
}

func (c *PacketPairCallBackConn) ReaderReplaceable() bool {
	return true
}

var _ N.ExtendedConn = (*PacketPairCallBackConn)(nil)

// NewPacketPairCallBackConn wraps c with a passive packet-pair reader.
func NewPacketPairCallBackConn(c C.Conn) *PacketPairCallBackConn {
	return &PacketPairCallBackConn{Conn: c}
}
