package outboundgroup

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	SN "github.com/metacubex/sing/common/network"
)

// pldTTFRObservation owns only shadow clocks. Legacy callbacks, packet-pair
// timing, quality gates, reward features and training labels remain unchanged.
type pldTTFRObservation struct {
	mu       sync.Mutex
	complete atomic.Bool // first-byte counter becomes a cheap no-op after observation
	isUDP    bool
	origin   time.Time
	first    time.Time
	pending  map[*pldPacketSend]struct{}
	settled  bool
	lateUDP  func(float64)
}

type pldPacketSend struct {
	start time.Time
	first time.Time
}

func (o *pldTTFRObservation) downlink(n int64) {
	if n <= 0 || o.complete.Load() {
		return
	}
	now := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.settled {
		return
	}
	if !o.origin.IsZero() && o.first.IsZero() && !now.Before(o.origin) {
		o.first = now
	}
	// A response can be delivered concurrently before WriteTo returns.
	// Keep it provisional until that write reports actual sent payload.
	for send := range o.pending {
		if send.first.IsZero() && !now.Before(send.start) {
			send.first = now
		}
	}
	if !o.first.IsZero() && len(o.pending) == 0 {
		o.complete.Store(true)
	}
}

func (o *pldTTFRObservation) beginPacketSend(size int) *pldPacketSend {
	if size <= 0 {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.settled || !o.origin.IsZero() {
		return nil
	}
	send := &pldPacketSend{start: time.Now()}
	if o.pending == nil {
		o.pending = make(map[*pldPacketSend]struct{})
	}
	o.pending[send] = struct{}{}
	return send
}

func (o *pldTTFRObservation) finishPacketSend(send *pldPacketSend, n int) {
	if send == nil {
		return
	}
	o.mu.Lock()
	delete(o.pending, send)
	if n > 0 && (o.origin.IsZero() || send.start.Before(o.origin)) {
		o.origin = send.start
		if !send.first.IsZero() && (o.first.IsZero() || send.first.Before(o.first)) {
			o.first = send.first
		}
	}
	var publish func(float64)
	var ms float64
	if len(o.pending) == 0 {
		if !o.origin.IsZero() && !o.first.IsZero() {
			o.complete.Store(true)
			publish, ms = o.lateUDP, float64(o.first.Sub(o.origin))/float64(time.Millisecond)
		}
		// No successful payload or no response: discard the provisional
		// observation and release its record owner without fabricating a zero.
		o.lateUDP = nil
	}
	o.mu.Unlock()
	if publish != nil {
		publish(ms) // Never call into record/cache/DB locks under the observer lock.
	}
}

// afterPacketSettlement resolves only sends already pending at settlement.
// It cannot replay the connection outcome or admit new post-close responses.
func (o *pldTTFRObservation) afterPacketSettlement(publish func(float64)) {
	if o == nil || !o.isUDP {
		return
	}
	o.mu.Lock()
	if o.settled {
		o.mu.Unlock()
		return
	}
	o.settled = true
	var ready bool
	var ms float64
	if len(o.pending) > 0 {
		o.lateUDP = publish
	} else if !o.origin.IsZero() && !o.first.IsZero() {
		ready, ms = true, float64(o.first.Sub(o.origin))/float64(time.Millisecond)
	}
	o.mu.Unlock()
	if ready && publish != nil {
		publish(ms)
	}
}

func (o *pldTTFRObservation) apply(m *smart.RewardedMetrics) {
	if o == nil {
		return // Old callers have unknown raw TTFR; never borrow legacy latency.
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.origin.IsZero() || o.first.IsZero() || (o.isUDP && len(o.pending) > 0) {
		return
	}
	ms := float64(o.first.Sub(o.origin)) / float64(time.Millisecond)
	if o.isUDP {
		m.UDPResponseMs, m.UDPResponseValid = ms, true
	} else {
		m.TCPConnectionTTFRMs, m.TCPConnectionTTFRValid = ms, true
	}
}

type pldTTFRConn struct {
	C.Conn
	observation *pldTTFRObservation
}

func (c *pldTTFRConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.observation.downlink(int64(n)) // Positive data + error is still valid.
	return n, err
}

func (c *pldTTFRConn) ReadBuffer(buffer *buf.Buffer) error {
	before := buffer.Len()
	err := c.Conn.ReadBuffer(buffer)
	c.observation.downlink(int64(buffer.Len() - before))
	return err
}

func (c *pldTTFRConn) Upstream() any           { return c.Conn }
func (c *pldTTFRConn) ReaderReplaceable() bool { return false }
func (c *pldTTFRConn) WriterReplaceable() bool { return true }

// Preserve existing accelerated relay/copy counters; do not disable raw sockets.
func (c *pldTTFRConn) UnwrapReader() (io.Reader, []SN.CountFunc) {
	return c.Conn, []SN.CountFunc{c.observation.downlink}
}

type pldTTFRPacketConn struct {
	C.PacketConn
	observation *pldTTFRObservation
}

func (c *pldTTFRPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	send := c.observation.beginPacketSend(len(b))
	n, err := c.PacketConn.WriteTo(b, addr)
	c.observation.finishPacketSend(send, n)
	return n, err
}

func (c *pldTTFRPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	c.observation.downlink(int64(n))
	return n, addr, err
}

func (c *pldTTFRPacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	data, put, addr, err := c.PacketConn.WaitReadFrom()
	c.observation.downlink(int64(len(data)))
	return data, put, addr, err
}

func (c *pldTTFRPacketConn) Upstream() any { return c.PacketConn }
func (c *pldTTFRPacketConn) ReaderReplaceable() bool {
	c.observation.mu.Lock()
	defer c.observation.mu.Unlock()
	return !c.observation.first.IsZero()
}
func (c *pldTTFRPacketConn) WriterReplaceable() bool {
	c.observation.mu.Lock()
	defer c.observation.mu.Unlock()
	return !c.observation.origin.IsZero()
}

var _ SN.ReadCounter = (*pldTTFRConn)(nil)
