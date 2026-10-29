package callback

import (
	"context"
	"errors"
	"io"
	"net"
	"os"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"
)

// errorCallBackConn records the first non-nil error of each direction so a
// connection that dies mid-transfer is not reported as a success.
// Keep the observer on ordinary copies. The production relay explicitly
// unwraps its error-reporting contract while retaining the raw socket fast path.
type errorCallBackConn struct {
	C.Conn
	readErr  *atomic.TypedValue[error]
	writeErr *atomic.TypedValue[error]
}

// our own close, a transient deadline, and locally-initiated cancels
// (io.ErrClosedPipe from Hysteria2/QUIC session cancel) must not be charged
// to the node
func isConnectionLifecycle(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.Canceled)
}

// IsConnectionLifecycle identifies locally initiated cancellation and socket
// deadlines. It deliberately does not classify every net.Error timeout as local.
func IsConnectionLifecycle(err error) bool { return isConnectionLifecycle(err) }

// TransferError selects a genuine transfer failure, ignoring normal EOF and
// local stream lifecycle signals. First-read and close observers share this
// classification so a local cancellation cannot bypass the observer filter.
func TransferError(errs ...error) error {
	for _, err := range errs {
		if err != nil && !errors.Is(err, io.EOF) && !isConnectionLifecycle(err) {
			return err
		}
	}
	return nil
}

func (c *errorCallBackConn) record(slot *atomic.TypedValue[error], err error) {
	if err == nil || isConnectionLifecycle(err) {
		return
	}
	slot.CompareAndSwap(nil, err)
}

func (c *errorCallBackConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.record(c.readErr, err)
	return n, err
}

func (c *errorCallBackConn) ReadBuffer(buffer *buf.Buffer) error {
	err := c.Conn.ReadBuffer(buffer)
	c.record(c.readErr, err)
	return err
}

func (c *errorCallBackConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.record(c.writeErr, err)
	return n, err
}

func (c *errorCallBackConn) WriteBuffer(buffer *buf.Buffer) error {
	err := c.Conn.WriteBuffer(buffer)
	c.record(c.writeErr, err)
	return err
}

// These explicit contracts are used by the production relay, not by ordinary
// sing unwrapping, so raw socket copying retains the direction-specific sinks.
func (c *errorCallBackConn) UnwrapReadError() (io.Reader, func(error)) {
	return c.Conn, func(err error) { c.record(c.readErr, err) }
}

func (c *errorCallBackConn) UnwrapWriteError() (io.Writer, func(error)) {
	return c.Conn, func(err error) { c.record(c.writeErr, err) }
}

func (c *errorCallBackConn) Upstream() any {
	return c.Conn
}

func NewErrorCallBackConn(c C.Conn, readErr, writeErr *atomic.TypedValue[error]) C.Conn {
	return &errorCallBackConn{Conn: c, readErr: readErr, writeErr: writeErr}
}

// errorCallBackPacketConn is the udp counterpart of errorCallBackConn and
// shares its peel contract. The ICMP refusal of the far side is filtered: it
// must not be charged to the node.
type errorCallBackPacketConn struct {
	C.PacketConn
	readErr  *atomic.TypedValue[error]
	writeErr *atomic.TypedValue[error]
}

func (c *errorCallBackPacketConn) record(slot *atomic.TypedValue[error], err error) {
	if err == nil || isConnectionLifecycle(err) || isICMPRefusal(err) {
		return
	}
	slot.CompareAndSwap(nil, err)
}

func (c *errorCallBackPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	c.record(c.readErr, err)
	return n, addr, err
}

func (c *errorCallBackPacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = c.PacketConn.WaitReadFrom()
	c.record(c.readErr, err)
	return
}

func (c *errorCallBackPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(b, addr)
	c.record(c.writeErr, err)
	return n, err
}

func (c *errorCallBackPacketConn) Upstream() any {
	return c.PacketConn
}

func NewErrorCallBackPacketConn(pc C.PacketConn, readErr, writeErr *atomic.TypedValue[error]) C.PacketConn {
	return &errorCallBackPacketConn{PacketConn: pc, readErr: readErr, writeErr: writeErr}
}
