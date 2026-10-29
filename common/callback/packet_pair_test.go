package callback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"
)

func TestPacketPairEarlyWindow(t *testing.T) {
	base := time.Unix(100, 0)
	c := &PacketPairCallBackConn{wrappedNs: base.UnixNano()}
	if got := c.Readout(); got != (PacketPairResult{}) {
		t.Fatal(got)
	}
	c.accountRead(0, base)
	c.accountRead(-1, base)
	c.accountRead(PPProbeBytes/2, base.Add(5*time.Millisecond))
	if got := c.Readout(); got.FirstByteMs != 5 || got.BytesTo64KMs != 0 {
		t.Fatal(got)
	}
	c.accountRead(PPProbeBytes/2, base.Add(20*time.Millisecond))
	want := PacketPairResult{FirstByteMs: 5, BytesTo64KMs: 15}
	if got := c.Readout(); got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	c.accountRead(PPProbeBytes, base.Add(time.Second))
	if c.bytesRead.Load() != PPProbeBytes || c.Readout() != want {
		t.Fatal("probe continued after its byte budget")
	}
	fast := &PacketPairCallBackConn{wrappedNs: base.UnixNano()}
	fast.accountRead(PPProbeBytes, base)
	if fast.Readout().BytesTo64KMs != 1 {
		t.Fatal("completed fast probe was reported as incomplete")
	}
}

type packetPairBufferConn struct {
	C.Conn
	reader *bytes.Reader
	err    error
}

func (c *packetPairBufferConn) Read(b []byte) (int, error) {
	n, e := c.reader.Read(b)
	if c.err != nil {
		e = c.err
	}
	return n, e
}
func (c *packetPairBufferConn) ReadBuffer(b *buf.Buffer) error {
	if b == nil {
		return c.err
	}
	payload := make([]byte, c.reader.Len())
	_, _ = c.reader.Read(payload)
	_, _ = b.Write(payload)
	return c.err
}
func (c *packetPairBufferConn) Write(b []byte) (int, error)   { return len(b), c.err }
func (c *packetPairBufferConn) WriteBuffer(*buf.Buffer) error { return c.err }

func TestTCPObserverLifecycleAndErrors(t *testing.T) {
	for _, e := range []error{nil, context.Canceled, io.ErrClosedPipe, net.ErrClosed, errors.New("real reset")} {
		var rd, wr atomic.TypedValue[error]
		base := &packetPairBufferConn{reader: bytes.NewReader(nil), err: e}
		c := NewErrorCallBackConn(base, &rd, &wr)
		_, _ = c.Read(nil)
		buffer := buf.NewSize(16)
		_ = c.ReadBuffer(buffer)
		_, _ = c.Write([]byte("x"))
		_ = c.WriteBuffer(buffer)
		buffer.Release()
		if e != nil && !IsConnectionLifecycle(e) {
			if rd.Load() != e || wr.Load() != e {
				t.Fatal("real error lost", e, rd.Load(), wr.Load())
			}
		} else if TransferError(rd.Load(), wr.Load()) != nil {
			t.Fatal("local error charged", e)
		}
		if c.(interface{ Upstream() any }).Upstream() != base {
			t.Fatal("wrong upstream")
		}
	}
	var rd, wr atomic.TypedValue[error]
	first := errors.New("first reset")
	base := &packetPairBufferConn{reader: bytes.NewReader(nil), err: first}
	c := NewErrorCallBackConn(base, &rd, &wr)
	_, _ = c.Read(nil)
	base.err = errors.New("later reset")
	_, _ = c.Read(nil)
	if rd.Load() != first {
		t.Fatal("first error overwritten")
	}
}

func TestPacketPairDirectReads(t *testing.T) {
	c := NewPacketPairCallBackConn(&packetPairBufferConn{reader: bytes.NewReader([]byte("abc"))})
	if n, e := c.Read(make([]byte, 3)); n != 3 || e != nil {
		t.Fatal(n, e)
	}
	if c.bytesRead.Load() != 3 {
		t.Fatal(c.bytesRead.Load())
	}
	buffer := buf.NewSize(32)
	defer buffer.Release()
	_, _ = buffer.Write([]byte("prefix"))
	c = NewPacketPairCallBackConn(&packetPairBufferConn{reader: bytes.NewReader([]byte("data")), err: io.EOF})
	if e := c.ReadBuffer(buffer); e != io.EOF {
		t.Fatal(e)
	}
	if c.bytesRead.Load() != 4 {
		t.Fatalf("counted existing prefix: %d", c.bytesRead.Load())
	}
	c = NewPacketPairCallBackConn(&packetPairBufferConn{reader: bytes.NewReader(nil), err: io.ErrClosedPipe})
	if e := c.ReadBuffer(nil); e != io.ErrClosedPipe {
		t.Fatal(e)
	}
	if c.Readout() != (PacketPairResult{}) {
		t.Fatal(c.Readout())
	}
	if c.ReaderReplaceable() || !c.WriterReplaceable() || c.Upstream() != c.Conn {
		t.Fatal("invalid peel contract")
	}
	_, counters := c.UnwrapReader()
	counters[0](-1)
	counters[0](0)
	counters[0](PPProbeBytes)
	if c.Readout().BytesTo64KMs == 0 {
		t.Fatal("copy counter was not retained")
	}
}
func TestPacketPairConcurrentAccounting(t *testing.T) {
	c := NewPacketPairCallBackConn(nil)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				c.accountRead(1024, time.Now())
				_ = c.Readout()
			}
		}()
	}
	wg.Wait()
	if c.Readout().BytesTo64KMs <= 0 {
		t.Fatal(c.Readout())
	}
}

type lifecyclePacketConn struct {
	C.PacketConn
	err error
}

func (c *lifecyclePacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	return nil, nil, nil, c.err
}
func (c *lifecyclePacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, c.err }
func (c *lifecyclePacketConn) WriteTo([]byte, net.Addr) (int, error)  { return 0, c.err }
func TestLifecycleAndTransferError(t *testing.T) {
	local := []error{net.ErrClosed, os.ErrDeadlineExceeded, io.ErrClosedPipe, context.Canceled}
	for _, e := range local {
		e = fmt.Errorf("wrapped: %w", e)
		if !IsConnectionLifecycle(e) || TransferError(io.EOF, e) != nil {
			t.Fatal(e)
		}
		var rd, wr atomic.TypedValue[error]
		c := NewErrorCallBackPacketConn(&lifecyclePacketConn{err: e}, &rd, &wr)
		c.WaitReadFrom()
		c.ReadFrom(nil)
		c.WriteTo(nil, nil)
		if rd.Load() != nil || wr.Load() != nil {
			t.Fatal("local closure recorded", rd.Load(), wr.Load())
		}
	}
	real := errors.New("upstream reset")
	if IsConnectionLifecycle(real) || TransferError(io.EOF, context.Canceled, real) != real {
		t.Fatal("real error hidden")
	}
	if TransferError(io.ErrUnexpectedEOF) != io.ErrUnexpectedEOF || TransferError() != nil {
		t.Fatal("wrong EOF classification")
	}
	var rd, wr atomic.TypedValue[error]
	pc := NewErrorCallBackPacketConn(&lifecyclePacketConn{err: real}, &rd, &wr)
	pc.WaitReadFrom()
	pc.WriteTo(nil, nil)
	if rd.Load() != real || wr.Load() != real {
		t.Fatal("real UDP error hidden")
	}
}
