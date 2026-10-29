package callback

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

type callbackCloseOnlyConn struct {
	C.Conn
	closes atomic.Int64
}

func (c *callbackCloseOnlyConn) Close() error { c.closes.Add(1); return nil }

type callbackHalfCloseConn struct {
	callbackCloseOnlyConn
	writes atomic.Int64
	err    error
}

func (c *callbackHalfCloseConn) CloseWrite() error { c.writes.Add(1); return c.err }

type callbackCloseOnlyPacketConn struct {
	C.PacketConn
	closes atomic.Int64
}

func (c *callbackCloseOnlyPacketConn) Close() error { c.closes.Add(1); return nil }

type callbackHalfClosePacketConn struct {
	callbackCloseOnlyPacketConn
	writes atomic.Int64
}

func (c *callbackHalfClosePacketConn) CloseWrite() error { c.writes.Add(1); return nil }

func TestCloseCallbackHalfCloseDefersFinalization(t *testing.T) {
	for _, fail := range []bool{false, true} {
		base := &callbackHalfCloseConn{}
		if fail {
			base.err = errors.New("shutdown-write failed")
		}
		var finalized atomic.Int64
		inner := NewErrorCallBackConn(base, nil, nil)
		inner = NewFirstReadCallBackConn(inner, func(error) {})
		inner = NewPacketPairCallBackConn(inner)
		conn := NewCloseCallbackConn(inner, func() { finalized.Add(1) })
		if err := conn.(*closeCallbackConn).CloseWrite(); !errors.Is(err, base.err) {
			t.Fatalf("CloseWrite err=%v", err)
		}
		if base.writes.Load() != 1 || base.closes.Load() != 0 || finalized.Load() != 0 {
			t.Fatalf("half-close writes=%d closes=%d finalized=%d", base.writes.Load(), base.closes.Load(), finalized.Load())
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _ = conn.Close() }()
		}
		wg.Wait()
		if finalized.Load() != 1 {
			t.Fatalf("full Close finalizations=%d", finalized.Load())
		}
	}
}

func TestCloseCallbackFallbackFinalizes(t *testing.T) {
	base := &callbackCloseOnlyConn{}
	var finalized atomic.Int64
	conn := NewCloseCallbackConn(base, func() { finalized.Add(1) })
	if err := conn.(*closeCallbackConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if base.closes.Load() != 1 || finalized.Load() != 1 {
		t.Fatal("non-duplex fallback did not finalize full Close")
	}
	_ = conn.Close()
	if finalized.Load() != 1 {
		t.Fatal("fallback finalized twice")
	}
}

func TestCloseCallbackPacketHalfCloseAndFallback(t *testing.T) {
	base := &callbackHalfClosePacketConn{}
	var finalized atomic.Int64
	inner := NewErrorCallBackPacketConn(base, nil, nil)
	conn := NewCloseCallbackPacketConn(inner, func() { finalized.Add(1) })
	if err := conn.(*closeCallbackPacketConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if base.writes.Load() != 1 || base.closes.Load() != 0 || finalized.Load() != 0 {
		t.Fatal("packet half-close finalized or closed read direction")
	}
	_ = conn.Close()
	_ = conn.Close()
	if finalized.Load() != 1 {
		t.Fatal("packet full Close must finalize once")
	}
	fallback := &callbackCloseOnlyPacketConn{}
	finalized.Store(0)
	conn = NewCloseCallbackPacketConn(fallback, func() { finalized.Add(1) })
	if err := conn.(*closeCallbackPacketConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if fallback.closes.Load() != 1 || finalized.Load() != 1 {
		t.Fatal("packet fallback did not finalize")
	}
}
