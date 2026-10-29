package callback_test

import (
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

func TestPacketPairOptimizedCopyPreservesRawSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	server.SetDeadline(time.Now().Add(3 * time.Second))
	p := callback.NewPacketPairCallBackConn(outbound.NewConn(client, outbound.NewDirect()))
	reader, counters := N.UnwrapCountReader(p, nil)
	if _, ok := reader.(syscall.Conn); !ok {
		t.Fatalf("sampling disabled raw socket fast path: %T", reader)
	}
	if len(counters) != 1 {
		t.Fatalf("expected one retained sampling counter, got %d", len(counters))
	}
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		_, e := server.Write(make([]byte, callback.PPProbeBytes/2))
		if e == nil {
			time.Sleep(15 * time.Millisecond)
			_, e = server.Write(make([]byte, callback.PPProbeBytes*2))
		}
		done <- e
	}()
	// This is the actual sing relay, not io.Copy or manual callback invocation.
	n, err := bufio.Copy(io.Discard, callback.NewCloseCallbackConn(p, func() {}))
	if err != nil {
		t.Fatal(err)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if want := int64(callback.PPProbeBytes/2 + callback.PPProbeBytes*2); n != want {
		t.Fatal(n, want)
	}
	if ro := p.Readout(); ro.BytesTo64KMs <= 0 {
		t.Fatalf("real optimized relay bypassed sampling: %+v", ro)
	} else {
		t.Logf("raw socket retained, copied=%d, timing=%+v", n, ro)
	}
}
