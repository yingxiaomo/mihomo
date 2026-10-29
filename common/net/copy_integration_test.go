package net_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	A "github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/bufio"
	SN "github.com/metacubex/sing/common/network"
)

type copyTestConn struct{ N.ExtendedConn }

func (c *copyTestConn) Chains() C.Chain               { return C.Chain{"copy-test"} }
func (c *copyTestConn) ProviderChains() C.Chain       { return nil }
func (c *copyTestConn) AppendToChains(C.ProxyAdapter) {}
func (c *copyTestConn) RemoteDestination() string     { return c.RemoteAddr().String() }
func (c *copyTestConn) Upstream() any                 { return c.ExtendedConn }
func (c *copyTestConn) ReaderReplaceable() bool       { return true }
func (c *copyTestConn) WriterReplaceable() bool       { return true }
func copyConn(c net.Conn) C.Conn                      { return &copyTestConn{N.NewExtendedConn(c)} }

type copyMonitoredTCP struct {
	*net.TCPConn
	reads, writes atomic.Int64
}

func (c *copyMonitoredTCP) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.TCPConn.Read(p)
}
func (c *copyMonitoredTCP) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.TCPConn.Write(p)
}

func copyTCPPair(t *testing.T) (*copyMonitoredTCP, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptTCP()
	if err != nil {
		peer.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); peer.Close() })
	deadline := time.Now().Add(5 * time.Second)
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	return &copyMonitoredTCP{TCPConn: conn}, peer
}

func copyReset(t *testing.T, peer *net.TCPConn) {
	t.Helper()
	if err := peer.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
}

func copyIsReset(err error) bool {
	// Windows exposes Winsock's WSAECONNRESET, not the POSIX errno constant.
	return errors.Is(err, syscall.ECONNRESET) ||
		(runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(10054)))
}

func copyObserved(conn C.Conn) (C.Conn, *A.TypedValue[error], *A.TypedValue[error]) {
	readErr, writeErr := A.NewTypedValue[error](nil), A.NewTypedValue[error](nil)
	return callback.NewErrorCallBackConn(conn, &readErr, &writeErr), &readErr, &writeErr
}

func TestProductionCopyTCPReadRSTDirection(t *testing.T) {
	for _, rawDestination := range []bool{false, true} {
		t.Run(map[bool]string{false: "extended", true: "raw-capable"}[rawDestination], func(t *testing.T) {
			source, peer := copyTCPPair(t)
			observed, readErr, writeErr := copyObserved(copyConn(source))
			var destination io.Writer = io.Discard
			var destinationWriteErr *A.TypedValue[error]
			if rawDestination {
				dst, _ := copyTCPPair(t)
				var dstConn C.Conn
				dstConn, _, destinationWriteErr = copyObserved(copyConn(dst))
				destination = dstConn
			}
			copyReset(t, peer)
			n, err := N.Copy(destination, observed)
			if err == nil || !copyIsReset(err) {
				t.Fatalf("expected actual TCP RST, copied=%d err=%v", n, err)
			}
			if callback.TransferError(readErr.Load()) == nil || writeErr.Load() != nil {
				t.Fatalf("read RST attribution: read=%v write=%v", readErr.Load(), writeErr.Load())
			}
			if destinationWriteErr != nil && destinationWriteErr.Load() != nil {
				t.Fatalf("source RST polluted destination write: %v", destinationWriteErr.Load())
			}
			if runtime.GOOS == "linux" && rawDestination && source.reads.Load() != 0 {
				t.Fatal("raw-capable copy did not use splice")
			}
			t.Logf("copied=%d real read RST=%v", n, err)
		})
	}
}

func TestProductionCopyTCPWriteRSTDirection(t *testing.T) {
	for _, rawSource := range []bool{false, true} {
		t.Run(map[bool]string{false: "extended", true: "raw-capable"}[rawSource], func(t *testing.T) {
			destination, peer := copyTCPPair(t)
			copyReset(t, peer)
			// Consume the peer's actual TCP reset before starting the write. This
			// avoids a successful buffered write racing delivery of the RST.
			if _, err := destination.TCPConn.Read(make([]byte, 1)); !copyIsReset(err) {
				t.Fatalf("peer did not reset TCP: %v", err)
			}
			observed, dstRead, dstWrite := copyObserved(copyConn(destination))
			payload := bytes.Repeat([]byte("write-rst"), 1024)
			var source io.Reader = bytes.NewReader(payload)
			var sourceRead *A.TypedValue[error]
			var monitoredSource *copyMonitoredTCP
			if rawSource {
				var sourcePeer *net.TCPConn
				monitoredSource, sourcePeer = copyTCPPair(t)
				var conn C.Conn
				conn, sourceRead, _ = copyObserved(copyConn(monitoredSource))
				source = conn
				if _, err := sourcePeer.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := sourcePeer.CloseWrite(); err != nil {
					t.Fatal(err)
				}
			}
			n, err := N.Copy(observed, source)
			if callback.TransferError(err) == nil || callback.TransferError(dstWrite.Load()) == nil {
				t.Fatalf("lost real write failure: copied=%d err=%v slot=%v", n, err, dstWrite.Load())
			}
			if dstRead.Load() != nil || (sourceRead != nil && callback.TransferError(sourceRead.Load()) != nil) {
				t.Fatalf("write RST reported to read direction: destination=%v", dstRead.Load())
			}
			if runtime.GOOS == "linux" && rawSource && (destination.writes.Load() != 0 || monitoredSource.reads.Load() != 0) {
				t.Fatal("raw-capable copy did not use splice")
			}
			t.Logf("copied=%d real write failure=%v", n, err)
		})
	}
}

func TestProductionCopyMidTransferTCPRST(t *testing.T) {
	source, sourcePeer := copyTCPPair(t)
	destination, destinationPeer := copyTCPPair(t)
	observedSource, sourceRead, sourceWrite := copyObserved(copyConn(source))
	observedDestination, _, destinationWrite := copyObserved(copyConn(destination))
	var firstCalls atomic.Int64
	firstSource := callback.NewFirstReadCallBackConn(observedSource, func(err error) {
		firstCalls.Add(1)
		if err != nil {
			t.Errorf("first read failed before reset: %v", err)
		}
	})
	payload := bytes.Repeat([]byte("before-reset"), 16*1024)
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() { n, err := N.Copy(observedDestination, firstSource); done <- result{n, err} }()
	written := make(chan error, 1)
	go func() { _, err := sourcePeer.Write(payload); written <- err }()
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(destinationPeer, received); err != nil || !bytes.Equal(received, payload) {
		t.Fatalf("transfer before reset failed: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	copyReset(t, sourcePeer)
	var copied result
	select {
	case copied = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("copy blocked after mid-transfer RST")
	}
	if copied.n != int64(len(payload)) || !copyIsReset(copied.err) || !copyIsReset(sourceRead.Load()) {
		t.Fatalf("mid-transfer reset lost: copied=%d want=%d err=%v slot=%v", copied.n, len(payload), copied.err, sourceRead.Load())
	}
	if sourceWrite.Load() != nil || destinationWrite.Load() != nil {
		t.Fatalf("mid-transfer read RST polluted writer: source=%v destination=%v", sourceWrite.Load(), destinationWrite.Load())
	}
	if firstCalls.Load() != 1 {
		t.Fatalf("first-read callback calls=%d", firstCalls.Load())
	}
	if runtime.GOOS == "linux" && (source.reads.Load() > 1 || destination.writes.Load() > 1) {
		t.Fatal("mid-transfer copy did not return to splice after first read")
	}
	t.Logf("transferred=%d before real RST: %v; destination write remains nil", copied.n, copied.err)
}

func TestProductionRelayClientRSTDoesNotPolluteUpstream(t *testing.T) {
	client, clientPeer := copyTCPPair(t)
	upstream, _ := copyTCPPair(t)
	clientConn, clientRead, clientWrite := copyObserved(copyConn(client))
	upstreamConn, upstreamRead, upstreamWrite := copyObserved(copyConn(upstream))
	copyReset(t, clientPeer)
	done := make(chan struct{})
	go func() { N.Relay(clientConn, upstreamConn); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not stop after client RST")
	}
	if callback.TransferError(clientRead.Load()) == nil {
		t.Fatalf("client read reset was lost: %v", clientRead.Load())
	}
	if callback.TransferError(upstreamRead.Load(), upstreamWrite.Load()) != nil {
		t.Fatalf("client reset polluted upstream: read=%v write=%v", upstreamRead.Load(), upstreamWrite.Load())
	}
	t.Logf("clientRead=%v clientWrite=%v upstreamRead=%v upstreamWrite=%v",
		clientRead.Load(), clientWrite.Load(), upstreamRead.Load(), upstreamWrite.Load())
}

func TestProductionRelayPreservesTCPHalfClose(t *testing.T) {
	client, clientPeer := copyTCPPair(t)
	upstream, upstreamPeer := copyTCPPair(t)
	left, leftRead, leftWrite := copyObserved(copyConn(client))
	right, rightRead, rightWrite := copyObserved(copyConn(upstream))
	done := make(chan struct{})
	go func() { N.Relay(left, right); close(done) }()
	request, response := []byte("request"), []byte("response-after-client-fin")
	if _, err := clientPeer.Write(request); err != nil {
		t.Fatal(err)
	}
	if err := clientPeer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	received, err := io.ReadAll(upstreamPeer)
	if err != nil || !bytes.Equal(received, request) {
		t.Fatalf("upload/FIN lost: data=%q err=%v", received, err)
	}
	if _, err := upstreamPeer.Write(response); err != nil {
		t.Fatal(err)
	}
	if err := upstreamPeer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	received, err = io.ReadAll(clientPeer)
	if err != nil || !bytes.Equal(received, response) {
		t.Fatalf("download after FIN lost: data=%q err=%v", received, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not complete after two FINs")
	}
	if err := callback.TransferError(leftRead.Load(), leftWrite.Load(), rightRead.Load(), rightWrite.Load()); err != nil {
		t.Fatal(err)
	}
}

func TestProductionCopyFirstReadPacketPairTrackerCache(t *testing.T) {
	source, peer := copyTCPPair(t)
	destination, destinationPeer := copyTCPPair(t)
	cached := []byte("cached-prefix:")
	innerCount, outerCount := new(atomic.Int64), new(atomic.Int64)
	inner := bufio.NewInt64CounterConn(N.NewCachedConn(source, cached), []*atomic.Int64{innerCount}, nil)
	observed, readErr, writeErr := copyObserved(copyConn(inner))
	pair := callback.NewPacketPairCallBackConn(observed)
	var firstCalls atomic.Int64
	first := callback.NewFirstReadCallBackConn(pair, func(err error) {
		firstCalls.Add(1)
		if err != nil {
			t.Errorf("first read: %v", err)
		}
	})
	outer := bufio.NewInt64CounterConn(first, []*atomic.Int64{outerCount}, nil)
	sourceTracker := statistic.NewTCPTracker(copyConn(outer), statistic.DefaultManager, &C.Metadata{}, nil, 0, 0, false)
	t.Cleanup(func() { sourceTracker.Close() })
	dstObserved, _, dstWrite := copyObserved(copyConn(destination))
	destinationTracker := statistic.NewTCPTracker(dstObserved, statistic.DefaultManager, &C.Metadata{}, nil, 0, 0, false)
	t.Cleanup(func() { destinationTracker.Close() })
	payload := bytes.Repeat([]byte("native-copy"), 32*1024)
	writeDone := make(chan error, 1)
	go func() {
		_, err := peer.Write(payload)
		if err == nil {
			err = peer.CloseWrite()
		}
		writeDone <- err
	}()
	receiveDone := make(chan []byte, 1)
	go func() {
		data, err := io.ReadAll(destinationPeer)
		if err != nil {
			t.Errorf("destination read: %v", err)
		}
		receiveDone <- data
	}()
	n, err := N.Copy(destinationTracker, sourceTracker)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	received := <-receiveDone
	want := append(append([]byte(nil), cached...), payload...)
	if !bytes.Equal(received, want) || n != int64(len(want)) {
		t.Fatalf("copy/cache mismatch: n=%d received=%d want=%d", n, len(received), len(want))
	}
	for name, count := range map[string]int64{
		"inner counter": innerCount.Load(), "outer counter": outerCount.Load(),
		"tracker download": sourceTracker.Info().DownloadTotal.Load(),
		"tracker upload":   destinationTracker.Info().UploadTotal.Load(),
	} {
		if count != n {
			t.Fatalf("%s = %d, want %d", name, count, n)
		}
	}
	if firstCalls.Load() != 1 || pair.Readout().BytesTo64KMs <= 0 {
		t.Fatalf("first read / packet-pair bypassed: calls=%d timing=%+v", firstCalls.Load(), pair.Readout())
	}
	if callback.TransferError(readErr.Load(), writeErr.Load(), dstWrite.Load()) != nil {
		t.Fatalf("successful transfer reported failure: read=%v write=%v dst=%v", readErr.Load(), writeErr.Load(), dstWrite.Load())
	}
	if runtime.GOOS == "linux" && (source.reads.Load() != 0 || destination.writes.Load() > 1) {
		t.Fatalf("first-read/cache blocked splice: reads=%d writes=%d", source.reads.Load(), destination.writes.Load())
	}
	t.Logf("copied=%d firstCalls=%d counters match; packetPair=%+v syscall-path reads=%d writes=%d",
		n, firstCalls.Load(), pair.Readout(), source.reads.Load(), destination.writes.Load())
}

var _ SN.ReadCounter = (*bufio.CounterConn)(nil)
