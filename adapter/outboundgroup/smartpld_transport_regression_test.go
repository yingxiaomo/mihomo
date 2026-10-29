package outboundgroup

import (
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/bufio"
)

func waitTransportStats(t *testing.T, s *SmartPLD, md *C.Metadata, node string) {
	t.Helper()
	key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), md.SmartTarget, node)
	until := time.Now().Add(3 * time.Second)
	for {
		s.store.FlushQueue(true)
		data, err := s.store.DBViewGetItem(key)
		if err == nil && len(data) > 0 {
			// The snapshot can become visible before recordConnectionStats returns.
			lock := smart.GetTargetNodeLock(md.SmartTarget, s.Name(), node)
			lock.Lock()
			lock.Unlock()
			return
		}
		if time.Now().After(until) {
			t.Fatal("close statistics never committed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPLDExplicitCloseWithoutResponseIsNeutral(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "without-read"
		if pending {
			name = "pending-read"
		}
		t.Run(name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			md.DstPort = 443
			r, w := net.Pipe()
			defer w.Close()
			var latency atomic.Int64
			var first, readErr, writeErr atomic.TypedValue[error]
			o := &pldConnectionOutcome{}
			base := callback.NewErrorCallBackConn(outbound.NewConn(r, p.Adapter()), &readErr, &writeErr)
			base = callback.NewFirstReadCallBackConn(base, func(err error) { first.Store(o.readError(err)) })
			c := s.registerClosureMetricsCallback(base, p, md, 5, &latency, &first, &readErr, &writeErr, o, nil)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
			tracker.Info().Start = time.Now().Add(-200 * time.Millisecond)
			var done chan error
			if pending {
				done = make(chan error, 1)
				started := make(chan struct{})
				go func() { close(started); _, err := tracker.Read(make([]byte, 1)); done <- err }()
				<-started
			}
			if err := tracker.Close(); err != nil {
				t.Fatal(err)
			}
			if pending {
				select {
				case err := <-done:
					if !callback.IsConnectionLifecycle(err) {
						t.Fatalf("pending read returned %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Close did not release pending read")
				}
			}
			until := time.Now().Add(3 * time.Second)
			for {
				// The outcome lock is held for the entire final statistics transaction.
				o.mu.Lock()
				recorded := o.recorded
				o.mu.Unlock()
				if recorded {
					break
				}
				if time.Now().After(until) {
					t.Fatal("final callback did not complete")
				}
				time.Sleep(time.Millisecond)
			}
			if n := outcomeHostStrikes(t, s, md, p.Name()); n != 0 {
				t.Fatalf("local Close charged host: %d", n)
			}
			_, _, count := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
			if count != 0 {
				t.Fatalf("local Close created reward sample: %d", count)
			}
		})
	}
}

func transportTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.AcceptTCP()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	if err = client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = server.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func TestPLDRawCopyResetRecordsFailure(t *testing.T) {
	for _, production := range []bool{false, true} {
		name, copyConn := "sing", bufio.Copy
		if production {
			name, copyConn = "production", N.Copy
		}
		t.Run(name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			client, server := transportTCPPair(t)
			c := s.WrapConnWithMetric(outbound.NewConn(client, p.Adapter()), p, md, 5)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
			done := make(chan error, 1)
			go func() {
				_, err := server.Write(make([]byte, callback.PPProbeBytes))
				time.Sleep(20 * time.Millisecond)
				if err == nil {
					err = server.SetLinger(0)
				}
				if err == nil {
					err = server.Close()
				}
				done <- err
			}()
			n, err := copyConn(io.Discard, tracker)
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			// Windows TCP RST discards the receive buffer (Linux retains it),
			// so the copy may see fewer bytes than were sent before the RST.
			// Accept any nonzero transfer with a transfer error on those
			// platforms; require exact byte count elsewhere.
			if runtime.GOOS == "windows" {
				if n == 0 || callback.TransferError(err) == nil {
					t.Fatalf("RST not reached: bytes=%d err=%v", n, err)
				}
			} else {
				if n != callback.PPProbeBytes || callback.TransferError(err) == nil {
					t.Fatalf("RST not reached: bytes=%d err=%v", n, err)
				}
			}
			if e := tracker.Close(); e != nil {
				t.Fatal(e)
			}
			waitTransportStats(t, s, md, p.Name())
			if n := outcomeHostStrikes(t, s, md, p.Name()); n != 1 {
				t.Fatalf("raw copy error lost or double counted: %d", n)
			}
			_, _, count := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
			if count != 1 {
				t.Fatalf("RST reward count=%d", count)
			}
			t.Logf("real TCP copy bytes=%d returned %v; one host strike and reward", n, err)
		})
	}
}

func TestPLDRawWriteResetRecordsFailure(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	client, server := transportTCPPair(t)
	c := s.WrapConnWithMetric(outbound.NewConn(client, p.Adapter()), p, md, 5)
	tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
	if _, err := server.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(tracker, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := server.SetLinger(0); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	var err error
	for i := 0; i < 10; i++ {
		_, err = tracker.Write(make([]byte, 4096))
		if err != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if callback.TransferError(err) == nil {
		t.Fatalf("write RST not reached: %v", err)
	}
	if e := tracker.Close(); e != nil {
		t.Fatal(e)
	}
	waitTransportStats(t, s, md, p.Name())
	if n := outcomeHostStrikes(t, s, md, p.Name()); n != 1 {
		t.Fatalf("raw write error lost or double counted: %d", n)
	}
}

func TestPLDGateAndCloseUseFailureStatistics(t *testing.T) {
	for _, err := range []error{nil, errors.New("upstream reset")} {
		s, p, md := newOutcomeTestGroup(t)
		o := &pldConnectionOutcome{}
		if !s.failPLDConnection(o, md, p.Name(), 3) {
			t.Fatal("gate did not fire")
		}
		o.finish()
		recordOutcomeSample(s, p, md, o, err, 1024)
		recordOutcomeSample(s, p, md, o, err, 1024)
		key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), md.SmartTarget, p.Name())
		rec := s.store.GetOrCreateAtomicRecord(key, s.Name(), s.configName, md.SmartTarget, p.Name())
		if rec.Failure() != 1 || rec.Success() != 0 {
			t.Fatalf("gate failure=%d success=%d", rec.Failure(), rec.Success())
		}
		if n := outcomeHostStrikes(t, s, md, p.Name()); n != 1 {
			t.Fatalf("gate strikes=%d", n)
		}
		_, _, count := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
		if count != 1 {
			t.Fatalf("gate reward count=%d", count)
		}
	}
}
