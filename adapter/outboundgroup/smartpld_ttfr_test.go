package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/bufio"
)

func ttfrClosureSnapshot(t *testing.T, s *SmartPLD, p C.Proxy, md *C.Metadata) *smart.PLDRecord {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := json.Marshal(s.getPLDRecord(md.SmartTarget, p.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var r smart.PLDRecord
		if err = json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		if r.AttemptCount > 0 {
			// Wait for the asynchronous non-neutral settlement's target lock.
			lock := smart.GetTargetNodeLock(md.SmartTarget, s.Name(), p.Name())
			lock.Lock()
			lock.Unlock()
			s.store.FlushQueue(true)
			return &r
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("closure observation did not settle")
	return nil
}

func ttfrAssertLegacyReward(t *testing.T, s *SmartPLD, p C.Proxy, md *C.Metadata, r *smart.PLDRecord, bytes int64, failed bool) int64 {
	t.Helper()
	key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), md.SmartTarget, p.Name())
	a := s.store.GetOrCreateAtomicRecord(key, s.Name(), s.configName, md.SmartTarget, p.Name())
	latency := a.Latency()
	want := smart.ComputeReward(smart.RewardedMetrics{IsUDP: md.NetWork == C.UDP, ConnectTimeMs: 5, FirstByteMs: latency, DownloadMB: float64(bytes) / (1024 * 1024), Failed: failed})
	if math.Abs(r.Reward-want) > 1e-12 || r.SampleCount != 1 {
		t.Fatalf("legacy reward changed: got=%v want=%v samples=%d latency=%d", r.Reward, want, r.SampleCount, latency)
	}
	return latency
}

type ttfrClosureReadConn struct {
	net.Conn
	zeroFirst bool
	terminal  error
}

func (c *ttfrClosureReadConn) Read(b []byte) (int, error) {
	if c.zeroFirst {
		c.zeroFirst = false
		return 0, nil
	}
	n, err := c.Conn.Read(b)
	if n > 0 && c.terminal != nil {
		err = c.terminal
	}
	return n, err
}

func TestTTFRShadowTCPActualClosure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		zeroFirst bool
		terminal  error
	}{
		{"positive", false, nil}, {"zero_read_then_payload", true, nil},
		{"payload_and_eof", false, io.EOF}, {"payload_and_failure", false, errors.New("transport failed after data")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			client, server := net.Pipe()
			defer server.Close()
			base := &ttfrClosureReadConn{Conn: client, zeroFirst: tc.zeroFirst, terminal: tc.terminal}
			c := s.WrapConnWithMetric(outbound.NewConn(base, p.Adapter()), p, md, 5)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
			defer tracker.Close()
			if tc.zeroFirst {
				if n, e := tracker.Read(make([]byte, 1)); n != 0 || e != nil {
					t.Fatalf("zero read=%d %v", n, e)
				}
			}
			time.Sleep(60 * time.Millisecond)
			done := make(chan error, 1)
			go func() { _, e := server.Write([]byte("x")); done <- e }()
			if n, e := tracker.Read(make([]byte, 1)); n != 1 || !errors.Is(e, tc.terminal) {
				t.Fatalf("read=%d %v", n, e)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			_ = tracker.Close()
			_ = tracker.Close()
			r := ttfrClosureSnapshot(t, s, p, md)
			if r.TCPConnectionTTFR_Cnt != 1 || r.UDPResponseCnt != 0 || r.AttemptCount != 1 || r.TCPConnectionTTFR_EMA < 55 {
				t.Fatalf("TCP raw clock/count=%+v", r)
			}
			failed := tc.terminal != nil && !errors.Is(tc.terminal, io.EOF)
			legacy := ttfrAssertLegacyReward(t, s, p, md, r, 1, failed)
			if tc.zeroFirst && (legacy >= 30 || r.TCPConnectionTTFR_EMA < float64(legacy)+40) {
				t.Fatalf("shadow borrowed first-read clock: legacy=%d raw=%v", legacy, r.TCPConnectionTTFR_EMA)
			}
		})
	}
}

func TestTTFRShadowTCPNoPayloadAndNeutral(t *testing.T) {
	for _, terminal := range []error{io.EOF, errors.New("reset before payload"), context.Canceled} {
		t.Run(terminal.Error(), func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			client, server := net.Pipe()
			defer server.Close()
			c := s.WrapConnWithMetric(outbound.NewConn(&outcomeReadErrorConn{Conn: client, err: terminal}, p.Adapter()), p, md, 5)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
			if n, e := tracker.Read(make([]byte, 1)); n != 0 || !errors.Is(e, terminal) {
				t.Fatalf("zero terminal read=%d %v", n, e)
			}
			_ = tracker.Close()
			r := ttfrClosureSnapshot(t, s, p, md)
			if r.AttemptCount != 1 || r.ConnectCnt != 1 || r.TCPConnectionTTFR_Cnt != 0 || r.UDPResponseCnt != 0 {
				t.Fatalf("no payload became TTFR: %+v", r)
			}
			if errors.Is(terminal, context.Canceled) && (r.SampleCount != 0 || r.SampleCountLarge != 0 || r.FailureCount != 0) {
				t.Fatal("neutral closure changed legacy reward")
			}
		})
	}
}

func TestTTFRShadowTCPActualCopyCounters(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	client, server := net.Pipe()
	defer server.Close()
	c := s.WrapConnWithMetric(outbound.NewConn(client, p.Adapter()), p, md, 5)
	tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
	done := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, e := server.Write([]byte("actual relay payload"))
		_ = server.Close()
		done <- e
	}()
	n, err := bufio.Copy(io.Discard, tracker)
	if err != nil || n != 20 {
		t.Fatalf("copy=%d %v", n, err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	_ = tracker.Close()
	r := ttfrClosureSnapshot(t, s, p, md)
	if r.TCPConnectionTTFR_Cnt != 1 || r.TCPConnectionTTFR_EMA < 15 {
		t.Fatalf("unwrapped copy lost TTFR: %+v", r)
	}
}

type ttfrClosurePacketConn struct {
	C.PacketConn
	terminal     error
	writeDelay   time.Duration
	writeRelease <-chan struct{}
}

func (c *ttfrClosurePacketConn) WriteTo(b []byte, a net.Addr) (int, error) {
	n, e := c.PacketConn.WriteTo(b, a)
	time.Sleep(c.writeDelay)
	if c.writeRelease != nil {
		<-c.writeRelease
	}
	return n, e
}
func (c *ttfrClosurePacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, e := c.PacketConn.ReadFrom(b)
	if n > 0 && c.terminal != nil {
		e = c.terminal
	}
	return n, a, e
}
func (c *ttfrClosurePacketConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	b, put, a, e := c.PacketConn.WaitReadFrom()
	if len(b) > 0 && c.terminal != nil {
		e = c.terminal
	}
	return b, put, a, e
}

func TestTTFRShadowUDPActualPacketClosure(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		send, preSend, waitRead bool
		terminal                error
	}{
		{"response", true, false, false, nil}, {"wait_response", true, false, true, nil},
		{"pre_send_only", false, true, false, nil}, {"pre_send_then_response", true, true, false, nil},
		{"payload_and_eof", true, false, false, io.EOF}, {"wait_payload_and_failure", true, false, true, errors.New("packet failed after payload")},
		{"unsent_close", false, false, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			md.NetWork = C.UDP
			client, e := net.ListenPacket("udp4", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			server, e := net.ListenPacket("udp4", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer server.Close()
			_ = client.SetDeadline(time.Now().Add(3 * time.Second))
			_ = server.SetDeadline(time.Now().Add(3 * time.Second))
			base := &ttfrClosurePacketConn{PacketConn: outbound.NewPacketConn(client, p.Adapter().(outbound.ProxyAdapter)), terminal: tc.terminal, writeDelay: 40 * time.Millisecond}
			pc := s.WrapPacketConnWithMetric(base, p, md, 5)
			tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, md, nil, 0, 0, false)
			defer tracker.Close()
			var downloaded int64
			read := func() {
				t.Helper()
				if tc.waitRead {
					b, put, _, err := tracker.WaitReadFrom()
					if put != nil {
						defer put()
					}
					if len(b) != 1 || !errors.Is(err, tc.terminal) {
						t.Fatalf("WaitReadFrom=%d %v", len(b), err)
					}
					downloaded += int64(len(b))
				} else {
					n, _, err := tracker.ReadFrom(make([]byte, 4))
					if n != 1 || !errors.Is(err, tc.terminal) {
						t.Fatalf("ReadFrom=%d %v", n, err)
					}
					downloaded += int64(n)
				}
			}
			if tc.preSend {
				if _, e = server.WriteTo([]byte("p"), client.LocalAddr()); e != nil {
					t.Fatal(e)
				}
				read()
			}
			time.Sleep(60 * time.Millisecond) // Deliberately separate wrapper install from request.
			if tc.send {
				if n, e := tracker.WriteTo([]byte("q"), server.LocalAddr()); n != 1 || e != nil {
					t.Fatalf("send=%d %v", n, e)
				}
				if n, _, e := server.ReadFrom(make([]byte, 4)); n != 1 || e != nil {
					t.Fatalf("server request=%d %v", n, e)
				}
				time.Sleep(20 * time.Millisecond)
				if _, e = server.WriteTo([]byte("r"), client.LocalAddr()); e != nil {
					t.Fatal(e)
				}
				read()
			}
			_ = tracker.Close()
			_ = tracker.Close()
			r := ttfrClosureSnapshot(t, s, p, md)
			wantCnt := int64(0)
			if tc.send {
				wantCnt = 1
			}
			if r.AttemptCount != 1 || r.TCPConnectionTTFR_Cnt != 0 || r.UDPResponseCnt != wantCnt || r.LossCnt != 0 {
				t.Fatalf("UDP contaminated TCP/missing loss/count: %+v", r)
			}
			failed := tc.terminal != nil && !errors.Is(tc.terminal, io.EOF)
			legacy := ttfrAssertLegacyReward(t, s, p, md, r, downloaded, failed)
			if tc.send && (r.UDPResponseEMA < 55 || r.UDPResponseEMA < float64(legacy)+30) {
				t.Fatalf("independent packet origin missing: raw=%v legacy=%d", r.UDPResponseEMA, legacy)
			}
			if tc.send && tc.terminal != nil && legacy != 0 {
				t.Fatalf("legacy callback error behavior changed: %d", legacy)
			}
		})
	}
}

func TestTTFRShadowUDPZeroDatagramDoesNotBecomeResponse(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	md.NetWork = C.UDP
	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))
	pc := s.WrapPacketConnWithMetric(outbound.NewPacketConn(client, p.Adapter().(outbound.ProxyAdapter)), p, md, 5)
	tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, md, nil, 0, 0, false)
	defer tracker.Close()
	if _, err = tracker.WriteTo([]byte("q"), server.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = server.ReadFrom(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err = server.WriteTo(nil, client.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if n, _, err := tracker.ReadFrom(make([]byte, 4)); n != 0 || err != nil {
		t.Fatalf("zero datagram=%d %v", n, err)
	}
	_ = tracker.Close()
	r := ttfrClosureSnapshot(t, s, p, md)
	if r.UDPResponseCnt != 0 || r.TCPConnectionTTFR_Cnt != 0 {
		t.Fatal("zero datagram created TTFR")
	}
	if legacy := ttfrAssertLegacyReward(t, s, p, md, r, 0, false); legacy < 15 {
		t.Fatal("legacy zero-datagram timer changed")
	}
}

// This real UDP request/response completes while the payload write is still
// returning, so shadow timing cannot depend on the legacy post-write stamp.
func TestTTFRShadowUDPResponseDuringPendingSend(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	md.NetWork = C.UDP
	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	base := &ttfrClosurePacketConn{PacketConn: outbound.NewPacketConn(client, p.Adapter().(outbound.ProxyAdapter)), writeRelease: release}
	pc := s.WrapPacketConnWithMetric(base, p, md, 5)
	tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, md, nil, 0, 0, false)
	defer tracker.Close()
	done := make(chan error, 1)
	go func() { _, e := tracker.WriteTo([]byte("q"), server.LocalAddr()); done <- e }()
	if _, _, err = server.ReadFrom(make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err = server.WriteTo([]byte("r"), client.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	if n, _, e := tracker.ReadFrom(make([]byte, 4)); n != 1 || e != nil {
		t.Fatalf("pending response=%d %v", n, e)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	_ = tracker.Close()
	r := ttfrClosureSnapshot(t, s, p, md)
	if r.UDPResponseCnt != 1 || r.TCPConnectionTTFR_Cnt != 0 {
		t.Fatalf("pending response lost: %+v", r)
	}
	if legacy := ttfrAssertLegacyReward(t, s, p, md, r, 1, false); legacy != 0 {
		t.Fatalf("legacy timer changed: %d", legacy)
	}
}

func TestTTFRShadowSettlementUnknownAndExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name        string
		network     C.NetWork
		err         error
		download    int64
		observation *pldTTFRObservation
	}{
		{"tcp_unknown", C.TCP, nil, 1024, nil}, {"udp_unknown", C.UDP, nil, 1024, nil},
		{"failure_unknown", C.TCP, errors.New("dial failed"), 0, nil}, {"neutral_unknown", C.UDP, context.Canceled, 0, nil},
		{"tcp_known", C.TCP, nil, 1024, &pldTTFRObservation{origin: time.Now().Add(-time.Second), first: time.Now()}},
		{"udp_known", C.UDP, nil, 1024, &pldTTFRObservation{isUDP: true, origin: time.Now().Add(-time.Second), first: time.Now()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			md.NetWork = tc.network
			o := &pldConnectionOutcome{}
			for i := 0; i < 2; i++ {
				s.recordConnectionStats(md, p, 5, 90, 0, tc.download, 0, 0, 1000, nil, 0, tc.err, o, tc.observation)
			}
			r := ttfrClosureSnapshot(t, s, p, md)
			if r.AttemptCount != 1 {
				t.Fatalf("duplicate settlement=%d", r.AttemptCount)
			}
			want := int64(0)
			if tc.observation != nil {
				want = 1
			}
			if r.TCPConnectionTTFR_Cnt+r.UDPResponseCnt != want {
				t.Fatal("unknown clock inferred from legacy or known sample dropped")
			}
			if errors.Is(tc.err, context.Canceled) && r.SampleCount != 0 {
				t.Fatal("neutral settlement trained legacy reward")
			}
		})
	}
}
