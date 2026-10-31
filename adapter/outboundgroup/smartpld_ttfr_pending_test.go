package outboundgroup

import (
	"errors"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/metacubex/sing/common/bufio"
	SN "github.com/metacubex/sing/common/network"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestTTFRShadowIndependentReadBufferActualClosure(t *testing.T) {
	s, p, md := newOutcomeTestGroup(t)
	client, server := net.Pipe()
	defer server.Close()
	c := s.WrapConnWithMetric(outbound.NewConn(client, p.Adapter()), p, md, 5)
	tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
	defer tracker.Close()
	done := make(chan error, 1)
	go func() { time.Sleep(25 * time.Millisecond); _, e := server.Write([]byte("buffer reply")); done <- e }()
	b := buf.NewSize(32)
	defer b.Release()
	if e := tracker.ReadBuffer(b); e != nil || string(b.Bytes()) != "buffer reply" {
		t.Fatalf("buffer=%q err=%v", b.Bytes(), e)
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	_ = tracker.Close()
	_ = tracker.Close()
	r := ttfrClosureSnapshot(t, s, p, md)
	if r.AttemptCount != 1 || r.TCPConnectionTTFR_Cnt != 1 || r.UDPResponseCnt != 0 || r.TCPConnectionTTFR_EMA < 20 {
		t.Fatalf("ReadBuffer settlement=%+v", r)
	}
}

type reviewZeroReadTCP struct {
	*net.TCPConn
	zeroFirst bool
}

func (c *reviewZeroReadTCP) Read(b []byte) (int, error) {
	if c.zeroFirst {
		c.zeroFirst = false
		return 0, nil
	}
	return c.TCPConn.Read(b)
}
func TestTTFRShadowIndependentZeroReadThenRawCounters(t *testing.T) {
	for _, production := range []bool{false, true} {
		name, copyConn := "sing", bufio.Copy
		if production {
			name, copyConn = "production", N.Copy
		}
		t.Run(name, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			client, server := transportTCPPair(t)
			base := &reviewZeroReadTCP{TCPConn: client, zeroFirst: true}
			c := s.WrapConnWithMetric(outbound.NewConn(base, p.Adapter()), p, md, 5)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 0, 0, false)
			defer tracker.Close()
			if n, e := tracker.Read(make([]byte, 1)); n != 0 || e != nil {
				t.Fatalf("zero read=%d %v", n, e)
			}
			reader, counters := SN.UnwrapCountReader(tracker, nil)
			if len(counters) < 3 {
				t.Fatalf("lost measurement counters: %d", len(counters))
			}
			if production {
				for {
					observer, ok := reader.(N.ReadErrorUnwrapper)
					if !ok {
						break
					}
					reader, _ = observer.UnwrapReadError()
					reader, counters = SN.UnwrapCountReader(reader, counters)
				}
				if _, ok := N.UnwrapReader(reader).(syscall.Conn); !ok {
					t.Fatalf("raw eligibility lost: %T", N.UnwrapReader(reader))
				}
			}
			done := make(chan error, 1)
			go func() {
				time.Sleep(25 * time.Millisecond)
				_, e := server.Write([]byte("raw payload"))
				_ = server.Close()
				done <- e
			}()
			n, e := copyConn(io.Discard, tracker)
			if n != 11 || e != nil {
				t.Fatalf("copy=%d %v", n, e)
			}
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			_ = tracker.Close()
			r := ttfrClosureSnapshot(t, s, p, md)
			if r.AttemptCount != 1 || r.TCPConnectionTTFR_Cnt != 1 || r.TCPConnectionTTFR_EMA < 20 {
				t.Fatalf("raw counter settlement=%+v", r)
			}
		})
	}
}

type reviewZeroPacket struct {
	C.PacketConn
	zero     bool
	writeN   int
	writeErr error
}

func (c *reviewZeroPacket) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	if c.zero {
		c.zero = false
		return nil, nil, nil, nil
	}
	return c.PacketConn.WaitReadFrom()
}
func (c *reviewZeroPacket) WriteTo(b []byte, a net.Addr) (int, error) { return c.writeN, c.writeErr }
func TestTTFRShadowIndependentPacketWaitReadValidity(t *testing.T) {
	for _, sent := range []bool{false, true} {
		for _, zero := range []bool{false, true} {
			name := "presend"
			if sent {
				name = "sent"
			}
			if zero {
				name += "_zero"
			}
			t.Run(name, func(t *testing.T) {
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
				base := outbound.NewPacketConn(client, p.Adapter().(outbound.ProxyAdapter))
				raw := &reviewZeroPacket{PacketConn: base, zero: zero, writeN: 1, writeErr: errors.New("send returned data+error")}
				pc := s.WrapPacketConnWithMetric(raw, p, md, 5)
				tracker := statistic.NewUDPTracker(pc, statistic.DefaultManager, md, nil, 0, 0, false)
				defer tracker.Close()
				if sent {
					n, e := tracker.WriteTo([]byte("q"), server.LocalAddr())
					if n != 1 || e == nil {
						t.Fatalf("positive send=%d %v", n, e)
					}
				}
				if !zero {
					if _, e = server.WriteTo([]byte("r"), client.LocalAddr()); e != nil {
						t.Fatal(e)
					}
				}
				b, put, _, e := tracker.WaitReadFrom()
				if put != nil {
					put()
				}
				if e != nil {
					t.Fatal(e)
				}
				if zero && len(b) != 0 {
					t.Fatal("not zero")
				}
				if !zero && len(b) != 1 {
					t.Fatal("no response")
				}
				_ = tracker.Close()
				r := ttfrClosureSnapshot(t, s, p, md)
				want := int64(0)
				if sent && !zero {
					want = 1
				}
				if r.AttemptCount != 1 || r.TCPConnectionTTFR_Cnt != 0 || r.UDPResponseCnt != want {
					t.Fatalf("packet counts=%+v want=%d", r, want)
				}
			})
		}
	}
}

func TestTTFRShadowIndependentPendingSendClose(t *testing.T) {
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
	type sendResult struct {
		n   int
		err error
	}
	done := make(chan sendResult, 1)
	go func() { n, e := tracker.WriteTo([]byte("q"), server.LocalAddr()); done <- sendResult{n, e} }()
	if _, _, e = server.ReadFrom(make([]byte, 4)); e != nil {
		t.Fatal(e)
	}
	if _, e = server.WriteTo([]byte("r"), client.LocalAddr()); e != nil {
		t.Fatal(e)
	}
	if n, _, e := tracker.ReadFrom(make([]byte, 4)); n != 1 || e != nil {
		t.Fatalf("response=%d %v", n, e)
	}
	_ = tracker.Close()
	before := ttfrClosureSnapshot(t, s, p, md)
	close(release)
	sent := <-done
	if sent.n != 1 || sent.err != nil {
		t.Fatalf("WriteTo returned %d %v", sent.n, sent.err)
	}
	var metrics smart.RewardedMetrics
	// Inspect the actual wrapped observation after the successful pending write.
	shadow := pc.(interface{ Upstream() any }).Upstream().(*pldTTFRPacketConn).observation
	shadow.apply(&metrics)
	after := ttfrClosureSnapshot(t, s, p, md)
	t.Logf("settled=%d UDPCount=%d afterSendUDPCount=%d observerValid=%v", before.AttemptCount, before.UDPResponseCnt, after.UDPResponseCnt, metrics.UDPResponseValid)
	if !metrics.UDPResponseValid {
		t.Fatal("observer did not finish")
	}
	if after.UDPResponseCnt != 1 {
		t.Fatalf("valid response lost when Close precedes WriteTo return: cnt=%d", after.UDPResponseCnt)
	}
}

func TestTTFRShadowLatePacketObservationRetiredByFlush(t *testing.T) {
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
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	base := &ttfrClosurePacketConn{PacketConn: outbound.NewPacketConn(client, p.Adapter().(outbound.ProxyAdapter)), writeRelease: release}
	tracker := statistic.NewUDPTracker(s.WrapPacketConnWithMetric(base, p, md, 5), statistic.DefaultManager, md, nil, 0, 0, false)
	defer tracker.Close()
	done := make(chan error, 1)
	go func() { _, e := tracker.WriteTo([]byte("q"), server.LocalAddr()); done <- e }()
	if _, _, e = server.ReadFrom(make([]byte, 4)); e != nil {
		t.Fatal(e)
	}
	if _, e = server.WriteTo([]byte("r"), client.LocalAddr()); e != nil {
		t.Fatal(e)
	}
	if n, _, e := tracker.ReadFrom(make([]byte, 4)); n != 1 || e != nil {
		t.Fatalf("response=%d %v", n, e)
	}
	_ = tracker.Close()
	before := ttfrClosureSnapshot(t, s, p, md)
	if before.AttemptCount != 1 || before.UDPResponseCnt != 0 {
		t.Fatalf("premature or duplicate settlement: %+v", before)
	}
	if e = s.store.FlushByLevel("group", s.configName, s.Name()); e != nil {
		t.Fatal(e)
	}
	fresh := s.getPLDRecord(md.SmartTarget, p.Name())
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if fresh.UDPResponseCnt != 0 || fresh.AttemptCount != 0 {
		t.Fatalf("retired callback changed fresh identity: %+v", fresh)
	}
	if _, readErr := s.store.DBViewGetItem(smart.FormatPLDKey(s.configName, s.Name(), md.SmartTarget, p.Name())); readErr == nil || readErr.Error() != "item not found" {
		t.Fatalf("retired callback resurrected persisted row or DB read failed: %v", readErr)
	}
}

func TestTTFRShadowLatePacketCallbackExactlyOnce(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed-send", true: "positive-send"}[confirmed], func(t *testing.T) {
			o := &pldTTFRObservation{isUDP: true}
			send := o.beginPacketSend(1)
			o.downlink(1)
			r := new(smart.PLDRecord)
			r.AttemptCount, r.FailureCount, r.Reward, r.SampleCount = 7, 2, .8, 5
			calls := 0
			o.afterPacketSettlement(func(ms float64) { calls++; r.ObserveUDPResponse(ms) })
			o.afterPacketSettlement(func(ms float64) { t.Fatal("second settlement callback accepted") })
			o.downlink(1)
			if o.beginPacketSend(1) != nil {
				t.Fatal("post-settlement send admitted")
			}
			n := 0
			if confirmed {
				n = 1
			}
			o.finishPacketSend(send, n)
			o.finishPacketSend(send, n)
			want := int64(0)
			if confirmed {
				want = 1
			}
			if int64(calls) != want || r.UDPResponseCnt != want || r.AttemptCount != 7 || r.FailureCount != 2 || r.Reward != .8 || r.SampleCount != 5 {
				t.Fatalf("late observation replayed outcome: calls=%d record=%+v", calls, r)
			}
			o.mu.Lock()
			retained := o.lateUDP != nil
			o.mu.Unlock()
			if retained {
				t.Fatal("completed/failed pending send retained live owner")
			}
		})
	}
	var absent *pldTTFRObservation
	absent.afterPacketSettlement(nil)
	(&pldTTFRObservation{}).afterPacketSettlement(func(float64) { t.Fatal("TCP acquired UDP callback") })
}
