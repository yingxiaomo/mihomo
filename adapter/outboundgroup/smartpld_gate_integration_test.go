package outboundgroup

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// Exercise time.AfterFunc and the real tracker lookup, not just the gate
// transaction. Only the initial sampling delay is shortened; the production
// six-second second sample remains unchanged.
func TestPLDRealGateLifecycle(t *testing.T) {
	for _, kind := range []string{"first-byte", "zero-progress", "healthy"} {
		t.Run(kind, func(t *testing.T) {
			s, p, md := newOutcomeTestGroup(t)
			if kind == "zero-progress" {
				s.zeroProgressMs = 5
			} else {
				s.firstByteGateMs = 30
			}
			r, w := net.Pipe()
			defer w.Close()
			c := s.WrapConnWithMetric(outbound.NewConn(r, p.Adapter()), p, md, 5)
			tracker := statistic.NewTCPTracker(c, statistic.DefaultManager, md, nil, 1024, 0, false)
			defer func() {
				_ = tracker.Close()
				// A Close schedules statistics; wait for its committed snapshot
				// before a later fixture switches the process-global store.
				until := time.Now().Add(2 * time.Second)
				key := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), md.SmartTarget, p.Name())
				for {
					s.store.FlushQueue(true)
					data, e := s.store.DBViewGetItem(key)
					if e == nil && len(data) > 0 {
						break
					}
					if time.Now().After(until) {
						t.Error("close statistics never committed")
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				lock := smart.GetTargetNodeLock(md.SmartTarget, s.Name(), p.Name())
				lock.Lock()
				lock.Unlock()
			}()
			if kind == "healthy" {
				done := make(chan error, 1)
				go func() { _, e := w.Write([]byte("x")); done <- e }()
				if _, e := io.ReadFull(tracker, make([]byte, 1)); e != nil {
					t.Fatal(e)
				}
				if e := <-done; e != nil {
					t.Fatal(e)
				}
				time.Sleep(60 * time.Millisecond)
				if n := outcomeHostStrikes(t, s, md, p.Name()); n != 0 {
					t.Fatal("healthy first byte punished", n)
				}
				if e := tracker.Close(); e != nil {
					t.Fatal(e)
				}
				time.Sleep(30 * time.Millisecond)
				return
			}
			deadline := time.Now().Add(8 * time.Second)
			for outcomeHostStrikes(t, s, md, p.Name()) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("actual timer never recorded its failure")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if e := tracker.Close(); e != nil {
				t.Fatal(e)
			}
			time.Sleep(30 * time.Millisecond)
			if n := outcomeHostStrikes(t, s, md, p.Name()); n != 1 {
				t.Fatal("gate plus real close charged twice", n)
			}
			_, _, n := s.getPLDRecord(md.SmartTarget, p.Name()).ChannelStats(false)
			if n != 1 {
				t.Fatal("gate plus real close updated reward twice", n)
			}
		})
	}
}
