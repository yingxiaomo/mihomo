package outboundgroup

import (
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
)

// This is the original production-selector reproducer: pause AFTER the unwrap
// snapshot, publish C concurrently, then resume the stale A->B calculation.
func TestPLDSwitchClosureCurrentRequestFollowsPublishedPrimary(t *testing.T) {
	for _, markFreshClock := range []bool{false, true} {
		name := "fresh-publication"
		if markFreshClock {
			name = "fresh-publication-with-clock"
		}
		t.Run(name, func(t *testing.T) {
			st := liveCacheStore(t)
			real := func(name string) C.Proxy {
				return adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: name}))
			}
			a := &unwrapBarrierProxy{Proxy: real("A"), name: "A", alive: true, udp: true}
			b := &unwrapBarrierProxy{Proxy: real("B"), name: "B", alive: true, udp: true}
			c := &unwrapBarrierProxy{Proxy: real("C"), name: "C", alive: true, udp: true}
			ps := []C.Proxy{a, b, c}
			s, _ := liveCacheFixture(t, st, "cfg", "switch-closure", ps)
			s.switchMargin = 1.15
			target := "switch-closure.example"
			md := &C.Metadata{NetWork: C.TCP, Host: target, SmartTarget: target, DstPort: 443, DstIPASN: "unknown"}
			for i, p := range ps {
				r := s.getPLDRecord(target, p.Name())
				r.SampleCount, r.SampleCountLarge = 10, 10
				r.Reward, r.RewardLarge = []float64{0.2, 1, 0.4}[i], []float64{0.2, 1, 0.4}[i]
			}
			st.ForceStoreUnwrapResult(s.Name(), s.configName, target, []C.Proxy{a})
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan []C.Proxy, 1)
			var once, releaseOnce sync.Once
			b.check = func() bool { once.Do(func() { close(entered); <-release }); return true }
			defer releaseOnce.Do(func() { close(release) })
			go func() { selected, _ := s.selectProxies(md, ps); done <- selected }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("selector did not reach observation barrier")
			}
			st.ForceStoreUnwrapResult(s.Name(), s.configName, target, []C.Proxy{c})
			fresh, expiry, _ := st.GetUnwrapSnapshotWithExpiry(s.Name(), s.configName, target)
			if markFreshClock {
				s.markPrimarySwitch(target)
			}
			releaseOnce.Do(func() { close(release) })
			var selected []C.Proxy
			select {
			case selected = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("selector did not finish")
			}
			current, currentExpiry, found := st.GetUnwrapSnapshotWithExpiry(s.Name(), s.configName, target)
			if !found || current.Version != fresh.Version || current.Proxies[0] != "C" || currentExpiry != expiry {
				t.Fatalf("stale selector mutated fresh publication: %+v / %+v", fresh, current)
			}
			if len(selected) == 0 || selected[0].Name() != "C" {
				t.Fatalf("current request did not follow C: %v", selected)
			}
			if s.switchCooldownElapsed(target) == markFreshClock {
				t.Fatal("rejected selector consumed or cleared another request's clock")
			}
		})
	}
}

func TestPLDSwitchClosureRejectDoesNotOverrideEligibility(t *testing.T) {
	current := []string{"C", "C", ""}
	ranked := []string{"B", "A", "B"}
	a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
	b := &unwrapBarrierProxy{name: "B", alive: true, udp: true}
	c := &unwrapBarrierProxy{name: "C", alive: true, udp: false}
	names := publishedPrimaryOrder(current, ranked, []C.Proxy{a, b, c, nil})
	if len(names) != 3 || names[0] != "C" || names[1] != "B" || names[2] != "A" {
		t.Fatalf("unexpected candidate order: %v", names)
	}
	st := liveCacheStore(t)
	s, _ := liveCacheFixture(t, st, "cfg", "switch-eligibility", []C.Proxy{a, b, c})
	md := &C.Metadata{NetWork: C.UDP, Host: "udp.example", SmartTarget: "udp.example"}
	chosen := s.filterProxies(md, md.Host, names, nil, []C.Proxy{a, b, c}, maxSelected, true, false)
	if len(chosen) == 0 {
		t.Fatal("eligible backups disappeared")
	}
	for _, p := range chosen {
		if !p.SupportUDP() {
			t.Fatalf("rebase bypassed UDP eligibility: %s", p.Name())
		}
	}
}
