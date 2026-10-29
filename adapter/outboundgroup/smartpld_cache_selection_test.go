package outboundgroup

import (
	"math"
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

func TestCachePLDSelectObservesForcedPrimary(t *testing.T) {
	st := liveCacheStore(t)
	a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
	b := &unwrapBarrierProxy{name: "B", alive: true, udp: true}
	s, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
	s.usePLD = false
	for _, p := range []C.Proxy{a, b, b, a} {
		st.ForceStoreUnwrapResult("grp", "cfg", "target", []C.Proxy{p})
		before, _ := st.GetUnwrapSnapshot("grp", "cfg", "target")
		md := &C.Metadata{Host: "example.com", SmartTarget: "target", DstIP: netip.MustParseAddr("1.1.1.1"), NetWork: C.TCP}
		selected, pinned := s.selectProxies(md, s.GetProxies(true))
		if !pinned || len(selected) != 1 || selected[0].Name() != p.Name() {
			t.Fatalf("forced %s selection=%v pinned=%v", p.Name(), selected, pinned)
		}
		after, _ := st.GetUnwrapSnapshot("grp", "cfg", "target")
		if before.Version != after.Version {
			t.Fatal("unexpired pin was unnecessarily replaced")
		}
	}
}

func TestCachePLDCacheSaveErrorEvictionAndClose(t *testing.T) {
	st := liveCacheStore(t)
	a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
	s, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a})
	r := s.getPLDRecord("target", "A")
	r.Reward = math.NaN()
	s.savePLDRecord("target", "A", r)
	if _, err := st.DBViewGetItem(smart.FormatPLDKey("cfg", "grp", "target", "A")); err == nil {
		t.Fatal("invalid JSON was persisted")
	}
	r.Reward = 0
	s.evictStalePLDState()
	if s.getPLDRecord("target", "A") == r {
		t.Fatal("stale memory record retained")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	closed := s.getPLDRecord("target", "A")
	if closed == s.getPLDRecord("target", "A") {
		t.Fatal("closed instance retains records")
	}
}
