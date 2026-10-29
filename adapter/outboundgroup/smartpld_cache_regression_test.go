package outboundgroup

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type unwrapBarrierProxy struct {
	C.Proxy
	name  string
	alive bool
	udp   bool
	check func() bool
}

func (p *unwrapBarrierProxy) Name() string { return p.name }
func (p *unwrapBarrierProxy) AliveForTestUrl(string) bool {
	if p.check != nil {
		return p.check()
	}
	return p.alive
}
func (p *unwrapBarrierProxy) SupportUDP() bool { return p.udp }

type cacheMutableProvider struct {
	P.ProxyProvider
	mu      sync.Mutex
	proxies []C.Proxy
	version uint32
}

func (p *cacheMutableProvider) Touch()          {}
func (p *cacheMutableProvider) Version() uint32 { p.mu.Lock(); defer p.mu.Unlock(); return p.version }
func (p *cacheMutableProvider) Proxies() []C.Proxy {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]C.Proxy(nil), p.proxies...)
}
func (p *cacheMutableProvider) set(ps []C.Proxy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.proxies = ps
	p.version++
}

func liveCacheFixture(t *testing.T, st *smart.Store, cfg, group string, ps []C.Proxy) (*SmartPLD, *cacheMutableProvider) {
	t.Helper()
	pd := &cacheMutableProvider{proxies: ps, version: 1}
	s := &SmartPLD{GroupBase: NewGroupBase(GroupBaseOption{Name: group, Type: C.SmartPLD, Providers: []P.ProxyProvider{pd}}), store: st, configName: cfg, testUrl: "https://example.com", usePLD: true, switchCooldown: time.Hour}
	t.Cleanup(func() { s.rewardCache().Close() })
	return s, pd
}
func liveCacheStore(t *testing.T) *smart.Store {
	t.Helper()
	d, err := bbolt.Open(filepath.Join(t.TempDir(), "smart.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.MaxBatchSize = 1
	st := smart.NewStore(d)
	if err := st.FlushAll(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.FlushQueue(true)
		smart.NewStore(nil)
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return st
}

func TestCachePLDRefreshConditionalBarrier(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		for _, replacement := range []string{"B", "A"} {
			name := replacement + "/delete"
			if healthy {
				name = replacement + "/renew"
			}
			t.Run(name, func(t *testing.T) {
				st := liveCacheStore(t)
				entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				a := &unwrapBarrierProxy{name: "A", udp: true, check: func() bool { close(entered); <-release; return healthy }}
				b := &unwrapBarrierProxy{name: "B", alive: true, udp: true}
				s, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
				st.ForceStoreUnwrapResult("grp", "cfg", "target", []C.Proxy{a})
				old, _ := st.GetUnwrapSnapshot("grp", "cfg", "target")
				go func() { defer close(done); s.refreshUnwrapCache(false, false, "target", old) }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("refresh did not hit barrier")
				}
				newPrimary := C.Proxy(b)
				if replacement == "A" {
					newPrimary = a
				}
				st.ForceStoreUnwrapResult("grp", "cfg", "target", []C.Proxy{newPrimary})
				next, _ := st.GetUnwrapSnapshot("grp", "cfg", "target")
				close(release)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("refresh stuck")
				}
				current, ok := st.GetUnwrapSnapshot("grp", "cfg", "target")
				if !ok || current.Proxies[0] != replacement || current.Version != next.Version {
					t.Fatalf("old worker changed new generation: %+v vs %+v", current, next)
				}
			})
		}
	}
}

func TestCachePLDRefreshBranches(t *testing.T) {
	cases := []struct {
		name                                                                       string
		present, alive, udp, isUDP, blocked, cooldown, soft, usePLD, large, remove bool
	}{
		{"missing", false, true, true, false, false, false, false, true, false, true},
		{"dead", true, false, true, false, false, false, false, true, false, true},
		{"tcp-only", true, true, false, true, false, false, false, true, false, true},
		{"blocked", true, true, true, false, true, false, false, true, false, true},
		{"cooldown", true, true, true, false, false, true, false, true, false, true},
		{"soft-small", true, true, true, false, false, false, true, true, false, true},
		{"soft-large", true, true, true, true, false, false, true, true, true, true},
		{"healthy", true, true, true, false, false, false, false, true, false, false},
		{"soft-disabled", true, true, true, false, false, false, true, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := liveCacheStore(t)
			a := &unwrapBarrierProxy{name: "A", alive: tc.alive, udp: tc.udp}
			var pool []C.Proxy
			if tc.present {
				pool = []C.Proxy{a}
			}
			s, _ := liveCacheFixture(t, st, "cfg", "grp", pool)
			s.usePLD = tc.usePLD
			st.ForceStoreUnwrapResult("grp", "cfg", "target", []C.Proxy{a})
			old, _ := st.GetUnwrapSnapshot("grp", "cfg", "target")
			if tc.blocked {
				st.UpdateBlockedNodesCache("grp", "cfg", map[string]*smart.NodeState{"A": {BlockedUntil: time.Now().Unix() + 60}})
			}
			if tc.cooldown || tc.soft {
				r := s.getPLDRecord("target", "A")
				cd := int64(0)
				if tc.cooldown {
					cd = 60
				}
				r.FillFailure(cd)
				r.FillFailure(cd)
			}
			s.refreshUnwrapCache(tc.isUDP, tc.large, "target", old)
			current, ok := st.GetUnwrapSnapshot("grp", "cfg", "target")
			if ok == tc.remove {
				t.Fatalf("remove=%v current=%+v", tc.remove, current)
			}
			if ok && (current.Version == old.Version || current.Proxies[0] != "A") {
				t.Fatal("healthy primary not renewed")
			}
			s.refreshUnwrapCache(false, false, "", smart.UnwrapMap{})
		})
	}
}

func TestCachePLDLiveFlushLevels(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		t.Run(level, func(t *testing.T) {
			st := liveCacheStore(t)
			a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
			configs := []string{"cfg", "cfg", "cfg-other"}
			groups := []string{"grp", "grp-other", "grp"}
			ss := make([]*SmartPLD, 3)
			old := make([]*smart.PLDRecord, 3)
			for i := range ss {
				ss[i], _ = liveCacheFixture(t, st, configs[i], groups[i], []C.Proxy{a})
				old[i] = ss[i].getPLDRecord("target", "A")
				old[i].FillFailure(60)
				ss[i].savePLDRecord("target", "A", old[i])
				ss[i].markPrimarySwitch("target")
			}
			if err := st.FlushByLevel(level, "cfg", "grp"); err != nil {
				t.Fatal(err)
			}
			for i, s := range ss {
				removed := level == "all" || configs[i] == "cfg" && (level == "config" || groups[i] == "grp")
				s.savePLDRecord("target", "A", old[i])
				fresh := s.getPLDRecord("target", "A")
				if (fresh != old[i]) != removed || fresh.InFailureCooldown(time.Now().Unix()) == removed || s.switchCooldownElapsed("target") != removed {
					t.Fatalf("live scope %s/%s not correctly reset", configs[i], groups[i])
				}
				if _, err := st.DBViewGetItem(smart.FormatPLDKey(configs[i], groups[i], "target", "A")); (err != nil) != removed {
					t.Fatalf("old save revived flushed live record: %v", err)
				}
			}
		})
	}
}

func TestCachePLDProviderRemoveReadd(t *testing.T) {
	st := liveCacheStore(t)
	a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
	b := &unwrapBarrierProxy{name: "B", alive: true, udp: true}
	s, pd := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
	old := s.getPLDRecord("removed", "A")
	old.FillFailure(60)
	s.savePLDRecord("removed", "A", old)
	s.markPrimarySwitch("removed")
	keep := s.getPLDRecord("kept", "B")
	keep.FillFailure(60)
	s.savePLDRecord("kept", "B", keep)
	s.markPrimarySwitch("kept")
	// A live-only record must also be retired even though no stats or PLD row
	// exists for it yet. A second instance witnesses the same scoped deletion.
	liveOnly := s.getPLDRecord("not-saved", "A")
	liveOnly.FillFailure(60)
	other, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
	otherOld := other.getPLDRecord("removed", "A")
	pd.set([]C.Proxy{b})
	s.cleanupOrphanedNodeCache()
	pd.set([]C.Proxy{a, b})
	for _, r := range []*smart.PLDRecord{old, otherOld} {
		s.savePLDRecord("removed", "A", r)
	}
	s.savePLDRecord("not-saved", "A", liveOnly)
	fresh := s.getPLDRecord("removed", "A")
	if fresh == old || fresh.InFailureCooldown(time.Now().Unix()) || !s.switchCooldownElapsed("removed") {
		t.Fatal("same-name readd inherited old state")
	}
	if other.getPLDRecord("removed", "A") == otherOld {
		t.Fatal("other live instance not invalidated")
	}
	if s.getPLDRecord("kept", "B") != keep || s.switchCooldownElapsed("kept") {
		t.Fatal("unrelated node reset")
	}
	if _, err := st.DBViewGetItem(smart.FormatPLDKey("cfg", "grp", "removed", "A")); err == nil {
		t.Fatal("old ref revived orphan")
	}
	if _, err := st.DBViewGetItem(smart.FormatPLDKey("cfg", "grp", "not-saved", "A")); err == nil {
		t.Fatal("live-only orphan ref revived")
	}
	fresh.FillFailure(60)
	s.savePLDRecord("removed", "A", fresh)
	s.savePLDRecord("removed", "A", old)
	reader, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
	if _, _, n := reader.getPLDRecord("removed", "A").ChannelStats(false); n != 1 {
		t.Fatalf("readd writes rejected/overwritten: %d", n)
	}
}
