package smart

import (
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/metacubex/bbolt"
	C "github.com/metacubex/mihomo/constant"
)

type cacheTestProxy struct {
	C.Proxy
	name string
}

func (p cacheTestProxy) Name() string { return p.name }
func cacheProxies(names ...string) []C.Proxy {
	ps := make([]C.Proxy, len(names))
	for i, n := range names {
		ps[i] = cacheTestProxy{name: n}
	}
	return ps
}
func realCacheStore(t *testing.T) *Store {
	t.Helper()
	d, err := bbolt.Open(filepath.Join(t.TempDir(), "smart.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.MaxBatchSize = 1
	st := NewStore(d)
	if err := st.FlushAll(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.FlushQueue(true)
		NewStore(nil)
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return st
}

func TestCacheUnwrapConditionalIdentity(t *testing.T) {
	st := NewStore(nil)
	const cfg, grp, target = "cas-cfg", "cas-grp", "cas-target"
	st.DeleteUnwrapResult(grp, cfg, target)
	st.StoreUnwrapResult(grp, cfg, "", cacheProxies("A"))
	st.StoreUnwrapResult(grp, cfg, target, nil)
	st.ForceStoreUnwrapResult(grp, cfg, "", cacheProxies("A"))
	st.ForceStoreUnwrapResult(grp, cfg, target, nil)
	if _, ok := st.GetUnwrapSnapshot(grp, cfg, ""); ok {
		t.Fatal("empty target")
	}
	if ps, _ := st.GetUnwrapResult(grp, cfg, ""); ps != nil {
		t.Fatal("empty target")
	}
	if ps, _ := st.GetUnwrapResult(grp, cfg, target); ps != nil {
		t.Fatal("unexpected missing")
	}
	st.StoreUnwrapResult(grp, cfg, target, cacheProxies("A"))
	old, _ := st.GetUnwrapSnapshot(grp, cfg, target)
	copySnapshot, _ := st.GetUnwrapSnapshot(grp, cfg, target)
	copySnapshot.Proxies[0] = "changed-by-reader"
	copyNames, _ := st.GetUnwrapResult(grp, cfg, target)
	copyNames[0] = "also-changed-by-reader"
	unchanged, _ := st.GetUnwrapSnapshot(grp, cfg, target)
	if unchanged.Proxies[0] != "A" {
		t.Fatal("reader mutated immutable cache snapshot")
	}
	st.StoreUnwrapResult(grp, cfg, target, cacheProxies("B"))
	cur, _ := st.GetUnwrapSnapshot(grp, cfg, target)
	if cur.Version != old.Version {
		t.Fatal("first writer not pinned")
	}
	for _, replacement := range []string{"B", "A"} {
		st.ForceStoreUnwrapResult(grp, cfg, target, cacheProxies(replacement))
		if st.DeleteUnwrapSnapshot(grp, cfg, target, old) || st.RenewUnwrapSnapshot(grp, cfg, target, old) {
			t.Fatal("stale generation changed replacement")
		}
		cur, _ = st.GetUnwrapSnapshot(grp, cfg, target)
		if cur.Proxies[0] != replacement {
			t.Fatal("replacement lost")
		}
	}
	if !st.RenewUnwrapSnapshot(grp, cfg, target, cur) {
		t.Fatal("matching renewal failed")
	}
	if st.RenewUnwrapSnapshot(grp, cfg, target, cur) {
		t.Fatal("same snapshot reused")
	}
	cur, _ = st.GetUnwrapSnapshot(grp, cfg, target)
	unwrapCache.SetWithExpire(FormatDBKey(cfg, grp, target), cur, time.Now().Add(-time.Hour))
	if _, expired := st.GetUnwrapResult(grp, cfg, target); !expired {
		t.Fatal("expiry lost")
	}
	st.StoreUnwrapResult(grp, cfg, target, cacheProxies("B"))
	if st.DeleteUnwrapResultIfPrimary(grp, cfg, target, "A") {
		t.Fatal("failed A unpinned B")
	}
	if !st.DeleteUnwrapResultIfPrimary(grp, cfg, target, "B") {
		t.Fatal("matching primary not deleted")
	}
	if st.DeleteUnwrapSnapshot(grp, cfg, target, cur) || st.RenewUnwrapSnapshot(grp, cfg, target, cur) {
		t.Fatal("missing snapshot recreated")
	}
	st.ForceStoreUnwrapResult(grp, cfg, target, cacheProxies("A"))
	cur, _ = st.GetUnwrapSnapshot(grp, cfg, target)
	if !st.DeleteUnwrapSnapshot(grp, cfg, target, cur) {
		t.Fatal("matching delete failed")
	}
	if st.DeleteUnwrapSnapshot(grp, cfg, "", cur) || st.RenewUnwrapSnapshot(grp, cfg, "", cur) || st.RenewUnwrapSnapshot(grp, cfg, target, UnwrapMap{}) || st.DeleteUnwrapResultIfPrimary(grp, cfg, "", "A") || st.DeleteUnwrapResultIfPrimary(grp, cfg, target, "") {
		t.Fatal("invalid args accepted")
	}
}

func TestCacheFlushScopeAndDetachedBatch(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			scopes := []writeScope{{"cfg", "grp", "A"}, {"cfg", "grp-other", "A"}, {"cfg-other", "grp", "A"}}
			caches := make([]*PLDCache, len(scopes))
			old := make([]*PLDRecord, len(scopes))
			for i, scope := range scopes {
				caches[i] = st.NewPLDCache(scope.config, scope.group)
				defer caches[i].Close()
				old[i] = caches[i].Get("target", scope.node)
				old[i].FillFailure(60)
				if err := caches[i].Save("target", scope.node, old[i]); err != nil {
					t.Fatal(err)
				}
				caches[i].MarkSwitch("target")
				st.AppendToGlobalQueue(StoreOperation{Type: OpSaveStats, Config: scope.config, Group: scope.group, Target: "target", Node: scope.node, Data: []byte(`{"success":7}`)})
			}
			detached := opQueue.drain(true)
			if err := st.FlushByLevel(level, "cfg", "grp"); err != nil {
				t.Fatal(err)
			}
			if err := st.BatchSave(detached); err != nil {
				t.Fatal(err)
			}
			for i, scope := range scopes {
				removed := matchesScope(level, "cfg", "grp", scope.config, scope.group)
				if err := caches[i].Save("target", scope.node, old[i]); err != nil {
					t.Fatal(err)
				}
				r := caches[i].Get("target", scope.node)
				_, _, count := r.ChannelStats(false)
				if (count == 0) != removed || (r != old[i]) != removed {
					t.Fatalf("scope %v reset=%v count=%d", scope, removed, count)
				}
				if caches[i].SwitchCooldownElapsed("target", time.Hour) != removed {
					t.Fatalf("switch scope %v", scope)
				}
				if _, err := st.DBViewGetItem(FormatPLDKey(scope.config, scope.group, "target", scope.node)); (err != nil) != removed {
					t.Fatalf("stale PLD revived %v: %v", scope, err)
				}
				if _, err := st.DBViewGetItem(FormatDBKey(KeyTypeStats, scope.config, scope.group, "target", scope.node)); (err != nil) != removed {
					t.Fatalf("detached stats revived %v: %v", scope, err)
				}
			}
		})
	}
}

func TestCacheOrphanRetiresOnlyRemovedNode(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	defer c.Close()
	old := c.Get("removed-target", "A")
	old.FillFailure(60)
	keep := c.Get("kept-target", "B")
	keep.FillFailure(60)
	for _, pair := range []struct {
		target, node string
		r            *PLDRecord
	}{{"removed-target", "A", old}, {"kept-target", "B", keep}} {
		if err := c.Save(pair.target, pair.node, pair.r); err != nil {
			t.Fatal(err)
		}
		c.MarkSwitch(pair.target)
		st.AppendToGlobalQueue(StoreOperation{Type: OpSaveStats, Config: "cfg", Group: "grp", Target: pair.target, Node: pair.node, Data: []byte(`{"success":9}`)})
		st.ForceStoreUnwrapResult("grp", "cfg", pair.target, cacheProxies(pair.node))
		recordCache.Set(FormatDBKey(KeyTypeStats, "cfg", "grp", pair.target, pair.node), &AtomicStatsRecord{})
	}
	detached := opQueue.drain(true)
	if err := st.RemoveNodesData("grp", "cfg", 5, []string{"A"}); err != nil {
		t.Fatal(err)
	}
	if err := st.BatchSave(detached); err != nil {
		t.Fatal(err)
	}
	if err := c.Save("removed-target", "A", old); err != nil {
		t.Fatal(err)
	}
	fresh := c.Get("removed-target", "A")
	if fresh == old || fresh.InFailureCooldown(time.Now().Unix()) {
		t.Fatal("old incarnation survives")
	}
	if keep != c.Get("kept-target", "B") || c.SwitchCooldownElapsed("kept-target", time.Hour) {
		t.Fatal("unrelated node reset")
	}
	if !c.SwitchCooldownElapsed("removed-target", time.Hour) {
		t.Fatal("orphan switch retained")
	}
	if _, err := st.DBViewGetItem(FormatPLDKey("cfg", "grp", "removed-target", "A")); err == nil {
		t.Fatal("old PLD revived")
	}
	if _, err := st.DBViewGetItem(FormatDBKey(KeyTypeStats, "cfg", "grp", "removed-target", "A")); err == nil {
		t.Fatal("old batch revived")
	}
	if ps, _ := st.GetUnwrapResult("grp", "cfg", "removed-target"); len(ps) > 0 {
		t.Fatal("orphan primary retained")
	}
	if ps, _ := st.GetUnwrapResult("grp", "cfg", "kept-target"); len(ps) != 1 {
		t.Fatal("unrelated primary deleted")
	}
	fresh.FillFailure(60)
	if err := c.Save("removed-target", "A", fresh); err != nil {
		t.Fatal(err)
	}
	if err := c.Save("removed-target", "A", old); err != nil {
		t.Fatal(err)
	}
	reader := st.NewPLDCache("cfg", "grp")
	defer reader.Close()
	if _, _, n := reader.Get("removed-target", "A").ChannelStats(false); n != 1 {
		t.Fatalf("fresh incarnation overwritten: %d", n)
	}
}

func TestCacheFlushSaveInterleaving(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	defer c.Close()
	for i := 0; i < 30; i++ {
		r := c.Get("target", "A")
		r.FillFailure(60)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 5; j++ {
				if err := c.Save("target", "A", r); err != nil {
					t.Error(err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if err := st.FlushByGroup("grp", "cfg"); err != nil {
				t.Error(err)
			}
		}()
		close(start)
		wg.Wait()
		if _, err := st.DBViewGetItem(FormatPLDKey("cfg", "grp", "target", "A")); err == nil {
			t.Fatal("interleaved old save revived flushed record")
		}
	}
}

func TestCacheLifecycleDecodeEvictCloseAndWeak(t *testing.T) {
	st := realCacheStore(t)
	key := FormatPLDKey("cfg", "grp", "target", "A")
	if err := st.DBBatchPutItem(key, []byte(`{"reward":-2,"sample_count":"bad"}`)); err != nil {
		t.Fatal(err)
	}
	c := st.NewPLDCache("cfg", "grp")
	if _, _, n := c.Get("target", "A").ChannelStats(false); n != 0 {
		t.Fatal("partial corrupt record accepted")
	}
	if err := st.DBBatchPutItem(FormatPLDKey("cfg", "grp", "db-only", "B"), []byte(`{"sample_count":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.DBBatchPutItem("pld:cfg:grp:malformed", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if len(c.Nodes()) != 2 {
		t.Fatal("PLD-only nodes not listed")
	}
	r := c.Get("target", "A")
	c.MarkSwitch("target")
	c.Evict(time.Now().Unix()+1, time.Now().Add(time.Hour))
	if c.Get("target", "A") == r || !c.SwitchCooldownElapsed("target", time.Hour) {
		t.Fatal("eviction not applied")
	}
	c.Close()
	c.Close()
	c.MarkSwitch("target")
	if err := c.Save("target", "A", r); err != nil {
		t.Fatal(err)
	}
	if c.Get("closed", "A") == c.Get("closed", "A") {
		t.Fatal("closed cache retained new records")
	}
	if err := c.Save("target", "A", nil); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushByLevel("", "cfg", "grp"); err == nil {
		t.Fatal("empty flush accepted")
	}
	if err := st.FlushByLevel("bad", "cfg", "grp"); err == nil {
		t.Fatal("bad flush accepted")
	}
	if err := st.RemoveNodesData("grp", "cfg", 5, nil); err != nil {
		t.Fatal(err)
	}
	// A deliberately unreachable registered cache is collectible, proving the
	// listener registry does not own a strong reference (no finalizer sleeps).
	p := func() weak.Pointer[PLDCache] { return weak.Make(st.NewPLDCache("weak", "grp")) }()
	for i := 0; i < 10 && p.Value() != nil; i++ {
		runtime.GC()
	}
	if p.Value() != nil {
		t.Fatal("weak registry retained cache")
	}
	storeBarrier.Lock()
	invalidateLive("all", "", "", nil)
	storeBarrier.Unlock()
	cacheRegistry.mu.Lock()
	_, present := cacheRegistry.caches[p]
	cacheRegistry.mu.Unlock()
	if present {
		t.Fatal("dead listener not swept")
	}
}
