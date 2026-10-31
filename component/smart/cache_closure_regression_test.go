package smart

import (
	"bytes"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/bbolt"
)

// closurePause is one-shot so a fresh public read can finish while old work
// remains paused after its real DB scan, with no sleeps or fabricated rows.
func closurePause() (func(), <-chan struct{}, func()) {
	entered, release := make(chan struct{}), make(chan struct{})
	var used, released atomic.Bool
	return func() {
			if used.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}, entered, func() {
			if released.CompareAndSwap(false, true) {
				close(release)
			}
		}
}

func closureWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle operation did not reach/leave its channel barrier")
	}
}

func closureFlush(t *testing.T, st *Store, level string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- st.FlushByLevel(level, "cfg", "grp") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush waited for a paused completed DB scan/derived publication")
	}
}

func closureSeedNode(t *testing.T, st *Store, config, group, node string) []byte {
	t.Helper()
	data, err := json.Marshal(&NodeState{Name: node, BlockedUntil: time.Now().Unix() + 600})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DBBatchPutItem(FormatDBKey(KeyTypeNode, config, group, node), data); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCacheClosureBlockedPublicLoaderFlush(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		for _, background := range []bool{false, true} {
			name := level + "/sync"
			if background {
				name = level + "/background"
			}
			t.Run(name, func(t *testing.T) {
				st := realCacheStore(t)
				row := FormatDBKey(KeyTypeNode, "cfg", "grp", "A")
				closureSeedNode(t, st, "cfg", "grp", "A")
				neighbors := []writeScope{{"cfg", "grp-other", "G"}, {"cfg-other", "grp", "C"}}
				for _, scope := range neighbors {
					closureSeedNode(t, st, scope.config, scope.group, scope.node)
					if !st.GetBlockedNodes(scope.group, scope.config)[scope.node] {
						t.Fatal("neighbor not actually cached")
					}
				}
				cacheKey := FormatDBKey("cfg", "grp")
				if background {
					// Real public stale lookup must start its own async loader.
					if !st.GetBlockedNodes("grp", "cfg")["A"] {
						t.Fatal("old public lookup did not scan A")
					}
					blockedNodesCache.SetWithExpire(cacheKey, map[string]bool{"A": true}, time.Now().Add(-time.Hour))
					dbResultCache.Delete(FormatDBKey(KeyTypeNode, "cfg", "grp"))
				}
				pause, entered, release := closurePause()
				st.afterBlockedNodesLoad = pause
				defer release()
				done := make(chan struct{})
				result := make(chan map[string]bool, 1)
				go func() {
					defer close(done)
					result <- st.GetBlockedNodes("grp", "cfg")
				}()
				closureWait(t, entered)
				// This hook is outside GetNodeStates: its real DB scan AND
				// guarded source publication already completed before Flush.
				if states, ok := dbResultCache.Get(FormatDBKey(KeyTypeNode, "cfg", "grp")); !ok || states[row] == nil {
					t.Fatal("public loader did not complete the real source scan")
				}
				closureFlush(t, st, level)
				if _, err := st.DBViewGetItem(row); err == nil {
					t.Fatal("Flush failed to delete A")
				}
				closureSeedNode(t, st, "cfg", "grp", "B")
				if got := st.GetBlockedNodes("grp", "cfg"); len(got) != 1 || !got["B"] || got["A"] {
					t.Fatalf("fresh public derived result missing: %v", got)
				}
				release()
				closureWait(t, done)
				if background {
					deadline := time.Now().Add(5 * time.Second)
					for {
						if _, loading := blockedNodesRefreshFlags.Load(cacheKey); !loading {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("public background loader did not finish its final Set attempt")
						}
						runtime.Gosched()
					}
				} else if old := <-result; len(old) != 0 {
					t.Fatalf("retired synchronous result returned stale nodes: %v", old)
				}
				if got := st.GetBlockedNodes("grp", "cfg"); len(got) != 1 || !got["B"] || got["A"] {
					t.Fatalf("old final Set resurrected A or erased B: %v", got)
				}
				for _, scope := range neighbors {
					removed := matchesScope(level, "cfg", "grp", scope.config, scope.group)
					_, cached := blockedNodesCache.Get(FormatDBKey(scope.config, scope.group))
					if cached == removed {
						t.Fatalf("wrong neighbor cache invalidation: %v removed=%v", scope, removed)
					}
					_, err := st.DBViewGetItem(FormatDBKey(KeyTypeNode, scope.config, scope.group, scope.node))
					if (err != nil) != removed {
						t.Fatalf("wrong neighbor DB deletion: %v err=%v", scope, err)
					}
				}
			})
		}
	}
}

func TestCacheClosureBlockedUpdateFlush(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			st.UpdateBlockedNodesCache("grp", "cfg", map[string]*NodeState{"A": {BlockedUntil: time.Now().Unix() + 600}})
			pause, entered, release := closurePause()
			st.beforeBlockedNodesUpdatePublish = pause
			defer release()
			done := make(chan struct{})
			go func() {
				defer close(done)
				st.UpdateBlockedNodesCache("grp", "cfg", map[string]*NodeState{"old": {BlockedUntil: time.Now().Unix() + 600}})
			}()
			closureWait(t, entered)
			closureFlush(t, st, level)
			closureSeedNode(t, st, "cfg", "grp", "B")
			if got := st.GetBlockedNodes("grp", "cfg"); len(got) != 1 || !got["B"] {
				t.Fatalf("fresh public result missing: %v", got)
			}
			release()
			closureWait(t, done)
			if got := st.GetBlockedNodes("grp", "cfg"); len(got) != 1 || !got["B"] {
				t.Fatalf("old update revived pre-Flush map: %v", got)
			}
		})
	}
}

func TestCacheClosureBlockedAncestorFlush(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			closureSeedNode(t, st, "cfg", "grp", "A")
			closureSeedNode(t, st, "cfg-other", "grp", "C")
			if !st.GetBlockedNodes("", "cfg")["A"] || !st.GetBlockedNodes("", "cfg-other")["C"] {
				t.Fatal("config ancestor was not cached")
			}
			closureFlush(t, st, level)
			closureSeedNode(t, st, "cfg", "grp", "B")
			if got := st.GetBlockedNodes("", "cfg"); len(got) != 1 || !got["B"] {
				t.Fatalf("cached config root survived scoped Flush: %v", got)
			}
			if got, found := blockedNodesCache.Get(FormatDBKey("cfg-other")); level != "all" && (!found || !got["C"]) {
				t.Fatal("neighbor config ancestor was incorrectly cleared")
			}
		})
	}
}

func closureOldPLD(t *testing.T, c *PLDCache, target, node string, age int64) *PLDRecord {
	t.Helper()
	r := c.Get(target, node)
	r.FillFailure(60)
	r.mu.Lock()
	r.LastUpdated = age
	r.mu.Unlock()
	if err := c.Save(target, node, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCacheClosureEvictFreshSaveAfterScan(t *testing.T) {
	for _, update := range []string{"fresh-save", "identical-save", "transaction-recheck", "fresh-live-owner"} {
		t.Run(update, func(t *testing.T) {
			st := realCacheStore(t)
			c, second := st.NewPLDCache("cfg", "grp"), st.NewPLDCache("cfg", "grp")
			defer c.Close()
			defer second.Close()
			cutoff := time.Now().Unix() - 60
			r := closureOldPLD(t, c, "target", "A", cutoff-60)
			other := second.Get("target", "A")
			key := FormatPLDKey("cfg", "grp", "target", "A")
			before, err := st.DBViewGetItem(key)
			if err != nil {
				t.Fatal(err)
			}
			// Another unchanged aged key must still be evicted: freshness
			// protection cannot disable the whole namespace's cleanup.
			stale := closureOldPLD(t, c, "stale", "A", cutoff-60)
			pause, entered, release := closurePause()
			st.afterPLDEvictionScan = pause
			defer release()
			done := make(chan struct{})
			go func() { defer close(done); st.EvictOldPLDRecords(cutoff) }()
			closureWait(t, entered)
			switch update {
			case "fresh-save":
				r.FillFailure(60)
				if err := c.Save("target", "A", r); err != nil {
					t.Fatal(err)
				}
			case "identical-save":
				if err := c.Save("target", "A", r); err != nil {
					t.Fatal(err)
				}
				got, err := st.DBViewGetItem(key)
				if err != nil || !bytes.Equal(before, got) {
					t.Fatal("test failed to perform an identical-value save")
				}
			case "transaction-recheck":
				// A real DB writer bypasses the version publisher. The delete
				// transaction must independently reread the value and age.
				data, err := json.Marshal(&PLDRecord{LastUpdated: cutoff + 10, SampleCount: 17})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(bucketSmartStats).Put([]byte(key), data) }); err != nil {
					t.Fatal(err)
				}
			case "fresh-live-owner":
				other.FillFailure(60) // not yet saved; any live owner protects it
			}
			want, err := st.DBViewGetItem(key)
			if err != nil {
				t.Fatal(err)
			}
			release()
			closureWait(t, done)
			got, err := st.DBViewGetItem(key)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("new save/current transaction row lost: got=%s want=%s err=%v", got, want, err)
			}
			if c.Get("target", "A") != r || second.Get("target", "A") != other {
				t.Fatal("protected live owners were retired")
			}
			if err := c.Save("stale", "A", stale); err != nil {
				t.Fatal(err)
			}
			if _, err := st.DBViewGetItem(FormatPLDKey("cfg", "grp", "stale", "A")); err == nil {
				t.Fatal("unchanged aged key was skipped or its old reference revived")
			}
		})
	}
}

func TestCacheClosurePLDVersionScopeCleanup(t *testing.T) {
	for _, level := range []string{"all", "config", "group"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			scopes := []writeScope{{"cfg", "grp", "A"}, {"cfg", "grp-other", "A"}, {"cfg-other", "grp", "A"}}
			for _, scope := range scopes {
				if err := st.DBBatchPutItem(FormatPLDKey(scope.config, scope.group, "target", scope.node), []byte(`{"last_updated":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			closureFlush(t, st, level)
			pldWrites.mu.Lock()
			defer pldWrites.mu.Unlock()
			for _, scope := range scopes {
				_, found := pldWrites.versions[FormatPLDKey(scope.config, scope.group, "target", scope.node)]
				if found == matchesScope(level, "cfg", "grp", scope.config, scope.group) {
					t.Errorf("PLD write generation cleanup crossed/retained scope: %v", scope)
				}
			}
		})
	}
}

func TestCacheClosureEvictAllLiveOwners(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "store"
		if scoped {
			name = "cache"
		}
		t.Run(name, func(t *testing.T) {
			st := realCacheStore(t)
			cutoff := time.Now().Unix() - 60
			owners := []*PLDCache{st.NewPLDCache("cfg", "grp"), st.NewPLDCache("cfg", "grp")}
			old := closureOldPLD(t, owners[0], "target", "A", cutoff-60)
			oldOther := owners[1].Get("target", "A")
			for _, c := range owners {
				defer c.Close()
				c.MarkSwitch("target")
			}
			neighbors := []*PLDCache{st.NewPLDCache("cfg", "grp-other"), st.NewPLDCache("cfg-other", "grp")}
			kept := make([]*PLDRecord, len(neighbors))
			for i, c := range neighbors {
				defer c.Close()
				age := cutoff + 1
				if scoped {
					age = cutoff - 60 // scoped eviction must preserve even aged neighbors
				}
				kept[i] = closureOldPLD(t, c, "target", "A", age)
				c.MarkSwitch("target")
			}
			pause, entered, release := closurePause()
			st.afterPLDEvictionScan = pause
			defer release()
			done := make(chan struct{})
			go func() {
				defer close(done)
				if scoped {
					owners[0].Evict(cutoff, time.Now().Add(time.Hour))
				} else {
					st.EvictOldPLDRecords(cutoff)
				}
			}()
			closureWait(t, entered)
			// Reload/adoption through another live owner while the completed
			// DB scan is paused must also be coordinated by the commit.
			late := st.NewPLDCache("cfg", "grp")
			defer late.Close()
			lateOld := late.Get("target", "A")
			release()
			closureWait(t, done)
			refs := []*PLDRecord{old, oldOther, lateOld}
			allOwners := append(owners, late)
			for i, c := range allOwners {
				refs[i].FillFailure(60)
				if err := c.Save("target", "A", refs[i]); err != nil {
					t.Fatal(err)
				}
				if r := c.Get("target", "A"); r == refs[i] || r.GetLastUpdated() != 0 {
					t.Fatal("old live owner reloaded/revived deleted data")
				}
			}
			key := FormatPLDKey("cfg", "grp", "target", "A")
			if _, err := st.DBViewGetItem(key); err == nil {
				t.Fatal("old reference resurrected the evicted row")
			}
			fresh := owners[0].Get("target", "A")
			fresh.FillFailure(60)
			if err := owners[0].Save("target", "A", fresh); err != nil {
				t.Fatal(err)
			}
			want, err := st.DBViewGetItem(key)
			if err != nil {
				t.Fatal(err)
			}
			for i, c := range allOwners {
				if err := c.Save("target", "A", refs[i]); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := st.DBViewGetItem(key); err != nil || !bytes.Equal(got, want) {
				t.Fatal("retired same-key references overwrote a new incarnation")
			}
			for i, c := range neighbors {
				if c.Get("target", "A") != kept[i] || c.SwitchCooldownElapsed("target", time.Hour) {
					t.Fatal("neighbor owner/reward/switch was incorrectly retired")
				}
				if _, err := st.DBViewGetItem(FormatPLDKey(c.config, c.group, "target", "A")); err != nil {
					t.Fatal("neighbor DB row was incorrectly deleted")
				}
			}
		})
	}
}
