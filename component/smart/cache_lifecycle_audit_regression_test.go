package smart

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCacheAuditOrphanGroupBoundary(t *testing.T) {
	st := realCacheStore(t)
	rows := make(map[string][]byte)
	for _, scope := range []struct{ cfg, grp string }{{"cfg", "grp"}, {"cfg", "grp-other"}, {"cfg-other", "grp"}} {
		for _, row := range []struct{ kind, target, node, data string }{
			{KeyTypeStats, "target", "A", `{"success":7}`},
			{KeyTypeNode, "", "A", `{"name":"A","blocked_until":9999999999}`},
			{KeyTypePrefetch, "target", "", `{"tcp":{"nodes":["A"],"weights":[1]}}`},
			{KeyTypeRanking, "", "", `{"result":[{"Name":"A","Weight":1}]}`},
			{KeyTypeHostFailures, "target", "", `{"codes":{"3":{"nodes":{"A":9999999999}}}}`},
		} {
			key := FormatDBKey(row.kind, scope.cfg, scope.grp, row.target, row.node)
			rows[key] = []byte(row.data)
			if err := st.DBBatchPutItem(key, rows[key]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.RemoveNodesData("grp", "cfg", 5, []string{"A"}); err != nil {
		t.Fatal(err)
	}
	for key, before := range rows {
		got, err := st.DBViewGetItem(key)
		own := false
		for _, kind := range []string{KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeRanking, KeyTypeHostFailures} {
			if key == FormatDBKey(kind, "cfg", "grp") || len(key) > len(FormatDBKey(kind, "cfg", "grp")) && key[:len(FormatDBKey(kind, "cfg", "grp"))+1] == FormatDBKey(kind, "cfg", "grp")+"/" {
				own = true
			}
		}
		if own {
			if err == nil {
				t.Errorf("orphan row survived: %s", key)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(got, before) {
			t.Errorf("neighbor row changed: %s got=%s err=%v", key, got, err)
		}
	}
}

func TestCacheAuditAtomicOldReference(t *testing.T) {
	for _, level := range []string{"all", "config", "group", "node"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			key := FormatDBKey(KeyTypeStats, "cfg", "grp", "target", "A")
			old := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "target", "A")
			old.AddSuccess(7)
			if level == "node" {
				if err := st.RemoveNodesData("grp", "cfg", 5, []string{"A"}); err != nil {
					t.Fatal(err)
				}
			} else if err := st.FlushByLevel(level, "cfg", "grp"); err != nil {
				t.Fatal(err)
			}
			fresh := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "target", "A")
			old.AddSuccess(1)
			snapshot := old.CreateStatsSnapshot(key)
			if r, _ := recordCache.Get(key); r != fresh {
				t.Error("old snapshot replaced fresh recordCache identity")
			}
			data, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			st.AppendToGlobalQueue(StoreOperation{Type: OpSaveStats, Config: "cfg", Group: "grp", Target: "target", Node: "A", Data: data, StatsSnapshot: snapshot})
			st.FlushQueue(true)
			if got, err := st.DBViewGetItem(key); err == nil {
				t.Errorf("old reference resurrected DB: %s", got)
			}
			if fresh.Success() != 0 {
				t.Fatalf("fresh inherited success=%d", fresh.Success())
			}
		})
	}
}

func TestCacheAuditViewRefreshAfterFlush(t *testing.T) {
	for _, level := range []string{"all", "config", "group", "node"} {
		t.Run(level, func(t *testing.T) {
			st := realCacheStore(t)
			prefix := FormatDBKey(KeyTypeNode, "cfg", "grp")
			row := FormatDBKey(KeyTypeNode, "cfg", "grp", "A")
			raw := []byte(`{"name":"A","blocked_until":9999999999}`)
			if err := st.DBBatchPutItem(row, raw); err != nil {
				t.Fatal(err)
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				_, _ = st.refreshViewCache(prefix, 100, true, func(p string, n int, strict bool) (map[string][]byte, error) {
					rows, err := st.DBViewPrefixScan(p, n, strict)
					close(entered)
					<-release
					return rows, err
				})
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real scan did not finish")
			}
			flushed := make(chan error, 1)
			go func() {
				if level == "node" {
					flushed <- st.RemoveNodesData("grp", "cfg", 5, []string{"A"})
				} else {
					flushed <- st.FlushByLevel(level, "cfg", "grp")
				}
			}()
			select {
			case err := <-flushed:
				if err != nil {
					close(release)
					<-done
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				close(release)
				<-done
				<-flushed
				t.Fatal("Flush blocked on paused scan/publication: lifecycle lock held too long")
			}
			if _, err := st.DBViewGetItem(row); err == nil {
				close(release)
				<-done
				t.Fatal("Flush did not remove DB row")
			}
			// A freshly scanned view must remain intact when the retired scan
			// publishes later (not merely an empty cache planted by the test).
			freshRow := FormatDBKey(KeyTypeNode, "cfg", "grp", "B")
			freshRaw := []byte(`{"name":"B","blocked_until":0}`)
			if err := st.DBBatchPutItem(freshRow, freshRaw); err != nil {
				close(release)
				<-done
				t.Fatal(err)
			}
			freshStates, err := st.GetNodeStates("grp", "cfg")
			if err != nil || len(freshStates) != 1 || string(freshStates["B"]) != string(freshRaw) {
				close(release)
				<-done
				t.Fatalf("fresh view missing: %v %v", freshStates, err)
			}
			close(release)
			<-done
			states, err := st.GetNodeStates("grp", "cfg")
			if err != nil || len(states) != 1 || states["A"] != nil || string(states["B"]) != string(freshRaw) {
				t.Fatalf("stale DB view resurrected A or erased fresh B: %v err=%v", states, err)
			}
		})
	}
}

func TestCacheAuditViewAncestorAndNeighborScope(t *testing.T) {
	for _, scope := range []struct {
		name, cfg, grp    string
		ancestor, removed bool
	}{
		{"group", "cfg", "grp", false, true},
		{"config-ancestor", "cfg", "grp", true, true},
		{"neighbor-group", "cfg", "grp-other", false, false},
		{"neighbor-config", "cfg-other", "grp", false, false},
	} {
		t.Run(scope.name, func(t *testing.T) {
			st := realCacheStore(t)
			prefix := FormatDBKey(KeyTypeNode, scope.cfg, scope.grp)
			if scope.ancestor {
				prefix = FormatDBKey(KeyTypeNode, scope.cfg)
			}
			row := FormatDBKey(KeyTypeNode, scope.cfg, scope.grp, "A")
			if err := st.DBBatchPutItem(row, []byte(`{"name":"A"}`)); err != nil {
				t.Fatal(err)
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				_, _ = st.refreshViewCache(prefix, 100, true, func(p string, n int, strict bool) (map[string][]byte, error) {
					rows, err := st.DBViewPrefixScan(p, n, strict)
					close(entered)
					<-release
					return rows, err
				})
			}()
			<-entered
			if err := st.FlushByGroup("grp", "cfg"); err != nil {
				close(release)
				<-done
				t.Fatal(err)
			}
			close(release)
			<-done
			rows, err := st.GetSubBytesByPath(prefix)
			if err != nil {
				t.Fatal(err)
			}
			_, found := rows[row]
			if found == scope.removed {
				t.Fatalf("wrong read invalidation scope: rows=%v removed=%v", rows, scope.removed)
			}
		})
	}
}

func TestCacheAuditPublishedConfigViewsFlush(t *testing.T) {
	for _, kind := range []string{KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeRanking, KeyTypeHostFailures} {
		t.Run(kind, func(t *testing.T) {
			st := realCacheStore(t)
			prefix := FormatDBKey(kind, "cfg")
			neighborPrefix := FormatDBKey(kind, "cfg-other")
			oldKey := FormatDBKey(kind, "cfg", "grp", "target", "A")
			freshKey := FormatDBKey(kind, "cfg", "grp", "target", "B")
			neighborKey := FormatDBKey(kind, "cfg-other", "grp", "target", "N")
			oldRaw, freshRaw, neighborRaw := []byte(`{"name":"A"}`), []byte(`{"name":"B"}`), []byte(`{"name":"N"}`)
			for key, raw := range map[string][]byte{oldKey: oldRaw, neighborKey: neighborRaw} {
				if err := st.DBBatchPutItem(key, raw); err != nil {
					t.Fatal(err)
				}
			}
			if rows, err := st.GetSubBytesByPath(prefix); err != nil || string(rows[oldKey]) != string(oldRaw) {
				t.Fatalf("config view was not published: %v %v", rows, err)
			}
			if rows, err := st.GetSubBytesByPath(neighborPrefix); err != nil || string(rows[neighborKey]) != string(neighborRaw) {
				t.Fatalf("neighbor view was not published: %v %v", rows, err)
			}
			// Stats/failures deliberately bypass view caching to merge live
			// queued writes; only require an existing cached neighbor to survive.
			_, neighborWasCached := dbResultCache.Get(neighborPrefix)
			if err := st.FlushByConfig("cfg"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.DBViewGetItem(oldKey); err == nil {
				t.Fatal("Flush did not remove actual DB row")
			}
			if _, ok := dbResultCache.Get(prefix); ok {
				t.Error("published exact config view survived Flush")
			}
			if _, ok := dbResultCache.Get(neighborPrefix); neighborWasCached && !ok {
				t.Error("neighbor config view was invalidated")
			}
			if err := st.DBBatchPutItem(freshKey, freshRaw); err != nil {
				t.Fatal(err)
			}
			rows, err := st.GetSubBytesByPath(prefix)
			if err != nil || len(rows) != 1 || rows[oldKey] != nil || string(rows[freshKey]) != string(freshRaw) {
				t.Fatalf("config view retained A or lost fresh B: %v err=%v", rows, err)
			}
			rows, err = st.GetSubBytesByPath(neighborPrefix)
			if err != nil || len(rows) != 1 || string(rows[neighborKey]) != string(neighborRaw) {
				t.Fatalf("neighbor config changed: %v err=%v", rows, err)
			}
		})
	}
}

func cacheAuditStatsOperation(t *testing.T, snapshot *StatsRecord, cfg, grp, target, node string) StoreOperation {
	t.Helper()
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return StoreOperation{Type: OpSaveStats, Config: cfg, Group: grp, Target: target, Node: node, Data: data, StatsSnapshot: snapshot}
}

func TestCacheAuditSnapshotProvenanceAndFreshWrites(t *testing.T) {
	for _, target := range []string{"target", "IP-CIDR [10.0.0.0/8]"} {
		t.Run(target, func(t *testing.T) {
			st := realCacheStore(t)
			key := FormatDBKey(KeyTypeStats, "cfg", "grp", target, "A")
			old := st.GetOrCreateAtomicRecord(key, "grp", "cfg", target, "A")
			old.AddSuccess(7)
			before := old.CreateStatsSnapshot(key)
			serialized := cacheAuditStatsOperation(t, before, "cfg", "grp", target, "A")
			st.AppendToGlobalQueue(serialized)
			detached := opQueue.drain(true)
			if err := st.FlushByGroup("grp", "cfg"); err != nil {
				t.Fatal(err)
			}
			// A serialized snapshot created before Flush but enqueued afterwards
			// must retain its source token, never receive a fresh write generation.
			st.AppendToGlobalQueue(serialized)
			if opQueue.size() != 0 {
				t.Fatal("retired serialized snapshot accepted")
			}
			fresh := st.GetOrCreateAtomicRecord(key, "grp", "cfg", target, "A")
			fresh.AddSuccess(3)
			freshOp := cacheAuditStatsOperation(t, fresh.CreateStatsSnapshot(key), "cfg", "grp", target, "A")
			st.AppendToGlobalQueue(freshOp)
			old.AddSuccess(1)
			st.AppendToGlobalQueue(cacheAuditStatsOperation(t, old.CreateStatsSnapshot(key), "cfg", "grp", target, "A"))
			if queued, ok := opQueue.get(key); !ok || string(queued.Data) != string(freshOp.Data) {
				t.Fatal("old write replaced queued fresh data")
			}
			if err := st.BatchSave(detached); err != nil {
				t.Fatal(err)
			}
			st.FlushQueue(true)
			if err := st.BatchSave([]StoreOperation{serialized}); err != nil {
				t.Fatal(err)
			}
			got, err := st.DBViewGetItem(key)
			if err != nil || string(got) != string(freshOp.Data) {
				t.Fatalf("fresh write lost/replaced: %s err=%v", got, err)
			}
			if current, _ := recordCache.Get(key); current != fresh {
				t.Fatal("fresh cache identity lost")
			}
		})
	}
}

func TestCacheAuditManualSnapshotAndSchema(t *testing.T) {
	st := realCacheStore(t)
	key := FormatDBKey(KeyTypeStats, "cfg", "grp", "manual", "A")
	manual := &AtomicStatsRecord{}
	manual.AddSuccess(5)
	snapshot := manual.CreateStatsSnapshot(key)
	if got, _ := recordCache.Get(key); got != manual {
		t.Fatal("manual uncached first writer not inserted")
	}
	op := cacheAuditStatsOperation(t, snapshot, "cfg", "grp", "manual", "A")
	var jsonFields map[string]json.RawMessage
	if err := json.Unmarshal(op.Data, &jsonFields); err != nil {
		t.Fatal(err)
	}
	want := []string{"success", "failure", "connect_time", "latency", "last_used", "weights", "upload_total", "download_total", "max_upload_rate", "max_download_rate", "connection_duration"}
	if len(jsonFields) != len(want) {
		t.Fatalf("schema gained/lost fields: %s", op.Data)
	}
	for _, field := range want {
		if _, ok := jsonFields[field]; !ok {
			t.Errorf("missing schema field %s", field)
		}
	}
	st.AppendToGlobalQueue(op)
	// Truly manual raw rows are still supported; provenance is runtime data,
	// not a new JSON field and not a blanket rejection of statistics writes.
	raw := StoreOperation{Type: OpSaveStats, Config: "cfg", Group: "grp", Target: "raw", Node: "B", Data: []byte(`{"success":4}`)}
	st.AppendToGlobalQueue(raw)
	st.FlushQueue(true)
	if got, err := st.DBViewGetItem(key); err != nil || string(got) != string(op.Data) {
		t.Fatalf("manual snapshot rejected: %s %v", got, err)
	}
	if got, err := st.DBViewGetItem(formatOperationKey(&raw)); err != nil || string(got) != string(raw.Data) {
		t.Fatalf("raw manual write rejected: %s %v", got, err)
	}
	// Identity alone also prevents an evicted incarnation from repinning a
	// replacement without waiting for a scoped Flush to invalidate its token.
	recordCache.Delete(key)
	replacement := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "manual", "A")
	manual.AddSuccess(2)
	st.AppendToGlobalQueue(cacheAuditStatsOperation(t, manual.CreateStatsSnapshot(key), "cfg", "grp", "manual", "A"))
	st.FlushQueue(true)
	if got, _ := recordCache.Get(key); got != replacement {
		t.Fatal("evicted identity replaced current incarnation")
	}
	if got, _ := st.DBViewGetItem(key); string(got) != string(op.Data) {
		t.Fatalf("evicted incarnation committed: %s", got)
	}
	// A sourced snapshot cannot silently be retargeted to a different node.
	wrong := op
	wrong.Node = "Z"
	st.AppendToGlobalQueue(wrong)
	st.FlushQueue(true)
	if _, err := st.DBViewGetItem(formatOperationKey(&wrong)); err == nil {
		t.Fatal("source identity retargeted")
	}
}

func TestCacheAuditReadGroupAndTargetBoundaries(t *testing.T) {
	st := realCacheStore(t)
	for _, scope := range []writeScope{{"cfg", "grp", "A"}, {"cfg", "grp-other", "Z"}, {"cfg-other", "grp", "Y"}} {
		for _, kind := range []string{KeyTypeNode, KeyTypeStats, KeyTypePrefetch, KeyTypeHostFailures, KeyTypeRanking} {
			key := FormatDBKey(kind, scope.config, scope.group)
			switch kind {
			case KeyTypeNode:
				key = FormatDBKey(kind, scope.config, scope.group, scope.node)
			case KeyTypeStats:
				key = FormatDBKey(kind, scope.config, scope.group, "target", scope.node)
			case KeyTypePrefetch, KeyTypeHostFailures:
				key = FormatDBKey(kind, scope.config, scope.group, "target")
			}
			if err := st.DBBatchPutItem(key, []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, kind := range []string{KeyTypeNode, KeyTypeStats, KeyTypePrefetch, KeyTypeHostFailures, KeyTypeRanking} {
		prefix := FormatDBKey(kind, "cfg", "grp")
		rows, err := st.GetSubBytesByPath(prefix)
		if err != nil || len(rows) != 1 {
			t.Fatalf("group scan %s rows=%v err=%v", kind, rows, err)
		}
		for key := range rows {
			if key != prefix && !strings.HasPrefix(key, prefix+"/") {
				t.Fatalf("group prefix leak: %s", key)
			}
		}
	}
	st.AppendToGlobalQueue(StoreOperation{Type: OpSaveNodeState, Config: "cfg", Group: "grp", Node: "B", Data: []byte(`{}`)})
	states, err := st.GetNodeStates("grp", "cfg")
	if err != nil || len(states) != 2 || states["A"] == nil || states["B"] == nil {
		t.Fatalf("strict group read stopped merging DB and queue: %v %v", states, err)
	}
	if err := st.DBBatchPutItem(FormatDBKey(KeyTypeStats, "cfg", "grp", "target-other", "Z"), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.GetStatsForTarget("grp", "cfg", "target", "")
	if err != nil || len(stats) != 1 || stats["A"] == nil {
		t.Fatalf("target prefix leak: %v %v", stats, err)
	}
}

func TestCacheAuditNodesAllLiveOwners(t *testing.T) {
	st := realCacheStore(t)
	current := st.NewPLDCache("cfg", "grp")
	defer current.Close()
	other := st.NewPLDCache("cfg", "grp")
	defer other.Close()
	neighbor := st.NewPLDCache("cfg", "grp-other")
	defer neighbor.Close()
	other.Get("live-only", "A").FillFailure(60)
	neighbor.Get("neighbor", "Z").FillFailure(60)
	nodes := current.Nodes()
	if !reflect.DeepEqual(nodes, []string{"A"}) {
		t.Fatalf("missing other live owner / neighbor leaked: %v", nodes)
	}
	other.MarkSwitch("live-only")
	if err := st.RemoveNodesData("grp", "cfg", 5, nodes); err != nil {
		t.Fatal(err)
	}
	if !other.SwitchCooldownElapsed("live-only", time.Hour) {
		t.Fatal("other clock retained")
	}
}
