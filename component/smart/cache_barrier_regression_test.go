package smart

import (
	"math"
	"runtime"
	"testing"
	"time"
	"weak"
)

func TestCacheFlushWaitsForInflightSave(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	defer c.Close()
	r := c.Get("target", "A")
	r.FillFailure(60)
	// Hold the record marshal lock. When Save has acquired c.mu it has also
	// acquired storeBarrier.RLock and must keep it until bbolt commits.
	r.mu.Lock()
	saved := make(chan error, 1)
	go func() { saved <- c.Save("target", "A", r) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if !c.mu.TryLock() {
			break
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			r.mu.Unlock()
			t.Fatal("save never reached record barrier")
		}
		runtime.Gosched()
	}
	flushed := make(chan error, 1)
	go func() { flushed <- st.FlushByGroup("grp", "cfg") }()
	// A queued writer blocks TryRLock: wait for this state, not a sleep-based
	// guess about goroutine scheduling, then assert Flush cannot complete.
	for {
		if !storeBarrier.TryRLock() {
			break
		}
		storeBarrier.RUnlock()
		if time.Now().After(deadline) {
			r.mu.Unlock()
			t.Fatal("flush never queued on barrier")
		}
		runtime.Gosched()
	}
	select {
	case <-flushed:
		r.mu.Unlock()
		t.Fatal("flush bypassed inflight save")
	default:
	}
	r.mu.Unlock()
	if err := <-saved; err != nil {
		t.Fatal(err)
	}
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if _, err := st.DBViewGetItem(FormatPLDKey("cfg", "grp", "target", "A")); err == nil {
		t.Fatal("inflight commit survived flush")
	}
	if err := c.Save("target", "A", r); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DBViewGetItem(FormatPLDKey("cfg", "grp", "target", "A")); err == nil {
		t.Fatal("retired inflight record revived")
	}
}

func TestCacheLifecycleErrorAndQueueBranches(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	defer c.Close()
	r := c.Get("target", "A")
	r.Reward = math.NaN()
	if err := c.Save("target", "A", r); err == nil {
		t.Fatal("invalid JSON reward accepted")
	}
	r.Reward = 0
	st.AppendToGlobalQueue()
	ops := []StoreOperation{{Type: OpSaveStats, Config: "cfg", Group: "grp", Target: "target", Node: "A", Data: []byte(`{}`)}}
	storeBarrier.RLock()
	stampOperations(ops)
	stampOperations(ops)
	storeBarrier.RUnlock()
	st.AppendToGlobalQueue(ops...)
	st.AppendToGlobalQueue(StoreOperation{Type: OpSaveStats, Config: "cfg", Group: "other", Target: "target", Node: "B", Data: []byte(`{}`)})
	if err := st.RemoveNodesData("grp", "cfg", 5, []string{"A"}); err != nil {
		t.Fatal(err)
	}
	if opQueue.size() != 1 {
		t.Fatal("unrelated queued write removed")
	}
	if err := st.FlushByConfig("cfg"); err != nil {
		t.Fatal(err)
	}
	st.DeleteUnwrapResult("grp", "cfg", "")
	// Dead weak write tokens are swept periodically and during invalidation.
	cacheRegistry.mu.Lock()
	cacheRegistry.writes[writeScope{"dead", "dead", "dead"}] = weak.Pointer[writeGeneration]{}
	cacheRegistry.writeSweeps = 256
	cacheRegistry.mu.Unlock()
	storeBarrier.RLock()
	stampOperations(ops)
	storeBarrier.RUnlock()
	cacheRegistry.mu.Lock()
	_, exists := cacheRegistry.writes[writeScope{"dead", "dead", "dead"}]
	cacheRegistry.mu.Unlock()
	if exists {
		t.Fatal("dead write generation not swept")
	}
	cacheRegistry.mu.Lock()
	cacheRegistry.writes[writeScope{"dead", "dead", "dead"}] = weak.Pointer[writeGeneration]{}
	cacheRegistry.mu.Unlock()
	if err := st.FlushAll(); err != nil {
		t.Fatal(err)
	}
	// Real API failure propagation, without substituting a fake DB response.
	oldDB := db
	db = nil
	defer func() { db = oldDB }()
	for _, level := range []string{"all", "config", "group"} {
		if err := st.FlushByLevel(level, "cfg", "grp"); err == nil {
			t.Fatalf("%s swallowed DB failure", level)
		}
	}
}
