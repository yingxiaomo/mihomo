package smart

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPLDSwitchClosureVersionAndClock(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	t.Cleanup(c.Close)
	const target = "target"
	if c.TryPrimarySwitch("", cacheProxies("A"), 0, time.Hour, false) || c.TryPrimarySwitch(target, nil, 0, time.Hour, false) {
		t.Fatal("invalid switch accepted")
	}
	st.ForceStoreUnwrapResult("grp", "cfg", target, cacheProxies("A"))
	old, _ := st.GetUnwrapSnapshot("grp", "cfg", target)
	// Same-name A->A publication is still a new generation (ABA).
	st.ForceStoreUnwrapResult("grp", "cfg", target, cacheProxies("A"))
	key := FormatDBKey("cfg", "grp", target)
	fresh, expiry, _ := unwrapCache.GetWithExpire(key)
	for _, version := range []uint64{0, old.Version} {
		if c.TryPrimarySwitch(target, cacheProxies("B"), version, time.Hour, true) {
			t.Fatal("stale forced switch accepted")
		}
		current, currentExpiry, _ := unwrapCache.GetWithExpire(key)
		if current.Version != fresh.Version || !currentExpiry.Equal(expiry) || !c.SwitchCooldownElapsed(target, time.Hour) {
			t.Fatal("rejected switch mutated version, TTL or clock")
		}
	}
	if !c.TryPrimarySwitch(target, cacheProxies("B"), fresh.Version, time.Hour, false) {
		t.Fatal("legal switch rejected")
	}
	b, bExpiry, _ := unwrapCache.GetWithExpire(key)
	c.mu.Lock()
	committedAt := c.switches[target]
	c.mu.Unlock()
	if c.SwitchCooldownElapsed(target, time.Hour) {
		t.Fatal("successful switch did not consume clock")
	}
	if c.TryPrimarySwitch(target, cacheProxies("C"), b.Version, time.Hour, false) {
		t.Fatal("fresh-generation challenger bypassed cooldown")
	}
	after, afterExpiry, _ := unwrapCache.GetWithExpire(key)
	c.mu.Lock()
	afterAt := c.switches[target]
	c.mu.Unlock()
	if after.Version != b.Version || !afterExpiry.Equal(bExpiry) || !afterAt.Equal(committedAt) {
		t.Fatal("cooldown rejection changed publication or clock")
	}
	if !c.TryPrimarySwitch(target, cacheProxies("C"), b.Version, time.Hour, true) {
		t.Fatal("failure avoidance did not bypass clock")
	}
	latest, _ := st.GetUnwrapSnapshot("grp", "cfg", target)
	if latest.Proxies[0] != "C" {
		t.Fatalf("wrong forced primary: %+v", latest)
	}
	c.Close()
	if c.TryPrimarySwitch(target, cacheProxies("D"), latest.Version, 0, true) {
		t.Fatal("closed owner committed a switch")
	}
}

func TestPLDSwitchClosureExpectedAbsenceAndConcurrentCooldown(t *testing.T) {
	st := realCacheStore(t)
	c := st.NewPLDCache("cfg", "grp")
	t.Cleanup(c.Close)
	start := make(chan struct{})
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			current, found := st.GetUnwrapSnapshot("grp", "cfg", "target")
			var expected uint64
			if found {
				expected = current.Version
			}
			if c.TryPrimarySwitch("target", cacheProxies("B"), expected, time.Hour, false) {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("concurrent healthy switches committed %d times", successes.Load())
	}
	current, found := st.GetUnwrapSnapshot("grp", "cfg", "target")
	if !found || current.Proxies[0] != "B" || c.SwitchCooldownElapsed("target", time.Hour) {
		t.Fatal("commit result/clock not published together")
	}
}

func TestPLDSwitchClosureLargeSettlementsAvailabilityDenominator(t *testing.T) {
	r := new(PLDRecord)
	good := RewardedMetrics{ConnectTimeMs: 10, FirstByteMs: 50, DownloadMB: 2, DurationSec: 1, LossValid: true}
	bad := RewardedMetrics{ConnectTimeMs: 10, DurationSec: 1, Failed: true}
	for i := 0; i < 995; i++ {
		r.UpdateWithMetrics(good, 0)
		r.ObserveMetrics(good)
	}
	for i := 0; i < 5; i++ {
		r.UpdateWithMetrics(bad, 0)
		r.ObserveMetrics(bad)
	}
	with := r.ScenarioScore(true)
	if r.AttemptCount != 1000 || r.FailureCount != 5 || r.SampleCount != 5 {
		t.Fatal("unexpected settlement population")
	}
	r.FailureCount = 0
	without := r.ScenarioScore(true)
	if with != without {
		t.Fatalf("large-success node received spurious availability penalty: %.6f vs %.6f", with, without)
	}
}
