package outboundgroup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

func TestCacheAuditStatsSaveIntegration(t *testing.T) {
	for _, mode := range []string{"smart", "smart-pld"} {
		t.Run(mode, func(t *testing.T) {
			st := liveCacheStore(t)
			a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
			pld, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a})
			regular := &Smart{GroupBase: NewGroupBase(GroupBaseOption{Name: "grp", Type: C.Smart}), store: st, configName: "cfg"}
			save := func(snapshot *smart.StatsRecord) {
				if mode == "smart" {
					regular.saveStatsRecord("target", a, snapshot)
				} else {
					pld.saveStatsRecord("target", a, snapshot)
				}
			}
			key := smart.FormatDBKey(smart.KeyTypeStats, "cfg", "grp", "target", "A")
			old := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "target", "A")
			old.AddSuccess(7)
			before := old.CreateStatsSnapshot(key)
			save(before)
			st.FlushQueue(true)
			if err := st.FlushByGroup("grp", "cfg"); err != nil {
				t.Fatal(err)
			}
			fresh := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "target", "A")
			old.AddSuccess(1)
			save(old.CreateStatsSnapshot(key))
			save(before)
			st.FlushQueue(true)
			if data, err := st.DBViewGetItem(key); err == nil {
				t.Fatalf("old saveStatsRecord resurrected DB: %s", data)
			}
			fresh.AddSuccess(3)
			save(fresh.CreateStatsSnapshot(key))
			save(old.CreateStatsSnapshot(key))
			st.FlushQueue(true)
			data, err := st.DBViewGetItem(key)
			if err != nil {
				t.Fatal(err)
			}
			var persisted smart.StatsRecord
			if err := json.Unmarshal(data, &persisted); err != nil || persisted.Success != 3 {
				t.Fatalf("fresh save lost/overwritten: %s %v", data, err)
			}
			if got := st.GetOrCreateAtomicRecord(key, "grp", "cfg", "target", "A"); got != fresh || got.Success() != 3 {
				t.Fatal("old snapshot replaced fresh cache identity")
			}
		})
	}
}

func TestCacheAuditOrphanOtherLiveOnly(t *testing.T) {
	st := liveCacheStore(t)
	a := &unwrapBarrierProxy{name: "A", alive: true, udp: true}
	b := &unwrapBarrierProxy{name: "B", alive: true, udp: true}
	current, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{b})
	other, _ := liveCacheFixture(t, st, "cfg", "grp", []C.Proxy{a, b})
	neighbor, _ := liveCacheFixture(t, st, "cfg", "grp-other", []C.Proxy{a, b})
	old := other.getPLDRecord("live-only", "A")
	old.FillFailure(60)
	other.markPrimarySwitch("live-only")
	keep := other.getPLDRecord("kept", "B")
	keep.FillFailure(60)
	other.markPrimarySwitch("kept")
	n := neighbor.getPLDRecord("live-only", "A")
	n.FillFailure(60)
	neighbor.markPrimarySwitch("live-only")
	current.cleanupOrphanedNodeCache()
	other.savePLDRecord("live-only", "A", old)
	if _, err := st.DBViewGetItem(smart.FormatPLDKey("cfg", "grp", "live-only", "A")); err == nil {
		t.Error("other live-only old save resurrected DB")
	}
	fresh := other.getPLDRecord("live-only", "A")
	if fresh == old || fresh.InFailureCooldown(time.Now().Unix()) || !other.switchCooldownElapsed("live-only") {
		t.Error("other live-only identity/cooldown/switch clock survived orphan cleanup")
	}
	if other.getPLDRecord("kept", "B") != keep || other.switchCooldownElapsed("kept") {
		t.Error("unrelated B reset")
	}
	if neighbor.getPLDRecord("live-only", "A") != n || neighbor.switchCooldownElapsed("live-only") {
		t.Error("neighbor group reset")
	}
	fresh.FillFailure(60)
	other.savePLDRecord("live-only", "A", fresh)
	other.savePLDRecord("live-only", "A", old)
	reader := st.NewPLDCache("cfg", "grp")
	defer reader.Close()
	if _, _, count := reader.Get("live-only", "A").ChannelStats(false); count != 1 {
		t.Errorf("fresh write lost/old replaced: samples=%d", count)
	}
}
