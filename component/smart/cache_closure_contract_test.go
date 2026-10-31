package smart

import (
	"math"
	"testing"
	"time"
)

func TestCacheClosureBlockedIncrementalNilAndUnblock(t *testing.T) {
	st := realCacheStore(t)
	future := time.Now().Unix() + 600
	st.UpdateBlockedNodesCache("grp", "cfg", map[string]*NodeState{"keep": {BlockedUntil: future}, "remove": {BlockedUntil: future}})
	old := st.GetBlockedNodes("grp", "cfg")
	st.UpdateBlockedNodesCache("grp", "cfg", map[string]*NodeState{"keep": nil, "remove": {}, "add": {BlockedUntil: future}})
	current := st.GetBlockedNodes("grp", "cfg")
	if !old["keep"] || !old["remove"] || old["add"] || !current["keep"] || current["remove"] || !current["add"] {
		t.Fatal("incremental publication mutated old map, lost nil-state entry or failed to unblock")
	}
}

func TestTTFRShadowScenarioMissingSignalsAndBounds(t *testing.T) {
	records := []*PLDRecord{
		{AttemptCount: 1},
		{AttemptCount: 10, ConnectionTTFR_EMA: 100, ConnectionTTFR_Cnt: 10},
		{AttemptCount: 10, ConnectCnt: -1, ThrCount: -1},
		{AttemptCount: 10, ConnectCnt: 1, ConnectEMA: 100},
		{AttemptCount: 10, TCPConnectionTTFR_Cnt: 1, TCPConnectionTTFR_EMA: 100},
		{AttemptCount: 10, ThrCount: 1, ThrKBpsEMA: 3000},
		{AttemptCount: 10, LossCnt: 1, LossEMA: .2},
		{AttemptCount: 10, ConnectCnt: 1, TCPConnectionTTFR_Cnt: 1, ThrCount: 1, LossCnt: 1, SampleCount: 5, FailureCount: 3},
		{AttemptCount: 10, ConnectCnt: 1, TCPConnectionTTFR_Cnt: 1, ThrCount: 1, LossCnt: 1, SampleCount: 5, FailureCount: 15, SuspectedStallCount: 20},
		{AttemptCount: 10, ConnectCnt: 1, ThrCount: 1, LossCnt: 1, SampleCount: 5, FailureCount: 7, LossEMA: 2},
	}
	for i, r := range records {
		for _, large := range []bool{false, true} {
			score := r.ScenarioScore(large)
			if math.IsNaN(score) || score < 0 || score > 1 {
				t.Fatalf("case %d large=%v score out of bounds: %v", i, large, score)
			}
		}
	}
	if records[1].ScenarioScore(false) != .5 || records[1].ScenarioScore(true) != .5 || records[2].ScenarioScore(false) != .5 || records[2].ScenarioScore(true) != .5 {
		t.Fatal("legacy mixed clock or missing valid signals became a quality observation")
	}
}
