package smart

import (
	"encoding/json"
	"math"
	"testing"
)

func TestTTFRShadowLateObservationMetricsOnly(t *testing.T) {
	r := &PLDRecord{AttemptCount: 7, FailureCount: 2, Reward: .8, SampleCount: 5, RewardLarge: .6, SampleCountLarge: 3, ConnectEMA: 20, ConnectCnt: 7, LastUpdated: 1234}
	before, _ := json.Marshal(r)
	for _, ms := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		r.ObserveUDPResponse(ms)
	}
	afterInvalid, _ := json.Marshal(r)
	if string(before) != string(afterInvalid) {
		t.Fatal("invalid late observation changed record")
	}
	r.ObserveUDPResponse(0)
	if r.UDPResponseCnt != 1 || r.UDPResponseEMA != 0 || r.AttemptCount != 7 || r.FailureCount != 2 || r.Reward != .8 || r.SampleCount != 5 || r.RewardLarge != .6 || r.SampleCountLarge != 3 || r.ConnectEMA != 20 || r.ConnectCnt != 7 || r.LastUpdated != 1234 {
		t.Fatalf("late observation replayed settlement or lost valid zero time: %+v", r)
	}
}
