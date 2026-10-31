package smart

import (
	"encoding/json"
	"math"
	"testing"
)

func TestTTFRShadowLegacyJSONStartsSeparateEpoch(t *testing.T) {
	old := []byte(`{"reward":0.7,"sample_count":50,"conn_ttfr_ema":9,"conn_ttfr_cnt":100,"attempt_count":100}`)
	var r PLDRecord
	if err := json.Unmarshal(old, &r); err != nil {
		t.Fatal(err)
	}
	if r.TCPConnectionTTFR_Cnt != 0 || r.UDPResponseCnt != 0 || r.LossCnt != 0 {
		t.Fatal("old unknown metrics became valid")
	}
	if got := r.ScenarioScore(false); got != 0.5 {
		t.Fatalf("legacy mixed TTFR scored as TCP: %v", got)
	}
	r.ObserveMetrics(RewardedMetrics{TCPConnectionTTFRMs: 123, TCPConnectionTTFRValid: true})
	r.ObserveMetrics(RewardedMetrics{IsUDP: true, UDPResponseMs: 7, UDPResponseValid: true})
	if r.TCPConnectionTTFR_Cnt != 1 || r.TCPConnectionTTFR_EMA != 123 || r.UDPResponseCnt != 1 || r.UDPResponseEMA != 7 {
		t.Fatalf("new epoch imported mixed history: %+v", &r)
	}
	if r.ConnectionTTFR_Cnt != 100 || r.ConnectionTTFR_EMA != 9 || r.Reward != 0.7 || r.SampleCount != 50 {
		t.Fatal("legacy state mutated by shadow")
	}
	encoded, err := json.Marshal(&r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PLDRecord
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ConnectionTTFR_Cnt != 100 || decoded.TCPConnectionTTFR_Cnt != 1 || decoded.UDPResponseCnt != 1 {
		t.Fatalf("additive persistence lost fields: %s", encoded)
	}
}

func TestTTFRShadowValidityAndTransportIsolation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		m        RewardedMetrics
		tcp, udp int64
	}{
		{"legacy_only_unknown", RewardedMetrics{FirstByteMs: 80, DownloadMB: 1}, 0, 0},
		{"tcp_zero_valid", RewardedMetrics{TCPConnectionTTFRValid: true}, 1, 0},
		{"udp_zero_valid", RewardedMetrics{IsUDP: true, UDPResponseValid: true}, 0, 1},
		{"tcp_failure_valid", RewardedMetrics{Failed: true, TCPConnectionTTFRMs: 0.1, TCPConnectionTTFRValid: true}, 1, 0},
		{"udp_failure_valid", RewardedMetrics{IsUDP: true, Failed: true, UDPResponseMs: 0.1, UDPResponseValid: true}, 0, 1},
		{"udp_cannot_create_tcp", RewardedMetrics{IsUDP: true, TCPConnectionTTFRMs: 1, TCPConnectionTTFRValid: true}, 0, 0},
		{"tcp_cannot_create_udp", RewardedMetrics{UDPResponseMs: 1, UDPResponseValid: true}, 0, 0},
		{"negative", RewardedMetrics{TCPConnectionTTFRMs: -1, TCPConnectionTTFRValid: true}, 0, 0},
		{"nan", RewardedMetrics{TCPConnectionTTFRMs: math.NaN(), TCPConnectionTTFRValid: true}, 0, 0},
		{"infinity", RewardedMetrics{IsUDP: true, UDPResponseMs: math.Inf(1), UDPResponseValid: true}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r PLDRecord
			r.ObserveMetrics(tc.m)
			if r.TCPConnectionTTFR_Cnt != tc.tcp || r.UDPResponseCnt != tc.udp {
				t.Fatalf("counts tcp=%d udp=%d", r.TCPConnectionTTFR_Cnt, r.UDPResponseCnt)
			}
		})
	}
	var r PLDRecord
	r.ObserveMetrics(RewardedMetrics{TCPConnectionTTFRMs: 100, TCPConnectionTTFRValid: true})
	r.ObserveMetrics(RewardedMetrics{TCPConnectionTTFRMs: 100, TCPConnectionTTFRValid: true})
	before := r.ScenarioScore(false)
	for i := 0; i < 8; i++ {
		r.ObserveMetrics(RewardedMetrics{IsUDP: true, UDPResponseMs: 1, UDPResponseValid: true, DownloadMB: 2, LossValid: true})
	}
	if r.ScenarioScore(false) != before || r.TCPConnectionTTFR_Cnt != 2 || r.UDPResponseCnt != 8 || r.LossCnt != 0 {
		t.Fatal("UDP response or missing loss contaminated TCP score")
	}
}

func TestTTFRShadowNeverChangesLegacyReward(t *testing.T) {
	for _, m := range []RewardedMetrics{
		{ConnectTimeMs: 23, FirstByteMs: 80, BytesTo64KMs: 40, DownloadMB: 0.1},
		{IsUDP: true, ConnectTimeMs: 23, FirstByteMs: 80, DownloadMB: 0.1},
		{DownloadMB: 2, DurationSec: 1, Failed: true},
		{Failed: true},
	} {
		with := m
		with.TCPConnectionTTFRValid = true
		with.TCPConnectionTTFRMs = 9999
		with.UDPResponseValid = true
		with.UDPResponseMs = 0.001
		legacyReward, legacyParts := ComputeRewardDetail(m)
		reward, parts := ComputeRewardDetail(with)
		if reward != legacyReward || parts != legacyParts {
			t.Fatal("shadow altered reward or label")
		}
		var a, b PLDRecord
		a.UpdateWithMetrics(m, 0)
		b.UpdateWithMetrics(with, 0)
		if a.Reward != b.Reward || a.RewardLarge != b.RewardLarge || a.SampleCount != b.SampleCount || a.SampleCountLarge != b.SampleCountLarge || a.ThrKBpsEMA != b.ThrKBpsEMA || a.BWCeilingKBps != b.BWCeilingKBps {
			t.Fatal("shadow altered legacy model state")
		}
	}
}
