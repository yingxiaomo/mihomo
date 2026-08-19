package smart

import (
	"math"
	"testing"
)

func TestUpdateRewardEMA(t *testing.T) {
	// First sample seeds the mean.
	mean, varr, cnt := UpdateRewardEMA(0, 0, 0, 0.8)
	if cnt != 1 || math.Abs(mean-0.8) > 1e-9 || varr != 0 {
		t.Fatalf("first sample: got mean=%v var=%v cnt=%v, want 0.8/0/1", mean, varr, cnt)
	}
	// Second sample moves partially.
	mean, _, cnt = UpdateRewardEMA(mean, varr, cnt, 1.0)
	if cnt != 2 {
		t.Fatalf("cnt=%v want 2", cnt)
	}
	if mean <= 0.8 || mean >= 1.0 {
		t.Fatalf("second sample mean=%v must lie in (0.8,1.0)", mean)
	}
	// Alpha decays: after many samples, a wild value only nudges.
	mean, _, cnt = UpdateRewardEMA(mean, 0, cnt, 0.0)
	// After 2 samples alpha = 0.4/(1+0.1) ≈ 0.364; from ~0.87 → ~0.55
	if mean >= 0.7 {
		t.Fatalf("alpha decay not effective: mean=%v", mean)
	}
}

func TestComputeRewardFailure(t *testing.T) {
	r := ComputeReward(RewardedMetrics{Failed: true})
	if r != -2.0 {
		t.Fatalf("failed reward=%v want -2", r)
	}
}

func TestComputeRewardSmallTransfer(t *testing.T) {
	// Small transfer (<512KB), good RTT, no loss → positive.
	r := ComputeReward(RewardedMetrics{
		ConnectTimeMs: 30,
		FirstByteMs:   80,
		BytesTo64KMs:  60,
		DownloadMB:    0.05,
		DurationSec:   0.5,
	})
	if r <= 0 {
		t.Fatalf("good small transfer reward=%v should be >0", r)
	}
	// Slow connection → lower.
	rSlow := ComputeReward(RewardedMetrics{
		ConnectTimeMs: 500,
		FirstByteMs:   2000,
		BytesTo64KMs:  400,
		DownloadMB:    0.05,
		DurationSec:   1.0,
	})
	if rSlow >= r {
		t.Fatalf("slow small transfer should score lower: fast=%v slow=%v", r, rSlow)
	}
}

func TestComputeRewardLargeTransfer(t *testing.T) {
	// Large transfer (≥512KB): throughput dominates; loss penalizes.
	rFast := ComputeReward(RewardedMetrics{
		DownloadMB: 10.0, // 10MB
		DurationSec: 2.0, // ~5MB/s
		LossRate:   0,
	})
	rLossy := ComputeReward(RewardedMetrics{
		DownloadMB: 10.0,
		DurationSec: 2.0,
		LossRate:   0.15,
	})
	if rLossy >= rFast {
		t.Fatalf("loss should penalize: fast=%v lossy=%v", rFast, rLossy)
	}
}

func TestFinalWeightColdStart(t *testing.T) {
	// Cold node (n=0) with flat prior 0.5: leans on prior.
	w := FinalWeight(0.5, 0, 0, 100, 0.6, 10, 0.05)
	if w < 0.25 || w > 0.6 {
		t.Fatalf("cold-start weight=%v out of sane band", w)
	}
	// Mature node (n=100) with strong reward: leans on reward.
	wMature := FinalWeight(0.5, 0.9, 100, 200, 0.6, 10, 0.05)
	if wMature < 0.7 {
		t.Fatalf("mature node should track reward: w=%v", wMature)
	}
	// Bad reward pulls weight down hard.
	wBad := FinalWeight(0.5, -2.0, 50, 200, 0.6, 10, 0.05)
	if wBad >= wMature {
		t.Fatalf("negative reward must rank below positive: bad=%v mature=%v", wBad, wMature)
	}
}

func TestComputeRewardPartialSuccess(t *testing.T) {
	// Partial success: downloaded 2MB at ~2MB/s then the client closed the
	// connection (short-video swipe / prefetch cancel). This is NOT a hard
	// failure — it proved real throughput and must keep a positive-ish score.
	r, parts := ComputeRewardDetail(RewardedMetrics{
		DownloadMB:  2.0, // 2MB
		DurationSec: 1.0,
		Failed:      true,
	})
	if parts.Failure {
		t.Fatalf("downloaded-then-closed must not be a hard failure")
	}
	if r <= 0.1 || r >= 0.25 {
		t.Fatalf("partial reward out of band: %v", r) // thrScore 0.4057 → 0.175
	}

	// True failure: nothing downloaded before closing → hard -2.
	r2, parts2 := ComputeRewardDetail(RewardedMetrics{Failed: true})
	if !parts2.Failure || r2 != -2.0 {
		t.Fatalf("real failure must stay -2: r=%v failure=%v", r2, parts2.Failure)
	}

	// Partial success updates the throughput channel + archive, not latency.
	rec := &PLDRecord{}
	rec.UpdateWithMetrics(RewardedMetrics{DownloadMB: 2.0, DurationSec: 1.0, Failed: true})
	if rec.RewardLarge <= 0 || rec.SampleCountLarge != 1 {
		t.Fatalf("partial success must feed the throughput channel: large=%v cnt=%d", rec.RewardLarge, rec.SampleCountLarge)
	}
	if rec.ThrCount != 1 || math.Abs(rec.ThrKBpsEMA-2048) > 20 {
		t.Fatalf("partial success must feed the throughput archive: thr=%v", rec.ThrKBpsEMA)
	}
	if rec.Reward != 0 || rec.SampleCount != 0 {
		t.Fatalf("partial success must NOT touch the latency channel: rew=%v cnt=%d", rec.Reward, rec.SampleCount)
	}
}

func TestPLDRecordDualChannelIsolation(t *testing.T) {
	// Small transfer updates ONLY the latency channel; the throughput channel
	// and the throughput archive must stay untouched (the core fix: page loads
	// can no longer dilute download-speed decisions).
	rec := &PLDRecord{}
	_, parts := rec.UpdateWithMetrics(RewardedMetrics{
		ConnectTimeMs: 30,
		FirstByteMs:   80,
		BytesTo64KMs:  60,
		DownloadMB:    0.05,
		DurationSec:   0.5,
	})
	if !parts.SmallFlow {
		t.Fatalf("0.05MB should be treated as small flow")
	}
	if rec.SampleCount != 1 || rec.Reward <= 0 {
		t.Fatalf("latency channel not updated: cnt=%d reward=%v", rec.SampleCount, rec.Reward)
	}
	if rec.SampleCountLarge != 0 || rec.RewardLarge != 0 {
		t.Fatalf("throughput channel polluted by small flow: largeCnt=%d largeRew=%v", rec.SampleCountLarge, rec.RewardLarge)
	}
	if rec.ThrCount != 0 {
		t.Fatalf("throughput archive polluted by small flow: thrCnt=%d", rec.ThrCount)
	}

	// Large transfer updates ONLY the throughput channel + throughput archive.
	rec2 := &PLDRecord{}
	_, parts2 := rec2.UpdateWithMetrics(RewardedMetrics{
		DownloadMB:  10.0, // 10MB
		DurationSec: 2.0,  // 5MB/s
	})
	if parts2.SmallFlow {
		t.Fatalf("10MB should be treated as large flow")
	}
	if rec2.SampleCountLarge != 1 || rec2.RewardLarge <= 0 {
		t.Fatalf("throughput channel not updated: largeCnt=%d largeRew=%v", rec2.SampleCountLarge, rec2.RewardLarge)
	}
	if rec2.ThrCount != 1 || rec2.ThrKBpsEMA <= 0 || math.Abs(rec2.ThrKBpsEMA-5120) > 100 {
		t.Fatalf("throughput archive wrong: thr=%v", rec2.ThrKBpsEMA)
	}
	if rec2.SampleCount != 0 || rec2.Reward != 0 {
		t.Fatalf("latency channel polluted by large flow: cnt=%d rew=%v", rec2.SampleCount, rec2.Reward)
	}

	// Failure must punish BOTH channels (a dead node must not be selected under
	// either flow priority) and count in both sample counters.
	rec3 := &PLDRecord{Reward: 0.8, RewardLarge: 0.7}
	rec3.UpdateWithMetrics(RewardedMetrics{Failed: true})
	if !(rec3.Reward < 0.8 && rec3.RewardLarge < 0.7) {
		t.Fatalf("failure must punish both channels: rew=%v large=%v", rec3.Reward, rec3.RewardLarge)
	}
	if rec3.SampleCount != 1 || rec3.SampleCountLarge != 1 {
		t.Fatalf("failure must count in both channels: cnt=%d largeCnt=%d", rec3.SampleCount, rec3.SampleCountLarge)
	}
}

func TestPLDRecordChannelStatsAndThrKBps(t *testing.T) {
	rec := &PLDRecord{Reward: 0.3, SampleCount: 5, RewardLarge: 0.8, SampleCountLarge: 9}
	ema, _, cnt := rec.ChannelStats(false)
	if ema != 0.3 || cnt != 5 {
		t.Fatalf("latency channel stats: ema=%v cnt=%v", ema, cnt)
	}
	ema, _, cnt = rec.ChannelStats(true)
	if ema != 0.8 || cnt != 9 {
		t.Fatalf("throughput channel stats: ema=%v cnt=%v", ema, cnt)
	}

	// No throughput archive yet -> fall back to the packet-pair ceiling.
	cold := &PLDRecord{BWCeilingKBps: 3000}
	if cold.ThrKBps() != 3000 {
		t.Fatalf("cold ThrKBps fallback: %v", cold.ThrKBps())
	}
	warm := &PLDRecord{ThrKBpsEMA: 5000, ThrCount: 3}
	if warm.ThrKBps() != 5000 {
		t.Fatalf("warm ThrKBps: %v", warm.ThrKBps())
	}
}

func TestPreferLargeFlowChannel(t *testing.T) {
	if !PreferLargeFlowChannel(2, 0) {
		t.Fatalf("medium profile must prefer the large-flow channel")
	}
	if !PreferLargeFlowChannel(3, 0) {
		t.Fatalf("heavy profile must prefer the large-flow channel")
	}
	if PreferLargeFlowChannel(0, 0) {
		t.Fatalf("unknown profile + web class should stay on the small channel")
	}
	if !PreferLargeFlowChannel(0, 1) {
		t.Fatalf("stream class must prefer the large-flow channel")
	}
	if !PreferLargeFlowChannel(0, 2) {
		t.Fatalf("transfer class must prefer the large-flow channel")
	}
	if PreferLargeFlowChannel(0, 3) {
		t.Fatalf("interactive class stays on the small channel")
	}
}

func TestSampleWeightFromMB(t *testing.T) {
	if w := SampleWeightFromMB(0.05); w < 0.5 || w > 0.6 {
		t.Fatalf("50KB floor: %v", w)
	}
	if w := SampleWeightFromMB(0.5); math.Abs(w-1) > 0.01 {
		t.Fatalf("512KB should weigh ~1: %v", w)
	}
	w5 := SampleWeightFromMB(5)
	if w5 <= 3 || w5 >= 4 {
		t.Fatalf("5MB weight: %v", w5)
	}
	w100 := SampleWeightFromMB(100)
	if w100 <= 7.5 || w100 >= 7.8 {
		t.Fatalf("100MB weight: %v", w100)
	}
	if w100 <= w5 {
		t.Fatalf("bigger transfer must weigh more: 100MB=%v 5MB=%v", w100, w5)
	}
	if w := SampleWeightFromMB(4096); w > 12.01 {
		t.Fatalf("weight cap: %v", w)
	}
}


