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


