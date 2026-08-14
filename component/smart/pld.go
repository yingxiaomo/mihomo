// Package-level PLD (Predictive-LightGBM & Discounted-UCB with Packet-Pair)
// core math and per-node reward state. Kept in this file so the official
// smart implementation (weight model / CalculateWeight) stays untouched —
// PLD is purely additive and decoupled from the official files.
package smart

import "math"

// Default PLD tuning knobs. Flat constants so runtime config can override.
const (
	// PriorWeightDefault is the initial prior weight a0.
	PriorWeightDefault = 0.6
	// PriorDecayK controls how fast the prior yields to live reward:
	// a = a0*k/(k+n). Higher k = prior trusted longer.
	PriorDecayKDefault = 10.0
	// ExploreStrengthDefault is the UCB exploration coefficient c.
	ExploreStrengthDefault = 0.05
	// RewardAlphaBase is the base EWMA alpha for reward updates.
	RewardAlphaBase = 0.4
	// RewardAlphaDecay scales a down as samples accumulate:
	// a_eff = base/(1 + 0.05*n) — "discounted" UCB flavour.
	RewardAlphaDecay = 0.05
)

// UpdateRewardEMA folds a scalar reward into an EWMA with a sample-count
// discounted alpha: early samples move the estimate a lot, later ones nudge it.
// Returns (newMean, newVar, newCount). Variance is the EWMA of squared error,
// used by the UCB exploration term.
func UpdateRewardEMA(mean, variance float64, count int64, reward float64) (float64, float64, int64) {
	n := float64(count)
	alpha := RewardAlphaBase / (1.0 + RewardAlphaDecay*n)
	if alpha > 1 {
		alpha = 1
	}
	if count == 0 {
		return reward, 0, count + 1
	}
	diff := reward - mean
	newMean := mean + alpha*diff
	newVar := (1 - alpha) * (variance + alpha*diff*diff)
	return newMean, newVar, count + 1
}

// RewardedMetrics bundles the physical signals measured post-connection that a
// Packet-Pair style reward is derived from.
type RewardedMetrics struct {
	IsUDP         bool
	ConnectTimeMs int64 // full proxy dial (incl handshake)
	FirstByteMs   int64 // end-to-end time to first response byte (TTFR)
	BytesTo64KMs  int64 // ms from first byte to 64KiB received (packet-pair)
	DownloadMB    float64
	DurationSec   float64
	MaxDownloadKB float64 // peak KB/s
	LossRate      float64
	Failed        bool
	FailureCount  int64 // consecutive failures this node has accumulated
}

// SmallTransferKB is the threshold below which a transfer is treated as
// "small web" (packet-pair / TTFR dominated) rather than "big transfer".
const SmallTransferKB = 512.0

// RewardParts breaks down a reward into its physical components so detailed
// [PLD] logs can show exactly why a node scored what it did (contrast with the
// official smart group's weight breakdown).
type RewardParts struct {
	ConnectScore float64 // RTT reciprocal: connect ~100ms -> ~0.5 ; ~20ms -> ~0.83
	TTFRScore    float64 // end-to-end first-byte reciprocal: ~300ms -> 0.5
	PairScore    float64 // packet-pair 64KiB bandwidth ceiling (neutral 0.5 if unused)
	ThrKBps      float64 // sustained download throughput KB/s (large-flow only)
	LossPenalty  float64 // accumulated loss penalty applied
	SmallFlow    bool    // true = small-transfer branch, false = large-transfer
	Failure      bool
}

// ComputeRewardDetail returns the scalar reward plus its physical breakdown.
// The scalar stays the same as ComputeReward; the parts only feed logging.
func ComputeRewardDetail(m RewardedMetrics) (float64, RewardParts) {
	if m.Failed {
		return -2.0, RewardParts{Failure: true}
	}

	// RTT/TTFR scoring, saturating reciprocal curves.
	// connect ~100ms -> ~0.5 ; ~20ms -> ~0.83 ; ~500ms -> ~0.17
	connectScore := 1.0 / (1.0 + float64(m.ConnectTimeMs)/100.0)
	ttfrScore := 1.0 / (1.0 + float64(m.FirstByteMs)/300.0)
	if m.FirstByteMs <= 0 {
		ttfrScore = connectScore // fallback: no response observed -> rely on dial
	}

	// Packet-pair bandwidth ceiling: how many MB/s the first 64KiB implied.
	// bytesTo64K ~ 0 -> fast; 100ms -> 640KB/s ceiling.
	var pairScore float64
	if m.BytesTo64KMs > 0 {
		kbps := 64.0 / (float64(m.BytesTo64KMs) / 1000.0) // KB/s
		pairScore = kbps / (kbps + 2000.0)                // 2MB/s -> 0.5
	} else {
		pairScore = 0.5 // no pair sample yet — neutral
	}

	var parts RewardParts
	parts.ConnectScore = connectScore
	parts.TTFRScore = ttfrScore
	parts.PairScore = pairScore

	if m.DownloadMB < SmallTransferKB/1024.0 {
		// Small traffic: physical timing dominates.
		r := 0.5*connectScore + 0.35*ttfrScore + 0.15*pairScore
		if m.IsUDP {
			// UDP has no dial handshake meaning; weight TTFR more.
			r = 0.3*connectScore + 0.5*ttfrScore + 0.2*pairScore
		}
		if m.LossRate > 0 {
			r -= m.LossRate * 0.8
			parts.LossPenalty = m.LossRate * 0.8
		}
		parts.SmallFlow = true
		return clampReward(r), parts
	}

	// Large transfer: sustained throughput + loss.
	thrKbps := 0.0
	if m.DurationSec > 0 {
		thrKbps = m.DownloadMB * 1024.0 / m.DurationSec
	}
	if thrKbps <= 0 {
		thrKbps = m.MaxDownloadKB
	}
	parts.ThrKBps = thrKbps
	thrScore := thrKbps / (thrKbps + 3000.0)
	r := 0.8*thrScore - m.LossRate*1.2
	parts.LossPenalty = m.LossRate * 1.2
	// penalize repeated failures softly
	if m.FailureCount > 1 {
		r -= 0.1 * float64(m.FailureCount-1)
	}
	return clampReward(r), parts
}

// ComputeReward turns physical metrics into a scalar reward in roughly [-2, 1]:
//   - hard failure -> -2 (strong negative credit)
//   - small transfer -> blend of RTT, TTFR and packet-pair bandwidth ceiling
//   - large transfer -> blend of sustained throughput and loss penalty
//
// Higher is better. The reward is the label the LightGBM prior eventually learns.
func ComputeReward(m RewardedMetrics) float64 {
	r, _ := ComputeRewardDetail(m)
	return r
}

func clampReward(r float64) float64 {
	if r > 1 {
		return 1
	}
	if r < -2 {
		return -2
	}
	return r
}

// FinalWeight is the D-UCB decision formula:
//
//	Final = a*prior + (1-a)*rewardEMA + c*sqrt(ln N_total/n_node)
//
// a decays with the node's sample count so a cold node leans on the prior and a
// well-measured node leans on live reward. The exploration bonus keeps cold
// nodes from being starved (Discounting + exploration).
func FinalWeight(prior, rewardEMA float64, nNode, nTotal int64, alpha0, k, c float64) float64 {
	if alpha0 <= 0 {
		alpha0 = PriorWeightDefault
	}
	if k <= 0 {
		k = PriorDecayKDefault
	}
	if c < 0 {
		c = ExploreStrengthDefault
	}

	n := float64(nNode)
	alpha := alpha0 * k / (k + n)
	if alpha > 1 {
		alpha = 1
	}
	// clamp rewardEMA into a sane band before mixing with prior
	rew := rewardEMA
	if rew < -2 {
		rew = -2
	}
	if rew > 1 {
		rew = 1
	}

	explore := 0.0
	if c > 0 {
		nTot := float64(nTotal)
		if nTot < 0 {
			nTot = 0
		}
		explore = c * math.Sqrt(math.Log(nTot+1.0)/(n+1.0))
	}

	return alpha*prior + (1.0-alpha)*rew + explore
}

// ---------------------------------------------------------------------------
// PLD per-node reward persistence. Decoupled from the official StatsRecord:
// stored under its own key namespace ("pld:" prefix) in the shared smart.db,
// serialized as PLDRecord so the official smart implementation never sees it.
// ---------------------------------------------------------------------------

// PLDRecord is the persisted per-(target,node) reward state owned by PLD only.
type PLDRecord struct {
	Reward        float64 `json:"reward,omitempty"`
	RewardVar     float64 `json:"reward_var,omitempty"`
	SampleCount   int64   `json:"sample_count,omitempty"`
	FirstByteMs   int64   `json:"first_byte_ms,omitempty"`
	BWCeilingKBps float64 `json:"bw_ceiling_kbps,omitempty"`
	LastUpdated   int64   `json:"last_updated,omitempty"`
}

// PLDKeyPrefix is the independent key namespace under the shared smart.db
// bucket. Nothing in the official smart path reads or writes keys with this
// prefix.
const PLDKeyPrefix = "pld:"

// FormatPLDKey builds the storage key for a (group, config, target, node) PLD
// reward record.
func FormatPLDKey(config, group, target, node string) string {
	return PLDKeyPrefix + config + ":" + group + ":" + target + ":" + node
}
