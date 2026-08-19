// Package-level PLD (Predictive-LightGBM & Discounted-UCB with Packet-Pair)
// core math and per-node reward state. Kept in this file so the official
// smart implementation (weight model / CalculateWeight) stays untouched —
// PLD is purely additive and decoupled from the official files.
package smart

import (
	"math"
	"time"
)

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
		// Partial success: a connection that downloaded a large amount BEFORE
		// closing abnormally (short-video swipe, prefetch cancel, closeSame-
		// Connection killing a long-lived conn) still proves real throughput.
		// Scoring it as a hard -2.0 would let everyday usage punish every
		// node's throughput channel to the floor, destroying the ability to
		// tell fast nodes from slow ones. So: score by sustained throughput
		// with a fixed small partial-failure penalty.
		if m.DownloadMB >= SmallTransferKB/1024.0 && m.DurationSec >= 0.5 {
			thrKbps := m.DownloadMB * 1024.0 / m.DurationSec
			thrScore := thrKbps / (thrKbps + 3000.0)
			r := 0.8*thrScore - 0.15 - m.LossRate*1.2
			return clampReward(r), RewardParts{ThrKBps: thrKbps, LossPenalty: m.LossRate * 1.2}
		}
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
//
// The state is split into two independent feedback channels so that small
// (latency-dominated) transfers can never pollute throughput decisions and
// vice versa:
//
//   - Reward / RewardVar / SampleCount (the latency channel): updated by
//     small successful transfers (< SmallTransferKB) and by failures. Drives
//     web-browsing / interactive selection, and keeps the soft-fail semantics
//     (Reward < 0 → candidate excluded for this target).
//   - RewardLarge / RewardLargeVar / SampleCountLarge (the throughput
//     channel): updated ONLY by large successful transfers (>= SmallTransferKB)
//     and by failures. Drives video-streaming / bulk-download selection. Small
//     transfers never touch it, so thousands of page loads cannot dilute "how
//     fast does this node download".
//   - ThrKBpsEMA / ThrVar / ThrCount: the explicit sustained-throughput
//     archive (KB/s) learned from large successful transfers. It is the
//     physical "how fast does this node actually download" estimate, distinct
//     from the normalized reward.
//
// Failures are applied to BOTH channels: a node that cannot reach the target
// must not be selected under either flow priority.
type PLDRecord struct {
	Reward        float64 `json:"reward,omitempty"`
	RewardVar     float64 `json:"reward_var,omitempty"`
	SampleCount   int64   `json:"sample_count,omitempty"`

	RewardLarge      float64 `json:"reward_large,omitempty"`
	RewardLargeVar   float64 `json:"reward_large_var,omitempty"`
	SampleCountLarge int64   `json:"sample_count_large,omitempty"`

	ThrKBpsEMA float64 `json:"thr_kbps_ema,omitempty"`
	ThrVar     float64 `json:"thr_var,omitempty"`
	ThrCount   int64   `json:"thr_count,omitempty"`

	FirstByteMs   int64   `json:"first_byte_ms,omitempty"`
	BWCeilingKBps float64 `json:"bw_ceiling_kbps,omitempty"`
	LastUpdated   int64   `json:"last_updated,omitempty"`
}

// FillFailure applies a hard failure reward (-2) to BOTH channels: a node that
// fails the target (dial error, first-byte gate timeout, abnormal response)
// must drop out of ranking for latency-sensitive and throughput-sensitive
// requests alike.
func (r *PLDRecord) FillFailure() {
	rew := -2.0
	newR, newVar, newCnt := UpdateRewardEMA(r.Reward, r.RewardVar, r.SampleCount, rew)
	r.Reward, r.RewardVar, r.SampleCount = newR, newVar, newCnt
	newRL, newVarL, newCntL := UpdateRewardEMA(r.RewardLarge, r.RewardLargeVar, r.SampleCountLarge, rew)
	r.RewardLarge, r.RewardLargeVar, r.SampleCountLarge = newRL, newVarL, newCntL
	r.LastUpdated = time.Now().Unix()
}

// UpdateWithMetrics folds one completed connection into the channel-appropriate
// reward state:
//
//   - hard failure (nothing downloaded) → FillFailure (both channels)
//   - partial success (large download then abnormal close) → throughput
//     channel + throughput archive, scored with a small penalty
//   - small transfer → latency channel only (throughput channel untouched)
//   - large transfer → throughput channel + sustained-throughput archive
//     (latency channel untouched)
//
// It returns the scalar reward and its physical breakdown for logging/training.
func (r *PLDRecord) UpdateWithMetrics(m RewardedMetrics) (float64, RewardParts) {
	scalar, parts := ComputeRewardDetail(m)
	if m.Failed && parts.Failure {
		// True failure (no meaningful download): punish BOTH channels so the
		// node drops out of ranking for latency- and throughput-sensitive
		// requests alike.
		r.FillFailure()
		return scalar, parts
	}
	if !m.SmallFlow() {
		// Large-flow channel: throughput reward + explicit throughput archive.
		// Partial successes update it too — they carried real bytes.
		newR, newVar, newCnt := UpdateRewardEMA(r.RewardLarge, r.RewardLargeVar, r.SampleCountLarge, scalar)
		r.RewardLarge, r.RewardLargeVar, r.SampleCountLarge = newR, newVar, newCnt
		if parts.ThrKBps > 0 {
			nThr, vThr, cThr := UpdateRewardEMA(r.ThrKBpsEMA, r.ThrVar, r.ThrCount, parts.ThrKBps)
			r.ThrKBpsEMA, r.ThrVar, r.ThrCount = nThr, vThr, cThr
		}
	} else {
		// Small-flow channel: latency-dominated reward.
		newR, newVar, newCnt := UpdateRewardEMA(r.Reward, r.RewardVar, r.SampleCount, scalar)
		r.Reward, r.RewardVar, r.SampleCount = newR, newVar, newCnt
	}
	// Packet-pair ceiling stays useful on any successful transfer that
	// crossed 64KiB (cold-throughput fallback for the large-flow channel).
	if !m.Failed && m.BytesTo64KMs > 0 {
		r.BWCeilingKBps = 64.0 / (float64(m.BytesTo64KMs) / 1000.0)
	}
	r.LastUpdated = time.Now().Unix()
	return scalar, parts
}

// SmallFlow reports whether the transfer is below the small-transfer threshold.
func (m RewardedMetrics) SmallFlow() bool {
	return m.DownloadMB < SmallTransferKB/1024.0
}

// ChannelStats returns the reward state (EMA, variance, sample count) for the
// selected flow channel: large==true picks the throughput channel, otherwise
// the latency channel.
func (r *PLDRecord) ChannelStats(large bool) (float64, float64, int64) {
	if large {
		return r.RewardLarge, r.RewardLargeVar, r.SampleCountLarge
	}
	return r.Reward, r.RewardVar, r.SampleCount
}

// ThrKBps returns the sustained-throughput estimate (KB/s) for this node,
// falling back to the packet-pair 64KiB ceiling when no large-flow samples
// exist yet (cold throughput channel).
func (r *PLDRecord) ThrKBps() float64 {
	if r.ThrCount > 0 {
		return r.ThrKBpsEMA
	}
	return r.BWCeilingKBps
}

// PreferLargeFlowChannel decides whether a request should be ranked by the
// throughput channel (video streaming / bulk download) instead of the latency
// channel (web browsing / interactive). destLevel is the target's data-driven
// traffic profile (feature 20: 0=unknown, 1=small, 2=medium, 3=heavy);
// trafficClass is the cheap request heuristic (0=web, 1=stream, 2=transfer,
// 3=interactive). A medium/heavy profile dominates (real usage data beats
// heuristics); otherwise QUIC-media and bulk-port heuristics opt into the
// throughput channel.
func PreferLargeFlowChannel(destLevel, trafficClass float64) bool {
	if destLevel >= 2 {
		return true
	}
	return trafficClass == 1 || trafficClass == 2
}

// SampleWeightFromMB returns the training sample weight for a connection of
// downloadMB MiB. Small transfers keep a 0.5 floor so latency-channel signal
// survives in the training data; large transfers dominate through a log scale
// (512KiB→1.0, 5MB→3.5, 100MB→6.7, 1GB→11.0) so "big transfers are gold" is
// actually reflected in the LightGBM fit.
func SampleWeightFromMB(downloadMB float64) float64 {
	mb := downloadMB
	if mb < 0 {
		mb = 0
	}
	w := math.Log2(mb*2 + 1)
	if w < 0.5 {
		w = 0.5
	}
	if w > 12 {
		w = 12
	}
	return w
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
