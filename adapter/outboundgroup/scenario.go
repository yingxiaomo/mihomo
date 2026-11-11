package outboundgroup

import (
	"strings"
	"sync/atomic"

	"github.com/metacubex/mihomo/component/smart/lightgbm"
)

// Phase 2 scenario layer: map a target to a demand class, and weight the
// small/large channel rewards accordingly when feeding the D-UCB exploit
// input. Static keyword classification first (per docs/pld-phase2-design.md
// §2); the class table grows with shadow evidence later.
//
// Semantics per class:
//   realtime   — dns/api/communication: signaling dominates, anchor selection
//                should weight the latency channel heavily (TG DC churn, the
//                top regret target, lives here).
//   interactive— streaming/game/unknown: mixed traffic, keep the current
//                per-request channel behavior (0.5/0.5).
//   bulk       — explicit download endpoints: throughput dominates; these
//                targets also get hard affinity (switch margin ×3).
//
// Conservative by design: bulk only matches explicit download signals, and
// game targets deliberately stay interactive (game downloads are usually
// DIRECT-ruled and never reach smart groups; what flows through PLD is
// session traffic).

type ScenarioClass int

const (
	ScenarioRealtime ScenarioClass = iota
	ScenarioInteractive
	ScenarioBulk
)

// scenarioBulkKeywords: explicit download-endpoint signals. Deliberately
// short — a misclassified mixed target skews its anchor toward download
// nodes at the expense of session traffic; grow this list from shadow
// evidence, not guesses.
var scenarioBulkKeywords = []string{
	"download", "dl.", "dl-", "steamcontent", "mirror", "pkg",
	"releases", "package", "update",
}

// scenarioSmallWeight returns the weight the SMALL (latency) channel
// carries in the blended reward for this class; the large channel gets
// the complement.
func (sc ScenarioClass) smallWeight() float64 {
	switch sc {
	case ScenarioRealtime:
		return 0.75
	case ScenarioBulk:
		return 0.25
	default:
		return 0.5
	}
}

// scenarioStallPenaltyWeight scales the stall-rate demotion applied to the
// final weight: w *= 1 - penaltyWeight*stallRate. 0.5 means a node with
// half its settlements stalling loses 25% of its weight — graded demotion,
// not disqualification (hard failures already have their own path).
const scenarioStallPenaltyWeight = 0.5

func (sc ScenarioClass) String() string {
	switch sc {
	case ScenarioRealtime:
		return "realtime"
	case ScenarioBulk:
		return "bulk"
	default:
		return "interactive"
	}
}

// classifyScenario maps a PLD target (set-level "GeoSite [telegram]" /
// "RuleSet [谷歌FCM]" or host "*.air-outer.com") to a demand class.
// Set-level names are unwrapped and matched through the same keyword
// tables the model's domain-type feature uses; unmatched targets fall
// back to interactive (the current behavior's closest equivalent).
func classifyScenario(target string) ScenarioClass {
	t := strings.ToLower(strings.TrimSpace(target))
	if t == "" {
		return ScenarioInteractive
	}

	// Set-level target: keep the inner name for keyword matching.
	if i := strings.Index(t, "["); i >= 0 && strings.HasSuffix(t, "]") {
		t = t[i+1 : len(t)-1]
	} else {
		// Host target: drop the wildcard prefix so "air-outer.com" style
		// names go through keyword matching cleanly.
		t = strings.TrimPrefix(t, "*.")
	}
	if t == "" {
		return ScenarioInteractive
	}

	// Bulk: explicit download signals first — keyword order in the shared
	// classifier would otherwise bury them under streaming/api matches.
	for _, kw := range scenarioBulkKeywords {
		if strings.Contains(t, kw) {
			return ScenarioBulk
		}
	}

	switch lightgbm.ClassifyDomainFeature(t) {
	case 6, 5, 4: // dns / api / communication
		return ScenarioRealtime
	default: // streaming / game / unknown / TLD-only
		return ScenarioInteractive
	}
}

// blendChannelRewards mixes the small/large channel reward EMAs per the
// class weights. Both channels are the reward-domain scalars the D-UCB
// exploit term already consumes, so the blend stays on-scale.
func blendChannelRewards(sc ScenarioClass, rewSmall, rewLarge float64) float64 {
	wS := sc.smallWeight()
	return wS*rewSmall + (1-wS)*rewLarge
}

// applyStallPenalty demotes the final weight by the record's stall rate.
// Pure arithmetic on the merged weight — the D-UCB prior/explore structure
// is untouched; hard failures keep their own cool-off path.
func applyStallPenalty(weight, stallRate float64) float64 {
	if stallRate <= 0 {
		return weight
	}
	if stallRate > 1 {
		stallRate = 1
	}
	return weight * (1 - scenarioStallPenaltyWeight*stallRate)
}

// Shadow comparison counters: per class, how often the scenario ranking's
// top-1 agrees with the active ranking's top-1. Accumulated on every
// shadow-mode selection decision (in-memory, survives log rotation, resets
// on restart). Read via GET /cache/smart/scenario. Arrays are indexed —
// never copied — so the embedded atomics stay valid.
var (
	scenarioShadowAgree [3]uint64Counters
	scenarioShadowTotal [3]uint64Counters
)

// uint64Counters is a tiny atomic pair used by index only.
type uint64Counters struct {
	agree atomic.Uint64
	total atomic.Uint64
}

func recordShadowOutcome(sc ScenarioClass, agree bool) {
	scenarioShadowTotal[sc].total.Add(1)
	if agree {
		scenarioShadowAgree[sc].agree.Add(1)
	}
}

// ScenarioShadowStats returns a snapshot of the shadow comparison counters
// keyed by class name: {"realtime": {"agree": n, "total": m}, ...}.
func ScenarioShadowStats() map[string]map[string]uint64 {
	out := make(map[string]map[string]uint64, 3)
	for sc := ScenarioRealtime; sc <= ScenarioBulk; sc++ {
		out[sc.String()] = map[string]uint64{
			"agree": scenarioShadowAgree[sc].agree.Load(),
			"total": scenarioShadowTotal[sc].total.Load(),
		}
	}
	return out
}
