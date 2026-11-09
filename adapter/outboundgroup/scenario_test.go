package outboundgroup

import "testing"

func TestClassifyScenario(t *testing.T) {
	cases := []struct {
		target string
		want   ScenarioClass
	}{
		// set-level targets: inner name goes through the keyword tables
		{"GeoSite [telegram]", ScenarioRealtime}, // communication
		{"GeoIP [telegram]", ScenarioRealtime},
		{"GeoSite [youtube]", ScenarioInteractive}, // streaming
		{"RuleSet [Cloudflare]", ScenarioInteractive},
		{"RuleSet [谷歌FCM]", ScenarioInteractive}, // Chinese name, no keyword match
		// host targets
		{"*.doh.pubdefault", ScenarioRealtime},        // dns
		{"api.cloudflare.com", ScenarioRealtime},      // api keyword table
		{"registry.npmjs.org", ScenarioRealtime},      // api keyword table
		{"*.download.example.com", ScenarioBulk},
		{"dl.example.net", ScenarioBulk},
		{"*.air-outer.com", ScenarioInteractive}, // no bulk signal
		{"GeoSite [游戏平台]", ScenarioInteractive},  // game stays interactive
		{"", ScenarioInteractive},
	}
	for _, c := range cases {
		if got := classifyScenario(c.target); got != c.want {
			t.Errorf("classifyScenario(%q) = %v, want %v", c.target, got, c.want)
		}
	}
}

func TestScenarioSmallWeight(t *testing.T) {
	if w := ScenarioRealtime.smallWeight(); w != 0.75 {
		t.Errorf("realtime smallWeight = %v, want 0.75", w)
	}
	if w := ScenarioInteractive.smallWeight(); w != 0.5 {
		t.Errorf("interactive smallWeight = %v, want 0.5", w)
	}
	if w := ScenarioBulk.smallWeight(); w != 0.25 {
		t.Errorf("bulk smallWeight = %v, want 0.25", w)
	}
}

func TestBlendChannelRewards(t *testing.T) {
	// realtime: latency channel dominates
	if got := blendChannelRewards(ScenarioRealtime, 0.6, 0.4); got < 0.5499 || got > 0.5501 {
		t.Errorf("realtime blend = %v, want ~0.55", got)
	}
	// bulk: throughput channel dominates
	if got := blendChannelRewards(ScenarioBulk, 0.6, 0.4); got < 0.4499 || got > 0.4501 {
		t.Errorf("bulk blend = %v, want ~0.45", got)
	}
	// interactive: symmetric
	if got := blendChannelRewards(ScenarioInteractive, 0.6, 0.4); got != 0.5 {
		t.Errorf("interactive blend = %v, want 0.5", got)
	}
}

func TestApplyStallPenalty(t *testing.T) {
	if got := applyStallPenalty(1.0, 0); got != 1.0 {
		t.Errorf("zero stall must not demote: %v", got)
	}
	if got := applyStallPenalty(1.0, 0.5); got != 0.75 {
		t.Errorf("50%% stall must demote to 0.75: %v", got)
	}
	if got := applyStallPenalty(1.0, 1.5); got != 0.5 {
		t.Errorf("stall rate must clamp to 1.0 before penalty: %v", got)
	}
	if got := applyStallPenalty(1.0, -1); got != 1.0 {
		t.Errorf("negative stall must not demote: %v", got)
	}
}
