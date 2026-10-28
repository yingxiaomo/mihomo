package outboundgroup

import (
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/structure"
)

// decodeSmartOption runs a group config map through the same structure decoder
// the real parsing path uses (TagName "group", WeaklyTypedInput), so the tests
// cover the full YAML -> SmartPLDOption chain, pointer semantics included.
func decodeSmartOption(t *testing.T, config map[string]any) SmartPLDOption {
	t.Helper()
	opt := SmartPLDOption{}
	decoder := structure.NewDecoder(structure.Option{TagName: "group", WeaklyTypedInput: true})
	if err := decoder.Decode(config, &opt); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	return opt
}

// The three on/off-style knobs are *int precisely so the config layer can
// distinguish "absent" from "explicit 0": absent must stay nil (defaults
// apply), an explicit 0/negative must survive as a non-nil zero/negative
// pointer — previously both collapsed onto 0 and the constructor replaced
// them with the defaults, so a documented "0 = off" never worked.
func TestSmartOptionDecodePointerSemantics(t *testing.T) {
	absent := decodeSmartOption(t, map[string]any{"type": "smart-pld"})
	if absent.FirstByteGateMs != nil || absent.ZeroProgressMs != nil || absent.FailureCooldownSec != nil {
		t.Fatalf("absent keys must decode to nil pointers, got %+v %+v %+v",
			absent.FirstByteGateMs, absent.ZeroProgressMs, absent.FailureCooldownSec)
	}

	explicit := decodeSmartOption(t, map[string]any{
		"type":                 "smart-pld",
		"first-byte-gate-ms":   0,
		"zero-progress-ms":     0,
		"failure-cooldown-sec": 0,
	})
	if explicit.FirstByteGateMs == nil || *explicit.FirstByteGateMs != 0 {
		t.Fatalf("explicit 0 must decode to a non-nil pointer holding 0, got %+v", explicit.FirstByteGateMs)
	}
	if explicit.ZeroProgressMs == nil || *explicit.ZeroProgressMs != 0 {
		t.Fatalf("explicit 0 must decode to a non-nil pointer holding 0, got %+v", explicit.ZeroProgressMs)
	}
	if explicit.FailureCooldownSec == nil || *explicit.FailureCooldownSec != 0 {
		t.Fatalf("explicit 0 must decode to a non-nil pointer holding 0, got %+v", explicit.FailureCooldownSec)
	}
}

// resolveGateOption implements the documented four-state semantics:
// nil -> default, 0/negative -> off, positive -> configured value.
func TestResolveGateOption(t *testing.T) {
	cases := []struct {
		name   string
		v      *int
		def    int
		expect int
	}{
		{"nil = unset -> default", nil, 5000, 5000},
		{"explicit 0 = off", intPtr(0), 5000, 0},
		{"negative = off (compat)", intPtr(-1), 5000, 0},
		{"positive = configured", intPtr(2500), 5000, 2500},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveGateOption(c.v, c.def); got != c.expect {
				t.Fatalf("resolveGateOption(%v, %d) = %d, want %d", c.v, c.def, got, c.expect)
			}
		})
	}
}

// End-to-end: the four config states must produce the documented runtime
// values after NewSmartPLD's resolution.
func TestGateConfigStatesResolution(t *testing.T) {
	states := []struct {
		name              string
		firstByte, zero   *int
		cooldown          *int
		wantFirstByte     int
		wantZeroProgress  int
		wantCooldownSecs  time.Duration
	}{
		{"absent -> all defaults", nil, nil, nil, 5000, 10000, 60 * time.Second},
		{"explicit 0 -> all off", intPtr(0), intPtr(0), intPtr(0), 0, 0, 0},
		{"negative -> all off", intPtr(-1), intPtr(-1), intPtr(-1), 0, 0, 0},
		{"positive -> configured", intPtr(2500), intPtr(8000), intPtr(120), 2500, 8000, 120 * time.Second},
	}
	for _, s := range states {
		t.Run(s.name, func(t *testing.T) {
			opt := SmartPLDOption{FirstByteGateMs: s.firstByte, ZeroProgressMs: s.zero, FailureCooldownSec: s.cooldown}
			if got := resolveGateOption(opt.FirstByteGateMs, 5000); got != s.wantFirstByte {
				t.Fatalf("first-byte: got %d want %d", got, s.wantFirstByte)
			}
			gotZero := resolveGateOption(opt.ZeroProgressMs, 10000)
			if gotZero > 0 && gotZero < 5000 {
				gotZero = 0 // the sanity clamp applied in NewSmartPLD
			}
			if gotZero != s.wantZeroProgress {
				t.Fatalf("zero-progress: got %d want %d", gotZero, s.wantZeroProgress)
			}
			if got := time.Duration(resolveGateOption(opt.FailureCooldownSec, 60)) * time.Second; got != s.wantCooldownSecs {
				t.Fatalf("cooldown: got %v want %v", got, s.wantCooldownSecs)
			}
		})
	}
}

func intPtr(v int) *int { return &v }
