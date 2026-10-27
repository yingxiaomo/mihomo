package lightgbm

import (
	"math"
	"testing"
)

// priorFromRaw must map the LightGBM raw regression output (trained on the
// raw reward label, range [-2, 1]) onto a commensurate prior scale: negative
// predictions pass through as real "bad for this target" signals instead of
// collapsing into the uninformative flat 0.5.
func TestPriorFromRawPassthrough(t *testing.T) {
	cases := []struct {
		name  string
		raw   float64
		want  float64
		used  bool
	}{
		{"negative passthrough", -1.5, -1.5, true},
		{"zero is a valid neutral prediction", 0, 0, true},
		{"positive passthrough", 0.7, 0.7, true},
		{"clamp far below label min", -5, rewardLabelMin, true},
		{"clamp above label max", 3, rewardLabelMax, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, used := priorFromRaw(c.raw)
			if got != c.want || used != c.used {
				t.Fatalf("priorFromRaw(%v) = (%v, %v), want (%v, %v)", c.raw, got, used, c.want, c.used)
			}
		})
	}
}

func TestPriorFromRawNaNFallsBackFlat(t *testing.T) {
	got, used := priorFromRaw(math.NaN())
	if used || got != flatPrior {
		t.Fatalf("NaN should fall back to flat prior (got %v, %v)", got, used)
	}
}

// Regression guard for the old sigmoid+flat-0.5 mapping: two clearly
// different negative predictions must stay distinguishable, and a negative
// prediction must rank below a positive one.
func TestPriorFromRawKeepsNegativeDiscrimination(t *testing.T) {
	bad, _ := priorFromRaw(-1.5)
	mediocre, _ := priorFromRaw(-0.1)
	good, _ := priorFromRaw(0.8)

	if bad == mediocre || bad == flatPrior {
		t.Fatalf("negative predictions collapsed: bad=%v mediocre=%v flat=%v", bad, mediocre, flatPrior)
	}
	if !(bad < mediocre && mediocre < good) {
		t.Fatalf("ordering broken: bad=%v mediocre=%v good=%v", bad, mediocre, good)
	}
}

// The prior must stay within the reward label band that FinalWeight clamps
// the online RewardEMA to, so the two blended terms are commensurate.
func TestPriorFromRawStaysInLabelBand(t *testing.T) {
	for _, raw := range []float64{-100, -2.0001, -1, 0, 0.5, 1, 1.0001, 100} {
		got, used := priorFromRaw(raw)
		if !used {
			continue
		}
		if got < rewardLabelMin || got > rewardLabelMax {
			t.Fatalf("priorFromRaw(%v) = %v outside [%v, %v]", raw, got, rewardLabelMin, rewardLabelMax)
		}
	}
}
