package outboundgroup

import "testing"

// The zero-progress detector compares DOWNLOAD PROGRESS between the two
// samples, so it catches both the pure black hole (nothing ever delivered)
// and the header-then-stall case (a response header arrived, then the body
// stopped growing). The header-then-stall rows expect the detector to fire
// (the gate logs them observation-only), while flowing/idle patterns must
// stay exempt.
func TestIsZeroProgressConnection(t *testing.T) {
	cases := []struct {
		name                string
		upload1             int64
		upload2             int64
		download1           int64
		download2           int64
		wantStalled         bool
		wantHeaderStallOnly bool // download arrived, so the gate logs instead of hard-failing
	}{
		{"stalled request: client sent, server silent", 1500, 1500, 0, 0, true, false},
		{"stalled request: header arrived then body stalled", 1500, 1500, 120, 120, true, true},
		{"stalled request: header grew then froze", 1500, 1500, 2048, 2048, true, true},
		{"idle keep-alive: nothing sent", 0, 0, 0, 0, false, false},
		{"active upload: client keeps pushing", 100, 5000, 0, 0, false, false},
		{"active download: data flowing", 100, 100, 4096, 8192, false, false},
		{"request sent, then upload stalls, header arrived", 1500, 1500, 2048, 4096, false, false},
		{"upload stalled but exactly 0 down", 800, 800, 0, 0, true, false},
	}
	for _, c := range cases {
		got := isZeroProgressConnection(c.upload1, c.upload2, c.download1, c.download2)
		if got != c.wantStalled {
			t.Errorf("%s: isZeroProgressConnection(%d,%d,%d,%d) = %v, want %v",
				c.name, c.upload1, c.upload2, c.download1, c.download2, got, c.wantStalled)
		}
		if got && c.wantHeaderStallOnly && c.download1 == 0 {
			t.Errorf("%s: header-stall-only flagged but download1 == 0", c.name)
		}
	}
}
