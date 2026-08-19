package outboundgroup

import "testing"

func TestIsZeroProgressConnection(t *testing.T) {
	cases := []struct {
		name          string
		upload1       int64
		upload2       int64
		download      int64
		wantBlackhole bool
	}{
		{"stalled request: client sent, server silent", 1500, 1500, 0, true},
		{"stalled request: tiny response arrived", 1500, 1500, 120, false}, // first-byte gate's domain
		{"idle keep-alive: nothing sent", 0, 0, 0, false},
		{"active upload: client keeps pushing", 100, 5000, 0, false},
		{"active download: data flowing", 100, 100, 4096, false},
		{"request sent, then upload stalls, header arrived", 1500, 1500, 2048, false},
		{"upload stalled but exactly 0 down", 800, 800, 0, true},
	}
	for _, c := range cases {
		got := isZeroProgressConnection(c.upload1, c.upload2, c.download)
		if got != c.wantBlackhole {
			t.Errorf("%s: isZeroProgressConnection(%d,%d,%d) = %v, want %v",
				c.name, c.upload1, c.upload2, c.download, got, c.wantBlackhole)
		}
	}
}
