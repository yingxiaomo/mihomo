package smart

import (
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// TestUpdateHostStatusBlockSemantics pins the host-failure ledger rules that
// the smart-pld switching depends on:
//   - code 3 (dial failure) and code 5 (low weight) count consecutive failures
//     and only block after maxFailedTimes — they must NOT disappear for 24h on
//     a single occurrence (code 5 regressed to instant-24h via the default
//     branch; that was fixed by routing it through the counting branch).
//   - code 4 / code 7 (strong signals: zero-traffic conn, zero-progress gate)
//     block instantly for HostFailureNodeTTL.
func TestUpdateHostStatusBlockSemantics(t *testing.T) {
	md := &C.Metadata{Host: "example.com"}
	const cfg, grp, wt = "cfg-test", "grp-test", "*.example.com"

	blocked := s.UpdateHostStatus(grp, cfg, wt, md, "node-5", 5, 10, true, true, 5)
	if blocked {
		t.Fatalf("single low-weight failure must not block instantly")
	}
	// Consecutive code-5 events accumulate toward maxFailedTimes.
	for i := 0; i < 3; i++ {
		blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-5", 5, 10, true, true, 5)
	}
	if blocked {
		t.Fatalf("4 low-weight failures must not yet block with maxFailedTimes=5")
	}
	blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-5", 5, 10, true, true, 5)
	if !blocked {
		t.Fatalf("5 low-weight failures must block")
	}

	// A checked success clears the counter but does not unblock an existing
	// block (Nodes entry outlives FailCounts).
	blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-3", 5, 10, false, true, 0)
	if blocked {
		t.Fatalf("checked success must not set the blocked flag")
	}
	for i := 0; i < 4; i++ {
		s.UpdateHostStatus(grp, cfg, wt, md, "node-3", 5, 10, true, true, 3)
	}
	blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-3", 5, 10, true, true, 3)
	if !blocked {
		t.Fatalf("5 dial failures must block")
	}

	// code 7 (zero-progress gate) and code 4 (zero-traffic) block instantly.
	blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-7", 5, 10, true, true, 7)
	if !blocked {
		t.Fatalf("zero-progress gate (code 7) must block instantly")
	}
	blocked = s.UpdateHostStatus(grp, cfg, wt, md, "node-4", 5, 10, true, true, 4)
	if !blocked {
		t.Fatalf("zero-traffic (code 4) must block instantly")
	}

	s.ClearHostStatus(grp, cfg, wt)
}

// Store is the concrete store used by these tests; keep a package-level
// instance so tests stay readable. NewStore(nil) initializes the in-memory
// caches/queue; every persistence path gracefully skips a nil bbolt db.
var s = NewStore(nil)

func (st *Store) ClearHostStatus(group, config, wildcardTarget string) {
	pathPrefix := FormatDBKey(KeyTypeHostFailures, config, group, wildcardTarget)
	hostStatusCache.Delete(pathPrefix)
}

func TestHostFailureNodeTTLPositive(t *testing.T) {
	if HostFailureNodeTTL <= 0 {
		t.Fatalf("HostFailureNodeTTL must be positive, got %v", HostFailureNodeTTL)
	}
	if HostFailureNodeTTL < 24*time.Hour {
		t.Fatalf("expected at least 24h block window, got %v", HostFailureNodeTTL)
	}
}
