package outboundgroup

import (
	"sync"

	"github.com/metacubex/mihomo/common/callback"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// pldConnectionOutcome serializes gates with final statistics. The old atomic
// latch guarded only PLD reward mutation, not host strikes, and could race a
// successful close update. One connection now has one finalized outcome.
type pldConnectionOutcome struct {
	mu         sync.Mutex
	closed     bool
	recorded   bool
	failed     bool // a gate has already charged host status AND PLD state
	localClose bool
}

func (o *pldConnectionOutcome) finish() {
	o.mu.Lock()
	o.closed = true
	// Explicit Close is a local lifecycle event even if no Read was attempted.
	// A recorded gate or a genuine observed transfer error still takes priority.
	o.localClose = true
	o.mu.Unlock()
}

func (o *pldConnectionOutcome) readError(err error) error {
	if callback.IsConnectionLifecycle(err) {
		o.mu.Lock()
		o.localClose = true
		o.mu.Unlock()
	}
	return callback.TransferError(err)
}

// findPLDTracker uses the immutable metadata identity published by Manager.Join.
// The delayed gates are scheduled before NewTCPTracker assigns metadata.UUID,
// so reading that mutable string there has no happens-before relationship.
// This lookup is only on delayed quality gates, never the read/write hot path.
func findPLDTracker(metadata *C.Metadata) (found statistic.Tracker) {
	statistic.DefaultManager.Range(func(tracker statistic.Tracker) bool {
		if tracker.Info().Metadata == metadata {
			found = tracker
			return false
		}
		return true
	})
	return
}

// failPLDConnection is the shared first-byte/zero-progress gate transaction.
// A completed connection cannot be punished by a late timer. The mutex also
// makes simultaneous gates and asynchronous close statistics exactly-once.
func (s *SmartPLD) failPLDConnection(o *pldConnectionOutcome, metadata *C.Metadata, node string, code int64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.failed || o.localClose {
		return false
	}
	o.failed = true
	s.markNodeFailure(metadata, node, true, true, code)
	target := metadata.SmartTarget
	if target == "" {
		target = metadata.WildcardTarget
	}
	// An old connection through A must not evict a newly selected B.
	s.store.DeleteUnwrapResultIfPrimary(s.Name(), s.configName, target, node)
	rec := s.getPLDRecord(target, node)
	rec.FillFailure(s.failureCooldownSec())
	s.savePLDRecord(target, node, rec)
	return true
}
