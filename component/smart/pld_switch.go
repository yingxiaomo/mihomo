package smart

import (
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// TryPrimarySwitch commits the observed primary and its switch clock as one
// target transition. Readers of the clock use the same cache mutex, so a
// challenger cannot observe a newly published primary before its cooldown.
// Failure avoidance may bypass the clock, but never the observed version.
func (c *PLDCache) TryPrimarySwitch(target string, proxies []C.Proxy, observedVersion uint64, cooldown time.Duration, force bool) bool {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || target == "" || len(proxies) == 0 {
		return false
	}
	if !force {
		if last, found := c.switches[target]; found && time.Since(last) < cooldown {
			return false
		}
	}
	if !c.store.CASStoreUnwrapResult(c.group, c.config, target, proxies, observedVersion) {
		return false
	}
	c.switches[target] = time.Now()
	return true
}
