package outboundgroup

import (
	"time"

	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
)

func (s *SmartPLD) rewardCache() *smart.PLDCache {
	s.pldCacheOnce.Do(func() { s.pldCache = s.store.NewPLDCache(s.configName, s.Name()) })
	return s.pldCache
}

// refreshUnwrapCache acts only on the generation observed by the request. Every
// rejection and renewal is conditional, including same-name replacements (ABA).
// Request metadata is deliberately not captured by the background worker.
func (s *SmartPLD) refreshUnwrapCache(isUDP, largeMode bool, target string, old smart.UnwrapMap) {
	if len(old.Proxies) == 0 {
		return
	}
	remove := func() { s.store.DeleteUnwrapSnapshot(s.Name(), s.configName, target, old) }
	var proxy C.Proxy
	for _, p := range s.GetProxies(true) {
		if p != nil && p.Name() == old.Proxies[0] {
			proxy = p
			break
		}
	}
	if proxy == nil || !proxy.AliveForTestUrl(s.testUrl) || (isUDP && !proxy.SupportUDP()) {
		remove()
		return
	}
	if blocked := s.store.GetBlockedNodes(s.Name(), s.configName); blocked[old.Proxies[0]] {
		remove()
		return
	}
	rec := s.getPLDRecord(target, old.Proxies[0])
	if rec.InFailureCooldown(time.Now().Unix()) {
		remove()
		return
	}
	if s.usePLD {
		if reward, _, count := rec.ChannelStats(largeMode); count >= 2 && reward < 0 {
			remove()
			return
		}
	}
	s.store.RenewUnwrapSnapshot(s.Name(), s.configName, target, old)
}
