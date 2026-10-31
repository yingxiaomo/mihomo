package outboundgroup

import C "github.com/metacubex/mihomo/constant"

// publishedPrimaryOrder retains the complete candidate field while following
// an updated anchor. Omitting the other candidates would let filterProxies'
// fill-up path prepend an old challenger ahead of the valid published primary.
func publishedPrimaryOrder(current, ranked []string, proxies []C.Proxy) []string {
	seen := make(map[string]bool, len(proxies))
	ordered := make([]string, 0, len(proxies))
	appendName := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			ordered = append(ordered, name)
		}
	}
	for _, name := range current {
		appendName(name)
	}
	for _, name := range ranked {
		appendName(name)
	}
	for _, p := range proxies {
		if p != nil {
			appendName(p.Name())
		}
	}
	return ordered
}
