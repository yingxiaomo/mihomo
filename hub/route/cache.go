package route

import (
	"encoding/json"

	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func cacheRouter() http.Handler {
	r := chi.NewRouter()
	r.Post("/fakeip/flush", flushFakeIPPool)
	r.Post("/smart/flush", flushAllSmartCache)
	r.Post("/smart/flush/{config}", flushSmartConfigCache)
	r.Get("/smart/pld", getSmartPLDState)
	r.Post("/dns/flush", flushDnsCache)
	return r
}

func flushFakeIPPool(w http.ResponseWriter, r *http.Request) {
	err := resolver.FlushFakeIP()
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	render.NoContent(w, r)
}

func flushAllSmartCache(w http.ResponseWriter, r *http.Request) {
	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart cache not available"))
		return
	}

	if err := smartStore.FlushAll(); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}

func flushSmartConfigCache(w http.ResponseWriter, r *http.Request) {
	configName := chi.URLParam(r, "config")
	if configName == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("config name is required"))
		return
	}

	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart cache not available"))
		return
	}

	if err := smartStore.FlushByConfig(configName); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}

// getSmartPLDState dumps every persisted PLD reward record for debugging and
// observation — the same (config, group, target, node) keyed state the D-UCB
// selection reads. Optional query filters: ?group=<name>&target=<target>.
// Read-only: unlike the weight trails in the core log this survives the
// scheduled log clears, so multi-day observation does not depend on them.
func getSmartPLDState(w http.ResponseWriter, r *http.Request) {
	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart cache not available"))
		return
	}

	// maxResults < 0 = full scan (0 would return nothing); strict must be
	// false because PLD keys continue with the config name, not a '/'.
	items, err := smartStore.DBViewPrefixScan(smart.PLDKeyPrefix, -1, false)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	groupFilter := r.URL.Query().Get("group")
	targetFilter := r.URL.Query().Get("target")
	states := make([]smart.PLDKeyState, 0, len(items))
	for key, data := range items {
		config, group, target, node, ok := smart.ParsePLDKey(key)
		if !ok {
			continue
		}
		if groupFilter != "" && group != groupFilter {
			continue
		}
		if targetFilter != "" && target != targetFilter {
			continue
		}
		var rec smart.PLDRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			continue
		}
		states = append(states, smart.PLDKeyState{
			Config:     config,
			Group:      group,
			Target:     target,
			Node:       node,
			Record:     &rec,
			SmallScore: rec.ScenarioScore(false),
			LargeScore: rec.ScenarioScore(true),
		})
	}
	render.JSON(w, r, states)
}

func flushDnsCache(w http.ResponseWriter, r *http.Request) {
	resolver.ClearCache()
	render.NoContent(w, r)
}
