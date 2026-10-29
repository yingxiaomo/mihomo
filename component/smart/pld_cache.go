package smart

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

// Lock order: storeBarrier -> PLDCache.mu -> PLDRecord.mu -> bbolt. Flush and
// orphan deletion hold the write side until DB deletion and live invalidation
// finish. A save holds the read side through the actual DB commit, not merely
// through validation. Never acquire storeBarrier from inside a bbolt callback.
var storeBarrier sync.RWMutex

var cacheRegistry struct {
	mu          sync.Mutex
	caches      map[weak.Pointer[PLDCache]]struct{}
	writes      map[writeScope]weak.Pointer[writeGeneration]
	writeSweeps uint64
}

type writeScope struct{ config, group, node string }
type writeGeneration struct {
	invalid atomic.Bool
	// Avoid Go's tiny allocator: a weak pointer to a tiny object can remain
	// non-nil forever when it shares an allocation with another live object.
	_ *byte
}

// PLDCache owns one live group's rewards and switch clocks. The registry holds
// only weak pointers: forgotten groups cannot be retained by a listener closure.
// Pointer identity in records is the generation token for in-flight saves.
type PLDCache struct {
	mu            sync.Mutex
	store         *Store
	config, group string
	records       map[string]*PLDRecord
	switches      map[string]time.Time
	closed        bool
}

func (s *Store) NewPLDCache(config, group string) *PLDCache {
	c := &PLDCache{store: s, config: config, group: group, records: make(map[string]*PLDRecord), switches: make(map[string]time.Time)}
	cacheRegistry.mu.Lock()
	defer cacheRegistry.mu.Unlock()
	if cacheRegistry.caches == nil {
		cacheRegistry.caches = make(map[weak.Pointer[PLDCache]]struct{})
	}
	for p := range cacheRegistry.caches {
		if p.Value() == nil {
			delete(cacheRegistry.caches, p)
		}
	}
	cacheRegistry.caches[weak.Make(c)] = struct{}{}
	return c
}

func (c *PLDCache) Close() {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	c.closed = true
	clear(c.records)
	clear(c.switches)
	c.mu.Unlock()
	cacheRegistry.mu.Lock()
	delete(cacheRegistry.caches, weak.Make(c))
	cacheRegistry.mu.Unlock()
}

func (c *PLDCache) Get(target, node string) *PLDRecord {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	key := FormatPLDKey(c.config, c.group, target, node)
	if r := c.records[key]; r != nil {
		return r
	}
	r := &PLDRecord{}
	if c.closed {
		return r
	}
	if data, err := c.store.DBViewGetItem(key); err == nil {
		// Invalid/corrupt records are rebuilding state, not partial rewards.
		if json.Unmarshal(data, r) != nil {
			r = &PLDRecord{}
		}
	}
	c.records[key] = r
	return r
}

// Save discards a retired record, even if a new incarnation has the same key.
func (c *PLDCache) Save(target, node string, r *PLDRecord) error {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	key := FormatPLDKey(c.config, c.group, target, node)
	if r == nil || c.closed || c.records[key] != r {
		return nil
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.store.DBBatchPutItem(key, data)
}

func (c *PLDCache) SwitchCooldownElapsed(target string, cooldown time.Duration) bool {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	last, ok := c.switches[target]
	return !ok || time.Since(last) >= cooldown
}

func (c *PLDCache) MarkSwitch(target string) {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.switches[target] = time.Now()
	}
}

func (c *PLDCache) Nodes() []string {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	nodes := make(map[string]struct{})
	// Upgrade weak pointers only for this enumeration. Do not keep a snapshot
	// of owners in a long-lived closure or acquire registry.mu from c.mu.
	cacheRegistry.mu.Lock()
	for p := range cacheRegistry.caches {
		owner := p.Value()
		if owner == nil {
			delete(cacheRegistry.caches, p)
			continue
		}
		if owner.config != c.config || owner.group != c.group {
			continue
		}
		owner.mu.Lock()
		for key := range owner.records {
			_, _, _, node, ok := ParsePLDKey(key)
			if ok {
				nodes[node] = struct{}{}
			}
		}
		owner.mu.Unlock()
	}
	cacheRegistry.mu.Unlock()
	// Include PLD-only persisted nodes, which official smart stats cannot see.
	prefix := PLDKeyPrefix + escapeKeyPart(c.config) + ":" + escapeKeyPart(c.group) + ":"
	if rows, err := c.store.DBViewPrefixScan(prefix, -1, false); err == nil {
		for key := range rows {
			_, _, _, node, ok := ParsePLDKey(key)
			if ok {
				nodes[node] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(nodes))
	for node := range nodes {
		result = append(result, node)
	}
	return result
}

func (c *PLDCache) Evict(cutoff int64, switchCutoff time.Time) {
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, r := range c.records {
		if r.GetLastUpdated() < cutoff {
			delete(c.records, key)
		}
	}
	for target, at := range c.switches {
		if at.Before(switchCutoff) {
			delete(c.switches, target)
		}
	}
}

func matchesScope(level, config, group, c, g string) bool {
	return level == "all" || c == config && (level == "config" || g == group)
}

// invalidateLive is called only with storeBarrier's write side held. Node
// invalidation drops only matching rewards and their targets' switch clocks.
func invalidateLive(level, config, group string, nodes map[string]struct{}) {
	// Config-level views include this group's rows; invalidate only the
	// ancestor views, not neighboring groups' cached views or records.
	if level == "group" {
		for _, kind := range []string{KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeRanking, KeyTypeHostFailures} {
			dbResultCache.Delete(FormatDBKey(kind, config))
		}
	}
	cacheRegistry.mu.Lock()
	defer cacheRegistry.mu.Unlock()
	for p := range cacheRegistry.caches {
		c := p.Value()
		if c == nil {
			delete(cacheRegistry.caches, p)
			continue
		}
		if !matchesScope(level, config, group, c.config, c.group) {
			continue
		}
		c.mu.Lock()
		if nodes == nil {
			clear(c.records)
			clear(c.switches)
		} else {
			for key := range c.records {
				_, _, target, node, ok := ParsePLDKey(key)
				if _, removed := nodes[node]; ok && removed {
					delete(c.records, key)
					delete(c.switches, target)
				}
			}
		}
		c.mu.Unlock()
	}
	for scope, p := range cacheRegistry.writes {
		token := p.Value()
		if token == nil {
			delete(cacheRegistry.writes, scope)
			continue
		}
		if !matchesScope(level, config, group, scope.config, scope.group) &&
			!(level == "group" && scope.config == config && scope.group == "") {
			continue
		}
		if nodes != nil && scope.node != "" {
			if _, removed := nodes[scope.node]; !removed {
				continue
			}
		}
		token.invalid.Store(true)
		delete(cacheRegistry.writes, scope)
	}
}

// stampOperations runs under storeBarrier's read side. Detached async batches
// carry their own token, so invalidating a scope also cancels drained snapshots.
func stampOperations(ops []StoreOperation) {
	cacheRegistry.mu.Lock()
	defer cacheRegistry.mu.Unlock()
	if cacheRegistry.writes == nil {
		cacheRegistry.writes = make(map[writeScope]weak.Pointer[writeGeneration])
	}
	// Do not sweep every active key for every connection statistic. Periodic
	// sweeping bounds dead weak entries without making append O(active nodes).
	cacheRegistry.writeSweeps++
	if cacheRegistry.writeSweeps%256 == 1 {
		for scope, p := range cacheRegistry.writes {
			if p.Value() == nil {
				delete(cacheRegistry.writes, scope)
			}
		}
	}
	for i := range ops {
		if ops[i].generation != nil {
			continue
		}
		if snapshot := ops[i].StatsSnapshot; snapshot != nil && snapshot.origin != nil {
			ops[i].generation = snapshot.origin.generation
			continue
		}
		scope := writeScope{ops[i].Config, ops[i].Group, ops[i].Node}
		ops[i].generation = generationForScopeLocked(scope)
	}
}

// generationForScopeLocked requires registry.mu and storeBarrier's read side.
func generationForScopeLocked(scope writeScope) *writeGeneration {
	if cacheRegistry.writes == nil {
		cacheRegistry.writes = make(map[writeScope]weak.Pointer[writeGeneration])
	}
	token := cacheRegistry.writes[scope].Value()
	if token == nil {
		token = &writeGeneration{}
		cacheRegistry.writes[scope] = weak.Make(token)
	}
	return token
}

func statsOriginForKey(key string) *statsOrigin {
	// Targets can contain slashes (for example IP-CIDR rule payloads).
	parts := strings.SplitN(key, "/", 5)
	if len(parts) != 5 || parts[0] != "smart" || parts[1] != KeyTypeStats {
		return nil
	}
	lastSlash := strings.LastIndexByte(parts[4], '/')
	if lastSlash < 0 {
		return nil
	}
	return statsOriginForScope(key, writeScope{parts[2], parts[3], parts[4][lastSlash+1:]})
}

func statsOriginForScope(key string, scope writeScope) *statsOrigin {
	cacheRegistry.mu.Lock()
	defer cacheRegistry.mu.Unlock()
	return &statsOrigin{key: key, generation: generationForScopeLocked(scope)}
}

// beginView obtains a cancellable group (or config ancestor) token. The DB
// scan never holds storeBarrier; only token acquisition and publication do.
func beginView(prefix string) *writeGeneration {
	parts := strings.Split(prefix, "/")
	if len(parts) < 3 || parts[0] != "smart" {
		return nil
	}
	scope := writeScope{config: parts[2]}
	if len(parts) > 3 {
		scope.group = parts[3]
	}
	storeBarrier.RLock()
	defer storeBarrier.RUnlock()
	cacheRegistry.mu.Lock()
	defer cacheRegistry.mu.Unlock()
	return generationForScopeLocked(scope)
}
