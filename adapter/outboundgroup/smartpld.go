package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/callback"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/common/xsync"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/geodata"
	"github.com/metacubex/mihomo/component/mmdb"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	"github.com/metacubex/mihomo/component/smart/tcpstats"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"
	"github.com/metacubex/mihomo/tunnel/statistic"
	"github.com/samber/lo"
)

// SmartPLDOption is the option set for the smart-pld group type. It is fully
// independent from the official smart group: PLD owns its own switches, data
// collection and decision layer; the official weight model is never used.
type SmartPLDOption struct {
	PolicyPriority string  `group:"policy-priority,omitempty"`
	SampleRate     float64 `group:"sample-rate,omitempty"`
	PreferASN      bool    `group:"prefer-asn,omitempty"`
	Tolerance      uint16  `group:"tolerance,omitempty"`

	// PLD-specific switches and tuning knobs.
	PldCollect      bool    `group:"pld-collect,omitempty"`   // PLD 数据收集（SQLite/CSV 样本），独立于官方 collectdata
	PriorWeight     float64 `group:"prior-weight,omitempty"`  // α0 default 0.6
	PriorDecayK     float64 `group:"prior-decay-k,omitempty"` // default 10
	ExploreStrength float64 `group:"explore-strength,omitempty"` // default 0.05
}

type SmartPLD struct {
	*GroupBase
	store                  *smart.Store

	wg                     sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc

	configName             string
	selected               string
	testUrl                string
	expectedStatus         string
	disableUDP             bool

	dataCollector          *lightgbm.PLDDataCollector
	policyPriority         []priorityRule
	priorityCache          xsync.Map[string, float64]
	sampleRate             float64
	pldCollect             bool
	preferASN              bool
	hostFailLimit          int
	tolerance              uint16

	// PLD runtime state
	utorPriorModel    *lightgbm.PriorModel
	usePLD            bool // always true for the smart-pld group; kept for readability of the PLD branches
	priorWeight       float64
	priorDecayK       float64
	exploreStrength   float64

	// pldRecordCache caches per-(target,node) reward state in memory; persisted
	// under the independent "pld:" key namespace of the shared smart.db.
	pldRecordMu    sync.Mutex
	pldRecordCache map[string]*smart.PLDRecord

	// healthChecking guards the proactive group-wide URLTest so concurrent
	// first connections don't fire duplicate full-group probes.
	healthChecking atomic.Bool
}

func NewSmartPLD(option GroupCommonOption, smartOption SmartPLDOption, emptyFallback C.Proxy, providers []provider.ProxyProvider) (*SmartPLD, error) {
	if option.URL == "" {
		option.URL = C.DefaultTestURL
	}

	configName := getConfigFilename()

	s := &SmartPLD{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:            option.Name,
			Type:            C.SmartPLD,
			Hidden:          option.Hidden,
			Icon:            option.Icon,
			Filter:          option.Filter,
			ExcludeFilter:   option.ExcludeFilter,
			ExcludeType:     option.ExcludeType,
			TestTimeout:     option.TestTimeout,
			MaxFailedTimes:  option.MaxFailedTimes,
			EmptyFallback:   emptyFallback,
			Providers:       providers,
		}),
		testUrl:              option.URL,
		expectedStatus:       option.ExpectedStatus,
		configName:           configName,
		disableUDP:           option.DisableUDP,
		policyPriority:       make([]priorityRule, 0),
		sampleRate:           1,
		pldCollect:           smartOption.PldCollect,
		preferASN:            smartOption.PreferASN,
		tolerance:            smartOption.Tolerance,
		usePLD:               true,
		priorWeight:          smartOption.PriorWeight,
		priorDecayK:          smartOption.PriorDecayK,
		exploreStrength:      smartOption.ExploreStrength,
		pldRecordCache:       make(map[string]*smart.PLDRecord),
	}

	s.hostFailLimit = s.maxFailedTimes

	if smartOption.SampleRate > 0 && smartOption.SampleRate <= 1 {
		s.sampleRate = smartOption.SampleRate
	}

	if smartOption.PolicyPriority != "" {
		applyPolicyPriorityPLD(s, smartOption.PolicyPriority)
	}

	s.InitSmart()

	return s, nil
}

func (s *SmartPLD) GetConfigFilename() string {
	return s.configName
}

// ref: component/dialer/dialer.go:314
func (s *SmartPLD) ParallelDialContext(ctx context.Context, proxies []C.Proxy, metadata *C.Metadata, start time.Time, singleDialFunc func(context.Context, C.Proxy, *C.Metadata, time.Time) (C.Conn, int64, error)) (C.Proxy, C.Conn, int64, error) {
	if len(proxies) == 1 {
		conn, connectTime, err := singleDialFunc(ctx, proxies[0], metadata, start)
		return proxies[0], conn, connectTime, err
	}

	n := len(proxies)
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan dialResult, n)

	for i := 0; i < n; i++ {
		go func(proxyIndex int) {
			conn, connectTime, err := singleDialFunc(childCtx, proxies[proxyIndex], metadata, start)
			results <- dialResult{
				proxyIndex:  proxyIndex,
				conn:        conn,
				connectTime: connectTime,
				error:       err,
			}
		}(i)
	}

	drainRemaining := func(pending int) {
		go func() {
			for i := 0; i < pending; i++ {
				if r := <-results; r.conn != nil && r.error == nil {
					_ = r.conn.Close()
				}
			}
		}()
	}

	errs := make([]error, 0, n)
	for received := 0; received < n; received++ {
		select {
		case res := <-results:
			if res.error == nil {
				cancel()
				drainRemaining(n - received - 1)
				return proxies[res.proxyIndex], res.conn, res.connectTime, nil
			}
			errs = append(errs, res.error)

		case <-ctx.Done():
			drainRemaining(n - received)
			return nil, nil, 0, ctx.Err()
		}
	}

	if len(errs) > 0 {
		return nil, nil, 0, errors.Join(errs...)
	}
	return nil, nil, 0, os.ErrDeadlineExceeded
}

func (s *SmartPLD) singleDialContext(ctx context.Context, proxy C.Proxy, metadata *C.Metadata, start time.Time) (c C.Conn, connectTime int64, err error) {
	c, err = proxy.DialContext(ctx, metadata)
	connectTime = time.Since(start).Milliseconds()

	if err != nil {
		// err if ShouldStopRetry should not record as failed in node stats and stop retry
		if tunnel.ShouldStopRetry(err) {
			return nil, connectTime, err
		}
		if !errors.Is(err, context.Canceled) {
			go s.recordConnectionStats(metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, nil, 0, err)
		}
		return nil, connectTime, err
	}

	return c, connectTime, nil
}

func (s *SmartPLD) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	getBatch := func(proxies []C.Proxy, i int) ([]C.Proxy, time.Duration) {
		var batch []C.Proxy
		var historyConnectTime int64
		var timeout time.Duration
		if len(proxies) == 1 {
			batch = proxies[0:1]
		} else if i == 0 {
			batch = proxies[0:1]
		} else {
			begin := 1 + (i-1) * parallelDials
			if begin >= len(proxies) {
				return nil, 0
			}
			end := begin + parallelDials
			if end > len(proxies) {
				end = len(proxies)
			}
			batch = proxies[begin:end]
		}

		for _, p := range batch {
			hct := s.getHistoryConnectStats(metadata, p)
			if hct > historyConnectTime {
				historyConnectTime = hct
			}
		}

		if historyConnectTime > 0 {
			timeout = time.Duration(float64(historyConnectTime) * connectThreshold) * time.Millisecond
		}

		if timeout > C.DefaultTCPTimeout || timeout <= 0 {
			timeout = C.DefaultTCPTimeout
		}

		return batch, timeout
	}

	tryDial := func(proxies []C.Proxy, asnNumber string) (C.Conn, error) {
		var finalErr error
		for i := 0; i < maxRetries; i++ {
			batch, timeout := getBatch(proxies, i)
			if len(batch) == 0 {
				break
			}

			ctxDial, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			p, c, connectTime, err := s.ParallelDialContext(ctxDial, batch, metadata, start, s.singleDialContext)
			cancel()

			if err != nil {
				if tunnel.ShouldStopRetry(err) {
					return nil, err
				}
				finalErr = err
			} else {
				s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget, []C.Proxy{p})
				s.closeSameConnection(metadata, p.Name(), metadata.SmartTarget, asnNumber, false)
				return s.WrapConnWithMetric(c, p, metadata, connectTime), nil
			}
		}

		s.store.DeleteUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget)

		return nil, finalErr
	}

	proxies, asnNumber := s.selectProxies(metadata, s.GetProxies(true))

	// The unwrap cache pins the previously-selected single node. If that node
	// just went down (it stays "alive" until the next health check marks it
	// dead), the first attempt has no other candidate to fall back to and every
	// request fails. Drop the stale unwrap result and re-select over the full
	// healthy pool before giving up.
	conn, err := tryDial(proxies, asnNumber)
	if err != nil && !tunnel.ShouldStopRetry(err) && len(proxies) < maxSelected {
		fallbackProxies := s.selectProxiesFull(metadata, s.GetProxies(true))
		if len(fallbackProxies) > len(proxies) {
			log.Debugln("[PLD] group [%s] target [%s]: %d-candidate selection failed (%v), retrying over %d-node full pool",
				s.Name(), metadata.SmartTarget, len(proxies), err, len(fallbackProxies))
			fallbackConn, fallbackErr := tryDial(fallbackProxies, asnNumber)
			if fallbackErr == nil {
				return fallbackConn, nil
			}
			if !tunnel.ShouldStopRetry(fallbackErr) {
				err = fallbackErr
			}
		}
	}

	return conn, err
}

// selectProxiesFull bypasses the unwrap / prefetch / best-node caches and builds
// a candidate list from the entire healthy proxy pool. Used as a fallback when a
// short (unwrap-pinned, single-node) candidate list fails to connect.
func (s *SmartPLD) selectProxiesFull(metadata *C.Metadata, proxies []C.Proxy) []C.Proxy {
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	isUDP := metadata.NetWork == C.UDP
	return s.filterProxies(metadata, wildcardTarget, nil, nil, proxies, maxSelected, isUDP)
}

func (s *SmartPLD) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	tryPacket := func(proxies []C.Proxy, asnNumber string) (C.PacketConn, error) {
		var finalErr error
		limit := len(proxies)
		if limit > maxSelected {
			limit = maxSelected
		}

		singleProxyRetry := (len(proxies) == 1)

		for i := 0; i < limit; i++ {
			proxy := proxies[i]
			attempts := 1
			if singleProxyRetry {
				attempts = maxRetries
			}

			for a := 0; a < attempts; a++ {
				historyConnectTime := s.getHistoryConnectStats(metadata, proxy)
				var timeout time.Duration
				if historyConnectTime > 0 {
					timeout = time.Duration(float64(historyConnectTime)*connectThreshold) * time.Millisecond
				}
				if timeout > C.DefaultUDPTimeout || timeout <= 0 {
					timeout = C.DefaultUDPTimeout
				}
				ctxDial, cancel := context.WithTimeout(ctx, timeout)
				start := time.Now()
				pc, err := proxy.ListenPacketContext(ctxDial, metadata)
				cancel()
				connectTime := time.Since(start).Milliseconds()

				if err != nil {
					if tunnel.ShouldStopRetry(err) {
						return nil, err
					}
					finalErr = err
					go s.recordConnectionStats(metadata, proxy, connectTime, 0, 0, 0, 0, 0, 0, nil, 0, err)
					continue
				}

				s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget, []C.Proxy{proxy})
				s.closeSameConnection(metadata, proxy.Name(), metadata.SmartTarget, asnNumber, false)
				return s.WrapPacketConnWithMetric(pc, proxy, metadata, connectTime), nil
			}

			if singleProxyRetry {
				break
			}
		}

		s.store.DeleteUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget)

		return nil, finalErr
	}

	proxies, asnNumber := s.selectProxies(metadata, s.GetProxies(true))

	// Same single-node unwrap-cache pitfall as DialContext: fall back to the
	// full healthy pool when the pinned node cannot establish a packet conn.
	pc, err := tryPacket(proxies, asnNumber)
	if err != nil && !tunnel.ShouldStopRetry(err) && len(proxies) < maxSelected {
		fallbackProxies := s.selectProxiesFull(metadata, s.GetProxies(true))
		if len(fallbackProxies) > len(proxies) {
			log.Debugln("[PLD] group [%s] target [%s]: %d-candidate UDP selection failed (%v), retrying over %d-node full pool",
				s.Name(), metadata.SmartTarget, len(proxies), err, len(fallbackProxies))
			fallbackPC, fallbackErr := tryPacket(fallbackProxies, asnNumber)
			if fallbackErr == nil {
				return fallbackPC, nil
			}
			if !tunnel.ShouldStopRetry(fallbackErr) {
				err = fallbackErr
			}
		}
	}

	return pc, err
}

func (s *SmartPLD) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := s.GetProxies(touch)

	if metadata == nil {
		return proxies[0]
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return p
			}
		}
	}

	proxies, _ = s.selectProxies(metadata, proxies)

	return proxies[0]
}

func (s *SmartPLD) IsL3Protocol(metadata *C.Metadata) bool {
	return s.Unwrap(metadata, false).IsL3Protocol(metadata)
}

func (s *SmartPLD) SupportUDP() bool {
	return !s.disableUDP
}

func (s *SmartPLD) WrapConnWithMetric(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.Conn {
	c.AppendToChains(s)

	start := time.Now()

	var firstWriteErr atomic.TypedValue[error]
	var firstReadErr atomic.TypedValue[error]
	var firstReadLatency atomic.Int64

	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err != nil {
				firstWriteErr.Store(err)
			}
		})
	}

	c = callback.NewFirstReadCallBackConn(c, func(err error) {
		firstReadLatency.Store(time.Since(start).Milliseconds())
		if err != nil {
			firstReadErr.Store(err)
		}
	})

	// Passive Packet-Pair readout: stamp first-byte + 64KiB arrival on the real
	// connection. Only installed under usepld; the tracker is read at close.
	var pp *callback.PacketPairCallBackConn
	if s.usePLD {
		pp = callback.NewPacketPairCallBackConn(c)
		c = pp
	}

	return s.registerClosureMetricsCallback(
		c, proxy, metadata, connectTime,
		&firstReadLatency, &firstReadErr, &firstWriteErr,
		pp,
	)
}

func (s *SmartPLD) WrapPacketConnWithMetric(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64) C.PacketConn {
	pc.AppendToChains(s)
	
	var udpLatency atomic.Int64

	pc = callback.NewFirstReadCallBackPacketConn(pc, func(latency int64) {
		udpLatency.Store(latency)
	})

	return s.registerPacketClosureMetricsCallback(pc, proxy, metadata, connectTime, &udpLatency)
}

func (s *SmartPLD) Set(name string) error {
	var p C.Proxy
	for _, proxy := range s.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	s.ForceSet(name)

	return nil
}

func (s *SmartPLD) ForceSet(name string) {
	s.selected = name
}

func (s *SmartPLD) Now() string {
	if s.selected != "" {
		for _, p := range s.GetProxies(false) {
			if p.Name() == s.selected {
				return p.Name()
			}
		}
		s.selected = ""
	}

	return "SmartPLD - Select"
}

func (s *SmartPLD) MarshalJSON() ([]byte, error) {
	proxies := s.GetProxies(false)
	all := make([]string, len(proxies))
	for i, proxy := range proxies {
		all[i] = proxy.Name()
	}

	var policyPriorityBuf strings.Builder
	for i, rule := range s.policyPriority {
		if i > 0 {
			policyPriorityBuf.WriteByte(';')
		}
		fmt.Fprintf(&policyPriorityBuf, "%s:%.2f", rule.pattern, rule.factor)
	}

	return json.Marshal(map[string]any{
		"type":            s.Type().String(),
		"now":             s.Now(),
		"all":             all,
		"testUrl":         s.testUrl,
		"expectedStatus":  s.expectedStatus,
		"fixed":           s.selected,
		"hidden":          s.Hidden(),
		"icon":            s.Icon(),
		"emptyFallback":   s.EmptyFallback().Name(),
		"policy-priority": policyPriorityBuf.String(),
		"pldCollect":      s.pldCollect,
		"sampleRate":      s.sampleRate,
		"preferASN":       s.preferASN,
		"tolerance":       s.tolerance,
	})
}

func (s *SmartPLD) Providers() []provider.ProxyProvider {
	return s.providers
}

func (s *SmartPLD) Proxies() []C.Proxy {
	return s.GetProxies(false)
}

func (s *SmartPLD) filterProxies(metadata *C.Metadata, wildcardTarget string, names []string, weights []float64, all []C.Proxy, minCount int, isUDP bool) []C.Proxy {
	blockedNodes := s.store.GetBlockedNodes(s.Name(), s.configName)
	wtFailNodes, _, _, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, wildcardTarget, metadata.SmartTarget)

	var proxyByName map[string]C.Proxy
	if len(names) > 0 {
		proxyByName = make(map[string]C.Proxy, len(all))
		for _, p := range all {
			proxyByName[p.Name()] = p
		}
	}

	checkNodeUsed := make(map[string]bool, len(names)+len(wtFailNodes))

	// softFailed marks nodes whose PLD reward EMA for this target is negative:
	// they have been failing recently (e.g. Cloudflare-fronted nodes that cannot
	// reach certain destinations) even though url-test still reports them alive.
	// Unwrap/pool candidates that keep selecting such nodes cause per-request
	// timeouts and connection churn, so exclude them from both the ranked names
	// and the fill-up pool (the full fallbackAll keeps them as a last resort).
	softFailed := func(name string) bool {
		if !s.usePLD {
			return false
		}
		target := metadata.SmartTarget
		if target == "" {
			target = wildcardTarget
		}
		rec := s.getPLDRecord(target, name)
		if rec.SampleCount < 2 {
			return false // keep cold / exploratory nodes
		}
		return rec.Reward < 0
	}

	selected := make([]C.Proxy, 0, minCount+1)
	var failedSelected []C.Proxy

	for i, name := range names {
		checkNodeUsed[name] = true
		proxy := proxyByName[name]
		if proxy == nil || blockedNodes[name] || !proxy.AliveForTestUrl(s.testUrl) || (isUDP && !proxy.SupportUDP()) {
			continue
		}
		if softFailed(name) {
			continue
		}
		w := 0.0
		if weights != nil && i < len(weights) {
			w = weights[i]
		}
		if weights != nil {
			if s.usePLD {
				// PLD hybrid scores stay in (0, ~1] and a cold node's prior keeps it
				// above zero, so the hard 0.4 gate would starve exploration. Only
				// drop nodes that are truly negative (strong failure credit).
				if w <= 0 {
					continue
				}
			} else if w < smart.AllowedWeight {
				continue
			}
		}
		if wtFailNodes[name] != 0 {
			failedSelected = append(failedSelected, proxy)
		} else {
			selected = append(selected, proxy)
		}
	}

	if wtBlocked {
		for _, p := range failedSelected {
			if wtFailNodes[p.Name()] != 1 {
				selected = append(selected, p)
			}
		}
	}

	for name := range wtFailNodes {
		checkNodeUsed[name] = true
	}

	// Unwrap result should not filled
	if weights == nil && len(selected) > 0 {
		return selected
	}

	if len(selected) >= len(all) {
		return selected
	}

	if len(selected) >= minCount {
		return selected[:minCount]
	}

	hasPriority := len(s.policyPriority) > 0

	type sortKey struct {
		delay  uint16
		factor float64
		index  int
	}
	allKeys := make(map[string]sortKey, len(all))
	for i, p := range all {
		name := p.Name()
		k := sortKey{delay: p.LastDelayForTestUrl(s.testUrl), index: i}
		if hasPriority {
			k.factor = s.getPriorityFactor(name)
		}
		allKeys[name] = k
	}

	defaultSort := func(proxies []C.Proxy) []C.Proxy {
		sort.SliceStable(proxies, func(i, j int) bool {
			ni, nj := proxies[i].Name(), proxies[j].Name()
			ki, kj := allKeys[ni], allKeys[nj]
			if hasPriority && ki.factor != kj.factor {
				return ki.factor > kj.factor
			}
			// Tolerance: delays within tolerance are treated as equal, preventing jitter
			if s.tolerance > 0 {
				var diff uint16
				if ki.delay > kj.delay {
					diff = ki.delay - kj.delay
				} else {
					diff = kj.delay - ki.delay
				}
				if diff <= s.tolerance {
					return ki.index < kj.index
				}
			}
			if ki.delay != kj.delay {
				return ki.delay < kj.delay
			}
			return ki.index < kj.index
		})
		return proxies
	}

	filteredAll := make([]C.Proxy, 0, len(all))

	for _, p := range all {
		name := p.Name()
		if checkNodeUsed[name] {
			continue
		}
		if blockedNodes[name] {
			continue
		}
		if !p.AliveForTestUrl(s.testUrl) || (isUDP && !p.SupportUDP()) {
			continue
		}
		if softFailed(name) {
			continue
		}
		filteredAll = append(filteredAll, p)
	}

	filteredAll = defaultSort(filteredAll)

	if !hasPriority {
		if len(filteredAll) < len(all)/3 {
			rand.Shuffle(len(filteredAll), func(i, j int) {
				filteredAll[i], filteredAll[j] = filteredAll[j], filteredAll[i]
			})
		}
	}

	var prependProxy C.Proxy
	var hasPrepend bool

	for _, p := range filteredAll {
		if !hasPrepend && (len(selected) < minCount/2 || len(wtFailNodes) <= 0 || wtBlocked) {
			prependProxy = p
			hasPrepend = true
		} else {
			selected = append(selected, p)
		}
		total := len(selected)
		if hasPrepend {
			total++
		}
		if total >= minCount {
			break
		}
	}
	if hasPrepend {
		selected = append(selected, nil)
		copy(selected[1:], selected)
		selected[0] = prependProxy
		if len(selected) > minCount {
			selected = selected[:minCount]
		}
	}

	if len(selected) == 0 {
		fallbackAll := defaultSort(all)
		for _, p := range fallbackAll {
			if p.AliveForTestUrl(s.testUrl) {
				selected = append(selected, p)
			}
			if len(selected) >= minCount {
				break
			}
		}

		if len(selected) == 0 {
			for _, p := range fallbackAll {
				selected = append(selected, p)
				if len(selected) >= minCount {
					break
				}
			}
		}
	}

	return selected
}

// 节点选择
func (s *SmartPLD) selectProxies(metadata *C.Metadata, proxies []C.Proxy) ([]C.Proxy, string) {
	// 添加ASN信息
	asnNumber := s.getASNCode(metadata)
	wildcardTarget := smart.GetEffectiveTarget(metadata.Host, metadata.DstIP.String())
	metadata.WildcardTarget = wildcardTarget
	if metadata.SmartTarget == "" {
		metadata.SmartTarget = wildcardTarget
	}

	if s.selected != "" {
		for _, p := range proxies {
			if p.Name() == s.selected {
				return []C.Proxy{p}, asnNumber
			}
		}
	}

	// 预解析缓存或实时计算
	computeFreshNodes := func(isUDP bool) ([]string, []float64) {
		if proxiesName, weights := s.store.GetPrefetchResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); len(proxiesName) > 0 {
			return proxiesName, weights
		}
		if proxiesName, weights, err := s.store.GetBestProxyForTarget(s.Name(), s.configName, metadata.SmartTarget, asnNumber, isUDP); err == nil && len(proxiesName) > 0 {
			return proxiesName, weights
		}
		return nil, nil
	}

	// 异步更新过期缓存（stale-while-revalidate）
	refreshUnwrapCache := func(isUDP bool) {
		names, _ := computeFreshNodes(isUDP)
		if len(names) == 0 {
			return
		}
		allProxies := s.GetProxies(true)
		proxyByName := make(map[string]C.Proxy, len(allProxies))
		for _, p := range allProxies {
			proxyByName[p.Name()] = p
		}
		resultProxies := make([]C.Proxy, 0, len(names))
		for _, name := range names {
			if p, ok := proxyByName[name]; ok {
				resultProxies = append(resultProxies, p)
			}
		}
		if len(resultProxies) > 0 {
			s.store.StoreUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget, resultProxies)
		}
	}

	trySelector := func(isUDP bool) ([]string, []float64) {
		// 检查匹配缓存
		if proxiesName, expired := s.store.GetUnwrapResult(s.Name(), s.configName, metadata.SmartTarget, asnNumber, metadata.WildcardTarget); len(proxiesName) > 0 {
			if expired {
				go refreshUnwrapCache(isUDP)
			}
			return proxiesName, nil
		}
		return computeFreshNodes(isUDP)
	}

	isUDP := metadata.NetWork == C.UDP
	resultNames, resultWeights := trySelector(isUDP)

	// PLD: always re-rank the candidate nodes by the hybrid D-UCB score
	// (prior + EWMA reward + exploration bonus) when enabled — even on
	// unwrap-cache hits. The cache only saves the bbolt ranking lookup;
	// PLD's in-memory scoring is cheap and its detailed P/F decision trail
	// should be visible in the dashboard logs for every selection.
	if s.usePLD && len(resultNames) > 0 {
		resultNames, resultWeights = s.computePLDWeights(metadata, resultNames, proxies)
	}

	result := s.filterProxies(metadata, wildcardTarget, resultNames, resultWeights, proxies, maxSelected, isUDP)

	return result, asnNumber
}

// computePLDWeights computes the D-UCB final weight for each candidate node and
// re-ranks names/weights by it. It needs the request features (metadata) and the
// per-node EWMA reward / sample count from the stats store.
func (s *SmartPLD) computePLDWeights(metadata *C.Metadata, names []string, proxies []C.Proxy) ([]string, []float64) {
	if len(names) == 0 {
		return names, nil
	}

	features := lightgbm.BuildRequestFeatures(metadata, s.Name(), time.Now())
	target := metadata.SmartTarget

	type scoreEntry struct {
		name  string
		prior float64 // 阶段一：PriorModel 先验分（无模型时为 flat 0.5）
		w     float64 // 阶段二：D-UCB FinalWeight（prior + reward EMA + explore）
	}
	entries := make([]scoreEntry, 0, len(names))

	totalSamples := int64(0)
	rewards := make(map[string]float64, len(names))
	counts := make(map[string]float64, len(names))
	rewardVars := make(map[string]float64, len(names))
	delays := make(map[string]float64, len(names))
	nodeTypes := make(map[string]float64, len(names))

	// 该 target 的累计流量统计（画像聚合用）：所有候选节点记录的
	// downloadTotal / sampleCount / maxDownloadRate 汇总 → 平均单次下载量。
	var dlTotal, peakRate float64
	var cntTotal int64

	// name → proxy 索引，取节点实时健康检查延迟与协议类型（feature 15/18）。
	proxyByName := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyByName[p.Name()] = p
	}

	for _, name := range names {
		// reward state comes from PLD's own key namespace; the official record
		// still supplies downloadTotal/maxDownloadRate for the traffic profile.
		pldRec := s.getPLDRecord(target, name)
		rew := pldRec.Reward
		cnt := pldRec.SampleCount
		rv := pldRec.RewardVar
		rewards[name] = rew
		counts[name] = float64(cnt)
		rewardVars[name] = rv
		totalSamples += cnt
		cntTotal += cnt
		cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, name)
		rec := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, name)
		if dl, ok := rec.Get("downloadTotal").(float64); ok {
			dlTotal += dl
		}
		if mx, ok := rec.Get("maxDownloadRate").(float64); ok && mx > peakRate {
			peakRate = mx
		}

		var delay float64
		if p, ok := proxyByName[name]; ok {
			delay = float64(p.LastDelayForTestUrl(s.testUrl))
			nodeTypes[name] = float64(p.Type())
		}
		delays[name] = delay
	}

	// 刷新域名流量档位画像（feature 20 的数据驱动源）。本次请求先用旧缓存，
	// 聚合结果下次命中 —— 热路径零额外扫描。
	lightgbm.UpdateTargetTrafficLevel(target, dlTotal, peakRate, cntTotal)

	for _, name := range names {
		var prior float64
		if s.utorPriorModel != nil {
			p, _ := s.utorPriorModel.PredictPrior(features, name, rewards[name], counts[name],
				rewardVars[name], delays[name], nodeTypes[name])
			prior = p
		} else {
			prior = 0.5 // flat prior when no model
		}
		w := smart.FinalWeight(prior, rewards[name], int64(counts[name]), totalSamples,
			s.priorWeight, s.priorDecayK, s.exploreStrength)
		entries = append(entries, scoreEntry{name: name, prior: prior, w: w})
	}

	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].w != entries[j].w {
			return entries[i].w > entries[j].w
		}
		return entries[i].name < entries[j].name
	})

	// PLD debug：选路热路径上的追踪日志（debug 级别，不刷生产日志）。
	// 每个节点同时给出阶段一 prior 分（P，PriorModel 输出）与阶段二 D-UCB
	// 融合分（F，FinalWeight），证明模型确实主导了本次排序；无模型时
	// P=0.500（flat prior），F 只剩在线奖励 + 探索项。
	// DomainLevel 为 feature 20 域名流量档位（0=unknown 1=small 2=medium 3=heavy），
	// 让日志面板能直接看到"域名 → 流量档位"先验在选路中生效。
	if len(entries) > 0 {
		top := 5
		if len(entries) < top {
			top = len(entries)
		}
		var b strings.Builder
		for i := 0; i < top; i++ {
			if i > 0 {
				b.WriteString(" | ")
			}
			fmt.Fprintf(&b, "%s(P:%.3f,F:%.3f)", entries[i].name, entries[i].prior, entries[i].w)
		}
		log.Debugln("[PLD-Track] Group: [%s] Target: [%s] DomainLevel:[%d] Rank(%d): %s",
			s.Name(), target, int(features.DomainTrafficLevel), len(entries), b.String())
	}

	outNames := make([]string, len(entries))
	outWeights := make([]float64, len(entries))
	for i, e := range entries {
		outNames[i] = e.name
		outWeights[i] = e.w
	}
	return outNames, outWeights
}

func (s *SmartPLD) InitSmart() {
	s.store = cachefile.GetSmartStore()

	s.ctx, s.cancel = context.WithCancel(context.Background())

	smartInitOnce.Do(func() {
		s.startTimedTask(5*time.Minute, checkInterval, "Global orphaned groups Clean up", s.cleanupOrphanedGroups, true)
		s.startTimedTask(5*time.Second, cacheParamAdjustInterval, "Global cache parameters adjustment", s.store.AdjustCacheParameters, false)
		s.startTimedTask(5*time.Minute, flushQueueInterval, "Global queues flush", func() {
			s.store.FlushQueue(true)
		}, false)
		// try load ASN database
		if s.preferASN {
			if err := geodata.InitASN(); err != nil {
				log.Warnln("[PLD] Failed to load ASN database: %v", err)
			}
		}
	})

	s.startTimedTask(10*time.Minute, cleanupInterval, "Group orphaned nodes clean up", s.cleanupOrphanedNodeCache, true)
	// Proactive group-wide health check: prime every node's delay/alive state
	// right after startup (5s) and keep it fresh (5min), so cold-start routing
	// never falls back to blind selection over untested nodes.
	s.startTimedTask(5*time.Second, 5*time.Minute, "Group nodes health check", s.runPLDHealthCheck, false)
	s.startTimedTask(5*time.Minute, prefetchInterval, "Group targets prefetch", s.runPrefetch, false)
	s.startTimedTask(5*time.Minute, checkInterval, "Group nodes stable check", s.checkNodesStable, false)
	s.startTimedTask(5*time.Minute, rankingInterval, "Group nodes Ranking", s.updateNodeRanking, false)
	s.startTimedTask(5*time.Minute, recoveryCheckInterval, "Group nodes recovery check", s.checkBlockedNodes, false)
	s.startTimedTask(15*time.Minute, hostStatusCheckInterval, "Group host status check", s.checkHostStatus, false)
	s.startTimedTask(1*time.Minute, prefetchInterval, "Group maxFailedTimes refresh", s.applyMaxFailedTimes, false)
	s.startTimedTask(10*time.Minute, cleanupInterval, "Group old records clean up", func() {
		s.store.CleanupOldRecords(s.Name(), s.configName)
	}, false)
	if s.pldCollect {
		// PLD owns its own sample collector (SQLite, CSV fallback on
		// unsupported arches); the official smart-collector-size knob is not
		// consulted — 0 means the PLD default (100 MB).
		lightgbm.InitPLDCollector(0)
		s.dataCollector = lightgbm.GetPLDCollector()
	}

	// PLD runs independently of uselightgbm: the prior model is optional (falls
	// back to a flat prior when absent), but the online D-UCB + reward feedback
	// must activate with usepld on its own.
	if s.usePLD {
		s.utorPriorModel = lightgbm.GetPriorModel()
		if s.priorWeight <= 0 {
			s.priorWeight = smart.PriorWeightDefault
		}
		if s.priorDecayK <= 0 {
			s.priorDecayK = smart.PriorDecayKDefault
		}
		if s.exploreStrength <= 0 {
			s.exploreStrength = smart.ExploreStrengthDefault
		}
		log.Infoln("[PLD] group %s enabled (prior=%.2f decayK=%.1f explore=%.2f)",
			s.Name(), s.priorWeight, s.priorDecayK, s.exploreStrength)
	}
}

// runPLDHealthCheck proactively measures every node's delay against the group
// test URL so filterProxies' AliveForTestUrl gate has real data to work with.
// Without it, a cold-start group (or a group whose providers were never lazily
// tested) filters out every node and falls back to blind alphabetical selection.
// The measured delays also let LastDelayForTestUrl return sane values that the
// defaultSort tie-breaks and the PLD-Track trail can rely on.
func (s *SmartPLD) runPLDHealthCheck() {
	if s.healthChecking.Load() {
		return
	}
	s.healthChecking.Store(true)
	defer s.healthChecking.Store(false)

	expected, _ := utils.NewUnsignedRanges[uint16](s.expectedStatus)
	// NewUnsignedRanges returns (range, error) — call through the same shape the
	// parser uses; empty expectedStatus is fine (matches any non-error response).
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.testTimeout)*time.Millisecond+30*time.Second)
	defer cancel()

	mp, err := s.URLTest(ctx, s.testUrl, expected)
	if err != nil {
		log.Debugln("[PLD] group %s health check: %v", s.Name(), err)
		return
	}
	aliveCnt, timeoutCnt := 0, 0
	for _, d := range mp {
		if d > 0 && d < 0xffff {
			aliveCnt++
		} else {
			timeoutCnt++
		}
	}
	log.Debugln("[PLD] group %s health check done: %d alive, %d timeout (of %d)",
		s.Name(), aliveCnt, timeoutCnt, len(mp))
}

// task run after tunnel.Running
func (s *SmartPLD) startTimedTask(initialDelay, interval time.Duration, taskName string, task func(), runOnce bool) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		waitTicker := time.NewTicker(100 * time.Millisecond)
		for tunnel.Status() != tunnel.Running {
			select {
			case <-waitTicker.C:
			case <-s.ctx.Done():
				waitTicker.Stop()
				return
			}
		}
		waitTicker.Stop()

		jitterRange := 30.0
		intervalJitter := time.Duration(rand.Float64() * jitterRange * float64(time.Second))

		adjustedInitialDelay := initialDelay + intervalJitter
		adjustedInterval := interval + intervalJitter

		select {
		case <-time.After(adjustedInitialDelay):
		case <-s.ctx.Done():
			return
		}

		if tunnel.Status() == tunnel.Running {
			task()
		}

		if runOnce {
			log.Debugln("[PLD] Task [%s] completed", taskName)
			return
		} else {
			log.Debugln("[PLD] Task [%s] for group [%s] started, interval: %s", taskName, s.Name(), adjustedInterval.Round(time.Second).String())
		}

		ticker := time.NewTicker(adjustedInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if tunnel.Status() == tunnel.Running {
					task()
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *SmartPLD) runPrefetch() {
	proxies := s.GetProxies(true)
	proxyMap := make(map[string]bool, len(proxies))
	for _, proxy := range proxies {
		proxyMap[proxy.Name()] = true
	}
	s.store.RunPrefetch(s.Name(), s.configName, proxyMap)
}

func (s *SmartPLD) updateNodeRanking() {
	proxies := s.GetProxies(true)
	rankingWrapper, _ := s.store.GetNodeWeightRankingCache(s.Name(), s.configName)

	if len(rankingWrapper.Result) > 0 {
		now := time.Now().Unix()
		lastUpdated := rankingWrapper.LastUpdated
		cacheAge := time.Duration(now - lastUpdated) * time.Second

		if cacheAge < 30 * time.Minute {
			rankedNodes := make(map[string]bool, len(rankingWrapper.Result))
			for _, r := range rankingWrapper.Result {
				rankedNodes[r.Name] = true
			}
			hasUnrankedProxy := false
			for _, p := range proxies {
				if !rankedNodes[p.Name()] {
					hasUnrankedProxy = true
					break
				}
			}

			if !hasUnrankedProxy {
				if cacheAge <= 10 * time.Minute {
					return
				}
				proxyMap := make(map[string]C.Proxy, len(proxies))
				for _, p := range proxies {
					proxyMap[p.Name()] = p
				}
				hasDeadRankedNode := false
				for _, r := range rankingWrapper.Result {
					if r.Rank != smart.RankRarelyUsed {
						if p, exists := proxyMap[r.Name]; exists {
							if !p.AliveForTestUrl(s.testUrl) {
								hasDeadRankedNode = true
								break
							}
						}
					}
				}
				if !hasDeadRankedNode {
					return
				}
			}
		}
	}

	log.Debugln("[PLD] Starting node ranking update for policy group [%s]", s.Name())

	rankingWrapper, err := s.store.GetNodeWeightRanking(s.Name(), s.configName, s.testUrl, proxies)
	if err != nil {
		log.Warnln("[PLD] Failed to update node ranking: %v", err)
		return
	}
	if len(rankingWrapper.Result) == 0 {
		log.Debugln("[PLD] Policy group [%s] doesn't have enough data to generate node ranking", s.Name())
		return
	}
	categoryCounts := make(map[string]int)
	for _, rank := range rankingWrapper.Result {
		categoryCounts[rank.Rank]++
	}

	log.Debugln("[PLD] Policy group [%s] node ranking update completed: %d nodes total (%s: %d, %s: %d, %s: %d)",
		s.Name(), len(rankingWrapper.Result),
		smart.RankMostUsed, categoryCounts[smart.RankMostUsed],
		smart.RankOccasional, categoryCounts[smart.RankOccasional],
		smart.RankRarelyUsed, categoryCounts[smart.RankRarelyUsed])
}

func (s *SmartPLD) cleanupOrphanedGroups() {
	allProxies := tunnel.Proxies()
	existingSmartGroups := make(map[string]bool)

	for name, proxy := range allProxies {
		if proxy.Type() == C.Smart || proxy.Type() == C.SmartPLD {
			existingSmartGroups[name] = true
		}
	}

	cachedGroups, err := s.store.GetAllGroupsForConfig(s.configName)
	if err != nil {
		return
	}

	var orphanedGroups []string
	for _, groupName := range cachedGroups {
		if !existingSmartGroups[groupName] {
			orphanedGroups = append(orphanedGroups, groupName)
		}
	}

	if len(orphanedGroups) > 0 {
		for _, group := range orphanedGroups {
			log.Debugln("[PLD] Cleaning up cache data for non-existent policy group [%s]", group)
			err := s.store.FlushByGroup(group, s.configName)
			if err != nil {
				log.Warnln("[PLD] Failed to clean up policy group [%s] cache: %v", group, err)
			}
		}
	}
}

func (s *SmartPLD) cleanupOrphanedNodeCache() {
	proxies := s.GetProxies(true)
	proxyMap := make(map[string]bool, len(proxies))
	for _, proxy := range proxies {
		proxyMap[proxy.Name()] = true
	}

	cachedNodes, err := s.store.GetAllNodesForGroup(s.Name(), s.configName)
	if err != nil {
		return
	}

	var orphanedNodes []string
	for _, nodeName := range cachedNodes {
		if !proxyMap[nodeName] {
			orphanedNodes = append(orphanedNodes, nodeName)
		}
	}

	if len(orphanedNodes) > 0 {
		for _, node := range orphanedNodes {
			log.Debugln("[PLD] Cleaning up cache data for non-existent node [%s]", node)
		}

		err := s.store.RemoveNodesData(s.Name(), s.configName, orphanedNodes)
		if err != nil {
			log.Warnln("[PLD] Failed to clean up non-existent node caches: %v", err)
		}
	}
}

// 获取历史 connectTime
func (s *SmartPLD) getHistoryConnectStats(metadata *C.Metadata, proxy C.Proxy) int64 {
	target := metadata.SmartTarget
	proxyName := proxy.Name()
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)
	return atomicRecord.Get("connectTime").(int64)
}

// 连接持续时间更新
func (s *SmartPLD) updateConnectionDuration(record *smart.AtomicStatsRecord, connectionDuration int64) {
	durationMinutes := float64(connectionDuration) / 60000.0
	currentDuration := record.Get("duration").(float64)

	if currentDuration > 0 {
		record.Set("duration", (currentDuration+durationMinutes)/2.0)
	} else {
		record.Set("duration", durationMinutes)
	}
}

// 记录保存
func (s *SmartPLD) saveStatsRecord(target string, proxy C.Proxy, record *smart.StatsRecord) {
	if data, err := json.Marshal(record); err == nil {
		s.store.AppendToGlobalQueue(smart.StoreOperation{
			Type:   smart.OpSaveStats,
			Group:  s.Name(),
			Config: s.configName,
			Target: target,
			Node:   proxy.Name(),
			Data:   data,
		})
	}
}

func (s *SmartPLD) calcMADMetrics(delays []float64) (currentAnomaly bool, unstable bool, threshold float64, grade int) {
	calcGrade := func(t float64) int {
		if t <= 500 {
			return 1
		} else if t <= 1000 {
			return 2
		} else if t <= 2000 {
			return 3
		}
		return 4
	}

	n := len(delays)
	if n == 0 {
		return false, false, 0, 0
	}

	var median float64
	var mad float64
	var robustCV float64

	const scale = 1.4826
	const defaultK = 2.5
	const smallK = 3.5
	const cvThreshold = 0.6
	const minSamples = 3
	const SentinelThreshold = 0.5
	const sentinel = float64(0xffff)

	recentCount := minSamples
	if n < recentCount {
		recentCount = n
	}
	recentStart := n - recentCount
	recentSentinels := 0

	filtered := make([]float64, 0, n)
	for i, v := range delays {
		if v >= sentinel {
			if i >= recentStart {
				recentSentinels++
			}
			continue
		}
		filtered = append(filtered, v)
	}

	m := len(filtered)
	if m == 0 {
		return true, true, 0, 0
	}

	if float64(recentSentinels) / float64(recentCount) > SentinelThreshold {
		unstable = true
	}

	if m < minSamples {
		last := delays[n - 1]
		currentAnomaly = last >= sentinel
		return currentAnomaly, unstable, 0, 0
	}

	sort.Float64s(filtered)

	if m % 2 == 1 {
		median = filtered[m / 2]
	} else {
		median = (filtered[m / 2 - 1] + filtered[m / 2]) / 2
	}

	devs := make([]float64, 0, m)
	for _, v := range filtered {
		devs = append(devs, math.Abs(v - median))
	}
	sort.Float64s(devs)

	if m % 2 == 1 {
		mad = devs[m / 2]
	} else {
		mad = (devs[m / 2 - 1] + devs[m / 2]) / 2
	}

	if mad == 0 {
		mean := 0.0
		for _, v := range filtered {
			mean += v
		}
		mean /= float64(m)
		varSum := 0.0
		for _, v := range filtered {
			d := v - mean
			varSum += d * d
		}
		std := math.Sqrt(varSum / float64(m))
		threshold = mean + 2 * std
		last := delays[n - 1]
		if last >= sentinel {
			currentAnomaly = true
		} else {
			currentAnomaly = last > threshold && delays[n - 2] > threshold
		}

		return currentAnomaly, unstable, threshold, calcGrade(threshold)
	}

	k := defaultK
	if m < maxSelected {
		k = smallK
	}

	threshold = median + k * scale * mad

	if median > 0 {
		robustCV = scale * mad / median
	} else {
		robustCV = 0
	}

	last := delays[n - 1]
	if last >= sentinel {
		currentAnomaly = true
	} else {
		currentAnomaly = last > threshold && delays[n - 2] > threshold
	}

	if !unstable {
		unstable = robustCV >= cvThreshold
	}

	return currentAnomaly, unstable, threshold, calcGrade(threshold)
}

func (s *SmartPLD) checkNodesStable() {
	proxies := s.GetProxies(true)
	operations := make([]smart.StoreOperation, 0, len(proxies))
	nodesToBlock := make(map[string]*smart.NodeState, len(proxies))
	now := time.Now().Unix()
	blockedUntil := time.Now().Add(checkInterval + 2 * time.Minute).Unix()

	nodeStateData, _ := s.store.GetNodeStates(s.Name(), s.configName)

	for _, p := range proxies {
		if !p.AliveForTestUrl(s.testUrl) {
			continue
		}

		histories := p.DelayHistoryForTestUrl(s.testUrl)
		if len(histories) == 0 {
			continue
		}

		delays := make([]float64, 0, len(histories))
		for _, h := range histories {
			if h.Delay == 0 {
				h.Delay = 0xffff
			}
			delays = append(delays, float64(h.Delay))
		}

		currentAnomaly, unstable, _, newGrade := s.calcMADMetrics(delays)

		proxyName := p.Name()
		var state smart.NodeState
		if data, exists := nodeStateData[proxyName]; exists {
			json.Unmarshal(data, &state)
		}
		prevGrade := state.ThresholdGrade
		state.Name = proxyName
		state.LastChecked = now
		if newGrade > 0 {
			state.ThresholdGrade = newGrade
		}

		gradeDecreased := (prevGrade > 0 && newGrade > 0 && newGrade > prevGrade) || newGrade > 2
		if currentAnomaly || unstable || gradeDecreased {
			state.BlockedUntil = blockedUntil
			blockCopy := state
			nodesToBlock[proxyName] = &blockCopy
		}

		data, err := json.Marshal(&state)
		if err != nil {
			continue
		}
		operations = append(operations, smart.StoreOperation{
			Type:   smart.OpSaveNodeState,
			Group:  s.Name(),
			Config: s.configName,
			Node:   proxyName,
			Data:   data,
		})
	}

	if len(operations) > 0 {
		s.store.AppendToGlobalQueue(operations...)
	}
	if len(nodesToBlock) > 0 {
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToBlock)
	}
}

// 检查节点屏蔽状态
func (s *SmartPLD) checkBlockedNodes() {
	stateData, err := s.store.GetNodeStates(s.Name(), s.configName)
	if err != nil {
		return
	}

	nodesToUpdate := make(map[string]*smart.NodeState)

	for nodeName, data := range stateData {
		var state smart.NodeState
		err := json.Unmarshal(data, &state)
		if err != nil {
			continue
		}

		if state.BlockedUntil > 0 && state.BlockedUntil <= time.Now().Unix() {
			state.BlockedUntil = 0
			nodesToUpdate[nodeName] = &state
			log.Debugln("[PLD] Node [%s] block period expired, unblocking", nodeName)
		}
	}

	if len(nodesToUpdate) > 0 {
		operations := make([]smart.StoreOperation, 0, len(nodesToUpdate))
		for nodeName, state := range nodesToUpdate {
			data, err := json.Marshal(state)
			if err != nil {
				continue
			}
			operations = append(operations, smart.StoreOperation{
				Type:   smart.OpSaveNodeState,
				Group:  s.Name(),
				Config: s.configName,
				Node:   nodeName,
				Data:   data,
			})
		}
		s.store.AppendToGlobalQueue(operations...)
		s.store.UpdateBlockedNodesCache(s.Name(), s.configName, nodesToUpdate)
	}
}

// 单位转换


// 日志记录
func (s *SmartPLD) logConnectionStats(err error, record *smart.StatsRecord, metadata *C.Metadata, baseWeight, priorityFactor float64,
	addressDisplay, proxyName string, connectTime int64, latency int64, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate float64,
	connectionDuration int64, asnNumber string, ModelPredicted bool, lossRate, cumulLossRate float64,
	pldReward float64, pldParts *smart.RewardParts, pldEMA, pldVar float64, pldSamples int64, pldBWCeilKBps float64) {

	var tcpAsnWeight, udpAsnWeight float64

	if asnNumber != "" {
		tcpAsnWeightKey := smart.WeightTypeTCPASN + ":" + asnNumber
		udpAsnWeightKey := smart.WeightTypeUDPASN + ":" + asnNumber
		if record.Weights != nil {
			if w, ok := record.Weights[tcpAsnWeightKey]; ok {
				tcpAsnWeight = w
			}
			if w, ok := record.Weights[udpAsnWeightKey]; ok {
				udpAsnWeight = w
			}
		}
	}

	weightSource := "Traditional"
	if ModelPredicted {
		weightSource = "LightGBM"
	}

	statusStr := "closed"
	if err != nil {
		statusStr = "failed"
	}

	// PLD detail: this connection's scalar reward + physical breakdown and the
	// post-update EWMA state that the D-UCB decision layer reads next time.
	pldDetailStr := "-"
	if pldParts != nil {
		flowStr := "large"
		if pldParts.Failure {
			flowStr = "failed"
		} else if pldParts.SmallFlow {
			flowStr = "small"
		}
		pldDetailStr = fmt.Sprintf("Reward: [%.3f], Flow: [%s], RTT: [%.3f], TTFR: [%.3f], Pair: [%.3f], Thr: [%s], LossPen: [%.3f] -> EMA: (Reward: [%.3f], Var: [%.3f], Samples: [%d], BWCeil: [%s])",
			pldReward, flowStr,
			pldParts.ConnectScore, pldParts.TTFRScore, pldParts.PairScore,
			formatTrafficUnit(pldParts.ThrKBps*1024, true),
			pldParts.LossPenalty,
			pldEMA, pldVar, pldSamples,
			formatTrafficUnit(pldBWCeilKBps*1024, true))
	}

	log.Debugln("[PLD] Connection status: [%s], Updated weights: (Model: [%s], TCP: [%.4f], UDP: [%.4f], TCP ASN: [%.4f], UDP ASN: [%.4f], Base: [%.4f], Priority: [%.2f]) "+
		"For (Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s]) "+
		"- Current: (Connect: [%s], Latency: [%s], LossRate: [%.2f%%], Up: [%s], Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Duration: [%s]) "+
		"- History: (Success: [%d], Failure: [%d], EMA Connect: [%s], EMA Latency: [%s], Cumul LossRate: [%.2f%%], Total Up: [%s], Total Down: [%s], Max Up Speed: [%s], Max Down Speed: [%s], Avg Duration: [%s]) "+
		"- PLD: (%s)",
		statusStr, weightSource, record.Weights[smart.WeightTypeTCP], record.Weights[smart.WeightTypeUDP], tcpAsnWeight, udpAsnWeight, baseWeight, priorityFactor,
		s.Name(), proxyName, metadata.NetWork.String(), addressDisplay,
		formatTimeUnit(float64(connectTime)),
		formatTimeUnit(float64(latency)),
		lossRate*100,
		formatTrafficUnit(uploadTotal*1024*1024, false),
		formatTrafficUnit(downloadTotal*1024*1024, false),
		formatTrafficUnit(maxUploadRate*1024, true),
		formatTrafficUnit(maxDownloadRate*1024, true),
		formatTimeUnit(float64(connectionDuration)),
		record.Success, record.Failure,
		formatTimeUnit(float64(record.ConnectTime)),
		formatTimeUnit(float64(record.Latency)),
		cumulLossRate*100,
		formatTrafficUnit(record.UploadTotal*1024*1024, false),
		formatTrafficUnit(record.DownloadTotal*1024*1024, false),
		formatTrafficUnit(record.MaxUploadRate*1024, true),
		formatTrafficUnit(record.MaxDownloadRate*1024, true),
		formatTimeUnit(record.ConnectionDuration*60000),
		pldDetailStr,
	)
}

// 数据收集
func (s *SmartPLD) collectConnectionData(input *smart.ModelInput, metadata *C.Metadata,
	baseWeight, reward float64, proxyName string, ModelPredicted bool, nodeDelayMs int64, nodeType float64) {

	// 采样率控制
	if s.sampleRate < 1.0 && rand.Float64() > s.sampleRate {
		return
	}

	input.GroupName = s.Name()
	input.NodeName = proxyName
	weightSource := "Traditional"

	if ModelPredicted {
		weightSource = "LightGBM"
	}

	s.dataCollector.AddSample(input, metadata, baseWeight, reward, weightSource, nodeDelayMs, nodeType)
}



func (s *SmartPLD) recordConnectionStats(metadata *C.Metadata, proxy C.Proxy,
	connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate,
	connectionDuration int64, tcpStats *tcpstats.Stats, bytesTo64KMs int64, err error) {

	if proxy.Type() == C.Compatible || proxy.Type() == C.Reject || proxy.Type() == C.Pass || proxy.Type() == C.RejectDrop {
		return
	}

	var lossRate float64
	var cumulLossRate float64
	var calculatedWeight float64
	var ModelPredicted bool

	proxyName := proxy.Name()
	isUDP := metadata.NetWork == C.UDP
	networkStr := metadata.NetWork.String()

	target := metadata.SmartTarget
	wildcardTarget := metadata.WildcardTarget
	cacheKey := smart.FormatDBKey(smart.KeyTypeStats, s.configName, s.Name(), target, proxyName)
	asnNumber := s.getASNCode(metadata)
	priorityFactor := s.getPriorityFactor(proxyName)

	asnDisplay := "unknown"
	if asnNumber != "" {
		asnDisplay = asnNumber
	}
	var addressDisplay string
	if metadata.Host != "" {
		addressDisplay = fmt.Sprintf("Host: [%s] - Target: [%s] - WildcardTarget: [%s] - ASN: [%s]", metadata.Host, target, wildcardTarget, asnDisplay)
	} else {
		addressDisplay = fmt.Sprintf("IP: [%s] - Target: [%s] - ASN: [%s]", metadata.DstIP.String(), target, asnDisplay)
	}

	weightType := smart.WeightTypeTCP
	if asnNumber != "" {
		if isUDP {
			weightType = smart.WeightTypeUDPASN + ":" + asnNumber
		} else {
			weightType = smart.WeightTypeTCPASN + ":" + asnNumber
		}
	} else if isUDP {
		weightType = smart.WeightTypeUDP
	}

	lock := smart.GetTargetNodeLock(target, s.Name(), proxyName)
	lock.Lock()
	defer lock.Unlock()

	atomicRecord := s.store.GetOrCreateAtomicRecord(cacheKey, s.Name(), s.configName, target, proxyName)

	switch {
	case err != nil:
		s.onDialFailed(proxy.Type(), err, s.healthCheck)
		atomicRecord.Add("failure", int64(1))
	default:
		s.onDialSuccess()
		atomicRecord.Add("success", int64(1))
	}

	if connectTime > 0 {
		oldConnectTime := atomicRecord.Get("connectTime").(int64)
		newConnectTime := updateEMAInt(oldConnectTime, connectTime)
		atomicRecord.Set("connectTime", newConnectTime)
	}

	if latency > 0 {
		oldLatency := atomicRecord.Get("latency").(int64)
		newLatency := updateEMAInt(oldLatency, latency)
		atomicRecord.Set("latency", newLatency)
	}

	if connectionDuration > 0 {
		s.updateConnectionDuration(atomicRecord, connectionDuration)
	}

	if tcpStats != nil {
		atomicRecord.Add("cumulSent", int64(tcpStats.TotalSent()))
		atomicRecord.Add("cumulRetrans", int64(tcpStats.TotalRetrans()))
		lossRate = tcpStats.LossRate()
	}

	if sent := atomicRecord.Get("cumulSent").(int64); sent > 0 {
		cumulLossRate = float64(atomicRecord.Get("cumulRetrans").(int64)) / float64(sent)
		if cumulLossRate > 1 {
			cumulLossRate = 1
		}
	}

	emaLossRate := atomicRecord.Get("lossRate").(float64)
	emaLossRate = updateEMAFloat(emaLossRate, lossRate)
	atomicRecord.Set("lossRate", emaLossRate)

	oldWeight := atomicRecord.GetWeight(weightType)
	uploadTotalMB := float64(uploadTotal) / (1024.0 * 1024.0)
	downloadTotalMB := float64(downloadTotal) / (1024.0 * 1024.0)
	maxUploadRateKB := float64(maxUploadRate) / 1024.0
	maxDownloadRateKB := float64(maxDownloadRate) / 1024.0

	atomicRecord.Add("uploadTotal", uploadTotalMB)
	atomicRecord.Add("downloadTotal", downloadTotalMB)

	oldMaxUploadRate := atomicRecord.Get("maxUploadRate").(float64)
	if maxUploadRateKB > oldMaxUploadRate {
		atomicRecord.Set("maxUploadRate", maxUploadRateKB)
	}

	oldMaxDownloadRate := atomicRecord.Get("maxDownloadRate").(float64)
	if maxDownloadRateKB > oldMaxDownloadRate {
		atomicRecord.Set("maxDownloadRate", maxDownloadRateKB)
	}

	input := lightgbm.CreateModelInputFromStatsRecord(
		atomicRecord, metadata,
		uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, float64(connectionDuration) / 60000.0, wildcardTarget,
		lossRate, cumulLossRate,
	)
	input.ConnectionFailed = err != nil

	// smart-pld never uses the official LightGBM weight model: the weight
	// chain (rule-based CalculateWeight + EMA feedback) only feeds the shared
	// candidate-pool / ranking caches; PLD's own D-UCB makes the decision.
	calculatedWeight, ModelPredicted = smart.CalculateWeight(input, priorityFactor)

	// 额外检查和权重调整
	// 不再进行强制权重调整，仅在异常时对特定域名屏蔽节点，防止优秀节点被整个 target 完全屏蔽
	adjWeight, isDegraded, checked, blockCode := s.checkNodeQuality(
		err, metadata, proxy, wildcardTarget,
		addressDisplay, proxyName, calculatedWeight, oldWeight,
		connectionDuration, uploadTotalMB, downloadTotalMB,
		networkStr, asnNumber, isUDP, lossRate, emaLossRate)

	// 针对具体 域名/IP 屏蔽节点（wildcardTarget + SmartTarget 双级记录）
	failedBlock := s.markNodeFailure(metadata, proxyName, isDegraded, checked, blockCode)
	
	if isDegraded || failedBlock {
		s.closeSameConnection(metadata, proxyName, target, asnNumber, true)
	}

	// 平均权重(适应 target 调整为 rule based 和 asn based 的情况)
	newWeight := updateEMAFloat(oldWeight, adjWeight)
	atomicRecord.Set("lastUsed", time.Now().Unix())
	atomicRecord.SetWeight(weightType, newWeight, isUDP)

	// PLD feedback: derive a scalar reward from the physical metrics of this
	// connection and fold it into the per-node EWMA reward. This is what the
	// D-UCB decision layer reads at selection time. The reward parts collected
	// here are surfaced in the [PLD] connection log for side-by-side comparison
	// with the official smart group's weight trail.
	var pldReward float64
	var pldParts *smart.RewardParts
	var pldEMA, pldVar float64
	var pldSamples int64
	var pldBWCeilKBps float64
	if s.usePLD {
		// PLD reward state lives under its own "pld:" key namespace — the
		// official smart implementation never reads or writes it.
		rec := s.getPLDRecord(target, proxyName)
		rm := smart.RewardedMetrics{
			IsUDP:         isUDP,
			ConnectTimeMs: connectTime,
			FirstByteMs:   latency, // firstReadLatency == TTFR
			BytesTo64KMs:  bytesTo64KMs,
			DownloadMB:    downloadTotalMB,
			DurationSec:   float64(connectionDuration) / 1000.0,
			MaxDownloadKB: maxDownloadRateKB,
			LossRate:      lossRate,
			Failed:        err != nil,
			FailureCount:  rec.SampleCount,
		}
		r, parts := smart.ComputeRewardDetail(rm)
		newR, newVar, newCnt := smart.UpdateRewardEMA(rec.Reward, rec.RewardVar, rec.SampleCount, r)
		rec.Reward = newR
		rec.RewardVar = newVar
		rec.SampleCount = newCnt
		if !rm.Failed && bytesTo64KMs > 0 {
			// keep bandwidth ceiling useful only on successful transfers
			rec.BWCeilingKBps = 64.0 / (float64(bytesTo64KMs)/1000.0)
		}
		s.savePLDRecord(target, proxyName, rec)
		pldReward = r
		pldParts = &parts
		pldEMA = newR
		pldVar = newVar
		pldSamples = newCnt
		pldBWCeilKBps = rec.BWCeilingKBps
	}

	statsSnapshot := atomicRecord.CreateStatsSnapshot(cacheKey)
	s.saveStatsRecord(target, proxy, statsSnapshot)

	if s.pldCollect {
		collectedWeight := adjWeight / priorityFactor
		if isDegraded || failedBlock {
			// 对于异常连接强制调整，便于模型训练时进行识别
			if collectedWeight >= smart.AllowedWeight {
				collectedWeight = collectedWeight * 0.1
			} else {
				if collectedWeight == 0 {
					collectedWeight = smart.AllowedWeight * rand.Float64()
				}
			}
		}
		reward := s.getPLDRecord(target, proxyName).Reward
		nodeDelayMs := int64(proxy.LastDelayForTestUrl(s.testUrl))
		nodeType := float64(proxy.Type())
		s.collectConnectionData(input, metadata, collectedWeight, reward, proxyName, ModelPredicted, nodeDelayMs, nodeType)
	}

	s.logConnectionStats(err, statsSnapshot, metadata, calculatedWeight / priorityFactor, priorityFactor, addressDisplay, proxyName,
		connectTime, latency, uploadTotalMB, downloadTotalMB, maxUploadRateKB, maxDownloadRateKB, connectionDuration, asnNumber, ModelPredicted, lossRate, cumulLossRate,
		pldReward, pldParts, pldEMA, pldVar, pldSamples, pldBWCeilKBps)
}

func (s *SmartPLD) registerClosureMetricsCallback(c C.Conn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, firstReadLatency *atomic.Int64, firstReadErr *atomic.TypedValue[error], firstWriteErr *atomic.TypedValue[error], pp *callback.PacketPairCallBackConn) C.Conn {
	return callback.NewCloseCallbackConn(c, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			latency := firstReadLatency.Load()
			readErr := firstReadErr.Load()
			writeErr := firstWriteErr.Load()

			// Packet-Pair readout (passive). BytesTo64KMs drives the small-flow
			// bandwidth-ceiling branch; 0 when the flow never crossed 64KiB.
			var bytesTo64KMs int64
			if pp != nil {
				if ro := pp.Readout(); ro.BytesTo64KMs > 0 {
					bytesTo64KMs = ro.BytesTo64KMs
				}
			}

			var tcpStats *tcpstats.Stats
			if trackerConn, ok := tracker.(net.Conn); ok {
				tcpStats = tcpstats.GetTCPStats(trackerConn)
			}

			var closeErr error
			if readErr != nil {
				if readErr == io.EOF {
					if writeErr != nil && writeErr != io.EOF {
						closeErr = writeErr
					}
				} else {
					closeErr = readErr
				}
			}

			if closeErr != nil {
				s.markNodeFailure(metadata, proxy.Name(), true, true, 3)
			}

			go s.recordConnectionStats(metadata, proxy, connectTime, latency, uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, tcpStats, bytesTo64KMs, closeErr)
			return
		}
	})
}

func (s *SmartPLD) registerPacketClosureMetricsCallback(pc C.PacketConn, proxy C.Proxy, metadata *C.Metadata, connectTime int64, udpLatency *atomic.Int64) C.PacketConn {
	return callback.NewCloseCallbackPacketConn(pc, func() {
		tracker := statistic.DefaultManager.Get(metadata.UUID)
		if tracker != nil {
			info := tracker.Info()
			uploadTotal := info.UploadTotal.Load()
			downloadTotal := info.DownloadTotal.Load()
			connectionDuration := time.Since(info.Start).Milliseconds()
			maxUploadRate := info.MaxUploadRate.Load()
			maxDownloadRate := info.MaxDownloadRate.Load()

			go s.recordConnectionStats(metadata, proxy, connectTime, udpLatency.Load(),
				uploadTotal, downloadTotal, maxUploadRate, maxDownloadRate, connectionDuration, nil, 0, nil)
			return
		}
	})
}

func (s *SmartPLD) checkNodeQuality(
	err error, metadata *C.Metadata, proxy C.Proxy, wildcardTarget string,
	addressDisplay, proxyName string,
	newWeight, oldWeight float64,
	connectionDuration int64, uploadTotal, downloadTotal float64,
	networkType string, asnNumber string, isUDP bool, lossRate, emaLossRate float64) (float64, bool, bool, int64) {

	if s.selected != "" {
		return newWeight, false, false, 0
	}

	now := time.Now().Unix()

	// 用户手动屏蔽
	if metadata.SmartBlock == "blocked" {
		log.Debugln("[PLD] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected manual block...",
			s.Name(), proxyName, networkType, addressDisplay)
		return newWeight, true, true, 1
	}

	// 强制关闭的连接，跳过质量检查避免误降级
	if metadata.SmartBlock == "degraded" {
		return oldWeight, false, false, 0
	}

	_, wtLastCheck, wtLastFailure, wtBlocked := s.store.GetHostStatus(s.Name(), s.configName, wildcardTarget)

	if wtBlocked {
		return newWeight, false, false, 0
	}

	if newWeight > 0 && newWeight < smart.AllowedWeight {
		return newWeight, true, true, 5
	}

	if err != nil {
		return newWeight, false, true, 3
	}

	// 零流量连接
	if connectionDuration > 100 && downloadTotal == 0 && uploadTotal == 0 && metadata.DstPort == 443 && !isUDP {
		log.Debugln("[PLD] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected zero-traffic...",
			s.Name(), proxyName, networkType, addressDisplay)
		return newWeight, true, true, 4
	}

	// 异常状态码检测
	if downloadTotal < 0.03 && metadata.Host != "" && metadata.DstPort == 443 && !isUDP && metadata.Type != C.INNER {
		var failure bool
		var checked bool
		if now - wtLastCheck > 300 || now - wtLastFailure < 300 {
			checked = true
			status, ok, err := s.StatusTest(proxy, metadata.Host)
			if err == nil {
				failure = !ok
				if failure {
					log.Debugln("[PLD] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected abnormal response [%d]...",
						s.Name(), proxyName, networkType, addressDisplay, status)
				}
			}
		}
		if failure {
			return newWeight, true, checked, 2
		}
		return newWeight, false, checked, 0
	}

	// 高丢包率检测
	if lossRate >= 0.1 || emaLossRate >= 0.05 {
		log.Debugln("[PLD] Connection Group: [%s] - Node: [%s] - Network: [%s] - Address: [%s] detected high packet loss [current: %.2f%%, history EMA: %.2f%%]...",
			s.Name(), proxyName, networkType, addressDisplay, lossRate*100, emaLossRate*100)
		return newWeight, true, true, 6
	}

	return newWeight, false, false, 0
}

func (s *SmartPLD) markNodeFailure(metadata *C.Metadata, proxyName string, isDegraded bool, checked bool, blockCode int64) bool {
	wildcardTarget := metadata.WildcardTarget
	target := metadata.SmartTarget

	failedBlock := s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, metadata, proxyName, s.maxFailedTimes, s.hostFailLimit, isDegraded, checked, blockCode)

	if isDegraded || failedBlock {
		if target != "" && target != wildcardTarget {
			s.store.UpdateHostStatus(s.Name(), s.configName, target, metadata, proxyName, s.maxFailedTimes, s.hostFailLimit, isDegraded, checked, blockCode)
		}
	}

	return failedBlock
}

func (s *SmartPLD) closeSameConnection(metadata *C.Metadata, proxyName, target, asnNumber string, force bool) {
	statistic.DefaultManager.RangeSmartTarget(target, func(id string) bool {
		if id == metadata.UUID {
			return true
		}
		tracker := statistic.DefaultManager.Get(id)
		if tracker == nil {
			return true
		}
		if !lo.Contains(tracker.Chains(), s.Name()) {
			return true
		}
		if force {
			tracker.Info().Metadata.SmartBlock = "degraded"
			_ = tracker.Close()
		} else if proxyName != "" {
			if !lo.Contains(tracker.Chains(), proxyName) {
				_ = tracker.Close()
			}
		}
		return true
	})
}

func (s *SmartPLD) checkHostStatus() {
	proxies := s.GetProxies(false)
	proxyMap := make(map[string]C.Proxy, len(proxies))
	for _, p := range proxies {
		proxyMap[p.Name()] = p
	}

	toCheck, err := s.store.CheckHostStatus(s.Name(), s.configName)
	if err != nil {
		return
	}

	for wildcardTarget, nodeMap := range toCheck {
		for nodeName, host := range nodeMap {
			p, ok := proxyMap[nodeName]
			if !ok {
				continue
			}
			status, okRes, err := s.StatusTest(p, host)
			if err == nil && okRes {
				s.store.UpdateHostStatus(s.Name(), s.configName, wildcardTarget, &C.Metadata{Host: host}, nodeName, s.maxFailedTimes, s.hostFailLimit, false, true, 0)
				log.Debugln("[PLD] Recover Group: [%s] - Node: [%s] for Host: [%s] with HTTP Status: [%d]", s.Name(), nodeName, host, status)
			} else if err == nil {
				log.Debugln("[PLD] Recover Group: [%s] - Node: [%s] for Host: [%s] still abnormal with HTTP Status: [%d]", s.Name(), nodeName, host, status)
			}
		}
	}
}

func (s *SmartPLD) StatusTest(proxy C.Proxy, host string) (uint16, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), C.DefaultTCPTimeout)
	defer cancel()
	url := "https://" + host + "/?z=" + strconv.FormatInt(rand.Int63(), 10)
	return proxy.StatusTest(ctx, url)
}

func (s *SmartPLD) getPriorityFactor(proxyName string) float64 {
	if len(s.policyPriority) == 0 {
		return 1.0
	}
	if v, ok := s.priorityCache.Load(proxyName); ok {
		return v
	}
	factor := 1.0
	for _, rule := range s.policyPriority {
		if rule.isRegex && rule.regex != nil {
			if matched, _ := rule.regex.MatchString(proxyName); matched {
				factor = rule.factor
				break
			}
		} else if strings.Contains(proxyName, rule.pattern) {
			factor = rule.factor
			break
		}
	}
	s.priorityCache.Store(proxyName, factor)
	return factor
}

func (s *SmartPLD) applyMaxFailedTimes() {
	if proxyCount := len(s.GetProxies(true)); proxyCount > 0 {
		hostFailLimit := proxyCount / 3
		if hostFailLimit < 2 {
			hostFailLimit = 2
		}
		s.hostFailLimit = hostFailLimit
	}
}


func (s *SmartPLD) getASNCode(metadata *C.Metadata) string {
	if metadata.DstIPASN == "unknown" {
		return ""
	}

	if metadata.DstIPASN == "" {
		if !s.preferASN {
			return ""
		}
		var ip netip.Addr
		if metadata.Host != "" && !metadata.Resolved() {
			ctx, cancel := context.WithTimeout(context.Background(), resolver.DefaultDNSTimeout)
			defer cancel()
			var err error
			ip, err = resolver.ResolveIP(ctx, metadata.Host)
			if err != nil {
				log.Debugln("[DNS] resolve %s error: %s", metadata.Host, err.Error())
				metadata.DstIPASN = "unknown"
				return ""
			} else {
				log.Debugln("[DNS] %s --> %s", metadata.Host, ip.String())
				if !ip.IsValid() {
					metadata.DstIPASN = "unknown"
					return ""
				}
			}
		} else {
			ip = metadata.DstIP
		}

		asn, aso := mmdb.ASNInstance().LookupASN(ip.AsSlice())
		if asn == "" {
			metadata.DstIPASN = "unknown"
		} else {
			metadata.DstIPASN = asn + " " + aso
		}
		return asn
	}

	if idx := strings.IndexByte(metadata.DstIPASN, ' '); idx >= 0 {
		return metadata.DstIPASN[:idx]
	}
	return metadata.DstIPASN
}

func (s *SmartPLD) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	if !flushQueueOnce.Swap(true) {
		s.store.FlushQueue(true)
	}

	lightgbm.CloseAllPLDCollectors()

	smartInitOnce = sync.Once{}

	return nil
}

// getPLDRecord returns the cached (or freshly loaded) per-(target,node) PLD
// reward record. It never touches the official smart stats: PLD data lives
// under the independent "pld:" key namespace of the shared smart.db.
func (s *SmartPLD) getPLDRecord(target, node string) *smart.PLDRecord {
	key := smart.FormatPLDKey(s.configName, s.Name(), target, node)
	s.pldRecordMu.Lock()
	defer s.pldRecordMu.Unlock()
	if rec, ok := s.pldRecordCache[key]; ok {
		return rec
	}
	rec := &smart.PLDRecord{}
	if data, err := s.store.DBViewGetItem(key); err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, rec); err != nil {
			log.Warnln("[PLD] record decode failed for key %s: %v", key, err)
		}
	}
	s.pldRecordCache[key] = rec
	return rec
}

// savePLDRecord persists the PLD reward record to its independent key
// namespace and updates the in-memory cache.
func (s *SmartPLD) savePLDRecord(target, node string, rec *smart.PLDRecord) {
	key := smart.FormatPLDKey(s.configName, s.Name(), target, node)
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := s.store.DBBatchPutItem(key, data); err != nil {
		log.Warnln("[PLD] record save failed for key %s: %v", key, err)
	}
}

// applyPolicyPriorityPLD parses the smart-pld policy-priority option. It is the
// PLD-owned twin of the official applyPolicyPriority (which takes *Smart); the
// official one stays untouched in smart.go.
func applyPolicyPriorityPLD(s *SmartPLD, policyPriority string) {
	lastUnescapedColon := func(str string) int {
		for i := len(str) - 1; i >= 0; i-- {
			if str[i] == ':' {
				bs := 0
				j := i - 1
				for j >= 0 && str[j] == '\\' {
					bs++
					j--
				}
				if bs%2 == 0 {
					return i
				}
			}
		}
		return -1
	}

	unescapePattern := func(p string) string {
		var b strings.Builder
		for i := 0; i < len(p); i++ {
			if p[i] == '\\' && i+1 < len(p) {
				b.WriteByte(p[i+1])
				i++
			} else {
				b.WriteByte(p[i])
			}
		}
		return b.String()
	}

	pairs := strings.Split(policyPriority, ";")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		idx := lastUnescapedColon(pair)
		if idx <= 0 || idx == len(pair)-1 {
			log.Warnln("[PLD] Invalid policy-priority rule: [%s], must be in 'pattern:factor' format and factor is required", pair)
			continue
		}

		patternRaw := strings.TrimSpace(pair[:idx])
		factorStr := strings.TrimSpace(pair[idx+1:])

		factor, err := strconv.ParseFloat(factorStr, 64)
		if err != nil {
			log.Warnln("[PLD] Invalid priority factor format for pattern [%s:%v]", patternRaw, err)
			continue
		}
		if factor <= 0 {
			log.Warnln("[PLD] Invalid priority factor [%.2f] for pattern [%s], factor must be positive", factor, patternRaw)
			continue
		}

		rule := priorityRule{
			pattern: unescapePattern(patternRaw),
			factor:  factor,
		}

		if re, err := regexp2.Compile(rule.pattern, regexp2.None); err == nil {
			rule.regex = re
			rule.isRegex = true
		}

		s.policyPriority = append(s.policyPriority, rule)
	}
}
