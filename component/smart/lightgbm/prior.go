package lightgbm

import (
	"math"
	"os"
	"sync"
	"time"

	"github.com/vernesong/leaves"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

// RequestFeatures carries only request-side signals available at selection time.
// It deliberately excludes per-node history (Success/Failure/Latency/...) so that
// a completely fresh request (first YouTube open, cold node pool) still yields a
// meaningful prior per candidate node.
type RequestFeatures struct {
	// ASN of the destination (numeric string from mmdb), "" if unknown.
	DestASN string
	// Protocol: true when this is a QUIC/UDP flow.
	IsUDP bool
	// Hour of day 0-23 (local ring), encoded twice for cyclic continuity.
	HourSin float64
	HourCos float64
	// Late-evening peak flag (20:00-24:00) — a crude demand proxy.
	IsPeak float64
	// Destination domain category (reuse existing keyword extractor output).
	DomainFeature float64
	// Destination geo/continent feature (reuse existing extractor output).
	GeoFeature float64
	// Destination port feature (reuse existing extractor output).
	PortFeature float64
	// Traffic class heuristic derived from port/protocol/target: 0=web,1=stream,
	// 2=transfer,3=interactive. Independent of bytes seen so far.
	TrafficClass float64
	// FNV hashes for high-cardinality side channels (host, ip, asn).
	HostHash     float64
	IPHash       float64
	ASNHash      float64

	// --- 节点实时/语义特征（feature 15-19，占用原 reserved 位） ---
	// NodeDelayMs: 节点最近一次 url-test 健康检查延迟（0=未知），log1p 后在
	// preparePriorFeatures 中缩放。弥补 reward 反馈滞后的"此刻快不快"信号。
	NodeDelayMs float64
	// NodeRewardVar: 节点 reward 的 EWMA 方差（内核 AtomicStatsRecord.RewardVar），
	// 表示该节点历史表现的稳定性/不确定性（UCB 语义）。
	NodeRewardVar float64
	// TargetHash: 归一化目标（SmartTarget, wildcard 域名/IP）哈希，让模型直接学
	// 站点集群偏好，而不是靠 host_hash 近似。
	TargetHash float64
	// NodeType: 节点协议类型编码（AdapterType 枚举数值），让模型学协议结构差异
	// （tuic/naive/trojan 抗 QoS 性能不同等）。
	NodeType float64
	// GroupHash: 策略组名哈希，模型全局共享时学"不同组偏好不同"。
	GroupHash float64
	// DomainTrafficLevel: 域名流量档位（feature 20，0-3）。
	// 由内置 CDN 子域表 + 该 target 实测画像得出（见 traffic_profile.go），
	// 表示"本次连接预计是大流量（4K 视频）还是小流量（刷网页）"——
	// 让 prior 能学"大流量档位 → 高吞吐节点 / 小流量档位 → 低延迟节点"。
	DomainTrafficLevel float64
}

// PriorFeatureSize is the fixed feature width the prior model is trained against.
// Keep in lockstep with the offline training pipeline (30 features incl. these).
const PriorFeatureSize = 30

// PriorScore holds one node's predicted base score for a given request.
type PriorScore struct {
	Node  string
	Score float64 // in (0,1], higher = more likely to satisfy this request
	Used  bool    // false when model/path unavailable and prior fell back to prior_flat
}

// PriorModel is the request-feature → per-node prior-score predictor. It reuses
// the same leaves ensemble infrastructure as WeightModel but runs a separate
// inference over RequestFeatures (+ node-side auxiliary features), NOT the
// post-close stats-heavy feature vector.
type PriorModel struct {
	model      *leaves.Ensemble
	transforms *FeatureTransforms
	lastUpdate time.Time
	mutex      sync.RWMutex
}

var (
	priorModel     *PriorModel
	priorModelOnce sync.Once
)

// flatPrior is the degenerate prior used when no model is loaded: every node gets
// the same small base, so D-UCB degenerates to pure online reward + exploration
// (zero-failure cold start requirement).
const flatPrior = 0.5

// GetPriorModel loads (once) the prior model from PriorModel.bin — a dedicated
// file separate from the official weight model's Model.bin. The two models use
// different feature orders (PLD request-side vs weight post-close stats), so
// sharing one file would corrupt one of them.
func GetPriorModel() *PriorModel {
	priorModelOnce.Do(func() {
		m := &PriorModel{}
		modelPath := C.Path.SmartPriorModel()
		if _, err := os.Stat(modelPath); err == nil {
			if err := m.loadModel(modelPath); err != nil {
				log.Warnln("[PLD] prior model load failed, using flat prior: %v", err)
			} else {
				log.Infoln("[PLD] prior model loaded successfully")
			}
		} else {
			log.Infoln("[PLD] no model file, using flat prior (online-only D-UCB)")
		}
		priorModel = m
	})
	return priorModel
}

func (m *PriorModel) loadModel(path string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	model, err := leaves.LGEnsembleFromFile(path, false)
	if err != nil {
		return err
	}

	// Parse the [transforms] section the PLD trainer appends (e.g. RobustScaler
	// on node_sample_count) and apply it at inference time, exactly like
	// WeightModel does. The PLD feature vector is also PriorFeatureSize=30 wide,
	// so ValidateTransforms(MaxFeatureSize) and the pool-backed ApplyTransforms
	// are shared as-is; feature-name compatibility vs WeightModel is NOT checked
	// here because the two models use different feature orders by design.
	transforms, err := LoadTransformsFromModel(path)
	if err != nil {
		log.Warnln("[PLD] failed to load transforms parameters: %v, using no transforms", err)
		transforms = &FeatureTransforms{TransformsEnabled: false}
	} else if transforms.TransformsEnabled {
		if err := transforms.ValidateTransforms(MaxFeatureSize); err != nil {
			log.Warnln("[PLD] ValidateTransforms failed: %v, disabling transforms", err)
			transforms.TransformsEnabled = false
		} else {
			transforms.DebugTransforms()
		}
	}

	m.transforms = transforms
	m.model = model
	m.lastUpdate = time.Now()
	return nil
}

// PredictPrior computes the prior score for one candidate node on this request.
// nodeStats (may be nil) supplies the node's own EWMA reward + sample count as
// auxiliary inputs so the model can blend "what this node historically did" with
// "what this request needs". rewardVar/delayMs/nodeType are the node's live
// state (reward variance, last url-test delay, protocol type) feeding features
// 15/16/18. Returns Used=false and flatPrior when no model.
func (m *PriorModel) PredictPrior(features RequestFeatures, nodeName string, rewardEMA, sampleCount, rewardVar, delayMs, nodeType float64) (float64, bool) {
	if m == nil {
		return flatPrior, false
	}
	m.mutex.RLock()
	model := m.model
	transforms := m.transforms
	m.mutex.RUnlock()
	if model == nil {
		return flatPrior, false
	}

	vec := preparePriorFeatures(features, nodeName, rewardEMA, sampleCount, rewardVar, delayMs, nodeType)
	if len(vec) == 0 {
		return flatPrior, false
	}

	// Apply the model's [transforms] section (train_pld.py appends a
	// RobustScaler for node_sample_count) so inference matches training.
	if transforms != nil && transforms.TransformsEnabled {
		vec = transforms.ApplyTransforms(vec)
	}

	// Wrap with a 12-feature ontology: pseudo softmax/normalization happens in
	// the Go layer, the model stays a point regressor.
	raw := float64(0)
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorln("[PLD] prior model predict panic: %v", r)
				raw = 0
			}
		}()
		raw = model.PredictSingle(vec, 0)
	}()

	if math.IsNaN(raw) || raw <= 0 {
		return flatPrior, false
	}
	// squash to (0,1] for a stable bounded prior
	score := 1.0 / (1.0 + math.Exp(-raw))
	if score > 1 {
		score = 1
	}
	if score < 0.05 {
		score = 0.05
	}
	return score, true
}

// PriorBatch computes priors for a slice of candidate nodes in one call.
func (m *PriorModel) PriorBatch(features RequestFeatures, nodes []string, rewards map[string]float64, counts map[string]float64) []PriorScore {
	out := make([]PriorScore, 0, len(nodes))
	for _, name := range nodes {
		s, used := m.PredictPrior(features, name, rewards[name], counts[name], 0, 0, 0)
		out = append(out, PriorScore{Node: name, Score: s, Used: used})
	}
	return out
}

// preparePriorFeatures assembles the fixed-width prior feature vector.
// Layout: 12 request-feature + 3 node-aux + 5 node-live/semantic + 10 reserved,
// total PriorFeatureSize=30. Everything is request-scoped except node-aux.
func preparePriorFeatures(f RequestFeatures, nodeName string, rewardEMA, sampleCount, rewardVar, delayMs, nodeType float64) []float64 {
	// (0) ASN
	asnCat := extractASNFeature(f.DestASN)
	// (1) protocol
	proto := boolToFloat(f.IsUDP)
	// (2)(3) hour cyclic
	// (4) peak
	// (5) domain category
	// (6) geo category
	// (7) port category
	// (8) traffic class
	// (9)(10)(11) hashes
	// (12) node reward EMA (clamped)
	// (13) node sample count (log1p)
	// (14) node hash
	// (15)-(19) reserved
	// (20)-(29) reserved
	rew := rewardEMA
	if rew < 0 {
		rew = 0
	}
	if rew > 1 {
		rew = 1
	}
	// node_reward_var → clamp [0,2] → /2（EWMA 方差量级）
	rv := rewardVar
	if rv < 0 {
		rv = 0
	}
	if rv > 2 {
		rv = 2
	}
	// node_delay → log1p(delayMs/100)（200ms→0.69, 1s→1.39, 3s→2.09）
	var dl float64
	if delayMs > 0 {
		dl = math.Log1p(delayMs / 100.0)
	}

	features := []float64{
		float64(asnCat),
		proto,
		f.HourSin,
		f.HourCos,
		f.IsPeak,
		f.DomainFeature,
		f.GeoFeature,
		f.PortFeature,
		f.TrafficClass,
		hashStringToFloat(f.DestASN, 500),
		f.HostHash,
		f.IPHash,
		rew,
		math.Log1p(sampleCount),
		hashStringToFloat(nodeName, 1000),
		dl,              // 15: node_delay（实时健康检查延迟）
		rv / 2.0,        // 16: node_reward_var（不确定性）
		f.TargetHash,    // 17: target_hash（站点集群）
		nodeType,        // 18: node_type（协议编码）
		f.GroupHash,     // 19: group_hash（策略组）
		f.DomainTrafficLevel, // 20: 域名流量档位（0-3）
		0, 0, 0, 0, 0, 0, 0, 0, 0, // 21-29 reserved
	}
	if len(features) > PriorFeatureSize {
		features = features[:PriorFeatureSize]
	}
	for len(features) < PriorFeatureSize {
		features = append(features, 0)
	}
	return features
}

// ExtractHourCyclic returns sin/cos encodings of local hour for cyclic continuity
// (0:00 and 23:00 must be adjacent in feature space).
func ExtractHourCyclic(now time.Time) (sin, cos float64) {
	hour := float64(now.Hour()) + float64(now.Minute())/60.0
	ang := hour / 24.0 * 2.0 * math.Pi
	return math.Sin(ang), math.Cos(ang)
}

// ClassifyTrafficFromMetadata is a cheap heuristic over request signals only.
// It feeds the model's traffic-class prior; independent of observed bytes.
func ClassifyTrafficFromMetadata(isUDP bool, port uint16, portFeature float64) int {
	switch {
	case isUDP:
		// QUIC media: treat large-QUIC targets (443) as stream-ish but keep
		// interactive class low for latency-critical game UDP.
		if portFeature >= 30 && portFeature < 35 { // game ports region
			return 3
		}
		return 1
	case port == 443:
		return 0 // default web/TLS
	case port <= 1024 && portFeature < 20:
		return 2 // established service
	default:
		return 0
	}
}
// BuildRequestFeatures assembles the request-side feature vector from a
// metadata at selection time. All fields are request-scoped; no node history is
// consulted so a cold request still yields a usable prior. groupName feeds the
// group_hash feature (feature 19) so the shared model can learn per-group
// preferences; target_hash (feature 17) comes from metadata.SmartTarget.
func BuildRequestFeatures(metadata *C.Metadata, groupName string, now time.Time) RequestFeatures {
	hourSin, hourCos := ExtractHourCyclic(now)
	hour := float64(now.Hour())
	isPeak := 0.0
	if hour >= 20 || hour < 1 {
		isPeak = 1.0
	}

	domainFeature := extractDomainTypeFeature(metadata.Host)
	if metadata.Host == "" {
		domainFeature = extractIPFeature(metadata.DstIP.String())
	}
	portFeature := extractPortFeature(metadata.DstPort)

	target := metadata.SmartTarget
	if target == "" {
		target = metadata.WildcardTarget
	}

	return RequestFeatures{
		DestASN:           metadata.DstIPASN,
		IsUDP:             metadata.NetWork == C.UDP,
		HourSin:           hourSin,
		HourCos:           hourCos,
		IsPeak:            isPeak,
		DomainFeature:     float64(domainFeature),
		GeoFeature:        float64(extractGeoIPFeature(metadata.DstGeoIP)),
		PortFeature:       float64(portFeature),
		TrafficClass:      float64(ClassifyTrafficFromMetadata(metadata.NetWork == C.UDP, metadata.DstPort, float64(portFeature))),
		HostHash:          hashStringToFloat(metadata.Host, 1000),
		IPHash:            hashStringToFloat(metadata.DstIP.String(), 10000),
		ASNHash:           hashStringToFloat(metadata.DstIPASN, 500),
		TargetHash:        hashStringToFloat(target, 1000),
		GroupHash:         hashStringToFloat(groupName, 200),
		DomainTrafficLevel: float64(LookupDomainTrafficLevel(metadata.Host, target)),
	}
}
