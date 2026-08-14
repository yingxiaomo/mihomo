package lightgbm

import (
	"strings"
	"sync"

	"github.com/metacubex/mihomo/common/lru"
)

// 域名流量档位 —— 选路时刻可用的事前信号（feature 20）：
//
//	0 = unknown    未知（无内置表命中、无实测画像）
//	1 = small      小流量：刷网页 / API / 即时通讯（低延迟优先）
//	2 = medium     中流量：图文页 / 缩略图流 / 音频 / 混合 CDN
//	3 = heavy      大流量：4K/1080p 视频流 / 大文件下载（吞吐优先）
//
// 与训练侧 train_pld.py 的 domain_traffic_level 构造保持一致（阈值同源）。
const (
	TrafficLevelUnknown = 0
	TrafficLevelSmall   = 1
	TrafficLevelMedium  = 2
	TrafficLevelHeavy   = 3
)

// 档位阈值（与 ComputeReward.SmallTransferKB=512KiB 语义对齐）。
const (
	// SmallTransferBytes：平均单次下载 < 512 KiB → small。
	SmallTransferBytes = 512 * 1024
	// HeavyTransferBytes：平均单次下载 >= 4 MiB → heavy。
	HeavyTransferBytes = 4 * 1024 * 1024
	// HeavyPeakKBps：历史峰值速率 >= 8 MB/s 也可判 heavy。
	HeavyPeakKBps = 8192.0
)

// builtinTrafficTable 内置「域名后缀 → 档位」表。只收录高置信条目：
//   - 明确承载视频流的 CDN 子域 → heavy（纯视频，无图片/页面混合）
//   - 混合型 CDN（图片+视频）→ medium，避免把图片流量误判成大流量
//   - 应用壳域名（youtube.com / netflix.com 本身）刻意不在此列：它们只是
//     API/页面，流量反而偏小，应靠实测画像或默认档位兜底 —— 这是与旧
//     streamingKeywords（把 youtube.com 标成 stream）最大的行为差异。
var builtinTrafficTable = []struct {
	suffix string
	level  int
}{
	// --- heavy：纯视频流域名 ---
	{".googlevideo.com", TrafficLevelHeavy}, // YouTube 视频流
	{".gvt1.com", TrafficLevelHeavy},        // YouTube 视频边缘节点
	{".bilivideo.com", TrafficLevelHeavy},   // B 站视频流
	{".nflxvideo.net", TrafficLevelHeavy},   // Netflix 视频流
	{".hulustream.com", TrafficLevelHeavy},  // Hulu 视频流
	{".ttvnw.net", TrafficLevelHeavy},       // Twitch 视频（hls/vod）
	{".vhcdn.net", TrafficLevelHeavy},       // 虎牙直播流
	{".hls.ttvnw.net", TrafficLevelHeavy},   // Twitch HLS（显式，避免歧义）

	// --- medium：混合型 / 音频 / 图片 CDN ---
	{".akamaized.net", TrafficLevelMedium},  // 混合（Netflix 视频 + 网站图片）
	{".bmcdn.net", TrafficLevelMedium},      // B 站 CDN（视频+图片混合）
	{".cloudfront.net", TrafficLevelMedium}, // AWS 混合 CDN
	{".scdn.co", TrafficLevelMedium},        // Spotify 音频
	{".tiktokcdn.com", TrafficLevelMedium},  // TikTok 混合
	{".bytecdn.cn", TrafficLevelMedium},     // 抖音混合
	{".gtimg.com", TrafficLevelMedium},      // 腾讯混合 CDN
	{".iqiyipic.com", TrafficLevelMedium},   // 爱奇艺图片
	{".ykimg.com", TrafficLevelMedium},      // 优酷图片
}

var (
	trafficProfileCache *lru.LruCache[string, int]
	trafficProfileOnce  sync.Once
)

// trafficProfileCacheSize 画像 LRU 上限，超出按最近使用淘汰。
const trafficProfileCacheSize = 2000

func getTrafficProfileCache() *lru.LruCache[string, int] {
	trafficProfileOnce.Do(func() {
		trafficProfileCache = lru.New[string, int](lru.WithSize[string, int](trafficProfileCacheSize))
	})
	return trafficProfileCache
}

// UpdateTargetTrafficLevel 用该 target 的累计统计刷新画像缓存（L2 在线画像）。
// 由 computePLDWeights 在选路热路径上调用：数据（downloadTotal / sampleCount /
// maxDownloadRate）已经从 stats 记录读出，聚合成本为零，不需要额外扫描。
func UpdateTargetTrafficLevel(target string, downloadTotalMB, maxDownloadRateKB float64, sampleCount int64) {
	if target == "" || sampleCount <= 0 {
		return
	}
	avgBytes := downloadTotalMB * 1024 * 1024 / float64(sampleCount)
	level := TrafficLevelSmall
	switch {
	case avgBytes >= HeavyTransferBytes || maxDownloadRateKB >= HeavyPeakKBps:
		level = TrafficLevelHeavy
	case avgBytes >= SmallTransferBytes:
		level = TrafficLevelMedium
	}
	getTrafficProfileCache().Set(target, level)
}

// LookupDomainTrafficLevel 选路时刻查询档位。优先级：
//  1. 内置表（强语义，视频 CDN 子域）
//  2. 实测画像缓存（该 target 历史平均单次下载量）
//  3. unknown（端口启发式 TrafficClass 继续兜底）
func LookupDomainTrafficLevel(host, target string) int {
	if level := lookupBuiltinTrafficLevel(host); level != TrafficLevelUnknown {
		return level
	}
	if target != "" {
		if v, ok := getTrafficProfileCache().Get(target); ok {
			return v
		}
	}
	return TrafficLevelUnknown
}

func lookupBuiltinTrafficLevel(host string) int {
	if host == "" {
		return TrafficLevelUnknown
	}
	h := strings.ToLower(host)
	for _, e := range builtinTrafficTable {
		if strings.HasSuffix(h, e.suffix) {
			return e.level
		}
	}
	return TrafficLevelUnknown
}
