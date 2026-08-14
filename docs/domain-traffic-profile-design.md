# 域名流量档位（Domain Traffic Profile）设计

> 状态：设计稿（未实施）。目标分支：`dev-smart`。
> 一句话：在选路时刻，用「域名 → 本次连接是大流量（4K 视频）还是小流量（刷网页）」的先验，
> 让 PLD prior 直接偏向适配该场景的节点。

---

## 1. 动机与现状

### 1.1 痛点

选路一刻（连接开始前）能用的信号只有：

- `TrafficClass`（`ClassifyTrafficFromMetadata`）：**只看端口/UDP**，443 一律 web，
  UDP 非游戏端口一律 stream —— youtube.com 与新闻页在它眼里没有区别；
  QUIC/h3 视频（UDP 443）完全无法从端口分辨流量大小。
- `DomainFeature`（`extractDomainTypeFeature`）：域名**应用类别**（streaming/game/comm/api/dns），
  不是**流量大小**；且流媒体关键词打在了 `youtube.com` 这类「API 壳」上，而不是
  `googlevideo.com` 这类真正的视频流域名上。
- `TargetHash`：不可泛化，新域名 hash 完全不同，模型学不到「新的大流量域名该用带宽节点」。

与此同时，**事后**信号是完整的：

- `ComputeReward` 已按 `SmallTransferKB=512KB` 分流：小流量用
  `0.5·connect + 0.35·TTFR + 0.15·packet-pair`，大流量用 `0.8·吞吐 − 丢包惩罚`。
- 即 reward 已经编码了「节点在**大流量场景**快 / 在**小流量场景**快」的差异，
  但它是 per-域名×per-节点 的，**选路时不知道本次是哪种场景**，新域名/冷节点 prior=flat 0.5。

### 1.2 数据现状（已具备，无需新采集）

bbolt `smart/stats/{config}/{group}/{target}/{node}`（target 经 `GetEffectiveTarget` 通配符归一）
已按域名存有每个节点的：`downloadTotal`、`maxDownloadRate`、`duration`、
`FirstByteMs`、`BWCeilingKBps`、`reward`、`SampleCount`、`LastUsed`。
`Store.GetAllStats`（`stats.go:1179`）可直接聚合出「该域名历史上平均每次下载量 / 峰值速率」——
**这就是「看 4K 还是刷网页」的客观度量**，而且是从用户自己的使用历史里学的。

---

## 2. 目标 / 非目标

### 2.1 目标

1. 选路时刻产出一个**语义化、可泛化**的「域名流量档位」特征（大流量 / 中流量 / 小流量 / 未知），
   喂给 PLD prior（`RequestFeatures`），使先验能表达「大流量档位 → 高带宽 ceiling 节点 /
   小流量档位 → 低 TTFR 节点」的全局偏好。
2. 档位来源优先级：**在线实测画像 → 内置知名表 → 中性值**，冷启动也能用。
3. 与现有 `usepld` 门控一致：默认不改变原行为，新逻辑严格门控。

### 2.2 非目标

- 不做实时流量整形 / 分域 QoS。
- 不做「域名 → 节点」的硬绑定（那是现有 target 维度 prefetch 的事），只做**先验特征**。
- 不引入新的网络探测；只用被动统计 + 内置表。

---

## 3. 核心概念：流量档位定义

对每个 target（通配符归一后的域名/IP），定义：

- `AvgBytesPerConn`：历史平均单次连接下载字节数 ≈ `downloadTotal(Byte) / max(SampleCount,1)`。
  口径注意：`SampleCount` 目前是 reward 采样计数（TCP 每次连接 +1），UDP 也计，
  直接除会略低估，可接受（档位是粗粒度）。
- `PeakKBps`：`maxDownloadRate` 的 EWMA 峰值。
- `TrafficLevel ∈ {0=unknown, 1=small, 2=medium, 3=large}`：
  - `large`：`AvgBytesPerConn ≥ 4 MiB` 或 `PeakKBps ≥ 8 MB/s`（4K/高清视频、大文件）
  - `medium`：`512 KiB ≤ AvgBytesPerConn < 4 MiB`（图文页、缩略图流、小视频）
  - `small`：`AvgBytesPerConn < 512 KiB`（刷网页、API、即时通讯）
  - 阈值与 `ComputeReward.SmallTransferKB` 对齐（512 KiB），后续可配置。

档位只依赖**域名自身的历史**，与节点无关 —— 保证全组共享、可全局复用。

---

## 4. 特征接入（内核改动点）

### 4.1 `RequestFeatures` 扩展

`component/smart/lightgbm/prior.go`：

- 新增字段 `DomainTrafficLevel float64`（0/1/2/3，直接数值化，模型可学单调关系）。
- `PriorFeatureSize = 30` 当前只用 0–19 位，20–29 reserved：
  **占用 feature 20**，不改变既有特征布局（训练侧同步即可）。

### 4.2 `BuildRequestFeatures` 变更

`prior.go:317`，在 `metadata.SmartTarget` 已就绪后调用档位查询：

```
level := DomainTrafficProfiler.Lookup(metadata.SmartTarget)  // 画像命中
if level == 0 {
    level = builtinTrafficTable.Lookup(metadata.Host, metadata.SmartTarget) // 内置表兜底
}
```

- `ClassifyTrafficFromMetadata`（端口启发式）保持不动，作为**最后一个 fallback** 输入到
  `TrafficClass`；`DomainTrafficLevel` 是独立新特征，二者不互斥。

### 4.3 训练侧

- `train_pld.py`（离线，不在本仓库）特征列同步加 `domain_traffic_level`；
- 训练 CSV 来源建议加一个 bbolt 直出脚本（见 §7.4），带出 `target → 聚合档位` 列。

---

## 5. 三层实现

### 5.1 L1：内置知名域名档位表（静态）

新增 `component/smart/trafficprofile/builtin.go`（或并入 lightgbm 包），结构：

```go
type builtinEntry struct {
    suffix  string   // 精确后缀匹配（如 ".googlevideo.com"）
    level   int      // 1/2/3
    note    string
}
```

表的内容要点（**关键：打在视频 CDN 子域上，不是应用壳域名**）：

| 档位 | 条目示例 | 理由 |
|---|---|---|
| large | `.googlevideo.com`、`.akamaized.net`（媒体桶）、`.bmcdn.net`、`.bilivideo.com`、`.hls`/`.m3u8` 相关 CDN、`.cloudfront.net`（媒体桶）、`.gvt1.com`… | 实际承载视频流的域名 |
| medium | `.github.com` 的 release/objects CDN、`.cdninstagram.com`、普通图片 CDN 后缀 | 中大流量但非连续流 |
| small | 新闻/搜索/文档主域（`.wikipedia.org`、`.github.com` 页面本身…） | 页面为主 |

- 匹配方式：`strings.HasSuffix(host, suffix)`，全小写，长度优先。
- 明确**不要**把 `youtube.com`/`netflix.com` 主域标成 large —— 它们只是 API/页面壳，
  视频在 CDN 子域；主域流量反而偏 small。这是与现有 `streamingKeywords` 最大的行为差异。

### 5.2 L2：在线域名画像（核心）

新增 `component/smart/trafficprofile/profiler.go`：

```go
type DomainProfile struct {
    Target         string  `json:"target"`
    Level          int     `json:"level"`
    AvgBytesPerConn float64 `json:"avg_bytes_per_conn"`
    PeakKBps       float64 `json:"peak_kbps"`
    SampleCount    int64   `json:"sample_count"`
    LastUsed       int64   `json:"last_used"`
}

type Profiler struct {
    cache *lru.LruCache[string, DomainProfile]  // 复用 common/lru
    mu    sync.RWMutex
}

var Global *Profiler  // 单例，与 priorModel 同级
```

- **数据来源**：`Store.GetAllStats(group, config)` 返回 `map[target]map[node]StatsRecord`，
  对所有 group/config 汇总。注意：同一个 target 可能出现在多个组，按 target 合并
  （`downloadTotal`、`SampleCount` 求和，`maxDownloadRate` 取最大，`LastUsed` 取最大）。
- **刷新**：复用 `Smart` 现有定时任务（`startTimedTask`，见 `smart.go:984`），
  周期建议 5–10 分钟，扫描一次；每次扫描只在 bbolt 上跑一次前缀扫描（已有 `CleanupOldRecords` 同款成本）。
- **容量**：LRU 上限 2000 个 target，超出按 `LastUsed` 淘汰；配合现有 `RecordExpiredTime`（7 天）语义。
- **惰性回填**：`Lookup(target)` 未命中时可触发一次轻量按 target 的单点查询
  （`GetStatsForTarget(group, config, target, "")`），成功后写缓存 —— 热点域名首查即热。
- **API**：`hub/route` 增加 `GET /smart/profiles`（可选，先不做也不阻塞）。

### 5.3 L3：训练接入

- train.py / train_pld.py 加 `domain_traffic_level` 特征列（与 Go 侧 4.3 对齐）。
- 目标：让 LightGBM 学出「×档位请求 → 该选带宽/延迟型节点」的交互，
  而不是靠 `TargetHash` 死记。
- 顺带完成 `pld-design.md` 待办：Label 从 `weight` 切到 `reward`。

---

## 6. 选路查询优先级（汇总）

```
DomainTrafficProfiler.Lookup(SmartTarget)   // L2 实测画像（含惰性回填）
  → builtinTrafficTable.Lookup(host)        // L1 内置表（视频 CDN 子域优先）
  → 0 (unknown)                             // 端口启发式 TrafficClass 照旧兜底
```

`usepld:false` 时整条链路不生效（特征保持 0），完全兼容原行为。

---

## 7. 与现有机制的协同 & 关键坑

1. **视频壳 vs 视频流**：`youtube.com` 标 large 是错的；`googlevideo.com` 才是流。
   表必须按 CDN 后缀打。图片 CDN（`cdn.*` 通用后缀）归 medium，别一刀切 large。
2. **UDP 443（h3）**：端口完全看不出流量大小，域名档位是**唯一线索** —— 这正是本特征的最大增量价值。
3. **reward 协同**：`ComputeReward` 已按大小流量给出不同权重公式，
   prior 有了档位后，`FinalWeight = α·prior + (1-α)·rewardEMA + explore` 中
   prior 与 reward 的语义天然对齐（大流量请求 → 大流量 reward 更有分量）。
4. **Packet-Pair 协同**：小流量连接往往到不了 64 KiB，`BytesTo64KMs` 缺失；
   若先验知道「这是大流量域名」，可对 `BWCeilingKBps` 的置信度加权（后续增强，本期不做）。
5. **统计口径**：`AvgBytesPerConn` 用 `SampleCount` 做分母会因 UDP 计数略低估，可接受；
   若后续要精确，需区分 TCP/UDP 连接数（StatsRecord 已有 Success/Failure，可近似）。
6. **DB 成本**：画像扫描复用 `GetAllStats` 的一次前缀扫描 + LRU，不新增存储；
   上限 2000 条，杜绝无限膨胀。
7. **隐私**：画像只存聚合统计（字节数/速率/计数），不存原始域名日志；
   与现有 smart_stats 同桶，`/smart/flush` 一并清除。

---

## 8. 测试与验证

- 单测（`component/smart/trafficprofile/`）：
  - 内置表匹配：`googlevideo.com` → large；`youtube.com` 主域 → 不命中 large；CDN 后缀优先。
  - 聚合口径：构造假 `map[target]map[node]StatsRecord` → 验证 Level 阈值边界（512KiB / 4MiB）。
  - LRU 淘汰与惰性回填。
- 集成验证：`go build ./...` + `go test ./...` + `-tags with_gvisor` 全绿，`go vet` 干净。
- 行为验证：`usepld:false` 时特征恒 0，与改前完全一致（回归）。
- 实机：跑一个 smart 组看 `[PLD-Track]` 日志中 prior 分是否随域名档位分化。

---

## 9. 实施里程碑

| 里程碑 | 内容 | 预估 |
|---|---|---|
| M1 | `RequestFeatures.DomainTrafficLevel` + feature 20 接入 + L1 内置表 + 单测 | 小（~150 行） |
| M2 | L2 Profiler（聚合/LRU/刷新/惰性回填）+ `BuildRequestFeatures` 接线 + 单测 | 中（~300 行） |
| M3 | 训练侧：train_pld.py 特征列 + Label 切 reward + CSV 导出脚本 | 中（Python 侧） |
| M4 | （可选）`GET /smart/profiles` API + 阈值配置化（`traffic-large-bytes` 等） | 小 |

建议顺序：M1 → M2 → M3；M1/M2 可合并一次提交。
