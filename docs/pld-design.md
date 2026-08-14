
---

## 实现状态（dev-smart, 已编译+测试通过）

**已完成**（本次落地，全部在 dev-smart 分支工作区未提交）：

| 组件 | 文件 | 状态 |
|---|---|---|
| 阶段一 prior.go | `component/smart/lightgbm/prior.go` | ✅ RequestFeatures/BuildRequestFeatures/PriorModel/PredictPrior/PriorBatch，flat-prior 退化 |
| 阶段二 D-UCB | `adapter/outboundgroup/smart.go` | ✅ SmartOption(usepld/prior-weight/explore-strength/prior-decay-k) + computePLDWeights 注入 selectProxies + filterProxies 冷启动门槛放宽 |
| 阶段三 Packet-Pair | `common/callback/packet_pair.go` | ✅ 被动首字节+64KiB 时戳 → BytesTo64KMs |
| 阶段三 Reward | `component/smart/stats.go` + `common.go` | ✅ Reward/RewardVar/SampleCount/FirstByteMs/BWCeilingKBps 字段全链路 + UpdateRewardEMA(discounted α) + ComputeReward + recordConnectionStats 注入 |
| 单测 | `component/smart/pld_test.go` | ✅ EMA/reward/finalWeight 收敛验证 |

**验证**：`go build ./...`(exit 0) + `go test ./...`(59 包全绿) + `-tags with_gvisor`(全绿) + `go vet`(干净)。`usepld:false` 完全保持原行为（所有新逻辑严格门控）。

**待办（离线训练侧，不在内核内）**：
- weekends 重训脚本切换 Label 为 `reward`（当前训练 CSV label=weight，weight 是 reward 的 EMA 代理，可先用）
- 独立 prior 模型文件 `PriorModel.bin`（可选，当前模型路径已独立，训练管线需产出 PriorModel.bin）

---

## 解耦重构（497b985a 之后，2026-08-14 完成）

PLD 已从官方 smart 组中**彻底解耦**，目标：官方文件回滚到 vernesong 基线、PLD 作为纯新增功能、merge 上游零冲突。

### 官方侧（回滚到 218cc1f3^ = vernesong 基线 + 通用修复）
- `adapter/outboundgroup/smart.go`：删除全部 PLD 注入（usePLD 分支、computePLDWeights、reward 回填、PacketPair），恢复官方逻辑
- `component/smart/common.go` / `stats.go`：回滚 PLD 增补（FinalWeight/ComputeReward/UpdateRewardEMA/Reward 字段全部搬走）
- `component/smart/lightgbm/collector.go`：恢复官方 CSV collector（4 参 AddSample、smart_weight_data.csv）
- 保留：`memory.go` 的 unwrap 缓存 merge 修复（81ff0839/c93ee2e8，通用 bug 修复，官方也有益）

### PLD 侧（全部独立文件/类型）
- **组类型 `smart-pld`**：`adapter/outboundgroup/smartpld.go`（自包含实现，receiver `*SmartPLD`），parser 新增 `case "smart-pld"`，`constant.SmartPLD` 类型
- **核心数学/数据结构**：`component/smart/pld.go`（FinalWeight、ComputeReward、UpdateRewardEMA、RewardedMetrics、PLDRecord、FormatPLDKey）
- **数据收集**：`pld_collector_sqlite.go`（SQLite 主，build tag 白名单）+ `pld_collector_csv.go`（回退），独立 `InitPLDCollector`/`GetPLDCollector`/`PLDDataCollector`（官方 `InitCollector`/`DataCollector` 不受影响）
- **数据隔离**：PLD reward 状态不再写官方 StatsRecord 的 reward 字段，改存 `pld:` key 前缀的自有 `PLDRecord`（同一 smart.db、独立命名空间，官方代码不可见）

### 开关（完全独立）
- 官方 `smart` 组：`uselightgbm`/`collectdata`/`sample-rate`/`prefer-asn`/`tolerance`/`policy-priority`（上游原样）
- `smart-pld` 组：`pld-collect`/`prior-weight`/`prior-decay-k`/`explore-strength`/`sample-rate`/`prefer-asn`/`tolerance`/`policy-priority`（PLD 专属，无官方开关）

### 配置示例
```yaml
proxy-groups:
  - name: PLD-Group
    type: smart-pld          # 新增组类型，PLD 在线 D-UCB
    usepld 相关开关不再需要：smart-pld 恒为 PLD
    pld-collect: true       # 收集训练样本（SQLite smart_samples.db）
    prior-weight: 0.6
    prior-decay-k: 10
    explore-strength: 0.05
    proxies: [...]
```

### merge 上游影响
- merge metacubex/Alpha：无 smart 文件，全部纯新增，零冲突
- merge vernesong/Alpha：官方 smart.go/common.go/stats.go/collector.go 已回滚 → 零差异；`constant/adapters.go` 仅新增 `SmartPLD` 枚举（唯一官方文件小改动）
