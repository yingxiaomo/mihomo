# Phase 2 设计规范：场景感知多指标选路（v0.2，影子已实现）

状态：**v0.2 影子实现完成**（默认 shadow-only，行为零变更）。
实现：`adapter/outboundgroup/scenario.go` + `computePLDWeights` 接入 +
`scenario-weighting` 组选项。硬亲和延迟到 active 阶段（见 §4）。

## 0. 依据（观察期结论）

- Phase 1 影子数据 9 天（2026-11-01 → 11-10），ScenarioScore 分层真实有效。
- Regret 分析（104k 样本，`smart_trainer/analyze_regret.py`）：
  - 稳定预言机遗憾 **16.2%**；峰值口径 39.2%（含突发运气，非可改进空间）。
  - 遗憾集中在大流量 × 特定目标：GeoIP [telegram] 29.9%、`*.air-outer.com` 85.8%、游戏平台 40.7%。
  - 小流量连接遗憾中位数仅 27ms（近最优）；尾部 p90 783ms。
- 服务失活检测实战（2026-11-07/08）：停滞是"传输成功但体验差"的独立层级——
  us15-xhttp × TG web 30 分钟 91 次 header-then-stall（SuspectedStall=108），
  churn 检测器按设计不覆盖（>1KB 有真实数据），只能由场景评分消费。
- 经验贝叶斯基线对比（`compare_prior_baseline.py`，82k 样本、同 GroupShuffleSplit 协议）：
  LightGBM 先验 **0.1841** vs EB 查表(3维分层) **0.4711** vs 全局均值 0.3476；
  分通道看，大流量段 LightGBM 0.5298 vs EB 0.5867 vs 全局 1.0030。
  **结论：模型层保留**。查表输给的根因有信息量——节点质量在同 target 内的方差
  远大于 target 间难度差，纯均值查表无法表达节点×上下文交互，而大流量段
  （regret 集中区）恰恰是所有预测器都最难的一段（RMSE 0.53-1.0）。

## 1. 目标与非目标

**目标**：把选路评分从"单通道标量 reward EMA + D-UCB"升级为"按需求场景加权的
多指标评分"，让大流量目标真正吃到吞吐通道、实时目标吃到低延迟+低停滞通道。

**非目标**：
- 不改 unwrap/主锚点事务、CAS 切换、churn 检测器——它们已被实战验证。
- 不做每请求动态权重学习（静态分类表起步，regret 数据后续喂）。
- 不动先验模型层（待 EB 基线结论：若查表可打平则考虑简化，默认保留）。

## 2. 场景分类（静态起步，已实现）

复用内核已有域名分类器（`ClassifyDomainFeature`，与模型 domain-type 特征同表），
按 target 归类。**实现修订**：game 类目标落在 interactive 而非 bulk——游戏下载
通常走 DIRECT 规则不进智能组，经 PLD 的游戏流量是会话型；bulk 只匹配显式
下载信号（download/steamcontent/mirror/pkg 等保守关键词表）。

| 场景类 | 典型目标 | 小通道权重 | 语义 |
|---|---|---|---|
| realtime | dns/api/communication（TG、IM、API） | 0.75 | 信令主导，锚点选择偏延迟通道 |
| interactive | streaming/game/未分类 | 0.50 | 混合，等同现行每请求通道行为 |
| bulk | 显式下载端点 | 0.25 | 吞吐主导（+硬亲和，active 阶段） |

## 3. 评分合成（已实现，v0.2 修订）

实现采用**通道混合**而非独立三分量公式——复用已被验证的双通道数据结构：

```
ucbReward = wS*rewSmall + (1-wS)*rewLarge      // 按类混合通道 reward EMA
w         = FinalWeight(prior, ucbReward, ...) // D-UCB 机制完全不动
merged    = applyStallPenalty(merged, stallRate)
          = merged * (1 - 0.5 * stallRate)     // 合并官方权重后统一降权
```

- 混合发生在 D-UCB exploit 输入上：prior、探索项、官方协同权重、滞回全部原样。
- stallRate = SuspectedStallCount/AttemptCount（clamp [0,1]），样本 <5 为 0
  （冷节点不吃薄证据惩罚）；停滞是"疑似"信号，降权而非冷却（硬失败有自己的路径）。
- PredictPrior 的输入保持通道匹配 reward（模型自己知道请求上下文，不被混合污染）。

**D-UCB 保留**：探索项不变；exploit 输入从"本请求通道 reward"变为场景混合 reward。
混合而非替换：reward EMA 是在线反馈兜底，通道混合是结构化先验。

## 4. 软锚点 / 硬亲和（active 阶段实现）

- 现状即软锚点（unwrap primary + 滞回 + margin）。
- **bulk 类硬亲和延迟到 active 切换时实现**（影子阶段 bulk 混合权重不生效，
  硬亲和无对象）：同 target 会话粘性优先，challenger 门槛从 `margin` 提高为
  `margin × 3`，故障/冷却兜底保留。

## 5. 影子先行（灰度，已实现）

1. 影子模式（默认）：每 50 次选路决策采样一条 debug 日志——
   `[PLD-Phase2] shadow: Group/Target/Class/Current/Scenario/Agree`，
   Current = 现行排序 top-1，Scenario = 场景混合+停滞惩罚重算的 top-1。
2. 对比期 3-4 天：统计 Agree 率 + 离线用影子数据重算 regret（同
   analyze_regret 口径），场景选择 ≤ 现行选择遗憾才准切换。
3. 切换开关：`scenario-weighting: *bool`（指针语义，默认 false=影子），
   每组可独立开关。

## 6. 配置与兼容

- 新组选项：`scenario-weighting`（*bool，默认 false）、`scenario-weights`（可选）。
- 不改样本 schema；ScenarioScore 的 API 字段保持兼容（small/large_score 保留，
  新增 class_score 三元组只进影子日志，不动 API）。
- 回滚：关 `scenario-weighting` 即回到现行行为，无持久化状态需要清理。

## 7. 风险

| 风险 | 缓解 |
|---|---|
| 分类错误导致权重错配（如把 bulk 判成 realtime） | 分类表保守起步 + 影子期人工抽查 top 目标分类 |
| stallRate 惩罚误伤冷节点 | 样本 <5 不计惩罚；影子期对比 |
| 场景分与 reward EMA 量纲冲突 | 混合权重起步保守（0.5-0.7），影子期校准 |
| 双口径并存的日志开销 | debug 级 + 1/50 采样 |

## 8. 待决问题（评审重点）

1. ~~EB 基线结论 → 先验模型层去留~~ **已决：保留模型层**（见第 0 节，LightGBM 全口径胜出）。
2. 硬亲和的 margin×3 是否足够"硬"？还是 bulk 类彻底跳过滞回？
3. `w_scenario` 三档起步值是否合理，还是先全部 0.5 观察一轮？
4. 未分类 target 落 interactive 还是按历史 ChannelStats 自动二选一？
