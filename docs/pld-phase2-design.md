# Phase 2 设计规范：场景感知多指标选路（草案 v0.1，待评审）

状态：**草案**——设计决策待用户确认 + 并行 AI 审查后冻结，冻结前不动代码。

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

## 2. 场景分类（静态起步）

复用内核已有域名分类器（`lightgbm.go` 的 domainKeywordEntry：dns/api/communication/
game/streaming 五类 + 既有关键词表），按 target 归类：

| 场景类 | 典型目标 | 通道语义 |
|---|---|---|
| realtime | dns/api/communication（TG、IM、API） | 小流量：connect + ttfr + stall 惩罚 |
| interactive | streaming/web 默认 | 混合：connect + thr 中庸 + stall |
| bulk | game 下载/大文件 | 大流量：thr + loss |

映射规则：优先对 wildcardTarget 的 host 过既有分类器；集合级 target
（如 `GeoSite [telegram]`）对 target 名做同表关键词匹配；未命中 → interactive。
分类结果按 target 缓存（进程内 map，目标级，无每请求开销）。

## 3. 评分合成

Phase 1 的 `ScenarioScore(large bool)`（二通道）扩展为 `ScenarioScoreFor(class)`：

```
realtime:     0.45*connectScore + 0.30*ttfrScore + 0.25*(1-stallRate)
interactive:  0.30*connectScore + 0.20*ttfrScore + 0.30*thrScore + 0.20*(1-stallRate)
bulk:         0.65*thrScore + 0.25*lossScore + 0.10*(1-stallRate)
```

- stallRate = SuspectedStallCount/AttemptCount（影子指标已有数据），上限 1.0，
  样本 <5 时按 0 处理（不给冷节点无谓惩罚）。
- 各分项沿用 Phase 1 的有效性规则（cnt>0 才计入，缺项权重重分配）。

**D-UCB 保留**：探索项不变；exploit 项的输入从 `ChannelStats(large).reward`
改为场景加权分与 reward EMA 的混合：
`exploit = w_scenario*ScenarioScoreFor(class) + (1-w_scenario)*rewardEMA`，
`w_scenario` 按 class 配置（realtime 0.6 / interactive 0.5 / bulk 0.7，起步值）。
混合而非替换：reward EMA 是在线反馈的兜底，场景分是结构化先验，缺数据时互相补位。

## 4. 软锚点 / 硬亲和

- 现状即软锚点（unwrap primary + 滞回 + margin）。**bulk 类升级硬亲和**：
  同 target 会话粘性优先——只有当前锚点失败/冷却时才切换，不因 margin 竞争者
  漂移（游戏会话换 IP 断连代价 > 吞吐优化收益）。
- 硬亲和实现点：hysteresis 块中 bulk 类 target 的 challenger 门槛从
  `margin` 提高为 `margin × 3`（实质等效不切，仍保留故障兜底）。

## 5. 影子先行（灰度）

1. 实现期：场景分与现行 FinalWeight **并行计算**，每个选择决策记一条
   `[PLD-Phase2] shadow: group=%s target=%s class=%s current=%s scenario=%s agree=%v`
   （debug 级，避免刷爆日志——采样 1/50）。
2. 对比期 3-4 天：离线用影子数据重算 regret（同 analyze_regret 口径），
   场景选择 ≤ 现行选择遗憾才准切换。
3. 切换开关：`scenario-weighting: *bool`（指针语义，默认 false=影子），
   每组可独立开关；`scenario-weights` 可选 JSON 覆盖默认权重表。

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
