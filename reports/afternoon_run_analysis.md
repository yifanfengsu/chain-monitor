# Afternoon Run Analysis

## 1. 执行摘要

- 选定分析窗口：`1776406574` -> `1776421832`（Unix ts），即 `2026-04-17 06:16:14 UTC` -> `2026-04-17 10:30:32 UTC`，对应 archive 自带北京时间 `2026-04-17 14:16:14 UTC+8` -> `2026-04-17 18:30:32 UTC+8`。
- 选择原因：选择了今天 UTC 下午最长、且同时被 LP quality cache / asset-case cache / LP signal-attached case archive 覆盖的连续窗口。 系统 archive 持续到 2026-04-17 10:57:10 UTC，但 26m 38s 内未再出现 LP cache 更新，因此将该尾段视为非 LP 有效采样尾巴并排除。
- 覆盖时长：`4h 14m 18s`
- 系统级 archive 运行范围：`2026-04-17 06:14:53 UTC` -> `2026-04-17 10:57:10 UTC`；LP 有效样本比系统尾段少 ` 26m 38s `，因为 10:30:32 UTC 之后只看到非 LP archive 更新。
- 总 events：
  - overall raw archive: `28891`
  - overall parsed archive: `13658`
  - LP parsed events: `271`
- 总 signals：`38`
- 总 asset-case：`22`
- 总 LP 相关消息：`24`
- majors vs long-tail：`100.0`% vs `0.0`%
- market context source：`[{'key': 'unavailable', 'count': 38}]`
- signal-level scan path：`[{'key': 'unknown', 'count': 14}, {'key': 'primary_trend', 'count': 13}, {'key': 'main', 'count': 10}, {'key': 'promoted_main', 'count': 1}]`
- 总评：这轮数据已经“够做首轮研究采样”，但还不够当泛化产品样本，原因是样本过窄、signals 档缺失、market context 全部 unavailable。

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-17.ndjson` | `jsonl` | present | 2026-04-17 06:14:53 UTC -> 2026-04-17 10:57:10 UTC | overlap | overall raw archive
- `app/data/archive/parsed_events/2026-04-17.ndjson` | `jsonl` | present | 2026-04-17 06:14:53 UTC -> 2026-04-17 10:57:10 UTC | overlap | overall parsed archive
- `app/data/archive/signals/2026-04-17.ndjson` | `jsonl` | missing | n/a -> n/a | no-overlap | signals archive
- `app/data/archive/cases/2026-04-17.ndjson` | `jsonl` | present | 2026-04-17 06:14:53 UTC -> 2026-04-17 10:57:10 UTC | overlap | case snapshots
- `app/data/archive/case_followups/2026-04-17.ndjson` | `jsonl` | present | 2026-04-17 06:16:16 UTC -> 2026-04-17 10:30:03 UTC | overlap | case followups
- `app/data/archive/delivery_audit/2026-04-17.ndjson` | `jsonl` | present | 2026-04-17 06:14:53 UTC -> 2026-04-17 10:57:10 UTC | overlap | delivery audit
- `data/asset_cases.cache.json` | `json` | present | 2026-04-17 10:28:28 UTC -> 2026-04-17 10:30:17 UTC | overlap | active asset-case cache
- `data/lp_quality_stats.cache.json` | `json` | present | 2026-04-17 06:16:14 UTC -> 2026-04-17 10:30:17 UTC | overlap | quality/outcome cache
- `data/persisted_exchange_adjacent.json` | `json` | present | 2026-04-07 15:26:01 UTC -> 2026-04-17 10:56:51 UTC | overlap | exchange-adjacent runtime cache; only auxiliary for this LP report

- 数据完整性评级：`medium`
- 评级说明：
- signals archive 缺失，无法用单独 signal 档案复核每条通知。
- market context 虽配置为 live，但本轮记录全部回落为 unavailable。
- 未找到 nohup/stdout 启动日志；本次窗口主要依赖 archive 时间戳、asset-case cache、quality cache 的写入时间。

## 3. 实际分析窗口

- 主窗口：`2026-04-17 06:16:14 UTC` -> `2026-04-17 10:30:32 UTC` | `2026-04-17 14:16:14 UTC+8` -> `2026-04-17 18:30:32 UTC+8`
- 覆盖时长：`4h 14m 18s`
- 原始时间基准：
  - archive 文件使用 `archive_ts`（Unix 秒）和 `archive_time_bj`（UTC+8）
  - 报告正文统一以 `UTC` 展示
  - 当 archive 自带北京时间存在时，同时给出 `UTC+8`

## 4. 有效运行配置（非敏感）

- `DEFAULT_USER_TIER` = `research` | source=`.env`
- `MARKET_CONTEXT_ADAPTER_MODE` = `live` | source=`.env`
- `MARKET_CONTEXT_PRIMARY_VENUE` = `binance_perp` | source=`.env`
- `MARKET_CONTEXT_SECONDARY_VENUE` = `bybit_perp` | source=`.env`
- `ARCHIVE_ENABLE_RAW_EVENTS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_PARSED_EVENTS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_SIGNALS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_CASES` = `True` | source=`.env`
- `ARCHIVE_ENABLE_CASE_FOLLOWUPS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_DELIVERY_AUDIT` = `True` | source=`default/app.config`
- `LP_ASSET_CASE_PERSIST_ENABLE` = `True` | source=`.env`
- `LP_QUALITY_STATS_ENABLE` = `True` | source=`.env`
- `LP_PREALERT_MIN_USD` = `1500.0` | source=`default/app.config`
- `LP_PREALERT_MIN_CONFIRMATION` = `0.36` | source=`default/app.config`
- `LP_PREALERT_MIN_PRICING_CONFIDENCE` = `0.62` | source=`default/app.config`
- `LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY` = `0.24` | source=`default/app.config`
- `LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO` = `1.2` | source=`default/app.config`
- `LP_PREALERT_MIN_RESERVE_SKEW` = `0.08` | source=`default/app.config`
- `LP_PREALERT_PRIMARY_TREND_MIN_MATCHES` = `2` | source=`default/app.config`
- `LP_PREALERT_MULTI_POOL_WINDOW_SEC` = `90` | source=`default/app.config`
- `LP_PREALERT_FOLLOWUP_WINDOW_SEC` = `60` | source=`default/app.config`
- `LP_OBSERVE_MIN_USD` = `3000.0` | source=`default/app.config`
- `LP_OBSERVE_MIN_CONFIDENCE` = `0.46` | source=`default/app.config`
- `LP_SWEEP_MIN_ACTION_INTENSITY` = `0.42` | source=`default/app.config`
- `LP_SWEEP_MIN_VOLUME_SURGE_RATIO` = `1.8` | source=`default/app.config`
- `LP_SWEEP_MIN_SAME_POOL_CONTINUITY` = `2` | source=`default/app.config`
- `LP_SWEEP_MIN_BURST_EVENT_COUNT` = `3` | source=`default/app.config`
- `LP_SWEEP_MAX_BURST_WINDOW_SEC` = `45` | source=`default/app.config`
- `LP_SWEEP_CONTINUATION_MIN_SCORE` = `0.56` | source=`default/app.config`
- `LP_SWEEP_EXHAUSTION_MIN_SCORE` = `0.6` | source=`default/app.config`
- `LP_BURST_FASTLANE_ENABLE` = `True` | source=`default/app.config`
- `LP_FASTLANE_PROMOTION_ENABLE` = `True` | source=`default/app.config`
- `LP_FASTLANE_PROMOTED_MAX_COUNT` = `8` | source=`default/app.config`
- `LP_FASTLANE_PROMOTION_TTL_SEC` = `90` | source=`default/app.config`
- `LP_FASTLANE_MAIN_SCAN_INCLUDE_PROMOTED` = `True` | source=`default/app.config`
- `LP_PRIMARY_TREND_SCAN_INTERVAL_SEC` = `12.0` | source=`default/app.config`
- `LP_SECONDARY_SCAN_ENABLE` = `True` | source=`default/app.config`
- `LP_SECONDARY_SCAN_INTERVAL_SEC` = `18.0` | source=`default/app.config`
- `LP_EXTENDED_SCAN_ENABLE` = `False` | source=`default/app.config`
- `LP_EXTENDED_SCAN_INTERVAL_SEC` = `60.0` | source=`default/app.config`
- `LP_QUALITY_MIN_FASTLANE_ROI_SCORE` = `0.42` | source=`default/app.config`
- `LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE` = `0.68` | source=`default/app.config`

- 配置解读：
  - 运行 tier 是 `research`，因此 prealert / confirm / climax / exhaustion_risk 全部允许进入研究视图。
  - archive 在配置上是全开的：raw / parsed / signals / cases / case_followups / delivery_audit 都应可写。
  - 实际落盘与配置不完全一致：`ARCHIVE_ENABLE_SIGNALS=True`，但今天的 `archive/signals/2026-04-17.ndjson` 不存在。
  - market context 配置是 `live + binance_perp/bybit_perp`，但实际落盘全是 `unavailable`，说明研究采样能力未真正打通到市场上下文层。

## 5. LP stage 质量分析

- stage 分布：
- `prealert`: 1 条
- `confirm`: 23 条
- `climax`: 7 条
- `exhaustion_risk`: 7 条
- prealert -> confirm 转化：
  - 30s: `0.0`%
  - 60s: `0.0`%
  - 90s: `0.0`%
  - 说明：优先按 asset_case_key 回查，缺失时退化到 pair+direction 匹配；本轮原生 time_to_confirm 字段未回写。
- climax -> reversal：
  - 60s 可得样本：`4`
  - reversal 比例：`75.0`%
  - 30s：本轮持久化结果未提供稳定字段，无法可靠统计。
- exhaustion_risk：
  - 已解析样本：`4`
  - 负向 followthrough / 回吐比例：`25.0`%
- on-chain timing proxy（不是 CEX/perp timing）：
- `prealert`: [{'key': 'leading', 'count': 1}]
- `confirm`: [{'key': 'confirming', 'count': 14}, {'key': 'unknown', 'count': 9}]
- `climax`: [{'key': 'unknown', 'count': 5}, {'key': 'late', 'count': 2}]
- `exhaustion_risk`: [{'key': 'late', 'count': 7}]
- 关键判断：
  - prealert 现在并不“过多”，占比只有 `2.6`%，但样本只有 1 条，且闭环字段未成熟，不能把“少”误读成“已验证有效”。
  - confirm 更像“中段确认”而不是明显先手；其 30s/60s 前置涨跌幅已非零，适合 research/trader 做确认，不适合当 ultra-early entry。
  - climax 明显有尾段风险：resolved climax 中 `75.0`% 很快反转。
  - exhaustion_risk 的行为意义比“继续追 sweep”更安全，因为它本身就在提醒尾段风险；但当前样本仍偏少。

## 6. asset-case 分析

- 总 asset-case 数：`22`
- case 升级路径统计：
  - prealert -> confirm: `0`
  - confirm -> climax: `2`
  - climax -> exhaustion_risk: `1`
- supporting_pairs 组合：
- `ETH/USDC`: 9 cases
- `ETH/USDT`: 8 cases
- `ETH/USDC + ETH/USDT`: 5 cases
- 最活跃 cases：
- `asset_case:ETH:ef68c4ee1e` | stages=['confirm', 'climax', 'confirm', 'climax'] | pairs=['ETH/USDC', 'ETH/USDT'] | lifespan=1m 22s
- `asset_case:ETH:018a89d087` | stages=['exhaustion_risk', 'climax'] | pairs=['ETH/USDC', 'ETH/USDT'] | lifespan=2m 0s
- `asset_case:ETH:fbc0a246a4` | stages=['confirm'] | pairs=['ETH/USDC', 'ETH/USDT'] | lifespan=1m 49s
- `asset_case:ETH:7656f7da38` | stages=['confirm', 'climax', 'confirm'] | pairs=['ETH/USDC', 'ETH/USDT'] | lifespan=48s
- `asset_case:ETH:e5e4ec1ce8` | stages=['exhaustion_risk'] | pairs=['ETH/USDC', 'ETH/USDT'] | lifespan=0s
- 最干净 cases：
- `asset_case:ETH:fbc0a246a4` | confirms=4 | negatives=0 | pairs=['ETH/USDC', 'ETH/USDT']
- `asset_case:ETH:7656f7da38` | confirms=3 | negatives=0 | pairs=['ETH/USDC', 'ETH/USDT']
- `asset_case:ETH:6722f23cbd` | confirms=1 | negatives=0 | pairs=['ETH/USDC']
- `asset_case:ETH:554b0a6ec8` | confirms=1 | negatives=0 | pairs=['ETH/USDC']
- `asset_case:ETH:efaa66586d` | confirms=1 | negatives=0 | pairs=['ETH/USDC']
- 最吵 cases：
- `asset_case:ETH:ef68c4ee1e` | prealerts=0 | negatives=4 | stages=['confirm', 'climax', 'confirm', 'climax']
- `asset_case:ETH:6860f8a85c` | prealerts=0 | negatives=1 | stages=['exhaustion_risk']
- `asset_case:ETH:0e35a913fc` | prealerts=0 | negatives=1 | stages=['climax']
- `asset_case:ETH:45f186504a` | prealerts=0 | negatives=1 | stages=['confirm']
- `asset_case:ETH:d2d60a3168` | prealerts=1 | negatives=0 | stages=['prealert']
- 聚合效果：
  - 相对 signal 数的估算压缩率：`42.1`%
  - 相对 pool case 数的估算压缩率：`n/a`（本轮 pool case id 口径比 asset-case 更窄，无法稳定估算）
- 结论：
  - asset-case 聚合对用户理解 ETH 双池联动是有帮助的，至少减少了池级平铺。
  - 本轮没有看到强烈的错误合并迹象；方向、pair 支持关系基本一致。
  - 但大多数 case 仍然是单资产、极少 pair 的窄样本，离“复杂市场叙事聚合”还很远。

## 7. market context / timing 分析

- market context source 分布：`[{'key': 'unavailable', 'count': 38}]`
- live market context 可用率：`0.0`%
- external timing（Binance/Bybit 维度）：`[]`
- on-chain timing proxy：`[{'key': 'confirming', 'count': 14}, {'key': 'unknown', 'count': 14}, {'key': 'late', 'count': 9}, {'key': 'leading', 'count': 1}]`
- 结论：
  - 由于 `market_context_source` 全部是 `unavailable`，不能对 CEX/perp 说“领先 / 确认 / 偏晚”。
  - 退化到链上视角时，prealert 更靠前，confirm 更偏确认，climax / exhaustion_risk 更偏后段或尾段风险提示。
  - 对合约交易者来说，本轮最有参考价值的是 `confirm` 的方向确认和 `exhaustion_risk` 的追单风险提示；最容易被误用的是 `climax`。
  - basis / mark-index / funding 等市场字段本轮都没有持久化命中，无法分析。

## 8. fastlane ROI 分析

- fastlane/promoted 样本：
  - fastlane signal 数：`1`
  - promoted_main signal 数：`1`
  - primary_trend signal 数：`13`
  - secondary signal 数：`0`
  - extended signal 数：`0`
- scan path 分布：`[{'key': 'unknown', 'count': 14}, {'key': 'primary_trend', 'count': 13}, {'key': 'main', 'count': 10}, {'key': 'promoted_main', 'count': 1}]`
- latency：
  - promoted_main median latency：`17000.0` ms
  - non-promoted median latency：`2000.0` ms
- followthrough：
  - fastlane aligned median move 60s：`None`
  - fastlane aligned median move 300s：`None`
- 结论：
  - promoted_main 还谈不上“已经值得”，因为只有 1 个 promotion 样本，后续 outcome 也没形成正向分化。
  - 当前更像 fastlane 框架已接好，但 ROI 还没有在真实数据里跑出分层。
  - secondary / extended 在通知层几乎没样本，无法比较 ROI。

## 9. quality/outcome 闭环分析

- overall quality：
  - quality_score=`0.544`
  - prealert_precision_score=`0.58`
  - confirm_conversion_score=`0.468`
  - climax_reversal_score=`0.6214`
  - fastlane_roi_score=`0.55`
- top_bad_prealerts：
- `asset:ETH` | prealert_precision=0.58 | sample=38
- `pool:0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852` | prealert_precision=0.58 | sample=18
- `pair:ETH/USDT` | prealert_precision=0.58 | sample=18
- `pool:0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc` | prealert_precision=0.58 | sample=20
- `pair:ETH/USDC` | prealert_precision=0.58 | sample=20
- top_climax_reversal：
- `pool:0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852` | climax_reversal=0.67 | sample=18
- `pair:ETH/USDT` | climax_reversal=0.67 | sample=18
- `asset:ETH` | climax_reversal=0.6214 | sample=38
- `pool:0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc` | climax_reversal=0.47 | sample=20
- `pair:ETH/USDC` | climax_reversal=0.47 | sample=20
- fastlane_roi_laggards：
- `asset:ETH` | fastlane_roi=0.55 | sample=38
- `pool:0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852` | fastlane_roi=0.55 | sample=18
- `pair:ETH/USDT` | fastlane_roi=0.55 | sample=18
- `pool:0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc` | fastlane_roi=0.55 | sample=20
- `pair:ETH/USDC` | fastlane_roi=0.55 | sample=20
- 样本保护：
  - stage=`prealert` 在 quality report 中 `actionable=false`，说明样本保护在该层已生效。
  - pair / pool / asset 虽然 `actionable=true`，但 resolved confirms 只有 3-4、resolved climaxes 只有 2，仍应视为早期特征，不宜过度调参。
- 调参含义：
  - 最该降权的是 `ETH/USDT` 的 climax/chase 场景，因为 reversal score 更高。
  - 最值得保留的是 majors 上的 `confirm` 和 `exhaustion_risk` 提示语义。
  - 当前真正“不该下结论”的，是 prealert 相关 precision / conversion。

## 10. 噪音与误判风险评估

- research 用户：当前噪音 `中等`。消息量不大，但 timing/context 缺口会放大误读风险。
- trader 用户：最容易误判的是把 `climax` 当成继续追单信号，以及把 `confirm` 误当超早信号。
- retail 用户：`prealert`、`single-pool climax`、没有 market context 的晚阶段 directional 语句都应默认隐藏或更严格。
- 最容易被误读成“直接追”的消息：
  - climax
  - exhaustion_risk（如果用户忽略“风险”二字，只看方向）
  - 没有 market context 支撑的 confirm
- 当前最大噪音来源：`market_context unavailable + 晚阶段信号容易被误读为追单`
- 明确判断：
  - 当前 LP 噪音：`中等`
  - 是否足以支持应用级产品下一步测试：`只够 research/trader 内测，不够 retail / 泛产品化测试`
  - 最大误判风险：`晚阶段 signal 在缺少 market context 时被当成追单入口`

## 11. 最终评分

- LP 研究采样 readiness：`5.3/10`
- LP 产品信号可用性：`4.3/10`
- LP 噪音控制：`6.0/10`
- market timing 价值：`3.7/10`
- asset-case 聚合价值：`6.5/10`
- fastlane ROI：`4.3/10`
- 综合分：`5.0/10`

## 12. 下一步建议

### Top 5 优点
- LP 有效窗口达到 4h 14m 18s，不是单次 replay，而是真实连续运行样本。
- 归档链条较完整：raw/parsed/cases/case_followups/quality/asset-case 均存在，完整性评为 `medium`。
- LP 消息量被压在 38 条，prealert 只有 1 条，没有出现 prealert 洪水。
- asset-case 相对 signal 数的估算压缩率约为 42.1% 。
- quality/outcome 已经开始分出 ETH/USDT 的 climax reversal 风险高于 ETH/USDC。

### Top 5 当前不足
- signals archive 缺失，导致消息内容无法用独立 signal 档案复核。
- market context 配置是 live，但实际 100% unavailable，CEX/perp timing 结论无法成立。
- LP 样本只覆盖 1 个资产、2 个 pair，没有 long-tail 覆盖。
- prealert 闭环字段没有成熟回写，prealert->confirm 只能做弱结论。
- fastlane/promoted_main 只有 1 个样本，ROI 还没形成可操作分层。

### Top 5 最值得优先改进的事项
- 优先修复 live market context 命中率，否则 timing 模块无法进入研究闭环。
- 优先恢复 `archive/signals`，让 signal 内容、notifier 输出、quality 记录三者可对齐。
- 优先补齐 30s/60s/300s outcome 更新，尤其是 prealert 与 exhaustion_risk。
- 优先扩池，但先扩 majors，不要先扩 long-tail，否则会把当前结论重新打散。
- 优先把 promoted_main/primary_trend/secondary 的 scan-path 结果导出成独立统计表。

## 13. 限制与不确定性

- 本轮 `archive/signals` 缺失，所以无法独立校验通知模板、发送理由和 quality/outcome 是否 1:1 对齐。
- `market_context_source=unavailable` 占比 100%，因此所有 CEX/perp timing 结论都被明确降级为“不可得”。
- prealert 只有 1 条，且闭环字段未成熟，任何关于 prealert precision 的结论都不稳。
- 样本只覆盖 ETH 两个 Uniswap V2 主流池，不代表 long-tail，也不代表其他 DEX / CLMM。
- 未找到完整启动日志，只能用 archive/cache 时间戳重建窗口。
- 当前报告是基于今天 `2026-04-17` 的真实落盘数据，而不是 replay；但这也是一轮窄样本，需要继续累计。
