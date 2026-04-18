# Overnight Run Analysis

## 1. 执行摘要

- 这轮 overnight 数据**够做研究分析**：`archive/signals`、`delivery_audit`、`case_followups`、`lp_quality_stats` 都可用，且 LP signal / quality / delivery 可做到 `1:1` 对账。
- 最大优点：`sweep_building` 的用户可见映射已经纠偏，主窗口 `24` 个 `sweep_building` 样本里，`0` 个仍把自己显示成“高潮”。
- 最大问题：live market context **没有真正修好**，主窗口 `live_public hit rate = 0.0%`，相比下午报告的 `0%` 没有提升。
- 最关键的 3 个发现：
  - `confirm` 的“诚实降级”已经生效：full overnight 非 sweep 的 `30` 个 confirm 中，`late_confirm=13`、`unconfirmed_confirm=16`、`chase_risk=1`、`clean_confirm=0`。
  - `archive/signals` 已稳定落盘：full overnight `96` 个 LP signal_id 与 quality `96`、delivery `96` 完全对齐；其中已送达的 `32` 条又与 cases.signal_attached `32`、case_followups `32` 完全一致。
  - 研究样本仍然几乎只代表 `ETH/USDT + ETH/USDC` 两个 ETH 主池：full overnight `96/96` 都是 majors，但 `BTC/SOL` 仍然是 `0`。
- 是否支持继续连续采样：**有条件支持**。可以继续做 ETH 主池研究采样与 patch 回归验证，但还不适合把这轮结果外推成“整体 LP 产品表现”。

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-18.ndjson` | `jsonl` | missing | n/a -> n/a | no-overlap | raw events archive
- `app/data/archive/parsed_events/2026-04-18.ndjson` | `jsonl` | missing | n/a -> n/a | no-overlap | parsed events archive
- `app/data/archive/signals/2026-04-18.ndjson` | `jsonl` | present | 2026-04-17 16:23:53 UTC -> 2026-04-18 01:26:58 UTC | overlap | signal archive
- `app/data/archive/cases/2026-04-18.ndjson` | `jsonl` | present | 2026-04-17 16:22:28 UTC -> 2026-04-18 01:27:27 UTC | overlap | case archive
- `app/data/archive/case_followups/2026-04-18.ndjson` | `jsonl` | present | 2026-04-17 16:23:54 UTC -> 2026-04-18 01:12:17 UTC | overlap | case followups archive
- `app/data/archive/delivery_audit/2026-04-18.ndjson` | `jsonl` | present | 2026-04-17 16:22:28 UTC -> 2026-04-18 01:27:28 UTC | overlap | delivery audit archive
- `data/asset_cases.cache.json` | `json` | present | 2026-04-18 01:12:15 UTC -> 2026-04-18 01:12:17 UTC | overlap | active asset-case snapshot cache; not a historical ledger
- `data/lp_quality_stats.cache.json` | `json` | present | 2026-04-17 06:16:14 UTC -> 2026-04-18 01:12:19 UTC | overlap | lp outcome / quality cache
- 辅助 CLI：
  - `venv/bin/python -m app.quality_reports --market-context-health`
  - `venv/bin/python -m app.quality_reports --major-pool-coverage`
- 数据完整性评级：`medium`
- 评级理由：
  - `archive/signals` / `delivery_audit` / `case_followups` / `lp_quality_stats` 足够支撑信号、消息、outcome 的闭环对账。
  - `raw_events` / `parsed_events` 在昨晚对应路径缺失，因此无法回放原始事件级噪音来源。
  - `asset_cases.cache.json` 只保留最后活跃 snapshot，不能当历史账本使用。
  - 未找到运行日志 / stdout 捕获文件，本次运行窗口主要靠 archive + cache 时间戳识别。

## 3. 实际分析窗口

- archive 总覆盖：`2026-04-17 16:22:28 UTC` -> `2026-04-18 01:27:28 UTC` | `2026-04-18 00:22:28 UTC+08:00` -> `2026-04-18 09:27:28 UTC+08:00`
- LP overnight 活跃总包络：`2026-04-17 16:23:51 UTC` -> `2026-04-18 01:12:15 UTC` | `2026-04-18 00:23:51 UTC+08:00` -> `2026-04-18 09:12:15 UTC+08:00`
- 选定主分析窗口：`2026-04-17 16:22:29 UTC` -> `2026-04-18 01:27:16 UTC` | `2026-04-18 00:22:29 UTC+08:00` -> `2026-04-18 09:27:16 UTC+08:00`
- 总时长：`9h 4m 47s`
- 选择原因：选择了昨晚 archive 范围内最长、且同时被 signals / quality / delivery / case followups 覆盖的连续 LP 活跃段；该段共有 96 条 LP signal archive、96 条 quality 记录、96 个 delivery_audit signal_id。
- 多段运行概览：
  - `2026-04-18 00:22:29 UTC+08:00` -> `2026-04-18 09:27:16 UTC+08:00` | `9h 4m 47s` | signals=96 | quality=96 | delivery_unique=96
- 时间基准：
  - 报告正文统一写 `UTC`。
  - “本地时间”按 archive 自带的 `archive_time_bj` 推断为 `UTC+8`。

## 4. 有效运行配置（非敏感）

- `DEFAULT_USER_TIER` = `research` | source=`.env`
- `MARKET_CONTEXT_ADAPTER_MODE` = `live` | source=`.env`
- `MARKET_CONTEXT_PRIMARY_VENUE` = `binance_perp` | source=`.env`
- `MARKET_CONTEXT_SECONDARY_VENUE` = `bybit_perp` | source=`.env`
- `ARCHIVE_ENABLE_RAW_EVENTS` = `False` | source=`.env`
- `ARCHIVE_ENABLE_PARSED_EVENTS` = `False` | source=`.env`
- `ARCHIVE_ENABLE_SIGNALS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_CASES` = `True` | source=`.env`
- `ARCHIVE_ENABLE_CASE_FOLLOWUPS` = `True` | source=`.env`
- `ARCHIVE_ENABLE_DELIVERY_AUDIT` = `True` | source=`default/app.config`
- `LP_ASSET_CASE_PERSIST_ENABLE` = `True` | source=`.env`
- `LP_QUALITY_STATS_ENABLE` = `True` | source=`.env`
- `LP_MAJOR_ASSETS` = `ETH,WETH,BTC,WBTC,CBBTC,SOL,WSOL` | source=`default/app.config`
- `LP_MAJOR_QUOTES` = `USDT,USDC,USDC.E` | source=`default/app.config`
- `LP_PREALERT_MIN_USD` = `1500.0` | source=`default/app.config`
- `LP_PREALERT_MIN_CONFIRMATION` = `0.36` | source=`default/app.config`
- `LP_PREALERT_MIN_PRICING_CONFIDENCE` = `0.62` | source=`default/app.config`
- `LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY` = `0.24` | source=`default/app.config`
- `LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO` = `1.2` | source=`default/app.config`
- `LP_PREALERT_MIN_RESERVE_SKEW` = `0.08` | source=`default/app.config`
- `LP_PREALERT_PRIMARY_TREND_MIN_MATCHES` = `2` | source=`default/app.config`
- `LP_PREALERT_MULTI_POOL_WINDOW_SEC` = `90` | source=`default/app.config`
- `LP_PREALERT_FOLLOWUP_WINDOW_SEC` = `60` | source=`default/app.config`
- `LP_SWEEP_MIN_ACTION_INTENSITY` = `0.42` | source=`default/app.config`
- `LP_SWEEP_MIN_VOLUME_SURGE_RATIO` = `1.8` | source=`default/app.config`
- `LP_SWEEP_MIN_SAME_POOL_CONTINUITY` = `2` | source=`default/app.config`
- `LP_SWEEP_MIN_BURST_EVENT_COUNT` = `3` | source=`default/app.config`
- `LP_SWEEP_MAX_BURST_WINDOW_SEC` = `45` | source=`default/app.config`
- `LP_SWEEP_CONTINUATION_MIN_SCORE` = `0.56` | source=`default/app.config`
- `LP_SWEEP_EXHAUSTION_MIN_SCORE` = `0.6` | source=`default/app.config`
- `LP_FASTLANE_PROMOTION_ENABLE` = `True` | source=`default/app.config`
- `LP_FASTLANE_MAIN_SCAN_INCLUDE_PROMOTED` = `True` | source=`default/app.config`
- `LP_PRIMARY_TREND_SCAN_INTERVAL_SEC` = `12.0` | source=`default/app.config`
- `LP_SECONDARY_SCAN_ENABLE` = `True` | source=`default/app.config`
- `LP_SECONDARY_SCAN_INTERVAL_SEC` = `18.0` | source=`default/app.config`
- `LP_EXTENDED_SCAN_ENABLE` = `False` | source=`default/app.config`
- `LP_EXTENDED_SCAN_INTERVAL_SEC` = `60.0` | source=`default/app.config`
- `LP_QUALITY_MIN_FASTLANE_ROI_SCORE` = `0.42` | source=`default/app.config`
- `LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE` = `0.68` | source=`default/app.config`
- confirm downgrade 逻辑（来自 `app/pipeline.py`，硬编码逻辑，不是敏感配置）：
  - `late_confirm` 关键触发：`market_context unavailable + pool pre-move >= 0.6%`、`pool pre-move >= 0.9%`、`single-pool + no broader confirmation`、`detect latency >= 4.5s`、`case age >= 150s`。
  - `chase_risk` 关键触发：`chase_risk_score >= 0.58` 且伴随更强预走 / 更晚 timing / broader confirmation 缺失 / `detect latency >= 8s` / `case age >= 240s`。
- 研究型采样适配度：
  - `DEFAULT_USER_TIER=research`，四个 LP stage 都会被保留到研究视图。
  - archive 开关在配置上是够的，但 `raw_events/parsed_events` 昨晚对应文件缺失，影响事件级回放。
  - `MARKET_CONTEXT_ADAPTER_MODE=live` 但昨晚实际 hit rate 仍为 0%，这会限制 timing / broader confirmation 研究。

## 5. LP stage 质量分析

- 主窗口 LP signals：`96`，其中消息送达 `32`，asset-cases `32`。
- stage 分布：
  - `prealert`: `0`
  - `confirm`: `54`
  - `climax`: `31`
  - `exhaustion_risk`: `11`
- 主窗口只覆盖 ETH majors：`ETH/USDT` 与 `ETH/USDC` 两个池。
- prealert -> confirm 转化：
  - 30s: `None`
  - 60s: `None`
  - 90s: `None`
  - 说明：本窗口无 prealert 样本，无法判断 prealert->confirm 转化。
- 各 stage 核心统计：
  - `prealert` | pairs=[] | median move_before_30s=None | median move_before_60s=None | median move_after_60s=None | median move_after_300s=None | aligned_after_60s=None | aligned_after_300s=None
  - `confirm` | pairs=[{'key': 'ETH/USDC', 'count': 31}, {'key': 'ETH/USDT', 'count': 23}] | median move_before_30s=-0.000444 | median move_before_60s=-0.000461 | median move_after_60s=-0.000529 | median move_after_300s=-0.001608 | aligned_after_60s=0.000673 | aligned_after_300s=0.001608
  - `climax` | pairs=[{'key': 'ETH/USDT', 'count': 18}, {'key': 'ETH/USDC', 'count': 13}] | median move_before_30s=-0.000859 | median move_before_60s=-0.00076 | median move_after_60s=-0.001068 | median move_after_300s=-0.002154 | aligned_after_60s=0.001204 | aligned_after_300s=0.002154
  - `exhaustion_risk` | pairs=[{'key': 'ETH/USDT', 'count': 7}, {'key': 'ETH/USDC', 'count': 4}] | median move_before_30s=0.000622 | median move_before_60s=0.000673 | median move_after_60s=0.000742 | median move_after_300s=0.000742 | aligned_after_60s=0.000742 | aligned_after_300s=0.000742
- stage latency：
  - `prealert` | median detect latency=`None` ms | median end-to-end latency=`None` ms
  - `confirm` | median detect latency=`2000.0` ms | median end-to-end latency=`3500.0` ms
  - `climax` | median detect latency=`2000.0` ms | median end-to-end latency=`4000.0` ms
  - `exhaustion_risk` | median detect latency=`1000.0` ms | median end-to-end latency=`4000.0` ms
- stage market context / timing：
  - `prealert` | market_context_source=[] | live available=None% | on-chain timing proxy=[]
  - `confirm` | market_context_source=[{'key': 'unavailable', 'count': 54}] | live available=0.0% | on-chain timing proxy=[{'key': 'confirming', 'count': 53}, {'key': 'late', 'count': 1}]
  - `climax` | market_context_source=[{'key': 'unavailable', 'count': 31}] | live available=0.0% | on-chain timing proxy=[{'key': 'confirming', 'count': 26}, {'key': 'late', 'count': 5}]
  - `exhaustion_risk` | market_context_source=[{'key': 'unavailable', 'count': 11}] | live available=0.0% | on-chain timing proxy=[{'key': 'late', 'count': 11}]
- reversal / followthrough：
  - `climax` adverse move proxy at 60s: `0.0%` (resolved `15`)
  - `climax` adverse move proxy at 300s: `0.0%` (resolved `23`)
  - `exhaustion_risk` adverse move proxy at 60s: `0.0%` (resolved `1`)
  - `exhaustion_risk` adverse move proxy at 300s: `0.0%` (resolved `3`)
- 明确判断：
  - `prealert` 现在不是“太多”，而是**几乎没有**。full overnight `0/96`，这说明系统昨晚仍更像确认器，而不是前置研究采样器。
  - `confirm` 依然偏晚，但比以前诚实：主窗口非 sweep confirm 共 `30` 条，`late_confirm=13`、`unconfirmed_confirm=16`、`chase_risk=1`、`clean_confirm=0`。
  - `climax` 在这段主窗口里**没有录到明显 reversal**；这比下午那轮好，但样本共有 `31` 条，且 60s/300s resolved 仍分别只有 `15` / `23`。
  - `exhaustion_risk` 的价值仍高于继续报 sweep：主窗口共有 `11` 条，alert 前 median move_before_60s=`0.000673`，之后 median move_after_300s=`0.000742`，更像尾段风险提示而不是新起点。

## 6. 纠偏 patch 验证

### A. `sweep_building` 不再显示成“高潮”

- 主窗口 `sweep_building` 样本：`24`
- full overnight `sweep_building` 样本：`24`
- 残留“高潮/climax”映射：`0`
- 结论：从 archive 中可见的 `headline_label` / `lp_state_label` / `lp_sweep_display_stage` 看，这个 patch **真实生效**；样本显示为“买方/卖方清扫延续待确认”，不再冒充高潮。

### B. `confirm` 的 `late_confirm / chase_risk`

- full overnight confirm buckets：`{'chase_risk': 1, 'late_confirm': 13, 'sweep_building_confirm': 24, 'unconfirmed_confirm': 16}`
- 主窗口 confirm buckets：`{'chase_risk': 1, 'late_confirm': 13, 'sweep_building_confirm': 24, 'unconfirmed_confirm': 16}`
- 解释：
  - `sweep_building_confirm` = confirm stage，但走的是 sweep_building 路径，因此不挂 clean/late/chase 标签。
  - 非 sweep confirm 在 full overnight 一共 `30` 条，全部都被明确分流到了 `late_confirm / unconfirmed_confirm / chase_risk`，`clean_confirm=0`。
  - 这说明 patch 至少已经把“偏晚确认”从普通 confirm 里剥离出来，没有再留下未标记的 generic confirm。
- 数据表现：
  - `late_confirm` `13` 条，median move_before_alert_60s=`-0.000888`，aligned move_after_alert_300s=`0.001852`。
  - `unconfirmed_confirm` `16` 条，median move_before_alert_60s=`-0.000583`，aligned move_after_alert_300s=`0.001559`。
  - `chase_risk` `1` 条，仅出现于后半夜较小段样本，标签和 message headline 都直接写成“追空风险”。

### C. `archive/signals`

- `archive/signals` 存在，full overnight LP rows=`96`。
- 与 quality 对账：`96` / `96`，缺口=`0`。
- 与 delivery_audit 对账：`96` / `96`，缺口=`0`。
- 已送达子集：signals `32` -> delivery delivered `32` -> cases.signal_attached `32` -> case_followups `32`，diff 全为 `0`。
- 结论：`archive/signals` 已经可以稳定当作 signal ledger 使用，且能与 delivery/outcome 做到实用上的 1:1 对账。

### D. live market context

- 主窗口 LP signals `live_public hit rate = 0.0%`，full archive CLI 也是 `0.0%`。
- 对比下午报告：**没有高于 0%**，功能上仍未修复。
- 但 telemetry 有进步：现在至少能从 signals archive 里看到 attempts、venue、endpoint、http status 和 final failure reason。

### E. absorption / broader confirmation

- `local_sell_pressure_absorption`: `36`
- `local_buy_pressure_absorption`: `18`
- `broader_sell_pressure_confirmed`: `0`
- `broader_buy_pressure_confirmed`: `0`
- `pool_only_unconfirmed_pressure`: `42`
- 判断：
  - `local_sell_pressure_absorption` 与 `local_buy_pressure_absorption` 已真实出现，不再只有单一“持续买压/卖压”说法。
  - 但 `broader_sell_pressure_confirmed` / `broader_buy_pressure_confirmed` 都是 `0`，说明 patch 只修好了“局部 vs 未确认”的表达层，还没拿到真实 broader confirmation 样本。

## 7. “卖压后涨 / 买压后跌”专项分析

- confirm 卖压样本：`36`
- confirm 买压样本：`18`
- 反向统计：
  - `持续卖压` 之后 60s 上涨：`0` / `16`
  - `持续卖压` 之后 300s 上涨：`0` / `27`
  - `持续买压` 之后 60s 下跌：`0` / `5`
  - `持续买压` 之后 300s 下跌：`1` / `8`
- 数据限制：当前持久化只稳定回写 60s/300s 结果，缺少可靠的 after-alert 30s 字段；因此 reversal 专题只对 60s/300s 做定量统计。
- 典型反例（样本只有 1 个，不足以列 3 个）：
  - `sig_383b5a5ecb12beb3` | `2026-04-17 17:18:04 UTC` | `ETH/USDT` | `pool_buy_pressure` | `move_before_30s=0.005808` | `move_after_300s=-0.007172`
    - 原始 signal：`买方清扫延续待确认`
    - broader confirmation：`unavailable`；market context：`unavailable` / `http_403`
    - 吸收标签：`local_buy_pressure_absorption`；supporting pairs=`1`；multi_pool=`False`
    - 解释：这是一个 `sweep_building + local_buy_pressure_absorption + market_context unavailable` 的单池样本，更像局部冲击被承接，而不是更广市场确认。
- 明确判断：
  - 昨晚主窗口**没有复现**“持续卖压后上涨”的 confirm 反例；卖压 confirm 在已解析的 60s/300s 样本里没有被打脸。
  - 昨晚只出现 `1` 个“持续买压后 300s 下跌”的 clear counterexample，而且它本身就是 `sweep_building + local_buy_pressure_absorption + unavailable`。
  - 因此，这轮 overnight 更支持的解释不是“代码方向错了”，而是：
    - 首因：`局部池子与 broader market 脱钩 + live market context 缺失`。
    - 次因：`confirm` 仍是确认型信号，容易被误读成预测。
    - 代码错误：本轮数据里**没有直接证据**。

## 8. market context 健康度

- 主窗口 LP signals: `96` | attempts=`768`
- `live_public hit rate`: `0.0%`
- `unavailable rate`: `100.0%`
- per venue：
  - `binance_perp` | signal_hit_rate=`0.0` | attempt_failure_rate=`1.0` | attempts=`384`
  - `bybit_perp` | signal_hit_rate=`0.0` | attempt_failure_rate=`1.0` | attempts=`384`
- per endpoint：
  - `https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDC` | attempt_failure_rate=`1.0` | attempts=`192`
  - `https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDT` | attempt_failure_rate=`1.0` | attempts=`192`
  - `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDC` | attempt_failure_rate=`1.0` | attempts=`192`
  - `https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT` | attempt_failure_rate=`1.0` | attempts=`192`
- per resolved symbol：
  - `ETHUSDC` | signal_hit_rate=`0.0` | signal_total=`48`
  - `ETHUSDT` | signal_hit_rate=`0.0` | signal_total=`48`
- top failure reasons：`[{'key': 'http_451', 'count': 384}, {'key': 'http_403', 'count': 384}]`
- final failure reasons on signal rows：`[{'key': 'http_403', 'count': 96}]`
- symbol fallback records：`[]`
- 解释：
  - ETH majors 上仍然经常失效，昨晚只尝试了 `ETHUSDT` / `ETHUSDC`，没有成功样本。
  - `ETH/USDC -> ETHUSDT`、`BTC/USDC -> BTCUSDT`、`SOL/USDC -> SOLUSDT` 这类 fallback **昨晚没有真实出现**；requested_symbol 与 resolved_symbol 全部相同。
  - full archive CLI 结果与主窗口 LP-only 结论一致：`hit rate = 0.0%`，top failures 为 `http_451`（Binance）与 `http_403`（Bybit）。
- 明确判断：live market context 现在仍然是 **blocker**，还没有进入“可用但需继续优化”的阶段。

## 9. majors 覆盖与样本代表性

- overnight 实际覆盖的 assets：`[{'key': 'ETH', 'count': 96}]`
- overnight 实际覆盖的 pairs：`[{'key': 'ETH/USDC', 'count': 48}, {'key': 'ETH/USDT', 'count': 48}]`
- 主样本是否仍被 ETH/USDT + ETH/USDC 主导：`True`
- CLI major-pool-coverage active pools：`[{'canonical_pair_label': 'ETH/USDC', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'priority': 1, 'sample_size': 97, 'trend_pool_match_mode': 'explicit_whitelist'}, {'canonical_pair_label': 'ETH/USDT', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'priority': 1, 'sample_size': 96, 'trend_pool_match_mode': 'explicit_whitelist'}]`
- 缺失的 expected major pairs：`['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- 判断：
  - majors-first 目前只在“把噪音压在 ETH 双主池”这件事上起了作用。
  - 它还**没有**把样本覆盖从 ETH 扩展到 BTC/SOL，因此代表性仍弱。
  - 这轮样本不能代表“产品总体 LP 表现”，只能代表“ETH 主池场景下 patch 是否变诚实”。

## 10. quality/outcome 与 fastlane ROI

- prealert_precision_score 最差对象：`{'dimension_key': 'ETH/USDC', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.6945, 'climax_reversal_score': 0.231, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.625, 'pool_quality_score': 0.625}`
- confirm_conversion_score 最强对象：`{'dimension_key': 'ETH/USDT', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.7695, 'climax_reversal_score': 0.256, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.665, 'pool_quality_score': 0.665}`
- climax_reversal_score 最高对象：`{'dimension_key': 'ETH/USDT', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.7695, 'climax_reversal_score': 0.256, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.665, 'pool_quality_score': 0.665}`
- market_context_alignment_score 强对象：`{'dimension_key': 'ETH/USDC', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.6945, 'climax_reversal_score': 0.231, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.625, 'pool_quality_score': 0.625}`
- market_context_alignment_score 弱对象：`{'dimension_key': 'ETH/USDC', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.6945, 'climax_reversal_score': 0.231, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.625, 'pool_quality_score': 0.625}`
- fastlane_roi_score 强对象：`{'dimension_key': 'ETH/USDC', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.6945, 'climax_reversal_score': 0.231, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.625, 'pool_quality_score': 0.625}`
- fastlane_roi_score 弱对象：`{'dimension_key': 'ETH/USDC', 'sample_size': 48, 'prealert_precision_score': 0.58, 'confirm_conversion_score': 0.6945, 'climax_reversal_score': 0.231, 'market_context_alignment_score': 0.5, 'fastlane_roi_score': 0.55, 'pair_quality_score': 0.625, 'pool_quality_score': 0.625}`
- promoted_main / fastlane：主窗口 `promoted_main=0`、`primary_trend=0`、`secondary=0`、`extended=0`，full overnight 也没有 promotion。
- 判断：
  - 当前 quality/outcome 已经能区分 `ETH/USDT` 与 `ETH/USDC` 的 confirm_conversion / pair_quality 差异。
  - 但 `market_context_alignment_score` 和 `fastlane_roi_score` 几乎是平的，因为 live context 没命中、promotion 没发生。
  - 因此可以开始指导**ETH 主池内部的小范围调参**，但还不够支持更大范围的全局调参。

## 11. 噪音与误判风险评估

- research 用户：
  - 当前 LP 噪音：`中等`
  - 最大误判来源：prealert 缺席导致研究样本偏后段，且 broader market context 仍缺失
  - 最值得保留：confirm / climax / exhaustion_risk + 完整 archive/reconciliation
  - 最该降级/隐藏：无 broader confirmation 的单池 confirm 应继续显式降级
- trader 用户：
  - 当前 LP 噪音：`中高`
  - 最大误判来源：把 confirm 或 climax 当成预测而不是已发生的链上确认
  - 最值得保留：晚期风险明确的 exhaustion_risk
  - 最该降级/隐藏：sweep_building / late_confirm / unconfirmed_confirm 不应伪装成 clean entry
- 未来 retail 用户：
  - 当前 LP 噪音：`高`
  - 最大误判来源：live market context 缺失下的过度解读
  - 最值得保留：目前不建议直接面向 retail 默认开放
  - 最该降级/隐藏：confirm/climax 默认都需要更强的解释层和过滤层
- 总结判断：
  - 距离“可应用级使用”还有明显距离。
  - 当前最大短板不是单点 timing，而是 **broader confirmation 缺失 + live market context 不可用 + 样本覆盖过窄** 的组合。
  - 在这三个里，最先需要解决的是 live market context；否则 timing 与 broader confirmation 两块都很难闭环。

## 12. 最终评分

- `LP_研究采样_readiness`: `4.4/10`
- `LP_产品信号可用性`: `4.8/10`
- `live_market_context_readiness`: `1.5/10`
- `signal_archive_完整性`: `8.8/10`
- `majors_覆盖代表性`: `2.8/10`
- `噪音控制`: `5.8/10`
- `综合分`: `4.7/10`

## 13. 下一步建议

- Top 5 优点：
  - `archive/signals` 已经稳定，signal / quality / delivery / case_followups 可 1:1 对账。
  - `sweep_building` 不再伪装成高潮，用户可见语义更诚实。
  - `late_confirm / unconfirmed_confirm / chase_risk` 已真正开始把偏晚 confirm 从普通 confirm 里剥离。
  - 局部承接标签已经真实出现，说明 patch 不只是改词面。
  - 送达层闭环稳定：已送达 `32` 条在 cases / followups 里都有完整落点。
- Top 5 当前不足：
  - live market context 仍是 `0% hit rate`。
  - prealert 完全缺席，研究采样仍偏后段。
  - majors 覆盖仍只有 ETH/USDT 与 ETH/USDC。
  - raw/parsed archives 缺失，无法做原始事件级噪音复盘。
  - broader_*_confirmed 标签仍然是 `0`，因为 broader market 根本没连上。
- Top 5 最值得优先改进的事项：
  - 先把 live market context 真正打通，至少让 ETH majors 拿到可用 `live_public`。
  - 为 `ETH/USDC -> ETHUSDT` 一类 fallback 建真实命中样本，不要停留在理论支持。
  - 补上 BTC/SOL 主池，让 majors-first 真正成为 majors-first，而不是 ETH-only。
  - 把 prealert 重新带回来，否则研究型采样缺少前段样本。
  - 恢复 raw / parsed archives，下一轮才能把“为什么没有 prealert”拆到事件级。

## 14. 限制与不确定性

- 这份报告的主窗口只分析了昨晚最连续、最完整的一段，不代表整个文件里每个小尾段都完全同质。
- `after-alert 30s` 没有稳定持久化字段，因此 reversal 专题只能对 `60s / 300s` 做定量。
- `asset_cases.cache.json` 是 active snapshot，不是历史归档，所以主窗口 asset-case 统计主要依赖 signals/quality，而不是 cache 本身。
- `reversal_after_climax` 本轮没有稳定回写，因此 climax reversal 用 adverse move proxy 补充判读。
- `market_context_alignment_score` / `fastlane_roi_score` 在昨晚样本里几乎是平的，不能过度解读。
