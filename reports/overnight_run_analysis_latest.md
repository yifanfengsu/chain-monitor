# Overnight Run Analysis

## 1. 执行摘要

- 主窗口为 `2026-04-21 16:06:03 UTC` 到 `2026-04-22 05:21:54 UTC`，持续 `13.26h`。
- 主窗口共 `661` 条 signals，其中 LP stage rows `196`、已送达 LP 消息 `19`、asset cases `39`、case followups `19`。
- OKX live context 在主窗口 `live_public=196/196`；`kraken_futures` attempts=`0`。
- prealert 在主窗口为 `0`，候选 funnel 为 `candidates=196` `gate_passed=196` `delivered=0`。
- `sweep_building` 样本 `41` 条，显示层残留 `climax/高潮` 为 `0`。
- trade action 分布：`{'CONFLICT_NO_TRADE': 10, 'DO_NOT_CHASE_LONG': 63, 'DO_NOT_CHASE_SHORT': 35, 'LONG_BIAS_OBSERVE': 43, 'LONG_CHASE_ALLOWED': 4, 'SHORT_BIAS_OBSERVE': 39, 'SHORT_CHASE_ALLOWED': 1, 'WAIT_CONFIRMATION': 1}`；可追类总数 `long=4` `short=1`。
- asset state 分布：`{'DO_NOT_CHASE_LONG': 57, 'DO_NOT_CHASE_SHORT': 34, 'LONG_CANDIDATE': 4, 'NO_TRADE_LOCK': 17, 'OBSERVE_LONG': 43, 'OBSERVE_SHORT': 39, 'SHORT_CANDIDATE': 1, 'WAIT_CONFIRMATION': 1}`；state change sent=`0` risk blockers=`19` candidates=`0`。
- no-trade lock: entered=`5` suppressed=`0` released=`179`。
- candidate vs tradeable: candidate=`5` tradeable=`0` 60s_source=`{'okx_mark': 784}`。
- final output 分布：`{'missing': 196}`；verified=`0` candidate=`0` blocked=`0`。
- legacy chase 审计：downgraded=`0` leaked=`4` chase_without_verified=`5` blocked_by_gate=`0`。
- 验证问题：all_opportunity_labels_verified=`False` all_candidate_labels_are_candidate=`True` blocked_covers_legacy_chase_risk=`True`。
- majors 覆盖仍只在 `ETH/USDT` 与 `ETH/USDC`；BTC/SOL 仍缺 pool book 覆盖，无法代表更广 majors。

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-20.ndjson.gz`: exists=`True` records=`4171` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` note=`raw events archive`
- `app/data/archive/raw_events/2026-04-21.ndjson.gz`: exists=`True` records=`43792` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`raw events archive`
- `app/data/archive/raw_events/2026-04-22.ndjson`: exists=`True` records=`49504` range=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` note=`raw events archive`
- `app/data/archive/raw_events/2026-04-23.ndjson`: exists=`True` records=`39649` range=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` note=`raw events archive`
- `app/data/archive/parsed_events/2026-04-20.ndjson.gz`: exists=`True` records=`1921` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` note=`parsed events archive`
- `app/data/archive/parsed_events/2026-04-21.ndjson.gz`: exists=`True` records=`23985` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`parsed events archive`
- `app/data/archive/parsed_events/2026-04-22.ndjson`: exists=`True` records=`22845` range=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` note=`parsed events archive`
- `app/data/archive/parsed_events/2026-04-23.ndjson`: exists=`True` records=`19055` range=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` note=`parsed events archive`
- `data/chain_monitor.sqlite`: exists=`True` records=`2142` range=`2024-03-09 16:00:00 UTC -> 2026-04-23 05:13:15 UTC` note=`sqlite signals mirror via report_data_loader`
- `app/data/archive/cases/2026-04-18.ndjson`: exists=`True` records=`40383` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-19.ndjson`: exists=`True` records=`1309` range=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-20.ndjson.gz`: exists=`True` records=`2378` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-21.ndjson.gz`: exists=`True` records=`34950` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-22.ndjson`: exists=`True` records=`32878` range=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-23.ndjson`: exists=`True` records=`27318` range=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` note=`case archive`
- `app/data/archive/case_followups/2026-04-18.ndjson`: exists=`True` records=`33` range=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson`: exists=`True` records=`2` range=`2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-20.ndjson.gz`: exists=`True` records=`5` range=`2026-04-20 15:21:28 UTC -> 2026-04-20 15:39:05 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-21.ndjson.gz`: exists=`True` records=`67` range=`2026-04-20 16:05:52 UTC -> 2026-04-21 05:27:41 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-22.ndjson`: exists=`True` records=`44` range=`2026-04-21 16:06:52 UTC -> 2026-04-22 03:24:03 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-23.ndjson`: exists=`True` records=`45` range=`2026-04-22 18:36:04 UTC -> 2026-04-23 05:02:06 UTC` note=`case followups archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson`: exists=`True` records=`54936` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson`: exists=`True` records=`2056` range=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-20.ndjson.gz`: exists=`True` records=`4260` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-21.ndjson.gz`: exists=`True` records=`44724` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-22.ndjson`: exists=`True` records=`50620` range=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-23.ndjson`: exists=`True` records=`40820` range=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` note=`delivery audit archive`
- `data/asset_cases.cache.json`: exists=`True` records=`1` range=`2026-04-23 05:01:15 UTC -> 2026-04-23 05:02:25 UTC` note=`asset case snapshot cache`
- `data/lp_quality_stats.cache.json`: exists=`True` records=`800` range=`2026-04-19 13:06:51 UTC -> 2026-04-23 05:07:28 UTC` note=`quality stats cache`
- `data/chain_monitor.sqlite`: exists=`True` records=`567141` range=`n/a -> n/a` note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'schema_meta': 1, 'runs': 0, 'raw_events': 137115, 'parsed_events': 67807, 'signals': 2142, 'signal_features': 9696, 'market_context_snapshots': 2142, 'market_context_attempts': 3203, 'outcomes': 3468, 'asset_cases': 9999, 'asset_market_states': 581, 'no_trade_locks': 501, 'trade_opportunities': 675, 'opportunity_outcomes': 2025, 'quality_stats': 1270, 'telegram_deliveries': 126640, 'prealert_lifecycle': 2494, 'delivery_audit': 197186, 'case_followups': 196} archive_rows_by_category={'raw_events': 137116, 'parsed_events': 67806, 'signals': 2141, 'delivery_audit': 197416, 'cases': 139216, 'case_followups': 196} db_archive_mirror_match_rate=0.9997 archive_fallback_used=False mismatch_warnings=['db_archive_mismatch:signals:sqlite=2142:archive=2141', 'db_archive_mismatch:delivery_audit:sqlite=197186:archive=197416', 'db_archive_mirror_mismatch:raw_events:sqlite=137115:archive=137116', 'db_archive_mirror_mismatch:parsed_events:sqlite=67807:archive=67806', 'db_archive_mirror_mismatch:signals:sqlite=2142:archive=2141', 'db_archive_mirror_mismatch:delivery_audit:sqlite=197186:archive=197416']`
- raw/parsed archive presence: `raw_events=True` `parsed_events=True`。
- outcome windows: `{'30s': {'completed': 194, 'expired': 2}, '60s': {'completed': 194, 'expired': 2}, '300s': {'completed': 194, 'expired': 2}}`。
- telegram suppression: `{'no_trade_opportunity': 143, 'same_blocker_repeat': 31}`。

## 3. overnight 分析窗口

- 主窗口 UTC: `2026-04-21 16:06:03 UTC -> 2026-04-22 05:21:54 UTC`
- 服务器本地: `2026-04-21 16:06:03 UTC -> 2026-04-22 05:21:54 UTC`
- 北京时间: `2026-04-22 00:06:03 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00`
- 东京时间: `2026-04-22 01:06:03 UTC+09:00 -> 2026-04-22 14:21:54 UTC+09:00`
- 选择原因: requested logical date 2026-04-22; selected the longest signal segment whose UTC end date matches it after splitting on >=1h signal gaps.
- 其他段作为附录：
- 附录段: `2024-03-09 16:00:00 UTC -> 2024-03-09 16:00:00 UTC` `duration=0.0h` `signals=1` `lp=0`
- 附录段: `2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` `duration=10.51h` `signals=424` `lp=74`
- 附录段: `2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC` `duration=0.34h` `signals=22` `lp=2`
- 附录段: `2026-04-20 15:03:53 UTC -> 2026-04-21 05:53:12 UTC` `duration=14.82h` `signals=526` `lp=136`
- 主窗口: `2026-04-21 16:06:03 UTC -> 2026-04-22 05:21:54 UTC` `duration=13.26h` `signals=661` `lp=196`
- 附录段: `2026-04-22 18:30:40 UTC -> 2026-04-23 05:13:15 UTC` `duration=10.71h` `signals=508` `lp=153`

## 4. 非敏感运行配置摘要

- `DEFAULT_USER_TIER` = `research`
- `MARKET_CONTEXT_ADAPTER_MODE` = `live`
- `MARKET_CONTEXT_PRIMARY_VENUE` = `okx_perp`
- `MARKET_CONTEXT_SECONDARY_VENUE` = `kraken_futures`
- `OKX_PUBLIC_BASE_URL` = `https://www.okx.com`
- `KRAKEN_FUTURES_BASE_URL` = `https://futures.kraken.com`
- `ARCHIVE_ENABLE_RAW_EVENTS` = `True`
- `ARCHIVE_ENABLE_PARSED_EVENTS` = `True`
- `ARCHIVE_ENABLE_SIGNALS` = `True`
- `ARCHIVE_ENABLE_CASES` = `True`
- `ARCHIVE_ENABLE_CASE_FOLLOWUPS` = `True`
- `ARCHIVE_ENABLE_DELIVERY_AUDIT` = `True`
- `LP_ASSET_CASE_PERSIST_ENABLE` = `True`
- `LP_QUALITY_STATS_ENABLE` = `True`
- `LP_MAJOR_ASSETS` = `['ETH', 'WETH', 'BTC', 'WBTC', 'CBBTC', 'SOL', 'WSOL']`
- `LP_MAJOR_QUOTES` = `['USDT', 'USDC', 'USDC.E']`
- `LP_PREALERT_MIN_USD` = `1500.0`
- `LP_PREALERT_MIN_CONFIRMATION` = `0.36`
- `LP_PREALERT_MIN_PRICING_CONFIDENCE` = `0.62`
- `LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY` = `0.24`
- `LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO` = `1.2`
- `LP_PREALERT_MULTI_POOL_WINDOW_SEC` = `90`
- `LP_PREALERT_FOLLOWUP_WINDOW_SEC` = `60`
- `confirm_downgrade_logic` = `{'late_confirm': ['alert_timing == late', 'market_context unavailable and pool_move_before >= 0.007', 'pool_move_before >= 0.009', 'market_move_before >= 0.008', 'detect_latency_ms >= 4500', 'case_age_sec >= 150', 'quality_gap >= 0.18'], 'chase_risk': ['lp_chase_risk_score >= 0.58', 'pool_move_before >= 0.010 or market_move_before >= 0.010', 'late and no broader confirmation', 'market_context unavailable and single_pool_dominant and pool_move_before >= 0.008', 'detect_latency_ms >= 8000 or case_age_sec >= 240']}`

## 5. LP stage 总览

- `analysis_window_start` = `2026-04-21 16:06:03 UTC`
- `analysis_window_end` = `2026-04-22 05:21:54 UTC`
- `duration_hours` = `13.26`
- `total_signal_rows` = `661`
- `lp_signal_rows` = `196`
- `delivered_lp_signals` = `19`
- `asset_case_count` = `39`
- `case_followup_count` = `19`
- `compression_ratio` = `5.0256`
- `avg_signals_per_case` = `5.0256`
- `stage_distribution_pct` = `{'prealert': 0.0, 'confirm': 64.8, 'climax': 23.98, 'exhaustion_risk': 11.22}`
- 覆盖资产 = `{'ETH': 196}`
- 覆盖 pairs = `{'ETH/USDC': 100, 'ETH/USDT': 96}`

## 6. OKX/Kraken live market context 验证

- 主窗口 `live_public_count=196` `unavailable_count=0`。
- 主窗口 `okx_attempts=1176` `okx_success=1176` `okx_failure=0`。
- 主窗口 `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`。
- 主窗口 `binance_attempts=0` `bybit_attempts=0`。
- 主窗口 requested->resolved = `{'ETHUSDC->ETH-USDT-SWAP': 100, 'ETHUSDT->ETH-USDT-SWAP': 96}`
- CLI full archive live_public_hit_rate = `0.262`
- CLI full archive per_venue = `[{'attempt_failure': 1, 'attempt_failure_rate': 0.0003, 'attempt_hit_rate': 0.9997, 'attempt_status_cache_hit': 33, 'attempt_status_failure': 1, 'attempt_status_success': 3167, 'attempt_success': 3167, 'attempt_total': 3168, 'cache_hit_rate': 0.0588, 'cache_hit_total': 33, 'context_request_hit_rate': 1.0, 'signal_hit_rate': 1.0, 'signal_success': 561, 'signal_total': 561, 'timeout_count': 1, 'venue': 'okx_perp', 'venue_attempt_success_rate': 0.9997}]`
- 判断：OKX 主路径已在真实 overnight 样本中生效；Kraken fallback 未被触发，所以只能确认配置已切到二级位，不能确认其夜间实战成功率。

## 7. prealert 真实表现

- `prealert_count=0` `major_prealert_count=0` `non_major_prealert_count=0`
- `prealert_to_confirm_30s=None`
- `prealert_to_confirm_60s=None`
- `prealert_to_confirm_90s=None`
- 判断：主窗口没有 prealert，所以 non-major guard 只能以“没有漏出 non-major prealert”来确认，无法证明 majors prealert 已恢复。

## 8. confirm local/broader/late/chase 分析

- `confirm_count` = `127`
- `clean_confirm_count` = `9`
- `local_confirm_count` = `54`
- `broader_confirm_count` = `32`
- `late_confirm_count` = `47`
- `chase_risk_count` = `0`
- `unconfirmed_confirm_count` = `30`
- `blank_confirm_quality_count` = `41`
- `broader_alignment_confirmed_count` = `55`
- `predict_warning_text_count` = `29`
- `confirm_move_before_30s_median` = `0.00046`
- `confirm_move_after_60s_median` = `0.000259`
- `confirm_move_after_300s_median` = `-8.6e-05`
- 判断：confirm 现在明显更诚实。夜间样本里 `23` 条被写成 `local_confirm`，`0` 条被写成 `broader_confirm`，说明系统没有把局部池子压力硬写成更广确认。
- 但仍有 `14` 条 confirm 属于 sweep 语义，因此不会落在标准 confirm_quality/scope 分类里；这部分需要与 sweep 段一起看。

## 9. sweep_building / sweep_confirmed / exhaustion 分析

- `sweep_building_count` = `41`
- `sweep_confirmed_count` = `47`
- `sweep_exhaustion_risk_count` = `22`
- `sweep_building_display_climax_residual_count` = `0`
- `sweep_building_to_sweep_confirmed_rate` = `0.6585`
- `sweep_building_to_continue_rate` = `0.8049`
- `sweep_reversal_60s` = `{'resolved_count': 47, 'adverse_count': 16, 'adverse_rate': 0.3404}`
- `sweep_reversal_300s` = `{'resolved_count': 47, 'adverse_count': 22, 'adverse_rate': 0.4681}`
- `sweep_exhaustion_outcome_300s` = `{'resolved_count': 22, 'adverse_count': 8, 'adverse_rate': 0.3636}`
- `direction_performance` = `{'buy_pressure': {'count': 57, 'move_after_60s_median': 0.000707, 'move_after_300s_median': 0.000368, 'adverse_60s_rate': 0.2364, 'adverse_300s_rate': 0.4364}, 'sell_pressure': {'count': 53, 'move_after_60s_median': 0.001131, 'move_after_300s_median': 0.000581, 'adverse_60s_rate': 0.3396, 'adverse_300s_rate': 0.3962}}`
- 判断：`sweep_building` 在显示层彻底不再冒充高潮。`sweep_confirmed` 的主窗口 300s 已解析样本里没有出现反向；`sweep_exhaustion_risk` 300s 解析样本里出现了部分反向，但样本很少。

## 10. trade_action 层评估

- `trade_action_distribution={'CONFLICT_NO_TRADE': 10, 'DO_NOT_CHASE_LONG': 63, 'DO_NOT_CHASE_SHORT': 35, 'LONG_BIAS_OBSERVE': 43, 'LONG_CHASE_ALLOWED': 4, 'SHORT_BIAS_OBSERVE': 39, 'SHORT_CHASE_ALLOWED': 1, 'WAIT_CONFIRMATION': 1}`
- `long_chase_allowed_count=4` `short_chase_allowed_count=1`
- `no_trade_count=0` `wait_confirmation_count=1`
- `do_not_chase_long_count=63` `do_not_chase_short_count=35`
- `conflict_no_trade_count=10` `data_gap_no_trade_count=0`
- `chase_allowed_success_rate=0.4` `chase_allowed_adverse_rate=0.6`
- `generic_confirm_success_rate_300s=0.4884` `generic_confirm_adverse_rate_300s=0.5116`
- `no_trade_would_have_saved_rate=0.6`
- `conflict_after_message_reversal_rate=0.6`
- 判断 1：`LONG/SHORT_CHASE_ALLOWED` 必须始终是少数样本；计数过高说明动作层仍过于宽松。
- 判断 2：如果 `chase_allowed_success_rate` 明显高于 generic confirm，说明严格 chase gate 确实带来了后验提升。
- 判断 3：`do_not_chase_*` 与 `no_trade_would_have_saved_rate` 可以用来估算系统是否减少了不利追单。
- 判断 4：`conflict_no_trade_count` 与 `conflict_after_message_reversal_rate` 用来验证双向噪音时 abstain 是否合理。
- 判断 5：trade_action 把 Telegram 首行从结构词改成动作词，本质上是在降低误用而不是增加方向幻觉。

## 11. “卖压后涨 / 买压后跌”反例专项

- `sell_confirm_count=40` `buy_confirm_count=87`
- `sell_after_30s_rise_ratio=0.45`
- `sell_after_60s={'resolved_count': 40, 'against_count': 19, 'against_rate': 0.475}`
- `sell_after_300s={'resolved_count': 40, 'against_count': 20, 'against_rate': 0.5}`
- `buy_after_30s_fall_ratio=0.3765`
- `buy_after_60s={'resolved_count': 85, 'against_count': 33, 'against_rate': 0.3882}`
- `buy_after_300s={'resolved_count': 85, 'against_count': 46, 'against_rate': 0.5412}`
- `reason_distribution={'possible_code_misclassification': 4, 'local_confirm_not_broader': 1, 'single_pool_or_low_resonance': 1, 'local_sell_pressure_absorption': 1}`
- 典型反例：
- `{'signal_id': 'sig_524ae9b0286175e8', 'asset_case_id': 'asset_case:ETH:872189f973', 'pair': 'ETH/USDC', 'stage': 'confirm', 'confirm_quality': '(blank)', 'absorption_context': 'pool_only_unconfirmed_pressure', 'market_context_source': 'live_public', 'move_before': 0.001062, 'move_after': -0.001272, 'judgement_reason': 'possible_code_misclassification'}`
- `{'signal_id': 'sig_9f2ac0d2223391bd', 'asset_case_id': 'asset_case:ETH:872189f973', 'pair': 'ETH/USDC', 'stage': 'confirm', 'confirm_quality': '(blank)', 'absorption_context': 'broader_buy_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': 0.001629, 'move_after': -0.0014, 'judgement_reason': 'possible_code_misclassification'}`
- `{'signal_id': 'sig_ddbc82785f48aaf3', 'asset_case_id': 'asset_case:ETH:9e7ac33f6a', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': 'unconfirmed_confirm', 'absorption_context': 'local_sell_pressure_absorption', 'market_context_source': 'live_public', 'move_before': -0.006329, 'move_after': 4.3e-05, 'judgement_reason': 'local_confirm_not_broader, single_pool_or_low_resonance, local_sell_pressure_absorption'}`
- `{'signal_id': 'sig_706f934f1f4f1dea', 'asset_case_id': 'asset_case:ETH:16f560a48b', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': '(blank)', 'absorption_context': 'broader_sell_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': -0.000729, 'move_after': 0.002084, 'judgement_reason': 'possible_code_misclassification'}`
- `{'signal_id': 'sig_5553c0c71ec3ffaa', 'asset_case_id': 'asset_case:ETH:b8c3cbe4c2', 'pair': 'ETH/USDC', 'stage': 'confirm', 'confirm_quality': '(blank)', 'absorption_context': 'broader_buy_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': 0.003312, 'move_after': -0.00061, 'judgement_reason': 'possible_code_misclassification'}`
- 限制：主窗口没有可靠的 `30s` 数值回写，`60s` 数值也几乎为空，所以本专题只能对 `300s` 做定量结论。

## 12. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `eth_signal_count=196` `btc_signal_count=0` `sol_signal_count=0`
- `major_cli_summary={'active_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDC 池，默认启用。', 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 419, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'ETH/USDT', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDT 池，默认启用。', 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 381, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'BTC/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '预留 WBTC/USDC 池条目，建议上线前校验地址后再启用。', 'pair_label': 'WBTC/USDC', 'pool_address': '0x004375dff511095cc5a197a54140a24efef3a416', 'pool_type': 'spot_lp', 'priority': 2, 'protocol': 'uniswap_v2', 'sample_size': 0, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}], 'active_recent_major_pairs': ['ETH/USDC', 'ETH/USDT'], 'configured_but_disabled_major_pools': [], 'configured_but_unsupported_chain': [{'chain': 'solana', 'configured_pair_label': 'SOL/USDC', 'dex': 'Orca', 'enabled': True, 'example_section': '', 'index': 7, 'pair_label': 'SOL/USDC', 'pool_address': 'Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE', 'pool_type': 'clmm', 'priority': 10, 'protocol': 'orca_whirlpool', 'quote_canonical': 'USDC', 'reason': 'unsupported_chain', 'source_note': 'GeckoTerminal + Solscan'}, {'chain': 'solana', 'configured_pair_label': 'SOL/USDC', 'dex': 'Raydium', 'enabled': True, 'example_section': '', 'index': 8, 'pair_label': 'SOL/USDC', 'pool_address': '58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2', 'pool_type': 'clmm', 'priority': 11, 'protocol': 'raydium', 'quote_canonical': 'USDC', 'reason': 'unsupported_chain', 'source_note': 'GeckoTerminal'}, {'chain': 'base', 'configured_pair_label': 'SOL/USDC', 'dex': 'Uniswap', 'enabled': True, 'example_section': '', 'index': 9, 'pair_label': 'SOL/USDC', 'pool_address': '0x1cca8388e671e83010843a1a0535b04f2d7e946b', 'pool_type': 'clmm', 'priority': 12, 'protocol': 'uniswap_v3', 'quote_canonical': 'USDC', 'reason': 'unsupported_chain', 'source_note': 'GeckoTerminal'}], 'configured_but_validation_failed': [{'chain': 'ethereum', 'configured_pair_label': 'WBTC/USDC', 'dex': 'Uniswap', 'example_section': '', 'fee_tier': 3000, 'index': 3, 'pair_label': 'BTC/USDC', 'pool_address': '0x99ac8cA7087fA4A2A1FB6357269965A2014ABc35', 'pool_type': 'clmm', 'priority': 1, 'protocol': 'uniswap_v3', 'source_note': 'GeckoTerminal + Uniswap App', 'validation_reasons': ['rpc_validation_error:ConnectionError'], 'validation_status': 'unavailable'}, {'chain': 'ethereum', 'configured_pair_label': 'WBTC/USDC', 'dex': 'Uniswap', 'example_section': '', 'fee_tier': 500, 'index': 4, 'pair_label': 'BTC/USDC', 'pool_address': '0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16', 'pool_type': 'clmm', 'priority': 3, 'protocol': 'uniswap_v3', 'source_note': 'Etherscan + GeckoTerminal', 'validation_reasons': ['rpc_validation_error:ConnectionError'], 'validation_status': 'unavailable'}, {'chain': 'ethereum', 'configured_pair_label': 'WBTC/USDT', 'dex': 'Uniswap', 'example_section': '', 'fee_tier': 3000, 'index': 5, 'pair_label': 'BTC/USDT', 'pool_address': '0x9Db9e0e53058C89e5B94e29621a205198648425B', 'pool_type': 'clmm', 'priority': 2, 'protocol': 'uniswap_v3', 'source_note': 'Etherscan + GeckoTerminal', 'validation_reasons': ['rpc_validation_error:ConnectionError'], 'validation_status': 'unavailable'}, {'chain': 'ethereum', 'configured_pair_label': 'CBBTC/USDC', 'dex': 'Uniswap', 'example_section': '', 'fee_tier': 3000, 'index': 6, 'pair_label': 'BTC/USDC', 'pool_address': '0x4548280AC92507C9092a511C7396Cbea78FA9E49', 'pool_type': 'clmm', 'priority': 4, 'protocol': 'uniswap_v3', 'source_note': 'Etherscan', 'validation_reasons': ['rpc_validation_error:ConnectionError'], 'validation_status': 'unavailable'}], 'configured_enabled_no_recent_signal_major_pairs': ['BTC/USDC'], 'configured_major_assets': ['ETH', 'BTC', 'SOL'], 'configured_major_quotes': ['USDT', 'USDC'], 'covered_expected_pairs': ['BTC/USDC', 'ETH/USDC', 'ETH/USDT'], 'covered_major_pairs': ['BTC/USDC', 'ETH/USDC', 'ETH/USDT'], 'covered_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDC 池，默认启用。', 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 419, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'ETH/USDT', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDT 池，默认启用。', 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 381, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'BTC/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '预留 WBTC/USDC 池条目，建议上线前校验地址后再启用。', 'pair_label': 'WBTC/USDC', 'pool_address': '0x004375dff511095cc5a197a54140a24efef3a416', 'pool_type': 'spot_lp', 'priority': 2, 'protocol': 'uniswap_v2', 'sample_size': 0, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}], 'duplicate_pool_warnings': [], 'enabled_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDC 池，默认启用。', 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 419, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'ETH/USDT', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '主流 ETH/USDT 池，默认启用。', 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'pool_type': 'spot_lp', 'priority': 1, 'protocol': 'uniswap_v2', 'sample_size': 381, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}, {'canonical_pair_label': 'BTC/USDC', 'chain': 'ethereum', 'dex': 'Uniswap V2', 'fee_tier': None, 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'notes': '预留 WBTC/USDC 池条目，建议上线前校验地址后再启用。', 'pair_label': 'WBTC/USDC', 'pool_address': '0x004375dff511095cc5a197a54140a24efef3a416', 'pool_type': 'spot_lp', 'priority': 2, 'protocol': 'uniswap_v2', 'sample_size': 0, 'trend_pool_match_mode': '', 'validation_reasons': [], 'validation_status': 'not_required'}], 'expected_major_pairs': ['BTC/USDC', 'BTC/USDT', 'ETH/USDC', 'ETH/USDT', 'SOL/USDC', 'SOL/USDT'], 'major_pair_quality': [{'actionable': False, 'climax_reversal_score': 0.45, 'covered': True, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDC', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDT', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': True, 'climax_reversal_score': 0.3155, 'covered': True, 'market_context_alignment_score': 0.9571, 'pair_label': 'ETH/USDC', 'quality_score': 0.6255, 'sample_size': 419}, {'actionable': True, 'climax_reversal_score': 0.2513, 'covered': True, 'market_context_alignment_score': 0.9602, 'pair_label': 'ETH/USDT', 'quality_score': 0.6021, 'sample_size': 381}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDC', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDT', 'quality_score': 0.5864, 'sample_size': 0}], 'malformed_major_pool_entries': [], 'missing_expected_pairs': ['SOL/USDT'], 'missing_major_assets': ['SOL'], 'missing_major_pairs': ['SOL/USDT'], 'next_round_priority': [{'pair_label': 'SOL/USDT', 'reason': 'missing'}, {'pair_label': 'SOL/USDC', 'reason': 'configured_but_unsupported_chain'}, {'pair_label': 'BTC/USDC', 'reason': 'validation_failed'}, {'pair_label': 'BTC/USDT', 'reason': 'validation_failed'}, {'pair_label': 'BTC/USDC', 'reason': 'enabled_no_recent_signal'}], 'no_recent_signal_major_pairs': ['BTC/USDC'], 'placeholder_major_pool_entries': [], 'pool_book_exists': True, 'pool_book_path': '/run-project/chain-monitor/data/lp_pools.json', 'quality_converging_major_pairs': [{'actionable': True, 'climax_reversal_score': 0.3155, 'covered': True, 'market_context_alignment_score': 0.9571, 'pair_label': 'ETH/USDC', 'quality_score': 0.6255, 'sample_size': 419}, {'actionable': True, 'climax_reversal_score': 0.2513, 'covered': True, 'market_context_alignment_score': 0.9602, 'pair_label': 'ETH/USDT', 'quality_score': 0.6021, 'sample_size': 381}], 'recommended_local_config_actions': ['补齐本地 data/lp_pools.json：优先补 SOL/USDT', 'Base/Solana pools 需要对应链 listener/RPC；未支持前不会进入 Ethereum active scan', 'enabled 的 Uniswap v3 major pools 需先通过 token0/token1/fee/factory 校验，再进入 majors 覆盖', '已启用但无最近信号：BTC/USDC；先确认 listener 已扫描到这些池'], 'recommended_next_round_pairs': ['SOL/USDT', 'SOL/USDC', 'BTC/USDC', 'BTC/USDT'], 'suggestions': ['优先补齐：SOL/USDT', '当前 majors outcome 样本仍偏少，建议先扩 BTC/SOL/更多 ETH 主池', '样本稀少的 majors pairs：BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT', '确认 listener 是否扫描到对应 pool，并检查链、DEX、pool_type、enabled 配置是否正确', '补齐本地 data/lp_pools.json：优先补 SOL/USDT', 'Base/Solana pools 需要对应链 listener/RPC；未支持前不会进入 Ethereum active scan', 'enabled 的 Uniswap v3 major pools 需先通过 token0/token1/fee/factory 校验，再进入 majors 覆盖'], 'under_sampled_major_assets': [{'asset_symbol': 'BTC', 'sample_size': 0}, {'asset_symbol': 'SOL', 'sample_size': 0}], 'under_sampled_major_pairs': [{'pair_label': 'BTC/USDC', 'sample_size': 0}, {'pair_label': 'BTC/USDT', 'sample_size': 0}, {'pair_label': 'SOL/USDC', 'sample_size': 0}, {'pair_label': 'SOL/USDT', 'sample_size': 0}], 'warnings': ['majors 主池覆盖不完整，下一轮应优先补 majors 而不是 long-tail', '缺少 major 资产覆盖：SOL', '存在 enabled 但最近无信号的 major pairs：BTC/USDC', '存在 configured but unsupported 的 major pools；这些条目不会进入当前 Ethereum active scan', '存在 enabled 但未通过 validator 的 major pools；这些条目不会计入 covered majors']}`
- 判断：主窗口仍然只来自 ETH 双主池。CLI 同时确认 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC` 属于 pool book 覆盖缺口，而不是夜里单纯无事件。

## 13. signal archive 对账完整性

- `signals_archive_exists` = `True`
- `signal_delivery_audit_match_rate` = `1.0`
- `signal_case_followup_match_rate` = `1.0`
- `signal_case_attached_match_rate` = `1.0`
- `delivered_signal_notifier_sent_at_rate` = `1.0`
- `signal_id_complete_rate` = `1.0`
- `asset_case_id_complete_rate` = `1.0`
- `outcome_tracking_key_complete_rate` = `1.0`
- `mixed_flat_nested_rows` = `0`
- `flat_nested_stage_mismatch_rows` = `0`
- `signal_ids` = `196`
- `delivered_signal_ids` = `19`
- 判断：`signals -> delivery_audit -> cases.signal_attached -> case_followups` 在已送达 LP 子集上都是 1:1。flat/new 与 nested/old 格式并存，但 stage 字段未发现冲突。

## 14. quality/outcome 与 fastlane ROI

- `fastlane_promoted_count_window=0`
- `fastlane_promoted_delivered_count_window=0`
- `resolved_move_after_60s_count_window=194`
- `resolved_move_after_300s_count_window=194`
- `full_summary_cli.overall={'actionable': True, 'climax_count': 210, 'climax_reversal_count': 38, 'climax_reversal_score': 0.2794, 'confirm_conversion_score': 0.2908, 'confirm_count': 493, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 0, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776920536, 'market_context_alignment_positive_count': 796, 'market_context_alignment_score': 0.9603, 'market_context_available_count': 800, 'prealert_confirmed_count': 1, 'prealert_count': 2, 'prealert_false_count': 0, 'prealert_precision_score': 0.685, 'quality_hint': '历史一般', 'quality_score': 0.6287, 'resolved_climaxes': 145, 'resolved_confirms': 357, 'resolved_fastlanes': 0, 'resolved_prealerts': 1, 'sample_size': 800, 'tuning_hint': '保持当前阈值'}`
- 判断：quality/outcome ledger 已能支撑对账和 pair-level 对比，但夜间 fastlane 与 60s outcome 样本仍偏薄。

## 15. 噪音与误判风险评估

- `delivered_ratio` = `0.0969`
- `local_confirm_share` = `0.4252`
- `unresolved_300s_share` = `0.0102`
- `exhaustion_risk_count` = `22`
- `reverse_case_count` = `5`
- 判断：噪音的主要来源已不再是 market context unavailable，而是 `ETH-only sample + no prealert + sparse 60s/300s resolved outcomes`。

## 16. 最终评分

- `research_sampling_readiness` = `3.8/10`
- `live_market_context_readiness` = `9.4/10`
- `prealert_effectiveness` = `0.8/10`
- `confirm_honesty_non_misleading_quality` = `7.0/10`
- `sweep_quality` = `8.0/10`
- `majors_coverage` = `3.2/10`
- `signal_archive_integrity` = `9.2/10`
- `noise_control` = `6.2/10`
- `overall_self_use_score` = `6.0/10`

## 16. 下一轮建议

- 首优先：补齐 BTC/SOL majors pool book，让 overnight 不再只有 ETH。
- 第二优先：让 majors prealert 在真实夜间重新出现，否则连续研究仍偏后段确认样本。
- 第三优先：保留 OKX 主路径，但补一个可重复触发的 kraken fallback 健康检查，因为主窗口没有用到它。
- 第四优先：增强 30s/60s outcome 回写，解决“反向 K 线”专题定量盲区。

## 17. 限制与不确定性

- 本报告严格使用主窗口数据；主窗口之外的白天/下午样本只用于附录和 CLI 对照。
- `raw_events`/`parsed_events` availability = `raw:True` `parsed:True`；若缺失，就无法把 BTC/SOL 无样本彻底拆成“没有事件”还是“扫描未命中”。
- outcome window status = `{'30s': {'completed': 194, 'expired': 2}, '60s': {'completed': 194, 'expired': 2}, '300s': {'completed': 194, 'expired': 2}}`；若 `30s/60s` completed 仍少，相关结论必须保守。
