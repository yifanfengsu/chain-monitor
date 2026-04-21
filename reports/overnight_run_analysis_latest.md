# Overnight Run Analysis

## 1. 执行摘要

- 主窗口为 `2026-04-20 15:03:53 UTC` 到 `2026-04-21 05:53:12 UTC`，持续 `14.82h`。
- 主窗口共 `526` 条 signals，其中 LP stage rows `136`、已送达 LP 消息 `7`、asset cases `44`、case followups `7`。
- OKX live context 在主窗口 `live_public=136/136`；`kraken_futures` attempts=`0`。
- prealert 在主窗口为 `1`，候选 funnel 为 `candidates=136` `gate_passed=136` `delivered=0`。
- `sweep_building` 样本 `35` 条，显示层残留 `climax/高潮` 为 `0`。
- trade action 分布：`{'CONFLICT_NO_TRADE': 8, 'DO_NOT_CHASE_LONG': 31, 'DO_NOT_CHASE_SHORT': 33, 'LONG_BIAS_OBSERVE': 38, 'SHORT_BIAS_OBSERVE': 24, 'SHORT_CHASE_ALLOWED': 1, 'WAIT_CONFIRMATION': 1}`；可追类总数 `long=0` `short=1`。
- asset state 分布：`{'DO_NOT_CHASE_LONG': 30, 'DO_NOT_CHASE_SHORT': 29, 'NO_TRADE_CHOP': 3, 'NO_TRADE_LOCK': 13, 'OBSERVE_LONG': 37, 'OBSERVE_SHORT': 22, 'SHORT_CANDIDATE': 1, 'WAIT_CONFIRMATION': 1}`；state change sent=`0` risk blockers=`7` candidates=`0`。
- no-trade lock: entered=`4` suppressed=`0` released=`123`。
- candidate vs tradeable: candidate=`1` tradeable=`0` 60s_source=`{'okx_mark': 544}`。
- majors 覆盖仍只在 `ETH/USDT` 与 `ETH/USDC`；BTC/SOL 仍缺 pool book 覆盖，无法代表更广 majors。

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-20.ndjson`: exists=`True` records=`4171` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` note=`raw events archive`
- `app/data/archive/raw_events/2026-04-21.ndjson`: exists=`True` records=`43792` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`raw events archive`
- `app/data/archive/parsed_events/2026-04-20.ndjson`: exists=`True` records=`1921` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` note=`parsed events archive`
- `app/data/archive/parsed_events/2026-04-21.ndjson`: exists=`True` records=`23985` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`parsed events archive`
- `data/chain_monitor.sqlite`: exists=`True` records=`972` range=`2026-04-18 05:26:52 UTC -> 2026-04-21 05:53:12 UTC` note=`sqlite signals mirror via report_data_loader`
- `app/data/archive/cases/2026-04-18.ndjson`: exists=`True` records=`40383` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-19.ndjson`: exists=`True` records=`1309` range=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-20.ndjson`: exists=`True` records=`2378` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-21.ndjson`: exists=`True` records=`34950` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`case archive`
- `app/data/archive/case_followups/2026-04-18.ndjson`: exists=`True` records=`33` range=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson`: exists=`True` records=`2` range=`2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-20.ndjson`: exists=`True` records=`5` range=`2026-04-20 15:21:28 UTC -> 2026-04-20 15:39:05 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-21.ndjson`: exists=`True` records=`67` range=`2026-04-20 16:05:52 UTC -> 2026-04-21 05:27:41 UTC` note=`case followups archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson`: exists=`True` records=`54936` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson`: exists=`True` records=`2056` range=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-20.ndjson`: exists=`True` records=`4260` range=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-21.ndjson`: exists=`True` records=`44724` range=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` note=`delivery audit archive`
- `data/asset_cases.cache.json`: exists=`True` records=`1` range=`2026-04-21 05:08:38 UTC -> 2026-04-21 05:08:56 UTC` note=`asset case snapshot cache`
- `data/lp_quality_stats.cache.json`: exists=`True` records=`791` range=`2026-04-17 06:16:14 UTC -> 2026-04-21 05:08:58 UTC` note=`quality stats cache`
- `data/chain_monitor.sqlite`: exists=`True` records=`248955` range=`n/a -> n/a` note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'schema_meta': 1, 'runs': 0, 'raw_events': 47964, 'parsed_events': 25907, 'signals': 972, 'signal_features': 3208, 'market_context_snapshots': 972, 'market_context_attempts': 1107, 'outcomes': 2421, 'asset_cases': 1, 'asset_market_states': 223, 'no_trade_locks': 145, 'trade_opportunities': 150, 'opportunity_outcomes': 450, 'quality_stats': 816, 'telegram_deliveries': 57563, 'prealert_lifecycle': 972, 'delivery_audit': 105976, 'case_followups': 107} archive_rows_by_category={'raw_events': 47963, 'parsed_events': 25906, 'signals': 972, 'delivery_audit': 105976, 'cases': 79020, 'case_followups': 107} db_archive_mirror_match_rate=1.0 archive_fallback_used=False mismatch_warnings=['db_archive_mirror_mismatch:raw_events:sqlite=47964:archive=47963', 'db_archive_mirror_mismatch:parsed_events:sqlite=25907:archive=25906']`
- raw/parsed archive presence: `raw_events=True` `parsed_events=True`。
- outcome windows: `{'30s': {'completed': 62, 'expired': 72, 'pending': 2}, '60s': {'expired': 78, 'completed': 56, 'pending': 2}, '300s': {'expired': 134, 'pending': 2}}`。
- telegram suppression: `{'no_trade_opportunity': 121, 'same_blocker_repeat': 6}`。

## 3. overnight 分析窗口

- 主窗口 UTC: `2026-04-20 15:03:53 UTC -> 2026-04-21 05:53:12 UTC`
- 服务器本地: `2026-04-20 15:03:53 UTC -> 2026-04-21 05:53:12 UTC`
- 北京时间: `2026-04-20 23:03:53 UTC+08:00 -> 2026-04-21 13:53:12 UTC+08:00`
- 东京时间: `2026-04-21 00:03:53 UTC+09:00 -> 2026-04-21 14:53:12 UTC+09:00`
- 选择原因: latest segment with the longest continuous signal activity after splitting on >=1h signal gaps; it also has the highest overnight LP row count.
- 其他段作为附录：
- 附录段: `2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` `duration=10.51h` `signals=424` `lp=74`
- 附录段: `2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC` `duration=0.34h` `signals=22` `lp=2`
- 主窗口: `2026-04-20 15:03:53 UTC -> 2026-04-21 05:53:12 UTC` `duration=14.82h` `signals=526` `lp=136`

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

- `analysis_window_start` = `2026-04-20 15:03:53 UTC`
- `analysis_window_end` = `2026-04-21 05:53:12 UTC`
- `duration_hours` = `14.82`
- `total_signal_rows` = `526`
- `lp_signal_rows` = `136`
- `delivered_lp_signals` = `7`
- `asset_case_count` = `44`
- `case_followup_count` = `7`
- `compression_ratio` = `3.0909`
- `avg_signals_per_case` = `3.0909`
- `stage_distribution_pct` = `{'prealert': 0.74, 'confirm': 55.88, 'climax': 27.94, 'exhaustion_risk': 15.44}`
- 覆盖资产 = `{'ETH': 136}`
- 覆盖 pairs = `{'ETH/USDC': 69, 'ETH/USDT': 67}`

## 6. OKX/Kraken live market context 验证

- 主窗口 `live_public_count=136` `unavailable_count=0`。
- 主窗口 `okx_attempts=741` `okx_success=740` `okx_failure=1`。
- 主窗口 `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`。
- 主窗口 `binance_attempts=0` `bybit_attempts=0`。
- 主窗口 requested->resolved = `{'ETHUSDC->ETH-USDT-SWAP': 69, 'ETHUSDT->ETH-USDT-SWAP': 67}`
- CLI full archive live_public_hit_rate = `0.2181`
- CLI full archive per_venue = `[{'attempt_failure': 1, 'attempt_failure_rate': 0.0009, 'attempt_hit_rate': 0.9991, 'attempt_status_cache_hit': 33, 'attempt_status_failure': 1, 'attempt_status_success': 1073, 'attempt_success': 1073, 'attempt_total': 1074, 'cache_hit_rate': 0.1557, 'cache_hit_total': 33, 'context_request_hit_rate': 1.0, 'signal_hit_rate': 1.0, 'signal_success': 212, 'signal_total': 212, 'timeout_count': 1, 'venue': 'okx_perp', 'venue_attempt_success_rate': 0.9991}]`
- 判断：OKX 主路径已在真实 overnight 样本中生效；Kraken fallback 未被触发，所以只能确认配置已切到二级位，不能确认其夜间实战成功率。

## 7. prealert 真实表现

- `prealert_count=1` `major_prealert_count=1` `non_major_prealert_count=0`
- `prealert_to_confirm_30s=0.0`
- `prealert_to_confirm_60s=1.0`
- `prealert_to_confirm_90s=1.0`
- 判断：主窗口没有 prealert，所以 non-major guard 只能以“没有漏出 non-major prealert”来确认，无法证明 majors prealert 已恢复。

## 8. confirm local/broader/late/chase 分析

- `confirm_count` = `76`
- `clean_confirm_count` = `2`
- `local_confirm_count` = `37`
- `broader_confirm_count` = `4`
- `late_confirm_count` = `16`
- `chase_risk_count` = `0`
- `unconfirmed_confirm_count` = `23`
- `blank_confirm_quality_count` = `35`
- `broader_alignment_confirmed_count` = `11`
- `predict_warning_text_count` = `22`
- `confirm_move_before_30s_median` = `-0.000111`
- `confirm_move_after_60s_median` = `0.000194`
- `confirm_move_after_300s_median` = `None`
- 判断：confirm 现在明显更诚实。夜间样本里 `23` 条被写成 `local_confirm`，`0` 条被写成 `broader_confirm`，说明系统没有把局部池子压力硬写成更广确认。
- 但仍有 `14` 条 confirm 属于 sweep 语义，因此不会落在标准 confirm_quality/scope 分类里；这部分需要与 sweep 段一起看。

## 9. sweep_building / sweep_confirmed / exhaustion 分析

- `sweep_building_count` = `35`
- `sweep_confirmed_count` = `38`
- `sweep_exhaustion_risk_count` = `21`
- `sweep_building_display_climax_residual_count` = `0`
- `sweep_building_to_sweep_confirmed_rate` = `0.3429`
- `sweep_building_to_continue_rate` = `0.6571`
- `sweep_reversal_60s` = `{'resolved_count': 19, 'adverse_count': 8, 'adverse_rate': 0.4211}`
- `sweep_reversal_300s` = `{'resolved_count': 0, 'adverse_count': 0, 'adverse_rate': None}`
- `sweep_exhaustion_outcome_300s` = `{'resolved_count': 0, 'adverse_count': 0, 'adverse_rate': None}`
- `direction_performance` = `{'buy_pressure': {'count': 58, 'move_after_60s_median': 0.001843, 'move_after_300s_median': None, 'adverse_60s_rate': 0.2083, 'adverse_300s_rate': None}, 'sell_pressure': {'count': 36, 'move_after_60s_median': -0.000316, 'move_after_300s_median': None, 'adverse_60s_rate': 0.625, 'adverse_300s_rate': None}}`
- 判断：`sweep_building` 在显示层彻底不再冒充高潮。`sweep_confirmed` 的主窗口 300s 已解析样本里没有出现反向；`sweep_exhaustion_risk` 300s 解析样本里出现了部分反向，但样本很少。

## 10. trade_action 层评估

- `trade_action_distribution={'CONFLICT_NO_TRADE': 8, 'DO_NOT_CHASE_LONG': 31, 'DO_NOT_CHASE_SHORT': 33, 'LONG_BIAS_OBSERVE': 38, 'SHORT_BIAS_OBSERVE': 24, 'SHORT_CHASE_ALLOWED': 1, 'WAIT_CONFIRMATION': 1}`
- `long_chase_allowed_count=0` `short_chase_allowed_count=1`
- `no_trade_count=0` `wait_confirmation_count=1`
- `do_not_chase_long_count=31` `do_not_chase_short_count=33`
- `conflict_no_trade_count=8` `data_gap_no_trade_count=0`
- `chase_allowed_success_rate=None` `chase_allowed_adverse_rate=None`
- `generic_confirm_success_rate_300s=None` `generic_confirm_adverse_rate_300s=None`
- `no_trade_would_have_saved_rate=None`
- `conflict_after_message_reversal_rate=0.0`
- 判断 1：`LONG/SHORT_CHASE_ALLOWED` 必须始终是少数样本；计数过高说明动作层仍过于宽松。
- 判断 2：如果 `chase_allowed_success_rate` 明显高于 generic confirm，说明严格 chase gate 确实带来了后验提升。
- 判断 3：`do_not_chase_*` 与 `no_trade_would_have_saved_rate` 可以用来估算系统是否减少了不利追单。
- 判断 4：`conflict_no_trade_count` 与 `conflict_after_message_reversal_rate` 用来验证双向噪音时 abstain 是否合理。
- 判断 5：trade_action 把 Telegram 首行从结构词改成动作词，本质上是在降低误用而不是增加方向幻觉。

## 11. “卖压后涨 / 买压后跌”反例专项

- `sell_confirm_count=39` `buy_confirm_count=37`
- `sell_after_30s_rise_ratio=0.3158`
- `sell_after_60s={'resolved_count': 18, 'against_count': 6, 'against_rate': 0.3333}`
- `sell_after_300s={'resolved_count': 0, 'against_count': 0, 'against_rate': None}`
- `buy_after_30s_fall_ratio=0.0833`
- `buy_after_60s={'resolved_count': 10, 'against_count': 1, 'against_rate': 0.1}`
- `buy_after_300s={'resolved_count': 0, 'against_count': 0, 'against_rate': None}`
- `reason_distribution={}`
- 主窗口没有找到可落为“可能代码误判”的 confirm 反例。
- 限制：主窗口没有可靠的 `30s` 数值回写，`60s` 数值也几乎为空，所以本专题只能对 `300s` 做定量结论。

## 12. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `eth_signal_count=136` `btc_signal_count=0` `sol_signal_count=0`
- `major_cli_summary={'active_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'priority': 1, 'sample_size': 402, 'trend_pool_match_mode': 'explicit_whitelist'}, {'canonical_pair_label': 'ETH/USDT', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'priority': 1, 'sample_size': 389, 'trend_pool_match_mode': 'explicit_whitelist'}], 'configured_but_disabled_major_pools': [{'index': 2, 'pair_label': 'BTC/USDT', 'placeholder': False, 'pool_address': '0x9db9e0e53058c89e5b94e29621a205198648425b'}, {'index': 3, 'pair_label': 'BTC/USDC', 'placeholder': False, 'pool_address': '0x004375dff511095cc5a197a54140a24efef3a416'}], 'configured_major_assets': ['ETH', 'BTC', 'SOL'], 'configured_major_quotes': ['USDT', 'USDC'], 'covered_expected_pairs': ['ETH/USDT', 'ETH/USDC'], 'covered_major_pairs': ['ETH/USDT', 'ETH/USDC'], 'covered_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'priority': 1, 'sample_size': 402, 'trend_pool_match_mode': 'explicit_whitelist'}, {'canonical_pair_label': 'ETH/USDT', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'priority': 1, 'sample_size': 389, 'trend_pool_match_mode': 'explicit_whitelist'}], 'expected_major_pairs': ['ETH/USDT', 'ETH/USDC', 'BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'major_pair_quality': [{'actionable': True, 'climax_reversal_score': 0.2193, 'covered': True, 'market_context_alignment_score': 0.9656, 'pair_label': 'ETH/USDT', 'quality_score': 0.6688, 'sample_size': 389}, {'actionable': True, 'climax_reversal_score': 0.2621, 'covered': True, 'market_context_alignment_score': 0.9579, 'pair_label': 'ETH/USDC', 'quality_score': 0.6746, 'sample_size': 402}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDT', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDC', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDT', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDC', 'quality_score': 0.5864, 'sample_size': 0}], 'malformed_major_pool_entries': [], 'missing_expected_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'missing_major_assets': ['BTC', 'SOL'], 'missing_major_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'pool_book_exists': True, 'pool_book_path': '/run-project/chain-monitor/data/lp_pools.json', 'quality_converging_major_pairs': [{'actionable': True, 'climax_reversal_score': 0.2193, 'covered': True, 'market_context_alignment_score': 0.9656, 'pair_label': 'ETH/USDT', 'quality_score': 0.6688, 'sample_size': 389}, {'actionable': True, 'climax_reversal_score': 0.2621, 'covered': True, 'market_context_alignment_score': 0.9579, 'pair_label': 'ETH/USDC', 'quality_score': 0.6746, 'sample_size': 402}], 'recommended_local_config_actions': ['补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT', '校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH'], 'recommended_next_round_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'suggestions': ['优先补齐：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC', '当前 majors outcome 样本仍偏少，建议先扩 BTC/SOL/更多 ETH 主池', '样本稀少的 majors pairs：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC', '补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT', '校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH'], 'under_sampled_major_assets': [{'asset_symbol': 'BTC', 'sample_size': 0}, {'asset_symbol': 'SOL', 'sample_size': 0}], 'under_sampled_major_pairs': [{'pair_label': 'BTC/USDT', 'sample_size': 0}, {'pair_label': 'BTC/USDC', 'sample_size': 0}, {'pair_label': 'SOL/USDT', 'sample_size': 0}, {'pair_label': 'SOL/USDC', 'sample_size': 0}], 'warnings': ['majors 主池覆盖不完整，下一轮应优先补 majors 而不是 long-tail', '缺少 major 资产覆盖：BTC, SOL', '存在 configured but disabled 的 major pools；覆盖缺口不一定是识别问题']}`
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
- `signal_ids` = `136`
- `delivered_signal_ids` = `7`
- 判断：`signals -> delivery_audit -> cases.signal_attached -> case_followups` 在已送达 LP 子集上都是 1:1。flat/new 与 nested/old 格式并存，但 stage 字段未发现冲突。

## 14. quality/outcome 与 fastlane ROI

- `fastlane_promoted_count_window=0`
- `fastlane_promoted_delivered_count_window=0`
- `resolved_move_after_60s_count_window=56`
- `resolved_move_after_300s_count_window=0`
- `full_summary_cli.overall={'actionable': True, 'climax_count': 230, 'climax_reversal_count': 31, 'climax_reversal_score': 0.2361, 'confirm_conversion_score': 0.4908, 'confirm_count': 445, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 1, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776748132, 'market_context_alignment_positive_count': 598, 'market_context_alignment_score': 0.964, 'market_context_available_count': 598, 'prealert_confirmed_count': 1, 'prealert_count': 3, 'prealert_false_count': 0, 'prealert_precision_score': 0.685, 'quality_hint': '历史一般', 'quality_score': 0.6859, 'resolved_climaxes': 134, 'resolved_confirms': 194, 'resolved_fastlanes': 0, 'resolved_prealerts': 1, 'sample_size': 791, 'tuning_hint': '保持当前阈值'}`
- 判断：quality/outcome ledger 已能支撑对账和 pair-level 对比，但夜间 fastlane 与 60s outcome 样本仍偏薄。

## 15. 噪音与误判风险评估

- `delivered_ratio` = `0.0515`
- `local_confirm_share` = `0.4868`
- `unresolved_300s_share` = `1.0`
- `exhaustion_risk_count` = `21`
- `reverse_case_count` = `0`
- 判断：噪音的主要来源已不再是 market context unavailable，而是 `ETH-only sample + no prealert + sparse 60s/300s resolved outcomes`。

## 16. 最终评分

- `research_sampling_readiness` = `5.4/10`
- `live_market_context_readiness` = `9.4/10`
- `prealert_effectiveness` = `3.5/10`
- `confirm_honesty_non_misleading_quality` = `7.0/10`
- `sweep_quality` = `8.0/10`
- `majors_coverage` = `3.2/10`
- `signal_archive_integrity` = `9.2/10`
- `noise_control` = `6.2/10`
- `overall_self_use_score` = `6.5/10`

## 16. 下一轮建议

- 首优先：补齐 BTC/SOL majors pool book，让 overnight 不再只有 ETH。
- 第二优先：让 majors prealert 在真实夜间重新出现，否则连续研究仍偏后段确认样本。
- 第三优先：保留 OKX 主路径，但补一个可重复触发的 kraken fallback 健康检查，因为主窗口没有用到它。
- 第四优先：增强 30s/60s outcome 回写，解决“反向 K 线”专题定量盲区。

## 17. 限制与不确定性

- 本报告严格使用主窗口数据；主窗口之外的白天/下午样本只用于附录和 CLI 对照。
- `raw_events`/`parsed_events` availability = `raw:True` `parsed:True`；若缺失，就无法把 BTC/SOL 无样本彻底拆成“没有事件”还是“扫描未命中”。
- outcome window status = `{'30s': {'completed': 62, 'expired': 72, 'pending': 2}, '60s': {'expired': 78, 'completed': 56, 'pending': 2}, '300s': {'expired': 134, 'pending': 2}}`；若 `30s/60s` completed 仍少，相关结论必须保守。
