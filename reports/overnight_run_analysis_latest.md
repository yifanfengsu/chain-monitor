# Overnight Run Analysis

## 1. 执行摘要

- 主窗口为 `2026-04-18 18:33:40 UTC` 到 `2026-04-19 07:52:41 UTC`，持续 `13.32h`。
- 主窗口共 `107` 条 signals，其中 LP stage rows `68`、已送达 LP 消息 `55`、asset cases `54`、case followups `55`。
- OKX live context 在主窗口 `live_public=68/68`；`kraken_futures` attempts=`0`。
- prealert 在主窗口为 `0`，候选 funnel 为 `candidates=68` `gate_passed=0` `delivered=0`。
- `sweep_building` 样本 `14` 条，显示层残留 `climax/高潮` 为 `0`。
- majors 覆盖仍只在 `ETH/USDT` 与 `ETH/USDC`；BTC/SOL 仍缺 pool book 覆盖，无法代表更广 majors。

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/*.ndjson`: exists=`False` records=`0` range=`n/a -> n/a` note=`raw events archive`
- `app/data/archive/parsed_events/*.ndjson`: exists=`False` records=`0` range=`n/a -> n/a` note=`parsed events archive`
- `app/data/archive/signals/2026-04-18.ndjson`: exists=`True` records=`424` range=`2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` note=`signals archive`
- `app/data/archive/signals/2026-04-19.ndjson`: exists=`True` records=`158` range=`2026-04-18 16:02:05 UTC -> 2026-04-19 07:52:41 UTC` note=`signals archive`
- `app/data/archive/cases/2026-04-18.ndjson`: exists=`True` records=`40383` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` note=`case archive`
- `app/data/archive/cases/2026-04-19.ndjson`: exists=`True` records=`42025` range=`2026-04-18 16:00:04 UTC -> 2026-04-19 07:53:54 UTC` note=`case archive`
- `app/data/archive/case_followups/2026-04-18.ndjson`: exists=`True` records=`33` range=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` note=`case followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson`: exists=`True` records=`105` range=`2026-04-18 16:21:28 UTC -> 2026-04-19 07:52:41 UTC` note=`case followups archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson`: exists=`True` records=`54936` range=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` note=`delivery audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson`: exists=`True` records=`31253` range=`2026-04-18 16:00:04 UTC -> 2026-04-19 07:53:54 UTC` note=`delivery audit archive`
- `data/asset_cases.cache.json`: exists=`True` records=`1` range=`2026-04-19 07:52:39 UTC -> 2026-04-19 07:52:41 UTC` note=`asset case snapshot cache`
- `data/lp_quality_stats.cache.json`: exists=`True` records=`340` range=`2026-04-17 06:16:14 UTC -> 2026-04-19 07:52:44 UTC` note=`quality stats cache`
- raw/parsed archive presence: `raw_events=False` `parsed_events=False`。
- outcome windows: `{'30s': {'pending': 68}, '60s': {'pending': 68}, '300s': {'pending': 52, 'completed': 16}}`。

## 3. overnight 分析窗口

- 主窗口 UTC: `2026-04-18 18:33:40 UTC -> 2026-04-19 07:52:41 UTC`
- 服务器本地: `2026-04-18 18:33:40 UTC -> 2026-04-19 07:52:41 UTC`
- 北京时间: `2026-04-19 02:33:40 UTC+08:00 -> 2026-04-19 15:52:41 UTC+08:00`
- 东京时间: `2026-04-19 03:33:40 UTC+09:00 -> 2026-04-19 16:52:41 UTC+09:00`
- 选择原因: latest segment with the longest continuous signal activity after splitting on >=1h signal gaps; it also has the highest overnight LP row count.
- 其他段作为附录：
- 附录段: `2026-04-18 05:26:52 UTC -> 2026-04-18 17:30:19 UTC` `duration=12.06h` `signals=475` `lp=79`
- 主窗口: `2026-04-18 18:33:40 UTC -> 2026-04-19 07:52:41 UTC` `duration=13.32h` `signals=107` `lp=68`

## 4. 非敏感运行配置摘要

- `DEFAULT_USER_TIER` = `research`
- `MARKET_CONTEXT_ADAPTER_MODE` = `unavailable`
- `MARKET_CONTEXT_PRIMARY_VENUE` = `okx_perp`
- `MARKET_CONTEXT_SECONDARY_VENUE` = `kraken_futures`
- `OKX_PUBLIC_BASE_URL` = `https://www.okx.com`
- `KRAKEN_FUTURES_BASE_URL` = `https://futures.kraken.com`
- `ARCHIVE_ENABLE_RAW_EVENTS` = `True`
- `ARCHIVE_ENABLE_PARSED_EVENTS` = `True`
- `ARCHIVE_ENABLE_SIGNALS` = `True`
- `ARCHIVE_ENABLE_CASES` = `False`
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

- `analysis_window_start` = `2026-04-18 18:33:40 UTC`
- `analysis_window_end` = `2026-04-19 07:52:41 UTC`
- `duration_hours` = `13.32`
- `total_signal_rows` = `107`
- `lp_signal_rows` = `68`
- `delivered_lp_signals` = `55`
- `asset_case_count` = `54`
- `case_followup_count` = `55`
- `compression_ratio` = `1.2593`
- `avg_signals_per_case` = `1.2593`
- `stage_distribution_pct` = `{'prealert': 0.0, 'confirm': 54.41, 'climax': 13.24, 'exhaustion_risk': 32.35}`
- 覆盖资产 = `{'ETH': 68}`
- 覆盖 pairs = `{'ETH/USDT': 38, 'ETH/USDC': 30}`

## 6. OKX/Kraken live market context 验证

- 主窗口 `live_public_count=68` `unavailable_count=0`。
- 主窗口 `okx_attempts=328` `okx_success=328` `okx_failure=0`。
- 主窗口 `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`。
- 主窗口 `binance_attempts=0` `bybit_attempts=0`。
- 主窗口 requested->resolved = `{'ETHUSDC->ETH-USDT-SWAP': 30, 'ETHUSDT->ETH-USDT-SWAP': 38}`
- CLI full archive live_public_hit_rate = `0.2526`
- CLI full archive per_venue = `[{'attempt_failure_rate': 0.0, 'attempt_hit_rate': 1.0, 'attempt_status_cache_hit': 36, 'attempt_status_success': 666, 'attempt_success': 666, 'attempt_total': 666, 'cache_hit_rate': 0.2449, 'cache_hit_total': 36, 'context_request_hit_rate': 1.0, 'signal_hit_rate': 1.0, 'signal_success': 147, 'signal_total': 147, 'venue': 'okx_perp', 'venue_attempt_success_rate': 1.0}]`
- 判断：OKX 主路径已在真实 overnight 样本中生效；Kraken fallback 未被触发，所以只能确认配置已切到二级位，不能确认其夜间实战成功率。

## 7. prealert 真实表现

- `prealert_count=0` `major_prealert_count=0` `non_major_prealert_count=0`
- `prealert_to_confirm_30s=None`
- `prealert_to_confirm_60s=None`
- `prealert_to_confirm_90s=None`
- 判断：主窗口没有 prealert，所以 non-major guard 只能以“没有漏出 non-major prealert”来确认，无法证明 majors prealert 已恢复。

## 8. confirm local/broader/late/chase 分析

- `confirm_count` = `37`
- `clean_confirm_count` = `2`
- `local_confirm_count` = `23`
- `broader_confirm_count` = `0`
- `late_confirm_count` = `3`
- `chase_risk_count` = `0`
- `unconfirmed_confirm_count` = `18`
- `blank_confirm_quality_count` = `14`
- `broader_alignment_confirmed_count` = `5`
- `predict_warning_text_count` = `20`
- `confirm_move_before_30s_median` = `-0.000356`
- `confirm_move_after_60s_median` = `None`
- `confirm_move_after_300s_median` = `-0.002083`
- 判断：confirm 现在明显更诚实。夜间样本里 `23` 条被写成 `local_confirm`，`0` 条被写成 `broader_confirm`，说明系统没有把局部池子压力硬写成更广确认。
- 但仍有 `14` 条 confirm 属于 sweep 语义，因此不会落在标准 confirm_quality/scope 分类里；这部分需要与 sweep 段一起看。

## 9. sweep_building / sweep_confirmed / exhaustion 分析

- `sweep_building_count` = `14`
- `sweep_confirmed_count` = `9`
- `sweep_exhaustion_risk_count` = `22`
- `sweep_building_display_climax_residual_count` = `0`
- `sweep_building_to_sweep_confirmed_rate` = `0.0`
- `sweep_building_to_continue_rate` = `0.0`
- `sweep_reversal_60s` = `{'resolved_count': 0, 'adverse_count': 0, 'adverse_rate': None}`
- `sweep_reversal_300s` = `{'resolved_count': 6, 'adverse_count': 0, 'adverse_rate': 0.0}`
- `sweep_exhaustion_outcome_300s` = `{'resolved_count': 6, 'adverse_count': 2, 'adverse_rate': 0.3333}`
- `direction_performance` = `{'buy_pressure': {'count': 12, 'move_after_60s_median': None, 'move_after_300s_median': -0.005003, 'adverse_60s_rate': None, 'adverse_300s_rate': 1.0}, 'sell_pressure': {'count': 33, 'move_after_60s_median': None, 'move_after_300s_median': 0.002914, 'adverse_60s_rate': None, 'adverse_300s_rate': 0.0}}`
- 判断：`sweep_building` 在显示层彻底不再冒充高潮。`sweep_confirmed` 的主窗口 300s 已解析样本里没有出现反向；`sweep_exhaustion_risk` 300s 解析样本里出现了部分反向，但样本很少。

## 10. “卖压后涨 / 买压后跌”反例专项

- `sell_confirm_count=19` `buy_confirm_count=18`
- `sell_after_30s_rise_ratio=None`
- `sell_after_60s={'resolved_count': 0, 'against_count': 0, 'against_rate': None}`
- `sell_after_300s={'resolved_count': 3, 'against_count': 0, 'against_rate': 0.0}`
- `buy_after_30s_fall_ratio=None`
- `buy_after_60s={'resolved_count': 0, 'against_count': 0, 'against_rate': None}`
- `buy_after_300s={'resolved_count': 1, 'against_count': 1, 'against_rate': 1.0}`
- `reason_distribution={'late_or_chase': 1, 'local_confirm_not_broader': 1, 'single_pool_or_low_resonance': 1, 'local_buy_pressure_absorption': 1}`
- 典型反例：
- `{'signal_id': 'sig_7815ff2a69821dae', 'asset_case_id': 'asset_case:ETH:0596620168', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': 'late_confirm', 'absorption_context': 'local_buy_pressure_absorption', 'market_context_source': 'live_public', 'move_before': 0.000278, 'move_after': -0.006036, 'judgement_reason': 'late_confirm, local_confirm_not_broader, single_pool_or_low_resonance, local_buy_pressure_absorption'}`
- 限制：主窗口没有可靠的 `30s` 数值回写，`60s` 数值也几乎为空，所以本专题只能对 `300s` 做定量结论。

## 11. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `eth_signal_count=68` `btc_signal_count=0` `sol_signal_count=0`
- `major_cli_summary={'active_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'priority': 1, 'sample_size': 162, 'trend_pool_match_mode': 'explicit_whitelist'}, {'canonical_pair_label': 'ETH/USDT', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'priority': 1, 'sample_size': 178, 'trend_pool_match_mode': 'explicit_whitelist'}], 'configured_but_disabled_major_pools': [{'index': 2, 'pair_label': 'BTC/USDT', 'placeholder': False, 'pool_address': '0x9db9e0e53058c89e5b94e29621a205198648425b'}, {'index': 3, 'pair_label': 'BTC/USDC', 'placeholder': False, 'pool_address': '0x004375dff511095cc5a197a54140a24efef3a416'}], 'configured_major_assets': ['ETH', 'BTC', 'SOL'], 'configured_major_quotes': ['USDT', 'USDC'], 'covered_expected_pairs': ['ETH/USDT', 'ETH/USDC'], 'covered_major_pairs': ['ETH/USDT', 'ETH/USDC'], 'covered_major_pools': [{'canonical_pair_label': 'ETH/USDC', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDC', 'pool_address': '0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc', 'priority': 1, 'sample_size': 162, 'trend_pool_match_mode': 'explicit_whitelist'}, {'canonical_pair_label': 'ETH/USDT', 'is_primary_trend_pool': True, 'major_match_mode': 'major_family_match', 'major_priority_score': 1.25, 'pair_label': 'ETH/USDT', 'pool_address': '0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852', 'priority': 1, 'sample_size': 178, 'trend_pool_match_mode': 'explicit_whitelist'}], 'expected_major_pairs': ['ETH/USDT', 'ETH/USDC', 'BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'major_pair_quality': [{'actionable': True, 'climax_reversal_score': 0.1793, 'covered': True, 'market_context_alignment_score': 0.956, 'pair_label': 'ETH/USDT', 'quality_score': 0.7143, 'sample_size': 178}, {'actionable': True, 'climax_reversal_score': 0.0957, 'covered': True, 'market_context_alignment_score': 0.9409, 'pair_label': 'ETH/USDC', 'quality_score': 0.7028, 'sample_size': 162}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDT', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'BTC/USDC', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDT', 'quality_score': 0.5864, 'sample_size': 0}, {'actionable': False, 'climax_reversal_score': 0.45, 'covered': False, 'market_context_alignment_score': 0.5, 'pair_label': 'SOL/USDC', 'quality_score': 0.5864, 'sample_size': 0}], 'malformed_major_pool_entries': [], 'missing_expected_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'missing_major_assets': ['BTC', 'SOL'], 'missing_major_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'pool_book_exists': True, 'pool_book_path': '/run-project/chain-monitor/data/lp_pools.json', 'quality_converging_major_pairs': [{'actionable': True, 'climax_reversal_score': 0.1793, 'covered': True, 'market_context_alignment_score': 0.956, 'pair_label': 'ETH/USDT', 'quality_score': 0.7143, 'sample_size': 178}, {'actionable': True, 'climax_reversal_score': 0.0957, 'covered': True, 'market_context_alignment_score': 0.9409, 'pair_label': 'ETH/USDC', 'quality_score': 0.7028, 'sample_size': 162}], 'recommended_local_config_actions': ['补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT', '校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH'], 'recommended_next_round_pairs': ['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'suggestions': ['优先补齐：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC', '当前 majors outcome 样本仍偏少，建议先扩 BTC/SOL/更多 ETH 主池', '样本稀少的 majors pairs：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC', '补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT', '校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH'], 'under_sampled_major_assets': [{'asset_symbol': 'BTC', 'sample_size': 0}, {'asset_symbol': 'SOL', 'sample_size': 0}], 'under_sampled_major_pairs': [{'pair_label': 'BTC/USDT', 'sample_size': 0}, {'pair_label': 'BTC/USDC', 'sample_size': 0}, {'pair_label': 'SOL/USDT', 'sample_size': 0}, {'pair_label': 'SOL/USDC', 'sample_size': 0}], 'warnings': ['majors 主池覆盖不完整，下一轮应优先补 majors 而不是 long-tail', '缺少 major 资产覆盖：BTC, SOL', '存在 configured but disabled 的 major pools；覆盖缺口不一定是识别问题']}`
- 判断：主窗口仍然只来自 ETH 双主池。CLI 同时确认 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC` 属于 pool book 覆盖缺口，而不是夜里单纯无事件。

## 12. signal archive 对账完整性

- `signals_archive_exists` = `True`
- `signal_delivery_audit_match_rate` = `1.0`
- `signal_case_followup_match_rate` = `1.0`
- `signal_case_attached_match_rate` = `1.0`
- `delivered_signal_notifier_sent_at_rate` = `1.0`
- `signal_id_complete_rate` = `1.0`
- `asset_case_id_complete_rate` = `1.0`
- `outcome_tracking_key_complete_rate` = `1.0`
- `mixed_flat_nested_rows` = `68`
- `flat_nested_stage_mismatch_rows` = `0`
- `signal_ids` = `68`
- `delivered_signal_ids` = `55`
- 判断：`signals -> delivery_audit -> cases.signal_attached -> case_followups` 在已送达 LP 子集上都是 1:1。flat/new 与 nested/old 格式并存，但 stage 字段未发现冲突。

## 13. quality/outcome 与 fastlane ROI

- `fastlane_promoted_count_window=0`
- `fastlane_promoted_delivered_count_window=0`
- `resolved_move_after_60s_count_window=0`
- `resolved_move_after_300s_count_window=16`
- `full_summary_cli.overall={'actionable': True, 'climax_count': 101, 'climax_reversal_count': 8, 'climax_reversal_score': 0.1281, 'confirm_conversion_score': 0.6622, 'confirm_count': 182, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 1, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776585159, 'market_context_alignment_positive_count': 147, 'market_context_alignment_score': 0.9583, 'market_context_available_count': 147, 'prealert_confirmed_count': 0, 'prealert_count': 1, 'prealert_false_count': 0, 'prealert_precision_score': 0.58, 'quality_hint': '历史传导较强', 'quality_score': 0.7119, 'resolved_climaxes': 70, 'resolved_confirms': 69, 'resolved_fastlanes': 0, 'resolved_prealerts': 0, 'sample_size': 340, 'tuning_hint': 'retail 建议关闭该维度 prealert'}`
- 判断：quality/outcome ledger 已能支撑对账和 pair-level 对比，但夜间 fastlane 与 60s outcome 样本仍偏薄。

## 14. 噪音与误判风险评估

- `delivered_ratio` = `0.8088`
- `local_confirm_share` = `0.6216`
- `unresolved_300s_share` = `0.7647`
- `exhaustion_risk_count` = `22`
- `reverse_case_count` = `1`
- 判断：噪音的主要来源已不再是 market context unavailable，而是 `ETH-only sample + no prealert + sparse 60s/300s resolved outcomes`。

## 15. 最终评分

- `research_sampling_readiness` = `3.8/10`
- `live_market_context_readiness` = `9.4/10`
- `prealert_effectiveness` = `0.8/10`
- `confirm_honesty_non_misleading_quality` = `6.4/10`
- `sweep_quality` = `8.0/10`
- `majors_coverage` = `3.2/10`
- `signal_archive_integrity` = `9.2/10`
- `noise_control` = `5.7/10`
- `overall_self_use_score` = `5.8/10`

## 16. 下一轮建议

- 首优先：补齐 BTC/SOL majors pool book，让 overnight 不再只有 ETH。
- 第二优先：让 majors prealert 在真实夜间重新出现，否则连续研究仍偏后段确认样本。
- 第三优先：保留 OKX 主路径，但补一个可重复触发的 kraken fallback 健康检查，因为主窗口没有用到它。
- 第四优先：增强 30s/60s outcome 回写，解决“反向 K 线”专题定量盲区。

## 17. 限制与不确定性

- 本报告严格使用主窗口数据；主窗口之外的白天/下午样本只用于附录和 CLI 对照。
- `raw_events`/`parsed_events` availability = `raw:False` `parsed:False`；若缺失，就无法把 BTC/SOL 无样本彻底拆成“没有事件”还是“扫描未命中”。
- outcome window status = `{'30s': {'pending': 68}, '60s': {'pending': 68}, '300s': {'pending': 52, 'completed': 16}}`；若 `30s/60s` completed 仍少，相关结论必须保守。
