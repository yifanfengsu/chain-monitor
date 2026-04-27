# Overnight Run Analysis

## 1. 执行摘要

- 主窗口为 `2026-04-23 16:42:06 UTC` 到 `2026-04-24 15:51:50 UTC`，持续 `23.16h`。
- 主窗口共 `914` 条 signals，其中 LP stage rows `197`、已送达 LP 消息 `67`、asset cases `62`、case followups `67`。
- OKX live context 在主窗口 `live_public=197/197`；`kraken_futures` attempts=`0`。
- prealert 在主窗口为 `0`，候选 funnel 为 `candidates=197` `gate_passed=197` `delivered=0`。
- `sweep_building` 样本 `36` 条，显示层残留 `climax/高潮` 为 `0`。
- trade action 分布：`{'CONFLICT_NO_TRADE': 17, 'DO_NOT_CHASE_LONG': 45, 'DO_NOT_CHASE_SHORT': 48, 'LONG_BIAS_OBSERVE': 44, 'SHORT_BIAS_OBSERVE': 41, 'WAIT_CONFIRMATION': 2}`；可追类总数 `long=0` `short=0`。
- asset state 分布：`{'DO_NOT_CHASE_LONG': 44, 'DO_NOT_CHASE_SHORT': 47, 'NO_TRADE_CHOP': 2, 'NO_TRADE_LOCK': 18, 'OBSERVE_LONG': 44, 'OBSERVE_SHORT': 40, 'WAIT_CONFIRMATION': 2}`；state change sent=`21` risk blockers=`46` candidates=`0`。
- no-trade lock: entered=`7` suppressed=`0` released=`179`。
- candidate vs tradeable: candidate=`0` tradeable=`0` 60s_source=`{'okx_mark': 788}`。
- final output 分布：`{'asset_market_state': 84, 'trade_action_legacy': 99, 'trade_opportunity': 14}`；verified=`0` candidate=`0` blocked=`4`。
- opportunity maturity: verified_maturity=`immature` should_not_trade=`outcome_completion_rate_below_0.70` maturity_reasons=`['outcome_completion_rate_below_0.70']`。
- legacy chase 审计：downgraded=`0` leaked=`0` chase_without_verified=`0` blocked_by_gate=`53`。
- 验证问题：all_opportunity_labels_verified=`True` all_candidate_labels_are_candidate=`True` blocked_covers_legacy_chase_risk=`True`。
- majors 覆盖保持透明：BTC 若出现在 configured_but_disabled，需要在本地 `data/lp_pools.json` 明确启用；SOL 原生池仍需要 Solana listener，Base 池需要 Base chain routing，不能误算作 Ethereum 池。

## 2. 数据源与完整性说明

- `data/chain_monitor.sqlite`: exists=`True` records=`85477` range=`2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC` note=`raw events archive; sqlite window-scoped inventory for raw_events`
- `data/chain_monitor.sqlite`: exists=`True` records=`45164` range=`2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC` note=`parsed events archive; sqlite window-scoped inventory for parsed_events`
- `data/chain_monitor.sqlite`: exists=`True` records=`1207` range=`2026-04-23 00:00:27 UTC -> 2026-04-24 15:51:50 UTC` note=`sqlite signals mirror via report_data_loader`
- `data/chain_monitor.sqlite`: exists=`True` records=`914` range=`2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC` note=`sqlite signals-derived case linkage window`
- `data/chain_monitor.sqlite`: exists=`True` records=`101` range=`2026-04-23 16:54:16 UTC -> 2026-04-24 15:42:40 UTC` note=`sqlite case_followups window`
- `data/chain_monitor.sqlite`: exists=`True` records=`87935` range=`2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC` note=`sqlite delivery_audit window`
- `data/asset_cases.cache.json`: exists=`True` records=`1` range=`2026-04-24 15:42:38 UTC -> 2026-04-24 15:45:01 UTC` note=`asset case snapshot cache`
- `data/lp_quality_stats.cache.json`: exists=`True` records=`800` range=`2026-04-20 06:33:25 UTC -> 2026-04-24 15:49:57 UTC` note=`quality stats cache`
- `data/trade_opportunities.cache.json`: exists=`True` records=`682` range=`2026-04-20 15:03:51 UTC -> 2026-04-24 15:45:51 UTC` note=`trade opportunity cache`
- `data/chain_monitor.sqlite`: exists=`True` records=`1002626` range=`n/a -> n/a` note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'asset_cases': 19549, 'asset_market_states': 780, 'case_followups': 297, 'delivery_audit': 285345, 'market_context_attempts': 4385, 'outcomes': 4059, 'parsed_events': 113046, 'quality_stats': 1511, 'raw_events': 222724, 'signals': 3056, 'telegram_deliveries': 347002, 'trade_opportunities': 872} archive_rows_by_category={} db_archive_mirror_match_rate=None archive_fallback_used=False mismatch_warnings=[]`
- raw/parsed archive presence: `raw_events=False` `parsed_events=False`。
- outcome windows: `{'30s': {'completed': 197}, '60s': {'completed': 197}, '300s': {'completed': 197}}`。
- telegram suppression: `{'blocked_output_requires_blocked_or_risk_state': 2, 'no_trade_opportunity': 99, 'same_blocker_repeat': 2}`。

## 3. overnight 分析窗口

- 主窗口 UTC: `2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC`
- 服务器本地: `2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC`
- 北京时间: `2026-04-24 00:42:06 UTC+08:00 -> 2026-04-24 23:51:50 UTC+08:00`
- 东京时间: `2026-04-24 01:42:06 UTC+09:00 -> 2026-04-25 00:51:50 UTC+09:00`
- 选择原因: requested logical date 2026-04-24; selected the longest signal segment whose UTC end date matches it after splitting on >=1h signal gaps.
- 其他段作为附录：
- 附录段: `2026-04-23 00:00:27 UTC -> 2026-04-23 05:13:15 UTC` `duration=5.21h` `signals=293` `lp=106`
- 主窗口: `2026-04-23 16:42:06 UTC -> 2026-04-24 15:51:50 UTC` `duration=23.16h` `signals=914` `lp=197`

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

- `analysis_window_start` = `2026-04-23 16:42:06 UTC`
- `analysis_window_end` = `2026-04-24 15:51:50 UTC`
- `duration_hours` = `23.16`
- `total_signal_rows` = `914`
- `lp_signal_rows` = `197`
- `delivered_lp_signals` = `67`
- `asset_case_count` = `62`
- `case_followup_count` = `67`
- `compression_ratio` = `3.1774`
- `avg_signals_per_case` = `3.1774`
- `stage_distribution_pct` = `{'prealert': 0.0, 'confirm': 46.7, 'climax': 33.5, 'exhaustion_risk': 19.8}`
- 覆盖资产 = `{'ETH': 197}`
- 覆盖 pairs = `{'ETH/USDC': 96, 'ETH/USDT': 101}`

## 6. OKX/Kraken live market context 验证

- 主窗口 `live_public_count=197` `unavailable_count=0`。
- 主窗口 `okx_attempts=1184` `okx_success=1182` `okx_failure=2`。
- 主窗口 `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`。
- 主窗口 `binance_attempts=0` `bybit_attempts=0`。
- 主窗口 requested->resolved = `{'ETHUSDC->ETH-USDT-SWAP': 96, 'ETHUSDT->ETH-USDT-SWAP': 101}`
- CLI full archive live_public_hit_rate = `skipped`
- CLI full archive per_venue = `{}`
- 判断：OKX 主路径已在真实 overnight 样本中生效；Kraken fallback 未被触发，所以只能确认配置已切到二级位，不能确认其夜间实战成功率。

## 7. prealert 真实表现

- `prealert_count=0` `major_prealert_count=0` `non_major_prealert_count=0`
- `prealert_to_confirm_30s=None`
- `prealert_to_confirm_60s=None`
- `prealert_to_confirm_90s=None`
- 判断：主窗口没有 prealert，所以 non-major guard 只能以“没有漏出 non-major prealert”来确认，无法证明 majors prealert 已恢复。

## 8. confirm local/broader/late/chase 分析

- `confirm_count` = `92`
- `clean_confirm_count` = `2`
- `local_confirm_count` = `48`
- `broader_confirm_count` = `8`
- `late_confirm_count` = `27`
- `chase_risk_count` = `0`
- `unconfirmed_confirm_count` = `27`
- `blank_confirm_quality_count` = `36`
- `broader_alignment_confirmed_count` = `20`
- `predict_warning_text_count` = `25`
- `confirm_move_before_30s_median` = `-0.000227`
- `confirm_move_after_60s_median` = `-3.6e-05`
- `confirm_move_after_300s_median` = `0.000116`
- 判断：confirm 现在明显更诚实。夜间样本里 `23` 条被写成 `local_confirm`，`0` 条被写成 `broader_confirm`，说明系统没有把局部池子压力硬写成更广确认。
- 但仍有 `14` 条 confirm 属于 sweep 语义，因此不会落在标准 confirm_quality/scope 分类里；这部分需要与 sweep 段一起看。

## 9. sweep_building / sweep_confirmed / exhaustion 分析

- `sweep_building_count` = `36`
- `sweep_confirmed_count` = `66`
- `sweep_exhaustion_risk_count` = `39`
- `sweep_building_display_climax_residual_count` = `0`
- `sweep_building_to_sweep_confirmed_rate` = `0.1944`
- `sweep_building_to_continue_rate` = `0.5833`
- `sweep_reversal_60s` = `{'resolved_count': 66, 'adverse_count': 24, 'adverse_rate': 0.3636}`
- `sweep_reversal_300s` = `{'resolved_count': 66, 'adverse_count': 28, 'adverse_rate': 0.4242}`
- `sweep_exhaustion_outcome_300s` = `{'resolved_count': 39, 'adverse_count': 21, 'adverse_rate': 0.5385}`
- `direction_performance` = `{'buy_pressure': {'count': 64, 'move_after_60s_median': 0.000307, 'move_after_300s_median': 0.000185, 'adverse_60s_rate': 0.4062, 'adverse_300s_rate': 0.4375}, 'sell_pressure': {'count': 77, 'move_after_60s_median': 1.7e-05, 'move_after_300s_median': -0.0004, 'adverse_60s_rate': 0.4805, 'adverse_300s_rate': 0.5844}}`
- 判断：`sweep_building` 在显示层彻底不再冒充高潮。`sweep_confirmed` 的主窗口 300s 已解析样本里没有出现反向；`sweep_exhaustion_risk` 300s 解析样本里出现了部分反向，但样本很少。

## 10. trade_action 层评估

- `trade_action_distribution={'CONFLICT_NO_TRADE': 17, 'DO_NOT_CHASE_LONG': 45, 'DO_NOT_CHASE_SHORT': 48, 'LONG_BIAS_OBSERVE': 44, 'SHORT_BIAS_OBSERVE': 41, 'WAIT_CONFIRMATION': 2}`
- `long_chase_allowed_count=0` `short_chase_allowed_count=0`
- `no_trade_count=0` `wait_confirmation_count=2`
- `do_not_chase_long_count=45` `do_not_chase_short_count=48`
- `conflict_no_trade_count=17` `data_gap_no_trade_count=0`
- `chase_allowed_success_rate=0.0` `chase_allowed_adverse_rate=None`
- `generic_confirm_success_rate_300s=0.5` `generic_confirm_adverse_rate_300s=0.5`
- `no_trade_would_have_saved_rate=0.7059`
- `conflict_after_message_reversal_rate=0.7059`
- 判断 1：`LONG/SHORT_CHASE_ALLOWED` 必须始终是少数样本；计数过高说明动作层仍过于宽松。
- 判断 2：如果 `chase_allowed_success_rate` 明显高于 generic confirm，说明严格 chase gate 确实带来了后验提升。
- 判断 3：`do_not_chase_*` 与 `no_trade_would_have_saved_rate` 可以用来估算系统是否减少了不利追单。
- 判断 4：`conflict_no_trade_count` 与 `conflict_after_message_reversal_rate` 用来验证双向噪音时 abstain 是否合理。
- 判断 5：trade_action 把 Telegram 首行从结构词改成动作词，本质上是在降低误用而不是增加方向幻觉。

## 11. “卖压后涨 / 买压后跌”反例专项

- `sell_confirm_count=50` `buy_confirm_count=42`
- `sell_after_30s_rise_ratio=0.46`
- `sell_after_60s={'resolved_count': 50, 'against_count': 25, 'against_rate': 0.5}`
- `sell_after_300s={'resolved_count': 50, 'against_count': 30, 'against_rate': 0.6}`
- `buy_after_30s_fall_ratio=0.4286`
- `buy_after_60s={'resolved_count': 42, 'against_count': 23, 'against_rate': 0.5476}`
- `buy_after_300s={'resolved_count': 42, 'against_count': 21, 'against_rate': 0.5}`
- `reason_distribution={'late_or_chase': 4, 'single_pool_or_low_resonance': 1, 'local_sell_pressure_absorption': 1}`
- 典型反例：
- `{'signal_id': 'sig_d86544e1bcdda46d', 'asset_case_id': 'asset_case:ETH:87f44ac761', 'pair': 'ETH/USDC', 'stage': 'confirm', 'confirm_quality': 'late_confirm', 'absorption_context': 'broader_sell_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': -0.002046, 'move_after': 0.000398, 'judgement_reason': 'late_confirm'}`
- `{'signal_id': 'sig_7abd1cf9bb23160f', 'asset_case_id': 'asset_case:ETH:87f44ac761', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': 'late_confirm', 'absorption_context': 'broader_sell_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': -0.000915, 'move_after': 0.001416, 'judgement_reason': 'late_confirm'}`
- `{'signal_id': 'sig_c6bd0f6d0783445a', 'asset_case_id': 'asset_case:ETH:87f44ac761', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': 'late_confirm', 'absorption_context': 'broader_sell_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': -0.001546, 'move_after': 0.000415, 'judgement_reason': 'late_confirm'}`
- `{'signal_id': 'sig_0449a1fe477ee095', 'asset_case_id': 'asset_case:ETH:5d2db9d4d5', 'pair': 'ETH/USDC', 'stage': 'confirm', 'confirm_quality': '(blank)', 'absorption_context': 'local_sell_pressure_absorption', 'market_context_source': 'live_public', 'move_before': -0.006545, 'move_after': 0.003764, 'judgement_reason': 'single_pool_or_low_resonance, local_sell_pressure_absorption'}`
- `{'signal_id': 'sig_00b0c1d64a1af4d9', 'asset_case_id': 'asset_case:ETH:39b45b2b3f', 'pair': 'ETH/USDT', 'stage': 'confirm', 'confirm_quality': 'late_confirm', 'absorption_context': 'broader_buy_pressure_confirmed', 'market_context_source': 'live_public', 'move_before': 0.000625, 'move_after': -5.6e-05, 'judgement_reason': 'late_confirm'}`
- 限制：主窗口没有可靠的 `30s` 数值回写，`60s` 数值也几乎为空，所以本专题只能对 `300s` 做定量结论。

## 12. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `eth_signal_count=197` `btc_signal_count=0` `sol_signal_count=0`
- `major_cli_summary={'available': False, 'reason': 'skipped_for_date_scoped_report', 'expected_major_pairs': ['ETH/USDT', 'ETH/USDC', 'BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC'], 'missing_expected_pairs': []}`
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
- `signal_ids` = `197`
- `delivered_signal_ids` = `67`
- 判断：`signals -> delivery_audit -> cases.signal_attached -> case_followups` 在已送达 LP 子集上都是 1:1。flat/new 与 nested/old 格式并存，但 stage 字段未发现冲突。

## 14. quality/outcome 与 fastlane ROI

- `fastlane_promoted_count_window=0`
- `fastlane_promoted_delivered_count_window=0`
- `resolved_move_after_60s_count_window=197`
- `resolved_move_after_300s_count_window=197`
- `full_summary_cli.overall={}`
- 判断：quality/outcome ledger 已能支撑对账和 pair-level 对比，但夜间 fastlane 与 60s outcome 样本仍偏薄。

## 15. 噪音与误判风险评估

- `delivered_ratio` = `0.3401`
- `local_confirm_share` = `0.5217`
- `unresolved_300s_share` = `0.0`
- `exhaustion_risk_count` = `39`
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
- `raw_events`/`parsed_events` availability = `raw:False` `parsed:False`；若缺失，就无法把 BTC/SOL 无样本彻底拆成“没有事件”还是“扫描未命中”。
- outcome window status = `{'30s': {'completed': 197}, '60s': {'completed': 197}, '300s': {'completed': 197}}`；若 `30s/60s` completed 仍少，相关结论必须保守。
