# Overnight Opportunity Retention Analysis

## 1. 执行摘要

- 主窗口为 2026-04-20T15:03:53+00:00 到 2026-04-21T05:53:12+00:00 UTC，北京时间 2026-04-20T23:03:53+08:00 到 2026-04-21T13:53:12+08:00，持续 14.82 小时。
- 窗口内 raw=47838 parsed=25789 signals=526 LP=136，没有只看 Telegram 已送达样本。
- trade_opportunity cache 存在且窗口内 136 条；CANDIDATE=0 VERIFIED=0 BLOCKED=15 NONE=121。
- 本轮没有 CANDIDATE/VERIFIED；高于 candidate 分数阈值的行有 8 条，但被历史完成率、NO_TRADE_LOCK、late/chase、local_absorption 等门禁压住。
- verified 缺失不是单一“样本数不足”：opportunity 原生 candidate/verified history 为 0，但 LP fallback 样本最高 398；真正硬门槛是 outcome completion rate 远低于配置要求。
- BLOCKED 有效工作：blocked=15，主要 blocker={'history_completion_too_low': 11, 'late_or_chase': 1, 'no_trade_lock': 3}。
- Telegram 明显降噪：估算 suppression ratio=0.9485，before=136 after=7。
- asset_market_state 仍在冷启动后运行：state transitions=63，NO_TRADE_LOCK entered=4 suppressed=0。
- prealert 有 candidate=136 gate_passed=136，但 delivered=0，生命周期仍弱。
- outcome 60s completed_rate=0.4118，300s expired_rate=0.9853，是机会层可信度的主要短板。
- OKX live context 在窗口内 live_public_rate=1.0；okx attempts=741 success=740，Kraken attempts=0。
- majors 代表性不足：窗口样本资产分布={'ETH': 136}，缺失 major pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']。
- 这轮是部分冷启动：raw/parsed 覆盖短于 signals/cases/delivery，asset_cases 与 state cache 只剩少量当前状态，trade_opportunity candidate/verified history 为 0。
- 不要在运行前删除 lp_quality_stats、trade_opportunities、asset_market_states、asset_cases、signals、delivery_audit、cases、case_followups。
- 数据库不必今天强切；个人自用最现实方案是继续 NDJSON/JSON 保留满 7 天，再做 SQLite 只读索引第一期。

## 2. 数据源与完整性说明

### raw_events
- `app/data/archive/raw_events/2026-04-20.ndjson` exists=True records=4171 range_utc=2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC size=14416834
- `app/data/archive/raw_events/2026-04-21.ndjson` exists=True records=43792 range_utc=2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC size=151614404

### parsed_events
- `app/data/archive/parsed_events/2026-04-20.ndjson` exists=True records=1921 range_utc=2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC size=59556945
- `app/data/archive/parsed_events/2026-04-21.ndjson` exists=True records=23985 range_utc=2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC size=759222591

### signals
- `app/data/archive/signals/2026-04-18.ndjson` exists=True records=424 range_utc=2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC size=30871336
- `app/data/archive/signals/2026-04-19.ndjson` exists=True records=22 range_utc=2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC size=1593828
- `app/data/archive/signals/2026-04-20.ndjson` exists=True records=76 range_utc=2026-04-20 15:03:53 UTC -> 2026-04-20 15:56:39 UTC size=7017351
- `app/data/archive/signals/2026-04-21.ndjson` exists=True records=450 range_utc=2026-04-20 16:00:15 UTC -> 2026-04-21 05:53:12 UTC size=41207245

### cases
- `app/data/archive/cases/2026-04-18.ndjson` exists=True records=40383 range_utc=2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC size=899251909
- `app/data/archive/cases/2026-04-19.ndjson` exists=True records=1309 range_utc=2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC size=29177915
- `app/data/archive/cases/2026-04-20.ndjson` exists=True records=2378 range_utc=2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC size=52910147
- `app/data/archive/cases/2026-04-21.ndjson` exists=True records=34950 range_utc=2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC size=795862418

### case_followups
- `app/data/archive/case_followups/2026-04-18.ndjson` exists=True records=33 range_utc=2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC size=8726
- `app/data/archive/case_followups/2026-04-19.ndjson` exists=True records=2 range_utc=2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC size=529
- `app/data/archive/case_followups/2026-04-20.ndjson` exists=True records=5 range_utc=2026-04-20 15:21:28 UTC -> 2026-04-20 15:39:05 UTC size=5429
- `app/data/archive/case_followups/2026-04-21.ndjson` exists=True records=67 range_utc=2026-04-20 16:05:52 UTC -> 2026-04-21 05:27:41 UTC size=61103

### delivery_audit
- `app/data/archive/delivery_audit/2026-04-18.ndjson` exists=True records=54936 range_utc=2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC size=477817032
- `app/data/archive/delivery_audit/2026-04-19.ndjson` exists=True records=2056 range_utc=2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC size=17630162
- `app/data/archive/delivery_audit/2026-04-20.ndjson` exists=True records=4260 range_utc=2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC size=34619242
- `app/data/archive/delivery_audit/2026-04-21.ndjson` exists=True records=44724 range_utc=2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC size=397322388

### asset_cases.cache
- `data/asset_cases.cache.json` exists=True records=1 range_utc=2026-04-21 05:08:38 UTC -> 2026-04-21 05:08:56 UTC size=2561

### lp_quality_stats.cache
- `data/lp_quality_stats.cache.json` exists=True records=791 range_utc=2026-04-17 06:16:14 UTC -> 2026-04-21 05:08:58 UTC size=3382217

### asset_market_states.cache
- `data/asset_market_states.cache.json` exists=True records=1 range_utc=2026-04-20 05:10:15 UTC -> 2026-04-21 05:08:58 UTC size=6489

### trade_opportunities.cache
- `data/trade_opportunities.cache.json` exists=True records=136 range_utc=2026-04-20 15:03:51 UTC -> 2026-04-21 05:13:52 UTC size=863881

### persisted_exchange_adjacent
- `data/persisted_exchange_adjacent.json` exists=True records=1803 range_utc=2026-04-07 15:26:01 UTC -> 2026-04-21 05:56:14 UTC size=2405215

### clmm_positions.cache
- `data/clmm_positions.cache.json` exists=True records=14 range_utc=2026-03-29 13:00:00 UTC -> 2026-04-03 16:00:00 UTC size=15633

- 缺失/断档判断：["raw_events date coverage is shorter than signals", "parsed_events date coverage is shorter than signals", "trade_opportunity candidate_history and verified_history are empty", "asset_market_states cache has only one asset record", "asset_cases cache has only one current case"]
- 缺失影响：raw/parsed 断档影响 raw->parsed->signal 漏报定位；cache 冷启动影响 quality score、asset state、NO_TRADE_LOCK、prealert lifecycle、Telegram suppression 与 candidate->verified 后验。

## 3. overnight 分析窗口

- UTC: `2026-04-20T15:03:53+00:00 -> 2026-04-21T05:53:12+00:00`
- 服务器本地: `2026-04-20T15:03:53+00:00 -> 2026-04-21T05:53:12+00:00`
- 北京时间 UTC+8: `2026-04-20T23:03:53+08:00 -> 2026-04-21T13:53:12+08:00`
- duration_hours: `14.82`
- 选择原因: 按 signals 归档切分 >=1h 断点后，选择最长且最新、LP row 最多、同时 raw/parsed/signals/cases/delivery_audit/case_followups 均有重叠覆盖的北京夜间连续段。
- 连续段：
- `2026-04-18T05:26:52+00:00 -> 2026-04-18T15:57:32+00:00` duration=10.51h signals=424 lp=74 bj_overnight=False
- `2026-04-19T15:39:05+00:00 -> 2026-04-19T15:59:27+00:00` duration=0.34h signals=22 lp=2 bj_overnight=False
- `2026-04-20T15:03:53+00:00 -> 2026-04-21T05:53:12+00:00` duration=14.82h signals=526 lp=136 bj_overnight=True

## 4. 冷启动 / 数据删除影响说明

- `looks_like_cold_start`: True
- `trade_opportunities_cache_exists`: True
- `trade_opportunities_record_count`: 136
- `lp_quality_stats_cache_exists`: True
- `lp_quality_stats_record_count`: 791
- `asset_market_states_cache_exists`: True
- `asset_market_states_record_count`: 1
- `deletion_blocks_verified`: True
- `deletion_resets_quality_score`: False
- `deletion_weakens_telegram_and_no_trade_lock`: True
- `deletion_weakens_prealert_candidate_posterior`: True
- `minimum_continuous_days_to_recover`: 至少 3-7 天；要恢复 verified 可信度，建议先连续保留 7 天以上，覆盖不同波动环境。
- `evidence_of_data_reset`: ["raw_events date coverage is shorter than signals", "parsed_events date coverage is shorter than signals", "trade_opportunity candidate_history and verified_history are empty", "asset_market_states cache has only one asset record", "asset_cases cache has only one current case"]
- 删除影响结论：删除历史会让 CANDIDATE -> VERIFIED 缺少原生后验，使 quality/outcome 回到 fallback 或中性，使 asset state 与 NO_TRADE_LOCK 失去连续状态，也会削弱 prealert lifecycle 与 Telegram repeat suppression。

## 5. 非敏感运行配置摘要

- `DEFAULT_USER_TIER` = `research`
- `MARKET_CONTEXT_ADAPTER_MODE` = `live`
- `MARKET_CONTEXT_PRIMARY_VENUE` = `okx_perp`
- `MARKET_CONTEXT_SECONDARY_VENUE` = `kraken_futures`
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
- `LP_PREALERT_MIN_PRICING_CONFIDENCE` = `0.62`
- `LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY` = `0.24`
- `LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO` = `1.2`
- `LP_PREALERT_LIQUIDITY_REMOVAL_MIN_ACTION_INTENSITY` = `0.26`
- `LP_PREALERT_LIQUIDITY_REMOVAL_MIN_VOLUME_SURGE_RATIO` = `1.25`
- `LP_PREALERT_LIQUIDITY_ADDITION_MIN_ACTION_INTENSITY` = `0.3`
- `LP_PREALERT_LIQUIDITY_ADDITION_MIN_VOLUME_SURGE_RATIO` = `1.45`
- `LP_PREALERT_MIN_RESERVE_SKEW` = `0.08`
- `LP_PREALERT_MIN_CONFIRMATION` = `0.36`
- `LP_PREALERT_PRIMARY_TREND_MIN_MATCHES` = `2`
- `LP_PREALERT_MIN_USD` = `1500.0`
- `LP_PREALERT_MULTI_POOL_WINDOW_SEC` = `90`
- `LP_PREALERT_FOLLOWUP_WINDOW_SEC` = `60`
- `LP_ASSET_CASE_WINDOW_SEC` = `90`
- `LP_ASSET_CASE_MAX_TRACKED` = `512`
- `LP_QUALITY_HISTORY_LIMIT` = `800`
- `TRADE_ACTION_ENABLE` = `True`
- `TRADE_ACTION_REQUIRE_LIVE_CONTEXT_FOR_CHASE` = `True`
- `TRADE_ACTION_REQUIRE_BROADER_CONFIRM_FOR_CHASE` = `True`
- `TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE` = `0.62`
- `TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE` = `0.58`
- `TRADE_ACTION_CONFLICT_WINDOW_SEC` = `120`
- `TRADE_ACTION_MAX_LONG_BASIS_BPS` = `12.0`
- `TRADE_ACTION_MIN_SHORT_BASIS_BPS` = `-12.0`
- `ASSET_MARKET_STATE_ENABLE` = `True`
- `ASSET_MARKET_STATE_PERSIST_ENABLE` = `True`
- `ASSET_MARKET_STATE_RECOVER_ON_START` = `True`
- `ASSET_MARKET_STATE_MAX_TRACKED` = `256`
- `ASSET_MARKET_STATE_RECENT_SIGNAL_MAXLEN` = `24`
- `ASSET_MARKET_STATE_WAIT_TTL_SEC` = `180`
- `ASSET_MARKET_STATE_OBSERVE_TTL_SEC` = `240`
- `ASSET_MARKET_STATE_CANDIDATE_TTL_SEC` = `300`
- `ASSET_MARKET_STATE_RISK_TTL_SEC` = `300`
- `ASSET_MARKET_STATE_DATA_GAP_TTL_SEC` = `180`
- `NO_TRADE_LOCK_ENABLE` = `True`
- `NO_TRADE_LOCK_WINDOW_SEC` = `120`
- `NO_TRADE_LOCK_TTL_SEC` = `300`
- `NO_TRADE_LOCK_MIN_CONFLICT_SCORE` = `0.55`
- `NO_TRADE_LOCK_SUPPRESS_LOCAL_SIGNALS` = `True`
- `PREALERT_MIN_LIFETIME_SEC` = `60`
- `CHASE_ENABLE_AFTER_MIN_SAMPLES` = `20`
- `CHASE_MIN_FOLLOWTHROUGH_60S_RATE` = `0.58`
- `CHASE_MAX_ADVERSE_60S_RATE` = `0.35`
- `CHASE_REQUIRE_OUTCOME_COMPLETION_RATE` = `0.7`
- `TELEGRAM_SEND_ONLY_STATE_CHANGES` = `True`
- `TELEGRAM_SUPPRESS_REPEAT_STATE_SEC` = `300`
- `TELEGRAM_ALLOW_RISK_BLOCKERS` = `True`
- `TELEGRAM_ALLOW_CANDIDATES` = `True`
- `TELEGRAM_DEBUG_SUPPRESSED_IN_RESEARCH_REPORT` = `True`
- `OPPORTUNITY_ENABLE` = `True`
- `OPPORTUNITY_PERSIST_ENABLE` = `True`
- `OPPORTUNITY_RECOVER_ON_START` = `True`
- `OPPORTUNITY_HISTORY_LIMIT` = `1200`
- `OPPORTUNITY_MIN_CANDIDATE_SCORE` = `0.68`
- `OPPORTUNITY_MIN_VERIFIED_SCORE` = `0.78`
- `OPPORTUNITY_REQUIRE_LIVE_CONTEXT` = `True`
- `OPPORTUNITY_REQUIRE_BROADER_CONFIRM` = `True`
- `OPPORTUNITY_REQUIRE_OUTCOME_HISTORY` = `True`
- `OPPORTUNITY_MIN_HISTORY_SAMPLES` = `20`
- `OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE` = `0.58`
- `OPPORTUNITY_MAX_60S_ADVERSE_RATE` = `0.35`
- `OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE` = `0.7`
- `OPPORTUNITY_MAX_PER_ASSET_PER_HOUR` = `2`
- `OPPORTUNITY_COOLDOWN_SEC` = `900`
- `OPPORTUNITY_REPEAT_CANDIDATE_SUPPRESS_SEC` = `300`
- `OPPORTUNITY_RECENT_OPPOSITE_SIGNAL_WINDOW_SEC` = `120`

## 6. trade_opportunity 总览

- `cache_record_count`: 136
- `window_record_count`: 136
- `creation_status_distribution`: {"BLOCKED": 15, "NONE": 121}
- `current_status_distribution`: {"BLOCKED": 15, "NONE": 121}
- `opportunity_score_median`: 0.5136
- `opportunity_score_p90`: 0.6476
- `opportunity_score_max`: 0.8636
- `high_score_above_candidate_threshold_count`: 8
- `high_score_above_verified_threshold_count`: 1
- `why_no_verified`: ["no_candidate_rows_created", "opportunity_native_candidate_verified_history_is_empty_or_insufficient", "blocked_rows_top_blockers={'history_completion_too_low': 11, 'no_trade_lock': 3, 'late_or_chase': 1}", "lp_fallback_history_completion_rate_below_verified_gate"]

## 7. CANDIDATE / VERIFIED / BLOCKED 分析

- `opportunity_none_count`: 121
- `opportunity_candidate_count`: 0
- `opportunity_verified_count`: 0
- `opportunity_blocked_count`: 15
- `opportunity_expired_count`: 0
- `opportunity_invalidated_count`: 0
- `opportunity_candidate_to_verified_rate`: None
- `candidate_history_count`: 0
- `verified_history_count`: 0
- `blocker_history_count`: 15
- `estimated_samples_needed_for_verified`: 20
- 解释：本轮没有 candidate/verified，不能把 high-score NONE/BLOCKED 当成可交易机会；trade_opportunity 只作为个人辅助提示质量评估。

## 8. opportunity score 与机会质量分析

- score median/p90/max: `0.5136 / 0.6476 / 0.8636`
- all primary blocker distribution: `{"direction_conflict": 3, "history_completion_too_low": 13, "late_or_chase": 11, "local_absorption": 76, "no_trade_lock": 13, "sweep_exhaustion_risk": 19}`
- opportunity native history max: `0`; LP fallback history max: `398`

## 9. blockers 是否有效

- `blocked_count`: 15
- `no_trade_lock_blocked_count`: 3
- `data_gap_blocked_count`: 0
- `do_not_chase_blocked_count`: 1
- `conflict_blocked_count`: 0
- `blocker_avoided_adverse_rate`: None
- `blocker_avoided_adverse_rate_all_blocked_denominator`: 0.0
- hard blockers: `{"history_completion_too_low": 11, "late_or_chase": 1, "no_trade_lock": 3}`

## 10. Telegram 降噪与 suppression 分析

`{"telegram_should_send_count": 9, "telegram_suppressed_count": 129, "telegram_suppression_ratio": 0.9485, "telegram_suppression_reasons": {"no_trade_opportunity": 121, "same_blocker_repeat": 6}, "messages_before_suppression_estimate": 136, "messages_after_suppression_actual": 7, "delivered_lp_signals": 7, "suppressed_lp_signals": 129, "telegram_update_kind_distribution": {"risk_blocker": 9, "suppressed": 127}}`

## 11. asset_market_state 与 NO_TRADE_LOCK 分析

`{"state_distribution": {"DO_NOT_CHASE_LONG": 30, "DO_NOT_CHASE_SHORT": 29, "NO_TRADE_CHOP": 3, "NO_TRADE_LOCK": 13, "OBSERVE_LONG": 37, "OBSERVE_SHORT": 22, "SHORT_CANDIDATE": 1, "WAIT_CONFIRMATION": 1}, "state_transition_count": 63, "state_transition_distribution": {"DO_NOT_CHASE_LONG->DO_NOT_CHASE_SHORT": 4, "DO_NOT_CHASE_LONG->NO_TRADE_CHOP": 2, "DO_NOT_CHASE_LONG->NO_TRADE_LOCK": 1, "DO_NOT_CHASE_LONG->OBSERVE_LONG": 10, "DO_NOT_CHASE_SHORT->DO_NOT_CHASE_LONG": 6, "DO_NOT_CHASE_SHORT->NO_TRADE_CHOP": 1, "DO_NOT_CHASE_SHORT->NO_TRADE_LOCK": 1, "DO_NOT_CHASE_SHORT->OBSERVE_LONG": 1, "DO_NOT_CHASE_SHORT->OBSERVE_SHORT": 6, "NO_TRADE_CHOP->DO_NOT_CHASE_SHORT": 1, "NO_TRADE_CHOP->OBSERVE_SHORT": 2, "NO_TRADE_LOCK->DO_NOT_CHASE_LONG": 1, "NO_TRADE_LOCK->DO_NOT_CHASE_SHORT": 1, "NO_TRADE_LOCK->OBSERVE_LONG": 1, "NO_TRADE_LOCK->SHORT_CANDIDATE": 1, "OBSERVE_LONG->DO_NOT_CHASE_LONG": 8, "OBSERVE_LONG->DO_NOT_CHASE_SHORT": 4, "OBSERVE_LONG->OBSERVE_SHORT": 1, "OBSERVE_SHORT->DO_NOT_CHASE_LONG": 2, "OBSERVE_SHORT->DO_NOT_CHASE_SHORT": 5, "OBSERVE_SHORT->NO_TRADE_LOCK": 1, "OBSERVE_SHORT->WAIT_CONFIRMATION": 1, "SHORT_CANDIDATE->NO_TRADE_LOCK": 1, "WAIT_CONFIRMATION->OBSERVE_LONG": 1}, "final_state_by_asset": {"ETH": {"state": "OBSERVE_LONG", "label": "偏多观察", "updated_at_utc": "2026-04-21T05:08:56+00:00", "telegram_update_kind": "suppressed"}}, "no_trade_lock_entered_count": 4, "no_trade_lock_suppressed_count": 0, "no_trade_lock_released_count": 123, "cache_record_count": 1, "cache_final_records": [{"asset_symbol": "ETH", "asset_market_state_key": "OBSERVE_LONG", "asset_market_state_updated_at": 1776748132, "restored_from_cache": false, "transition_count": 124, "state_history_count": 16}]}`

## 12. prealert 生命周期分析

`{"prealert_candidate_count": 136, "prealert_gate_passed_count": 136, "prealert_active_count": 1, "prealert_delivered_count": 0, "prealert_upgraded_to_confirm_count": 4, "prealert_expired_count": 0, "median_prealert_to_confirm_sec": 48.0, "prealert_stage_count": 1, "lifecycle_distribution": {"active": 1, "merged": 59, "upgraded_to_confirm": 76}, "gate_fail_reasons": {}, "delivery_block_reasons": {"no_trade_opportunity": 116, "same_blocker_repeat": 5, "stage_budget_observe_tier2_tier_cap": 8}, "stage_overwritten_count": 135, "asset_case_preserved_count": 5}`

## 13. outcome 30s/60s/300s 完整性分析

`{"outcome_30s_completed_rate": 0.4559, "outcome_30s_completed_count": 62, "outcome_30s_status_distribution": {"completed": 62, "expired": 72, "pending": 2}, "outcome_60s_completed_rate": 0.4118, "outcome_60s_completed_count": 56, "outcome_60s_status_distribution": {"completed": 56, "expired": 78, "pending": 2}, "outcome_300s_completed_rate": 0.0, "outcome_300s_completed_count": 0, "outcome_300s_status_distribution": {"expired": 134, "pending": 2}, "outcome_price_source_distribution": {"okx_mark": 544}, "expired_rate_by_window": {"30s": 0.5294, "60s": 0.5735, "300s": 0.9853}, "outcome_failure_reason_distribution": {"window_elapsed_without_price_update": 418}}`

## 14. OKX/Kraken live market context 分析

`{"live_public_count": 136, "unavailable_count": 0, "live_public_rate": 1.0, "okx_attempts": 741, "okx_success": 740, "kraken_attempts": 0, "kraken_success": 0, "resolved_symbol_distribution": {"ETH-USDT-SWAP": 136}, "top_failure_reasons": {"timeout": 1}}`
- quality_reports --market-context-health live_public_hit_rate: `0.2181`

## 15. majors 覆盖与样本代表性

`{"asset_distribution": {"ETH": 136}, "pair_distribution": {"ETH/USDC": 69, "ETH/USDT": 67}, "covered_major_pairs": ["ETH/USDT", "ETH/USDC"], "missing_major_pairs": ["BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"], "missing_major_assets": ["BTC", "SOL"], "configured_but_disabled_major_pools": [{"index": 2, "pair_label": "BTC/USDT", "placeholder": false, "pool_address": "0x9db9e0e53058c89e5b94e29621a205198648425b"}, {"index": 3, "pair_label": "BTC/USDC", "placeholder": false, "pool_address": "0x004375dff511095cc5a197a54140a24efef3a416"}], "under_sampled_major_pairs": [{"pair_label": "BTC/USDT", "sample_size": 0}, {"pair_label": "BTC/USDC", "sample_size": 0}, {"pair_label": "SOL/USDT", "sample_size": 0}, {"pair_label": "SOL/USDC", "sample_size": 0}], "quality_converging_major_pairs": [{"actionable": true, "climax_reversal_score": 0.2193, "covered": true, "market_context_alignment_score": 0.9656, "pair_label": "ETH/USDT", "quality_score": 0.6688, "sample_size": 389}, {"actionable": true, "climax_reversal_score": 0.2621, "covered": true, "market_context_alignment_score": 0.9579, "pair_label": "ETH/USDC", "quality_score": 0.6746, "sample_size": 402}], "major_pool_coverage_cli": {"active_major_pools": [{"canonical_pair_label": "ETH/USDC", "is_primary_trend_pool": true, "major_match_mode": "major_family_match", "major_priority_score": 1.25, "pair_label": "ETH/USDC", "pool_address": "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "priority": 1, "sample_size": 402, "trend_pool_match_mode": "explicit_whitelist"}, {"canonical_pair_label": "ETH/USDT", "is_primary_trend_pool": true, "major_match_mode": "major_family_match", "major_priority_score": 1.25, "pair_label": "ETH/USDT", "pool_address": "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852", "priority": 1, "sample_size": 389, "trend_pool_match_mode": "explicit_whitelist"}], "configured_but_disabled_major_pools": [{"index": 2, "pair_label": "BTC/USDT", "placeholder": false, "pool_address": "0x9db9e0e53058c89e5b94e29621a205198648425b"}, {"index": 3, "pair_label": "BTC/USDC", "placeholder": false, "pool_address": "0x004375dff511095cc5a197a54140a24efef3a416"}], "configured_major_assets": ["ETH", "BTC", "SOL"], "configured_major_quotes": ["USDT", "USDC"], "covered_expected_pairs": ["ETH/USDT", "ETH/USDC"], "covered_major_pairs": ["ETH/USDT", "ETH/USDC"], "covered_major_pools": [{"canonical_pair_label": "ETH/USDC", "is_primary_trend_pool": true, "major_match_mode": "major_family_match", "major_priority_score": 1.25, "pair_label": "ETH/USDC", "pool_address": "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", "priority": 1, "sample_size": 402, "trend_pool_match_mode": "explicit_whitelist"}, {"canonical_pair_label": "ETH/USDT", "is_primary_trend_pool": true, "major_match_mode": "major_family_match", "major_priority_score": 1.25, "pair_label": "ETH/USDT", "pool_address": "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852", "priority": 1, "sample_size": 389, "trend_pool_match_mode": "explicit_whitelist"}], "expected_major_pairs": ["ETH/USDT", "ETH/USDC", "BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"], "major_pair_quality": [{"actionable": true, "climax_reversal_score": 0.2193, "covered": true, "market_context_alignment_score": 0.9656, "pair_label": "ETH/USDT", "quality_score": 0.6688, "sample_size": 389}, {"actionable": true, "climax_reversal_score": 0.2621, "covered": true, "market_context_alignment_score": 0.9579, "pair_label": "ETH/USDC", "quality_score": 0.6746, "sample_size": 402}, {"actionable": false, "climax_reversal_score": 0.45, "covered": false, "market_context_alignment_score": 0.5, "pair_label": "BTC/USDT", "quality_score": 0.5864, "sample_size": 0}, {"actionable": false, "climax_reversal_score": 0.45, "covered": false, "market_context_alignment_score": 0.5, "pair_label": "BTC/USDC", "quality_score": 0.5864, "sample_size": 0}, {"actionable": false, "climax_reversal_score": 0.45, "covered": false, "market_context_alignment_score": 0.5, "pair_label": "SOL/USDT", "quality_score": 0.5864, "sample_size": 0}, {"actionable": false, "climax_reversal_score": 0.45, "covered": false, "market_context_alignment_score": 0.5, "pair_label": "SOL/USDC", "quality_score": 0.5864, "sample_size": 0}], "malformed_major_pool_entries": [], "missing_expected_pairs": ["BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"], "missing_major_assets": ["BTC", "SOL"], "missing_major_pairs": ["BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"], "pool_book_exists": true, "pool_book_path": "/run-project/chain-monitor/data/lp_pools.json", "quality_converging_major_pairs": [{"actionable": true, "climax_reversal_score": 0.2193, "covered": true, "market_context_alignment_score": 0.9656, "pair_label": "ETH/USDT", "quality_score": 0.6688, "sample_size": 389}, {"actionable": true, "climax_reversal_score": 0.2621, "covered": true, "market_context_alignment_score": 0.9579, "pair_label": "ETH/USDC", "quality_score": 0.6746, "sample_size": 402}], "recommended_local_config_actions": ["补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT", "校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH"], "recommended_next_round_pairs": ["BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"], "suggestions": ["优先补齐：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC", "当前 majors outcome 样本仍偏少，建议先扩 BTC/SOL/更多 ETH 主池", "样本稀少的 majors pairs：BTC/USDT, BTC/USDC, SOL/USDT, SOL/USDC", "补齐本地 data/lp_pools.json：优先补 BTC/USDC, BTC/USDT, SOL/USDC, SOL/USDT", "校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH"], "under_sampled_major_assets": [{"asset_symbol": "BTC", "sample_size": 0}, {"asset_symbol": "SOL", "sample_size": 0}], "under_sampled_major_pairs": [{"pair_label": "BTC/USDT", "sample_size": 0}, {"pair_label": "BTC/USDC", "sample_size": 0}, {"pair_label": "SOL/USDT", "sample_size": 0}, {"pair_label": "SOL/USDC", "sample_size": 0}], "warnings": ["majors 主池覆盖不完整，下一轮应优先补 majors 而不是 long-tail", "缺少 major 资产覆盖：BTC, SOL", "存在 configured but disabled 的 major pools；覆盖缺口不一定是识别问题"]}}`

## 16. 数据保留策略建议

- `must_keep_long_term`: ["data/lp_quality_stats.cache.json", "data/trade_opportunities.cache.json", "data/asset_market_states.cache.json", "data/asset_cases.cache.json", "app/data/archive/signals/", "app/data/archive/delivery_audit/", "app/data/archive/cases/", "app/data/archive/case_followups/"]
- `keep_30_90_days`: ["app/data/archive/raw_events/", "app/data/archive/parsed_events/", "data/persisted_exchange_adjacent.json", "data/clmm_positions.cache.json"]
- `compressible_archive`: ["old app/data/archive/raw_events/*.ndjson", "old app/data/archive/parsed_events/*.ndjson", "old app/data/archive/cases/*.ndjson", "old app/data/archive/delivery_audit/*.ndjson"]
- `deletable_or_regenerable`: ["reports/*_latest.* regenerated reports", "data/*.example.json", "Python __pycache__ and test caches"]
- `specific_files`: {"app/data/archive/raw_events/": "至少保留 30-90 天；压缩可以，删除会破坏 raw->parsed->signal 回溯。", "app/data/archive/parsed_events/": "至少保留 30-90 天；删除会影响解析层漏报/误报审计。", "app/data/archive/signals/": "长期保留；这是 opportunity、Telegram suppression、state machine 的主审计账本。", "app/data/archive/cases/": "长期保留；删除会削弱 asset-case 生命周期与 prealert upgrade 回放。", "app/data/archive/case_followups/": "长期保留或压缩；用于验证 lifecycle 和后续消息。", "app/data/archive/delivery_audit/": "长期保留；用于证明 suppression 与实际送达，不应只依赖 Telegram 聊天记录。", "data/asset_cases.cache.json": "不要跑前删除；删除会造成 case 聚合和 prealert 继承冷启动。", "data/lp_quality_stats.cache.json": "不要跑前删除；删除会使 quality score 和 outcome history 回中性/低可信。", "data/asset_market_states.cache.json": "不要跑前删除；删除会让 NO_TRADE_LOCK、state transition、repeat suppression 冷启动。", "data/trade_opportunities.cache.json": "不要跑前删除；删除会清空 candidate/verified 后验、budget/cooldown 和机会原生样本。", "data/persisted_exchange_adjacent.json": "保留 30-90 天；删除会影响 adjacent/exchange 噪声识别和角色积累。", "data/clmm_positions.cache.json": "保留 30-90 天；删除会影响 CLMM position 历史与 LP 行为解释。"}

## 17. 数据库化建议

`{"must_add_database_now": false, "single_user_recommendation": "继续 JSON/NDJSON 跑满至少 7 天，同时准备 SQLite 第一期；暂不需要 Postgres。", "recommended_store": "SQLite for derived state/cache/report indexes; NDJSON remains the append-only audit log.", "phase_1_sqlite_tables": ["signals(signal_id, archive_ts, asset, pair, stage, direction, telegram fields, opportunity_id)", "lp_outcomes(signal_id, window_sec, status, source, direction_adjusted_move, adverse)", "trade_opportunities(opportunity_id, created_at, asset, pair, side, status_at_creation, current_status, score, primary_blocker)", "asset_market_states(asset, updated_at, state_key, previous_state_key, no_trade_lock fields)", "delivery_audit(signal_id, archive_ts, should_send, delivered, suppression_reason)", "asset_cases(asset_case_id, asset, started_at, updated_at, stage, supporting_pairs)"], "keep_ndjson_for": ["raw_events", "parsed_events", "full signal payloads", "delivery audit append-only evidence"], "cache_to_sqlite": ["lp_quality_stats.cache.json", "trade_opportunities.cache.json", "asset_market_states.cache.json", "asset_cases.cache.json"], "migration_plan": ["先写只读 backfill：NDJSON/cache -> SQLite，不改 runtime 写路径。", "对 latest reports 同时支持 SQLite 和 JSON/NDJSON source，结果做 diff。", "连续 7 天 diff 稳定后，再考虑 runtime 双写。"], "retention_policy": ["NDJSON 30-90 天热数据，超过后 gzip/zstd 压缩。", "SQLite 保留聚合索引和关键 outcome/opportunity/state 长期历史。", "latest 报告可覆盖，带日期快照的周报/月报保留。"], "must_index_fields": ["archive_ts", "signal_id", "asset_symbol", "pair_label", "asset_case_id", "outcome_tracking_key", "trade_opportunity_id", "trade_opportunity_status_at_creation", "trade_opportunity_primary_blocker", "telegram_suppression_reason", "asset_market_state_key"], "report_read_strategy_after_sqlite": "报表优先读 SQLite 索引表；需要原始 payload 时按 signal_id 回查 NDJSON。", "timing": "不建议今天立刻切 runtime；先连续保留 7 天数据，验证 schema 和字段稳定性后做 SQLite 第一期。"}`

## 18. 当前系统是否接近高质量个人交易辅助系统

当前系统已经具备研究采样、LP stage-first、state machine、NO_TRADE_LOCK、Telegram suppression 和 opportunity 审计层；但 latest overnight 数据里没有 CANDIDATE/VERIFIED，outcome completion 与 majors 覆盖不足，因此还不能称为高质量机会提示层。它更接近“可审计的个人研究辅助系统”，还不是稳定的机会筛选系统。

## 19. 最终评分

- `research_sampling_readiness` = `7.3/10`
- `opportunity_layer_usability` = `4.6/10`
- `candidate_quality` = `2.2/10`
- `verified_credibility` = `0.5/10`
- `blocker_protection_value` = `7.1/10`
- `telegram_noise_reduction` = `7.4/10`
- `outcome_completeness` = `4.3/10`
- `historical_data_sufficiency` = `4.0/10`
- `data_governance_readiness` = `5.0/10`
- `database_readiness` = `6.1/10`
- `overall_self_use_trading_assistant_score` = `4.7/10`

## 20. 下一轮建议

- 第一优先级是连续保留 7 天以上完整 NDJSON/cache，不再跑前删除关键 cache。
- 第二优先级是提高 outcome 30s/60s/300s completion，尤其是 60s 与 300s，否则 verified 门永远不稳。
- 第三优先级是补齐并启用 BTC/SOL major pools，让样本不再只有 ETH。
- 第四优先级是把 SQLite 作为只读索引层先接入 reports，不急着改 runtime 写路径。
- 第五优先级是单独复盘 high-score NONE/BLOCKED 样本，确认 local_absorption/no_trade_lock/late_or_chase 是否符合预期。

## 21. 限制与不确定性

- 本报告不提供仓位、止损、止盈或自动下单建议；trade_opportunity 仅作为个人交易辅助提示质量评估。
- 本报告严格避免输出 .env 中敏感字段；仅输出白名单非敏感运行配置。
- 机会层没有 CANDIDATE/VERIFIED，因此 candidate/verified 后验率为空，不可编造成胜率。
- opportunity outcome 在 cache 中仍全部 pending；blocker 避险率无法用 opportunity 自身 outcome 做强结论。
- 部分历史 raw/parsed 断档会限制 raw->parsed->signal 的完整回放能力。
- Kraken fallback 没有在本窗口真实触发，不能把 unavailable 解释为 broader confirmation。
