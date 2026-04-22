# Afternoon / Evening State Analysis

## 1. 执行摘要

- selected a fully covered Beijing afternoon-evening window from 2026-04-22 13:00:01 UTC+08:00 to 2026-04-22 13:21:54 UTC+08:00 (0.36h); raw/parsed/signals/delivery/cases all overlap in this interval.
- LP sample size in the selected window is 4 rows with 0 delivered LP Telegram messages.
- Telegram denoise is materially active: delivered/raw ratio is 0.0, suppression ratio is 1.0, and suppression reasons are dominated by {'no_trade_opportunity': 4}.
- asset-level market state is live in real data: 2 state changes, distribution {'DO_NOT_CHASE_LONG': 1, 'OBSERVE_LONG': 3}, final states {'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776835290, 'updated_at_utc': '2026-04-22 05:21:30 UTC', 'updated_at_beijing': '2026-04-22 13:21:30 UTC+08:00', 'telegram_update_kind': 'suppressed'}}.
- NO_TRADE_LOCK is real rather than theoretical: entered=0 suppressed=0 released=0.
- prealert lifecycle evidence is limited in this window: prealert stage rows=0, first_seen_stage=prealert count=1, median_prealert_to_confirm_sec=0.0.
- candidate/tradeable ladder is only partially validated: candidate rows=0, tradeable rows=0, blockers=['candidate_sample_count_below_min_samples(0<20)'].
- outcome price sourcing shifted away from prior pool_quote_proxy-heavy behavior: current sources={'okx_mark': 12}, previous pool_quote_proxy share=0.0, current share=0.0.

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-20.ndjson.gz` | records=`4171` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:52 UTC+08:00` | window_records=`` | note=`raw_events archive`
- `app/data/archive/raw_events/2026-04-21.ndjson` | records=`43792` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`raw_events archive`
- `app/data/archive/raw_events/2026-04-22.ndjson` | records=`49504` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`534` | note=`raw_events archive`
- `app/data/archive/parsed_events/2026-04-20.ndjson.gz` | records=`1921` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:50 UTC+08:00` | window_records=`` | note=`parsed_events archive`
- `app/data/archive/parsed_events/2026-04-21.ndjson` | records=`23985` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`parsed_events archive`
- `app/data/archive/parsed_events/2026-04-22.ndjson` | records=`22845` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`534` | note=`parsed_events archive`
- `app/data/archive/signals/2026-04-18.ndjson` | records=`424` | range_utc=`2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` | range_bj=`2026-04-18 13:26:52 UTC+08:00 -> 2026-04-18 23:57:32 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-19.ndjson` | records=`22` | range_utc=`2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC` | range_bj=`2026-04-19 23:39:05 UTC+08:00 -> 2026-04-19 23:59:27 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-20.ndjson.gz` | records=`76` | range_utc=`2026-04-20 15:03:53 UTC -> 2026-04-20 15:56:39 UTC` | range_bj=`2026-04-20 23:03:53 UTC+08:00 -> 2026-04-20 23:56:39 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-21.ndjson` | records=`450` | range_utc=`2026-04-20 16:00:15 UTC -> 2026-04-21 05:53:12 UTC` | range_bj=`2026-04-21 00:00:15 UTC+08:00 -> 2026-04-21 13:53:12 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-22.ndjson` | records=`661` | range_utc=`2026-04-21 16:06:03 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:06:03 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`6` | note=`signals archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson` | records=`54936` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:56 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson` | records=`2056` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:52 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-20.ndjson.gz` | records=`4260` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:52 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-21.ndjson` | records=`44724` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-22.ndjson` | records=`50620` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`534` | note=`delivery_audit archive`
- `app/data/archive/cases/2026-04-18.ndjson` | records=`40383` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:55 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-19.ndjson` | records=`1309` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:51 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-20.ndjson.gz` | records=`2378` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:50 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-21.ndjson` | records=`34950` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-22.ndjson` | records=`32878` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`833` | note=`cases archive`
- `app/data/archive/case_followups/2026-04-18.ndjson` | records=`33` | range_utc=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` | range_bj=`2026-04-18 13:32:17 UTC+08:00 -> 2026-04-18 22:53:00 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson` | records=`2` | range_utc=`2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC` | range_bj=`2026-04-19 23:41:15 UTC+08:00 -> 2026-04-19 23:54:32 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-20.ndjson.gz` | records=`5` | range_utc=`2026-04-20 15:21:28 UTC -> 2026-04-20 15:39:05 UTC` | range_bj=`2026-04-20 23:21:28 UTC+08:00 -> 2026-04-20 23:39:05 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-21.ndjson` | records=`67` | range_utc=`2026-04-20 16:05:52 UTC -> 2026-04-21 05:27:41 UTC` | range_bj=`2026-04-21 00:05:52 UTC+08:00 -> 2026-04-21 13:27:41 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-22.ndjson` | records=`44` | range_utc=`2026-04-21 16:06:52 UTC -> 2026-04-22 03:24:03 UTC` | range_bj=`2026-04-22 00:06:52 UTC+08:00 -> 2026-04-22 11:24:03 UTC+08:00` | window_records=`0` | note=`case_followups archive`
- `data/lp_quality_stats.cache.json` | records=`800` | range_utc=`2026-04-18 00:34:38 UTC -> 2026-04-22 05:21:51 UTC` | range_bj=`2026-04-18 08:34:38 UTC+08:00 -> 2026-04-22 13:21:51 UTC+08:00` | window_records=`` | note=`quality stats cache`
- `data/asset_cases.cache.json` | records=`1` | range_utc=`2026-04-22 05:16:27 UTC -> 2026-04-22 05:21:30 UTC` | range_bj=`2026-04-22 13:16:27 UTC+08:00 -> 2026-04-22 13:21:30 UTC+08:00` | window_records=`` | note=`asset case snapshot cache`
- `data/asset_market_states.cache.json` | records=`1` | range_utc=`2026-04-20 05:10:15 UTC -> 2026-04-22 05:21:32 UTC` | range_bj=`2026-04-20 13:10:15 UTC+08:00 -> 2026-04-22 13:21:32 UTC+08:00` | window_records=`` | note=`asset market state cache`
- `data/trade_opportunities.cache.json` | records=`332` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-22 05:26:26 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-22 13:26:26 UTC+08:00` | window_records=`` | note=`trade opportunity cache`
- `data/chain_monitor.sqlite` | records=`435250` | range_utc=` -> ` | range_bj=` -> ` | window_records=`` | note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'schema_meta': 1, 'runs': 0, 'raw_events': 97467, 'parsed_events': 48752, 'signals': 1633, 'signal_features': 6440, 'market_context_snapshots': 1633, 'market_context_attempts': 2283, 'outcomes': 3009, 'asset_cases': 5390, 'asset_market_states': 420, 'no_trade_locks': 342, 'trade_opportunities': 346, 'opportunity_outcomes': 1038, 'quality_stats': 1026, 'telegram_deliveries': 107031, 'prealert_lifecycle': 1829, 'delivery_audit': 156459, 'case_followups': 151} archive_rows_by_category={'raw_events': 97467, 'parsed_events': 48751, 'signals': 1633, 'delivery_audit': 156596, 'cases': 111898, 'case_followups': 151} db_archive_mirror_match_rate=0.9998 archive_fallback_used=False mismatch_warnings=['db_archive_mismatch:delivery_audit:sqlite=156459:archive=156596', 'db_archive_mirror_mismatch:parsed_events:sqlite=48752:archive=48751', 'db_archive_mirror_mismatch:delivery_audit:sqlite=156459:archive=156596']`

## 3. 北京时间下午到晚上分析窗口

- `cutoff_beijing=2026-04-22 13:00:00 UTC+08:00`
- `analysis_window_utc=2026-04-22 05:00:01 UTC -> 2026-04-22 05:21:54 UTC`
- `analysis_window_server=2026-04-22 05:00:01 UTC -> 2026-04-22 05:21:54 UTC`
- `analysis_window_beijing=2026-04-22 13:00:01 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00`
- `duration_hours=0.36`
- selection_reason: selected the longest continuous segment after Beijing 13:00 using raw_events and parsed_events as the runtime heartbeat; signals/delivery_audit/cases are analyzed inside the chosen heartbeat window instead of defining it, continuity itself is anchored on raw/parsed heartbeats rather than LP-signal gaps, because the service can be healthy even when LP opportunities are sparse.
- partial_lead_counts_before_window={'raw_events': 0, 'parsed_events': 0}
- gap_summary={'gap_ge_300_count': 0, 'gap_ge_300_examples': [], 'gap_ge_600_count': 0, 'gap_ge_600_examples': [], 'gap_ge_900_count': 0, 'gap_ge_900_examples': [], 'gap_ge_1800_count': 0, 'gap_ge_1800_examples': []}

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
- `TRADE_ACTION_ENABLE` = `True`
- `ASSET_MARKET_STATE_ENABLE` = `True`
- `NO_TRADE_LOCK_ENABLE` = `True`
- `NO_TRADE_LOCK_WINDOW_SEC` = `120`
- `NO_TRADE_LOCK_TTL_SEC` = `300`
- `TELEGRAM_SEND_ONLY_STATE_CHANGES` = `True`
- `TELEGRAM_SUPPRESS_REPEAT_STATE_SEC` = `300`
- `CHASE_ENABLE_AFTER_MIN_SAMPLES` = `20`
- `CHASE_MIN_FOLLOWTHROUGH_60S_RATE` = `0.58`
- `CHASE_MAX_ADVERSE_60S_RATE` = `0.35`
- `CHASE_REQUIRE_OUTCOME_COMPLETION_RATE` = `0.7`
- `OPPORTUNITY_ENABLE` = `True`
- `OPPORTUNITY_REQUIRE_LIVE_CONTEXT` = `True`
- `OPPORTUNITY_REQUIRE_BROADER_CONFIRM` = `True`
- `OPPORTUNITY_REQUIRE_OUTCOME_HISTORY` = `True`
- `OPPORTUNITY_MIN_CANDIDATE_SCORE` = `0.68`
- `OPPORTUNITY_MIN_VERIFIED_SCORE` = `0.78`
- `OPPORTUNITY_MIN_HISTORY_SAMPLES` = `20`
- `OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE` = `0.58`
- `OPPORTUNITY_MAX_60S_ADVERSE_RATE` = `0.35`
- `OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE` = `0.7`
- `OPPORTUNITY_MAX_PER_ASSET_PER_HOUR` = `2`
- `OPPORTUNITY_COOLDOWN_SEC` = `900`
- `OUTCOME_SCHEDULER_ENABLE` = `True`
- `OUTCOME_TICK_INTERVAL_SEC` = `5`
- `OUTCOME_WINDOW_GRACE_SEC` = `20`
- `OUTCOME_CATCHUP_MAX_SEC` = `900`
- `OUTCOME_SETTLE_BATCH_SIZE` = `200`
- `OUTCOME_USE_MARKET_CONTEXT_PRICE` = `True`
- `OUTCOME_MARKET_CONTEXT_REFRESH_ON_DUE` = `True`
- `OUTCOME_PREFER_OKX_MARK` = `True`
- `OUTCOME_ALLOW_CATCHUP_WITH_LATEST_MARK` = `True`
- `OUTCOME_EXPIRE_AFTER_SEC` = `1800`
- `SQLITE_ENABLE` = `True`
- `SQLITE_DB_PATH` = `data/chain_monitor.sqlite`
- `SQLITE_REPORT_READ_PREFER_DB` = `True`
- `SQLITE_REPORT_FALLBACK_TO_ARCHIVE` = `True`
- `REPORT_ARCHIVE_READ_GZIP` = `True`
- `REPORT_DB_ARCHIVE_COMPARE` = `True`
- `REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH` = `False`

## 5. asset-level market state 总览

- `state_distribution={'DO_NOT_CHASE_LONG': 1, 'OBSERVE_LONG': 3}`
- `state_transition_count=2` `state_changed_count=2`
- `repeated_state_suppressed_count=0`
- `final_state_by_asset={'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776835290, 'updated_at_utc': '2026-04-22 05:21:30 UTC', 'updated_at_beijing': '2026-04-22 13:21:30 UTC+08:00', 'telegram_update_kind': 'suppressed'}}`
- `eth_state_path=[{'ts': 1776834988, 'utc': '2026-04-22 05:16:28 UTC', 'beijing': '2026-04-22 13:16:28 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'suppressed'}, {'ts': 1776835011, 'utc': '2026-04-22 05:16:51 UTC', 'beijing': '2026-04-22 13:16:51 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'suppressed'}]`

## 6. Telegram 降噪与 suppression 分析

- `messages_before_suppression_estimate=4` `messages_after_suppression_actual=0`
- `telegram_should_send_count=0` `telegram_suppressed_count=4`
- `telegram_suppression_ratio=1.0`
- `telegram_suppression_reasons={'no_trade_opportunity': 4}`
- `telegram_update_kind_distribution={'suppressed': 4}`
- `high_value_suppressed_count=2` `should_send_but_not_sent_count=0`

## 7. NO_TRADE_LOCK 分析

- `lock_entered_count=0` `lock_suppressed_count=0` `lock_released_count=0`
- `avg_lock_duration_sec=None` `median_lock_duration_sec=None`
- `lock_release_reasons={}`
- `lock_suppressed_stage_distribution={}`
- `missed_lock_examples=[]`

## 8. prealert 生命周期分析

- `prealert_count=0`
- `prealert_candidate_count=1`
- `prealert_gate_passed_count=1`
- `prealert_active_count=0`
- `prealert_delivered_count=0`
- `prealert_merged_count=0`
- `prealert_upgraded_to_confirm_count=1`
- `prealert_expired_count=0`
- `prealert_suppressed_by_lock_count=0`
- `median_prealert_to_confirm_sec=0.0`
- `first_seen_stage_prealert_count=1`
- `prealert_stage_overwritten_count=1`
- `first_seen_stage_distribution={'prealert': 4}`

## 9. candidate vs tradeable 分析

- `long_candidate_count=0`
- `short_candidate_count=0`
- `tradeable_long_count=0`
- `tradeable_short_count=0`
- `candidate_to_tradeable_count=0`
- `candidate_outcome_completed_rate=None`
- `candidate_followthrough_60s_rate=None`
- `candidate_adverse_60s_rate=None`
- `legacy_chase_action_rows=0`
- `primary_tradeable_blockers=['candidate_sample_count_below_min_samples(0<20)']`

## 9A. trade_opportunity 分析

- `opportunity_summary={'candidates': 0, 'verified': 0, 'blocked': 68, 'none': 264}`
- `opportunity_score_median=0.52775` `opportunity_score_p90=0.6865`
- `candidate_outcome_60s={'count': 0, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {}}`
- `verified_outcome_60s={'count': 0, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {}}`
- `blocker_effectiveness={'count': 68, 'avoided_adverse_count': 0, 'avoided_adverse_rate': 0.0}`
- `opportunity_budget_suppressed_count=0` `opportunity_cooldown_suppressed_count=0`
- `why_no_opportunities=["top_blockers={'history_completion_too_low': 39, 'late_or_chase': 21, 'no_trade_lock': 8}", 'no_candidate_reached_opportunity_score_gate']`
- `top_blockers={'history_completion_too_low': 39, 'late_or_chase': 21, 'no_trade_lock': 8}`
- `next_threshold_suggestions=[]`

## 10. outcome price source 与 30s/60s/300s 分析

- `window_status_distribution={'30s': {'completed': 2, 'pending': 2}, '60s': {'completed': 2, 'pending': 2}, '300s': {'completed': 2, 'pending': 2}}`
- `outcome_price_source_distribution={'okx_mark': 12}`
- `outcome_failure_reason_distribution={}`
- `scheduler_health_summary={'pending_count': 6, 'completed_count': 6, 'unavailable_count': 0, 'expired_count': 0, 'catchup_completed_count': 0, 'catchup_expired_count': 0, 'settled_by_distribution': {'outcome_scheduler': 6}}`
- `expired_rate_by_window={'30s': 0.0, '60s': 0.0, '300s': 0.0}`
- previous baseline `source_distribution={'okx_mark': 784}`
- `pool_quote_proxy_share_delta_vs_previous=0.0`

## 11. OKX/Kraken live market context 分析

- window `live_public_count=4` `unavailable_count=0`
- window `okx_attempts=24` `okx_success=24` `okx_failure=0`
- window `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`
- window `requested_to_resolved_distribution={'ETHUSDC->ETH-USDT-SWAP': 2, 'ETHUSDT->ETH-USDT-SWAP': 2}`
- CLI full `live_public_hit_rate=0.2498` `unavailable_rate=0.7502`

## 12. trade_action 与交易辅助价值分析

- `trade_action_distribution={'DO_NOT_CHASE_LONG': 1, 'LONG_BIAS_OBSERVE': 3}`
- `delivered_distribution={}`
- `do_not_chase_would_have_avoided_bad_chase_rate=0.0`
- `no_trade_high_noise_rate_300s=None`
- `generic_confirm_success_rate_300s=1.0` `generic_confirm_adverse_rate_300s=0.0`

## 13. 卖压后涨 / 买压后跌 反例专项

- `sell_pressure_adverse_30s_rate=None`
- `sell_pressure_adverse_60s_rate=None`
- `sell_pressure_adverse_300s_rate=None`
- `buy_pressure_adverse_30s_rate=0.0`
- `buy_pressure_adverse_60s_rate=0.0`
- `buy_pressure_adverse_300s_rate=0.0`
- `counterexample_reason_distribution={}`
- `top_counterexamples=[]`

## 14. raw/parsed/signals archive 完整性

- `raw_archive_exists=True`
- `parsed_archive_exists=True`
- `signals_archive_exists=True`
- `signal_delivery_audit_match_rate=1.0`
- `signal_case_followup_match_rate=None`
- `signal_case_attached_match_rate=None`
- `delivered_signal_notifier_sent_at_rate=None`
- `signal_event_in_parsed_rate=1.0`
- `suppressed_signal_delivery_match_rate=1.0`

## 15. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `asset_distribution={'ETH': 4}`
- `pair_distribution={'ETH/USDC': 2, 'ETH/USDT': 2}`
- CLI `recommended_next_round_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`

## 16. 噪音与误判风险评估

- `delivered_lp_ratio=0.0`
- `suppressed_ratio=1.0`
- `high_value_suppressed_count=2`
- `direction_conflict_pairs_within_5m=0`
- `state_change_message_count=0`
- `risk_blocker_message_count=0`

## 17. 最终评分

- `research_sampling_readiness=5.0/10`
- `telegram_denoise_effect=8.0/10`
- `asset_market_state_usability=6.5/10`
- `no_trade_lock_protection_value=3.5/10`
- `prealert_lifecycle_effectiveness=6.0/10`
- `candidate_tradeable_credibility=3.8/10`
- `outcome_completeness=7.0/10`
- `live_market_context_readiness=9.5/10`
- `trade_action_assistive_value=5.0/10`
- `noise_control=7.1/10`
- `overall_self_use_score=6.1/10`

## 18. 下一轮建议

- collect more Beijing-day prealert samples and verify first_seen_stage/prealert_to_confirm_sec survive all later upgrades.
- keep improving 30s/60s outcome completion until short-window reversal checks are reliably usable.
- prioritize enabling and validating BTC/SOL major pools before expanding any long-tail coverage.
- audit suppressed high-value rows and confirm they were delivery-policy drops rather than unintended Telegram suppression.

## 19. 限制与不确定性

- selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- 30s outcome completion is still incomplete enough that very short-term reversal conclusions must stay conservative.
- BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.

## 附录: quality_reports CLI 参考

- `market-context-health.live_public_hit_rate=0.2498`
- `major-pool-coverage.covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `summary.overall={'actionable': True, 'climax_count': 221, 'climax_reversal_count': 27, 'climax_reversal_score': 0.2122, 'confirm_conversion_score': 0.3445, 'confirm_count': 460, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 0, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776835286, 'market_context_alignment_positive_count': 792, 'market_context_alignment_score': 0.9615, 'market_context_available_count': 794, 'prealert_confirmed_count': 1, 'prealert_count': 2, 'prealert_false_count': 0, 'prealert_precision_score': 0.685, 'quality_hint': '历史一般', 'quality_score': 0.6501, 'resolved_climaxes': 140, 'resolved_confirms': 266, 'resolved_fastlanes': 0, 'resolved_prealerts': 1, 'sample_size': 800, 'tuning_hint': '保持当前阈值'}`
