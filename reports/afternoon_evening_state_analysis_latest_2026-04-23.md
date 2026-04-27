# Afternoon / Evening State Analysis

## 1. 执行摘要

- selected a fully covered Beijing afternoon-evening window from 2026-04-23 13:00:06 UTC+08:00 to 2026-04-23 13:16:15 UTC+08:00 (0.27h); raw/parsed/signals/delivery/cases all overlap in this interval.
- LP sample size in the selected window is 5 rows with 2 delivered LP Telegram messages.
- Telegram denoise is materially active: delivered/raw ratio is 0.4, suppression ratio is 0.6, and suppression reasons are dominated by {'no_trade_opportunity': 2, 'same_opportunity_repeat': 1}.
- asset-level market state is live in real data: 1 state changes, distribution {'OBSERVE_LONG': 5}, final states {'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776920542, 'updated_at_utc': '2026-04-23 05:02:22 UTC', 'updated_at_beijing': '2026-04-23 13:02:22 UTC+08:00', 'telegram_update_kind': 'suppressed'}}.
- NO_TRADE_LOCK is real rather than theoretical: entered=0 suppressed=0 released=1.
- prealert lifecycle evidence is limited in this window: prealert stage rows=0, first_seen_stage=prealert count=1, median_prealert_to_confirm_sec=0.0.
- candidate/tradeable ladder is only partially validated: candidate rows=0, tradeable rows=0, blockers=['candidate_sample_count_below_min_samples(0<20)'].
- outcome price sourcing shifted away from prior pool_quote_proxy-heavy behavior: current sources={'okx_mark': 15}, previous pool_quote_proxy share=0.0, current share=0.0.

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-20.ndjson.gz` | records=`4171` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:52 UTC+08:00` | window_records=`` | note=`raw_events archive`
- `app/data/archive/raw_events/2026-04-21.ndjson.gz` | records=`43792` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`raw_events archive`
- `app/data/archive/raw_events/2026-04-22.ndjson` | records=`49504` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`` | note=`raw_events archive`
- `app/data/archive/raw_events/2026-04-23.ndjson` | records=`39649` | range_utc=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` | range_bj=`2026-04-23 00:05:04 UTC+08:00 -> 2026-04-23 13:16:15 UTC+08:00` | window_records=`1064` | note=`raw_events archive`
- `app/data/archive/parsed_events/2026-04-20.ndjson.gz` | records=`1921` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:50 UTC+08:00` | window_records=`` | note=`parsed_events archive`
- `app/data/archive/parsed_events/2026-04-21.ndjson.gz` | records=`23985` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`parsed_events archive`
- `app/data/archive/parsed_events/2026-04-22.ndjson` | records=`22845` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`` | note=`parsed_events archive`
- `app/data/archive/parsed_events/2026-04-23.ndjson` | records=`19055` | range_utc=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` | range_bj=`2026-04-23 00:05:04 UTC+08:00 -> 2026-04-23 13:16:15 UTC+08:00` | window_records=`578` | note=`parsed_events archive`
- `app/data/archive/signals/2026-04-18.ndjson` | records=`424` | range_utc=`2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` | range_bj=`2026-04-18 13:26:52 UTC+08:00 -> 2026-04-18 23:57:32 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-19.ndjson` | records=`22` | range_utc=`2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC` | range_bj=`2026-04-19 23:39:05 UTC+08:00 -> 2026-04-19 23:59:27 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-20.ndjson.gz` | records=`76` | range_utc=`2026-04-20 15:03:53 UTC -> 2026-04-20 15:56:39 UTC` | range_bj=`2026-04-20 23:03:53 UTC+08:00 -> 2026-04-20 23:56:39 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-21.ndjson.gz` | records=`450` | range_utc=`2026-04-20 16:00:15 UTC -> 2026-04-21 05:53:12 UTC` | range_bj=`2026-04-21 00:00:15 UTC+08:00 -> 2026-04-21 13:53:12 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-22.ndjson` | records=`661` | range_utc=`2026-04-21 16:06:03 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:06:03 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-23.ndjson` | records=`508` | range_utc=`2026-04-22 18:30:40 UTC -> 2026-04-23 05:13:15 UTC` | range_bj=`2026-04-23 02:30:40 UTC+08:00 -> 2026-04-23 13:13:15 UTC+08:00` | window_records=`9` | note=`signals archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson` | records=`54936` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:56 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson` | records=`2056` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:52 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-20.ndjson.gz` | records=`4260` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:52 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:52 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-21.ndjson.gz` | records=`44724` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-22.ndjson` | records=`50620` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-23.ndjson` | records=`40820` | range_utc=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` | range_bj=`2026-04-23 00:05:04 UTC+08:00 -> 2026-04-23 13:16:15 UTC+08:00` | window_records=`1097` | note=`delivery_audit archive`
- `app/data/archive/cases/2026-04-18.ndjson` | records=`40383` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:55 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-19.ndjson` | records=`1309` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:51 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-20.ndjson.gz` | records=`2378` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-20 15:59:50 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-20 23:59:50 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-21.ndjson.gz` | records=`34950` | range_utc=`2026-04-20 16:00:05 UTC -> 2026-04-21 05:56:14 UTC` | range_bj=`2026-04-21 00:00:05 UTC+08:00 -> 2026-04-21 13:56:14 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-22.ndjson` | records=`32878` | range_utc=`2026-04-21 16:04:52 UTC -> 2026-04-22 05:21:54 UTC` | range_bj=`2026-04-22 00:04:52 UTC+08:00 -> 2026-04-22 13:21:54 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-23.ndjson` | records=`27318` | range_utc=`2026-04-22 16:05:04 UTC -> 2026-04-23 05:16:15 UTC` | range_bj=`2026-04-23 00:05:04 UTC+08:00 -> 2026-04-23 13:16:15 UTC+08:00` | window_records=`831` | note=`cases archive`
- `app/data/archive/case_followups/2026-04-18.ndjson` | records=`33` | range_utc=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` | range_bj=`2026-04-18 13:32:17 UTC+08:00 -> 2026-04-18 22:53:00 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson` | records=`2` | range_utc=`2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC` | range_bj=`2026-04-19 23:41:15 UTC+08:00 -> 2026-04-19 23:54:32 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-20.ndjson.gz` | records=`5` | range_utc=`2026-04-20 15:21:28 UTC -> 2026-04-20 15:39:05 UTC` | range_bj=`2026-04-20 23:21:28 UTC+08:00 -> 2026-04-20 23:39:05 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-21.ndjson.gz` | records=`67` | range_utc=`2026-04-20 16:05:52 UTC -> 2026-04-21 05:27:41 UTC` | range_bj=`2026-04-21 00:05:52 UTC+08:00 -> 2026-04-21 13:27:41 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-22.ndjson` | records=`44` | range_utc=`2026-04-21 16:06:52 UTC -> 2026-04-22 03:24:03 UTC` | range_bj=`2026-04-22 00:06:52 UTC+08:00 -> 2026-04-22 11:24:03 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-23.ndjson` | records=`45` | range_utc=`2026-04-22 18:36:04 UTC -> 2026-04-23 05:02:06 UTC` | range_bj=`2026-04-23 02:36:04 UTC+08:00 -> 2026-04-23 13:02:06 UTC+08:00` | window_records=`2` | note=`case_followups archive`
- `data/lp_quality_stats.cache.json` | records=`800` | range_utc=`2026-04-19 13:06:51 UTC -> 2026-04-23 05:07:28 UTC` | range_bj=`2026-04-19 21:06:51 UTC+08:00 -> 2026-04-23 13:07:28 UTC+08:00` | window_records=`` | note=`quality stats cache`
- `data/asset_cases.cache.json` | records=`1` | range_utc=`2026-04-23 05:01:15 UTC -> 2026-04-23 05:02:25 UTC` | range_bj=`2026-04-23 13:01:15 UTC+08:00 -> 2026-04-23 13:02:25 UTC+08:00` | window_records=`` | note=`asset case snapshot cache`
- `data/asset_market_states.cache.json` | records=`1` | range_utc=`2026-04-20 05:10:15 UTC -> 2026-04-23 05:02:24 UTC` | range_bj=`2026-04-20 13:10:15 UTC+08:00 -> 2026-04-23 13:02:24 UTC+08:00` | window_records=`` | note=`asset market state cache`
- `data/trade_opportunities.cache.json` | records=`485` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-23 05:07:04 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-23 13:07:04 UTC+08:00` | window_records=`` | note=`trade opportunity cache`
- `data/chain_monitor.sqlite` | records=`605437` | range_utc=` -> ` | range_bj=` -> ` | window_records=`` | note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'schema_meta': 1, 'runs': 0, 'raw_events': 137116, 'parsed_events': 67807, 'signals': 2142, 'signal_features': 9696, 'market_context_snapshots': 2142, 'market_context_attempts': 3203, 'outcomes': 3468, 'asset_cases': 9999, 'asset_market_states': 582, 'no_trade_locks': 502, 'trade_opportunities': 675, 'opportunity_outcomes': 2025, 'quality_stats': 1270, 'telegram_deliveries': 164840, 'prealert_lifecycle': 2494, 'delivery_audit': 197279, 'case_followups': 196} archive_rows_by_category={'raw_events': 137116, 'parsed_events': 67806, 'signals': 2141, 'delivery_audit': 197416, 'cases': 139216, 'case_followups': 196} db_archive_mirror_match_rate=0.9998 archive_fallback_used=False mismatch_warnings=['db_archive_mismatch:signals:sqlite=2142:archive=2141', 'db_archive_mismatch:delivery_audit:sqlite=197279:archive=197416', 'db_archive_mirror_mismatch:parsed_events:sqlite=67807:archive=67806', 'db_archive_mirror_mismatch:signals:sqlite=2142:archive=2141', 'db_archive_mirror_mismatch:delivery_audit:sqlite=197279:archive=197416']`

## 3. 北京时间下午到晚上分析窗口

- `cutoff_beijing=2026-04-23 13:00:00 UTC+08:00`
- `analysis_window_utc=2026-04-23 05:00:06 UTC -> 2026-04-23 05:16:15 UTC`
- `analysis_window_server=2026-04-23 05:00:06 UTC -> 2026-04-23 05:16:15 UTC`
- `analysis_window_beijing=2026-04-23 13:00:06 UTC+08:00 -> 2026-04-23 13:16:15 UTC+08:00`
- `duration_hours=0.27`
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
- `OPPORTUNITY_CALIBRATION_ENABLE` = `True`
- `OPPORTUNITY_CALIBRATION_MIN_SAMPLES` = `20`
- `OPPORTUNITY_CALIBRATION_STRONG_SAMPLES` = `50`
- `OPPORTUNITY_CALIBRATION_MAX_POSITIVE_ADJUSTMENT` = `0.08`
- `OPPORTUNITY_CALIBRATION_MAX_NEGATIVE_ADJUSTMENT` = `-0.12`
- `OPPORTUNITY_CALIBRATION_COMPLETION_MIN` = `0.7`
- `OPPORTUNITY_CALIBRATION_ADVERSE_MAX` = `0.35`
- `OPPORTUNITY_CALIBRATION_FOLLOWTHROUGH_MIN` = `0.58`
- `OPPORTUNITY_NON_LP_EVIDENCE_ENABLE` = `True`
- `OPPORTUNITY_NON_LP_EVIDENCE_WINDOW_SEC` = `300`
- `OPPORTUNITY_NON_LP_SCORE_WEIGHT` = `0.1`
- `OPPORTUNITY_NON_LP_STRONG_BLOCKER_ENABLE` = `True`
- `OPPORTUNITY_NON_LP_TENTATIVE_WEIGHT` = `0.25`
- `OPPORTUNITY_NON_LP_OBSERVE_WEIGHT` = `0.0`
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

- `state_distribution={'OBSERVE_LONG': 5}`
- `state_transition_count=1` `state_changed_count=1`
- `repeated_state_suppressed_count=0`
- `final_state_by_asset={'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776920542, 'updated_at_utc': '2026-04-23 05:02:22 UTC', 'updated_at_beijing': '2026-04-23 13:02:22 UTC+08:00', 'telegram_update_kind': 'suppressed'}}`
- `eth_state_path=[{'ts': 1776920477, 'utc': '2026-04-23 05:01:17 UTC', 'beijing': '2026-04-23 13:01:17 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}]`

## 6. Telegram 降噪与 suppression 分析

- `messages_before_suppression_estimate=5` `messages_after_suppression_actual=2`
- `telegram_should_send_count=2` `telegram_suppressed_count=3`
- `telegram_suppression_ratio=0.6`
- `telegram_suppression_reasons={'no_trade_opportunity': 2, 'same_opportunity_repeat': 1}`
- `telegram_update_kind_distribution={'candidate': 1, 'state_change': 1, 'suppressed': 3}`
- `high_value_suppressed_count=0` `should_send_but_not_sent_count=0`

## 7. NO_TRADE_LOCK 分析

- `lock_entered_count=0` `lock_suppressed_count=0` `lock_released_count=1`
- `avg_lock_duration_sec=None` `median_lock_duration_sec=None`
- `lock_release_reasons={'ttl_expired': 1}`
- `lock_suppressed_stage_distribution={}`
- `missed_lock_examples=[]`

## 8. prealert 生命周期分析

- `prealert_count=0`
- `prealert_candidate_count=1`
- `prealert_gate_passed_count=1`
- `prealert_active_count=0`
- `prealert_delivered_count=0`
- `prealert_merged_count=1`
- `prealert_upgraded_to_confirm_count=1`
- `prealert_expired_count=0`
- `prealert_suppressed_by_lock_count=0`
- `median_prealert_to_confirm_sec=0.0`
- `first_seen_stage_prealert_count=1`
- `prealert_stage_overwritten_count=1`
- `first_seen_stage_distribution={'prealert': 5}`

## 9. candidate vs tradeable 分析

- `long_candidate_count=0`
- `short_candidate_count=0`
- `tradeable_long_count=0`
- `tradeable_short_count=0`
- `candidate_to_tradeable_count=0`
- `candidate_outcome_completed_rate=None`
- `candidate_followthrough_60s_rate=None`
- `candidate_adverse_60s_rate=None`
- `legacy_chase_action_rows=2`
- `primary_tradeable_blockers=['candidate_sample_count_below_min_samples(0<20)']`

## 9A. trade_opportunity 分析

- `opportunity_summary={'candidates': 3, 'verified': 0, 'blocked': 84, 'none': 398}`
- `opportunity_score_median=0.5175` `opportunity_score_p90=0.6821`
- `raw_score_vs_calibrated_score={'raw_median': 0.5261, 'calibrated_median': 0.5175, 'raw_p90': 0.7087, 'calibrated_p90': 0.6821}`
- `calibration_adjustment_distribution={'positive_count': 0, 'negative_count': 151, 'neutral_count': 2, 'median': -0.03, 'p90': -0.03, 'min': -0.03, 'max': 0.0}`
- `candidate_outcome_60s={'count': 3, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {'neutral': 3}}`
- `verified_outcome_60s={'count': 0, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {}}`
- `opportunity_profile_count=31`
- `top_profiles_by_sample=[{'profile_key': 'ETH|LONG|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 2, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|SHORT|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 1, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}]`
- `top_profiles_by_followthrough=[]`
- `top_profiles_by_adverse=[]`
- `profiles_ready_for_verified=[]`
- `estimated_samples_needed_for_verified_by_profile={'ETH|LONG|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 18, 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 19, 'ETH|SHORT|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20}`
- `final_trading_output_distribution={'asset_market_state': 1, 'trade_action_legacy': 2, 'trade_opportunity': 2}`
- `legacy_chase_downgraded_count=2` `legacy_chase_leaked_count=0` `messages_blocked_by_opportunity_gate=0`
- `all_opportunity_labels_verified=True` `all_candidate_labels_are_candidate=True` `blocked_covers_legacy_chase_risk=True`
- `blocker_effectiveness={'count': 84, 'avoided_adverse_count': 0, 'avoided_adverse_rate': 0.0, 'false_block_count': 0, 'false_block_rate': 0.0, 'top_effective_blockers': [{'blocker': 'history_completion_too_low', 'count': 39, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'late_or_chase', 'count': 32, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'no_trade_lock', 'count': 12, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'strong_opposite_signal', 'count': 1, 'saved_rate': 0.0, 'false_block_rate': 0.0}], 'top_overblocking_blockers': [{'blocker': 'history_completion_too_low', 'count': 39, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'late_or_chase', 'count': 32, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'no_trade_lock', 'count': 12, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'strong_opposite_signal', 'count': 1, 'saved_rate': 0.0, 'false_block_rate': 0.0}]}`
- `hard_blocker_distribution={'history_completion_too_low': 39, 'late_or_chase': 32, 'no_trade_lock': 12, 'strong_opposite_signal': 1}`
- `verification_blocker_distribution={'profile_completion_too_low': 3}`
- `non_lp_evidence_summary={'available_count': 0, 'support_count': 0, 'risk_count': 0, 'blocked_count': 0, 'conflict_count': 0, 'upgraded_count': 0}`
- `opportunity_non_lp_support_count=0` `opportunity_non_lp_risk_count=0`
- `top_non_lp_supporting_evidence=[]`
- `top_non_lp_blockers=[]`
- `opportunities_upgraded_by_non_lp=0` `opportunities_blocked_by_non_lp=0`
- `opportunities_upgraded_by_calibration=0` `opportunities_downgraded_by_calibration=3`
- `candidates_blocked_by_calibration=0` `verified_allowed_by_calibration=0`
- `top_positive_adjustments=[]`
- `top_negative_adjustments=[{'trade_opportunity_id': 'opp_001f1f41d9977137', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_00bf79dbcfedcc47', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_017d9150c5b63cc5', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_0213df5b0f4e1882', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_028de77db6e7469a', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_04623485866c720e', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_04c756aca8791bce', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_07968978db798690', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_08c9fc97d40f8e5c', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_0bb4eff1b1a131e7', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}]`
- `calibration_reason_distribution={'completion<0.50; adverse>0.45': 72, 'completion<0.50; adverse>0.45; followthrough<0.50': 79, 'neutral_calibration': 2}`
- `calibration_source_distribution={'none': 485}`
- `calibration_confidence_distribution={'low': 485}`
- `non_lp_conflict_cases=[]`
- `opportunity_budget_suppressed_count=0` `opportunity_cooldown_suppressed_count=0`
- `why_no_opportunities=["top_blockers={'history_completion_too_low': 39, 'late_or_chase': 32, 'no_trade_lock': 12}", 'candidates_exist_but_verified_gate_not_open']`
- `top_blockers={'history_completion_too_low': 39, 'late_or_chase': 32, 'no_trade_lock': 12, 'strong_opposite_signal': 1}`
- `next_threshold_suggestions=['继续收集 candidate 60s 后验样本，优先把样本量补到 verified 门槛。']`

## 10. outcome price source 与 30s/60s/300s 分析

- `window_status_distribution={'30s': {'completed': 5}, '60s': {'completed': 5}, '300s': {'completed': 5}}`
- `outcome_price_source_distribution={'okx_mark': 15}`
- `outcome_failure_reason_distribution={}`
- `scheduler_health_summary={'pending_count': 0, 'completed_count': 15, 'unavailable_count': 0, 'expired_count': 0, 'catchup_completed_count': 0, 'catchup_expired_count': 0, 'settled_by_distribution': {'outcome_scheduler': 15}}`
- `expired_rate_by_window={'30s': 0.0, '60s': 0.0, '300s': 0.0}`
- previous baseline `source_distribution={'okx_mark': 612}`
- `pool_quote_proxy_share_delta_vs_previous=0.0`

## 11. OKX/Kraken live market context 分析

- window `live_public_count=5` `unavailable_count=0`
- window `okx_attempts=30` `okx_success=30` `okx_failure=0`
- window `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`
- window `requested_to_resolved_distribution={'ETHUSDC->ETH-USDT-SWAP': 2, 'ETHUSDT->ETH-USDT-SWAP': 3}`
- CLI full `live_public_hit_rate=0.262` `unavailable_rate=0.738`

## 12. trade_action 与交易辅助价值分析

- `trade_action_distribution={'LONG_BIAS_OBSERVE': 3, 'LONG_CHASE_ALLOWED': 2}`
- `delivered_distribution={'LONG_BIAS_OBSERVE': 1, 'LONG_CHASE_ALLOWED': 1}`
- `do_not_chase_would_have_avoided_bad_chase_rate=None`
- `no_trade_high_noise_rate_300s=None`
- `generic_confirm_success_rate_300s=0.2` `generic_confirm_adverse_rate_300s=0.8`

## 13. 卖压后涨 / 买压后跌 反例专项

- `sell_pressure_adverse_30s_rate=None`
- `sell_pressure_adverse_60s_rate=None`
- `sell_pressure_adverse_300s_rate=None`
- `buy_pressure_adverse_30s_rate=0.0`
- `buy_pressure_adverse_60s_rate=0.6`
- `buy_pressure_adverse_300s_rate=0.8`
- `counterexample_reason_distribution={'direction_aware_adverse_outcome': 4}`
- `top_counterexamples=[{'signal_id': 'sig_78cefec64e5499dd', 'time_beijing': '2026-04-23 13:02:10 UTC+08:00', 'pair': 'ETH/USDT', 'stage': 'confirm', 'trade_action': 'LONG_BIAS_OBSERVE', 'asset_market_state': 'OBSERVE_LONG', 'move_after_30s': 0.000255, 'move_after_60s': -0.000213, 'move_after_300s': -0.000374, 'reasons': ['direction_aware_adverse_outcome']}, {'signal_id': 'sig_284d255470ed60d8', 'time_beijing': '2026-04-23 13:02:18 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'confirm', 'trade_action': 'LONG_CHASE_ALLOWED', 'asset_market_state': 'OBSERVE_LONG', 'move_after_30s': 6.8e-05, 'move_after_60s': -0.000111, 'move_after_300s': -0.000234, 'reasons': ['direction_aware_adverse_outcome']}, {'signal_id': 'sig_a099ac77d2614ecf', 'time_beijing': '2026-04-23 13:02:22 UTC+08:00', 'pair': 'ETH/USDT', 'stage': 'climax', 'trade_action': 'LONG_BIAS_OBSERVE', 'asset_market_state': 'OBSERVE_LONG', 'move_after_30s': 5.1e-05, 'move_after_60s': -9.4e-05, 'move_after_300s': -0.000217, 'reasons': ['direction_aware_adverse_outcome']}, {'signal_id': 'sig_74031760efad2088', 'time_beijing': '2026-04-23 13:02:06 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'confirm', 'trade_action': 'LONG_CHASE_ALLOWED', 'asset_market_state': 'OBSERVE_LONG', 'move_after_30s': 0.000472, 'move_after_60s': 0.0, 'move_after_300s': -0.000179, 'reasons': ['direction_aware_adverse_outcome']}]`

## 14. raw/parsed/signals archive 完整性

- `raw_archive_exists=True`
- `parsed_archive_exists=True`
- `signals_archive_exists=True`
- `signal_delivery_audit_match_rate=1.0`
- `signal_case_followup_match_rate=1.0`
- `signal_case_attached_match_rate=1.0`
- `delivered_signal_notifier_sent_at_rate=1.0`
- `signal_event_in_parsed_rate=1.0`
- `suppressed_signal_delivery_match_rate=1.0`

## 15. majors 覆盖与样本代表性

- `covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `missing_major_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`
- `asset_distribution={'ETH': 5}`
- `pair_distribution={'ETH/USDT': 3, 'ETH/USDC': 2}`
- CLI `recommended_next_round_pairs=['SOL/USDT', 'SOL/USDC', 'BTC/USDC', 'BTC/USDT']`

## 16. 噪音与误判风险评估

- `delivered_lp_ratio=0.4`
- `suppressed_ratio=0.6`
- `high_value_suppressed_count=0`
- `direction_conflict_pairs_within_5m=0`
- `state_change_message_count=1`
- `risk_blocker_message_count=0`

## 17. 最终评分

- `research_sampling_readiness=5.0/10`
- `telegram_denoise_effect=7.6/10`
- `asset_market_state_usability=6.5/10`
- `no_trade_lock_protection_value=4.5/10`
- `prealert_lifecycle_effectiveness=6.0/10`
- `candidate_tradeable_credibility=3.0/10`
- `outcome_completeness=10.0/10`
- `live_market_context_readiness=9.5/10`
- `trade_action_assistive_value=4.2/10`
- `noise_control=6.8/10`
- `overall_self_use_score=6.3/10`

## 18. 下一轮建议

- fully eliminate legacy LONG_CHASE_ALLOWED / SHORT_CHASE_ALLOWED wording from user-facing and archived action summaries when the state machine only wants candidate status.
- collect more Beijing-day prealert samples and verify first_seen_stage/prealert_to_confirm_sec survive all later upgrades.
- prioritize enabling and validating BTC/SOL major pools before expanding any long-tail coverage.

## 19. 限制与不确定性

- selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.

## 附录: quality_reports CLI 参考

- `market-context-health.live_public_hit_rate=0.262`
- `major-pool-coverage.covered_major_pairs=['BTC/USDC', 'BTC/USDT', 'ETH/USDC', 'ETH/USDT']`
- `summary.overall={'actionable': True, 'climax_count': 210, 'climax_reversal_count': 38, 'climax_reversal_score': 0.2794, 'confirm_conversion_score': 0.2908, 'confirm_count': 493, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 0, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776920536, 'market_context_alignment_positive_count': 796, 'market_context_alignment_score': 0.9603, 'market_context_available_count': 800, 'prealert_confirmed_count': 1, 'prealert_count': 2, 'prealert_false_count': 0, 'prealert_precision_score': 0.685, 'quality_hint': '历史一般', 'quality_score': 0.6287, 'resolved_climaxes': 145, 'resolved_confirms': 357, 'resolved_fastlanes': 0, 'resolved_prealerts': 1, 'sample_size': 800, 'tuning_hint': '保持当前阈值'}`
