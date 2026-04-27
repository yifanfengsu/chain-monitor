# Afternoon / Evening State Analysis

## 1. 执行摘要

- selected a fully covered Beijing afternoon-evening window from 2026-04-24 13:00:03 UTC+08:00 to 2026-04-24 23:52:46 UTC+08:00 (10.88h); raw/parsed/signals/delivery/cases all overlap in this interval.
- LP sample size in the selected window is 64 rows with 26 delivered LP Telegram messages.
- Telegram denoise is materially active: delivered/raw ratio is 0.4062, suppression ratio is 0.5938, and suppression reasons are dominated by {'no_trade_opportunity': 29}.
- asset-level market state is live in real data: 27 state changes, distribution {'DO_NOT_CHASE_LONG': 15, 'DO_NOT_CHASE_SHORT': 21, 'NO_TRADE_LOCK': 1, 'OBSERVE_LONG': 13, 'OBSERVE_SHORT': 13, 'WAIT_CONFIRMATION': 1}, final states {'ETH': {'state_key': 'DO_NOT_CHASE_LONG', 'state_label': '不追多', 'updated_at': 1777045499, 'updated_at_utc': '2026-04-24 15:44:59 UTC', 'updated_at_beijing': '2026-04-24 23:44:59 UTC+08:00', 'telegram_update_kind': 'suppressed'}}.
- NO_TRADE_LOCK is real rather than theoretical: entered=1 suppressed=0 released=2.
- prealert lifecycle evidence is limited in this window: prealert stage rows=0, first_seen_stage=prealert count=25, median_prealert_to_confirm_sec=0.0.
- candidate/tradeable ladder is only partially validated: candidate rows=0, tradeable rows=0, blockers=['candidate_sample_count_below_min_samples(0<20)'].
- outcome price sourcing shifted away from prior pool_quote_proxy-heavy behavior: current sources={'okx_mark': 192}, previous pool_quote_proxy share=0.0, current share=0.0.

## 2. 数据源与完整性说明

- `data/chain_monitor.sqlite` | records=`47932` | range_utc=`2026-04-24 05:00:03 UTC -> 2026-04-24 15:52:46 UTC` | range_bj=`2026-04-24 13:00:03 UTC+08:00 -> 2026-04-24 23:52:46 UTC+08:00` | window_records=`47932` | note=`raw events archive; sqlite window-scoped inventory for raw_events`
- `data/chain_monitor.sqlite` | records=`27185` | range_utc=`2026-04-24 05:00:03 UTC -> 2026-04-24 15:52:46 UTC` | range_bj=`2026-04-24 13:00:03 UTC+08:00 -> 2026-04-24 23:52:46 UTC+08:00` | window_records=`27185` | note=`parsed events archive; sqlite window-scoped inventory for parsed_events`
- `data/chain_monitor.sqlite` | records=`517` | range_utc=`2026-04-24 05:00:40 UTC -> 2026-04-24 15:51:50 UTC` | range_bj=`2026-04-24 13:00:40 UTC+08:00 -> 2026-04-24 23:51:50 UTC+08:00` | window_records=`517` | note=`sqlite signals mirror via report_data_loader`
- `data/chain_monitor.sqlite` | records=`49038` | range_utc=`2026-04-24 05:00:03 UTC -> 2026-04-24 15:52:46 UTC` | range_bj=`2026-04-24 13:00:03 UTC+08:00 -> 2026-04-24 23:52:46 UTC+08:00` | window_records=`49038` | note=`sqlite delivery_audit window`
- `data/chain_monitor.sqlite` | records=`517` | range_utc=`2026-04-24 05:00:40 UTC -> 2026-04-24 15:51:50 UTC` | range_bj=`2026-04-24 13:00:40 UTC+08:00 -> 2026-04-24 23:51:50 UTC+08:00` | window_records=`517` | note=`sqlite signals-derived case linkage window`
- `data/chain_monitor.sqlite` | records=`43` | range_utc=`2026-04-24 05:18:07 UTC -> 2026-04-24 15:42:40 UTC` | range_bj=`2026-04-24 13:18:07 UTC+08:00 -> 2026-04-24 23:42:40 UTC+08:00` | window_records=`43` | note=`sqlite case_followups window`
- `data/lp_quality_stats.cache.json` | records=`800` | range_utc=`2026-04-20 06:33:25 UTC -> 2026-04-24 15:49:57 UTC` | range_bj=`2026-04-20 14:33:25 UTC+08:00 -> 2026-04-24 23:49:57 UTC+08:00` | window_records=`` | note=`quality stats cache`
- `data/asset_cases.cache.json` | records=`1` | range_utc=`2026-04-24 15:42:38 UTC -> 2026-04-24 15:45:01 UTC` | range_bj=`2026-04-24 23:42:38 UTC+08:00 -> 2026-04-24 23:45:01 UTC+08:00` | window_records=`` | note=`asset case snapshot cache`
- `data/asset_market_states.cache.json` | records=`1` | range_utc=`2026-04-20 05:10:15 UTC -> 2026-04-24 15:45:00 UTC` | range_bj=`2026-04-20 13:10:15 UTC+08:00 -> 2026-04-24 23:45:00 UTC+08:00` | window_records=`` | note=`asset market state cache`
- `data/trade_opportunities.cache.json` | records=`682` | range_utc=`2026-04-20 15:03:51 UTC -> 2026-04-24 15:45:51 UTC` | range_bj=`2026-04-20 23:03:51 UTC+08:00 -> 2026-04-24 23:45:51 UTC+08:00` | window_records=`` | note=`trade opportunity cache`
- `data/chain_monitor.sqlite` | records=`1002626` | range_utc=` -> ` | range_bj=` -> ` | window_records=`` | note=`sqlite mirror/query layer report_data_source=sqlite sqlite_rows_by_table={'asset_cases': 19549, 'asset_market_states': 780, 'case_followups': 297, 'delivery_audit': 285345, 'market_context_attempts': 4385, 'outcomes': 4059, 'parsed_events': 113046, 'quality_stats': 1511, 'raw_events': 222724, 'signals': 3056, 'telegram_deliveries': 347002, 'trade_opportunities': 872} archive_rows_by_category={} db_archive_mirror_match_rate=None archive_fallback_used=False mismatch_warnings=[]`

## 3. 北京时间下午到晚上分析窗口

- `cutoff_beijing=2026-04-24 13:00:00 UTC+08:00`
- `analysis_window_utc=2026-04-24 05:00:03 UTC -> 2026-04-24 15:52:46 UTC`
- `analysis_window_server=2026-04-24 05:00:03 UTC -> 2026-04-24 15:52:46 UTC`
- `analysis_window_beijing=2026-04-24 13:00:03 UTC+08:00 -> 2026-04-24 23:52:46 UTC+08:00`
- `duration_hours=10.88`
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

- `state_distribution={'DO_NOT_CHASE_LONG': 15, 'DO_NOT_CHASE_SHORT': 21, 'NO_TRADE_LOCK': 1, 'OBSERVE_LONG': 13, 'OBSERVE_SHORT': 13, 'WAIT_CONFIRMATION': 1}`
- `state_transition_count=27` `state_changed_count=27`
- `repeated_state_suppressed_count=0`
- `final_state_by_asset={'ETH': {'state_key': 'DO_NOT_CHASE_LONG', 'state_label': '不追多', 'updated_at': 1777045499, 'updated_at_utc': '2026-04-24 15:44:59 UTC', 'updated_at_beijing': '2026-04-24 23:44:59 UTC+08:00', 'telegram_update_kind': 'suppressed'}}`
- `eth_state_path=[{'ts': 1777007887, 'utc': '2026-04-24 05:18:07 UTC', 'beijing': '2026-04-24 13:18:07 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777009563, 'utc': '2026-04-24 05:46:03 UTC', 'beijing': '2026-04-24 13:46:03 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777017927, 'utc': '2026-04-24 08:05:27 UTC', 'beijing': '2026-04-24 16:05:27 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777018853, 'utc': '2026-04-24 08:20:53 UTC', 'beijing': '2026-04-24 16:20:53 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777018879, 'utc': '2026-04-24 08:21:19 UTC', 'beijing': '2026-04-24 16:21:19 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777021701, 'utc': '2026-04-24 09:08:21 UTC', 'beijing': '2026-04-24 17:08:21 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_LONG', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777021733, 'utc': '2026-04-24 09:08:53 UTC', 'beijing': '2026-04-24 17:08:53 UTC+08:00', 'state_key': 'WAIT_CONFIRMATION', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部结构已经出现，但仍缺足够 clean/broader 证据，先等确认而不是直接追空。', 'trade_action_key': 'WAIT_CONFIRMATION', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777021737, 'utc': '2026-04-24 09:08:57 UTC', 'beijing': '2026-04-24 17:08:57 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'WAIT_CONFIRMATION', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777021788, 'utc': '2026-04-24 09:09:48 UTC', 'beijing': '2026-04-24 17:09:48 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777026942, 'utc': '2026-04-24 10:35:42 UTC', 'beijing': '2026-04-24 18:35:42 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777027099, 'utc': '2026-04-24 10:38:19 UTC', 'beijing': '2026-04-24 18:38:19 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '卖方清扫更像局部高潮，反抽风险抬升，不宜继续追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777027541, 'utc': '2026-04-24 10:45:41 UTC', 'beijing': '2026-04-24 18:45:41 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777028471, 'utc': '2026-04-24 11:01:11 UTC', 'beijing': '2026-04-24 19:01:11 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1777032164, 'utc': '2026-04-24 12:02:44 UTC', 'beijing': '2026-04-24 20:02:44 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777032169, 'utc': '2026-04-24 12:02:49 UTC', 'beijing': '2026-04-24 20:02:49 UTC+08:00', 'state_key': 'NO_TRADE_LOCK', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '近期冲突严重，进入冷却锁，暂停追单。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777032969, 'utc': '2026-04-24 12:16:09 UTC', 'beijing': '2026-04-24 20:16:09 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777035028, 'utc': '2026-04-24 12:50:28 UTC', 'beijing': '2026-04-24 20:50:28 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777038484, 'utc': '2026-04-24 13:48:04 UTC', 'beijing': '2026-04-24 21:48:04 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777038871, 'utc': '2026-04-24 13:54:31 UTC', 'beijing': '2026-04-24 21:54:31 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '卖方清扫更像局部高潮，反抽风险抬升，不宜继续追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777038876, 'utc': '2026-04-24 13:54:36 UTC', 'beijing': '2026-04-24 21:54:36 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1777039390, 'utc': '2026-04-24 14:03:10 UTC', 'beijing': '2026-04-24 22:03:10 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '卖方清扫更像局部高潮，反抽风险抬升，不宜继续追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777039421, 'utc': '2026-04-24 14:03:41 UTC', 'beijing': '2026-04-24 22:03:41 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1777040730, 'utc': '2026-04-24 14:25:30 UTC', 'beijing': '2026-04-24 22:25:30 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777040994, 'utc': '2026-04-24 14:29:54 UTC', 'beijing': '2026-04-24 22:29:54 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777043204, 'utc': '2026-04-24 15:06:44 UTC', 'beijing': '2026-04-24 23:06:44 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1777043228, 'utc': '2026-04-24 15:07:08 UTC', 'beijing': '2026-04-24 23:07:08 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1777045360, 'utc': '2026-04-24 15:42:40 UTC', 'beijing': '2026-04-24 23:42:40 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}]`

## 6. Telegram 降噪与 suppression 分析

- `messages_before_suppression_estimate=64` `messages_after_suppression_actual=26`
- `telegram_should_send_count=35` `telegram_suppressed_count=29`
- `telegram_suppression_ratio=0.5938`
- `telegram_suppression_reasons={'no_trade_opportunity': 29}`
- `telegram_update_kind_distribution={'risk_blocker': 25, 'state_change': 10, 'suppressed': 29}`
- `high_value_suppressed_count=0` `should_send_but_not_sent_count=9`

## 7. NO_TRADE_LOCK 分析

- `lock_entered_count=1` `lock_suppressed_count=0` `lock_released_count=2`
- `avg_lock_duration_sec=300.0` `median_lock_duration_sec=300.0`
- `lock_release_reasons={'ttl_expired': 2}`
- `lock_suppressed_stage_distribution={}`
- `missed_lock_examples=[]`

## 8. prealert 生命周期分析

- `prealert_count=0`
- `prealert_candidate_count=25`
- `prealert_gate_passed_count=25`
- `prealert_active_count=0`
- `prealert_delivered_count=0`
- `prealert_merged_count=15`
- `prealert_upgraded_to_confirm_count=25`
- `prealert_expired_count=0`
- `prealert_suppressed_by_lock_count=0`
- `median_prealert_to_confirm_sec=0.0`
- `first_seen_stage_prealert_count=25`
- `prealert_stage_overwritten_count=25`
- `first_seen_stage_distribution={'prealert': 64}`

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

- `opportunity_summary={'candidates': 3, 'verified': 0, 'blocked': 98, 'none': 581, 'verified_maturity': 'immature', 'verified_should_not_be_traded_reason': 'outcome_completion_rate_below_0.70', 'maturity_reasons': ['outcome_completion_rate_below_0.70']}`
- `verified_maturity=immature` `verified_should_not_be_traded_reason=outcome_completion_rate_below_0.70` `maturity_reasons=['outcome_completion_rate_below_0.70']`
- `opportunity_score_median=0.5069` `opportunity_score_p90=0.6672`
- `raw_score_vs_calibrated_score={'raw_median': 0.5179, 'calibrated_median': 0.5069, 'raw_p90': 0.6673, 'calibrated_p90': 0.6672}`
- `calibration_adjustment_distribution={'positive_count': 0, 'negative_count': 348, 'neutral_count': 2, 'median': -0.03, 'p90': -0.03, 'min': -0.03, 'max': 0.0}`
- `candidate_outcome_60s={'count': 3, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {'neutral': 3}}`
- `verified_outcome_60s={'count': 0, 'resolved_count': 0, 'followthrough_count': 0, 'followthrough_rate': None, 'adverse_count': 0, 'adverse_rate': None, 'expired_count': 0, 'unavailable_count': 0, 'result_distribution': {}}`
- `opportunity_profile_count=53`
- `top_profiles_by_sample=[{'profile_key': 'ETH|LONG|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 2, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|SHORT|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 1, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_low', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_low', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_medium', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}, {'profile_key': 'ETH|LONG|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_low', 'sample_count': 0, 'followthrough_60s_rate': 0.0, 'adverse_60s_rate': 1.0}]`
- `top_profiles_by_followthrough=[]`
- `top_profiles_by_adverse=[]`
- `profiles_ready_for_verified=[]`
- `estimated_samples_needed_for_verified_by_profile={'ETH|LONG|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 18, 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|LONG|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|NONE|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|NONE|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|NONE|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|NONE|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|NONE|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|broader_confirm|broader_confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|broader_confirm|broader_confirm|leading|broader_absorption|major|basis_normal|quality_medium': 19, 'ETH|SHORT|local_confirm|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|unknown|confirm|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|unknown|confirm|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|confirming|broader_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|unknown|exhaustion_risk|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|unknown|exhaustion_risk|leading|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|confirming|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|confirming|local_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|leading|broader_absorption|major|basis_normal|quality_medium': 20, 'ETH|SHORT|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_low': 20, 'ETH|SHORT|unknown|sweep_confirmed|leading|local_absorption|major|basis_normal|quality_medium': 20}`
- `final_trading_output_distribution={'asset_market_state': 27, 'trade_action_legacy': 29, 'trade_opportunity': 8}`
- `legacy_chase_downgraded_count=0` `legacy_chase_leaked_count=0` `messages_blocked_by_opportunity_gate=20`
- `all_opportunity_labels_verified=True` `all_candidate_labels_are_candidate=True` `blocked_covers_legacy_chase_risk=True`
- `blocker_effectiveness={'count': 98, 'avoided_adverse_count': 0, 'avoided_adverse_rate': 0.0, 'false_block_count': 0, 'false_block_rate': 0.0, 'top_effective_blockers': [{'blocker': 'history_completion_too_low', 'count': 39, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'late_or_chase', 'count': 38, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'no_trade_lock', 'count': 12, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'low_quality', 'count': 8, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'strong_opposite_signal', 'count': 1, 'saved_rate': 0.0, 'false_block_rate': 0.0}], 'top_overblocking_blockers': [{'blocker': 'history_completion_too_low', 'count': 39, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'late_or_chase', 'count': 38, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'no_trade_lock', 'count': 12, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'low_quality', 'count': 8, 'saved_rate': 0.0, 'false_block_rate': 0.0}, {'blocker': 'strong_opposite_signal', 'count': 1, 'saved_rate': 0.0, 'false_block_rate': 0.0}]}`
- `hard_blocker_distribution={'history_completion_too_low': 39, 'late_or_chase': 38, 'low_quality': 8, 'no_trade_lock': 12, 'strong_opposite_signal': 1}`
- `verification_blocker_distribution={'profile_completion_too_low': 3}`
- `non_lp_evidence_summary={'available_count': 0, 'support_count': 0, 'risk_count': 0, 'blocked_count': 0, 'conflict_count': 0, 'upgraded_count': 0}`
- `opportunity_non_lp_support_count=0` `opportunity_non_lp_risk_count=0`
- `top_non_lp_supporting_evidence=[]`
- `top_non_lp_blockers=[]`
- `opportunities_upgraded_by_non_lp=0` `opportunities_blocked_by_non_lp=0`
- `opportunities_upgraded_by_calibration=0` `opportunities_downgraded_by_calibration=3`
- `candidates_blocked_by_calibration=0` `verified_allowed_by_calibration=0`
- `top_positive_adjustments=[]`
- `top_negative_adjustments=[{'trade_opportunity_id': 'opp_001f1f41d9977137', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_00bf79dbcfedcc47', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_01066f88d874a4bb', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_01203b257c575f8a', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_017d9150c5b63cc5', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_01cb7e05ce831c61', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_0213df5b0f4e1882', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45; followthrough<0.50'}, {'trade_opportunity_id': 'opp_028de77db6e7469a', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_02c83f357df0b6f2', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}, {'trade_opportunity_id': 'opp_03df1f66aa43424a', 'adjustment': -0.03, 'source': 'none', 'reason': 'completion<0.50; adverse>0.45'}]`
- `calibration_reason_distribution={'completion<0.50; adverse>0.45': 264, 'completion<0.50; adverse>0.45; followthrough<0.50': 84, 'neutral_calibration': 2}`
- `calibration_source_distribution={'none': 682}`
- `calibration_confidence_distribution={'low': 682}`
- `non_lp_conflict_cases=[]`
- `opportunity_budget_suppressed_count=0` `opportunity_cooldown_suppressed_count=0`
- `why_no_opportunities=["top_blockers={'history_completion_too_low': 39, 'late_or_chase': 38, 'no_trade_lock': 12}", 'candidates_exist_but_verified_gate_not_open']`
- `top_blockers={'history_completion_too_low': 39, 'late_or_chase': 38, 'no_trade_lock': 12, 'low_quality': 8, 'strong_opposite_signal': 1}`
- `next_threshold_suggestions=['继续收集 candidate 60s 后验样本，优先把样本量补到 verified 门槛。']`

## 10. outcome price source 与 30s/60s/300s 分析

- `window_status_distribution={'30s': {'completed': 64}, '60s': {'completed': 64}, '300s': {'completed': 64}}`
- `outcome_price_source_distribution={'okx_mark': 192}`
- `outcome_failure_reason_distribution={}`
- `scheduler_health_summary={'pending_count': 0, 'completed_count': 192, 'unavailable_count': 0, 'expired_count': 0, 'catchup_completed_count': 0, 'catchup_expired_count': 0, 'settled_by_distribution': {'outcome_scheduler': 192}}`
- `expired_rate_by_window={'30s': 0.0, '60s': 0.0, '300s': 0.0}`
- previous baseline `source_distribution={'okx_mark': 1212}`
- `pool_quote_proxy_share_delta_vs_previous=0.0`

## 11. OKX/Kraken live market context 分析

- window `live_public_count=64` `unavailable_count=0`
- window `okx_attempts=384` `okx_success=384` `okx_failure=0`
- window `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`
- window `requested_to_resolved_distribution={'ETHUSDC->ETH-USDT-SWAP': 34, 'ETHUSDT->ETH-USDT-SWAP': 30}`
- CLI full `live_public_hit_rate=None` `unavailable_rate=None`

## 12. trade_action 与交易辅助价值分析

- `trade_action_distribution={'CONFLICT_NO_TRADE': 1, 'DO_NOT_CHASE_LONG': 15, 'DO_NOT_CHASE_SHORT': 21, 'LONG_BIAS_OBSERVE': 13, 'SHORT_BIAS_OBSERVE': 13, 'WAIT_CONFIRMATION': 1}`
- `delivered_distribution={'CONFLICT_NO_TRADE': 1, 'DO_NOT_CHASE_LONG': 6, 'DO_NOT_CHASE_SHORT': 7, 'LONG_BIAS_OBSERVE': 5, 'SHORT_BIAS_OBSERVE': 6, 'WAIT_CONFIRMATION': 1}`
- `do_not_chase_would_have_avoided_bad_chase_rate=0.6111`
- `no_trade_high_noise_rate_300s=1.0`
- `generic_confirm_success_rate_300s=0.4783` `generic_confirm_adverse_rate_300s=0.5217`

## 13. 卖压后涨 / 买压后跌 反例专项

- `sell_pressure_adverse_30s_rate=0.4444`
- `sell_pressure_adverse_60s_rate=0.3889`
- `sell_pressure_adverse_300s_rate=0.6111`
- `buy_pressure_adverse_30s_rate=0.2857`
- `buy_pressure_adverse_60s_rate=0.3929`
- `buy_pressure_adverse_300s_rate=0.5357`
- `counterexample_reason_distribution={'direction_aware_adverse_outcome': 5, 'local_confirm_not_broader_confirm': 3, 'local_sell_pressure_absorption': 1, 'single_pool_or_low_multi_pool_resonance': 2}`
- `top_counterexamples=[{'signal_id': 'sig_52c48730cf55741c', 'time_beijing': '2026-04-24 23:06:44 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'confirm', 'trade_action': 'DO_NOT_CHASE_SHORT', 'asset_market_state': 'DO_NOT_CHASE_SHORT', 'move_after_30s': 0.001048, 'move_after_60s': 0.001113, 'move_after_300s': 0.003651, 'reasons': ['local_confirm_not_broader_confirm', 'single_pool_or_low_multi_pool_resonance', 'local_sell_pressure_absorption', 'direction_aware_adverse_outcome']}, {'signal_id': 'sig_75bf4b1541197f5f', 'time_beijing': '2026-04-24 21:54:36 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'climax', 'trade_action': 'SHORT_BIAS_OBSERVE', 'asset_market_state': 'OBSERVE_SHORT', 'move_after_30s': -9e-06, 'move_after_60s': -9e-06, 'move_after_300s': 0.002523, 'reasons': ['local_confirm_not_broader_confirm', 'direction_aware_adverse_outcome']}, {'signal_id': 'sig_6de5be2d9e47413b', 'time_beijing': '2026-04-24 16:21:36 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'confirm', 'trade_action': 'LONG_BIAS_OBSERVE', 'asset_market_state': 'OBSERVE_LONG', 'move_after_30s': -0.001163, 'move_after_60s': -0.001766, 'move_after_300s': -0.002425, 'reasons': ['direction_aware_adverse_outcome']}, {'signal_id': 'sig_d981938df56a0333', 'time_beijing': '2026-04-24 21:54:31 UTC+08:00', 'pair': 'ETH/USDT', 'stage': 'exhaustion_risk', 'trade_action': 'DO_NOT_CHASE_SHORT', 'asset_market_state': 'DO_NOT_CHASE_SHORT', 'move_after_30s': -0.000485, 'move_after_60s': -0.000485, 'move_after_300s': 0.002245, 'reasons': ['local_confirm_not_broader_confirm', 'direction_aware_adverse_outcome']}, {'signal_id': 'sig_c07f7b00bffdfc6c', 'time_beijing': '2026-04-24 23:07:02 UTC+08:00', 'pair': 'ETH/USDC', 'stage': 'exhaustion_risk', 'trade_action': 'DO_NOT_CHASE_SHORT', 'asset_market_state': 'DO_NOT_CHASE_SHORT', 'move_after_30s': 0.000626, 'move_after_60s': 0.000639, 'move_after_300s': 0.002211, 'reasons': ['single_pool_or_low_multi_pool_resonance', 'direction_aware_adverse_outcome']}]`

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
- `asset_distribution={'ETH': 64}`
- `pair_distribution={'ETH/USDC': 34, 'ETH/USDT': 30}`
- CLI `recommended_next_round_pairs=None`

## 16. 噪音与误判风险评估

- `delivered_lp_ratio=0.4062`
- `suppressed_ratio=0.5938`
- `high_value_suppressed_count=0`
- `direction_conflict_pairs_within_5m=2`
- `state_change_message_count=10`
- `risk_blocker_message_count=1`

## 17. 最终评分

- `research_sampling_readiness=5.0/10`
- `telegram_denoise_effect=7.6/10`
- `asset_market_state_usability=8.0/10`
- `no_trade_lock_protection_value=6.5/10`
- `prealert_lifecycle_effectiveness=6.0/10`
- `candidate_tradeable_credibility=3.8/10`
- `outcome_completeness=10.0/10`
- `live_market_context_readiness=9.5/10`
- `trade_action_assistive_value=6.8/10`
- `noise_control=6.4/10`
- `overall_self_use_score=7.0/10`

## 18. 下一轮建议

- collect more Beijing-day prealert samples and verify first_seen_stage/prealert_to_confirm_sec survive all later upgrades.
- prioritize enabling and validating BTC/SOL major pools before expanding any long-tail coverage.

## 19. 限制与不确定性

- selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.

## 附录: quality_reports CLI 参考

- `market-context-health.live_public_hit_rate=None`
- `major-pool-coverage.covered_major_pairs=[]`
- `summary.overall={}`
