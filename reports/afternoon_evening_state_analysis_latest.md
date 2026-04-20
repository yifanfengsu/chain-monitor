# Afternoon / Evening State Analysis

## 1. 执行摘要

- selected a fully covered Beijing afternoon-evening window from 2026-04-20 13:10:04 UTC+08:00 to 2026-04-20 20:32:31 UTC+08:00 (7.37h); raw/parsed/signals/delivery/cases all overlap in this interval.
- LP sample size in the selected window is 137 rows with 31 delivered LP Telegram messages.
- Telegram denoise is materially active: delivered/raw ratio is 0.2263, suppression ratio is 0.7737, and suppression reasons are dominated by {'no_trade_lock_local_signal_suppressed': 18, 'same_asset_state_repeat': 63}.
- asset-level market state is live in real data: 56 state changes, distribution {'DO_NOT_CHASE_LONG': 31, 'DO_NOT_CHASE_SHORT': 18, 'LONG_CANDIDATE': 4, 'NO_TRADE_CHOP': 3, 'NO_TRADE_LOCK': 22, 'OBSERVE_LONG': 36, 'OBSERVE_SHORT': 21, 'WAIT_CONFIRMATION': 2}, final states {'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776687670, 'updated_at_utc': '2026-04-20 12:21:10 UTC', 'updated_at_beijing': '2026-04-20 20:21:10 UTC+08:00', 'telegram_update_kind': 'suppressed'}}.
- NO_TRADE_LOCK is real rather than theoretical: entered=4 suppressed=18 released=4.
- prealert lifecycle evidence is limited in this window: prealert stage rows=1, first_seen_stage=prealert count=29, median_prealert_to_confirm_sec=0.0.
- candidate/tradeable ladder is only partially validated: candidate rows=4, tradeable rows=0, blockers=['candidate_sample_count_below_min_samples(4<20)'].
- outcome price sourcing shifted away from prior pool_quote_proxy-heavy behavior: current sources={'okx_mark': 411}, previous pool_quote_proxy share=1.0, current share=0.0.

## 2. 数据源与完整性说明

- `app/data/archive/raw_events/2026-04-20.ndjson` | records=`41609` | range_utc=`2026-04-20 05:10:04 UTC -> 2026-04-20 12:32:31 UTC` | range_bj=`2026-04-20 13:10:04 UTC+08:00 -> 2026-04-20 20:32:31 UTC+08:00` | window_records=`41609` | note=`raw_events archive`
- `app/data/archive/parsed_events/2026-04-20.ndjson` | records=`25539` | range_utc=`2026-04-20 05:10:04 UTC -> 2026-04-20 12:32:31 UTC` | range_bj=`2026-04-20 13:10:04 UTC+08:00 -> 2026-04-20 20:32:31 UTC+08:00` | window_records=`25539` | note=`parsed_events archive`
- `app/data/archive/signals/2026-04-18.ndjson` | records=`424` | range_utc=`2026-04-18 05:26:52 UTC -> 2026-04-18 15:57:32 UTC` | range_bj=`2026-04-18 13:26:52 UTC+08:00 -> 2026-04-18 23:57:32 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-19.ndjson` | records=`22` | range_utc=`2026-04-19 15:39:05 UTC -> 2026-04-19 15:59:27 UTC` | range_bj=`2026-04-19 23:39:05 UTC+08:00 -> 2026-04-19 23:59:27 UTC+08:00` | window_records=`` | note=`signals archive`
- `app/data/archive/signals/2026-04-20.ndjson` | records=`1018` | range_utc=`2026-04-19 16:00:26 UTC -> 2026-04-20 12:32:16 UTC` | range_bj=`2026-04-20 00:00:26 UTC+08:00 -> 2026-04-20 20:32:16 UTC+08:00` | window_records=`528` | note=`signals archive`
- `app/data/archive/delivery_audit/2026-04-18.ndjson` | records=`54936` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:56 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:56 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-19.ndjson` | records=`2056` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:52 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:52 UTC+08:00` | window_records=`` | note=`delivery_audit archive`
- `app/data/archive/delivery_audit/2026-04-20.ndjson` | records=`85095` | range_utc=`2026-04-19 16:00:05 UTC -> 2026-04-20 12:32:31 UTC` | range_bj=`2026-04-20 00:00:05 UTC+08:00 -> 2026-04-20 20:32:31 UTC+08:00` | window_records=`42474` | note=`delivery_audit archive`
- `app/data/archive/cases/2026-04-18.ndjson` | records=`40383` | range_utc=`2026-04-18 05:26:16 UTC -> 2026-04-18 15:59:55 UTC` | range_bj=`2026-04-18 13:26:16 UTC+08:00 -> 2026-04-18 23:59:55 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-19.ndjson` | records=`1309` | range_utc=`2026-04-19 15:38:27 UTC -> 2026-04-19 15:59:51 UTC` | range_bj=`2026-04-19 23:38:27 UTC+08:00 -> 2026-04-19 23:59:51 UTC+08:00` | window_records=`` | note=`cases archive`
- `app/data/archive/cases/2026-04-20.ndjson` | records=`60822` | range_utc=`2026-04-19 16:00:15 UTC -> 2026-04-20 12:32:31 UTC` | range_bj=`2026-04-20 00:00:15 UTC+08:00 -> 2026-04-20 20:32:31 UTC+08:00` | window_records=`32293` | note=`cases archive`
- `app/data/archive/case_followups/2026-04-18.ndjson` | records=`33` | range_utc=`2026-04-18 05:32:17 UTC -> 2026-04-18 14:53:00 UTC` | range_bj=`2026-04-18 13:32:17 UTC+08:00 -> 2026-04-18 22:53:00 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-19.ndjson` | records=`2` | range_utc=`2026-04-19 15:41:15 UTC -> 2026-04-19 15:54:32 UTC` | range_bj=`2026-04-19 23:41:15 UTC+08:00 -> 2026-04-19 23:54:32 UTC+08:00` | window_records=`` | note=`case_followups archive`
- `app/data/archive/case_followups/2026-04-20.ndjson` | records=`119` | range_utc=`2026-04-19 16:37:21 UTC -> 2026-04-20 12:21:02 UTC` | range_bj=`2026-04-20 00:37:21 UTC+08:00 -> 2026-04-20 20:21:02 UTC+08:00` | window_records=`49` | note=`case_followups archive`
- `data/lp_quality_stats.cache.json` | records=`655` | range_utc=`2026-04-17 06:16:14 UTC -> 2026-04-20 12:21:12 UTC` | range_bj=`2026-04-17 14:16:14 UTC+08:00 -> 2026-04-20 20:21:12 UTC+08:00` | window_records=`` | note=`quality stats cache`
- `data/asset_cases.cache.json` | records=`1` | range_utc=`2026-04-20 12:17:51 UTC -> 2026-04-20 12:21:10 UTC` | range_bj=`2026-04-20 20:17:51 UTC+08:00 -> 2026-04-20 20:21:10 UTC+08:00` | window_records=`` | note=`asset case snapshot cache`
- `data/asset_market_states.cache.json` | records=`1` | range_utc=`2026-04-20 05:10:15 UTC -> 2026-04-20 12:21:12 UTC` | range_bj=`2026-04-20 13:10:15 UTC+08:00 -> 2026-04-20 20:21:12 UTC+08:00` | window_records=`` | note=`asset market state cache`

## 3. 北京时间下午到晚上分析窗口

- `cutoff_beijing=2026-04-20 13:00:00 UTC+08:00`
- `analysis_window_utc=2026-04-20 05:10:04 UTC -> 2026-04-20 12:32:31 UTC`
- `analysis_window_server=2026-04-20 05:10:04 UTC -> 2026-04-20 12:32:31 UTC`
- `analysis_window_beijing=2026-04-20 13:10:04 UTC+08:00 -> 2026-04-20 20:32:31 UTC+08:00`
- `duration_hours=7.37`
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

## 5. asset-level market state 总览

- `state_distribution={'DO_NOT_CHASE_LONG': 31, 'DO_NOT_CHASE_SHORT': 18, 'LONG_CANDIDATE': 4, 'NO_TRADE_CHOP': 3, 'NO_TRADE_LOCK': 22, 'OBSERVE_LONG': 36, 'OBSERVE_SHORT': 21, 'WAIT_CONFIRMATION': 2}`
- `state_transition_count=56` `state_changed_count=56`
- `repeated_state_suppressed_count=63`
- `final_state_by_asset={'ETH': {'state_key': 'OBSERVE_LONG', 'state_label': '偏多观察', 'updated_at': 1776687670, 'updated_at_utc': '2026-04-20 12:21:10 UTC', 'updated_at_beijing': '2026-04-20 20:21:10 UTC+08:00', 'telegram_update_kind': 'suppressed'}}`
- `eth_state_path=[{'ts': 1776661817, 'utc': '2026-04-20 05:10:17 UTC', 'beijing': '2026-04-20 13:10:17 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'prealert', 'telegram_update_kind': 'state_change'}, {'ts': 1776661818, 'utc': '2026-04-20 05:10:18 UTC', 'beijing': '2026-04-20 13:10:18 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776662383, 'utc': '2026-04-20 05:19:43 UTC', 'beijing': '2026-04-20 13:19:43 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776663421, 'utc': '2026-04-20 05:37:01 UTC', 'beijing': '2026-04-20 13:37:01 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '卖方清扫更像局部高潮，反抽风险抬升，不宜继续追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776664675, 'utc': '2026-04-20 05:57:55 UTC', 'beijing': '2026-04-20 13:57:55 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776664683, 'utc': '2026-04-20 05:58:03 UTC', 'beijing': '2026-04-20 13:58:03 UTC+08:00', 'state_key': 'NO_TRADE_CHOP', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '买卖清扫/买卖压同时出现，缺稳定 broader 确认，追多追空都容易被反扫。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776665039, 'utc': '2026-04-20 06:03:59 UTC', 'beijing': '2026-04-20 14:03:59 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'NO_TRADE_CHOP', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776665040, 'utc': '2026-04-20 06:04:00 UTC', 'beijing': '2026-04-20 14:04:00 UTC+08:00', 'state_key': 'NO_TRADE_CHOP', 'previous_state_key': 'OBSERVE_LONG', 'reason': '买卖清扫/买卖压同时出现，缺稳定 broader 确认，追多追空都容易被反扫。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776665041, 'utc': '2026-04-20 06:04:01 UTC', 'beijing': '2026-04-20 14:04:01 UTC+08:00', 'state_key': 'NO_TRADE_LOCK', 'previous_state_key': 'NO_TRADE_CHOP', 'reason': '近期冲突严重，进入冷却锁，暂停追单。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776665784, 'utc': '2026-04-20 06:16:24 UTC', 'beijing': '2026-04-20 14:16:24 UTC+08:00', 'state_key': 'NO_TRADE_CHOP', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '当前不是明确的多空方向性 LP 结构，不能直接转成交易动作。', 'trade_action_key': 'NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776666785, 'utc': '2026-04-20 06:33:05 UTC', 'beijing': '2026-04-20 14:33:05 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'NO_TRADE_CHOP', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776666787, 'utc': '2026-04-20 06:33:07 UTC', 'beijing': '2026-04-20 14:33:07 UTC+08:00', 'state_key': 'NO_TRADE_LOCK', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '近期冲突严重，进入冷却锁，暂停追单。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776667355, 'utc': '2026-04-20 06:42:35 UTC', 'beijing': '2026-04-20 14:42:35 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776667403, 'utc': '2026-04-20 06:43:23 UTC', 'beijing': '2026-04-20 14:43:23 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776667555, 'utc': '2026-04-20 06:45:55 UTC', 'beijing': '2026-04-20 14:45:55 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776667557, 'utc': '2026-04-20 06:45:57 UTC', 'beijing': '2026-04-20 14:45:57 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776669605, 'utc': '2026-04-20 07:20:05 UTC', 'beijing': '2026-04-20 15:20:05 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776669607, 'utc': '2026-04-20 07:20:07 UTC', 'beijing': '2026-04-20 15:20:07 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776669627, 'utc': '2026-04-20 07:20:27 UTC', 'beijing': '2026-04-20 15:20:27 UTC+08:00', 'state_key': 'LONG_CANDIDATE', 'previous_state_key': 'OBSERVE_LONG', 'reason': '更广方向确认已出现，但后验 completion/followthrough 仍未稳定达标，先列为候选。', 'trade_action_key': 'LONG_CHASE_ALLOWED', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'candidate'}, {'ts': 1776669642, 'utc': '2026-04-20 07:20:42 UTC', 'beijing': '2026-04-20 15:20:42 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'LONG_CANDIDATE', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776669654, 'utc': '2026-04-20 07:20:54 UTC', 'beijing': '2026-04-20 15:20:54 UTC+08:00', 'state_key': 'LONG_CANDIDATE', 'previous_state_key': 'OBSERVE_LONG', 'reason': '更广方向确认已出现，但后验 completion/followthrough 仍未稳定达标，先列为候选。', 'trade_action_key': 'LONG_CHASE_ALLOWED', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'candidate'}, {'ts': 1776669656, 'utc': '2026-04-20 07:20:56 UTC', 'beijing': '2026-04-20 15:20:56 UTC+08:00', 'state_key': 'WAIT_CONFIRMATION', 'previous_state_key': 'LONG_CANDIDATE', 'reason': '局部结构已经出现，但仍缺足够 clean/broader 证据，先等确认而不是直接追多。', 'trade_action_key': 'WAIT_CONFIRMATION', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776669665, 'utc': '2026-04-20 07:21:05 UTC', 'beijing': '2026-04-20 15:21:05 UTC+08:00', 'state_key': 'LONG_CANDIDATE', 'previous_state_key': 'WAIT_CONFIRMATION', 'reason': '更广方向确认已出现，但后验 completion/followthrough 仍未稳定达标，先列为候选。', 'trade_action_key': 'LONG_CHASE_ALLOWED', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'candidate'}, {'ts': 1776669667, 'utc': '2026-04-20 07:21:07 UTC', 'beijing': '2026-04-20 15:21:07 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'LONG_CANDIDATE', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776669722, 'utc': '2026-04-20 07:22:02 UTC', 'beijing': '2026-04-20 15:22:02 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776669737, 'utc': '2026-04-20 07:22:17 UTC', 'beijing': '2026-04-20 15:22:17 UTC+08:00', 'state_key': 'WAIT_CONFIRMATION', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部结构已经出现，但仍缺足够 clean/broader 证据，先等确认而不是直接追多。', 'trade_action_key': 'WAIT_CONFIRMATION', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776669776, 'utc': '2026-04-20 07:22:56 UTC', 'beijing': '2026-04-20 15:22:56 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'WAIT_CONFIRMATION', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776669802, 'utc': '2026-04-20 07:23:22 UTC', 'beijing': '2026-04-20 15:23:22 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776669831, 'utc': '2026-04-20 07:23:51 UTC', 'beijing': '2026-04-20 15:23:51 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776669993, 'utc': '2026-04-20 07:26:33 UTC', 'beijing': '2026-04-20 15:26:33 UTC+08:00', 'state_key': 'NO_TRADE_LOCK', 'previous_state_key': 'OBSERVE_LONG', 'reason': '近期冲突严重，进入冷却锁，暂停追单。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776670294, 'utc': '2026-04-20 07:31:34 UTC', 'beijing': '2026-04-20 15:31:34 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776670296, 'utc': '2026-04-20 07:31:36 UTC', 'beijing': '2026-04-20 15:31:36 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776670575, 'utc': '2026-04-20 07:36:15 UTC', 'beijing': '2026-04-20 15:36:15 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776670638, 'utc': '2026-04-20 07:37:18 UTC', 'beijing': '2026-04-20 15:37:18 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776670640, 'utc': '2026-04-20 07:37:20 UTC', 'beijing': '2026-04-20 15:37:20 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776671119, 'utc': '2026-04-20 07:45:19 UTC', 'beijing': '2026-04-20 15:45:19 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776671420, 'utc': '2026-04-20 07:50:20 UTC', 'beijing': '2026-04-20 15:50:20 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776671427, 'utc': '2026-04-20 07:50:27 UTC', 'beijing': '2026-04-20 15:50:27 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776671647, 'utc': '2026-04-20 07:54:07 UTC', 'beijing': '2026-04-20 15:54:07 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776672884, 'utc': '2026-04-20 08:14:44 UTC', 'beijing': '2026-04-20 16:14:44 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776673616, 'utc': '2026-04-20 08:26:56 UTC', 'beijing': '2026-04-20 16:26:56 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部卖压可能被承接，不宜把单池卖压误读成可以追空。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776674357, 'utc': '2026-04-20 08:39:17 UTC', 'beijing': '2026-04-20 16:39:17 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776675686, 'utc': '2026-04-20 09:01:26 UTC', 'beijing': '2026-04-20 17:01:26 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776676866, 'utc': '2026-04-20 09:21:06 UTC', 'beijing': '2026-04-20 17:21:06 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776677043, 'utc': '2026-04-20 09:24:03 UTC', 'beijing': '2026-04-20 17:24:03 UTC+08:00', 'state_key': 'DO_NOT_CHASE_SHORT', 'previous_state_key': 'OBSERVE_LONG', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。', 'trade_action_key': 'DO_NOT_CHASE_SHORT', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776677166, 'utc': '2026-04-20 09:26:06 UTC', 'beijing': '2026-04-20 17:26:06 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'DO_NOT_CHASE_SHORT', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776677936, 'utc': '2026-04-20 09:38:56 UTC', 'beijing': '2026-04-20 17:38:56 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776678234, 'utc': '2026-04-20 09:43:54 UTC', 'beijing': '2026-04-20 17:43:54 UTC+08:00', 'state_key': 'NO_TRADE_LOCK', 'previous_state_key': 'OBSERVE_LONG', 'reason': '近期冲突严重，进入冷却锁，暂停追单。', 'trade_action_key': 'CONFLICT_NO_TRADE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776679519, 'utc': '2026-04-20 10:05:19 UTC', 'beijing': '2026-04-20 18:05:19 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'NO_TRADE_LOCK', 'reason': '买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'exhaustion_risk', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776679889, 'utc': '2026-04-20 10:11:29 UTC', 'beijing': '2026-04-20 18:11:29 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '局部买压/买方清扫建立中，但还没到可以追多的严格条件。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776680237, 'utc': '2026-04-20 10:17:17 UTC', 'beijing': '2026-04-20 18:17:17 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776680299, 'utc': '2026-04-20 10:18:19 UTC', 'beijing': '2026-04-20 18:18:19 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}, {'ts': 1776682411, 'utc': '2026-04-20 10:53:31 UTC', 'beijing': '2026-04-20 18:53:31 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_LONG', 'reason': '局部买压可能被吸收，不宜把单池压力误读成可以追多。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776682649, 'utc': '2026-04-20 10:57:29 UTC', 'beijing': '2026-04-20 18:57:29 UTC+08:00', 'state_key': 'OBSERVE_SHORT', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。', 'trade_action_key': 'SHORT_BIAS_OBSERVE', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'state_change'}, {'ts': 1776684189, 'utc': '2026-04-20 11:23:09 UTC', 'beijing': '2026-04-20 19:23:09 UTC+08:00', 'state_key': 'DO_NOT_CHASE_LONG', 'previous_state_key': 'OBSERVE_SHORT', 'reason': '确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。', 'trade_action_key': 'DO_NOT_CHASE_LONG', 'lp_alert_stage': 'confirm', 'telegram_update_kind': 'risk_blocker'}, {'ts': 1776687662, 'utc': '2026-04-20 12:21:02 UTC', 'beijing': '2026-04-20 20:21:02 UTC+08:00', 'state_key': 'OBSERVE_LONG', 'previous_state_key': 'DO_NOT_CHASE_LONG', 'reason': '方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。', 'trade_action_key': 'LONG_BIAS_OBSERVE', 'lp_alert_stage': 'climax', 'telegram_update_kind': 'state_change'}]`

## 6. Telegram 降噪与 suppression 分析

- `messages_before_suppression_estimate=137` `messages_after_suppression_actual=31`
- `telegram_should_send_count=56` `telegram_suppressed_count=81`
- `telegram_suppression_ratio=0.7737`
- `telegram_suppression_reasons={'no_trade_lock_local_signal_suppressed': 18, 'same_asset_state_repeat': 63}`
- `telegram_update_kind_distribution={'candidate': 3, 'risk_blocker': 27, 'state_change': 26, 'suppressed': 81}`
- `high_value_suppressed_count=0` `should_send_but_not_sent_count=25`

## 7. NO_TRADE_LOCK 分析

- `lock_entered_count=4` `lock_suppressed_count=18` `lock_released_count=4`
- `avg_lock_duration_sec=300.0` `median_lock_duration_sec=300.0`
- `lock_release_reasons={'ttl_expired': 4}`
- `lock_suppressed_stage_distribution={'climax': 3, 'confirm': 15}`
- `missed_lock_examples=[{'asset': 'ETH', 'first_signal_id': 'sig_abdabd2fb411beeb', 'second_signal_id': 'sig_a05d3f32ae1115f5', 'first_time_beijing': '2026-04-20 13:57:55 UTC+08:00', 'second_time_beijing': '2026-04-20 13:58:03 UTC+08:00', 'first_trade_action': 'DO_NOT_CHASE_LONG', 'second_trade_action': 'CONFLICT_NO_TRADE', 'first_state': 'DO_NOT_CHASE_LONG', 'second_state': 'NO_TRADE_CHOP'}]`

## 8. prealert 生命周期分析

- `prealert_count=1`
- `prealert_candidate_count=29`
- `prealert_gate_passed_count=29`
- `prealert_active_count=1`
- `prealert_delivered_count=1`
- `prealert_merged_count=19`
- `prealert_upgraded_to_confirm_count=29`
- `prealert_expired_count=0`
- `prealert_suppressed_by_lock_count=0`
- `median_prealert_to_confirm_sec=0.0`
- `first_seen_stage_prealert_count=29`
- `prealert_stage_overwritten_count=29`
- `first_seen_stage_distribution={'prealert': 137}`

## 9. candidate vs tradeable 分析

- `long_candidate_count=4`
- `short_candidate_count=0`
- `tradeable_long_count=0`
- `tradeable_short_count=0`
- `candidate_to_tradeable_count=0`
- `candidate_outcome_completed_rate=1.0`
- `candidate_followthrough_60s_rate=1.0`
- `candidate_adverse_60s_rate=0.0`
- `legacy_chase_action_rows=4`
- `primary_tradeable_blockers=['candidate_sample_count_below_min_samples(4<20)']`

## 10. outcome price source 与 30s/60s/300s 分析

- `window_status_distribution={'30s': {'completed': 82, 'expired': 50, 'pending': 5}, '60s': {'completed': 79, 'expired': 53, 'pending': 5}, '300s': {'completed': 3, 'expired': 127, 'pending': 7}}`
- `outcome_price_source_distribution={'okx_mark': 411}`
- `outcome_failure_reason_distribution={'window_elapsed_without_price_update': 230}`
- `expired_rate_by_window={'30s': 0.365, '60s': 0.3869, '300s': 0.927}`
- previous baseline `source_distribution={'pool_quote_proxy': 552}`
- `pool_quote_proxy_share_delta_vs_previous=-1.0`

## 11. OKX/Kraken live market context 分析

- window `live_public_count=137` `unavailable_count=0`
- window `okx_attempts=632` `okx_success=632` `okx_failure=0`
- window `kraken_attempts=0` `kraken_success=0` `kraken_failure=0`
- window `requested_to_resolved_distribution={'ETHUSDC->ETH-USDT-SWAP': 78, 'ETHUSDT->ETH-USDT-SWAP': 59}`
- CLI full `live_public_hit_rate=0.2452` `unavailable_rate=0.7548`

## 12. trade_action 与交易辅助价值分析

- `trade_action_distribution={'CONFLICT_NO_TRADE': 11, 'DO_NOT_CHASE_LONG': 35, 'DO_NOT_CHASE_SHORT': 25, 'LONG_BIAS_OBSERVE': 36, 'LONG_CHASE_ALLOWED': 4, 'NO_TRADE': 1, 'SHORT_BIAS_OBSERVE': 23, 'WAIT_CONFIRMATION': 2}`
- `delivered_distribution={'CONFLICT_NO_TRADE': 3, 'DO_NOT_CHASE_LONG': 10, 'DO_NOT_CHASE_SHORT': 6, 'LONG_BIAS_OBSERVE': 6, 'NO_TRADE': 1, 'SHORT_BIAS_OBSERVE': 5}`
- `do_not_chase_would_have_avoided_bad_chase_rate=0.0`
- `no_trade_high_noise_rate_300s=None`
- `generic_confirm_success_rate_300s=1.0` `generic_confirm_adverse_rate_300s=0.0`

## 13. 卖压后涨 / 买压后跌 反例专项

- `sell_pressure_adverse_30s_rate=0.1622`
- `sell_pressure_adverse_60s_rate=0.1765`
- `sell_pressure_adverse_300s_rate=0.0`
- `buy_pressure_adverse_30s_rate=0.2222`
- `buy_pressure_adverse_60s_rate=0.2222`
- `buy_pressure_adverse_300s_rate=0.0`
- `counterexample_reason_distribution={}`
- `top_counterexamples=[]`

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
- `asset_distribution={'ETH': 137}`
- `pair_distribution={'ETH/USDC': 78, 'ETH/USDT': 59}`
- CLI `recommended_next_round_pairs=['BTC/USDT', 'BTC/USDC', 'SOL/USDT', 'SOL/USDC']`

## 16. 噪音与误判风险评估

- `delivered_lp_ratio=0.2263`
- `suppressed_ratio=0.7737`
- `high_value_suppressed_count=0`
- `direction_conflict_pairs_within_5m=5`
- `state_change_message_count=26`
- `risk_blocker_message_count=3`

## 17. 最终评分

- `research_sampling_readiness=6.2/10`
- `telegram_denoise_effect=8.2/10`
- `asset_market_state_usability=8.8/10`
- `no_trade_lock_protection_value=7.5/10`
- `prealert_lifecycle_effectiveness=6.0/10`
- `candidate_tradeable_credibility=4.8/10`
- `outcome_completeness=6.4/10`
- `live_market_context_readiness=9.5/10`
- `trade_action_assistive_value=4.8/10`
- `noise_control=6.3/10`
- `overall_self_use_score=6.8/10`

## 18. 下一轮建议

- fully eliminate legacy LONG_CHASE_ALLOWED / SHORT_CHASE_ALLOWED wording from user-facing and archived action summaries when the state machine only wants candidate status.
- collect more Beijing-day prealert samples and verify first_seen_stage/prealert_to_confirm_sec survive all later upgrades.
- keep improving 30s/60s outcome completion until short-window reversal checks are reliably usable.
- prioritize enabling and validating BTC/SOL major pools before expanding any long-tail coverage.

## 19. 限制与不确定性

- selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- 30s outcome completion is still incomplete enough that very short-term reversal conclusions must stay conservative.
- BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.

## 附录: quality_reports CLI 参考

- `market-context-health.live_public_hit_rate=0.2452`
- `major-pool-coverage.covered_major_pairs=['ETH/USDT', 'ETH/USDC']`
- `summary.overall={'actionable': True, 'climax_count': 192, 'climax_reversal_count': 26, 'climax_reversal_score': 0.2318, 'confirm_conversion_score': 0.5338, 'confirm_count': 369, 'dimension': 'overall', 'fastlane_positive_followthrough': 0, 'fastlane_promotions': 1, 'fastlane_roi_score': 0.55, 'key': 'all', 'label': 'all', 'last_updated_at': 1776687665, 'market_context_alignment_positive_count': 462, 'market_context_alignment_score': 0.9631, 'market_context_available_count': 462, 'prealert_confirmed_count': 0, 'prealert_count': 2, 'prealert_false_count': 0, 'prealert_precision_score': 0.58, 'quality_hint': '历史一般', 'quality_score': 0.668, 'resolved_climaxes': 115, 'resolved_confirms': 165, 'resolved_fastlanes': 0, 'resolved_prealerts': 0, 'sample_size': 655, 'tuning_hint': 'retail 建议关闭该维度 prealert'}`
