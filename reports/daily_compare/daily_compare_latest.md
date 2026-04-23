# Daily Compare Report

## 执行摘要
- today_date: `2026-04-22`
- previous_date: `2026-04-21`
- compare_basis: `exact_previous_day`
- 整体判断：证据不足，无法把整体明确判定为进步/持平/退步；当前可直接判定的方向性指标太少，仅有 db_archive_mirror_match_rate, mismatch_warning_count, live_public_rate, unavailable_rate, market_context_attempt_success_rate, missing_major_pairs_count。
- 判断依据：判断依据基于 compare_basis=exact_previous_day 的结构化 JSON 指标，不使用 markdown 正文字符串比较。

## 数据来源与 Compare Basis
- today:
  - afternoon_evening_state: reports/afternoon_evening_state_summary_latest_2026-04-22.json (dated_summary_json, logical_date=2026-04-22)
  - overnight_trade_action: reports/overnight_trade_action_summary_latest_2026-04-22.json (dated_summary_json, logical_date=2026-04-22)
  - overnight_run: missing (logical_date=2026-04-22, source_kind=None; warnings=missing_summary:overnight_run:2026-04-22)
- previous:
  - afternoon_evening_state: missing (logical_date=2026-04-21, source_kind=None; warnings=missing_summary:afternoon_evening_state:2026-04-21)
  - overnight_trade_action: missing (logical_date=2026-04-21, source_kind=None; warnings=missing_summary:overnight_trade_action:2026-04-21)
  - overnight_run: reports/overnight_run_summary_latest.json (latest_summary_json, logical_date=2026-04-21)

## 核心指标对比表
| metric | today | previous | abs_change | pct_change | interpretation |
| --- | --- | --- | --- | --- | --- |
| raw_events_count | 534 | n/a |  |  | previous 字段缺失，无法判断 |
| parsed_events_count | 534 | n/a |  |  | previous 字段缺失，无法判断 |
| lp_signal_rows | 4 | n/a |  |  | previous 字段缺失，无法判断 |
| delivered_lp_signals | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| suppressed_lp_signals | 4 | n/a |  |  | previous 字段缺失，无法判断 |
| telegram_suppression_ratio | 1 | n/a |  |  | previous 字段缺失，无法判断 |
| opportunity_candidate_count | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| opportunity_verified_count | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| blocker_saved_rate | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| blocker_false_block_rate | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| outcome_60s_completed_rate | 0.5 | n/a |  |  | previous 字段缺失，无法判断 |
| market_context_attempt_success_rate | 1 | 0.9987 | 0.0013 | +0.13% | 改善：指标上升 |
| missing_major_pairs_count | 4 | 4 | 0 | +0.00% | 与 previous 持平 |

## 噪音与消息质量对比
- 实际发出消息 7 -> 0，但缺少高价值误抑制或 suppression previous 数据，不足以严格判断 Telegram 噪音是否改善。

## 候选/机会/阻止对比
- Candidate 数量和质量的 day-over-day 证据都不足。
- 今天没有 verified；主要结构化原因是 top_blockers={'history_completion_too_low': 39, 'late_or_chase': 21, 'no_trade_lock': 8}, no_candidate_reached_opportunity_score_gate。
- 证据不足；缺少 blocker_saved_rate 或 blocker_false_block_rate。

## Outcome 完整性对比
- 证据不足；缺少 outcome completed rate。

## Market Context 与 Majors 覆盖对比
- Market context 命中率改善；market_context_attempt_success_rate: 0.9987 -> 1 (+0.13%)。
- covered_major_pairs: ["ETH/USDT", "ETH/USDC"] -> ["ETH/USDT", "ETH/USDC"]；btc_signal_count: 0 -> 0；sol_signal_count: 0 -> 0
- 今天仍然主要只在 ETH 上学习；current_sample_still_eth_only=True。

## 今天相对昨天的进步
- market_context_attempt_success_rate: 0.9987 -> 1 (+0.13%)

## 今天相对昨天的退步
- 证据不足；没有足够强的方向性退步指标。

## 明天最该做什么
- 明天最优先处理 verified 缺失的主因：top_blockers={'history_completion_too_low': 39, 'late_or_chase': 21, 'no_trade_lock': 8}, no_candidate_reached_opportunity_score_gate。

## 限制与不确定性
- missing_metrics=asset_case_count,blocker_distribution,blocker_false_block_rate,blocker_saved_rate,candidate_300s_adverse_rate,candidate_300s_followthrough_rate,candidate_30s_adverse_rate,candidate_30s_followthrough_rate,candidate_60s_adverse_rate,candidate_60s_followthrough_rate,candidate_outcome_completion_rate,candidate_to_verified_rate,case_followup_count,catchup_completed_count,catchup_expired_count,configured_but_unsupported_chain,configured_but_validation_failed,delivered_lp_signals,expired_rate_by_window,hard_blocker_distribution
- missing_summary:afternoon_evening_state:2026-04-21
- missing_summary:overnight_run:2026-04-22
- missing_summary:overnight_trade_action:2026-04-21
- previous:overnight_run:Exact 30s move_after fields are not persisted in a way that supports overnight ratio calculation.
- previous:overnight_run:Window-level 60s move_after coverage is near zero, so only 300s reversal analysis is robust.
- previous:overnight_run:raw_events and parsed_events archives are missing.
- today:afternoon_evening_state:30s outcome completion is still incomplete enough that very short-term reversal conclusions must stay conservative.
- today:afternoon_evening_state:BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.
- today:afternoon_evening_state:selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- today:afternoon_evening_state:selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- today:overnight_trade_action:300s outcome coverage is almost entirely expired due window_elapsed_without_price_update.
- today:overnight_trade_action:parsed_events archive is missing in the selected overnight window.
- today:overnight_trade_action:prealert diagnostics show overwritten=true everywhere, so the exact overwrite point cannot be reconstructed from raw/parsed layers.
- today:overnight_trade_action:raw_events archive is missing in the selected overnight window.
