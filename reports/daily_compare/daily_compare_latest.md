# Daily Compare Report

## 执行摘要
- today_date: `2026-04-22`
- previous_date: `2026-04-21`
- compare_basis: `exact_previous_day`
- 整体判断：有进步；主要依据是 market_context_attempt_success_rate: 0.9987 -> 1 (+0.13%); outcome_300s_completed_rate: 0 -> 0.5; outcome_30s_completed_rate: 0.4559 -> 0.5 (+9.67%)。
- 判断依据：判断依据基于 compare_basis=exact_previous_day 的结构化 JSON 指标，不使用 markdown 正文字符串比较。

## 数据来源与 Compare Basis
- today:
  - afternoon_evening_state: reports/afternoon_evening_state_summary_latest_2026-04-22.json (dated_summary_json, logical_date=2026-04-22)
  - overnight_trade_action: reports/overnight_trade_action_summary_latest_2026-04-22.json (dated_summary_json, logical_date=2026-04-22)
  - overnight_run: reports/overnight_run_summary_latest_2026-04-22.json (dated_summary_json, logical_date=2026-04-22)
- previous:
  - afternoon_evening_state: missing (logical_date=2026-04-21, source_kind=None; warnings=missing_summary:afternoon_evening_state:2026-04-21)
  - overnight_trade_action: reports/overnight_trade_action_summary_latest_2026-04-21.json (dated_summary_json, logical_date=2026-04-21)
  - overnight_run: reports/overnight_run_summary_latest_2026-04-21.json (dated_summary_json, logical_date=2026-04-21)

## 重建尝试与严格模式结果
- strict_mode: `false`
- rebuild_mode: `true`
- compare_basis: `exact_previous_day`
- selected_today: `2026-04-22`
- selected_previous: `2026-04-21`
- strict_failure_reason: `null`
- rebuild_attempted: `true`
- today rebuilt success: none
- today rebuilt failed: none
- previous rebuilt success: overnight_trade_action, overnight_run
- previous rebuilt failed: afternoon_evening_state(generator_failed)
- rebuild_warnings: rebuild_failed:afternoon_evening_state:2026-04-21:exit=1:Traceback (most recent call last):
  File "/run-project/chain-monitor/reports/generate_afternoon_evening_state_analysis_latest.py", line 2494, in <module>
    raise SystemExit(main
- 最终仍有 limitations 时，表示数据不足或输入不完整，不是脚本猜值。

## 核心指标对比表
| metric | today | previous | abs_change | pct_change | interpretation |
| --- | --- | --- | --- | --- | --- |
| raw_events_count | 534 | n/a |  |  | previous 字段缺失，无法判断 |
| parsed_events_count | 534 | n/a |  |  | previous 字段缺失，无法判断 |
| lp_signal_rows | 4 | n/a |  |  | previous 字段缺失，无法判断 |
| delivered_lp_signals | 0 | n/a |  |  | previous 字段缺失，无法判断 |
| suppressed_lp_signals | 4 | n/a |  |  | previous 字段缺失，无法判断 |
| telegram_suppression_ratio | 1 | n/a |  |  | previous 字段缺失，无法判断 |
| opportunity_candidate_count | 0 | 3 | -3 | -100.00% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| opportunity_verified_count | 0 | 0 | 0 |  | 与 previous 持平 |
| blocker_saved_rate | 0 | 0 | 0 |  | 与 previous 持平 |
| blocker_false_block_rate | 0 | 0 | 0 |  | 与 previous 持平 |
| outcome_60s_completed_rate | 0.5 | 0.4118 | 0.0882 | +21.42% | 改善：指标上升 |
| market_context_attempt_success_rate | 1 | 0.9987 | 0.0013 | +0.13% | 改善：指标上升 |
| missing_major_pairs_count | 4 | 4 | 0 | +0.00% | 与 previous 持平 |

## 噪音与消息质量对比
- 实际发出消息 7 -> 0，但缺少高价值误抑制或 suppression previous 数据，不足以严格判断 Telegram 噪音是否改善。

## 候选/机会/阻止对比
- Candidate 数量变化为 opportunity_candidate_count: 3 -> 0 (-100.00%)；但 followthrough/adverse/completion 的 previous 数据不足，无法严格判断质量提升或下降。
- 今天没有 verified；主要结构化原因是 top_blockers={'history_completion_too_low': 39, 'late_or_chase': 21, 'no_trade_lock': 8}, no_candidate_reached_opportunity_score_gate。
- Blocker 效果基本持平；blocker_saved_rate: 0 -> 0，blocker_false_block_rate: 0 -> 0。

## Outcome 完整性对比
- 30s/60s/300s outcome 完整性改善；outcome_30s_completed_rate: 0.4559 -> 0.5 (+9.67%)；outcome_60s_completed_rate: 0.4118 -> 0.5 (+21.42%)；outcome_300s_completed_rate: 0 -> 0.5。

## Market Context 与 Majors 覆盖对比
- Market context 命中率改善；market_context_attempt_success_rate: 0.9987 -> 1 (+0.13%)。
- covered_major_pairs: ["ETH/USDT", "ETH/USDC"] -> ["ETH/USDT", "ETH/USDC"]；btc_signal_count: 0 -> 0；sol_signal_count: 0 -> 0
- 今天仍然主要只在 ETH 上学习；current_sample_still_eth_only=True。

## 今天相对昨天的进步
- market_context_attempt_success_rate: 0.9987 -> 1 (+0.13%)

## 今天相对昨天的退步
- legacy_chase_leaked_count: 0 -> 4

## 明天最该做什么
- 明天最优先修 legacy_chase_leaked_count 对应链路；当前最明显退步是 legacy_chase_leaked_count: 0 -> 4。

## 限制与不确定性
- missing_metrics=asset_case_count,candidate_300s_adverse_rate,candidate_300s_followthrough_rate,candidate_30s_adverse_rate,candidate_30s_followthrough_rate,candidate_60s_adverse_rate,candidate_60s_followthrough_rate,candidate_outcome_completion_rate,candidate_to_verified_rate,case_followup_count,delivered_lp_signals,expired_rate_by_window,high_value_suppressed_count,lp_signal_rows,median_prealert_to_confirm_sec,opportunity_invalidated_count,parsed_events_count,raw_events_count,signals_count,suppressed_lp_signals
- missing_summary:afternoon_evening_state:2026-04-21
- previous:overnight_run:Exact 30s move_after fields are not persisted in a way that supports overnight ratio calculation.
- previous:overnight_run:Window-level 60s move_after coverage is near zero, so only 300s reversal analysis is robust.
- previous:overnight_run:raw_events and parsed_events archives are missing.
- previous:overnight_trade_action:300s outcome coverage is almost entirely expired due window_elapsed_without_price_update.
- previous:overnight_trade_action:parsed_events archive is missing in the selected overnight window.
- previous:overnight_trade_action:prealert diagnostics show overwritten=true everywhere, so the exact overwrite point cannot be reconstructed from raw/parsed layers.
- previous:overnight_trade_action:raw_events archive is missing in the selected overnight window.
- rebuild_failed:afternoon_evening_state:2026-04-21:exit=1:Traceback (most recent call last):
  File "/run-project/chain-monitor/reports/generate_afternoon_evening_state_analysis_latest.py", line 2494, in <module>
    raise SystemExit(main
- today:afternoon_evening_state:30s outcome completion is still incomplete enough that very short-term reversal conclusions must stay conservative.
- today:afternoon_evening_state:BTC/SOL have no real selected-window samples, so market-state, lock, and outcome conclusions are ETH-centric.
- today:afternoon_evening_state:selected window contains no TRADEABLE_* states, so candidate-to-tradeable promotion is only validated negatively (blocked), not positively (triggered).
- today:afternoon_evening_state:selected window contains too few prealert-stage rows to fully validate the new prealert lifecycle on its own.
- today:overnight_run:Exact 30s move_after fields are not persisted in a way that supports overnight ratio calculation.
- today:overnight_run:Window-level 60s move_after coverage is near zero, so only 300s reversal analysis is robust.
- today:overnight_run:raw_events and parsed_events archives are missing.
- today:overnight_trade_action:300s outcome coverage is almost entirely expired due window_elapsed_without_price_update.
- today:overnight_trade_action:parsed_events archive is missing in the selected overnight window.
- today:overnight_trade_action:prealert diagnostics show overwritten=true everywhere, so the exact overwrite point cannot be reconstructed from raw/parsed layers.
