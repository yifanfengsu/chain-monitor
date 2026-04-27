# Daily Compare Report

## 执行摘要
- today_date: `2026-04-24`
- previous_date: `2026-04-23`
- compare_basis: `exact_previous_day`
- source_mode: `canonical_daily_report`
- 整体判断：证据不足，无法把整体明确判定为进步/持平/退步；当前可直接判定的方向性指标太少，仅有 mismatch_warning_count, high_value_suppressed_count, legacy_chase_leaked_count, candidate_outcome_completion_rate, blocker_saved_rate, blocker_false_block_rate。
- 判断依据：判断依据基于 compare_basis=exact_previous_day 的结构化 JSON 指标，不使用 markdown 正文字符串比较。

## 数据来源与 Compare Basis
- today:
  - daily_report: reports/daily/daily_report_2026-04-24.json (dated_daily_report_json, logical_date=2026-04-24)
- previous:
  - daily_report: reports/daily/daily_report_2026-04-23.json (dated_daily_report_json, logical_date=2026-04-23)

## 重建尝试与严格模式结果
- strict_mode: `false`
- rebuild_mode: `false`
- compare_basis: `exact_previous_day`
- selected_today: `2026-04-24`
- selected_previous: `2026-04-23`
- strict_failure_reason: `null`
- rebuild_attempted: `false`
- today rebuilt success: none
- today rebuilt failed: none
- previous rebuilt success: none
- previous rebuilt failed: none
- 最终仍有 limitations 时，表示数据不足或输入不完整，不是脚本猜值。

## 核心指标对比表
| metric | today | previous | abs_change | pct_change | interpretation |
| --- | --- | --- | --- | --- | --- |
| raw_events_count | 85608 | 39649 | 45959 | +115.91% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| parsed_events_count | 45239 | 19055 | 26184 | +137.41% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| lp_signal_rows | 197 | 153 | 44 | +28.76% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| delivered_lp_signals | 67 | 42 | 25 | +59.52% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| suppressed_lp_signals | 130 | 111 | 19 | +17.12% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| telegram_suppression_ratio | 0.5228 | 0.5556 | -0.0328 | -5.90% | 数量变化；仅表示规模/活动变化，不直接代表质量改善 |
| opportunity_candidate_count | 5 | 5 | 0 | +0.00% | 与 previous 持平 |
| opportunity_verified_count | 0 | 0 | 0 |  | 与 previous 持平 |
| blocker_saved_rate | 0 | 0 | 0 |  | 与 previous 持平 |
| blocker_false_block_rate | 0 | 0 | 0 |  | 与 previous 持平 |
| outcome_60s_completed_rate | 1 | 1 | 0 | +0.00% | 与 previous 持平 |
| market_context_attempt_success_rate | 0.9983 | 1 | -0.0017 | -0.17% | 退步：指标下降 |
| missing_major_pairs_count | 4 | 4 | 0 | +0.00% | 与 previous 持平 |

## 噪音与消息质量对比
- Telegram 噪音变多了；实际发出消息 42 -> 67。telegram_suppression_ratio: 0.5556 -> 0.5228 (-5.90%)

## 候选/机会/阻止对比
- Candidate 数量变化为 opportunity_candidate_count: 5 -> 5 (+0.00%)；质量指标没有明显方向性变化。
- 今天没有 verified；主要结构化原因是 top_blockers={'late_or_chase': 57, 'history_completion_too_low': 39, 'no_trade_lock': 15}, candidates_exist_but_verified_gate_not_open。
- Blocker 效果基本持平；blocker_saved_rate: 0 -> 0，blocker_false_block_rate: 0 -> 0。

## Outcome 完整性对比
- 30s/60s/300s outcome 完整性变化混合；outcome_30s_completed_rate: 1 -> 1 (+0.00%)；outcome_60s_completed_rate: 1 -> 1 (+0.00%)；outcome_300s_completed_rate: 1 -> 1 (+0.00%)。

## Market Context 与 Majors 覆盖对比
- Market context 命中率退步；market_context_attempt_success_rate: 1 -> 0.9983 (-0.17%)。
- covered_major_pairs: ["ETH/USDT", "ETH/USDC"] -> ["ETH/USDT", "ETH/USDC"]；btc_signal_count: 0 -> 0；sol_signal_count: 0 -> 0
- 今天仍然主要只在 ETH 上学习；current_sample_still_eth_only=True。

## 今天相对昨天的进步
- 证据不足；没有足够强的方向性改善指标。

## 今天相对昨天的退步
- market_context_attempt_success_rate: 1 -> 0.9983 (-0.17%)

## 明天最该做什么
- 明天最优先修 market_context_attempt_success_rate 对应链路；当前最明显退步是 market_context_attempt_success_rate: 1 -> 0.9983 (-0.17%)。

## 限制与不确定性
- missing_metrics=candidate_300s_adverse_rate,candidate_300s_followthrough_rate,candidate_30s_adverse_rate,candidate_30s_followthrough_rate,candidate_60s_adverse_rate,candidate_60s_followthrough_rate,configured_but_unsupported_chain,configured_but_validation_failed,db_archive_mirror_match_rate,expired_rate_by_window,median_prealert_to_confirm_sec,missing_tables,missing_tables_count,opportunity_invalidated_count,verified_300s_adverse_rate,verified_300s_followthrough_rate,verified_30s_adverse_rate,verified_30s_followthrough_rate,verified_60s_adverse_rate,verified_60s_followthrough_rate
- previous:daily_report:CANDIDATE is not a trade signal
- previous:daily_report:multiple_active_segments; report does not claim continuous runtime
- previous:daily_report:verified_maturity=immature; VERIFIED must not be treated as mature trade signal
- today:daily_report:CANDIDATE is not a trade signal
- today:daily_report:multiple_active_segments; report does not claim continuous runtime
- today:daily_report:verified_maturity=immature; VERIFIED must not be treated as mature trade signal
