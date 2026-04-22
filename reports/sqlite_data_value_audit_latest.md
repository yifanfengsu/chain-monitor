# SQLite Data Value Audit

- DB path: `/run-project/chain-monitor/data/chain_monitor.sqlite`
- DB size: `5203.8008` MB
- WAL size: `0.004` MB
- Total JSON payload: `4846.949` MB
- Estimated compact savings: `4518.1117` MB

## Long-term Full

signals, signal_features, market_context_snapshots, outcomes, asset_cases, asset_market_states, no_trade_locks, trade_opportunities, opportunity_outcomes, quality_stats, prealert_lifecycle

## Long-term Slim

market_context_attempts, telegram_deliveries, delivery_audit, case_followups

## Index-only

raw_events, parsed_events

## Archive-only Full Payload

raw_events.raw_json, parsed_events.parsed_json, delivery_audit.audit_json, telegram_deliveries.message_json, case_followups.followup_json

## Table Audit

| table | rows | value_class | current_mode | recommended_mode | json_payload_mb | table_size_mb | can_compact |
| --- | ---: | --- | --- | --- | ---: | ---: | --- |
| schema_meta | 1 | disposable_or_rebuildable | full | full | 0.0000 | 0.0039 | no |
| runs | 0 | disposable_or_rebuildable | full | full | 0.0000 | 0.0039 | no |
| raw_events | 97467 | archive_debug | index_only | index_only | 315.6794 | 383.7109 | yes |
| parsed_events | 48752 | archive_debug | index_only | index_only | 1517.1377 | 1565.7656 | yes |
| signals | 1633 | core_learning | full | full | 130.7994 | 139.2461 | no |
| signal_features | 6440 | core_learning | full | full | 0.0000 | 0.4336 | no |
| market_context_snapshots | 1633 | core_learning | full | full | 0.0000 | 0.2344 | no |
| market_context_attempts | 2283 | operational_diagnostics | slim | slim | 0.0000 | 0.3594 | no |
| outcomes | 3009 | core_outcome | full | full | 0.0000 | 0.6602 | no |
| asset_cases | 5391 | core_state | full | full | 3.9975 | 6.9375 | no |
| asset_market_states | 420 | core_state | full | full | 55.0854 | 58.0898 | no |
| no_trade_locks | 342 | core_state | full | full | 0.0061 | 0.0625 | no |
| trade_opportunities | 491 | core_learning | full | full | 4.0642 | 4.9688 | no |
| opportunity_outcomes | 1473 | core_outcome | full | full | 0.0000 | 0.2539 | no |
| quality_stats | 1042 | core_learning | full | full | 3.6476 | 4.3359 | no |
| telegram_deliveries | 107031 | operational_diagnostics | slim | slim | 1424.7038 | 1456.1211 | yes |
| prealert_lifecycle | 1829 | core_state | full | full | 131.2372 | 139.5859 | no |
| delivery_audit | 156459 | operational_diagnostics | slim | slim | 1260.5053 | 1351.3828 | yes |
| case_followups | 151 | operational_diagnostics | slim | slim | 0.0854 | 0.1289 | yes |

## Audit Answers

- 必须长期保留：signals, signal_features, market_context_snapshots, outcomes, asset_cases, asset_market_states, no_trade_locks, trade_opportunities, opportunity_outcomes, quality_stats, prealert_lifecycle
- 应只保留 slim/index：market_context_attempts, telegram_deliveries, delivery_audit, case_followups, raw_events, parsed_events
- full JSON 只留 archive：raw_events.raw_json, parsed_events.parsed_json, delivery_audit.audit_json, telegram_deliveries.message_json, case_followups.followup_json
- 可安全 compact/prune：raw_events, parsed_events, telegram_deliveries, delivery_audit, case_followups
- 不应再导入数据库字段：raw_events.raw_json, parsed_events.parsed_json, delivery_audit.audit_json, telegram_deliveries.message_json
- 支持 verified/opportunity_score 必留字段：signals.signal_id, signals.asset, signals.pair, signals.trade_opportunity_status, signal_features.trade_opportunity_score, outcomes.direction_adjusted_move, opportunity_outcomes.followthrough, opportunity_outcomes.adverse, quality_stats.sample_count, quality_stats.stats_json, asset_market_states.current_state, no_trade_locks.reason, trade_opportunities.status, trade_opportunities.score_components_json, trade_opportunities.history_snapshot_json, trade_opportunities.evidence_json
