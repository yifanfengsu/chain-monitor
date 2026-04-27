import contextlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "reports"
LEGACY_REPORTS_DIR = REPORTS_DIR / "legacy"
if str(LEGACY_REPORTS_DIR) not in sys.path:
    sys.path.insert(0, str(LEGACY_REPORTS_DIR))

from reports.legacy import generate_afternoon_evening_state_analysis_latest as report  # noqa: E402


class AfternoonEveningStateReportTests(unittest.TestCase):
    def test_main_wires_final_outputs_only_to_markdown_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            window = {
                "start_ts": 1_000,
                "end_ts": 1_060,
                "start_utc": "2026-04-22 05:00:00 UTC",
                "end_utc": "2026-04-22 05:01:00 UTC",
            }
            sqlite_source = {
                "report_data_source": "sqlite",
                "data_source": "sqlite",
                "sqlite_rows_by_table": {},
                "archive_rows_by_category": {},
                "compressed_archive_rows": {},
                "db_archive_mirror_detail": {},
                "mismatch_warnings": [],
            }
            run_overview = {"lp_signal_rows": 1}
            stage_summary = {"stage_distribution_pct": {}}
            asset_market_states = {"summary": "states"}
            no_trade_lock = {"summary": "lock"}
            telegram = {"summary": "telegram"}
            prealerts = {"summary": "prealerts"}
            candidate_tradeable = {"summary": "candidate_tradeable"}
            opportunities = {"summary": "opportunities"}
            final_outputs = {
                "final_trading_output_distribution": {"VERIFIED": 1},
                "delivered_verified_count": 1,
                "delivered_candidate_count": 0,
                "delivered_blocked_count": 0,
                "legacy_chase_downgraded_count": 0,
                "legacy_chase_leaked_count": 0,
                "opportunity_gate_failures": {},
                "messages_blocked_by_opportunity_gate": 0,
            }
            outcome_detail = {"summary": "outcomes"}
            market_context = {"summary": "market_context"}
            trade_actions = {"summary": "trade_actions"}
            adverse_direction = {"summary": "adverse_direction"}
            archive_integrity = {"summary": "archive_integrity"}
            majors = {"summary": "majors"}
            noise = {"summary": "noise"}
            scorecard = {"summary": "scorecard"}
            expected_data_sources = [
                {
                    "path": "data/chain_monitor.sqlite",
                    "exists": False,
                    "record_count": 0,
                    "start_ts": None,
                    "end_ts": None,
                    "start_utc": "",
                    "end_utc": "",
                    "start_beijing": "",
                    "end_beijing": "",
                    "window_record_count": "",
                    "after_cutoff_count": "",
                    "notes": (
                        "sqlite mirror/query layer report_data_source=sqlite "
                        "sqlite_rows_by_table={} "
                        "archive_rows_by_category={} "
                        "db_archive_mirror_match_rate=None "
                        "archive_fallback_used=False "
                        "mismatch_warnings=[]"
                    ),
                }
            ]

            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.object(report, "REPORTS_DIR", tmp_path))
                stack.enter_context(mock.patch.object(report, "MARKDOWN_PATH", tmp_path / "report.md"))
                stack.enter_context(mock.patch.object(report, "CSV_PATH", tmp_path / "report.csv"))
                stack.enter_context(mock.patch.object(report, "JSON_PATH", tmp_path / "report.json"))
                stack.enter_context(mock.patch.object(report, "load_runtime_config", return_value={}))
                stack.enter_context(mock.patch.object(report, "sqlite_report_source_summary", return_value=sqlite_source))
                stack.enter_context(mock.patch.object(report, "latest_archive_date", return_value="2026-04-22"))
                stack.enter_context(mock.patch.object(report, "choose_analysis_window", return_value=window))
                stack.enter_context(mock.patch.object(report, "load_signals", return_value=([], None)))
                stack.enter_context(mock.patch.object(report, "load_quality_cache", return_value=([], {}, None)))
                stack.enter_context(mock.patch.object(report, "load_asset_case_cache", return_value=({}, None)))
                stack.enter_context(mock.patch.object(report, "load_asset_market_state_cache", return_value=({}, None)))
                stack.enter_context(mock.patch.object(report, "load_trade_opportunity_cache", return_value=({}, None)))
                stack.enter_context(mock.patch.object(report, "join_lp_rows", return_value=([], [], [{"signal_id": "sig-1"}])))
                stack.enter_context(mock.patch.object(report, "stream_delivery_audit", return_value=({}, {"delivery_rows": 0})))
                stack.enter_context(mock.patch.object(report, "stream_cases", return_value=({}, {"case_ids": set()})))
                stack.enter_context(mock.patch.object(report, "stream_case_followups", return_value=({}, {"followup_rows": 0})))
                stack.enter_context(mock.patch.object(report, "run_cli", side_effect=[{}, {}, {}, "metric,value\n"]))
                stack.enter_context(mock.patch.object(report, "baseline_previous_report", return_value={}))
                stack.enter_context(mock.patch.object(report, "build_data_source_inventory", return_value=([], [])))
                stack.enter_context(mock.patch.object(report, "compute_run_overview", return_value=run_overview))
                stack.enter_context(mock.patch.object(report, "compute_stage_summary", return_value=stage_summary))
                stack.enter_context(mock.patch.object(report, "compute_asset_market_state_detail", return_value=asset_market_states))
                stack.enter_context(mock.patch.object(report, "compute_no_trade_lock_detail", return_value=no_trade_lock))
                stack.enter_context(mock.patch.object(report, "compute_telegram_detail", return_value=telegram))
                stack.enter_context(mock.patch.object(report, "compute_prealert_lifecycle_detail", return_value=prealerts))
                stack.enter_context(mock.patch.object(report, "compute_candidate_tradeable_detail", return_value=candidate_tradeable))
                stack.enter_context(mock.patch.object(report, "compute_trade_opportunities", return_value=opportunities))
                stack.enter_context(mock.patch.object(report, "compute_final_trading_output_summary", return_value=final_outputs))
                stack.enter_context(mock.patch.object(report, "compute_outcome_detail", return_value=outcome_detail))
                stack.enter_context(mock.patch.object(report, "compute_market_context", return_value=market_context))
                stack.enter_context(mock.patch.object(report, "compute_trade_action_detail", return_value=trade_actions))
                stack.enter_context(mock.patch.object(report, "compute_directional_adverse_summary", return_value=adverse_direction))
                stack.enter_context(mock.patch.object(report, "compute_archive_detail", return_value=archive_integrity))
                stack.enter_context(mock.patch.object(report, "compute_majors", return_value=majors))
                stack.enter_context(mock.patch.object(report, "compute_noise_assessment", return_value=noise))
                stack.enter_context(mock.patch.object(report, "compute_scorecard", return_value=scorecard))
                stack.enter_context(mock.patch.object(report, "build_top_findings", return_value=[]))
                stack.enter_context(mock.patch.object(report, "build_recommendations", return_value=[]))
                stack.enter_context(mock.patch.object(report, "build_limitations", return_value=[]))
                stack.enter_context(mock.patch.object(report, "write_dated_report_copies"))
                build_csv_rows_mock = stack.enter_context(mock.patch.object(report, "build_csv_rows", autospec=True, return_value=[]))
                build_markdown_mock = stack.enter_context(mock.patch.object(report, "build_markdown", autospec=True, return_value="# ok\n"))
                result = report.main([])

            self.assertEqual(0, result)
            normalized_opportunities = report.normalize_opportunity_summary(opportunities)
            self.assertEqual(
                (
                    window,
                    run_overview,
                    stage_summary,
                    asset_market_states,
                    no_trade_lock,
                    telegram,
                    prealerts,
                    candidate_tradeable,
                    normalized_opportunities,
                    outcome_detail,
                    market_context,
                    trade_actions,
                    adverse_direction,
                    archive_integrity,
                    majors,
                    scorecard,
                ),
                build_csv_rows_mock.call_args.args,
            )
            self.assertEqual(
                (
                    expected_data_sources,
                    window,
                    {},
                    run_overview,
                    stage_summary,
                    asset_market_states,
                    telegram,
                    no_trade_lock,
                    prealerts,
                    candidate_tradeable,
                    final_outputs,
                    normalized_opportunities,
                    outcome_detail,
                    market_context,
                    trade_actions,
                    adverse_direction,
                    archive_integrity,
                    majors,
                    noise,
                    scorecard,
                    {},
                    {},
                    {},
                    [
                        "统一出口审计：final_output_distribution={'VERIFIED': 1} verified=1 candidate=0 blocked=0.",
                        "legacy chase 审计：downgraded=0 leaked=0 gate_failures={} blocked_by_gate=0.",
                    ],
                    [],
                    [],
                ),
                build_markdown_mock.call_args.args,
            )


if __name__ == "__main__":
    unittest.main()
