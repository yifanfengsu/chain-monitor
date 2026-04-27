import contextlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import reports.legacy.generate_overnight_run_analysis_latest as report


class DefaultRuntimeConfig(dict):
    def __missing__(self, key):
        value = {"runtime_value": "n/a"}
        self[key] = value
        return value


class DefaultSummary(dict):
    def __missing__(self, key):
        return 0


def _summary(**values):
    payload = DefaultSummary()
    payload.update(values)
    return payload


def _render_markdown(opportunities):
    start_ts = 1_700_000_000
    end_ts = start_ts + 60
    return report.build_markdown(
        [report.FileInventory("data/signals.ndjson", True, 1, start_ts, end_ts, "signals")],
        {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_sec": 60,
            "duration_hours": 0.02,
            "selection_reason": "unit_test",
        },
        [
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "duration_hours": 0.02,
                "total_signal_rows": 1,
                "lp_signal_rows": 1,
            }
        ],
        DefaultRuntimeConfig(),
        _summary(
            total_signal_rows=1,
            lp_signal_rows=1,
            delivered_lp_signals=1,
            asset_case_count=1,
            case_followup_count=0,
        ),
        _summary(stage_distribution_pct={}),
        _summary(live_public_count=1, kraken_attempts=0),
        _summary(live_public_hit_rate=1.0, per_venue={}),
        _summary(prealert_count=0),
        _summary(),
        _summary(sweep_building_count=0, sweep_building_display_climax_residual_count=0),
        _summary(),
        _summary(trade_action_distribution={}, long_chase_allowed_count=0, short_chase_allowed_count=0),
        _summary(state_distribution={}),
        _summary(lock_entered_count=0, suppressed_count=0, release_count=0),
        _summary(),
        _summary(candidate_count=0, tradeable_count=0),
        _summary(
            final_trading_output_distribution={},
            delivered_verified_count=0,
            delivered_candidate_count=0,
            delivered_blocked_count=0,
            legacy_chase_downgraded_count=0,
            legacy_chase_leaked_count=0,
            trade_action_chase_without_opportunity_count=0,
            messages_blocked_by_opportunity_gate=0,
            all_opportunity_labels_verified=True,
            all_candidate_labels_are_candidate=True,
            blocked_covers_legacy_chase_risk=True,
        ),
        opportunities,
        _summary(source_distribution={}),
        _summary(suppression_reasons={}),
        _summary(state_changes_sent=0, risk_blockers_sent=0, candidates_sent=0),
        _summary(examples=[]),
        _summary(
            asset_distribution={},
            pair_distribution={},
            covered_major_pairs=[],
            missing_major_pairs=[],
            major_cli_summary={},
        ),
        {},
        _summary(
            outcome_window_status={},
            fastlane_promoted_count_window=0,
            fastlane_promoted_delivered_count_window=0,
            resolved_move_after_60s_count_window=0,
            resolved_move_after_300s_count_window=0,
            full_summary_cli={"overall": {}},
        ),
        {},
        {},
    )


class OvernightRunReportTests(unittest.TestCase):
    def test_build_markdown_uses_opportunity_summary_when_present(self):
        markdown = _render_markdown(
            {
                "verified_maturity": "immature",
                "verified_should_not_be_traded_reason": "profile_sample_count_below_20",
                "maturity_reasons": ["profile_sample_count_below_20"],
            }
        )

        self.assertIn("opportunity maturity: verified_maturity=`immature`", markdown)
        self.assertIn("should_not_trade=`profile_sample_count_below_20`", markdown)
        self.assertIn("maturity_reasons=`['profile_sample_count_below_20']`", markdown)

    def test_build_markdown_uses_safe_defaults_when_opportunity_summary_missing(self):
        markdown = _render_markdown({})

        self.assertIn("opportunity maturity: verified_maturity=`unknown`", markdown)
        self.assertIn("should_not_trade=`n/a`", markdown)
        self.assertIn("maturity_reasons=`n/a`", markdown)

    def test_normalize_opportunity_summary_defaults_missing_maturity_fields(self):
        normalized = report.normalize_opportunity_summary({"opportunity_candidate_count": 1})

        self.assertEqual("unknown", normalized["verified_maturity"])
        self.assertEqual("", normalized["verified_should_not_be_traded_reason"])
        self.assertEqual([], normalized["maturity_reasons"])
        self.assertEqual("unknown", normalized["opportunity_summary"]["verified_maturity"])

    def test_main_writes_summary_defaults_and_passes_opportunities_to_markdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            inventory = report.FileInventory("data/unit.ndjson", True, 1, 1_700_000_000, 1_700_000_060, "unit")
            window = {
                "start_ts": 1_700_000_000,
                "end_ts": 1_700_000_060,
                "duration_sec": 60,
                "duration_hours": 0.02,
                "selection_reason": "unit_test",
            }
            sqlite_source = {
                "db_path": "data/chain_monitor.sqlite",
                "db_exists": False,
                "report_data_source": "archive",
                "data_source": "archive",
                "sqlite_rows_by_table": {},
                "archive_rows_by_category": {},
                "compressed_archive_rows": {},
                "sqlite_health": {},
                "db_archive_mirror_detail": {},
                "mismatch_warnings": [],
            }

            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.object(report, "REPORTS_DIR", tmp_path))
                stack.enter_context(mock.patch.object(report, "MARKDOWN_PATH", tmp_path / "report.md"))
                stack.enter_context(mock.patch.object(report, "CSV_PATH", tmp_path / "report.csv"))
                stack.enter_context(mock.patch.object(report, "JSON_PATH", tmp_path / "report.json"))
                stack.enter_context(mock.patch.object(report, "load_runtime_config", return_value={}))
                stack.enter_context(mock.patch.object(report, "sqlite_report_source_summary", return_value=sqlite_source))
                stack.enter_context(mock.patch.object(report, "load_signals", return_value=([{"archive_ts": 1_700_000_000}], [inventory])))
                stack.enter_context(mock.patch.object(report, "choose_window", return_value=(window, [window])))
                stack.enter_context(mock.patch.object(report, "load_quality_cache", return_value=([], {}, inventory)))
                stack.enter_context(mock.patch.object(report, "load_asset_case_cache", return_value=({}, inventory)))
                stack.enter_context(mock.patch.object(report, "load_trade_opportunity_cache", return_value=({}, inventory)))
                stack.enter_context(mock.patch.object(report, "join_lp_rows", return_value=([], [], [{"signal_id": "sig-1", "sent_to_telegram": True}])))
                stack.enter_context(mock.patch.object(report, "stream_delivery_audit", return_value=([inventory], {})))
                stack.enter_context(mock.patch.object(report, "stream_cases", return_value=([inventory], {"case_ids": set()})))
                stack.enter_context(mock.patch.object(report, "stream_case_followups", return_value=([inventory], {"followup_rows": 0})))
                stack.enter_context(mock.patch.object(report, "inventory_category", return_value=[inventory]))
                stack.enter_context(mock.patch.object(report, "run_cli", side_effect=[{}, {"expected_major_pairs": []}, {}, "metric,value\n"]))
                stack.enter_context(mock.patch.object(report, "stage_distribution", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_market_context", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_prealerts", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_confirms", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_sweeps", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_absorption", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_trade_actions", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_asset_market_states", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_no_trade_lock_summary", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_prealert_lifecycle_summary", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_candidate_tradeable_summary", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_final_trading_output_summary", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_trade_opportunities", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_outcome_price_sources", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_telegram_suppression", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_noise_reduction", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_reversal_special", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_majors", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_archive_integrity", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_quality_and_fastlane", return_value={}))
                stack.enter_context(mock.patch.object(report, "compute_noise", return_value={}))
                stack.enter_context(mock.patch.object(report, "scorecard", return_value={}))
                stack.enter_context(mock.patch.object(report, "build_csv_rows", return_value=[]))
                stack.enter_context(mock.patch.object(report, "write_csv"))
                stack.enter_context(mock.patch.object(report, "write_dated_report_copies", return_value={}))
                build_markdown = stack.enter_context(mock.patch.object(report, "build_markdown", return_value="# ok\n"))
                result = report.main([])

            self.assertEqual(0, result)
            payload = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
            self.assertEqual("unknown", payload["verified_maturity"])
            self.assertEqual("", payload["verified_should_not_be_traded_reason"])
            self.assertEqual([], payload["maturity_reasons"])
            self.assertIn("trade_opportunity_summary", payload)
            passed_opportunities = build_markdown.call_args.args[18]
            self.assertEqual("unknown", passed_opportunities["verified_maturity"])


if __name__ == "__main__":
    unittest.main()
