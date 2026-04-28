import subprocess
import unittest
from pathlib import Path
from unittest import mock

import reports.generate_daily_report_latest as report


ROOT = Path(__file__).resolve().parents[1]
LOADER_NAMES = (
    "raw_events",
    "parsed_events",
    "signals",
    "delivery_audit",
    "case_followups",
    "asset_cases",
    "asset_market_states",
    "trade_opportunities",
    "outcomes",
    "quality_stats",
    "telegram_deliveries",
    "market_context_attempts",
    "prealert_lifecycle",
)


def _load_result(rows=None, source="archive"):
    rows = list(rows or [])
    return report.report_data_loader.LoadResult(
        rows=rows,
        source=source,
        row_count=len(rows),
        warnings=[],
        fallback_used=False,
        mismatch_info={},
    )


def _summary_for(db_summary=None, **datasets):
    results = {name: _load_result(datasets.get(name, [])) for name in LOADER_NAMES}
    rows = {name: result.rows for name, result in results.items()}
    with mock.patch.object(report, "_load_window", return_value=(rows, results)), mock.patch.object(
        report.report_data_loader,
        "sqlite_health",
        return_value={"missing_tables": []},
    ), mock.patch.object(
        report,
        "_read_opportunity_db_summary",
        return_value=db_summary or {"available": False, "reason": "unit_test"},
    ):
        return report.build_daily_report("2026-04-27")


class DailyCanonicalReportTests(unittest.TestCase):
    def test_canonical_generator_exists_and_is_not_gitignored(self) -> None:
        self.assertTrue((ROOT / "reports" / "generate_daily_report_latest.py").exists())
        result = subprocess.run(
            ["git", "check-ignore", "-q", "reports/generate_daily_report_latest.py"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(0, result.returncode)

    def test_beijing_logical_window_is_natural_day(self) -> None:
        window = report._logical_window("2026-04-27")

        self.assertEqual("2026-04-26 16:00:00 UTC", window["logical_window_start_utc"])
        self.assertEqual("2026-04-27 15:59:59 UTC", window["logical_window_end_utc"])
        self.assertEqual(24.0, window["wall_clock_duration_hours"])

    def test_summary_contains_required_canonical_fields(self) -> None:
        window = report._logical_window("2026-04-27")
        signal = {
            "archive_ts": window["start_ts"] + 60,
            "signal_id": "sig-1",
            "lp_event": True,
            "telegram_should_send": False,
            "telegram_suppression_reason": "unit_test",
            "trade_opportunity_status_at_creation": "CANDIDATE",
            "trade_opportunity_score": 0.7,
            "market_context_source": "unavailable",
            "pair_label": "ETH/USDT",
            "asset_symbol": "ETH",
        }
        summary = _summary_for(signals=[signal])

        for key in (
            "logical_date",
            "logical_window_start_utc",
            "logical_window_end_utc",
            "segment_summary",
            "active_duration_hours",
            "wall_clock_duration_hours",
            "data_source_summary",
            "market_context_health",
            "trade_opportunity_summary",
            "candidate_verified_summary",
            "blocker_summary",
            "outcome_summary",
            "telegram_suppression_summary",
            "major_coverage_summary",
            "trade_action_summary",
            "prealert_lifecycle_summary",
            "limitations",
        ):
            self.assertIn(key, summary)
        self.assertEqual("daily_canonical", summary["report_type"])
        self.assertEqual("2026-04-27", summary["logical_date"])
        self.assertIn("CANDIDATE is not a trade signal", " ".join(summary["limitations"]))

    def test_daily_report_does_not_hardcode_verified_maturity_immature(self) -> None:
        signal = {
            "archive_ts": report._logical_window("2026-04-27")["start_ts"] + 60,
            "signal_id": "candidate-1",
            "trade_opportunity_status_at_creation": "CANDIDATE",
        }

        summary = _summary_for(signals=[signal])

        trade_summary = summary["trade_opportunity_summary"]
        self.assertEqual("unknown", trade_summary["verified_maturity"])
        self.assertIn("no_verified_opportunity", trade_summary["maturity_reasons"])
        self.assertNotIn("verified_maturity=immature", " ".join(summary["limitations"]))
        self.assertNotIn("verified_maturity=immature", " ".join(summary["key_findings"]))

    def test_missing_maturity_data_uses_unknown_and_insufficient_data(self) -> None:
        summary = _summary_for()

        trade_summary = summary["trade_opportunity_summary"]
        self.assertEqual("unknown", trade_summary["verified_maturity"])
        self.assertEqual(["insufficient_data"], trade_summary["maturity_reasons"])
        self.assertEqual("insufficient_data", trade_summary["verified_should_not_be_traded_reason"])
        self.assertIn("verified_maturity_unknown:insufficient_data", summary["limitations"])

    def test_zero_verified_rows_do_not_inherit_db_immature_maturity(self) -> None:
        db_summary = {
            "available": True,
            "verified_maturity": "immature",
            "maturity_reasons": ["outcome_completion_rate_below_0.70"],
            "verified_should_not_be_traded_reason": "outcome_completion_rate_below_0.70",
        }

        summary = _summary_for(
            db_summary=db_summary,
            signals=[
                {
                    "archive_ts": report._logical_window("2026-04-27")["start_ts"] + 60,
                    "signal_id": "candidate-1",
                    "trade_opportunity_status_at_creation": "CANDIDATE",
                }
            ],
        )

        trade_summary = summary["trade_opportunity_summary"]
        self.assertEqual("unknown", trade_summary["verified_maturity"])
        self.assertIn("no_verified_opportunity", trade_summary["maturity_reasons"])
        self.assertNotIn("verified_maturity=immature", " ".join(summary["limitations"]))

    def test_immature_limitation_is_written_only_when_real_maturity_is_immature(self) -> None:
        opportunity = {
            "trade_opportunity_id": "opp-1",
            "status": "VERIFIED",
            "score": 0.84,
            "verified_maturity": "immature",
            "maturity_reasons": ["outcome_completion_rate_below_0.70"],
            "verified_should_not_be_traded_reason": "outcome_completion_rate_below_0.70",
        }

        summary = _summary_for(trade_opportunities=[opportunity])

        self.assertEqual("immature", summary["trade_opportunity_summary"]["verified_maturity"])
        self.assertIn(
            "verified_maturity=immature; VERIFIED must not be treated as mature trade signal",
            summary["limitations"],
        )

    def test_trade_action_summary_counts_distribution_from_signal_and_delivery_rows(self) -> None:
        signals = [
            {"signal_id": "sig-1", "archive_ts": 1, "trade_action_key": "WAIT_CONFIRMATION"},
            {"signal_id": "sig-2", "archive_ts": 2, "final_trading_output_label": "DO_NOT_CHASE_LONG"},
        ]
        delivery = [{"audit_id": "audit-1", "archive_ts": 3, "action_label": "WAIT_CONFIRMATION"}]

        summary = _summary_for(signals=signals, delivery_audit=delivery)

        action_summary = summary["trade_action_summary"]
        self.assertTrue(action_summary["available"])
        self.assertEqual(
            {"DO_NOT_CHASE_LONG": 1, "WAIT_CONFIRMATION": 2},
            action_summary["trade_action_distribution"],
        )
        self.assertEqual({"WAIT_CONFIRMATION": 2, "DO_NOT_CHASE_LONG": 1}, action_summary["trade_action_distribution_top"])

    def test_trade_action_summary_is_unavailable_when_fields_are_missing(self) -> None:
        summary = _summary_for(signals=[{"signal_id": "sig-1", "archive_ts": 1}])

        action_summary = summary["trade_action_summary"]
        self.assertFalse(action_summary["available"])
        self.assertEqual({}, action_summary["trade_action_distribution"])
        self.assertEqual("no_trade_action_fields_found", action_summary["reason"])

    def test_prealert_lifecycle_summary_uses_prealert_lifecycle_rows(self) -> None:
        prealert_rows = [
            {
                "prealert_id": "pre-1",
                "candidate": 1,
                "gate_passed": 1,
                "delivered": 1,
                "upgraded_to_confirm": 1,
                "prealert_to_confirm_sec": 30,
                "prealert_lifecycle_state": "delivered",
            },
            {
                "prealert_id": "pre-2",
                "candidate": 1,
                "active": 1,
                "expired": 1,
                "prealert_to_confirm_sec": 90,
                "prealert_lifecycle_state": "expired",
            },
        ]

        summary = _summary_for(prealert_lifecycle=prealert_rows)

        prealert = summary["prealert_lifecycle_summary"]
        self.assertTrue(prealert["available"])
        self.assertEqual("prealert_lifecycle", prealert["source"])
        self.assertEqual(2, prealert["prealert_candidate_count"])
        self.assertEqual(1, prealert["prealert_gate_passed_count"])
        self.assertEqual(1, prealert["prealert_delivered_count"])
        self.assertEqual(1, prealert["prealert_upgraded_to_confirm_count"])
        self.assertEqual(1, prealert["prealert_expired_count"])
        self.assertEqual(60.0, prealert["median_prealert_to_confirm_sec"])

    def test_missing_prealert_lifecycle_adds_limitation(self) -> None:
        summary = _summary_for(signals=[{"signal_id": "sig-1", "archive_ts": 1}])

        prealert = summary["prealert_lifecycle_summary"]
        self.assertFalse(prealert["available"])
        self.assertEqual("missing", prealert["source"])
        self.assertIn("prealert_lifecycle_missing", summary["limitations"])

    def test_markdown_and_csv_include_hardened_metric_fields(self) -> None:
        summary = _summary_for(
            signals=[
                {
                    "signal_id": "sig-1",
                    "archive_ts": 1,
                    "trade_action_key": "WAIT_CONFIRMATION",
                    "lp_alert_stage": "prealert",
                    "lp_prealert_candidate": True,
                    "sent_to_telegram": True,
                }
            ]
        )

        markdown = report._markdown(summary)
        csv_text = report._csv_text(summary)

        self.assertIn("verified_maturity", markdown)
        self.assertIn("trade_action_distribution_top", markdown)
        self.assertIn("prealert_lifecycle_available", markdown)
        self.assertIn("verified_maturity", csv_text)
        self.assertIn("trade_action_distribution_top", csv_text)
        self.assertIn("prealert_candidate_count", csv_text)


if __name__ == "__main__":
    unittest.main()
