import subprocess
import unittest
from pathlib import Path
from unittest import mock

import reports.generate_daily_report_latest as report


ROOT = Path(__file__).resolve().parents[1]


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
        results = {
            name: _load_result()
            for name in (
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
            )
        }
        results["signals"] = _load_result([signal])
        rows = {name: result.rows for name, result in results.items()}

        with mock.patch.object(report, "_load_window", return_value=(rows, results)), mock.patch.object(
            report.report_data_loader,
            "sqlite_health",
            return_value={"missing_tables": []},
        ):
            summary = report.build_daily_report("2026-04-27")

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
            "limitations",
        ):
            self.assertIn(key, summary)
        self.assertEqual("daily_canonical", summary["report_type"])
        self.assertEqual("2026-04-27", summary["logical_date"])
        self.assertIn("CANDIDATE is not a trade signal", " ".join(summary["limitations"]))


if __name__ == "__main__":
    unittest.main()
