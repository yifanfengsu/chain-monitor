import json
import tempfile
import unittest
from pathlib import Path

from reports.generate_daily_compare_report import discover_summary_inventory, generate_daily_compare, select_compare_dates

from app.test_daily_compare_report import _sample_values, _seed_day


class DailyCompareSourceSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.reports_dir = self.root / "reports"
        self.output_dir = self.reports_dir / "daily_compare"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_auto_selects_latest_vs_previous_available_date(self) -> None:
        _seed_day(self.reports_dir, "2026-04-20", _sample_values(raw_events=100, candidate_count=1, candidate_ft=0.4, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.5, blocker_false=0.2, outcome_rate=0.5, market_success=0.6, messages_after=3, high_value_suppressed=1))
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=120, candidate_count=2, candidate_ft=0.6, candidate_adv=0.1, candidate_completion=1.0, blocker_saved=0.7, blocker_false=0.1, outcome_rate=0.7, market_success=0.8, messages_after=2, high_value_suppressed=0))

        inventory = discover_summary_inventory(self.reports_dir)
        selection = select_compare_dates(inventory)

        self.assertEqual("2026-04-22", selection["today_date"])
        self.assertEqual("2026-04-20", selection["previous_date"])
        self.assertEqual("previous_available_day", selection["compare_basis"])

    def test_requested_date_uses_previous_available_date(self) -> None:
        _seed_day(self.reports_dir, "2026-04-20", _sample_values(raw_events=100, candidate_count=1, candidate_ft=0.4, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.5, blocker_false=0.2, outcome_rate=0.5, market_success=0.6, messages_after=3, high_value_suppressed=1))
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=120, candidate_count=2, candidate_ft=0.6, candidate_adv=0.1, candidate_completion=1.0, blocker_saved=0.7, blocker_false=0.1, outcome_rate=0.7, market_success=0.8, messages_after=2, high_value_suppressed=0))
        _seed_day(self.reports_dir, "2026-04-23", _sample_values(raw_events=130, candidate_count=2, candidate_ft=0.5, candidate_adv=0.1, candidate_completion=1.0, blocker_saved=0.6, blocker_false=0.1, outcome_rate=0.8, market_success=0.9, messages_after=1, high_value_suppressed=0))

        inventory = discover_summary_inventory(self.reports_dir)
        selection = select_compare_dates(inventory, requested_date="2026-04-22")

        self.assertEqual("2026-04-22", selection["today_date"])
        self.assertEqual("2026-04-20", selection["previous_date"])
        self.assertEqual("previous_available_day", selection["compare_basis"])

    def test_matching_latest_json_is_used_when_dated_file_missing(self) -> None:
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=100, candidate_count=1, candidate_ft=0.4, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.5, blocker_false=0.2, outcome_rate=0.5, market_success=0.6, messages_after=3, high_value_suppressed=1))
        _seed_day(self.reports_dir, "2026-04-23", _sample_values(raw_events=140, candidate_count=3, candidate_ft=0.6, candidate_adv=0.1, candidate_completion=1.0, blocker_saved=0.7, blocker_false=0.05, outcome_rate=0.8, market_success=0.9, messages_after=1, high_value_suppressed=0), latest=True)

        exit_code, payload, _ = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )

        self.assertEqual(0, exit_code)
        self.assertEqual("2026-04-23", payload["today_date"])
        self.assertEqual("2026-04-22", payload["previous_date"])
        self.assertIn("strict_failure_reason", payload)
        self.assertIn("rebuild_summary", payload)
        self.assertIn("rebuild_warnings", payload)
        self.assertFalse(payload["rebuild_summary"]["attempted"])
        today_sources = payload["source_files"]["today"]
        self.assertEqual("latest_summary_json", today_sources["afternoon_evening_state"]["source_kind"])
        self.assertEqual("latest_summary_json", today_sources["overnight_trade_action"]["source_kind"])
        self.assertEqual("latest_summary_json", today_sources["overnight_run"]["source_kind"])


if __name__ == "__main__":
    unittest.main()
