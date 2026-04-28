import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OLD_SCRIPT_NAMES = (
    "reports/generate_afternoon_evening_state_analysis_latest.py",
    "reports/generate_overnight_trade_action_analysis_latest.py",
    "reports/generate_overnight_run_analysis_latest.py",
    "reports/generate_overnight_opportunity_retention_analysis_latest.py",
)
DAILY_SCRIPT = "reports/generate_daily_report_latest.py"
LEGACY_SCRIPT_NAMES = (
    "reports/legacy/generate_afternoon_evening_state_analysis_latest.py",
    "reports/legacy/generate_overnight_trade_action_analysis_latest.py",
    "reports/legacy/generate_overnight_run_analysis_latest.py",
)


class MakefileReportTargetTests(unittest.TestCase):
    def _make_dry_run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["make", "-n", *args],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

    def test_report_all_generates_daily_then_compare(self) -> None:
        result = self._make_dry_run("report-all")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("report-daily", result.stdout)
        self.assertIn(DAILY_SCRIPT, result.stdout)
        self.assertIn("daily-compare", result.stdout)
        for script_name in OLD_SCRIPT_NAMES:
            self.assertNotIn(script_name, result.stdout)

    def test_daily_compare_uses_canonical_compare_generator_only(self) -> None:
        result = self._make_dry_run("daily-compare", "DATE=2026-04-24")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("reports/generate_daily_compare_report.py --date", result.stdout)
        self.assertNotIn(DAILY_SCRIPT, result.stdout)
        for script_name in OLD_SCRIPT_NAMES:
            self.assertNotIn(script_name, result.stdout)

    def test_report_daily_date_calls_canonical_generator(self) -> None:
        result = self._make_dry_run("report-daily-date", "DATE=2026-04-24")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn(DAILY_SCRIPT, result.stdout)
        self.assertIn("--date \"2026-04-24\"", result.stdout)
        self.assertIn("2026-04-24", result.stdout)

    def test_report_daily_range_calls_canonical_generator(self) -> None:
        result = self._make_dry_run(
            "report-daily-range",
            "START_DATE=2026-04-22",
            "END_DATE=2026-04-24",
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn(DAILY_SCRIPT, result.stdout)
        self.assertIn("--start-date \"2026-04-22\"", result.stdout)
        self.assertIn("--end-date \"2026-04-24\"", result.stdout)

    def test_report_legacy_all_is_legacy_debug_only(self) -> None:
        result = self._make_dry_run("report-legacy-all")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("reports/legacy", result.stdout)
        for script_name in LEGACY_SCRIPT_NAMES:
            self.assertIn(script_name, result.stdout)
        for script_name in OLD_SCRIPT_NAMES:
            self.assertNotIn(script_name, result.stdout)

    def test_help_marks_legacy_reports_as_debug_only(self) -> None:
        result = subprocess.run(
            ["make", "help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("make report-legacy-all", result.stdout)
        self.assertIn("Legacy/debug only; runs reports/legacy only", result.stdout)


if __name__ == "__main__":
    unittest.main()
