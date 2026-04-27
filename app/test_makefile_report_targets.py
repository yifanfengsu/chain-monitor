import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OLD_SCRIPT_NAMES = (
    "reports/generate_afternoon_evening_state_analysis_latest.py",
    "reports/generate_overnight_trade_action_analysis_latest.py",
    "reports/generate_overnight_run_analysis_latest.py",
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

    def test_report_all_uses_only_daily_and_compare_defaults(self) -> None:
        result = self._make_dry_run("report-all")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("report-daily", result.stdout)
        self.assertIn("daily-compare", result.stdout)
        for script_name in OLD_SCRIPT_NAMES:
            self.assertNotIn(script_name, result.stdout)

    def test_report_daily_date_invokes_canonical_generator(self) -> None:
        result = self._make_dry_run("report-daily-date", "DATE=2026-04-24")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("reports/generate_daily_report_latest.py --date", result.stdout)
        self.assertIn("2026-04-24", result.stdout)

    def test_report_daily_range_invokes_canonical_range_generator(self) -> None:
        result = self._make_dry_run(
            "report-daily-range",
            "START_DATE=2026-04-22",
            "END_DATE=2026-04-24",
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("reports/generate_daily_report_latest.py --start-date", result.stdout)
        self.assertIn("2026-04-22", result.stdout)
        self.assertIn("2026-04-24", result.stdout)

    def test_report_legacy_all_is_legacy_debug_only(self) -> None:
        result = self._make_dry_run("report-legacy-all")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertTrue(
            "reports/legacy/generate_overnight_trade_action_analysis_latest.py" in result.stdout
            or "Legacy report generators removed." in result.stdout
        )


if __name__ == "__main__":
    unittest.main()
