import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ReportCleanCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_report = ROOT / "reports" / "daily" / "daily_report_test_cleanup.json"
        self.temp_report.parent.mkdir(parents=True, exist_ok=True)
        self.temp_report.write_text("{}", encoding="utf-8")

    def tearDown(self) -> None:
        if self.temp_report.exists():
            self.temp_report.unlink()

    def test_clean_dry_run_does_not_delete_files(self) -> None:
        result = subprocess.run(
            ["make", "report-clean-dry-run"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode)
        self.assertIn("daily_report_test_cleanup.json", result.stdout)
        self.assertTrue(self.temp_report.exists())

    def test_clean_generated_requires_confirm(self) -> None:
        result = subprocess.run(
            ["make", "report-clean-generated"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("CONFIRM=YES", result.stdout)
        self.assertTrue(self.temp_report.exists())

    def test_report_daily_range_target_invokes_range_generator(self) -> None:
        result = subprocess.run(
            ["make", "-n", "report-daily-range", "START_DATE=2026-04-22", "END_DATE=2026-04-24"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode)
        self.assertIn("generate_daily_report_latest.py --start-date", result.stdout)
        self.assertIn("2026-04-22", result.stdout)
        self.assertIn("2026-04-24", result.stdout)

    def test_report_all_no_longer_calls_afternoon_state(self) -> None:
        result = subprocess.run(
            ["make", "-n", "report-all"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode)
        self.assertIn("report-daily", result.stdout)
        self.assertIn("daily-compare", result.stdout)
        self.assertNotIn("generate_afternoon_evening_state_analysis_latest.py", result.stdout)


if __name__ == "__main__":
    unittest.main()
