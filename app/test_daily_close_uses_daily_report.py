import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OLD_DAILY_ENTRYPOINTS = (
    "report-state",
    "report-overnight",
    "report-run",
    "reports/generate_afternoon_evening_state_analysis_latest.py",
    "reports/generate_overnight_trade_action_analysis_latest.py",
    "reports/generate_overnight_run_analysis_latest.py",
)


class DailyCloseUsesDailyReportTests(unittest.TestCase):
    def test_daily_close_dry_run_uses_canonical_daily_report(self) -> None:
        result = subprocess.run(
            ["make", "-n", "daily-close", "DATE=2026-04-24"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("report-daily-date DATE=\"2026-04-24\"", result.stdout)
        self.assertIn("daily-compare DATE=\"2026-04-24\"", result.stdout)
        self.assertIn("--migrate-archive --date \"2026-04-24\"", result.stdout)
        self.assertIn("--checkpoint", result.stdout)
        for entrypoint in OLD_DAILY_ENTRYPOINTS:
            self.assertNotIn(entrypoint, result.stdout)


if __name__ == "__main__":
    unittest.main()
