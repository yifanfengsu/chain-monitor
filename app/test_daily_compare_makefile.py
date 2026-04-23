import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DailyCompareDocsAndMakefileTests(unittest.TestCase):
    def test_readme_mentions_daily_compare(self) -> None:
        readme = (ROOT / "README.MD").read_text(encoding="utf-8")
        self.assertIn("每日对比报告", readme)
        self.assertIn("make daily-compare", readme)
        self.assertIn("make daily-compare DATE=YYYY-MM-DD", readme)
        self.assertIn("reports/daily_compare/", readme)
        self.assertIn("compare 报告不是交易建议", readme)

    def test_makefile_help_mentions_daily_compare(self) -> None:
        result = subprocess.run(
            ["make", "help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode)
        self.assertIn("daily-compare", result.stdout)
        self.assertIn("daily-compare-strict", result.stdout)


if __name__ == "__main__":
    unittest.main()
