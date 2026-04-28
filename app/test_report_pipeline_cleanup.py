import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.MD"
MAKEFILE = ROOT / "Makefile"
REPORTS = ROOT / "reports"
LEGACY_SCRIPT_NAMES = (
    "generate_afternoon_evening_state_analysis_latest.py",
    "generate_overnight_trade_action_analysis_latest.py",
    "generate_overnight_run_analysis_latest.py",
)
RETIRED_TOP_LEVEL_SCRIPT_NAMES = (
    *LEGACY_SCRIPT_NAMES,
    "generate_overnight_opportunity_retention_analysis_latest.py",
)
CANONICAL_DAILY_SCRIPT = REPORTS / "generate_daily_report_latest.py"


class ReportPipelineCleanupTests(unittest.TestCase):
    def test_legacy_generators_are_not_top_level_default_scripts(self) -> None:
        for script_name in LEGACY_SCRIPT_NAMES:
            top_level_path = REPORTS / script_name
            if top_level_path.exists():
                text = top_level_path.read_text(encoding="utf-8")
                legacy_relpath = f"reports/legacy/{script_name}"
                self.assertLessEqual(len(text.splitlines()), 30, script_name)
                self.assertIn("Deprecated", text, script_name)
                self.assertIn(legacy_relpath, text, script_name)
                self.assertNotIn("compute_trade", text, script_name)
                self.assertNotIn("compute_market_context", text, script_name)
            legacy_path = REPORTS / "legacy" / script_name
            if legacy_path.exists():
                header = legacy_path.read_text(encoding="utf-8")[:240]
                self.assertIn("Deprecated legacy report generator.", header)
                self.assertIn("Daily workflow uses make daily-compare", header)

    def test_top_level_retired_generators_are_not_tracked(self) -> None:
        result = subprocess.run(
            ["git", "ls-files", *(f"reports/{script_name}" for script_name in RETIRED_TOP_LEVEL_SCRIPT_NAMES)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("", result.stdout.strip())

    def test_canonical_daily_generator_is_tracked_code(self) -> None:
        self.assertTrue(CANONICAL_DAILY_SCRIPT.exists())
        result = subprocess.run(
            ["git", "check-ignore", "-q", "reports/generate_daily_report_latest.py"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(0, result.returncode)

    def test_makefile_default_targets_do_not_reference_old_script_paths(self) -> None:
        makefile = MAKEFILE.read_text(encoding="utf-8")
        report_all = makefile.split("\nreport-all:", 1)[1].split("\nreport-clean-dry-run:", 1)[0]
        daily_close = makefile.split("\ndaily-close:", 1)[1].split("\ndaily-close-strict:", 1)[0]

        self.assertIn("report-daily", report_all)
        self.assertIn("report-daily-date", daily_close)
        for target_body in (report_all, daily_close):
            self.assertIn("daily-compare", target_body)
            self.assertNotIn("generate_afternoon_evening_state_analysis_latest.py", target_body)
            self.assertNotIn("generate_overnight_trade_action_analysis_latest.py", target_body)
            self.assertNotIn("generate_overnight_run_analysis_latest.py", target_body)

        report_daily = makefile.split("\nreport-daily:", 1)[1].split("\nreport-daily-date:", 1)[0]
        report_daily_date = makefile.split("\nreport-daily-date:", 1)[1].split("\nreport-daily-range:", 1)[0]
        self.assertIn("$(REPORT_DAILY_SCRIPT)", report_daily)
        self.assertIn("$(REPORT_DAILY_SCRIPT)", report_daily_date)
        self.assertIn("REPORT_DAILY_SCRIPT = $(REPORTS)/generate_daily_report_latest.py", makefile)

    def test_readme_no_longer_recommends_retired_generators_as_daily_reports(self) -> None:
        readme = README.read_text(encoding="utf-8")

        self.assertIn("canonical daily report 是日常唯一主报告", readme)
        self.assertIn("`reports/generate_daily_report_latest.py`", readme)
        self.assertIn("daily-close` 会先做 archive -> SQLite migrate", readme)
        self.assertIn("旧三件套已退役", readme)
        self.assertIn("reports/legacy/` 仅作为 legacy/debug", readme)
        self.assertIn("`reports/` 根目录旧三件套脚本不再使用", readme)
        self.assertIn("generated reports 默认不进 Git", readme)
        self.assertIn("git rm --cached", readme)
        self.assertNotIn("`reports/generate_daily_report_latest.py` 已退役", readme)
        self.assertNotIn("日报生成入口已退役", readme)
        self.assertNotIn("不执行任何 generator", readme)
        self.assertIn("默认不会 fallback 到旧三件套 summary", readme)
        self.assertNotIn("如果 canonical daily report 缺失，才 fallback 到旧三件套 summary", readme)

    def test_opportunity_retention_report_is_legacy_only(self) -> None:
        self.assertFalse((REPORTS / "generate_overnight_opportunity_retention_analysis_latest.py").exists())
        legacy_path = REPORTS / "legacy" / "generate_overnight_opportunity_retention_analysis_latest.py"
        self.assertTrue(legacy_path.exists())
        header = legacy_path.read_text(encoding="utf-8")[:240]
        self.assertIn("Deprecated legacy report generator.", header)
        self.assertIn("Daily workflow uses make daily-compare", header)

    def test_gitignore_does_not_ignore_canonical_daily_generator(self) -> None:
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

        self.assertNotIn("reports/generate_daily_report_latest.py", gitignore)


if __name__ == "__main__":
    unittest.main()
