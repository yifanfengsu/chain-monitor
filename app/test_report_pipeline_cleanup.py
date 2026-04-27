import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.MD"
MAKEFILE = ROOT / "Makefile"
REPORTS = ROOT / "reports"


class ReportPipelineCleanupTests(unittest.TestCase):
    def test_legacy_generators_are_not_top_level_default_scripts(self) -> None:
        moved_scripts = (
            "generate_afternoon_evening_state_analysis_latest.py",
            "generate_overnight_trade_action_analysis_latest.py",
            "generate_overnight_run_analysis_latest.py",
        )
        for script_name in moved_scripts:
            self.assertFalse((REPORTS / script_name).exists(), script_name)
            legacy_path = REPORTS / "legacy" / script_name
            if legacy_path.exists():
                header = legacy_path.read_text(encoding="utf-8")[:240]
                self.assertIn("Deprecated legacy report generator.", header)
                self.assertIn("Daily workflow uses reports/generate_daily_report_latest.py.", header)

    def test_makefile_default_targets_do_not_reference_old_script_paths(self) -> None:
        makefile = MAKEFILE.read_text(encoding="utf-8")
        report_all = makefile.split("\nreport-all:", 1)[1].split("\nreport-clean-dry-run:", 1)[0]
        daily_close = makefile.split("\ndaily-close:", 1)[1].split("\ndaily-close-strict:", 1)[0]

        for target_body in (report_all, daily_close):
            self.assertIn("daily-compare", target_body)
            self.assertNotIn("generate_afternoon_evening_state_analysis_latest.py", target_body)
            self.assertNotIn("generate_overnight_trade_action_analysis_latest.py", target_body)
            self.assertNotIn("generate_overnight_run_analysis_latest.py", target_body)

    def test_readme_no_longer_recommends_legacy_trio_as_daily_reports(self) -> None:
        readme = README.read_text(encoding="utf-8")

        self.assertIn("日常只看一份 canonical daily report", readme)
        self.assertIn("旧三件套已退役", readme)
        self.assertIn("默认不会 fallback 到旧三件套 summary", readme)
        self.assertNotIn("如果 canonical daily report 缺失，才 fallback 到旧三件套 summary", readme)


if __name__ == "__main__":
    unittest.main()
