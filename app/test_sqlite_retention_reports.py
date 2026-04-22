import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import quality_reports
import sqlite_store


class SQLiteRetentionReportsTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_project_root = sqlite_store.PROJECT_ROOT
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")
        sqlite_store.write_signal(
            {
                "signal_id": "sig-report",
                "signal_archive_key": "sig-report",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "lp_alert_stage": "confirm",
                "archive_written_at": 1_710_000_000,
            }
        )

    def tearDown(self) -> None:
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_quality_report_cli_outputs_db_retention_payloads(self) -> None:
        for argv in (["--db-size-breakdown"], ["--db-value-audit"], ["--db-retention-recommendation"]):
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = quality_reports.main(argv)
            self.assertEqual(0, code)
            payload = json.loads(buffer.getvalue())
            self.assertIsInstance(payload, dict)
        reports_dir = self.root / "reports"
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.json").exists())
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.csv").exists())
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.md").exists())


if __name__ == "__main__":
    unittest.main()
