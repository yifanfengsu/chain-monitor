import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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
        payloads = {}
        for argv in (["--db-size-breakdown"], ["--db-value-audit"], ["--db-retention-recommendation"]):
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = quality_reports.main(argv)
            self.assertEqual(0, code)
            payload = json.loads(buffer.getvalue())
            self.assertIsInstance(payload, dict)
            payloads[argv[0]] = payload
        size_payload = payloads["--db-size-breakdown"]
        self.assertIn("db_size_mb", size_payload)
        self.assertIn("wal_size_mb", size_payload)
        signals = next(item for item in size_payload["tables"] if item["table"] == "signals")
        self.assertIn("table_size_mb", signals)
        self.assertIn("json_payload_size_mb", signals)
        value_audit = payloads["--db-value-audit"]
        self.assertIn("estimated_compact_savings_mb", value_audit)
        self.assertIn("long_term_slim_tables", value_audit)
        retention = payloads["--db-retention-recommendation"]
        self.assertIn("market_context_attempts", retention["recommended_modes"])
        self.assertIn("recommended_actions", retention)
        reports_dir = self.root / "reports"
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.json").exists())
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.csv").exists())
        self.assertTrue((reports_dir / "sqlite_data_value_audit_latest.md").exists())

    def test_fast_db_integrity_skips_archive_mirror_counts(self) -> None:
        with mock.patch.object(sqlite_store, "archive_row_counts", side_effect=AssertionError("archive scan called")):
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = quality_reports.main(["--db-integrity", "--fast"])

        self.assertEqual(0, code)
        payload = json.loads(buffer.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual("skipped_fast_mode", payload["pragma_integrity_check"])
        self.assertEqual([], payload["db_archive_mismatch"])
        self.assertEqual("fast_mode", payload["archive_mirror"]["skipped"])


if __name__ == "__main__":
    unittest.main()
