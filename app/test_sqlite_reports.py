import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from archive_store import ArchiveStore
import quality_reports
import sqlite_store


class SQLiteReportsTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        sqlite_store.ARCHIVE_BASE_DIR = str(self.root / "archive")
        sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_report_can_read_signal_rows_from_db(self) -> None:
        sqlite_store.write_signal(
            {
                "signal_id": "sig-report-db",
                "signal_archive_key": "sig-report-db",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "lp_alert_stage": "confirm",
                "market_context_source": "live_public",
                "archive_written_at": 1_710_000_000,
            }
        )
        rows = sqlite_store.load_signal_rows_from_db()
        self.assertEqual(1, len(rows))
        self.assertEqual("sig-report-db", rows[0]["signal_id"])

    def test_db_empty_archive_report_fallback_still_works(self) -> None:
        archive = ArchiveStore(base_dir=self.root / "archive", category_enabled={"signals": True})
        archive.write_signal(
            {
                "signal_id": "sig-archive-fallback",
                "market_context_source": "unavailable",
                "market_context_attempts": [],
            },
            archive_ts=1_710_000_010,
        )
        report = quality_reports.build_market_context_health_report(base_dir=self.root / "archive")
        self.assertEqual(1, report["signal_rows"])
        self.assertEqual(1, report["unavailable_count"])

    def test_db_archive_mismatch_is_reported_without_crash(self) -> None:
        archive = ArchiveStore(base_dir=self.root / "archive", category_enabled={"signals": True})
        archive.write_signal({"signal_id": "sig-match-1"}, archive_ts=1_710_000_020)
        sqlite_store.write_signal({"signal_id": "sig-db-only", "signal_archive_key": "sig-db-only"})
        payload = sqlite_store.report_source_summary()
        self.assertIn("db_archive_mirror_match_rate", payload)
        self.assertIn("signals", payload["db_archive_mirror_detail"])

    def test_quality_reports_db_cli_commands_run(self) -> None:
        for argv in (["--db-summary"], ["--opportunity-db-summary"]):
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = quality_reports.main(argv)
            self.assertEqual(0, code)
            self.assertIsInstance(json.loads(buffer.getvalue()), dict)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = quality_reports.main(["--db-integrity"])
        self.assertEqual(0, code)
        self.assertTrue(json.loads(buffer.getvalue()).get("ok"))


if __name__ == "__main__":
    unittest.main()
