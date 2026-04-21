import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import report_data_loader
import sqlite_store


class ReportDbFirstLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.db_path = self.root / "chain_monitor.sqlite"
        sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive_signal(self, signal_id: str, *, archive_ts: int = 1_710_000_000) -> None:
        path = self.archive_dir / "signals" / "2026-04-21.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "archive_ts": archive_ts,
            "data": {
                "signal_id": signal_id,
                "signal_archive_key": signal_id,
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "lp_alert_stage": "confirm",
                "archive_written_at": archive_ts,
            },
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

    def test_db_has_data_uses_sqlite(self) -> None:
        sqlite_store.write_signal(
            {
                "signal_id": "sig-db-first",
                "signal_archive_key": "sig-db-first",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "lp_alert_stage": "confirm",
                "archive_written_at": 1_710_000_000,
            }
        )
        self._write_archive_signal("sig-archive-sidecar", archive_ts=1_710_000_001)

        result = report_data_loader.load_signals(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual("sqlite", result.source)
        self.assertEqual(1, result.row_count)
        self.assertFalse(result.fallback_used)
        self.assertEqual("sig-db-first", result.rows[0]["signal_id"])

    def test_db_empty_falls_back_to_archive(self) -> None:
        self._write_archive_signal("sig-archive-fallback")

        result = report_data_loader.load_signals(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual("archive", result.source)
        self.assertTrue(result.fallback_used)
        self.assertEqual(1, result.row_count)
        self.assertEqual("sig-archive-fallback", result.rows[0]["signal_id"])

    def test_db_missing_table_falls_back_to_archive(self) -> None:
        missing_table_db = self.root / "missing-table.sqlite"
        sqlite3.connect(missing_table_db).close()
        self._write_archive_signal("sig-missing-table-fallback")

        result = report_data_loader.load_signals(
            db_path=missing_table_db,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual("archive", result.source)
        self.assertTrue(result.fallback_used)
        self.assertIn("db_table_missing", result.warnings)

    def test_db_read_exception_falls_back_to_archive(self) -> None:
        self._write_archive_signal("sig-read-error-fallback")
        original = report_data_loader._load_db_rows

        def fail_loader(*args, **kwargs):
            return [], "db_read_failed:boom"

        try:
            report_data_loader._load_db_rows = fail_loader
            result = report_data_loader.load_signals(
                db_path=self.db_path,
                archive_base_dir=self.archive_dir,
            )
        finally:
            report_data_loader._load_db_rows = original

        self.assertEqual("archive", result.source)
        self.assertTrue(result.fallback_used)
        self.assertIn("db_read_failed:boom", result.warnings)


if __name__ == "__main__":
    unittest.main()
