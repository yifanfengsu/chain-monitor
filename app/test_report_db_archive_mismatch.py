import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import quality_reports
import report_data_loader
import sqlite_store


class ReportDbArchiveMismatchTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.db_path = self.root / "chain_monitor.sqlite"
        self.original_sqlite_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_loader_archive_base = report_data_loader.ARCHIVE_BASE_DIR
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        report_data_loader.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.ARCHIVE_BASE_DIR = self.original_sqlite_archive_base
        report_data_loader.ARCHIVE_BASE_DIR = self.original_loader_archive_base
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive_signal(self, signal_id: str, archive_ts: int) -> None:
        path = self.archive_dir / "signals" / "2026-04-21.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "archive_ts": archive_ts,
            "data": {
                "signal_id": signal_id,
                "signal_archive_key": signal_id,
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "archive_written_at": archive_ts,
            },
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")

    def test_db_archive_mismatch_warns_without_crashing(self) -> None:
        sqlite_store.write_signal(
            {
                "signal_id": "sig-db-only",
                "signal_archive_key": "sig-db-only",
                "asset_symbol": "ETH",
                "archive_written_at": 1_710_000_000,
            }
        )
        self._write_archive_signal("sig-archive-1", 1_710_000_001)
        self._write_archive_signal("sig-archive-2", 1_710_000_002)

        result = report_data_loader.load_signals(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual("sqlite", result.source)
        self.assertTrue(result.mismatch_info["mismatch"])
        self.assertTrue(any("db_archive_mismatch:signals" in warning for warning in result.warnings))

    def test_report_source_summary_contains_required_keys(self) -> None:
        self._write_archive_signal("sig-summary", 1_710_000_003)

        payload = report_data_loader.report_source_summary(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertIn("report_data_source", payload)
        self.assertIn("sqlite_rows_by_table", payload)
        self.assertIn("archive_rows_by_category", payload)
        self.assertIn("db_archive_mirror_match_rate", payload)
        self.assertIn("mismatch_warnings", payload)
        self.assertIn("compressed_archive_rows", payload)

    def test_quality_reports_report_source_summary_cli_runs(self) -> None:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = quality_reports.main(["--report-source-summary"])

        self.assertEqual(0, code)
        payload = json.loads(buffer.getvalue())
        self.assertIn("current_report_read_preference", payload)
        self.assertIn("fallback_mode", payload)


if __name__ == "__main__":
    unittest.main()
