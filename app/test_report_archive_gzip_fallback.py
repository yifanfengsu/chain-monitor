import gzip
import json
import tempfile
import unittest
from pathlib import Path

import report_data_loader
import sqlite_store


class ReportArchiveGzipFallbackTests(unittest.TestCase):
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

    def _payload(self, signal_id: str, archive_ts: int) -> dict:
        return {
            "archive_ts": archive_ts,
            "data": {
                "signal_id": signal_id,
                "signal_archive_key": signal_id,
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "archive_written_at": archive_ts,
            },
        }

    def _write_plain(self, date: str, signal_id: str, archive_ts: int = 1_710_000_000) -> Path:
        path = self.archive_dir / "signals" / f"{date}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self._payload(signal_id, archive_ts)) + "\n", encoding="utf-8")
        return path

    def _write_gzip(self, date: str, signal_id: str, archive_ts: int = 1_710_000_000) -> Path:
        path = self.archive_dir / "signals" / f"{date}.ndjson.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(self._payload(signal_id, archive_ts)) + "\n")
        return path

    def test_archive_ndjson_gz_is_read_as_fallback(self) -> None:
        self._write_gzip("2026-04-21", "sig-gzip-fallback")

        result = report_data_loader.load_signals(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual("archive", result.source)
        self.assertEqual(1, result.row_count)
        self.assertEqual("sig-gzip-fallback", result.rows[0]["signal_id"])
        self.assertEqual(1, result.compressed_archive_rows)

    def test_plain_archive_wins_over_same_day_gzip_without_duplicate(self) -> None:
        self._write_plain("2026-04-21", "sig-plain")
        self._write_gzip("2026-04-21", "sig-gzip-duplicate")

        paths = report_data_loader.archive_paths("signals", base_dir=self.archive_dir)
        result = report_data_loader.load_signals(
            db_path=self.db_path,
            archive_base_dir=self.archive_dir,
        )

        self.assertEqual(1, len(paths))
        self.assertEqual(".ndjson", paths[0].suffix)
        self.assertEqual(1, result.row_count)
        self.assertEqual("sig-plain", result.rows[0]["signal_id"])


if __name__ == "__main__":
    unittest.main()
