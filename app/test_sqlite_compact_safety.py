import gzip
import json
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteCompactSafetyTests(unittest.TestCase):
    ARCHIVE_TS = 1_773_532_800

    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_project_root = sqlite_store.PROJECT_ROOT
        self.original_raw_mode = sqlite_store.SQLITE_RAW_EVENTS_MODE
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "full"
        self.conn = sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.SQLITE_RAW_EVENTS_MODE = self.original_raw_mode
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive(self, *, gzip_archive: bool = False) -> str:
        path = self.archive_dir / "raw_events" / "2026-03-15.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "archive_ts": self.ARCHIVE_TS,
            "data": {
                "event_id": "raw-safe",
                "tx_hash": "0xsafe",
                "captured_at": self.ARCHIVE_TS,
            },
        }
        if gzip_archive:
            with gzip.open(f"{path}.gz", "wt", encoding="utf-8") as handle:
                handle.write(json.dumps(payload) + "\n")
        else:
            path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
        return str(path)

    def _write_full_row(self) -> None:
        self.assertTrue(
            sqlite_store.write_raw_event(
                {
                    "archive_ts": self.ARCHIVE_TS,
                    "data": {
                        "event_id": "raw-safe",
                        "tx_hash": "0xsafe",
                        "captured_at": self.ARCHIVE_TS,
                    },
                }
            )
        )

    def test_compact_dry_run_does_not_modify_row(self) -> None:
        self._write_archive()
        self._write_full_row()
        before = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-safe'").fetchone()["raw_json"]
        payload = sqlite_store.compact_table("raw_events", dry_run=True)
        after = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-safe'").fetchone()["raw_json"]
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["rows_compacted"])
        self.assertEqual(before, after)
        aggregate = sqlite_store.compact(dry_run=True, table="raw_events")
        self.assertEqual(["raw_events"], aggregate["tables_compacted"])

    def test_compact_execute_requires_archive_and_payload_hash(self) -> None:
        self._write_full_row()
        payload = sqlite_store.compact_table("raw_events", dry_run=False)
        row = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-safe'").fetchone()
        self.assertTrue(payload["ok"])
        self.assertEqual(0, payload["rows_compacted"])
        self.assertEqual(1, payload["skipped_missing_archive"])
        self.assertIsNotNone(row["raw_json"])

        self._write_archive()
        self.conn.execute("UPDATE raw_events SET payload_hash=NULL WHERE event_id='raw-safe'")
        self.conn.commit()
        payload = sqlite_store.compact_table("raw_events", dry_run=False)
        row = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-safe'").fetchone()
        self.assertEqual(0, payload["rows_compacted"])
        self.assertEqual(1, payload["skipped_no_payload_hash"])
        self.assertIsNotNone(row["raw_json"])

    def test_compact_accepts_gzip_archive_reference(self) -> None:
        self._write_archive(gzip_archive=True)
        self._write_full_row()
        payload = sqlite_store.compact_table("raw_events", dry_run=False)
        row = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-safe'").fetchone()
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["rows_compacted"])
        self.assertIsNone(row["raw_json"])
        aggregate = sqlite_store.compact(dry_run=False, table="raw_events")
        self.assertEqual([], aggregate["tables_compacted"])


if __name__ == "__main__":
    unittest.main()
