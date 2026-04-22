import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import report_data_loader
import sqlite_store


class SQLiteMigrationModesTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_project_root = sqlite_store.PROJECT_ROOT
        self.original_modes = {
            "SQLITE_RAW_EVENTS_MODE": sqlite_store.SQLITE_RAW_EVENTS_MODE,
            "SQLITE_PARSED_EVENTS_MODE": sqlite_store.SQLITE_PARSED_EVENTS_MODE,
            "SQLITE_DELIVERY_AUDIT_MODE": sqlite_store.SQLITE_DELIVERY_AUDIT_MODE,
        }
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_PARSED_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_DELIVERY_AUDIT_MODE = "slim"
        sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        sqlite_store.PROJECT_ROOT = self.original_project_root
        for key, value in self.original_modes.items():
            setattr(sqlite_store, key, value)
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive(self, category: str, date: str, rows: list[dict], *, gzip_archive: bool = False) -> None:
        path = self.archive_dir / category / f"{date}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        content = "".join(json.dumps(row) + "\n" for row in rows)
        if gzip_archive:
            with gzip.open(f"{path}.gz", "wt", encoding="utf-8") as handle:
                handle.write(content)
        else:
            path.write_text(content, encoding="utf-8")

    def test_migration_respects_index_and_slim_modes(self) -> None:
        date = "2026-04-21"
        self._write_archive(
            "raw_events",
            date,
            [{"archive_ts": 1_710_000_000, "data": {"event_id": "raw-migrate-mode", "tx_hash": "0xraw"}}],
        )
        self._write_archive(
            "parsed_events",
            date,
            [{"archive_ts": 1_710_000_001, "data": {"event_id": "parsed-migrate-mode", "tx_hash": "0xparsed"}}],
        )
        self._write_archive(
            "delivery_audit",
            date,
            [{"archive_ts": 1_710_000_002, "data": {"audit_id": "audit-migrate-mode", "signal_id": "sig-1", "stage": "candidate"}}],
        )
        payload = sqlite_store.migrate_archive(date=date)
        self.assertEqual("index_only", payload["mode_by_category"]["raw_events"])
        self.assertEqual("index_only", payload["mode_by_category"]["parsed_events"])
        self.assertEqual("slim", payload["mode_by_category"]["delivery_audit"])
        conn = sqlite_store.get_connection()
        self.assertIsNone(conn.execute("SELECT raw_json FROM raw_events WHERE event_id='raw-migrate-mode'").fetchone()[0])
        self.assertIsNone(conn.execute("SELECT parsed_json FROM parsed_events WHERE event_id='parsed-migrate-mode'").fetchone()[0])
        self.assertIsNone(conn.execute("SELECT audit_json FROM delivery_audit WHERE audit_id='audit-migrate-mode'").fetchone()[0])

    def test_db_loader_can_read_index_only_rows(self) -> None:
        self._write_archive(
            "raw_events",
            "2026-04-20",
            [{"archive_ts": 1_710_000_100, "data": {"event_id": "raw-loader", "tx_hash": "0xloader"}}],
        )
        sqlite_store.migrate_archive(all_dates=True)
        result = report_data_loader.load_raw_events(
            db_path=self.root / "chain_monitor.sqlite",
            archive_base_dir=self.archive_dir,
        )
        self.assertEqual("sqlite", result.source)
        self.assertEqual(1, result.row_count)
        self.assertEqual("raw-loader", result.rows[0]["event_id"])

    def test_old_schema_is_migrated_with_new_archive_columns(self) -> None:
        sqlite_store.close()
        legacy_db = self.root / "legacy.sqlite"
        conn = sqlite3.connect(legacy_db)
        conn.execute(
            """
            CREATE TABLE raw_events (
                event_id TEXT PRIMARY KEY,
                tx_hash TEXT,
                block_number INTEGER,
                chain TEXT,
                address TEXT,
                pool_address TEXT,
                raw_kind TEXT,
                listener_scan_path TEXT,
                captured_at REAL,
                raw_json TEXT,
                created_at REAL,
                updated_at REAL
            )
            """
        )
        conn.commit()
        conn.close()
        new_conn = sqlite_store.init_sqlite_store(legacy_db)
        columns = {
            row[1]
            for row in new_conn.execute("PRAGMA table_info(raw_events)").fetchall()
        }
        self.assertIn("archive_path", columns)
        self.assertIn("archive_date", columns)
        self.assertIn("payload_hash", columns)


if __name__ == "__main__":
    unittest.main()
