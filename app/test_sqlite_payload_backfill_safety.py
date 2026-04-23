import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLitePayloadBackfillSafetyTests(unittest.TestCase):
    ARCHIVE_TS = 1_776_787_492
    ARCHIVE_DATE = "2026-04-22"

    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.original_project_root = sqlite_store.PROJECT_ROOT
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_modes = {
            "SQLITE_RAW_EVENTS_MODE": sqlite_store.SQLITE_RAW_EVENTS_MODE,
            "SQLITE_TELEGRAM_DELIVERY_MODE": sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE,
        }
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE = "slim"
        self.conn = sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        for key, value in self.original_modes.items():
            setattr(sqlite_store, key, value)
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive(self, category: str, data: dict) -> str:
        path = self.archive_dir / category / f"{self.ARCHIVE_DATE}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"archive_ts": self.ARCHIVE_TS, "data": data}, ensure_ascii=False) + "\n")
        return str(path.resolve())

    def test_archive_missing_rows_are_not_backfilled(self) -> None:
        data = {
            "event_id": "raw-missing-archive",
            "tx_hash": "0xraw-missing-archive",
            "block_number": 999,
            "captured_at": self.ARCHIVE_TS,
        }
        self.conn.execute(
            """
            INSERT INTO raw_events(event_id, tx_hash, block_number, chain, captured_at, raw_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["event_id"],
                data["tx_hash"],
                data["block_number"],
                "ethereum",
                self.ARCHIVE_TS,
                json.dumps(data, ensure_ascii=False),
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="raw_events")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash FROM raw_events WHERE event_id=?",
            (data["event_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_archive_missing"])
        self.assertEqual(0, payload["rows_backfilled"])
        self.assertIsNone(row["archive_path"])
        self.assertIsNone(row["archive_date"])
        self.assertIsNone(row["payload_hash"])

    def test_archive_hash_mismatch_rows_are_not_backfilled(self) -> None:
        db_data = {
            "event_id": "raw-hash-mismatch",
            "tx_hash": "0xraw-hash-mismatch",
            "block_number": 777,
            "captured_at": self.ARCHIVE_TS,
            "marker": "db",
        }
        archive_data = dict(db_data)
        archive_data["marker"] = "archive"
        self._write_archive("raw_events", archive_data)
        self.conn.execute(
            """
            INSERT INTO raw_events(event_id, tx_hash, block_number, chain, captured_at, raw_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                db_data["event_id"],
                db_data["tx_hash"],
                db_data["block_number"],
                "ethereum",
                self.ARCHIVE_TS,
                json.dumps(db_data, ensure_ascii=False),
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="raw_events")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash FROM raw_events WHERE event_id=?",
            (db_data["event_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_hash_mismatch"])
        self.assertEqual(0, payload["rows_backfilled"])
        self.assertIsNone(row["archive_path"])
        self.assertIsNone(row["archive_date"])
        self.assertIsNone(row["payload_hash"])

    def test_telegram_backfill_is_conservative_without_confirmable_archive(self) -> None:
        message = {
            "telegram_delivery_id": "tg-unresolved",
            "signal_id": "sig-unresolved",
            "headline": "无法确认 archive",
            "sent_at": self.ARCHIVE_TS,
            "archive_written_at": self.ARCHIVE_TS,
        }
        self._write_archive(
            "signals",
            {
                "telegram_delivery_id": "tg-other",
                "signal_id": "sig-other",
                "headline": "别的消息",
                "sent_at": self.ARCHIVE_TS,
                "archive_written_at": self.ARCHIVE_TS,
            },
        )
        self.conn.execute(
            """
            INSERT INTO telegram_deliveries(telegram_delivery_id, created_at, message_json)
            VALUES(?, ?, ?)
            """,
            (
                message["telegram_delivery_id"],
                self.ARCHIVE_TS,
                json.dumps(message, ensure_ascii=False),
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="telegram_deliveries")
        row = self.conn.execute(
            "SELECT archive_path, payload_hash FROM telegram_deliveries WHERE telegram_delivery_id=?",
            (message["telegram_delivery_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_unresolved"])
        self.assertEqual(0, payload["rows_backfilled"])
        self.assertIsNone(row["archive_path"])
        self.assertIsNone(row["payload_hash"])

    def test_makefile_execute_targets_require_confirm(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            ["make", "db-backfill-payload-execute"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        combined = f"{result.stdout}\n{result.stderr}"
        self.assertEqual(2, result.returncode)
        self.assertIn("Refusing to backfill payload metadata", combined)


if __name__ == "__main__":
    unittest.main()
