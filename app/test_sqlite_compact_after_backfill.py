import json
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteCompactAfterBackfillTests(unittest.TestCase):
    ARCHIVE_TS = 1_776_787_492
    ARCHIVE_DATE = "2026-04-22"

    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.original_project_root = sqlite_store.PROJECT_ROOT
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_raw_mode = sqlite_store.SQLITE_RAW_EVENTS_MODE
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        self.conn = sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        sqlite_store.SQLITE_RAW_EVENTS_MODE = self.original_raw_mode
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive(self, data: dict) -> str:
        path = self.archive_dir / "raw_events" / f"{self.ARCHIVE_DATE}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"archive_ts": self.ARCHIVE_TS, "data": data}, ensure_ascii=False) + "\n")
        return str(path.resolve())

    def _insert_raw_row(
        self,
        *,
        event_id: str,
        payload: dict | None,
        payload_hash: str | None = None,
        archive_path: str | None = None,
    ) -> None:
        payload_json = json.dumps(payload, ensure_ascii=False) if payload is not None else None
        block_number = int(payload.get("block_number") or 0) if isinstance(payload, dict) else None
        tx_hash = str(payload.get("tx_hash") or "") if isinstance(payload, dict) else None
        self.conn.execute(
            """
            INSERT INTO raw_events(
                event_id,
                tx_hash,
                block_number,
                chain,
                captured_at,
                raw_json,
                payload_hash,
                archive_path,
                archive_date,
                payload_mode,
                created_at,
                updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                tx_hash,
                block_number,
                "ethereum",
                self.ARCHIVE_TS,
                payload_json,
                payload_hash,
                archive_path,
                self.ARCHIVE_DATE if archive_path else None,
                "index_only" if payload_hash or archive_path else None,
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

    def test_compact_clears_payload_after_backfill(self) -> None:
        data = {
            "event_id": "raw-compactable",
            "tx_hash": "0xcompactable",
            "block_number": 111,
            "captured_at": self.ARCHIVE_TS,
        }
        self._write_archive(data)
        self._insert_raw_row(event_id=data["event_id"], payload=data)

        backfill = sqlite_store.backfill_payload_metadata(dry_run=False, table="raw_events")
        dry_run = sqlite_store.compact_table("raw_events", dry_run=True)
        execute = sqlite_store.compact_table("raw_events", dry_run=False)
        row = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id=?", (data["event_id"],)).fetchone()
        self.assertEqual(1, backfill["rows_backfilled"])
        self.assertEqual(1, dry_run["rows_compacted"])
        self.assertEqual(1, execute["rows_compacted"])
        self.assertIsNone(row["raw_json"])

    def test_compact_does_not_clear_hash_mismatch_rows(self) -> None:
        db_data = {
            "event_id": "raw-mismatch",
            "tx_hash": "0xmismatch",
            "block_number": 222,
            "captured_at": self.ARCHIVE_TS,
            "marker": "db",
        }
        archive_data = dict(db_data)
        archive_data["marker"] = "archive"
        archive_path = self._write_archive(archive_data)
        self._insert_raw_row(
            event_id=db_data["event_id"],
            payload=db_data,
            payload_hash=sqlite_store.stable_payload_hash(json.dumps(db_data, ensure_ascii=False)),
            archive_path=archive_path,
        )

        payload = sqlite_store.compact_table("raw_events", dry_run=False)
        row = self.conn.execute("SELECT raw_json FROM raw_events WHERE event_id=?", (db_data["event_id"],)).fetchone()
        self.assertEqual(0, payload["rows_compacted"])
        self.assertEqual(1, payload["skipped_hash_mismatch"])
        self.assertIsNotNone(row["raw_json"])

    def test_compact_reports_skipped_breakdown(self) -> None:
        good = {
            "event_id": "raw-good",
            "tx_hash": "0xgood",
            "block_number": 1,
            "captured_at": self.ARCHIVE_TS,
        }
        missing_hash = {
            "event_id": "raw-missing-hash",
            "tx_hash": "0xmissinghash",
            "block_number": 10,
            "captured_at": self.ARCHIVE_TS,
        }
        no_archive_path = {
            "event_id": "raw-no-path",
            "tx_hash": "0xnopath",
            "block_number": 2,
            "captured_at": self.ARCHIVE_TS,
        }
        missing_archive = {
            "event_id": "raw-missing-archive",
            "tx_hash": "0xmissingarchive",
            "block_number": 3,
            "captured_at": self.ARCHIVE_TS,
        }
        mismatch = {
            "event_id": "raw-breakdown-mismatch",
            "tx_hash": "0xbreakdownmismatch",
            "block_number": 4,
            "captured_at": self.ARCHIVE_TS,
            "marker": "db",
        }
        mismatch_archive = dict(mismatch)
        mismatch_archive["marker"] = "archive"
        good_archive_path = self._write_archive(good)
        self._write_archive(missing_hash)
        mismatch_archive_path = self._write_archive(mismatch_archive)
        self._insert_raw_row(event_id="raw-empty", payload=None)
        self._insert_raw_row(event_id=good["event_id"], payload=good)
        self._insert_raw_row(event_id=missing_hash["event_id"], payload=missing_hash)
        self._insert_raw_row(
            event_id=no_archive_path["event_id"],
            payload=no_archive_path,
            payload_hash=sqlite_store.stable_payload_hash(json.dumps(no_archive_path, ensure_ascii=False)),
        )
        self._insert_raw_row(
            event_id=missing_archive["event_id"],
            payload=missing_archive,
            payload_hash=sqlite_store.stable_payload_hash(json.dumps(missing_archive, ensure_ascii=False)),
            archive_path=str((self.archive_dir / "raw_events" / "2099-01-01.ndjson").resolve()),
        )
        self._insert_raw_row(
            event_id=mismatch["event_id"],
            payload=mismatch,
            payload_hash=sqlite_store.stable_payload_hash(json.dumps(mismatch, ensure_ascii=False)),
            archive_path=mismatch_archive_path,
        )
        self.conn.execute(
            "UPDATE raw_events SET archive_path=?, archive_date=?, payload_hash=?, payload_mode=? WHERE event_id=?",
            (
                good_archive_path,
                self.ARCHIVE_DATE,
                sqlite_store.stable_payload_hash(json.dumps(good, ensure_ascii=False)),
                "index_only",
                good["event_id"],
            ),
        )
        self.conn.commit()

        payload = sqlite_store.compact_table("raw_events", dry_run=True)
        self.assertEqual(1, payload["rows_compacted"])
        self.assertEqual(1, payload["skipped_no_payload_hash"])
        self.assertEqual(1, payload["skipped_no_archive_path"])
        self.assertEqual(1, payload["skipped_archive_missing"])
        self.assertEqual(1, payload["skipped_hash_mismatch"])
        self.assertGreaterEqual(payload["skipped_payload_already_empty"], 1)
        self.assertIn("Run --backfill-payload-metadata before compact.", payload["backfillable_hint"])

    def test_db_value_audit_reports_backfill_coverage(self) -> None:
        data = {
            "event_id": "raw-audit",
            "tx_hash": "0xaudit",
            "block_number": 555,
            "captured_at": self.ARCHIVE_TS,
        }
        self._write_archive(data)
        self._insert_raw_row(event_id=data["event_id"], payload=data)

        payload = sqlite_store.sqlite_data_value_audit(write_reports=False)
        self.assertIn("payload_hash_coverage_by_table", payload)
        self.assertIn("archive_path_coverage_by_table", payload)
        self.assertIn("backfill_needed_rows_by_table", payload)
        self.assertIn("legacy_full_payload_rows_by_table", payload)
        self.assertIn("estimated_savings_after_backfill_mb", payload)
        self.assertEqual(1, payload["backfill_needed_rows_by_table"]["raw_events"])
        self.assertEqual(1, payload["legacy_full_payload_rows_by_table"]["raw_events"])


if __name__ == "__main__":
    unittest.main()
