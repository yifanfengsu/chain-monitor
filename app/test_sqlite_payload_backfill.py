import gzip
import json
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLitePayloadBackfillTests(unittest.TestCase):
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
            "SQLITE_PARSED_EVENTS_MODE": sqlite_store.SQLITE_PARSED_EVENTS_MODE,
            "SQLITE_DELIVERY_AUDIT_MODE": sqlite_store.SQLITE_DELIVERY_AUDIT_MODE,
            "SQLITE_TELEGRAM_DELIVERY_MODE": sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE,
            "SQLITE_CASE_FOLLOWUP_MODE": sqlite_store.SQLITE_CASE_FOLLOWUP_MODE,
        }
        sqlite_store.PROJECT_ROOT = self.root
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_PARSED_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_DELIVERY_AUDIT_MODE = "slim"
        sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE = "slim"
        sqlite_store.SQLITE_CASE_FOLLOWUP_MODE = "slim"
        self.conn = sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        for key, value in self.original_modes.items():
            setattr(sqlite_store, key, value)
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_archive(self, category: str, data: dict, *, gzip_archive: bool = False) -> str:
        path = self.archive_dir / category / f"{self.ARCHIVE_DATE}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"archive_ts": self.ARCHIVE_TS, "data": data}
        if gzip_archive:
            with gzip.open(f"{path}.gz", "wt", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            return str(Path(f"{path}.gz").resolve())
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return str(path.resolve())

    def test_legacy_raw_events_backfill_dry_run_does_not_modify_row(self) -> None:
        data = {
            "event_id": "raw-legacy",
            "tx_hash": "0xraw-legacy",
            "block_number": 321,
            "captured_at": self.ARCHIVE_TS,
        }
        self._write_archive("raw_events", data)
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

        before = dict(
            self.conn.execute(
                "SELECT archive_path, archive_date, payload_hash, payload_mode FROM raw_events WHERE event_id=?",
                (data["event_id"],),
            ).fetchone()
        )
        payload = sqlite_store.backfill_payload_metadata(dry_run=True, table="raw_events")
        after = dict(
            self.conn.execute(
                "SELECT archive_path, archive_date, payload_hash, payload_mode FROM raw_events WHERE event_id=?",
                (data["event_id"],),
            ).fetchone()
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["rows_examined"])
        self.assertEqual(1, payload["rows_backfillable"])
        self.assertEqual(0, payload["rows_backfilled"])
        self.assertEqual(before, after)

    def test_legacy_raw_events_backfill_execute_writes_metadata(self) -> None:
        data = {
            "event_id": "raw-execute",
            "tx_hash": "0xraw-execute",
            "block_number": 456,
            "captured_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("raw_events", data)
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
            "SELECT raw_json, archive_path, archive_date, payload_hash, payload_mode FROM raw_events WHERE event_id=?",
            (data["event_id"],),
        ).fetchone()
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["rows_backfillable"])
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertEqual(sqlite_store.stable_payload_hash(json.dumps(data, ensure_ascii=False)), row["payload_hash"])
        self.assertEqual("index_only", row["payload_mode"])
        self.assertIsNotNone(row["raw_json"])

    def test_backfill_supports_gzip_archive(self) -> None:
        data = {
            "event_id": "raw-gzip",
            "tx_hash": "0xraw-gzip",
            "block_number": 654,
            "captured_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("raw_events", data, gzip_archive=True)
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
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertTrue(str(row["payload_hash"] or "").strip())

    def test_parsed_events_backfill_execute(self) -> None:
        data = {
            "event_id": "parsed-legacy",
            "tx_hash": "0xparsed-legacy",
            "parsed_kind": "token_transfer",
            "parsed_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("parsed_events", data)
        self.conn.execute(
            """
            INSERT INTO parsed_events(event_id, tx_hash, chain, parsed_kind, parsed_at, parsed_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["event_id"],
                data["tx_hash"],
                "ethereum",
                data["parsed_kind"],
                self.ARCHIVE_TS,
                json.dumps(data, ensure_ascii=False),
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="parsed_events")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash, payload_mode FROM parsed_events WHERE event_id=?",
            (data["event_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertEqual("index_only", row["payload_mode"])
        self.assertTrue(str(row["payload_hash"] or "").strip())

    def test_delivery_audit_backfill_execute(self) -> None:
        data = {
            "audit_id": "audit-legacy",
            "signal_id": "sig-audit",
            "delivery_decision": "observe",
            "sent_to_telegram": False,
            "timestamp": self.ARCHIVE_TS,
            "archive_written_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("delivery_audit", data)
        self.conn.execute(
            """
            INSERT INTO delivery_audit(audit_id, archive_written_at, audit_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                data["audit_id"],
                self.ARCHIVE_TS,
                json.dumps(data, ensure_ascii=False),
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="delivery_audit")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash, payload_mode FROM delivery_audit WHERE audit_id=?",
            (data["audit_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertEqual("slim", row["payload_mode"])
        self.assertTrue(str(row["payload_hash"] or "").strip())

    def test_case_followups_backfill_execute(self) -> None:
        data = {
            "case_id": "case-legacy",
            "followup": {
                "followup_id": "followup-legacy",
                "signal_id": "sig-followup",
                "status": "confirmed",
                "stage": "confirmed",
            },
            "archive_written_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("case_followups", data)
        self.conn.execute(
            """
            INSERT INTO case_followups(followup_id, archive_written_at, followup_json, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                "followup-legacy",
                self.ARCHIVE_TS,
                json.dumps(data, ensure_ascii=False),
                self.ARCHIVE_TS,
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="case_followups")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash, payload_mode FROM case_followups WHERE followup_id=?",
            ("followup-legacy",),
        ).fetchone()
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertEqual("slim", row["payload_mode"])
        self.assertTrue(str(row["payload_hash"] or "").strip())

    def test_telegram_deliveries_backfill_can_reference_signals_archive(self) -> None:
        data = {
            "telegram_delivery_id": "tg-legacy",
            "signal_id": "sig-telegram",
            "headline": "观察级｜中性｜测试",
            "sent_at": self.ARCHIVE_TS,
            "archive_written_at": self.ARCHIVE_TS,
        }
        expected_archive_path = self._write_archive("signals", data)
        self.conn.execute(
            """
            INSERT INTO telegram_deliveries(telegram_delivery_id, created_at, message_json)
            VALUES(?, ?, ?)
            """,
            (
                data["telegram_delivery_id"],
                self.ARCHIVE_TS,
                json.dumps(data, ensure_ascii=False),
            ),
        )
        self.conn.commit()

        payload = sqlite_store.backfill_payload_metadata(dry_run=False, table="telegram_deliveries")
        row = self.conn.execute(
            "SELECT archive_path, archive_date, payload_hash, payload_mode FROM telegram_deliveries WHERE telegram_delivery_id=?",
            (data["telegram_delivery_id"],),
        ).fetchone()
        self.assertEqual(1, payload["rows_backfilled"])
        self.assertEqual(expected_archive_path, row["archive_path"])
        self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
        self.assertEqual("slim", row["payload_mode"])
        self.assertTrue(str(row["payload_hash"] or "").strip())


if __name__ == "__main__":
    unittest.main()
