import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sqlite_store


class SQLiteOperationalPayloadExportTests(unittest.TestCase):
    ARCHIVE_TS = 1_776_787_492
    ARCHIVE_DATE = "2026-04-22"

    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "chain_monitor.sqlite"
        self.archive_dir = self.root / "archive" / "sqlite_payloads"
        self.original_project_root = sqlite_store.PROJECT_ROOT
        sqlite_store.PROJECT_ROOT = self.root
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _insert_operational_rows(self) -> None:
        for idx in range(2):
            message = {
                "telegram_delivery_id": f"tg-{idx}",
                "signal_id": f"sig-{idx}",
                "trade_opportunity_id": f"opp-{idx}",
                "created_at": self.ARCHIVE_TS + idx,
                "body": {"line": idx, "text": "diagnostic payload"},
            }
            self.conn.execute(
                """
                INSERT INTO telegram_deliveries(
                    telegram_delivery_id,
                    signal_id,
                    trade_opportunity_id,
                    sent_at,
                    message_json,
                    created_at
                )
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    message["telegram_delivery_id"],
                    message["signal_id"],
                    message["trade_opportunity_id"],
                    self.ARCHIVE_TS + idx,
                    json.dumps(message, ensure_ascii=False),
                    self.ARCHIVE_TS + idx,
                ),
            )
            audit = {
                "audit_id": f"audit-{idx}",
                "signal_id": f"sig-{idx}",
                "event_id": f"event-{idx}",
                "delivery_decision": "suppress",
                "sent_to_telegram": False,
                "timestamp": self.ARCHIVE_TS + idx,
                "archive_written_at": self.ARCHIVE_TS + idx,
                "detail": {"line": idx, "reason": "test"},
            }
            self.conn.execute(
                """
                INSERT INTO delivery_audit(
                    audit_id,
                    signal_id,
                    event_id,
                    delivery_decision,
                    sent_to_telegram,
                    timestamp,
                    audit_json,
                    archive_written_at,
                    created_at,
                    updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit["audit_id"],
                    audit["signal_id"],
                    audit["event_id"],
                    audit["delivery_decision"],
                    0,
                    self.ARCHIVE_TS + idx,
                    json.dumps(audit, ensure_ascii=False),
                    self.ARCHIVE_TS + idx,
                    self.ARCHIVE_TS + idx,
                    self.ARCHIVE_TS + idx,
                ),
            )
        self.conn.commit()

    def _read_archive_rows(self, table: str) -> list[dict]:
        path = self.archive_dir / table / f"{self.ARCHIVE_DATE}.ndjson.gz"
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def test_dry_run_does_not_modify_db_or_write_archive(self) -> None:
        self._insert_operational_rows()
        before = {
            "telegram_deliveries": self.conn.execute(
                "SELECT COUNT(*) FROM telegram_deliveries WHERE message_json IS NOT NULL AND message_json != ''"
            ).fetchone()[0],
            "delivery_audit": self.conn.execute(
                "SELECT COUNT(*) FROM delivery_audit WHERE audit_json IS NOT NULL AND audit_json != ''"
            ).fetchone()[0],
        }

        payload = sqlite_store.export_operational_payloads(
            dry_run=True,
            table="telegram_deliveries",
            archive_dir=self.archive_dir,
        )
        after = {
            "telegram_deliveries": self.conn.execute(
                "SELECT COUNT(*) FROM telegram_deliveries WHERE message_json IS NOT NULL AND message_json != ''"
            ).fetchone()[0],
            "delivery_audit": self.conn.execute(
                "SELECT COUNT(*) FROM delivery_audit WHERE audit_json IS NOT NULL AND audit_json != ''"
            ).fetchone()[0],
        }

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(2, payload["candidate_rows"])
        self.assertEqual(2, payload["would_update_rows"])
        self.assertEqual("message_json", payload["would_clear_json_column"])
        self.assertEqual(before, after)
        self.assertFalse(self.archive_dir.exists())

    def test_execute_writes_gzip_archive_and_clears_payload_columns(self) -> None:
        self._insert_operational_rows()

        payload = sqlite_store.export_operational_payloads(
            execute=True,
            confirm=True,
            archive_dir=self.archive_dir,
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(4, payload["rows_exported"])
        telegram_rows = self._read_archive_rows("telegram_deliveries")
        audit_rows = self._read_archive_rows("delivery_audit")
        self.assertEqual(2, len(telegram_rows))
        self.assertEqual(2, len(audit_rows))
        for row in telegram_rows + audit_rows:
            self.assertEqual(sqlite_store.OPERATIONAL_PAYLOAD_EXPORT_SCHEMA, row["schema"])
            self.assertEqual(sqlite_store.OPERATIONAL_PAYLOAD_EXPORT_SOURCE, row["source"])
            self.assertTrue(str(row["payload_hash"]).startswith("sha256:"))
            self.assertIn("row_identity", row)
            self.assertIn("payload", row)

        tg_db_rows = self.conn.execute(
            "SELECT message_json, archive_path, archive_date, payload_hash, payload_mode FROM telegram_deliveries"
        ).fetchall()
        audit_db_rows = self.conn.execute(
            "SELECT audit_json, archive_path, archive_date, payload_hash, payload_mode FROM delivery_audit"
        ).fetchall()
        for row in list(tg_db_rows) + list(audit_db_rows):
            self.assertIsNone(row[0])
            self.assertFalse(Path(str(row["archive_path"])).is_absolute())
            self.assertEqual(self.ARCHIVE_DATE, row["archive_date"])
            self.assertTrue(str(row["payload_hash"] or "").startswith("sha256:"))
            self.assertEqual("archived", row["payload_mode"])

    def test_execute_is_idempotent_for_already_archived_rows(self) -> None:
        self._insert_operational_rows()
        first = sqlite_store.export_operational_payloads(
            execute=True,
            confirm=True,
            archive_dir=self.archive_dir,
        )
        before_counts = {
            table: len(self._read_archive_rows(table))
            for table in ("telegram_deliveries", "delivery_audit")
        }

        second = sqlite_store.export_operational_payloads(
            execute=True,
            confirm=True,
            archive_dir=self.archive_dir,
        )
        after_counts = {
            table: len(self._read_archive_rows(table))
            for table in ("telegram_deliveries", "delivery_audit")
        }

        self.assertEqual(4, first["rows_exported"])
        self.assertEqual(0, second["rows_exported"])
        self.assertEqual(before_counts, after_counts)

    def test_execute_requires_confirm(self) -> None:
        self._insert_operational_rows()

        payload = sqlite_store.export_operational_payloads(
            execute=True,
            confirm=False,
            table="telegram_deliveries",
            archive_dir=self.archive_dir,
        )

        row = self.conn.execute(
            "SELECT message_json, archive_path, payload_hash FROM telegram_deliveries WHERE telegram_delivery_id='tg-0'"
        ).fetchone()
        self.assertFalse(payload["ok"])
        self.assertEqual("confirm_required", payload["reason"])
        self.assertIsNotNone(row["message_json"])
        self.assertIsNone(row["archive_path"])
        self.assertIsNone(row["payload_hash"])

    def test_core_tables_are_blocked(self) -> None:
        self.conn.execute(
            "INSERT INTO signals(signal_id, signal_json, created_at, updated_at) VALUES(?, ?, ?, ?)",
            ("sig-core", json.dumps({"signal_id": "sig-core"}), self.ARCHIVE_TS, self.ARCHIVE_TS),
        )
        self.conn.execute(
            "INSERT INTO prealert_lifecycle(prealert_id, lifecycle_json, created_at, updated_at) VALUES(?, ?, ?, ?)",
            ("prealert-core", json.dumps({"prealert_id": "prealert-core"}), self.ARCHIVE_TS, self.ARCHIVE_TS),
        )
        self.conn.execute(
            "INSERT INTO asset_market_states(state_id, state_json, evidence_json, created_at) VALUES(?, ?, ?, ?)",
            (
                "state-core",
                json.dumps({"state_id": "state-core"}),
                json.dumps({"evidence": True}),
                self.ARCHIVE_TS,
            ),
        )
        self.conn.commit()

        for table in ("signals", "prealert_lifecycle", "asset_market_states"):
            payload = sqlite_store.export_operational_payloads(
                dry_run=True,
                table=table,
                archive_dir=self.archive_dir,
            )
            self.assertFalse(payload["ok"])
            self.assertEqual("core_table_blocked", payload["reason"])

        self.assertIsNotNone(
            self.conn.execute("SELECT signal_json FROM signals WHERE signal_id='sig-core'").fetchone()["signal_json"]
        )
        self.assertIsNotNone(
            self.conn.execute(
                "SELECT lifecycle_json FROM prealert_lifecycle WHERE prealert_id='prealert-core'"
            ).fetchone()["lifecycle_json"]
        )
        self.assertIsNotNone(
            self.conn.execute(
                "SELECT state_json FROM asset_market_states WHERE state_id='state-core'"
            ).fetchone()["state_json"]
        )

    def test_archive_write_failure_does_not_clear_payload(self) -> None:
        self._insert_operational_rows()

        with mock.patch.object(sqlite_store, "_append_gzip_ndjson_fsynced", side_effect=OSError("disk full")):
            payload = sqlite_store.export_operational_payloads(
                execute=True,
                confirm=True,
                table="telegram_deliveries",
                archive_dir=self.archive_dir,
            )

        row = self.conn.execute(
            "SELECT message_json, archive_path, payload_hash FROM telegram_deliveries WHERE telegram_delivery_id='tg-0'"
        ).fetchone()
        self.assertFalse(payload["ok"])
        self.assertEqual("archive_write_failed", payload["reason"])
        self.assertIsNotNone(row["message_json"])
        self.assertIsNone(row["archive_path"])
        self.assertIsNone(row["payload_hash"])


if __name__ == "__main__":
    unittest.main()
