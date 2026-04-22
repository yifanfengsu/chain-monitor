import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteCompactModeTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "chain_monitor.sqlite"
        self.original_modes = {
            "SQLITE_RAW_EVENTS_MODE": sqlite_store.SQLITE_RAW_EVENTS_MODE,
            "SQLITE_PARSED_EVENTS_MODE": sqlite_store.SQLITE_PARSED_EVENTS_MODE,
            "SQLITE_DELIVERY_AUDIT_MODE": sqlite_store.SQLITE_DELIVERY_AUDIT_MODE,
            "SQLITE_TELEGRAM_DELIVERY_MODE": sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE,
            "SQLITE_CASE_FOLLOWUP_MODE": sqlite_store.SQLITE_CASE_FOLLOWUP_MODE,
        }
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_PARSED_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_DELIVERY_AUDIT_MODE = "slim"
        sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE = "slim"
        sqlite_store.SQLITE_CASE_FOLLOWUP_MODE = "slim"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        for key, value in self.original_modes.items():
            setattr(sqlite_store, key, value)
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_raw_and_parsed_index_only_do_not_store_full_json(self) -> None:
        self.assertTrue(
            sqlite_store.write_raw_event(
                {
                    "event_id": "raw-index",
                    "tx_hash": "0xraw",
                    "captured_at": 1_710_000_000,
                }
            )
        )
        self.assertTrue(
            sqlite_store.write_parsed_event(
                {
                    "event_id": "parsed-index",
                    "tx_hash": "0xparsed",
                    "parsed_at": 1_710_000_010,
                }
            )
        )
        row = self.conn.execute(
            "SELECT raw_json, archive_path, payload_hash, payload_mode FROM raw_events WHERE event_id='raw-index'"
        ).fetchone()
        self.assertIsNone(row["raw_json"])
        self.assertTrue(str(row["archive_path"] or "").endswith(".ndjson"))
        self.assertTrue(str(row["payload_hash"] or "").strip())
        self.assertEqual("index_only", row["payload_mode"])
        row = self.conn.execute(
            "SELECT parsed_json, archive_path, payload_hash, payload_mode FROM parsed_events WHERE event_id='parsed-index'"
        ).fetchone()
        self.assertIsNone(row["parsed_json"])
        self.assertTrue(str(row["archive_path"] or "").endswith(".ndjson"))
        self.assertTrue(str(row["payload_hash"] or "").strip())
        self.assertEqual("index_only", row["payload_mode"])

    def test_delivery_and_telegram_slim_do_not_store_full_payload(self) -> None:
        self.assertTrue(
            sqlite_store.write_delivery_audit(
                {
                    "audit_id": "audit-slim",
                    "signal_id": "sig-slim",
                    "asset_symbol": "ETH",
                    "stage": "candidate",
                    "delivery_decision": "suppress",
                    "reason": "duplicate_signal",
                    "telegram_suppression_reason": "duplicate_signal",
                    "notifier_variant": "research",
                    "notifier_template": "candidate_v2",
                    "sent_to_telegram": False,
                    "archive_written_at": 1_710_000_020,
                }
            )
        )
        self.assertTrue(
            sqlite_store.write_telegram_delivery(
                {
                    "telegram_delivery_id": "tg-slim",
                    "signal_id": "sig-slim",
                    "asset_symbol": "ETH",
                    "headline": "多头候选",
                    "archive_written_at": 1_710_000_020,
                }
            )
        )
        audit_row = self.conn.execute(
            """
            SELECT audit_json,
                   archive_path,
                   payload_hash,
                   payload_mode,
                   delivery_decision,
                   reason,
                   sent_to_telegram,
                   suppressed,
                   notifier_variant,
                   notifier_template
            FROM delivery_audit
            WHERE audit_id='audit-slim'
            """
        ).fetchone()
        self.assertIsNone(audit_row["audit_json"])
        self.assertTrue(str(audit_row["archive_path"] or "").endswith(".ndjson"))
        self.assertTrue(str(audit_row["payload_hash"] or "").strip())
        self.assertEqual("slim", audit_row["payload_mode"])
        self.assertEqual("suppress", audit_row["delivery_decision"])
        self.assertEqual("duplicate_signal", audit_row["reason"])
        self.assertEqual(0, audit_row["sent_to_telegram"])
        self.assertEqual(1, audit_row["suppressed"])
        self.assertEqual("research", audit_row["notifier_variant"])
        self.assertEqual("candidate_v2", audit_row["notifier_template"])
        telegram_row = self.conn.execute(
            """
            SELECT message_json,
                   archive_path,
                   payload_hash,
                   payload_mode,
                   headline,
                   sent,
                   suppressed,
                   suppression_reason
            FROM telegram_deliveries
            WHERE telegram_delivery_id='tg-slim'
            """
        ).fetchone()
        self.assertIsNone(telegram_row["message_json"])
        self.assertTrue(str(telegram_row["archive_path"] or "").endswith(".ndjson"))
        self.assertTrue(str(telegram_row["payload_hash"] or "").strip())
        self.assertEqual("slim", telegram_row["payload_mode"])
        self.assertEqual("多头候选", telegram_row["headline"])
        self.assertEqual(0, telegram_row["sent"])
        self.assertIsNone(telegram_row["suppressed"])
        self.assertIsNone(telegram_row["suppression_reason"])

    def test_core_signal_outcome_opportunity_rows_still_keep_full_analysis_payload(self) -> None:
        self.assertTrue(
            sqlite_store.write_signal(
                {
                    "signal_id": "sig-full",
                    "signal_archive_key": "sig-full",
                    "asset_symbol": "ETH",
                    "pair_label": "ETH/USDC",
                    "lp_alert_stage": "confirm",
                    "archive_written_at": 1_710_000_030,
                }
            )
        )
        self.assertTrue(
            sqlite_store.write_outcome(
                {
                    "signal_id": "sig-full",
                    "asset_symbol": "ETH",
                    "pair_label": "ETH/USDC",
                    "created_at": 1_710_000_030,
                    "outcome_windows": {
                        "60s": {
                            "status": "completed",
                            "price_source": "okx_mark",
                            "price_start": 100.0,
                            "price_end": 101.0,
                            "direction_adjusted_move_after": 0.01,
                            "raw_move_after": 0.01,
                            "followthrough_positive": True,
                            "adverse_by_direction": False,
                            "completed_at": 1_710_000_090,
                        }
                    },
                }
            )
        )
        self.assertTrue(
            sqlite_store.upsert_trade_opportunity(
                {
                    "trade_opportunity_id": "opp-full",
                    "signal_id": "sig-full",
                    "asset_symbol": "ETH",
                    "pair_label": "ETH/USDC",
                    "trade_opportunity_status": "CANDIDATE",
                    "trade_opportunity_side": "LONG",
                    "trade_opportunity_score": 0.74,
                    "trade_opportunity_score_components": {"lp_structure": 0.8},
                    "trade_opportunity_history_snapshot": {"sample_count": 12},
                    "trade_opportunity_evidence": [{"source": "lp"}],
                }
            )
        )
        self.assertIsNotNone(
            self.conn.execute("SELECT signal_json FROM signals WHERE signal_id='sig-full'").fetchone()["signal_json"]
        )
        self.assertIsNotNone(
            self.conn.execute(
                "SELECT opportunity_json FROM trade_opportunities WHERE trade_opportunity_id='opp-full'"
            ).fetchone()["opportunity_json"]
        )
        self.assertEqual(
            1,
            int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM outcomes WHERE signal_id='sig-full' AND window_sec=60"
                ).fetchone()[0]
            ),
        )


if __name__ == "__main__":
    unittest.main()
