import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteOpportunityPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        sqlite_store.init_sqlite_store(Path(self.temp_dir.name) / "chain_monitor.sqlite")
        self.conn = sqlite_store.get_connection()

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_all_opportunity_statuses_can_be_persisted(self) -> None:
        for status in ("CANDIDATE", "VERIFIED", "BLOCKED", "NONE", "EXPIRED", "INVALIDATED"):
            payload = {
                "trade_opportunity_id": f"opp-{status.lower()}",
                "signal_id": f"sig-{status.lower()}",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "trade_opportunity_side": "LONG" if status != "NONE" else "NONE",
                "trade_opportunity_status": status,
                "trade_opportunity_score": 0.5,
                "trade_opportunity_created_at": 1_710_000_000,
                "trade_opportunity_expires_at": 1_710_000_300,
                "trade_opportunity_primary_blocker": "alignment_lost" if status in {"BLOCKED", "INVALIDATED"} else "",
                "opportunity_outcome_30s": "completed",
                "opportunity_outcome_60s": "unavailable",
                "opportunity_outcome_300s": "expired",
                "opportunity_followthrough_30s": status == "VERIFIED",
                "opportunity_adverse_30s": status == "BLOCKED",
            }
            self.assertTrue(sqlite_store.upsert_trade_opportunity(payload))
        counts = {
            row["status"]: row["count"]
            for row in self.conn.execute("SELECT status, COUNT(*) AS count FROM trade_opportunities GROUP BY status")
        }
        self.assertEqual(1, counts["CANDIDATE"])
        self.assertEqual(1, counts["VERIFIED"])
        self.assertEqual(1, counts["BLOCKED"])
        self.assertEqual(18, self.conn.execute("SELECT COUNT(*) FROM opportunity_outcomes").fetchone()[0])

    def test_signal_mirror_persists_related_state_outcome_and_suppression(self) -> None:
        record = {
            "signal_id": "sig-full",
            "signal_archive_key": "sig-full",
            "asset_symbol": "ETH",
            "pair_label": "ETH/USDC",
            "lp_alert_stage": "confirm",
            "asset_market_state_key": "NO_TRADE_LOCK",
            "asset_market_state_changed": True,
            "no_trade_lock_active": True,
            "no_trade_lock_started_at": 1_710_000_000,
            "telegram_should_send": False,
            "telegram_suppression_reason": "no_trade_lock_local_signal_suppressed",
            "prealert_lifecycle_state": "suppressed_by_lock",
            "lp_prealert_candidate": True,
            "lp_prealert_gate_passed": True,
            "trade_opportunity_id": "opp-full",
            "trade_opportunity_status": "BLOCKED",
            "trade_opportunity_side": "LONG",
            "trade_opportunity_score": 0.7,
            "opportunity_outcome_30s": "pending",
        }
        self.assertTrue(sqlite_store.write_signal(record))
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM asset_market_states").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM no_trade_locks").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM prealert_lifecycle").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM telegram_deliveries").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM trade_opportunities").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
