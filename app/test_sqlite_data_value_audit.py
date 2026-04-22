import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteDataValueAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.conn = sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_data_value_audit_classifies_tables(self) -> None:
        sqlite_store.write_signal(
            {
                "signal_id": "sig-audit",
                "signal_archive_key": "sig-audit",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "lp_alert_stage": "confirm",
                "archive_written_at": 1_710_000_000,
            }
        )
        sqlite_store.write_raw_event({"event_id": "raw-audit", "tx_hash": "0xraw", "captured_at": 1_710_000_001})
        payload = sqlite_store.sqlite_data_value_audit()
        by_table = {item["table"]: item for item in payload["tables"]}
        self.assertEqual("archive_debug", by_table["raw_events"]["data_value_class"])
        self.assertEqual("core_learning", by_table["signals"]["data_value_class"])
        self.assertEqual("index_only", by_table["raw_events"]["recommended_mode"])
        self.assertEqual("slim", by_table["market_context_attempts"]["recommended_mode"])
        self.assertEqual("operational_diagnostics", by_table["market_context_attempts"]["data_value_class"])
        self.assertTrue(by_table["signals"]["critical_for_verified"])
        self.assertIn("signals", payload["must_keep_full_tables"])
        self.assertIn("raw_events", payload["index_only_tables"])
        self.assertIn("market_context_attempts", payload["long_term_slim_tables"])
        self.assertIn("raw_events.raw_json", payload["fields_not_recommended_for_db"])


if __name__ == "__main__":
    unittest.main()
