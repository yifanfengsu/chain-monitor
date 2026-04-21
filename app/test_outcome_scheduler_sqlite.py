import tempfile
import unittest
from pathlib import Path

import sqlite_store
from outcome_scheduler import OutcomeScheduler
from state_manager import StateManager
from test_outcome_scheduler import FakeMarketContextAdapter, make_record


class OutcomeSchedulerSQLiteTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)
        self.state_manager = StateManager()

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_opportunity_outcomes_are_written_on_settle(self) -> None:
        record = make_record(signal_id="sig-opp")
        record["trade_opportunity_id"] = "opp-1"
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = OutcomeScheduler(
            state_manager=self.state_manager,
            market_context_adapter=FakeMarketContextAdapter({"market_context_venue": "okx_perp", "perp_mark_price": 101.0}),
        )

        scheduler.register_from_lp_record(record)
        scheduler.settle_due_outcomes(now=1_031)

        row = self.conn.execute(
            "SELECT status, outcome_price_source, raw_move, direction_adjusted_move FROM opportunity_outcomes WHERE trade_opportunity_id=? AND window_sec=?",
            ("opp-1", 30),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("completed", row[0])
        self.assertEqual("okx_mark", row[1])
        self.assertAlmostEqual(0.01, float(row[2]), places=6)
        self.assertAlmostEqual(0.01, float(row[3]), places=6)

    def test_sqlite_disabled_does_not_crash_and_state_still_updates(self) -> None:
        original_enable = sqlite_store.SQLITE_ENABLE
        original_write_outcomes = sqlite_store.SQLITE_WRITE_OUTCOMES
        sqlite_store.close()
        sqlite_store.SQLITE_ENABLE = False
        sqlite_store.SQLITE_WRITE_OUTCOMES = False
        try:
            state_manager = StateManager()
            record = make_record(signal_id="sig-sqlite-disabled")
            state_manager.restore_lp_outcome_records([record])
            scheduler = OutcomeScheduler(
                state_manager=state_manager,
                market_context_adapter=FakeMarketContextAdapter({"market_context_venue": "okx_perp", "perp_mark_price": 101.0}),
            )

            scheduler.register_from_lp_record(record)
            result = scheduler.settle_due_outcomes(now=1_031)

            self.assertEqual(1, result["completed_count"])
            window = state_manager.get_lp_outcome_record("sig-sqlite-disabled")["outcome_windows"]["30s"]
            self.assertEqual("completed", window["status"])
        finally:
            sqlite_store.SQLITE_ENABLE = original_enable
            sqlite_store.SQLITE_WRITE_OUTCOMES = original_write_outcomes
            sqlite_store.init_sqlite_store(self.db_path)


if __name__ == "__main__":
    unittest.main()
