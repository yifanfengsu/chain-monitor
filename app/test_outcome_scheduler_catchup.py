import tempfile
import unittest
from pathlib import Path

import sqlite_store
from outcome_scheduler import OutcomeScheduler
from state_manager import StateManager
from test_outcome_scheduler import FakeMarketContextAdapter, make_record


class OutcomeSchedulerCatchupTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_restart_restores_pending_outcomes_from_sqlite(self) -> None:
        state_manager = StateManager()
        record = make_record(signal_id="sig-restore")
        state_manager.restore_lp_outcome_records([record])
        scheduler = OutcomeScheduler(
            state_manager=state_manager,
            market_context_adapter=FakeMarketContextAdapter({"perp_mark_price": 101.0}),
        )
        scheduler.register_from_lp_record(record)

        restored_state = StateManager()
        restored_scheduler = OutcomeScheduler(
            state_manager=restored_state,
            market_context_adapter=FakeMarketContextAdapter({"perp_mark_price": 101.0}),
        )
        result = restored_scheduler.restore_pending_outcomes_from_sqlite(now=1_001, catchup=False)

        self.assertEqual(3, result["restored_pending_count"])
        self.assertEqual(3, restored_scheduler.pending_count())
        self.assertEqual("pending", restored_state.get_lp_outcome_record("sig-restore")["outcome_windows"]["60s"]["status"])

    def test_due_pending_catchup_completes_once(self) -> None:
        state_manager = StateManager()
        record = make_record(signal_id="sig-catchup")
        state_manager.restore_lp_outcome_records([record])
        scheduler = OutcomeScheduler(
            state_manager=state_manager,
            market_context_adapter=FakeMarketContextAdapter({"market_context_venue": "okx_perp", "perp_mark_price": 101.0}),
            catchup_max_sec=900,
        )
        scheduler.register_from_lp_record(record)

        restored_scheduler = OutcomeScheduler(
            state_manager=state_manager,
            market_context_adapter=FakeMarketContextAdapter({"market_context_venue": "okx_perp", "perp_mark_price": 101.0}),
            catchup_max_sec=900,
        )
        first = restored_scheduler.restore_pending_outcomes_from_sqlite(now=1_031, catchup=True)
        second = restored_scheduler.catchup_due_outcomes(now=1_031)

        self.assertEqual(1, first["catchup"]["catchup_completed_count"])
        self.assertEqual(0, second["processed_count"])
        count = int(
            self.conn.execute(
                "SELECT COUNT(*) FROM outcomes WHERE signal_id=? AND window_sec=? AND status='completed'",
                ("sig-catchup", 30),
            ).fetchone()[0]
        )
        self.assertEqual(1, count)

    def test_catchup_window_exceeded_expires(self) -> None:
        state_manager = StateManager()
        record = make_record(signal_id="sig-expired")
        state_manager.restore_lp_outcome_records([record])
        scheduler = OutcomeScheduler(
            state_manager=state_manager,
            market_context_adapter=FakeMarketContextAdapter({"perp_mark_price": 101.0}),
            catchup_max_sec=10,
        )
        scheduler.register_from_lp_record(record)

        result = scheduler.catchup_due_outcomes(now=1_400)

        self.assertEqual(3, result["catchup_expired_count"])
        rows = self.conn.execute(
            "SELECT failure_reason FROM outcomes WHERE signal_id=? AND status='expired'",
            ("sig-expired",),
        ).fetchall()
        self.assertEqual({"catchup_window_exceeded"}, {row[0] for row in rows})


if __name__ == "__main__":
    unittest.main()
