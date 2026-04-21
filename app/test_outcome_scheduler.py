import tempfile
import unittest
from pathlib import Path

import sqlite_store
from outcome_scheduler import OutcomeScheduler
from state_manager import StateManager


class FakeMarketContextAdapter:
    def __init__(self, context: dict) -> None:
        self.context = dict(context)
        self.calls = 0

    def get_market_context(self, token_or_pair, ts, venue: str = "okx_perp") -> dict:
        self.calls += 1
        payload = dict(self.context)
        payload.setdefault("market_context_source", "live_public")
        payload.setdefault("market_context_venue", venue)
        return payload


def make_record(signal_id: str = "sig-outcome-scheduler", *, created_at: int = 1_000, start_price: float = 100.0) -> dict:
    windows = {}
    for window_sec in (30, 60, 300):
        windows[f"{window_sec}s"] = {
            "status": "pending",
            "window_sec": window_sec,
            "due_at": created_at + window_sec,
            "price_source": "okx_mark",
            "start_price_source": "okx_mark",
            "price_start": start_price,
            "price_end": None,
            "completed_at": None,
            "failure_reason": "",
        }
    return {
        "record_id": signal_id,
        "signal_id": signal_id,
        "trade_opportunity_id": "opp-scheduler",
        "asset_symbol": "ETH",
        "pair_label": "ETH/USDC",
        "pool_address": "0xpool",
        "direction_bucket": "buy_pressure",
        "created_at": created_at,
        "outcome_price_source": "okx_mark",
        "outcome_price_start": start_price,
        "outcome_windows": windows,
    }


class OutcomeSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        sqlite_store.init_sqlite_store(self.db_path)
        self.state_manager = StateManager()

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _scheduler(self, context: dict) -> OutcomeScheduler:
        return OutcomeScheduler(
            state_manager=self.state_manager,
            market_context_adapter=FakeMarketContextAdapter(context),
            catchup_max_sec=900,
            expire_after_sec=1800,
        )

    def test_register_signal_creates_30s_60s_300s_pending_outcomes(self) -> None:
        record = make_record()
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = self._scheduler({"perp_mark_price": 101.0})

        created = scheduler.register_from_lp_record(record)

        self.assertEqual(3, len(created))
        self.assertEqual(3, scheduler.pending_count())
        rows = sqlite_store.load_pending_outcome_rows()
        self.assertEqual([30, 60, 300], sorted(int(row["window_sec"]) for row in rows))
        restored = self.state_manager.get_lp_outcome_record("sig-outcome-scheduler")
        self.assertEqual("pending", restored["outcome_windows"]["300s"]["status"])
        self.assertEqual(1_300, restored["outcome_windows"]["300s"]["due_at"])

    def test_due_outcome_uses_okx_mark_price(self) -> None:
        record = make_record(signal_id="sig-okx-mark")
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = self._scheduler({"market_context_venue": "okx_perp", "perp_mark_price": 101.0})
        scheduler.register_from_lp_record(record)

        result = scheduler.settle_due_outcomes(now=1_031)

        self.assertEqual(1, result["completed_count"])
        updated = self.state_manager.get_lp_outcome_record("sig-okx-mark")
        window = updated["outcome_windows"]["30s"]
        self.assertEqual("completed", window["status"])
        self.assertEqual("okx_mark", window["price_source"])
        self.assertAlmostEqual(0.01, float(window["raw_move_after"]), places=6)

    def test_okx_mark_missing_falls_back_to_index(self) -> None:
        record = make_record(signal_id="sig-okx-index")
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = self._scheduler({"market_context_venue": "okx_perp", "perp_index_price": 102.0})
        scheduler.register_from_lp_record(record)

        scheduler.settle_due_outcomes(now=1_031)

        window = self.state_manager.get_lp_outcome_record("sig-okx-index")["outcome_windows"]["30s"]
        self.assertEqual("completed", window["status"])
        self.assertEqual("okx_index", window["price_source"])

    def test_okx_index_missing_falls_back_to_last(self) -> None:
        record = make_record(signal_id="sig-okx-last")
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = self._scheduler({"market_context_venue": "okx_perp", "perp_last_price": 103.0})
        scheduler.register_from_lp_record(record)

        scheduler.settle_due_outcomes(now=1_031)

        window = self.state_manager.get_lp_outcome_record("sig-okx-last")["outcome_windows"]["30s"]
        self.assertEqual("completed", window["status"])
        self.assertEqual("okx_last", window["price_source"])

    def test_market_price_unavailable_marks_unavailable(self) -> None:
        record = make_record(signal_id="sig-unavailable")
        self.state_manager.restore_lp_outcome_records([record])
        scheduler = self._scheduler({"market_context_venue": "okx_perp"})
        scheduler.register_from_lp_record(record)

        scheduler.settle_due_outcomes(now=1_031)

        window = self.state_manager.get_lp_outcome_record("sig-unavailable")["outcome_windows"]["30s"]
        self.assertEqual("unavailable", window["status"])
        self.assertEqual("market_price_unavailable", window["failure_reason"])


if __name__ == "__main__":
    unittest.main()
