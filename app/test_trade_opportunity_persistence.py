import json
import os
from pathlib import Path
import tempfile
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import (
    StubStateManager,
    make_event,
    make_outcome_record,
    make_outcome_row,
    make_signal,
)


class TradeOpportunityPersistenceTests(unittest.TestCase):
    def test_missing_cache_gracefully_falls_back_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "trade_opportunities.cache.json"
            manager = TradeOpportunityManager(
                state_manager=StubStateManager(),
                persistence_enabled=True,
                cache_path=str(cache_path),
                flush_interval_sec=0.0,
            )

            self.assertEqual("empty_or_missing", manager._last_load_status)
            self.assertEqual([], manager._opportunities)

    def test_candidate_outcome_is_recorded_with_okx_mark(self) -> None:
        event = make_event(ts=1_710_010_000)
        signal = make_signal(event)
        record_id = str(signal.context["outcome_tracking_key"])
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(outcome_records={record_id: make_outcome_record(record_id=record_id, source="okx_mark")}),
            persistence_enabled=False,
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertEqual("okx_mark", payload["opportunity_outcome_source"])
        self.assertEqual("completed", payload["opportunity_outcome_60s"])
        self.assertIs(payload["opportunity_followthrough_60s"], True)

    def test_verified_outcome_is_recorded_with_okx_mark(self) -> None:
        event = make_event(ts=1_710_010_100)
        signal = make_signal(event)
        record_id = str(signal.context["outcome_tracking_key"])
        history = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                records=history,
                outcome_records={record_id: make_outcome_record(record_id=record_id, source="okx_mark")},
            ),
            persistence_enabled=False,
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("okx_mark", payload["opportunity_outcome_source"])
        self.assertEqual("completed", payload["opportunity_outcome_60s"])
        self.assertIs(payload["opportunity_followthrough_60s"], True)

    def test_flush_and_reload_preserve_budget_cooldown_and_histories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "trade_opportunities.cache.json"
            history = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
            manager = TradeOpportunityManager(
                state_manager=StubStateManager(records=history),
                persistence_enabled=True,
                cache_path=str(cache_path),
                flush_interval_sec=0.0,
            )

            verified_event = make_event(ts=1_710_011_000)
            verified_signal = make_signal(verified_event)
            verified_payload = manager.apply_lp_signal(verified_event, verified_signal)

            invalidation_event = make_event(ts=1_710_011_030)
            invalidation_signal = make_signal(
                invalidation_event,
                lp_confirm_scope="local_confirm",
                lp_broader_alignment="weak_or_missing",
                lp_absorption_context="pool_only_unconfirmed_pressure",
                trade_action_key="WAIT_CONFIRMATION",
                trade_action_label="等待确认",
                asset_market_state_key="WAIT_CONFIRMATION",
                asset_market_state_label="等待确认",
                trade_action_debug={"strength_score": 0.20},
            )
            invalidation_payload = manager.apply_lp_signal(invalidation_event, invalidation_signal)
            manager.flush(force=True)

            restored = TradeOpportunityManager(
                state_manager=StubStateManager(records=history),
                persistence_enabled=True,
                cache_path=str(cache_path),
                flush_interval_sec=0.0,
            )
            cache_payload = json.loads(cache_path.read_text(encoding="utf-8"))
            asset_state = restored._asset_states["ETH"]

            self.assertEqual("VERIFIED", verified_payload["trade_opportunity_status"])
            self.assertEqual("INVALIDATED", invalidation_payload["trade_opportunity_status"])
            self.assertTrue(asset_state.restored_from_cache)
            self.assertGreater(asset_state.opportunity_cooldown_until, invalidation_event.ts)
            self.assertTrue(asset_state.opportunity_sent_count_by_hour)
            self.assertEqual("loaded", restored._last_load_status)
            self.assertIn("rolling_stats", cache_payload)
            self.assertIn("per_asset_budget", cache_payload)
            self.assertIn("candidate_history", cache_payload)
            self.assertIn("verified_history", cache_payload)
            self.assertIn("blocker_history", cache_payload)


if __name__ == "__main__":
    unittest.main()
