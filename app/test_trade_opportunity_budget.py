import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class TradeOpportunityBudgetTests(unittest.TestCase):
    def test_third_candidate_in_hour_is_suppressed(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)

        first_event = make_event(ts=1_710_000_000)
        first_signal = make_signal(first_event, lp_multi_pool_resonance=3)
        first_payload = manager.apply_lp_signal(first_event, first_signal)

        second_event = make_event(ts=1_710_000_400)
        second_signal = make_signal(second_event, lp_multi_pool_resonance=4, lp_sweep_phase="sweep_confirmed")
        second_payload = manager.apply_lp_signal(second_event, second_signal)

        third_event = make_event(ts=1_710_000_800)
        third_signal = make_signal(third_event, lp_multi_pool_resonance=2)
        third_payload = manager.apply_lp_signal(third_event, third_signal)

        self.assertTrue(first_payload["telegram_should_send"])
        self.assertTrue(second_payload["telegram_should_send"])
        self.assertFalse(third_payload["telegram_should_send"])
        self.assertEqual("opportunity_budget_exhausted", third_payload["telegram_suppression_reason"])

    def test_same_opportunity_repeat_is_suppressed(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        first_event = make_event(ts=1_710_001_000)
        first_signal = make_signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        repeat_event = make_event(ts=1_710_001_020)
        repeat_signal = make_signal(repeat_event)
        payload = manager.apply_lp_signal(repeat_event, repeat_signal)

        self.assertFalse(payload["telegram_should_send"])
        self.assertEqual("same_opportunity_repeat", payload["telegram_suppression_reason"])

    def test_cooldown_blocks_new_verified_after_invalidation(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)

        first_event = make_event(ts=1_710_002_000)
        first_signal = make_signal(first_event)
        first_payload = manager.apply_lp_signal(first_event, first_signal)

        invalidation_event = make_event(ts=1_710_002_030)
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

        retry_event = make_event(ts=1_710_002_090)
        retry_signal = make_signal(retry_event, lp_multi_pool_resonance=4)
        retry_payload = manager.apply_lp_signal(retry_event, retry_signal)

        self.assertEqual("VERIFIED", first_payload["trade_opportunity_status"])
        self.assertEqual("INVALIDATED", invalidation_payload["trade_opportunity_status"])
        self.assertEqual("VERIFIED", retry_payload["trade_opportunity_status"])
        self.assertFalse(retry_payload["telegram_should_send"])
        self.assertEqual("opportunity_cooldown_active", retry_payload["telegram_suppression_reason"])

    def test_no_trade_lock_prevents_opportunity(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_003_000)
        signal = make_signal(
            event,
            no_trade_lock_active=True,
            asset_market_state_key="NO_TRADE_LOCK",
            trade_action_key="CONFLICT_NO_TRADE",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertNotEqual("opportunity", payload["telegram_update_kind"])


if __name__ == "__main__":
    unittest.main()
