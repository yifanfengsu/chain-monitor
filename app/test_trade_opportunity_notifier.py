import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from notifier import format_signal_message
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class TradeOpportunityNotifierTests(unittest.TestCase):
    def test_observe_state_is_not_sent_by_default(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_200)
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="weak_or_missing",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            trade_action_key="WAIT_CONFIRMATION",
            trade_action_label="等待确认",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
            trade_action_debug={"strength_score": 0.20},
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("NONE", payload["trade_opportunity_status"])
        self.assertFalse(payload["telegram_should_send"])
        self.assertEqual("suppressed", payload["telegram_update_kind"])

    def test_candidate_sends_opportunity_card(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_210)
        signal = make_signal(event)

        manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("多头候选｜ETH｜"))
        self.assertIn("状态：候选，不可盲追", message)
        self.assertNotIn("可顺势追多", message)

    def test_verified_sends_opportunity_card(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_000_220)
        signal = make_signal(event)

        manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("多头机会｜ETH｜"))
        self.assertIn("说明：不是自动下单", message)
        self.assertIn("调试：score_components=", message)

    def test_blocker_sends_risk_card(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_230)
        signal = make_signal(
            event,
            no_trade_lock_active=True,
            asset_market_state_key="NO_TRADE_LOCK",
            asset_market_state_label="不交易锁定",
            trade_action_key="CONFLICT_NO_TRADE",
            trade_action_label="不交易",
        )

        manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("不交易｜ETH｜不交易锁定"))
        self.assertIn("阻止原因：不交易锁定", message)

    def test_repeated_candidate_is_suppressed(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        first_event = make_event(ts=1_710_000_240)
        first_signal = make_signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        second_event = make_event(ts=1_710_000_260)
        second_signal = make_signal(second_event)
        payload = manager.apply_lp_signal(second_event, second_signal)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertFalse(payload["telegram_should_send"])
        self.assertEqual("same_opportunity_repeat", payload["telegram_suppression_reason"])


if __name__ == "__main__":
    unittest.main()
