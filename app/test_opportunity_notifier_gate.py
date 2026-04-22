import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from notifier import format_signal_message
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class OpportunityNotifierGateTests(unittest.TestCase):
    def test_notifier_prefers_final_output_over_legacy_labels(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_100_100)
        signal = make_signal(event)

        manager.apply_lp_signal(event, signal)
        signal.context["trade_action_label"] = "可顺势追多"
        signal.context["asset_market_state_label"] = "可顺势追多"
        signal.context["headline_label"] = "可顺势追多"
        signal.context["market_state_label"] = "可顺势追多"
        signal.metadata.update(signal.context)
        event.metadata.update(signal.context)

        message = format_signal_message(signal, event)

        self.assertTrue(message.splitlines()[0].startswith("多头候选｜ETH｜"))
        self.assertNotIn("可顺势追多", message)

    def test_terminal_state_change_avoids_opportunity_wording_in_first_line(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)

        first_event = make_event(ts=1_710_100_110)
        first_signal = make_signal(first_event)
        manager.apply_lp_signal(first_event, first_signal)

        second_event = make_event(ts=1_710_100_150)
        second_signal = make_signal(
            second_event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="confirmed",
            lp_absorption_context="broader_buy_pressure_confirmed",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
        )
        payload = manager.apply_lp_signal(second_event, second_signal)
        message = format_signal_message(second_signal, second_event)

        self.assertIn(payload["trade_opportunity_status"], {"INVALIDATED", "EXPIRED"})
        self.assertTrue(message.splitlines()[0].startswith("状态变化｜ETH｜"))
        self.assertNotIn("多头机会", message.splitlines()[0])
        self.assertNotIn("空头机会", message.splitlines()[0])
        self.assertNotIn("候选", message.splitlines()[0])


if __name__ == "__main__":
    unittest.main()
