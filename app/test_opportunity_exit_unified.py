import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from notifier import format_signal_message
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class OpportunityExitUnifiedTests(unittest.TestCase):
    def test_long_chase_allowed_with_none_suppresses_telegram(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_100_000)
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="confirmed",
            lp_absorption_context="broader_buy_pressure_confirmed",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("NONE", payload["trade_opportunity_status"])
        self.assertFalse(payload["telegram_should_send"])
        self.assertEqual("suppressed", payload["final_trading_output_source"])
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertEqual("no_trade_opportunity", payload["opportunity_gate_failure_reason"])

    def test_long_chase_allowed_with_candidate_shows_candidate_not_chase(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_100_010)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertEqual("trade_opportunity", payload["final_trading_output_source"])
        self.assertEqual("多头候选", payload["final_trading_output_label"])
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertTrue(message.splitlines()[0].startswith("多头候选｜ETH｜"))
        self.assertIn("候选，不可盲追", message)
        self.assertNotIn("可顺势追多", message)

    def test_short_chase_allowed_with_blocked_shows_do_not_chase(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(intent_type="pool_sell_pressure", ts=1_710_100_020)
        signal = make_signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_key="SHORT_CHASE_ALLOWED",
            trade_action_label="可顺势追空",
        )

        payload = manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("trade_opportunity", payload["final_trading_output_source"])
        self.assertEqual("不追空", payload["final_trading_output_label"])
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertTrue(message.splitlines()[0].startswith("不追空｜ETH｜"))
        self.assertIn("阻止原因：", message)
        self.assertNotIn("可顺势追空", message)

    def test_verified_long_shows_long_opportunity(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_100_030)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("多头机会", payload["final_trading_output_label"])
        self.assertFalse(payload["legacy_chase_downgraded"])
        self.assertTrue(message.splitlines()[0].startswith("多头机会｜ETH｜"))

    def test_verified_short_shows_short_opportunity(self) -> None:
        records = [make_outcome_row(direction_bucket="sell_pressure", move_60s=0.005, adverse=False) for _ in range(20)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(intent_type="pool_sell_pressure", ts=1_710_100_040)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)
        message = format_signal_message(signal, event)

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("空头机会", payload["final_trading_output_label"])
        self.assertTrue(message.splitlines()[0].startswith("空头机会｜ETH｜"))


if __name__ == "__main__":
    unittest.main()
