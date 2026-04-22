import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from analyzer import BehaviorAnalyzer
from notifier import format_signal_message
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class OpportunityNotifierGateTests(unittest.TestCase):
    def _pipeline(self) -> SignalPipeline:
        state_manager = StateManager()
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            quality_manager=QualityManager(state_manager=state_manager),
        )

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

    def test_delivery_gate_blocks_unverified_opportunity_like_copy(self) -> None:
        pipeline = self._pipeline()
        event = make_event(ts=1_710_100_170)
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="confirmed",
            lp_absorption_context="broader_buy_pressure_confirmed",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
        )
        signal.context.update(
            {
                "trade_opportunity_status": "NONE",
                "trade_action_key": "LONG_CHASE_ALLOWED",
                "trade_action_label": "可顺势追多",
                "final_trading_output_source": "trade_action_legacy",
                "final_trading_output_label": "可顺势追多",
                "final_trading_output_allowed": True,
                "telegram_should_send": True,
                "opportunity_gate_required": True,
                "opportunity_gate_passed": False,
            }
        )
        signal.metadata.update(signal.context)
        event.metadata.update(signal.context)

        allowed, reason = pipeline._enforce_lp_final_output_gate(event, signal, telegram_should_send=True)

        self.assertFalse(allowed)
        self.assertEqual("suppressed", signal.context["final_trading_output_source"])
        self.assertEqual("", signal.context["final_trading_output_label"])
        self.assertFalse(signal.context["telegram_should_send"])
        self.assertIn(
            reason,
            {
                "verified_output_requires_verified",
                "legacy_trade_action_not_deliverable",
                "legacy_chase_requires_verified_opportunity",
            },
        )


if __name__ == "__main__":
    unittest.main()
