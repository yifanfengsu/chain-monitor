import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_signal


class OpportunityLegacyChaseDowngradeTests(unittest.TestCase):
    def test_candidate_marks_legacy_chase_downgrade(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_100_200)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertEqual("candidate_only_not_verified", payload["legacy_chase_downgrade_reason"])
        self.assertTrue(payload["opportunity_gate_passed"])

    def test_blocked_marks_legacy_chase_downgrade_reason(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(intent_type="pool_sell_pressure", ts=1_710_100_210)
        signal = make_signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_key="SHORT_CHASE_ALLOWED",
            trade_action_label="可顺势追空",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertTrue(payload["legacy_chase_downgrade_reason"].startswith("blocked:sweep_exhaustion_risk"))
        self.assertTrue(payload["opportunity_gate_passed"])

    def test_none_records_gate_failure_for_legacy_chase(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_100_220)
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
        self.assertTrue(payload["legacy_chase_downgraded"])
        self.assertEqual("no_trade_opportunity", payload["legacy_chase_downgrade_reason"])
        self.assertFalse(payload["opportunity_gate_passed"])


if __name__ == "__main__":
    unittest.main()
