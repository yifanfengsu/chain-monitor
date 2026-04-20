import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from config import OPPORTUNITY_MIN_HISTORY_SAMPLES
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


class TradeOpportunityFunnelTests(unittest.TestCase):
    def test_insufficient_samples_stays_candidate(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=[]), persistence_enabled=False)
        event = make_event(ts=1_710_000_100)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertIn("history_samples_insufficient", payload["trade_opportunity_risk_flags"])

    def test_sufficient_samples_and_good_followthrough_unlock_verified(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=0.005, adverse=False) for _ in range(OPPORTUNITY_MIN_HISTORY_SAMPLES)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_000_110)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual(OPPORTUNITY_MIN_HISTORY_SAMPLES, payload["trade_opportunity_history_snapshot"]["sample_size"])

    def test_high_adverse_history_blocks_verified(self) -> None:
        records = [make_outcome_row(direction_bucket="buy_pressure", move_60s=-0.004, adverse=True) for _ in range(OPPORTUNITY_MIN_HISTORY_SAMPLES)]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_000_120)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("history_adverse_too_high", payload["trade_opportunity_primary_blocker"])

    def test_expired_outcomes_cannot_open_verified(self) -> None:
        records = [
            make_outcome_row(direction_bucket="buy_pressure", move_60s=None, adverse=None, status="expired")
            for _ in range(OPPORTUNITY_MIN_HISTORY_SAMPLES)
        ]
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=records), persistence_enabled=False)
        event = make_event(ts=1_710_000_130)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("history_completion_too_low", payload["trade_opportunity_primary_blocker"])


if __name__ == "__main__":
    unittest.main()
