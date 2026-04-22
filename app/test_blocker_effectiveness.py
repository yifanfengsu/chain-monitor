import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_record, make_signal


def _record_id(event) -> str:
    return f"outcome:{event.intent_type}:{event.ts}"


class BlockerEffectivenessTests(unittest.TestCase):
    def test_blocker_saved_trade_is_true_when_blocked_direction_turns_adverse(self) -> None:
        event = make_event(ts=1_710_240_000)
        record_id = _record_id(event)
        state = StubStateManager(
            records=[],
            outcome_records={
                record_id: make_outcome_record(
                    record_id=record_id,
                    move_60s=-0.004,
                    adverse_60s=True,
                )
            },
        )
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)
        signal = make_signal(event, market_context_source="unavailable", market_context_venue="", alert_relative_timing="")

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertTrue(payload["blocker_saved_trade"])
        self.assertFalse(payload["blocker_false_block_possible"])

    def test_blocker_false_block_possible_is_true_when_blocked_direction_would_have_worked(self) -> None:
        event = make_event(intent_type="pool_sell_pressure", ts=1_710_240_100)
        record_id = _record_id(event)
        state = StubStateManager(
            records=[],
            outcome_records={
                record_id: make_outcome_record(
                    record_id=record_id,
                    move_60s=0.005,
                    adverse_60s=False,
                )
            },
        )
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)
        signal = make_signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_key="DO_NOT_CHASE_SHORT",
            trade_action_label="不追空",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertFalse(payload["blocker_saved_trade"])
        self.assertTrue(payload["blocker_false_block_possible"])


if __name__ == "__main__":
    unittest.main()
