import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from config import OPPORTUNITY_MIN_HISTORY_SAMPLES
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_record, make_signal


def _record_id(event) -> str:
    return f"outcome:{event.intent_type}:{event.ts}"


class OpportunityVerificationBlockerTests(unittest.TestCase):
    def test_history_insufficient_does_not_block_candidate(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(records=[], outcome_records={}), persistence_enabled=False)
        event = make_event(ts=1_710_230_000)

        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertEqual("", payload["trade_opportunity_primary_hard_blocker"])
        self.assertEqual("outcome_history_insufficient", payload["trade_opportunity_primary_verification_blocker"])

    def test_low_completion_only_blocks_verified_not_candidate(self) -> None:
        state = StubStateManager(records=[], outcome_records={})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)

        for offset in range(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            event = make_event(ts=1_710_230_100 + offset)
            record_id = _record_id(event)
            state._outcome_records[record_id] = make_outcome_record(
                record_id=record_id,
                move_30s=None,
                move_60s=None,
                move_300s=None,
                adverse_30s=None,
                adverse_60s=None,
                adverse_300s=None,
                status_30s="expired",
                status_60s="expired",
                status_300s="expired",
            )
            manager.apply_lp_signal(event, make_signal(event))

        event = make_event(ts=1_710_230_500)
        record_id = _record_id(event)
        state._outcome_records[record_id] = make_outcome_record(record_id=record_id)

        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertEqual("profile_completion_too_low", payload["trade_opportunity_primary_verification_blocker"])

    def test_good_profile_metrics_allow_verified(self) -> None:
        state = StubStateManager(records=[], outcome_records={})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)

        for offset in range(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            event = make_event(ts=1_710_231_000 + offset)
            record_id = _record_id(event)
            state._outcome_records[record_id] = make_outcome_record(record_id=record_id)
            manager.apply_lp_signal(event, make_signal(event))

        event = make_event(ts=1_710_231_100)
        record_id = _record_id(event)
        state._outcome_records[record_id] = make_outcome_record(record_id=record_id)

        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("", payload["trade_opportunity_primary_verification_blocker"])


if __name__ == "__main__":
    unittest.main()
