import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from config import OPPORTUNITY_MIN_HISTORY_SAMPLES
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_record, make_signal


def _record_id(event) -> str:
    return f"outcome:{event.intent_type}:{event.ts}"


class OpportunityPosteriorAccumulationTests(unittest.TestCase):
    def test_profile_accumulation_unlocks_verified(self) -> None:
        state = StubStateManager(records=[], outcome_records={})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)

        for offset in range(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            event = make_event(ts=1_710_210_000 + offset)
            record_id = _record_id(event)
            state._outcome_records[record_id] = make_outcome_record(record_id=record_id)
            payload = manager.apply_lp_signal(event, make_signal(event))
            self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])

        verified_event = make_event(ts=1_710_210_100)
        verified_record_id = _record_id(verified_event)
        state._outcome_records[verified_record_id] = make_outcome_record(record_id=verified_record_id)

        payload = manager.apply_lp_signal(verified_event, make_signal(verified_event))

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("opportunity_profile", payload["trade_opportunity_history_snapshot"]["history_source"])
        self.assertEqual(OPPORTUNITY_MIN_HISTORY_SAMPLES, payload["trade_opportunity_history_snapshot"]["sample_size"])

    def test_profile_sample_insufficient_falls_back_to_asset_side(self) -> None:
        state = StubStateManager(records=[], outcome_records={})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)

        for offset in range(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            event = make_event(ts=1_710_220_000 + offset)
            record_id = _record_id(event)
            state._outcome_records[record_id] = make_outcome_record(record_id=record_id)
            manager.apply_lp_signal(event, make_signal(event))

        fallback_event = make_event(ts=1_710_220_100)
        fallback_record_id = _record_id(fallback_event)
        state._outcome_records[fallback_record_id] = make_outcome_record(record_id=fallback_record_id)
        fallback_signal = make_signal(fallback_event, alert_relative_timing="leading")

        payload = manager.apply_lp_signal(fallback_event, fallback_signal)

        self.assertEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertTrue(payload["trade_opportunity_history_snapshot"]["fallback_used"])
        self.assertEqual("asset_side", payload["trade_opportunity_history_snapshot"]["history_scope_type"])


if __name__ == "__main__":
    unittest.main()
