import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_record, make_signal


def _record_id(event) -> str:
    return f"outcome:{event.intent_type}:{event.ts}"


class OpportunityProfileStatsTests(unittest.TestCase):
    def test_opportunity_generates_profile_key_and_completed_outcome_updates_profile_stats(self) -> None:
        event = make_event(ts=1_710_200_000)
        record_id = _record_id(event)
        state = StubStateManager(outcome_records={record_id: make_outcome_record(record_id=record_id)})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertTrue(payload["opportunity_profile_key"].startswith("ETH|LONG|broader_confirm"))
        stats = manager._profile_stats[payload["opportunity_profile_key"]]
        self.assertEqual(1, stats["sample_count"])
        self.assertEqual(1, stats["candidate_count"])
        self.assertEqual(1, stats["completed_60s"])
        self.assertEqual(1, stats["followthrough_60s_count"])
        self.assertEqual(0, stats["adverse_60s_count"])

    def test_sqlite_disabled_graceful_fallback_keeps_profile_stats_in_memory(self) -> None:
        event = make_event(ts=1_710_200_010)
        record_id = _record_id(event)
        state = StubStateManager(outcome_records={record_id: make_outcome_record(record_id=record_id)})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)

        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertIn(payload["opportunity_profile_key"], manager._profile_stats)
        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])


if __name__ == "__main__":
    unittest.main()
