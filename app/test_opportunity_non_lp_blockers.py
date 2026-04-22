import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_recent_signal_row, make_signal


class OpportunityNonLpBlockerTests(unittest.TestCase):
    def test_smart_money_exit_blocks_long_candidate(self) -> None:
        event_ts = 1_710_101_000
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="smart_money_exit_execution",
                        confidence=0.92,
                        confirmation_score=0.88,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("non_lp_opposite_smart_money", payload["trade_opportunity_primary_blocker"])

    def test_maker_distribute_blocks_long_candidate(self) -> None:
        event_ts = 1_710_101_100
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="market_maker_inventory_distribute",
                        strategy_role="market_maker_wallet",
                        confidence=0.90,
                        confirmation_score=0.87,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("non_lp_maker_against", payload["trade_opportunity_primary_blocker"])

    def test_clmm_position_exit_blocks_long_candidate(self) -> None:
        event_ts = 1_710_101_200
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="clmm_position_exit",
                        strategy_role="lp_pool",
                        confidence=0.88,
                        confirmation_score=0.84,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("non_lp_clmm_position_against", payload["trade_opportunity_primary_blocker"])

    def test_conflicting_strong_non_lp_evidence_blocks(self) -> None:
        event_ts = 1_710_101_300
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 90,
                        operational_intent_key="smart_money_entry_execution",
                        confidence=0.92,
                        confirmation_score=0.88,
                    ),
                    make_recent_signal_row(
                        operational_intent_key="market_maker_inventory_distribute",
                        strategy_role="market_maker_wallet",
                        ts=event_ts - 30,
                        confidence=0.90,
                        confirmation_score=0.86,
                    ),
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("non_lp_evidence_conflict", payload["trade_opportunity_primary_blocker"])


if __name__ == "__main__":
    unittest.main()
