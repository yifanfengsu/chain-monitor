import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_recent_signal_row, make_signal


class OpportunityNonLpEvidenceTests(unittest.TestCase):
    def test_long_candidate_gets_small_boost_from_smart_money_entry(self) -> None:
        event_ts = 1_710_100_000
        base_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        supported_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="smart_money_entry_execution",
                        confidence=0.90,
                        confirmation_score=0.88,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        signal = make_signal(event)
        supported_event = make_event(ts=event_ts)
        supported_signal = make_signal(supported_event)

        base_payload = base_manager.apply_lp_signal(event, signal)
        supported_payload = supported_manager.apply_lp_signal(supported_event, supported_signal)

        self.assertEqual("CANDIDATE", supported_payload["trade_opportunity_status"])
        self.assertGreater(supported_payload["trade_opportunity_score"], base_payload["trade_opportunity_score"])
        self.assertIn("smart_money", supported_payload["trade_opportunity_non_lp_evidence_context"]["non_lp_support_families"])

    def test_short_candidate_gets_boost_from_maker_distribute(self) -> None:
        event_ts = 1_710_100_100
        base_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        supported_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="market_maker_inventory_distribute",
                        strategy_role="market_maker_wallet",
                        confidence=0.90,
                        confirmation_score=0.86,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(intent_type="pool_sell_pressure", ts=event_ts)
        supported_event = make_event(intent_type="pool_sell_pressure", ts=event_ts)

        base_payload = base_manager.apply_lp_signal(event, make_signal(event))
        supported_payload = supported_manager.apply_lp_signal(supported_event, make_signal(supported_event))

        self.assertEqual("CANDIDATE", supported_payload["trade_opportunity_status"])
        self.assertGreater(supported_payload["trade_opportunity_score"], base_payload["trade_opportunity_score"])
        self.assertIn("maker", supported_payload["trade_opportunity_non_lp_evidence_context"]["non_lp_support_families"])

    def test_clmm_partial_support_does_not_add_directional_score(self) -> None:
        event_ts = 1_710_100_200
        base_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        partial_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="clmm_partial_support_observation",
                        confidence=0.44,
                        confirmation_score=0.24,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        partial_event = make_event(ts=event_ts)
        base_payload = base_manager.apply_lp_signal(event, make_signal(event))
        partial_payload = partial_manager.apply_lp_signal(partial_event, make_signal(partial_event))

        self.assertAlmostEqual(base_payload["trade_opportunity_score"], partial_payload["trade_opportunity_score"], places=4)
        self.assertEqual(0.5, partial_payload["trade_opportunity_non_lp_component_score"])
        self.assertTrue(partial_payload["trade_opportunity_non_lp_evidence_context"]["non_lp_evidence_available"])

    def test_clmm_recenter_can_support_long(self) -> None:
        event_ts = 1_710_100_300
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="clmm_range_recenter",
                        strategy_role="lp_pool",
                        confidence=0.86,
                        confirmation_score=0.82,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        payload = manager.apply_lp_signal(event, make_signal(event))

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertIn("clmm", payload["trade_opportunity_non_lp_evidence_context"]["non_lp_support_families"])

    def test_tentative_evidence_is_lower_weight_than_strong(self) -> None:
        event_ts = 1_710_100_400
        strong_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="smart_money_entry_execution",
                        confidence=0.90,
                        confirmation_score=0.88,
                    )
                ]
            ),
            persistence_enabled=False,
        )
        tentative_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="smart_money_entry_execution",
                        confidence=0.50,
                        confirmation_score=0.48,
                        intent_stage="candidate",
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        strong_payload = strong_manager.apply_lp_signal(event, make_signal(event))
        tentative_event = make_event(ts=event_ts)
        tentative_payload = tentative_manager.apply_lp_signal(tentative_event, make_signal(tentative_event))

        self.assertGreater(
            strong_payload["trade_opportunity_non_lp_score_delta"],
            tentative_payload["trade_opportunity_non_lp_score_delta"],
        )
        self.assertGreater(tentative_payload["trade_opportunity_non_lp_score_delta"], 0.0)

    def test_observe_evidence_does_not_add_score(self) -> None:
        event_ts = 1_710_100_500
        base_manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        observe_manager = TradeOpportunityManager(
            state_manager=StubStateManager(
                recent_signal_records=[
                    make_recent_signal_row(
                        ts=event_ts - 60,
                        operational_intent_key="exchange_external_inflow_observation",
                        confidence=0.42,
                        confirmation_score=0.36,
                        intent_stage="observe",
                    )
                ]
            ),
            persistence_enabled=False,
        )
        event = make_event(ts=event_ts)
        observe_event = make_event(ts=event_ts)
        base_payload = base_manager.apply_lp_signal(event, make_signal(event))
        observe_payload = observe_manager.apply_lp_signal(observe_event, make_signal(observe_event))

        self.assertAlmostEqual(base_payload["trade_opportunity_score"], observe_payload["trade_opportunity_score"], places=4)
        self.assertEqual(0.5, observe_payload["trade_opportunity_non_lp_component_score"])


if __name__ == "__main__":
    unittest.main()
