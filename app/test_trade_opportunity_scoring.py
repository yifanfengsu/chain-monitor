import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from config import OPPORTUNITY_MIN_CANDIDATE_SCORE
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_signal


class TradeOpportunityScoringTests(unittest.TestCase):
    def test_broader_confirm_live_context_clean_quality_scores_high_candidate(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_000)
        signal = make_signal(event)

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("CANDIDATE", payload["trade_opportunity_status"])
        self.assertIn("opportunity_raw_score", payload)
        self.assertIn("opportunity_calibrated_score", payload)
        self.assertGreaterEqual(payload["trade_opportunity_score"], OPPORTUNITY_MIN_CANDIDATE_SCORE)
        self.assertEqual("candidate", payload["telegram_update_kind"])

    def test_local_confirm_only_stays_below_candidate_gate(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_010)
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_broader_alignment="weak_or_missing",
            lp_absorption_context="pool_only_unconfirmed_pressure",
            trade_action_key="WAIT_CONFIRMATION",
            trade_action_label="等待确认",
            asset_market_state_key="WAIT_CONFIRMATION",
            asset_market_state_label="等待确认",
            trade_action_debug={"strength_score": 0.30},
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("NONE", payload["trade_opportunity_status"])
        self.assertLess(payload["trade_opportunity_score"], OPPORTUNITY_MIN_CANDIDATE_SCORE)
        self.assertFalse(payload["telegram_should_send"])

    def test_data_gap_is_hard_blocker(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_020)
        signal = make_signal(
            event,
            market_context_source="unavailable",
            market_context_venue="",
            alert_relative_timing="",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("data_gap", payload["trade_opportunity_primary_blocker"])
        self.assertEqual("risk_blocker", payload["telegram_update_kind"])

    def test_no_trade_lock_is_hard_blocker(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_030)
        signal = make_signal(
            event,
            no_trade_lock_active=True,
            no_trade_lock_reason="double_sided_conflict",
            asset_market_state_key="NO_TRADE_LOCK",
            asset_market_state_label="不交易锁定",
            trade_action_key="CONFLICT_NO_TRADE",
            trade_action_label="不交易",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("no_trade_lock", payload["trade_opportunity_primary_blocker"])

    def test_sweep_exhaustion_is_hard_blocker(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_040)
        signal = make_signal(
            event,
            lp_alert_stage="exhaustion_risk",
            lp_sweep_phase="sweep_exhaustion_risk",
            trade_action_key="DO_NOT_CHASE_LONG",
            trade_action_label="不追多",
            trade_action_reason="高潮后回吐风险",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("sweep_exhaustion_risk", payload["trade_opportunity_primary_blocker"])

    def test_crowded_basis_does_not_become_opportunity(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_050)
        signal = make_signal(
            event,
            basis_bps=28.0,
            mark_index_spread_bps=12.0,
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("crowded_basis", payload["trade_opportunity_primary_blocker"])


if __name__ == "__main__":
    unittest.main()
