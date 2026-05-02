from __future__ import annotations

import unittest

from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


def _profile_key(manager: TradeOpportunityManager, event, signal) -> str:
    summary = manager._summary(event, signal)
    profile = manager._build_profile(summary=summary, side=str(summary.get("trade_opportunity_side") or "NONE"))
    return str(profile["opportunity_profile_key"])


class TradeOpportunityReplayGateTests(unittest.TestCase):
    def _manager(self, records=None) -> TradeOpportunityManager:
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(records=records or []),
            persistence_enabled=False,
        )
        manager._mirror_sqlite_opportunity = lambda _payload: None
        return manager

    def test_high_confidence_negative_profile_blocks_candidate_and_verified(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertEqual("replay_profile_negative", payload["trade_opportunity_primary_hard_blocker"])
        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])

    def test_low_sample_positive_profile_does_not_promote_verified(self) -> None:
        manager = self._manager(records=[])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 1,
            "avg_net_pnl_bps": 70.38,
            "win_rate": 1.0,
            "clean_followthrough_rate": 1.0,
            "chop_rate": 0.0,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertNotIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertEqual("positive_profile_needs_more_samples", payload["trade_opportunity_replay_profile_research_hint"])

    def test_replay_blocker_does_not_affect_non_matching_profile(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        manager._replay_profile_gate_stats["ETH|LONG|other|profile"] = {
            "profile_key": "ETH|LONG|other|profile",
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertNotEqual("replay_profile_negative", payload["trade_opportunity_primary_hard_blocker"])

    def test_local_absorption_quality_low_with_negative_profile_has_replay_blocker(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_absorption_context="local_buy_pressure_absorption",
            asset_case_supporting_pair_count=1,
            lp_multi_pool_resonance=1,
            pool_quality_score=0.40,
            pair_quality_score=0.42,
            asset_case_quality_score=0.44,
            trade_action_key="DO_NOT_CHASE_LONG",
            asset_market_state_key="DO_NOT_CHASE_LONG",
        )
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertIn("local_absorption_quality_low", payload["trade_opportunity_risk_flags"])

    def test_positive_profile_does_not_auto_verified(self) -> None:
        manager = self._manager(records=[])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 30,
            "avg_net_pnl_bps": 15.5,
            "win_rate": 0.70,
            "clean_followthrough_rate": 0.60,
            "chop_rate": 0.10,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("positive_profile_research_only", payload["trade_opportunity_replay_profile_research_hint"])


if __name__ == "__main__":
    unittest.main()
