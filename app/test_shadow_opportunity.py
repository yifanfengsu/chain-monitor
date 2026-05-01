"""Test shadow opportunity layer."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from config import (
    SHADOW_CANDIDATE_MIN_SCORE,
    SHADOW_MAX_60S_ADVERSE_RATE,
    SHADOW_MIN_60S_FOLLOWTHROUGH_RATE,
    SHADOW_MIN_HISTORY_SAMPLES,
    SHADOW_MIN_OUTCOME_COMPLETION_RATE,
    SHADOW_OPPORTUNITY_ENABLE,
    SHADOW_REQUIRE_BROADER_CONFIRM,
    SHADOW_REQUIRE_LIVE_CONTEXT,
    SHADOW_REQUIRE_OUTCOME_HISTORY,
    SHADOW_VERIFIED_MIN_SCORE,
)
from trade_opportunity import TradeOpportunityManager
from trade_replay import _shadow_funnel_summary


class TestShadowOpportunity(unittest.TestCase):
    """Test shadow opportunity evaluation logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = TradeOpportunityManager(
            state_manager=MagicMock(),
        )

    def test_shadow_disabled(self):
        """Test shadow returns NONE when disabled."""
        if not SHADOW_OPPORTUNITY_ENABLE:
            self.skipTest("SHADOW_OPPORTUNITY_ENABLE is False")
        
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
        }
        history = {"sample_size": 0}
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=0.70,
            candidate_ready=False,
            verified_ready=False,
            production_status="NONE",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["outcome_history_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Should return valid shadow result
        self.assertIn("shadow_status", result)
        self.assertIn("shadow_reason", result)
        self.assertIn("shadow_score", result)

    def test_shadow_funnel_counts_missing_fields_separately(self):
        summary = _shadow_funnel_summary(
            candidates=[
                {
                    "input_source": ["trade_opportunities"],
                    "signal_id": "sig-shadow-missing-score",
                    "asset": "ETH",
                    "side": "LONG",
                    "profile_key": "ETH|LONG|shadow",
                    "market_context_source": "live_public",
                    "quality_snapshot": {"ok": True},
                    "opportunity_features": {"feature": True},
                    "shadow_status": "NONE",
                    "shadow_evaluated": False,
                },
                {
                    "input_source": ["trade_opportunities"],
                    "signal_id": "sig-shadow-missing-side",
                    "asset": "ETH",
                    "score": 0.7,
                    "profile_key": "ETH|LONG|shadow",
                    "market_context_source": "live_public",
                    "quality_snapshot": {"ok": True},
                    "opportunity_features": {"feature": True},
                    "shadow_status": "NONE",
                    "shadow_evaluated": False,
                },
                {
                    "input_source": ["trade_opportunities"],
                    "signal_id": "sig-shadow-missing-market",
                    "asset": "ETH",
                    "side": "LONG",
                    "score": 0.7,
                    "profile_key": "ETH|LONG|shadow",
                    "quality_snapshot": {"ok": True},
                    "opportunity_features": {"feature": True},
                    "shadow_status": "NONE",
                    "shadow_evaluated": False,
                },
            ],
            replay_rows=[],
        )

        self.assertEqual(0, summary["shadow_evaluated_count"])
        self.assertEqual(1, summary["shadow_missing_field_reasons"]["missing_score"])
        self.assertEqual(1, summary["shadow_missing_field_reasons"]["missing_side"])
        self.assertEqual(1, summary["shadow_missing_field_reasons"]["missing_market_context"])
        self.assertIn("no_shadow_evaluation_fields", summary["zero_shadow_reasons"])

    def test_shadow_funnel_counts_evaluated_fixture(self):
        summary = _shadow_funnel_summary(
            candidates=[
                {
                    "input_source": ["trade_opportunities"],
                    "signal_id": "sig-shadow",
                    "asset": "ETH",
                    "side": "LONG",
                    "score": 0.8,
                    "profile_key": "ETH|LONG|shadow",
                    "market_context_source": "live_public",
                    "quality_snapshot": {"ok": True},
                    "opportunity_features": {"feature": True},
                    "shadow_status": "SHADOW_CANDIDATE",
                    "shadow_evaluated": True,
                }
            ],
            replay_rows=[],
        )

        self.assertEqual(1, summary["shadow_input_count"])
        self.assertEqual(1, summary["shadow_evaluated_count"])
        self.assertEqual(1, summary["shadow_candidate_count"])

    def test_shadow_candidate_when_production_blocked(self):
        """Test shadow candidate when production is blocked by history."""
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
            "asset_case_quality_score": 0.70,
            "pair_quality_score": 0.65,
            "pool_quality_score": 0.68,
        }
        history = {
            "sample_size": 0,
            "completion_rate": 0.0,
            "followthrough_rate": 0.0,
            "adverse_rate": 0.0,
        }
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=float(SHADOW_CANDIDATE_MIN_SCORE) + 0.05,
            candidate_ready=False,
            verified_ready=False,
            production_status="BLOCKED",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["outcome_history_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Should be SHADOW_CANDIDATE when production blocked but score sufficient
        if SHADOW_OPPORTUNITY_ENABLE:
            self.assertEqual(result["shadow_status"], "SHADOW_CANDIDATE")
            self.assertIn("production_gate_blocked", result["shadow_reason"])

    def test_shadow_verified_when_production_none_but_history_sufficient(self):
        """Test shadow verified when production is NONE but shadow history gate passes."""
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
            "asset_case_quality_score": 0.75,
            "pair_quality_score": 0.70,
            "pool_quality_score": 0.72,
        }
        history = {
            "sample_size": int(SHADOW_MIN_HISTORY_SAMPLES) + 5,
            "completion_rate": float(SHADOW_MIN_OUTCOME_COMPLETION_RATE) + 0.10,
            "followthrough_rate": float(SHADOW_MIN_60S_FOLLOWTHROUGH_RATE) + 0.10,
            "adverse_rate": float(SHADOW_MAX_60S_ADVERSE_RATE) - 0.10,
        }
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=float(SHADOW_VERIFIED_MIN_SCORE) + 0.05,
            candidate_ready=False,
            verified_ready=False,
            production_status="NONE",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["profile_sample_count_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Should be SHADOW_VERIFIED when shadow history gate passes
        if SHADOW_OPPORTUNITY_ENABLE and SHADOW_REQUIRE_OUTCOME_HISTORY:
            self.assertEqual(result["shadow_status"], "SHADOW_VERIFIED")

    def test_shadow_none_when_production_verified(self):
        """Test shadow returns NONE when production is already VERIFIED."""
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
        }
        history = {"sample_size": 25, "completion_rate": 0.80, "followthrough_rate": 0.65, "adverse_rate": 0.25}
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=0.85,
            candidate_ready=True,
            verified_ready=True,
            production_status="VERIFIED",
            hard_blockers=[],
            verification_gate={"status": "verified_ready", "blockers": []},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Should be NONE when production already VERIFIED
        self.assertEqual(result["shadow_status"], "NONE")

    def test_shadow_none_when_hard_blocker_present(self):
        """Test shadow returns NONE when hard blocker is present."""
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
            "no_trade_lock_active": True,
        }
        history = {"sample_size": 10, "completion_rate": 0.75, "followthrough_rate": 0.60, "adverse_rate": 0.30}
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=0.75,
            candidate_ready=False,
            verified_ready=False,
            production_status="BLOCKED",
            hard_blockers=["no_trade_lock"],
            verification_gate={"status": "blocked", "blockers": []},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Hard blockers should prevent shadow opportunity
        # (no_trade_lock is a hard blocker that even shadow respects)
        self.assertEqual(result["shadow_status"], "NONE")

    def test_shadow_candidate_score_threshold(self):
        """Test shadow candidate requires minimum score."""
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "live_public",
            "lp_confirm_scope": "broader_confirm",
            "asset_case_quality_score": 0.70,
            "pair_quality_score": 0.65,
            "pool_quality_score": 0.68,
        }
        history = {"sample_size": 0}
        
        # Below threshold
        result_low = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=float(SHADOW_CANDIDATE_MIN_SCORE) - 0.05,
            candidate_ready=False,
            verified_ready=False,
            production_status="NONE",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["outcome_history_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        # Above threshold
        result_high = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=float(SHADOW_CANDIDATE_MIN_SCORE) + 0.05,
            candidate_ready=False,
            verified_ready=False,
            production_status="NONE",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["outcome_history_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=True,
            broader_confirm=True,
        )
        
        if SHADOW_OPPORTUNITY_ENABLE:
            self.assertEqual(result_low["shadow_status"], "NONE")
            self.assertIn(result_high["shadow_status"], {"SHADOW_CANDIDATE", "SHADOW_VERIFIED"})

    def test_shadow_respects_live_context_requirement(self):
        """Test shadow respects SHADOW_REQUIRE_LIVE_CONTEXT setting."""
        if not SHADOW_REQUIRE_LIVE_CONTEXT:
            self.skipTest("SHADOW_REQUIRE_LIVE_CONTEXT is False")
        
        summary = {
            "trade_opportunity_side": "LONG",
            "market_context_source": "unavailable",
            "lp_confirm_scope": "broader_confirm",
            "asset_case_quality_score": 0.70,
            "pair_quality_score": 0.65,
            "pool_quality_score": 0.68,
        }
        history = {"sample_size": 0}
        
        result = self.manager._evaluate_shadow_opportunity(
            summary=summary,
            history=history,
            side="LONG",
            calibrated_score=0.70,
            candidate_ready=False,
            verified_ready=False,
            production_status="NONE",
            hard_blockers=[],
            verification_gate={"status": "blocked", "blockers": ["outcome_history_insufficient"]},
            strong_directional=True,
            preliminary_quality_pass=True,
            live_context=False,
            broader_confirm=True,
        )
        
        # Should be NONE when live context required but not available
        self.assertEqual(result["shadow_status"], "NONE")
        self.assertIn("shadow_require_live_context", result["shadow_reason"])


if __name__ == "__main__":
    unittest.main()
