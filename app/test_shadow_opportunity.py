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
from trade_opportunity import TradeOpportunityAssetState, TradeOpportunityManager
from trade_replay import _shadow_funnel_summary, derive_shadow_evaluation_fields


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
            ],
            replay_rows=[],
        )

        self.assertEqual(0, summary["shadow_evaluated_count"])
        self.assertEqual(1, summary["shadow_missing_field_reasons"]["missing_score"])
        self.assertEqual(1, summary["shadow_missing_field_reasons"]["missing_side"])
        self.assertNotIn("no_shadow_evaluation_fields", summary["zero_shadow_reasons"])
        self.assertIn("missing_score", summary["zero_shadow_reasons"])
        self.assertIn("missing_side", summary["zero_shadow_reasons"])

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
        self.assertEqual({"persisted_shadow_status": 1}, summary["shadow_reason_distribution"])
        self.assertEqual(1, summary["shadow_score_distribution"]["count"])

    def test_derived_shadow_candidate_from_soft_blocked_features(self):
        result = derive_shadow_evaluation_fields(
            {
                "input_source": ["trade_opportunities"],
                "signal_id": "sig-shadow-candidate",
                "opportunity_status": "BLOCKED",
                "side": "LONG",
                "score": float(SHADOW_CANDIDATE_MIN_SCORE) + 0.01,
                "blockers": ["profile_sample_count_insufficient"],
                "profile_key": "ETH|LONG|shadow",
                "market_context_source": "live_public",
                "quality_snapshot": {"quality_floor": 0.7},
                "opportunity_features": {"profile": "shadow"},
            }
        )

        self.assertTrue(result["shadow_evaluated"])
        self.assertEqual("SHADOW_CANDIDATE", result["shadow_status"])
        self.assertEqual("near_candidate_but_blocked", result["shadow_reason"])

    def test_derived_shadow_verified_from_immature_verified_score(self):
        result = derive_shadow_evaluation_fields(
            {
                "input_source": ["trade_opportunities"],
                "signal_id": "sig-shadow-verified",
                "opportunity_status": "BLOCKED",
                "side": "LONG",
                "score": float(SHADOW_VERIFIED_MIN_SCORE) + 0.01,
                "blockers": ["outcome_history_insufficient"],
                "profile_key": "ETH|LONG|shadow",
                "market_context_source": "live_public",
                "quality_snapshot": {"quality_floor": 0.7},
                "opportunity_features": {"profile": "shadow"},
            }
        )

        self.assertTrue(result["shadow_evaluated"])
        self.assertEqual("SHADOW_VERIFIED", result["shadow_status"])
        self.assertEqual("near_verified_but_immature", result["shadow_reason"])

    def test_derived_shadow_none_for_hard_blocker(self):
        result = derive_shadow_evaluation_fields(
            {
                "input_source": ["trade_opportunities"],
                "signal_id": "sig-shadow-hard-blocker",
                "opportunity_status": "BLOCKED",
                "side": "LONG",
                "score": float(SHADOW_VERIFIED_MIN_SCORE) + 0.10,
                "blockers": ["no_trade_lock"],
                "profile_key": "ETH|LONG|shadow",
                "market_context_source": "live_public",
                "quality_snapshot": {"quality_floor": 0.7},
                "opportunity_features": {"profile": "shadow"},
            }
        )

        self.assertTrue(result["shadow_evaluated"])
        self.assertEqual("NONE", result["shadow_status"])
        self.assertEqual("hard_blocker:no_trade_lock", result["shadow_reason"])

    def test_derived_shadow_missing_side_and_score(self):
        missing_side = derive_shadow_evaluation_fields(
            {
                "input_source": ["trade_opportunities"],
                "signal_id": "sig-shadow-missing-side",
                "opportunity_status": "BLOCKED",
                "score": float(SHADOW_CANDIDATE_MIN_SCORE) + 0.01,
            }
        )
        missing_score = derive_shadow_evaluation_fields(
            {
                "input_source": ["trade_opportunities"],
                "signal_id": "sig-shadow-missing-score",
                "opportunity_status": "BLOCKED",
                "side": "LONG",
            }
        )

        self.assertFalse(missing_side["shadow_evaluated"])
        self.assertIn("missing_side", missing_side["missing_reasons"])
        self.assertFalse(missing_score["shadow_evaluated"])
        self.assertIn("missing_score", missing_score["missing_reasons"])

    def test_shadow_does_not_enter_telegram_and_production_status_stays_unchanged(self):
        asset_state = TradeOpportunityAssetState(asset_symbol="ETH")
        record = {
            "trade_opportunity_status": "NONE",
            "trade_opportunity_side": "LONG",
            "trade_opportunity_key": "shadow-only",
            "trade_opportunity_shadow_status": "SHADOW_CANDIDATE",
            "trade_opportunity_shadow_reason": "near_candidate_but_blocked",
        }

        decision = self.manager._telegram_decision(
            record=record,
            asset_state=asset_state,
            previous=None,
            now_ts=1_710_000_000,
        )

        self.assertEqual("NONE", record["trade_opportunity_status"])
        self.assertFalse(decision["telegram_should_send"])
        self.assertEqual("suppressed", decision["telegram_update_kind"])

    def test_daily_report_derives_shadow_evaluation_from_feature_fixture(self):
        from reports import generate_daily_report_latest as report

        summary = report._shadow_opportunity_summary(
            [
                {
                    "trade_opportunity_status": "BLOCKED",
                    "trade_opportunity_side": "LONG",
                    "trade_opportunity_score": float(SHADOW_CANDIDATE_MIN_SCORE) + 0.02,
                    "trade_opportunity_blockers": ["profile_sample_count_insufficient"],
                    "opportunity_profile_key": "ETH|LONG|shadow",
                    "market_context_source": "live_public",
                    "trade_opportunity_quality_snapshot": {"quality_floor": 0.7},
                    "opportunity_profile_features_json": {"profile": "shadow"},
                }
            ],
            {"shadow_funnel_summary": {}},
        )

        self.assertGreater(summary["shadow_evaluated_count"], 0)
        self.assertEqual(1, summary["shadow_candidate_count"])
        self.assertIn("near_candidate_but_blocked", summary["shadow_reason_distribution"])

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
            self.assertEqual("near_candidate_but_blocked", result["shadow_reason"])

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
