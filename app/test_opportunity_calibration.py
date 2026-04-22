import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from config import OPPORTUNITY_MIN_CANDIDATE_SCORE
from opportunity_calibration import calibrate_opportunity_score
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_signal


def _profile_row(
    *,
    profile_key: str = "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
    sample_count: int = 60,
    completed_60s: int = 54,
    followthrough_60s_count: int = 40,
    adverse_60s_count: int = 8,
    blocked_completed_60s: int = 10,
    blocker_saved_trade_60s_count: int = 6,
    blocker_false_block_60s_count: int = 2,
) -> dict:
    return {
        "profile_key": profile_key,
        "profile_version": "v1",
        "side": "LONG",
        "asset": "ETH",
        "pair_family": "USDC",
        "strategy": "lp_continuation",
        "features_json": {},
        "sample_count": sample_count,
        "candidate_count": min(sample_count, 20),
        "verified_count": min(sample_count, 8),
        "blocked_count": min(sample_count, 4),
        "completed_60s": completed_60s,
        "followthrough_60s_count": followthrough_60s_count,
        "adverse_60s_count": adverse_60s_count,
        "blocked_completed_60s": blocked_completed_60s,
        "blocker_saved_trade_60s_count": blocker_saved_trade_60s_count,
        "blocker_false_block_60s_count": blocker_false_block_60s_count,
        "last_updated": 1_710_000_000,
    }


class OpportunityCalibrationTests(unittest.TestCase):
    def test_sample_insufficient_does_not_allow_positive_adjustment(self) -> None:
        payload = calibrate_opportunity_score(
            raw_score=0.66,
            profile_key="p1",
            asset="ETH",
            side="LONG",
            components={},
            pair_family="USDC",
            calibration_rows=[],
            profile_rows=[
                _profile_row(
                    profile_key="p1",
                    sample_count=10,
                    completed_60s=9,
                    followthrough_60s_count=8,
                    adverse_60s_count=1,
                    blocked_completed_60s=0,
                    blocker_saved_trade_60s_count=0,
                    blocker_false_block_60s_count=0,
                )
            ],
        )

        self.assertEqual(0.0, payload["opportunity_calibration_adjustment"])
        self.assertEqual(0.66, payload["opportunity_calibrated_score"])

    def test_high_adverse_profile_lowers_score(self) -> None:
        payload = calibrate_opportunity_score(
            raw_score=0.76,
            profile_key="p2",
            asset="ETH",
            side="LONG",
            components={},
            pair_family="USDC",
            calibration_rows=[],
            profile_rows=[
                _profile_row(
                    profile_key="p2",
                    sample_count=60,
                    completed_60s=54,
                    followthrough_60s_count=36,
                    adverse_60s_count=28,
                )
            ],
        )

        self.assertLess(payload["opportunity_calibration_adjustment"], 0.0)
        self.assertLess(payload["opportunity_calibrated_score"], 0.76)

    def test_low_completion_lowers_score(self) -> None:
        payload = calibrate_opportunity_score(
            raw_score=0.74,
            profile_key="p3",
            asset="ETH",
            side="LONG",
            components={},
            pair_family="USDC",
            calibration_rows=[],
            profile_rows=[
                _profile_row(
                    profile_key="p3",
                    sample_count=60,
                    completed_60s=24,
                    followthrough_60s_count=18,
                    adverse_60s_count=5,
                )
            ],
        )

        self.assertLess(payload["opportunity_calibration_adjustment"], 0.0)
        self.assertIn("completion", payload["opportunity_calibration_reason"])

    def test_adjustment_is_clamped_to_configured_bounds(self) -> None:
        payload = calibrate_opportunity_score(
            raw_score=0.78,
            profile_key="p4",
            asset="ETH",
            side="LONG",
            components={},
            pair_family="USDC",
            calibration_rows=[],
            profile_rows=[
                _profile_row(
                    profile_key="p4",
                    sample_count=80,
                    completed_60s=76,
                    followthrough_60s_count=72,
                    adverse_60s_count=0,
                    blocked_completed_60s=20,
                    blocker_saved_trade_60s_count=0,
                    blocker_false_block_60s_count=20,
                )
            ],
        )

        self.assertLessEqual(payload["opportunity_calibration_adjustment"], 0.08)
        self.assertGreaterEqual(payload["opportunity_calibration_adjustment"], -0.12)

    def test_calibrated_score_is_used_for_candidate_gate(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_100)
        signal = make_signal(event, lp_confirm_quality="")
        summary = manager._summary(event, signal)
        summary.update(manager._build_profile(summary=summary, side=str(summary.get("trade_opportunity_side") or "NONE")))

        result = manager._evaluate(
            summary=summary,
            history=manager._empty_history_metrics("none"),
            score_payload={
                "trade_opportunity_score": 0.70,
                "trade_opportunity_score_without_non_lp": 0.66,
                "trade_opportunity_non_lp_score_delta": 0.0,
                "trade_opportunity_non_lp_component_score": 0.5,
                "trade_opportunity_score_components": {},
                "opportunity_raw_score": 0.66,
                "opportunity_calibrated_score": 0.70,
                "opportunity_calibration_adjustment": 0.04,
                "opportunity_calibration_reason": "followthrough>0.65",
                "opportunity_calibration_sample_count": 60,
                "opportunity_calibration_confidence": 0.9,
                "opportunity_calibration_source": "profile",
                "trade_opportunity_calibration_snapshot": {"opportunity_calibration_reason": "followthrough>0.65"},
            },
        )

        self.assertEqual("CANDIDATE", result["trade_opportunity_status"])
        self.assertLess(result["opportunity_raw_score"], OPPORTUNITY_MIN_CANDIDATE_SCORE)
        self.assertGreaterEqual(result["opportunity_calibrated_score"], OPPORTUNITY_MIN_CANDIDATE_SCORE)

    def test_calibration_does_not_bypass_hard_blocker(self) -> None:
        manager = TradeOpportunityManager(state_manager=StubStateManager(), persistence_enabled=False)
        event = make_event(ts=1_710_000_120)
        signal = make_signal(event, market_context_source="unavailable", market_context_venue="", alert_relative_timing="")
        summary = manager._summary(event, signal)
        summary.update(manager._build_profile(summary=summary, side=str(summary.get("trade_opportunity_side") or "NONE")))

        result = manager._evaluate(
            summary=summary,
            history=manager._empty_history_metrics("none"),
            score_payload={
                "trade_opportunity_score": 0.90,
                "trade_opportunity_score_without_non_lp": 0.82,
                "trade_opportunity_non_lp_score_delta": 0.02,
                "trade_opportunity_non_lp_component_score": 0.7,
                "trade_opportunity_score_components": {},
                "opportunity_raw_score": 0.78,
                "opportunity_calibrated_score": 0.90,
                "opportunity_calibration_adjustment": 0.08,
                "opportunity_calibration_reason": "followthrough>0.65",
                "opportunity_calibration_sample_count": 60,
                "opportunity_calibration_confidence": 0.9,
                "opportunity_calibration_source": "profile",
                "trade_opportunity_calibration_snapshot": {"opportunity_calibration_reason": "followthrough>0.65"},
            },
        )

        self.assertEqual("BLOCKED", result["trade_opportunity_status"])
        self.assertEqual("data_gap", result["trade_opportunity_primary_blocker"])


if __name__ == "__main__":
    unittest.main()
