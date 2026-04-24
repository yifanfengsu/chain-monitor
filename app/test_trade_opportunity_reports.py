import tempfile
import unittest
from pathlib import Path

from config import OPPORTUNITY_MIN_HISTORY_SAMPLES
from reports.generate_overnight_run_analysis_latest import compute_trade_opportunities
import sqlite_store


def _row(
    status: str,
    *,
    score: float,
    blocker: str = "",
    verification_blocker: str = "",
    profile_key: str = "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
    sample_size: int = 0,
    outcome_status: str = "pending",
    followthrough: bool | None = None,
    adverse: bool | None = None,
    result_label: str = "neutral",
    suppression_reason: str = "",
) -> dict:
    return {
        "trade_opportunity_id": f"opp:{status}:{score}:{blocker}:{sample_size}:{outcome_status}",
        "trade_opportunity_status": status,
        "trade_opportunity_status_at_creation": status,
        "trade_opportunity_key": f"key:{status}:{score}:{blocker}",
        "trade_opportunity_side": "LONG",
        "trade_opportunity_score": score,
        "trade_opportunity_primary_blocker": blocker,
        "trade_opportunity_primary_hard_blocker": blocker,
        "trade_opportunity_primary_verification_blocker": verification_blocker,
        "opportunity_profile_key": profile_key,
        "opportunity_profile_pair_family": "USDC",
        "trade_opportunity_history_snapshot": {"sample_size": sample_size},
        "opportunity_outcome_60s": outcome_status,
        "opportunity_followthrough_60s": followthrough,
        "opportunity_adverse_60s": adverse,
        "opportunity_result_label": result_label,
        "blocker_saved_trade": adverse if status == "BLOCKED" else None,
        "blocker_false_block_possible": (followthrough is True and adverse is False) if status == "BLOCKED" else None,
        "telegram_suppression_reason": suppression_reason,
        "trade_opportunity_created_at": 1_710_000_000,
        "asset_symbol": "ETH",
    }


class TradeOpportunityReportTests(unittest.TestCase):
    def test_opportunity_summary_and_outcomes_are_reported(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("CANDIDATE", score=0.71, sample_size=8, outcome_status="completed", followthrough=True, adverse=False, result_label="success"),
                    _row("VERIFIED", score=0.84, sample_size=24, outcome_status="completed", followthrough=True, adverse=False, result_label="success"),
                    _row("BLOCKED", score=0.66, blocker="data_gap", sample_size=24, outcome_status="completed", followthrough=False, adverse=True, result_label="adverse"),
                    _row("NONE", score=0.42, sample_size=0),
                ]
            }
        )

        self.assertEqual(1, payload["opportunity_candidate_count"])
        self.assertEqual(1, payload["opportunity_verified_count"])
        self.assertEqual(1, payload["opportunity_blocked_count"])
        self.assertEqual(1.0, payload["opportunity_verified_followthrough_60s_rate"])
        self.assertEqual(0.0, payload["opportunity_candidate_adverse_60s_rate"])
        self.assertEqual(1.0, payload["opportunity_blocker_avoided_adverse_rate"])
        self.assertEqual(1, payload["opportunity_profile_count"])
        self.assertEqual({"data_gap": 1}, payload["top_blockers"])

    def test_no_opportunities_reason_is_explained(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("CANDIDATE", score=0.61, verification_blocker="profile_sample_count_insufficient", sample_size=0),
                    _row("BLOCKED", score=0.58, blocker="data_gap", sample_size=0),
                    _row("NONE", score=0.45, sample_size=0, profile_key=""),
                ]
            }
        )

        self.assertIn("history_samples_insufficient", payload["why_no_opportunities"])
        self.assertIn("data_gap", str(payload["why_no_opportunities"]))
        self.assertEqual({"profile_sample_count_insufficient": 1}, payload["verification_blocker_distribution"])

    def test_budget_and_cooldown_suppression_are_reported(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("CANDIDATE", score=0.72, sample_size=5, suppression_reason="opportunity_budget_exhausted"),
                    _row("VERIFIED", score=0.83, sample_size=25, suppression_reason="opportunity_cooldown_active"),
                ]
            }
        )

        self.assertEqual(1, payload["opportunity_budget_suppressed_count"])
        self.assertEqual(1, payload["opportunity_cooldown_suppressed_count"])

    def test_verified_maturity_is_immature_when_completion_is_low(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("VERIFIED", score=0.84, sample_size=OPPORTUNITY_MIN_HISTORY_SAMPLES, outcome_status="pending"),
                ],
                "opportunity_profile_stats": [
                    {
                        "scope_key": "p1",
                        "stats_json": {
                            "profile_key": "p1",
                            "sample_count": OPPORTUNITY_MIN_HISTORY_SAMPLES,
                            "completion_60s_rate": 0.5,
                            "followthrough_60s_rate": 0.8,
                            "adverse_60s_rate": 0.1,
                        },
                    }
                ],
            }
        )

        self.assertEqual("immature", payload["verified_maturity"])
        self.assertIn("outcome_completion_rate_below_0.70", payload["maturity_reasons"])
        self.assertIn("immature_verified_warning", payload["maturity_reasons"])
        self.assertTrue(payload["verified_should_not_be_traded_reason"])

    def test_verified_maturity_is_immature_when_profile_sample_is_low(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("VERIFIED", score=0.84, sample_size=OPPORTUNITY_MIN_HISTORY_SAMPLES - 1, outcome_status="completed", followthrough=True, adverse=False),
                ],
                "opportunity_profile_stats": [
                    {
                        "scope_key": "p1",
                        "stats_json": {
                            "profile_key": "p1",
                            "sample_count": OPPORTUNITY_MIN_HISTORY_SAMPLES - 1,
                            "completion_60s_rate": 1.0,
                            "followthrough_60s_rate": 0.8,
                            "adverse_60s_rate": 0.1,
                        },
                    }
                ],
            }
        )

        self.assertEqual("immature", payload["verified_maturity"])
        self.assertIn(f"profile_sample_count_below_{OPPORTUNITY_MIN_HISTORY_SAMPLES}", payload["maturity_reasons"])
        self.assertIn("immature_verified_warning", payload["maturity_reasons"])

    def test_verified_maturity_is_mature_when_completion_and_profile_are_ready(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    _row("VERIFIED", score=0.84, sample_size=OPPORTUNITY_MIN_HISTORY_SAMPLES, outcome_status="completed", followthrough=True, adverse=False),
                ],
                "opportunity_profile_stats": [
                    {
                        "scope_key": "p1",
                        "stats_json": {
                            "profile_key": "p1",
                            "sample_count": OPPORTUNITY_MIN_HISTORY_SAMPLES,
                            "completion_60s_rate": 1.0,
                            "followthrough_60s_rate": 0.8,
                            "adverse_60s_rate": 0.1,
                        },
                    }
                ],
            }
        )

        self.assertEqual("mature", payload["verified_maturity"])
        self.assertEqual([], payload["maturity_reasons"])
        self.assertEqual("", payload["verified_should_not_be_traded_reason"])


class SQLiteOpportunityMaturitySummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        sqlite_store.init_sqlite_store(Path(self.temp_dir.name) / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_verified(self, *, outcome_status: str, profile_sample_count: int) -> dict:
        self.assertTrue(
            sqlite_store.upsert_trade_opportunity(
                {
                    "trade_opportunity_id": "opp-verified",
                    "signal_id": "sig-verified",
                    "asset_symbol": "ETH",
                    "pair_label": "ETH/USDC",
                    "trade_opportunity_side": "LONG",
                    "trade_opportunity_status": "VERIFIED",
                    "trade_opportunity_score": 0.84,
                    "trade_opportunity_created_at": 1_710_000_000,
                    "opportunity_profile_key": "p1",
                    "opportunity_outcome_60s": outcome_status,
                    "opportunity_followthrough_60s": outcome_status == "completed",
                    "opportunity_adverse_60s": False,
                }
            )
        )
        self.assertTrue(
            sqlite_store.upsert_quality_stat(
                {
                    "scope_type": "opportunity_profile",
                    "scope_key": "p1",
                    "asset": "ETH",
                    "pair": "USDC",
                    "stage": "all",
                    "sample_count": profile_sample_count,
                    "stats_json": {
                        "profile_key": "p1",
                        "sample_count": profile_sample_count,
                        "completion_60s_rate": 1.0,
                        "followthrough_60s_rate": 0.8,
                        "adverse_60s_rate": 0.1,
                    },
                }
            )
        )
        return sqlite_store.opportunity_db_summary()

    def test_db_summary_marks_verified_immature_when_completion_is_low(self) -> None:
        payload = self._write_verified(
            outcome_status="pending",
            profile_sample_count=OPPORTUNITY_MIN_HISTORY_SAMPLES,
        )

        self.assertEqual("immature", payload["verified_maturity"])
        self.assertIn("outcome_completion_rate_below_0.70", payload["maturity_reasons"])
        self.assertIn("immature_verified_warning", payload["maturity_reasons"])

    def test_db_summary_marks_verified_immature_when_profile_sample_is_low(self) -> None:
        payload = self._write_verified(
            outcome_status="completed",
            profile_sample_count=OPPORTUNITY_MIN_HISTORY_SAMPLES - 1,
        )

        self.assertEqual("immature", payload["verified_maturity"])
        self.assertIn(f"profile_sample_count_below_{OPPORTUNITY_MIN_HISTORY_SAMPLES}", payload["maturity_reasons"])

    def test_db_summary_marks_verified_mature_when_samples_are_ready(self) -> None:
        payload = self._write_verified(
            outcome_status="completed",
            profile_sample_count=OPPORTUNITY_MIN_HISTORY_SAMPLES,
        )

        self.assertEqual("mature", payload["verified_maturity"])
        self.assertEqual([], payload["maturity_reasons"])


if __name__ == "__main__":
    unittest.main()
