import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import quality_reports
import sqlite_store
from reports.generate_overnight_run_analysis_latest import compute_trade_opportunities


class OpportunityCalibrationReportTests(unittest.TestCase):
    def test_report_outputs_calibration_metrics(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    {
                        "trade_opportunity_id": "opp-up",
                        "trade_opportunity_status": "CANDIDATE",
                        "trade_opportunity_status_at_creation": "CANDIDATE",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.71,
                        "opportunity_raw_score": 0.66,
                        "opportunity_calibrated_score": 0.71,
                        "opportunity_calibration_adjustment": 0.05,
                        "opportunity_calibration_reason": "followthrough>0.65",
                        "opportunity_calibration_source": "profile",
                        "opportunity_calibration_confidence": 0.88,
                        "opportunity_profile_key": "p1",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 8},
                        "opportunity_outcome_60s": "pending",
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-down",
                        "trade_opportunity_status": "NONE",
                        "trade_opportunity_status_at_creation": "NONE",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.63,
                        "opportunity_raw_score": 0.72,
                        "opportunity_calibrated_score": 0.63,
                        "opportunity_calibration_adjustment": -0.09,
                        "opportunity_calibration_reason": "adverse>0.45; completion<0.70",
                        "opportunity_calibration_source": "profile",
                        "opportunity_calibration_confidence": 0.82,
                        "opportunity_profile_key": "p2",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 24},
                        "opportunity_outcome_60s": "completed",
                        "opportunity_followthrough_60s": False,
                        "opportunity_adverse_60s": True,
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-verified",
                        "trade_opportunity_status": "VERIFIED",
                        "trade_opportunity_status_at_creation": "VERIFIED",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.81,
                        "opportunity_raw_score": 0.77,
                        "opportunity_calibrated_score": 0.81,
                        "opportunity_calibration_adjustment": 0.04,
                        "opportunity_calibration_reason": "followthrough>0.65; adverse<0.25",
                        "opportunity_calibration_source": "asset_side",
                        "opportunity_calibration_confidence": 0.74,
                        "opportunity_profile_key": "p3",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 24},
                        "opportunity_outcome_60s": "completed",
                        "opportunity_followthrough_60s": True,
                        "opportunity_adverse_60s": False,
                        "asset_symbol": "ETH",
                    },
                ]
            }
        )

        self.assertEqual(1, payload["opportunities_upgraded_by_calibration"])
        self.assertEqual(1, payload["opportunities_downgraded_by_calibration"])
        self.assertEqual(1, payload["candidates_blocked_by_calibration"])
        self.assertEqual(1, payload["verified_allowed_by_calibration"])
        self.assertEqual({"high": 2, "medium": 1}, payload["calibration_confidence_distribution"])
        self.assertTrue(payload["top_positive_adjustments"])
        self.assertTrue(payload["top_negative_adjustments"])
        self.assertIn("raw_median", payload["raw_score_vs_calibrated_score"])

    def test_cli_opportunity_calibration_runs(self) -> None:
        sqlite_store.close()
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db_path = Path(temp_dir.name) / "chain_monitor.sqlite"
            sqlite_store.init_sqlite_store(db_path)
            sqlite_store.upsert_quality_stat(
                {
                    "scope_type": "opportunity_calibration",
                    "scope_key": "profile:p1",
                    "asset": "ETH",
                    "pair": "USDC",
                    "stage": "60s",
                    "sample_count": 60,
                    "candidate_followthrough_rate": 0.71,
                    "candidate_adverse_rate": 0.18,
                    "verified_followthrough_rate": 0.71,
                    "verified_adverse_rate": 0.18,
                    "outcome_completion_rate": 0.92,
                    "updated_at": 1_710_000_000,
                    "stats_json": {
                        "scope_type": "profile",
                        "scope_key": "profile:p1",
                        "sample_count": 60,
                        "completion_rate_60s": 0.92,
                        "followthrough_rate_60s": 0.71,
                        "adverse_rate_60s": 0.18,
                        "adjustment": 0.04,
                        "confidence": 0.9,
                        "reason": "followthrough>0.65; adverse<0.25",
                        "updated_at": 1_710_000_000,
                    },
                }
            )
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = quality_reports.main(["--opportunity-calibration"])
            self.assertEqual(0, code)
            payload = json.loads(buffer.getvalue())
            self.assertIn("profiles_calibrated_count", payload)
        finally:
            sqlite_store.close()
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
