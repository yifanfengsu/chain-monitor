import unittest

from reports.generate_overnight_run_analysis_latest import compute_trade_opportunities


class OpportunityPosteriorReportTests(unittest.TestCase):
    def test_report_outputs_profile_stats_and_blocker_metrics(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    {
                        "trade_opportunity_id": "opp-candidate",
                        "trade_opportunity_status": "CANDIDATE",
                        "trade_opportunity_status_at_creation": "CANDIDATE",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.74,
                        "trade_opportunity_primary_verification_blocker": "profile_sample_count_insufficient",
                        "opportunity_profile_key": "p2",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 8},
                        "opportunity_outcome_60s": "pending",
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-verified",
                        "trade_opportunity_status": "VERIFIED",
                        "trade_opportunity_status_at_creation": "VERIFIED",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.86,
                        "opportunity_profile_key": "p1",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 24},
                        "opportunity_outcome_60s": "completed",
                        "opportunity_followthrough_60s": True,
                        "opportunity_adverse_60s": False,
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-blocked",
                        "trade_opportunity_status": "BLOCKED",
                        "trade_opportunity_status_at_creation": "BLOCKED",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.68,
                        "trade_opportunity_primary_blocker": "data_gap",
                        "trade_opportunity_primary_hard_blocker": "data_gap",
                        "opportunity_profile_key": "p1",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 24},
                        "opportunity_outcome_60s": "completed",
                        "opportunity_followthrough_60s": False,
                        "opportunity_adverse_60s": True,
                        "blocker_saved_trade": True,
                        "blocker_false_block_possible": False,
                        "asset_symbol": "ETH",
                    },
                ],
                "opportunity_profile_stats": [
                    {
                        "scope_key": "p1",
                        "asset": "ETH",
                        "pair": "USDC",
                        "stats_json": {
                            "profile_key": "p1",
                            "sample_count": 24,
                            "candidate_count": 1,
                            "verified_count": 1,
                            "blocked_count": 1,
                            "completed_60s": 24,
                            "followthrough_60s_rate": 0.79,
                            "adverse_60s_rate": 0.12,
                            "completion_60s_rate": 0.92,
                            "blocker_saved_rate": 0.8,
                            "blocker_false_block_rate": 0.1,
                        },
                    },
                    {
                        "scope_key": "p2",
                        "asset": "ETH",
                        "pair": "USDC",
                        "stats_json": {
                            "profile_key": "p2",
                            "sample_count": 8,
                            "candidate_count": 1,
                            "verified_count": 0,
                            "blocked_count": 0,
                            "completed_60s": 6,
                            "followthrough_60s_rate": 0.5,
                            "adverse_60s_rate": 0.2,
                            "completion_60s_rate": 0.75,
                            "blocker_saved_rate": 0.0,
                            "blocker_false_block_rate": 0.0,
                        },
                    },
                ],
            }
        )

        self.assertEqual(2, payload["opportunity_profile_count"])
        self.assertIn("p1", payload["profiles_ready_for_verified"])
        self.assertEqual(1, payload["profiles_blocked_by_sample_count"])
        self.assertEqual({"data_gap": 1}, payload["hard_blocker_distribution"])
        self.assertEqual({"profile_sample_count_insufficient": 1}, payload["verification_blocker_distribution"])
        self.assertEqual(1.0, payload["blocker_saved_rate"])
        self.assertGreater(payload["estimated_samples_needed_for_verified_by_profile"]["p2"], 0)
        self.assertEqual(1.0, payload["candidate_to_verified_by_profile"]["p1"]["rate"])


if __name__ == "__main__":
    unittest.main()
