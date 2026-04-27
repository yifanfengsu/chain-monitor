import unittest

from reports.legacy.generate_overnight_run_analysis_latest import compute_trade_opportunities


class OpportunityNonLpReportTests(unittest.TestCase):
    def test_report_outputs_non_lp_support_and_blockers(self) -> None:
        payload = compute_trade_opportunities(
            {
                "opportunities": [
                    {
                        "trade_opportunity_id": "opp-upgraded",
                        "trade_opportunity_status": "CANDIDATE",
                        "trade_opportunity_status_at_creation": "CANDIDATE",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.69,
                        "trade_opportunity_score_without_non_lp": 0.66,
                        "trade_opportunity_primary_verification_blocker": "profile_sample_count_insufficient",
                        "opportunity_profile_key": "p1",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 6},
                        "trade_opportunity_non_lp_evidence_summary": "smart_money support",
                        "trade_opportunity_non_lp_evidence_context": {
                            "non_lp_evidence_available": True,
                            "non_lp_support_score": 0.42,
                            "non_lp_risk_score": 0.0,
                            "non_lp_evidence_items": [{"intent_key": "smart_money_entry_execution", "effect": "support"}],
                        },
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-blocked",
                        "trade_opportunity_status": "BLOCKED",
                        "trade_opportunity_status_at_creation": "BLOCKED",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.64,
                        "trade_opportunity_primary_blocker": "non_lp_maker_against",
                        "trade_opportunity_primary_hard_blocker": "non_lp_maker_against",
                        "opportunity_profile_key": "p1",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 20},
                        "trade_opportunity_non_lp_evidence_summary": "maker against",
                        "trade_opportunity_non_lp_evidence_context": {
                            "non_lp_evidence_available": True,
                            "non_lp_support_score": 0.0,
                            "non_lp_risk_score": 0.38,
                            "non_lp_evidence_items": [{"intent_key": "market_maker_inventory_distribute", "effect": "risk"}],
                        },
                        "asset_symbol": "ETH",
                    },
                    {
                        "trade_opportunity_id": "opp-conflict",
                        "trade_opportunity_status": "BLOCKED",
                        "trade_opportunity_status_at_creation": "BLOCKED",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_score": 0.63,
                        "trade_opportunity_primary_blocker": "non_lp_evidence_conflict",
                        "trade_opportunity_primary_hard_blocker": "non_lp_evidence_conflict",
                        "opportunity_profile_key": "p2",
                        "opportunity_profile_pair_family": "USDC",
                        "trade_opportunity_history_snapshot": {"sample_size": 18},
                        "trade_opportunity_non_lp_evidence_summary": "conflict",
                        "trade_opportunity_non_lp_evidence_context": {
                            "non_lp_evidence_available": True,
                            "non_lp_support_score": 0.42,
                            "non_lp_risk_score": 0.38,
                            "non_lp_strong_conflict": True,
                            "non_lp_evidence_items": [
                                {"intent_key": "smart_money_entry_execution", "effect": "support"},
                                {"intent_key": "market_maker_inventory_distribute", "effect": "risk"},
                            ],
                        },
                        "asset_symbol": "ETH",
                    },
                ]
            }
        )

        self.assertEqual(1, payload["opportunities_upgraded_by_non_lp"])
        self.assertEqual(2, payload["opportunities_blocked_by_non_lp"])
        self.assertEqual(1, payload["opportunity_non_lp_support_count"])
        self.assertEqual(1, payload["opportunity_non_lp_risk_count"])
        self.assertEqual("smart_money_entry_execution", payload["top_non_lp_supporting_evidence"][0]["intent_key"])
        self.assertEqual("non_lp_maker_against", payload["top_non_lp_blockers"][0]["blocker"])
        self.assertEqual(1, len(payload["non_lp_conflict_cases"]))


if __name__ == "__main__":
    unittest.main()
