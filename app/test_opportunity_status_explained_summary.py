from __future__ import annotations

import unittest

from opportunity_status_explanation import explain_opportunity_status_rows


class OpportunityStatusExplainedSummaryTests(unittest.TestCase):
    def test_raw_none_rows_are_split_by_derived_status(self) -> None:
        rows = []
        for idx in range(50):
            rows.append(
                {
                    "trade_opportunity_id": f"blocked-{idx}",
                    "status": "NONE",
                    "primary_hard_blocker": "data_gap",
                    "hard_blockers_json": '["data_gap"]',
                }
            )
        for idx in range(30):
            rows.append(
                {
                    "trade_opportunity_id": f"gate-{idx}",
                    "status": "NONE",
                    "opportunity_gate_required": 1,
                    "opportunity_gate_passed": 0,
                    "opportunity_gate_failure_reason": "opportunity_gate_rejected",
                }
            )
        for idx in range(20):
            rows.append(
                {
                    "trade_opportunity_id": f"near-{idx}",
                    "status": "NONE",
                    "would_have_been_candidate": 1,
                    "shadow_reason": "score_below_shadow_candidate",
                }
            )
        for idx in range(8):
            rows.append({"trade_opportunity_id": f"true-none-{idx}", "status": "NONE"})

        payload = explain_opportunity_status_rows(rows)

        self.assertEqual(108, payload["raw_status_distribution"]["NONE"])
        self.assertEqual(50, payload["derived_status_distribution"]["BLOCKED_LIKE"])
        self.assertEqual(30, payload["derived_status_distribution"]["GATE_FAILED"])
        self.assertEqual(20, payload["derived_status_distribution"]["NEAR_CANDIDATE"])
        self.assertEqual(8, payload["derived_status_distribution"]["TRUE_NONE"])
        self.assertEqual(8, payload["true_none_count"])
        self.assertEqual(50, payload["blocked_like_none_count"])
        self.assertEqual(30, payload["gate_failed_none_count"])
        self.assertEqual(20, payload["near_candidate_none_count"])
        self.assertEqual("raw_status_all_none_but_blocked_like_present", payload["status_assignment_warning"])


if __name__ == "__main__":
    unittest.main()
