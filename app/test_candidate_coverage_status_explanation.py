from __future__ import annotations

import unittest

import reports.generate_daily_report_latest as report


class CandidateCoverageStatusExplanationTests(unittest.TestCase):
    def test_raw_none_rows_are_split_into_true_none_and_blocked_like_none(self) -> None:
        rows = []
        for idx in range(80):
            rows.append(
                {
                    "trade_opportunity_id": f"blocked-like-{idx}",
                    "trade_opportunity_status": "NONE",
                    "trade_opportunity_primary_blocker": "replay_profile_negative" if idx < 40 else "low_quality",
                    "trade_opportunity_blockers": ["replay_profile_negative"] if idx < 40 else ["low_quality"],
                    "trade_opportunity_hard_blockers": ["replay_profile_negative"] if idx < 40 else ["low_quality"],
                }
            )
        for idx in range(28):
            rows.append(
                {
                    "trade_opportunity_id": f"true-none-{idx}",
                    "trade_opportunity_status": "NONE",
                    "trade_opportunity_shadow_reason": "score_below_shadow_candidate" if idx < 10 else "",
                }
            )

        payload = report._candidate_coverage_summary(
            signal_rows=[],
            opportunity_rows=rows,
            replay_rows=[],
            candidate_frontier={
                "none_count": 108,
                "blocked_count": 0,
                "candidate_count": 0,
                "verified_count": 0,
                "near_candidate_count": 0,
                "diagnosis": "gate_closed_because_quality_low",
            },
        )
        explained = payload["opportunity_status_explained_summary"]

        self.assertEqual(108, explained["raw_status_distribution"]["NONE"])
        self.assertEqual(80, explained["blocked_like_none_count"])
        self.assertEqual(18, explained["true_none_count"])
        self.assertEqual(10, explained["near_candidate_none_count"])
        self.assertEqual(80, explained["none_with_blockers_count"])
        self.assertEqual("raw_status_all_none_but_blocked_like_present", explained["status_assignment_warning"])
        self.assertEqual(40, explained["top_derived_reasons"]["replay_profile_negative"])
        self.assertEqual(40, explained["top_derived_reasons"]["low_quality"])
        self.assertEqual(10, explained["top_derived_reasons"]["score_below_shadow_candidate"])


if __name__ == "__main__":
    unittest.main()
