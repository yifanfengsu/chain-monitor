from __future__ import annotations

import unittest

import reports.generate_daily_report_latest as report


class DailyReportOutcomeCandidateSummaryTests(unittest.TestCase):
    def test_outcome_diagnosis_summary_counts_links_and_pending(self) -> None:
        signals = [{"signal_id": f"sig-{idx}"} for idx in range(1, 5)]
        opportunities = [
            {"trade_opportunity_id": "opp-1", "signal_id": "sig-1"},
            {"trade_opportunity_id": "opp-2", "signal_id": "sig-2"},
        ]
        outcomes = [
            {"signal_id": "sig-1", "trade_opportunity_id": "opp-1", "window_sec": 60, "status": "completed"},
            {"signal_id": "sig-2", "trade_opportunity_id": "opp-2", "window_sec": 60, "status": "completed"},
        ]
        opportunity_outcomes = [
            {"trade_opportunity_id": "opp-1", "window_sec": 60, "status": "pending", "due_at": 1},
            {"trade_opportunity_id": "opp-2", "window_sec": 60, "status": "pending", "due_at": 1},
        ]
        replay_rows = [
            {"trade_opportunity_id": "opp-1", "replay_scope": "full"},
            {"trade_opportunity_id": "opp-2", "replay_scope": "full"},
        ]

        summary = report._outcome_diagnosis_summary(
            signals,
            opportunities,
            outcomes,
            opportunity_outcomes,
            replay_rows,
        )

        self.assertTrue(summary["available"])
        self.assertEqual(4, summary["signals_total"])
        self.assertEqual(2, summary["trade_opportunities_total"])
        self.assertEqual(2, summary["outcomes_total"])
        self.assertEqual(2, summary["outcomes_completed"])
        self.assertEqual(2, summary["opportunity_outcomes_total"])
        self.assertEqual(0, summary["opportunity_outcomes_completed"])
        self.assertEqual(2, summary["opportunity_outcomes_pending"])
        self.assertEqual(0.5, summary["signals_to_outcomes_match_rate"])
        self.assertEqual(1.0, summary["opportunities_to_opportunity_outcomes_match_rate"])
        self.assertEqual(1.0, summary["opportunities_to_replay_examples_match_rate"])
        self.assertEqual("opportunity_outcomes_pending_not_settled", summary["diagnosis"])
        self.assertEqual("outcome-catchup dry-run", summary["recommended_next_check"])

    def test_candidate_coverage_summary_uses_frontier_and_replay(self) -> None:
        signals = [{"signal_id": f"sig-{idx}"} for idx in range(3)]
        opportunities = [
            {"trade_opportunity_id": "opp-1", "status": "NONE"},
            {"trade_opportunity_id": "opp-2", "status": "BLOCKED"},
        ]
        replay_rows = [
            {"trade_opportunity_id": "opp-1", "opportunity_status": "NONE", "net_pnl_bps": -10.0},
            {"trade_opportunity_id": "opp-2", "opportunity_status": "CANDIDATE", "net_pnl_bps": 5.0},
            {"trade_opportunity_id": "", "opportunity_status": "", "net_pnl_bps": 1.0},
        ]
        frontier = {
            "none_count": 1,
            "blocked_count": 1,
            "candidate_count": 0,
            "verified_count": 0,
            "near_candidate_count": 2,
            "near_candidate_avg_net_pnl_bps": -2.5,
            "diagnosis": "gate_closed_because_quality_low",
        }

        summary = report._candidate_coverage_summary(signals, opportunities, replay_rows, frontier)

        self.assertTrue(summary["available"])
        self.assertEqual(3, summary["signals_total"])
        self.assertEqual(2, summary["trade_opportunities_total"])
        self.assertEqual(1, summary["none_count"])
        self.assertEqual(1, summary["blocked_count"])
        self.assertEqual(0, summary["candidate_count"])
        self.assertEqual(0, summary["verified_count"])
        self.assertEqual(3, summary["replay_examples_total"])
        self.assertEqual(2, summary["replay_examples_with_opportunity"])
        self.assertEqual(1, summary["replay_examples_status_candidate"])
        self.assertEqual(2, summary["near_candidate_count"])
        self.assertEqual(-2.5, summary["near_candidate_avg_net_pnl_bps"])
        self.assertEqual("gate_closed_because_quality_low", summary["diagnosis"])


if __name__ == "__main__":
    unittest.main()
