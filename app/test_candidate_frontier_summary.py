from __future__ import annotations

import unittest

import config
import reports.generate_daily_report_latest as report


def _opportunity(index: int, status: str, *, score: float, blockers: list[str] | None = None) -> dict:
    return {
        "trade_opportunity_id": f"opp-{index}",
        "status": status,
        "score": score,
        "side": "LONG",
        "asset": "ETH",
        "pair": "ETH/USDC",
        "blockers_json": blockers or [],
    }


class CandidateFrontierSummaryTests(unittest.TestCase):
    def test_near_candidate_summary_does_not_promote_status(self) -> None:
        threshold = float(config.SHADOW_CANDIDATE_MIN_SCORE)
        rows = [_opportunity(i, "NONE", score=0.20) for i in range(92)]
        rows.extend(
            [
                _opportunity(92, "NONE", score=threshold - 0.02, blockers=["score_below_shadow_candidate"]),
                _opportunity(93, "NONE", score=threshold - 0.01, blockers=["score_below_shadow_candidate"]),
                _opportunity(94, "NONE", score=threshold - 0.03, blockers=["profile_sample_count_insufficient"]),
                _opportunity(95, "BLOCKED", score=threshold + 0.01, blockers=["score_below_shadow_candidate"]),
                _opportunity(96, "BLOCKED", score=0.30, blockers=["no_trade_lock"]),
            ]
        )
        replay_rows = [
            {"trade_opportunity_id": "opp-92", "data_valid": 1, "net_pnl_bps": -10.0},
            {"trade_opportunity_id": "opp-93", "data_valid": 1, "net_pnl_bps": -20.0},
            {"trade_opportunity_id": "opp-94", "data_valid": 1, "net_pnl_bps": -30.0},
            {"trade_opportunity_id": "opp-95", "data_valid": 1, "net_pnl_bps": -40.0},
        ]

        summary = report._candidate_frontier_summary(rows, replay_rows, {"shadow_funnel_summary": {}})

        self.assertTrue(summary["available"])
        self.assertEqual(97, summary["opportunities_total"])
        self.assertEqual(95, summary["none_count"])
        self.assertEqual(2, summary["blocked_count"])
        self.assertEqual(0, summary["candidate_count"])
        self.assertEqual(0, summary["verified_count"])
        self.assertEqual(4, summary["near_candidate_count"])
        self.assertEqual(4, summary["near_candidate_replay_count"])
        self.assertEqual(-25.0, summary["near_candidate_avg_net_pnl_bps"])
        self.assertEqual(3, summary["top_near_candidate_blockers"]["score_below_shadow_candidate"])
        self.assertEqual("gate_closed_because_quality_low", summary["diagnosis"])
        self.assertTrue(all(row["status"] in {"NONE", "BLOCKED"} for row in rows))


if __name__ == "__main__":
    unittest.main()
