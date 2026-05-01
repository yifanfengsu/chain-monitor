from __future__ import annotations

import unittest
from unittest import mock

import reports.generate_daily_report_latest as report
from app.test_daily_canonical_report import _summary_for


class DailyReportTradeReplayTests(unittest.TestCase):
    def test_daily_report_without_replay_adds_limitation(self) -> None:
        with mock.patch.object(
            report,
            "_read_trade_replay_summary",
            return_value={"trade_replay_available": False, "reason": "trade_replay_missing", "replay_count": 0},
        ), mock.patch.object(
            report,
            "_runtime_health_summary",
            return_value={
                "active_hours": 0.0,
                "raw_events_count": 0,
                "parsed_events_count": 0,
                "signals_count": 0,
                "max_raw_event_gap_sec": None,
                "max_signal_gap_sec": None,
                "zero_activity_day": 1,
                "data_quality_status": "invalid_or_no_activity",
                "data_gap_warnings": [],
            },
        ):
            payload = _summary_for(signals=[])

        self.assertIn("trade_replay_summary", payload)
        self.assertFalse(payload["trade_replay_summary"]["trade_replay_available"])
        self.assertIn("trade_replay_missing", payload["limitations"])
        self.assertIn("runtime_health", payload)
        self.assertIn("data_quality_summary", payload)

    def test_daily_report_with_replay_outputs_replay_shadow_and_data_quality(self) -> None:
        replay_summary = {
            "trade_replay_available": True,
            "replay_count": 8,
            "valid_replay_count": 6,
            "win_rate": 0.5,
            "avg_net_pnl_bps": 12.3,
            "clean_followthrough_rate": 0.33,
            "bad_entry_rate": 0.17,
            "absorption_reversal_rate": 0.17,
            "chop_rate": 0.17,
            "data_invalid_rate": 0.25,
            "shadow_replay_count": 2,
            "top_positive_profiles": [{"profile_key": "ETH|LONG|SHADOW_A", "avg_net_pnl_bps": 15.0}],
            "top_negative_profiles": [{"profile_key": "ETH|SHORT|SHADOW_B", "avg_net_pnl_bps": -8.0}],
            "recommended_profile_actions": [],
        }
        runtime = {
            "active_hours": 12.0,
            "raw_events_count": 100,
            "parsed_events_count": 90,
            "signals_count": 12,
            "max_raw_event_gap_sec": 120,
            "max_signal_gap_sec": 300,
            "zero_activity_day": 0,
            "data_quality_status": "valid",
            "data_gap_warnings": [],
        }
        opportunity_rows = [
            {"trade_opportunity_shadow_status": "SHADOW_CANDIDATE"},
            {"trade_opportunity_shadow_status": "SHADOW_VERIFIED"},
        ]
        with mock.patch.object(report, "_read_trade_replay_summary", return_value=replay_summary), mock.patch.object(
            report,
            "_runtime_health_summary",
            return_value=runtime,
        ):
            payload = _summary_for(trade_opportunities=opportunity_rows)

        self.assertEqual(8, payload["trade_replay_summary"]["replay_count"])
        self.assertIn("trade_replay_profile_summary", payload)
        self.assertEqual(1, payload["shadow_opportunity_summary"]["shadow_candidate_count"])
        self.assertEqual(1, payload["shadow_opportunity_summary"]["shadow_verified_count"])
        self.assertEqual(2, payload["shadow_opportunity_summary"]["shadow_replay_count"])
        self.assertEqual("valid", payload["data_quality_summary"]["data_quality_status"])
        self.assertEqual(runtime["active_hours"], payload["runtime_health"]["active_hours"])
        markdown = report._markdown(payload)
        self.assertIn("交易后验 / Trade Replay", markdown)
        self.assertIn("Shadow Opportunity 学习样本", markdown)
        self.assertIn("数据质量 / Runtime Health", markdown)


if __name__ == "__main__":
    unittest.main()
