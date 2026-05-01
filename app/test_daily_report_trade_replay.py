from __future__ import annotations

import unittest
from unittest import mock

import reports.generate_daily_report_latest as report
from app.test_daily_canonical_report import _summary_for


class DailyReportTradeReplayTests(unittest.TestCase):
    def test_read_trade_replay_summary_prefers_full_persisted(self) -> None:
        def fake_run(*_args, **kwargs):
            scope = kwargs.get("replay_scope")
            if scope == "full":
                return {
                    "trade_replay_available": True,
                    "replay_source": "persisted",
                    "replay_scope": "full",
                    "persisted_rows_found": 104,
                    "strategy_config_hash": "hashfull",
                    "replay_count": 104,
                    "valid_replay_count": 104,
                }
            return {
                "trade_replay_available": True,
                "replay_source": "persisted",
                "replay_scope": "default",
                "persisted_rows_found": 48,
                "strategy_config_hash": "hashdefault",
                "replay_count": 48,
                "valid_replay_count": 48,
            }

        with mock.patch("trade_replay.run_trade_replay", side_effect=fake_run):
            summary = report._read_trade_replay_summary("2026-04-30")

        self.assertEqual("persisted", summary["replay_source"])
        self.assertEqual("full", summary["replay_scope"])
        self.assertEqual(104, summary["persisted_rows_found"])

    def test_read_trade_replay_summary_falls_back_to_default_persisted(self) -> None:
        def fake_run(*_args, **kwargs):
            scope = kwargs.get("replay_scope")
            if scope == "full":
                return {
                    "trade_replay_available": True,
                    "replay_source": "dry_run",
                    "replay_scope": "full",
                    "persisted_rows_found": 0,
                    "replay_count": 104,
                    "valid_replay_count": 104,
                }
            return {
                "trade_replay_available": True,
                "replay_source": "persisted",
                "replay_scope": "default",
                "persisted_rows_found": 48,
                "strategy_config_hash": "hashdefault",
                "replay_count": 48,
                "valid_replay_count": 48,
            }

        with mock.patch("trade_replay.run_trade_replay", side_effect=fake_run):
            summary = report._read_trade_replay_summary("2026-04-30")

        self.assertEqual("persisted", summary["replay_source"])
        self.assertEqual("default", summary["replay_scope"])
        self.assertEqual(48, summary["replay_count"])

    def test_dry_run_replay_adds_limitation(self) -> None:
        with mock.patch.object(
            report,
            "_read_trade_replay_summary",
            return_value={
                "trade_replay_available": True,
                "replay_source": "dry_run",
                "replay_scope": "full",
                "persisted_rows_found": 0,
                "replay_count": 2,
                "valid_replay_count": 2,
                "warnings": [],
            },
        ), mock.patch.object(
            report,
            "_runtime_health_summary",
            return_value={
                "active_hours": 1.0,
                "raw_events_count": 1,
                "parsed_events_count": 1,
                "signals_count": 1,
                "max_raw_event_gap_sec": 0,
                "max_signal_gap_sec": 0,
                "zero_activity_day": 0,
                "data_quality_status": "valid",
                "data_gap_warnings": [],
            },
        ):
            payload = _summary_for(signals=[])

        self.assertEqual("dry_run", payload["trade_replay_summary"]["replay_source"])
        self.assertIn("replay_dry_run_used", payload["limitations"])

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
            "input_source_counts": {
                "signals": 3,
                "trade_opportunities": 2,
                "delivery_audit": 1,
                "telegram_deliveries": 1,
                "shadow_opportunities": 2,
                "suppressed": 1,
                "blocked": 1,
            },
            "eligibility_summary": {
                "eligible_count": 6,
                "ineligible_count": 1,
                "ineligible_reasons": {"direction_ambiguous": 1},
                "ineligible_reason_by_source": {"direction_ambiguous": {"signals": 1}},
                "replay_coverage_rate": 0.8571,
                "replay_coverage_warning": "",
            },
            "win_rate": 0.5,
            "avg_net_pnl_bps": 12.3,
            "clean_followthrough_rate": 0.33,
            "bad_entry_rate": 0.17,
            "absorption_reversal_rate": 0.17,
            "chop_rate": 0.17,
            "data_invalid_rate": 0.25,
            "shadow_replay_count": 2,
            "suppressed_replay_count": 1,
            "suppressed_profitable_rate": 1.0,
            "suppressed_avg_net_pnl_bps": 8.5,
            "suppressed_clean_followthrough_rate": 1.0,
            "suppressed_bad_entry_rate": 0.0,
            "suppressed_replay_zero_reasons": [],
            "shadow_funnel_summary": {
                "shadow_evaluated_count": 2,
                "shadow_gate_passed_count": 2,
                "shadow_candidate_count": 1,
                "shadow_verified_count": 1,
                "shadow_blocked_count": 0,
                "shadow_blocked_reasons": {},
                "shadow_replay_count": 2,
                "zero_shadow_reasons": [],
            },
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
        self.assertEqual(8.5, payload["trade_replay_summary"]["suppressed_avg_net_pnl_bps"])
        self.assertEqual(3, payload["trade_replay_summary"]["input_source_counts"]["signals"])
        self.assertEqual(1, payload["trade_replay_summary"]["eligibility_summary"]["ineligible_reasons"]["direction_ambiguous"])
        self.assertIn("trade_replay_profile_summary", payload)
        self.assertEqual(1, payload["shadow_opportunity_summary"]["shadow_candidate_count"])
        self.assertEqual(1, payload["shadow_opportunity_summary"]["shadow_verified_count"])
        self.assertEqual(2, payload["shadow_funnel_summary"]["shadow_gate_passed_count"])
        self.assertEqual(2, payload["shadow_opportunity_summary"]["shadow_replay_count"])
        self.assertEqual("valid", payload["data_quality_summary"]["data_quality_status"])
        self.assertEqual(runtime["active_hours"], payload["runtime_health"]["active_hours"])
        markdown = report._markdown(payload)
        self.assertIn("交易回放 / Replay 后验", markdown)
        self.assertIn("Shadow Opportunity 学习样本", markdown)
        self.assertIn("数据质量 / Runtime Health", markdown)


if __name__ == "__main__":
    unittest.main()
