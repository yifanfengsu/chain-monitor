from __future__ import annotations

import sqlite3
import unittest
from unittest import mock

import reports.generate_daily_report_latest as report
from app.test_daily_canonical_report import _summary_for
from replay_profile_gate import replay_profile_summary


class DailyReportTradeReplayTests(unittest.TestCase):
    def test_sqlite_trade_replay_summary_repairs_profile_key_from_replay_sources(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                CREATE TABLE trade_replay_examples (
                    logical_date TEXT,
                    replay_scope TEXT,
                    strategy_config_hash TEXT,
                    signal_ts INTEGER,
                    data_valid INTEGER,
                    label TEXT,
                    net_pnl_bps REAL,
                    signal_stage TEXT,
                    opportunity_status TEXT,
                    shadow_status TEXT,
                    profile_key TEXT,
                    lp_stage TEXT,
                    sweep_phase TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE trade_replay_profile_daily_stats (
                    logical_date TEXT,
                    replay_scope TEXT,
                    strategy_config_hash TEXT,
                    profile_key TEXT,
                    valid_sample_count INTEGER,
                    avg_net_pnl_bps REAL,
                    win_rate REAL,
                    clean_followthrough_rate REAL,
                    chop_rate REAL,
                    recommended_action TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "2026-04-30",
                    "full",
                    "hash",
                    1_777_000_000,
                    1,
                    "clean_followthrough",
                    12.0,
                    "",
                    "CANDIDATE",
                    "NONE",
                    "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
                    "confirm",
                    "confirm",
                ),
            )
            conn.execute(
                "INSERT INTO trade_replay_profile_daily_stats VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    "2026-04-30",
                    "full",
                    "hash",
                    "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
                    1,
                    12.0,
                    1.0,
                    1.0,
                    0.0,
                    "needs_more_samples",
                ),
            )
            conn.commit()

            summary = report._trade_replay_summary_from_sqlite(conn, "2026-04-30")
        finally:
            conn.close()

        self.assertEqual(
            "ETH|LONG|confirm|confirm|leading|local_absorption|major|basis_normal|quality_low",
            summary["top_positive_profiles"][0]["profile_key"],
        )
        self.assertNotIn("lp_stage", summary["profile_unknown_diagnostics"]["unknown_by_dimension"])

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

    def test_daily_report_profile_layers_negative_low_sample_and_unknown(self) -> None:
        profiles = [
            {
                "profile_key": "ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low",
                "valid_sample_count": 48,
                "avg_net_pnl_bps": -21.67,
                "win_rate": 0.0,
                "clean_followthrough_rate": 0.0,
                "chop_rate": 0.9583,
            },
            {
                "profile_key": "ETH|SHORT|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low",
                "valid_sample_count": 18,
                "avg_net_pnl_bps": -18.49,
                "win_rate": 0.1111,
                "clean_followthrough_rate": 0.0,
                "chop_rate": 0.8889,
            },
            {
                "profile_key": "ETH|LONG|unknown|confirm|leading|no_absorption|major|basis_normal|quality_high",
                "valid_sample_count": 1,
                "avg_net_pnl_bps": 70.38,
                "win_rate": 1.0,
                "clean_followthrough_rate": 1.0,
                "chop_rate": 0.0,
            },
        ]
        replay_summary = {
            "trade_replay_available": True,
            "replay_source": "persisted",
            "replay_scope": "full",
            "persisted_rows_found": 67,
            "replay_count": 67,
            "valid_replay_count": 67,
            "top_positive_profiles": [profiles[2]],
            "top_negative_profiles": profiles[:2],
            "recommended_profile_actions": [],
            **replay_profile_summary(profiles),
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

        with mock.patch.object(report, "_read_trade_replay_summary", return_value=replay_summary), mock.patch.object(
            report,
            "_runtime_health_summary",
            return_value=runtime,
        ):
            payload = _summary_for(signals=[])

        profile_summary = payload["trade_replay_profile_summary"]
        self.assertEqual(2, profile_summary["replay_profile_blocker_count"])
        self.assertEqual(2, len(profile_summary["sampled_negative_profiles"]))
        self.assertEqual(2, len(profile_summary["blocker_grade_negative_profiles"]))
        self.assertEqual(48, profile_summary["high_confidence_negative_profiles"][0]["valid_sample_count"])
        self.assertEqual(70.38, profile_summary["low_sample_positive_profiles"][0]["avg_net_pnl_bps"])
        self.assertEqual([], profile_summary["high_confidence_positive_profiles"])
        self.assertIn("lp_stage", profile_summary["profile_unknown_diagnostics"]["unknown_by_dimension"])
        self.assertIn("dimension_names", profile_summary["profile_unknown_diagnostics"])
        self.assertIn("unknown_rate_by_dimension", profile_summary["profile_unknown_diagnostics"])
        self.assertIn("unknown_missing_sources", profile_summary["profile_unknown_diagnostics"])
        self.assertIn("missing_lp_stage", profile_summary["profile_unknown_diagnostics"]["unknown_missing_sources"])
        self.assertNotEqual("replay_positive_profiles_found", payload["report_conclusion"])
        markdown = report._markdown(payload)
        self.assertIn("sampled_negative_profiles", markdown)
        self.assertIn("blocker_grade_negative_profiles", markdown)

    def test_trade_opportunity_summary_counts_replay_profile_negative_any_blocker(self) -> None:
        payload = report._trade_opportunity_summary(
            [
                {
                    "trade_opportunity_status": "BLOCKED",
                    "trade_opportunity_primary_blocker": "no_trade_lock",
                    "trade_opportunity_blockers": ["no_trade_lock", "replay_profile_negative"],
                }
            ]
        )

        self.assertEqual(1, payload["replay_profile_negative_count"])
        self.assertEqual(0, payload["replay_profile_negative_primary_count"])
        self.assertEqual(1, payload["replay_profile_negative_non_primary_count"])
        self.assertEqual(0, payload["legacy_primary_blocker_mismatch_count"])
        self.assertEqual(1, payload["replay_profile_negative_summary"]["expected_non_primary_count"])

    def test_trade_opportunity_summary_marks_replay_profile_legacy_primary_mismatch(self) -> None:
        payload = report._trade_opportunity_summary(
            [
                {
                    "trade_opportunity_status": "BLOCKED",
                    "trade_opportunity_primary_blocker": "profile_followthrough_too_low",
                    "trade_opportunity_blockers": ["profile_followthrough_too_low", "replay_profile_negative"],
                }
            ]
        )

        self.assertEqual(1, payload["replay_profile_negative_count"])
        self.assertEqual(1, payload["legacy_primary_blocker_mismatch_count"])
        self.assertEqual(
            {"profile_followthrough_too_low": 1},
            payload["replay_profile_negative_summary"]["mismatch_primary_blocker_distribution"],
        )


if __name__ == "__main__":
    unittest.main()
