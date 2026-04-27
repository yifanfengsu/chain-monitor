import json
import tempfile
import unittest
from pathlib import Path

from reports.generate_daily_report_latest import write_daily_artifacts


def _minimal_summary(logical_date: str = "2026-04-24") -> dict:
    return {
        "report_type": "daily_canonical",
        "logical_date": logical_date,
        "timezone": "Asia/Shanghai",
        "logical_window_start_utc": "2026-04-23 16:00:00 UTC",
        "logical_window_end_utc": "2026-04-24 15:59:59 UTC",
        "logical_window_start_beijing": "2026-04-24 00:00:00 UTC+8",
        "logical_window_end_beijing": "2026-04-24 23:59:59 UTC+8",
        "analysis_window": {"end_utc": "2026-04-24 15:59:59 UTC", "duration_hours": 24.0},
        "segment_summary": {
            "segment_count": 1,
            "segments": [{"segment_id": "segment_1", "start_utc": "2026-04-23 16:00:00 UTC", "end_utc": "2026-04-23 16:10:00 UTC", "duration_hours": 0.17, "row_count": 2, "source_counts": {"signals": 2}}],
            "selected_segments": ["segment_1"],
            "active_duration_hours": 0.17,
            "wall_clock_duration_hours": 24.0,
            "max_gap_hours": 0.0,
            "gap_warnings": [],
        },
        "segment_count": 1,
        "segments": [],
        "active_duration_hours": 0.17,
        "wall_clock_duration_hours": 24.0,
        "max_gap_hours": 0.0,
        "gap_warnings": [],
        "data_sources": {},
        "data_source_summary": {"data_source": "sqlite", "source_components": {"signals": "sqlite"}, "archive_fallback_used": False},
        "data_source": "sqlite",
        "archive_fallback_used": False,
        "source_warnings": [],
        "db_archive_mismatch_warnings": [],
        "sqlite_health": {"initialized": True},
        "archive_health": {"archive_rows_by_category": {}},
        "run_overview": {"lp_signal_rows": 2, "total_raw_events": 2, "total_parsed_events": 2, "total_signal_rows": 2, "delivered_lp_signals": 1},
        "lp_stage_summary": {"stage_distribution": {"confirm": 2}},
        "trade_action_summary": {"trade_action_distribution": {"WAIT_CONFIRMATION": 1}},
        "asset_market_state_summary": {"state_distribution": {"OBSERVE_LONG": 1}},
        "no_trade_lock_summary": {"lock_entered_count": 0},
        "trade_opportunity_summary": {"opportunity_candidate_count": 1, "opportunity_verified_count": 0, "opportunity_blocked_count": 1, "verified_maturity": "immature"},
        "candidate_verified_summary": {"candidate_count": 1},
        "candidate_tradeable_summary": {"candidate_count": 1},
        "blocker_summary": {"hard_blocker_distribution": {"history_completion_too_low": 1}},
        "outcome_summary": {"window_status": {"30s": {"completed": 1}}, "outcome_30s_completed_rate": 0.5, "outcome_60s_completed_rate": 0.5, "outcome_300s_completed_rate": 0.5},
        "outcome_source_summary": {"outcome_60s_completed_rate": 0.5},
        "telegram_suppression_summary": {"telegram_suppressed_count": 1},
        "prealert_lifecycle_summary": {"prealert_candidate_count": 1},
        "major_coverage_summary": {"covered_major_pairs": ["ETH/USDT"], "missing_major_pairs": ["BTC/USDT"]},
        "majors_coverage_summary": {"covered_major_pairs": ["ETH/USDT"], "missing_major_pairs": ["BTC/USDT"]},
        "market_context_health": {"window": {"live_public_rate": 1.0}},
        "maturity_summary": {"verified_maturity": "immature", "maturity_reasons": ["sample_low"]},
        "limitations": ["CANDIDATE is not a trade signal", "verified_maturity=immature; VERIFIED must not be treated as mature trade signal"],
        "key_findings": [],
        "key_risks": [],
        "next_actions": ["continue"],
        "daily_state_classification": "filtering",
    }


class DailyCanonicalReportTests(unittest.TestCase):
    def test_daily_artifacts_and_json_core_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "reports" / "daily"
            summary = _minimal_summary()
            written = write_daily_artifacts(summary, output_dir=output_dir)

            dated_json = Path(written["dated_json"])
            latest_json = Path(written["latest_json"])
            self.assertTrue(dated_json.exists())
            self.assertTrue(latest_json.exists())
            payload = json.loads(dated_json.read_text(encoding="utf-8"))
            for key in (
                "report_type",
                "logical_date",
                "timezone",
                "analysis_window",
                "segment_summary",
                "data_sources",
                "data_source_summary",
                "sqlite_health",
                "archive_health",
                "market_context_health",
                "lp_stage_summary",
                "trade_action_summary",
                "asset_market_state_summary",
                "no_trade_lock_summary",
                "trade_opportunity_summary",
                "candidate_verified_summary",
                "blocker_summary",
                "outcome_summary",
                "telegram_suppression_summary",
                "prealert_lifecycle_summary",
                "major_coverage_summary",
                "maturity_summary",
                "limitations",
                "key_findings",
                "key_risks",
                "next_actions",
            ):
                self.assertIn(key, payload)

            markdown = Path(written["dated_markdown"]).read_text(encoding="utf-8")
            self.assertIn("CANDIDATE", markdown)
            self.assertIn("VERIFIED maturity=immature", markdown)


if __name__ == "__main__":
    unittest.main()
