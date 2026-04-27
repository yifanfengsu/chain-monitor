import json
import tempfile
import unittest
from pathlib import Path

from reports.generate_daily_compare_report import generate_daily_compare
from app.test_daily_compare_report import _sample_values, _seed_day


def _daily_payload(logical_date: str, raw_events: int, candidates: int) -> dict:
    return {
        "report_type": "daily_canonical",
        "logical_date": logical_date,
        "analysis_window": {"end_utc": f"{logical_date} 15:59:59 UTC"},
        "data_source_summary": {
            "report_data_source": "sqlite",
            "data_source": "sqlite",
            "source_components": {"signals": "sqlite"},
            "archive_fallback_used": False,
            "db_archive_mirror_match_rate": 1.0,
            "mismatch_warnings": [],
            "sqlite_health": {"missing_tables": []},
        },
        "run_overview": {
            "total_raw_events": raw_events,
            "total_parsed_events": raw_events,
            "total_signal_rows": raw_events // 2,
            "lp_signal_rows": candidates + 3,
            "delivered_lp_signals": candidates,
            "suppressed_lp_signals": 1,
            "asset_case_count": 1,
            "case_followup_count": 1,
        },
        "telegram_suppression_summary": {
            "telegram_should_send_count": candidates,
            "telegram_suppressed_count": 1,
            "telegram_suppression_ratio": 0.25,
            "telegram_suppression_reasons": {"no_trade": 1},
            "messages_before_suppression_estimate": candidates + 1,
            "messages_after_suppression_actual": candidates,
            "high_value_suppressed_count": 0,
        },
        "asset_market_state_summary": {
            "state_distribution": {"OBSERVE_LONG": 1},
            "state_transition_count": 1,
            "final_state_by_asset": {"ETH": {"state_key": "OBSERVE_LONG"}},
        },
        "no_trade_lock_summary": {
            "lock_entered_count": 0,
            "lock_suppressed_count": 0,
            "lock_released_count": 0,
        },
        "trade_action_summary": {"trade_action_distribution": {"WAIT_CONFIRMATION": 1}},
        "trade_opportunity_summary": {
            "opportunity_none_count": 1,
            "opportunity_candidate_count": candidates,
            "opportunity_verified_count": 0,
            "opportunity_blocked_count": 1,
            "opportunity_candidate_to_verified_rate": 0.0,
            "candidate_outcome_60s": {"count": candidates, "resolved_count": candidates, "followthrough_rate": 0.5, "adverse_rate": 0.0},
            "verified_maturity": "immature",
            "verified_should_not_be_traded_reason": "sample_low",
            "maturity_reasons": ["sample_low"],
            "top_blockers": {"history_completion_too_low": 1},
        },
        "candidate_tradeable_summary": {"candidate_outcome_completed_rate": 1.0},
        "prealert_lifecycle_summary": {
            "prealert_candidate_count": 1,
            "prealert_gate_passed_count": 1,
            "prealert_active_count": 0,
            "prealert_delivered_count": 0,
            "prealert_upgraded_to_confirm_count": 0,
            "prealert_expired_count": 0,
        },
        "outcome_source_summary": {
            "outcome_30s_completed_rate": 1.0,
            "outcome_60s_completed_rate": 1.0,
            "outcome_300s_completed_rate": 1.0,
            "outcome_price_source_distribution": {"okx_mark": 1},
            "outcome_failure_reason_distribution": {},
        },
        "market_context_health": {
            "window": {
                "live_public_rate": 1.0,
                "unavailable_rate": 0.0,
                "okx_attempts": 1,
                "okx_success": 1,
                "kraken_attempts": 0,
                "kraken_success": 0,
            }
        },
        "majors_coverage_summary": {
            "covered_major_pairs": ["ETH/USDT"],
            "missing_major_pairs": ["BTC/USDT"],
            "eth_signal_count": 1,
            "btc_signal_count": 0,
            "sol_signal_count": 0,
            "current_sample_still_eth_only": True,
        },
        "limitations": [],
    }


class DailyCompareUsesDailyReportTests(unittest.TestCase):
    def test_compare_prefers_daily_report_without_afternoon_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "reports"
            daily_dir = reports_dir / "daily"
            output_dir = reports_dir / "daily_compare"
            daily_dir.mkdir(parents=True)
            for logical_date, raw_events, candidates in (
                ("2026-04-23", 100, 1),
                ("2026-04-24", 120, 2),
            ):
                (daily_dir / f"daily_report_{logical_date}.json").write_text(
                    json.dumps(_daily_payload(logical_date, raw_events, candidates), ensure_ascii=False),
                    encoding="utf-8",
                )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-24",
            )

            self.assertEqual(0, exit_code)
            self.assertTrue(payload["compare_available"])
            self.assertEqual("canonical_daily_report", payload["source_mode"])
            self.assertIn("daily_report", payload["source_files"]["today"])
            self.assertIn("daily_report_2026-04-24.json", payload["source_files"]["today"]["daily_report"]["path"])
            self.assertNotIn("afternoon_evening_state", payload["source_files"]["today"])
            self.assertNotIn("legacy_source_used", payload["limitations"])

    def test_default_compare_does_not_use_legacy_three_report_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "reports"
            output_dir = reports_dir / "daily_compare"
            _seed_day(
                reports_dir,
                "2026-04-23",
                _sample_values(
                    raw_events=100,
                    candidate_count=1,
                    candidate_ft=0.4,
                    candidate_adv=0.2,
                    candidate_completion=1.0,
                    blocker_saved=0.5,
                    blocker_false=0.2,
                    outcome_rate=0.5,
                    market_success=0.6,
                    messages_after=3,
                    high_value_suppressed=1,
                ),
            )
            _seed_day(
                reports_dir,
                "2026-04-24",
                _sample_values(
                    raw_events=120,
                    candidate_count=2,
                    candidate_ft=0.6,
                    candidate_adv=0.1,
                    candidate_completion=1.0,
                    blocker_saved=0.7,
                    blocker_false=0.1,
                    outcome_rate=0.7,
                    market_success=0.8,
                    messages_after=2,
                    high_value_suppressed=0,
                ),
            )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-24",
            )

            self.assertEqual(0, exit_code)
            self.assertFalse(payload["compare_available"])
            self.assertNotEqual("legacy_three_report_bundle", payload["source_mode"])
            self.assertFalse(payload["legacy_fallback_used"])
            self.assertEqual(["daily_report"], list(payload["source_files"]["today"].keys()))
            self.assertIn("missing_summary:daily_report:2026-04-24", payload["limitations"])

    def test_legacy_three_report_bundle_requires_explicit_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reports_dir = root / "reports"
            output_dir = reports_dir / "daily_compare"
            for logical_date, raw_events, candidates in (
                ("2026-04-23", 100, 1),
                ("2026-04-24", 120, 2),
            ):
                _seed_day(
                    reports_dir,
                    logical_date,
                    _sample_values(
                        raw_events=raw_events,
                        candidate_count=candidates,
                        candidate_ft=0.5,
                        candidate_adv=0.1,
                        candidate_completion=1.0,
                        blocker_saved=0.6,
                        blocker_false=0.1,
                        outcome_rate=0.8,
                        market_success=0.9,
                        messages_after=2,
                        high_value_suppressed=0,
                    ),
                )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-24",
                legacy_fallback=True,
            )

            self.assertEqual(0, exit_code)
            self.assertTrue(payload["compare_available"])
            self.assertEqual("legacy_three_report_bundle", payload["source_mode"])
            self.assertTrue(payload["legacy_fallback_used"])
            self.assertEqual(
                {"afternoon_evening_state", "overnight_trade_action", "overnight_run"},
                set(payload["source_files"]["today"].keys()),
            )
            self.assertIn("legacy_source_used", payload["limitations"])


if __name__ == "__main__":
    unittest.main()
