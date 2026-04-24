import csv
import io
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from reports.generate_daily_compare_report import REPORT_ORDER, generate_daily_compare


def _base_data_source_summary() -> dict:
    return {
        "report_data_source": "sqlite",
        "data_source": "sqlite",
        "source_components": {"signals": "sqlite"},
        "archive_fallback_used": False,
        "db_archive_mirror_match_rate": 1.0,
        "mismatch_warnings": [],
        "sqlite_health": {"missing_tables": []},
    }


def _build_afternoon_summary(logical_date: str, values: dict) -> dict:
    return {
        "analysis_window": {"end_utc": f"{logical_date} 05:00:00 UTC"},
        "run_overview": {
            "total_raw_events": values["raw_events_count"],
            "total_parsed_events": values["parsed_events_count"],
            "total_signal_rows": values["signals_count"],
            "lp_signal_rows": values["lp_signal_rows"],
            "delivered_lp_signals": values["delivered_lp_signals"],
            "suppressed_lp_signals": values["suppressed_lp_signals"],
            "asset_case_count": values["asset_case_count"],
            "case_followup_count": values["case_followup_count"],
        },
        "telegram_suppression_summary": {
            "telegram_should_send_count": values["telegram_should_send_count"],
            "telegram_suppressed_count": values["telegram_suppressed_count"],
            "telegram_suppression_ratio": values["telegram_suppression_ratio"],
            "telegram_suppression_reasons": values["telegram_suppression_reasons"],
            "messages_before_suppression_estimate": values["messages_before_suppression_estimate"],
            "messages_after_suppression_actual": values["messages_after_suppression_actual"],
            "high_value_suppressed_count": values["high_value_suppressed_count"],
        },
        "asset_market_state_summary": {
            "state_distribution": values["state_distribution"],
            "state_transition_count": values["state_transition_count"],
            "final_state_by_asset": values["final_state_by_asset"],
        },
        "no_trade_lock_summary": {
            "lock_entered_count": values["no_trade_lock_entered_count"],
            "lock_suppressed_count": values["no_trade_lock_suppressed_count"],
            "lock_released_count": values["no_trade_lock_released_count"],
        },
        "trade_action_summary": {"trade_action_distribution": values["trade_action_distribution"]},
        "trade_opportunity_summary": {
            "opportunity_none_count": values["opportunity_none_count"],
            "opportunity_candidate_count": values["opportunity_candidate_count"],
            "opportunity_verified_count": values["opportunity_verified_count"],
            "opportunity_blocked_count": values["opportunity_blocked_count"],
            "opportunity_candidate_to_verified_rate": values["candidate_to_verified_rate"],
            "opportunity_score_median": values["opportunity_score_median"],
            "opportunity_score_p90": values["opportunity_score_p90"],
            "opportunity_hard_blocker_distribution": values["hard_blocker_distribution"],
            "candidate_outcome_60s": {
                "count": values["opportunity_candidate_count"],
                "resolved_count": values["candidate_resolved_count"],
                "followthrough_rate": values["candidate_60s_followthrough_rate"],
                "adverse_rate": values["candidate_60s_adverse_rate"],
            },
            "verified_outcome_60s": {
                "count": values["opportunity_verified_count"],
                "resolved_count": values["verified_resolved_count"],
                "followthrough_rate": values["verified_60s_followthrough_rate"],
                "adverse_rate": values["verified_60s_adverse_rate"],
            },
            "top_blockers": values["blocker_distribution"],
            "why_no_opportunities": values["why_no_opportunities"],
        },
        "candidate_tradeable_summary": {
            "candidate_outcome_completed_rate": values["candidate_outcome_completion_rate"],
        },
        "prealert_lifecycle_summary": {
            "prealert_candidate_count": values["prealert_candidate_count"],
            "prealert_gate_passed_count": values["prealert_gate_passed_count"],
            "prealert_active_count": values["prealert_active_count"],
            "prealert_delivered_count": values["prealert_delivered_count"],
            "prealert_upgraded_to_confirm_count": values["prealert_upgraded_to_confirm_count"],
            "prealert_expired_count": values["prealert_expired_count"],
            "median_prealert_to_confirm_sec": values["median_prealert_to_confirm_sec"],
        },
        "outcome_source_summary": {
            "outcome_30s_completed_rate": values["outcome_30s_completed_rate"],
            "outcome_60s_completed_rate": values["outcome_60s_completed_rate"],
            "outcome_300s_completed_rate": values["outcome_300s_completed_rate"],
            "outcome_price_source_distribution": values["outcome_price_source_distribution"],
            "expired_rate_by_window": values["expired_rate_by_window"],
            "outcome_failure_reason_distribution": values["outcome_failure_reason_distribution"],
            "catchup_completed_count": values["catchup_completed_count"],
            "catchup_expired_count": values["catchup_expired_count"],
            "scheduler_health_summary": values["scheduler_health_summary"],
        },
        "market_context_health": {
            "window": {
                "live_public_rate": values["live_public_rate"],
                "unavailable_rate": values["unavailable_rate"],
                "okx_attempts": values["okx_attempts"],
                "okx_success": values["okx_success"],
                "kraken_attempts": values["kraken_attempts"],
                "kraken_success": values["kraken_success"],
                "resolved_symbol_distribution": values["resolved_symbol_distribution"],
                "top_failure_reasons": values["top_failure_reasons"],
            }
        },
        "majors_coverage_summary": {
            "covered_major_pairs": values["covered_major_pairs"],
            "missing_major_pairs": values["missing_major_pairs"],
            "eth_signal_count": values["eth_signal_count"],
            "btc_signal_count": values["btc_signal_count"],
            "sol_signal_count": values["sol_signal_count"],
            "current_sample_still_eth_only": values["current_sample_still_eth_only"],
            "major_cli_summary": {
                "configured_but_disabled_major_pools": values["configured_but_disabled_major_pools"],
            },
        },
        "data_source_summary": _base_data_source_summary(),
        "limitations": values.get("limitations", []),
    }


def _build_overnight_trade_action_summary(logical_date: str, values: dict) -> dict:
    return {
        "analysis_window": {"end_utc": f"{logical_date} 06:00:00 UTC"},
        "telegram_suppression_summary": {
            "total_suppressed": values["telegram_suppressed_count"],
            "suppression_reasons": values["telegram_suppression_reasons"],
            "messages_before_after_suppression_estimate": {
                "raw_lp_signals": values["messages_before_suppression_estimate"],
                "sent_telegram_messages": values["messages_after_suppression_actual"],
            },
        },
        "asset_market_state_summary": {
            "state_distribution": values["state_distribution"],
            "state_change_count": values["state_transition_count"],
            "current_final_state_per_asset": values["final_state_by_asset"],
        },
        "no_trade_lock_summary": {
            "lock_entered_count": values["no_trade_lock_entered_count"],
            "suppressed_count": values["no_trade_lock_suppressed_count"],
            "release_count": values["no_trade_lock_released_count"],
        },
        "trade_action_summary": {"trade_action_distribution": values["trade_action_distribution"]},
        "trade_opportunity_summary": {
            "opportunity_none_count": values["opportunity_none_count"],
            "opportunity_candidate_count": values["opportunity_candidate_count"],
            "opportunity_verified_count": values["opportunity_verified_count"],
            "opportunity_blocked_count": values["opportunity_blocked_count"],
            "opportunity_candidate_to_verified_rate": values["candidate_to_verified_rate"],
            "opportunity_score_median": values["opportunity_score_median"],
            "opportunity_score_p90": values["opportunity_score_p90"],
            "blocker_saved_rate": values["blocker_saved_rate"],
            "blocker_false_block_rate": values["blocker_false_block_rate"],
            "blocker_effectiveness": {
                "top_effective_blockers": values["top_effective_blockers"],
                "top_overblocking_blockers": values["top_overblocking_blockers"],
            },
            "hard_blocker_distribution": values["hard_blocker_distribution"],
            "verification_blocker_distribution": values["verification_blocker_distribution"],
            "candidate_outcome_60s": {
                "count": values["opportunity_candidate_count"],
                "resolved_count": values["candidate_resolved_count"],
                "followthrough_rate": values["candidate_60s_followthrough_rate"],
                "adverse_rate": values["candidate_60s_adverse_rate"],
            },
            "verified_outcome_60s": {
                "count": values["opportunity_verified_count"],
                "resolved_count": values["verified_resolved_count"],
                "followthrough_rate": values["verified_60s_followthrough_rate"],
                "adverse_rate": values["verified_60s_adverse_rate"],
            },
            "top_blockers": values["blocker_distribution"],
            "why_no_opportunities": values["why_no_opportunities"],
        },
        "prealert_lifecycle_summary": {
            "active_count": values["prealert_active_count"],
            "delivered_count": values["prealert_delivered_count"],
            "upgraded_count": values["prealert_upgraded_to_confirm_count"],
            "expired_count": values["prealert_expired_count"],
        },
        "outcome_price_source_summary": {
            "outcome_30s_completed_rate": values["outcome_30s_completed_rate"],
            "outcome_60s_completed_rate": values["outcome_60s_completed_rate"],
            "outcome_300s_completed_rate": values["outcome_300s_completed_rate"],
            "outcome_price_source_distribution": values["outcome_price_source_distribution"],
            "outcome_failure_reason_distribution": values["outcome_failure_reason_distribution"],
            "catchup_completed_count": values["catchup_completed_count"],
            "catchup_expired_count": values["catchup_expired_count"],
            "scheduler_health_summary": values["scheduler_health_summary"],
        },
        "market_context_health": {
            "window": {
                "live_public_rate": values["live_public_rate"],
                "unavailable_rate": values["unavailable_rate"],
                "okx_attempts": values["okx_attempts"],
                "okx_success": values["okx_success"],
                "kraken_attempts": values["kraken_attempts"],
                "kraken_success": values["kraken_success"],
                "resolved_symbol_distribution": values["resolved_symbol_distribution"],
                "top_failure_reasons": values["top_failure_reasons"],
            }
        },
        "majors_coverage_summary": {
            "covered_major_pairs": values["covered_major_pairs"],
            "missing_major_pairs": values["missing_major_pairs"],
            "eth_signal_count": values["eth_signal_count"],
            "btc_signal_count": values["btc_signal_count"],
            "sol_signal_count": values["sol_signal_count"],
            "current_sample_still_eth_only": values["current_sample_still_eth_only"],
            "major_cli_summary": {
                "configured_but_disabled_major_pools": values["configured_but_disabled_major_pools"],
            },
        },
        "data_source_summary": _base_data_source_summary(),
        "final_trading_output_summary": {
            "legacy_chase_leaked_count": values["legacy_chase_leaked_count"],
        },
        "limitations": values.get("limitations", []),
    }


def _build_overnight_run_summary(logical_date: str, values: dict) -> dict:
    return {
        "analysis_window": {"end_utc": f"{logical_date} 04:00:00 UTC"},
        "telegram_suppression_summary": {
            "total_suppressed": values["telegram_suppressed_count"],
            "suppression_reasons": values["telegram_suppression_reasons"],
            "messages_before_after_suppression_estimate": {
                "raw_lp_signals": values["messages_before_suppression_estimate"],
                "sent_telegram_messages": values["messages_after_suppression_actual"],
            },
        },
        "asset_market_state_summary": {
            "state_distribution": values["state_distribution"],
            "state_change_count": values["state_transition_count"],
            "current_final_state_per_asset": values["final_state_by_asset"],
        },
        "no_trade_lock_summary": {
            "lock_entered_count": values["no_trade_lock_entered_count"],
            "suppressed_count": values["no_trade_lock_suppressed_count"],
            "release_count": values["no_trade_lock_released_count"],
        },
        "prealert_lifecycle_summary": {
            "active_count": values["prealert_active_count"],
            "delivered_count": values["prealert_delivered_count"],
            "upgraded_count": values["prealert_upgraded_to_confirm_count"],
            "expired_count": values["prealert_expired_count"],
        },
        "market_context_health": {
            "window": {
                "live_public_rate": values["live_public_rate"],
                "unavailable_rate": values["unavailable_rate"],
                "okx_attempts": values["okx_attempts"],
                "okx_success": values["okx_success"],
                "kraken_attempts": values["kraken_attempts"],
                "kraken_success": values["kraken_success"],
                "resolved_symbol_distribution": values["resolved_symbol_distribution"],
                "top_failure_reasons": values["top_failure_reasons"],
            }
        },
        "majors_coverage_summary": {
            "covered_major_pairs": values["covered_major_pairs"],
            "missing_major_pairs": values["missing_major_pairs"],
            "eth_signal_count": values["eth_signal_count"],
            "btc_signal_count": values["btc_signal_count"],
            "sol_signal_count": values["sol_signal_count"],
            "current_sample_still_eth_only": values["current_sample_still_eth_only"],
            "major_cli_summary": {
                "configured_but_disabled_major_pools": values["configured_but_disabled_major_pools"],
            },
        },
        "data_source_summary": _base_data_source_summary(),
        "limitations": values.get("limitations", []),
    }


def _write_summary(reports_dir: Path, filename: str, payload: dict) -> None:
    path = reports_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _seed_day(reports_dir: Path, logical_date: str, values: dict, *, latest: bool = False, filename_date: str | None = None) -> None:
    day_suffix = filename_date or logical_date
    afternoon_payload = _build_afternoon_summary(logical_date, values)
    action_payload = _build_overnight_trade_action_summary(logical_date, values)
    run_payload = _build_overnight_run_summary(logical_date, values)
    if latest:
        _write_summary(reports_dir, "afternoon_evening_state_summary_latest.json", afternoon_payload)
        _write_summary(reports_dir, "overnight_trade_action_summary_latest.json", action_payload)
        _write_summary(reports_dir, "overnight_run_summary_latest.json", run_payload)
        return
    _write_summary(reports_dir, f"afternoon_evening_state_summary_latest_{day_suffix}.json", afternoon_payload)
    _write_summary(reports_dir, f"overnight_trade_action_summary_latest_{day_suffix}.json", action_payload)
    _write_summary(reports_dir, f"overnight_run_summary_latest_{day_suffix}.json", run_payload)


def _sample_values(*, raw_events: int, candidate_count: int, candidate_ft: float | None, candidate_adv: float | None, candidate_completion: float | None, blocker_saved: float, blocker_false: float, outcome_rate: float, market_success: float, messages_after: int, high_value_suppressed: int, verified_count: int = 0, missing_major_pairs: list[str] | None = None, eth_only: bool = True, leaked: int = 0) -> dict:
    missing_major_pairs = missing_major_pairs if missing_major_pairs is not None else ["BTC/USDT", "SOL/USDT"]
    okx_attempts = 10
    kraken_attempts = 0
    okx_success = int(round(okx_attempts * market_success))
    return {
        "raw_events_count": raw_events,
        "parsed_events_count": raw_events - 5,
        "signals_count": raw_events // 2,
        "lp_signal_rows": max(candidate_count + 4, 4),
        "delivered_lp_signals": max(candidate_count, 0),
        "suppressed_lp_signals": max(raw_events // 10, 1),
        "asset_case_count": 3,
        "case_followup_count": 1,
        "telegram_should_send_count": max(candidate_count, 1),
        "telegram_suppressed_count": max(raw_events // 20, 1),
        "telegram_suppression_ratio": 0.5,
        "telegram_suppression_reasons": {"no_trade_opportunity": max(raw_events // 20, 1)},
        "messages_before_suppression_estimate": max(raw_events // 20 + messages_after, 1),
        "messages_after_suppression_actual": messages_after,
        "high_value_suppressed_count": high_value_suppressed,
        "state_distribution": {"OBSERVE_LONG": 2, "NO_TRADE_LOCK": 1},
        "state_transition_count": 2,
        "final_state_by_asset": {"ETH": {"state_key": "OBSERVE_LONG", "state_label": "偏多观察"}},
        "no_trade_lock_entered_count": 1,
        "no_trade_lock_suppressed_count": 1,
        "no_trade_lock_released_count": 1,
        "trade_action_distribution": {"WAIT_CONFIRMATION": 2, "NO_TRADE": 1},
        "opportunity_none_count": 5,
        "opportunity_candidate_count": candidate_count,
        "opportunity_verified_count": verified_count,
        "opportunity_blocked_count": 2,
        "candidate_to_verified_rate": 0.0 if not candidate_count else round(verified_count / candidate_count, 4),
        "opportunity_score_median": 0.55,
        "opportunity_score_p90": 0.80,
        "hard_blocker_distribution": {"history_completion_too_low": 2},
        "verification_blocker_distribution": {"history_completion_too_low": 2},
        "candidate_resolved_count": 0 if candidate_count == 0 or candidate_completion is None else int(round(candidate_count * candidate_completion)),
        "candidate_60s_followthrough_rate": candidate_ft,
        "candidate_60s_adverse_rate": candidate_adv,
        "candidate_outcome_completion_rate": candidate_completion,
        "verified_resolved_count": verified_count,
        "verified_60s_followthrough_rate": 0.5 if verified_count else None,
        "verified_60s_adverse_rate": 0.25 if verified_count else None,
        "blocker_saved_rate": blocker_saved,
        "blocker_false_block_rate": blocker_false,
        "blocker_distribution": {"history_completion_too_low": 2},
        "top_effective_blockers": [{"blocker": "history_completion_too_low", "saved_rate": blocker_saved}],
        "top_overblocking_blockers": [{"blocker": "history_completion_too_low", "false_block_rate": blocker_false}],
        "why_no_opportunities": ["history_completion_too_low"] if not verified_count else [],
        "prealert_candidate_count": 1,
        "prealert_gate_passed_count": 1,
        "prealert_active_count": 0,
        "prealert_delivered_count": 0,
        "prealert_upgraded_to_confirm_count": 1,
        "prealert_expired_count": 0,
        "median_prealert_to_confirm_sec": 30,
        "outcome_30s_completed_rate": outcome_rate,
        "outcome_60s_completed_rate": outcome_rate,
        "outcome_300s_completed_rate": outcome_rate,
        "outcome_price_source_distribution": {"okx_mark": 12},
        "expired_rate_by_window": {"30s": 0.0, "60s": 0.0, "300s": 0.0},
        "outcome_failure_reason_distribution": {},
        "catchup_completed_count": 0,
        "catchup_expired_count": 0,
        "scheduler_health_summary": {"pending_count": 0, "completed_count": 12},
        "live_public_rate": market_success,
        "unavailable_rate": round(1.0 - market_success, 4),
        "okx_attempts": okx_attempts,
        "okx_success": okx_success,
        "kraken_attempts": kraken_attempts,
        "kraken_success": 0,
        "resolved_symbol_distribution": {"ETH-USDT-SWAP": 4},
        "top_failure_reasons": [],
        "covered_major_pairs": ["ETH/USDT"],
        "missing_major_pairs": missing_major_pairs,
        "configured_but_disabled_major_pools": ["BTC/USDT", "SOL/USDT"],
        "eth_signal_count": 4,
        "btc_signal_count": 0 if "BTC/USDT" in missing_major_pairs else 1,
        "sol_signal_count": 0 if "SOL/USDT" in missing_major_pairs else 1,
        "current_sample_still_eth_only": eth_only,
        "legacy_chase_leaked_count": leaked,
    }


class DailyCompareReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.reports_dir = self.root / "reports"
        self.output_dir = self.reports_dir / "daily_compare"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_missing_previous_fails_closed_without_fabrication(self) -> None:
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=100, candidate_count=1, candidate_ft=0.6, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.6, blocker_false=0.1, outcome_rate=0.8, market_success=0.9, messages_after=2, high_value_suppressed=0))

        exit_code, payload, written = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )

        self.assertEqual(0, exit_code)
        self.assertFalse(payload["compare_available"])
        self.assertEqual("2026-04-22", payload["today_date"])
        self.assertIsNone(payload["previous_date"])
        self.assertIn("missing_previous_date", payload["limitations"])
        self.assertEqual(set(REPORT_ORDER), set(payload["source_files"]["today"].keys()))
        self.assertEqual(set(REPORT_ORDER), set(payload["source_files"]["previous"].keys()))
        self.assertIsNone(payload["strict_failure_reason"])
        self.assertEqual([], payload["rebuild_warnings"])
        self.assertIn("rebuild_summary", payload)
        self.assertFalse(payload["rebuild_summary"]["attempted"])
        self.assertNotIn("dated_json", written)
        self.assertTrue((self.output_dir / "daily_compare_latest.json").exists())

        strict_code, strict_payload, strict_written = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
            strict=True,
        )
        self.assertEqual(2, strict_code)
        self.assertFalse(strict_payload["compare_available"])
        self.assertEqual("no_previous_available", strict_payload["strict_failure_reason"])
        self.assertFalse(strict_payload["rebuild_summary"]["attempted"])
        self.assertIn("## 重建尝试与严格模式结果", strict_payload["markdown"])
        self.assertIn("strict_failure_reason: `no_previous_available`", strict_payload["markdown"])
        self.assertEqual({}, strict_written)

    def test_csv_math_limitations_and_markdown_output(self) -> None:
        previous = _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2, missing_major_pairs=["BTC/USDT", "SOL/USDT"], eth_only=True, leaked=2)
        today = _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1, missing_major_pairs=["SOL/USDT"], eth_only=False, leaked=0)
        today["limitations"] = ["today_field_gap"]
        _seed_day(self.reports_dir, "2026-04-21", previous)
        _seed_day(self.reports_dir, "2026-04-22", today)

        exit_code, payload, written = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )

        self.assertEqual(0, exit_code)
        self.assertTrue(payload["compare_available"])
        self.assertEqual("2026-04-22", payload["today_date"])
        self.assertEqual("2026-04-21", payload["previous_date"])
        self.assertEqual("exact_previous_day", payload["compare_basis"])
        self.assertIn("dated_json", written)

        csv_rows = list(csv.DictReader(io.StringIO(payload["csv"])))
        raw_row = next(row for row in csv_rows if row["metric_name"] == "raw_events_count")
        self.assertEqual("25.0", raw_row["abs_change"])
        self.assertEqual("25.0", raw_row["pct_change"])

        self.assertTrue(any("candidate_30s_followthrough_rate" in item for item in payload["limitations"]))

        markdown = payload["markdown"]
        self.assertIn("today_date: `2026-04-22`", markdown)
        self.assertIn("previous_date: `2026-04-21`", markdown)
        self.assertIn("afternoon_evening_state_summary_latest_2026-04-22.json", markdown)
        self.assertIn("compare_basis: `exact_previous_day`", markdown)

        latest_json = json.loads((self.output_dir / "daily_compare_latest.json").read_text(encoding="utf-8"))
        self.assertEqual("2026-04-22", latest_json["today_date"])

    def test_compare_json_contains_explicit_source_files_structure(self) -> None:
        _seed_day(self.reports_dir, "2026-04-21", _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2))
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1))

        exit_code, payload, _ = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )

        self.assertEqual(0, exit_code)
        self.assertIn("source_files", payload)
        self.assertIn("strict_failure_reason", payload)
        self.assertIn("rebuild_summary", payload)
        self.assertIn("rebuild_warnings", payload)
        self.assertEqual(set(REPORT_ORDER), set(payload["source_files"]["today"].keys()))
        self.assertEqual(set(REPORT_ORDER), set(payload["source_files"]["previous"].keys()))
        entry = payload["source_files"]["today"]["afternoon_evening_state"]
        self.assertIn("path", entry)
        self.assertIn("source_kind", entry)
        self.assertIn("logical_date", entry)
        self.assertIn("warnings", entry)

        latest_json = json.loads((self.output_dir / "daily_compare_latest.json").read_text(encoding="utf-8"))
        self.assertIn("source_files", latest_json)
        self.assertIn("strict_failure_reason", latest_json)
        self.assertIn("rebuild_summary", latest_json)
        self.assertIn("rebuild_warnings", latest_json)
        self.assertEqual(set(REPORT_ORDER), set(latest_json["source_files"]["today"].keys()))

    def test_normal_mode_keeps_missing_source_entry_and_limitations(self) -> None:
        previous = _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2)
        today = _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1)
        _seed_day(self.reports_dir, "2026-04-21", previous)
        _write_summary(self.reports_dir, "afternoon_evening_state_summary_latest_2026-04-22.json", _build_afternoon_summary("2026-04-22", today))
        _write_summary(self.reports_dir, "overnight_trade_action_summary_latest_2026-04-22.json", _build_overnight_trade_action_summary("2026-04-22", today))

        exit_code, payload, _ = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )

        self.assertEqual(0, exit_code)
        self.assertTrue(payload["compare_available"])
        missing_entry = payload["source_files"]["today"]["overnight_run"]
        self.assertIsNone(missing_entry["path"])
        self.assertIn("missing_summary:overnight_run:2026-04-22", missing_entry["warnings"])
        self.assertIn("missing_summary:overnight_run:2026-04-22", payload["limitations"])
        self.assertIsNone(payload["strict_failure_reason"])
        self.assertFalse(payload["rebuild_summary"]["attempted"])
        self.assertEqual([], payload["rebuild_warnings"])

    def test_strict_mode_attempts_rebuild_before_succeeding(self) -> None:
        previous = _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2)
        today = _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1)
        _seed_day(self.reports_dir, "2026-04-21", previous)
        _write_summary(self.reports_dir, "afternoon_evening_state_summary_latest_2026-04-22.json", _build_afternoon_summary("2026-04-22", today))
        _write_summary(self.reports_dir, "overnight_trade_action_summary_latest_2026-04-22.json", _build_overnight_trade_action_summary("2026-04-22", today))

        def _fake_refresh(*, logical_date: str, report_types: list[str] | None = None, **_: object) -> tuple[dict, list[str]]:
            self.assertEqual("2026-04-22", logical_date)
            self.assertEqual(["overnight_run"], report_types)
            _write_summary(
                self.reports_dir,
                "overnight_run_summary_latest_2026-04-22.json",
                _build_overnight_run_summary("2026-04-22", today),
            )
            return (
                {
                    "overnight_run": {
                        "attempted": True,
                        "success": True,
                        "expected_logical_date": "2026-04-22",
                        "actual_logical_date": "2026-04-22",
                        "output_path": "reports/overnight_run_summary_latest_2026-04-22.json",
                        "failure_reason": None,
                        "warning": None,
                    }
                },
                [],
            )

        with mock.patch("reports.generate_daily_compare_report._refresh_dated_reports_for_date", side_effect=_fake_refresh) as refresh_mock:
            exit_code, payload, written = generate_daily_compare(
                reports_dir=self.reports_dir,
                output_dir=self.output_dir,
                strict=True,
            )

        self.assertEqual(0, exit_code)
        self.assertTrue(payload["compare_available"])
        self.assertIsNone(payload["strict_failure_reason"])
        self.assertTrue(payload["rebuild_summary"]["attempted"])
        self.assertTrue(payload["rebuild_summary"]["strict_mode"])
        self.assertFalse(payload["rebuild_summary"]["rebuild_mode"])
        today_attempt = payload["rebuild_summary"]["attempted_report_types"]["today"]["overnight_run"]
        self.assertTrue(today_attempt["attempted"])
        self.assertTrue(today_attempt["success"])
        self.assertEqual("2026-04-22", today_attempt["expected_logical_date"])
        self.assertEqual("2026-04-22", today_attempt["actual_logical_date"])
        self.assertEqual([], payload["rebuild_warnings"])
        self.assertIn("dated_json", written)
        refresh_mock.assert_called_once()

    def test_strict_mode_fails_closed_when_rebuild_cannot_fill_missing_inputs(self) -> None:
        previous = _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2)
        today = _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1)
        _seed_day(self.reports_dir, "2026-04-21", previous)
        _write_summary(self.reports_dir, "afternoon_evening_state_summary_latest_2026-04-22.json", _build_afternoon_summary("2026-04-22", today))
        _write_summary(self.reports_dir, "overnight_trade_action_summary_latest_2026-04-22.json", _build_overnight_trade_action_summary("2026-04-22", today))

        with mock.patch(
            "reports.generate_daily_compare_report._refresh_dated_reports_for_date",
            return_value=(
                {
                    "overnight_run": {
                        "attempted": True,
                        "success": False,
                        "expected_logical_date": "2026-04-22",
                        "actual_logical_date": None,
                        "output_path": "reports/overnight_run_summary_latest_2026-04-22.json",
                        "failure_reason": "generator_failed",
                        "warning": "rebuild_failed:overnight_run:2026-04-22",
                    }
                },
                ["rebuild_failed:overnight_run:2026-04-22"],
            ),
        ) as refresh_mock:
            exit_code, payload, written = generate_daily_compare(
                reports_dir=self.reports_dir,
                output_dir=self.output_dir,
                strict=True,
            )

        self.assertEqual(2, exit_code)
        self.assertFalse(payload["compare_available"])
        self.assertEqual("rebuild_failed", payload["strict_failure_reason"])
        self.assertEqual({}, written)
        self.assertIn("rebuild_failed:overnight_run:2026-04-22", payload["rebuild_warnings"])
        self.assertIn("## 重建尝试与严格模式结果", payload["markdown"])
        self.assertIn("strict_failure_reason: `rebuild_failed`", payload["markdown"])
        refresh_mock.assert_called_once()

    def test_rebuild_mode_sets_rebuild_summary_attempted_true(self) -> None:
        previous = _sample_values(raw_events=100, candidate_count=2, candidate_ft=0.40, candidate_adv=0.30, candidate_completion=0.50, blocker_saved=0.40, blocker_false=0.20, outcome_rate=0.50, market_success=0.60, messages_after=3, high_value_suppressed=2)
        today = _sample_values(raw_events=125, candidate_count=3, candidate_ft=0.60, candidate_adv=0.10, candidate_completion=1.00, blocker_saved=0.70, blocker_false=0.10, outcome_rate=0.75, market_success=0.90, messages_after=1, high_value_suppressed=1)
        _seed_day(self.reports_dir, "2026-04-21", previous)
        _write_summary(self.reports_dir, "afternoon_evening_state_summary_latest_2026-04-22.json", _build_afternoon_summary("2026-04-22", today))
        _write_summary(self.reports_dir, "overnight_trade_action_summary_latest_2026-04-22.json", _build_overnight_trade_action_summary("2026-04-22", today))

        def _fake_refresh(*, logical_date: str, report_types: list[str] | None = None, **_: object) -> tuple[dict, list[str]]:
            self.assertEqual("2026-04-22", logical_date)
            self.assertEqual(["overnight_run"], report_types)
            _write_summary(
                self.reports_dir,
                "overnight_run_summary_latest_2026-04-22.json",
                _build_overnight_run_summary("2026-04-22", today),
            )
            return (
                {
                    "overnight_run": {
                        "attempted": True,
                        "success": True,
                        "expected_logical_date": "2026-04-22",
                        "actual_logical_date": "2026-04-22",
                        "output_path": "reports/overnight_run_summary_latest_2026-04-22.json",
                        "failure_reason": None,
                        "warning": None,
                    }
                },
                [],
            )

        with mock.patch("reports.generate_daily_compare_report._refresh_dated_reports_for_date", side_effect=_fake_refresh):
            exit_code, payload, _ = generate_daily_compare(
                reports_dir=self.reports_dir,
                output_dir=self.output_dir,
                rebuild=True,
            )

        self.assertEqual(0, exit_code)
        self.assertTrue(payload["compare_available"])
        self.assertTrue(payload["rebuild_summary"]["attempted"])
        self.assertFalse(payload["rebuild_summary"]["strict_mode"])
        self.assertTrue(payload["rebuild_summary"]["rebuild_mode"])
        self.assertTrue(payload["rebuild_summary"]["attempted_report_types"]["today"]["overnight_run"]["success"])

    def test_latest_outputs_are_overwritten_by_newer_compare(self) -> None:
        _seed_day(self.reports_dir, "2026-04-21", _sample_values(raw_events=100, candidate_count=1, candidate_ft=0.4, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.5, blocker_false=0.2, outcome_rate=0.5, market_success=0.6, messages_after=3, high_value_suppressed=1))
        _seed_day(self.reports_dir, "2026-04-22", _sample_values(raw_events=110, candidate_count=2, candidate_ft=0.5, candidate_adv=0.2, candidate_completion=1.0, blocker_saved=0.6, blocker_false=0.1, outcome_rate=0.6, market_success=0.7, messages_after=2, high_value_suppressed=1))

        first_code, first_payload, _ = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )
        self.assertEqual(0, first_code)
        self.assertEqual("2026-04-22", first_payload["today_date"])

        _seed_day(self.reports_dir, "2026-04-23", _sample_values(raw_events=140, candidate_count=3, candidate_ft=0.6, candidate_adv=0.1, candidate_completion=1.0, blocker_saved=0.7, blocker_false=0.05, outcome_rate=0.8, market_success=0.9, messages_after=1, high_value_suppressed=0))
        second_code, second_payload, _ = generate_daily_compare(
            reports_dir=self.reports_dir,
            output_dir=self.output_dir,
            allow_generate=False,
        )
        self.assertEqual(0, second_code)
        self.assertEqual("2026-04-23", second_payload["today_date"])
        latest_json = json.loads((self.output_dir / "daily_compare_latest.json").read_text(encoding="utf-8"))
        self.assertEqual("2026-04-23", latest_json["today_date"])
        self.assertEqual("2026-04-22", latest_json["previous_date"])


if __name__ == "__main__":
    unittest.main()
