from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from reports.generate_daily_compare_report import generate_daily_compare


class DailyCompareTradeReplayTests(unittest.TestCase):
    def _daily_payload(
        self,
        logical_date: str,
        *,
        replay_available: bool,
        replay_count: int,
        valid_replay_count: int,
        avg_net_pnl_bps: float,
        clean_followthrough_rate: float,
        bad_entry_rate: float,
        absorption_reversal_rate: float,
        data_invalid_rate: float,
        shadow_candidate_count: int,
        shadow_verified_count: int,
        data_quality_status: str,
    ) -> dict:
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
                "total_raw_events": 100,
                "total_parsed_events": 100,
                "total_signal_rows": 10,
                "lp_signal_rows": 4,
                "delivered_lp_signals": 2,
                "suppressed_lp_signals": 1,
                "asset_case_count": 1,
                "case_followup_count": 1,
            },
            "telegram_suppression_summary": {
                "telegram_should_send_count": 2,
                "telegram_suppressed_count": 1,
                "telegram_suppression_ratio": 0.5,
                "telegram_suppression_reasons": {"no_trade": 1},
                "messages_before_suppression_estimate": 3,
                "messages_after_suppression_actual": 2,
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
                "opportunity_candidate_count": 2,
                "opportunity_verified_count": 0,
                "opportunity_blocked_count": 1,
                "opportunity_candidate_to_verified_rate": 0.0,
                "candidate_outcome_60s": {"count": 2, "resolved_count": 2, "followthrough_rate": 0.5, "adverse_rate": 0.0},
                "verified_maturity": "unknown",
                "verified_should_not_be_traded_reason": "insufficient_data",
                "maturity_reasons": ["insufficient_data"],
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
                    "market_context_attempt_success_rate": 1.0,
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
            "trade_replay_summary": {
                "trade_replay_available": replay_available,
                "replay_count": replay_count,
                "valid_replay_count": valid_replay_count,
                "avg_net_pnl_bps": avg_net_pnl_bps,
                "clean_followthrough_rate": clean_followthrough_rate,
                "bad_entry_rate": bad_entry_rate,
                "absorption_reversal_rate": absorption_reversal_rate,
                "data_invalid_rate": data_invalid_rate,
                "top_positive_profiles": [],
                "top_negative_profiles": [],
            },
            "shadow_opportunity_summary": {
                "shadow_candidate_count": shadow_candidate_count,
                "shadow_verified_count": shadow_verified_count,
                "shadow_replay_count": replay_count if replay_available else 0,
            },
            "data_quality_summary": {
                "data_quality_status": data_quality_status,
                "db_archive_mirror_match_rate": 1.0,
                "mismatch_categories": [],
                "mismatch_rows": 0,
                "market_context_success_rate": 1.0,
                "zero_activity_day": False,
            },
            "runtime_health": {
                "active_hours": 10.0,
                "raw_events_count": 100,
                "parsed_events_count": 100,
                "signals_count": 10,
                "max_raw_event_gap_sec": 60,
                "max_signal_gap_sec": 120,
                "zero_activity_day": 0,
            },
            "limitations": [] if replay_available else ["trade_replay_missing"],
        }

    def test_daily_compare_outputs_replay_compare(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports_dir = Path(temp_dir) / "reports"
            daily_dir = reports_dir / "daily"
            output_dir = reports_dir / "daily_compare"
            daily_dir.mkdir(parents=True)
            (daily_dir / "daily_report_2026-04-29.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-29",
                        replay_available=True,
                        replay_count=8,
                        valid_replay_count=6,
                        avg_net_pnl_bps=10.0,
                        clean_followthrough_rate=0.30,
                        bad_entry_rate=0.20,
                        absorption_reversal_rate=0.10,
                        data_invalid_rate=0.25,
                        shadow_candidate_count=2,
                        shadow_verified_count=1,
                        data_quality_status="valid",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (daily_dir / "daily_report_2026-04-30.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-30",
                        replay_available=True,
                        replay_count=10,
                        valid_replay_count=7,
                        avg_net_pnl_bps=12.0,
                        clean_followthrough_rate=0.40,
                        bad_entry_rate=0.10,
                        absorption_reversal_rate=0.05,
                        data_invalid_rate=0.20,
                        shadow_candidate_count=3,
                        shadow_verified_count=2,
                        data_quality_status="valid",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-30",
            )

            self.assertEqual(0, exit_code)
            self.assertIn("replay_compare", payload)
            self.assertEqual("ok", payload["replay_compare"]["status"])
            self.assertEqual(
                "improvement",
                payload["replay_compare"]["metrics"]["clean_followthrough_rate"]["classification"],
            )
            self.assertIn("Replay / Shadow / Data Quality 对比", payload["markdown"])

    def test_daily_compare_marks_insufficient_when_one_side_missing_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports_dir = Path(temp_dir) / "reports"
            daily_dir = reports_dir / "daily"
            output_dir = reports_dir / "daily_compare"
            daily_dir.mkdir(parents=True)
            (daily_dir / "daily_report_2026-04-29.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-29",
                        replay_available=False,
                        replay_count=0,
                        valid_replay_count=0,
                        avg_net_pnl_bps=0.0,
                        clean_followthrough_rate=0.0,
                        bad_entry_rate=0.0,
                        absorption_reversal_rate=0.0,
                        data_invalid_rate=0.0,
                        shadow_candidate_count=0,
                        shadow_verified_count=0,
                        data_quality_status="valid",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (daily_dir / "daily_report_2026-04-30.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-30",
                        replay_available=True,
                        replay_count=10,
                        valid_replay_count=7,
                        avg_net_pnl_bps=12.0,
                        clean_followthrough_rate=0.40,
                        bad_entry_rate=0.10,
                        absorption_reversal_rate=0.05,
                        data_invalid_rate=0.20,
                        shadow_candidate_count=3,
                        shadow_verified_count=2,
                        data_quality_status="valid",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-30",
            )

            self.assertEqual(0, exit_code)
            self.assertEqual("insufficient", payload["replay_compare"]["status"])
            self.assertIn("replay_compare_insufficient:missing_replay", payload["replay_compare"]["warnings"])

    def test_daily_compare_degrades_when_data_quality_is_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            reports_dir = Path(temp_dir) / "reports"
            daily_dir = reports_dir / "daily"
            output_dir = reports_dir / "daily_compare"
            daily_dir.mkdir(parents=True)
            (daily_dir / "daily_report_2026-04-29.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-29",
                        replay_available=True,
                        replay_count=8,
                        valid_replay_count=6,
                        avg_net_pnl_bps=10.0,
                        clean_followthrough_rate=0.30,
                        bad_entry_rate=0.20,
                        absorption_reversal_rate=0.10,
                        data_invalid_rate=0.25,
                        shadow_candidate_count=2,
                        shadow_verified_count=1,
                        data_quality_status="degraded",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (daily_dir / "daily_report_2026-04-30.json").write_text(
                json.dumps(
                    self._daily_payload(
                        "2026-04-30",
                        replay_available=True,
                        replay_count=10,
                        valid_replay_count=7,
                        avg_net_pnl_bps=12.0,
                        clean_followthrough_rate=0.40,
                        bad_entry_rate=0.10,
                        absorption_reversal_rate=0.05,
                        data_invalid_rate=0.20,
                        shadow_candidate_count=3,
                        shadow_verified_count=2,
                        data_quality_status="valid",
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            exit_code, payload, _ = generate_daily_compare(
                reports_dir=reports_dir,
                output_dir=output_dir,
                allow_generate=False,
                requested_date="2026-04-30",
            )

            self.assertEqual(0, exit_code)
            self.assertEqual("evidence_insufficient", payload["replay_compare"]["status"])
            self.assertIn("evidence_insufficient", payload["question_answers"]["1"])


if __name__ == "__main__":
    unittest.main()
