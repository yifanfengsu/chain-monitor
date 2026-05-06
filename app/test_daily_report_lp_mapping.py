from __future__ import annotations

import unittest
from unittest import mock

import reports.generate_daily_report_latest as report
from app.test_daily_canonical_report import _summary_for
from scripts import hermes_daily_report_schema_check as schema_check


class DailyReportLpMappingTests(unittest.TestCase):
    def test_lp_mapping_fields_are_emitted_from_lp_rows_and_delivery_audit(self) -> None:
        signals = [
            {
                "signal_id": "sig-lp-1",
                "archive_ts": 1,
                "lp_alert_stage": "confirm",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDC",
                "trade_action_key": "LONG_BIAS_OBSERVE",
                "sent_to_telegram": True,
                "market_context_source": "live_public",
                "signal_json": {"kind": "clmm_position", "event": "decrease_liquidity"},
            },
            {
                "signal_id": "sig-lp-2",
                "archive_ts": 2,
                "lp_alert_stage": "exhaustion_risk",
                "asset_symbol": "ETH",
                "pair_label": "ETH/USDT",
                "trade_action_key": "DO_NOT_CHASE_LONG",
                "telegram_suppression_reason": "gate/lp_noise_filtered",
                "signal_json": {"kind": "lp_signal", "pool": "fixture"},
            },
        ]
        delivery = [
            {
                "audit_id": "audit-1",
                "signal_id": "sig-lp-1",
                "archive_ts": 3,
                "sent_to_telegram": 1,
                "delivery_decision": "sent",
                "audit_json": {"kind": "lp"},
            },
            {
                "audit_id": "audit-2",
                "signal_id": "sig-lp-2",
                "archive_ts": 4,
                "sent_to_telegram": 0,
                "suppression_reason": "gate/lp_noise_filtered",
                "audit_json": {"kind": "lp"},
            },
        ]
        sqlite_counts = {
            "available": True,
            "signals": 12,
            "raw_events": 20,
            "parsed_events": 18,
            "delivery_audit": 2,
            "delivery_summary": {
                "available": True,
                "total": 2,
                "delivered": 1,
                "suppressed": 1,
                "suppression_rate": 0.5,
                "by_reason": {"gate/lp_noise_filtered": 1},
            },
            "warnings": [],
        }

        with mock.patch.object(report, "_sqlite_lp_like_counts", return_value=sqlite_counts), mock.patch.object(
            report,
            "_read_trade_replay_summary",
            return_value={"trade_replay_available": False, "replay_count": 0, "shadow_funnel_summary": {}},
        ), mock.patch.object(report, "_read_trade_replay_rows", return_value=[]), mock.patch.object(
            report,
            "_runtime_health_summary",
            return_value={
                "active_hours": 1.0,
                "raw_events_count": 2,
                "parsed_events_count": 2,
                "signals_count": 2,
                "max_raw_event_gap_sec": 0,
                "max_signal_gap_sec": 0,
                "zero_activity_day": 0,
                "data_quality_status": "valid",
                "data_gap_warnings": [],
            },
        ):
            payload = _summary_for(signals=signals, delivery_audit=delivery)

        self.assertIn("lp_signal_summary", payload)
        self.assertIn("lp_stage_summary", payload)
        self.assertIn("clmm_summary", payload)
        self.assertIn("lp_suppression_summary", payload)
        self.assertTrue(payload["lp_signal_summary"]["available"])
        self.assertEqual(2, payload["lp_signal_summary"]["lp_signal_rows"])
        self.assertEqual(12, payload["lp_signal_summary"]["lp_like_signals_sqlite"])
        self.assertEqual({"confirm": 1, "exhaustion_risk": 1, "prealert": 0, "unknown": 0}, payload["lp_stage_summary"]["by_stage"])
        self.assertEqual(1, payload["lp_suppression_summary"]["suppressed"])
        self.assertGreaterEqual(payload["clmm_summary"]["clmm_like_rows"], 1)

    def test_schema_check_reports_mapping_missing_when_sqlite_has_lp_but_report_field_is_absent(self) -> None:
        reason = schema_check.lp_missing_reason(
            {"run_overview": {"lp_signal_rows": 2}},
            {"signals": 3, "raw_events": 0, "parsed_events": 0, "delivery_audit": 0},
            ["lp_signal_summary"],
        )

        self.assertEqual("report_mapping_missing", reason)


if __name__ == "__main__":
    unittest.main()
