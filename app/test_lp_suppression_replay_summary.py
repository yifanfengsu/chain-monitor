from __future__ import annotations

import unittest

import reports.generate_daily_report_latest as report


class LpSuppressionReplaySummaryTests(unittest.TestCase):
    def test_reason_level_replay_actions_are_computed(self) -> None:
        signals = [
            {"signal_id": "sig-noise-1", "lp_alert_stage": "confirm", "signal_json": {"kind": "lp"}},
            {"signal_id": "sig-noise-2", "lp_alert_stage": "confirm", "signal_json": {"kind": "lp"}},
            {"signal_id": "sig-noise-3", "lp_alert_stage": "confirm", "signal_json": {"kind": "lp"}},
            {"signal_id": "sig-low", "lp_alert_stage": "confirm", "signal_json": {"kind": "lp"}},
        ]
        delivery = [
            {"audit_id": "audit-noise-1", "signal_id": "sig-noise-1", "sent_to_telegram": 0, "suppression_reason": "gate/lp_noise_filtered"},
            {"audit_id": "audit-noise-2", "signal_id": "sig-noise-2", "sent_to_telegram": 0, "suppression_reason": "gate/lp_noise_filtered"},
            {"audit_id": "audit-noise-3", "signal_id": "sig-noise-3", "sent_to_telegram": 0, "suppression_reason": "gate/lp_noise_filtered"},
            {"audit_id": "audit-low", "signal_id": "sig-low", "sent_to_telegram": 0, "suppression_reason": "delivery_policy"},
        ]
        replay_rows = [
            {"delivery_audit_id": "audit-noise-1", "signal_stage": "SUPPRESSED", "data_valid": 1, "net_pnl_bps": -9.0},
            {"delivery_audit_id": "audit-noise-2", "signal_stage": "SUPPRESSED", "data_valid": 1, "net_pnl_bps": -12.0},
            {"delivery_audit_id": "audit-noise-3", "signal_stage": "SUPPRESSED", "data_valid": 1, "net_pnl_bps": -15.0},
            {"delivery_audit_id": "audit-low", "signal_stage": "SUPPRESSED", "data_valid": 1, "net_pnl_bps": 20.0},
        ]
        suppression = {
            "available": True,
            "total": 4,
            "delivered": 0,
            "suppressed": 4,
            "suppression_rate": 1.0,
            "by_reason": {"gate/lp_noise_filtered": 3, "delivery_policy": 1},
        }

        summary = report._lp_suppression_replay_summary(
            suppression,
            delivery,
            replay_rows,
            signals,
            {"suppressed_avg_net_pnl_bps": -4.0},
        )
        by_reason = {item["reason"]: item for item in summary["by_reason"]}

        self.assertEqual(3, by_reason["gate/lp_noise_filtered"]["replay_count"])
        self.assertEqual(-12.0, by_reason["gate/lp_noise_filtered"]["avg_net_pnl_bps"])
        self.assertEqual("keep_suppressed", by_reason["gate/lp_noise_filtered"]["recommended_action"])
        self.assertEqual(1, by_reason["delivery_policy"]["replay_count"])
        self.assertEqual("needs_more_samples", by_reason["delivery_policy"]["recommended_action"])
        self.assertEqual("suppression_seems_correct", summary["diagnosis"])
        self.assertEqual(-4.0, summary["overall_suppressed_avg_net_pnl_bps"])


if __name__ == "__main__":
    unittest.main()
