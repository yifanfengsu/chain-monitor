import unittest

from reports.legacy.generate_overnight_run_analysis_latest import compute_trade_actions


class TradeActionReportTests(unittest.TestCase):
    def test_trade_action_distribution_and_counts_are_reported(self) -> None:
        rows = [
            {
                "trade_action_key": "LONG_CHASE_ALLOWED",
                "direction_adjusted_move_after_300s": 0.012,
                "adverse_by_direction_300s": False,
                "direction_adjusted_move_after_60s": 0.006,
                "adverse_by_direction_60s": False,
                "direction_adjusted_move_after_30s": 0.003,
                "adverse_by_direction_30s": False,
                "lp_alert_stage": "confirm",
            },
            {
                "trade_action_key": "DO_NOT_CHASE_LONG",
                "direction_adjusted_move_after_300s": -0.010,
                "adverse_by_direction_300s": True,
                "direction_adjusted_move_after_60s": -0.004,
                "adverse_by_direction_60s": True,
                "direction_adjusted_move_after_30s": -0.002,
                "adverse_by_direction_30s": True,
                "lp_alert_stage": "exhaustion_risk",
            },
            {
                "trade_action_key": "CONFLICT_NO_TRADE",
                "direction_adjusted_move_after_300s": -0.008,
                "adverse_by_direction_300s": True,
                "move_after_alert_300s": -0.008,
                "lp_alert_stage": "confirm",
            },
        ]

        payload = compute_trade_actions(rows)

        self.assertEqual(1, payload["long_chase_allowed_count"])
        self.assertEqual(1, payload["do_not_chase_long_count"])
        self.assertEqual(1, payload["conflict_no_trade_count"])
        self.assertIn("LONG_CHASE_ALLOWED", payload["trade_action_distribution"])

    def test_chase_allowed_post_hoc_metrics_are_available(self) -> None:
        rows = [
            {
                "trade_action_key": "LONG_CHASE_ALLOWED",
                "direction_adjusted_move_after_300s": 0.011,
                "adverse_by_direction_300s": False,
                "direction_adjusted_move_after_60s": 0.005,
                "adverse_by_direction_60s": False,
                "direction_adjusted_move_after_30s": 0.002,
                "adverse_by_direction_30s": False,
                "lp_alert_stage": "confirm",
            },
            {
                "trade_action_key": "SHORT_CHASE_ALLOWED",
                "direction_adjusted_move_after_300s": 0.009,
                "adverse_by_direction_300s": False,
                "direction_adjusted_move_after_60s": 0.004,
                "adverse_by_direction_60s": False,
                "direction_adjusted_move_after_30s": 0.001,
                "adverse_by_direction_30s": False,
                "lp_alert_stage": "confirm",
            },
            {
                "trade_action_key": "WAIT_CONFIRMATION",
                "direction_adjusted_move_after_300s": -0.006,
                "adverse_by_direction_300s": True,
                "direction_adjusted_move_after_60s": -0.002,
                "adverse_by_direction_60s": True,
                "direction_adjusted_move_after_30s": -0.001,
                "adverse_by_direction_30s": True,
                "lp_alert_stage": "confirm",
            },
        ]

        payload = compute_trade_actions(rows)

        self.assertGreaterEqual(payload["chase_allowed_success_rate"], 0.5)
        self.assertIn("LONG_CHASE_ALLOWED", payload["trade_action_followthrough_300s"])
        self.assertIn("SHORT_CHASE_ALLOWED", payload["trade_action_adverse_300s"])

    def test_no_data_does_not_crash(self) -> None:
        payload = compute_trade_actions([])

        self.assertEqual({}, payload["trade_action_distribution"])
        self.assertEqual(0, payload["long_chase_allowed_count"])
        self.assertEqual(0.0, payload["chase_allowed_success_rate"])


if __name__ == "__main__":
    unittest.main()
