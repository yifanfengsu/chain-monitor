import unittest

from reports.legacy.generate_overnight_run_analysis_latest import (
    compute_asset_market_states,
    compute_candidate_tradeable_summary,
    compute_no_trade_lock_summary,
    compute_outcome_price_sources,
    compute_telegram_suppression,
)


class TradeStateReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = [
            {
                "archive_ts": 1_710_000_000,
                "signal_id": "sig1",
                "asset_symbol": "ETH",
                "asset_market_state_key": "WAIT_CONFIRMATION",
                "asset_market_state_label": "等待确认",
                "asset_market_state_changed": True,
                "previous_asset_market_state_key": "",
                "telegram_update_kind": "state_change",
                "telegram_should_send": True,
                "sent_to_telegram": True,
                "outcome_price_source": "okx_mark",
                "outcome_windows": {"60s": {"status": "completed", "price_source": "okx_mark"}},
            },
            {
                "archive_ts": 1_710_000_060,
                "signal_id": "sig2",
                "asset_symbol": "ETH",
                "asset_market_state_key": "LONG_CANDIDATE",
                "asset_market_state_label": "偏多候选",
                "asset_market_state_changed": True,
                "previous_asset_market_state_key": "WAIT_CONFIRMATION",
                "telegram_update_kind": "candidate",
                "telegram_should_send": True,
                "sent_to_telegram": True,
                "direction_adjusted_move_after_60s": 0.005,
                "adverse_by_direction_60s": False,
                "outcome_price_source": "okx_mark",
                "outcome_windows": {"60s": {"status": "completed", "price_source": "okx_mark", "direction_adjusted_move_after": 0.005, "adverse_by_direction": False}},
            },
            {
                "archive_ts": 1_710_000_120,
                "signal_id": "sig3",
                "asset_symbol": "ETH",
                "asset_market_state_key": "NO_TRADE_LOCK",
                "asset_market_state_label": "不交易锁定",
                "asset_market_state_changed": True,
                "previous_asset_market_state_key": "LONG_CANDIDATE",
                "telegram_update_kind": "risk_blocker",
                "telegram_should_send": True,
                "sent_to_telegram": True,
                "no_trade_lock_started_at": 1_710_000_120,
                "no_trade_lock_until": 1_710_000_420,
                "telegram_suppression_reason": "",
                "outcome_price_source": "okx_mark",
                "outcome_windows": {"60s": {"status": "unavailable", "price_source": "unavailable", "failure_reason": "live_market_price_unavailable"}},
            },
            {
                "archive_ts": 1_710_000_150,
                "signal_id": "sig4",
                "asset_symbol": "ETH",
                "asset_market_state_key": "NO_TRADE_LOCK",
                "asset_market_state_label": "不交易锁定",
                "asset_market_state_changed": False,
                "previous_asset_market_state_key": "NO_TRADE_LOCK",
                "telegram_update_kind": "suppressed",
                "telegram_should_send": False,
                "telegram_suppression_reason": "no_trade_lock_local_signal_suppressed",
                "sent_to_telegram": False,
                "no_trade_lock_released_by": "",
                "outcome_price_source": "unavailable",
                "outcome_windows": {"60s": {"status": "expired", "price_source": "unavailable", "failure_reason": "window_elapsed_without_price_update"}},
            },
            {
                "archive_ts": 1_710_000_500,
                "signal_id": "sig5",
                "asset_symbol": "ETH",
                "asset_market_state_key": "TRADEABLE_LONG",
                "asset_market_state_label": "可顺势追多",
                "asset_market_state_changed": True,
                "previous_asset_market_state_key": "NO_TRADE_LOCK",
                "telegram_update_kind": "candidate",
                "telegram_should_send": True,
                "sent_to_telegram": True,
                "direction_adjusted_move_after_60s": 0.006,
                "adverse_by_direction_60s": False,
                "no_trade_lock_released_by": "broader_clean_dominance",
                "outcome_price_source": "okx_index",
                "outcome_windows": {"60s": {"status": "completed", "price_source": "okx_index", "direction_adjusted_move_after": 0.006, "adverse_by_direction": False}},
            },
        ]

    def test_state_distribution_and_transitions_are_reported(self) -> None:
        payload = compute_asset_market_states(self.rows)
        self.assertIn("LONG_CANDIDATE", payload["state_distribution"])
        self.assertIn("WAIT_CONFIRMATION->LONG_CANDIDATE", payload["state_transitions"])

    def test_no_trade_lock_stats_are_reported(self) -> None:
        payload = compute_no_trade_lock_summary(self.rows)
        self.assertEqual(1, payload["lock_entered_count"])
        self.assertEqual(1, payload["release_count"])

    def test_candidate_and_tradeable_stats_are_reported(self) -> None:
        payload = compute_candidate_tradeable_summary(self.rows)
        self.assertEqual(1, payload["candidate_count"])
        self.assertEqual(1, payload["tradeable_count"])
        self.assertGreater(payload["candidate_outcome_60s"]["followthrough_rate"], 0.0)

    def test_outcome_price_source_distribution_is_reported(self) -> None:
        payload = compute_outcome_price_sources(self.rows)
        self.assertIn("okx_mark", payload["source_distribution"])
        self.assertIn("okx_index", payload["source_distribution"])

    def test_suppression_stats_are_reported(self) -> None:
        payload = compute_telegram_suppression(self.rows)
        self.assertEqual(1, payload["total_suppressed"])
        self.assertIn("no_trade_lock_local_signal_suppressed", payload["suppression_reasons"])


if __name__ == "__main__":
    unittest.main()
