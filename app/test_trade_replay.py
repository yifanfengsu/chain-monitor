"""Test trade replay engine."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from trade_replay import (
    replay_signal,
    aggregate_profile_stats,
    _classify_replay_label,
    _calc_gross_pnl_bps,
    _infer_profile_action,
)


class TestTradeReplay(unittest.TestCase):
    """Test trade replay functionality."""

    def test_replay_signal_with_valid_data(self):
        """Test replay with valid signal data."""
        signal_row = {
            "signal_id": "test_signal_001",
            "asset_symbol": "ETH",
            "pair_label": "ETH/USDT",
            "trade_opportunity_side": "LONG",
            "direction": "LONG",
            "archive_ts": 1700000000,
            "outcome_price_start": 2000.0,
            "outcome_60s_price_end": 2010.0,
            "opportunity_profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
            "trade_opportunity_status": "CANDIDATE",
        }
        
        result = replay_signal(signal_row, max_hold_sec=60)
        
        self.assertTrue(result["data_valid"])
        self.assertEqual(result["signal_id"], "test_signal_001")
        self.assertEqual(result["direction"], "LONG")
        self.assertIsNotNone(result["entry_price"])
        self.assertIsNotNone(result["exit_price"])
        self.assertIsNotNone(result["net_pnl_bps"])
        self.assertIn(result["label"], {
            "clean_followthrough", "small_win", "small_loss", 
            "neutral", "chop_no_edge", "bad_entry", "absorption_reversal"
        })

    def test_replay_signal_missing_signal_id(self):
        """Test replay with missing signal_id."""
        signal_row = {
            "asset_symbol": "ETH",
            "direction": "LONG",
            "archive_ts": 1700000000,
        }
        
        result = replay_signal(signal_row)
        
        self.assertFalse(result["data_valid"])
        self.assertEqual(result["label"], "data_invalid")

    def test_replay_signal_ambiguous_direction(self):
        """Test replay with ambiguous direction."""
        signal_row = {
            "signal_id": "test_signal_002",
            "asset_symbol": "ETH",
            "direction": "NONE",
            "archive_ts": 1700000000,
        }
        
        result = replay_signal(signal_row)
        
        self.assertFalse(result["data_valid"])
        self.assertEqual(result["close_reason"], "direction_ambiguous")

    def test_calc_gross_pnl_bps_long(self):
        """Test PnL calculation for LONG position."""
        # 1% gain
        pnl = _calc_gross_pnl_bps(2000.0, 2020.0, "LONG")
        self.assertAlmostEqual(pnl, 100.0, places=1)
        
        # 1% loss
        pnl = _calc_gross_pnl_bps(2000.0, 1980.0, "LONG")
        self.assertAlmostEqual(pnl, -100.0, places=1)

    def test_calc_gross_pnl_bps_short(self):
        """Test PnL calculation for SHORT position."""
        # 1% gain (price drops)
        pnl = _calc_gross_pnl_bps(2000.0, 1980.0, "SHORT")
        self.assertAlmostEqual(pnl, 101.0, places=0)
        
        # 1% loss (price rises)
        pnl = _calc_gross_pnl_bps(2000.0, 2020.0, "SHORT")
        self.assertAlmostEqual(pnl, -99.0, places=0)

    def test_classify_replay_label_clean_followthrough(self):
        """Test clean followthrough classification."""
        label = _classify_replay_label(
            net_pnl_bps=30.0,
            mfe_bps=35.0,
            mae_bps=-5.0,
            close_reason="take_profit",
            direction="LONG",
        )
        self.assertEqual(label, "clean_followthrough")

    def test_classify_replay_label_bad_entry(self):
        """Test bad entry classification."""
        label = _classify_replay_label(
            net_pnl_bps=-25.0,
            mfe_bps=5.0,
            mae_bps=-30.0,
            close_reason="stop_loss",
            direction="LONG",
        )
        self.assertEqual(label, "bad_entry")

    def test_classify_replay_label_absorption_reversal(self):
        """Test absorption reversal classification."""
        label = _classify_replay_label(
            net_pnl_bps=-15.0,
            mfe_bps=25.0,
            mae_bps=-20.0,
            close_reason="max_hold",
            direction="LONG",
        )
        self.assertEqual(label, "absorption_reversal")

    def test_classify_replay_label_chop(self):
        """Test chop classification."""
        label = _classify_replay_label(
            net_pnl_bps=2.0,
            mfe_bps=8.0,
            mae_bps=-6.0,
            close_reason="max_hold",
            direction="LONG",
        )
        self.assertEqual(label, "chop_no_edge")

    def test_aggregate_profile_stats_empty(self):
        """Test profile stats aggregation with empty list."""
        stats = aggregate_profile_stats([])
        self.assertEqual(len(stats), 0)

    def test_aggregate_profile_stats_single_profile(self):
        """Test profile stats aggregation with single profile."""
        replays = [
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
                "data_valid": True,
                "net_pnl_bps": 30.0,
                "gross_pnl_bps": 42.0,
                "label": "clean_followthrough",
                "mfe_bps": 35.0,
                "mae_bps": -5.0,
            },
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
                "data_valid": True,
                "net_pnl_bps": 20.0,
                "gross_pnl_bps": 32.0,
                "label": "small_win",
                "mfe_bps": 25.0,
                "mae_bps": -8.0,
            },
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
                "data_valid": True,
                "net_pnl_bps": -10.0,
                "gross_pnl_bps": 2.0,
                "label": "small_loss",
                "mfe_bps": 10.0,
                "mae_bps": -15.0,
            },
        ]
        
        stats = aggregate_profile_stats(replays)
        
        self.assertEqual(len(stats), 1)
        profile_key = "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high"
        self.assertIn(profile_key, stats)
        
        profile_stats = stats[profile_key]
        self.assertEqual(profile_stats["sample_count"], 3)
        self.assertEqual(profile_stats["valid_count"], 3)
        self.assertEqual(profile_stats["win_count"], 2)
        self.assertEqual(profile_stats["loss_count"], 1)
        self.assertAlmostEqual(profile_stats["win_rate"], 0.6667, places=3)
        self.assertAlmostEqual(profile_stats["avg_net_pnl_bps"], 13.33, places=1)

    def test_infer_profile_action_needs_more_samples(self):
        """Test profile action inference with insufficient samples."""
        stats = {
            "valid_count": 5,
            "win_rate": 0.80,
            "avg_net_pnl_bps": 30.0,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "needs_more_samples")

    def test_infer_profile_action_hard_block(self):
        """Test profile action inference for hard block."""
        stats = {
            "valid_count": 20,
            "win_rate": 0.30,
            "avg_net_pnl_bps": -15.0,
            "absorption_rate": 0.60,
            "bad_entry_rate": 0.20,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "hard_block")

    def test_infer_profile_action_eligible_verified(self):
        """Test profile action inference for eligible verified."""
        stats = {
            "valid_count": 30,
            "win_rate": 0.75,
            "avg_net_pnl_bps": 28.0,
            "absorption_rate": 0.10,
            "bad_entry_rate": 0.05,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "eligible_verified")

    def test_infer_profile_action_promote_candidate(self):
        """Test profile action inference for promote candidate."""
        stats = {
            "valid_count": 25,
            "win_rate": 0.64,
            "avg_net_pnl_bps": 18.0,
            "absorption_rate": 0.15,
            "bad_entry_rate": 0.10,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "promote_candidate")

    def test_aggregate_profile_stats_with_invalid_data(self):
        """Test profile stats aggregation with some invalid data."""
        replays = [
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
                "data_valid": True,
                "net_pnl_bps": 25.0,
                "gross_pnl_bps": 37.0,
                "label": "clean_followthrough",
            },
            {
                "profile_key": "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high",
                "data_valid": False,
                "label": "data_invalid",
            },
        ]
        
        stats = aggregate_profile_stats(replays)
        
        profile_key = "ETH|LONG|broader_confirm|confirm|leading|no_absorption|major|clean|high"
        profile_stats = stats[profile_key]
        
        self.assertEqual(profile_stats["sample_count"], 2)
        self.assertEqual(profile_stats["valid_count"], 1)
        self.assertEqual(profile_stats["data_invalid_count"], 1)


if __name__ == "__main__":
    unittest.main()
