#!/usr/bin/env python3
"""Tests for trade_replay module."""

import unittest
from trade_replay import (
    classify_replay_label,
    compute_replay_pnl,
    replay_single_signal,
    _build_profile_key,
    _determine_side,
    _stable_replay_id,
)


class TestComputeReplayPnl(unittest.TestCase):
    def test_long_profitable(self):
        pnl = compute_replay_pnl(2000.0, 2010.0, "LONG", fee_bps=6, slippage_bps=5)
        # gross: (2010-2000)/2000*10000 = 50 bps, net: 50-11=39
        self.assertAlmostEqual(pnl["gross_pnl_bps"], 50.0, places=1)
        self.assertAlmostEqual(pnl["net_pnl_bps"], 39.0, places=1)

    def test_short_profitable(self):
        pnl = compute_replay_pnl(2000.0, 1990.0, "SHORT", fee_bps=6, slippage_bps=5)
        # gross: (2000-1990)/2000*10000 = 50 bps, net: 39
        self.assertAlmostEqual(pnl["gross_pnl_bps"], 50.0, places=1)
        self.assertAlmostEqual(pnl["net_pnl_bps"], 39.0, places=1)

    def test_long_loss(self):
        pnl = compute_replay_pnl(2000.0, 1995.0, "LONG", fee_bps=6, slippage_bps=5)
        # gross: -25, net: -36
        self.assertAlmostEqual(pnl["gross_pnl_bps"], -25.0, places=1)
        self.assertAlmostEqual(pnl["net_pnl_bps"], -36.0, places=1)

    def test_invalid_prices(self):
        pnl = compute_replay_pnl(0, 2000, "LONG")
        self.assertEqual(pnl["net_pnl_bps"], 0.0)

    def test_neutral_side(self):
        pnl = compute_replay_pnl(2000.0, 2010.0, "NONE")
        self.assertEqual(pnl["gross_pnl_bps"], 0.0)


class TestClassifyReplayLabel(unittest.TestCase):
    def test_clean_followthrough(self):
        label = classify_replay_label(net_pnl_bps=10, mfe_bps=15, mae_bps=-3)
        self.assertEqual(label, "clean_followthrough")

    def test_bad_entry(self):
        label = classify_replay_label(net_pnl_bps=5, mfe_bps=25, mae_bps=-22)
        self.assertEqual(label, "followthrough_but_bad_entry")

    def test_absorption_reversal(self):
        label = classify_replay_label(net_pnl_bps=-30, mfe_bps=2, mae_bps=-35)
        self.assertEqual(label, "absorption_reversal")

    def test_chop_no_edge(self):
        label = classify_replay_label(net_pnl_bps=2, mfe_bps=3, mae_bps=-2)
        self.assertEqual(label, "chop_no_edge")

    def test_marginal_positive(self):
        label = classify_replay_label(net_pnl_bps=8, mfe_bps=10, mae_bps=-5)
        self.assertIn(label, ["clean_followthrough", "followthrough_but_bad_entry"])


class TestReplaySingleSignal(unittest.TestCase):
    def test_data_invalid_no_price(self):
        signal = {
            "signal_id": "test_1",
            "asset": "ETH",
            "pair": "ETH/USDC",
            "side": "LONG",
            "signal_ts": 1777478400,
            "logical_date": "2026-04-29",
        }
        rules = {"entry_delay_sec": 5, "max_hold_sec": 60, "stop_loss_bps": 30, "take_profit_bps": 50, "fee_bps": 6, "slippage_bps": 5}

        # With no price data in DB, should return data_invalid
        result = replay_single_signal(signal, rules)
        self.assertEqual(result["label"], "data_invalid")
        self.assertFalse(result["data_valid"])
        self.assertEqual(result["invalid_reason"], "no_entry_price")

    def test_stop_loss_scenario(self):
        """Test stop loss using mock price source."""
        signal = {
            "signal_id": "test_sl",
            "asset": "ETH",
            "pair": "ETH/USDC",
            "side": "LONG",
            "signal_ts": 1777478400,
            "logical_date": "2026-04-29",
        }
        rules = {"entry_delay_sec": 5, "max_hold_sec": 60, "stop_loss_bps": 30, "take_profit_bps": 50, "fee_bps": 6, "slippage_bps": 5}

        # Mock: entry at 2000, price drops to 1935 (65 bps drop = stop loss triggered)
        call_count = [0]

        def mock_price(asset, pair, ts, prefer_source=""):
            call_count[0] += 1
            if call_count[0] == 1:
                return 2000.0  # entry price
            return 1935.0  # dropped below stop loss

        result = replay_single_signal(signal, rules, price_source_fn=mock_price)
        self.assertEqual(result["close_reason"], "stop_loss")
        self.assertTrue(result["net_pnl_bps"] < -30)
        self.assertEqual(result["label"], "absorption_reversal")

    def test_take_profit_scenario(self):
        """Test take profit using mock price source."""
        signal = {
            "signal_id": "test_tp",
            "asset": "ETH",
            "pair": "ETH/USDC",
            "side": "LONG",
            "signal_ts": 1777478400,
            "logical_date": "2026-04-29",
        }
        rules = {"entry_delay_sec": 5, "max_hold_sec": 60, "stop_loss_bps": 30, "take_profit_bps": 50, "fee_bps": 6, "slippage_bps": 5}

        call_count = [0]

        def mock_price(asset, pair, ts, prefer_source=""):
            call_count[0] += 1
            if call_count[0] == 1:
                return 2000.0
            return 2012.0  # up 60 bps = take profit

        result = replay_single_signal(signal, rules, price_source_fn=mock_price)
        self.assertEqual(result["close_reason"], "take_profit")
        self.assertTrue(result["net_pnl_bps"] > 35)
        self.assertEqual(result["label"], "clean_followthrough")

    def test_max_hold(self):
        """Test max hold expiration."""
        signal = {
            "signal_id": "test_mh",
            "asset": "ETH",
            "pair": "ETH/USDC",
            "side": "LONG",
            "signal_ts": 1777478400,
            "logical_date": "2026-04-29",
        }
        rules = {"entry_delay_sec": 5, "max_hold_sec": 30, "stop_loss_bps": 50, "take_profit_bps": 100, "fee_bps": 6, "slippage_bps": 5}

        def mock_price(asset, pair, ts, prefer_source=""):
            return 2000.5  # barely moves

        result = replay_single_signal(signal, rules, price_source_fn=mock_price)
        self.assertEqual(result["close_reason"], "max_hold")
        self.assertTrue(abs(result["net_pnl_bps"]) < 15)


class TestProfileKey(unittest.TestCase):
    def test_build_profile_key(self):
        signal = {
            "asset_symbol": "ETH",
            "direction": "LONG",
            "lp_alert_stage": "confirm",
            "lp_sweep_phase": "sweep_confirmed",
            "lp_confirm_scope": "broader_confirm",
            "lp_absorption_context": "broader_buy_pressure_confirmed",
            "market_context_source": "live_public",
            "basis_bps": 5,
        }
        key = _build_profile_key(signal, "NONE", "SHADOW_CANDIDATE")
        self.assertIn("ETH|LONG|confirm|sweep_confirmed|broader_true", key)
        self.assertIn("SHADOW_CANDIDATE", key)

    def test_determine_side_from_direction(self):
        self.assertEqual(_determine_side({"direction": "LONG"}), "LONG")
        self.assertEqual(_determine_side({"direction": "SHORT"}), "SHORT")
        self.assertEqual(_determine_side({"intent_type": "pool_buy_pressure"}), "LONG")
        self.assertEqual(_determine_side({"intent_type": "pool_sell_pressure"}), "SHORT")

    def test_stable_replay_id(self):
        id1 = _stable_replay_id("sig_1", "2026-04-29", 1777478400, {"entry_delay_sec": 5})
        id2 = _stable_replay_id("sig_1", "2026-04-29", 1777478400, {"entry_delay_sec": 5})
        self.assertEqual(id1, id2)
        id3 = _stable_replay_id("sig_1", "2026-04-29", 1777478400, {"entry_delay_sec": 10})
        self.assertNotEqual(id1, id3)


if __name__ == "__main__":
    unittest.main()
