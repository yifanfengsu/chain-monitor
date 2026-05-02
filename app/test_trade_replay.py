"""Test trade replay engine."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from unittest.mock import MagicMock

from trade_replay import (
    ReplayDiagnostics,
    _calc_gross_pnl_bps,
    _classify_replay_label,
    _infer_profile_action,
    _update_profile_stats,
    aggregate_profile_stats,
    get_price_at_ts,
    replay_single_signal,
    replay_signal,
    run_trade_replay,
)


class TestTradeReplay(unittest.TestCase):
    """Test trade replay functionality."""

    def test_replay_signal_with_valid_data(self):
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
        self.assertIn(
            result["label"],
            {
                "clean_followthrough",
                "small_win",
                "small_loss",
                "neutral",
                "chop_no_edge",
                "bad_entry",
                "absorption_reversal",
            },
        )

    def test_replay_signal_missing_signal_id(self):
        signal_row = {
            "asset_symbol": "ETH",
            "direction": "LONG",
            "archive_ts": 1700000000,
        }

        result = replay_signal(signal_row)

        self.assertFalse(result["data_valid"])
        self.assertEqual(result["label"], "data_invalid")

    def test_replay_signal_ambiguous_direction(self):
        signal_row = {
            "signal_id": "test_signal_002",
            "asset_symbol": "ETH",
            "direction": "NONE",
            "archive_ts": 1700000000,
        }

        result = replay_signal(signal_row)

        self.assertFalse(result["data_valid"])
        self.assertEqual(result["close_reason"], "direction_ambiguous")

    def test_replay_signal_market_context_exception_is_visible(self):
        adapter = MagicMock()
        adapter.get_market_context.side_effect = RuntimeError("adapter boom")
        signal_row = {
            "signal_id": "test_signal_003",
            "asset_symbol": "ETH",
            "pair_label": "ETH/USDT",
            "direction": "LONG",
            "archive_ts": 1700000000,
        }
        diagnostics = ReplayDiagnostics()

        result = replay_signal(signal_row, market_context_adapter=adapter, replay_diagnostics=diagnostics)

        self.assertFalse(result["data_valid"])
        self.assertEqual("market_context_unavailable", result["invalid_reason"])
        self.assertTrue(any("market_context_lookup_failed:entry" in item for item in diagnostics.price_errors))

    def test_calc_gross_pnl_bps_long(self):
        pnl = _calc_gross_pnl_bps(2000.0, 2020.0, "LONG")
        self.assertAlmostEqual(pnl, 100.0, places=1)
        pnl = _calc_gross_pnl_bps(2000.0, 1980.0, "LONG")
        self.assertAlmostEqual(pnl, -100.0, places=1)

    def test_calc_gross_pnl_bps_short(self):
        pnl = _calc_gross_pnl_bps(2000.0, 1980.0, "SHORT")
        self.assertAlmostEqual(pnl, 101.0, places=0)
        pnl = _calc_gross_pnl_bps(2000.0, 2020.0, "SHORT")
        self.assertAlmostEqual(pnl, -99.0, places=0)

    def test_classify_replay_label_clean_followthrough(self):
        label = _classify_replay_label(
            net_pnl_bps=30.0,
            mfe_bps=35.0,
            mae_bps=-5.0,
            close_reason="take_profit",
            direction="LONG",
        )
        self.assertEqual(label, "clean_followthrough")

    def test_classify_replay_label_bad_entry(self):
        label = _classify_replay_label(
            net_pnl_bps=-25.0,
            mfe_bps=5.0,
            mae_bps=-30.0,
            close_reason="stop_loss",
            direction="LONG",
        )
        self.assertEqual(label, "bad_entry")

    def test_classify_replay_label_absorption_reversal(self):
        label = _classify_replay_label(
            net_pnl_bps=-15.0,
            mfe_bps=25.0,
            mae_bps=-20.0,
            close_reason="max_hold",
            direction="LONG",
        )
        self.assertEqual(label, "absorption_reversal")

    def test_classify_replay_label_chop(self):
        label = _classify_replay_label(
            net_pnl_bps=2.0,
            mfe_bps=8.0,
            mae_bps=-6.0,
            close_reason="max_hold",
            direction="LONG",
        )
        self.assertEqual(label, "chop_no_edge")

    def test_aggregate_profile_stats_empty(self):
        stats = aggregate_profile_stats([])
        self.assertEqual(len(stats), 0)

    def test_aggregate_profile_stats_single_profile(self):
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
        stats = {
            "valid_count": 5,
            "win_rate": 0.80,
            "avg_net_pnl_bps": 30.0,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "needs_more_samples")

    def test_infer_profile_action_negative_block_profile(self):
        stats = {
            "valid_count": 20,
            "win_rate": 0.30,
            "avg_net_pnl_bps": -15.0,
            "absorption_rate": 0.60,
            "bad_entry_rate": 0.20,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.85,
        }
        action = _infer_profile_action(stats)
        self.assertEqual(action, "block_profile")

    def test_infer_profile_action_eligible_verified(self):
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

    def test_update_profile_stats_builds_stat_before_recommendation(self):
        stats = {}
        results = [
            {
                "profile_key": "ETH|LONG|profile",
                "data_valid": True,
                "net_pnl_bps": 30.0,
                "gross_pnl_bps": 42.0,
                "label": "clean_followthrough",
            },
            {
                "profile_key": "ETH|LONG|profile",
                "data_valid": True,
                "net_pnl_bps": -10.0,
                "gross_pnl_bps": 2.0,
                "label": "bad_entry",
            },
        ]

        updated = _update_profile_stats(stats, results)

        profile_stats = updated["ETH|LONG|profile"]
        self.assertEqual(2, profile_stats["valid_sample_count"])
        self.assertEqual(0.5, profile_stats["win_rate"])
        self.assertEqual(10.0, profile_stats["avg_net_pnl_bps"])
        self.assertTrue(profile_stats["recommended_action"])

    def test_replay_single_signal_passes_signal_and_opportunity_to_price_source(self):
        calls = []

        def price_source_fn(**kwargs):
            calls.append(kwargs)
            return {"price": 2000.0 if len(calls) == 1 else 2010.0, "source": "test", "reason": None}

        result = replay_single_signal(
            {
                "signal_id": "sig-eth-1",
                "trade_opportunity_id": "opp-eth-1",
                "asset": "ETH",
                "pair": "ETH/USDT",
                "direction": "LONG",
                "archive_ts": 1000,
            },
            price_source_fn=price_source_fn,
        )

        self.assertTrue(result["data_valid"])
        self.assertEqual("sig-eth-1", calls[0]["signal_id"])
        self.assertEqual("opp-eth-1", calls[0]["trade_opportunity_id"])

    def test_replay_single_signal_legacy_price_source_still_works(self):
        def price_source_fn(asset, pair, ts, prefer_source=""):
            return 2000.0 if ts == 1005 else 2010.0

        result = replay_single_signal(
            {
                "signal_id": "sig-eth-legacy",
                "trade_opportunity_id": "opp-eth-legacy",
                "asset": "ETH",
                "pair": "ETH/USDT",
                "direction": "LONG",
                "archive_ts": 1000,
            },
            price_source_fn=price_source_fn,
        )

        self.assertTrue(result["data_valid"])

    def test_replay_single_signal_records_no_price_invalid_reason(self):
        def price_source_fn(**kwargs):
            return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}

        result = replay_single_signal(
            {
                "signal_id": "sig-no-price",
                "trade_opportunity_id": "opp-no-price",
                "asset": "ETH",
                "pair": "ETH/USDT",
                "direction": "LONG",
                "archive_ts": 1000,
            },
            price_source_fn=price_source_fn,
        )

        self.assertFalse(result["data_valid"])
        self.assertEqual("no_price_for_signal_id", result["invalid_reason"])


class TestTradeReplaySqlLookups(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "trade_replay.sqlite"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE outcomes (
                outcome_id TEXT PRIMARY KEY,
                signal_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                pair TEXT,
                direction TEXT,
                window_sec INTEGER,
                due_at REAL,
                price_source TEXT,
                start_price_source TEXT,
                end_price_source TEXT,
                outcome_price_source TEXT,
                start_price REAL,
                end_price REAL,
                raw_move REAL,
                direction_adjusted_move REAL,
                followthrough INTEGER,
                adverse INTEGER,
                reversal INTEGER,
                status TEXT,
                failure_reason TEXT,
                completed_at REAL,
                created_at REAL,
                updated_at REAL,
                settled_by TEXT,
                catchup INTEGER
            );
            CREATE TABLE opportunity_outcomes (
                trade_opportunity_id TEXT,
                window_sec INTEGER,
                start_price REAL,
                end_price REAL,
                completed_at REAL,
                created_at REAL,
                PRIMARY KEY(trade_opportunity_id, window_sec)
            );
            CREATE TABLE market_context_snapshots (
                context_id TEXT PRIMARY KEY,
                signal_id TEXT,
                asset TEXT,
                pair TEXT,
                venue TEXT,
                resolved_symbol TEXT,
                source TEXT,
                perp_last_price REAL,
                perp_mark_price REAL,
                perp_index_price REAL,
                spot_reference_price REAL,
                basis_bps REAL,
                mark_index_spread_bps REAL,
                funding_rate REAL,
                alert_relative_timing TEXT,
                market_move_before_30s REAL,
                market_move_before_60s REAL,
                market_move_after_60s REAL,
                market_move_after_300s REAL,
                failure_reason TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE TABLE trade_replay_examples (
                replay_id TEXT PRIMARY KEY,
                logical_date TEXT NOT NULL,
                signal_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT NOT NULL,
                pair TEXT,
                signal_ts INTEGER NOT NULL,
                entry_ts INTEGER,
                exit_ts INTEGER,
                entry_price REAL,
                exit_price REAL,
                net_pnl_bps REAL,
                data_valid INTEGER DEFAULT 1,
                invalid_reason TEXT,
                label TEXT
            );
            CREATE TABLE trade_replay_profile_stats (
                profile_key TEXT PRIMARY KEY,
                valid_sample_count INTEGER DEFAULT 0,
                avg_net_pnl_bps REAL,
                win_rate REAL,
                recommended_action TEXT
            );
            """
        )
        self.conn.execute(
            "INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "out-eth",
                "sig-eth-1",
                "opp-eth-1",
                "ETH",
                "ETH/USDT",
                "LONG",
                60,
                1000,
                "okx_mark",
                "okx_mark",
                "okx_mark",
                "okx_mark",
                2000.0,
                2010.0,
                None,
                None,
                1,
                0,
                0,
                "completed",
                None,
                1005,
                1000,
                1005,
                "scheduler",
                0,
            ),
        )
        self.conn.execute(
            "INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "out-btc",
                "sig-btc-1",
                "opp-btc-1",
                "BTC",
                "BTC/USDT",
                "LONG",
                60,
                1000,
                "okx_mark",
                "okx_mark",
                "okx_mark",
                "okx_mark",
                30000.0,
                30100.0,
                None,
                None,
                1,
                0,
                0,
                "completed",
                None,
                1005,
                1000,
                1005,
                "scheduler",
                0,
            ),
        )
        self.conn.execute(
            "INSERT INTO opportunity_outcomes VALUES (?,?,?,?,?,?)",
            ("opp-eth-1", 60, 1999.0, 2005.0, 1004, 1000),
        )
        self.conn.execute(
            "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ctx-eth-1",
                "sig-ctx-1",
                "ETH",
                "ETH/USDT",
                "okx_perp",
                "ETH-USDT-SWAP",
                "live",
                2001.0,
                2002.0,
                2000.5,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                1001,
                1001,
            ),
        )
        self.conn.execute(
            "INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "replay-1",
                "2026-04-30",
                "sig-eth-1",
                "opp-eth-1",
                "ETH",
                "ETH/USDT",
                1000,
                1005,
                1065,
                2000.0,
                2010.0,
                38.0,
                1,
                None,
                "clean_followthrough",
            ),
        )
        self.conn.execute(
            "INSERT INTO trade_replay_profile_stats VALUES (?,?,?,?,?)",
            ("ETH|LONG|profile", 1, 38.0, 1.0, "needs_more_samples"),
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        self.temp_dir.cleanup()

    def test_market_context_priority_over_outcomes(self):
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            signal_id="sig-eth-1",
            trade_opportunity_id="opp-eth-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )
        self.assertEqual(2002.0, result["price"])
        self.assertEqual("okx_mark", result["source"])
        self.assertEqual([], diagnostics.schema_errors)

    def test_signal_id_exact_match_returns_outcome_price_without_market_context(self):
        self.conn.execute("DELETE FROM market_context_snapshots WHERE asset='BTC'")
        self.conn.commit()
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "BTC",
            "BTC/USDT",
            1002,
            signal_id="sig-btc-1",
            trade_opportunity_id="opp-btc-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )
        self.assertEqual(30000.0, result["price"])
        self.assertEqual("outcomes.signal_id", result["source"])
        self.assertEqual([], diagnostics.schema_errors)

    def test_opportunity_outcomes_exact_match_without_signal_id(self):
        self.conn.execute("DELETE FROM market_context_snapshots WHERE asset='ETH'")
        self.conn.commit()
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            trade_opportunity_id="opp-eth-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )
        self.assertEqual(1999.0, result["price"])
        self.assertEqual("opportunity_outcomes.trade_opportunity_id", result["source"])
        self.assertIn("outcome_fallback_skipped_missing_signal_id", diagnostics.warnings)

    def test_signal_id_exact_match_does_not_cross_asset(self):
        self.conn.execute("DELETE FROM market_context_snapshots WHERE asset='ETH'")
        self.conn.commit()
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            signal_id="sig-btc-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )

        self.assertIsNone(result["price"])
        self.assertEqual("no_price_for_signal_id", result["reason"])
        self.assertTrue(any("outcomes_asset_mismatch" in item for item in diagnostics.price_errors))

    def test_eth_signal_does_not_match_btc_outcome(self):
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            signal_id="sig-btc-1",
            trade_opportunity_id="opp-btc-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )
        self.assertEqual(2002.0, result["price"])
        self.assertEqual("okx_mark", result["source"])
        self.assertFalse(any("outcomes_asset_mismatch" in item for item in diagnostics.price_errors))

    def test_no_signal_id_skips_outcome_fallback(self):
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            trade_opportunity_id="opp-eth-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )
        self.assertEqual(2002.0, result["price"])
        self.assertEqual("okx_mark", result["source"])
        self.assertIn("outcome_fallback_skipped_missing_signal_id", diagnostics.warnings)

    def test_no_signal_id_and_no_market_context_returns_no_price(self):
        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "BTC",
            "BTC/USDT",
            1002,
            trade_opportunity_id="opp-btc-1",
            conn=self.conn,
            replay_diagnostics=diagnostics,
        )

        self.assertIsNone(result["price"])
        self.assertEqual("no_price_for_signal_id", result["reason"])
        self.assertIn("outcome_fallback_skipped_missing_signal_id", diagnostics.warnings)

    def test_run_trade_replay_reports_schema_errors(self):
        broken = sqlite3.connect(Path(self.temp_dir.name) / "broken.sqlite")
        broken.execute("CREATE TABLE trade_replay_examples (replay_id TEXT PRIMARY KEY, logical_date TEXT)")
        broken.execute("CREATE TABLE trade_replay_profile_stats (profile_key TEXT PRIMARY KEY)")
        broken.commit()
        broken.close()

        summary = run_trade_replay("2026-04-30", db_path=Path(self.temp_dir.name) / "broken.sqlite", dry_run=True)

        self.assertEqual("degraded", summary["status"])
        self.assertTrue(summary["schema_errors"])
        self.assertTrue(any("schema_error" in item for item in summary["warnings"]))

    def test_run_trade_replay_query_exception_reports_schema_error(self):
        db_path = Path(self.temp_dir.name) / "raises.sqlite"
        db_path.write_text("", encoding="utf-8")
        conn = mock.Mock()
        conn.execute.side_effect = sqlite3.OperationalError("mock query boom")
        with mock.patch("trade_replay.sqlite3.connect", return_value=conn):
            summary = run_trade_replay("2026-04-30", db_path=db_path)

        self.assertEqual("error", summary["status"])
        self.assertTrue(summary["query_errors"])
        self.assertTrue(summary["schema_errors"])

    def test_price_lookup_sql_exception_is_visible(self):
        class BrokenConnection:
            def execute(self, *args, **kwargs):
                raise sqlite3.OperationalError("mock sql boom")

        diagnostics = ReplayDiagnostics()
        result = get_price_at_ts(
            "ETH",
            "ETH/USDT",
            1002,
            signal_id="sig-eth-1",
            conn=BrokenConnection(),
            replay_diagnostics=diagnostics,
        )

        self.assertIsNone(result["price"])
        self.assertEqual("schema_error", result["reason"])
        self.assertTrue(diagnostics.query_errors)
        self.assertTrue(diagnostics.schema_errors)


if __name__ == "__main__":
    unittest.main()
