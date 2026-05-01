from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from trade_replay import run_trade_replay


def _ts(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


class TradeReplaySqliteInputCoverageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "coverage.sqlite"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.conn.close()
        self.temp_dir.cleanup()

    def _create_signals(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE signals (
                signal_id TEXT PRIMARY KEY,
                trade_opportunity_id TEXT,
                asset TEXT,
                pair TEXT,
                timestamp REAL,
                archive_written_at REAL,
                created_at REAL,
                direction TEXT,
                trade_action_key TEXT,
                trade_opportunity_status TEXT,
                trade_opportunity_shadow_status TEXT,
                opportunity_profile_key TEXT,
                lp_alert_stage TEXT,
                delivery_decision TEXT,
                sent_to_telegram INTEGER,
                replay_eligible INTEGER,
                signal_json TEXT
            )
            """
        )

    def _create_delivery_audit(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE delivery_audit (
                audit_id TEXT PRIMARY KEY,
                signal_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                opportunity_status TEXT,
                shadow_status TEXT,
                blocked_reason TEXT,
                profile_key TEXT,
                replay_eligible INTEGER,
                delivery_decision TEXT,
                reason TEXT,
                sent_to_telegram INTEGER,
                suppressed INTEGER,
                timestamp REAL,
                stage TEXT,
                gate_reason TEXT,
                delivered INTEGER,
                notifier_sent_at REAL,
                suppression_reason TEXT,
                audit_json TEXT,
                archive_written_at REAL,
                created_at REAL
            )
            """
        )

    def _create_market_context(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE market_context_snapshots (
                context_id TEXT PRIMARY KEY,
                asset TEXT,
                pair TEXT,
                venue TEXT,
                perp_mark_price REAL,
                created_at REAL
            )
            """
        )
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.executemany(
            "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
            [
                ("ctx-entry", "ETH", "ETH/USDT", "okx_perp", 2000.0, start + 5),
                ("ctx-exit", "ETH", "ETH/USDT", "okx_perp", 2010.0, start + 65),
            ],
        )

    def test_date_uses_beijing_logical_window(self) -> None:
        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual("Asia/Shanghai", summary["timezone"])
        self.assertEqual("2026-04-29 16:00:00 UTC", summary["logical_window_start_utc"])
        self.assertEqual("2026-04-30 15:59:59 UTC", summary["logical_window_end_utc"])

    def test_signals_without_trade_opportunities_build_replay_inputs(self) -> None:
        self._create_signals()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-signal-only",
                "",
                "ETH",
                "ETH/USDT",
                start,
                start,
                start,
                "LONG",
                "LONG_CHASE_ALLOWED",
                "CANDIDATE",
                "NONE",
                "ETH|LONG|profile",
                "confirm",
                "",
                0,
                1,
                "{}",
            ),
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(1, summary["input_source_counts"]["signals"])
        self.assertEqual(0, summary["input_source_counts"]["trade_opportunities"])
        self.assertEqual(1, summary["eligibility_summary"]["eligible_count"])
        self.assertEqual(1, summary["replay_count"])

    def test_blocked_do_not_chase_replays_by_default(self) -> None:
        self._create_signals()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-blocked",
                "",
                "ETH",
                "ETH/USDT",
                start,
                start,
                start,
                "",
                "DO_NOT_CHASE_LONG",
                "BLOCKED",
                "NONE",
                "ETH|LONG|blocked",
                "confirm",
                "",
                0,
                1,
                "{}",
            ),
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(1, summary["input_source_counts"]["blocked"])
        self.assertEqual(1, summary["eligibility_summary"]["eligible_count"])
        self.assertEqual(1, summary["replay_count"])

    def test_suppressed_requires_include_suppressed(self) -> None:
        self._create_delivery_audit()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "audit-suppressed",
                "sig-suppressed",
                "",
                "ETH",
                "CANDIDATE",
                "NONE",
                "",
                "ETH|LONG|suppressed",
                1,
                "WAIT_CONFIRMATION",
                "",
                0,
                1,
                start,
                "confirm",
                "",
                0,
                None,
                "same_asset_state_repeat",
                '{"direction":"LONG","pair":"ETH/USDT"}',
                start,
                start,
            ),
        )
        self.conn.commit()

        default_summary = run_trade_replay("2026-04-30", db_path=self.db_path)
        full_summary = run_trade_replay("2026-04-30", db_path=self.db_path, include_suppressed=True)

        self.assertEqual(0, default_summary["replay_count"])
        self.assertEqual(1, default_summary["eligibility_summary"]["ineligible_reasons"]["filter_excluded_suppressed"])
        self.assertEqual(1, full_summary["replay_count"])

    def test_zero_replay_outputs_counts_and_specific_reason(self) -> None:
        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(0, summary["replay_count"])
        self.assertIn("input_source_counts", summary)
        self.assertIn("eligibility_summary", summary)
        self.assertIn("trade_replay_missing:no_signals_in_window", summary["warnings"])

    def test_all_rows_missing_direction_is_diagnosed(self) -> None:
        self._create_signals()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-no-direction",
                "",
                "ETH",
                "ETH/USDT",
                start,
                start,
                start,
                "",
                "",
                "CANDIDATE",
                "NONE",
                "ETH|unknown",
                "confirm",
                "",
                0,
                1,
                "{}",
            ),
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(0, summary["replay_count"])
        self.assertEqual(1, summary["eligibility_summary"]["ineligible_reasons"]["direction_ambiguous"])
        self.assertIn("trade_replay_missing:all_rows_missing_direction", summary["warnings"])

    def test_no_trade_lock_ambiguous_direction_is_not_silent(self) -> None:
        self._create_signals()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-lock",
                "",
                "ETH",
                "ETH/USDT",
                start,
                start,
                start,
                "",
                "NO_TRADE_LOCK",
                "BLOCKED",
                "NONE",
                "ETH|lock",
                "confirm",
                "",
                0,
                1,
                "{}",
            ),
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(0, summary["replay_count"])
        self.assertEqual(1, summary["input_source_counts"]["blocked"])
        self.assertEqual(1, summary["eligibility_summary"]["ineligible_reasons"]["direction_ambiguous"])


if __name__ == "__main__":
    unittest.main()
