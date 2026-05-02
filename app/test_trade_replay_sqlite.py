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

    def _create_trade_opportunities(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE trade_opportunities (
                signal_id TEXT,
                trade_opportunity_id TEXT PRIMARY KEY,
                asset TEXT,
                pair TEXT,
                opportunity_profile_key TEXT,
                side TEXT,
                status TEXT,
                replay_eligible INTEGER,
                raw_score REAL,
                market_context_source TEXT,
                lp_alert_stage TEXT,
                lp_sweep_phase TEXT,
                lp_confirm_scope TEXT,
                lp_absorption_context TEXT,
                alert_relative_timing TEXT,
                profile_features_json TEXT,
                created_at REAL,
                updated_at REAL,
                opportunity_json TEXT
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
        self._insert_market_context_pair(start)

    def _insert_market_context_pair(self, start: int, *, entry_price: float = 2000.0, exit_price: float = 2010.0) -> None:
        self.conn.executemany(
            "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
            [
                (f"ctx-entry-{start}", "ETH", "ETH/USDT", "okx_perp", entry_price, start + 5),
                (f"ctx-exit-{start}", "ETH", "ETH/USDT", "okx_perp", exit_price, start + 65),
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
        profile_row = self.conn.execute(
            "SELECT valid_sample_count, avg_net_pnl_bps, recommended_action FROM trade_replay_profile_stats WHERE profile_key=?",
            ("ETH|LONG|profile",),
        ).fetchone()
        self.assertIsNotNone(profile_row)
        self.assertEqual(1, int(profile_row["valid_sample_count"]))
        self.assertIsNotNone(profile_row["avg_net_pnl_bps"])
        self.assertTrue(profile_row["recommended_action"])

    def test_trade_opportunity_profile_key_repairs_lp_stage_before_persisting(self) -> None:
        self._create_trade_opportunities()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            """
            INSERT INTO trade_opportunities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "sig-opportunity",
                "opp-profile-repair",
                "ETH",
                "ETH/USDT",
                "ETH|LONG|unknown|confirm|leading|local_absorption|major|basis_normal|quality_low",
                "LONG",
                "CANDIDATE",
                1,
                0.72,
                "live_public",
                "confirm",
                "",
                "",
                "local_buy_pressure_absorption",
                "leading",
                "{}",
                start,
                start,
                "{}",
            ),
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path)

        self.assertEqual(1, summary["input_source_counts"]["trade_opportunities"])
        self.assertEqual(1, summary["replay_count"])
        profile_keys = [
            str(row["profile_key"])
            for row in self.conn.execute("SELECT profile_key FROM trade_replay_profile_stats").fetchall()
        ]
        self.assertIn(
            "ETH|LONG|confirm|confirm|leading|local_absorption|major|basis_normal|quality_low",
            profile_keys,
        )
        self.assertNotIn(
            "lp_stage",
            summary["profile_unknown_diagnostics"]["unknown_by_dimension"],
        )

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
        self.assertEqual(["suppressed_excluded_by_mode"], default_summary["suppressed_replay_zero_reasons"])
        self.assertEqual(1, full_summary["replay_count"])
        self.assertEqual(1, full_summary["suppressed_replay_count"])

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
        self.assertEqual(1, summary["eligibility_summary"]["ineligible_reason_by_source"]["direction_ambiguous"]["signals"])
        self.assertTrue(summary["eligibility_summary"]["ineligible_top_examples"]["direction_ambiguous"])
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

    def test_full_and_default_scopes_do_not_overwrite_each_other(self) -> None:
        self._create_signals()
        self._create_delivery_audit()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-shared",
                "",
                "ETH",
                "ETH/USDT",
                start,
                start,
                start,
                "LONG",
                "LONG_CANDIDATE",
                "CANDIDATE",
                "NONE",
                "ETH|LONG|shared",
                "confirm",
                "",
                0,
                1,
                "{}",
            ),
        )
        self.conn.execute(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "audit-suppressed-scope",
                "sig-suppressed-scope",
                "",
                "ETH",
                "CANDIDATE",
                "NONE",
                "",
                "ETH|LONG|suppressed",
                1,
                "LONG_CANDIDATE",
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

        full_summary = run_trade_replay("2026-04-30", db_path=self.db_path, include_suppressed=True, replay_scope="full")
        default_summary = run_trade_replay("2026-04-30", db_path=self.db_path, replay_scope="default")

        self.assertEqual("full", full_summary["replay_scope"])
        self.assertEqual("default", default_summary["replay_scope"])
        counts = {
            row["replay_scope"]: int(row["count"])
            for row in self.conn.execute(
                """
                SELECT replay_scope, COUNT(*) AS count
                FROM trade_replay_examples
                WHERE logical_date='2026-04-30'
                GROUP BY replay_scope
                """
            ).fetchall()
        }
        self.assertEqual(2, counts["full"])
        self.assertEqual(1, counts["default"])
        replay_ids = [
            str(row["replay_id"])
            for row in self.conn.execute(
                "SELECT replay_id FROM trade_replay_examples WHERE signal_id='sig-shared' ORDER BY replay_scope"
            ).fetchall()
        ]
        self.assertEqual(2, len(replay_ids))
        self.assertTrue(any("_full_" in replay_id for replay_id in replay_ids))
        self.assertTrue(any("_default_" in replay_id for replay_id in replay_ids))

    def test_profile_daily_stats_keep_date_and_scope_history(self) -> None:
        self._create_signals()
        self._create_market_context()
        start_0430 = _ts("2026-04-29T16:00:00+00:00")
        start_0429 = _ts("2026-04-28T16:00:00+00:00")
        self._insert_market_context_pair(start_0429, entry_price=1900.0, exit_price=1910.0)
        for signal_id, start in (("sig-0430", start_0430), ("sig-0429", start_0429)):
            self.conn.execute(
                "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signal_id,
                    "",
                    "ETH",
                    "ETH/USDT",
                    start,
                    start,
                    start,
                    "LONG",
                    "LONG_CANDIDATE",
                    "CANDIDATE",
                    "NONE",
                    "ETH|LONG|daily",
                    "confirm",
                    "",
                    0,
                    1,
                    "{}",
                ),
            )
        self.conn.commit()

        run_trade_replay("2026-04-29", db_path=self.db_path, replay_scope="default")
        run_trade_replay("2026-04-30", db_path=self.db_path, replay_scope="default")
        run_trade_replay("2026-04-30", db_path=self.db_path, include_suppressed=True, replay_scope="full")

        daily_rows = [
            (row["logical_date"], row["replay_scope"], row["profile_key"])
            for row in self.conn.execute(
                """
                SELECT logical_date, replay_scope, profile_key
                FROM trade_replay_profile_daily_stats
                WHERE profile_key='ETH|LONG|daily'
                ORDER BY logical_date, replay_scope
                """
            ).fetchall()
        ]
        self.assertIn(("2026-04-29", "default", "ETH|LONG|daily"), daily_rows)
        self.assertIn(("2026-04-30", "default", "ETH|LONG|daily"), daily_rows)
        self.assertIn(("2026-04-30", "full", "ETH|LONG|daily"), daily_rows)
        latest_count = self.conn.execute(
            "SELECT COUNT(*) AS count FROM trade_replay_profile_stats WHERE profile_key='ETH|LONG|daily'"
        ).fetchone()["count"]
        self.assertEqual(1, int(latest_count))

    def test_coverage_universe_excludes_low_value_audit_rows_and_keeps_actions(self) -> None:
        self._create_delivery_audit()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        rows = [
            (
                "audit-low",
                "",
                "",
                "",
                "",
                "NONE",
                "",
                "",
                0,
                "",
                "stable_non_swap_filtered",
                0,
                0,
                start,
                "filter",
                "",
                0,
                None,
                "stable_non_swap_filtered",
                "{}",
                start,
                start,
            ),
            (
                "audit-do-not-chase",
                "sig-dnc",
                "",
                "ETH",
                "BLOCKED",
                "NONE",
                "",
                "ETH|LONG|dnc",
                1,
                "DO_NOT_CHASE_LONG",
                "",
                0,
                0,
                start,
                "confirm",
                "",
                0,
                None,
                "do_not_chase",
                '{"direction":"LONG","pair":"ETH/USDT"}',
                start,
                start,
            ),
            (
                "audit-wait",
                "sig-wait",
                "",
                "ETH",
                "CANDIDATE",
                "NONE",
                "",
                "ETH|WAIT|ambiguous",
                1,
                "WAIT_CONFIRMATION",
                "",
                0,
                0,
                start,
                "confirm",
                "",
                0,
                None,
                "wait_confirmation",
                '{"pair":"ETH/USDT"}',
                start,
                start,
            ),
        ]
        self.conn.executemany(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path, replay_scope="default")
        eligibility = summary["eligibility_summary"]

        self.assertEqual(3, eligibility["raw_audit_universe_count"])
        self.assertEqual(2, eligibility["replay_candidate_universe_count"])
        self.assertEqual(1, eligibility["not_replay_candidate_reasons"]["stable_non_swap_filtered"])
        self.assertEqual(1, eligibility["eligible_by_action"]["DO_NOT_CHASE_LONG"])
        self.assertEqual(1, eligibility["ambiguous_by_action"]["WAIT_CONFIRMATION"])
        self.assertGreater(eligibility["replay_coverage_rate_candidate"], eligibility["replay_coverage_rate_raw"])

    def test_short_bias_observe_is_directed_and_wait_confirmation_is_ambiguous(self) -> None:
        self._create_delivery_audit()
        self._create_market_context()
        start = _ts("2026-04-29T16:00:00+00:00")
        self.conn.executemany(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    "audit-short-bias",
                    "sig-short-bias",
                    "",
                    "ETH",
                    "CANDIDATE",
                    "NONE",
                    "",
                    "ETH|SHORT|bias",
                    1,
                    "SHORT_BIAS_OBSERVE",
                    "",
                    0,
                    0,
                    start,
                    "confirm",
                    "",
                    0,
                    None,
                    "",
                    '{"pair":"ETH/USDT"}',
                    start,
                    start,
                ),
                (
                    "audit-wait-ambiguous",
                    "sig-wait-ambiguous",
                    "",
                    "ETH",
                    "CANDIDATE",
                    "NONE",
                    "",
                    "ETH|WAIT|ambiguous",
                    1,
                    "WAIT_CONFIRMATION",
                    "",
                    0,
                    0,
                    start,
                    "confirm",
                    "",
                    0,
                    None,
                    "",
                    '{"pair":"ETH/USDT"}',
                    start,
                    start,
                ),
            ],
        )
        self.conn.commit()

        summary = run_trade_replay("2026-04-30", db_path=self.db_path, replay_scope="default")
        eligibility = summary["eligibility_summary"]

        self.assertEqual(1, eligibility["eligible_by_action"]["SHORT_BIAS_OBSERVE"])
        self.assertEqual(1, eligibility["ambiguous_by_action"]["WAIT_CONFIRMATION"])
        self.assertEqual(1, eligibility["ineligible_reason_by_action"]["direction_ambiguous"]["WAIT_CONFIRMATION"])


if __name__ == "__main__":
    unittest.main()
