from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sqlite_store
from trade_opportunity import TradeOpportunityManager, choose_primary_blocker
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_row, make_signal


def _profile_key(manager: TradeOpportunityManager, event, signal) -> str:
    summary = manager._summary(event, signal)
    profile = manager._build_profile(summary=summary, side=str(summary.get("trade_opportunity_side") or "NONE"))
    return str(profile["opportunity_profile_key"])


class TradeOpportunityReplayGateTests(unittest.TestCase):
    def _manager(self, records=None) -> TradeOpportunityManager:
        manager = TradeOpportunityManager(
            state_manager=StubStateManager(records=records or []),
            persistence_enabled=False,
        )
        manager._mirror_sqlite_opportunity = lambda _payload: None
        return manager

    def _create_replay_profile_db(self, db_path: Path, profile_key: str, *, use_daily: bool = False) -> None:
        conn = sqlite3.connect(db_path)
        if use_daily:
            conn.execute(
                """
                CREATE TABLE trade_replay_profile_daily_stats (
                    logical_date TEXT NOT NULL,
                    replay_scope TEXT NOT NULL DEFAULT 'default',
                    strategy_config_hash TEXT NOT NULL DEFAULT '',
                    profile_key TEXT NOT NULL,
                    valid_sample_count INTEGER DEFAULT 0,
                    sample_count INTEGER DEFAULT 0,
                    avg_net_pnl_bps REAL,
                    win_rate REAL,
                    clean_followthrough_rate REAL,
                    chop_rate REAL,
                    recommended_action TEXT,
                    PRIMARY KEY(logical_date, replay_scope, strategy_config_hash, profile_key)
                )
                """
            )
            conn.execute(
                "INSERT INTO trade_replay_profile_daily_stats VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("2026-04-30", "full", "hash", profile_key, 48, 48, -21.67, 0.0, 0.0, 0.9583, "block_profile"),
            )
        else:
            conn.execute(
                """
                CREATE TABLE trade_replay_profile_stats (
                    profile_key TEXT PRIMARY KEY,
                    valid_sample_count INTEGER DEFAULT 0,
                    sample_count INTEGER DEFAULT 0,
                    avg_net_pnl_bps REAL,
                    win_rate REAL,
                    clean_followthrough_rate REAL,
                    chop_rate REAL,
                    recommended_action TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO trade_replay_profile_stats VALUES (?,?,?,?,?,?,?,?)",
                (profile_key, 48, 48, -21.67, 0.0, 0.0, 0.9583, "block_profile"),
            )
        conn.commit()
        conn.close()

    def test_choose_primary_blocker_prefers_replay_over_legacy_profile_blockers(self) -> None:
        self.assertEqual(
            "replay_profile_negative",
            choose_primary_blocker(["profile_adverse_too_high", "replay_profile_negative"]),
        )
        self.assertEqual(
            "no_trade_lock",
            choose_primary_blocker(["no_trade_lock", "replay_profile_negative"]),
        )
        self.assertEqual(
            "replay_profile_negative",
            choose_primary_blocker(["profile_followthrough_too_low", "replay_profile_negative"]),
        )

    def test_high_confidence_negative_profile_blocks_candidate_and_verified(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertEqual("replay_profile_negative", payload["trade_opportunity_primary_hard_blocker"])
        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])

    def test_replay_profile_negative_reselects_primary_over_profile_adverse_blocker(self) -> None:
        manager = self._manager(records=[make_outcome_row(adverse=True) for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("profile_adverse_too_high", payload["trade_opportunity_blockers"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertEqual("replay_profile_negative", payload["trade_opportunity_primary_blocker"])

    def test_replay_profile_negative_remains_when_higher_priority_blocker_exists(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(
            event,
            no_trade_lock_active=True,
            asset_market_state_key="NO_TRADE_LOCK",
        )
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("no_trade_lock", payload["trade_opportunity_primary_blocker"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_hard_blockers"])

    def test_sqlite_opportunity_json_persists_replay_profile_negative(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }
        payload = manager.apply_lp_signal(event, signal)

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_store.close()
            conn = sqlite_store.init_sqlite_store(Path(tmpdir) / "chain_monitor.sqlite")
            try:
                self.assertTrue(sqlite_store.upsert_trade_opportunity(payload))
                row = conn.execute(
                    """
                    SELECT primary_blocker, opportunity_json, blockers_json, hard_blockers_json
                    FROM trade_opportunities
                    WHERE trade_opportunity_id=?
                    """,
                    (payload["trade_opportunity_id"],),
                ).fetchone()
            finally:
                sqlite_store.close()

        opportunity_json = json.loads(row["opportunity_json"])
        blockers_json = json.loads(row["blockers_json"])
        hard_blockers_json = json.loads(row["hard_blockers_json"])
        self.assertEqual("replay_profile_negative", row["primary_blocker"])
        self.assertEqual(row["primary_blocker"], opportunity_json["trade_opportunity_primary_blocker"])
        self.assertIn("replay_profile_negative", blockers_json)
        self.assertIn("replay_profile_negative", hard_blockers_json)
        self.assertIn("replay_profile_negative", opportunity_json["trade_opportunity_blockers"])

    def test_low_sample_positive_profile_does_not_promote_verified(self) -> None:
        manager = self._manager(records=[])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 1,
            "avg_net_pnl_bps": 70.38,
            "win_rate": 1.0,
            "clean_followthrough_rate": 1.0,
            "chop_rate": 0.0,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertNotIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertNotEqual("promote_candidate", payload["trade_opportunity_replay_profile_action"])
        self.assertEqual("needs_more_samples", payload["trade_opportunity_replay_profile_action"])
        self.assertEqual("positive_profile_needs_more_samples", payload["trade_opportunity_replay_profile_research_hint"])
        display_text = " ".join(
            str(payload.get(key) or "")
            for key in ("headline_label", "market_state_label", "trade_opportunity_reason", "final_trading_output_label")
        )
        self.assertNotIn("正收益", display_text)

    def test_replay_blocker_does_not_affect_non_matching_profile(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        manager._replay_profile_gate_stats["ETH|LONG|other|profile"] = {
            "profile_key": "ETH|LONG|other|profile",
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertNotEqual("replay_profile_negative", payload["trade_opportunity_primary_hard_blocker"])

    def test_local_absorption_quality_low_with_negative_profile_has_replay_blocker(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_absorption_context="local_buy_pressure_absorption",
            asset_case_supporting_pair_count=1,
            lp_multi_pool_resonance=1,
            pool_quality_score=0.40,
            pair_quality_score=0.42,
            asset_case_quality_score=0.44,
            trade_action_key="DO_NOT_CHASE_LONG",
            asset_market_state_key="DO_NOT_CHASE_LONG",
        )
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 48,
            "avg_net_pnl_bps": -21.67,
            "win_rate": 0.0,
            "clean_followthrough_rate": 0.0,
            "chop_rate": 0.9583,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertIn("local_absorption_quality_low", payload["trade_opportunity_risk_flags"])
        self.assertEqual("replay_profile_negative", payload["lp_market_read_evidence"])
        self.assertIn("历史 replay 显示该 profile 无交易优势", payload["trade_opportunity_reason"])

    def test_local_absorption_quality_low_without_replay_stats_uses_heuristic_evidence(self) -> None:
        manager = self._manager(records=[make_outcome_row() for _ in range(25)])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(
            event,
            lp_confirm_scope="local_confirm",
            lp_absorption_context="local_buy_pressure_absorption",
            asset_case_supporting_pair_count=1,
            lp_multi_pool_resonance=1,
            pool_quality_score=0.40,
            pair_quality_score=0.42,
            asset_case_quality_score=0.44,
            trade_action_key="DO_NOT_CHASE_LONG",
            asset_market_state_key="DO_NOT_CHASE_LONG",
        )

        payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertEqual("heuristic_quality_low", payload["lp_market_read_evidence"])
        self.assertNotIn("历史 replay", payload["trade_opportunity_reason"])

    def test_positive_profile_does_not_auto_verified(self) -> None:
        manager = self._manager(records=[])
        event = make_event(intent_type="pool_buy_pressure")
        signal = make_signal(event)
        profile_key = _profile_key(manager, event, signal)
        manager._replay_profile_gate_stats[profile_key] = {
            "profile_key": profile_key,
            "valid_sample_count": 30,
            "avg_net_pnl_bps": 15.5,
            "win_rate": 0.70,
            "clean_followthrough_rate": 0.60,
            "chop_rate": 0.10,
        }

        payload = manager.apply_lp_signal(event, signal)

        self.assertNotEqual("VERIFIED", payload["trade_opportunity_status"])
        self.assertEqual("positive_profile_research_only", payload["trade_opportunity_replay_profile_research_hint"])

    def test_manager_initialization_loads_sqlite_replay_profile_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager_for_key = self._manager()
            event = make_event(intent_type="pool_buy_pressure")
            signal = make_signal(event)
            profile_key = _profile_key(manager_for_key, event, signal)
            db_path = Path(tmpdir) / "replay.sqlite"
            self._create_replay_profile_db(db_path, profile_key)

            with mock.patch("trade_opportunity.SQLITE_DB_PATH", str(db_path)):
                manager = TradeOpportunityManager(
                    state_manager=StubStateManager(records=[]),
                    persistence_enabled=True,
                    cache_path=str(Path(tmpdir) / "opportunity-cache.json"),
                    recover_on_start=False,
                )

            self.assertIn(profile_key, manager._replay_profile_gate_stats)
            self.assertEqual("loaded", manager._replay_profile_gate_diagnostics["load_status"])
            self.assertEqual("trade_replay_profile_stats", manager._replay_profile_gate_diagnostics["source"])

    def test_apply_lp_signal_uses_runtime_loaded_sqlite_profile_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manager_for_key = self._manager(records=[make_outcome_row() for _ in range(25)])
            event = make_event(intent_type="pool_buy_pressure")
            signal = make_signal(event)
            profile_key = _profile_key(manager_for_key, event, signal)
            db_path = Path(tmpdir) / "replay.sqlite"
            self._create_replay_profile_db(db_path, profile_key)

            with mock.patch("trade_opportunity.SQLITE_DB_PATH", str(db_path)):
                manager = TradeOpportunityManager(
                    state_manager=StubStateManager(records=[make_outcome_row() for _ in range(25)]),
                    persistence_enabled=True,
                    cache_path=str(Path(tmpdir) / "opportunity-cache.json"),
                    recover_on_start=False,
                )
                manager._mirror_sqlite_opportunity = lambda _payload: None
                payload = manager.apply_lp_signal(event, signal)

        self.assertEqual("BLOCKED", payload["trade_opportunity_status"])
        self.assertIn("replay_profile_negative", payload["trade_opportunity_blockers"])
        self.assertEqual("trade_replay_profile_stats", payload["trade_opportunity_replay_profile_gate"]["load_diagnostics"]["source"])

    def test_daily_replay_profile_stats_fallback_loads_when_latest_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_key = "ETH|LONG|local_confirm|confirm|leading|local_absorption|major|basis_normal|quality_low"
            db_path = Path(tmpdir) / "replay.sqlite"
            self._create_replay_profile_db(db_path, profile_key, use_daily=True)

            with mock.patch("trade_opportunity.SQLITE_DB_PATH", str(db_path)):
                manager = TradeOpportunityManager(
                    state_manager=StubStateManager(records=[]),
                    persistence_enabled=True,
                    cache_path=str(Path(tmpdir) / "opportunity-cache.json"),
                    recover_on_start=False,
                )

        self.assertIn(profile_key, manager._replay_profile_gate_stats)
        self.assertEqual("trade_replay_profile_daily_stats", manager._replay_profile_gate_diagnostics["source"])

    def test_sqlite_unavailable_does_not_crash_and_records_missing_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_db = Path(tmpdir) / "missing.sqlite"
            with mock.patch("trade_opportunity.SQLITE_DB_PATH", str(missing_db)):
                manager = TradeOpportunityManager(
                    state_manager=StubStateManager(records=[]),
                    persistence_enabled=True,
                    cache_path=str(Path(tmpdir) / "opportunity-cache.json"),
                    recover_on_start=False,
                )

        self.assertEqual({}, manager._replay_profile_gate_stats)
        self.assertEqual("missing", manager._replay_profile_gate_diagnostics["load_status"])
        self.assertEqual("sqlite_db_missing", manager._replay_profile_gate_diagnostics["missing_reason"])


if __name__ == "__main__":
    unittest.main()
