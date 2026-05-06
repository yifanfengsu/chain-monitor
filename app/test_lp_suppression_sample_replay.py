from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from trade_replay import (
    LP_SUPPRESSION_SAMPLE_SCOPE,
    infer_lp_sample_replay_side,
    normalize_lp_sample_asset_pair,
    run_lp_suppression_sample_replay,
)


def _ts(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


class LpSuppressionSampleReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "sample.sqlite"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_delivery_audit()
        self._create_signals()
        self._create_market_context()
        self._create_trade_opportunities()

    def tearDown(self) -> None:
        self.conn.close()
        self.temp_dir.cleanup()

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
                final_trading_output_label TEXT,
                opportunity_gate_failure_reason TEXT,
                delivered INTEGER,
                notifier_sent_at REAL,
                suppression_reason TEXT,
                audit_json TEXT,
                archive_written_at REAL,
                created_at REAL
            )
            """
        )

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
                canonical_semantic_key TEXT,
                lp_alert_stage TEXT,
                signal_json TEXT
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

    def _create_trade_opportunities(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE trade_opportunities (
                trade_opportunity_id TEXT PRIMARY KEY,
                signal_id TEXT,
                status TEXT
            )
            """
        )

    def _insert_sample(
        self,
        index: int,
        *,
        reason: str,
        asset: str = "ETH",
        signal_asset: str | None = None,
        pair: str = "ETH/USDT",
        action: str = "LONG_BIAS_OBSERVE",
        stage: str = "confirm",
        entry_price: float = 2000.0,
        exit_price: float = 1990.0,
        add_market_context: bool = True,
        market_asset: str | None = None,
        market_pair: str | None = None,
        audit_payload: dict | None = None,
        signal_payload: dict | None = None,
        signal_direction: str = "",
        signal_action: str | None = None,
    ) -> None:
        start = _ts("2026-05-04T16:00:00+00:00") + index * 120
        signal_id = f"sig-{index}"
        audit_id = f"audit-{index}"
        opportunity_id = f"opp-{index}"
        payload = audit_payload if audit_payload is not None else {
            "direction": "LONG",
            "pair": pair,
            "trade_action_key": action,
            "lp_stage": stage,
            "suppression_reason": reason,
        }
        signal_payload = signal_payload if signal_payload is not None else {
            "pair": pair,
            "trade_action_key": action,
            "lp_stage": stage,
        }
        signal_action = action if signal_action is None else signal_action
        self.conn.execute(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                audit_id,
                signal_id,
                opportunity_id,
                asset,
                "BLOCKED",
                "NONE",
                "",
                "ETH|LONG|confirm",
                0,
                action,
                reason,
                0,
                1,
                start,
                stage,
                reason,
                action,
                "",
                0,
                None,
                reason,
                json.dumps(payload),
                start,
                start,
            ),
        )
        signal_asset = asset if signal_asset is None else signal_asset
        self.conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                signal_id,
                opportunity_id,
                signal_asset,
                pair,
                start,
                start,
                start,
                signal_direction,
                signal_action,
                signal_payload.get("intent_type") or signal_payload.get("canonical_semantic_key") or "",
                stage,
                json.dumps(signal_payload),
            ),
        )
        self.conn.execute("INSERT INTO trade_opportunities VALUES (?,?,?)", (opportunity_id, signal_id, "BLOCKED"))
        if add_market_context:
            normalized = normalize_lp_sample_asset_pair({"asset": signal_asset, "pair": pair})
            context_asset = market_asset or str(normalized.get("asset") or signal_asset)
            context_pair = market_pair or str(normalized.get("pair") or pair)
            self.conn.executemany(
                "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
                [
                    (f"ctx-entry-{index}", context_asset, context_pair, "okx_perp", entry_price, start + 5),
                    (f"ctx-exit-{index}", context_asset, context_pair, "okx_perp", exit_price, start + 65),
                ],
            )

    def _insert_negative_samples(self, count_per_reason: int = 10) -> None:
        index = 0
        for reason in ("gate/lp_noise_filtered", "listener_prefilter/drop"):
            for _ in range(count_per_reason):
                pair = "ETH/USDC" if index % 2 else "ETH/USDT"
                action = "DO_NOT_CHASE_LONG" if index % 3 == 0 else "LONG_BIAS_OBSERVE"
                stage = "exhaustion_risk" if index % 4 == 0 else "confirm"
                self._insert_sample(index, reason=reason, pair=pair, action=action, stage=stage)
                index += 1
        self.conn.commit()

    def test_normalize_pair_asset_valid(self) -> None:
        result = normalize_lp_sample_asset_pair({"pair": "ETH/USDC", "asset": "ETH"})

        self.assertEqual("ETH", result["asset"])
        self.assertEqual("ETH/USDC", result["pair"])
        self.assertEqual("ETH", result["base"])
        self.assertEqual("USDC", result["quote"])
        self.assertIsNone(result["invalid_reason"])

    def test_normalize_weth_pair(self) -> None:
        result = normalize_lp_sample_asset_pair({"pair": "WETH/USDC", "asset": "WETH"})

        self.assertEqual("ETH", result["asset"])
        self.assertEqual("ETH/USDC", result["pair"])
        self.assertIsNone(result["invalid_reason"])

    def test_normalize_wbtc_pair(self) -> None:
        result = normalize_lp_sample_asset_pair({"pair": "WBTC/USDC", "asset": "WBTC"})

        self.assertEqual("BTC", result["asset"])
        self.assertEqual("BTC/USDC", result["pair"])
        self.assertIsNone(result["invalid_reason"])

    def test_normalize_asset_pair_mismatch(self) -> None:
        result = normalize_lp_sample_asset_pair({"pair": "ETH/USDC", "asset": "WBTC"})

        self.assertEqual("ETH", result["asset"])
        self.assertEqual("ETH/USDC", result["pair"])
        self.assertEqual("asset_pair_mismatch", result["invalid_reason"])

    def test_single_asset_pair_is_ambiguous(self) -> None:
        for value in ("USDC", "WBTC"):
            result = normalize_lp_sample_asset_pair({"pair": value})
            self.assertIsNone(result["pair"])
            self.assertEqual("asset_pair_ambiguous", result["invalid_reason"])

    def test_dry_run_does_not_write_replay_rows(self) -> None:
        self._insert_negative_samples()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual("ok", summary["status"])
        self.assertEqual(20, summary["candidate_sample_count"])
        self.assertEqual(20, summary["would_insert_replay_rows"])
        table = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_replay_examples'"
        ).fetchone()
        self.assertIsNone(table)

    def test_signal_json_long_bias_observe_infers_long_and_valid(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            action="DROP",
            audit_payload={"suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "LONG_BIAS_OBSERVE", "intent_type": "pool_buy_pressure"},
            signal_action="LONG_BIAS_OBSERVE",
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(1, summary["valid_replay_count"])
        self.assertEqual(1, summary["direction_inference_summary"]["long"])
        self.assertEqual({"LONG_BIAS_OBSERVE": 1}, summary["by_intent"])

    def test_signal_json_short_bias_observe_infers_short(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            action="DROP",
            audit_payload={"suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "SHORT_BIAS_OBSERVE", "intent_type": "pool_sell_pressure"},
            signal_action="SHORT_BIAS_OBSERVE",
            entry_price=2000.0,
            exit_price=1990.0,
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(1, summary["valid_replay_count"])
        self.assertEqual(1, summary["direction_inference_summary"]["short"])

    def test_do_not_chase_long_is_low_confidence_replay_direction(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            action="DROP",
            audit_payload={"suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "DO_NOT_CHASE_LONG"},
            signal_action="DO_NOT_CHASE_LONG",
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(1, summary["valid_replay_count"])
        self.assertEqual(1, summary["direction_inference_summary"]["do_not_chase_long"])
        self.assertEqual(0.55, infer_lp_sample_replay_side({"signal_json": {"trade_action_key": "DO_NOT_CHASE_LONG"}})["direction_confidence"])
        rendered = json.dumps(summary, ensure_ascii=False)
        for forbidden in ("交易建议", "买入", "卖出", "仓位", "杠杆"):
            self.assertNotIn(forbidden, rendered)

    def test_no_trade_or_missing_direction_is_ambiguous(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            action="DROP",
            audit_payload={"suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "NO_TRADE"},
            signal_action="NO_TRADE",
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(0, summary["valid_replay_count"])
        self.assertEqual({"direction_ambiguous": 1}, summary["invalid_reason_counts"])
        self.assertEqual(1, summary["direction_inference_summary"]["ambiguous"])

    def test_conflicting_direction_fields_are_invalid(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            action="DROP",
            audit_payload={"side": "long", "suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "SHORT_BIAS_OBSERVE"},
            signal_action="SHORT_BIAS_OBSERVE",
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(0, summary["valid_replay_count"])
        self.assertEqual({"direction_conflict": 1}, summary["invalid_reason_counts"])
        self.assertEqual(1, summary["direction_inference_summary"]["conflict"])

    def test_direction_valid_asset_pair_mismatch_is_invalid(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            asset="WBTC",
            signal_asset="WBTC",
            pair="ETH/USDC",
            action="LONG_BIAS_OBSERVE",
            audit_payload={"direction": "LONG", "pair": "ETH/USDC", "asset": "WBTC", "suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "LONG_BIAS_OBSERVE", "pair": "ETH/USDC", "asset": "WBTC"},
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(0, summary["valid_replay_count"])
        self.assertEqual({"asset_pair_mismatch": 1}, summary["invalid_reason_counts"])
        self.assertEqual(1, summary["invalid_count"])
        self.assertEqual(0, summary["would_insert_valid_replay_rows"])
        self.assertEqual(1, summary["invalid_rows_not_inserted"])
        self.assertEqual("asset_pair_mismatch", summary["asset_pair_mismatch_examples"][0]["invalid_reason"])

    def test_single_asset_pair_not_counted_by_pair(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            asset="USDC",
            signal_asset="USDC",
            pair="USDC",
            action="LONG_BIAS_OBSERVE",
            add_market_context=False,
            audit_payload={"direction": "LONG", "pair": "USDC", "suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "LONG_BIAS_OBSERVE", "pair": "USDC"},
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual({"asset_pair_ambiguous": 1}, summary["invalid_reason_counts"])
        self.assertNotIn("USDC", summary["by_pair"])
        self.assertEqual(1, summary["asset_pair_summary"]["invalid_single_asset_pair_count"])

    def test_direction_valid_market_context_missing_is_invalid(self) -> None:
        self._insert_sample(
            0,
            reason="gate/lp_noise_filtered",
            pair="ETH/USDC",
            action="LONG_BIAS_OBSERVE",
            add_market_context=False,
            audit_payload={"direction": "LONG", "pair": "ETH/USDC", "suppression_reason": "gate/lp_noise_filtered"},
            signal_payload={"trade_action_key": "LONG_BIAS_OBSERVE", "pair": "ETH/USDC"},
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(0, summary["valid_replay_count"])
        self.assertEqual({"market_context_missing": 1}, summary["invalid_reason_counts"])

    def test_listener_prefilter_signal_join_recovers_direction(self) -> None:
        self._insert_sample(
            0,
            reason="listener_prefilter/drop",
            action="DROP",
            audit_payload={"suppression_reason": "listener_prefilter/drop"},
            signal_payload={"trade_action_key": "SHORT_BIAS_OBSERVE", "intent_type": "pool_sell_pressure", "pair": "ETH/USDT"},
            signal_action="SHORT_BIAS_OBSERVE",
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(1, summary["valid_replay_count"])
        self.assertEqual(1, summary["listener_prefilter_join_success_count"])
        self.assertEqual(1, summary["listener_prefilter_direction_recovered_count"])
        self.assertEqual(0, summary["listener_prefilter_still_ambiguous_count"])

    def test_listener_prefilter_drop_metadata_recovers_direction_without_signal_join(self) -> None:
        start = _ts("2026-05-04T16:00:00+00:00")
        audit_payload = {
            "drop_metadata_version": 1,
            "event_id": "evt-listener-metadata",
            "tx_hash": "0xabc",
            "pool_address": "0x1111111111111111111111111111111111111111",
            "pair": "WETH/USDC",
            "asset": "WETH",
            "base": "WETH",
            "quote": "USDC",
            "side": "short",
            "intent_type": "pool_sell_pressure",
            "lp_stage": "confirm",
            "event_ts": start,
            "drop_reason": "listener_prefilter/drop",
            "suppression_reason": "listener_prefilter/drop",
        }
        self.conn.execute(
            "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "audit-listener-metadata",
                "",
                "",
                "",
                "BLOCKED",
                "NONE",
                "",
                "ETH|SHORT|confirm",
                0,
                "DROP",
                "listener_prefilter/drop",
                0,
                1,
                start,
                "listener_prefilter",
                "lp_adjacent_noise_skipped_in_listener",
                "DROP",
                "",
                0,
                None,
                "listener_prefilter/drop",
                json.dumps(audit_payload),
                start,
                start,
            ),
        )
        self.conn.executemany(
            "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
            [
                ("ctx-metadata-entry", "ETH", "ETH/USDC", "okx_perp", 2000.0, start + 5),
                ("ctx-metadata-exit", "ETH", "ETH/USDC", "okx_perp", 1990.0, start + 65),
            ],
        )
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        self.assertEqual(1, summary["valid_replay_count"])
        self.assertEqual(1, summary["listener_prefilter_metadata_rows"])
        self.assertEqual(1, summary["listener_prefilter_metadata_direction_rows"])
        self.assertEqual(1, summary["listener_prefilter_metadata_pair_rows"])
        self.assertEqual(0, summary["listener_prefilter_join_success_count"])
        self.assertEqual(1, summary["listener_prefilter_direction_recovered_count"])
        self.assertEqual("metadata", summary["listener_prefilter_recovery_mode"])
        self.assertEqual({"ETH/USDC": 1}, summary["by_pair"])
        by_reason = {item["reason"]: item for item in summary["by_reason"]}
        self.assertEqual(1, by_reason["listener_prefilter/drop"]["valid_replay_count"])

    def test_execute_writes_only_lp_suppression_sample_scope(self) -> None:
        self._insert_negative_samples()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=False,
            limit_per_reason=100,
        )

        self.assertEqual("ok", summary["status"])
        self.assertEqual(20, summary["inserted_replay_rows"])
        scope_counts = {
            str(row["replay_scope"]): int(row["count"])
            for row in self.conn.execute(
                "SELECT replay_scope, COUNT(*) AS count FROM trade_replay_examples GROUP BY replay_scope"
            ).fetchall()
        }
        self.assertEqual({LP_SUPPRESSION_SAMPLE_SCOPE: 20}, scope_counts)
        features = json.loads(
            self.conn.execute(
                "SELECT features_json FROM trade_replay_examples WHERE replay_scope=? LIMIT 1",
                (LP_SUPPRESSION_SAMPLE_SCOPE,),
            ).fetchone()[0]
        )
        self.assertEqual(LP_SUPPRESSION_SAMPLE_SCOPE, features["replay_scope"])
        self.assertIn("direction_source", features)
        full_count = self.conn.execute(
            "SELECT COUNT(*) FROM trade_replay_examples WHERE replay_scope='full'"
        ).fetchone()[0]
        self.assertEqual(0, int(full_count))

    def test_execute_only_writes_valid_rows(self) -> None:
        index = 0
        for reason in ("gate/lp_noise_filtered", "listener_prefilter/drop"):
            for _ in range(6):
                self._insert_sample(index, reason=reason, action="LONG_BIAS_OBSERVE")
                index += 1
            for _ in range(94):
                self._insert_sample(
                    index,
                    reason=reason,
                    action="DROP",
                    audit_payload={"suppression_reason": reason, "pair": "ETH/USDT"},
                    signal_payload={"trade_action_key": "NO_TRADE", "pair": "ETH/USDT"},
                    signal_action="NO_TRADE",
                )
                index += 1
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=False,
            limit_per_reason=100,
        )

        self.assertEqual(12, summary["valid_replay_count"])
        self.assertEqual(188, summary["invalid_count"])
        self.assertEqual(12, summary["inserted_replay_rows"])
        self.assertEqual(188, summary["invalid_rows_not_inserted"])
        persisted = self.conn.execute(
            "SELECT COUNT(*) FROM trade_replay_examples WHERE replay_scope=?",
            (LP_SUPPRESSION_SAMPLE_SCOPE,),
        ).fetchone()[0]
        invalid_persisted = self.conn.execute(
            "SELECT COUNT(*) FROM trade_replay_examples WHERE replay_scope=? AND data_valid=0",
            (LP_SUPPRESSION_SAMPLE_SCOPE,),
        ).fetchone()[0]
        self.assertEqual(12, int(persisted))
        self.assertEqual(0, int(invalid_persisted))

    def test_by_reason_average_and_negative_action(self) -> None:
        self._insert_negative_samples()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=False,
            limit_per_reason=100,
        )

        by_reason = {item["reason"]: item for item in summary["by_reason"]}
        self.assertEqual(10, by_reason["gate/lp_noise_filtered"]["sample_count"])
        self.assertEqual(10, by_reason["listener_prefilter/drop"]["sample_count"])
        self.assertAlmostEqual(-72.0, by_reason["gate/lp_noise_filtered"]["avg_net_pnl_bps"])
        self.assertAlmostEqual(-72.0, by_reason["listener_prefilter/drop"]["avg_net_pnl_bps"])
        self.assertEqual("keep_suppressed", by_reason["gate/lp_noise_filtered"]["recommended_action"])
        self.assertEqual("keep_suppressed", by_reason["listener_prefilter/drop"]["recommended_action"])
        self.assertEqual("early_suppression_seems_correct", summary["diagnosis"])

    def test_low_sample_needs_more_samples(self) -> None:
        self._insert_sample(0, reason="gate/lp_noise_filtered")
        self.conn.commit()

        summary = run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=True,
            limit_per_reason=100,
        )

        by_reason = {item["reason"]: item for item in summary["by_reason"]}
        self.assertEqual("needs_more_samples", by_reason["gate/lp_noise_filtered"]["recommended_action"])
        self.assertEqual("needs_more_samples", summary["diagnosis"])

    def test_execute_does_not_change_delivery_or_opportunity_state(self) -> None:
        self._insert_negative_samples()

        run_lp_suppression_sample_replay(
            "2026-05-05",
            db_path=self.db_path,
            dry_run=False,
            limit_per_reason=100,
        )

        sent_count = self.conn.execute("SELECT SUM(sent_to_telegram) FROM delivery_audit").fetchone()[0]
        delivered_count = self.conn.execute("SELECT SUM(delivered) FROM delivery_audit").fetchone()[0]
        statuses = {
            str(row["status"])
            for row in self.conn.execute("SELECT DISTINCT status FROM trade_opportunities").fetchall()
        }
        self.assertEqual(0, int(sent_count or 0))
        self.assertEqual(0, int(delivered_count or 0))
        self.assertEqual({"BLOCKED"}, statuses)


if __name__ == "__main__":
    unittest.main()
