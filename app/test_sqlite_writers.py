import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.original_modes = {
            "SQLITE_RAW_EVENTS_MODE": sqlite_store.SQLITE_RAW_EVENTS_MODE,
            "SQLITE_PARSED_EVENTS_MODE": sqlite_store.SQLITE_PARSED_EVENTS_MODE,
            "SQLITE_TELEGRAM_DELIVERY_MODE": sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE,
            "SQLITE_MARKET_CONTEXT_ATTEMPT_MODE": sqlite_store.SQLITE_MARKET_CONTEXT_ATTEMPT_MODE,
        }
        sqlite_store.SQLITE_RAW_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_PARSED_EVENTS_MODE = "index_only"
        sqlite_store.SQLITE_TELEGRAM_DELIVERY_MODE = "slim"
        sqlite_store.SQLITE_MARKET_CONTEXT_ATTEMPT_MODE = "slim"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        for key, value in self.original_modes.items():
            setattr(sqlite_store, key, value)
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _count(self, table: str) -> int:
        return int(self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def test_signal_writer_is_idempotent_and_writes_features(self) -> None:
        record = {
            "signal_id": "sig-sqlite-1",
            "signal_archive_key": "sig-sqlite-1",
            "event_id": "evt-1",
            "asset_symbol": "ETH",
            "pair_label": "ETH/USDC",
            "lp_alert_stage": "confirm",
            "trade_action_key": "LONG_CHASE_ALLOWED",
            "asset_market_state_key": "TRADEABLE_LONG",
            "trade_opportunity_status": "CANDIDATE",
            "trade_opportunity_score": 0.71,
            "market_context_source": "live_public",
            "archive_written_at": 1_710_000_000,
        }
        self.assertTrue(sqlite_store.write_signal(record))
        self.assertTrue(sqlite_store.write_signal({**record, "trade_opportunity_score": 0.72}))
        self.assertEqual(1, self._count("signals"))
        self.assertGreaterEqual(self._count("signal_features"), 1)

    def test_raw_parsed_market_context_outcome_and_telegram_writers(self) -> None:
        self.assertTrue(sqlite_store.write_raw_event({"event_id": "evt-raw", "tx_hash": "0xraw", "captured_at": 1}))
        self.assertTrue(sqlite_store.write_parsed_event({"event_id": "evt-parsed", "tx_hash": "0xparsed", "parsed_at": 2}))
        self.assertTrue(
            sqlite_store.write_market_context_snapshot(
                {
                    "signal_id": "sig-mc",
                    "asset_symbol": "ETH",
                    "pair_label": "ETH/USDC",
                    "market_context_source": "live_public",
                    "market_context_venue": "okx",
                    "market_context_resolved_symbol": "ETH-USDT-SWAP",
                    "perp_mark_price": 3200.0,
                }
            )
        )
        self.assertTrue(
            sqlite_store.write_market_context_attempt(
                {
                    "signal_id": "sig-mc",
                    "venue": "okx",
                    "endpoint": "/api/v5/public/mark-price",
                    "symbol": "ETH-USDT-SWAP",
                    "status": "success",
                    "latency_ms": 12.5,
                }
            )
        )
        outcome = {
            "record_id": "sig-outcome",
            "signal_id": "sig-outcome",
            "asset_symbol": "ETH",
            "pair_label": "ETH/USDC",
            "direction_bucket": "buy_pressure",
            "created_at": 10,
            "outcome_windows": {
                "30s": {"status": "completed", "price_source": "okx_mark", "price_start": 100, "price_end": 101, "raw_move_after": 0.01, "direction_adjusted_move_after": 0.01, "followthrough_positive": True, "adverse_by_direction": False, "completed_at": 40},
                "60s": {"status": "unavailable", "price_source": "unavailable", "failure_reason": "price_unavailable", "completed_at": 70},
                "300s": {"status": "expired", "price_source": "okx_mark", "failure_reason": "window_elapsed_without_price_update", "completed_at": 310},
            },
        }
        self.assertTrue(sqlite_store.write_outcome(outcome))
        self.assertTrue(sqlite_store.write_telegram_delivery({"signal_id": "sig-tg", "sent_to_telegram": False, "telegram_suppression_reason": "same_asset_state_repeat"}))
        self.assertEqual(1, self._count("raw_events"))
        self.assertEqual(1, self._count("parsed_events"))
        self.assertEqual(1, self._count("market_context_snapshots"))
        self.assertEqual(1, self._count("market_context_attempts"))
        self.assertEqual(3, self._count("outcomes"))
        self.assertEqual(1, self._count("telegram_deliveries"))
        raw_row = self.conn.execute(
            "SELECT raw_json, payload_mode FROM raw_events WHERE event_id='evt-raw'"
        ).fetchone()
        parsed_row = self.conn.execute(
            "SELECT parsed_json, payload_mode FROM parsed_events WHERE event_id='evt-parsed'"
        ).fetchone()
        attempt_row = self.conn.execute(
            "SELECT payload_hash, payload_mode FROM market_context_attempts WHERE signal_id='sig-mc'"
        ).fetchone()
        telegram_row = self.conn.execute(
            "SELECT message_json, payload_mode FROM telegram_deliveries WHERE signal_id='sig-tg'"
        ).fetchone()
        self.assertIsNone(raw_row["raw_json"])
        self.assertEqual("index_only", raw_row["payload_mode"])
        self.assertIsNone(parsed_row["parsed_json"])
        self.assertEqual("index_only", parsed_row["payload_mode"])
        self.assertTrue(str(attempt_row["payload_hash"] or "").strip())
        self.assertEqual("slim", attempt_row["payload_mode"])
        self.assertIsNone(telegram_row["message_json"])
        self.assertEqual("slim", telegram_row["payload_mode"])

    def test_trade_opportunity_asset_state_and_quality_writers(self) -> None:
        for status in ("CANDIDATE", "VERIFIED", "BLOCKED"):
            self.assertTrue(
                sqlite_store.upsert_trade_opportunity(
                    {
                        "trade_opportunity_id": f"opp-{status.lower()}",
                        "signal_id": f"sig-{status.lower()}",
                        "asset_symbol": "ETH",
                        "pair_label": "ETH/USDC",
                        "opportunity_profile_key": "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
                        "opportunity_profile_version": "v1",
                        "opportunity_profile_side": "LONG",
                        "opportunity_profile_asset": "ETH",
                        "opportunity_profile_pair_family": "USDC",
                        "opportunity_profile_strategy": "lp_continuation",
                        "trade_opportunity_side": "LONG",
                        "trade_opportunity_status": status,
                        "opportunity_raw_score": 0.76,
                        "opportunity_calibrated_score": 0.8,
                        "opportunity_calibration_adjustment": 0.04,
                        "opportunity_calibration_reason": "followthrough>0.65",
                        "opportunity_calibration_sample_count": 60,
                        "opportunity_calibration_confidence": 0.88,
                        "opportunity_calibration_source": "profile",
                        "trade_opportunity_score": 0.8,
                        "trade_opportunity_primary_blocker": "no_trade_lock" if status == "BLOCKED" else "",
                        "trade_opportunity_primary_hard_blocker": "no_trade_lock" if status == "BLOCKED" else "",
                        "trade_opportunity_primary_verification_blocker": "profile_sample_count_insufficient" if status == "CANDIDATE" else "",
                        "opportunity_outcome_30s": "pending",
                        "opportunity_outcome_60s": "completed",
                        "opportunity_outcome_300s": "expired",
                        "opportunity_followthrough_60s": True,
                        "opportunity_adverse_60s": False,
                        "blocker_saved_trade": True if status == "BLOCKED" else None,
                        "blocker_false_block_possible": False if status == "BLOCKED" else None,
                    }
                )
            )
        self.assertTrue(
            sqlite_store.upsert_asset_market_state(
                {
                    "state_id": "state-1",
                    "signal_id": "sig-state",
                    "asset_symbol": "ETH",
                    "previous_asset_market_state_key": "WAIT_CONFIRMATION",
                    "asset_market_state_key": "NO_TRADE_LOCK",
                    "asset_market_state_changed": True,
                    "no_trade_lock_active": True,
                    "no_trade_lock_started_at": 100,
                }
            )
        )
        self.assertTrue(sqlite_store.upsert_quality_stat({"scope_type": "asset", "scope_key": "ETH", "stage": "confirm", "sample_count": 4}))
        self.assertEqual(3, self._count("trade_opportunities"))
        self.assertEqual(9, self._count("opportunity_outcomes"))
        self.assertEqual(1, self._count("asset_market_states"))
        self.assertEqual(1, self._count("no_trade_locks"))
        self.assertEqual(1, self._count("quality_stats"))
        self.assertEqual(
            "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
            self.conn.execute("SELECT opportunity_profile_key FROM trade_opportunities WHERE trade_opportunity_id='opp-candidate'").fetchone()[0],
        )
        row = self.conn.execute(
            "SELECT raw_score, calibrated_score, calibration_adjustment, calibration_source FROM trade_opportunities WHERE trade_opportunity_id='opp-candidate'"
        ).fetchone()
        self.assertEqual((0.76, 0.8, 0.04, "profile"), tuple(row))
        self.assertEqual(
            "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
            self.conn.execute("SELECT opportunity_profile_key FROM opportunity_outcomes WHERE trade_opportunity_id='opp-candidate' AND window_sec=60").fetchone()[0],
        )


if __name__ == "__main__":
    unittest.main()
