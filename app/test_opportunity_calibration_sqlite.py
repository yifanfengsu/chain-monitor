import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import quality_reports
import sqlite_store
from opportunity_calibration import build_calibration_quality_stats
from trade_opportunity import TradeOpportunityManager
from trade_opportunity_test_helpers import StubStateManager, make_event, make_outcome_record, make_signal


class OpportunityCalibrationSQLiteTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_calibration_scope_rows_can_be_written(self) -> None:
        rows = build_calibration_quality_stats(
            [
                {
                    "profile_key": "ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
                    "profile_version": "v1",
                    "side": "LONG",
                    "asset": "ETH",
                    "pair_family": "USDC",
                    "strategy": "lp_continuation",
                    "sample_count": 60,
                    "completed_60s": 54,
                    "followthrough_60s_count": 42,
                    "adverse_60s_count": 8,
                    "blocked_completed_60s": 10,
                    "blocker_saved_trade_60s_count": 5,
                    "blocker_false_block_60s_count": 2,
                }
            ],
            profile_key="ETH|LONG|broader_confirm|confirm|confirming|no_absorption|major|basis_normal|quality_high",
            asset="ETH",
            side="LONG",
            pair_family="USDC",
            updated_at=1_710_000_000,
        )

        for row in rows:
            self.assertTrue(sqlite_store.upsert_quality_stat(row))

        count = int(
            self.conn.execute("SELECT COUNT(*) FROM quality_stats WHERE scope_type='opportunity_calibration'").fetchone()[0]
        )
        self.assertEqual(4, count)

    def test_manager_writes_calibration_snapshot_and_cli_reads_it(self) -> None:
        event = make_event(ts=1_710_000_220)
        record_id = f"outcome:{event.intent_type}:{event.ts}"
        state = StubStateManager(outcome_records={record_id: make_outcome_record(record_id=record_id)})
        manager = TradeOpportunityManager(state_manager=state, persistence_enabled=False)
        payload = manager.apply_lp_signal(event, make_signal(event))

        row = self.conn.execute(
            "SELECT raw_score, score, calibrated_score, calibration_adjustment, calibration_reason, calibration_source "
            "FROM trade_opportunities WHERE trade_opportunity_id=?",
            (payload["trade_opportunity_id"],),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(payload["opportunity_raw_score"], float(row[0] or 0.0), places=4)
        self.assertAlmostEqual(payload["trade_opportunity_score"], float(row[1] or 0.0), places=4)
        self.assertAlmostEqual(payload["opportunity_calibrated_score"], float(row[2] or 0.0), places=4)
        self.assertAlmostEqual(payload["opportunity_calibration_adjustment"], float(row[3] or 0.0), places=4)
        self.assertEqual(payload["opportunity_calibration_reason"], row[4] or "")
        self.assertEqual(payload["opportunity_calibration_source"], row[5] or "")

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = quality_reports.main(["--opportunity-calibration"])
        self.assertEqual(0, code)
        cli_payload = json.loads(buffer.getvalue())
        self.assertIn("profiles_calibrated_count", cli_payload)


if __name__ == "__main__":
    unittest.main()
