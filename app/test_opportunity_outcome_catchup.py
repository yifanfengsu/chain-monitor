from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from opportunity_outcome_catchup import logical_window, opportunity_outcome_catchup


class OpportunityOutcomeCatchupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "catchup.sqlite"
        self.logical_date = "2099-12-28"
        self.start_ts, _ = logical_window(self.logical_date)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE trade_opportunities (
                    trade_opportunity_id TEXT PRIMARY KEY,
                    signal_id TEXT,
                    created_at REAL
                );
                CREATE TABLE opportunity_outcomes (
                    trade_opportunity_id TEXT,
                    window_sec INTEGER,
                    due_at REAL,
                    start_price REAL,
                    end_price REAL,
                    status TEXT,
                    failure_reason TEXT,
                    completed_at REAL,
                    created_at REAL,
                    updated_at REAL,
                    settled_by TEXT,
                    catchup INTEGER,
                    result_label TEXT,
                    price_source TEXT,
                    outcome_price_source TEXT,
                    direction_adjusted_move REAL,
                    PRIMARY KEY(trade_opportunity_id, window_sec)
                );
                CREATE TABLE trade_replay_examples (
                    replay_id TEXT PRIMARY KEY,
                    logical_date TEXT,
                    replay_scope TEXT,
                    signal_id TEXT,
                    trade_opportunity_id TEXT,
                    asset TEXT,
                    pair TEXT,
                    opportunity_status TEXT,
                    entry_ts INTEGER,
                    exit_ts INTEGER,
                    entry_price REAL,
                    exit_price REAL,
                    net_pnl_bps REAL,
                    label TEXT,
                    price_source TEXT,
                    data_valid INTEGER
                );
                """
            )
            for idx in range(1, 4):
                conn.execute(
                    "INSERT INTO trade_opportunities VALUES (?, ?, ?)",
                    (f"opp-{idx}", f"sig-{idx}", self.start_ts + idx),
                )
                conn.execute(
                    "INSERT INTO opportunity_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        f"opp-{idx}",
                        60,
                        self.start_ts + 60,
                        100.0,
                        None,
                        "pending",
                        "",
                        None,
                        self.start_ts + idx,
                        self.start_ts + idx,
                        "",
                        0,
                        "",
                        "",
                        "",
                        None,
                    ),
                )
            replay_rows = [
                ("replay-1", self.logical_date, "full", "sig-1", "opp-1", "ETH", "ETH/USDT", "BLOCKED", self.start_ts + 5, self.start_ts + 65, 100.0, 101.2, 98.0, "clean_followthrough", "fixture", 1),
                ("replay-2", self.logical_date, "full", "sig-2", "opp-2", "ETH", "ETH/USDT", "BLOCKED", self.start_ts + 6, self.start_ts + 66, 100.0, 99.5, -72.0, "bad_entry", "fixture", 1),
            ]
            conn.executemany("INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", replay_rows)
            conn.commit()
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_reports_would_update_without_writing(self) -> None:
        result = opportunity_outcome_catchup(
            self.db_path,
            self.logical_date,
            dry_run=True,
            now_ts=self.start_ts + 1000,
        )

        self.assertEqual(3, result["pending_rows"])
        self.assertEqual(3, result["past_due_pending_count"])
        self.assertEqual(2, result["would_update_rows"])
        self.assertEqual(0, result["updated_count"])
        conn = sqlite3.connect(self.db_path)
        try:
            pending = conn.execute("SELECT COUNT(*) FROM opportunity_outcomes WHERE status='pending'").fetchone()[0]
            cols = {row[1] for row in conn.execute("PRAGMA table_info(opportunity_outcomes)").fetchall()}
        finally:
            conn.close()
        self.assertEqual(3, pending)
        self.assertNotIn("net_pnl_bps", cols)

    def test_execute_completes_replay_matches_and_records_unresolved_reason(self) -> None:
        result = opportunity_outcome_catchup(
            self.db_path,
            self.logical_date,
            dry_run=False,
            now_ts=self.start_ts + 1000,
        )

        self.assertEqual(2, result["updated_count"])
        self.assertEqual(2, result["completed_from_replay"])
        self.assertEqual(1, result["still_pending_count"])
        self.assertEqual({"missing_price_snapshot": 1}, result["unresolved_reason_distribution"])
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = {
                row["trade_opportunity_id"]: dict(row)
                for row in conn.execute(
                    "SELECT trade_opportunity_id, status, source, settled_by, net_pnl_bps, failure_reason FROM opportunity_outcomes"
                )
            }
        finally:
            conn.close()
        self.assertEqual("completed", rows["opp-1"]["status"])
        self.assertEqual("catchup_from_replay", rows["opp-1"]["source"])
        self.assertEqual("opportunity_outcome_catchup", rows["opp-1"]["settled_by"])
        self.assertEqual(98.0, rows["opp-1"]["net_pnl_bps"])
        self.assertEqual("completed", rows["opp-2"]["status"])
        self.assertEqual(-72.0, rows["opp-2"]["net_pnl_bps"])
        self.assertEqual("pending", rows["opp-3"]["status"])
        self.assertEqual("missing_price_snapshot", rows["opp-3"]["failure_reason"])


if __name__ == "__main__":
    unittest.main()
