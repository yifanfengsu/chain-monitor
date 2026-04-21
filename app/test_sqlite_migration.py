import json
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.archive_dir = self.root / "archive"
        self.original_archive_base = sqlite_store.ARCHIVE_BASE_DIR
        self.original_project_root = sqlite_store.PROJECT_ROOT
        sqlite_store.ARCHIVE_BASE_DIR = str(self.archive_dir)
        sqlite_store.PROJECT_ROOT = self.root
        (self.root / "data").mkdir(parents=True, exist_ok=True)
        sqlite_store.init_sqlite_store(self.root / "chain_monitor.sqlite")

    def tearDown(self) -> None:
        sqlite_store.ARCHIVE_BASE_DIR = self.original_archive_base
        sqlite_store.PROJECT_ROOT = self.original_project_root
        sqlite_store.close()
        self.temp_dir.cleanup()

    def _write_ndjson(self, category: str, date: str, rows: list[dict], *, bad_line: bool = False) -> None:
        path = self.archive_dir / category / f"{date}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            if bad_line:
                handle.write("{bad json\n")

    def test_migrate_archive_imports_and_is_repeatable(self) -> None:
        date = "2026-04-21"
        self._write_ndjson(
            "signals",
            date,
            [
                {
                    "archive_ts": 1_710_000_000,
                    "data": {
                        "signal_id": "sig-migrate",
                        "signal_archive_key": "sig-migrate",
                        "asset_symbol": "ETH",
                        "lp_alert_stage": "confirm",
                    },
                }
            ],
            bad_line=True,
        )
        self._write_ndjson(
            "raw_events",
            date,
            [{"archive_ts": 1_710_000_000, "data": {"event_id": "raw-migrate", "tx_hash": "0xmigrate"}}],
        )
        first = sqlite_store.migrate_archive(date=date)
        second = sqlite_store.migrate_archive(date=date)
        self.assertGreaterEqual(first["imported_rows"], 2)
        self.assertEqual(1, first["bad_row_count"])
        conn = sqlite_store.get_connection()
        self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM signals WHERE signal_id='sig-migrate'").fetchone()[0])
        self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM raw_events WHERE event_id='raw-migrate'").fetchone()[0])
        self.assertGreaterEqual(second["imported_rows"], 2)
        self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM signals WHERE signal_id='sig-migrate'").fetchone()[0])

    def test_cache_snapshot_migration_imports_quality_and_opportunity(self) -> None:
        data_dir = self.root / "data"
        (data_dir / "trade_opportunities.cache.json").write_text(
            json.dumps(
                {
                    "generated_at": 1_710_000_000,
                    "opportunities": [
                        {
                            "trade_opportunity_id": "opp-cache",
                            "signal_id": "sig-cache",
                            "asset_symbol": "ETH",
                            "trade_opportunity_status": "CANDIDATE",
                            "trade_opportunity_side": "LONG",
                            "opportunity_outcome_60s": "pending",
                        }
                    ],
                    "rolling_stats": {"opportunity_candidate_count": 1},
                }
            ),
            encoding="utf-8",
        )
        (data_dir / "lp_quality_stats.cache.json").write_text(
            json.dumps(
                {
                    "generated_at": 1_710_000_001,
                    "records": [
                        {
                            "record_id": "sig-quality-cache",
                            "signal_id": "sig-quality-cache",
                            "lp_alert_stage": "confirm",
                            "outcome_windows": {
                                "60s": {"status": "pending", "window_sec": 60}
                            },
                        }
                    ],
                    "summary": {"dimensions": {"asset": [{"key": "ETH", "sample_size": 1}]}},
                }
            ),
            encoding="utf-8",
        )
        payload = sqlite_store.migrate_cache_snapshots()
        self.assertGreaterEqual(payload["imported_rows"], 3)
        conn = sqlite_store.get_connection()
        self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM trade_opportunities WHERE trade_opportunity_id='opp-cache'").fetchone()[0])
        self.assertGreaterEqual(conn.execute("SELECT COUNT(*) FROM quality_stats").fetchone()[0], 2)

    def test_migrate_archive_all_dates(self) -> None:
        self._write_ndjson(
            "parsed_events",
            "2026-04-20",
            [{"archive_ts": 1_710_000_000, "data": {"event_id": "parsed-1", "tx_hash": "0x1"}}],
        )
        self._write_ndjson(
            "parsed_events",
            "2026-04-21",
            [{"archive_ts": 1_710_000_100, "data": {"event_id": "parsed-2", "tx_hash": "0x2"}}],
        )
        payload = sqlite_store.migrate_archive(all_dates=True)
        self.assertGreaterEqual(payload["imported_rows"], 2)
        count = sqlite_store.get_connection().execute("SELECT COUNT(*) FROM parsed_events").fetchone()[0]
        self.assertEqual(2, count)


if __name__ == "__main__":
    unittest.main()
