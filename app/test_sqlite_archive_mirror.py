import json
import tempfile
import unittest
from pathlib import Path

from archive_store import ArchiveStore
import archive_store as archive_store_module
import sqlite_store


class SQLiteArchiveMirrorTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "chain_monitor.sqlite"
        sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_archive_write_mirrors_to_sqlite(self) -> None:
        archive_dir = self.root / "archive"
        archive = ArchiveStore(base_dir=archive_dir, category_enabled={"signals": True})
        self.assertTrue(
            archive.write_signal(
                {
                    "signal_id": "sig-mirror",
                    "signal_archive_key": "sig-mirror",
                    "asset_symbol": "ETH",
                    "lp_alert_stage": "confirm",
                    "archive_written_at": 1_710_000_000,
                },
                archive_ts=1_710_000_000,
            )
        )
        self.assertTrue(list((archive_dir / "signals").glob("*.ndjson")))
        count = sqlite_store.get_connection().execute("SELECT COUNT(*) FROM signals WHERE signal_id='sig-mirror'").fetchone()[0]
        self.assertEqual(1, count)

    def test_sqlite_disabled_still_writes_archive(self) -> None:
        archive_dir = self.root / "archive-disabled"
        original = archive_store_module.SQLITE_ARCHIVE_MIRROR_ENABLE
        archive_store_module.SQLITE_ARCHIVE_MIRROR_ENABLE = False
        try:
            archive = ArchiveStore(base_dir=archive_dir, category_enabled={"raw_events": True})
            self.assertTrue(archive.write_raw_event({"event_id": "raw-disabled", "tx_hash": "0x1"}, archive_ts=1_710_000_001))
        finally:
            archive_store_module.SQLITE_ARCHIVE_MIRROR_ENABLE = original
        self.assertTrue(list((archive_dir / "raw_events").glob("*.ndjson")))

    def test_sqlite_failure_does_not_block_archive(self) -> None:
        archive_dir = self.root / "archive-failure"
        original = sqlite_store.write_raw_event

        def boom(_payload):
            raise RuntimeError("forced sqlite failure")

        sqlite_store.write_raw_event = boom
        try:
            archive = ArchiveStore(base_dir=archive_dir, category_enabled={"raw_events": True})
            self.assertTrue(archive.write_raw_event({"event_id": "raw-failure", "tx_hash": "0x2"}, archive_ts=1_710_000_002))
        finally:
            sqlite_store.write_raw_event = original
        path = next((archive_dir / "raw_events").glob("*.ndjson"))
        self.assertEqual("raw-failure", json.loads(path.read_text(encoding="utf-8").splitlines()[0])["data"]["event_id"])


if __name__ == "__main__":
    unittest.main()
