import concurrent.futures
import tempfile
import threading
import time
import unittest
from pathlib import Path

import sqlite_store


class SQLiteWriteLockingTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        sqlite_store.reset_sqlite_write_health()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        sqlite_store.reset_sqlite_write_health()
        self.temp_dir.cleanup()

    def _count_raw_events(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()
        return int(row[0] or 0)

    def test_concurrent_writers_are_serialized_in_process(self) -> None:
        def write_one(index: int) -> bool:
            return sqlite_store.write_raw_event(
                {
                    "event_id": f"evt-{index}",
                    "tx_hash": f"0x{index:064x}",
                    "captured_at": 1_800_000_000 + index,
                }
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(write_one, range(80)))

        self.assertTrue(all(results))
        self.assertEqual(80, self._count_raw_events())
        self.assertEqual(0, sqlite_store.get_sqlite_write_health()["locked_failures"])

    def test_write_waits_for_module_write_lock(self) -> None:
        sqlite_store._SQLITE_WRITE_LOCK.acquire()
        started = threading.Event()

        def write_blocked() -> bool:
            started.set()
            return sqlite_store.write_raw_event(
                {
                    "event_id": "evt-blocked",
                    "tx_hash": "0x" + "1" * 64,
                    "captured_at": 1_800_000_100,
                }
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(write_blocked)
            try:
                self.assertTrue(started.wait(timeout=1.0))
                time.sleep(0.05)
                self.assertFalse(future.done())
            finally:
                sqlite_store._SQLITE_WRITE_LOCK.release()
            self.assertTrue(future.result(timeout=2.0))


if __name__ == "__main__":
    unittest.main()
