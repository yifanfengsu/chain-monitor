import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteConnectionPragmaTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.old_busy = os.environ.get("SQLITE_BUSY_TIMEOUT_MS")
        self.old_connect = os.environ.get("SQLITE_CONNECT_TIMEOUT_SEC")
        os.environ["SQLITE_BUSY_TIMEOUT_MS"] = "30000"
        os.environ["SQLITE_CONNECT_TIMEOUT_SEC"] = "30"

    def tearDown(self) -> None:
        sqlite_store.close()
        if self.old_busy is None:
            os.environ.pop("SQLITE_BUSY_TIMEOUT_MS", None)
        else:
            os.environ["SQLITE_BUSY_TIMEOUT_MS"] = self.old_busy
        if self.old_connect is None:
            os.environ.pop("SQLITE_CONNECT_TIMEOUT_SEC", None)
        else:
            os.environ["SQLITE_CONNECT_TIMEOUT_SEC"] = self.old_connect
        self.temp_dir.cleanup()

    def test_write_connection_sets_wal_busy_timeout_and_normal_sync(self) -> None:
        conn = sqlite_store.open_sqlite_connection(self.db_path, readonly=False, row_factory=True)
        try:
            journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            busy_timeout = int(conn.execute("PRAGMA busy_timeout").fetchone()[0] or 0)
            synchronous = int(conn.execute("PRAGMA synchronous").fetchone()[0] or 0)
            temp_store = int(conn.execute("PRAGMA temp_store").fetchone()[0] or 0)
            foreign_keys = int(conn.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
            self.assertEqual("wal", journal_mode)
            self.assertGreaterEqual(busy_timeout, 30000)
            self.assertEqual(1, synchronous)
            self.assertEqual(2, temp_store)
            self.assertEqual(1, foreign_keys)
        finally:
            conn.close()

    def test_readonly_connection_does_not_force_wal(self) -> None:
        raw = sqlite3.connect(self.db_path)
        try:
            raw.execute("CREATE TABLE fixture(id INTEGER PRIMARY KEY)")
            raw.commit()
        finally:
            raw.close()

        conn = sqlite_store.open_sqlite_connection(self.db_path, readonly=True, row_factory=True)
        try:
            journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            busy_timeout = int(conn.execute("PRAGMA busy_timeout").fetchone()[0] or 0)
            self.assertNotEqual("wal", journal_mode)
            self.assertGreaterEqual(busy_timeout, 30000)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
