import sqlite3
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "chain_monitor.sqlite"
        self.conn = sqlite_store.init_sqlite_store(self.db_path)

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_init_creates_required_tables_and_schema_version(self) -> None:
        self.assertIsNotNone(self.conn)
        tables = {
            row[0]
            for row in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for table in sqlite_store.REQUIRED_TABLES:
            self.assertIn(table, tables)
        row = self.conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        self.assertEqual(sqlite_store.SQLITE_SCHEMA_VERSION, row[0])

    def test_key_indexes_exist_or_queries_work(self) -> None:
        index_names = {
            row[1]
            for row in self.conn.execute("PRAGMA index_list('signals')").fetchall()
        }
        self.assertIn("idx_signals_event_id", index_names)
        self.conn.execute("SELECT * FROM signals WHERE signal_id=?", ("missing",)).fetchall()
        self.conn.execute("SELECT * FROM outcomes WHERE signal_id=? AND window_sec=?", ("missing", 60)).fetchall()


if __name__ == "__main__":
    unittest.main()
