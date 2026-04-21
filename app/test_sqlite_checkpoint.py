import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import sqlite_store


class SQLiteCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.close()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "chain_monitor.sqlite"

    def tearDown(self) -> None:
        sqlite_store.close()
        self.temp_dir.cleanup()

    def test_checkpoint_runs_after_db_init(self) -> None:
        sqlite_store.init_sqlite_store(self.db_path)

        payload = sqlite_store.checkpoint()

        self.assertTrue(payload["success"])
        self.assertEqual(str(self.db_path), payload["db_path"])
        self.assertIn("checkpoint_result", payload)

    def test_checkpoint_missing_db_is_graceful(self) -> None:
        missing = self.root / "missing.sqlite"

        payload = sqlite_store.checkpoint(missing)

        self.assertTrue(payload["success"])
        self.assertFalse(payload["db_exists"])
        self.assertEqual("db_not_found", payload["message"])

    def test_checkpoint_with_wal_file(self) -> None:
        conn = sqlite_store.init_sqlite_store(self.db_path)
        self.assertIsNotNone(conn)
        conn.execute("INSERT OR REPLACE INTO schema_meta(key, value, updated_at) VALUES('checkpoint-test', '1', 1)")
        conn.commit()
        wal_path = Path(f"{self.db_path}-wal")

        payload = sqlite_store.checkpoint()

        self.assertTrue(payload["success"])
        self.assertEqual(str(self.db_path), payload["db_path"])
        self.assertIn("wal_exists_before", payload)
        self.assertIn("wal_size_after", payload)
        self.assertIn("checkpoint_result", payload)
        self.assertIsInstance(wal_path.exists(), bool)

    def test_checkpoint_cli_outputs_checkpoint_result(self) -> None:
        sqlite_store.init_sqlite_store(self.db_path)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = sqlite_store.main(["--checkpoint"])

        self.assertEqual(0, code)
        payload = json.loads(buffer.getvalue())
        self.assertIn("checkpoint_result", payload)
        self.assertTrue(payload["success"])


if __name__ == "__main__":
    unittest.main()
