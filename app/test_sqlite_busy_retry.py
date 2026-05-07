import sqlite3
import unittest
from unittest import mock

import sqlite_store


class _FlakyConnection:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.execute_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0

    def execute(self, _sql: str, _params: tuple = ()) -> object:
        self.execute_calls += 1
        if self.execute_calls <= self.failures:
            raise sqlite3.OperationalError("database is locked")
        return object()

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


class SQLiteBusyRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        sqlite_store.reset_sqlite_write_health()

    def tearDown(self) -> None:
        sqlite_store.reset_sqlite_write_health()

    def test_locked_write_retries_then_succeeds_and_updates_stats(self) -> None:
        conn = _FlakyConnection(failures=2)
        with mock.patch.object(sqlite_store.time, "sleep", return_value=None):
            ok = sqlite_store.execute_with_retry(
                conn,  # type: ignore[arg-type]
                "INSERT INTO fixture(id) VALUES(?)",
                (1,),
                op_name="fixture_insert",
                table_name="fixture",
                max_attempts=4,
            )

        self.assertTrue(ok)
        self.assertEqual(3, conn.execute_calls)
        self.assertEqual(1, conn.commit_calls)
        self.assertEqual(2, conn.rollback_calls)
        health = sqlite_store.get_sqlite_write_health()
        self.assertEqual(3, health["write_attempts"])
        self.assertEqual(1, health["write_success"])
        self.assertEqual(2, health["busy_retries"])
        self.assertEqual(0, health["locked_failures"])
        self.assertEqual("fixture_insert", health["last_locked_op"])
        self.assertEqual("fixture", health["last_locked_table"])
        self.assertEqual(2, health["max_retry_attempt_seen"])


if __name__ == "__main__":
    unittest.main()
