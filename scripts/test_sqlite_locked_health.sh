#!/usr/bin/env bash
set -euo pipefail

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sqlite_locked_health.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

PYTHON_BIN="${PYTHON_BIN:-./venv/bin/python}"
DB_PATH="$TMP_DIR/chain_monitor.sqlite"

PYTHONPATH="app${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON_BIN" - "$DB_PATH" >"$TMP_DIR/retry.out" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys
from unittest import mock

import sqlite_store


class FlakyConnection:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, _sql: str, _params: tuple = ()) -> object:
        self.calls += 1
        if self.calls == 1:
            raise sqlite3.OperationalError("database is locked")
        return object()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


db_path = sys.argv[1]
sqlite_store.close()
sqlite_store.init_sqlite_store(db_path)
sqlite_store.reset_sqlite_write_health()
with mock.patch.object(sqlite_store.time, "sleep", return_value=None):
    sqlite_store.execute_with_retry(
        FlakyConnection(),  # type: ignore[arg-type]
        "INSERT INTO fixture(id) VALUES(?)",
        (1,),
        op_name="locked_health_fixture",
        table_name="fixture",
        max_attempts=2,
    )
print(json.dumps(sqlite_store.get_sqlite_write_health(), sort_keys=True))
PY

grep -Fq '"busy_retries": 1' "$TMP_DIR/retry.out"
grep -Fq '"last_locked_op": "locked_health_fixture"' "$TMP_DIR/retry.out"
if grep -Fq "sqlite_store warning: database is locked" "$TMP_DIR/retry.out"; then
  echo "unexpected bare sqlite locked warning in retry output" >&2
  exit 1
fi
grep -Fq "sqlite_write_busy_retry op_name=locked_health_fixture table_name=fixture" "$TMP_DIR/retry.out"

HERMES_LISTENER_HEALTH_DB_PATH="$DB_PATH" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes.lock" \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
  ./scripts/hermes_cm_ops.sh listener-health >"$TMP_DIR/listener.out"

grep -Fq "sqlite_write_health:" "$TMP_DIR/listener.out"
grep -Fq "journal_mode=" "$TMP_DIR/listener.out"
grep -Fq "busy_timeout_ms=" "$TMP_DIR/listener.out"
if grep -Fq "sqlite_store warning: database is locked" "$TMP_DIR/listener.out"; then
  echo "unexpected bare sqlite locked warning in listener health output" >&2
  exit 1
fi

echo "sqlite locked health test ok"
