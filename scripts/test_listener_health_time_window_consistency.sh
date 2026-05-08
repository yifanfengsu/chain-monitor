#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/listener_health_time_window.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local pattern="$1"
  if ! grep -Fq -- "$pattern" "$TMP_DIR/out.txt"; then
    sed -n '1,180p' "$TMP_DIR/out.txt" >&2 || true
    fail "listener-health output missing pattern: ${pattern}"
  fi
}

forbid_out() {
  local pattern="$1"
  if grep -Fq -- "$pattern" "$TMP_DIR/out.txt"; then
    sed -n '1,180p' "$TMP_DIR/out.txt" >&2 || true
    fail "listener-health output contains forbidden pattern: ${pattern}"
  fi
}

extract_metric() {
  local label="$1"
  local key="$2"
  local line value
  line="$(grep -F "${label}：" "$TMP_DIR/out.txt" | tail -n 1 || true)"
  [[ -n "$line" ]] || fail "missing window line: ${label}"
  value="$(sed -n "s/.* ${key}=\([^ ]*\).*/\1/p" <<<"$line")"
  if [[ -z "$value" ]]; then
    value="$(sed -n "s/^${label}：${key}=\([^ ]*\).*/\1/p" <<<"$line")"
  fi
  [[ -n "$value" ]] || fail "missing metric ${key} in ${label}: ${line}"
  printf '%s\n' "$value"
}

PYTHON="${REPO_ROOT}/venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3 || true)"
fi
[[ -n "$PYTHON" ]] || fail "python3 or ./venv/bin/python is required"

DB_PATH="$TMP_DIR/chain_monitor.sqlite"
"$PYTHON" - "$DB_PATH" <<'PY'
from __future__ import annotations

import sqlite3
import sys
import time

db_path = sys.argv[1]
now = time.time()
conn = sqlite3.connect(db_path)
conn.executescript(
    """
    CREATE TABLE signals (
        signal_id TEXT PRIMARY KEY,
        timestamp REAL,
        created_at REAL,
        updated_at REAL
    );
    CREATE TABLE delivery_audit (
        audit_id TEXT PRIMARY KEY,
        signal_id TEXT,
        stage TEXT,
        gate_reason TEXT,
        delivered INTEGER,
        notifier_sent_at REAL,
        telegram_update_kind TEXT,
        suppression_reason TEXT,
        audit_json TEXT,
        archive_written_at REAL,
        created_at REAL,
        updated_at REAL,
        delivery_decision TEXT,
        reason TEXT,
        sent_to_telegram INTEGER,
        suppressed INTEGER,
        timestamp REAL,
        drop_reason TEXT
    );
    CREATE TABLE telegram_deliveries (
        telegram_delivery_id TEXT PRIMARY KEY,
        signal_id TEXT,
        sent INTEGER,
        sent_at REAL,
        suppressed INTEGER,
        suppression_reason TEXT,
        message_json TEXT,
        created_at REAL
    );
    """
)

for i in range(5):
    ts = now - 900 - i
    conn.execute("INSERT INTO signals VALUES (?, ?, ?, ?)", (f"recent-sig-{i}", ts, ts, ts))

for i in range(100):
    old_ts = now - 6 * 3600 - i
    conn.execute("INSERT INTO signals VALUES (?, ?, ?, ?)", (f"old-sig-{i}", old_ts, old_ts, old_ts))

for i in range(10):
    ts = now - 900 - i
    conn.execute(
        """
        INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"recent-audit-{i}",
            f"recent-sig-{i % 5}",
            "delivery_policy",
            "",
            0,
            None,
            "",
            "delivery_policy",
            "{}",
            ts,
            ts,
            ts,
            "delivery_policy",
            "delivery_policy",
            0,
            1,
            ts,
            "",
        ),
    )

for i in range(200):
    old_archive_ts = now - 6 * 3600 - i
    misleading_recent_timestamp = now - 600 - (i % 60)
    conn.execute(
        """
        INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"old-audit-{i}",
            f"old-sig-{i % 100}",
            "delivery_policy",
            "",
            0,
            None,
            "",
            "delivery_policy",
            "{}",
            old_archive_ts,
            old_archive_ts,
            old_archive_ts,
            "delivery_policy",
            "delivery_policy",
            0,
            1,
            misleading_recent_timestamp,
            "",
        ),
    )

for i in range(3):
    ts = now - 800 - i
    conn.execute(
        "INSERT INTO telegram_deliveries VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (f"tg-{i}", f"recent-sig-{i}", 1 if i == 0 else 0, ts if i == 0 else None, 1 if i else 0, "delivery_policy" if i else "", "{}", ts),
    )

conn.commit()
conn.close()
PY

HERMES_LISTENER_HEALTH_DB_PATH="$DB_PATH" \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes.lock" \
HERMES_OPS_MAX_CMD_BYTES=120000 \
HERMES_OPS_MAX_CMD_LINES=800 \
  "$SCRIPT" listener-health >"$TMP_DIR/out.txt"

require_out "最近2小时：signals=5"
require_out "signals_count=5"
require_out "delivery_audit_rows=10"
require_out "telegram_deliveries_rows=3"
require_out "telegram_deliveries_sent_ok=1"
require_out "telegram_deliveries_send_failed=0"
require_out "delivery_audit_time_field=archive_written_at"
require_out "signals_time_field=timestamp"
require_out "telegram_deliveries_time_field=created_at"
require_out "window_start_ts="
require_out "window_start_bj="
require_out "delivery_audit_latest_bj="
require_out "latest_row_bj="
require_out "source=sqlite"
require_out "SQLite 最新逻辑日期: unavailable reason=no_logical_date_column"
require_out "fallback_latest_table_time="
require_out "fallback_time_field="
forbid_out "SQLite 最新 logical_date: missing_time_column"
forbid_out "missing_time_column"
forbid_out "no such column"

rows_2h="$(extract_metric "最近2小时" "delivery_audit_rows")"
signals_2h="$(extract_metric "最近2小时" "signals_count")"
failed_2h="$(extract_metric "最近2小时" "sent_failed")"
[[ "$rows_2h" == "10" ]] || fail "2h delivery_audit_rows mixed in non-window rows: ${rows_2h}"
[[ "$signals_2h" == "5" ]] || fail "2h signals_count mixed in non-window rows: ${signals_2h}"
if (( failed_2h > rows_2h )); then
  sed -n '1,180p' "$TMP_DIR/out.txt" >&2 || true
  fail "sent_failed exceeded delivery_audit_rows: ${failed_2h} > ${rows_2h}"
fi

echo "listener-health time window consistency test ok"
