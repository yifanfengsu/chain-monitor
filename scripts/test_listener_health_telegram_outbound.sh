#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/listener_health_tg_outbound.XXXXXX")"

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
    sed -n '1,120p' "$TMP_DIR/out.txt" >&2 || true
    fail "listener-health output missing pattern: ${pattern}"
  fi
}

forbid_out() {
  local pattern="$1"
  if grep -Fq -- "$pattern" "$TMP_DIR/out.txt"; then
    sed -n '1,120p' "$TMP_DIR/out.txt" >&2 || true
    fail "listener-health output contains forbidden pattern: ${pattern}"
  fi
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
        sent_to_telegram INTEGER,
        notifier_sent_at REAL
    );
    CREATE TABLE delivery_audit (
        audit_id TEXT PRIMARY KEY,
        signal_id TEXT,
        reason TEXT,
        stage TEXT,
        sent_to_telegram INTEGER,
        delivered INTEGER,
        notifier_sent_at REAL,
        timestamp REAL,
        created_at REAL,
        audit_json TEXT
    );
    CREATE TABLE telegram_deliveries (
        telegram_delivery_id TEXT PRIMARY KEY,
        signal_id TEXT,
        sent INTEGER,
        sent_at REAL,
        suppressed INTEGER,
        created_at REAL
    );
    """
)
conn.execute(
    "INSERT INTO signals(signal_id, timestamp, created_at, sent_to_telegram) VALUES (?, ?, ?, ?)",
    ("sig-fail", now - 600, now - 600, 0),
)
conn.execute(
    """
    INSERT INTO delivery_audit(
        audit_id, signal_id, reason, stage, sent_to_telegram, delivered,
        notifier_sent_at, timestamp, created_at, audit_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        "audit-fail",
        "sig-fail",
        "notifier_send_failed",
        "notifier_delivery",
        0,
        0,
        now - 500,
        now - 500,
        now - 500,
        '{"error_type":"TimedOut","detail":"Pool timeout"}',
    ),
)
conn.execute(
    """
    INSERT INTO delivery_audit(
        audit_id, signal_id, reason, stage, sent_to_telegram, delivered,
        notifier_sent_at, timestamp, created_at, audit_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        "audit-old-success",
        "sig-old-success",
        "notifier_delivered",
        "notifier_delivery",
        1,
        1,
        now - 10800,
        now - 10800,
        now - 10800,
        "{}",
    ),
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

require_out "Telegram outbound"
require_out "最近30分钟：signals=1"
require_out "最近2小时：signals=1"
require_out "sent_ok=0"
require_out "sent_failed=1"
require_out "sent_to_telegram=1数量=0"
require_out "sent_to_telegram=0数量=1"
require_out "notifier_send_failed=1"
require_out "最近成功 Telegram 发送时间:"
require_out "最近失败时间:"
require_out "notifier.get_notifier_health"
require_out "Telegram outbound 可能异常"
require_out "pool timeout / NetworkError / TimedOut"

forbid_out "TELEGRAM_BOT_TOKEN"
forbid_out "chat_id="
forbid_out "raw message"
forbid_out "All connections occupied"

echo "listener-health Telegram outbound test ok"
