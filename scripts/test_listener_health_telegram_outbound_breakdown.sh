#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/listener_health_tg_breakdown.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local file="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$file"; then
    sed -n '1,160p' "$file" >&2 || true
    fail "listener-health output missing pattern: ${pattern}"
  fi
}

forbid_out() {
  local file="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$file"; then
    sed -n '1,160p' "$file" >&2 || true
    fail "listener-health output contains forbidden pattern: ${pattern}"
  fi
}

extract_metric() {
  local file="$1"
  local label="$2"
  local key="$3"
  local line value
  line="$(grep -F "${label}：" "$file" | tail -n 1 || true)"
  [[ -n "$line" ]] || fail "missing window line: ${label}"
  value="$(sed -n "s/.* ${key}=\([^ ]*\).*/\1/p" <<<"$line")"
  if [[ -z "$value" ]]; then
    value="$(sed -n "s/^${label}：${key}=\([^ ]*\).*/\1/p" <<<"$line")"
  fi
  [[ -n "$value" ]] || fail "missing metric ${key} in ${label}: ${line}"
  printf '%s\n' "$value"
}

run_health() {
  local db_path="$1"
  local out_path="$2"
  HERMES_LISTENER_HEALTH_DB_PATH="$db_path" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes.lock" \
  HERMES_OPS_MAX_CMD_BYTES=120000 \
  HERMES_OPS_MAX_CMD_LINES=800 \
    "$SCRIPT" listener-health >"$out_path"
}

PYTHON="${REPO_ROOT}/venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3 || true)"
fi
[[ -n "$PYTHON" ]] || fail "python3 or ./venv/bin/python is required"

POLICY_DB="$TMP_DIR/policy.sqlite"
"$PYTHON" - "$POLICY_DB" <<'PY'
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
        created_at REAL
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
    """
)
for i in range(100):
    ts = now - 600 - i
    conn.execute("INSERT INTO signals VALUES (?, ?, ?)", (f"sig-{i}", ts, ts))
    if i < 2:
        conn.execute(
            """
            INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"audit-ok-{i}",
                f"sig-{i}",
                "notifier_delivery",
                "",
                1,
                ts,
                "message",
                "",
                "{}",
                ts,
                ts,
                ts,
                "notifier_delivered",
                "notifier_delivered",
                1,
                0,
                ts,
                "",
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"audit-policy-{i}",
                f"sig-{i}",
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
conn.commit()
conn.close()
PY

POLICY_OUT="$TMP_DIR/policy.out"
run_health "$POLICY_DB" "$POLICY_OUT"
require_out "$POLICY_OUT" "最近2小时：signals=100"
require_out "$POLICY_OUT" "delivery_audit_rows=100"
require_out "$POLICY_OUT" "sent_ok=2"
require_out "$POLICY_OUT" "send_failed=0"
require_out "$POLICY_OUT" "sent_failed=0"
require_out "$POLICY_OUT" "send_attempted=2"
require_out "$POLICY_OUT" "suppressed_or_not_eligible=98"
require_out "$POLICY_OUT" "delivery_policy_dropped=98"
require_out "$POLICY_OUT" "source=sqlite"
require_out "$POLICY_OUT" "delivery_audit_time_field=archive_written_at"
require_out "$POLICY_OUT" "window_start_bj="
require_out "$POLICY_OUT" "delivery_audit_latest_bj="
require_out "$POLICY_OUT" "Telegram outbound 状态: 正常"
forbid_out "$POLICY_OUT" "Telegram outbound 可能异常"
forbid_out "$POLICY_OUT" "no such column"
forbid_out "$POLICY_OUT" "telegram_suppression_reason"
forbid_out "$POLICY_OUT" "sent_at"

FAIL_DB="$TMP_DIR/failure.sqlite"
"$PYTHON" - "$FAIL_DB" <<'PY'
from __future__ import annotations

import sqlite3
import sys
import time

db_path = sys.argv[1]
now = time.time()
conn = sqlite3.connect(db_path)
conn.executescript(
    """
    CREATE TABLE signals (signal_id TEXT PRIMARY KEY, timestamp REAL, created_at REAL);
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
    """
)
for i in range(100):
    ts = now - 500 - i
    conn.execute("INSERT INTO signals VALUES (?, ?, ?)", (f"sig-{i}", ts, ts))
    failed = i < 10
    conn.execute(
        """
        INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"audit-{i}",
            f"sig-{i}",
            "notifier_send_failed" if failed else "delivery_policy",
            "",
            0,
            ts if failed else None,
            "",
            "" if failed else "delivery_policy",
            '{"error_type":"TimedOut"}' if failed else "{}",
            ts,
            ts,
            ts,
            "notifier_send_failed" if failed else "delivery_policy",
            "notifier_send_failed" if failed else "delivery_policy",
            0,
            0 if failed else 1,
            ts,
            "",
        ),
    )
conn.commit()
conn.close()
PY

FAIL_OUT="$TMP_DIR/failure.out"
run_health "$FAIL_DB" "$FAIL_OUT"
require_out "$FAIL_OUT" "delivery_audit_rows=100"
require_out "$FAIL_OUT" "send_failed=10"
require_out "$FAIL_OUT" "sent_failed=10"
require_out "$FAIL_OUT" "send_attempted=10"
require_out "$FAIL_OUT" "suppressed_or_not_eligible=90"
require_out "$FAIL_OUT" "notifier_send_failed=10"
require_out "$FAIL_OUT" "Telegram outbound 可能异常"
forbid_out "$FAIL_OUT" "no such column"

MIXED_DB="$TMP_DIR/mixed.sqlite"
"$PYTHON" - "$MIXED_DB" <<'PY'
from __future__ import annotations

import sqlite3
import sys
import time

db_path = sys.argv[1]
now = time.time()
conn = sqlite3.connect(db_path)
conn.executescript(
    """
    CREATE TABLE signals (signal_id TEXT PRIMARY KEY, timestamp REAL, created_at REAL);
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
    """
)
reasons = [
    ("delivery_policy", "", "delivery_policy", "delivery_policy", ""),
    ("gate", "gate/lp_noise_filtered", "gate/lp_noise_filtered", "gate/lp_noise_filtered", ""),
    ("gate", "low_quality", "low_quality", "low_quality", ""),
    ("gate", "replay_profile_negative", "replay_profile_negative", "replay_profile_negative", ""),
    ("listener_prefilter", "", "listener_prefilter/drop", "listener_prefilter/drop", "listener_prefilter/drop"),
    ("gate", "score_below_shadow_candidate", "score_below_shadow_candidate", "score_below_shadow_candidate", ""),
]
for i in range(6533):
    ts = now - 700 - (i % 300)
    if i < 45:
        conn.execute("INSERT INTO signals VALUES (?, ?, ?)", (f"sig-{i}", ts, ts))
    delivered = 1 if i < 2 else 0
    stage, gate_reason, suppression_reason, decision, drop_reason = reasons[i % len(reasons)]
    conn.execute(
        """
        INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"audit-{i}",
            f"sig-{i}",
            stage if not delivered else "notifier_delivery",
            "" if delivered else gate_reason,
            delivered,
            ts if delivered else None,
            "message" if delivered else "",
            "" if delivered else suppression_reason,
            "{}",
            ts,
            ts,
            ts,
            "notifier_delivered" if delivered else decision,
            "notifier_delivered" if delivered else suppression_reason,
            delivered,
            0 if delivered else 1,
            ts,
            "" if delivered else drop_reason,
        ),
    )
conn.commit()
conn.close()
PY

MIXED_OUT="$TMP_DIR/mixed.out"
run_health "$MIXED_DB" "$MIXED_OUT"
require_out "$MIXED_OUT" "delivery_audit_rows=6533"
require_out "$MIXED_OUT" "send_failed=0"
require_out "$MIXED_OUT" "sent_failed=0"
require_out "$MIXED_OUT" "metric_invariant_warning=none"
forbid_out "$MIXED_OUT" "no such column"

rows="$(extract_metric "$MIXED_OUT" "最近2小时" "delivery_audit_rows")"
failed="$(extract_metric "$MIXED_OUT" "最近2小时" "sent_failed")"
if (( failed > rows )); then
  sed -n '1,160p' "$MIXED_OUT" >&2 || true
  fail "sent_failed exceeded delivery_audit_rows: ${failed} > ${rows}"
fi

echo "listener-health Telegram outbound breakdown test ok"
