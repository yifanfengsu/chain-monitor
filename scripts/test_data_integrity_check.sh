#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/data_integrity_check.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local path="$1"
  local pattern="$2"
  grep -Fq -- "$pattern" "$path" || fail "${path} missing pattern: ${pattern}"
}

forbid_out() {
  local path="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$path"; then
    fail "${path} contains forbidden pattern: ${pattern}"
  fi
}

mkdir -p "$TMP_DIR/archive/raw" "$TMP_DIR/reports/daily" "$TMP_DIR/logs" "$TMP_DIR/bin"

cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
echo "unexpected make call: $*" >&2
exit 99
MAKE
chmod +x "$TMP_DIR/bin/make"

for date in 2099-01-01 2099-01-02 2099-01-03; do
  printf '{"fixture":"%s"}\n' "$date" >"$TMP_DIR/archive/raw/archive_${date}.ndjson"
done

cat >"$TMP_DIR/logs/runtime.log" <<'LOG'
2099-01-03T12:00:00+00:00 sqlite_store warning: database is locked
2099-01-03T12:01:00+00:00 sqlite_write_failed final write failure
LOG

python3 - "$TMP_DIR/chain_monitor.sqlite" <<'PY'
from __future__ import annotations

import datetime as dt
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
try:
    conn.executescript(
        """
        CREATE TABLE raw_events (id TEXT, logical_date TEXT, created_at REAL);
        CREATE TABLE parsed_events (id TEXT, logical_date TEXT, created_at REAL);
        CREATE TABLE signals (id TEXT, logical_date TEXT, created_at REAL);
        CREATE TABLE delivery_audit (id TEXT, logical_date TEXT, created_at REAL, sent_at REAL);
        CREATE TABLE telegram_deliveries (id TEXT, logical_date TEXT, sent_at REAL);
        CREATE TABLE trade_opportunities (id TEXT, logical_date TEXT, created_at REAL, status TEXT);
        CREATE TABLE outcomes (id TEXT, logical_date TEXT, created_at REAL, status TEXT, completed_at REAL);
        CREATE TABLE opportunity_outcomes (id TEXT, logical_date TEXT, created_at REAL, status TEXT, completed_at REAL, due_ts REAL);
        CREATE TABLE trade_replay_examples (id TEXT, logical_date TEXT, created_at REAL, replay_scope TEXT, valid INTEGER, net_pnl_bps REAL);
        CREATE TABLE trade_replay_profile_daily_stats (id TEXT, logical_date TEXT, replay_scope TEXT, created_at REAL);
        """
    )
    base = int(dt.datetime(2099, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp())
    tables = (
        "raw_events",
        "parsed_events",
        "signals",
        "delivery_audit",
        "telegram_deliveries",
        "trade_opportunities",
        "outcomes",
        "opportunity_outcomes",
        "trade_replay_examples",
        "trade_replay_profile_daily_stats",
    )
    for idx, table in enumerate(tables):
        date = "2099-01-01"
        ts = base + idx + 10
        if table == "delivery_audit":
            conn.execute("INSERT INTO delivery_audit VALUES (?, ?, ?, ?)", (f"{table}-1", date, ts, ts))
        elif table == "telegram_deliveries":
            conn.execute("INSERT INTO telegram_deliveries VALUES (?, ?, ?)", (f"{table}-1", date, ts))
        elif table == "trade_opportunities":
            conn.execute("INSERT INTO trade_opportunities VALUES (?, ?, ?, ?)", (f"{table}-1", date, ts, "CANDIDATE"))
        elif table == "outcomes":
            conn.execute("INSERT INTO outcomes VALUES (?, ?, ?, ?, ?)", (f"{table}-1", date, ts, "completed", ts + 60))
        elif table == "opportunity_outcomes":
            conn.execute("INSERT INTO opportunity_outcomes VALUES (?, ?, ?, ?, ?, ?)", (f"{table}-1", date, ts, "completed", ts + 60, ts + 60))
        elif table == "trade_replay_examples":
            conn.execute("INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?)", (f"{table}-1", date, ts, "full", 1, 12.5))
        elif table == "trade_replay_profile_daily_stats":
            conn.execute("INSERT INTO trade_replay_profile_daily_stats VALUES (?, ?, ?, ?)", (f"{table}-1", date, "full", ts))
        else:
            conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", (f"{table}-1", date, ts))

    recoverable_date = "2099-01-02"
    for table in ("parsed_events", "signals"):
        conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", (f"{table}-recoverable", recoverable_date, base + 86400 + 20))

    degraded_date = "2099-01-03"
    for table in ("raw_events", "parsed_events", "signals"):
        conn.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", (f"{table}-degraded", degraded_date, base + 2 * 86400 + 20))
    conn.execute("INSERT INTO delivery_audit VALUES (?, ?, ?, ?)", ("delivery-degraded", degraded_date, base + 2 * 86400 + 21, base + 2 * 86400 + 21))
    conn.execute("INSERT INTO telegram_deliveries VALUES (?, ?, ?)", ("telegram-degraded", degraded_date, base + 2 * 86400 + 22))
    conn.execute("INSERT INTO trade_opportunities VALUES (?, ?, ?, ?)", ("opp-degraded", degraded_date, base + 2 * 86400 + 23, "BLOCKED"))
    conn.execute("INSERT INTO outcomes VALUES (?, ?, ?, ?, ?)", ("outcome-degraded", degraded_date, base + 2 * 86400 + 24, "completed", base + 2 * 86400 + 60))
    conn.execute(
        "INSERT INTO opportunity_outcomes VALUES (?, ?, ?, ?, ?, ?)",
        ("opp-outcome-degraded", degraded_date, base + 2 * 86400 + 25, "pending", None, 1),
    )
    conn.commit()
finally:
    conn.close()
PY

cat >"$TMP_DIR/reports/daily/daily_report_2099-01-01.json" <<'JSON'
{
  "data_quality_summary": {"data_quality_status": "valid", "zero_activity_day": false},
  "trade_replay_summary": {"replay_source": "persisted", "replay_scope": "full", "replay_count": 1, "valid_replay_count": 1},
  "outcome_diagnosis_summary": {
    "outcomes_completed_rate": 1.0,
    "opportunity_outcomes_completed_rate": 1.0,
    "opportunity_outcomes_past_due_pending": 0
  },
  "candidate_coverage_summary": {"available": true},
  "candidate_frontier_summary": {"available": true},
  "lp_signal_summary": {"available": true, "lp_signal_rows": 1},
  "lp_suppression_sample_replay_summary": {"available": true},
  "major_coverage_summary": {"available": true}
}
JSON

cat >"$TMP_DIR/reports/daily/daily_report_2099-01-03.json" <<'JSON'
{
  "data_quality_summary": {"data_quality_status": "valid", "zero_activity_day": false},
  "trade_replay_summary": {"replay_source": "persisted", "replay_scope": "full", "replay_count": 0, "valid_replay_count": 0},
  "outcome_diagnosis_summary": {
    "outcomes_completed_rate": 1.0,
    "opportunity_outcomes_completed_rate": 0.0,
    "opportunity_outcomes_past_due_pending": 1
  },
  "candidate_coverage_summary": {"available": true},
  "lp_signal_summary": {"available": true, "lp_signal_rows": 1}
}
JSON

run_integrity() {
  local date="$1"
  local out="$2"
  HERMES_DATA_INTEGRITY_DB_PATH="$TMP_DIR/chain_monitor.sqlite" \
  HERMES_DATA_INTEGRITY_ARCHIVE_ROOT="$TMP_DIR/archive" \
  HERMES_DATA_INTEGRITY_DAILY_DIR="$TMP_DIR/reports/daily" \
  HERMES_DATA_INTEGRITY_LOG_ROOTS="$TMP_DIR/logs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit_${date}.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/lock_${date}.lock" \
  PATH="$TMP_DIR/bin:$PATH" \
    "$SCRIPT" data-integrity --date "$date" >"$out"
}

run_integrity 2099-01-01 "$TMP_DIR/complete.out"
require_out "$TMP_DIR/complete.out" "archive_status=present"
require_out "$TMP_DIR/complete.out" "raw_events: exists=true rows=1"
require_out "$TMP_DIR/complete.out" "archive_sqlite_status=complete"
require_out "$TMP_DIR/complete.out" "final_status=complete"

run_integrity 2099-01-02 "$TMP_DIR/recoverable.out"
require_out "$TMP_DIR/recoverable.out" "archive_sqlite_status=recoverable"
require_out "$TMP_DIR/recoverable.out" "final_status=recoverable"
require_out "$TMP_DIR/recoverable.out" "make db-migrate-date DATE=2099-01-02"

run_integrity 2099-01-03 "$TMP_DIR/degraded.out"
require_out "$TMP_DIR/degraded.out" "final_status=degraded"
require_out "$TMP_DIR/degraded.out" "past_due_pending=1"
require_out "$TMP_DIR/degraded.out" "locked_status=final_failures_detected"

run_integrity 2099-01-04 "$TMP_DIR/invalid.out"
require_out "$TMP_DIR/invalid.out" "archive_status=missing"
require_out "$TMP_DIR/invalid.out" "final_status=invalid"

for output in "$TMP_DIR/complete.out" "$TMP_DIR/recoverable.out" "$TMP_DIR/degraded.out" "$TMP_DIR/invalid.out"; do
  for forbidden in "make run" "db-vacuum" "db-prune" "archive-compress-date execute" "买入" "卖出" "仓位" "杠杆" "止盈" "止损"; do
    forbid_out "$output" "$forbidden"
  done
done

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text '数据完整性检查2026-05-07' --dry-run --platform telegram >"$TMP_DIR/router.out"
require_out "$TMP_DIR/router.out" '"data-integrity"'
require_out "$TMP_DIR/router.out" '"--date"'
require_out "$TMP_DIR/router.out" '"2026-05-07"'

set +e
HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text '数据完整性检查昨天' --dry-run --platform telegram >"$TMP_DIR/router_relative.out" 2>"$TMP_DIR/router_relative.err"
relative_rc=$?
set -e
[[ "$relative_rc" -ne 0 ]] || fail "router accepted relative data-integrity date"
require_out "$TMP_DIR/router_relative.out" "YYYY-MM-DD"
forbid_out "$TMP_DIR/router_relative.out" "data-integrity"

echo "OK: data-integrity check tests passed"
