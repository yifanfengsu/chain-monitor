#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/outcome_catchup_router.XXXXXX")"
TEST_DATE="2099-12-28"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

python3 - "$TMP_DIR/catchup.sqlite" "$TEST_DATE" <<'PY'
import datetime as dt
import sqlite3
import sys

db_path = sys.argv[1]
logical_date = sys.argv[2]
date = dt.date.fromisoformat(logical_date)
start = int(dt.datetime(date.year, date.month, date.day, tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp())
conn = sqlite3.connect(db_path)
try:
    conn.executescript(
        """
        CREATE TABLE trade_opportunities (
            trade_opportunity_id TEXT PRIMARY KEY,
            signal_id TEXT,
            created_at REAL
        );
        CREATE TABLE opportunity_outcomes (
            trade_opportunity_id TEXT,
            window_sec INTEGER,
            due_at REAL,
            start_price REAL,
            end_price REAL,
            status TEXT,
            failure_reason TEXT,
            completed_at REAL,
            created_at REAL,
            updated_at REAL,
            PRIMARY KEY(trade_opportunity_id, window_sec)
        );
        CREATE TABLE trade_replay_examples (
            replay_id TEXT PRIMARY KEY,
            logical_date TEXT,
            replay_scope TEXT,
            signal_id TEXT,
            trade_opportunity_id TEXT,
            net_pnl_bps REAL,
            entry_ts INTEGER,
            exit_ts INTEGER,
            entry_price REAL,
            exit_price REAL,
            label TEXT,
            price_source TEXT,
            data_valid INTEGER
        );
        """
    )
    conn.execute("INSERT INTO trade_opportunities VALUES (?, ?, ?)", ("opp-1", "sig-1", start + 1))
    conn.execute("INSERT INTO opportunity_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("opp-1", 60, start + 60, 100, None, "pending", "", None, start + 1, start + 1))
    conn.execute("INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("replay-1", logical_date, "full", "sig-1", "opp-1", 12.5, start + 5, start + 65, 100, 100.4, "clean_followthrough", "fixture", 1))
    conn.commit()
finally:
    conn.close()
PY

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text "Outcome补全预检${TEST_DATE}" --dry-run --platform telegram >"$TMP_DIR/router.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "outcome-catchup"' "$TMP_DIR/router.out" || fail "router did not map to outcome-catchup"
grep -Fq '"--dry-run"' "$TMP_DIR/router.out" || fail "router did not force dry-run"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text "后验补全预检${TEST_DATE}" --dry-run --platform telegram >"$TMP_DIR/router_alias.out"
grep -Fq '"outcome-catchup"' "$TMP_DIR/router_alias.out" || fail "router alias did not map to outcome-catchup"

if HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "Outcome补全预检昨天" --dry-run --platform telegram >"$TMP_DIR/relative.out" 2>"$TMP_DIR/relative.err"; then
  fail "router accepted relative outcome catchup date"
fi
grep -Fq "YYYY-MM-DD" "$TMP_DIR/relative.out" "$TMP_DIR/relative.err" || fail "relative date refusal missing YYYY-MM-DD"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_OPS_PLATFORM=telegram \
HERMES_OUTCOME_CATCHUP_DB_PATH="$TMP_DIR/catchup.sqlite" \
HERMES_OUTCOME_CATCHUP_NOW_TS="$(( $(date -u -d "$TEST_DATE 17:00:00" +%s) ))" \
  "$OPS" outcome-catchup --date "$TEST_DATE" --dry-run >"$TMP_DIR/catchup.out"
grep -Fq "would_update_rows=1" "$TMP_DIR/catchup.out" || fail "dry-run did not report would_update_rows=1"
grep -Fq "updated_count=0" "$TMP_DIR/catchup.out" || fail "dry-run modified rows"

if HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
   HERMES_OPS_LOCK_PATH="$TMP_DIR/ops-exec.lock" \
   HERMES_OPS_ROUTER_OK=1 \
   HERMES_OPS_PLATFORM=telegram \
   HERMES_OUTCOME_CATCHUP_DB_PATH="$TMP_DIR/catchup.sqlite" \
   "$OPS" outcome-catchup --date "$TEST_DATE" --execute --confirm >"$TMP_DIR/execute.out" 2>"$TMP_DIR/execute.err"; then
  fail "Telegram platform was allowed to execute outcome-catchup"
fi
grep -Fq "Telegram outcome-catchup only allows --dry-run" "$TMP_DIR/execute.err" || fail "Telegram execute refusal missing"

echo "OK: outcome-catchup dry-run router test passed"
