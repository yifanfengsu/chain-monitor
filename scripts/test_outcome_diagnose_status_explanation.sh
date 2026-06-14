#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/outcome_status_explanation.XXXXXX")"
TEST_DATE="2099-12-25"

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
  grep -Fq -- "$pattern" "$TMP_DIR/outcome.out" || fail "Outcome diagnose output missing: ${pattern}"
}

forbid_out() {
  local pattern="$1"
  if grep -Fq -- "$pattern" "$TMP_DIR/outcome.out"; then
    fail "Outcome diagnose output contains forbidden text: ${pattern}"
  fi
}

mkdir -p "$TMP_DIR/reports/daily"
cat >"$TMP_DIR/reports/daily/daily_report_${TEST_DATE}.json" <<'JSON'
{
  "logical_date": "2099-12-25",
  "run_overview": {
    "total_signal_rows": 3
  },
  "trade_opportunity_summary": {
    "window_record_count": 3
  },
  "trade_replay_summary": {
    "replay_count": 3,
    "replay_scope": "full"
  }
}
JSON

python3 - "$TMP_DIR/outcome.sqlite" "$TEST_DATE" <<'PY'
import datetime as dt
import json
import sqlite3
import sys

db_path = sys.argv[1]
date = sys.argv[2]
parsed = dt.date.fromisoformat(date)
start = int(dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp())
conn = sqlite3.connect(db_path)
try:
    conn.executescript(
        """
        CREATE TABLE signals (
            signal_id TEXT PRIMARY KEY,
            trade_opportunity_id TEXT,
            asset TEXT,
            pair TEXT,
            timestamp REAL,
            trade_opportunity_status TEXT,
            canonical_semantic_key TEXT,
            trade_action_key TEXT,
            lp_alert_stage TEXT,
            delivery_decision TEXT,
            signal_json TEXT
        );
        CREATE TABLE trade_opportunities (
            trade_opportunity_id TEXT PRIMARY KEY,
            signal_id TEXT,
            asset TEXT,
            pair TEXT,
            status TEXT,
            side TEXT,
            maturity TEXT,
            opportunity_profile_strategy TEXT,
            opportunity_profile_key TEXT,
            label TEXT,
            created_at REAL,
            primary_hard_blocker TEXT,
            hard_blockers_json TEXT,
            opportunity_gate_required INTEGER,
            opportunity_gate_passed INTEGER,
            opportunity_gate_failure_reason TEXT,
            shadow_reason TEXT,
            would_have_been_candidate INTEGER,
            opportunity_json TEXT
        );
        CREATE TABLE outcomes (
            outcome_id TEXT PRIMARY KEY,
            signal_id TEXT,
            trade_opportunity_id TEXT,
            asset TEXT,
            pair TEXT,
            window_sec INTEGER,
            due_at REAL,
            status TEXT,
            failure_reason TEXT,
            completed_at REAL,
            created_at REAL,
            updated_at REAL,
            start_price REAL,
            end_price REAL,
            outcome_price_source TEXT,
            price_source TEXT
        );
        CREATE TABLE opportunity_outcomes (
            trade_opportunity_id TEXT,
            window_sec INTEGER,
            opportunity_profile_key TEXT,
            due_at REAL,
            start_price REAL,
            end_price REAL,
            outcome_price_source TEXT,
            price_source TEXT,
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
            opportunity_status TEXT,
            asset TEXT,
            pair TEXT,
            signal_stage TEXT,
            lp_stage TEXT,
            profile_key TEXT,
            data_valid INTEGER,
            invalid_reason TEXT,
            price_source TEXT,
            created_at TEXT
        );
        """
    )
    for idx in range(3):
        conn.execute(
            "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (f"sig-{idx}", f"opp-{idx}", "ETH", "ETH/USDT", start + idx, "NONE", "lp", "", "confirm", "observe", "{}"),
        )
    opportunities = [
        (
            "opp-0",
            "sig-0",
            "ETH",
            "ETH/USDT",
            "NONE",
            "LONG",
            "none",
            "lp",
            "ETH|LONG|lp",
            "none",
            start + 10,
            "replay_profile_negative",
            json.dumps(["replay_profile_negative"]),
            0,
            0,
            "",
            "",
            0,
            "{}",
        ),
        (
            "opp-1",
            "sig-1",
            "ETH",
            "ETH/USDT",
            "NONE",
            "LONG",
            "none",
            "lp",
            "ETH|LONG|lp",
            "none",
            start + 20,
            "",
            "[]",
            1,
            0,
            "opportunity_gate_rejected",
            "",
            0,
            "{}",
        ),
        (
            "opp-2",
            "sig-2",
            "ETH",
            "ETH/USDT",
            "NONE",
            "LONG",
            "none",
            "lp",
            "ETH|LONG|lp",
            "none",
            start + 30,
            "",
            "[]",
            0,
            0,
            "",
            "score_below_shadow_candidate",
            1,
            "{}",
        ),
    ]
    conn.executemany("INSERT INTO trade_opportunities VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", opportunities)
    for idx in range(3):
        conn.execute(
            "INSERT INTO opportunity_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"opp-{idx}", 60, "ETH|LONG|lp", start + 60 + idx, 100.0, 100.0, "fixture", "fixture", "completed", "", start + 90 + idx, start + idx, start + 90 + idx),
        )
        conn.execute(
            "INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"replay-{idx}", date, "full", f"sig-{idx}", f"opp-{idx}", "NONE", "ETH", "ETH/USDT", "signal", "confirm", "ETH|LONG|lp", 1, "", "fixture", "now"),
        )
    conn.commit()
finally:
    conn.close()
PY

cd "$REPO_ROOT"
HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_OUTCOME_DIAGNOSE_DB_PATH="$TMP_DIR/outcome.sqlite" \
HERMES_OUTCOME_DIAGNOSE_DAILY_DIR="$TMP_DIR/reports/daily" \
  "$OPS" outcome-diagnose --date "$TEST_DATE" >"$TMP_DIR/outcome.out"

require_out "Chain Monitor Outcome闭环诊断｜${TEST_DATE}"
require_out "trade_opportunity_status_distribution=NONE=3"
require_out "raw_status_distribution="
require_out "derived_status_distribution="
require_out "true_none_count=0"
require_out "blocked_like_none_count=1"
require_out "gate_failed_none_count=1"
require_out "near_candidate_count=1"
require_out "top_derived_reasons="
require_out "status_assignment_warning=raw_status_all_none_but_blocked_like_present"
require_out "不应简单理解为整天无机会"
forbid_out "交易建议"
forbid_out "买入"
forbid_out "卖出"
forbid_out "仓位"
forbid_out "杠杆"
forbid_out "止盈"
forbid_out "止损"

echo "OK: outcome diagnose status explanation test passed"
