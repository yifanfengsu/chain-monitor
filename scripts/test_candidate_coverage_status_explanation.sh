#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TEST_DATE="2099-12-28"
REPORT="${REPO_ROOT}/reports/daily/daily_report_${TEST_DATE}.json"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/candidate_coverage_status.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
  rm -f "$REPORT"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local pattern="$1"
  grep -Fq -- "$pattern" "$TMP_DIR/candidate.out" || fail "candidate-coverage output missing: ${pattern}"
}

mkdir -p "${REPO_ROOT}/reports/daily"
cat >"$REPORT" <<'JSON'
{
  "logical_date": "2099-12-28",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "replay_count": 80,
    "valid_replay_count": 80,
    "replay_coverage_rate_candidate": 0
  },
  "candidate_frontier_summary": {
    "near_candidate_count": 0,
    "near_candidate_replay_count": 0,
    "diagnosis": "gate_closed_because_quality_low"
  }
}
JSON

python3 - "$TMP_DIR/candidate.sqlite" <<'PY'
import datetime
import json
import sqlite3
import sys

db_path = sys.argv[1]
start = int(datetime.datetime(2099, 12, 28, tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp())
conn = sqlite3.connect(db_path)
try:
    conn.executescript(
        """
        CREATE TABLE signals (
            signal_id TEXT PRIMARY KEY,
            timestamp REAL,
            created_at REAL
        );
        CREATE TABLE trade_opportunities (
            trade_opportunity_id TEXT PRIMARY KEY,
            status TEXT,
            created_at REAL,
            primary_blocker TEXT,
            primary_hard_blocker TEXT,
            blockers_json TEXT,
            hard_blockers_json TEXT,
            opportunity_gate_failure_reason TEXT,
            shadow_reason TEXT,
            opportunity_json TEXT
        );
        CREATE TABLE trade_replay_examples (
            replay_id TEXT PRIMARY KEY,
            logical_date TEXT,
            replay_scope TEXT,
            trade_opportunity_id TEXT,
            opportunity_status TEXT,
            signal_ts REAL
        );
        CREATE TABLE delivery_audit (
            audit_id TEXT PRIMARY KEY,
            timestamp REAL,
            notifier_sent_at REAL,
            sent_to_telegram INTEGER,
            stage TEXT,
            opportunity_status TEXT
        );
        """
    )
    for idx in range(108):
        conn.execute("INSERT INTO signals VALUES (?, ?, ?)", (f"sig-{idx}", start + idx, start + idx))
    blockers = ["replay_profile_negative"] * 40 + ["low_quality"] * 20 + ["local_absorption"] * 20
    for idx, blocker in enumerate(blockers):
        payload = {
            "trade_opportunity_status": "NONE",
            "trade_opportunity_primary_blocker": blocker,
            "trade_opportunity_blockers": [blocker],
            "trade_opportunity_hard_blockers": [blocker],
        }
        conn.execute(
            "INSERT INTO trade_opportunities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"opp-blocked-{idx}",
                "NONE",
                start + 30 + idx,
                blocker,
                blocker,
                json.dumps([blocker]),
                json.dumps([blocker]),
                "",
                "",
                json.dumps(payload),
            ),
        )
        conn.execute(
            "INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?)",
            (f"replay-{idx}", "2099-12-28", "full", f"opp-blocked-{idx}", "NONE", start + 500 + idx),
        )
    for idx in range(28):
        conn.execute(
            "INSERT INTO trade_opportunities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"opp-true-none-{idx}",
                "NONE",
                start + 200 + idx,
                "",
                "",
                "[]",
                "[]",
                "",
                "score_below_shadow_candidate" if idx < 10 else "",
                "{}",
            ),
        )
    conn.commit()
finally:
    conn.close()
PY

cd "$REPO_ROOT"
HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_CANDIDATE_COVERAGE_DB_PATH="$TMP_DIR/candidate.sqlite" \
  "$OPS" candidate-coverage --date "$TEST_DATE" >"$TMP_DIR/candidate.out"

require_out "Chain Monitor CANDIDATE覆盖诊断｜${TEST_DATE}"
require_out "trade_opportunities_total=108"
require_out "trade_opportunity_status_distribution=NONE=108 / CANDIDATE=0 / VERIFIED=0 / BLOCKED=0 / INVALIDATED=0"
require_out "raw_status_distribution="
require_out "derived_status_distribution="
require_out "true_none_count=18"
require_out "blocked_like_none_count=80"
require_out "gate_failed_none_count=0"
require_out "near_candidate_count=10"
require_out "none_with_blockers_count=80"
require_out "none_with_hard_blockers_count=80"
require_out "none_with_primary_blocker_count=80"
require_out "top_derived_reasons="
require_out "top_blocker_like_reasons="
require_out "top_gate_failure_reasons="
require_out "status_assignment_diagnosis=raw_none_contains_explainable_opportunities"
require_out "status_assignment_warning=raw_status_all_none_but_blocked_like_present"

echo "OK: candidate-coverage status explanation test passed"
