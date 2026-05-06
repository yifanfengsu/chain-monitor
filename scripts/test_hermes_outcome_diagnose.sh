#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_outcome_diagnose_test.XXXXXX")"
TEST_DATE="2099-12-27"

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
  "logical_date": "2099-12-27",
  "run_overview": {
    "total_signal_rows": 4
  },
  "trade_opportunity_summary": {
    "window_record_count": 3,
    "candidate_outcome_completion_rate": 0.0411,
    "verified_outcome_completion_rate": 0
  },
  "outcome_source_summary": {
    "outcome_price_source_distribution": {
      "fixture": 1
    }
  },
  "trade_replay_summary": {
    "replay_count": 2,
    "replay_scope": "full"
  },
  "trade_replay_profile_summary": {
    "low_sample_profiles_count": 3
  }
}
JSON

python3 - "$TMP_DIR/outcome.sqlite" "$TEST_DATE" <<'PY'
import datetime as dt
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
        CREATE TABLE trade_replay_profile_daily_stats (
            logical_date TEXT,
            replay_scope TEXT,
            profile_key TEXT,
            valid_sample_count INTEGER,
            confidence_level TEXT
        );
        """
    )
    signals = [
        ("sig-1", "opp-1", "ETH", "ETH/USDT", start + 10, "CANDIDATE", "sweep_confirmed", "confirm", "prealert", "send", "{}"),
        ("sig-2", "opp-2", "ETH", "ETH/USDT", start + 20, "BLOCKED", "liquidity_shift", "lp_add", "confirm", "suppressed", "{}"),
        ("sig-3", "", "BTC", "BTC/USDT", start + 30, "", "pure_transfer", "", "observe", "observe", "{}"),
        ("sig-4", "opp-3", "SOL", "SOL/USDT", start + 40, "VERIFIED", "clmm_position", "clmm", "signal", "send", "{}"),
    ]
    conn.executemany("INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?)", signals)
    opportunities = [
        ("opp-1", "sig-1", "ETH", "ETH/USDT", "CANDIDATE", "LONG", "active", "sweep", "ETH|LONG|sweep", "candidate", start + 10, "{}"),
        ("opp-2", "sig-2", "ETH", "ETH/USDT", "BLOCKED", "LONG", "blocked", "lp", "ETH|LONG|lp", "blocked", start + 20, "{}"),
        ("opp-3", "sig-4", "SOL", "SOL/USDT", "VERIFIED", "SHORT", "active", "clmm", "SOL|SHORT|clmm", "verified", start + 40, "{}"),
    ]
    conn.executemany("INSERT INTO trade_opportunities VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", opportunities)
    conn.execute(
        "INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("out-1", "sig-1", "opp-1", "ETH", "ETH/USDT", 60, start + 70, "completed", "", start + 75, start + 10, start + 75, 100.0, 101.0, "fixture", "fixture"),
    )
    conn.execute(
        "INSERT INTO outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("out-2", "sig-2", "opp-2", "ETH", "ETH/USDT", 60, start + 80, "expired", "no_price_snapshot", None, start + 20, start + 80, None, None, "", ""),
    )
    conn.execute(
        "INSERT INTO opportunity_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("opp-1", 60, "ETH|LONG|sweep", start + 70, 100.0, 101.0, "fixture", "fixture", "completed", "", start + 75, start + 10, start + 75),
    )
    conn.execute(
        "INSERT INTO opportunity_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("opp-2", 60, "ETH|LONG|lp", start + 80, None, None, "", "", "pending", "", None, start + 20, start + 80),
    )
    conn.execute(
        "INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("replay-1", date, "full", "sig-1", "opp-1", "CANDIDATE", "ETH", "ETH/USDT", "signal", "prealert", "ETH|LONG|sweep", 1, "", "fixture", "now"),
    )
    conn.execute(
        "INSERT INTO trade_replay_examples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("replay-2", date, "full", "sig-3", "", "", "BTC", "BTC/USDT", "observe", "observe", "BTC|LONG|transfer", 0, "no_price_for_signal_id", "", "now"),
    )
    conn.execute("INSERT INTO trade_replay_profile_daily_stats VALUES (?,?,?,?,?)", (date, "full", "p1", 2, "low"))
    conn.execute("INSERT INTO trade_replay_profile_daily_stats VALUES (?,?,?,?,?)", (date, "full", "p2", 1, "low"))
    conn.commit()
finally:
    conn.close()
PY

test_now="$(python3 - "$TEST_DATE" <<'PY'
import datetime as dt
import sys
date = dt.date.fromisoformat(sys.argv[1])
start = int(dt.datetime(date.year, date.month, date.day, tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp())
print(start + 1000)
PY
)"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_OUTCOME_DIAGNOSE_DB_PATH="$TMP_DIR/outcome.sqlite" \
HERMES_OUTCOME_DIAGNOSE_DAILY_DIR="$TMP_DIR/reports/daily" \
HERMES_OUTCOME_DIAGNOSE_NOW_TS="$test_now" \
  "$OPS" outcome-diagnose --date "$TEST_DATE" >"$TMP_DIR/outcome.out"

require_out "Chain Monitor Outcome闭环诊断｜${TEST_DATE}"
require_out "daily_report_status=present"
require_out "signals_total=4"
require_out "trade_opportunities_total=3"
require_out "outcomes_total=2 completed=1 completed_rate=50.00% (1/2)"
require_out "opportunity_outcomes_total=2 completed=1 completed_rate=50.00% (1/2)"
require_out "trade_replay_examples_total=2"
require_out "signals_to_outcomes_match_rate=50.00% (2/4)"
require_out "opportunities_to_opportunity_outcomes_match_rate=66.67% (2/3)"
require_out "opportunities_to_replay_examples_match_rate=33.33% (1/3)"
require_out "profile_sample_summary=source=trade_replay_profile_daily_stats profile_count=2 max_sample_count=2 confidence_distribution=low=2"
require_out "signals_outcome_missing_by_asset=BTC=1；SOL=1"
require_out "opportunities_outcome_missing_by_status=VERIFIED=1"
require_out "outcome缺失原因推断:"
require_out "- price snapshot 缺失=可能 affected_rows=2"
require_out "- signal_id / opportunity_id 未关联=可能"
require_out "- outcome worker 未运行=可能 past_due_pending=1"
require_out "- report 字段映射错误=可能 diagnostics=daily_report_missing_outcome_60s_completed_rate"
require_out "下一步建议:"
require_out "说明=只读脱敏诊断，不含执行指令。"
forbid_out "买入"
forbid_out "卖出"
forbid_out "仓位"
forbid_out "杠杆"
forbid_out "止盈"
forbid_out "止损"
grep -Fq '"command":"outcome-diagnose"' "$TMP_DIR/ops_audit.ndjson" || fail "audit missing outcome-diagnose command"
grep -Fq '"allowed":true' "$TMP_DIR/ops_audit.ndjson" || fail "audit missing allowed=true"

echo "OK: Hermes outcome diagnose test passed"
