#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_lp_diagnose_test.XXXXXX")"
TEST_DATE="2099-12-28"

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
  grep -Fq -- "$pattern" "$TMP_DIR/lp.out" || fail "LP diagnose output missing: ${pattern}"
}

forbid_out() {
  local pattern="$1"
  if grep -Fq -- "$pattern" "$TMP_DIR/lp.out"; then
    fail "LP diagnose output contains forbidden text: ${pattern}"
  fi
}

mkdir -p "$TMP_DIR/reports/daily"
cat >"$TMP_DIR/reports/daily/daily_report_${TEST_DATE}.json" <<'JSON'
{
  "logical_date": "2099-12-28",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "run_overview": {
    "lp_signal_rows": 0,
    "delivered_lp_signals": 0,
    "suppressed_lp_signals": 0
  },
  "major_coverage_summary": {
    "covered_major_pairs": ["ETH/USDC"],
    "missing_major_pairs": ["ETH/USDT", "BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC"],
    "pair_distribution": {
      "ETH/USDC": 1
    },
    "current_sample_still_eth_only": true
  }
}
JSON

python3 - "$TMP_DIR/lp.sqlite" "$TEST_DATE" <<'PY'
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
            timestamp REAL,
            created_at REAL,
            signal_json TEXT,
            lp_alert_stage TEXT,
            pool_address TEXT,
            trade_action_key TEXT,
            asset_market_state_key TEXT,
            canonical_semantic_key TEXT,
            scan_path TEXT,
            delivery_decision TEXT
        );
        CREATE TABLE raw_events (
            event_id TEXT PRIMARY KEY,
            captured_at REAL,
            created_at REAL,
            raw_json TEXT,
            raw_kind TEXT,
            listener_scan_path TEXT,
            pool_address TEXT
        );
        CREATE TABLE parsed_events (
            event_id TEXT PRIMARY KEY,
            parsed_at REAL,
            created_at REAL,
            parsed_json TEXT,
            parsed_kind TEXT,
            role_group TEXT,
            lp_alert_stage_candidate TEXT,
            pool_address TEXT
        );
        CREATE TABLE delivery_audit (
            audit_id TEXT PRIMARY KEY,
            signal_id TEXT,
            timestamp REAL,
            notifier_sent_at REAL,
            sent_to_telegram INTEGER,
            suppressed INTEGER,
            delivery_decision TEXT,
            stage TEXT,
            reason TEXT,
            gate_reason TEXT,
            suppression_reason TEXT,
            audit_json TEXT,
            delivered INTEGER
        );
        """
    )
    conn.execute(
        "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "sig-lp-1",
            start + 60,
            start + 60,
            '{"event":"lp_add_liquidity","pool":"fixture","liquidity":"add","clmm_context":true}',
            "confirm",
            "pool-fixture",
            "lp_add_liquidity",
            "WATCH",
            "lp_signal",
            "lp_pool_scan",
            "send",
        ),
    )
    conn.execute(
        "INSERT INTO raw_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("raw-lp-1", start + 30, start + 30, '{"pool":"fixture","topic":"liquidity"}', "pool_log", "lp_pool_scan", "pool-fixture"),
    )
    conn.execute(
        "INSERT INTO parsed_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "parsed-lp-1",
            start + 40,
            start + 40,
            '{"intent":"clmm_position_add_range_liquidity","pool":"fixture"}',
            "clmm_position",
            "lp",
            "prealert",
            "pool-fixture",
        ),
    )
    conn.execute(
        "INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("audit-send", "sig-lp-1", start + 70, start + 70, 1, 0, "send", "signal", "", "", "", '{"kind":"lp"}', 1),
    )
    conn.execute(
        "INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "audit-suppress",
            "sig-lp-1",
            start + 80,
            start + 80,
            0,
            1,
            "suppressed",
            "signal",
            "gate_suppressed",
            "gate",
            "no_trade_gate",
            '{"kind":"lp"}',
            0,
        ),
    )
    conn.commit()
finally:
    conn.close()
PY

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_LP_DIAGNOSE_DB_PATH="$TMP_DIR/lp.sqlite" \
HERMES_LP_DIAGNOSE_DAILY_DIR="$TMP_DIR/reports/daily" \
  "$OPS" lp-diagnose --date "$TEST_DATE" >"$TMP_DIR/lp.out"

require_out "Chain Monitor LP诊断｜${TEST_DATE}"
require_out "daily_report字段存在性=lp_signal_summary=missing；lp_signals=missing；lp_stage_summary=missing；clmm_summary=missing；lp_suppression_summary=missing；lp_suppression_replay_summary=missing；major_coverage_summary=present"
require_out "SQLite LP-like计数 signals:"
require_out "signal_json LIKE '%lp%'=1"
require_out "signal_json LIKE '%pool%'=1"
require_out "signal_json LIKE '%liquidity%'=1"
require_out "signal_json LIKE '%clmm%'=1"
require_out "SQLite LP-like计数 raw_events:"
require_out "SQLite LP-like计数 parsed_events:"
require_out "delivery_audit LP相关推送抑制=lp_like_total=2 pushed=1 suppressed=1"
require_out "major coverage ETH/BTC/SOL x USDT/USDC=ETH/USDT:missing；ETH/USDC:covered(count=1)"
require_out "report_mapping判断=可能存在 report mapping 问题"
require_out "analyzer_gate判断=delivery_audit 显示 LP 相关抑制"
require_out "说明=只读脱敏诊断，不含执行指令。"
forbid_out "买入"
forbid_out "卖出"
forbid_out "仓位"
forbid_out "杠杆"
forbid_out "止盈"
forbid_out "止损"
grep -Fq '"command":"lp-diagnose"' "$TMP_DIR/ops_audit.ndjson" || fail "audit missing lp-diagnose command"
grep -Fq '"allowed":true' "$TMP_DIR/ops_audit.ndjson" || fail "audit missing allowed=true"

echo "OK: Hermes LP diagnose test passed"
