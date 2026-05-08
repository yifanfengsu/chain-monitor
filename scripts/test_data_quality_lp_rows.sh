#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/data_quality_lp_rows.XXXXXX")"
REPORT_DIR="$REPO_ROOT/reports/daily"

cleanup() {
  rm -rf "$TMP_DIR"
  rm -f \
    "$REPORT_DIR/daily_report_2099-11-01.json" \
    "$REPORT_DIR/daily_report_2099-11-02.json" \
    "$REPORT_DIR/daily_report_2099-11-03.json" \
    "$REPORT_DIR/daily_report_2099-11-04.json" \
    "$REPORT_DIR/daily_report_2099-11-05.json" \
    "$REPORT_DIR/daily_report_2099-11-06.json"
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

mkdir -p "$REPORT_DIR"

python3 - "$REPORT_DIR" "$TMP_DIR/lp.sqlite" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

report_dir = Path(sys.argv[1])
db_path = Path(sys.argv[2])

base = {
    "data_quality_summary": {
        "data_quality_status": "valid",
        "zero_activity_day": False,
        "active_hours": 24,
        "signals_count": 1,
        "raw_events_count": 1,
        "parsed_events_count": 1,
    },
    "trade_replay_summary": {"trade_replay_available": True, "replay_source": "persisted", "replay_scope": "full"},
    "major_coverage_summary": {"available": True},
}

payloads = {
    "2099-11-01": {**base, "run_overview": {"lp_signal_rows": 108}},
    "2099-11-02": {**base, "lp_signal_summary": {"available": True, "lp_signal_rows": 108}},
    "2099-11-03": {
        **base,
        "lp_signal_summary": {"available": True, "delivered_count": 42, "suppressed_count": 66},
    },
    "2099-11-04": {
        **base,
        "lp_suppression_summary": {"available": True, "delivered": 42, "suppressed": 66},
    },
    "2099-11-05": dict(base),
    "2099-11-06": dict(base),
}
for date, payload in payloads.items():
    (report_dir / f"daily_report_{date}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

conn = sqlite3.connect(db_path)
try:
    conn.execute("CREATE TABLE signals (id TEXT, logical_date TEXT, signal_json TEXT)")
    conn.execute("INSERT INTO signals VALUES (?, ?, ?)", ("sig-lp", "2099-11-05", '{"lp_alert_stage":"confirm"}'))
    conn.execute("INSERT INTO signals VALUES (?, ?, ?)", ("sig-non-lp", "2099-11-06", '{"kind":"regular"}'))
    conn.commit()
finally:
    conn.close()
PY

run_quality() {
  local date="$1"
  local name="$2"
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit_${name}.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/lock_${name}.lock" \
  HERMES_DATA_QUALITY_DB_PATH="$TMP_DIR/lp.sqlite" \
    "$OPS" data-quality --date "$date" >"$TMP_DIR/${name}.out"
}

run_quality 2099-11-01 run_overview
require_out "$TMP_DIR/run_overview.out" "lp_signal_rows=108"
require_out "$TMP_DIR/run_overview.out" "lp_status=ok"
require_out "$TMP_DIR/run_overview.out" "lp_rows_source=run_overview"
forbid_out "$TMP_DIR/run_overview.out" "lp_signal_rows=missing"

run_quality 2099-11-02 summary_rows
require_out "$TMP_DIR/summary_rows.out" "lp_signal_rows=108"
require_out "$TMP_DIR/summary_rows.out" "lp_status=ok"
require_out "$TMP_DIR/summary_rows.out" "lp_rows_source=lp_signal_summary"

run_quality 2099-11-03 delivered_suppressed
require_out "$TMP_DIR/delivered_suppressed.out" "lp_signal_rows=108"
require_out "$TMP_DIR/delivered_suppressed.out" "lp_status=ok"
require_out "$TMP_DIR/delivered_suppressed.out" "lp_rows_source=delivered_plus_suppressed"

run_quality 2099-11-04 suppression_summary
require_out "$TMP_DIR/suppression_summary.out" "lp_signal_rows=108"
require_out "$TMP_DIR/suppression_summary.out" "lp_status=ok"
require_out "$TMP_DIR/suppression_summary.out" "lp_rows_source=lp_suppression_summary"

run_quality 2099-11-05 sqlite_lp
require_out "$TMP_DIR/sqlite_lp.out" "lp_status=report_mapping_missing"
require_out "$TMP_DIR/sqlite_lp.out" "sqlite_lp_like_signals=1"

run_quality 2099-11-06 no_lp
require_out "$TMP_DIR/no_lp.out" "lp_status=no_lp_samples_or_coverage_gap"
require_out "$TMP_DIR/no_lp.out" "sqlite_lp_like_signals=0"

echo "OK: data-quality LP rows tests passed"
