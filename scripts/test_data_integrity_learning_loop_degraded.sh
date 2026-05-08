#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/data_integrity_learning_loop.XXXXXX")"
TEST_DATE="2099-10-01"

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

mkdir -p "$TMP_DIR/archive" "$TMP_DIR/reports/daily" "$TMP_DIR/logs" "$TMP_DIR/bin"

cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
echo "unexpected make call: $*" >&2
exit 99
MAKE
chmod +x "$TMP_DIR/bin/make"

printf '{"fixture":true}\n' >"$TMP_DIR/archive/archive_${TEST_DATE}.ndjson"
cat >"$TMP_DIR/logs/runtime.log" <<'LOG'
2099-10-01T12:01:00+00:00 sqlite_write_failed final write failure
2099-10-01T12:02:00+00:00 sqlite_store warning: database is locked
LOG

python3 - "$TMP_DIR/chain_monitor.sqlite" "$TEST_DATE" <<'PY'
from __future__ import annotations

import datetime as dt
import sqlite3
import sys

db_path = sys.argv[1]
logical_date = sys.argv[2]
start = int(dt.datetime(2099, 10, 1, tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp())
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
    conn.execute("INSERT INTO raw_events VALUES (?, ?, ?)", ("raw-1", logical_date, start + 1))
    conn.execute("INSERT INTO parsed_events VALUES (?, ?, ?)", ("parsed-1", logical_date, start + 2))
    conn.execute("INSERT INTO signals VALUES (?, ?, ?)", ("sig-1", logical_date, start + 3))
    conn.execute("INSERT INTO delivery_audit VALUES (?, ?, ?, ?)", ("delivery-1", logical_date, start + 4, start + 4))
    conn.execute("INSERT INTO telegram_deliveries VALUES (?, ?, ?)", ("tg-1", logical_date, start + 5))
    conn.execute("INSERT INTO trade_opportunities VALUES (?, ?, ?, ?)", ("opp-1", logical_date, start + 6, "BLOCKED"))
    conn.execute("INSERT INTO outcomes VALUES (?, ?, ?, ?, ?)", ("outcome-1", logical_date, start + 7, "completed", start + 67))
    conn.execute("INSERT INTO opportunity_outcomes VALUES (?, ?, ?, ?, ?, ?)", ("opp-outcome-1", logical_date, start + 8, "pending", None, 1))
    conn.execute("INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?)", ("replay-1", logical_date, start + 9, "full", 1, 12.5))
    conn.execute("INSERT INTO trade_replay_profile_daily_stats VALUES (?, ?, ?, ?)", ("profile-1", logical_date, "full", start + 10))
    conn.commit()
finally:
    conn.close()
PY

cat >"$TMP_DIR/reports/daily/daily_report_${TEST_DATE}.json" <<'JSON'
{
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false,
    "db_archive_mirror_match_rate": 0.125,
    "db_archive_mismatch_status": "match_or_unchecked"
  },
  "data_source_summary": {
    "db_archive_mirror_match_rate": 0.125,
    "db_archive_mirror_detail": {
      "raw_events": {"loader": "raw_events", "match_rate": null, "mismatch": null, "sqlite_rows": 1},
      "parsed_events": {"loader": "parsed_events", "match_rate": null, "mismatch": null, "sqlite_rows": 1},
      "signals": {"loader": "signals", "match_rate": 0.0, "mismatch": false, "sqlite_rows": 1, "compressed_archive_rows": 0},
      "delivery_audit": {"loader": "delivery_audit", "match_rate": 0.0, "mismatch": false, "sqlite_rows": 1, "compressed_archive_rows": 0},
      "trade_opportunities": {"loader": "trade_opportunities", "match_rate": 0.0, "mismatch": false, "sqlite_rows": 1, "compressed_archive_rows": 0}
    },
    "sqlite_health": {"archive_mirror": {"skipped": "fast_mode"}}
  },
  "trade_replay_summary": {"replay_source": "persisted", "replay_scope": "full", "replay_count": 1, "valid_replay_count": 1},
  "outcome_diagnosis_summary": {
    "outcomes_completed_rate": 1.0,
    "opportunity_outcomes_completed_rate": 0.0,
    "opportunity_outcomes_past_due_pending": 1
  },
  "candidate_coverage_summary": {"available": true},
  "candidate_frontier_summary": {"available": true},
  "lp_signal_summary": {"available": true, "lp_signal_rows": 1},
  "lp_suppression_sample_replay_summary": {"available": true},
  "major_coverage_summary": {"available": true}
}
JSON

HERMES_DATA_INTEGRITY_DB_PATH="$TMP_DIR/chain_monitor.sqlite" \
HERMES_DATA_INTEGRITY_ARCHIVE_ROOT="$TMP_DIR/archive" \
HERMES_DATA_INTEGRITY_DAILY_DIR="$TMP_DIR/reports/daily" \
HERMES_DATA_INTEGRITY_LOG_ROOTS="$TMP_DIR/logs" \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/lock.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$OPS" data-integrity --date "$TEST_DATE" >"$TMP_DIR/integrity.out"

require_out "$TMP_DIR/integrity.out" "archive_status=present"
require_out "$TMP_DIR/integrity.out" "raw_events: exists=true rows=1"
require_out "$TMP_DIR/integrity.out" "parsed_events: exists=true rows=1"
require_out "$TMP_DIR/integrity.out" "signals: exists=true rows=1"
require_out "$TMP_DIR/integrity.out" "trade_replay_examples: scope_distribution={\"full\": 1}"
require_out "$TMP_DIR/integrity.out" "final_status=degraded"
require_out "$TMP_DIR/integrity.out" "reason=learning_loop_degraded"
require_out "$TMP_DIR/integrity.out" "core_collection_status=complete"
require_out "$TMP_DIR/integrity.out" "collection_degraded=false"
require_out "$TMP_DIR/integrity.out" "learning_loop_degraded=true"
require_out "$TMP_DIR/integrity.out" "sqlite_write_warning=final_write_failure_detected"
require_out "$TMP_DIR/integrity.out" "mirror_rate_warning=low_match_rate_nonblocking_no_mismatch"
require_out "$TMP_DIR/integrity.out" "checked_tables="
require_out "$TMP_DIR/integrity.out" "unchecked_tables=parsed_events,raw_events"
require_out "$TMP_DIR/integrity.out" "mismatched_tables=none"
require_out "$TMP_DIR/integrity.out" "mismatch_by_table={}"
forbid_out "$TMP_DIR/integrity.out" "采集缺失"
forbid_out "$TMP_DIR/integrity.out" "collection_degraded=true"
forbid_out "$TMP_DIR/integrity.out" "final_status=invalid"

echo "OK: data-integrity learning-loop degraded test passed"
