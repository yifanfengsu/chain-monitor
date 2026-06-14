#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/data_quality_gap_reason.XXXXXX")"
REPORT_DIR="$REPO_ROOT/reports/daily"

cleanup() {
  rm -rf "$TMP_DIR"
  rm -f \
    "$REPORT_DIR/daily_report_2099-12-01.json" \
    "$REPORT_DIR/daily_report_2099-12-02.json" \
    "$REPORT_DIR/daily_report_2099-12-03.json"
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

python3 - "$REPORT_DIR" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

report_dir = Path(sys.argv[1])


def payload(root: str, final: str, status: str, collection_degraded: bool) -> dict:
    summary = {
        "root_diagnosis": root,
        "final_status": final,
        "collection_degraded": collection_degraded,
        "degraded_reason": root,
        "table_time_fields_used": {
            "raw_events": "captured_at" if root != "report_time_field_unavailable" else "unavailable",
            "parsed_events": "parsed_at" if root != "report_time_field_unavailable" else "unavailable",
            "signals": "timestamp",
            "delivery_audit": "archive_written_at",
        },
    }
    warning = final if final.startswith("warning_") else ""
    return {
        "data_quality_summary": {
            "data_quality_status": status,
            "zero_activity_day": False,
            "active_hours": 24,
            "signals_count": 120,
            "raw_events_count": 39002,
            "parsed_events_count": 39002,
            "gap_root_diagnosis": root,
            "gap_final_status": final,
            "collection_degraded": collection_degraded,
            "degraded_reason": root,
            "data_quality_warning": warning,
            "gap_diagnosis_summary": summary,
        },
        "gap_diagnosis_summary": summary,
        "trade_replay_summary": {"trade_replay_available": True, "replay_source": "persisted", "replay_scope": "full"},
        "major_coverage_summary": {"available": True},
        "run_overview": {"lp_signal_rows": 1},
    }


payloads = {
    "2099-12-01": payload("signal_generation_gap", "warning_signal_sparse", "valid", False),
    "2099-12-02": payload("listener_or_audit_gap", "collection_degraded", "degraded", True),
    "2099-12-03": payload("report_time_field_unavailable", "warning_time_field_unavailable", "valid", False),
}
for date, item in payloads.items():
    (report_dir / f"daily_report_{date}.json").write_text(
        json.dumps(item, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
PY

run_quality() {
  local date="$1"
  local name="$2"
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/audit_${name}.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/lock_${name}.lock" \
    "$OPS" data-quality --date "$date" >"$TMP_DIR/${name}.out"
}

run_quality 2099-12-01 signal_gap
require_out "$TMP_DIR/signal_gap.out" "gap_root_diagnosis=signal_generation_gap"
require_out "$TMP_DIR/signal_gap.out" "degraded_reason=signal_generation_gap"
require_out "$TMP_DIR/signal_gap.out" "data_quality_warning=warning_signal_sparse"
forbid_out "$TMP_DIR/signal_gap.out" "listener_runtime_gap"

run_quality 2099-12-02 audit_gap
require_out "$TMP_DIR/audit_gap.out" "data_quality_status=degraded"
require_out "$TMP_DIR/audit_gap.out" "collection_degraded=True"
require_out "$TMP_DIR/audit_gap.out" "gap_final_status=collection_degraded"

run_quality 2099-12-03 time_field_unavailable
require_out "$TMP_DIR/time_field_unavailable.out" "gap_root_diagnosis=report_time_field_unavailable"
require_out "$TMP_DIR/time_field_unavailable.out" "data_quality_warning=warning_time_field_unavailable"
require_out "$TMP_DIR/time_field_unavailable.out" '"raw_events": "unavailable"'

echo "data-quality gap reason tests passed"
