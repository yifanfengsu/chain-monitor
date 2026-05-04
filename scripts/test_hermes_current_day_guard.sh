#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBCTL="${REPO_ROOT}/scripts/hermes_cm_jobctl.py"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_current_day_guard.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_file_text() {
  local path="$1"
  local pattern="$2"
  grep -Fq -- "$pattern" "$path" || fail "${path} missing pattern: ${pattern}"
}

run_refused_submit() {
  local rc=0

  set +e
  HERMES_TEST_BEIJING_TODAY=2026-05-03 \
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
    python3 "$JOBCTL" submit --kind daily-flow --date 2026-05-03 \
      >"$TMP_DIR/refused.out" 2>"$TMP_DIR/refused.err"
  rc=$?
  set -e
  [[ "$rc" -eq 2 ]] || fail "current Beijing date submit rc=${rc}, expected 2"
}

run_ok() {
  local name="$1"
  shift
  if ! "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"; then
    echo "stdout:" >&2
    sed -n '1,120p' "$TMP_DIR/${name}.out" >&2 || true
    echo "stderr:" >&2
    sed -n '1,120p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected success: $*"
  fi
}

run_refused_submit
require_file_text "$TMP_DIR/refused.out" "当前北京时间逻辑日"
require_file_text "$TMP_DIR/refused.out" "00:05"
require_file_text "$TMP_DIR/refused.out" "标准日报流程2026-05-03"
require_file_text "$TMP_DIR/jobctl_audit.ndjson" "current_beijing_date_protected"

run_ok router_dry_run env \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text "标准日报流程2026-05-03" --dry-run --platform telegram
require_file_text "$TMP_DIR/router_dry_run.out" "submit 阶段会拒绝"

set +e
HERMES_TEST_BEIJING_TODAY=2026-05-03 \
HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/router_jobs" \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_execute_audit.ndjson" \
  "$ROUTER" --text "标准日报流程2026-05-03" --execute --platform telegram \
    >"$TMP_DIR/router_execute.out" 2>"$TMP_DIR/router_execute.err"
router_execute_rc=$?
set -e
[[ "$router_execute_rc" -eq 2 ]] || fail "router execute current date rc=${router_execute_rc}, expected 2"
require_file_text "$TMP_DIR/router_execute.err" "当前北京时间逻辑日"
require_file_text "$TMP_DIR/router_execute.err" "00:05"
require_file_text "$TMP_DIR/router_execute.err" "标准日报流程2026-05-03"
require_file_text "$TMP_DIR/router_execute_audit.ndjson" "current_beijing_date_protected"

if [[ -d "$TMP_DIR/router_jobs" ]] && find "$TMP_DIR/router_jobs" -mindepth 1 -maxdepth 1 -type d -name 'cmjob_*' | grep -q .; then
  fail "router execute current Beijing date refusal created a job directory"
fi

if [[ -d "$TMP_DIR/jobs" ]] && find "$TMP_DIR/jobs" -mindepth 1 -maxdepth 1 -type d -name 'cmjob_*' | grep -q .; then
  fail "current Beijing date refusal created a job directory"
fi

run_ok past_submit env \
  HERMES_TEST_BEIJING_TODAY=2026-05-03 \
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date 2026-05-02 --test-no-spawn
past_job_id="$(grep -Eo 'cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+' "$TMP_DIR/past_submit.out" | head -n1)"
[[ -n "$past_job_id" ]] || fail "past submit did not return job_id"
[[ -f "$TMP_DIR/jobs/${past_job_id}/meta.json" ]] || fail "past submit did not create meta.json"
require_file_text "$TMP_DIR/jobs/${past_job_id}/status.json" '"status": "pending"'

python3 - "$TMP_DIR/jobs/${past_job_id}/meta.json" <<'PY'
import json
import sys
from pathlib import Path

meta = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
argv = meta.get("argv")
if not isinstance(argv, list) or "__run-job" not in argv:
    raise SystemExit("safe argv does not contain __run-job")
if argv[:2] != ["./scripts/hermes_cm_ops.sh", "__run-job"]:
    raise SystemExit(f"unexpected safe argv prefix: {argv}")
PY

run_ok current_job_seed env \
  HERMES_ALLOW_CURRENT_BEIJING_DATE=1 \
  HERMES_TEST_BEIJING_TODAY=2026-05-03 \
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date 2026-05-03 --test-no-spawn
current_job_id="$(grep -Eo 'cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+' "$TMP_DIR/current_job_seed.out" | head -n1)"
[[ -n "$current_job_id" ]] || fail "current job seed did not return job_id"

set +e
HERMES_TEST_BEIJING_TODAY=2026-05-03 \
HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/run_job_audit.ndjson" \
HERMES_OPS_JOB_RUNNER_OK=1 \
  "$OPS" __run-job --job-id "$current_job_id" --kind daily-flow --date 2026-05-03 \
    >"$TMP_DIR/run_job.out" 2>"$TMP_DIR/run_job.err"
run_job_rc=$?
set -e
[[ "$run_job_rc" -eq 2 ]] || fail "__run-job current date rc=${run_job_rc}, expected 2"
require_file_text "$TMP_DIR/run_job.err" "当前北京时间逻辑日"
require_file_text "$TMP_DIR/jobs/${current_job_id}/status.json" '"refused_reason": "current_beijing_date_protected"'
require_file_text "$TMP_DIR/jobs/${current_job_id}/status.json" '"failed_substep": "preflight:current_beijing_date_guard"'
require_file_text "$TMP_DIR/jobs/${current_job_id}/status.json" '"timeout_hit": "false"'
require_file_text "$TMP_DIR/jobs/${current_job_id}/result.md" "refused_reason=current_beijing_date_protected"
require_file_text "$TMP_DIR/jobs/${current_job_id}/result.md" "failed_substep=preflight:current_beijing_date_guard"

run_ok diagnose env \
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" diagnose --job-id "$current_job_id"
require_file_text "$TMP_DIR/diagnose.out" "refused_reason=current_beijing_date_protected"
require_file_text "$TMP_DIR/diagnose.out" "北京时间次日 00:05"
require_file_text "$TMP_DIR/diagnose.out" "meta_argv="
require_file_text "$TMP_DIR/diagnose.out" "__run-job"

echo "OK: Hermes current Beijing date guard test passed"
