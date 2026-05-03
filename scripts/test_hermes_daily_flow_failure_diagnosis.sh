#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
JOBCTL="${REPO_ROOT}/scripts/hermes_cm_jobctl.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_daily_flow_diag_test.XXXXXX")"
TEST_DATE="2026-05-01"
ROUTER_JOB_ID="cmjob_20260503T090349Z_791e8d8ea814"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
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

require_file_text() {
  local path="$1"
  local pattern="$2"
  grep -Fq -- "$pattern" "$path" || fail "${path} missing pattern: ${pattern}"
}

forbid_file_text() {
  local path="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$path"; then
    fail "${path} contains forbidden pattern: ${pattern}"
  fi
}

mkdir -p "$TMP_DIR/bin" "$TMP_DIR/jobs"

cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
case "${1:-}" in
  daily-close|archive-compress-date|db-vacuum|db-prune-execute|db-compact-execute)
    echo "forbidden fake make target called: $*" >&2
    exit 90
    ;;
  *)
    echo "fake make ok: $*"
    exit 0
    ;;
esac
MAKE
chmod +x "$TMP_DIR/bin/make"

cat >"$TMP_DIR/fake_python" <<'PY'
#!/usr/bin/env bash
if [[ "$*" == *"app.archive_maintenance"* && "$*" == *"--mirror-check-date"* ]]; then
  echo "fake mirror stdout: mismatch detected for $*"
  echo "fake mirror stderr: simulated archive mirror failure" >&2
  exit "${HERMES_FAKE_MIRROR_RC:-1}"
fi
echo "fake python ok: $*"
exit 0
PY
chmod +x "$TMP_DIR/fake_python"

submit_job() {
  local name="$1"
  run_ok "$name" env \
    HERMES_JOBCTL_TEST_MODE=1 \
    HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
    HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
    python3 "$JOBCTL" submit --kind daily-flow --date "$TEST_DATE" --force-rerun
  grep -Eo 'cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+' "$TMP_DIR/${name}.out" | head -n1
}

run_daily_job() {
  local name="$1"
  local job_id="$2"
  local mirror_rc="$3"
  local rc=0

  set +e
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/${name}_ops_audit.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/${name}.lock" \
  HERMES_OPS_ROUTER_OK=1 \
  HERMES_OPS_JOB_RUNNER_OK=1 \
  HERMES_OPS_PYTHON_BIN="$TMP_DIR/fake_python" \
  HERMES_FAKE_MIRROR_RC="$mirror_rc" \
  PATH="$TMP_DIR/bin:$PATH" \
    "$OPS" __run-job --job-id "$job_id" --kind daily-flow --date "$TEST_DATE" \
      >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
  rc=$?
  set -e
  [[ "$rc" -ne 0 ]] || fail "${name} unexpectedly succeeded"
}

job_id="$(submit_job submit_fail_1)"
run_daily_job daily_fail_1 "$job_id" 1
result="$TMP_DIR/jobs/${job_id}/result.md"
status="$TMP_DIR/jobs/${job_id}/status.json"
[[ -f "$result" ]] || fail "missing result.md for ${job_id}"
[[ -f "$status" ]] || fail "missing status.json for ${job_id}"

require_file_text "$result" "failed_substep=daily-close:archive_mirror_check"
require_file_text "$result" "failed_command="
require_file_text "$result" "app.archive_maintenance"
require_file_text "$result" "exit_code=1"
require_file_text "$result" "timeout_hit=false"
require_file_text "$result" "stdout_tail:"
require_file_text "$result" "stderr_tail:"
require_file_text "$status" '"failed_substep": "daily-close:archive_mirror_check"'
require_file_text "$status" '"failed_command":'
forbid_file_text "$result" "每日收尾YYYY-MM-DD 我确认压缩"
forbid_file_text "$result" "daily-close 是最后一步"
forbid_file_text "$result" "前面的步骤可能已完成"
forbid_file_text "$result" "前面的步骤可能已经完成"

run_ok diagnose env \
  HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs" \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/diagnose_ops_audit.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/diagnose.lock" \
  HERMES_OPS_ROUTER_OK=1 \
  "$OPS" job-diagnose --job-id "$job_id"
require_file_text "$TMP_DIR/diagnose.out" "failed_substep=daily-close:archive_mirror_check"
require_file_text "$TMP_DIR/diagnose.out" "下一步建议"

timeout_job_id="$(submit_job submit_timeout)"
run_daily_job daily_timeout "$timeout_job_id" 124
timeout_result="$TMP_DIR/jobs/${timeout_job_id}/result.md"
require_file_text "$timeout_result" "failed_substep=daily-close:archive_mirror_check"
require_file_text "$timeout_result" "exit_code=124"
require_file_text "$timeout_result" "timeout_hit=true"
require_file_text "$timeout_result" "failure_reason=命令超时"

run_ok router_diagnose env \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text "诊断任务${ROUTER_JOB_ID}" --dry-run --platform telegram
require_file_text "$TMP_DIR/router_diagnose.out" '"job-diagnose"'
require_file_text "$TMP_DIR/router_diagnose.out" '"--job-id"'
require_file_text "$TMP_DIR/router_diagnose.out" "$ROUTER_JOB_ID"

run_ok router_diagnose_alt env \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text "任务诊断${ROUTER_JOB_ID}" --dry-run --platform telegram
require_file_text "$TMP_DIR/router_diagnose_alt.out" '"job-diagnose"'

echo "OK: Hermes daily-flow failure diagnosis test passed"
