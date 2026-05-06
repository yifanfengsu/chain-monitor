#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
JOBCTL="${REPO_ROOT}/scripts/hermes_cm_jobctl.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_async_jobs_test.XXXXXX")"
FAKE_JOB_ID="cmjob_20260501T120000Z_abcdef12"
JOBCTL_TEST_DATE="2099-12-31"

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
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
    echo "stderr:" >&2
    sed -n '1,80p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected success: $*"
  fi
}

run_fail() {
  local name="$1"
  local rc=0
  shift
  set +e
  "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    echo "stdout:" >&2
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
    echo "stderr:" >&2
    sed -n '1,80p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected failure: $*"
  fi
}

require_output() {
  local name="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} output missing pattern: ${pattern}"
  fi
}

forbid_output() {
  local name="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} output contains forbidden pattern: ${pattern}"
  fi
}

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson"
export HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs"

run_ok daily_flow "$ROUTER" --text '标准日报流程2026-05-01' --dry-run --platform telegram
require_output daily_flow 'submit-daily-flow'
forbid_output daily_flow '"daily-flow", "--date"'

run_fail daily_flow_rerun_missing_confirm "$ROUTER" --text '重新标准日报流程2026-05-01' --dry-run --platform telegram
require_output daily_flow_rerun_missing_confirm '我确认重跑'

run_ok daily_flow_rerun "$ROUTER" --text '重新标准日报流程2026-05-01 我确认重跑' --dry-run --platform telegram
require_output daily_flow_rerun 'submit-daily-flow'
require_output daily_flow_rerun '--force-rerun'

run_ok space_check "$ROUTER" --text '空间检查' --dry-run --platform telegram
require_output space_check 'submit-space-check'

run_ok space_fast "$ROUTER" --text '空间快检' --dry-run --platform telegram
require_output space_fast 'space-fast'

run_ok lock_status "$ROUTER" --text '锁状态' --dry-run --platform telegram
require_output lock_status 'lock-status'

run_ok db_size_diagnose "$ROUTER" --text '数据库体积诊断' --dry-run --platform telegram
require_output db_size_diagnose 'db-size-diagnose'

run_ok weekly_review "$ROUTER" --text '周复盘2026-04-27到2026-05-03' --dry-run --platform telegram
require_output weekly_review 'submit-weekly-review'

run_ok job_status "$ROUTER" --text "任务状态${FAKE_JOB_ID}" --dry-run --platform telegram
require_output job_status 'job-status'

run_ok job_result "$ROUTER" --text "查看结果${FAKE_JOB_ID}" --dry-run --platform telegram
require_output job_result 'job-result'

run_ok job_diagnose "$ROUTER" --text "诊断任务${FAKE_JOB_ID}" --dry-run --platform telegram
require_output job_diagnose 'job-diagnose'

run_ok job_list "$ROUTER" --text '最近任务' --dry-run --platform telegram
require_output job_list 'job-list'

run_fail cancel_missing "$ROUTER" --text "取消任务${FAKE_JOB_ID}" --dry-run --platform telegram
require_output cancel_missing '我确认取消'

run_ok cancel_confirm "$ROUTER" --text "取消任务${FAKE_JOB_ID} 我确认取消" --dry-run --platform telegram
require_output cancel_confirm 'job-cancel'
require_output cancel_confirm '--confirm'

run_ok jobctl_submit env \
  HERMES_JOBCTL_TEST_MODE=1 \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date "$JOBCTL_TEST_DATE"
require_output jobctl_submit 'job_id:'
job_id="$(grep -Eo 'cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+' "$TMP_DIR/jobctl_submit.out" | head -n1)"
[[ -n "$job_id" ]] || fail "jobctl submit did not return job_id"
[[ -f "$HERMES_JOBCTL_JOBS_ROOT/${job_id}/meta.json" ]] || fail "jobctl did not create meta.json"
[[ -f "$HERMES_JOBCTL_JOBS_ROOT/${job_id}/status.json" ]] || fail "jobctl did not create status.json"

python3 - "$HERMES_JOBCTL_JOBS_ROOT/${job_id}/meta.json" <<'PY'
import json
import sys
from pathlib import Path

meta = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if meta.get("schema") != "chain_monitor_hermes_job_v1":
    raise SystemExit("unexpected job schema")
if meta.get("status") not in {"pending", "running", "succeeded"}:
    raise SystemExit(f"unexpected job status: {meta.get('status')}")
if meta.get("kind") != "daily-flow":
    raise SystemExit("unexpected job kind")
if "标准日报流程" in json.dumps(meta, ensure_ascii=False):
    raise SystemExit("raw Telegram text leaked into metadata")
PY

run_ok jobctl_submit_duplicate env \
  HERMES_JOBCTL_TEST_MODE=1 \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date "$JOBCTL_TEST_DATE"
require_output jobctl_submit_duplicate '已有相同任务正在执行'
require_output jobctl_submit_duplicate "$job_id"

python3 "$JOBCTL" __update --job-id "$job_id" --status succeeded --exit-code 0

run_ok jobctl_submit_succeeded_duplicate env \
  HERMES_JOBCTL_TEST_MODE=1 \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date "$JOBCTL_TEST_DATE"
require_output jobctl_submit_succeeded_duplicate '已有成功任务'
require_output jobctl_submit_succeeded_duplicate "查看结果${job_id}"
require_output jobctl_submit_succeeded_duplicate '我确认重跑'

run_ok jobctl_submit_force_rerun env \
  HERMES_JOBCTL_TEST_MODE=1 \
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/jobctl_audit.ndjson" \
  python3 "$JOBCTL" submit --kind daily-flow --date "$JOBCTL_TEST_DATE" --force-rerun
force_job_id="$(grep -Eo 'cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]+' "$TMP_DIR/jobctl_submit_force_rerun.out" | head -n1)"
[[ -n "$force_job_id" ]] || fail "force rerun did not return job_id"
[[ "$force_job_id" != "$job_id" ]] || fail "force rerun reused succeeded job_id"
python3 "$JOBCTL" __update --job-id "$force_job_id" --status cancelled --exit-code -15

MENU_AUDIT="$TMP_DIR/menu_ops_audit.ndjson"
MENU_LOCK="$TMP_DIR/menu_ops.lock"
run_ok menu_execute env \
  HERMES_OPS_AUDIT_LOG="$MENU_AUDIT" \
  HERMES_OPS_LOCK_PATH="$MENU_LOCK" \
  "$ROUTER" --text '命令提示' --execute --platform telegram
require_output menu_execute '任务状态JOB_ID'
require_output menu_execute '查看结果JOB_ID'
require_output menu_execute '诊断任务JOB_ID'
require_output menu_execute '最近任务'
require_output menu_execute '锁状态'
require_output menu_execute '空间快检'
require_output menu_execute '数据库体积诊断'
require_output menu_execute '重新标准日报流程YYYY-MM-DD 我确认重跑'

echo "OK: Hermes async jobs test passed"
