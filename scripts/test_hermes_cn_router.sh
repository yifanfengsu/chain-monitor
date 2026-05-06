#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_cn_router_test.XXXXXX")"

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
  if ! "$ROUTER" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"; then
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
  "$ROUTER" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
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

require_out() {
  local name="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} output missing pattern: ${pattern}"
  fi
}

forbid_out() {
  local name="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} output contains forbidden pattern: ${pattern}"
  fi
}

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson"

run_ok fast --text '分析报告2026-05-01' --dry-run
require_out fast '"allowed": true'
require_out fast '"analyze"'
require_out fast '"--date"'
require_out fast '"2026-05-01"'
require_out fast '"--mode"'
require_out fast '"fast"'
forbid_out fast '"--auto-build"'

run_ok deep --text '深度分析报告2026-05-01' --dry-run
require_out deep '"--mode"'
require_out deep '"deep"'

run_ok lock_status --text '锁状态' --dry-run
require_out lock_status 'lock-status'

run_ok autobuild --text '构建并分析报告2026-05-01 快速' --dry-run
require_out autobuild '"--auto-build"'

run_ok daily_flow_submit --text '标准日报流程2026-05-01' --dry-run
require_out daily_flow_submit 'submit-daily-flow'
forbid_out daily_flow_submit '"daily-flow"'

run_ok learning_review --text '学习复盘2026-05-04' --dry-run
require_out learning_review 'learning-review'
require_out learning_review '"--date"'
require_out learning_review '"2026-05-04"'

run_ok learning_summary --text '学习总结2026-05-04' --dry-run
require_out learning_summary 'learning-review'
require_out learning_summary '"--date"'
require_out learning_summary '"2026-05-04"'

run_ok candidate_coverage --text 'CANDIDATE覆盖诊断2026-05-04' --dry-run
require_out candidate_coverage 'candidate-coverage'
require_out candidate_coverage '"--date"'
require_out candidate_coverage '"2026-05-04"'

run_ok schema_check --text '日报结构检查2026-05-04' --dry-run
require_out schema_check 'daily-report-schema-check'
require_out schema_check '"--date"'
require_out schema_check '"2026-05-04"'

run_ok outcome_diagnose --text 'Outcome闭环诊断2026-05-04' --dry-run
require_out outcome_diagnose 'outcome-diagnose'
require_out outcome_diagnose '"--date"'
require_out outcome_diagnose '"2026-05-04"'

run_ok outcome_diagnose_cn --text '后验闭环诊断2026-05-04' --dry-run
require_out outcome_diagnose_cn 'outcome-diagnose'

run_ok outcome_diagnose_result --text '结果闭环诊断2026-05-04' --dry-run
require_out outcome_diagnose_result 'outcome-diagnose'

run_ok lp_diagnose --text 'LP诊断2026-05-04' --dry-run
require_out lp_diagnose 'lp-diagnose'
require_out lp_diagnose '"--date"'
require_out lp_diagnose '"2026-05-04"'

run_ok lp_signal_diagnose --text 'LP信号诊断2026-05-04' --dry-run
require_out lp_signal_diagnose 'lp-diagnose'

run_ok job_status --text '任务状态cmjob_20260501T120000Z_abcdef12' --dry-run
require_out job_status 'job-status'

run_ok job_diagnose --text '诊断任务cmjob_20260503T090349Z_791e8d8ea814' --dry-run
require_out job_diagnose 'job-diagnose'

run_fail relative --text '分析昨天的报告' --dry-run
require_out relative '绝对日期'
require_out relative 'YYYY-MM-DD'
forbid_out relative 'hermes_cm_ops.sh'
[[ -f "$HERMES_OPS_AUDIT_LOG" ]] || fail "audit log was not generated"
grep -Fq 'relative_date_forbidden' "$HERMES_OPS_AUDIT_LOG" || fail "audit missing relative_date_forbidden"
grep -Fq 'original_text_hash' "$HERMES_OPS_AUDIT_LOG" || fail "audit missing original_text_hash"
if grep -Fq '分析昨天的报告' "$HERMES_OPS_AUDIT_LOG"; then
  fail "audit leaked raw relative-date command"
fi

run_fail today --text '生成今天的日报' --dry-run
require_out today '绝对日期'

run_fail learning_relative --text '学习复盘昨天' --dry-run
require_out learning_relative '绝对日期'
require_out learning_relative 'YYYY-MM-DD'
forbid_out learning_relative 'learning-review'

run_fail candidate_relative --text '候选覆盖昨天' --dry-run
require_out candidate_relative '绝对日期'
require_out candidate_relative 'YYYY-MM-DD'
forbid_out candidate_relative 'candidate-coverage'

run_fail outcome_relative --text 'Outcome闭环诊断昨天' --dry-run
require_out outcome_relative '绝对日期'
require_out outcome_relative 'YYYY-MM-DD'
forbid_out outcome_relative 'outcome-diagnose'

run_fail lp_relative --text 'LP诊断昨天' --dry-run
require_out lp_relative '绝对日期'
require_out lp_relative 'YYYY-MM-DD'
forbid_out lp_relative 'lp-diagnose'

run_fail digest_missing_mode --text '生成摘要2026-05-01' --dry-run
require_out digest_missing_mode '快速'
require_out digest_missing_mode '深度'

run_fail invalid_date --text '分析报告2026-02-30' --dry-run
require_out invalid_date 'invalid_date'

run_fail dangerous --text '帮我运行 make run' --dry-run
require_out dangerous '已拒绝'

python3 - "$HERMES_OPS_AUDIT_LOG" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    lines = [line for line in handle if line.strip()]
if not lines:
    raise SystemExit("empty audit log")
for line in lines:
    event = json.loads(line)
    if event.get("schema") != "chain_monitor_hermes_cn_router_audit_v1":
        raise SystemExit("unexpected router audit schema")
PY

bash "$REPO_ROOT/scripts/test_hermes_manual_menu.sh"

echo "OK: Hermes Chinese router test passed"
