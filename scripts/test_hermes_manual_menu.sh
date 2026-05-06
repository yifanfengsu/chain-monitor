#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_manual_menu_test.XXXXXX")"

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
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
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
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
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

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson"

run_ok command_menu --text '命令提示' --dry-run --platform telegram
require_out command_menu 'command-menu'

run_ok system_health --text '系统体检' --dry-run --platform telegram
require_out system_health 'system-health'

run_ok lock_status --text '锁状态' --dry-run --platform telegram
require_out lock_status 'lock-status'

run_ok lock_check --text '锁检查' --dry-run --platform telegram
require_out lock_check 'lock-status'

run_ok listener_health --text '监听器体检' --dry-run --platform telegram
require_out listener_health 'listener-health'

run_ok daily_flow --text '标准日报流程2026-05-01' --dry-run --platform telegram
require_out daily_flow 'submit-daily-flow'
require_out daily_flow '--date'
require_out daily_flow '2026-05-01'

run_fail daily_flow_rerun_missing_confirm --text '重新标准日报流程2026-05-01' --dry-run --platform telegram
require_out daily_flow_rerun_missing_confirm '我确认重跑'

run_ok daily_flow_rerun --text '重新标准日报流程2026-05-01 我确认重跑' --dry-run --platform telegram
require_out daily_flow_rerun 'submit-daily-flow'
require_out daily_flow_rerun '--force-rerun'

run_ok replay_check --text '检查回放2026-05-01' --dry-run --platform telegram
require_out replay_check 'replay-check'
require_out replay_check '--date'
require_out replay_check '2026-05-01'

run_ok data_quality --text '数据质量2026-05-01' --dry-run --platform telegram
require_out data_quality 'data-quality'
require_out data_quality '--date'
require_out data_quality '2026-05-01'

run_ok profile_review --text 'Profile复盘2026-05-01' --dry-run --platform telegram
require_out profile_review 'profile-review'
require_out profile_review '--date'
require_out profile_review '2026-05-01'

run_ok blocker_review --text 'Blocker复盘2026-05-01' --dry-run --platform telegram
require_out blocker_review 'blocker-review'
require_out blocker_review '--date'
require_out blocker_review '2026-05-01'

run_ok shadow_review --text 'Shadow复盘2026-05-01' --dry-run --platform telegram
require_out shadow_review 'shadow-review'
require_out shadow_review '--date'
require_out shadow_review '2026-05-01'

run_ok learning_review --text '学习复盘2026-05-04' --dry-run --platform telegram
require_out learning_review 'learning-review'
require_out learning_review '--date'
require_out learning_review '2026-05-04'

run_ok daily_learning_review --text '每日学习2026-05-04' --dry-run --platform telegram
require_out daily_learning_review 'learning-review'

run_ok learning_summary --text '学习总结2026-05-04' --dry-run --platform telegram
require_out learning_summary 'learning-review'

run_ok today_learning_review --text '今日学习复盘2026-05-04' --dry-run --platform telegram
require_out today_learning_review 'learning-review'

run_fail learning_relative --text '学习复盘昨天' --dry-run --platform telegram
require_out learning_relative 'YYYY-MM-DD'

run_ok candidate_coverage --text 'CANDIDATE覆盖诊断2026-05-04' --dry-run --platform telegram
require_out candidate_coverage 'candidate-coverage'
require_out candidate_coverage '--date'
require_out candidate_coverage '2026-05-04'

run_ok candidate_coverage_alias --text '候选覆盖诊断2026-05-04' --dry-run --platform telegram
require_out candidate_coverage_alias 'candidate-coverage'

run_ok candidate_coverage_short --text '候选覆盖2026-05-04' --dry-run --platform telegram
require_out candidate_coverage_short 'candidate-coverage'

run_fail candidate_relative --text '候选覆盖昨天' --dry-run --platform telegram
require_out candidate_relative 'YYYY-MM-DD'

run_ok schema_check --text '日报结构检查2026-05-04' --dry-run --platform telegram
require_out schema_check 'daily-report-schema-check'
require_out schema_check '--date'
require_out schema_check '2026-05-04'

run_fail schema_relative --text '日报结构检查昨天' --dry-run --platform telegram
require_out schema_relative 'YYYY-MM-DD'

run_ok outcome_diagnose --text 'Outcome闭环诊断2026-05-04' --dry-run --platform telegram
require_out outcome_diagnose 'outcome-diagnose'
require_out outcome_diagnose '--date'
require_out outcome_diagnose '2026-05-04'

run_ok outcome_diagnose_cn --text '后验闭环诊断2026-05-04' --dry-run --platform telegram
require_out outcome_diagnose_cn 'outcome-diagnose'

run_ok outcome_diagnose_result --text '结果闭环诊断2026-05-04' --dry-run --platform telegram
require_out outcome_diagnose_result 'outcome-diagnose'

run_ok outcome_catchup --text 'Outcome补全预检2026-05-04' --dry-run --platform telegram
require_out outcome_catchup 'outcome-catchup'
require_out outcome_catchup '--dry-run'

run_ok outcome_catchup_cn --text '后验补全预检2026-05-04' --dry-run --platform telegram
require_out outcome_catchup_cn 'outcome-catchup'

run_fail outcome_relative --text '后验闭环诊断昨天' --dry-run --platform telegram
require_out outcome_relative 'YYYY-MM-DD'

run_ok lp_sample --text 'LP抑制抽样预检2026-05-04' --dry-run --platform telegram
require_out lp_sample 'lp-suppression-sample-replay'
require_out lp_sample '--dry-run'

run_ok lp_diagnose --text 'LP诊断2026-05-04' --dry-run --platform telegram
require_out lp_diagnose 'lp-diagnose'
require_out lp_diagnose '--date'
require_out lp_diagnose '2026-05-04'

run_ok lp_diagnose_alias --text 'LP信号诊断2026-05-04' --dry-run --platform telegram
require_out lp_diagnose_alias 'lp-diagnose'

run_ok pool_diagnose_alias --text '池子诊断2026-05-04' --dry-run --platform telegram
require_out pool_diagnose_alias 'lp-diagnose'

run_ok clmm_diagnose_alias --text 'CLMM诊断2026-05-04' --dry-run --platform telegram
require_out clmm_diagnose_alias 'lp-diagnose'

run_fail lp_diagnose_relative --text 'LP诊断昨天' --dry-run --platform telegram
require_out lp_diagnose_relative 'YYYY-MM-DD'

run_fail lp_sample_relative --text 'LP抑制抽样预检昨天' --dry-run --platform telegram
require_out lp_sample_relative 'YYYY-MM-DD'

run_ok space_check --text '空间检查' --dry-run --platform telegram
require_out space_check 'submit-space-check'

run_ok space_fast --text '空间快检' --dry-run --platform telegram
require_out space_fast 'space-fast'

run_ok db_size_diagnose --text '数据库体积诊断' --dry-run --platform telegram
require_out db_size_diagnose 'db-size-diagnose'

run_ok db_slim_dry_run --text '数据库瘦身预检' --dry-run --platform telegram
require_out db_slim_dry_run 'db-slim-dry-run'

run_ok db_slim_alias --text 'DB瘦身预检' --dry-run --platform telegram
require_out db_slim_alias 'db-slim-dry-run'

run_ok db_cleanup_alias --text '数据库清理预检' --dry-run --platform telegram
require_out db_cleanup_alias 'db-slim-dry-run'

run_ok archive_check --text '归档压缩预检2026-05-01' --dry-run --platform telegram
require_out archive_check 'submit-archive-compress-check'
require_out archive_check '--date'
require_out archive_check '2026-05-01'
forbid_out archive_check 'archive-compress-date'

run_ok weekly_review --text '周复盘2026-04-27到2026-05-03' --dry-run --platform telegram
require_out weekly_review 'submit-weekly-review'
require_out weekly_review '--start'
require_out weekly_review '2026-04-27'
require_out weekly_review '--end'
require_out weekly_review '2026-05-03'

run_ok job_status --text '任务状态cmjob_20260501T120000Z_abcdef12' --dry-run --platform telegram
require_out job_status 'job-status'

run_ok job_result --text '查看结果cmjob_20260501T120000Z_abcdef12' --dry-run --platform telegram
require_out job_result 'job-result'

run_ok job_log --text '查看日志cmjob_20260501T120000Z_abcdef12' --dry-run --platform telegram
require_out job_log 'job-log'

run_ok job_diagnose --text '诊断任务cmjob_20260503T090349Z_791e8d8ea814' --dry-run --platform telegram
require_out job_diagnose 'job-diagnose'

run_ok job_list --text '最近任务' --dry-run --platform telegram
require_out job_list 'job-list'

run_fail job_cancel_missing_confirm --text '取消任务cmjob_20260501T120000Z_abcdef12' --dry-run --platform telegram
require_out job_cancel_missing_confirm '我确认取消'

run_ok job_cancel --text '取消任务cmjob_20260501T120000Z_abcdef12 我确认取消' --dry-run --platform telegram
require_out job_cancel 'job-cancel'
require_out job_cancel '--confirm'

run_fail relative_daily --text '标准日报流程昨天' --dry-run --platform telegram
require_out relative_daily 'YYYY-MM-DD'

run_fail relative_weekly --text '周复盘上周' --dry-run --platform telegram

run_ok analyze_fast --text '分析报告2026-05-01' --dry-run --platform telegram
require_out analyze_fast 'analyze'
require_out analyze_fast '--mode'
require_out analyze_fast 'fast'

run_ok analyze_deep --text '深度分析报告2026-05-01' --dry-run --platform telegram
require_out analyze_deep 'analyze'
require_out analyze_deep '--mode'
require_out analyze_deep 'deep'

run_fail analyze_relative --text '分析昨天的报告' --dry-run --platform telegram
require_out analyze_relative 'YYYY-MM-DD'

MENU_AUDIT="$TMP_DIR/menu_ops_audit.ndjson"
MENU_LOCK="$TMP_DIR/menu_ops.lock"
HERMES_OPS_AUDIT_LOG="$MENU_AUDIT" \
HERMES_OPS_LOCK_PATH="$MENU_LOCK" \
  "$ROUTER" --text '命令提示' --execute --platform telegram >"$TMP_DIR/menu_execute.out" 2>"$TMP_DIR/menu_execute.err"
grep -Fq '系统体检' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 系统体检"
grep -Fq '锁状态' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 锁状态"
grep -Fq '标准日报流程YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 标准日报流程YYYY-MM-DD"
grep -Fq '周复盘START到END' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 周复盘START到END"
grep -Fq '任务状态JOB_ID' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 任务状态JOB_ID"
grep -Fq '查看结果JOB_ID' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 查看结果JOB_ID"
grep -Fq '查看日志JOB_ID' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 查看日志JOB_ID"
grep -Fq '诊断任务JOB_ID' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 诊断任务JOB_ID"
grep -Fq '最近任务' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 最近任务"
grep -Fq '空间快检' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 空间快检"
grep -Fq '数据库体积诊断' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 数据库体积诊断"
grep -Fq '数据库瘦身预检' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 数据库瘦身预检"
grep -Fq '学习复盘YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 学习复盘YYYY-MM-DD"
grep -Fq '学习总结YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 学习总结YYYY-MM-DD"
grep -Fq 'CANDIDATE覆盖诊断YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing CANDIDATE覆盖诊断YYYY-MM-DD"
grep -Fq 'LP抑制抽样预检YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing LP抑制抽样预检YYYY-MM-DD"
grep -Fq 'LP诊断YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing LP诊断YYYY-MM-DD"
grep -Fq '生成日报YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 生成日报YYYY-MM-DD"
grep -Fq '深度分析报告YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 深度分析报告YYYY-MM-DD"
grep -Fq '生成摘要YYYY-MM-DD 快速' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 生成摘要YYYY-MM-DD 快速"
grep -Fq '生成摘要YYYY-MM-DD 深度' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 生成摘要YYYY-MM-DD 深度"
grep -Fq '重新标准日报流程YYYY-MM-DD 我确认重跑' "$TMP_DIR/menu_execute.out" || fail "menu execute missing rerun command"
grep -Fq 'command-menu' "$MENU_AUDIT" || grep -Fq 'command-menu' "$HERMES_OPS_AUDIT_LOG" || fail "audit missing command-menu"

echo "OK: Hermes manual menu router test passed"
