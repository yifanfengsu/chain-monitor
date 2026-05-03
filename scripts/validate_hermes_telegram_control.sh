#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKILL="${REPO_ROOT}/hermes_skills/chain-monitor-report-analyst/SKILL.md"
TELEGRAM_REF="${REPO_ROOT}/hermes_skills/chain-monitor-report-analyst/references/telegram_control.md"
CATALOG="${REPO_ROOT}/docs/hermes_telegram_command_catalog.md"
DOC="${REPO_ROOT}/docs/hermes_telegram_daily_ops.md"
E2E_DOC="${REPO_ROOT}/docs/hermes_telegram_e2e_deployment.md"
WRAPPER="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
JOBCTL="${REPO_ROOT}/scripts/hermes_cm_jobctl.py"
ROUTER_TEST="${REPO_ROOT}/scripts/test_hermes_cn_router.sh"
MANUAL_TEST="${REPO_ROOT}/scripts/test_hermes_manual_menu.sh"
INSTALL_SCRIPT="${REPO_ROOT}/scripts/install_hermes_skill.sh"
E2E_SMOKE="${REPO_ROOT}/scripts/hermes_telegram_e2e_smoke.sh"
RUNTIME_CHECK="${REPO_ROOT}/scripts/hermes_gateway_runtime_check.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  echo "PASS: Hermes Telegram Chinese control validation passed"
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing file: ${path#"$REPO_ROOT"/}"
}

require_text() {
  local path="$1"
  local pattern="$2"
  local label="${3:-$2}"
  if ! grep -Fq -- "$pattern" "$path"; then
    fail "missing ${label} in ${path#"$REPO_ROOT"/}: ${pattern}"
  fi
}

require_any_text() {
  local path="$1"
  shift
  local pattern
  for pattern in "$@"; do
    if grep -Fq -- "$pattern" "$path"; then
      return 0
    fi
  done
  fail "missing any required pattern in ${path#"$REPO_ROOT"/}: $*"
}

for path in "$SKILL" "$TELEGRAM_REF" "$CATALOG" "$DOC" "$E2E_DOC" "$WRAPPER" "$ROUTER" "$JOBCTL" "$ROUTER_TEST" "$MANUAL_TEST" "$INSTALL_SCRIPT" "$E2E_SMOKE" "$RUNTIME_CHECK"; do
  require_file "$path"
done

for pattern in \
  "subprocess.Popen" \
  "shell=False" \
  "start_new_session=True" \
  "meta.json" \
  "status.json" \
  "result.md" \
  "cmjob_" \
  "submit" \
  "status" \
  "list" \
  "result" \
  "log" \
  "diagnose" \
  "cancel"
do
  require_text "$JOBCTL" "$pattern" "jobctl source content"
done

for pattern in \
  "命令提示" \
  "系统体检" \
  "监听器体检" \
  "标准日报流程YYYY-MM-DD" \
  "分析报告YYYY-MM-DD" \
  "检查回放YYYY-MM-DD" \
  "数据质量YYYY-MM-DD" \
  "Profile复盘YYYY-MM-DD" \
  "Blocker复盘YYYY-MM-DD" \
  "Shadow复盘YYYY-MM-DD" \
  "空间检查" \
  "后台任务" \
  "job_id" \
  "任务状态JOB_ID" \
  "查看结果JOB_ID" \
  "查看日志JOB_ID" \
  "诊断任务JOB_ID" \
  "最近任务" \
  "空间快检" \
  "归档压缩预检YYYY-MM-DD" \
  "周复盘START到END" \
  "不支持 今天/昨天/前天" \
  "不提供交易建议" \
  "Telegram -> Hermes gateway -> /chain-monitor-report-analyst"
do
  require_text "$CATALOG" "$pattern" "command catalog content"
done

for pattern in \
  "中文 Telegram Command Router - Mandatory" \
  "中文 Telegram 手动控制菜单" \
  "命令提示" \
  "12 个功能" \
  "~/.hermes/bin/chain-monitor-cn-router" \
  "不得直接调用 make" \
  "不得直接调用 hermes_cm_ops.sh 的 dated operations" \
  "不得自动生成日报" \
  "不得解析昨天" \
  "不得把 今天/昨天/前天 转换为具体日期" \
  "不得运行 TZ=Asia/Shanghai date" \
  "Long Running Commands Must Be Submitted As Jobs" \
  "job_id" \
  "任务状态" \
  "查看结果" \
  "不得同步执行" \
  "不给交易建议" \
  "不输出原始地址" \
  "/chain-monitor-report-analyst 标准日报流程2026-05-01" \
  "/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03"
do
  require_text "$SKILL" "$pattern" "SKILL manual menu content"
done

for pattern in \
  "所有中文 Telegram 命令必须先经过" \
  "本文件是 router grammar 参考" \
  "命令提示" \
  "相对日期必须拒绝" \
  "command-menu" \
  "system-health" \
  "listener-health" \
  "daily-flow" \
  "submit-daily-flow" \
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "submit-space-check" \
  "space-fast" \
  "archive-compress-check" \
  "submit-archive-compress-check" \
  "weekly-review" \
  "submit-weekly-review" \
  "job-status" \
  "job-result" \
  "job-diagnose" \
  "job-list" \
  "普通“分析报告YYYY-MM-DD”不得自动生成日报" \
  "只有“构建并分析报告YYYY-MM-DD 快速/深度”才允许 auto-build"
do
  require_text "$TELEGRAM_REF" "$pattern" "telegram_control grammar content"
done

for pattern in \
  "command-menu" \
  "system-health" \
  "listener-health" \
  "daily-flow" \
  "submit-daily-flow" \
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "submit-space-check" \
  "space-fast" \
  "archive-compress-check" \
  "submit-archive-compress-check" \
  "weekly-review" \
  "submit-weekly-review" \
  "job-status" \
  "job-result" \
  "job-list" \
  "job-diagnose" \
  "shell=False" \
  "datetime.date.fromisoformat" \
  "relative_date_forbidden" \
  "HERMES_OPS_ROUTER_OK" \
  "HERMES_OPS_ORIGINAL_COMMAND_HASH" \
  "subprocess.run(" \
  "chain_monitor_hermes_cn_router_audit_v1"
do
  require_text "$ROUTER" "$pattern" "router source content"
done

for pattern in \
  "command-menu" \
  "system-health" \
  "listener-health" \
  "daily-flow" \
  "submit-daily-flow" \
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "submit-space-check" \
  "space-fast" \
  "archive-compress-check" \
  "submit-archive-compress-check" \
  "weekly-review" \
  "submit-weekly-review" \
  "job-status" \
  "job-list" \
  "job-result" \
  "job-log" \
  "job-diagnose" \
  "job-cancel" \
  "__run-job" \
  "HERMES_OPS_JOB_RUNNER_OK" \
  "job_runner_required" \
  "make daily-close" \
  "make trade-replay-full" \
  "make report-daily-date" \
  "make daily-compare" \
  "make sqlite-checkpoint" \
  "make db-export-operational-payloads-dry-run"
do
  require_text "$WRAPPER" "$pattern" "wrapper command handler"
done

for wrapper_argv in \
  "./scripts/hermes_cm_ops.sh command-menu" \
  "./scripts/hermes_cm_ops.sh system-health" \
  "./scripts/hermes_cm_ops.sh listener-health" \
  "./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh submit-space-check" \
  "./scripts/hermes_cm_ops.sh space-fast" \
  "./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh submit-weekly-review --start YYYY-MM-DD --end YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID" \
  "./scripts/hermes_cm_ops.sh job-list" \
  "./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm" \
  "./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode fast" \
  "./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD --mode deep" \
  "./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD --mode fast" \
  "./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress"
do
  require_text "$TELEGRAM_REF" "$wrapper_argv" "router validated argv"
  require_text "$SKILL" "$wrapper_argv" "SKILL wrapper internal argv"
done

for pattern in \
  "命令提示" \
  "手动控制菜单" \
  "后台任务模式" \
  "标准日报流程YYYY-MM-DD" \
  "任务状态" \
  "查看结果" \
  "诊断任务" \
  "不支持相对日期" \
  "不开放重启" \
  "tail -n 20 reports/hermes/ops_audit.ndjson"
do
  require_text "$DOC" "$pattern" "daily ops doc content"
done

for pattern in \
  "第 5 步验收" \
  "第 5.1 步验收" \
  "/chain-monitor-report-analyst 命令提示" \
  "/chain-monitor-report-analyst 系统体检" \
  "/chain-monitor-report-analyst 监听器体检" \
  "/chain-monitor-report-analyst 标准日报流程2026-05-01" \
  "/chain-monitor-report-analyst 任务状态cmjob_" \
  "/chain-monitor-report-analyst 查看结果cmjob_" \
  "/chain-monitor-report-analyst 诊断任务cmjob_" \
  "/chain-monitor-report-analyst 空间快检" \
  "/chain-monitor-report-analyst 周复盘2026-04-27到2026-05-03" \
  "命令提示”没有返回完整菜单" \
  "分析昨天的报告" \
  "raw make 执行建议" \
  "TZ=Asia/Shanghai date"
do
  require_text "$E2E_DOC" "$pattern" "E2E deployment doc content"
done

for pattern in \
  "references" \
  "telegram_control.md" \
  "chain-monitor-cn-router" \
  "SKILL.md" \
  "rsync -a --delete" \
  "命令提示" \
  "标准日报流程昨天" \
  "标准日报流程2026-05-01" \
  "长任务现在会返回 job_id" \
  "任务状态cmjob_" \
  "查看结果cmjob_"
do
  require_text "$INSTALL_SCRIPT" "$pattern" "install content"
done
require_any_text "$INSTALL_SCRIPT" "~/.hermes/bin" '${HOME}/.hermes/bin' '$HOME/.hermes/bin'

for forbidden in \
  "make run" \
  "make run-research" \
  "make db-compact-execute" \
  "make db-export-operational-payloads-execute" \
  "make db-vacuum" \
  "make db-prune-execute" \
  "systemctl" \
  "rm -rf" \
  "kill -9" \
  "bash -c" \
  "sh -c" \
  "python -c" \
  "curl | sh" \
  "wget | sh"
do
  if grep -Fq -- "$forbidden" "$ROUTER"; then
    fail "router source contains forbidden generated command text: ${forbidden}"
  fi
done

for forbidden in \
  "db-export-operational-payloads-execute" \
  "db-export-operational-payloads-table-execute"
do
  if grep -Fq -- "$forbidden" "$WRAPPER" "$ROUTER"; then
    fail "Telegram control exposes forbidden operational payload export execute: ${forbidden}"
  fi
done

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_telegram_validate.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "命令提示" --dry-run --platform telegram >"$TMP_DIR/menu.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "command-menu"' "$TMP_DIR/menu.out" || fail "router command-menu argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "标准日报流程2026-05-01" --dry-run --platform telegram >"$TMP_DIR/daily.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "submit-daily-flow"' "$TMP_DIR/daily.out" || fail "router daily-flow argv is not async submit"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "重新标准日报流程2026-05-01 我确认重跑" --dry-run --platform telegram >"$TMP_DIR/daily_rerun.out"
grep -Fq '"--force-rerun"' "$TMP_DIR/daily_rerun.out" || fail "router daily-flow rerun is not force-rerun"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "周复盘2026-04-27到2026-05-03" --dry-run --platform telegram >"$TMP_DIR/weekly.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "submit-weekly-review"' "$TMP_DIR/weekly.out" || fail "router weekly-review argv is not async submit"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "空间快检" --dry-run --platform telegram >"$TMP_DIR/space_fast.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "space-fast"' "$TMP_DIR/space_fast.out" || fail "router space-fast argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "数据库体积诊断" --dry-run --platform telegram >"$TMP_DIR/db_size.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "db-size-diagnose"' "$TMP_DIR/db_size.out" || fail "router db-size-diagnose argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "任务状态cmjob_20260501T120000Z_abcdef12" --dry-run --platform telegram >"$TMP_DIR/job_status.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "job-status"' "$TMP_DIR/job_status.out" || fail "router job-status argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "诊断任务cmjob_20260503T090349Z_791e8d8ea814" --dry-run --platform telegram >"$TMP_DIR/job_diagnose.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "job-diagnose"' "$TMP_DIR/job_diagnose.out" || fail "router job-diagnose argv is not wrapper-only"

for path in "$SKILL" "$TELEGRAM_REF" "$CATALOG" "$DOC" "$E2E_DOC"; do
  require_text "$path" "每日收尾YYYY-MM-DD 我确认压缩”不是 ordinary daily-flow failure 的修复方法" "daily-flow failure compression warning"
done

for path in "$SKILL" "$TELEGRAM_REF" "$CATALOG" "$DOC" "$E2E_DOC" "$WRAPPER" "$JOBCTL"; do
  if grep -Fq -- "建议执行 每日收尾YYYY-MM-DD 我确认压缩" "$path"; then
    fail "ordinary failure template suggests confirmed compression in ${path#"$REPO_ROOT"/}"
  fi
done

pass
