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

for path in "$SKILL" "$TELEGRAM_REF" "$CATALOG" "$DOC" "$E2E_DOC" "$WRAPPER" "$ROUTER" "$ROUTER_TEST" "$MANUAL_TEST" "$INSTALL_SCRIPT" "$E2E_SMOKE" "$RUNTIME_CHECK"; do
  require_file "$path"
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
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "archive-compress-check" \
  "weekly-review" \
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
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "archive-compress-check" \
  "weekly-review" \
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
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "archive-compress-check" \
  "weekly-review" \
  "make daily-close" \
  "make trade-replay-full" \
  "make report-daily-date" \
  "make daily-compare" \
  "make sqlite-checkpoint"
do
  require_text "$WRAPPER" "$pattern" "wrapper command handler"
done

for wrapper_argv in \
  "./scripts/hermes_cm_ops.sh command-menu" \
  "./scripts/hermes_cm_ops.sh system-health" \
  "./scripts/hermes_cm_ops.sh listener-health" \
  "./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh space-check" \
  "./scripts/hermes_cm_ops.sh archive-compress-check --date YYYY-MM-DD" \
  "./scripts/hermes_cm_ops.sh weekly-review --start YYYY-MM-DD --end YYYY-MM-DD" \
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
  "标准日报流程YYYY-MM-DD" \
  "不支持相对日期" \
  "不开放重启" \
  "tail -n 20 reports/hermes/ops_audit.ndjson"
do
  require_text "$DOC" "$pattern" "daily ops doc content"
done

for pattern in \
  "第 5 步验收" \
  "/chain-monitor-report-analyst 命令提示" \
  "/chain-monitor-report-analyst 系统体检" \
  "/chain-monitor-report-analyst 监听器体检" \
  "/chain-monitor-report-analyst 标准日报流程2026-05-01" \
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
  "标准日报流程2026-05-01"
do
  require_text "$INSTALL_SCRIPT" "$pattern" "install content"
done
require_any_text "$INSTALL_SCRIPT" "~/.hermes/bin" '${HOME}/.hermes/bin' '$HOME/.hermes/bin'

for forbidden in \
  "make run" \
  "make run-research" \
  "make db-compact-execute" \
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

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_telegram_validate.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "命令提示" --dry-run --platform telegram >"$TMP_DIR/menu.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "command-menu"' "$TMP_DIR/menu.out" || fail "router command-menu argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "标准日报流程2026-05-01" --dry-run --platform telegram >"$TMP_DIR/daily.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "daily-flow"' "$TMP_DIR/daily.out" || fail "router daily-flow argv is not wrapper-only"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" "$ROUTER" --text "周复盘2026-04-27到2026-05-03" --dry-run --platform telegram >"$TMP_DIR/weekly.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "weekly-review"' "$TMP_DIR/weekly.out" || fail "router weekly-review argv is not wrapper-only"

pass
