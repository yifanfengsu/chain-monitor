#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALLED_SKILL="${HOME}/.hermes/skills/chain-monitor-report-analyst/SKILL.md"
INSTALLED_REF="${HOME}/.hermes/skills/chain-monitor-report-analyst/references/telegram_control.md"
LAUNCHER="${HOME}/.hermes/bin/chain-monitor-cn-router"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_telegram_e2e_smoke.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing file: $path"
}

require_executable() {
  local path="$1"
  [[ -x "$path" ]] || fail "missing executable: $path"
}

require_text() {
  local path="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$path"; then
    fail "missing pattern in ${path}: ${pattern}"
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
  fail "missing any required pattern in ${path}: $*"
}

run_launcher_ok() {
  local name="$1"
  shift
  if ! "$LAUNCHER" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"; then
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
    sed -n '1,80p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected launcher success: $*"
  fi
}

run_launcher_fail() {
  local name="$1"
  local rc=0
  shift
  set +e
  "$LAUNCHER" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
    sed -n '1,80p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected launcher failure: $*"
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

cd "$REPO_ROOT"

require_file "scripts/hermes_cm_cn_router.py"
require_file "scripts/hermes_cm_ops.sh"
require_file "$INSTALLED_SKILL"
require_file "$INSTALLED_REF"
require_executable "$LAUNCHER"

run_launcher_ok menu_dry --text '命令提示' --dry-run --platform telegram
require_output menu_dry 'command-menu'

(
  export HERMES_OPS_AUDIT_LOG="$TMP_DIR/menu_ops_audit.ndjson"
  export HERMES_OPS_LOCK_PATH="$TMP_DIR/menu_ops.lock"
  run_launcher_ok menu_execute --text '命令提示' --execute --platform telegram
)
require_output menu_execute 'Chain Monitor 中文命令菜单'
require_output menu_execute '系统体检'
require_output menu_execute '标准日报流程YYYY-MM-DD'

run_launcher_ok daily_flow --text '标准日报流程2026-05-01' --dry-run --platform telegram
require_output daily_flow 'daily-flow'
require_output daily_flow '2026-05-01'

run_launcher_ok weekly_review --text '周复盘2026-04-27到2026-05-03' --dry-run --platform telegram
require_output weekly_review 'weekly-review'
require_output weekly_review '2026-04-27'
require_output weekly_review '2026-05-03'

run_launcher_fail relative_daily --text '标准日报流程昨天' --dry-run --platform telegram
require_output relative_daily 'YYYY-MM-DD'

run_launcher_fail relative --text '分析昨天的报告' --dry-run --platform telegram
require_output relative '绝对日期'
require_output relative 'YYYY-MM-DD'

run_launcher_fail today --text '生成今天的日报' --dry-run --platform telegram
require_output today '绝对日期'

run_launcher_ok absolute --text '分析报告2026-05-01' --dry-run --platform telegram
require_output absolute 'analyze'
require_output absolute '2026-05-01'
require_output absolute 'fast'
forbid_output absolute 'auto-build'

(
  export HERMES_EXEC_ASK=1
  export HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson"
  export HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes_ops.lock"

  set +e
  ./scripts/hermes_cm_ops.sh analyze --date 2026-05-01 --mode fast >"$TMP_DIR/guard.out" 2>"$TMP_DIR/guard.err"
  rc=$?
  set -e

  if [[ "$rc" -eq 0 ]]; then
    sed -n '1,80p' "$TMP_DIR/guard.out" >&2 || true
    sed -n '1,80p' "$TMP_DIR/guard.err" >&2 || true
    fail "gateway guard did not block direct analyze"
  fi
  [[ -f "$HERMES_OPS_AUDIT_LOG" ]] || fail "gateway guard audit was not generated"
  grep -Fq "router_required" "$HERMES_OPS_AUDIT_LOG" || fail "gateway guard audit missing router_required"
)

require_text "$INSTALLED_SKILL" "chain-monitor-cn-router"
require_text "$INSTALLED_SKILL" "不得先运行 date"
require_text "$INSTALLED_SKILL" "不得把 今天/昨天/前天 转换为具体日期"

require_text "$INSTALLED_REF" "所有中文 Telegram 命令必须先经过"
require_any_text "$INSTALLED_REF" "scripts/hermes_cm_cn_router.py" "chain-monitor-cn-router"
require_text "$INSTALLED_REF" "分析昨天的报告"
require_any_text "$INSTALLED_REF" "预期：拒绝" "必须拒绝"

echo "PASS: local Telegram E2E smoke checks passed"
