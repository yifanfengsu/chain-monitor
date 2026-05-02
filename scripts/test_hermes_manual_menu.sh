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

run_ok listener_health --text '监听器体检' --dry-run --platform telegram
require_out listener_health 'listener-health'

run_ok daily_flow --text '标准日报流程2026-05-01' --dry-run --platform telegram
require_out daily_flow 'daily-flow'
require_out daily_flow '--date'
require_out daily_flow '2026-05-01'

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

run_ok space_check --text '空间检查' --dry-run --platform telegram
require_out space_check 'space-check'

run_ok archive_check --text '归档压缩预检2026-05-01' --dry-run --platform telegram
require_out archive_check 'archive-compress-check'
require_out archive_check '--date'
require_out archive_check '2026-05-01'
forbid_out archive_check 'archive-compress-date'

run_ok weekly_review --text '周复盘2026-04-27到2026-05-03' --dry-run --platform telegram
require_out weekly_review 'weekly-review'
require_out weekly_review '--start'
require_out weekly_review '2026-04-27'
require_out weekly_review '--end'
require_out weekly_review '2026-05-03'

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
grep -Fq '标准日报流程YYYY-MM-DD' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 标准日报流程YYYY-MM-DD"
grep -Fq '周复盘START到END' "$TMP_DIR/menu_execute.out" || fail "menu execute missing 周复盘START到END"
grep -Fq 'command-menu' "$MENU_AUDIT" || grep -Fq 'command-menu' "$HERMES_OPS_AUDIT_LOG" || fail "audit missing command-menu"

echo "OK: Hermes manual menu router test passed"
