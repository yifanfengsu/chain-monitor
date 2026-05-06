#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
SKILL="${REPO_ROOT}/hermes_skills/chain-monitor-report-analyst/SKILL.md"
TELEGRAM_REF="${REPO_ROOT}/hermes_skills/chain-monitor-report-analyst/references/telegram_control.md"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_lock_status.XXXXXX")"

cleanup() {
  if [[ -n "${LOCK_PID:-}" ]]; then
    kill "$LOCK_PID" 2>/dev/null || true
    wait "$LOCK_PID" 2>/dev/null || true
  fi
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
  "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
  rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
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

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson"
export HERMES_OPS_LOCK_PATH="$TMP_DIR/chain-monitor-hermes-ops.lock"
export HERMES_OPS_LOCK_TIMEOUT_SEC=0
export HERMES_JOBCTL_JOBS_ROOT="$TMP_DIR/jobs"
export HERMES_OPS_PLATFORM="telegram"

: >"$HERMES_OPS_LOCK_PATH"

run_ok lock_status_unheld "$OPS" lock-status
require_output lock_status_unheld "Hermes 锁状态"
require_output lock_status_unheld "lock_file_exists=true"
require_output lock_status_unheld "flock_available=true"
require_output lock_status_unheld "lock_currently_held=false"
require_output lock_status_unheld "锁未被占用，后续命令可重试。"

run_fail unlocked_regular_command "$OPS" report --date not-a-date
require_output unlocked_regular_command "invalid date"
forbid_output unlocked_regular_command "lock_busy"

(
  exec 8>"$HERMES_OPS_LOCK_PATH"
  flock 8
  : >"$TMP_DIR/lock-held"
  sleep 10
) &
LOCK_PID=$!

for _ in 1 2 3 4 5 6 7 8 9 10; do
  [[ -f "$TMP_DIR/lock-held" ]] && break
  sleep 0.1
done
[[ -f "$TMP_DIR/lock-held" ]] || fail "test lock holder did not start"

run_ok lock_status_held "$OPS" lock-status
require_output lock_status_held "lock_file_exists=true"
require_output lock_status_held "flock_available=false"
require_output lock_status_held "lock_currently_held=true"
require_output lock_status_held "当前已有 Hermes 操作在执行，请稍后重试。"
require_output lock_status_held "可用：最近任务 / 任务状态JOB_ID / 锁状态"
forbid_output lock_status_held "rm -f"
forbid_output lock_status_held "删除 lock"

run_fail locked_regular_command "$OPS" report --date not-a-date
require_output locked_regular_command "refused_reason=lock_busy"
require_output locked_regular_command "当前已有 Hermes 操作在执行，请稍后重试。"
require_output locked_regular_command "可用：最近任务 / 任务状态JOB_ID / 锁状态"
forbid_output locked_regular_command "rm -f"
forbid_output locked_regular_command "删除 lock"

kill "$LOCK_PID" 2>/dev/null || true
wait "$LOCK_PID" 2>/dev/null || true
LOCK_PID=""

run_ok router_lock_status "$ROUTER" --text "锁状态" --dry-run --platform telegram
require_output router_lock_status "lock-status"

run_ok router_lock_status_execute "$ROUTER" --text "锁状态" --execute --platform telegram
require_output router_lock_status_execute "Hermes 锁状态"
forbid_output router_lock_status_execute "rm -f"
forbid_output router_lock_status_execute "删除 lock"

run_ok command_menu "$OPS" command-menu
require_output command_menu "锁状态"
forbid_output command_menu "删除锁"

for path in "$SKILL" "$TELEGRAM_REF"; do
  if grep -Eq 'rm[[:space:]]+-f[[:space:]]+/run/lock|/run/lock/chain-monitor-hermes-[^[:space:]]*.*rm[[:space:]]+-f' "$path"; then
    fail "${path#"$REPO_ROOT"/} suggests removing /run/lock files"
  fi
done

acquire_lock_block="$(sed -n '/^acquire_lock()/,/^related_hermes_process_count()/p' "$OPS")"
if grep -Eq '\[\[?[[:space:]]+(-e|-f)[[:space:]]+"\$HERMES_OPS_LOCK_PATH"' <<<"$acquire_lock_block"; then
  fail "acquire_lock uses lock file existence as a busy check"
fi
if ! grep -Fq "flock -n 9" <<<"$acquire_lock_block"; then
  fail "acquire_lock missing non-blocking flock check"
fi

echo "OK: Hermes lock-status test passed"
