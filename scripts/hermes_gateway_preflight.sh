#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="${HOME}/.hermes/bin/chain-monitor-cn-router"
INSTALLED_SKILL="${HOME}/.hermes/skills/chain-monitor-report-analyst/SKILL.md"
INSTALLED_REF="${HOME}/.hermes/skills/chain-monitor-report-analyst/references/telegram_control.md"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_gateway_preflight.XXXXXX")"
FAILS=0
WARNS=0

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

pass() {
  echo "PASS: $*"
}

warn() {
  echo "WARN: $*"
  WARNS=$((WARNS + 1))
}

fail_check() {
  echo "FAIL: $*"
  FAILS=$((FAILS + 1))
}

require_output() {
  local name="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail_check "${name} output missing pattern: ${pattern}"
  fi
}

cd "$REPO_ROOT"

echo "Hermes Telegram gateway preflight"
echo "scope: dry-run and config checks only; no health/analyze execution and no secrets printed"
echo

set +e
bash scripts/hermes_gateway_runtime_check.sh
runtime_rc=$?
set -e
if [[ "$runtime_rc" -eq 0 ]]; then
  pass "runtime check completed without FAIL"
else
  fail_check "runtime check reported FAIL"
fi

if [[ -f "$INSTALLED_SKILL" ]]; then
  pass "installed skill found"
else
  fail_check "installed skill missing; run scripts/install_hermes_skill.sh and Telegram /reset"
fi

if [[ -f "$INSTALLED_REF" ]]; then
  pass "installed reference found"
else
  fail_check "installed references/telegram_control.md missing; run scripts/install_hermes_skill.sh and Telegram /reset"
fi

if [[ -x "$LAUNCHER" ]]; then
  pass "chain-monitor-cn-router launcher found"

  set +e
  "$LAUNCHER" --text "分析昨天的报告" --dry-run --platform telegram >"$TMP_DIR/relative.out" 2>"$TMP_DIR/relative.err"
  relative_rc=$?
  set -e
  if [[ "$relative_rc" -eq 0 ]]; then
    fail_check "relative date dry-run was not refused"
  else
    pass "relative date dry-run was refused"
    require_output relative "绝对日期"
    require_output relative "YYYY-MM-DD"
  fi

  if "$LAUNCHER" --text "分析报告2026-05-01" --dry-run --platform telegram >"$TMP_DIR/absolute.out" 2>"$TMP_DIR/absolute.err"; then
    pass "absolute date dry-run succeeded"
    require_output absolute "analyze"
    require_output absolute "2026-05-01"
    require_output absolute "fast"
  else
    fail_check "absolute date dry-run failed"
  fi
else
  fail_check "chain-monitor-cn-router launcher missing; run scripts/install_hermes_skill.sh and Telegram /reset"
fi

echo
echo "Summary: PASS/WARN/FAIL counts"
echo "WARN: ${WARNS}"
echo "FAIL: ${FAILS}"

if (( FAILS > 0 )); then
  echo "FAIL: fix before Telegram control"
  exit 1
elif (( WARNS > 0 )); then
  echo "WARN: can test after reviewing warnings"
else
  echo "PASS: safe to test Telegram command"
fi
