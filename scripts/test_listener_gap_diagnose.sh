#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/listener_gap_diagnose.XXXXXX")"

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
  grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err" || fail "${name} missing pattern: ${pattern}"
}

forbid_out() {
  local name="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} contains forbidden pattern: ${pattern}"
  fi
}

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson"

run_ok listener_gap --text '监听间隔诊断2026-05-10' --dry-run
require_out listener_gap 'listener-gap-diagnose'
require_out listener_gap '"--date"'
require_out listener_gap '"2026-05-10"'

run_ok listener_empty_window --text '监听空窗诊断2026-05-10' --dry-run
require_out listener_empty_window 'listener-gap-diagnose'

run_ok data_gap --text '数据间隔诊断2026-05-10' --dry-run
require_out data_gap 'listener-gap-diagnose'

run_fail relative --text '监听间隔诊断昨天' --dry-run
require_out relative '绝对日期'
require_out relative 'YYYY-MM-DD'
forbid_out relative 'listener-gap-diagnose'

echo "listener-gap-diagnose router tests passed"
