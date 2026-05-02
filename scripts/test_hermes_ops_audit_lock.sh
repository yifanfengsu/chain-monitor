#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_ops_audit_lock.XXXXXX")"

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

run_expect_fail() {
  local rc=0

  set +e
  "$REPO_ROOT/scripts/hermes_cm_ops.sh" "$@" >"$TMP_DIR/cmd.out" 2>"$TMP_DIR/cmd.err"
  rc=$?
  set -e

  if [[ "$rc" -eq 0 ]]; then
    echo "stdout:" >&2
    sed -n '1,80p' "$TMP_DIR/cmd.out" >&2
    echo "stderr:" >&2
    sed -n '1,80p' "$TMP_DIR/cmd.err" >&2
    fail "expected command to fail: $*"
  fi
}

require_audit_contains() {
  local pattern="$1"

  [[ -f "$HERMES_OPS_AUDIT_LOG" ]] || fail "audit log was not generated"
  grep -Fq "$pattern" "$HERMES_OPS_AUDIT_LOG" || fail "audit log missing pattern: $pattern"
}

require_audit_not_contains() {
  local pattern="$1"

  if grep -Fq "$pattern" "$HERMES_OPS_AUDIT_LOG"; then
    fail "audit log leaked forbidden text: $pattern"
  fi
}

validate_json_lines() {
  local python_bin=""

  if command -v python3 >/dev/null 2>&1; then
    python_bin="python3"
  elif [[ -x "$REPO_ROOT/venv/bin/python" ]]; then
    python_bin="$REPO_ROOT/venv/bin/python"
  else
    fail "python3 or project venv python is required to validate NDJSON"
  fi

  "$python_bin" - "$HERMES_OPS_AUDIT_LOG" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    lines = [line for line in handle if line.strip()]

if not lines:
    raise SystemExit("empty audit log")

for line in lines:
    event = json.loads(line)
    for key, value in event.items():
        if key.endswith("_hash") or not isinstance(value, str):
            continue
        for raw in ("123456789", "-1001234567890", "test-session"):
            if raw in value:
                raise SystemExit(f"raw sensitive identifier leaked in {key}")
PY
}

export HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson"
export HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes_ops.lock"
export HERMES_OPS_LOCK_TIMEOUT_SEC=0
export HERMES_OPS_PLATFORM="telegram"
export HERMES_OPS_ACTOR_ID="123456789"
export HERMES_OPS_CHAT_ID="-1001234567890"
export HERMES_OPS_SESSION_ID="test-session"

mkdir -p "$TMP_DIR/bin"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'echo "unexpected fake make invocation: $*" >&2' \
  'exit 99' >"$TMP_DIR/bin/make"
chmod +x "$TMP_DIR/bin/make"
export PATH="$TMP_DIR/bin:$PATH"

run_expect_fail unknown-test-command
require_audit_contains '"allowed":false'
require_audit_contains '"refused_reason":"unknown_command"'
require_audit_contains '"command":"unknown"'
require_audit_contains '"actor_id_hash":"sha256:'
require_audit_contains '"chat_id_hash":"sha256:'
require_audit_contains '"session_id_hash":"sha256:'
require_audit_not_contains '"123456789"'
require_audit_not_contains '"-1001234567890"'
require_audit_not_contains '"test-session"'

run_expect_fail report --date not-a-date
require_audit_contains '"refused_reason":"invalid_date"'

run_expect_fail close --date 2020-01-01
require_audit_contains '"refused_reason":"missing_confirm_compress"'

(
  exec 8>"$HERMES_OPS_LOCK_PATH"
  flock 8
  : >"$TMP_DIR/lock-held"
  sleep 5
) &
LOCK_PID=$!

for _ in 1 2 3 4 5 6 7 8 9 10; do
  [[ -f "$TMP_DIR/lock-held" ]] && break
  sleep 0.1
done
[[ -f "$TMP_DIR/lock-held" ]] || fail "test lock holder did not start"

run_expect_fail report --date 2020-01-01
require_audit_contains '"refused_reason":"lock_busy"'

kill "$LOCK_PID" 2>/dev/null || true
wait "$LOCK_PID" 2>/dev/null || true
LOCK_PID=""

validate_json_lines

if grep -Eiq 'TELEGRAM_BOT_TOKEN|RPC_URL|alchemy|infura|quicknode' "$HERMES_OPS_AUDIT_LOG"; then
  fail "audit log leaked token or RPC marker"
fi
if grep -Eq '0x[0-9A-Fa-f]{40}' "$HERMES_OPS_AUDIT_LOG"; then
  fail "audit log leaked full EVM address"
fi
if grep -Eq '0x[0-9A-Fa-f]{64}' "$HERMES_OPS_AUDIT_LOG"; then
  fail "audit log leaked full tx hash"
fi
require_audit_not_contains '"123456789"'
require_audit_not_contains '"-1001234567890"'
require_audit_not_contains '"test-session"'

echo "OK: Hermes ops audit and lock test passed"
