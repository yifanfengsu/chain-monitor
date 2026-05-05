#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_ops_gateway_guard.XXXXXX")"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

run_fail() {
  local name="$1"
  local rc=0
  shift
  set +e
  "$OPS" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
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

run_ok() {
  local name="$1"
  shift
  if ! "$OPS" "$@" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"; then
    echo "stdout:" >&2
    sed -n '1,80p' "$TMP_DIR/${name}.out" >&2 || true
    echo "stderr:" >&2
    sed -n '1,80p' "$TMP_DIR/${name}.err" >&2 || true
    fail "expected success: $*"
  fi
}

require_output() {
  local name="$1"
  local pattern="$2"
  if ! grep -Fq -- "$pattern" "$TMP_DIR/${name}.out" "$TMP_DIR/${name}.err"; then
    fail "${name} output missing pattern: ${pattern}"
  fi
}

require_audit() {
  local pattern="$1"
  [[ -f "$HERMES_OPS_AUDIT_LOG" ]] || fail "audit log was not generated"
  grep -Fq -- "$pattern" "$HERMES_OPS_AUDIT_LOG" || fail "audit missing pattern: ${pattern}"
}

export HERMES_EXEC_ASK=1
export HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson"
export HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes_ops.lock"

run_fail analyze_direct analyze --date 2026-05-01 --mode fast
require_output analyze_direct '中文命令解析器'
require_audit '"refused_reason":"router_required"'

run_fail report_direct report --date 2026-05-01
require_output report_direct '中文命令解析器'
require_audit '"command":"report"'
require_audit '"refused_reason":"router_required"'

guarded_commands=(
  "daily_flow_direct daily-flow --date 2026-05-01"
  "replay_check_direct replay-check --date 2026-05-01"
  "job_diagnose_direct job-diagnose --job-id cmjob_20260503T090349Z_791e8d8ea814"
  "data_quality_direct data-quality --date 2026-05-01"
  "profile_review_direct profile-review --date 2026-05-01"
  "blocker_review_direct blocker-review --date 2026-05-01"
  "shadow_review_direct shadow-review --date 2026-05-01"
  "learning_review_direct learning-review --date 2026-05-01"
  "lp_diagnose_direct lp-diagnose --date 2026-05-01"
  "space_check_direct space-check"
  "archive_check_direct archive-compress-check --date 2026-05-01"
  "weekly_review_direct weekly-review --start 2026-04-27 --end 2026-05-03"
)

for item in "${guarded_commands[@]}"; do
  read -r name command arg1 arg2 arg3 arg4 arg5 <<<"$item"
  case "$command" in
    weekly-review)
      run_fail "$name" "$command" "$arg1" "$arg2" "$arg3" "$arg4"
      ;;
    space-check)
      run_fail "$name" "$command"
      ;;
    *)
      run_fail "$name" "$command" "$arg1" "$arg2"
      ;;
  esac
  require_output "$name" '中文命令解析器'
done
require_audit '"refused_reason":"router_required"'

run_ok help help
require_output help '中文控制帮助'

mkdir -p "$TMP_DIR/bin"
cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
echo "fake make ok: $*"
exit 0
MAKE
chmod +x "$TMP_DIR/bin/make"
export PATH="$TMP_DIR/bin:$PATH"
export HERMES_OPS_ROUTER_OK=1

run_ok report_router_ok report --date 2026-05-01
require_output report_router_ok 'fake make ok'

run_fail long_router_without_job daily-flow --date 2026-05-01
require_output long_router_without_job '后台 job'
require_audit '"refused_reason":"job_runner_required"'

python3 - "$HERMES_OPS_AUDIT_LOG" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line in handle:
        if line.strip():
            json.loads(line)
PY

echo "OK: Hermes ops gateway guard test passed"
