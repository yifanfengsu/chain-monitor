#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPS="${REPO_ROOT}/scripts/hermes_cm_ops.sh"
ROUTER="${REPO_ROOT}/scripts/hermes_cm_cn_router.py"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/daily_flow_outcome_catchup.XXXXXX")"
LATEST_FLOW="$REPO_ROOT/reports/hermes/hermes_daily_flow_latest.md"
LATEST_BACKUP="$TMP_DIR/hermes_daily_flow_latest.md.backup"
LATEST_EXISTED=0

if [[ -f "$LATEST_FLOW" ]]; then
  cp "$LATEST_FLOW" "$LATEST_BACKUP"
  LATEST_EXISTED=1
fi

cleanup() {
  if [[ "$LATEST_EXISTED" -eq 1 ]]; then
    cp "$LATEST_BACKUP" "$LATEST_FLOW"
  else
    rm -f "$LATEST_FLOW"
  fi
  rm -rf "$TMP_DIR"
  rm -f \
    "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-28.md" \
    "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-29.md" \
    "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-30.md" \
    "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-31.md"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

require_out() {
  local path="$1"
  local pattern="$2"
  grep -Fq -- "$pattern" "$path" || fail "${path} missing pattern: ${pattern}"
}

forbid_out() {
  local path="$1"
  local pattern="$2"
  if grep -Fq -- "$pattern" "$path"; then
    fail "${path} contains forbidden pattern: ${pattern}"
  fi
}

mkdir -p "$TMP_DIR/bin"

cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
case "${1:-}" in
  db-migrate-date|db-integrity|db-report|report-daily-date|daily-compare|archive-compress-dry-run|sqlite-checkpoint|trade-replay-full)
    echo "fake make ok: $*"
    exit 0
    ;;
  *)
    echo "unexpected fake make target: $*" >&2
    exit 91
    ;;
esac
MAKE
chmod +x "$TMP_DIR/bin/make"

cat >"$TMP_DIR/fake_python" <<'PY'
#!/usr/bin/env bash
if [[ "$*" == *"app.archive_maintenance"* && "$*" == *"--mirror-check-date"* ]]; then
  echo "fake mirror ok"
  exit 0
fi
if [[ "$*" == *"scripts/hermes_outcome_catchup.py"* && "$*" == *"--dry-run"* ]]; then
  echo "Chain Monitor outcome-catchup｜fake"
  echo "mode=dry-run"
  echo "would_update_rows=${HERMES_FAKE_CATCHUP_WOULD_UPDATE_ROWS:-0}"
  echo "updated_count=0"
  echo "still_pending_count=${HERMES_FAKE_CATCHUP_STILL_PENDING:-0}"
  exit 0
fi
if [[ "$*" == *"scripts/hermes_outcome_catchup.py"* && "$*" == *"--execute"* ]]; then
  rc="${HERMES_FAKE_CATCHUP_EXEC_RC:-0}"
  echo "Chain Monitor outcome-catchup｜fake"
  echo "mode=execute"
  echo "would_update_rows=${HERMES_FAKE_CATCHUP_WOULD_UPDATE_ROWS:-0}"
  echo "updated_count=${HERMES_FAKE_CATCHUP_WOULD_UPDATE_ROWS:-0}"
  echo "still_pending_count=${HERMES_FAKE_CATCHUP_EXEC_STILL_PENDING:-0}"
  if [[ "$rc" != "0" ]]; then
    echo "simulated outcome-catchup execute failure" >&2
  fi
  exit "$rc"
fi
echo "fake python ok: $*"
exit 0
PY
chmod +x "$TMP_DIR/fake_python"

run_flow() {
  local name="$1"
  local report_date="$2"
  local would_update="$3"
  local exec_rc="${4:-0}"
  local rc=0

  set +e
  HERMES_OPS_AUDIT_LOG="$TMP_DIR/${name}_ops_audit.ndjson" \
  HERMES_OPS_LOCK_PATH="$TMP_DIR/${name}.lock" \
  HERMES_OPS_JOB_RUNNER_OK=1 \
  HERMES_OPS_PYTHON_BIN="$TMP_DIR/fake_python" \
  HERMES_TEST_BEIJING_TODAY=2099-12-31 \
  HERMES_FAKE_CATCHUP_WOULD_UPDATE_ROWS="$would_update" \
  HERMES_FAKE_CATCHUP_EXEC_RC="$exec_rc" \
  PATH="$TMP_DIR/bin:$PATH" \
    "$OPS" daily-flow --date "$report_date" >"$TMP_DIR/${name}.out" 2>"$TMP_DIR/${name}.err"
  rc=$?
  set -e
  echo "$rc" >"$TMP_DIR/${name}.rc"
}

run_flow would_update 2099-12-28 10 0
[[ "$(cat "$TMP_DIR/would_update.rc")" == "0" ]] || fail "daily-flow with would_update_rows=10 failed"
require_out "$TMP_DIR/would_update.out" "outcome_catchup_dry_run"
require_out "$TMP_DIR/would_update.out" "outcome_catchup_execute"
require_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-28.md" "step=outcome_catchup_dry_run"
require_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-28.md" "step=outcome_catchup_execute"
require_out "$TMP_DIR/would_update_ops_audit.ndjson" '"substep":"outcome_catchup_dry_run"'
require_out "$TMP_DIR/would_update_ops_audit.ndjson" '"substep":"outcome_catchup_execute"'

run_flow no_update 2099-12-29 0 0
[[ "$(cat "$TMP_DIR/no_update.rc")" == "0" ]] || fail "daily-flow with would_update_rows=0 failed"
require_out "$TMP_DIR/no_update.out" "outcome_catchup_skipped"
require_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-29.md" "step=outcome_catchup_skipped"
forbid_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-29.md" "step=outcome_catchup_execute"

run_flow execute_fail 2099-12-30 10 7
[[ "$(cat "$TMP_DIR/execute_fail.rc")" != "0" ]] || fail "daily-flow succeeded despite outcome_catchup_execute failure"
require_out "$TMP_DIR/execute_fail.out" "failed_substep=outcome_catchup_execute"
require_out "$TMP_DIR/execute_fail.out" "exit_code=7"
require_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-30.md" "step=outcome_catchup_execute"
require_out "$REPO_ROOT/reports/hermes/hermes_daily_flow_2099-12-30.md" "status=failed"

set +e
HERMES_OPS_AUDIT_LOG="$TMP_DIR/current_ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/current.lock" \
HERMES_OPS_JOB_RUNNER_OK=1 \
HERMES_OPS_PYTHON_BIN="$TMP_DIR/fake_python" \
HERMES_TEST_BEIJING_TODAY=2099-12-31 \
PATH="$TMP_DIR/bin:$PATH" \
  "$OPS" daily-flow --date 2099-12-31 >"$TMP_DIR/current.out" 2>"$TMP_DIR/current.err"
current_rc=$?
set -e
[[ "$current_rc" -ne 0 ]] || fail "daily-flow accepted current Beijing date"
require_out "$TMP_DIR/current.err" "当前北京时间逻辑日"
forbid_out "$TMP_DIR/current.out" "outcome_catchup"
forbid_out "$TMP_DIR/current.err" "outcome_catchup"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text 'Outcome补全预检2099-12-28' --dry-run --platform telegram >"$TMP_DIR/router_dry.out"
require_out "$TMP_DIR/router_dry.out" '"outcome-catchup"'
require_out "$TMP_DIR/router_dry.out" '"--dry-run"'
forbid_out "$TMP_DIR/router_dry.out" '"--execute"'

set +e
HERMES_OPS_AUDIT_LOG="$TMP_DIR/router_audit.ndjson" \
  "$ROUTER" --text 'Outcome补全执行2099-12-28' --dry-run --platform telegram >"$TMP_DIR/router_execute.out" 2>"$TMP_DIR/router_execute.err"
router_execute_rc=$?
set -e
[[ "$router_execute_rc" -ne 0 ]] || fail "router accepted Outcome补全执行"
forbid_out "$TMP_DIR/router_execute.out" "outcome-catchup"
forbid_out "$TMP_DIR/router_execute.err" "outcome-catchup"

echo "OK: daily-flow outcome-catchup tests passed"
