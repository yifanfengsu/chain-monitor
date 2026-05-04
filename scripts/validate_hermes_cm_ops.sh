#!/usr/bin/env bash
set -Eeuo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

SCRIPT="scripts/hermes_cm_ops.sh"
[[ -f "$SCRIPT" ]] || die "missing ${SCRIPT}"

bash -n "$SCRIPT"

require_pattern() {
  local pattern="$1"
  if ! grep -Fq -- "$pattern" "$SCRIPT"; then
    die "missing required pattern in ${SCRIPT}: ${pattern}"
  fi
}

for pattern in \
  "cmd_help()" \
  "cmd_command_menu()" \
  "cmd_system_health()" \
  "cmd_listener_health()" \
  "cmd_daily_flow()" \
  "cmd_replay_check()" \
  "cmd_data_quality()" \
  "cmd_profile_review()" \
  "cmd_blocker_review()" \
  "cmd_shadow_review()" \
  "cmd_space_check()" \
  "cmd_archive_compress_check()" \
  "cmd_weekly_review()" \
  "cmd_report()" \
  "cmd_close()" \
  "cmd_health()" \
  "cmd_digest()" \
  "cmd_analyze()" \
  "cmd_submit_daily_flow()" \
  "cmd_submit_space_check()" \
  "cmd_submit_archive_compress_check()" \
  "cmd_submit_weekly_review()" \
  "cmd_job_status()" \
  "cmd_job_list()" \
  "cmd_job_result()" \
  "cmd_job_log()" \
  "cmd_job_diagnose()" \
  "cmd_job_cancel()" \
  "cmd_space_fast()" \
  "cmd_db_slim_dry_run()" \
  "cmd_run_job()" \
  "submit-daily-flow" \
  "submit-space-check" \
  "submit-archive-compress-check" \
  "submit-weekly-review" \
  "job-status" \
  "job-list" \
  "job-result" \
  "job-log" \
  "job-diagnose" \
  "job-cancel" \
  "db-slim-dry-run" \
  "__run-job" \
  "HERMES_OPS_JOB_RUNNER_OK" \
  "job_runner_required" \
  "HERMES_OPS_AUDIT_LOG" \
  "ops_audit.ndjson" \
  "HERMES_OPS_LOCK_PATH" \
  "flock" \
  "HERMES_OPS_REQUEST_ID" \
  "request_id" \
  "audit_write" \
  "refused_reason" \
  "lock_busy" \
  "runtime_missing_dependency" \
  "current_utc_date_protected" \
  "current_beijing_date_protected" \
  "Asia/Shanghai" \
  "HERMES_TEST_BEIJING_TODAY" \
  "missing_confirm_compress" \
  "HERMES_EXEC_ASK" \
  "HERMES_OPS_REQUIRE_ROUTER" \
  "HERMES_OPS_ROUTER_OK" \
  "router_required" \
  "enforce_router_guard" \
  "is_router_guarded_command" \
  "中文命令解析器" \
  "timeout" \
  "mktemp" \
  "trap on_exit EXIT" \
  "reports/hermes"
do
  require_pattern "$pattern"
done

require_pattern 'HERMES_DIGEST_WORKDIR="$REPO_ROOT"'
require_pattern 'HERMES_DIGEST_REDACT=1'
require_pattern "case \"\$1\" in"
require_pattern "help|command-menu|report|close|health|system-health|listener-health|digest|analyze|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|__run-job|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|space-check|archive-compress-check|weekly-review"
require_pattern "refuse unknown_command"
require_pattern "unknown command"
require_pattern "today_utc=\"\$(TZ=UTC date +%F)\""
require_pattern "refusing close for current UTC date"
require_pattern "refuse invalid_date"
require_pattern "refuse missing_confirm_compress"
require_pattern "refuse current_utc_date_protected"
require_pattern "report|digest|analyze|close|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|space-check|archive-compress-check|weekly-review"
require_pattern "enforce_router_guard \"\$AUDIT_COMMAND\""
require_pattern "enforce_long_job_runner_guard \"\$AUDIT_COMMAND\""
require_pattern "enforce_job_runner_guard \"\$AUDIT_COMMAND\""

extract_block() {
  local start="$1"
  local end="$2"
  sed -n "/^${start}/,/^${end}/p" "$SCRIPT"
}

help_block="$(extract_block 'cmd_help()' 'cmd_command_menu()')"
menu_block="$(extract_block 'cmd_command_menu()' 'die()')"
health_block="$(extract_block 'cmd_health()' 'cmd_system_health()')"
system_health_block="$(extract_block 'cmd_system_health()' 'cmd_listener_health()')"
listener_block="$(extract_block 'cmd_listener_health()' 'append_flow_step()')"
daily_flow_block="$(extract_block 'cmd_daily_flow()' 'cmd_replay_check()')"
space_block="$(extract_block 'cmd_space_check()' 'cmd_archive_compress_check()')"
db_size_block="$(extract_block 'cmd_db_size_diagnose()' 'run_output_value()')"
db_slim_block="$(extract_block 'cmd_db_slim_dry_run()' 'run_output_value()')"
archive_check_block="$(extract_block 'cmd_archive_compress_check()' 'cmd_weekly_review()')"
weekly_block="$(extract_block 'cmd_weekly_review()' 'cmd_digest()')"

[[ -n "$help_block" ]] || die "could not extract cmd_help"
[[ -n "$menu_block" ]] || die "could not extract cmd_command_menu"
[[ -n "$daily_flow_block" ]] || die "could not extract cmd_daily_flow"
[[ -n "$db_size_block" ]] || die "could not extract cmd_db_size_diagnose"
[[ -n "$db_slim_block" ]] || die "could not extract cmd_db_slim_dry_run"

if grep -Eq '(^|[^A-Za-z])make([[:space:]]|$)' <<<"$menu_block"; then
  die "command-menu must not execute or mention make commands"
fi

for required in \
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
  "空间快检" \
  "数据库体积诊断" \
  "数据库瘦身预检" \
  "重新标准日报流程YYYY-MM-DD 我确认重跑" \
  "生成日报YYYY-MM-DD" \
  "深度分析报告YYYY-MM-DD" \
  "生成摘要YYYY-MM-DD 快速" \
  "生成摘要YYYY-MM-DD 深度" \
  "归档压缩预检YYYY-MM-DD" \
  "周复盘START到END" \
  "任务状态JOB_ID" \
  "查看结果JOB_ID" \
  "查看日志JOB_ID" \
  "诊断任务JOB_ID" \
  "最近任务" \
  "YYYY-MM-DD" \
  "今天" \
  "昨天" \
  "脱敏" \
  "不提供交易建议" \
  "长任务会返回 job_id" \
  "数据库瘦身预检只能 dry-run" \
  "不开放 vacuum / prune / compact execute / export execute / delete DB / 修改地址簿"
do
  if ! grep -Fq -- "$required" <<<"$help_block$menu_block"; then
    die "help/menu missing required Chinese text: ${required}"
  fi
done

for required in \
  "daily-close:migrate_archive" \
  "make db-migrate-date" \
  "daily-close:db_integrity_fast" \
  "make db-integrity" \
  "daily-close:db_report" \
  "make db-report" \
  "daily-close:archive_mirror_check" \
  "app.archive_maintenance" \
  "--mirror-check-date" \
  "daily-close:archive_compress_dry_run" \
  "make archive-compress-dry-run" \
  "make trade-replay-full" \
  "make report-daily-date" \
  "make daily-compare" \
  "make sqlite-checkpoint" \
  "timeout_hit" \
  "failed_substep"
do
  grep -Fq -- "$required" <<<"$daily_flow_block" || die "daily-flow missing required command: ${required}"
done

if grep -Fq -- "make daily-close" <<<"$daily_flow_block"; then
  die "daily-flow must not call make daily-close as a black box"
fi

for forbidden in \
  "archive-compress-date" \
  "db-compact-execute" \
  "db-vacuum" \
  "db-prune-execute" \
  "report-clean-generated"
do
  if grep -Fq -- "$forbidden" <<<"$daily_flow_block"; then
    die "daily-flow contains forbidden command: ${forbidden}"
  fi
done

grep -Fq "archive-compress-dry-run" <<<"$archive_check_block" || die "archive-compress-check missing dry-run"
if grep -Fq "archive-compress-date" <<<"$archive_check_block"; then
  die "archive-compress-check must not call archive-compress-date"
fi

for forbidden in "rm " "db-vacuum" "db-prune-execute" "sqlite_store --vacuum" "sqlite_store --prune --execute"; do
  if grep -Fqi -- "$forbidden" <<<"$space_block"; then
    die "space-check contains forbidden maintenance term: ${forbidden}"
  fi
done

grep -Fq "make db-export-operational-payloads-dry-run" <<<"$db_size_block" || die "db-size-diagnose missing operational payload export dry-run"
grep -Fq "make db-export-operational-payloads-dry-run" <<<"$db_slim_block" || die "db-slim-dry-run missing operational payload export dry-run"
grep -Fq "dry-run only" <<<"$db_slim_block" || die "db-slim-dry-run missing dry-run policy"
grep -Fq "archive_write=false" <<<"$db_slim_block" || die "db-slim-dry-run missing archive_write=false"
grep -Fq "json_payload_clear=false" <<<"$db_slim_block" || die "db-slim-dry-run missing json_payload_clear=false"
grep -Fq "telegram_deliveries" <<<"$db_slim_block" || die "db-slim-dry-run missing telegram_deliveries summary"
grep -Fq "delivery_audit" <<<"$db_slim_block" || die "db-slim-dry-run missing delivery_audit summary"
for forbidden in "--execute" "--confirm" "db-vacuum" "db-prune-execute" "db-compact-execute" "archive-compress-date"; do
  if grep -Fq -- "$forbidden" <<<"$db_slim_block"; then
    die "db-slim-dry-run contains forbidden execute term: ${forbidden}"
  fi
done
for forbidden in "db-export-operational-payloads-execute" "db-export-operational-payloads-table-execute"; do
  if grep -Fq -- "$forbidden" "$SCRIPT"; then
    die "Telegram wrapper must not expose operational payload export execute: ${forbidden}"
  fi
done

for forbidden in "systemctl restart" "systemctl stop" "pkill"; do
  if grep -Fqi -- "$forbidden" <<<"$listener_block"; then
    die "listener-health contains forbidden listener action: ${forbidden}"
  fi
done

for forbidden in "make run" "make run-research" "VERIFIED 阈值"; do
  if grep -Fqi -- "$forbidden" <<<"$weekly_block"; then
    die "weekly-review contains forbidden action: ${forbidden}"
  fi
done

for guarded in \
  "daily-flow" \
  "submit-daily-flow" \
  "submit-space-check" \
  "submit-archive-compress-check" \
  "submit-weekly-review" \
  "job-status" \
  "job-list" \
  "job-result" \
  "job-log" \
  "job-diagnose" \
  "job-cancel" \
  "space-fast" \
  "db-size-diagnose" \
  "db-slim-dry-run" \
  "replay-check" \
  "data-quality" \
  "profile-review" \
  "blocker-review" \
  "shadow-review" \
  "space-check" \
  "archive-compress-check" \
  "weekly-review"
do
  grep -Fq -- "$guarded" <<<"$(extract_block 'is_router_guarded_command()' 'enforce_router_guard()')" || die "router guard missing ${guarded}"
done

for guarded in \
  "daily-flow" \
  "space-check" \
  "archive-compress-check" \
  "weekly-review"
do
  grep -Fq -- "$guarded" <<<"$(extract_block 'is_long_job_runner_command()' 'enforce_long_job_runner_guard()')" || die "long job runner guard missing ${guarded}"
done

for required in \
  "make env-check" \
  "make db-integrity DB_INTEGRITY_FAST=YES" \
  "make db-summary" \
  "make opportunity-db" \
  "make report-source-fast" \
  "make coverage"
do
  grep -Fq -- "$required" <<<"$health_block" || die "cmd_health missing safe command: ${required}"
done

for required in \
  "make db-report" \
  "make report-source-fast" \
  "make health" \
  "make coverage"
do
  grep -Fq -- "$required" <<<"$system_health_block" || die "cmd_system_health missing command: ${required}"
done

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_cm_ops_validate.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes_ops.lock" \
  "$SCRIPT" command-menu >"$TMP_DIR/menu.out"

grep -Fq "系统体检" "$TMP_DIR/menu.out" || die "command-menu output missing 系统体检"
grep -Fq "标准日报流程YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 标准日报流程YYYY-MM-DD"
grep -Fq "任务状态JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 任务状态JOB_ID"
grep -Fq "查看结果JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 查看结果JOB_ID"
grep -Fq "查看日志JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 查看日志JOB_ID"
grep -Fq "诊断任务JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 诊断任务JOB_ID"
grep -Fq "最近任务" "$TMP_DIR/menu.out" || die "command-menu output missing 最近任务"
grep -Fq "空间快检" "$TMP_DIR/menu.out" || die "command-menu output missing 空间快检"
grep -Fq "数据库体积诊断" "$TMP_DIR/menu.out" || die "command-menu output missing 数据库体积诊断"
grep -Fq "数据库瘦身预检" "$TMP_DIR/menu.out" || die "command-menu output missing 数据库瘦身预检"
grep -Fq "生成日报YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 生成日报YYYY-MM-DD"
grep -Fq "深度分析报告YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 深度分析报告YYYY-MM-DD"
grep -Fq "生成摘要YYYY-MM-DD 快速" "$TMP_DIR/menu.out" || die "command-menu output missing 生成摘要YYYY-MM-DD 快速"
grep -Fq "生成摘要YYYY-MM-DD 深度" "$TMP_DIR/menu.out" || die "command-menu output missing 生成摘要YYYY-MM-DD 深度"
grep -Fq "重新标准日报流程YYYY-MM-DD 我确认重跑" "$TMP_DIR/menu.out" || die "command-menu output missing rerun command"

mkdir -p "$TMP_DIR/bin"
cat >"$TMP_DIR/bin/make" <<'MAKE'
#!/usr/bin/env bash
echo "fake make ok: $*"
exit 0
MAKE
chmod +x "$TMP_DIR/bin/make"

HERMES_EXEC_ASK=1 \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/system_health_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/system_health.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" system-health >"$TMP_DIR/system_health.out"
grep -Fq "Hermes 系统体检摘要" "$TMP_DIR/system_health.out" || die "system-health did not run"

set +e
HERMES_EXEC_ASK=1 \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/daily_flow_guard_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/daily_flow_guard.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" daily-flow --date 2026-05-01 >"$TMP_DIR/daily_flow_guard.out" 2>"$TMP_DIR/daily_flow_guard.err"
guard_rc=$?
set -e
[[ "$guard_rc" -ne 0 ]] || die "daily-flow was not protected by router guard"
grep -Fq "router_required" "$TMP_DIR/daily_flow_guard_audit.ndjson" || die "daily-flow guard audit missing router_required"

set +e
HERMES_EXEC_ASK=1 \
HERMES_OPS_ROUTER_OK=1 \
HERMES_OPS_AUDIT_LOG="$TMP_DIR/daily_flow_job_guard_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/daily_flow_job_guard.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" daily-flow --date 2026-05-01 >"$TMP_DIR/daily_flow_job_guard.out" 2>"$TMP_DIR/daily_flow_job_guard.err"
job_guard_rc=$?
set -e
[[ "$job_guard_rc" -ne 0 ]] || die "daily-flow was not protected by job runner guard"
grep -Fq "job_runner_required" "$TMP_DIR/daily_flow_job_guard_audit.ndjson" || die "daily-flow job guard audit missing job_runner_required"

set +e
HERMES_OPS_AUDIT_LOG="$TMP_DIR/run_job_guard_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/run_job_guard.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" __run-job --job-id cmjob_20260501T120000Z_abcdef12 --kind daily-flow --date 2026-05-01 >"$TMP_DIR/run_job_guard.out" 2>"$TMP_DIR/run_job_guard.err"
run_job_guard_rc=$?
set -e
[[ "$run_job_guard_rc" -ne 0 ]] || die "__run-job was not protected by job runner guard"
grep -Fq "job_runner_required" "$TMP_DIR/run_job_guard_audit.ndjson" || die "__run-job guard audit missing job_runner_required"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/archive_check_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/archive_check.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" archive-compress-check --date 2026-05-01 >"$TMP_DIR/archive_check.out"
grep -Fq "fake make ok: archive-compress-dry-run DATE=2026-05-01" "$TMP_DIR/archive_check.out" || die "archive-compress-check did not call dry-run"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/db_slim_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/db_slim.lock" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" db-slim-dry-run >"$TMP_DIR/db_slim.out"
grep -Fq "fake make ok: db-export-operational-payloads-dry-run" "$TMP_DIR/db_slim.out" || die "db-slim-dry-run did not call operational payload export dry-run"
grep -Fq "archive_write=false" "$TMP_DIR/db_slim.out" || die "db-slim-dry-run output missing archive_write=false"
grep -Fq "json_payload_clear=false" "$TMP_DIR/db_slim.out" || die "db-slim-dry-run output missing json_payload_clear=false"
grep -Fq "未执行 export execute、VACUUM、compact、prune、删除 DB、清 JSON payload、写 archive" "$TMP_DIR/db_slim.out" || die "db-slim-dry-run output missing dry-run safety statement"

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "$SCRIPT" "$0"
fi

echo "OK: hermes_cm_ops validation passed"
