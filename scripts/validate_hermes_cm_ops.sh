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
  "cmd_lock_status()" \
  "cmd_system_health()" \
  "cmd_listener_health()" \
  "cmd_daily_flow()" \
  "cmd_replay_check()" \
  "cmd_data_quality()" \
  "cmd_profile_review()" \
  "cmd_blocker_review()" \
  "cmd_shadow_review()" \
  "cmd_learning_review()" \
  "cmd_candidate_coverage()" \
  "cmd_daily_report_schema_check()" \
  "cmd_outcome_diagnose()" \
  "cmd_lp_diagnose()" \
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
  "lock-status" \
  "job-status" \
  "job-list" \
  "job-result" \
  "job-log" \
  "job-diagnose" \
  "job-cancel" \
  "db-slim-dry-run" \
  "learning-review" \
  "candidate-coverage" \
  "daily-report-schema-check" \
  "outcome-diagnose" \
  "lp-diagnose" \
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
require_pattern "help|command-menu|lock-status|report|close|health|system-health|listener-health|digest|analyze|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|__run-job|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|learning-review|candidate-coverage|daily-report-schema-check|outcome-diagnose|lp-diagnose|space-check|archive-compress-check|weekly-review"
require_pattern "refuse unknown_command"
require_pattern "unknown command"
require_pattern "today_utc=\"\$(TZ=UTC date +%F)\""
require_pattern "refusing close for current UTC date"
require_pattern "refuse invalid_date"
require_pattern "refuse missing_confirm_compress"
require_pattern "refuse current_utc_date_protected"
require_pattern "lock-status|report|digest|analyze|close|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|learning-review|candidate-coverage|daily-report-schema-check|outcome-diagnose|lp-diagnose|space-check|archive-compress-check|weekly-review"
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
candidate_block="$(extract_block 'cmd_candidate_coverage()' 'cmd_outcome_diagnose()')"
schema_check_block="$(extract_block 'cmd_daily_report_schema_check()' 'cmd_outcome_diagnose()')"
outcome_block="$(extract_block 'cmd_outcome_diagnose()' 'cmd_lp_diagnose()')"
lp_block="$(extract_block 'cmd_lp_diagnose()' 'cmd_space_check()')"

[[ -n "$help_block" ]] || die "could not extract cmd_help"
[[ -n "$menu_block" ]] || die "could not extract cmd_command_menu"
[[ -n "$daily_flow_block" ]] || die "could not extract cmd_daily_flow"
[[ -n "$db_size_block" ]] || die "could not extract cmd_db_size_diagnose"
[[ -n "$db_slim_block" ]] || die "could not extract cmd_db_slim_dry_run"
[[ -n "$candidate_block" ]] || die "could not extract cmd_candidate_coverage"
[[ -n "$schema_check_block" ]] || die "could not extract cmd_daily_report_schema_check"
[[ -n "$outcome_block" ]] || die "could not extract cmd_outcome_diagnose"
[[ -n "$lp_block" ]] || die "could not extract cmd_lp_diagnose"

if grep -Eq '(^|[^A-Za-z])make([[:space:]]|$)' <<<"$menu_block"; then
  die "command-menu must not execute or mention make commands"
fi

for required in \
  "命令提示" \
  "锁状态" \
  "系统体检" \
  "监听器体检" \
  "标准日报流程YYYY-MM-DD" \
  "分析报告YYYY-MM-DD" \
  "检查回放YYYY-MM-DD" \
  "数据质量YYYY-MM-DD" \
  "Profile复盘YYYY-MM-DD" \
  "Blocker复盘YYYY-MM-DD" \
  "Shadow复盘YYYY-MM-DD" \
  "学习复盘YYYY-MM-DD" \
  "学习总结YYYY-MM-DD" \
  "CANDIDATE覆盖诊断YYYY-MM-DD" \
  "日报结构检查YYYY-MM-DD" \
  "Outcome闭环诊断YYYY-MM-DD" \
  "LP诊断YYYY-MM-DD" \
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

for required in \
  "HERMES_CANDIDATE_COVERAGE_DB_PATH" \
  "sqlite3.connect(db_path.resolve().as_uri() + \"?mode=ro\"" \
  "daily_report_status" \
  "signals_total" \
  "trade_opportunities_total" \
  "trade_opportunity_status_distribution" \
  "replay_examples_total" \
  "replay_examples_with_opportunity" \
  "replay_examples_status_CANDIDATE" \
  "delivery_audit_stage_status_distribution" \
  "near_candidate_count" \
  "top_near_candidate_blockers" \
  "NONE_to_CANDIDATE主要缺口" \
  "candidate_frontier_diagnosis" \
  "实时推送主要是 observe/signal，不是 opportunity candidate" \
  "replay 关联层可能漏接" \
  "candidate gate 太严格或当天无合格候选" \
  "只读聚合诊断"
do
  grep -Fq -- "$required" <<<"$candidate_block" || die "candidate-coverage missing required content: ${required}"
done
for forbidden in "make " "VACUUM" "prune" "compact" "archive-compress-date" "db-vacuum"; do
  if grep -Fqi -- "$forbidden" <<<"$candidate_block"; then
    die "candidate-coverage contains forbidden term: ${forbidden}"
  fi
done

for required in \
  "scripts/hermes_daily_report_schema_check.py" \
  "daily-report-schema-check requires --date YYYY-MM-DD" \
  "AUDIT_OUTPUT_HINT=\"daily-report-schema-check" \
  "AUDIT_ALLOWED=true"
do
  grep -Fq -- "$required" <<<"$schema_check_block" || die "daily-report-schema-check wrapper missing required content: ${required}"
done
for required in \
  "Chain Monitor 日报结构检查" \
  "lp_signal_summary" \
  "lp_stage_summary" \
  "clmm_summary" \
  "lp_suppression_summary" \
  "candidate_frontier_summary" \
  "schema_check" \
  "report_mapping_missing" \
  "lp_analyzer_or_gate_missing" \
  "HERMES_SCHEMA_CHECK_DB_PATH" \
  "?mode=ro" \
  "只读结构检查"
do
  grep -Fq -- "$required" scripts/hermes_daily_report_schema_check.py || die "schema check helper missing required content: ${required}"
done
for forbidden in "make " "VACUUM" "prune" "compact" "archive-compress-date" "db-vacuum"; do
  if grep -Fqi -- "$forbidden" <<<"$schema_check_block" || grep -Fqi -- "$forbidden" scripts/hermes_daily_report_schema_check.py; then
    die "daily-report-schema-check contains forbidden term: ${forbidden}"
  fi
done

for required in \
  "scripts/hermes_outcome_diagnose.py" \
  "outcome-diagnose requires --date YYYY-MM-DD" \
  "AUDIT_OUTPUT_HINT=\"outcome-diagnose" \
  "AUDIT_ALLOWED=true"
do
  grep -Fq -- "$required" <<<"$outcome_block" || die "outcome-diagnose wrapper missing required content: ${required}"
done
for required in \
  "Chain Monitor Outcome闭环诊断" \
  "signals_to_outcomes_match_rate" \
  "opportunities_to_opportunity_outcomes_match_rate" \
  "opportunities_to_replay_examples_match_rate" \
  "signals_outcome_missing_by_asset" \
  "opportunities_outcome_missing_by_status" \
  "outcome缺失原因推断" \
  "时间窗口未到" \
  "price snapshot 缺失" \
  "signal_id / opportunity_id 未关联" \
  "outcome worker 未运行" \
  "report 字段映射错误" \
  "HERMES_OUTCOME_DIAGNOSE_DB_PATH" \
  "?mode=ro" \
  "只读脱敏诊断"
do
  grep -Fq -- "$required" scripts/hermes_outcome_diagnose.py || die "Outcome diagnose helper missing required content: ${required}"
done
for forbidden in "make " "VACUUM" "prune" "compact" "archive-compress-date" "db-vacuum"; do
  if grep -Fqi -- "$forbidden" <<<"$outcome_block" || grep -Fqi -- "$forbidden" scripts/hermes_outcome_diagnose.py; then
    die "outcome-diagnose contains forbidden term: ${forbidden}"
  fi
done

for required in \
  "scripts/hermes_lp_diagnose.py" \
  "lp-diagnose requires --date YYYY-MM-DD" \
  "AUDIT_OUTPUT_HINT=\"lp-diagnose" \
  "AUDIT_ALLOWED=true"
do
  grep -Fq -- "$required" <<<"$lp_block" || die "lp-diagnose wrapper missing required content: ${required}"
done
for required in \
  "daily_report字段存在性" \
  "SQLite LP-like计数 signals" \
  "format_json_counts('signal_json'" \
  "raw_events" \
  "parsed_events" \
  "delivery_audit LP相关推送抑制" \
  "major coverage ETH/BTC/SOL x USDT/USDC" \
  "report_mapping判断" \
  "LP analyzer / gate" \
  "当天无可用 LP 样本或扫描覆盖不足" \
  "HERMES_LP_DIAGNOSE_DB_PATH" \
  "?mode=ro"
do
  grep -Fq -- "$required" scripts/hermes_lp_diagnose.py || die "LP diagnose helper missing required content: ${required}"
done
for forbidden in "make " "VACUUM" "prune" "compact" "archive-compress-date" "db-vacuum"; do
  if grep -Fqi -- "$forbidden" <<<"$lp_block" || grep -Fqi -- "$forbidden" scripts/hermes_lp_diagnose.py; then
    die "lp-diagnose contains forbidden term: ${forbidden}"
  fi
done

for forbidden in "systemctl restart" "systemctl stop" "pkill"; do
  if grep -Fqi -- "$forbidden" <<<"$listener_block"; then
    die "listener-health contains forbidden listener action: ${forbidden}"
  fi
done

for required in \
  "Telegram outbound health" \
  "HERMES_LISTENER_HEALTH_DB_PATH" \
  "notifier.get_notifier_health" \
  "attempted=" \
  "sent_ok=" \
  "sent_failed=" \
  "pool_timeout_count" \
  "retry_after_count" \
  "network_error_count" \
  "consecutive_failures" \
  "sent_to_telegram=1数量" \
  "sent_to_telegram=0数量" \
  "notifier_send_failed" \
  "最近成功 Telegram 发送时间" \
  "最近失败时间" \
  "Telegram outbound 可能异常" \
  "pool timeout / NetworkError / TimedOut" \
  "未发送 Telegram warning"
do
  grep -Fq -- "$required" <<<"$listener_block" || die "listener-health missing Telegram outbound check: ${required}"
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
  "learning-review" \
  "candidate-coverage" \
  "daily-report-schema-check" \
  "outcome-diagnose" \
  "lp-diagnose" \
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
LEARNING_REPORT="reports/daily/daily_report_2099-12-30.json"
LEARNING_INVALID_REPORT="reports/daily/daily_report_2099-12-31.json"
CANDIDATE_REPORT="reports/daily/daily_report_2099-12-29.json"
cleanup() {
  rm -rf "$TMP_DIR"
  rm -f "$LEARNING_REPORT"
  rm -f "$LEARNING_INVALID_REPORT"
  rm -f "$CANDIDATE_REPORT"
}
trap cleanup EXIT

bash scripts/test_listener_health_telegram_outbound.sh >"$TMP_DIR/listener_health_tg_outbound.out"
grep -Fq "listener-health Telegram outbound test ok" "$TMP_DIR/listener_health_tg_outbound.out" || die "listener-health Telegram outbound test did not pass"

bash scripts/test_hermes_lp_diagnose.sh >"$TMP_DIR/lp_diagnose.out"
grep -Fq "Hermes LP diagnose test passed" "$TMP_DIR/lp_diagnose.out" || die "LP diagnose test did not pass"

bash scripts/test_hermes_outcome_diagnose.sh >"$TMP_DIR/outcome_diagnose.out"
grep -Fq "Hermes outcome diagnose test passed" "$TMP_DIR/outcome_diagnose.out" || die "Outcome diagnose test did not pass"

HERMES_OPS_AUDIT_LOG="$TMP_DIR/ops_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/hermes_ops.lock" \
  "$SCRIPT" command-menu >"$TMP_DIR/menu.out"

grep -Fq "系统体检" "$TMP_DIR/menu.out" || die "command-menu output missing 系统体检"
grep -Fq "锁状态" "$TMP_DIR/menu.out" || die "command-menu output missing 锁状态"
grep -Fq "标准日报流程YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 标准日报流程YYYY-MM-DD"
grep -Fq "任务状态JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 任务状态JOB_ID"
grep -Fq "查看结果JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 查看结果JOB_ID"
grep -Fq "查看日志JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 查看日志JOB_ID"
grep -Fq "诊断任务JOB_ID" "$TMP_DIR/menu.out" || die "command-menu output missing 诊断任务JOB_ID"
grep -Fq "最近任务" "$TMP_DIR/menu.out" || die "command-menu output missing 最近任务"
grep -Fq "空间快检" "$TMP_DIR/menu.out" || die "command-menu output missing 空间快检"
grep -Fq "数据库体积诊断" "$TMP_DIR/menu.out" || die "command-menu output missing 数据库体积诊断"
grep -Fq "数据库瘦身预检" "$TMP_DIR/menu.out" || die "command-menu output missing 数据库瘦身预检"
grep -Fq "学习复盘YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 学习复盘YYYY-MM-DD"
grep -Fq "学习总结YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 学习总结YYYY-MM-DD"
grep -Fq "CANDIDATE覆盖诊断YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing CANDIDATE覆盖诊断YYYY-MM-DD"
grep -Fq "日报结构检查YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing 日报结构检查YYYY-MM-DD"
grep -Fq "Outcome闭环诊断YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing Outcome闭环诊断YYYY-MM-DD"
grep -Fq "LP诊断YYYY-MM-DD" "$TMP_DIR/menu.out" || die "command-menu output missing LP诊断YYYY-MM-DD"
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

[[ ! -f "$CANDIDATE_REPORT" ]] || die "temporary candidate-coverage fixture already exists: ${CANDIDATE_REPORT}"
mkdir -p reports/daily
cat >"$CANDIDATE_REPORT" <<'JSON'
{
  "logical_date": "2099-12-29",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "replay_count": 2,
    "valid_replay_count": 2,
    "replay_coverage_rate_candidate": 0
  }
}
JSON

python3 - "$TMP_DIR/candidate_coverage.sqlite" <<'PY'
import datetime
import sqlite3
import sys

db_path = sys.argv[1]
start = int(datetime.datetime(2099, 12, 29, tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp())
conn = sqlite3.connect(db_path)
try:
    conn.executescript(
        """
        CREATE TABLE signals (
            signal_id TEXT PRIMARY KEY,
            timestamp REAL,
            created_at REAL
        );
        CREATE TABLE trade_opportunities (
            trade_opportunity_id TEXT PRIMARY KEY,
            status TEXT,
            created_at REAL,
            opportunity_json TEXT
        );
        CREATE TABLE trade_replay_examples (
            replay_id TEXT PRIMARY KEY,
            logical_date TEXT,
            replay_scope TEXT,
            trade_opportunity_id TEXT,
            opportunity_status TEXT,
            signal_ts REAL
        );
        CREATE TABLE delivery_audit (
            audit_id TEXT PRIMARY KEY,
            timestamp REAL,
            notifier_sent_at REAL,
            sent_to_telegram INTEGER,
            stage TEXT,
            opportunity_status TEXT
        );
        """
    )
    for idx in range(4):
        conn.execute("INSERT INTO signals VALUES (?, ?, ?)", (f"sig-{idx}", start + 10 + idx, start + 10 + idx))
    for idx, status in enumerate(("CANDIDATE", "BLOCKED", "INVALIDATED", "")):
        conn.execute(
            "INSERT INTO trade_opportunities VALUES (?, ?, ?, ?)",
            (f"opp-{idx}", status, start + 30 + idx, "{}"),
        )
    conn.execute("INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?)", ("replay-1", "2099-12-29", "full", "opp-1", "BLOCKED", start + 60))
    conn.execute("INSERT INTO trade_replay_examples VALUES (?, ?, ?, ?, ?, ?)", ("replay-2", "2099-12-29", "full", "", "", start + 90))
    for idx in range(5):
        conn.execute(
            "INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?)",
            (f"audit-observe-{idx}", start + 120 + idx, start + 120 + idx, 1, "observe", ""),
        )
    conn.execute(
        "INSERT INTO delivery_audit VALUES (?, ?, ?, ?, ?, ?)",
        ("audit-signal-1", start + 140, start + 140, 1, "signal", "BLOCKED"),
    )
    conn.commit()
finally:
    conn.close()
PY

HERMES_OPS_AUDIT_LOG="$TMP_DIR/candidate_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/candidate.lock" \
HERMES_OPS_ROUTER_OK=1 \
HERMES_CANDIDATE_COVERAGE_DB_PATH="$TMP_DIR/candidate_coverage.sqlite" \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" candidate-coverage --date 2099-12-29 >"$TMP_DIR/candidate.out"
for required in \
  "Chain Monitor CANDIDATE覆盖诊断｜2099-12-29" \
  "daily_report_status=present" \
  "signals_total=4" \
  "trade_opportunities_total=4" \
  "trade_opportunity_status_distribution=NONE=1 / CANDIDATE=1 / VERIFIED=0 / BLOCKED=1 / INVALIDATED=1" \
  "replay_scope_used=full" \
  "replay_examples_total=2" \
  "replay_examples_with_opportunity=1" \
  "replay_examples_status_CANDIDATE=0" \
  "delivery_audit_telegram_push_total=6" \
  "observe/NONE=5" \
  "signal/BLOCKED=1" \
  "实时推送主要是 observe/signal，不是 opportunity candidate" \
  "replay 关联层可能漏接" \
  "只读聚合诊断"
do
  grep -Fq "$required" "$TMP_DIR/candidate.out" || die "candidate-coverage output missing: ${required}"
done
for forbidden in "交易建议" "买入" "卖出" "仓位" "杠杆" "止盈" "止损" "0x"; do
  if grep -Fq "$forbidden" "$TMP_DIR/candidate.out"; then
    die "candidate-coverage output contains forbidden term: ${forbidden}"
  fi
done
grep -Fq '"command":"candidate-coverage"' "$TMP_DIR/candidate_audit.ndjson" || die "candidate-coverage audit missing command"
grep -Fq '"allowed":true' "$TMP_DIR/candidate_audit.ndjson" || die "candidate-coverage audit missing allowed=true"

[[ ! -f "$LEARNING_REPORT" ]] || die "temporary learning-review fixture already exists: ${LEARNING_REPORT}"
mkdir -p reports/daily
cat >"$LEARNING_REPORT" <<'JSON'
{
  "logical_date": "2099-12-30",
  "data_quality_summary": {
    "data_quality_status": "valid",
    "zero_activity_day": false
  },
  "run_overview": {
    "lp_signal_rows": 4
  },
  "data_source_summary": {
    "row_counts": {
      "delivery_audit": 8,
      "telegram_deliveries": 2
    }
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "replay_count": 4,
    "avg_net_pnl_bps": -5.25,
    "suppressed_avg_net_pnl_bps": -2.5,
    "replay_coverage_rate_candidate": 0,
    "valid_replay_count": 4
  },
  "trade_replay_profile_summary": {
    "blocker_grade_negative_profiles": [
      {
        "valid_sample_count": 12,
        "avg_net_pnl_bps": -7.5,
        "profile_key": "ETH|LONG|fixture"
      }
    ],
    "high_confidence_positive_profiles": []
  },
  "blocker_summary": {
    "top_blockers": {
      "replay_profile_negative": 3,
      "low_quality": 1
    },
    "verification_blocker_distribution": {
      "profile_adverse_too_high": 4
    }
  },
  "shadow_funnel_summary": {
    "shadow_candidate_count": 1,
    "shadow_verified_count": 0,
    "shadow_reason_distribution": {
      "score_below_shadow_candidate": 2
    }
  },
  "telegram_suppression_summary": {
    "messages_before_suppression_estimate": 4,
    "messages_after_suppression_actual": 2,
    "telegram_suppression_ratio": 0.5,
    "high_value_suppressed_count": 0
  },
  "candidate_frontier_summary": {
    "near_candidate_count": 0,
    "near_candidate_replay_count": 0,
    "diagnosis": "gate_closed_because_quality_low"
  },
  "lp_signal_summary": {
    "available": true,
    "lp_signal_rows": 4,
    "lp_like_signals_sqlite": 4
  },
  "lp_stage_summary": {
    "available": true,
    "by_stage": {
      "confirm": 4,
      "prealert": 0,
      "exhaustion_risk": 0,
      "unknown": 0
    }
  },
  "clmm_summary": {
    "available": true,
    "clmm_like_rows": 4
  },
  "lp_suppression_summary": {
    "available": true,
    "total": 8,
    "suppressed": 4,
    "suppression_rate": 0.5
  },
  "lp_suppression_replay_summary": {
    "available": true,
    "diagnosis": "suppression_seems_correct",
    "overall_suppressed_avg_net_pnl_bps": -2.5
  }
}
JSON

HERMES_OPS_AUDIT_LOG="$TMP_DIR/learning_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/learning.lock" \
HERMES_OPS_ROUTER_OK=1 \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" learning-review --date 2099-12-30 >"$TMP_DIR/learning.out"
for required in \
  "数据是否有效=有效" \
  "replay_source / replay_scope=persisted / full" \
  "avg_net_pnl_bps=-5.25" \
  "suppressed_avg_net_pnl_bps=-2.5" \
  "CANDIDATE 覆盖率=0" \
  "LP signal rows 是否缺失=否 value=4" \
  "主要负收益 profile=" \
  "主要 blocker=" \
  "shadow_candidate / shadow_verified=1 / 0" \
  "Telegram 推送是否可能噪音过多=否" \
  "排查 candidate/opportunity/replay 连接" \
  "今日结论：收紧" \
  "明天只建议改一个点："
do
  grep -Fq "$required" "$TMP_DIR/learning.out" || die "learning-review output missing: ${required}"
done
for forbidden in "交易建议" "买入" "卖出" "仓位" "杠杆" "止盈" "止损"; do
  if grep -Fq "$forbidden" "$TMP_DIR/learning.out"; then
    die "learning-review output contains forbidden term: ${forbidden}"
  fi
done
grep -Fq '"command":"learning-review"' "$TMP_DIR/learning_audit.ndjson" || die "learning-review audit missing command"
grep -Fq '"allowed":true' "$TMP_DIR/learning_audit.ndjson" || die "learning-review audit missing allowed=true"

[[ ! -f "$LEARNING_INVALID_REPORT" ]] || die "temporary learning-review invalid fixture already exists: ${LEARNING_INVALID_REPORT}"
cat >"$LEARNING_INVALID_REPORT" <<'JSON'
{
  "logical_date": "2099-12-31",
  "data_quality_summary": {
    "data_quality_status": "invalid",
    "zero_activity_day": true
  },
  "trade_replay_summary": {
    "replay_source": "persisted",
    "replay_scope": "full",
    "avg_net_pnl_bps": 10,
    "suppressed_avg_net_pnl_bps": 11,
    "replay_coverage_rate_candidate": 0.3
  },
  "trade_replay_profile_summary": {
    "blocker_grade_negative_profiles": [
      {
        "valid_sample_count": 99,
        "avg_net_pnl_bps": -99,
        "profile_key": "SHOULD_NOT_BE_PRINTED"
      }
    ]
  }
}
JSON
HERMES_OPS_AUDIT_LOG="$TMP_DIR/learning_invalid_audit.ndjson" \
HERMES_OPS_LOCK_PATH="$TMP_DIR/learning_invalid.lock" \
HERMES_OPS_ROUTER_OK=1 \
PATH="$TMP_DIR/bin:$PATH" \
  "$SCRIPT" learning-review --date 2099-12-31 >"$TMP_DIR/learning_invalid.out"
grep -Fq "数据是否有效=无效" "$TMP_DIR/learning_invalid.out" || die "invalid learning-review output missing invalid data status"
grep -Fq "主要负收益 profile=不评价：data_quality invalid" "$TMP_DIR/learning_invalid.out" || die "invalid learning-review evaluated profile"
grep -Fq "主要 blocker=不评价：data_quality invalid" "$TMP_DIR/learning_invalid.out" || die "invalid learning-review evaluated blocker"
grep -Fq "策略评价=跳过：data_quality invalid" "$TMP_DIR/learning_invalid.out" || die "invalid learning-review missing strategy skip"
grep -Fq "今日结论：排障" "$TMP_DIR/learning_invalid.out" || die "invalid learning-review conclusion is not 排障"
if grep -Fq "SHOULD_NOT_BE_PRINTED" "$TMP_DIR/learning_invalid.out"; then
  die "invalid learning-review printed strategy profile details"
fi

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "$SCRIPT" "$0"
fi

echo "OK: hermes_cm_ops validation passed"
