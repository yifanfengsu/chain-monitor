#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/hermes_cm_ops.sh help
  ./scripts/hermes_cm_ops.sh --help
  ./scripts/hermes_cm_ops.sh command-menu
  ./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress [--allow-today]
  ./scripts/hermes_cm_ops.sh health
  ./scripts/hermes_cm_ops.sh system-health
  ./scripts/hermes_cm_ops.sh listener-health
  ./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD [--mode fast|deep]
  ./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD [--mode fast|deep] [--auto-build]
  ./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh space-check
  ./scripts/hermes_cm_ops.sh archive-compress-check --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh weekly-review --start YYYY-MM-DD --end YYYY-MM-DD

Environment:
  HERMES_OPS_MAX_CMD_BYTES=80000
  HERMES_OPS_MAX_CMD_LINES=400

  HERMES_OPS_CMD_TIMEOUT_SEC=120
  HERMES_OPS_REPORT_TIMEOUT_SEC=900
  HERMES_OPS_CLOSE_TIMEOUT_SEC=900
  HERMES_OPS_HEALTH_TIMEOUT_SEC=180
  HERMES_OPS_DIGEST_TIMEOUT_SEC=300

Exit codes:
  0 success
  1 partial failure or command failure
  2 argument error or refused execution
USAGE
}

cmd_help() {
  cat <<'HELP'
# Chain Monitor Hermes 中文控制帮助

推荐 Telegram 控制入口：
  Telegram -> Hermes gateway -> /chain-monitor-report-analyst -> ./scripts/hermes_cm_cn_router.py

中文 Telegram 示例：
  /chain-monitor-report-analyst 命令提示
  /chain-monitor-report-analyst 系统体检
  /chain-monitor-report-analyst 监听器体检
  /chain-monitor-report-analyst 标准日报流程YYYY-MM-DD
  /chain-monitor-report-analyst 分析报告YYYY-MM-DD
  /chain-monitor-report-analyst 检查回放YYYY-MM-DD
  /chain-monitor-report-analyst 数据质量YYYY-MM-DD
  /chain-monitor-report-analyst Profile复盘YYYY-MM-DD
  /chain-monitor-report-analyst Blocker复盘YYYY-MM-DD
  /chain-monitor-report-analyst Shadow复盘YYYY-MM-DD
  /chain-monitor-report-analyst 空间检查
  /chain-monitor-report-analyst 归档压缩预检YYYY-MM-DD
  /chain-monitor-report-analyst 周复盘START到END
  /chain-monitor-report-analyst 生成日报YYYY-MM-DD
  /chain-monitor-report-analyst 深度分析报告YYYY-MM-DD
  /chain-monitor-report-analyst 生成摘要YYYY-MM-DD 快速
  /chain-monitor-report-analyst 生成摘要YYYY-MM-DD 深度
  /chain-monitor-report-analyst 每日收尾YYYY-MM-DD 我确认压缩

日期规则：
  - 日期必须是 YYYY-MM-DD。
  - 不支持 今天/昨天/前天 自动执行。
  - 也不支持 today/yesterday 自动执行。

模式规则：
  - digest/analyze 支持 快速/深度。
  - “分析报告YYYY-MM-DD”默认快速分析。
  - 自动构建只能通过“构建并分析报告YYYY-MM-DD 快速/深度”触发。

高风险命令：
  - close 必须包含“我确认压缩”。
  - 当前 UTC 日期默认受保护。
  - 不建议从 skill 提示 --allow-today。
  - 新增标准日报流程默认拒绝当前北京时间逻辑日。

安全说明：
  - 中文 Telegram 请求必须先通过 ./scripts/hermes_cm_cn_router.py。
  - report/analyze/digest/close 在 gateway 场景下不能绕过 router。
  - 输出默认脱敏。
  - 不输出 token、RPC URL、完整地址、交易 hash 或私有路径。
HELP
  echo "audit log: $(display_safe_path "$HERMES_OPS_AUDIT_LOG")"
}

cmd_command_menu() {
  cat <<'MENU'
📋 Chain Monitor 中文命令菜单

【每日检查】
- 系统体检：检查 DB / report source / market / coverage
- 监听器体检：检查监听器是否停摆、最近数据时间
- 空间检查：查看 DB / archive / reports 占用

【日报流程】
- 标准日报流程YYYY-MM-DD：daily-close + full replay + report + compare + checkpoint
- 分析报告YYYY-MM-DD：分析已存在日报
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、scope=full
- 数据质量YYYY-MM-DD：判断该日是否有效

【后验复盘】
- Profile复盘YYYY-MM-DD：查看 profile 后验
- Blocker复盘YYYY-MM-DD：查看 blocker 分布
- Shadow复盘YYYY-MM-DD：查看 shadow funnel

【维护预检】
- 归档压缩预检YYYY-MM-DD：只 dry-run，不压缩

【周复盘】
- 周复盘START到END，例如：周复盘2026-04-27到2026-05-03

规则：
- 日期必须用 YYYY-MM-DD
- 不支持 今天/昨天/前天 自动执行
- 输出默认脱敏
- 不提供交易建议
MENU
}

die() {
  refuse invalid_arguments "$*"
}

refuse() {
  local reason="$1"
  shift

  AUDIT_ALLOWED=false
  if [[ -z "${AUDIT_REFUSED_REASON:-}" ]]; then
    AUDIT_REFUSED_REASON="$reason"
  fi
  echo "error: $*" >&2
  exit 2
}

is_positive_int() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_nonnegative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

now_ms() {
  local value

  value="$(TZ=UTC date +%s%3N 2>/dev/null || true)"
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$value"
    return
  fi
  echo "$(( $(TZ=UTC date +%s) * 1000 ))"
}

random_hex() {
  local value

  if command -v openssl >/dev/null 2>&1; then
    value="$(openssl rand -hex 6 2>/dev/null || true)"
    if [[ "$value" =~ ^[0-9a-fA-F]{8,16}$ ]]; then
      echo "$value"
      return
    fi
  fi

  if [[ -r /dev/urandom ]] && command -v od >/dev/null 2>&1; then
    value="$(od -An -N6 -tx1 /dev/urandom 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ "$value" =~ ^[0-9a-fA-F]{8,16}$ ]]; then
      echo "$value"
      return
    fi
  fi

  printf '%x%x' "$$" "$(TZ=UTC date +%s)"
}

sanitize_request_id() {
  local value="$1"

  value="${value//[^A-Za-z0-9_.-]/_}"
  if [[ -z "$value" ]]; then
    value="cmops_$(TZ=UTC date +%Y%m%dT%H%M%SZ)_$(random_hex)"
  fi
  printf '%s' "$value"
}

generate_request_id() {
  printf 'cmops_%s_%s' "$(TZ=UTC date +%Y%m%dT%H%M%SZ)" "$(random_hex)"
}

hash_value() {
  local value="$1"
  local sum=""
  local rest=""

  if [[ -z "$value" ]]; then
    echo ""
    return
  fi

  if ! command -v sha256sum >/dev/null 2>&1; then
    echo "unavailable"
    return
  fi

  read -r sum rest < <(printf '%s' "$value" | sha256sum)
  printf 'sha256:%s' "$sum"
}

json_escape() {
  local value="${1-}"

  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

json_string() {
  printf '"%s"' "$(json_escape "$1")"
}

safe_basename() {
  basename -- "$1" 2>/dev/null || printf '%s' "unknown"
}

display_safe_path() {
  local path="$1"
  local rel=""

  if [[ -z "$path" ]]; then
    echo ""
  elif [[ "$path" == reports/* ]]; then
    echo "$path"
  elif [[ "$path" == "$REPO_ROOT/"* ]]; then
    rel="${path#"$REPO_ROOT"/}"
    if [[ "$rel" == reports/* ]]; then
      echo "$rel"
    else
      printf '[PRIVATE_PATH]/%s' "$(safe_basename "$path")"
    fi
  else
    printf '[PRIVATE_PATH]/%s' "$(safe_basename "$path")"
  fi
}

default_lock_path() {
  if [[ -d "/run/lock" && -w "/run/lock" ]]; then
    echo "/run/lock/chain-monitor-hermes-ops.lock"
  else
    echo "reports/hermes/.hermes_ops.lock"
  fi
}

audit_prepare() {
  local audit_dir

  audit_dir="$(dirname -- "$HERMES_OPS_AUDIT_LOG")"
  if ! mkdir -p "$audit_dir"; then
    echo "error: unable to create audit log directory" >&2
    exit 2
  fi
  if ! : >>"$HERMES_OPS_AUDIT_LOG"; then
    echo "error: unable to write audit log" >&2
    exit 2
  fi
  AUDIT_READY=1
}

audit_write() {
  local event="$1"
  local exit_code="$2"
  local duration_ms="$3"
  local line

  line="{"
  line+="\"schema\":\"chain_monitor_hermes_ops_audit_v1\""
  line+=",\"ts_utc\":$(json_string "$(TZ=UTC date +%Y-%m-%dT%H:%M:%S+00:00)")"
  line+=",\"request_id\":$(json_string "$HERMES_OPS_REQUEST_ID")"
  line+=",\"event\":$(json_string "$event")"
  line+=",\"platform\":$(json_string "${HERMES_OPS_PLATFORM:-unknown}")"
  line+=",\"actor_id_hash\":$(json_string "$AUDIT_ACTOR_ID_HASH")"
  line+=",\"chat_id_hash\":$(json_string "$AUDIT_CHAT_ID_HASH")"
  line+=",\"session_id_hash\":$(json_string "$AUDIT_SESSION_ID_HASH")"
  line+=",\"command\":$(json_string "${AUDIT_COMMAND:-unknown}")"
  line+=",\"allowed\":${AUDIT_ALLOWED}"
  line+=",\"refused_reason\":$(json_string "${AUDIT_REFUSED_REASON:-}")"
  line+=",\"date\":$(json_string "${AUDIT_DATE:-}")"
  line+=",\"mode\":$(json_string "${AUDIT_MODE:-}")"
  line+=",\"auto_build\":${AUDIT_AUTO_BUILD}"
  line+=",\"confirm_compress\":${AUDIT_CONFIRM_COMPRESS}"
  line+=",\"allow_today\":${AUDIT_ALLOW_TODAY}"
  line+=",\"exit_code\":${exit_code}"
  line+=",\"duration_ms\":${duration_ms}"
  line+=",\"lock_path\":$(json_string "$(safe_basename "${HERMES_OPS_LOCK_PATH:-}")")"
  line+=",\"workdir\":$(json_string "$(safe_basename "$REPO_ROOT")")"
  line+=",\"output_hint\":$(json_string "$(display_safe_path "${AUDIT_OUTPUT_HINT:-}")")"
  line+="}"

  if ! printf '%s\n' "$line" >>"$HERMES_OPS_AUDIT_LOG"; then
    echo "error: unable to write audit log" >&2
    return 1
  fi
}

audit_finish() {
  local exit_code="$1"
  local end_ms
  local duration_ms

  [[ "${AUDIT_READY:-0}" -eq 1 ]] || return 0
  [[ "${AUDIT_FINISH_WRITTEN:-0}" -eq 0 ]] || return 0

  AUDIT_FINISH_WRITTEN=1
  end_ms="$(now_ms)"
  duration_ms="$(( end_ms - AUDIT_START_MS ))"
  if (( duration_ms < 0 )); then
    duration_ms=0
  fi
  audit_write "finish" "$exit_code" "$duration_ms"
}

emit_request_header() {
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "audit_log=$(display_safe_path "$HERMES_OPS_AUDIT_LOG")"
}

validate_date() {
  local value="$1"
  local normalized

  [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  if ! normalized="$(TZ=UTC date -d "$value" +%F 2>/dev/null)"; then
    return 1
  fi
  [[ "$normalized" == "$value" ]]
}

display_command() {
  local arg
  local first=1

  for arg in "$@"; do
    if [[ "$first" -eq 1 ]]; then
      printf '%q' "$arg"
      first=0
    else
      printf ' %q' "$arg"
    fi
  done
}

limit_output_file() {
  local source_path="$1"
  local dest_path="$2"

  LC_ALL=C awk -v max_bytes="$MAX_CMD_BYTES" -v max_lines="$MAX_CMD_LINES" '
    BEGIN {
      bytes = 0
    }
    NR <= max_lines {
      line = $0 "\n"
      next_bytes = bytes + length(line)
      if (next_bytes <= max_bytes) {
        printf "%s", line
        bytes = next_bytes
        next
      }
      remaining = max_bytes - bytes
      if (remaining > 0) {
        printf "%s", substr(line, 1, remaining)
      }
      exit
    }
  ' "$source_path" >"$dest_path"
  redact_file_in_place "$dest_path"
}

redact_file_in_place() {
  local path="$1"
  local tmp_path="${path}.redacted"

  sed -E \
    -e 's#0x[0-9A-Fa-f]{64}#0xTX_REDACTED#g' \
    -e 's#0x[0-9A-Fa-f]{40}#0xADDR_REDACTED#g' \
    -e 's#[0-9]{6,12}:[A-Za-z0-9_-]{25,}#[REDACTED_TELEGRAM_TOKEN]#g' \
    -e 's#https?://[^[:space:]"'"'"'`<>]*(alchemy|infura|quicknode|ankr|blast|drpc|getblock|chainstack|nodereal)[^[:space:]"'"'"'`<>]*#[REDACTED_RPC_URL]#Ig' \
    -e 's#((export[[:space:]]+)?[A-Za-z0-9_]*(TELEGRAM_BOT_TOKEN|RPC_URL|API_KEY|PASSWORD|SECRET|TOKEN)[A-Za-z0-9_]*=)[^[:space:];,"}]+#\1[REDACTED_SECRET]#Ig' \
    -e 's#/(root|home|run-project)(/[^[:space:]<>"'"'"'`;,)]*)*#[PRIVATE_PATH]#g' \
    "$path" >"$tmp_path"
  mv "$tmp_path" "$path"
}

run_with_timeout() {
  local timeout_sec="$1"
  shift
  timeout "${timeout_sec}s" "$@"
}

run_command() {
  local title="$1"
  local timeout_sec="$2"
  shift 2

  local safe_title="${title//[!A-Za-z0-9_]/_}"
  local raw_path="${TMP_DIR}/${safe_title}.raw"
  local limited_path="${TMP_DIR}/${safe_title}.out"
  local rc=0

  RUN_OUTPUT="$limited_path"
  RUN_RAW_BYTES=0
  RUN_RAW_LINES=0
  RUN_TRUNCATED=0
  RUN_TIMEOUT_SEC="$timeout_sec"

  if run_with_timeout "$timeout_sec" "$@" >"$raw_path" 2>&1; then
    rc=0
  else
    rc=$?
  fi

  RUN_RAW_BYTES="$(wc -c <"$raw_path" | tr -d '[:space:]')"
  RUN_RAW_LINES="$(wc -l <"$raw_path" | tr -d '[:space:]')"
  limit_output_file "$raw_path" "$limited_path"

  if (( RUN_RAW_BYTES > MAX_CMD_BYTES || RUN_RAW_LINES > MAX_CMD_LINES )); then
    RUN_TRUNCATED=1
  fi

  return "$rc"
}

emit_command_report() {
  local title="$1"
  local rc="$2"
  shift 2

  echo "## ${title}"
  echo "command: $(display_command "$@")"
  echo "timeout_sec: ${RUN_TIMEOUT_SEC:-$CMD_TIMEOUT_SEC}"
  echo "output_limit: max_bytes=${MAX_CMD_BYTES} max_lines=${MAX_CMD_LINES}"
  if [[ "$rc" -eq 0 ]]; then
    echo "status: ok"
  else
    echo "status: failed exit_code=${rc}"
  fi
  if [[ "$rc" -eq 124 ]]; then
    echo "warning: command_timeout"
  fi
  if [[ "$RUN_TRUNCATED" -eq 1 ]]; then
    echo "warning: command_output_truncated raw_bytes=${RUN_RAW_BYTES} raw_lines=${RUN_RAW_LINES}"
  fi
  echo
  cat "$RUN_OUTPUT"
  echo
}

ensure_repo_root() {
  if [[ ! -f "Makefile" || ! -f "AGENTS.md" ]]; then
    die "must run from the chain-monitor repository root"
  fi
}

ensure_runtime() {
  command -v timeout >/dev/null 2>&1 || refuse runtime_missing_dependency "timeout command not found"
  command -v flock >/dev/null 2>&1 || refuse runtime_missing_dependency "flock command not found"
  command -v mktemp >/dev/null 2>&1 || refuse runtime_missing_dependency "mktemp command not found"

  is_positive_int "$MAX_CMD_BYTES" || refuse invalid_arguments "HERMES_OPS_MAX_CMD_BYTES must be a positive integer"
  is_positive_int "$MAX_CMD_LINES" || refuse invalid_arguments "HERMES_OPS_MAX_CMD_LINES must be a positive integer"

  is_positive_int "$CMD_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_CMD_TIMEOUT_SEC must be a positive integer"
  is_positive_int "$REPORT_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_REPORT_TIMEOUT_SEC must be a positive integer"
  is_positive_int "$CLOSE_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_CLOSE_TIMEOUT_SEC must be a positive integer"
  is_positive_int "$HEALTH_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_HEALTH_TIMEOUT_SEC must be a positive integer"
  is_positive_int "$DIGEST_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_DIGEST_TIMEOUT_SEC must be a positive integer"
  is_nonnegative_int "$LOCK_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_LOCK_TIMEOUT_SEC must be a non-negative integer"
}

acquire_lock() {
  local lock_dir

  lock_dir="$(dirname -- "$HERMES_OPS_LOCK_PATH")"
  if ! mkdir -p "$lock_dir"; then
    refuse runtime_missing_dependency "unable to create lock directory"
  fi
  if ! exec 9>"$HERMES_OPS_LOCK_PATH"; then
    refuse runtime_missing_dependency "unable to open lock file"
  fi

  if [[ "$LOCK_TIMEOUT_SEC" -gt 0 ]]; then
    if ! flock -w "$LOCK_TIMEOUT_SEC" 9; then
      refuse lock_busy "another Hermes chain-monitor operation is already running"
    fi
  elif ! flock -n 9; then
    refuse lock_busy "another Hermes chain-monitor operation is already running"
  fi
}

parse_required_date() {
  local value="$1"

  validate_date "$value" || refuse invalid_date "invalid date: ${value}"
}

beijing_today() {
  TZ=Asia/Shanghai date +%F
}

python_bin() {
  if [[ -x "./venv/bin/python" ]]; then
    printf '%s\n' "./venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    command -v python3
  else
    refuse runtime_missing_dependency "python3 or ./venv/bin/python is required"
  fi
}

daily_report_json_path() {
  local report_date="$1"
  printf 'reports/daily/daily_report_%s.json' "$report_date"
}

require_daily_report_json() {
  local report_date="$1"
  local path

  path="$(daily_report_json_path "$report_date")"
  if [[ ! -f "$path" ]]; then
    echo "数据不足：缺少 ${path}" >&2
    echo "请先运行：标准日报流程${report_date}" >&2
    echo "或：生成日报${report_date}" >&2
    exit 1
  fi
}

is_gateway_router_required() {
  [[ "${HERMES_EXEC_ASK:-}" == "1" || "${HERMES_OPS_REQUIRE_ROUTER:-}" == "1" ]]
}

is_router_guarded_command() {
  case "$1" in
    report|digest|analyze|close|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|archive-compress-check|weekly-review)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

enforce_router_guard() {
  local command_name="$1"

  if is_router_guarded_command "$command_name" && is_gateway_router_required && [[ "${HERMES_OPS_ROUTER_OK:-}" != "1" ]]; then
    AUDIT_ALLOWED=false
    AUDIT_REFUSED_REASON="router_required"
    cat >&2 <<'MESSAGE'
❌ 已拒绝：Telegram/Hermes dated operation 必须先经过中文命令解析器。
请使用：/chain-monitor-report-analyst 分析报告YYYY-MM-DD
MESSAGE
    exit 2
  fi
}

fail_runtime() {
  echo "error: $*" >&2
  exit 1
}

canonical_daily_report_present() {
  local report_date="$1"

  [[ -f "reports/daily/daily_report_${report_date}.json" || -f "reports/daily/daily_report_${report_date}.md" ]]
}

daily_compare_paths_for_date() {
  local report_date="$1"

  if [[ ! -d "reports/daily_compare" ]]; then
    return
  fi

  find reports/daily_compare -maxdepth 1 -type f \
    \( -name "daily_compare_${report_date}.json" \
    -o -name "daily_compare_${report_date}.md" \
    -o -name "daily_compare_${report_date}_*.json" \
    -o -name "daily_compare_${report_date}_*.md" \) \
    | LC_ALL=C sort
}

daily_compare_present() {
  local report_date="$1"
  local -a paths=()

  mapfile -t paths < <(daily_compare_paths_for_date "$report_date")
  [[ "${#paths[@]}" -gt 0 ]]
}

digest_header_value() {
  local path="$1"
  local key="$2"
  local line=""

  line="$(grep -m1 -F "${key}=" "$path" 2>/dev/null || true)"
  printf '%s' "${line#*=}"
}

run_analyze_step() {
  local title="$1"
  local timeout_sec="$2"
  shift 2

  local rc=0

  echo "command: $(display_command "$@")"
  if run_command "$title" "$timeout_sec" "$@"; then
    rc=0
  else
    rc=$?
  fi

  if [[ "$rc" -eq 0 ]]; then
    echo "command_status=ok"
    if [[ "$RUN_TRUNCATED" -eq 1 ]]; then
      echo "warning=command_output_truncated raw_bytes=${RUN_RAW_BYTES} raw_lines=${RUN_RAW_LINES}"
    fi
  else
    emit_command_report "$title" "$rc" "$@"
  fi

  return "$rc"
}

prepare_digest_env() {
  export HERMES_DIGEST_WORKDIR="$REPO_ROOT"
  export HERMES_DIGEST_REDACT=1
}

cmd_report() {
  local report_date=""
  local -a cmd=()
  local rc=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "report --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown report argument"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "report requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_OUTPUT_HINT="reports/daily/daily_report_${report_date}.*"
  AUDIT_ALLOWED=true

  cmd=(make report-daily-date "DATE=${report_date}")
  echo "command: $(display_command "${cmd[@]}")"
  if run_command "report_${report_date}" "$REPORT_TIMEOUT_SEC" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi
  emit_command_report "report ${report_date}" "$rc" "${cmd[@]}"

  if [[ "$rc" -eq 0 ]]; then
    exit 0
  fi
  exit 1
}

cmd_close() {
  local close_date=""
  local confirm_compress=0
  local allow_today=0
  local today_utc
  local -a cmd=()
  local rc=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "close --date requires YYYY-MM-DD"
        close_date="$2"
        shift 2
        ;;
      --confirm-compress)
        confirm_compress=1
        AUDIT_CONFIRM_COMPRESS=true
        shift
        ;;
      --allow-today)
        allow_today=1
        AUDIT_ALLOW_TODAY=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown close argument"
        ;;
    esac
  done

  [[ -n "$close_date" ]] || die "close requires --date YYYY-MM-DD"
  parse_required_date "$close_date"
  AUDIT_DATE="$close_date"
  if [[ "$confirm_compress" -ne 1 ]]; then
    refuse missing_confirm_compress "refusing close: --confirm-compress is required"
  fi

  today_utc="$(TZ=UTC date +%F)"
  if [[ "$close_date" == "$today_utc" && "$allow_today" -ne 1 ]]; then
    refuse current_utc_date_protected "refusing close for current UTC date ${today_utc}; pass --allow-today only after explicit approval"
  fi
  AUDIT_ALLOWED=true

  cmd=(make daily-close "DATE=${close_date}" COMPRESS=YES CONFIRM=YES)
  if [[ "$allow_today" -eq 1 ]]; then
    cmd+=(ALLOW_TODAY=YES)
  fi

  echo "STATE-CHANGING COMMAND: daily-close with confirmed compression."
  echo "exact command: $(display_command "${cmd[@]}")"
  if run_command "close_${close_date}" "$CLOSE_TIMEOUT_SEC" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi
  emit_command_report "daily-close ${close_date}" "$rc" "${cmd[@]}"

  if [[ "$rc" -eq 0 ]]; then
    exit 0
  fi
  exit 1
}

append_health_section() {
  local output_path="$1"
  local title="$2"
  shift 2
  local rc=0
  local status

  echo "== ${title} =="
  echo "command: $(display_command "$@")"

  if run_command "$title" "$HEALTH_TIMEOUT_SEC" "$@"; then
    rc=0
  else
    rc=$?
  fi

  if [[ "$rc" -eq 0 ]]; then
    status="ok"
  else
    status="failed exit_code=${rc}"
    HEALTH_PARTIAL=1
  fi

  {
    echo "## ${title}"
    echo "command=$(display_command "$@")"
    echo "command_timeout_sec=${RUN_TIMEOUT_SEC:-$HEALTH_TIMEOUT_SEC}"
    echo "command_limit=max_bytes=${MAX_CMD_BYTES} max_lines=${MAX_CMD_LINES}"
    echo "command_status=${status}"
    if [[ "$rc" -eq 124 ]]; then
      echo "warning=command_timeout"
    fi
    if [[ "$RUN_TRUNCATED" -eq 1 ]]; then
      echo "warning=command_output_truncated raw_bytes=${RUN_RAW_BYTES} raw_lines=${RUN_RAW_LINES}"
    fi
    echo
    cat "$RUN_OUTPUT"
    echo
  } >>"$output_path"

  HEALTH_SUMMARY+=("${title}: ${status}")
  echo "status: ${status}"
  echo
}

cmd_health() {
  local output_dir="reports/hermes"
  local final_output="${output_dir}/hermes_health_latest.md"
  local tmp_output

  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="$final_output"
  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_health_latest.XXXXXX.md")"
  HEALTH_TMP_OUTPUT="$tmp_output"
  HEALTH_PARTIAL=0
  HEALTH_SUMMARY=()

  {
    echo "# Hermes Health Latest"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "workdir=$(safe_basename "$REPO_ROOT")"
    echo "output_policy=aggregate diagnostics only; no raw rows or raw payloads requested"
    echo "max_cmd_bytes=${MAX_CMD_BYTES}"
    echo "max_cmd_lines=${MAX_CMD_LINES}"
    echo "cmd_timeout_sec=${HEALTH_TIMEOUT_SEC}"
    echo
  } >"$tmp_output"

  append_health_section "$tmp_output" "Env Check" make env-check
  append_health_section "$tmp_output" "DB Integrity Fast" make db-integrity DB_INTEGRITY_FAST=YES
  append_health_section "$tmp_output" "DB Summary" make db-summary
  append_health_section "$tmp_output" "Opportunity DB" make opportunity-db
  append_health_section "$tmp_output" "Report Source Fast" make report-source-fast
  append_health_section "$tmp_output" "Coverage" make coverage

  mv "$tmp_output" "$final_output"
  HEALTH_TMP_OUTPUT=""

  echo "Hermes health summary"
  echo "output: ${final_output}"
  printf '%s\n' "${HEALTH_SUMMARY[@]}"

  if [[ "$HEALTH_PARTIAL" -eq 0 ]]; then
    exit 0
  fi
  exit 1
}

cmd_system_health() {
  local output_dir="reports/hermes"
  local final_output="${output_dir}/hermes_system_health_latest.md"
  local tmp_output

  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="$final_output"
  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_system_health_latest.XXXXXX.md")"
  HEALTH_TMP_OUTPUT="$tmp_output"
  HEALTH_PARTIAL=0
  HEALTH_SUMMARY=()

  {
    echo "# Hermes System Health Latest"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "workdir=$(safe_basename "$REPO_ROOT")"
    echo "output_policy=aggregate diagnostics only; no raw rows or raw payloads requested"
    echo "max_cmd_bytes=${MAX_CMD_BYTES}"
    echo "max_cmd_lines=${MAX_CMD_LINES}"
    echo "cmd_timeout_sec=${HEALTH_TIMEOUT_SEC}"
    echo
  } >"$tmp_output"

  append_health_section "$tmp_output" "SQLite DB Report" make db-report
  append_health_section "$tmp_output" "Report Source Fast" make report-source-fast
  append_health_section "$tmp_output" "Market Health" make health
  append_health_section "$tmp_output" "Coverage" make coverage

  mv "$tmp_output" "$final_output"
  HEALTH_TMP_OUTPUT=""

  echo "Hermes 系统体检摘要"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "output=$(display_safe_path "$final_output")"
  echo "SQLite 状态摘要: ${HEALTH_SUMMARY[0]:-unknown}"
  echo "report source 状态: ${HEALTH_SUMMARY[1]:-unknown}"
  echo "market health 状态: ${HEALTH_SUMMARY[2]:-unknown}"
  echo "coverage 状态: ${HEALTH_SUMMARY[3]:-unknown}"
  if [[ "$HEALTH_PARTIAL" -eq 0 ]]; then
    echo "warning 摘要: none"
    exit 0
  fi
  echo "warning 摘要: one_or_more_checks_failed"
  exit 1
}

cmd_listener_health() {
  local output_dir="reports/hermes"
  local final_output="${output_dir}/hermes_listener_health_latest.md"
  local tmp_output
  local pgrep_rc=0

  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="$final_output"
  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_listener_health_latest.XXXXXX.md")"

  {
    echo "# Hermes Listener Health Latest"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "workdir=$(safe_basename "$REPO_ROOT")"
    echo "output_policy=read-only listener diagnostics; no restart or kill"
    echo
  } >"$tmp_output"

  if run_command "listener_process" "$CMD_TIMEOUT_SEC" pgrep -af "app/main.py|app.main|make run|make run-research|chain-monitor"; then
    pgrep_rc=0
  else
    pgrep_rc=$?
  fi
  {
    echo "## Listener Process"
    echo "command=pgrep -af [listener-pattern]"
    if [[ "$pgrep_rc" -eq 0 ]]; then
      echo "status=process_match_found"
    else
      echo "status=no_process_match_or_pgrep_failed"
    fi
    cat "$RUN_OUTPUT"
    echo
  } >>"$tmp_output"

  "$(python_bin)" - "$final_output" >>"$tmp_output" <<'PY'
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

_final_output = sys.argv[1]
root = Path.cwd()
db_path = root / "data" / "chain_monitor.sqlite"


def safe_scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> object:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error as exc:
        return f"unavailable:{exc.__class__.__name__}"
    return row[0] if row else None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def max_field(conn: sqlite3.Connection, table: str, candidates: list[str]) -> str:
    if not table_exists(conn, table):
        return "missing_table"
    cols = columns(conn, table)
    for col in candidates:
        if col in cols:
            value = safe_scalar(conn, f"SELECT MAX({col}) FROM {table}")
            return str(value if value is not None else "empty")
    return "missing_time_column"


def latest_archive_mtime() -> str:
    archive = root / "app" / "data" / "archive"
    if not archive.exists():
        return "missing_archive_dir"
    latest: tuple[float, Path] | None = None
    for path in archive.rglob("*"):
        if not path.is_file():
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if latest is None or mtime > latest[0]:
            latest = (mtime, path)
    if latest is None:
        return "empty"
    return f"{latest[0]:.0f} {latest[1].name}"


def latest_zero_activity() -> str:
    path = root / "reports" / "daily" / "daily_report_latest.json"
    if not path.exists():
        return "missing_latest_daily_report"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return f"unavailable:{exc.__class__.__name__}"
    quality = payload.get("data_quality_summary") if isinstance(payload, dict) else {}
    if not isinstance(quality, dict):
        return "missing_data_quality_summary"
    return str(quality.get("zero_activity_day", "unknown"))


def spill_summary() -> str:
    candidates = [
        root / "data" / "queue_spill",
        root / "app" / "data" / "queue_spill",
        root / "reports" / "queue_spill",
    ]
    found = []
    for base in candidates:
        if not base.exists():
            continue
        size = 0
        for path in base.rglob("*"):
            if path.is_file():
                try:
                    size += path.stat().st_size
                except OSError:
                    pass
        found.append(f"{base.name}:{size}B")
    return ", ".join(found) if found else "not_found"


print("## Listener Data Freshness")
if db_path.exists():
    conn = sqlite3.connect(db_path)
    try:
        print(f"SQLite 最新 logical_date: {max_field(conn, 'signals', ['logical_date'])}")
        print(f"最近 raw_events 时间: {max_field(conn, 'raw_events', ['timestamp', 'block_timestamp', 'created_at', 'updated_at'])}")
        print(f"最近 parsed_events 时间: {max_field(conn, 'parsed_events', ['timestamp', 'block_timestamp', 'created_at', 'updated_at'])}")
        print(f"最近 signals 时间: {max_field(conn, 'signals', ['timestamp', 'created_at', 'updated_at', 'notifier_sent_at'])}")
        if table_exists(conn, "runtime_heartbeats"):
            print(f"runtime_heartbeats 最新 check_ts: {safe_scalar(conn, 'SELECT MAX(check_ts) FROM runtime_heartbeats')}")
    finally:
        conn.close()
else:
    print("SQLite 状态: missing data/chain_monitor.sqlite")
print(f"最近 archive 修改时间: {latest_archive_mtime()}")
print(f"latest zero_activity_day: {latest_zero_activity()}")
print(f"queue spill: {spill_summary()}")
PY

  mv "$tmp_output" "$final_output"

  echo "Hermes 监听器体检摘要"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "output=$(display_safe_path "$final_output")"
  if [[ "$pgrep_rc" -eq 0 ]]; then
    echo "listener_process=process_match_found"
  else
    echo "listener_process=no_process_match_or_pgrep_failed"
  fi
  grep -E '^(最近 raw_events 时间|最近 parsed_events 时间|最近 signals 时间|最近 archive 修改时间|latest zero_activity_day|SQLite 最新 logical_date|queue spill):' "$final_output" || true
  echo "action=只读体检；未重启 listener。"
  exit 0
}

append_flow_step() {
  local output_path="$1"
  local title="$2"
  local timeout_sec="$3"
  shift 3
  local rc=0

  if run_command "$title" "$timeout_sec" "$@"; then
    rc=0
  else
    rc=$?
  fi

  {
    echo "## ${title}"
    echo "status=$([[ "$rc" -eq 0 ]] && echo ok || echo failed)"
    echo "exit_code=${rc}"
    echo "timeout_sec=${RUN_TIMEOUT_SEC:-$timeout_sec}"
    if [[ "$RUN_TRUNCATED" -eq 1 ]]; then
      echo "warning=command_output_truncated raw_bytes=${RUN_RAW_BYTES} raw_lines=${RUN_RAW_LINES}"
    fi
    echo
    cat "$RUN_OUTPUT"
    echo
  } >>"$output_path"

  FLOW_STEP_SUMMARY+=("${title}: $([[ "$rc" -eq 0 ]] && echo ok || echo failed)")
  return "$rc"
}

cmd_daily_flow() {
  local report_date=""
  local today_bj
  local output_dir="reports/hermes"
  local final_output
  local latest_output="${output_dir}/hermes_daily_flow_latest.md"
  local tmp_output

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "daily-flow --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown daily-flow argument"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "daily-flow requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  today_bj="$(beijing_today)"
  if [[ "$report_date" == "$today_bj" ]]; then
    refuse current_beijing_date_protected "refusing daily-flow for current Beijing logical date ${today_bj}"
  fi

  AUDIT_ALLOWED=true
  final_output="${output_dir}/hermes_daily_flow_${report_date}.md"
  AUDIT_OUTPUT_HINT="$final_output"
  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_daily_flow_${report_date}.XXXXXX.md")"
  FLOW_STEP_SUMMARY=()

  {
    echo "# Hermes Daily Flow ${report_date}"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "beijing_today_guard=${today_bj}"
    echo "compression=disabled"
    echo "compact=disabled"
    echo "vacuum=disabled"
    echo "prune=disabled"
    echo
  } >"$tmp_output"

  append_flow_step "$tmp_output" "daily-close" "$CLOSE_TIMEOUT_SEC" make daily-close "DATE=${report_date}" || {
    mv "$tmp_output" "$final_output"
    cp "$final_output" "$latest_output"
    echo "标准日报流程失败：daily-close"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo "output=$(display_safe_path "$final_output")"
    printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
    exit 1
  }
  append_flow_step "$tmp_output" "trade-replay-full" "$REPORT_TIMEOUT_SEC" make trade-replay-full "DATE=${report_date}" || {
    mv "$tmp_output" "$final_output"
    cp "$final_output" "$latest_output"
    echo "标准日报流程失败：trade-replay-full"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo "output=$(display_safe_path "$final_output")"
    printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
    exit 1
  }
  append_flow_step "$tmp_output" "report-daily-date" "$REPORT_TIMEOUT_SEC" make report-daily-date "DATE=${report_date}" || {
    mv "$tmp_output" "$final_output"
    cp "$final_output" "$latest_output"
    echo "标准日报流程失败：report-daily-date"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo "output=$(display_safe_path "$final_output")"
    printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
    exit 1
  }
  append_flow_step "$tmp_output" "daily-compare" "$REPORT_TIMEOUT_SEC" make daily-compare "DATE=${report_date}" || {
    mv "$tmp_output" "$final_output"
    cp "$final_output" "$latest_output"
    echo "标准日报流程失败：daily-compare"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo "output=$(display_safe_path "$final_output")"
    printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
    exit 1
  }
  append_flow_step "$tmp_output" "sqlite-checkpoint" "$CMD_TIMEOUT_SEC" make sqlite-checkpoint || {
    mv "$tmp_output" "$final_output"
    cp "$final_output" "$latest_output"
    echo "标准日报流程失败：sqlite-checkpoint"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo "output=$(display_safe_path "$final_output")"
    printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
    exit 1
  }

  mv "$tmp_output" "$final_output"
  cp "$final_output" "$latest_output"

  echo "标准日报流程完成"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "date=${report_date}"
  echo "output=$(display_safe_path "$final_output")"
  echo "latest=$(display_safe_path "$latest_output")"
  printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
  echo "未执行 archive 压缩、compact、vacuum、prune。"
  exit 0
}

cmd_replay_check() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "replay-check --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown replay-check argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "replay-check requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  require_daily_report_json "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

date = sys.argv[1]
path = Path("reports/daily") / f"daily_report_{date}.json"
payload = json.loads(path.read_text(encoding="utf-8"))
summary = payload.get("trade_replay_summary") if isinstance(payload, dict) else {}
if not isinstance(summary, dict):
    summary = {}

scope_counts = {}
db_path = Path("data/chain_monitor.sqlite")
if db_path.exists():
    try:
        conn = sqlite3.connect(db_path)
        try:
            table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_replay_examples'").fetchone()
            if table:
                for scope, count in conn.execute(
                    "SELECT replay_scope, COUNT(*) FROM trade_replay_examples WHERE logical_date=? GROUP BY replay_scope",
                    (date,),
                ):
                    scope_counts[str(scope or "")] = int(count)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        scope_counts = {"sqlite_error": exc.__class__.__name__}

print("回放检查")
print(f"date={date}")
for key in [
    "replay_source",
    "replay_scope",
    "persisted_rows_found",
    "replay_count",
    "valid_replay_count",
    "avg_net_pnl_bps",
    "suppressed_replay_count",
    "suppressed_avg_net_pnl_bps",
    "replay_coverage_rate_candidate",
]:
    print(f"{key}={summary.get(key, 'missing')}")
print(f"scope_counts={json.dumps(scope_counts, ensure_ascii=False, sort_keys=True)}")
if summary.get("replay_source") != "persisted" or summary.get("replay_scope") != "full":
    print("warning=回放不是 persisted/full；建议运行 标准日报流程YYYY-MM-DD 后再复查。")
PY
}

cmd_data_quality() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "data-quality --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown data-quality argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "data-quality requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  require_daily_report_json "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

date = sys.argv[1]
payload = json.loads((Path("reports/daily") / f"daily_report_{date}.json").read_text(encoding="utf-8"))
quality = payload.get("data_quality_summary") if isinstance(payload, dict) else {}
replay = payload.get("trade_replay_summary") if isinstance(payload, dict) else {}
coverage = payload.get("major_coverage_summary") or payload.get("majors_coverage_summary") if isinstance(payload, dict) else {}
market = payload.get("market_context_health") if isinstance(payload, dict) else {}
quality = quality if isinstance(quality, dict) else {}
replay = replay if isinstance(replay, dict) else {}
coverage = coverage if isinstance(coverage, dict) else {}
market = market if isinstance(market, dict) else {}

lp_rows = payload.get("lp_signal_rows", payload.get("lp_rows", "missing")) if isinstance(payload, dict) else "missing"
status = quality.get("data_quality_status", "missing")
zero = quality.get("zero_activity_day", "missing")
print("数据质量检查")
print(f"date={date}")
print(f"data_quality_status={status}")
print(f"zero_activity_day={zero}")
print(f"active_hours={quality.get('active_hours', quality.get('active_duration', 'missing'))}")
print(f"signal_count={quality.get('signals_count', quality.get('signal_count', 'missing'))}")
print(f"raw_event_count={quality.get('raw_events_count', quality.get('raw_event_count', 'missing'))}")
print(f"parsed_event_count={quality.get('parsed_events_count', quality.get('parsed_event_count', 'missing'))}")
print(f"lp_signal_rows={lp_rows}")
print(f"market_context_success_rate={quality.get('market_context_success_rate', market.get('market_context_attempt_success_rate', 'missing'))}")
print(f"db_archive_mirror_match_rate={quality.get('db_archive_mirror_match_rate', 'missing')}")
print(f"db_archive_mismatch_summary={quality.get('db_archive_mismatch_status', quality.get('mismatch_categories', 'missing'))}")
print(f"major_coverage_summary_keys={','.join(sorted(coverage.keys())[:12]) if coverage else 'missing'}")
print(f"trade_replay_missing={not bool(replay.get('trade_replay_available', replay))}")
print(f"replay_source={replay.get('replay_source', 'missing')}")
print(f"replay_scope={replay.get('replay_scope', 'missing')}")
if status == "invalid_or_no_activity" or zero is True:
    print("结论=该日不适合用于策略质量判断，只适合运维排障。")
PY
}

cmd_profile_review() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "profile-review --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown profile-review argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "profile-review requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

date = sys.argv[1]
db = Path("data/chain_monitor.sqlite")
print("Profile复盘")
print(f"date={date}")
if not db.exists():
    print("数据不足：缺少 data/chain_monitor.sqlite")
    print(f"请先运行：标准日报流程{date}")
    raise SystemExit(0)
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
try:
    table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_replay_profile_daily_stats'").fetchone()
    if not table:
        print("数据不足：缺少 trade_replay_profile_daily_stats")
        print(f"请先运行：标准日报流程{date}")
        raise SystemExit(0)
    rows = conn.execute(
        """
        SELECT logical_date, replay_scope, profile_key, valid_sample_count, win_rate,
               avg_net_pnl_bps, clean_followthrough_rate, bad_entry_rate,
               absorption_reversal_rate, chop_rate, recommended_action
        FROM trade_replay_profile_daily_stats
        WHERE logical_date=? AND replay_scope='full'
        ORDER BY valid_sample_count DESC, avg_net_pnl_bps DESC
        LIMIT 30
        """,
        (date,),
    ).fetchall()
finally:
    conn.close()
if not rows:
    print("数据不足：没有 full replay profile 后验。")
    print(f"请先运行：标准日报流程{date} 或完整回放/报告流程。")
else:
    print(f"profile_count={len(rows)}")
    for index, row in enumerate(rows[:10], 1):
        print(
            f"{index}. samples={row['valid_sample_count']} avg_net_pnl_bps={row['avg_net_pnl_bps']} "
            f"clean={row['clean_followthrough_rate']} bad_entry={row['bad_entry_rate']} "
            f"absorption={row['absorption_reversal_rate']} chop={row['chop_rate']} "
            f"action={row['recommended_action']} profile={row['profile_key']}"
        )
    if len(rows) > 10:
        print("说明=仅展示样本最多的前 10 个 profile，完整查询限制为 30 行。")
PY
}

cmd_blocker_review() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "blocker-review --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown blocker-review argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "blocker-review requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

date = sys.argv[1]
print("Blocker复盘")
print(f"date={date}")
counters: Counter[str] = Counter()
db = Path("data/chain_monitor.sqlite")
if db.exists():
    try:
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        try:
            table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_opportunities'").fetchone()
            if table:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(trade_opportunities)").fetchall()}
                if {"created_at", "opportunity_json"}.issubset(cols):
                    rows = conn.execute(
                        "SELECT opportunity_json FROM trade_opportunities WHERE date(created_at, 'unixepoch')=?",
                        (date,),
                    ).fetchall()
                    for row in rows:
                        try:
                            payload = json.loads(row["opportunity_json"] or "{}")
                        except json.JSONDecodeError:
                            payload = {}
                        for key in (
                            "trade_opportunity_primary_blocker",
                            "trade_opportunity_primary_hard_blocker",
                            "trade_opportunity_primary_verification_blocker",
                            "blocker_type",
                        ):
                            value = str(payload.get(key) or "").strip()
                            if value:
                                counters[value] += 1
                        for key in ("trade_opportunity_blockers", "trade_opportunity_hard_blockers", "trade_opportunity_verification_blockers"):
                            values = payload.get(key)
                            if isinstance(values, list):
                                for item in values:
                                    value = str(item or "").strip()
                                    if value:
                                        counters[value] += 1
                elif "primary_blocker" in cols:
                    for blocker, count in conn.execute(
                        "SELECT primary_blocker, COUNT(*) FROM trade_opportunities GROUP BY primary_blocker"
                    ):
                        if blocker:
                            counters[str(blocker)] += int(count)
                else:
                    print("schema_warning=trade_opportunities 缺少可用 blocker 字段，已降级。")
            else:
                print("schema_warning=缺少 trade_opportunities 表。")
        finally:
            conn.close()
    except sqlite3.Error as exc:
        print(f"sqlite_warning={exc.__class__.__name__}")
else:
    print("sqlite_warning=missing data/chain_monitor.sqlite")

report = Path("reports/daily") / f"daily_report_{date}.json"
if report.exists():
    try:
        payload = json.loads(report.read_text(encoding="utf-8"))
        replay = payload.get("trade_replay_summary") if isinstance(payload, dict) else {}
        if isinstance(replay, dict):
            for item in replay.get("blocker_grade_negative_profiles", []) or []:
                if isinstance(item, dict) and item.get("recommended_action"):
                    counters[str(item["recommended_action"])] += int(item.get("valid_sample_count") or 0)
    except Exception as exc:  # noqa: BLE001
        print(f"daily_report_warning={exc.__class__.__name__}")

for key in ("replay_profile_negative", "no_trade_lock", "low_quality", "late_or_chase", "history_completion_too_low"):
    print(f"{key}={counters.get(key, 0)}")
print("primary_blocker_distribution=" + json.dumps(dict(counters.most_common(12)), ensure_ascii=False, sort_keys=True))
print("说明=不建议放宽 blocker；需要周度样本确认后再评估。")
PY
}

cmd_shadow_review() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "shadow-review --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown shadow-review argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "shadow-review requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  require_daily_report_json "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

date = sys.argv[1]
payload = json.loads((Path("reports/daily") / f"daily_report_{date}.json").read_text(encoding="utf-8"))
summary = payload.get("shadow_funnel_summary") or payload.get("shadow_opportunity_summary") if isinstance(payload, dict) else {}
summary = summary if isinstance(summary, dict) else {}
reasons = summary.get("shadow_reason_distribution") if isinstance(summary.get("shadow_reason_distribution"), dict) else {}
blocked = summary.get("shadow_blocked_reasons") if isinstance(summary.get("shadow_blocked_reasons"), dict) else {}
verified = summary.get("shadow_verified_count", 0)
print("Shadow复盘")
print(f"date={date}")
print(f"shadow_evaluated={summary.get('shadow_evaluated_count', summary.get('shadow_input_count', 'missing'))}")
print(f"shadow_candidate_count={summary.get('shadow_candidate_count', 'missing')}")
print(f"shadow_verified_count={verified}")
print(f"near_candidate_but_blocked={reasons.get('near_candidate_but_blocked', blocked.get('near_candidate_but_blocked', 0))}")
print(f"score_below_shadow_candidate={reasons.get('score_below_shadow_candidate', blocked.get('score_below_shadow_candidate', 0))}")
print(f"hard_blocker:replay_profile_negative={reasons.get('hard_blocker:replay_profile_negative', blocked.get('hard_blocker:replay_profile_negative', 0))}")
if not verified:
    print("说明=shadow_verified_count=0；不建议放宽 VERIFIED。")
PY
}

cmd_space_check() {
  local output_dir="reports/hermes"
  local final_output="${output_dir}/hermes_space_check_latest.md"
  local tmp_output
  local rc=0

  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="$final_output"
  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_space_check_latest.XXXXXX.md")"
  HEALTH_SUMMARY=()

  {
    echo "# Hermes Space Check Latest"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "output_policy=read-only disk usage; no delete, no vacuum, no prune"
    echo
  } >"$tmp_output"

  for title_cmd in \
    "SQLite Files::find data -maxdepth 1 -name chain_monitor.sqlite* -exec du -h {} +" \
    "Archive Usage::du -sh app/data/archive" \
    "Reports Usage::du -sh reports" \
    "DB Size::make db-size" \
    "DB Value Audit::make db-value-audit" \
    "DB Retention Dry Run::make db-retention"
  do
    local title="${title_cmd%%::*}"
    local cmd_string="${title_cmd#*::}"
    local -a cmd_parts=()
    read -r -a cmd_parts <<<"$cmd_string"
    if run_command "$title" "$HEALTH_TIMEOUT_SEC" "${cmd_parts[@]}"; then
      rc=0
    else
      rc=$?
    fi
    {
      echo "## ${title}"
      echo "status=$([[ "$rc" -eq 0 ]] && echo ok || echo failed)"
      echo
      cat "$RUN_OUTPUT"
      echo
    } >>"$tmp_output"
    HEALTH_SUMMARY+=("${title}: $([[ "$rc" -eq 0 ]] && echo ok || echo failed)")
  done

  mv "$tmp_output" "$final_output"

  echo "空间检查摘要"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "output=$(display_safe_path "$final_output")"
  printf '%s\n' "${HEALTH_SUMMARY[@]}"
  echo "未执行删除、压缩、sqlite-checkpoint、vacuum、prune。"
  exit 0
}

cmd_archive_compress_check() {
  local report_date=""
  local rc=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "archive-compress-check --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown archive-compress-check argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "archive-compress-check requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="archive compression dry-run ${report_date}"

  if run_command "archive_compress_check_${report_date}" "$REPORT_TIMEOUT_SEC" make archive-compress-dry-run "DATE=${report_date}"; then
    rc=0
  else
    rc=$?
  fi
  echo "归档压缩预检"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "date=${report_date}"
  echo "status=$([[ "$rc" -eq 0 ]] && echo ok || echo failed)"
  cat "$RUN_OUTPUT"
  echo "提醒：这只是预检，没有压缩。"
  echo "如果要真正压缩，暂时请 SSH 手动确认，Telegram 当前不开放执行压缩。"
  [[ "$rc" -eq 0 ]] && exit 0
  exit 1
}

cmd_weekly_review() {
  local start_date=""
  local end_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --start)
        [[ $# -ge 2 ]] || die "weekly-review --start requires YYYY-MM-DD"
        start_date="$2"
        shift 2
        ;;
      --end)
        [[ $# -ge 2 ]] || die "weekly-review --end requires YYYY-MM-DD"
        end_date="$2"
        shift 2
        ;;
      *) die "unknown weekly-review argument" ;;
    esac
  done

  [[ -n "$start_date" && -n "$end_date" ]] || die "weekly-review requires --start and --end"
  parse_required_date "$start_date"
  parse_required_date "$end_date"
  local range_rc=0
  set +e
  "$(python_bin)" - "$start_date" "$end_date" >/dev/null <<'PY'
import datetime
import sys
start = datetime.date.fromisoformat(sys.argv[1])
end = datetime.date.fromisoformat(sys.argv[2])
if start > end:
    raise SystemExit(2)
if (end - start).days > 13:
    raise SystemExit(3)
PY
  range_rc=$?
  set -e
  case "$range_rc" in
    0) ;;
    2) refuse invalid_date_range "weekly-review start must be <= end" ;;
    3) refuse date_range_too_large "weekly-review range must be <= 14 days" ;;
    *) refuse invalid_date_range "weekly-review date range invalid" ;;
  esac

  AUDIT_DATE="${start_date}..${end_date}"
  AUDIT_ALLOWED=true

  "$(python_bin)" - "$start_date" "$end_date" <<'PY'
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

start = dt.date.fromisoformat(sys.argv[1])
end = dt.date.fromisoformat(sys.argv[2])
dates = [(start + dt.timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
print("周复盘")
print(f"range={start.isoformat()}..{end.isoformat()}")

zero_days = 0
valid_days = 0
persisted_full_days = 0
shadow_candidate = 0
shadow_verified = 0
data_warnings = []
suppressed_avg_values = []
for date in dates:
    path = Path("reports/daily") / f"daily_report_{date}.json"
    if not path.exists():
        data_warnings.append(f"{date}:missing_daily_report")
        continue
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        data_warnings.append(f"{date}:{exc.__class__.__name__}")
        continue
    quality = payload.get("data_quality_summary") if isinstance(payload, dict) else {}
    replay = payload.get("trade_replay_summary") if isinstance(payload, dict) else {}
    shadow = payload.get("shadow_funnel_summary") or payload.get("shadow_opportunity_summary") if isinstance(payload, dict) else {}
    quality = quality if isinstance(quality, dict) else {}
    replay = replay if isinstance(replay, dict) else {}
    shadow = shadow if isinstance(shadow, dict) else {}
    if quality.get("zero_activity_day") is True:
        zero_days += 1
    if quality.get("data_quality_status") == "valid":
        valid_days += 1
    if replay.get("replay_source") == "persisted" and replay.get("replay_scope") == "full":
        persisted_full_days += 1
    if isinstance(replay.get("suppressed_avg_net_pnl_bps"), (int, float)):
        suppressed_avg_values.append(float(replay["suppressed_avg_net_pnl_bps"]))
    shadow_candidate += int(shadow.get("shadow_candidate_count") or 0)
    shadow_verified += int(shadow.get("shadow_verified_count") or 0)

recommended = Counter()
negative_profiles = []
positive_low_sample = []
db = Path("data/chain_monitor.sqlite")
if db.exists():
    try:
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        try:
            table = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trade_replay_profile_daily_stats'").fetchone()
            if table:
                rows = conn.execute(
                    """
                    SELECT profile_key, SUM(valid_sample_count) AS samples,
                           AVG(avg_net_pnl_bps) AS avg_pnl,
                           recommended_action
                    FROM trade_replay_profile_daily_stats
                    WHERE logical_date BETWEEN ? AND ? AND replay_scope='full'
                    GROUP BY profile_key, recommended_action
                    ORDER BY samples DESC, avg_pnl ASC
                    LIMIT 50
                    """,
                    (start.isoformat(), end.isoformat()),
                ).fetchall()
                for row in rows:
                    action = str(row["recommended_action"] or "unknown")
                    samples = int(row["samples"] or 0)
                    avg_pnl = float(row["avg_pnl"] or 0.0)
                    recommended[action] += 1
                    record = f"samples={samples} avg_net_pnl_bps={avg_pnl:.2f} action={action} profile={row['profile_key']}"
                    if samples >= 10 and avg_pnl < 0:
                        negative_profiles.append(record)
                    if samples < 10 and avg_pnl > 0:
                        positive_low_sample.append(record)
        finally:
            conn.close()
    except sqlite3.Error as exc:
        data_warnings.append(f"sqlite:{exc.__class__.__name__}")
else:
    data_warnings.append("sqlite:missing")

suppressed_avg = "missing"
if suppressed_avg_values:
    suppressed_avg = round(sum(suppressed_avg_values) / len(suppressed_avg_values), 4)

print(f"zero_activity_day天数={zero_days}")
print(f"valid_data_days={valid_days}")
print(f"replay_source=persisted/full覆盖天数={persisted_full_days}")
print("高样本负收益profile=" + json.dumps(negative_profiles[:5], ensure_ascii=False))
print("低样本正收益profile=" + json.dumps(positive_low_sample[:5], ensure_ascii=False))
print("recommended_action分布=" + json.dumps(dict(recommended), ensure_ascii=False, sort_keys=True))
print(f"suppressed_avg_net_pnl_bps周摘要={suppressed_avg}")
print(f"shadow_candidate周合计={shadow_candidate}")
print(f"shadow_verified周合计={shadow_verified}")
print("数据质量异常=" + json.dumps(data_warnings[:12], ensure_ascii=False))
print("空间治理建议=只读复盘；如空间异常，先运行 空间检查 和 归档压缩预检YYYY-MM-DD。")
print("说明=不修改参数，不放宽 VERIFIED，不提供交易建议。")
PY
}

cmd_digest() {
  local report_date=""
  local mode="fast"
  local digest_script="./scripts/hermes_daily_digest_input.sh"
  local -a cmd=()
  local rc=0
  local digest_path=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "digest --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --mode)
        [[ $# -ge 2 ]] || die "digest --mode requires fast or deep"
        mode="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown digest argument"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "digest requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ "$mode" == "fast" || "$mode" == "deep" ]] || die "invalid digest mode: ${mode}"
  [[ -f "$digest_script" ]] || refuse runtime_missing_dependency "missing ${digest_script}; cannot generate Hermes digest input"
  [[ -x "$digest_script" ]] || refuse runtime_missing_dependency "${digest_script} exists but is not executable"
  AUDIT_DATE="$report_date"
  AUDIT_MODE="$mode"
  AUDIT_OUTPUT_HINT="reports/hermes/hermes_digest_input_${report_date}.md"
  AUDIT_ALLOWED=true

  prepare_digest_env
  cmd=("$digest_script" --date "$report_date" --mode "$mode")
  echo "command: $(display_command "${cmd[@]}")"
  if run_command "digest_${report_date}_${mode}" "$DIGEST_TIMEOUT_SEC" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi
  emit_command_report "digest ${report_date} ${mode}" "$rc" "${cmd[@]}"

  digest_path="$(sed -n '$p' "$RUN_OUTPUT" || true)"
  if [[ -n "$digest_path" ]]; then
    AUDIT_OUTPUT_HINT="$digest_path"
    echo "digest_file: ${digest_path}"
  fi

  if [[ "$rc" -eq 0 ]]; then
    exit 0
  fi
  exit 1
}

cmd_analyze() {
  local report_date=""
  local mode="fast"
  local auto_build=0
  local digest_script="./scripts/hermes_daily_digest_input.sh"
  local canonical_report_status="missing"
  local daily_compare_status="unknown"
  local rebuild_performed="false"
  local digest_path=""
  local expected_digest=""
  local digest_report_date=""
  local digest_source_mode=""
  local digest_source_date_verified=""
  local digest_source_fallback_to_latest=""
  local rc=0
  local -a cmd=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "analyze --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --mode)
        [[ $# -ge 2 ]] || die "analyze --mode requires fast or deep"
        mode="$2"
        shift 2
        ;;
      --auto-build)
        auto_build=1
        AUDIT_AUTO_BUILD=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown analyze argument"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "analyze requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ "$mode" == "fast" || "$mode" == "deep" ]] || die "invalid analyze mode: ${mode}"
  [[ -f "$digest_script" ]] || refuse runtime_missing_dependency "missing ${digest_script}; cannot generate Hermes digest input"
  [[ -x "$digest_script" ]] || refuse runtime_missing_dependency "${digest_script} exists but is not executable"
  AUDIT_DATE="$report_date"
  AUDIT_MODE="$mode"
  AUDIT_OUTPUT_HINT="reports/hermes/hermes_digest_input_${report_date}.md"
  AUDIT_ALLOWED=true

  prepare_digest_env

  if canonical_daily_report_present "$report_date"; then
    canonical_report_status="present"
  elif [[ "$auto_build" -eq 1 ]]; then
    cmd=(make report-daily-date "DATE=${report_date}")
    if run_analyze_step "analyze_report_${report_date}" "$REPORT_TIMEOUT_SEC" "${cmd[@]}"; then
      if canonical_daily_report_present "$report_date"; then
        canonical_report_status="generated"
      else
        canonical_report_status="missing"
        fail_runtime "report generation completed but canonical daily report for ${report_date} is still missing"
      fi
    else
      fail_runtime "failed to generate canonical daily report for ${report_date}"
    fi
  else
    echo "数据不足：缺少 canonical daily report for ${report_date}." >&2
    echo "请先运行：标准日报流程${report_date}" >&2
    echo "或：生成日报${report_date}" >&2
    exit 1
  fi

  if daily_compare_present "$report_date"; then
    daily_compare_status="present"
  else
    daily_compare_status="missing"
  fi

  if [[ "$daily_compare_status" != "present" && "$auto_build" -eq 1 ]]; then
    cmd=(make daily-compare "DATE=${report_date}")
    if run_analyze_step "analyze_daily_compare_${report_date}" "$REPORT_TIMEOUT_SEC" "${cmd[@]}"; then
      if daily_compare_present "$report_date"; then
        daily_compare_status="generated"
      else
        cmd=(make daily-compare-rebuild "DATE=${report_date}")
        rebuild_performed="true"
        echo "rebuild_performed=true"
        if run_analyze_step "analyze_daily_compare_rebuild_${report_date}" "$REPORT_TIMEOUT_SEC" "${cmd[@]}"; then
          if daily_compare_present "$report_date"; then
            daily_compare_status="generated"
          else
            daily_compare_status="missing"
          fi
        else
          fail_runtime "failed to rebuild daily compare for ${report_date}"
        fi
      fi
    else
      cmd=(make daily-compare-rebuild "DATE=${report_date}")
      rebuild_performed="true"
      echo "rebuild_performed=true"
      if run_analyze_step "analyze_daily_compare_rebuild_${report_date}" "$REPORT_TIMEOUT_SEC" "${cmd[@]}"; then
        if daily_compare_present "$report_date"; then
          daily_compare_status="generated"
        else
          daily_compare_status="missing"
        fi
      else
        fail_runtime "failed to generate daily compare for ${report_date}"
      fi
    fi
  fi

  cmd=("$digest_script" --date "$report_date" --mode "$mode")
  if run_analyze_step "analyze_digest_${report_date}_${mode}" "$DIGEST_TIMEOUT_SEC" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi

  expected_digest="reports/hermes/hermes_digest_input_${report_date}.md"
  digest_path="$(grep -E '^reports/hermes/hermes_digest_input_[0-9]{4}-[0-9]{2}-[0-9]{2}\.md$' "$RUN_OUTPUT" 2>/dev/null | tail -n1 || true)"
  if [[ -z "$digest_path" && -f "$expected_digest" ]]; then
    digest_path="$expected_digest"
  fi
  if [[ -n "$digest_path" ]]; then
    AUDIT_OUTPUT_HINT="$digest_path"
  fi

  if [[ -z "$digest_path" || ! -f "$digest_path" ]]; then
    [[ "$rc" -eq 0 ]] || emit_command_report "analyze digest ${report_date} ${mode}" "$rc" "${cmd[@]}"
    fail_runtime "digest generation failed for ${report_date}; expected ${expected_digest}"
  fi

  digest_report_date="$(digest_header_value "$digest_path" "report_date")"
  digest_source_mode="$(digest_header_value "$digest_path" "source_mode")"
  digest_source_date_verified="$(digest_header_value "$digest_path" "source_date_verified")"
  digest_source_fallback_to_latest="$(digest_header_value "$digest_path" "source_fallback_to_latest")"

  if [[ "$digest_report_date" != "$report_date" ]]; then
    fail_runtime "digest report_date mismatch: expected ${report_date}, got ${digest_report_date:-missing}"
  fi

  echo "analysis_date=${report_date}"
  echo "canonical_report_status=${canonical_report_status}"
  echo "daily_compare_status=${daily_compare_status}"
  echo "rebuild_performed=${rebuild_performed}"
  echo "digest_path=${digest_path}"
  echo "digest_latest=reports/hermes/hermes_digest_latest.md"
  echo "digest_source_mode=${digest_source_mode:-unknown}"
  echo "digest_source_date_verified=${digest_source_date_verified:-unknown}"
  echo "digest_source_fallback_to_latest=${digest_source_fallback_to_latest:-unknown}"
  echo "next_step=Hermes should read ${digest_path} and generate the Chinese daily analysis with chain-monitor-report-analyst."

  if [[ "$rc" -eq 0 ]]; then
    exit 0
  fi
  exit 1
}

cleanup() {
  if [[ -n "${TMP_DIR:-}" && -d "$TMP_DIR" ]]; then
    rm -rf "$TMP_DIR"
  fi
  if [[ -n "${HEALTH_TMP_OUTPUT:-}" && -f "$HEALTH_TMP_OUTPUT" ]]; then
    rm -f "$HEALTH_TMP_OUTPUT"
  fi
}

on_exit() {
  local rc=$?
  local audit_rc=0

  set +e
  audit_finish "$rc"
  audit_rc=$?
  cleanup
  if [[ "$audit_rc" -ne 0 && "$rc" -eq 0 ]]; then
    rc=1
  fi
  exit "$rc"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

MAX_CMD_BYTES="${HERMES_OPS_MAX_CMD_BYTES:-80000}"
MAX_CMD_LINES="${HERMES_OPS_MAX_CMD_LINES:-400}"

# Generic fallback timeout.
CMD_TIMEOUT_SEC="${HERMES_OPS_CMD_TIMEOUT_SEC:-120}"

# Command-specific timeouts.
# If HERMES_OPS_CMD_TIMEOUT_SEC is explicitly set, it acts as the fallback for
# command-specific values that are not explicitly set.
REPORT_TIMEOUT_SEC="${HERMES_OPS_REPORT_TIMEOUT_SEC:-${HERMES_OPS_CMD_TIMEOUT_SEC:-900}}"
CLOSE_TIMEOUT_SEC="${HERMES_OPS_CLOSE_TIMEOUT_SEC:-${HERMES_OPS_CMD_TIMEOUT_SEC:-900}}"
HEALTH_TIMEOUT_SEC="${HERMES_OPS_HEALTH_TIMEOUT_SEC:-${HERMES_OPS_CMD_TIMEOUT_SEC:-180}}"
DIGEST_TIMEOUT_SEC="${HERMES_OPS_DIGEST_TIMEOUT_SEC:-${HERMES_OPS_CMD_TIMEOUT_SEC:-300}}"

TMP_DIR=""
HEALTH_TMP_OUTPUT=""
RUN_OUTPUT=""
RUN_RAW_BYTES=0
RUN_RAW_LINES=0
RUN_TRUNCATED=0
RUN_TIMEOUT_SEC="$CMD_TIMEOUT_SEC"
HEALTH_PARTIAL=0
HEALTH_SUMMARY=()

if [[ $# -gt 0 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
  usage
  exit 0
fi

cd "$REPO_ROOT"
ensure_repo_root
mkdir -p reports/hermes

HERMES_OPS_AUDIT_LOG="${HERMES_OPS_AUDIT_LOG:-reports/hermes/ops_audit.ndjson}"
HERMES_OPS_LOCK_PATH="${HERMES_OPS_LOCK_PATH:-$(default_lock_path)}"
HERMES_OPS_LOCK_TIMEOUT_SEC="${HERMES_OPS_LOCK_TIMEOUT_SEC:-0}"
HERMES_OPS_REQUEST_ID="$(sanitize_request_id "${HERMES_OPS_REQUEST_ID:-$(generate_request_id)}")"
HERMES_OPS_PLATFORM="${HERMES_OPS_PLATFORM:-unknown}"
HERMES_OPS_ACTOR_ID="${HERMES_OPS_ACTOR_ID:-}"
HERMES_OPS_CHAT_ID="${HERMES_OPS_CHAT_ID:-}"
HERMES_OPS_SESSION_ID="${HERMES_OPS_SESSION_ID:-}"
LOCK_TIMEOUT_SEC="$HERMES_OPS_LOCK_TIMEOUT_SEC"

AUDIT_START_MS="$(now_ms)"
AUDIT_READY=0
AUDIT_FINISH_WRITTEN=0
AUDIT_COMMAND="unknown"
AUDIT_ALLOWED=false
AUDIT_REFUSED_REASON=""
AUDIT_DATE=""
AUDIT_MODE=""
AUDIT_AUTO_BUILD=false
AUDIT_CONFIRM_COMPRESS=false
AUDIT_ALLOW_TODAY=false
AUDIT_OUTPUT_HINT=""
AUDIT_ACTOR_ID_HASH="$(hash_value "$HERMES_OPS_ACTOR_ID")"
AUDIT_CHAT_ID_HASH="$(hash_value "$HERMES_OPS_CHAT_ID")"
AUDIT_SESSION_ID_HASH="$(hash_value "$HERMES_OPS_SESSION_ID")"

trap on_exit EXIT
audit_prepare

if [[ $# -eq 0 ]]; then
  usage
  refuse invalid_arguments "command is required"
fi

case "$1" in
  help|command-menu|report|close|health|system-health|listener-health|digest|analyze|daily-flow|replay-check|data-quality|profile-review|blocker-review|shadow-review|space-check|archive-compress-check|weekly-review)
    AUDIT_COMMAND="$1"
    ;;
  *)
    AUDIT_COMMAND="unknown"
    refuse unknown_command "unknown command"
    ;;
esac

enforce_router_guard "$AUDIT_COMMAND"

export HERMES_DIGEST_WORKDIR="$REPO_ROOT"
export HERMES_DIGEST_REDACT=1

if [[ "$1" == "help" ]]; then
  shift
  [[ $# -eq 0 ]] || die "help does not accept arguments"
  AUDIT_ALLOWED=true
  emit_request_header
  cmd_help
  exit 0
fi

if [[ "$1" == "command-menu" ]]; then
  shift
  [[ $# -eq 0 ]] || die "command-menu does not accept arguments"
  AUDIT_ALLOWED=true
  emit_request_header
  cmd_command_menu
  exit 0
fi

ensure_runtime
emit_request_header
acquire_lock
TMP_DIR="$(mktemp -d)"

case "$1" in
  report)
    shift
    cmd_report "$@"
    ;;
  close)
    shift
    cmd_close "$@"
    ;;
  health)
    shift
    [[ $# -eq 0 ]] || die "health does not accept arguments"
    cmd_health
    ;;
  system-health)
    shift
    [[ $# -eq 0 ]] || die "system-health does not accept arguments"
    cmd_system_health
    ;;
  listener-health)
    shift
    [[ $# -eq 0 ]] || die "listener-health does not accept arguments"
    cmd_listener_health
    ;;
  digest)
    shift
    cmd_digest "$@"
    ;;
  analyze)
    shift
    cmd_analyze "$@"
    ;;
  daily-flow)
    shift
    cmd_daily_flow "$@"
    ;;
  replay-check)
    shift
    cmd_replay_check "$@"
    ;;
  data-quality)
    shift
    cmd_data_quality "$@"
    ;;
  profile-review)
    shift
    cmd_profile_review "$@"
    ;;
  blocker-review)
    shift
    cmd_blocker_review "$@"
    ;;
  shadow-review)
    shift
    cmd_shadow_review "$@"
    ;;
  space-check)
    shift
    [[ $# -eq 0 ]] || die "space-check does not accept arguments"
    cmd_space_check
    ;;
  archive-compress-check)
    shift
    cmd_archive_compress_check "$@"
    ;;
  weekly-review)
    shift
    cmd_weekly_review "$@"
    ;;
  *)
    die "unknown command: $1"
    ;;
esac
