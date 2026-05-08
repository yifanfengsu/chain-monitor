#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  # Telegram/user-facing commands for long jobs submit background work:
  ./scripts/hermes_cm_ops.sh help
  ./scripts/hermes_cm_ops.sh --help
  ./scripts/hermes_cm_ops.sh command-menu
  ./scripts/hermes_cm_ops.sh lock-status
  ./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress [--allow-today]
  ./scripts/hermes_cm_ops.sh health
  ./scripts/hermes_cm_ops.sh system-health
  ./scripts/hermes_cm_ops.sh listener-health
  ./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD [--mode fast|deep]
  ./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD [--mode fast|deep] [--auto-build]
  ./scripts/hermes_cm_ops.sh submit-daily-flow --date YYYY-MM-DD [--force-rerun]
  ./scripts/hermes_cm_ops.sh submit-space-check
  ./scripts/hermes_cm_ops.sh submit-archive-compress-check --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh submit-weekly-review --start YYYY-MM-DD --end YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh job-status --job-id JOB_ID
  ./scripts/hermes_cm_ops.sh job-list [--limit N]
  ./scripts/hermes_cm_ops.sh job-result --job-id JOB_ID
  ./scripts/hermes_cm_ops.sh job-log --job-id JOB_ID
  ./scripts/hermes_cm_ops.sh job-diagnose --job-id JOB_ID
  ./scripts/hermes_cm_ops.sh job-cancel --job-id JOB_ID --confirm
  ./scripts/hermes_cm_ops.sh space-fast
  ./scripts/hermes_cm_ops.sh db-size-diagnose
  ./scripts/hermes_cm_ops.sh db-slim-dry-run
  ./scripts/hermes_cm_ops.sh replay-check --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh data-quality --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh data-integrity --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh profile-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh blocker-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh shadow-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh learning-review --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh candidate-coverage --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh daily-report-schema-check --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh outcome-diagnose --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh outcome-catchup --date YYYY-MM-DD --dry-run
  ./scripts/hermes_cm_ops.sh lp-suppression-sample-replay --date YYYY-MM-DD --dry-run
  ./scripts/hermes_cm_ops.sh lp-suppression-sample-replay --date YYYY-MM-DD --execute --confirm
  ./scripts/hermes_cm_ops.sh lp-diagnose --date YYYY-MM-DD

  # Internal job runner only; Telegram users must use submit-* above:
  ./scripts/hermes_cm_ops.sh daily-flow --date YYYY-MM-DD
  HERMES_OPS_DAILY_FLOW_INTERNAL=1 ./scripts/hermes_cm_ops.sh outcome-catchup --date YYYY-MM-DD --execute --confirm
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
  /chain-monitor-report-analyst 锁状态
  /chain-monitor-report-analyst 标准日报流程YYYY-MM-DD
  /chain-monitor-report-analyst 重新标准日报流程YYYY-MM-DD 我确认重跑
  /chain-monitor-report-analyst 任务状态JOB_ID
  /chain-monitor-report-analyst 查看结果JOB_ID
  /chain-monitor-report-analyst 诊断任务JOB_ID
  /chain-monitor-report-analyst 最近任务
  /chain-monitor-report-analyst 分析报告YYYY-MM-DD
  /chain-monitor-report-analyst 检查回放YYYY-MM-DD
  /chain-monitor-report-analyst 数据质量YYYY-MM-DD
  /chain-monitor-report-analyst 数据完整性检查YYYY-MM-DD
  /chain-monitor-report-analyst Profile复盘YYYY-MM-DD
  /chain-monitor-report-analyst Blocker复盘YYYY-MM-DD
  /chain-monitor-report-analyst Shadow复盘YYYY-MM-DD
  /chain-monitor-report-analyst 学习复盘YYYY-MM-DD
  /chain-monitor-report-analyst 学习总结YYYY-MM-DD
  /chain-monitor-report-analyst CANDIDATE覆盖诊断YYYY-MM-DD
  /chain-monitor-report-analyst 日报结构检查YYYY-MM-DD
  /chain-monitor-report-analyst Outcome闭环诊断YYYY-MM-DD
  /chain-monitor-report-analyst Outcome补全预检YYYY-MM-DD
  /chain-monitor-report-analyst LP抑制抽样预检YYYY-MM-DD
  /chain-monitor-report-analyst LP诊断YYYY-MM-DD
  /chain-monitor-report-analyst 空间检查
  /chain-monitor-report-analyst 空间快检
  /chain-monitor-report-analyst 数据库体积诊断
  /chain-monitor-report-analyst 数据库瘦身预检
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
  - 标准日报流程只能跑已经结束的北京时间逻辑日，不支持当前北京时间日期。
  - 例如北京时间 2026-05-03 还没结束时，不能跑 标准日报流程2026-05-03。
  - 请在次日 00:05 后执行标准日报流程YYYY-MM-DD。

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
  - 标准日报流程、空间检查、归档压缩预检、周复盘在 Telegram 中只提交后台 job。
  - 长任务会立即返回 job_id，再用任务状态/查看结果/查看日志/诊断任务查询。
  - Telegram 长任务入口只使用 submit-daily-flow、submit-space-check、submit-archive-compress-check、submit-weekly-review。
  - daily-flow、space-check、archive-compress-check、weekly-review 是 internal job runner only。
  - 数据库瘦身预检只做 dry-run，不执行 export execute、VACUUM、compact、prune、删除 DB、修改地址簿。
  - Outcome补全预检只映射到 outcome-catchup dry-run；Telegram 不开放 execute。
  - LP抑制抽样预检只映射到 lp-suppression-sample-replay dry-run；Telegram 不开放 execute。
  - 锁状态只读诊断 Hermes lock，不删除 lock 文件。
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
- 监听器体检：检查监听器是否停摆、最近数据时间、Telegram outbound 健康
- 空间检查：提交后台任务查看 DB / archive / reports 占用
- 空间快检：快速同步查看 SQLite / WAL / SHM 文件大小
- 数据库体积诊断：解释 DB / WAL / archive / reports 哪个大
- 数据库瘦身预检：只运行 operational payload export dry-run，显示候选行和预计节省

【日报流程】
- 标准日报流程YYYY-MM-DD：提交后台任务，展开 daily-close 子步骤 + full replay + report + compare + checkpoint
- 重新标准日报流程YYYY-MM-DD 我确认重跑：仅在确实需要重跑时使用
- 生成日报YYYY-MM-DD：生成 canonical 日报
- 分析报告YYYY-MM-DD：分析已存在日报
- 深度分析报告YYYY-MM-DD：深度分析已存在日报
- 生成摘要YYYY-MM-DD 快速：生成快速分析输入包
- 生成摘要YYYY-MM-DD 深度：生成深度分析输入包
- 检查回放YYYY-MM-DD：确认 replay_source=persisted、scope=full
- 数据质量YYYY-MM-DD：判断该日是否有效
- 数据完整性检查YYYY-MM-DD / 数据入库检查YYYY-MM-DD / 入库完整性YYYY-MM-DD：只读检查 archive、SQLite mirror、replay/outcome 闭环和 locked warning

【后验复盘】
- Profile复盘YYYY-MM-DD：查看 profile 后验
- Blocker复盘YYYY-MM-DD：查看 blocker 分布
- Shadow复盘YYYY-MM-DD：查看 shadow funnel
- 学习复盘YYYY-MM-DD / 每日学习YYYY-MM-DD / 学习总结YYYY-MM-DD：整合数据质量、回放、profile、blocker、shadow、Telegram 去噪，输出中文学习结论
- CANDIDATE覆盖诊断YYYY-MM-DD：排查 signals -> opportunity -> replay 覆盖连接
- 日报结构检查YYYY-MM-DD：检查 canonical daily report 是否包含 LP / CLMM / candidate frontier 字段
- Outcome闭环诊断YYYY-MM-DD：排查 signals/opportunities -> outcomes/replay/profile 闭环不足
- Outcome补全预检YYYY-MM-DD / 后验补全预检YYYY-MM-DD：只做 opportunity_outcomes catchup dry-run，显示 would_update_rows
- LP抑制抽样预检YYYY-MM-DD：只做 LP early suppression sample replay dry-run，显示 candidate_sample_count / by_reason / would_insert_replay_rows
- LP诊断YYYY-MM-DD：排查 daily_report 中 LP signal rows 缺失的 report/analyzer/gate 链路

【维护预检】
- 归档压缩预检YYYY-MM-DD：提交后台 dry-run 任务，不压缩

【周复盘】
- 周复盘START到END：提交后台任务，例如：周复盘2026-04-27到2026-05-03

【后台任务 / 诊断】
- 任务状态JOB_ID：查看后台任务状态
- 查看结果JOB_ID：查看任务 result.md 摘要
- 查看日志JOB_ID：查看 stdout/stderr 日志尾部
- 诊断任务JOB_ID：查看失败子步骤、失败命令和下一步建议
- 最近任务：列出最近 10 个任务
- 锁状态：只读检查 Hermes lock 是否被占用，不删除 lock 文件
- 取消任务JOB_ID 我确认取消：取消 pending/running 后台任务

规则：
- 日期必须用 YYYY-MM-DD
- 不支持 今天/昨天/前天 自动执行
- 标准日报流程只能跑已经结束的北京时间逻辑日
- 不支持当前北京时间日期；例如北京时间 2026-05-03 还没结束时，不能跑 标准日报流程2026-05-03
- 请在次日 00:05 后执行标准日报流程YYYY-MM-DD
- 长任务会返回 job_id
- 标准日报流程、空间检查、归档压缩预检、周复盘是后台任务
- 数据库瘦身预检只能 dry-run
- 输出默认脱敏
- 不提供交易建议
- 不开放 vacuum / prune / compact execute / export execute / delete DB / 修改地址簿
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

refuse_lock_busy() {
  AUDIT_ALLOWED=false
  AUDIT_REFUSED_REASON="lock_busy"
  cat >&2 <<'MESSAGE'
refused_reason=lock_busy
当前已有 Hermes 操作在执行，请稍后重试。
可用：最近任务 / 任务状态JOB_ID / 锁状态
MESSAGE
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

audit_write_flow_step() {
  local substep="$1"
  local status="$2"
  local exit_code="$3"
  local timeout_hit="$4"
  local timeout_limit_sec="$5"
  local substep_command="$6"
  local output_hint="$7"
  local line

  [[ "${AUDIT_READY:-0}" -eq 1 ]] || return 0

  line="{"
  line+="\"schema\":\"chain_monitor_hermes_ops_audit_v1\""
  line+=",\"ts_utc\":$(json_string "$(TZ=UTC date +%Y-%m-%dT%H:%M:%S+00:00)")"
  line+=",\"request_id\":$(json_string "$HERMES_OPS_REQUEST_ID")"
  line+=",\"event\":\"daily_flow_substep_finish\""
  line+=",\"platform\":$(json_string "${HERMES_OPS_PLATFORM:-unknown}")"
  line+=",\"command\":$(json_string "${AUDIT_COMMAND:-unknown}")"
  line+=",\"substep\":$(json_string "$substep")"
  line+=",\"substep_status\":$(json_string "$status")"
  line+=",\"substep_command\":$(json_string "$substep_command")"
  line+=",\"allowed\":${AUDIT_ALLOWED}"
  line+=",\"refused_reason\":$(json_string "${AUDIT_REFUSED_REASON:-}")"
  line+=",\"date\":$(json_string "${AUDIT_DATE:-}")"
  line+=",\"mode\":$(json_string "${AUDIT_MODE:-}")"
  line+=",\"exit_code\":${exit_code}"
  line+=",\"timeout_hit\":$(json_string "$timeout_hit")"
  line+=",\"timeout_limit_sec\":$(json_string "$timeout_limit_sec")"
  line+=",\"output_hint\":$(json_string "$(display_safe_path "$output_hint")")"
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
  is_positive_int "$JOB_TIMEOUT_SEC" || refuse invalid_arguments "HERMES_OPS_JOB_TIMEOUT_SEC must be a positive integer"
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
      refuse_lock_busy
    fi
  elif ! flock -n 9; then
    refuse_lock_busy
  fi
}

related_hermes_process_count() {
  local count=""

  if command -v pgrep >/dev/null 2>&1; then
    count="$(pgrep -fc 'hermes_cm_ops\.sh|hermes_cm_jobctl\.py|chain-monitor-cn-router|chain-monitor-report-analyst' 2>/dev/null || true)"
  fi
  if [[ -z "$count" || ! "$count" =~ ^[0-9]+$ ]]; then
    count=0
  fi
  printf '%s\n' "$count"
}

emit_recent_jobs_summary() {
  local py=""

  echo "recent_jobs_summary:"
  if command -v python3 >/dev/null 2>&1; then
    py="python3"
  elif [[ -x "./venv/bin/python" ]]; then
    py="./venv/bin/python"
  fi
  if [[ -z "$py" ]]; then
    echo "unavailable_or_empty"
    return
  fi
  if ! "$py" - <<'PY'
import json
import os
from pathlib import Path

repo_root = Path.cwd()
jobs_root = Path(os.environ.get("HERMES_JOBCTL_JOBS_ROOT", "reports/hermes/jobs"))
if not jobs_root.is_absolute():
    jobs_root = repo_root / jobs_root

if not jobs_root.exists():
    print("no_recent_jobs")
    raise SystemExit(0)

metas = []
for meta_path in jobs_root.glob("cmjob_*/meta.json"):
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    metas.append(meta)

metas.sort(key=lambda item: str(item.get("created_at_utc", "")), reverse=True)
if not metas:
    print("no_recent_jobs")
    raise SystemExit(0)

print("job_id | kind | status | date/range | created_at")
for meta in metas[:5]:
    target = meta.get("date") or (
        f"{meta.get('start', '')}..{meta.get('end', '')}" if meta.get("start") or meta.get("end") else "-"
    )
    print(
        f"{meta.get('job_id')} | {meta.get('kind')} | {meta.get('status')} | "
        f"{target} | {meta.get('created_at_utc')}"
    )
PY
  then
    echo "unavailable_or_empty"
  fi
}

cmd_lock_status() {
  local lock_file_exists=false
  local flock_available=true
  local lock_currently_held=false
  local lock_status_fd=""
  local lock_open_warning=""

  [[ $# -eq 0 ]] || die "lock-status does not accept arguments"

  AUDIT_ALLOWED=true

  if [[ -e "$HERMES_OPS_LOCK_PATH" ]]; then
    lock_file_exists=true
    if exec {lock_status_fd}<>"$HERMES_OPS_LOCK_PATH"; then
      if flock -n "$lock_status_fd"; then
        flock_available=true
        lock_currently_held=false
        flock -u "$lock_status_fd" || true
      else
        flock_available=false
        lock_currently_held=true
      fi
      exec {lock_status_fd}>&-
    else
      flock_available=false
      lock_currently_held=true
      lock_open_warning="lock_file_open_failed"
    fi
  fi

  echo "Hermes 锁状态"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "lock_path=$(display_safe_path "$HERMES_OPS_LOCK_PATH")"
  echo "lock_path_basename=$(safe_basename "$HERMES_OPS_LOCK_PATH")"
  echo "lock_file_exists=${lock_file_exists}"
  echo "flock_available=${flock_available}"
  echo "lock_currently_held=${lock_currently_held}"
  echo "related_processes_count=$(related_hermes_process_count)"
  if [[ -n "$lock_open_warning" ]]; then
    echo "warning=${lock_open_warning}"
  fi
  emit_recent_jobs_summary
  echo "建议："
  if [[ "$lock_currently_held" == "false" ]]; then
    echo "锁未被占用，后续命令可重试。"
  else
    echo "当前已有 Hermes 操作在执行，请稍后重试。"
    echo "可用：最近任务 / 任务状态JOB_ID / 锁状态"
  fi
}

parse_required_date() {
  local value="$1"

  validate_date "$value" || refuse invalid_date "invalid date: ${value}"
}

validate_job_id() {
  [[ "$1" =~ ^cmjob_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9]{8,16}$ ]] || refuse invalid_job_id "invalid job_id"
}

validate_weekly_range() {
  local start_date="$1"
  local end_date="$2"
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
}

beijing_today() {
  if [[ -n "${HERMES_TEST_BEIJING_TODAY:-}" ]]; then
    validate_date "$HERMES_TEST_BEIJING_TODAY" || refuse invalid_date "invalid HERMES_TEST_BEIJING_TODAY: ${HERMES_TEST_BEIJING_TODAY}"
    printf '%s\n' "$HERMES_TEST_BEIJING_TODAY"
    return
  fi
  TZ=Asia/Shanghai date +%F
}

current_beijing_date_override_allowed() {
  [[ "${HERMES_ALLOW_CURRENT_BEIJING_DATE:-}" == "1" && "${HERMES_OPS_PLATFORM:-unknown}" != "telegram" && "${HERMES_OPS_ROUTER_OK:-}" != "1" ]]
}

print_current_beijing_date_refusal() {
  local report_date="$1"

  cat <<MESSAGE
❌ 已拒绝：${report_date} 仍是当前北京时间逻辑日，数据可能还在写入。
请在北京时间次日 00:05 后重试：
标准日报流程${report_date}
MESSAGE
}

refuse_current_beijing_date() {
  local report_date="$1"

  AUDIT_ALLOWED=false
  AUDIT_REFUSED_REASON="current_beijing_date_protected"
  print_current_beijing_date_refusal "$report_date" >&2
  exit 2
}

python_bin() {
  if [[ -n "${HERMES_OPS_PYTHON_BIN:-}" ]]; then
    printf '%s\n' "$HERMES_OPS_PYTHON_BIN"
    return
  fi
  if [[ -x "./venv/bin/python" ]]; then
    printf '%s\n' "./venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    command -v python3
  else
    refuse runtime_missing_dependency "python3 or ./venv/bin/python is required"
  fi
}

jobctl() {
  command -v python3 >/dev/null 2>&1 || refuse runtime_missing_dependency "python3 is required for job controller"
  python3 scripts/hermes_cm_jobctl.py "$@"
}

jobs_root_path() {
  local configured="${HERMES_JOBCTL_JOBS_ROOT:-reports/hermes/jobs}"

  if [[ "$configured" == /* ]]; then
    printf '%s\n' "$configured"
  else
    printf '%s/%s\n' "$REPO_ROOT" "$configured"
  fi
}

job_result_file() {
  local job_id="$1"
  printf '%s/%s/result.md\n' "$(jobs_root_path)" "$job_id"
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
    lock-status|report|digest|analyze|close|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|daily-flow|replay-check|data-quality|data-integrity|profile-review|blocker-review|shadow-review|learning-review|candidate-coverage|daily-report-schema-check|outcome-diagnose|outcome-catchup|lp-suppression-sample-replay|lp-diagnose|space-check|archive-compress-check|weekly-review)
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

is_long_job_runner_command() {
  case "$1" in
    daily-flow|space-check|archive-compress-check|weekly-review)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

enforce_long_job_runner_guard() {
  local command_name="$1"

  if is_long_job_runner_command "$command_name" && is_gateway_router_required && [[ "${HERMES_OPS_JOB_RUNNER_OK:-}" != "1" ]]; then
    AUDIT_ALLOWED=false
    AUDIT_REFUSED_REASON="job_runner_required"
    cat >&2 <<'MESSAGE'
❌ 已拒绝：Telegram/Hermes 长任务必须提交后台 job。
请使用：标准日报流程YYYY-MM-DD、空间检查、归档压缩预检YYYY-MM-DD 或 周复盘START到END。
MESSAGE
    exit 2
  fi
}

enforce_job_runner_guard() {
  local command_name="$1"

  if [[ "$command_name" == "__run-job" && "${HERMES_OPS_JOB_RUNNER_OK:-}" != "1" ]]; then
    AUDIT_ALLOWED=false
    AUDIT_REFUSED_REASON="job_runner_required"
    echo "❌ 已拒绝：__run-job 只能由后台 job controller 调用。" >&2
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
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

_final_output = sys.argv[1]
root = Path.cwd()
app_dir = root / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore
configured_db_path = os.getenv("HERMES_LISTENER_HEALTH_DB_PATH", "data/chain_monitor.sqlite")
db_path = Path(configured_db_path)
if not db_path.is_absolute():
    db_path = root / db_path
WINDOWS = (
    ("最近30分钟", 30 * 60),
    ("最近2小时", 2 * 60 * 60),
)
NOW_TS = time.time()
BJ_TZ = timezone(timedelta(hours=8))


def open_ro_sqlite(path: Path) -> sqlite3.Connection:
    return sqlite_store.open_sqlite_connection(path, readonly=True, row_factory=True)


def safe_scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> object:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error as exc:
        return f"unavailable:{exc.__class__.__name__}"
    return row[0] if row else None


def safe_count(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int | None:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error:
        return None
    if not row or row[0] is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _epoch_sql(column: str) -> str:
    col = ident(column)
    return f"(CASE WHEN {col} > 10000000000 THEN {col} / 1000.0 ELSE {col} END)"


def time_info(conn: sqlite3.Connection, table: str, candidates: list[str]) -> tuple[str | None, str | None]:
    if not table_exists(conn, table):
        return None, None
    cols = columns(conn, table)
    for col in candidates:
        if col in cols:
            return _epoch_sql(col), col
    return None, None


def time_expr(conn: sqlite3.Connection, table: str, candidates: list[str]) -> str | None:
    expr, _field = time_info(conn, table, candidates)
    return expr


def count_recent(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: list[str],
    window_sec: int,
    predicate: str = "",
) -> int | None:
    expr = time_expr(conn, table, time_candidates)
    if expr is None:
        return None
    sql = f"SELECT COUNT(*) FROM {table} WHERE {expr} >= ?"
    if predicate:
        sql += f" AND ({predicate})"
    return safe_count(conn, sql, (NOW_TS - window_sec,))


def count_window_expr(
    conn: sqlite3.Connection,
    table: str,
    expr: str | None,
    window_start: float,
    predicate: str = "",
) -> int | None:
    if expr is None or not table_exists(conn, table):
        return None
    sql = f"SELECT COUNT(*) FROM {table} WHERE {expr} >= ?"
    if predicate:
        sql += f" AND ({predicate})"
    return safe_count(conn, sql, (window_start,))


def max_recent_ts(conn: sqlite3.Connection, table: str, time_candidates: list[str], predicate: str = "") -> float | None:
    expr = time_expr(conn, table, time_candidates)
    if expr is None:
        return None
    sql = f"SELECT MAX({expr}) FROM {table}"
    params: tuple = ()
    if predicate:
        sql += f" WHERE {predicate}"
    value = safe_scalar(conn, sql, params)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def fmt_count(value: int | None) -> str:
    return "unavailable" if value is None else str(int(value))


def fmt_ts(value: object) -> str:
    if value in (None, "", "none"):
        return "none"
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return str(value)
    if ts > 10000000000:
        ts = ts / 1000.0
    utc = datetime.fromtimestamp(ts, timezone.utc)
    bj = utc.astimezone(BJ_TZ)
    return f"{utc.strftime('%Y-%m-%d %H:%M:%S')} UTC / 北京 {bj.strftime('%Y-%m-%d %H:%M:%S')}"


def fmt_ts_bj(value: object) -> str:
    if value in (None, "", "none"):
        return "none"
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return str(value)
    if ts > 10000000000:
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _text_match_predicate(
    conn: sqlite3.Connection,
    table: str,
    needles: list[str],
    candidate_cols: tuple[str, ...] | None = None,
) -> str:
    cols = columns(conn, table)
    text_cols = [
        col
        for col in (
            candidate_cols
            or (
                "reason",
                "blocked_reason",
                "gate_reason",
                "suppression_reason",
                "drop_reason",
                "stage",
                "delivery_decision",
                "audit_json",
                "message_json",
            )
        )
        if col in cols
    ]
    parts = []
    for col in text_cols:
        for needle in needles:
            escaped = needle.lower().replace("'", "''")
            parts.append(f"LOWER(COALESCE({ident(col)}, '')) LIKE '%{escaped}%'")
    return " OR ".join(parts) if parts else "0"


def notifier_failure_predicate(conn: sqlite3.Connection, table: str = "delivery_audit") -> str:
    return _text_match_predicate(
        conn,
        table,
        ["notifier_send_failed"],
        ("audit_json", "reason", "delivery_decision", "stage", "message_json"),
    )


def transport_error_predicate(conn: sqlite3.Connection, table: str = "delivery_audit") -> str:
    return _text_match_predicate(
        conn,
        table,
        ["RetryAfter", "TimedOut", "NetworkError", "Pool timeout", "BadRequest", "Forbidden", "Request was not sent"],
        ("audit_json", "reason", "delivery_decision", "stage", "message_json"),
    )


def delivery_policy_predicate(conn: sqlite3.Connection) -> str:
    return _text_match_predicate(
        conn,
        "delivery_audit",
        ["delivery_policy"],
        ("suppression_reason", "delivery_decision", "reason", "audit_json"),
    )


def gate_suppressed_predicate(conn: sqlite3.Connection) -> str:
    return _text_match_predicate(
        conn,
        "delivery_audit",
        [
            "gate/",
            "lp_noise_filtered",
            "low_quality",
            "replay_profile_negative",
            "no_trade",
            "profile_adverse_too_high",
            "score_below_shadow_candidate",
        ],
        ("gate_reason", "suppression_reason", "reason", "audit_json"),
    )


def listener_prefilter_drop_predicate(conn: sqlite3.Connection) -> str:
    return _text_match_predicate(
        conn,
        "delivery_audit",
        ["listener_prefilter/drop"],
        ("drop_reason", "suppression_reason", "reason", "audit_json"),
    )


def read_notifier_runtime_health() -> tuple[dict | None, str]:
    try:
        sys.path.insert(0, str(root / "app"))
        import notifier  # type: ignore

        getter = getattr(notifier, "get_notifier_health", None)
        if not callable(getter):
            return None, "missing_get_notifier_health"
        payload = getter()
    except Exception as exc:  # noqa: BLE001
        return None, f"unavailable:{exc.__class__.__name__}"
    if not isinstance(payload, dict):
        return None, "unavailable:non_dict_payload"
    return payload, "readable"


DELIVERY_AUDIT_TIME_CANDIDATES = ["archive_written_at", "timestamp", "created_at", "updated_at"]
SIGNALS_TIME_CANDIDATES = ["timestamp", "created_at", "updated_at", "archive_written_at", "notifier_sent_at"]
TELEGRAM_DELIVERIES_TIME_CANDIDATES = ["created_at", "sent_at", "updated_at", "timestamp"]


def _or_pred(parts: list[str]) -> str:
    real = [part for part in parts if part and part != "0"]
    return " OR ".join(real) if real else "0"


def _and_not_pred(predicate: str, excluded: str) -> str:
    if not predicate or predicate == "0":
        return "0"
    if not excluded or excluded == "0":
        return predicate
    return f"({predicate}) AND NOT ({excluded})"


def _flag_true(cols: set[str], column: str) -> str:
    return f"COALESCE({ident(column)}, 0) = 1" if column in cols else "0"


def _flag_false(cols: set[str], column: str) -> str:
    return f"COALESCE({ident(column)}, 0) = 0" if column in cols else "0"


def _int_or_zero(value: object) -> int:
    return int(value) if isinstance(value, int) else 0


def max_ts_expr(conn: sqlite3.Connection, table: str, expr: str | None, predicate: str = "") -> float | None:
    if expr is None or not table_exists(conn, table):
        return None
    sql = f"SELECT MAX({expr}) FROM {table}"
    if predicate:
        sql += f" WHERE {predicate}"
    value = safe_scalar(conn, sql)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _field_label(field: str | None) -> str:
    return field or "unavailable"


def count_telegram_window(conn: sqlite3.Connection, label: str, window_sec: int) -> dict[str, int | None | str]:
    window_start = NOW_TS - window_sec
    signals_expr, signals_field = time_info(conn, "signals", SIGNALS_TIME_CANDIDATES)
    audit_expr, audit_field = time_info(conn, "delivery_audit", DELIVERY_AUDIT_TIME_CANDIDATES)
    tg_expr, tg_field = time_info(conn, "telegram_deliveries", TELEGRAM_DELIVERIES_TIME_CANDIDATES)

    audit_cols = columns(conn, "delivery_audit") if table_exists(conn, "delivery_audit") else set()
    tg_cols = columns(conn, "telegram_deliveries") if table_exists(conn, "telegram_deliveries") else set()

    notifier_pred = notifier_failure_predicate(conn, "delivery_audit")
    transport_pred = transport_error_predicate(conn, "delivery_audit")
    send_failed_pred = _or_pred([f"({notifier_pred})", f"({transport_pred})"])
    sent_ok_base_pred = _or_pred([_flag_true(audit_cols, "sent_to_telegram"), _flag_true(audit_cols, "delivered")])
    sent_ok_pred = _and_not_pred(sent_ok_base_pred, send_failed_pred)
    sent_1_pred = _flag_true(audit_cols, "sent_to_telegram")
    sent_0_pred = _flag_false(audit_cols, "sent_to_telegram")

    signals_count = count_window_expr(conn, "signals", signals_expr, window_start)
    delivery_audit_rows = count_window_expr(conn, "delivery_audit", audit_expr, window_start)
    sent_ok = count_window_expr(conn, "delivery_audit", audit_expr, window_start, sent_ok_pred)
    send_failed = count_window_expr(conn, "delivery_audit", audit_expr, window_start, send_failed_pred)
    notifier_send_failed = count_window_expr(conn, "delivery_audit", audit_expr, window_start, notifier_pred)
    transport_error_records = count_window_expr(conn, "delivery_audit", audit_expr, window_start, transport_pred)
    sent_to_telegram_1 = count_window_expr(conn, "delivery_audit", audit_expr, window_start, sent_1_pred)
    sent_to_telegram_0 = count_window_expr(conn, "delivery_audit", audit_expr, window_start, sent_0_pred)
    delivery_policy_dropped = count_window_expr(
        conn, "delivery_audit", audit_expr, window_start, delivery_policy_predicate(conn)
    )
    gate_suppressed = count_window_expr(
        conn, "delivery_audit", audit_expr, window_start, gate_suppressed_predicate(conn)
    )
    listener_prefilter_drop = count_window_expr(
        conn, "delivery_audit", audit_expr, window_start, listener_prefilter_drop_predicate(conn)
    )

    send_attempted: int | None = None
    suppressed_or_not_eligible: int | None = None
    metric_warnings: list[str] = []
    if isinstance(sent_ok, int) and isinstance(send_failed, int):
        send_attempted = sent_ok + send_failed
    if isinstance(delivery_audit_rows, int) and isinstance(send_attempted, int):
        if sent_ok > delivery_audit_rows:
            metric_warnings.append("sent_ok_gt_delivery_audit_rows")
        if send_failed > delivery_audit_rows:
            metric_warnings.append("send_failed_gt_delivery_audit_rows")
        if send_attempted > delivery_audit_rows:
            metric_warnings.append("send_attempted_gt_delivery_audit_rows")
        bounded_attempted = min(send_attempted, delivery_audit_rows)
        suppressed_or_not_eligible = max(delivery_audit_rows - bounded_attempted, 0)
        if suppressed_or_not_eligible > delivery_audit_rows:
            metric_warnings.append("suppressed_or_not_eligible_gt_delivery_audit_rows")

    tg_sent_ok_pred = _or_pred([_flag_true(tg_cols, "sent_ok"), _flag_true(tg_cols, "sent")])
    tg_failure_pred = _or_pred(
        [f"({notifier_failure_predicate(conn, 'telegram_deliveries')})", f"({transport_error_predicate(conn, 'telegram_deliveries')})"]
    )
    telegram_deliveries_rows = count_window_expr(conn, "telegram_deliveries", tg_expr, window_start)
    telegram_deliveries_sent_ok = count_window_expr(conn, "telegram_deliveries", tg_expr, window_start, tg_sent_ok_pred)
    telegram_deliveries_send_failed = count_window_expr(conn, "telegram_deliveries", tg_expr, window_start, tg_failure_pred)

    if sent_ok in (None, 0) and isinstance(telegram_deliveries_sent_ok, int) and telegram_deliveries_sent_ok > 0:
        sent_ok = telegram_deliveries_sent_ok
        if isinstance(send_failed, int):
            send_attempted = sent_ok + send_failed
            if isinstance(delivery_audit_rows, int):
                if send_attempted > delivery_audit_rows:
                    metric_warnings.append("telegram_deliveries_sent_ok_exceeds_delivery_audit_rows")
                suppressed_or_not_eligible = max(delivery_audit_rows - min(send_attempted, delivery_audit_rows), 0)

    if isinstance(delivery_audit_rows, int) and isinstance(sent_ok, int) and isinstance(send_failed, int):
        if sent_ok > delivery_audit_rows:
            metric_warnings.append("sent_ok_gt_delivery_audit_rows")
            sent_ok = delivery_audit_rows
        if send_failed > delivery_audit_rows:
            metric_warnings.append("send_failed_gt_delivery_audit_rows")
            send_failed = delivery_audit_rows
        send_attempted = sent_ok + send_failed
        if send_attempted > delivery_audit_rows:
            metric_warnings.append("send_attempted_gt_delivery_audit_rows")
            send_attempted = delivery_audit_rows
        suppressed_or_not_eligible = max(delivery_audit_rows - send_attempted, 0)

    return {
        "label": label,
        "source": "sqlite",
        "window_start_ts": f"{window_start:.0f}",
        "window_start_bj": fmt_ts_bj(window_start),
        "signals_time_field": _field_label(signals_field),
        "signals_latest_row_bj": fmt_ts_bj(max_ts_expr(conn, "signals", signals_expr)),
        "delivery_audit_time_field": _field_label(audit_field),
        "delivery_audit_latest_bj": fmt_ts_bj(max_ts_expr(conn, "delivery_audit", audit_expr)),
        "latest_row_bj": fmt_ts_bj(max_ts_expr(conn, "delivery_audit", audit_expr)),
        "telegram_deliveries_time_field": _field_label(tg_field),
        "telegram_deliveries_latest_bj": fmt_ts_bj(max_ts_expr(conn, "telegram_deliveries", tg_expr)),
        "signals": signals_count,
        "signals_count": signals_count,
        "delivery_audit": delivery_audit_rows,
        "delivery_audit_rows": delivery_audit_rows,
        "telegram_deliveries_rows": telegram_deliveries_rows,
        "telegram_deliveries_window_rows": telegram_deliveries_rows,
        "telegram_deliveries_sent_ok": telegram_deliveries_sent_ok,
        "telegram_deliveries_send_failed": telegram_deliveries_send_failed,
        "send_attempted": send_attempted,
        "sent_ok": sent_ok,
        "send_failed": send_failed,
        "sent_failed": send_failed,
        "suppressed_or_not_eligible": suppressed_or_not_eligible,
        "delivery_policy_dropped": delivery_policy_dropped,
        "gate_suppressed": gate_suppressed,
        "listener_prefilter_drop": listener_prefilter_drop,
        "sent_to_telegram_1": sent_to_telegram_1,
        "sent_to_telegram_0": sent_to_telegram_0,
        "notifier_send_failed": notifier_send_failed,
        "transport_error_records": transport_error_records,
        "metric_invariant_warning": ",".join(sorted(set(metric_warnings))) if metric_warnings else "none",
    }


def print_telegram_outbound_health(conn: sqlite3.Connection) -> None:
    print()
    print("## Telegram outbound health")
    print("source=sqlite 聚合 + notifier.get_notifier_health；只读；未发送 Telegram warning")

    window_rows = [count_telegram_window(conn, label, window_sec) for label, window_sec in WINDOWS]
    recent_transport_errors = 0
    for row in window_rows:
        transport_errors = _int_or_zero(row["transport_error_records"])
        recent_transport_errors += int(transport_errors)
        print(
            f"{row['label']}："
            f"signals={fmt_count(row['signals'] if isinstance(row['signals'], int) else None)} "
            f"signals_count={fmt_count(row['signals_count'] if isinstance(row['signals_count'], int) else None)} "
            f"delivery_audit={fmt_count(row['delivery_audit'] if isinstance(row['delivery_audit'], int) else None)} "
            f"delivery_audit_rows={fmt_count(row['delivery_audit_rows'] if isinstance(row['delivery_audit_rows'], int) else None)} "
            f"telegram_deliveries_rows={fmt_count(row['telegram_deliveries_rows'] if isinstance(row['telegram_deliveries_rows'], int) else None)} "
            f"send_attempted={fmt_count(row['send_attempted'] if isinstance(row['send_attempted'], int) else None)} "
            f"sent_ok={fmt_count(row['sent_ok'] if isinstance(row['sent_ok'], int) else None)} "
            f"send_failed={fmt_count(row['send_failed'] if isinstance(row['send_failed'], int) else None)} "
            f"sent_failed={fmt_count(row['sent_failed'] if isinstance(row['sent_failed'], int) else None)} "
            f"suppressed_or_not_eligible={fmt_count(row['suppressed_or_not_eligible'] if isinstance(row['suppressed_or_not_eligible'], int) else None)} "
            f"delivery_policy_dropped={fmt_count(row['delivery_policy_dropped'] if isinstance(row['delivery_policy_dropped'], int) else None)} "
            f"gate_suppressed={fmt_count(row['gate_suppressed'] if isinstance(row['gate_suppressed'], int) else None)} "
            f"listener_prefilter_drop={fmt_count(row['listener_prefilter_drop'] if isinstance(row['listener_prefilter_drop'], int) else None)} "
            f"sent_to_telegram=1数量={fmt_count(row['sent_to_telegram_1'] if isinstance(row['sent_to_telegram_1'], int) else None)} "
            f"sent_to_telegram=0数量={fmt_count(row['sent_to_telegram_0'] if isinstance(row['sent_to_telegram_0'], int) else None)} "
            f"notifier_send_failed={fmt_count(row['notifier_send_failed'] if isinstance(row['notifier_send_failed'], int) else None)} "
            f"transport_error_records={fmt_count(row['transport_error_records'] if isinstance(row['transport_error_records'], int) else None)} "
            f"telegram_deliveries_window_rows={fmt_count(row['telegram_deliveries_window_rows'] if isinstance(row['telegram_deliveries_window_rows'], int) else None)} "
            f"telegram_deliveries_sent_ok={fmt_count(row['telegram_deliveries_sent_ok'] if isinstance(row['telegram_deliveries_sent_ok'], int) else None)} "
            f"telegram_deliveries_send_failed={fmt_count(row['telegram_deliveries_send_failed'] if isinstance(row['telegram_deliveries_send_failed'], int) else None)} "
            f"source={row['source']} "
            f"signals_time_field={row['signals_time_field']} "
            f"delivery_audit_time_field={row['delivery_audit_time_field']} "
            f"telegram_deliveries_time_field={row['telegram_deliveries_time_field']} "
            f"window_start_ts={row['window_start_ts']} "
            f"window_start_bj={row['window_start_bj']} "
            f"signals_latest_row_bj={row['signals_latest_row_bj']} "
            f"delivery_audit_latest_bj={row['delivery_audit_latest_bj']} "
            f"latest_row_bj={row['latest_row_bj']} "
            f"telegram_deliveries_latest_bj={row['telegram_deliveries_latest_bj']} "
            f"metric_invariant_warning={row['metric_invariant_warning']}"
        )

    audit_success = max_recent_ts(
        conn,
        "delivery_audit",
        ["notifier_sent_at", "archive_written_at", "timestamp", "created_at", "updated_at"],
        _or_pred([
            _flag_true(columns(conn, "delivery_audit"), "sent_to_telegram"),
            _flag_true(columns(conn, "delivery_audit"), "delivered"),
        ]),
    )
    tg_success = max_recent_ts(
        conn,
        "telegram_deliveries",
        ["sent_at", "created_at", "updated_at", "timestamp"],
        _or_pred([
            _flag_true(columns(conn, "telegram_deliveries"), "sent_ok"),
            _flag_true(columns(conn, "telegram_deliveries"), "sent"),
        ]),
    )
    success_candidates = [value for value in (audit_success, tg_success) if value is not None]
    last_success = max(success_candidates) if success_candidates else None

    failure_pred = notifier_failure_predicate(conn)
    audit_failure = max_recent_ts(
        conn,
        "delivery_audit",
        ["notifier_sent_at", "archive_written_at", "timestamp", "created_at", "updated_at"],
        _or_pred([f"({failure_pred})", f"({transport_error_predicate(conn)})"]),
    )
    tg_failure = max_recent_ts(
        conn,
        "telegram_deliveries",
        ["sent_at", "created_at", "updated_at", "timestamp"],
        _or_pred(
            [
                f"({notifier_failure_predicate(conn, 'telegram_deliveries')})",
                f"({transport_error_predicate(conn, 'telegram_deliveries')})",
            ]
        ),
    )
    failure_candidates = [value for value in (audit_failure, tg_failure) if value is not None]
    last_failure = max(failure_candidates) if failure_candidates else None
    print(f"最近成功 Telegram 发送时间: {fmt_ts(last_success)}")
    print(f"最近失败时间: {fmt_ts(last_failure)}")

    runtime_health, runtime_status = read_notifier_runtime_health()
    if runtime_health is None:
        print(f"notifier.get_notifier_health: {runtime_status}")
    else:
        print(
            "notifier.get_notifier_health: "
            "source=local_import_snapshot "
            f"attempted={int(runtime_health.get('attempted') or 0)} "
            f"sent_ok={int(runtime_health.get('sent_ok') or 0)} "
            f"sent_failed={int(runtime_health.get('sent_failed') or 0)} "
            f"pool_timeout_count={int(runtime_health.get('pool_timeout_count') or 0)} "
            f"retry_after_count={int(runtime_health.get('retry_after_count') or 0)} "
            f"network_error_count={int(runtime_health.get('network_error_count') or 0)} "
            f"consecutive_failures={int(runtime_health.get('consecutive_failures') or 0)} "
            f"last_success_ts={fmt_ts(runtime_health.get('last_success_ts'))} "
            f"last_failure_ts={fmt_ts(runtime_health.get('last_failure_ts'))}"
        )
        if int(runtime_health.get("pool_timeout_count") or 0) > 0 or int(runtime_health.get("network_error_count") or 0) > 0:
            recent_transport_errors += 1

    primary_row = window_rows[-1] if window_rows else {}
    any_sent_ok = any(
        _int_or_zero(row.get("sent_ok")) > 0 or _int_or_zero(row.get("telegram_deliveries_sent_ok")) > 0
        for row in window_rows
    )
    primary_signals = _int_or_zero(primary_row.get("signals_count"))
    primary_attempted = _int_or_zero(primary_row.get("send_attempted"))
    primary_sent_ok = _int_or_zero(primary_row.get("sent_ok"))
    primary_failed = _int_or_zero(primary_row.get("send_failed"))
    primary_notifier_failed = _int_or_zero(primary_row.get("notifier_send_failed"))
    primary_transport_failed = _int_or_zero(primary_row.get("transport_error_records"))

    warning_reasons: list[str] = []
    if any_sent_ok:
        print("Telegram outbound 状态: 正常")
    elif primary_attempted == 0 and primary_signals > 0:
        print("Telegram outbound 状态: 无实际 Telegram 发送尝试，近期信号多被 policy/gate 抑制，不是 outbound 停摆")
    elif primary_attempted > 0 and primary_sent_ok == 0 and primary_failed > 0:
        warning_reasons.append(f"{primary_row.get('label', '窗口')} send_attempted>0 且 sent_ok=0 且 send_failed>0")
    elif primary_notifier_failed == 0 and primary_transport_failed == 0:
        print("Telegram outbound 状态: 未发现明显异常")
    else:
        print("Telegram outbound 状态: 未发现明显异常")

    if warning_reasons:
        print(f"warning: Telegram outbound 可能异常（{'; '.join(warning_reasons)}）")
    if recent_transport_errors > 0:
        print("warning: 最近发现 Telegram pool timeout / NetworkError / TimedOut 记录")


def print_sqlite_write_health(conn: sqlite3.Connection) -> None:
    health = sqlite_store.get_sqlite_write_health()
    wal_path = Path(f"{db_path}-wal")
    journal_mode = safe_scalar(conn, "PRAGMA journal_mode")
    busy_timeout = safe_scalar(conn, "PRAGMA busy_timeout")
    print()
    print("## SQLite write health")
    print("source=sqlite_store.get_sqlite_write_health + read-only PRAGMA")
    print(
        "sqlite_write_health: "
        f"busy_retries={int(health.get('busy_retries') or 0)} "
        f"locked_failures={int(health.get('locked_failures') or 0)} "
        f"last_locked_op={health.get('last_locked_op') or ''} "
        f"last_locked_table={health.get('last_locked_table') or ''} "
        f"last_locked_ts={fmt_ts(health.get('last_locked_ts'))} "
        f"wal_size_bytes={wal_path.stat().st_size if wal_path.exists() else 0} "
        f"journal_mode={journal_mode} "
        f"busy_timeout_ms={busy_timeout}"
    )
    if int(health.get("locked_failures") or 0) > 0:
        print("warning: SQLite 写入竞争仍存在，请避免同时运行 heavy maintenance；如持续增长，检查后台 job。")


def max_field(conn: sqlite3.Connection, table: str, candidates: list[str]) -> str:
    if not table_exists(conn, table):
        return "missing_table"
    cols = columns(conn, table)
    for col in candidates:
        if col in cols:
            value = safe_scalar(conn, f"SELECT MAX({ident(col)}) FROM {table}")
            return str(value if value is not None else "empty")
    return "missing_time_column"


def latest_sqlite_logical_date(conn: sqlite3.Connection) -> str:
    date_candidates = ["logical_date", "archive_date", "date"]
    table_candidates = ["signals", "delivery_audit", "raw_events", "parsed_events", "telegram_deliveries"]
    found_date_column = False
    for table in table_candidates:
        if not table_exists(conn, table):
            continue
        cols = columns(conn, table)
        for field in date_candidates:
            if field not in cols:
                continue
            found_date_column = True
            value = safe_scalar(conn, f"SELECT MAX({ident(field)}) FROM {table}")
            if value not in (None, ""):
                return f"{value} source_table={table} source_field={field}"

    fallback_table = "unavailable"
    fallback_field = "unavailable"
    fallback_ts: float | None = None
    fallback_candidates: dict[str, list[str]] = {
        "signals": SIGNALS_TIME_CANDIDATES,
        "delivery_audit": DELIVERY_AUDIT_TIME_CANDIDATES,
        "raw_events": ["timestamp", "block_timestamp", "created_at", "updated_at"],
        "parsed_events": ["timestamp", "block_timestamp", "created_at", "updated_at"],
        "telegram_deliveries": TELEGRAM_DELIVERIES_TIME_CANDIDATES,
    }
    for table, time_candidates in fallback_candidates.items():
        expr, field = time_info(conn, table, time_candidates)
        ts = max_ts_expr(conn, table, expr)
        if ts is None:
            continue
        if fallback_ts is None or ts > fallback_ts:
            fallback_ts = ts
            fallback_table = table
            fallback_field = field or "unavailable"

    reason = "no_logical_date_value" if found_date_column else "no_logical_date_column"
    return (
        "unavailable "
        f"reason={reason} "
        f"fallback_latest_table_time={fmt_ts(fallback_ts)} "
        f"fallback_time_field={fallback_table}.{fallback_field}"
    )


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
    conn = open_ro_sqlite(db_path)
    try:
        print(f"SQLite 最新逻辑日期: {latest_sqlite_logical_date(conn)}")
        print(f"最近 raw_events 时间: {max_field(conn, 'raw_events', ['timestamp', 'block_timestamp', 'created_at', 'updated_at'])}")
        print(f"最近 parsed_events 时间: {max_field(conn, 'parsed_events', ['timestamp', 'block_timestamp', 'created_at', 'updated_at'])}")
        print(f"最近 signals 时间: {max_field(conn, 'signals', ['timestamp', 'created_at', 'updated_at', 'notifier_sent_at'])}")
        if table_exists(conn, "runtime_heartbeats"):
            print(f"runtime_heartbeats 最新 check_ts: {safe_scalar(conn, 'SELECT MAX(check_ts) FROM runtime_heartbeats')}")
        print_sqlite_write_health(conn)
        print_telegram_outbound_health(conn)
    finally:
        conn.close()
else:
    print("SQLite 状态: missing data/chain_monitor.sqlite")
    print()
    print("## SQLite write health")
    print("sqlite_write_health: db_missing")
    print()
    print("## Telegram outbound health")
    print("source=SQLite 聚合；只读；未发送 Telegram warning")
    print("Telegram outbound 状态: 数据不足，SQLite missing")
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
  grep -E '^(最近 raw_events 时间|最近 parsed_events 时间|最近 signals 时间|最近 archive 修改时间|latest zero_activity_day|SQLite 最新逻辑日期|queue spill|sqlite_write_health):' "$final_output" || true
  echo "Telegram outbound:"
  grep -E '^(最近30分钟|最近2小时)：|^(warning: Telegram outbound|warning: 最近发现 Telegram|warning: SQLite)|^(最近成功 Telegram 发送时间|最近失败时间|notifier\.get_notifier_health|Telegram outbound 状态):' "$final_output" || true
  echo "action=只读体检；未重启 listener。"
  exit 0
}

limit_tail_file() {
  local source_path="$1"
  local dest_path="$2"
  local tail_lines="${3:-80}"
  local tail_path="${dest_path}.tailraw"

  if [[ -s "$source_path" ]]; then
    tail -n "$tail_lines" "$source_path" >"$tail_path"
  else
    : >"$tail_path"
  fi
  limit_output_file "$tail_path" "$dest_path"
  rm -f "$tail_path"
}

run_flow_command() {
  local title="$1"
  local timeout_sec="$2"
  shift 2

  local safe_title="${title//[!A-Za-z0-9_]/_}"
  local stdout_raw="${TMP_DIR}/${safe_title}.stdout.raw"
  local stderr_raw="${TMP_DIR}/${safe_title}.stderr.raw"
  local stdout_tail="${TMP_DIR}/${safe_title}.stdout.tail"
  local stderr_tail="${TMP_DIR}/${safe_title}.stderr.tail"
  local rc=0

  FLOW_COMMAND="$(display_command "$@")"
  FLOW_TIMEOUT_LIMIT_SEC="$timeout_sec"
  FLOW_TIMEOUT_HIT=false
  FLOW_STDOUT_TAIL="$stdout_tail"
  FLOW_STDERR_TAIL="$stderr_tail"
  FLOW_STDOUT_RAW="$stdout_raw"
  FLOW_STDERR_RAW="$stderr_raw"
  FLOW_STDOUT_RAW_BYTES=0
  FLOW_STDOUT_RAW_LINES=0
  FLOW_STDERR_RAW_BYTES=0
  FLOW_STDERR_RAW_LINES=0
  FLOW_STDOUT_TRUNCATED=0
  FLOW_STDERR_TRUNCATED=0

  if run_with_timeout "$timeout_sec" "$@" >"$stdout_raw" 2>"$stderr_raw"; then
    rc=0
  else
    rc=$?
  fi

  FLOW_EXIT_CODE="$rc"
  if [[ "$rc" -eq 124 ]]; then
    FLOW_TIMEOUT_HIT=true
  fi

  FLOW_STDOUT_RAW_BYTES="$(wc -c <"$stdout_raw" | tr -d '[:space:]')"
  FLOW_STDOUT_RAW_LINES="$(wc -l <"$stdout_raw" | tr -d '[:space:]')"
  FLOW_STDERR_RAW_BYTES="$(wc -c <"$stderr_raw" | tr -d '[:space:]')"
  FLOW_STDERR_RAW_LINES="$(wc -l <"$stderr_raw" | tr -d '[:space:]')"
  limit_tail_file "$stdout_raw" "$stdout_tail" 80
  limit_tail_file "$stderr_raw" "$stderr_tail" 80

  if (( FLOW_STDOUT_RAW_BYTES > MAX_CMD_BYTES || FLOW_STDOUT_RAW_LINES > MAX_CMD_LINES )); then
    FLOW_STDOUT_TRUNCATED=1
  fi
  if (( FLOW_STDERR_RAW_BYTES > MAX_CMD_BYTES || FLOW_STDERR_RAW_LINES > MAX_CMD_LINES )); then
    FLOW_STDERR_TRUNCATED=1
  fi

  return "$rc"
}

append_flow_step() {
  local output_path="$1"
  local title="$2"
  local timeout_sec="$3"
  shift 3
  local rc=0

  if run_flow_command "$title" "$timeout_sec" "$@"; then
    rc=0
  else
    rc=$?
  fi

  {
    echo "## ${title}"
    echo "step=${title}"
    echo "command=${FLOW_COMMAND}"
    echo "status=$([[ "$rc" -eq 0 ]] && echo ok || echo failed)"
    echo "exit_code=${rc}"
    echo "timeout_limit_sec=${FLOW_TIMEOUT_LIMIT_SEC}"
    echo "timeout_hit=${FLOW_TIMEOUT_HIT}"
    echo "output_limit=max_bytes=${MAX_CMD_BYTES} max_lines=${MAX_CMD_LINES} tail_lines=80"
    if [[ "$FLOW_STDOUT_TRUNCATED" -eq 1 ]]; then
      echo "warning=stdout_tail_truncated raw_bytes=${FLOW_STDOUT_RAW_BYTES} raw_lines=${FLOW_STDOUT_RAW_LINES}"
    fi
    if [[ "$FLOW_STDERR_TRUNCATED" -eq 1 ]]; then
      echo "warning=stderr_tail_truncated raw_bytes=${FLOW_STDERR_RAW_BYTES} raw_lines=${FLOW_STDERR_RAW_LINES}"
    fi
    echo
    echo "stdout_tail:"
    if [[ -s "$FLOW_STDOUT_TAIL" ]]; then
      cat "$FLOW_STDOUT_TAIL"
    else
      echo "(empty)"
    fi
    echo
    echo "stderr_tail:"
    if [[ -s "$FLOW_STDERR_TAIL" ]]; then
      cat "$FLOW_STDERR_TAIL"
    else
      echo "(empty)"
    fi
    echo
  } >>"$output_path"

  FLOW_STEP_SUMMARY+=("${title}: $([[ "$rc" -eq 0 ]] && echo ok || echo failed) exit_code=${rc} timeout_hit=${FLOW_TIMEOUT_HIT}")
  audit_write_flow_step "$title" "$([[ "$rc" -eq 0 ]] && echo ok || echo failed)" "$rc" "$FLOW_TIMEOUT_HIT" "$FLOW_TIMEOUT_LIMIT_SEC" "$FLOW_COMMAND" "$output_path"
  return "$rc"
}

append_flow_skipped_step() {
  local output_path="$1"
  local title="$2"
  local reason="$3"

  {
    echo "## ${title}"
    echo "step=${title}"
    echo "command=(skipped)"
    echo "status=skipped"
    echo "exit_code=0"
    echo "reason=${reason}"
    echo
  } >>"$output_path"

  FLOW_STEP_SUMMARY+=("${title}: skipped reason=${reason}")
  audit_write_flow_step "$title" "skipped" 0 false 0 "(skipped)" "$output_path"
}

flow_stdout_value() {
  local key="$1"

  if [[ -z "${FLOW_STDOUT_RAW:-}" || ! -f "$FLOW_STDOUT_RAW" ]]; then
    return 0
  fi
  sed -n "s/^${key}=//p" "$FLOW_STDOUT_RAW" | tail -n1
}

finish_daily_flow_failure() {
  local tmp_output="$1"
  local final_output="$2"
  local latest_output="$3"
  local report_date="$4"
  local failed_substep="$5"
  local rc="$6"
  local failed_step="${failed_substep%%:*}"
  local timeout_text="命令返回失败"

  if [[ "$FLOW_TIMEOUT_HIT" == "true" ]]; then
    timeout_text="命令超时"
  fi

  mv "$tmp_output" "$final_output"
  cp "$final_output" "$latest_output"

  echo "📋 任务结果 | daily-flow"
  echo
  echo "类型：daily-flow"
  echo "日期：${report_date}"
  echo "状态：❌ failed"
  echo "失败步骤：${failed_substep}"
  echo "失败命令：${FLOW_COMMAND}"
  echo "exit_code：${rc}"
  echo "timeout_hit：${FLOW_TIMEOUT_HIT}"
  echo "timeout_limit_sec：${FLOW_TIMEOUT_LIMIT_SEC}"
  echo "result_path：$(display_safe_path "$final_output")"
  echo
  echo "failed_step=${failed_step}"
  echo "failed_substep=${failed_substep}"
  echo "failed_command=${FLOW_COMMAND}"
  echo "exit_code=${rc}"
  echo "timeout_limit_sec=${FLOW_TIMEOUT_LIMIT_SEC}"
  echo "timeout_hit=${FLOW_TIMEOUT_HIT}"
  echo "result_path=$(display_safe_path "$final_output")"
  echo "failure_reason=${timeout_text}"
  echo
  echo "stdout_tail:"
  if [[ -s "$FLOW_STDOUT_TAIL" ]]; then
    cat "$FLOW_STDOUT_TAIL"
  else
    echo "(empty)"
  fi
  echo
  echo "stderr_tail:"
  if [[ -s "$FLOW_STDERR_TAIL" ]]; then
    cat "$FLOW_STDERR_TAIL"
  else
    echo "(empty)"
  fi
  echo
  echo "建议："
  echo "请先修复上述失败子步骤，再重新运行："
  echo "/chain-monitor-report-analyst 标准日报流程${report_date}"
  echo
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
  exit "$rc"
}

cmd_submit_daily_flow() {
  local report_date=""
  local force_rerun=0
  local today_bj=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "submit-daily-flow --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --force-rerun)
        force_rerun=1
        shift
        ;;
      *) die "unknown submit-daily-flow argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "submit-daily-flow requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_OUTPUT_HINT="reports/hermes/jobs"
  today_bj="$(beijing_today)"
  if [[ "$report_date" == "$today_bj" ]] && ! current_beijing_date_override_allowed; then
    refuse_current_beijing_date "$report_date"
  fi
  AUDIT_ALLOWED=true
  if [[ "$force_rerun" -eq 1 ]]; then
    jobctl submit --kind daily-flow --date "$report_date" --force-rerun
  else
    jobctl submit --kind daily-flow --date "$report_date"
  fi
}

cmd_submit_space_check() {
  [[ $# -eq 0 ]] || die "submit-space-check does not accept arguments"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs"
  jobctl submit --kind space-check
}

cmd_submit_archive_compress_check() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "submit-archive-compress-check --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown submit-archive-compress-check argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "submit-archive-compress-check requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs"
  jobctl submit --kind archive-compress-check --date "$report_date"
}

cmd_submit_weekly_review() {
  local start_date=""
  local end_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --start)
        [[ $# -ge 2 ]] || die "submit-weekly-review --start requires YYYY-MM-DD"
        start_date="$2"
        shift 2
        ;;
      --end)
        [[ $# -ge 2 ]] || die "submit-weekly-review --end requires YYYY-MM-DD"
        end_date="$2"
        shift 2
        ;;
      *) die "unknown submit-weekly-review argument" ;;
    esac
  done

  [[ -n "$start_date" && -n "$end_date" ]] || die "submit-weekly-review requires --start and --end"
  parse_required_date "$start_date"
  parse_required_date "$end_date"
  validate_weekly_range "$start_date" "$end_date"
  AUDIT_DATE="${start_date}..${end_date}"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs"
  jobctl submit --kind weekly-review --start "$start_date" --end "$end_date"
}

cmd_job_status() {
  local job_id=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "job-status --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      *) die "unknown job-status argument" ;;
    esac
  done

  [[ -n "$job_id" ]] || die "job-status requires --job-id JOB_ID"
  validate_job_id "$job_id"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs/${job_id}/status.json"
  jobctl status --job-id "$job_id"
}

cmd_job_list() {
  local limit=10

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --limit)
        [[ $# -ge 2 ]] || die "job-list --limit requires N"
        is_positive_int "$2" || die "job-list --limit requires positive N"
        limit="$2"
        shift 2
        ;;
      *) die "unknown job-list argument" ;;
    esac
  done

  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs"
  jobctl list --limit "$limit"
}

cmd_job_result() {
  local job_id=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "job-result --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      *) die "unknown job-result argument" ;;
    esac
  done

  [[ -n "$job_id" ]] || die "job-result requires --job-id JOB_ID"
  validate_job_id "$job_id"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs/${job_id}/result.md"
  jobctl result --job-id "$job_id"
}

cmd_job_log() {
  local job_id=""
  local tail_lines=80

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "job-log --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      --tail)
        [[ $# -ge 2 ]] || die "job-log --tail requires N"
        is_positive_int "$2" || die "job-log --tail requires positive N"
        tail_lines="$2"
        shift 2
        ;;
      *) die "unknown job-log argument" ;;
    esac
  done

  [[ -n "$job_id" ]] || die "job-log requires --job-id JOB_ID"
  validate_job_id "$job_id"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs/${job_id}/stdout.log"
  jobctl log --job-id "$job_id" --tail "$tail_lines"
}

cmd_job_diagnose() {
  local job_id=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "job-diagnose --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      *) die "unknown job-diagnose argument" ;;
    esac
  done

  [[ -n "$job_id" ]] || die "job-diagnose requires --job-id JOB_ID"
  validate_job_id "$job_id"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs/${job_id}/result.md"
  jobctl diagnose --job-id "$job_id"
}

cmd_job_cancel() {
  local job_id=""
  local confirm=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "job-cancel --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      --confirm)
        confirm=1
        shift
        ;;
      *) die "unknown job-cancel argument" ;;
    esac
  done

  [[ -n "$job_id" ]] || die "job-cancel requires --job-id JOB_ID"
  validate_job_id "$job_id"
  [[ "$confirm" -eq 1 ]] || refuse missing_cancel_confirm "job-cancel requires --confirm"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="reports/hermes/jobs/${job_id}"
  jobctl cancel --job-id "$job_id" --confirm
}

cmd_space_fast() {
  local db="data/chain_monitor.sqlite"
  local wal="data/chain_monitor.sqlite-wal"
  local shm="data/chain_monitor.sqlite-shm"

  [[ $# -eq 0 ]] || die "space-fast does not accept arguments"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="space-fast"

  echo "空间快检"
  echo "SQLite 主文件大小: $(stat -c%s "$db" 2>/dev/null || echo missing)"
  echo "WAL 文件大小: $(stat -c%s "$wal" 2>/dev/null || echo missing)"
  echo "SHM 文件大小: $(stat -c%s "$shm" 2>/dev/null || echo missing)"
  if [[ -d "app/data/archive" ]]; then
    echo "archive 目录: exists"
  else
    echo "archive 目录: missing"
  fi
  if [[ -d "reports" ]]; then
    echo "reports 目录: exists"
  else
    echo "reports 目录: missing"
  fi
  echo "说明=快速同步检查；未递归扫描 archive/reports，未执行 du。"
}

cmd_db_size_diagnose() {
  local db="data/chain_monitor.sqlite"

  [[ $# -eq 0 ]] || die "db-size-diagnose does not accept arguments"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="db-size-diagnose"

  echo "数据库体积诊断"
  echo "generated_at_utc=$(TZ=UTC date -Is)"
  echo "policy=read-only; no delete, no checkpoint, no vacuum, no prune, no compact execute, no operational payload export execute"
  echo

  echo "## SQLite files"
  ls -lh data/chain_monitor.sqlite* 2>/dev/null || echo "missing=data/chain_monitor.sqlite*"
  echo

  "$(python_bin)" - "$db" <<'PY'
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore

db = Path(sys.argv[1])
wal = Path(str(db) + "-wal")
shm = Path(str(db) + "-shm")
archive = Path("app/data/archive")
reports = Path("reports")


def open_ro_sqlite(path: Path) -> sqlite3.Connection:
    return sqlite_store.open_sqlite_connection(path, readonly=True, row_factory=True)


def size_bytes(path: Path) -> int:
    try:
        if path.is_file():
            return int(path.stat().st_size)
        if path.is_dir():
            total = 0
            for root, _, files in os.walk(path):
                for name in files:
                    candidate = Path(root) / name
                    try:
                        total += int(candidate.stat().st_size)
                    except OSError:
                        continue
            return total
    except OSError:
        return 0
    return 0


def mb(value: int | float) -> float:
    return round(float(value) / 1024 / 1024, 2)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


db_bytes = size_bytes(db)
wal_bytes = size_bytes(wal)
shm_bytes = size_bytes(shm)
archive_bytes = size_bytes(archive)
reports_bytes = size_bytes(reports)

print("## SQLite pragmas")
if not db.exists():
    print("db_status=missing")
    print()
else:
    try:
        conn = open_ro_sqlite(db)
    except sqlite3.Error as exc:
        print(f"db_open_error={exc}")
        print()
    else:
        with conn:
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 0)
            page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
            freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
            db_mb = mb(page_size * page_count)
            free_mb = mb(page_size * freelist_count)
            print(f"journal_mode={journal_mode}")
            print(f"page_size={page_size}")
            print(f"page_count={page_count}")
            print(f"freelist_count={freelist_count}")
            print(f"db_mb={db_mb}")
            print(f"free_mb={free_mb}")
            print()

            print("## dbstat top 30 tables/indexes")
            try:
                rows = conn.execute(
                    """
                    SELECT name, COALESCE(path, '') AS path, SUM(pgsize) AS bytes, COUNT(*) AS pages
                    FROM dbstat
                    GROUP BY name
                    ORDER BY bytes DESC
                    LIMIT 30
                    """
                ).fetchall()
            except sqlite3.Error as exc:
                print(f"dbstat_unavailable={exc}")
            else:
                print("name | mb | pages")
                for row in rows:
                    print(f"{row['name']} | {mb(int(row['bytes'] or 0))} | {int(row['pages'] or 0)}")
            print()

            print("## key table counts")
            key_tables = [
                "raw_events",
                "parsed_events",
                "signals",
                "trade_opportunities",
                "delivery_audit",
                "telegram_deliveries",
                "market_context_snapshots",
                "outcomes",
                "opportunity_outcomes",
                "trade_replay_examples",
                "trade_replay_profile_stats",
                "trade_replay_profile_daily_stats",
            ]
            for table in key_tables:
                if not table_exists(conn, table):
                    print(f"{table}=missing")
                    continue
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except sqlite3.Error as exc:
                    print(f"{table}=error:{exc}")
                else:
                    print(f"{table}={int(count or 0)}")
            print()

print("## directory summaries")
print(f"main_db_mb={mb(db_bytes)}")
print(f"wal_mb={mb(wal_bytes)}")
print(f"shm_mb={mb(shm_bytes)}")
print(f"archive_mb={mb(archive_bytes)}")
print(f"reports_mb={mb(reports_bytes)}")
print()

print("## diagnosis")
findings: list[str] = []
db_mb_value = mb(db_bytes)
wal_mb_value = mb(wal_bytes)
archive_mb_value = mb(archive_bytes)
reports_mb_value = mb(reports_bytes)
if wal_mb_value >= max(64.0, db_mb_value * 0.25):
    findings.append("WAL 大：WAL 文件相对主 DB 偏大，可能需要在安全窗口做 checkpoint。")
if db.exists():
    try:
        conn2 = open_ro_sqlite(db)
        page_size2 = int(conn2.execute("PRAGMA page_size").fetchone()[0] or 0)
        freelist2 = int(conn2.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        free_mb_value = mb(page_size2 * freelist2)
        if free_mb_value >= max(64.0, db_mb_value * 0.20):
            findings.append("free pages 大：主 DB 内部空闲页偏多；不要从 Telegram 执行 vacuum。")
        try:
            stat_rows = conn2.execute(
                """
                SELECT name, SUM(pgsize) AS bytes
                FROM dbstat
                GROUP BY name
                ORDER BY bytes DESC
                LIMIT 5
                """
            ).fetchall()
            if stat_rows:
                top_name = str(stat_rows[0][0])
                top_mb = mb(int(stat_rows[0][1] or 0))
                findings.append(f"live rows/index 大：最大对象 {top_name} 约 {top_mb} MB。")
        except sqlite3.Error:
            pass
        conn2.close()
    except sqlite3.Error:
        pass
if archive_mb_value >= max(256.0, db_mb_value):
    findings.append("archive 大：归档目录是主要占用之一。")
if reports_mb_value >= max(128.0, db_mb_value * 0.5):
    findings.append("reports 大：reports 目录占用需要检查生成文件保留策略。")
if not findings:
    findings.append("未发现单一明显大头；请结合 dbstat top 和目录摘要判断。")
for item in findings:
    print(f"- {item}")
PY
  echo
  echo "## operational payload export dry-run"
  make db-export-operational-payloads-dry-run 2>/dev/null || echo "operational_payload_export_dry_run=failed"
  echo
  echo "## archive size summary"
  du -sh app/data/archive 2>/dev/null || echo "archive=missing"
  echo
  echo "## reports size summary"
  du -sh reports 2>/dev/null || echo "reports=missing"
}

cmd_db_slim_dry_run() {
  local rc=0

  [[ $# -eq 0 ]] || die "db-slim-dry-run does not accept arguments"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="db-slim-dry-run"

  if run_command "db_slim_dry_run" "$HEALTH_TIMEOUT_SEC" make db-export-operational-payloads-dry-run; then
    rc=0
  else
    rc=$?
  fi

  echo "数据库瘦身预检"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "status=$([[ "$rc" -eq 0 ]] && echo ok || echo failed)"
  echo "policy=dry-run only; no export execute, no VACUUM, no compact, no prune, no delete DB, no address-book changes"
  echo "archive_write=false"
  echo "json_payload_clear=false"
  echo "vacuum_executed=false"
  echo
  "$(python_bin)" - "$RUN_OUTPUT" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:  # noqa: BLE001
    print("dry_run_json_status=unparsed")
    print("raw_output_redacted:")
    print(path.read_text(encoding="utf-8", errors="replace").strip())
    raise SystemExit(0)

tables = payload.get("tables") if isinstance(payload, dict) else {}
if not isinstance(tables, dict):
    tables = {}

print(f"dry_run={bool(payload.get('dry_run'))}")
print(f"total_candidate_rows={int(payload.get('candidate_rows') or 0)}")
print(f"total_selected_rows={int(payload.get('selected_rows') or 0)}")
print(f"total_estimated_savings_mb={payload.get('estimated_savings_mb') or 0}")
for table in ("telegram_deliveries", "delivery_audit"):
    item = tables.get(table) if isinstance(tables.get(table), dict) else {}
    print(f"{table}.candidate_rows={int(item.get('candidate_rows') or 0)}")
    print(f"{table}.selected_rows={int(item.get('selected_rows') or 0)}")
    print(f"{table}.estimated_savings_mb={item.get('estimated_savings_mb') or 0}")
    print(f"{table}.would_update_rows={int(item.get('would_update_rows') or 0)}")
PY
  echo
  echo "未执行 export execute、VACUUM、compact、prune、删除 DB、清 JSON payload、写 archive。"
  [[ "$rc" -eq 0 ]] && exit 0
  exit 1
}

run_output_value() {
  local key="$1"

  if [[ -z "${RUN_OUTPUT:-}" || ! -f "$RUN_OUTPUT" ]]; then
    return 0
  fi
  sed -n "s/^${key}=//p" "$RUN_OUTPUT" | head -n1
}

jobctl_update_status() {
  local job_id="$1"
  local status="$2"
  local exit_code="${3:-}"
  local failed_step="${4:-}"
  local failed_substep="${5:-}"
  local failed_command="${6:-}"
  local timeout_hit="${7:-}"
  local timeout_limit_sec="${8:-}"
  local refused_reason="${9:-}"
  local -a args=(__update --job-id "$job_id" --status "$status")

  if [[ -n "$exit_code" ]]; then
    args+=(--exit-code "$exit_code")
  fi
  if [[ -n "$failed_step" ]]; then
    args+=(--failed-step "$failed_step")
  fi
  if [[ -n "$failed_substep" ]]; then
    args+=(--failed-substep "$failed_substep")
  fi
  if [[ -n "$failed_command" ]]; then
    args+=(--failed-command "$failed_command")
  fi
  if [[ -n "$refused_reason" ]]; then
    args+=(--refused-reason "$refused_reason")
  fi
  if [[ -n "$timeout_hit" ]]; then
    args+=(--timeout-hit "$timeout_hit")
  fi
  if [[ -n "$timeout_limit_sec" ]]; then
    args+=(--timeout-limit-sec "$timeout_limit_sec")
  fi
  jobctl "${args[@]}" >/dev/null
}

cmd_run_job() {
  local job_id=""
  local kind=""
  local report_date=""
  local start_date=""
  local end_date=""
  local timeout_sec="${HERMES_OPS_JOB_TIMEOUT_SEC:-7200}"
  local result_file=""
  local tmp_result=""
  local rc=0
  local final_status="failed"
  local failed_step=""
  local failed_substep=""
  local failed_command=""
  local failed_exit_code=""
  local timeout_hit=""
  local timeout_limit_sec=""
  local substep_result_path=""
  local failure_label=""
  local today_bj=""
  local -a cmd=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --job-id)
        [[ $# -ge 2 ]] || die "__run-job --job-id requires JOB_ID"
        job_id="$2"
        shift 2
        ;;
      --kind)
        [[ $# -ge 2 ]] || die "__run-job --kind requires KIND"
        kind="$2"
        shift 2
        ;;
      --date)
        [[ $# -ge 2 ]] || die "__run-job --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --start)
        [[ $# -ge 2 ]] || die "__run-job --start requires YYYY-MM-DD"
        start_date="$2"
        shift 2
        ;;
      --end)
        [[ $# -ge 2 ]] || die "__run-job --end requires YYYY-MM-DD"
        end_date="$2"
        shift 2
        ;;
      *) die "unknown __run-job argument" ;;
    esac
  done

  [[ -n "$job_id" && -n "$kind" ]] || die "__run-job requires --job-id and --kind"
  validate_job_id "$job_id"
  case "$kind" in
    daily-flow)
      [[ -n "$report_date" ]] || die "daily-flow job requires --date"
      parse_required_date "$report_date"
      AUDIT_DATE="$report_date"
      cmd=(./scripts/hermes_cm_ops.sh daily-flow --date "$report_date")
      ;;
    space-check)
      [[ -z "$report_date$start_date$end_date" ]] || die "space-check job does not accept date/range"
      cmd=(./scripts/hermes_cm_ops.sh space-check)
      ;;
    archive-compress-check)
      [[ -n "$report_date" ]] || die "archive-compress-check job requires --date"
      parse_required_date "$report_date"
      AUDIT_DATE="$report_date"
      cmd=(./scripts/hermes_cm_ops.sh archive-compress-check --date "$report_date")
      ;;
    weekly-review)
      [[ -n "$start_date" && -n "$end_date" ]] || die "weekly-review job requires --start and --end"
      parse_required_date "$start_date"
      parse_required_date "$end_date"
      validate_weekly_range "$start_date" "$end_date"
      AUDIT_DATE="${start_date}..${end_date}"
      cmd=(./scripts/hermes_cm_ops.sh weekly-review --start "$start_date" --end "$end_date")
      ;;
    *)
      refuse invalid_job_kind "unsupported job kind: ${kind}"
      ;;
  esac

  result_file="$(job_result_file "$job_id")"
  AUDIT_OUTPUT_HINT="$result_file"
  mkdir -p "$(dirname "$result_file")"
  if [[ "$kind" == "daily-flow" ]]; then
    today_bj="$(beijing_today)"
    if [[ "$report_date" == "$today_bj" ]] && ! current_beijing_date_override_allowed; then
      AUDIT_ALLOWED=false
      AUDIT_REFUSED_REASON="current_beijing_date_protected"
      tmp_result="$(mktemp "$(dirname "$result_file")/.result.XXXXXX.md")"
      {
        echo "# Hermes Background Job ${job_id}"
        echo
        echo "generated_at_utc=$(TZ=UTC date -Is)"
        echo "job_id=${job_id}"
        echo "kind=${kind}"
        echo "date=${report_date}"
        echo "request_id=${HERMES_OPS_REQUEST_ID}"
        echo
        echo "📋 任务结果 | ${job_id}"
        echo
        echo "类型：${kind}"
        echo "日期：${report_date}"
        echo "状态：❌ failed"
        echo "失败步骤：preflight:current_beijing_date_guard"
        echo "失败命令：$(display_command "${cmd[@]}")"
        echo "refused_reason：current_beijing_date_protected"
        echo "exit_code：2"
        echo "timeout_hit：false"
        echo "timeout_limit_sec：${timeout_sec}"
        echo "result_path：$(display_safe_path "$result_file")"
        echo
        echo "failed_step=preflight"
        echo "failed_substep=preflight:current_beijing_date_guard"
        echo "failed_command=$(display_command "${cmd[@]}")"
        echo "refused_reason=current_beijing_date_protected"
        echo "exit_code=2"
        echo "timeout_hit=false"
        echo "timeout_limit_sec=${timeout_sec}"
        echo "result_path=$(display_safe_path "$result_file")"
        echo
        echo "stdout_tail:"
        echo "(empty)"
        echo
        echo "stderr_tail:"
        print_current_beijing_date_refusal "$report_date"
        echo
        echo "建议："
        echo "等北京时间次日 00:05 后重新运行标准日报流程${report_date}。"
        echo "/chain-monitor-report-analyst 标准日报流程${report_date}"
      } >"$tmp_result"
      mv "$tmp_result" "$result_file"
      jobctl_update_status "$job_id" failed 2 preflight "preflight:current_beijing_date_guard" "$(display_command "${cmd[@]}")" false "$timeout_sec" "current_beijing_date_protected"
      print_current_beijing_date_refusal "$report_date" >&2
      echo "job_id=${job_id}"
      echo "kind=${kind}"
      echo "status=failed"
      echo "refused_reason=current_beijing_date_protected"
      echo "exit_code=2"
      echo "result=$(display_safe_path "$result_file")"
      exit 2
    fi
  fi
  AUDIT_ALLOWED=true
  tmp_result="$(mktemp "$(dirname "$result_file")/.result.XXXXXX.md")"
  jobctl_update_status "$job_id" running

  if run_command "job_${kind}_${job_id}" "$timeout_sec" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi

  failed_step="$(run_output_value failed_step)"
  failed_substep="$(run_output_value failed_substep)"
  failed_command="$(run_output_value failed_command)"
  failed_exit_code="$(run_output_value exit_code)"
  timeout_hit="$(run_output_value timeout_hit)"
  timeout_limit_sec="$(run_output_value timeout_limit_sec)"
  substep_result_path="$(run_output_value result_path)"
  if [[ -z "$failed_exit_code" ]]; then
    failed_exit_code="$rc"
  fi
  if [[ -z "$timeout_hit" ]]; then
    if [[ "$rc" -eq 124 ]]; then
      timeout_hit=true
    else
      timeout_hit=false
    fi
  fi
  if [[ -z "$timeout_limit_sec" ]]; then
    timeout_limit_sec="$timeout_sec"
  fi
  if [[ -n "$failed_substep" ]]; then
    failure_label="$failed_substep"
  else
    failure_label="$failed_step"
  fi

  {
    echo "# Hermes Background Job ${job_id}"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "job_id=${job_id}"
    echo "kind=${kind}"
    echo "date=${report_date}"
    echo "start=${start_date}"
    echo "end=${end_date}"
    echo "request_id=${HERMES_OPS_REQUEST_ID}"
    echo
    echo "📋 任务结果 | ${job_id}"
    echo
    echo "类型：${kind}"
    echo "日期：${report_date}"
    echo "状态：$([[ "$rc" -eq 0 ]] && echo "✅ succeeded" || echo "❌ failed")"
    if [[ "$rc" -ne 0 ]]; then
      echo "失败步骤：${failure_label:-${kind}}"
      echo "失败命令：${failed_command:-$(display_command "${cmd[@]}")}"
      echo "exit_code：${failed_exit_code}"
      echo "timeout_hit：${timeout_hit}"
      echo "timeout_limit_sec：${timeout_limit_sec}"
      echo "result_path：$(display_safe_path "$result_file")"
      if [[ -n "$substep_result_path" ]]; then
        echo "flow_result_path：${substep_result_path}"
      fi
      echo
      echo "failed_step=${failed_step:-${kind}}"
      echo "failed_substep=${failed_substep:-${failure_label:-${kind}}}"
      echo "failed_command=${failed_command:-$(display_command "${cmd[@]}")}"
      echo "exit_code=${failed_exit_code}"
      echo "timeout_hit=${timeout_hit}"
      echo "timeout_limit_sec=${timeout_limit_sec}"
      echo "result_path=$(display_safe_path "$result_file")"
      if [[ -n "$substep_result_path" ]]; then
        echo "flow_result_path=${substep_result_path}"
      fi
      echo
      echo "输出摘要："
      cat "$RUN_OUTPUT"
      if [[ "$substep_result_path" == reports/* && -f "$substep_result_path" ]]; then
        echo
        echo "## daily_flow_step_results"
        cat "$substep_result_path"
      fi
      echo
      echo "建议："
      if [[ "$kind" == "daily-flow" ]]; then
        echo "请先修复上述失败子步骤，再重新运行："
        echo "/chain-monitor-report-analyst 标准日报流程${report_date}"
      else
        echo "请先查看失败命令输出和日志，再重新提交对应后台任务。"
      fi
    else
      echo "exit_code=0"
      echo "timeout_hit=false"
      echo "timeout_limit_sec=${timeout_sec}"
      echo "result_path=$(display_safe_path "$result_file")"
      echo
      echo "输出摘要："
      cat "$RUN_OUTPUT"
      if [[ "$substep_result_path" == reports/* && -f "$substep_result_path" ]]; then
        echo
        echo "## daily_flow_step_results"
        cat "$substep_result_path"
      fi
    fi
  } >"$tmp_result"
  mv "$tmp_result" "$result_file"

  if [[ -f "$(jobs_root_path)/${job_id}/cancel.request" ]]; then
    final_status="cancelled"
  elif [[ "$rc" -eq 0 ]]; then
    final_status="succeeded"
  else
    final_status="failed"
  fi
  if [[ "$final_status" == "failed" ]]; then
    jobctl_update_status "$job_id" "$final_status" "$rc" "$failed_step" "$failed_substep" "$failed_command" "$timeout_hit" "$timeout_limit_sec"
  else
    jobctl_update_status "$job_id" "$final_status" "$rc"
  fi

  echo "job_id=${job_id}"
  echo "kind=${kind}"
  echo "status=${final_status}"
  echo "exit_code=${rc}"
  echo "result=$(display_safe_path "$result_file")"
  [[ "$rc" -eq 0 ]] && exit 0
  exit 1
}

cmd_daily_flow() {
  local report_date=""
  local today_bj
  local output_dir="reports/hermes"
  local final_output
  local latest_output="${output_dir}/hermes_daily_flow_latest.md"
  local tmp_output
  local outcome_would_update_rows=""
  local outcome_still_pending_count=""

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
  if [[ "$report_date" == "$today_bj" ]] && ! current_beijing_date_override_allowed; then
    refuse_current_beijing_date "$report_date"
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
    echo "daily_close_mode=expanded_substeps"
    echo "daily_close_compression=archive_compress_dry_run_only"
    echo "compression=disabled"
    echo "compact=disabled"
    echo "vacuum=disabled"
    echo "prune=disabled"
    echo "failure_fields=failed_step failed_substep failed_command exit_code timeout_hit timeout_limit_sec stdout_tail stderr_tail result_path"
    echo
  } >"$tmp_output"

  append_flow_step "$tmp_output" "daily-close:migrate_archive" "$CLOSE_TIMEOUT_SEC" make db-migrate-date "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:migrate_archive" "$?"
  append_flow_step "$tmp_output" "daily-close:db_integrity_fast" "$CLOSE_TIMEOUT_SEC" make db-integrity "DATE=${report_date}" DB_INTEGRITY_FAST=YES || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:db_integrity_fast" "$?"
  append_flow_step "$tmp_output" "daily-close:db_report" "$CLOSE_TIMEOUT_SEC" make db-report || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:db_report" "$?"
  append_flow_step "$tmp_output" "daily-close:archive_mirror_check" "$CLOSE_TIMEOUT_SEC" "$(python_bin)" -m app.archive_maintenance --mirror-check-date "$report_date" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:archive_mirror_check" "$?"
  append_flow_step "$tmp_output" "daily-close:initial_report_daily" "$REPORT_TIMEOUT_SEC" make report-daily-date "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:initial_report_daily" "$?"
  append_flow_step "$tmp_output" "daily-close:initial_daily_compare" "$REPORT_TIMEOUT_SEC" make daily-compare "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:initial_daily_compare" "$?"
  append_flow_step "$tmp_output" "daily-close:archive_compress_dry_run" "$CLOSE_TIMEOUT_SEC" make archive-compress-dry-run "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:archive_compress_dry_run" "$?"
  append_flow_step "$tmp_output" "daily-close:sqlite_checkpoint" "$CMD_TIMEOUT_SEC" make sqlite-checkpoint || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "daily-close:sqlite_checkpoint" "$?"
  append_flow_step "$tmp_output" "replay:trade_replay_full" "$REPORT_TIMEOUT_SEC" make trade-replay-full "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "replay:trade_replay_full" "$?"
  append_flow_step "$tmp_output" "outcome_catchup_dry_run" "$REPORT_TIMEOUT_SEC" "$(python_bin)" scripts/hermes_outcome_catchup.py --date "$report_date" --dry-run || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "outcome_catchup_dry_run" "$?"
  outcome_would_update_rows="$(flow_stdout_value would_update_rows)"
  if [[ ! "$outcome_would_update_rows" =~ ^[0-9]+$ ]]; then
    {
      echo "## outcome_catchup_decision"
      echo "error=outcome_catchup_dry_run_missing_would_update_rows"
      echo "raw_would_update_rows=${outcome_would_update_rows:-missing}"
      echo
    } >>"$tmp_output"
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "outcome_catchup_dry_run" 1
  fi
  if [[ "$outcome_would_update_rows" -gt 0 ]]; then
    append_flow_step "$tmp_output" "outcome_catchup_execute" "$REPORT_TIMEOUT_SEC" env CONFIRM=YES HERMES_OUTCOME_CATCHUP_EXECUTE_CONTEXT=daily-flow "$(python_bin)" scripts/hermes_outcome_catchup.py --date "$report_date" --execute --confirm || \
      finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "outcome_catchup_execute" "$?"
    outcome_still_pending_count="$(flow_stdout_value still_pending_count)"
    if [[ "$outcome_still_pending_count" =~ ^[0-9]+$ && "$outcome_still_pending_count" -gt 0 ]]; then
      {
        echo "## outcome_catchup_warning"
        echo "warning=still_pending_count_gt_zero"
        echo "still_pending_count=${outcome_still_pending_count}"
        echo
      } >>"$tmp_output"
      FLOW_STEP_SUMMARY+=("outcome_catchup_warning: still_pending_count=${outcome_still_pending_count}")
    fi
  else
    append_flow_skipped_step "$tmp_output" "outcome_catchup_skipped" "would_update_rows=0"
  fi
  append_flow_step "$tmp_output" "report:report_daily_after_full_replay" "$REPORT_TIMEOUT_SEC" make report-daily-date "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "report:report_daily_after_full_replay" "$?"
  append_flow_step "$tmp_output" "compare:daily_compare_after_full_replay" "$REPORT_TIMEOUT_SEC" make daily-compare "DATE=${report_date}" || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "compare:daily_compare_after_full_replay" "$?"
  append_flow_step "$tmp_output" "final:sqlite_checkpoint" "$CMD_TIMEOUT_SEC" make sqlite-checkpoint || \
    finish_daily_flow_failure "$tmp_output" "$final_output" "$latest_output" "$report_date" "final:sqlite_checkpoint" "$?"

  mv "$tmp_output" "$final_output"
  cp "$final_output" "$latest_output"

  echo "标准日报流程完成"
  echo "request_id=${HERMES_OPS_REQUEST_ID}"
  echo "date=${report_date}"
  echo "output=$(display_safe_path "$final_output")"
  echo "result_path=$(display_safe_path "$final_output")"
  echo "latest=$(display_safe_path "$latest_output")"
  printf '%s\n' "${FLOW_STEP_SUMMARY[@]}"
  echo "只执行 archive compression dry-run；未执行 archive 压缩、compact、vacuum、prune。"
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

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore

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
        conn = sqlite_store.open_sqlite_connection(db_path, readonly=True, row_factory=False)
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

import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

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

LP_TERMS = (
    "lp",
    "liquidity",
    "pool",
    "clmm",
    "lp_alert",
    "lp_stage",
    "lp_noise",
)


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def boolish_true(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def intish(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def pick_lp_rows(report_payload: dict[str, Any]) -> tuple[int | str, str]:
    run_overview = as_dict(report_payload.get("run_overview"))
    lp_signal_summary = as_dict(report_payload.get("lp_signal_summary"))
    lp_suppression_summary = as_dict(report_payload.get("lp_suppression_summary"))

    value = intish(run_overview.get("lp_signal_rows"))
    if value is not None:
        return value, "run_overview"
    value = intish(lp_signal_summary.get("lp_signal_rows"))
    if value is not None:
        return value, "lp_signal_summary"

    delivered = intish(lp_signal_summary.get("delivered_count"))
    suppressed = intish(lp_signal_summary.get("suppressed_count"))
    if delivered is not None or suppressed is not None:
        return int(delivered or 0) + int(suppressed or 0), "delivered_plus_suppressed"

    delivered = intish(lp_suppression_summary.get("delivered"))
    suppressed = intish(lp_suppression_summary.get("suppressed"))
    if delivered is not None or suppressed is not None:
        return int(delivered or 0) + int(suppressed or 0), "lp_suppression_summary"

    return "missing", "missing"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()}


def logical_window(logical_date: str) -> tuple[int, int]:
    parsed = dt.date.fromisoformat(logical_date)
    start = dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    start_ts = int(start.timestamp())
    return start_ts, start_ts + 24 * 3600


def epoch_expr(column: str) -> str:
    ref = quote_ident(column)
    return f"(CASE WHEN CAST({ref} AS REAL) > 10000000000 THEN CAST({ref} AS REAL) / 1000.0 ELSE CAST({ref} AS REAL) END)"


def sqlite_lp_like_signals(logical_date: str) -> int:
    configured = os.environ.get("HERMES_DATA_QUALITY_DB_PATH") or os.environ.get("SQLITE_DB_PATH") or "data/chain_monitor.sqlite"
    db_path = Path(configured)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    if not db_path.exists():
        return 0
    uri = db_path.resolve().as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
    except sqlite3.Error:
        return 0
    try:
        cols = columns(conn, "signals")
        if not cols:
            return 0
        params: list[Any] = []
        if "logical_date" in cols:
            time_sql = 'CAST("logical_date" AS TEXT) = ?'
            params.append(logical_date)
        else:
            start_ts, end_ts = logical_window(logical_date)
            time_parts = []
            for field in ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"):
                if field in cols:
                    expr = epoch_expr(field)
                    time_parts.append(f"({expr} >= ? AND {expr} < ?)")
                    params.extend([start_ts, end_ts])
            if not time_parts:
                return 0
            time_sql = "(" + " OR ".join(time_parts) + ")"

        lp_parts: list[str] = []
        for field in (
            "signal_json",
            "lp_alert_stage",
            "canonical_semantic_key",
            "trade_action_key",
            "asset_market_state_key",
            "pool_address",
            "scan_path",
            "notifier_template",
            "delivery_decision",
        ):
            if field not in cols:
                continue
            ref = quote_ident(field)
            for term in LP_TERMS:
                lp_parts.append(f"LOWER(COALESCE(CAST({ref} AS TEXT), '')) LIKE ?")
                params.append(f"%{term}%")
        for field in ("lp_alert_stage", "pool_address"):
            if field in cols:
                ref = quote_ident(field)
                lp_parts.append(f"TRIM(COALESCE(CAST({ref} AS TEXT), '')) != ''")
        if not lp_parts:
            return 0
        row = conn.execute(f"SELECT COUNT(*) FROM signals WHERE {time_sql} AND ({' OR '.join(lp_parts)})", params).fetchone()
        return int(row[0] or 0) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


payload = as_dict(payload)
lp_signal_summary = as_dict(payload.get("lp_signal_summary"))
lp_suppression_summary = as_dict(payload.get("lp_suppression_summary"))
lp_rows, lp_rows_source = pick_lp_rows(payload)
sqlite_lp_like = intish(lp_signal_summary.get("lp_like_signals_sqlite"))
if sqlite_lp_like is None:
    sqlite_lp_like = sqlite_lp_like_signals(date)
lp_rows_int = intish(lp_rows)
lp_report_available = any(
    (
        boolish_true(lp_signal_summary.get("available")),
        boolish_true(lp_suppression_summary.get("available")),
        isinstance(payload.get("lp_stage_summary"), dict),
        isinstance(payload.get("clmm_summary"), dict),
    )
)
if lp_rows_int is not None and lp_rows_int > 0:
    lp_status = "ok"
elif sqlite_lp_like and sqlite_lp_like > 0:
    lp_status = "report_mapping_missing"
elif not lp_report_available:
    lp_status = "no_lp_samples_or_coverage_gap"
else:
    lp_status = "no_lp_samples_or_coverage_gap"

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
print(f"lp_status={lp_status}")
print(f"lp_rows_source={lp_rows_source}")
print(f"sqlite_lp_like_signals={sqlite_lp_like}")
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

cmd_data_integrity() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "data-integrity --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown data-integrity argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "data-integrity requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="data-integrity ${report_date}"

  "$(python_bin)" scripts/hermes_data_integrity.py --date "$report_date"
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

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore

date = sys.argv[1]
db = Path("data/chain_monitor.sqlite")
print("Profile复盘")
print(f"date={date}")
if not db.exists():
    print("数据不足：缺少 data/chain_monitor.sqlite")
    print(f"请先运行：标准日报流程{date}")
    raise SystemExit(0)
conn = sqlite_store.open_sqlite_connection(db, readonly=True, row_factory=True)
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

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore

date = sys.argv[1]
print("Blocker复盘")
print(f"date={date}")
counters: Counter[str] = Counter()
db = Path("data/chain_monitor.sqlite")
if db.exists():
    try:
        conn = sqlite_store.open_sqlite_connection(db, readonly=True, row_factory=True)
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

cmd_learning_review() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "learning-review --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown learning-review argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "learning-review requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="learning-review ${report_date}"
  require_daily_report_json "$report_date"

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def first_dict(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict) and value:
            return value
    return {}


def num(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def intish(value: Any) -> int | None:
    parsed = num(value)
    return None if parsed is None else int(parsed)


def fmt(value: Any, digits: int = 4) -> str:
    parsed = num(value)
    if parsed is None:
        return "missing"
    return f"{parsed:.{digits}f}".rstrip("0").rstrip(".")


def value_or_missing(value: Any) -> str:
    if value is None or value == "":
        return "missing"
    return str(value)


def top_items(counter: Counter[str], limit: int = 3) -> str:
    if not counter:
        return "missing"
    return "；".join(f"{key}={count}" for key, count in counter.most_common(limit))


def add_counter(counter: Counter[str], value: Any) -> None:
    if not isinstance(value, dict):
        return
    for key, count in value.items():
        name = str(key or "").strip()
        parsed = intish(count)
        if name and parsed is not None:
            counter[name] += parsed


def profile_line(profile: Any) -> str:
    if not isinstance(profile, dict):
        return ""
    return (
        f"samples={value_or_missing(profile.get('valid_sample_count'))} "
        f"avg_net_pnl_bps={fmt(profile.get('avg_net_pnl_bps'), 2)} "
        f"profile={value_or_missing(profile.get('profile_key'))}"
    )


date = sys.argv[1]
path = Path("reports/daily") / f"daily_report_{date}.json"
payload = json.loads(path.read_text(encoding="utf-8"))
payload = as_dict(payload)

quality = as_dict(payload.get("data_quality_summary"))
replay = as_dict(payload.get("trade_replay_summary"))
profile_summary = first_dict(
    payload.get("trade_replay_profile_summary"),
    payload.get("profile_posterior_summary"),
    replay,
)
blocker = as_dict(payload.get("blocker_summary"))
shadow = first_dict(payload.get("shadow_funnel_summary"), payload.get("shadow_opportunity_summary"), replay.get("shadow_funnel_summary"))
telegram = as_dict(payload.get("telegram_suppression_summary"))
run_overview = as_dict(payload.get("run_overview"))
data_source = as_dict(payload.get("data_source_summary"))
row_counts = as_dict(data_source.get("row_counts"))
frontier = as_dict(payload.get("candidate_frontier_summary"))
outcome_diagnosis = as_dict(payload.get("outcome_diagnosis_summary"))
lp_signal_summary = as_dict(payload.get("lp_signal_summary"))
lp_suppression_summary = as_dict(payload.get("lp_suppression_summary"))
lp_suppression_replay = as_dict(payload.get("lp_suppression_replay_summary"))
lp_suppression_sample_replay = as_dict(payload.get("lp_suppression_sample_replay_summary"))
major_coverage = as_dict(payload.get("major_coverage_summary"))
required_schema_fields = (
    "lp_signal_summary",
    "lp_stage_summary",
    "clmm_summary",
    "lp_suppression_summary",
    "candidate_frontier_summary",
    "candidate_coverage_summary",
    "outcome_diagnosis_summary",
)
missing_schema_fields = [field for field in required_schema_fields if field not in payload]
schema_complete = not missing_schema_fields

data_status = str(quality.get("data_quality_status") or "missing")
zero_activity = quality.get("zero_activity_day")
data_valid = data_status == "valid" and zero_activity is not True and zero_activity != 1

lp_signal_rows = run_overview.get("lp_signal_rows")
if lp_signal_rows is None:
    lp_signal_rows = payload.get("lp_signal_rows", payload.get("lp_rows"))
if lp_signal_rows is None:
    lp_signal_rows = quality.get("lp_signal_rows")
lp_rows_missing = lp_signal_rows is None
lp_data_present_for_mapping = (intish(lp_signal_rows) or 0) > 0 or (intish(lp_signal_summary.get("lp_like_signals_sqlite")) or 0) > 0

replay_source = replay.get("replay_source")
replay_scope = replay.get("replay_scope")
avg_net = num(replay.get("avg_net_pnl_bps"))
suppressed_avg = num(replay.get("suppressed_avg_net_pnl_bps"))
candidate_coverage = num(
    replay.get(
        "replay_coverage_rate_candidate",
        as_dict(replay.get("eligibility_summary")).get("replay_coverage_rate_candidate"),
    )
)
candidate_coverage_zero = candidate_coverage == 0.0
replay_count = intish(replay.get("replay_count")) or intish(replay.get("persisted_rows_found")) or 0
replay_valid_day = replay_source == "persisted" and replay_scope == "full" and replay_count > 0

negative_profiles = (
    as_list(profile_summary.get("blocker_grade_negative_profiles"))
    or as_list(profile_summary.get("high_confidence_negative_profiles"))
    or as_list(profile_summary.get("sampled_negative_profiles"))
    or as_list(profile_summary.get("top_negative_profiles"))
)
negative_profile_lines = [line for line in (profile_line(item) for item in negative_profiles[:3]) if line]
high_sample_positive = as_list(profile_summary.get("high_confidence_positive_profiles"))

blocker_counter: Counter[str] = Counter()
add_counter(blocker_counter, blocker.get("verification_blocker_distribution"))
add_counter(blocker_counter, as_dict(blocker.get("replay_profile_negative_summary")).get("primary_blocker_distribution"))
add_counter(blocker_counter, blocker.get("top_blockers"))
add_counter(blocker_counter, blocker.get("hard_blocker_distribution"))
shadow_blockers = as_dict(shadow.get("shadow_reason_distribution")) or as_dict(shadow.get("shadow_blocked_reasons"))
add_counter(blocker_counter, shadow_blockers)

shadow_candidate = intish(shadow.get("shadow_candidate_count"))
shadow_verified = intish(shadow.get("shadow_verified_count"))
candidate_count = intish(first_dict(payload.get("trade_opportunity_summary")).get("opportunity_candidate_count")) or 0
near_candidate_count = intish(frontier.get("near_candidate_count")) or 0
near_candidate_avg = num(frontier.get("near_candidate_avg_net_pnl_bps"))
opportunity_outcomes_total = intish(outcome_diagnosis.get("opportunity_outcomes_total")) or 0
opportunity_outcomes_completed = intish(outcome_diagnosis.get("opportunity_outcomes_completed")) or 0
opportunity_outcomes_pending = intish(outcome_diagnosis.get("opportunity_outcomes_pending")) or 0
opportunity_outcomes_past_due = intish(outcome_diagnosis.get("opportunity_outcomes_past_due_pending")) or 0
opportunity_outcomes_completed_rate = (
    None if opportunity_outcomes_total <= 0 else opportunity_outcomes_completed / max(opportunity_outcomes_total, 1)
)
opportunity_outcomes_all_pending = (
    opportunity_outcomes_total > 0
    and opportunity_outcomes_completed_rate == 0
    and opportunity_outcomes_pending > 0
    and opportunity_outcomes_past_due > 0
)

delivery_audit_rows = intish(row_counts.get("delivery_audit"))
telegram_rows = intish(row_counts.get("telegram_deliveries"))
telegram_after = intish(telegram.get("messages_after_suppression_actual"))
telegram_before = intish(telegram.get("messages_before_suppression_estimate"))
high_value_suppressed = intish(telegram.get("high_value_suppressed_count")) or 0
telegram_ratio = num(telegram.get("telegram_suppression_ratio"))
if not data_valid:
    telegram_noise = "不评价：data_quality invalid"
elif telegram_after is None:
    telegram_noise = "数据不足"
elif telegram_after <= 30 and high_value_suppressed == 0:
    telegram_noise = "否"
elif telegram_after <= 50 and high_value_suppressed == 0:
    telegram_noise = "可控"
else:
    telegram_noise = "偏多"

troubleshoot_items: list[str] = []
if candidate_coverage_zero:
    troubleshoot_items.append("CANDIDATE覆盖率=0，排查 candidate/opportunity/replay 连接")
if lp_rows_missing:
    troubleshoot_items.append("LP signal rows 缺失")
if replay_source != "persisted" or replay_scope != "full":
    troubleshoot_items.append("replay 不是 persisted/full")
if opportunity_outcomes_all_pending:
    troubleshoot_items.insert(0, "P0：opportunity_outcomes 未结算，先修 outcome catchup / 结算闭环")

why_not_stronger: list[str] = []
if data_valid and negative_profiles and not high_sample_positive:
    why_not_stronger.append("all_profiles_negative")
if candidate_count == 0:
    why_not_stronger.append("candidate_zero_gate_closed")
if not shadow_candidate and not shadow_verified:
    why_not_stronger.append("no_shadow_candidates")
if missing_schema_fields and lp_data_present_for_mapping:
    why_not_stronger.append("lp_report_mapping_missing")
elif missing_schema_fields:
    why_not_stronger.append("daily_report_schema_incomplete")
lp_replay_diag = str(lp_suppression_replay.get("diagnosis") or "")
early_sample_available = bool(lp_suppression_sample_replay.get("available"))
early_sample_diagnosis = str(lp_suppression_sample_replay.get("diagnosis") or "")
early_sample_rows = as_list(lp_suppression_sample_replay.get("by_reason"))
early_listener_sample = {}
for item in early_sample_rows:
    if isinstance(item, dict) and str(item.get("reason") or "") == "listener_prefilter/drop":
        early_listener_sample = item
        break
early_sample_actions = {
    str(item.get("recommended_action") or "")
    for item in early_sample_rows
    if isinstance(item, dict)
}
early_direction_summary = as_dict(lp_suppression_sample_replay.get("direction_inference_summary"))
early_invalid_summary = as_dict(lp_suppression_sample_replay.get("invalid_reason_counts"))
early_replay_count = intish(lp_suppression_sample_replay.get("replay_count")) or 0
early_valid_count = intish(lp_suppression_sample_replay.get("valid_replay_count")) or 0
early_ambiguous_count = intish(early_direction_summary.get("ambiguous")) or 0
early_invalid_count = intish(lp_suppression_sample_replay.get("invalid_count")) or sum(intish(value) or 0 for value in early_invalid_summary.values())
early_listener_valid_count_raw = intish(early_listener_sample.get("valid_replay_count"))
early_listener_sample_count = intish(early_listener_sample.get("sample_count")) or 0
early_listener_valid_count = early_listener_valid_count_raw or 0
early_listener_metadata_rows = intish(lp_suppression_sample_replay.get("listener_prefilter_metadata_rows")) or 0
early_listener_metadata_direction_rows = intish(lp_suppression_sample_replay.get("listener_prefilter_metadata_direction_rows")) or 0
early_listener_metadata_pair_rows = intish(lp_suppression_sample_replay.get("listener_prefilter_metadata_pair_rows")) or 0
early_listener_metadata_missing = (
    early_listener_sample_count > 0
    and early_listener_valid_count_raw == 0
    and early_listener_metadata_rows == 0
)
early_listener_metadata_pair_no_direction = (
    early_listener_sample_count > 0
    and early_listener_valid_count_raw == 0
    and early_listener_metadata_pair_rows > 0
    and early_listener_metadata_direction_rows == 0
)
early_asset_mismatch_count = (
    (intish(early_invalid_summary.get("asset_pair_mismatch")) or 0)
    + (intish(early_invalid_summary.get("outcome_asset_mismatch")) or 0)
    + (intish(early_invalid_summary.get("outcome_pair_mismatch")) or 0)
)
early_only_direction_ambiguous = (
    early_invalid_count > 0
    and early_ambiguous_count >= early_invalid_count
    and set(early_invalid_summary.keys()) <= {"direction_ambiguous"}
)
early_sample_status = str(lp_suppression_sample_replay.get("status") or "")
early_sample_degraded_asset_mismatch = early_asset_mismatch_count > 0 and early_sample_status in {"", "degraded"}
early_sample_all_ambiguous = (
    data_valid
    and early_sample_available
    and early_replay_count > 0
    and early_valid_count == 0
    and early_ambiguous_count >= early_replay_count
)
early_sample_missing = data_valid and not early_sample_available
early_sample_negative = data_valid and early_sample_available and (
    early_sample_diagnosis == "early_suppression_seems_correct"
    or (early_sample_actions and early_sample_actions <= {"keep_suppressed"})
) and not early_sample_all_ambiguous
early_sample_positive = data_valid and early_sample_available and (
    early_sample_diagnosis == "possible_over_suppression"
    or "review_threshold" in early_sample_actions
) and not early_sample_all_ambiguous
early_sample_needs_more = data_valid and early_sample_available and (
    early_sample_diagnosis == "needs_more_samples"
    or "needs_more_samples" in early_sample_actions
) and not early_sample_all_ambiguous
if (
    num(lp_suppression_summary.get("suppression_rate")) is not None
    and (num(lp_suppression_summary.get("suppression_rate")) or 0) >= 0.95
    and avg_net is not None
    and suppressed_avg is not None
    and avg_net < 0
    and suppressed_avg < 0
):
    why_not_stronger.append("lp_suppression_too_high_but_negative")
candidate_completion = num(first_dict(payload.get("trade_opportunity_summary")).get("candidate_outcome_completion_rate"))
if candidate_completion is not None and candidate_completion < 0.5:
    why_not_stronger.append("outcome_completion_low")
if opportunity_outcomes_all_pending:
    why_not_stronger.append("opportunity_outcomes_unsettled")
if early_sample_missing:
    why_not_stronger.append("early_lp_suppression_sample_replay_missing")
elif early_listener_metadata_missing:
    why_not_stronger.append("listener_prefilter_drop_historical_direction_metadata_missing")
elif early_listener_metadata_pair_no_direction:
    why_not_stronger.append("listener_prefilter_drop_metadata_pair_without_direction")
elif early_sample_all_ambiguous:
    why_not_stronger.append("early_lp_suppression_direction_fields_missing")
elif early_sample_degraded_asset_mismatch:
    why_not_stronger.append("early_lp_suppression_asset_pair_metadata_mismatch")
elif early_sample_positive:
    why_not_stronger.append("early_lp_suppression_possible_over_suppression")
elif early_sample_needs_more:
    why_not_stronger.append("early_lp_suppression_sample_needs_more")
missing_major_pairs = major_coverage.get("missing_major_pairs") if isinstance(major_coverage.get("missing_major_pairs"), list) else []
if any(str(pair).startswith(("BTC/", "SOL/")) for pair in missing_major_pairs):
    why_not_stronger.append("btc_sol_coverage_gap")
if not why_not_stronger:
    why_not_stronger.append("insufficient_positive_learning_evidence")

learned: list[str] = []
if negative_profile_lines:
    learned.append("当前主要 ETH profile 后验为负")
if avg_net is not None and suppressed_avg is not None and avg_net < 0 and suppressed_avg < 0:
    learned.append("suppression 没有明显误杀证据")
replay_with_opp = intish(frontier.get("near_candidate_replay_count")) or 0
if candidate_count == 0 and replay_with_opp:
    learned.append("CANDIDATE=0 是 gate 行为，不是 pipeline 断连")
if lp_signal_summary:
    learned.append("LP 数据存在且报告映射已输出" if not missing_schema_fields else "LP 数据存在但报告 schema 不完整")
if near_candidate_count and near_candidate_avg is not None:
    learned.append(f"near_candidate 后验 avg_net_pnl_bps={fmt(near_candidate_avg, 2)}")
if opportunity_outcomes_all_pending:
    learned.append("opportunity/profile 学习闭环未完整结算")
if early_sample_missing:
    learned.append("早期 LP noise/drop 抑制尚未抽样 replay，无法判断是否误杀")
elif early_listener_metadata_missing:
    learned.append("listener_prefilter/drop 旧样本缺 direction metadata；新版本将从后续数据开始可回放")
elif early_listener_metadata_pair_no_direction:
    learned.append("listener_prefilter/drop metadata 有 pair 但无 direction，需检查 intent/side 推断")
elif early_sample_all_ambiguous:
    learned.append("早期 LP noise/drop 抽样 replay 仍 100% direction_ambiguous，问题是方向字段缺失")
elif early_sample_degraded_asset_mismatch:
    learned.append("早期 LP noise/drop 抽样 replay 存在 asset/pair 或 outcome asset 错配，先修归因")
elif early_sample_negative:
    learned.append("早期 LP noise/drop 抑制也未显示误杀")
elif early_sample_positive:
    learned.append("可能存在早期过度过滤，需要周复盘人工检查 suppression reason")
elif early_sample_needs_more:
    learned.append("早期 LP noise/drop 抽样已运行但样本仍不足")

if early_sample_degraded_asset_mismatch:
    conclusion = "排障"
    tomorrow = "修 LP sample asset/pair 归因"
elif early_listener_metadata_missing:
    conclusion = "观察"
    tomorrow = "继续运行并积累带 metadata 的 prefilter/drop 样本"
elif early_listener_metadata_pair_no_direction:
    conclusion = "排障"
    tomorrow = "检查 listener_prefilter/drop intent/side 推断"
elif early_only_direction_ambiguous or early_sample_all_ambiguous:
    conclusion = "排障"
    tomorrow = "补 listener_prefilter/drop 方向 metadata"
elif opportunity_outcomes_all_pending:
    conclusion = "收紧"
    tomorrow = "修 outcome catchup / opportunity_outcomes 结算"
elif early_sample_missing:
    conclusion = "观察"
    tomorrow = "运行 LP抑制抽样预检/SSH execute"
elif early_sample_positive:
    conclusion = "观察"
    tomorrow = "周复盘人工检查 early suppression reason"
elif early_sample_negative:
    conclusion = "保持"
    tomorrow = "继续积累 LP early suppression 样本，不改 gate"
elif early_sample_needs_more:
    conclusion = "观察"
    tomorrow = "继续积累 LP early suppression 样本，不改 gate"
elif not data_valid:
    conclusion = "排障"
    tomorrow = "修复 data_quality 输入完整性"
elif missing_schema_fields and lp_data_present_for_mapping:
    conclusion = "排障"
    tomorrow = "修 LP report mapping"
elif lp_rows_missing or replay_source != "persisted" or replay_scope != "full":
    conclusion = "排障"
    tomorrow = "修 daily report schema" if missing_schema_fields else "不改 gate，继续积累样本"
elif avg_net is not None and suppressed_avg is not None and avg_net < 0 and suppressed_avg < 0:
    conclusion = "收紧"
    tomorrow = "排查 candidate frontier" if candidate_coverage_zero else "观察 suppression replay"
elif candidate_count == 0 and near_candidate_count == 0:
    conclusion = "保持"
    tomorrow = "不改 gate，继续积累样本"
elif candidate_count == 0 and near_candidate_avg is not None and near_candidate_avg > 0:
    conclusion = "观察"
    tomorrow = "排查 candidate frontier"
elif candidate_coverage_zero or not high_sample_positive:
    conclusion = "观察"
    tomorrow = "排查 candidate frontier" if candidate_coverage_zero else "不改 gate，继续积累样本"
elif telegram_noise == "偏多":
    conclusion = "观察"
    tomorrow = "观察 suppression replay"
else:
    conclusion = "保持"
    tomorrow = "不改 gate，继续积累样本"

print(f"Chain Monitor 学习复盘｜{date}")
print(f"数据是否有效={ '有效' if data_valid else '无效' } data_quality_status={data_status} zero_activity_day={value_or_missing(zero_activity)}")
print(f"replay_source / replay_scope={value_or_missing(replay_source)} / {value_or_missing(replay_scope)}")
print(f"有效学习日={'是' if data_valid and replay_valid_day and schema_complete else '否'} replay_count={replay_count} daily_report_schema_complete={'是' if schema_complete else '否'}")
if missing_schema_fields:
    print("daily_report_schema_missing=" + ",".join(missing_schema_fields))
print(f"avg_net_pnl_bps={fmt(avg_net, 2)}")
print(f"suppressed_avg_net_pnl_bps={fmt(suppressed_avg, 2)}")
print(f"CANDIDATE 覆盖率={fmt(candidate_coverage, 4)}")
print(f"CANDIDATE=0={'是' if candidate_count == 0 else '否'} near_candidate_count={near_candidate_count} near_candidate_avg_net_pnl_bps={fmt(near_candidate_avg, 2)}")
print(
    "opportunity_outcomes 结算="
    f"completed_rate={fmt(opportunity_outcomes_completed_rate, 4)} "
    f"pending={opportunity_outcomes_pending} "
    f"past_due_pending={opportunity_outcomes_past_due}"
)
if opportunity_outcomes_all_pending:
    print("P0排查项=opportunity_outcomes 未结算；先修 outcome catchup / 结算闭环；不得因此放宽 gate")
print(f"LP signal rows 是否缺失={'是' if lp_rows_missing else '否'} value={value_or_missing(lp_signal_rows)}")
lp_mapping_normal = schema_complete and bool(lp_signal_summary)
lp_missing_reason_display = payload.get("lp_missing_reason") or ("none" if lp_mapping_normal else "missing")
print(f"LP mapping 状态={'正常' if lp_mapping_normal else '缺失/不完整'} lp_missing_reason={lp_missing_reason_display}")
print(
    "early LP suppression sample replay="
    f"available={early_sample_available} "
    f"diagnosis={value_or_missing(early_sample_diagnosis)} "
    f"sample_limit_per_reason={value_or_missing(lp_suppression_sample_replay.get('sample_limit_per_reason'))} "
    f"valid_replay_count={value_or_missing(lp_suppression_sample_replay.get('valid_replay_count'))} "
    f"listener_prefilter_metadata_rows={value_or_missing(lp_suppression_sample_replay.get('listener_prefilter_metadata_rows'))} "
    f"listener_prefilter_metadata_direction_rows={value_or_missing(lp_suppression_sample_replay.get('listener_prefilter_metadata_direction_rows'))} "
    f"listener_prefilter_metadata_pair_rows={value_or_missing(lp_suppression_sample_replay.get('listener_prefilter_metadata_pair_rows'))} "
    f"listener_prefilter_recovery_mode={value_or_missing(lp_suppression_sample_replay.get('listener_prefilter_recovery_mode'))} "
    f"invalid_reason_counts={json.dumps(early_invalid_summary, ensure_ascii=False, sort_keys=True)} "
    f"asset_pair_summary={json.dumps(as_dict(lp_suppression_sample_replay.get('asset_pair_summary')), ensure_ascii=False, sort_keys=True)} "
    f"direction_inference_summary={json.dumps(early_direction_summary, ensure_ascii=False, sort_keys=True)}"
)
if early_listener_metadata_missing:
    print("listener_prefilter/drop 旧样本缺 direction metadata；新版本将从后续数据开始可回放。")
elif early_listener_metadata_pair_no_direction:
    print("listener_prefilter/drop metadata 有 pair 但无 direction，需检查 intent/side 推断。")
for item in early_sample_rows:
    if not isinstance(item, dict):
        continue
    reason = str(item.get("reason") or "unknown")
    if reason not in {"gate/lp_noise_filtered", "listener_prefilter/drop"}:
        continue
    print(
        f"{reason} sampled replay="
        f"sample_count={value_or_missing(item.get('sample_count'))} "
        f"valid_replay_count={value_or_missing(item.get('valid_replay_count'))} "
        f"avg_net_pnl_bps={fmt(item.get('avg_net_pnl_bps'), 2)} "
        f"profitable_rate={fmt(item.get('profitable_rate'), 4)} "
        f"action={value_or_missing(item.get('recommended_action'))}"
    )
if data_valid:
    print(f"主要负收益 profile={negative_profile_lines[0] if negative_profile_lines else 'missing'}")
    for line in negative_profile_lines[1:3]:
        print(f"负收益 profile补充={line}")
    print(f"主要 blocker={top_items(blocker_counter)}")
else:
    print("主要负收益 profile=不评价：data_quality invalid")
    print("主要 blocker=不评价：data_quality invalid")
print(f"shadow_candidate / shadow_verified={value_or_missing(shadow_candidate)} / {value_or_missing(shadow_verified)}")
print("为什么系统没有提升=" + "；".join(dict.fromkeys(why_not_stronger)))
print("系统今天学到了什么=" + ("；".join(dict.fromkeys(learned)) if learned else "数据不足"))
print(
    "delivery_audit / telegram delivery summary="
    f"delivery_audit_rows={value_or_missing(delivery_audit_rows)} "
    f"telegram_deliveries={value_or_missing(telegram_rows)} "
    f"messages_before={value_or_missing(telegram_before)} "
    f"messages_after={value_or_missing(telegram_after)} "
    f"suppression_ratio={fmt(telegram_ratio, 4)} "
    f"high_value_suppressed={high_value_suppressed}"
)
print(f"Telegram 推送是否可能噪音过多={telegram_noise}")
print(f"高样本正收益 profile={len(high_sample_positive) if data_valid else '不评价'}")
if data_valid and not high_sample_positive:
    print("VERIFIED升级依据=不足，缺少高样本正收益 profile")
if troubleshoot_items:
    print("排查项=" + "；".join(troubleshoot_items))
if not data_valid:
    print("策略评价=跳过：data_quality invalid")
print(f"今日结论：{conclusion}")
print(f"明天只建议改一个点：{tomorrow}")
print("说明=仅为学习复盘，不含执行指令。")
PY
}

cmd_candidate_coverage() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "candidate-coverage --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown candidate-coverage argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "candidate-coverage requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="candidate-coverage ${report_date}"

  "$(python_bin)" - "$report_date" <<'PY'
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from collections import Counter
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore


STATUSES = ("NONE", "CANDIDATE", "VERIFIED", "BLOCKED", "INVALIDATED")
BJ_TZ = timezone(timedelta(hours=8))


def logical_window(logical_date: str) -> tuple[int, int]:
    parsed = date_type.fromisoformat(logical_date)
    start_bj = datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    return start_ts, start_ts + 24 * 3600 - 1


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def epoch_expr(column: str) -> str:
    return f"(CASE WHEN CAST({column} AS REAL) > 10000000000 THEN CAST({column} AS REAL) / 1000.0 ELSE CAST({column} AS REAL) END)"


def time_expr(cols: set[str], candidates: tuple[str, ...]) -> str:
    present = [column for column in candidates if column in cols]
    if not present:
        return ""
    if len(present) == 1:
        return epoch_expr(present[0])
    return "COALESCE(" + ", ".join(epoch_expr(column) for column in present) + ")"


def safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def norm_status(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"", "NONE", "NULL", "N/A", "NA"}:
        return "NONE"
    if raw in STATUSES:
        return raw
    return "OTHER"


def status_counter() -> Counter[str]:
    counter: Counter[str] = Counter()
    for status in STATUSES:
        counter[status] = 0
    return counter


def format_status_distribution(counter: Counter[str]) -> str:
    items = [f"{status}={safe_int(counter.get(status))}" for status in STATUSES]
    other = safe_int(counter.get("OTHER"))
    if other:
        items.append(f"OTHER={other}")
    return " / ".join(items)


def redact_text(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"0x[0-9A-Fa-f]{64}", "0xTX_REDACTED", text)
    text = re.sub(r"0x[0-9A-Fa-f]{40}", "0xADDR_REDACTED", text)
    if len(text) > 80:
        text = text[:77] + "..."
    return text or "unknown"


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def read_daily_report(logical_date: str) -> tuple[dict[str, Any], list[str], str]:
    path = Path("reports/daily") / f"daily_report_{logical_date}.json"
    if not path.exists():
        return {}, [f"daily_report_missing:{path.as_posix()}"], "missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {}, [f"daily_report_unreadable:{exc.__class__.__name__}"], "unreadable"
    if not isinstance(payload, dict):
        return {}, ["daily_report_schema_non_dict"], "unreadable"
    return payload, [], "present"


def count_window(
    conn: sqlite3.Connection,
    table: str,
    time_candidates: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    warnings: list[str],
    predicate: str = "",
) -> int:
    cols = columns(conn, table)
    if not cols:
        warnings.append(f"sqlite_missing_table:{table}")
        return 0
    expr = time_expr(cols, time_candidates)
    if not expr:
        warnings.append(f"sqlite_missing_time_column:{table}")
        return 0
    sql = f"SELECT COUNT(*) FROM {table} WHERE {expr} >= ? AND {expr} <= ?"
    if predicate:
        sql += f" AND ({predicate})"
    try:
        row = conn.execute(sql, (start_ts, end_ts)).fetchone()
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:{table}:{exc.__class__.__name__}")
        return 0
    return safe_int(row[0] if row else 0)


def trade_opportunity_distribution(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> tuple[int, Counter[str]]:
    table = "trade_opportunities"
    counter = status_counter()
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:trade_opportunities")
        return 0, counter
    expr = time_expr(cols, ("created_at", "updated_at"))
    if not expr:
        warnings.append("sqlite_missing_time_column:trade_opportunities")
        return 0, counter

    select_parts = []
    if "status" in cols:
        select_parts.append("status")
    if "opportunity_json" in cols:
        select_parts.append("opportunity_json")
    if not select_parts:
        warnings.append("sqlite_missing_status_column:trade_opportunities")
        return count_window(conn, table, ("created_at", "updated_at"), start_ts, end_ts, warnings), counter

    sql = f"SELECT {', '.join(select_parts)} FROM {table} WHERE {expr} >= ? AND {expr} <= ?"
    rows = []
    try:
        rows = conn.execute(sql, (start_ts, end_ts)).fetchall()
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:trade_opportunities:{exc.__class__.__name__}")
        return 0, counter

    for row in rows:
        status_value = row["status"] if "status" in row.keys() else ""
        if not status_value and "opportunity_json" in row.keys():
            try:
                payload = json.loads(row["opportunity_json"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict):
                status_value = payload.get("trade_opportunity_status") or payload.get("status")
        counter[norm_status(status_value)] += 1
    return len(rows), counter


def replay_stats(
    conn: sqlite3.Connection,
    logical_date: str,
    preferred_scope: str,
    warnings: list[str],
) -> tuple[str, int, int, int, dict[str, int]]:
    table = "trade_replay_examples"
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:trade_replay_examples")
        return "missing", 0, 0, 0, {}
    if "logical_date" not in cols:
        warnings.append("sqlite_missing_column:trade_replay_examples.logical_date")
        return "missing", 0, 0, 0, {}

    scope_counts: dict[str, int] = {}
    scope_used = "all"
    where = "logical_date = ?"
    params: list[Any] = [logical_date]
    if "replay_scope" in cols:
        try:
            scope_rows = conn.execute(
                "SELECT COALESCE(replay_scope, '') AS replay_scope, COUNT(*) FROM trade_replay_examples WHERE logical_date=? GROUP BY COALESCE(replay_scope, '')",
                (logical_date,),
            ).fetchall()
            scope_counts = {str(row[0] or "missing"): safe_int(row[1]) for row in scope_rows}
        except sqlite3.Error as exc:
            warnings.append(f"sqlite_query_failed:trade_replay_examples_scope:{exc.__class__.__name__}")
            scope_counts = {}
        preferred = preferred_scope.strip() if preferred_scope else ""
        if preferred and scope_counts.get(preferred, 0) > 0:
            scope_used = preferred
        elif scope_counts.get("full", 0) > 0:
            scope_used = "full"
        elif len(scope_counts) == 1:
            scope_used = next(iter(scope_counts))
        if scope_used != "all":
            where += " AND COALESCE(replay_scope, '') = ?"
            params.append("" if scope_used == "missing" else scope_used)

    try:
        total_row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params).fetchone()
        total = safe_int(total_row[0] if total_row else 0)
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:trade_replay_examples:{exc.__class__.__name__}")
        return scope_used, 0, 0, 0, scope_counts

    with_opportunity = 0
    if "trade_opportunity_id" in cols:
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {where} AND trade_opportunity_id IS NOT NULL AND TRIM(CAST(trade_opportunity_id AS TEXT)) != ''",
                params,
            ).fetchone()
            with_opportunity = safe_int(row[0] if row else 0)
        except sqlite3.Error as exc:
            warnings.append(f"sqlite_query_failed:trade_replay_examples_opportunity:{exc.__class__.__name__}")
    else:
        warnings.append("sqlite_missing_column:trade_replay_examples.trade_opportunity_id")

    candidate = 0
    if "opportunity_status" in cols:
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {where} AND UPPER(COALESCE(opportunity_status, '')) = 'CANDIDATE'",
                params,
            ).fetchone()
            candidate = safe_int(row[0] if row else 0)
        except sqlite3.Error as exc:
            warnings.append(f"sqlite_query_failed:trade_replay_examples_candidate:{exc.__class__.__name__}")
    else:
        warnings.append("sqlite_missing_column:trade_replay_examples.opportunity_status")
    return scope_used, total, with_opportunity, candidate, scope_counts


def delivery_audit_distribution(
    conn: sqlite3.Connection,
    start_ts: int,
    end_ts: int,
    warnings: list[str],
) -> tuple[int, Counter[str]]:
    table = "delivery_audit"
    dist: Counter[str] = Counter()
    cols = columns(conn, table)
    if not cols:
        warnings.append("sqlite_missing_table:delivery_audit")
        return 0, dist
    expr = time_expr(cols, ("notifier_sent_at", "timestamp", "archive_written_at", "created_at", "updated_at"))
    if not expr:
        warnings.append("sqlite_missing_time_column:delivery_audit")
        return 0, dist
    if "sent_to_telegram" in cols:
        sent_pred = "sent_to_telegram = 1"
    elif "delivered" in cols:
        sent_pred = "delivered = 1"
    else:
        warnings.append("sqlite_missing_sent_column:delivery_audit")
        return 0, dist

    stage_expr = "stage" if "stage" in cols else "''"
    status_expr = "opportunity_status" if "opportunity_status" in cols else "''"
    sql = (
        f"SELECT {stage_expr} AS stage, {status_expr} AS opportunity_status, COUNT(*) AS count "
        f"FROM {table} WHERE {expr} >= ? AND {expr} <= ? AND ({sent_pred}) "
        f"GROUP BY {stage_expr}, {status_expr}"
    )
    try:
        rows = conn.execute(sql, (start_ts, end_ts)).fetchall()
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_query_failed:delivery_audit:{exc.__class__.__name__}")
        return 0, dist
    total = 0
    for row in rows:
        count = safe_int(row["count"])
        stage = redact_text(row["stage"]).lower()
        status = norm_status(row["opportunity_status"])
        dist[f"{stage}/{status}"] += count
        total += count
    return total, dist


def format_distribution(counter: Counter[str], limit: int = 20) -> str:
    if not counter:
        return "missing"
    return "；".join(f"{key}={value}" for key, value in counter.most_common(limit))


logical_date = sys.argv[1]
start_ts, end_ts = logical_window(logical_date)
payload, report_warnings, report_status = read_daily_report(logical_date)
quality = as_dict(payload.get("data_quality_summary"))
replay = as_dict(payload.get("trade_replay_summary"))
frontier = as_dict(payload.get("candidate_frontier_summary"))
preferred_scope = str(replay.get("replay_scope") or "")
warnings = list(report_warnings)

configured_db = os.getenv("HERMES_CANDIDATE_COVERAGE_DB_PATH", "data/chain_monitor.sqlite")
db_path = Path(configured_db)
if not db_path.is_absolute():
    db_path = Path.cwd() / db_path

signals_total = 0
trade_total = 0
trade_status = status_counter()
replay_scope_used = "missing"
replay_total = 0
replay_with_opportunity = 0
replay_candidate = 0
replay_scope_counts: dict[str, int] = {}
delivery_total = 0
delivery_dist: Counter[str] = Counter()

if not db_path.exists():
    warnings.append("sqlite_missing:data/chain_monitor.sqlite")
else:
    try:
        conn = sqlite_store.open_sqlite_connection(db_path, readonly=True, row_factory=True)
    except sqlite3.Error as exc:
        warnings.append(f"sqlite_open_failed:{exc.__class__.__name__}")
    else:
        try:
            signals_total = count_window(
                conn,
                "signals",
                ("timestamp", "archive_written_at", "created_at", "updated_at", "notifier_sent_at"),
                start_ts,
                end_ts,
                warnings,
            )
            trade_total, trade_status = trade_opportunity_distribution(conn, start_ts, end_ts, warnings)
            replay_scope_used, replay_total, replay_with_opportunity, replay_candidate, replay_scope_counts = replay_stats(
                conn,
                logical_date,
                preferred_scope,
                warnings,
            )
            delivery_total, delivery_dist = delivery_audit_distribution(conn, start_ts, end_ts, warnings)
        finally:
            conn.close()

trade_candidate = safe_int(trade_status.get("CANDIDATE"))

print(f"Chain Monitor CANDIDATE覆盖诊断｜{logical_date}")
print(f"daily_report_status={report_status}")
print(
    "data_quality_status="
    f"{quality.get('data_quality_status', 'missing')} "
    f"zero_activity_day={quality.get('zero_activity_day', 'missing')}"
)
print(
    "replay_source / replay_scope="
    f"{replay.get('replay_source', 'missing')} / {replay.get('replay_scope', 'missing')}"
)
print(
    "daily_report_replay="
    f"replay_count={replay.get('replay_count', 'missing')} "
    f"valid_replay_count={replay.get('valid_replay_count', 'missing')} "
    f"replay_coverage_rate_candidate={replay.get('replay_coverage_rate_candidate', as_dict(replay.get('eligibility_summary')).get('replay_coverage_rate_candidate', 'missing'))}"
)
print(f"signals_total={signals_total}")
print(f"trade_opportunities_total={trade_total}")
print(f"trade_opportunity_status_distribution={format_status_distribution(trade_status)}")
print(f"replay_scope_used={replay_scope_used}")
print(f"replay_scope_counts={json.dumps(replay_scope_counts, ensure_ascii=False, sort_keys=True) if replay_scope_counts else 'missing'}")
print(f"replay_examples_total={replay_total}")
print(f"replay_examples_with_opportunity={replay_with_opportunity}")
print(f"replay_examples_status_CANDIDATE={replay_candidate}")
print(f"delivery_audit_telegram_push_total={delivery_total}")
print(f"delivery_audit_stage_status_distribution={format_distribution(delivery_dist)}")
print(f"near_candidate_count={frontier.get('near_candidate_count', 'missing')}")
print(f"near_candidate_replay_count={frontier.get('near_candidate_replay_count', 'missing')}")
print(f"near_candidate_avg_net_pnl_bps={frontier.get('near_candidate_avg_net_pnl_bps', 'missing')}")
print(
    "top_near_candidate_blockers="
    + json.dumps(as_dict(frontier.get("top_near_candidate_blockers")), ensure_ascii=False, sort_keys=True)
)
print(
    "NONE_to_CANDIDATE主要缺口="
    + json.dumps(as_dict(frontier.get("top_missing_requirements")), ensure_ascii=False, sort_keys=True)
)
print(f"candidate_frontier_diagnosis={frontier.get('diagnosis', 'missing')}")

diagnostics: list[str] = []
if delivery_total >= 5 and replay_candidate <= max(1, delivery_total // 2):
    diagnostics.append("实时推送主要是 observe/signal，不是 opportunity candidate。")
if trade_candidate > 0 and replay_candidate == 0:
    diagnostics.append("trade_opportunities 有 CANDIDATE 但 replay examples 未覆盖，replay 关联层可能漏接。")
if trade_candidate == 0:
    diagnostics.append("CANDIDATE 本身为 0，candidate gate 太严格或当天无合格候选。")
if replay_total > 0 and replay_with_opportunity == 0:
    diagnostics.append("replay examples 没有关联 opportunity_id，signals -> opportunity -> replay 连接需要排查。")
if trade_candidate == 0 and trade_total > 0 and replay_with_opportunity > 0:
    diagnostics.append("CANDIDATE=0 是 gate 行为，不是 replay/pipeline 漏接。")
if frontier.get("near_candidate_avg_net_pnl_bps") is not None:
    try:
        near_avg = float(frontier.get("near_candidate_avg_net_pnl_bps"))
    except (TypeError, ValueError):
        near_avg = None
    if near_avg is not None and near_avg <= 0:
        diagnostics.append("near_candidate 后验为负，建议保持 gate，不建议放宽。")
if not diagnostics:
    diagnostics.append("未发现 CANDIDATE 覆盖链路的明显断点；请结合日报质量字段继续复核。")

for item in diagnostics:
    print(f"提示={item}")
if warnings:
    print("limitations=" + "；".join(dict.fromkeys(warnings)))
print("说明=只读聚合诊断，不含执行指令。")
PY
}

cmd_daily_report_schema_check() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "daily-report-schema-check --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown daily-report-schema-check argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "daily-report-schema-check requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="daily-report-schema-check ${report_date}"
  require_daily_report_json "$report_date"

  "$(python_bin)" scripts/hermes_daily_report_schema_check.py --date "$report_date"
}

cmd_outcome_diagnose() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "outcome-diagnose --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown outcome-diagnose argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "outcome-diagnose requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="outcome-diagnose ${report_date}"

  "$(python_bin)" scripts/hermes_outcome_diagnose.py --date "$report_date"
}

cmd_outcome_catchup() {
  local report_date=""
  local mode=""
  local confirm=false
  local today_bj=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "outcome-catchup --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --dry-run)
        [[ -z "$mode" ]] || die "outcome-catchup accepts only one mode"
        mode="dry-run"
        shift
        ;;
      --execute)
        [[ -z "$mode" ]] || die "outcome-catchup accepts only one mode"
        mode="execute"
        shift
        ;;
      --confirm)
        confirm=true
        shift
        ;;
      *) die "unknown outcome-catchup argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "outcome-catchup requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ -n "$mode" ]] || die "outcome-catchup requires --dry-run or --execute"
  if [[ "$mode" == "execute" ]]; then
    if [[ "${HERMES_OPS_PLATFORM:-}" == "telegram" ]]; then
      refuse telegram_execute_forbidden "Telegram outcome-catchup only allows --dry-run"
    fi
    [[ "${HERMES_OPS_DAILY_FLOW_INTERNAL:-}" == "1" ]] || refuse outcome_catchup_execute_forbidden "outcome-catchup execute is restricted to internal daily-flow"
    today_bj="$(beijing_today)"
    if [[ "$report_date" == "$today_bj" ]]; then
      refuse_current_beijing_date "$report_date"
    fi
    [[ "$confirm" == true || "${CONFIRM:-}" == "YES" ]] || die "outcome-catchup --execute requires --confirm or CONFIRM=YES"
  fi

  AUDIT_DATE="$report_date"
  AUDIT_MODE="$mode"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="outcome-catchup ${mode} ${report_date}"

  if [[ "$mode" == "dry-run" ]]; then
    "$(python_bin)" scripts/hermes_outcome_catchup.py --date "$report_date" --dry-run
  else
    CONFIRM=YES HERMES_OUTCOME_CATCHUP_EXECUTE_CONTEXT=daily-flow "$(python_bin)" scripts/hermes_outcome_catchup.py --date "$report_date" --execute --confirm
  fi
}

cmd_lp_suppression_sample_replay() {
  local report_date=""
  local mode=""
  local confirm=false
  local sample_limit=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "lp-suppression-sample-replay --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      --dry-run)
        [[ -z "$mode" ]] || die "lp-suppression-sample-replay accepts only one mode"
        mode="dry-run"
        shift
        ;;
      --execute)
        [[ -z "$mode" ]] || die "lp-suppression-sample-replay accepts only one mode"
        mode="execute"
        shift
        ;;
      --confirm)
        confirm=true
        shift
        ;;
      --sample-limit)
        [[ $# -ge 2 ]] || die "lp-suppression-sample-replay --sample-limit requires N"
        is_positive_int "$2" || die "lp-suppression-sample-replay --sample-limit requires positive integer"
        sample_limit="$2"
        shift 2
        ;;
      *) die "unknown lp-suppression-sample-replay argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "lp-suppression-sample-replay requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ -n "$mode" ]] || die "lp-suppression-sample-replay requires --dry-run or --execute"
  if [[ "$mode" == "execute" ]]; then
    if [[ "${HERMES_OPS_PLATFORM:-}" == "telegram" ]]; then
      refuse telegram_execute_forbidden "Telegram lp-suppression-sample-replay only allows --dry-run"
    fi
    [[ "$confirm" == true || "${CONFIRM:-}" == "YES" ]] || die "lp-suppression-sample-replay --execute requires --confirm or CONFIRM=YES"
  fi

  AUDIT_DATE="$report_date"
  AUDIT_MODE="$mode"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="lp-suppression-sample-replay ${mode} ${report_date}"

  local args=(app/trade_replay.py --date "$report_date" --lp-suppression-sample)
  if [[ -n "$sample_limit" ]]; then
    args+=(--sample-limit "$sample_limit")
  fi
  if [[ "$mode" == "dry-run" ]]; then
    args+=(--dry-run)
  else
    args+=(--execute)
  fi
  "$(python_bin)" "${args[@]}"
}

cmd_lp_diagnose() {
  local report_date=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --date)
        [[ $# -ge 2 ]] || die "lp-diagnose --date requires YYYY-MM-DD"
        report_date="$2"
        shift 2
        ;;
      *) die "unknown lp-diagnose argument" ;;
    esac
  done

  [[ -n "$report_date" ]] || die "lp-diagnose requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  AUDIT_DATE="$report_date"
  AUDIT_ALLOWED=true
  AUDIT_OUTPUT_HINT="lp-diagnose ${report_date}"

  "$(python_bin)" scripts/hermes_lp_diagnose.py --date "$report_date"
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

app_dir = Path.cwd() / "app"
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))
import sqlite_store  # type: ignore

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
        conn = sqlite_store.open_sqlite_connection(db, readonly=True, row_factory=True)
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
JOB_TIMEOUT_SEC="${HERMES_OPS_JOB_TIMEOUT_SEC:-7200}"

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
FLOW_COMMAND=""
FLOW_EXIT_CODE=0
FLOW_TIMEOUT_LIMIT_SEC=0
FLOW_TIMEOUT_HIT=false
FLOW_STDOUT_TAIL=""
FLOW_STDERR_TAIL=""
FLOW_STDOUT_RAW=""
FLOW_STDERR_RAW=""
FLOW_STDOUT_RAW_BYTES=0
FLOW_STDOUT_RAW_LINES=0
FLOW_STDERR_RAW_BYTES=0
FLOW_STDERR_RAW_LINES=0
FLOW_STDOUT_TRUNCATED=0
FLOW_STDERR_TRUNCATED=0
HEALTH_PARTIAL=0
HEALTH_SUMMARY=()
FLOW_STEP_SUMMARY=()

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
  help|command-menu|lock-status|report|close|health|system-health|listener-health|digest|analyze|submit-daily-flow|submit-space-check|submit-archive-compress-check|submit-weekly-review|job-status|job-list|job-result|job-log|job-diagnose|job-cancel|space-fast|db-size-diagnose|db-slim-dry-run|__run-job|daily-flow|replay-check|data-quality|data-integrity|profile-review|blocker-review|shadow-review|learning-review|candidate-coverage|daily-report-schema-check|outcome-diagnose|outcome-catchup|lp-suppression-sample-replay|lp-diagnose|space-check|archive-compress-check|weekly-review)
    AUDIT_COMMAND="$1"
    ;;
  *)
    AUDIT_COMMAND="unknown"
    refuse unknown_command "unknown command"
    ;;
esac

enforce_router_guard "$AUDIT_COMMAND"
enforce_long_job_runner_guard "$AUDIT_COMMAND"
enforce_job_runner_guard "$AUDIT_COMMAND"

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

if [[ "$1" == "lock-status" ]]; then
  shift
  ensure_runtime
  emit_request_header
  cmd_lock_status "$@"
  exit $?
fi

case "$1" in
  submit-daily-flow)
    shift
    emit_request_header
    cmd_submit_daily_flow "$@"
    exit $?
    ;;
  submit-space-check)
    shift
    emit_request_header
    cmd_submit_space_check "$@"
    exit $?
    ;;
  submit-archive-compress-check)
    shift
    emit_request_header
    cmd_submit_archive_compress_check "$@"
    exit $?
    ;;
  submit-weekly-review)
    shift
    emit_request_header
    cmd_submit_weekly_review "$@"
    exit $?
    ;;
  job-status)
    shift
    emit_request_header
    cmd_job_status "$@"
    exit $?
    ;;
  job-list)
    shift
    emit_request_header
    cmd_job_list "$@"
    exit $?
    ;;
  job-result)
    shift
    emit_request_header
    cmd_job_result "$@"
    exit $?
    ;;
  job-log)
    shift
    emit_request_header
    cmd_job_log "$@"
    exit $?
    ;;
  job-diagnose)
    shift
    emit_request_header
    cmd_job_diagnose "$@"
    exit $?
    ;;
  job-cancel)
    shift
    emit_request_header
    cmd_job_cancel "$@"
    exit $?
    ;;
  space-fast)
    shift
    emit_request_header
    cmd_space_fast "$@"
    exit $?
    ;;
  db-size-diagnose)
    shift
    emit_request_header
    cmd_db_size_diagnose "$@"
    exit $?
    ;;
  __run-job)
    shift
    ensure_runtime
    emit_request_header
    TMP_DIR="$(mktemp -d)"
    cmd_run_job "$@"
    ;;
esac

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
  db-slim-dry-run)
    shift
    cmd_db_slim_dry_run "$@"
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
  data-integrity)
    shift
    cmd_data_integrity "$@"
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
  learning-review)
    shift
    cmd_learning_review "$@"
    ;;
  candidate-coverage)
    shift
    cmd_candidate_coverage "$@"
    ;;
  daily-report-schema-check)
    shift
    cmd_daily_report_schema_check "$@"
    ;;
  outcome-diagnose)
    shift
    cmd_outcome_diagnose "$@"
    ;;
  outcome-catchup)
    shift
    cmd_outcome_catchup "$@"
    ;;
  lp-suppression-sample-replay)
    shift
    cmd_lp_suppression_sample_replay "$@"
    ;;
  lp-diagnose)
    shift
    cmd_lp_diagnose "$@"
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
