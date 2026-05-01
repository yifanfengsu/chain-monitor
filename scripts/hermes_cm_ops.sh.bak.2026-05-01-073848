#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/hermes_cm_ops.sh --help
  ./scripts/hermes_cm_ops.sh report --date YYYY-MM-DD
  ./scripts/hermes_cm_ops.sh close --date YYYY-MM-DD --confirm-compress [--allow-today]
  ./scripts/hermes_cm_ops.sh health
  ./scripts/hermes_cm_ops.sh digest --date YYYY-MM-DD [--mode fast|deep]
  ./scripts/hermes_cm_ops.sh analyze --date YYYY-MM-DD [--mode fast|deep] [--auto-build]

Environment:
  HERMES_OPS_MAX_CMD_BYTES=80000
  HERMES_OPS_MAX_CMD_LINES=400
  HERMES_OPS_CMD_TIMEOUT_SEC=120

Exit codes:
  0 success
  1 partial failure or command failure
  2 argument error or refused execution
USAGE
}

die() {
  echo "error: $*" >&2
  exit 2
}

is_positive_int() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
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
}

run_with_timeout() {
  timeout "${CMD_TIMEOUT_SEC}s" "$@"
}

run_command() {
  local title="$1"
  shift
  local safe_title="${title//[!A-Za-z0-9_]/_}"
  local raw_path="${TMP_DIR}/${safe_title}.raw"
  local limited_path="${TMP_DIR}/${safe_title}.out"
  local rc=0

  RUN_OUTPUT="$limited_path"
  RUN_RAW_BYTES=0
  RUN_RAW_LINES=0
  RUN_TRUNCATED=0

  if run_with_timeout "$@" >"$raw_path" 2>&1; then
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
  echo "timeout_sec: ${CMD_TIMEOUT_SEC}"
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
  command -v timeout >/dev/null 2>&1 || die "timeout command not found"
  is_positive_int "$MAX_CMD_BYTES" || die "HERMES_OPS_MAX_CMD_BYTES must be a positive integer"
  is_positive_int "$MAX_CMD_LINES" || die "HERMES_OPS_MAX_CMD_LINES must be a positive integer"
  is_positive_int "$CMD_TIMEOUT_SEC" || die "HERMES_OPS_CMD_TIMEOUT_SEC must be a positive integer"
}

parse_required_date() {
  local value="$1"

  validate_date "$value" || die "invalid date: ${value}"
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
  shift
  local rc=0

  echo "command: $(display_command "$@")"
  if run_command "$title" "$@"; then
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
        die "unknown report argument: $1"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "report requires --date YYYY-MM-DD"
  parse_required_date "$report_date"

  cmd=(make report-daily-date "DATE=${report_date}")
  echo "command: $(display_command "${cmd[@]}")"
  if run_command "report_${report_date}" "${cmd[@]}"; then
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
        shift
        ;;
      --allow-today)
        allow_today=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown close argument: $1"
        ;;
    esac
  done

  [[ -n "$close_date" ]] || die "close requires --date YYYY-MM-DD"
  parse_required_date "$close_date"
  if [[ "$confirm_compress" -ne 1 ]]; then
    die "refusing close: --confirm-compress is required"
  fi

  today_utc="$(TZ=UTC date +%F)"
  if [[ "$close_date" == "$today_utc" && "$allow_today" -ne 1 ]]; then
    die "refusing close for current UTC date ${today_utc}; pass --allow-today only after explicit approval"
  fi

  cmd=(make daily-close "DATE=${close_date}" COMPRESS=YES CONFIRM=YES)
  if [[ "$allow_today" -eq 1 ]]; then
    cmd+=(ALLOW_TODAY=YES)
  fi

  echo "STATE-CHANGING COMMAND: daily-close with confirmed compression."
  echo "exact command: $(display_command "${cmd[@]}")"
  if run_command "close_${close_date}" "${cmd[@]}"; then
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

  if run_command "$title" "$@"; then
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
    echo "command_timeout_sec=${CMD_TIMEOUT_SEC}"
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

  mkdir -p "$output_dir"
  tmp_output="$(mktemp "${output_dir}/.hermes_health_latest.XXXXXX.md")"
  HEALTH_TMP_OUTPUT="$tmp_output"
  HEALTH_PARTIAL=0
  HEALTH_SUMMARY=()

  {
    echo "# Hermes Health Latest"
    echo
    echo "generated_at_utc=$(TZ=UTC date -Is)"
    echo "workdir=$(pwd)"
    echo "output_policy=aggregate diagnostics only; no raw rows or raw payloads requested"
    echo "max_cmd_bytes=${MAX_CMD_BYTES}"
    echo "max_cmd_lines=${MAX_CMD_LINES}"
    echo "cmd_timeout_sec=${CMD_TIMEOUT_SEC}"
    echo
  } >"$tmp_output"

  append_health_section "$tmp_output" "DB Report" make db-report
  append_health_section "$tmp_output" "Report Source Fast" make report-source-fast
  append_health_section "$tmp_output" "Health" make health
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
        die "unknown digest argument: $1"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "digest requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ "$mode" == "fast" || "$mode" == "deep" ]] || die "invalid digest mode: ${mode}"
  [[ -f "$digest_script" ]] || die "missing ${digest_script}; cannot generate Hermes digest input"
  [[ -x "$digest_script" ]] || die "${digest_script} exists but is not executable"

  cmd=("$digest_script" --date "$report_date" --mode "$mode")
  echo "command: $(display_command "${cmd[@]}")"
  if run_command "digest_${report_date}_${mode}" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi
  emit_command_report "digest ${report_date} ${mode}" "$rc" "${cmd[@]}"

  digest_path="$(sed -n '$p' "$RUN_OUTPUT" || true)"
  if [[ -n "$digest_path" ]]; then
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
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown analyze argument: $1"
        ;;
    esac
  done

  [[ -n "$report_date" ]] || die "analyze requires --date YYYY-MM-DD"
  parse_required_date "$report_date"
  [[ "$mode" == "fast" || "$mode" == "deep" ]] || die "invalid analyze mode: ${mode}"
  [[ -f "$digest_script" ]] || die "missing ${digest_script}; cannot generate Hermes digest input"
  [[ -x "$digest_script" ]] || die "${digest_script} exists but is not executable"

  if canonical_daily_report_present "$report_date"; then
    canonical_report_status="present"
  elif [[ "$auto_build" -eq 1 ]]; then
    cmd=(make report-daily-date "DATE=${report_date}")
    if run_analyze_step "analyze_report_${report_date}" "${cmd[@]}"; then
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
    echo "missing canonical daily report for ${report_date}; run ./scripts/hermes_cm_ops.sh report --date ${report_date} first, or rerun analyze with --auto-build" >&2
    exit 1
  fi

  if daily_compare_present "$report_date"; then
    daily_compare_status="present"
  else
    daily_compare_status="missing"
  fi

  if [[ "$daily_compare_status" != "present" ]]; then
    cmd=(make daily-compare "DATE=${report_date}")
    if run_analyze_step "analyze_daily_compare_${report_date}" "${cmd[@]}"; then
      if daily_compare_present "$report_date"; then
        daily_compare_status="generated"
      elif [[ "$auto_build" -eq 1 ]]; then
        cmd=(make daily-compare-rebuild "DATE=${report_date}")
        rebuild_performed="true"
        echo "rebuild_performed=true"
        if run_analyze_step "analyze_daily_compare_rebuild_${report_date}" "${cmd[@]}"; then
          if daily_compare_present "$report_date"; then
            daily_compare_status="generated"
          else
            daily_compare_status="missing"
          fi
        else
          fail_runtime "failed to rebuild daily compare for ${report_date}"
        fi
      else
        daily_compare_status="missing"
      fi
    elif [[ "$auto_build" -eq 1 ]]; then
      cmd=(make daily-compare-rebuild "DATE=${report_date}")
      rebuild_performed="true"
      echo "rebuild_performed=true"
      if run_analyze_step "analyze_daily_compare_rebuild_${report_date}" "${cmd[@]}"; then
        if daily_compare_present "$report_date"; then
          daily_compare_status="generated"
        else
          daily_compare_status="missing"
        fi
      else
        fail_runtime "failed to generate daily compare for ${report_date}"
      fi
    else
      daily_compare_status="missing"
    fi
  fi

  cmd=("$digest_script" --date "$report_date" --mode "$mode")
  if run_analyze_step "analyze_digest_${report_date}_${mode}" "${cmd[@]}"; then
    rc=0
  else
    rc=$?
  fi

  expected_digest="reports/hermes/hermes_digest_input_${report_date}.md"
  digest_path="$(grep -E '^reports/hermes/hermes_digest_input_[0-9]{4}-[0-9]{2}-[0-9]{2}\.md$' "$RUN_OUTPUT" 2>/dev/null | tail -n1 || true)"
  if [[ -z "$digest_path" && -f "$expected_digest" ]]; then
    digest_path="$expected_digest"
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MAX_CMD_BYTES="${HERMES_OPS_MAX_CMD_BYTES:-80000}"
MAX_CMD_LINES="${HERMES_OPS_MAX_CMD_LINES:-400}"
CMD_TIMEOUT_SEC="${HERMES_OPS_CMD_TIMEOUT_SEC:-120}"
TMP_DIR=""
HEALTH_TMP_OUTPUT=""
RUN_OUTPUT=""
RUN_RAW_BYTES=0
RUN_RAW_LINES=0
RUN_TRUNCATED=0
HEALTH_PARTIAL=0
HEALTH_SUMMARY=()

trap cleanup EXIT

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
  usage
  exit 0
fi

cd "$REPO_ROOT"
ensure_repo_root
ensure_runtime
mkdir -p reports/hermes
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
  digest)
    shift
    cmd_digest "$@"
    ;;
  analyze)
    shift
    cmd_analyze "$@"
    ;;
  *)
    die "unknown command: $1"
    ;;
esac
