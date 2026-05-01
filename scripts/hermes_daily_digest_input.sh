#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  scripts/hermes_daily_digest_input.sh
  scripts/hermes_daily_digest_input.sh YYYY-MM-DD
  scripts/hermes_daily_digest_input.sh --date YYYY-MM-DD [--mode fast|deep]
  scripts/hermes_daily_digest_input.sh --help

Environment:
  HERMES_DIGEST_WORKDIR=/run-project/chain-monitor
  HERMES_DIGEST_MODE=fast|deep
  HERMES_DIGEST_MAX_LINES=220
  HERMES_DIGEST_MAX_FILE_BYTES=120000
  HERMES_DIGEST_MAX_CMD_BYTES=60000
  HERMES_DIGEST_CMD_TIMEOUT_SEC=45
  HERMES_DIGEST_INCLUDE_DB=0|1         # deep mode only; default 1 in deep mode
  HERMES_DIGEST_INCLUDE_COVERAGE=0|1   # deep mode only; default 1 in deep mode
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
  if ! normalized="$(TZ=Asia/Shanghai date -d "$value" +%F 2>/dev/null)"; then
    return 1
  fi
  [[ "$normalized" == "$value" ]]
}

WORKDIR="${HERMES_DIGEST_WORKDIR:-/run-project/chain-monitor}"
MODE="${HERMES_DIGEST_MODE:-fast}"
MAX_LINES="${HERMES_DIGEST_MAX_LINES:-220}"
MAX_FILE_BYTES="${HERMES_DIGEST_MAX_FILE_BYTES:-120000}"
MAX_CMD_BYTES="${HERMES_DIGEST_MAX_CMD_BYTES:-60000}"
CMD_TIMEOUT_SEC="${HERMES_DIGEST_CMD_TIMEOUT_SEC:-45}"
INCLUDE_DB="${HERMES_DIGEST_INCLUDE_DB:-}"
INCLUDE_COVERAGE="${HERMES_DIGEST_INCLUDE_COVERAGE:-}"
REPORT_DATE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)
      [[ $# -ge 2 ]] || die "--date requires YYYY-MM-DD"
      REPORT_DATE="$2"
      shift 2
      ;;
    --mode)
      [[ $# -ge 2 ]] || die "--mode requires fast or deep"
      MODE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      die "unknown option: $1"
      ;;
    *)
      [[ -z "$REPORT_DATE" ]] || die "multiple report dates provided"
      REPORT_DATE="$1"
      shift
      ;;
  esac
done

REPORT_DATE="${REPORT_DATE:-$(TZ=Asia/Shanghai date -d 'yesterday' +%F)}"

validate_date "$REPORT_DATE" || die "invalid report date: ${REPORT_DATE}"
[[ "$MODE" == "fast" || "$MODE" == "deep" ]] || die "invalid mode: ${MODE}"
is_positive_int "$MAX_LINES" || die "HERMES_DIGEST_MAX_LINES must be a positive integer"
is_positive_int "$MAX_FILE_BYTES" || die "HERMES_DIGEST_MAX_FILE_BYTES must be a positive integer"
is_positive_int "$MAX_CMD_BYTES" || die "HERMES_DIGEST_MAX_CMD_BYTES must be a positive integer"
is_positive_int "$CMD_TIMEOUT_SEC" || die "HERMES_DIGEST_CMD_TIMEOUT_SEC must be a positive integer"
[[ -z "$INCLUDE_DB" || "$INCLUDE_DB" == "0" || "$INCLUDE_DB" == "1" ]] || die "HERMES_DIGEST_INCLUDE_DB must be 0 or 1"
[[ -z "$INCLUDE_COVERAGE" || "$INCLUDE_COVERAGE" == "0" || "$INCLUDE_COVERAGE" == "1" ]] || die "HERMES_DIGEST_INCLUDE_COVERAGE must be 0 or 1"

RUN_DB=0
RUN_COVERAGE=0
if [[ "$MODE" == "deep" ]]; then
  RUN_DB="${INCLUDE_DB:-1}"
  RUN_COVERAGE="${INCLUDE_COVERAGE:-1}"
fi

cd "$WORKDIR"
mkdir -p reports/hermes

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_digest.XXXXXX")"
TMP_OUT="$(mktemp "reports/hermes/.hermes_digest_input_${REPORT_DATE}.XXXXXX.md")"
LATEST_TMP="$(mktemp "reports/hermes/.hermes_digest_latest.XXXXXX.md")"
OUT="reports/hermes/hermes_digest_input_${REPORT_DATE}.md"
LATEST_OUT="reports/hermes/hermes_digest_latest.md"
PARTIAL=0

cleanup() {
  rm -rf "$TMP_DIR"
  rm -f "$TMP_OUT" "$LATEST_TMP"
}
trap cleanup EXIT

SOURCE_WARNINGS=()
SOURCE_DECLARED_DATES=()
COMPARE_DATE_MATCHES=()
DATE_SPECIFIC_SOURCE_COUNT=0
LATEST_FALLBACK_SOURCE_COUNT=0
MISSING_SOURCE_COUNT=0
SOURCE_MODE="date-specific"
SOURCE_DATE_VERIFIED="true"
SOURCE_FALLBACK_TO_LATEST="false"

DAILY_JSON_SOURCE=""
DAILY_JSON_ORIGIN="missing"
DAILY_MD_SOURCE=""
DAILY_MD_ORIGIN="missing"
COMPARE_JSON_SOURCE=""
COMPARE_JSON_ORIGIN="missing"
COMPARE_MD_SOURCE=""
COMPARE_MD_ORIGIN="missing"
QUALITY_SOURCE="reports/quality_rows.csv"

add_warning() {
  SOURCE_WARNINGS+=("$1")
}

join_values() {
  if [[ "$#" -eq 0 ]]; then
    echo "none"
    return
  fi

  local IFS="|"
  echo "$*"
}

join_warnings() {
  if [[ "${#SOURCE_WARNINGS[@]}" -eq 0 ]]; then
    echo "none"
    return
  fi

  local IFS="; "
  echo "${SOURCE_WARNINGS[*]}"
}

extract_declared_date() {
  local path="$1"
  local role="$2"
  local line=""

  if [[ "$role" == "compare" ]]; then
    line="$(grep -m1 -E '"today_date"[[:space:]]*:|"report_date"[[:space:]]*:|today_date[:=]|report_date[:=]' "$path" 2>/dev/null || true)"
  else
    line="$(grep -m1 -E '"logical_date"[[:space:]]*:|"report_date"[[:space:]]*:|logical_date[:=]|report_date[:=]' "$path" 2>/dev/null || true)"
  fi

  printf '%s\n' "$line" | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -n1 || true
}

discover_compare_matches() {
  if [[ ! -d "reports/daily_compare" ]]; then
    return
  fi

  mapfile -t COMPARE_DATE_MATCHES < <(
    find reports/daily_compare -maxdepth 1 -type f \
      \( -name "daily_compare_${REPORT_DATE}.json" \
      -o -name "daily_compare_${REPORT_DATE}.md" \
      -o -name "daily_compare_${REPORT_DATE}_*.json" \
      -o -name "daily_compare_${REPORT_DATE}_*.md" \
      -o -name "*${REPORT_DATE}*.json" \
      -o -name "*${REPORT_DATE}*.md" \) \
      | LC_ALL=C sort
  )
}

select_daily_source() {
  local ext="$1"
  local dated="reports/daily/daily_report_${REPORT_DATE}.${ext}"
  local latest="reports/daily/daily_report_latest.${ext}"

  if [[ -f "$dated" ]]; then
    printf '%s\tdate-specific\n' "$dated"
  elif [[ -f "$latest" ]]; then
    printf '%s\tlatest\n' "$latest"
  else
    printf '\tmissing\n'
  fi
}

select_compare_source() {
  local ext="$1"
  local exact="reports/daily_compare/daily_compare_${REPORT_DATE}.${ext}"
  local latest="reports/daily_compare/daily_compare_latest.${ext}"
  local -a matches=()

  if [[ -f "$exact" ]]; then
    printf '%s\tdate-specific\n' "$exact"
    return
  fi

  if [[ -d "reports/daily_compare" ]]; then
    mapfile -t matches < <(
      find reports/daily_compare -maxdepth 1 -type f \
        -name "daily_compare_${REPORT_DATE}_*.${ext}" \
        | LC_ALL=C sort
    )
  fi

  if [[ "${#matches[@]}" -gt 0 ]]; then
    printf '%s\tdate-specific\n' "${matches[0]}"
  elif [[ -f "$latest" ]]; then
    printf '%s\tlatest\n' "$latest"
  else
    printf '\tmissing\n'
  fi
}

read_source_selection() {
  local result="$1"
  local path_var="$2"
  local origin_var="$3"
  local path="${result%%$'\t'*}"
  local origin="${result#*$'\t'}"

  printf -v "$path_var" '%s' "$path"
  printf -v "$origin_var" '%s' "$origin"
}

count_source_origin() {
  local label="$1"
  local path="$2"
  local origin="$3"

  case "$origin" in
    date-specific)
      DATE_SPECIFIC_SOURCE_COUNT=$((DATE_SPECIFIC_SOURCE_COUNT + 1))
      ;;
    latest)
      LATEST_FALLBACK_SOURCE_COUNT=$((LATEST_FALLBACK_SOURCE_COUNT + 1))
      SOURCE_FALLBACK_TO_LATEST="true"
      SOURCE_DATE_VERIFIED="false"
      add_warning "latest_fallback_used_for_${label}; date-specific source missing for ${REPORT_DATE}"
      ;;
    missing)
      MISSING_SOURCE_COUNT=$((MISSING_SOURCE_COUNT + 1))
      SOURCE_DATE_VERIFIED="false"
      PARTIAL=1
      add_warning "missing_source_for_${label}; no date-specific or latest source available"
      ;;
  esac

  if [[ "$origin" != "missing" && -n "$path" && ! -f "$path" ]]; then
    MISSING_SOURCE_COUNT=$((MISSING_SOURCE_COUNT + 1))
    SOURCE_DATE_VERIFIED="false"
    PARTIAL=1
    add_warning "selected_source_not_readable_for_${label}: ${path}"
  fi
}

verify_source_date() {
  local label="$1"
  local path="$2"
  local origin="$3"
  local role="$4"
  local declared_date=""

  [[ "$origin" != "missing" && -n "$path" && -f "$path" ]] || return

  declared_date="$(extract_declared_date "$path" "$role")"
  SOURCE_DECLARED_DATES+=("${label}:${declared_date:-unknown}")

  if [[ "$origin" == "latest" ]]; then
    SOURCE_DATE_VERIFIED="false"
    add_warning "source_date_not_strict_for_${label}; latest fallback declared_date=${declared_date:-unknown}"
    return
  fi

  if [[ -z "$declared_date" ]]; then
    SOURCE_DATE_VERIFIED="false"
    add_warning "source_date_unknown_for_${label}; could not extract report_date"
  elif [[ "$declared_date" != "$REPORT_DATE" ]]; then
    SOURCE_DATE_VERIFIED="false"
    add_warning "source_date_mismatch_for_${label}; expected=${REPORT_DATE} actual=${declared_date}"
  fi
}

finalize_source_mode() {
  if [[ "$LATEST_FALLBACK_SOURCE_COUNT" -gt 0 && "$DATE_SPECIFIC_SOURCE_COUNT" -gt 0 ]]; then
    SOURCE_MODE="mixed"
  elif [[ "$LATEST_FALLBACK_SOURCE_COUNT" -gt 0 ]]; then
    SOURCE_MODE="latest-fallback"
  else
    SOURCE_MODE="date-specific"
  fi

  if [[ "${#COMPARE_DATE_MATCHES[@]}" -eq 0 ]]; then
    add_warning "no_daily_compare_date_match_found_for_${REPORT_DATE}"
  fi
}

discover_compare_matches
read_source_selection "$(select_daily_source json)" DAILY_JSON_SOURCE DAILY_JSON_ORIGIN
read_source_selection "$(select_daily_source md)" DAILY_MD_SOURCE DAILY_MD_ORIGIN
read_source_selection "$(select_compare_source json)" COMPARE_JSON_SOURCE COMPARE_JSON_ORIGIN
read_source_selection "$(select_compare_source md)" COMPARE_MD_SOURCE COMPARE_MD_ORIGIN

count_source_origin "daily_report_json" "$DAILY_JSON_SOURCE" "$DAILY_JSON_ORIGIN"
count_source_origin "daily_report_md" "$DAILY_MD_SOURCE" "$DAILY_MD_ORIGIN"
count_source_origin "daily_compare_json" "$COMPARE_JSON_SOURCE" "$COMPARE_JSON_ORIGIN"
count_source_origin "daily_compare_md" "$COMPARE_MD_SOURCE" "$COMPARE_MD_ORIGIN"

verify_source_date "daily_report_json" "$DAILY_JSON_SOURCE" "$DAILY_JSON_ORIGIN" "daily"
verify_source_date "daily_report_md" "$DAILY_MD_SOURCE" "$DAILY_MD_ORIGIN" "daily"
verify_source_date "daily_compare_json" "$COMPARE_JSON_SOURCE" "$COMPARE_JSON_ORIGIN" "compare"
verify_source_date "daily_compare_md" "$COMPARE_MD_SOURCE" "$COMPARE_MD_ORIGIN" "compare"
finalize_source_mode

run_with_timeout() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "${CMD_TIMEOUT_SEC}s" "$@"
  else
    "$@"
  fi
}

emit_limited_file_body() {
  local path="$1"
  head -c "$MAX_FILE_BYTES" "$path" | sed -n "1,${MAX_LINES}p" || true
}

emit_file() {
  local title="$1"
  local path="$2"
  local origin="$3"
  local role="$4"
  local resolved=""
  local declared_date=""

  echo "## ${title}"
  echo "source_path=${path:-missing}"
  echo "source_origin=${origin}"
  if [[ -n "$path" && -f "$path" ]]; then
    echo "source_status=present"
    if resolved="$(readlink -f "$path" 2>/dev/null)"; then
      echo "resolved_path=${resolved}"
    fi
    declared_date="$(extract_declared_date "$path" "$role")"
    echo "declared_date=${declared_date:-unknown}"
    if [[ -n "$declared_date" && "$declared_date" == "$REPORT_DATE" ]]; then
      echo "declared_date_matches_report_date=true"
    else
      echo "declared_date_matches_report_date=false"
    fi
    echo "source_limit=max_bytes=${MAX_FILE_BYTES} max_lines=${MAX_LINES}"
    emit_limited_file_body "$path"
  else
    echo "source_status=missing"
    echo "warning=missing_canonical_source"
    PARTIAL=1
  fi
  echo
}

emit_quality_sample() {
  local path="$1"
  local resolved=""

  echo "## ${path}"
  if [[ -f "$path" ]]; then
    echo "source_status=present"
    if resolved="$(readlink -f "$path" 2>/dev/null)"; then
      echo "resolved_path=${resolved}"
    fi
    echo "sample_strategy=header_plus_first_rows"
    echo "source_limit=max_bytes=${MAX_FILE_BYTES} max_lines=${MAX_LINES}"
    emit_limited_file_body "$path"
  else
    echo "source_status=missing"
    echo "warning=missing_quality_rows"
  fi
  echo
}

emit_inventory() {
  echo "## Selected canonical sources"
  echo "daily_report_json=${DAILY_JSON_SOURCE:-missing} origin=${DAILY_JSON_ORIGIN}"
  echo "daily_report_md=${DAILY_MD_SOURCE:-missing} origin=${DAILY_MD_ORIGIN}"
  echo "daily_compare_json=${COMPARE_JSON_SOURCE:-missing} origin=${COMPARE_JSON_ORIGIN}"
  echo "daily_compare_md=${COMPARE_MD_SOURCE:-missing} origin=${COMPARE_MD_ORIGIN}"
  echo "quality_rows=${QUALITY_SOURCE}"
  echo
  echo "## Daily compare date matches"
  echo "daily_compare_date_matches=$(join_values "${COMPARE_DATE_MATCHES[@]}")"
  echo
}

emit_limited_cmd_output() {
  local path="$1"
  head -c "$MAX_CMD_BYTES" "$path" | sed -n "1,${MAX_LINES}p" || true
}

run_optional_cmd() {
  local title="$1"
  shift
  local safe_title="${title//[^A-Za-z0-9_]/_}"
  local cmd_out="${TMP_DIR}/${safe_title}.out"
  local rc=0
  local output_bytes

  echo "## ${title}"
  echo "command=$*"
  echo "command_timeout_sec=${CMD_TIMEOUT_SEC}"
  echo "command_limit=max_bytes=${MAX_CMD_BYTES} max_lines=${MAX_LINES}"

  if run_with_timeout "$@" >"$cmd_out" 2>&1; then
    echo "command_status=ok"
  else
    rc=$?
    PARTIAL=1
    echo "command_status=failed exit_code=${rc}"
    if [[ "$rc" -eq 124 ]]; then
      echo "warning=command_timeout"
    else
      echo "warning=command_failed"
    fi
  fi

  output_bytes="$(wc -c <"$cmd_out" 2>/dev/null || echo 0)"
  if [[ "$output_bytes" =~ ^[0-9]+$ && "$output_bytes" -gt "$MAX_CMD_BYTES" ]]; then
    echo "warning=command_output_truncated"
  fi
  emit_limited_cmd_output "$cmd_out"
  echo
}

{
  echo "# Hermes Digest Input"
  echo
  echo "report_date=${REPORT_DATE}"
  echo "mode=${MODE}"
  echo "generated_at_utc=$(TZ=UTC date -Is)"
  echo "generated_at_beijing=$(TZ=Asia/Shanghai date -Is)"
  echo "workdir=$(pwd)"
  echo "max_lines=${MAX_LINES}"
  echo "max_file_bytes=${MAX_FILE_BYTES}"
  echo "max_cmd_bytes=${MAX_CMD_BYTES}"
  echo "cmd_timeout_sec=${CMD_TIMEOUT_SEC}"
  echo "source_mode=${SOURCE_MODE}"
  echo "source_date_verified=${SOURCE_DATE_VERIFIED}"
  echo "source_fallback_to_latest=${SOURCE_FALLBACK_TO_LATEST}"
  echo "source_date_warning=$(join_warnings)"
  echo "source_daily_report_json=${DAILY_JSON_SOURCE:-missing}"
  echo "source_daily_report_md=${DAILY_MD_SOURCE:-missing}"
  echo "source_daily_compare_json=${COMPARE_JSON_SOURCE:-missing}"
  echo "source_daily_compare_md=${COMPARE_MD_SOURCE:-missing}"
  echo "source_daily_compare_date_matches=$(join_values "${COMPARE_DATE_MATCHES[@]}")"
  echo "source_declared_dates=$(join_values "${SOURCE_DECLARED_DATES[@]}")"
  echo

  emit_inventory

  run_optional_cmd "Report Source Fast" make report-source-fast

  if [[ "$RUN_DB" == "1" ]]; then
    run_optional_cmd "DB Summary" make db-summary
    run_optional_cmd "Opportunity DB" make opportunity-db
  else
    echo "## DB Summary"
    echo "skipped: fast mode or HERMES_DIGEST_INCLUDE_DB=0"
    echo
    echo "## Opportunity DB"
    echo "skipped: fast mode or HERMES_DIGEST_INCLUDE_DB=0"
    echo
  fi

  if [[ "$RUN_COVERAGE" == "1" ]]; then
    run_optional_cmd "Coverage" make coverage
  else
    echo "## Coverage"
    echo "skipped: fast mode or HERMES_DIGEST_INCLUDE_COVERAGE=0"
    echo
  fi

  emit_file "Daily Report JSON" "$DAILY_JSON_SOURCE" "$DAILY_JSON_ORIGIN" "daily"
  emit_file "Daily Report Markdown" "$DAILY_MD_SOURCE" "$DAILY_MD_ORIGIN" "daily"
  emit_file "Daily Compare JSON" "$COMPARE_JSON_SOURCE" "$COMPARE_JSON_ORIGIN" "compare"
  emit_file "Daily Compare Markdown" "$COMPARE_MD_SOURCE" "$COMPARE_MD_ORIGIN" "compare"
  emit_quality_sample "$QUALITY_SOURCE"
} >"$TMP_OUT"

mv "$TMP_OUT" "$OUT"
cp "$OUT" "$LATEST_TMP"
mv "$LATEST_TMP" "$LATEST_OUT"

echo "$OUT"
exit "$PARTIAL"
