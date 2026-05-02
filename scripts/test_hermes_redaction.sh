#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DATE="2026-01-15"
TMP_WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/hermes_redaction_test.XXXXXX")"

cleanup() {
  rm -rf "$TMP_WORKDIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

ADDR="0x1234567890abcdef1234567890abcdef12345678"
TX_HASH="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TELEGRAM_TOKEN="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
RPC_URL="https://eth-mainnet.g.alchemy.com/v2/exampleSecret"
PRIVATE_PATH="/root/private/chain-monitor/.env"
SECRET_LINE="API_KEY=super-secret-value"

mkdir -p \
  "$TMP_WORKDIR/reports/daily" \
  "$TMP_WORKDIR/reports/daily_compare" \
  "$TMP_WORKDIR/reports/hermes"

cat >"$TMP_WORKDIR/Makefile" <<MAKEFILE
.PHONY: report-source-fast
report-source-fast:
	@printf '%s\n' 'report_source_addr=${ADDR}'
	@printf '%s\n' 'report_source_tx=${TX_HASH}'
	@printf '%s\n' 'telegram=${TELEGRAM_TOKEN}'
	@printf '%s\n' 'rpc=${RPC_URL}'
	@printf '%s\n' 'path=${PRIVATE_PATH}'
	@printf '%s\n' '${SECRET_LINE}'
MAKEFILE

cat >"$TMP_WORKDIR/reports/daily/daily_report_${REPORT_DATE}.json" <<JSON
{
  "logical_date": "${REPORT_DATE}",
  "address": "${ADDR}",
  "tx": "${TX_HASH}",
  "telegram": "${TELEGRAM_TOKEN}",
  "rpc": "${RPC_URL}",
  "path": "${PRIVATE_PATH}",
  "secret": "${SECRET_LINE}"
}
JSON

cat >"$TMP_WORKDIR/reports/daily/daily_report_${REPORT_DATE}.md" <<MARKDOWN
logical_date=${REPORT_DATE}
address=${ADDR}
tx=${TX_HASH}
telegram=${TELEGRAM_TOKEN}
rpc=${RPC_URL}
path=${PRIVATE_PATH}
${SECRET_LINE}
MARKDOWN

cat >"$TMP_WORKDIR/reports/daily_compare/daily_compare_${REPORT_DATE}.json" <<JSON
{
  "today_date": "${REPORT_DATE}",
  "address": "${ADDR}",
  "tx": "${TX_HASH}"
}
JSON

cat >"$TMP_WORKDIR/reports/daily_compare/daily_compare_${REPORT_DATE}.md" <<MARKDOWN
today_date=${REPORT_DATE}
address=${ADDR}
tx=${TX_HASH}
MARKDOWN

cat >"$TMP_WORKDIR/reports/quality_rows.csv" <<CSV
logical_date,address,tx,secret
${REPORT_DATE},${ADDR},${TX_HASH},${SECRET_LINE}
CSV

HERMES_DIGEST_WORKDIR="$TMP_WORKDIR" \
HERMES_DIGEST_REDACT=1 \
HERMES_DIGEST_MAX_LINES=500 \
HERMES_DIGEST_MAX_FILE_BYTES=200000 \
HERMES_DIGEST_MAX_CMD_BYTES=100000 \
  bash "$REPO_ROOT/scripts/hermes_daily_digest_input.sh" --date "$REPORT_DATE" --mode fast >/dev/null

DIGEST="$TMP_WORKDIR/reports/hermes/hermes_digest_input_${REPORT_DATE}.md"
[[ -f "$DIGEST" ]] || fail "digest was not generated"

for raw_label in address tx_hash telegram_token rpc_url private_path secret_value; do
  case "$raw_label" in
    address) raw="$ADDR" ;;
    tx_hash) raw="$TX_HASH" ;;
    telegram_token) raw="$TELEGRAM_TOKEN" ;;
    rpc_url) raw="$RPC_URL" ;;
    private_path) raw="$PRIVATE_PATH" ;;
    secret_value) raw="$SECRET_LINE" ;;
    *) fail "unknown raw label ${raw_label}" ;;
  esac

  if grep -Fq "$raw" "$DIGEST"; then
    fail "raw ${raw_label} was not redacted"
  fi
done

for redacted in \
  "0xADDR_REDACTED" \
  "0xTX_REDACTED" \
  "[REDACTED_TELEGRAM_TOKEN]" \
  "[REDACTED_RPC_URL]" \
  "[PRIVATE_PATH]" \
  "[REDACTED_SECRET]"
do
  if ! grep -Fq "$redacted" "$DIGEST"; then
    fail "missing redaction marker: ${redacted}"
  fi
done

grep -Fq "redaction_enabled=true" "$DIGEST" || fail "missing redaction_enabled=true header"
grep -Fq "report_date=${REPORT_DATE}" "$DIGEST" || fail "report_date header changed"
grep -Fq "mode=fast" "$DIGEST" || fail "mode header changed"

echo "OK: Hermes redaction test passed"
