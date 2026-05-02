#!/usr/bin/env bash
set -Eeuo pipefail

ENV_FILE="${HOME}/.hermes/.env"
CONFIG_FILE="${HOME}/.hermes/config.yaml"
FAILS=0
WARNS=0

pass() {
  echo "PASS: $*"
}

warn() {
  echo "WARN: $*"
  WARNS=$((WARNS + 1))
}

fail() {
  echo "FAIL: $*"
  FAILS=$((FAILS + 1))
}

env_has_key() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] && grep -Eq "^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=" "$ENV_FILE"
}

env_key_is_true() {
  local key="$1"
  [[ -f "$ENV_FILE" ]] && grep -Eiq "^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=[[:space:]]*['\"]?true['\"]?([[:space:]]*(#.*)?)?$" "$ENV_FILE"
}

config_has_pattern() {
  local pattern="$1"
  [[ -f "$CONFIG_FILE" ]] && grep -Eiq "$pattern" "$CONFIG_FILE"
}

echo "Hermes gateway runtime check"
echo "scope: set/missing only; no token, RPC, full user id, or full chat id printed"
echo

if command -v hermes >/dev/null 2>&1; then
  pass "hermes command found"
  if hermes gateway status >/tmp/hermes_gateway_status.out 2>/tmp/hermes_gateway_status.err; then
    pass "hermes gateway status succeeded"
  else
    warn "hermes gateway status failed; try: hermes gateway start"
    warn "system service check: sudo hermes gateway status --system"
  fi
  rm -f /tmp/hermes_gateway_status.out /tmp/hermes_gateway_status.err
else
  warn "hermes command not found; install Hermes or run this on the gateway host"
fi

if [[ -f "$ENV_FILE" ]]; then
  pass "~/.hermes/.env found"

  if env_has_key "TELEGRAM_BOT_TOKEN"; then
    pass "TELEGRAM_BOT_TOKEN is set"
  else
    warn "TELEGRAM_BOT_TOKEN is missing"
  fi

  if env_has_key "TELEGRAM_ALLOWED_USERS" || env_has_key "GATEWAY_ALLOWED_USERS"; then
    pass "allowed users are set"
  else
    fail "TELEGRAM_ALLOWED_USERS or GATEWAY_ALLOWED_USERS must be set"
  fi

  if env_key_is_true "GATEWAY_ALLOW_ALL_USERS"; then
    fail "GATEWAY_ALLOW_ALL_USERS=true is unsafe"
  else
    pass "GATEWAY_ALLOW_ALL_USERS=true not detected"
  fi

  if env_has_key "HERMES_YOLO_MODE"; then
    fail "HERMES_YOLO_MODE is set"
  else
    pass "HERMES_YOLO_MODE is not set"
  fi
else
  warn "~/.hermes/.env not found; Telegram env cannot be checked here"
fi

if [[ -f "$CONFIG_FILE" ]]; then
  pass "~/.hermes/config.yaml found"

  if config_has_pattern '^[[:space:]]*approvals\.mode:[[:space:]]*off([[:space:]]|$)' || \
     config_has_pattern '^[[:space:]]*mode:[[:space:]]*off([[:space:]]|$)'; then
    fail "approvals.mode appears to be off"
  else
    pass "approvals.mode is not off"
  fi

  if config_has_pattern 'command_allowlist|allowed_commands'; then
    if config_has_pattern '(^|[^A-Za-z0-9_-])(rm|systemctl|bash|sh|python|pkill|killall|curl|wget)([^A-Za-z0-9_-]|$)'; then
      warn "command allowlist appears to contain broad or dangerous commands; review before Telegram control"
    else
      pass "no broad dangerous command allowlist entries detected"
    fi
  else
    warn "command allowlist was not detected; confirm gateway approvals are constrained"
  fi

  if config_has_pattern 'telegram\.require_mention:[[:space:]]*true' || \
     config_has_pattern '^[[:space:]]*require_mention:[[:space:]]*true([[:space:]]|$)'; then
    pass "telegram.require_mention is true"
  else
    warn "telegram.require_mention true not detected; groups should enable it"
  fi

  if config_has_pattern 'display\.tool_progress_command:[[:space:]]*false' || \
     config_has_pattern '^[[:space:]]*tool_progress_command:[[:space:]]*false([[:space:]]|$)'; then
    pass "display.tool_progress_command is false"
  else
    warn "display.tool_progress_command false not detected"
  fi
else
  warn "~/.hermes/config.yaml not found; approvals and Telegram display config cannot be checked"
fi

echo
echo "Summary: PASS/WARN/FAIL counts"
echo "PASS: runtime check completed"
echo "WARN: ${WARNS}"
echo "FAIL: ${FAILS}"

if (( FAILS > 0 )); then
  exit 1
fi
exit 0
