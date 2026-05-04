#!/usr/bin/env bash
set -euo pipefail

export TELEGRAM_BOT_TOKEN="123456:unit-test-token"
export CHAT_ID="0"
export TELEGRAM_CONNECTION_POOL_SIZE="16"
export TELEGRAM_POOL_TIMEOUT="10"
export TELEGRAM_SEND_CONCURRENCY="3"

if [ -z "${PYTHON:-}" ] && [ -x "./venv/bin/python" ]; then
    PYTHON="./venv/bin/python"
else
    PYTHON="${PYTHON:-python3}"
fi

"$PYTHON" -m unittest app.test_notifier_telegram_request
