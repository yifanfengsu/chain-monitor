#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/trade_replay_idempotency.XXXXXX")"
DB_PATH="${TMP_DIR}/chain_monitor.sqlite"
REPORT_DATE="2026-05-01"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "error: $*" >&2
  exit 1
}

if [[ -n "${PY:-}" ]]; then
  PY_BIN="$PY"
elif [[ -x "${REPO_ROOT}/venv/bin/python" ]]; then
  PY_BIN="${REPO_ROOT}/venv/bin/python"
else
  PY_BIN="$(command -v python3 || true)"
fi
[[ -n "${PY_BIN:-}" ]] || fail "python runtime not found"

"$PY_BIN" - "$DB_PATH" <<'PY'
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

db_path = Path(sys.argv[1])
start = int(datetime.fromisoformat("2026-04-30T16:00:00+00:00").timestamp())
conn = sqlite3.connect(db_path)
conn.executescript(
    """
    CREATE TABLE signals (
        signal_id TEXT PRIMARY KEY,
        trade_opportunity_id TEXT,
        asset TEXT,
        pair TEXT,
        timestamp REAL,
        archive_written_at REAL,
        created_at REAL,
        direction TEXT,
        trade_action_key TEXT,
        trade_opportunity_status TEXT,
        trade_opportunity_shadow_status TEXT,
        opportunity_profile_key TEXT,
        lp_alert_stage TEXT,
        delivery_decision TEXT,
        sent_to_telegram INTEGER,
        replay_eligible INTEGER,
        signal_json TEXT
    );
    CREATE TABLE market_context_snapshots (
        context_id TEXT PRIMARY KEY,
        asset TEXT,
        pair TEXT,
        venue TEXT,
        perp_mark_price REAL,
        created_at REAL
    );
    """
)
conn.execute(
    "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    (
        "sig-idempotency-1",
        "opp-idempotency-1",
        "ETH",
        "ETH/USDT",
        start,
        start,
        start,
        "LONG",
        "LONG_CANDIDATE",
        "CANDIDATE",
        "NONE",
        "ETH|LONG|idempotency",
        "confirm",
        "LONG_CANDIDATE",
        1,
        1,
        "{}",
    ),
)
conn.executemany(
    "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
    [
        ("ctx-idempotency-entry", "ETH", "ETH/USDT", "okx_perp", 2000.0, start + 5),
        ("ctx-idempotency-exit", "ETH", "ETH/USDT", "okx_perp", 2010.0, start + 65),
    ],
)
conn.commit()
conn.close()
PY

snapshot() {
  "$PY_BIN" - "$DB_PATH" "$REPORT_DATE" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys

db_path, report_date = sys.argv[1], sys.argv[2]
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row


def scalar(sql: str, params: tuple[object, ...] = ()) -> int:
    return int(conn.execute(sql, params).fetchone()[0] or 0)


payload = {
    "examples_count": scalar(
        "SELECT COUNT(*) FROM trade_replay_examples WHERE logical_date=? AND replay_scope='full'",
        (report_date,),
    ),
    "daily_stats_count": scalar(
        "SELECT COUNT(*) FROM trade_replay_profile_daily_stats WHERE logical_date=? AND replay_scope='full'",
        (report_date,),
    ),
    "duplicate_groups": scalar(
        """
        SELECT COUNT(*) FROM (
            SELECT logical_date, replay_scope, signal_id, trade_opportunity_id, COUNT(*) AS row_count
            FROM trade_replay_examples
            WHERE logical_date=? AND replay_scope='full'
            GROUP BY logical_date, replay_scope, signal_id, trade_opportunity_id
            HAVING COUNT(*) > 1
        )
        """,
        (report_date,),
    ),
    "profile_stats_count": scalar(
        "SELECT COUNT(*) FROM trade_replay_profile_stats WHERE profile_key='ETH|LONG|idempotency'",
    ),
}
print(json.dumps(payload, sort_keys=True))
conn.close()
PY
}

run_replay() {
  make -s -C "$REPO_ROOT" PY="$PY_BIN" DB_PATH="$DB_PATH" trade-replay-full DATE="$REPORT_DATE"
}

run_replay >"$TMP_DIR/replay_first.out"
snapshot >"$TMP_DIR/first.json"
run_replay >"$TMP_DIR/replay_second.out"
snapshot >"$TMP_DIR/second.json"

"$PY_BIN" - "$TMP_DIR/first.json" "$TMP_DIR/second.json" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

first = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
second = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))

if first["examples_count"] <= 0:
    raise SystemExit(f"first replay wrote no examples: {first}")
if first["daily_stats_count"] <= 0:
    raise SystemExit(f"first replay wrote no daily stats: {first}")
if second["examples_count"] != first["examples_count"]:
    raise SystemExit(f"examples count changed after second replay: first={first} second={second}")
if second["daily_stats_count"] != first["daily_stats_count"]:
    raise SystemExit(f"daily stats count changed after second replay: first={first} second={second}")
if second["duplicate_groups"] != 0:
    raise SystemExit(f"duplicate logical_date/scope/signal/opportunity groups found: {second}")
if second["profile_stats_count"] != 1:
    raise SystemExit(f"profile stats should remain one row per profile_key: {second}")

print("trade_replay_idempotency=ok")
print(f"examples_count={second['examples_count']}")
print(f"profile_daily_stats_count={second['daily_stats_count']}")
print(f"duplicate_groups={second['duplicate_groups']}")
PY
