#!/usr/bin/env bash
set -Eeuo pipefail

fail() {
  echo "error: $*" >&2
  exit 1
}

ROUTER="./scripts/hermes_cm_cn_router.py"
WRAPPER="./scripts/hermes_cm_ops.sh"
PYTHON_BIN="${PYTHON_BIN:-./venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

"$ROUTER" --text 'LP抑制抽样预检2026-05-05' --dry-run --platform telegram >"$TMP_DIR/router.out"
grep -Fq '"./scripts/hermes_cm_ops.sh", "lp-suppression-sample-replay"' "$TMP_DIR/router.out" || fail "router did not map LP sample replay to wrapper"
grep -Fq '"--dry-run"' "$TMP_DIR/router.out" || fail "router did not force dry-run"
grep -Fq '"--date", "2026-05-05"' "$TMP_DIR/router.out" || fail "router did not pass absolute date"

set +e
"$ROUTER" --text 'LP抑制抽样预检昨天' --dry-run --platform telegram >"$TMP_DIR/relative.out" 2>"$TMP_DIR/relative.err"
relative_rc=$?
set -e
[[ "$relative_rc" -ne 0 ]] || fail "router accepted relative LP sample replay date"
grep -Fq 'YYYY-MM-DD' "$TMP_DIR/relative.out" "$TMP_DIR/relative.err" || fail "relative date refusal did not mention YYYY-MM-DD"

set +e
HERMES_OPS_PLATFORM=telegram HERMES_OPS_LOCK_PATH="$TMP_DIR/ops.lock" "$WRAPPER" lp-suppression-sample-replay --date 2026-05-05 --execute --confirm >"$TMP_DIR/execute.out" 2>"$TMP_DIR/execute.err"
execute_rc=$?
set -e
[[ "$execute_rc" -ne 0 ]] || fail "Telegram wrapper allowed lp-suppression-sample-replay execute"
grep -Fq 'Telegram lp-suppression-sample-replay only allows --dry-run' "$TMP_DIR/execute.err" || fail "Telegram execute refusal missing"

"$PYTHON_BIN" - "$TMP_DIR/lp_sample.sqlite" <<'PY'
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime


def ts(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp())


db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
conn.execute(
    """
    CREATE TABLE delivery_audit (
        audit_id TEXT PRIMARY KEY,
        signal_id TEXT,
        trade_opportunity_id TEXT,
        asset TEXT,
        opportunity_status TEXT,
        shadow_status TEXT,
        blocked_reason TEXT,
        profile_key TEXT,
        replay_eligible INTEGER,
        delivery_decision TEXT,
        reason TEXT,
        sent_to_telegram INTEGER,
        suppressed INTEGER,
        timestamp REAL,
        stage TEXT,
        gate_reason TEXT,
        final_trading_output_label TEXT,
        opportunity_gate_failure_reason TEXT,
        delivered INTEGER,
        notifier_sent_at REAL,
        suppression_reason TEXT,
        audit_json TEXT,
        archive_written_at REAL,
        created_at REAL
    )
    """
)
conn.execute(
    """
    CREATE TABLE signals (
        signal_id TEXT PRIMARY KEY,
        trade_opportunity_id TEXT,
        asset TEXT,
        pair TEXT,
        timestamp REAL,
        archive_written_at REAL,
        created_at REAL,
        trade_action_key TEXT,
        canonical_semantic_key TEXT,
        lp_alert_stage TEXT,
        signal_json TEXT
    )
    """
)
conn.execute(
    """
    CREATE TABLE market_context_snapshots (
        context_id TEXT PRIMARY KEY,
        asset TEXT,
        pair TEXT,
        venue TEXT,
        perp_mark_price REAL,
        created_at REAL
    )
    """
)
start = ts("2026-05-04T16:00:00+00:00")
for index, action in enumerate(("LONG_BIAS_OBSERVE", "SHORT_BIAS_OBSERVE")):
    signal_id = f"sig-{index}"
    audit_id = f"audit-{index}"
    reason = "gate/lp_noise_filtered" if index == 0 else "listener_prefilter/drop"
    row_ts = start + index * 120
    conn.execute(
        "INSERT INTO delivery_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            audit_id,
            signal_id,
            f"opp-{index}",
            "ETH",
            "BLOCKED",
            "NONE",
            "",
            "",
            0,
            "DROP",
            reason,
            0,
            1,
            row_ts,
            "drop",
            reason,
            "DROP",
            "",
            0,
            None,
            reason,
            json.dumps({"suppression_reason": reason}),
            row_ts,
            row_ts,
        ),
    )
    conn.execute(
        "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            signal_id,
            f"opp-{index}",
            "ETH",
            "ETH/USDT",
            row_ts,
            row_ts,
            row_ts,
            action,
            "pool_buy_pressure" if action.startswith("LONG") else "pool_sell_pressure",
            "confirm",
            json.dumps({"trade_action_key": action}),
        ),
    )
    conn.executemany(
        "INSERT INTO market_context_snapshots VALUES (?,?,?,?,?,?)",
        [
            (f"ctx-entry-{index}", "ETH", "ETH/USDT", "okx_perp", 2000.0, row_ts + 5),
            (f"ctx-exit-{index}", "ETH", "ETH/USDT", "okx_perp", 1990.0, row_ts + 65),
        ],
    )
conn.commit()
conn.close()
PY

SQLITE_DB_PATH="$TMP_DIR/lp_sample.sqlite" "$PYTHON_BIN" app/trade_replay.py --date 2026-05-05 --lp-suppression-sample --dry-run --sample-limit 10 --format json >"$TMP_DIR/sample_dry_run.json"
grep -Fq '"direction_inference_summary"' "$TMP_DIR/sample_dry_run.json" || fail "dry-run missing direction_inference_summary"
grep -Fq '"asset_pair_summary"' "$TMP_DIR/sample_dry_run.json" || fail "dry-run missing asset_pair_summary"
grep -Fq '"listener_prefilter_metadata_rows"' "$TMP_DIR/sample_dry_run.json" || fail "dry-run missing listener_prefilter_metadata_rows"
grep -Fq '"listener_prefilter_recovery_mode"' "$TMP_DIR/sample_dry_run.json" || fail "dry-run missing listener_prefilter_recovery_mode"
"$PYTHON_BIN" - "$TMP_DIR/sample_dry_run.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
valid = int(payload.get("valid_replay_count") or 0)
if valid <= 0:
    raise SystemExit("valid_replay_count not positive")
if int(payload.get("would_insert_valid_replay_rows") or 0) <= 0:
    raise SystemExit("would_insert_valid_replay_rows not positive")
if int(payload.get("would_insert_valid_replay_rows") or 0) != valid:
    raise SystemExit("would_insert_valid_replay_rows does not match valid count")
summary = payload.get("direction_inference_summary") or {}
if int(summary.get("ambiguous") or 0) >= int(payload.get("candidate_sample_count") or 0):
    raise SystemExit("all samples remained ambiguous")
asset_pair = payload.get("asset_pair_summary") or {}
valid_pairs = asset_pair.get("valid_pairs") or {}
if not valid_pairs:
    raise SystemExit("asset_pair_summary valid_pairs missing")
for forbidden in ("USDC", "USDT", "WBTC", "WETH"):
    if forbidden in (payload.get("by_pair") or {}):
        raise SystemExit(f"single asset leaked into by_pair: {forbidden}")
for item in payload.get("by_reason") or []:
    if "direction_source_distribution" not in item:
        raise SystemExit("by_reason missing direction_source_distribution")
PY

echo "OK: LP suppression sample replay router tests passed"
