#!/usr/bin/env python3
"""Trade Replay — evaluate historical signals as if traded with fixed rules.

CLI:
  python -m app.trade_replay --date YYYY-MM-DD
  python -m app.trade_replay --date YYYY-MM-DD --format json
  python -m app.trade_replay --date YYYY-MM-DD --dry-run
  python -m app.trade_replay --date YYYY-MM-DD --include-suppressed
  python -m app.trade_replay --date YYYY-MM-DD --include-blocked
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from config import (
    REPLAY_ABSORPTION_ADVERSE_BPS,
    REPLAY_BAD_ENTRY_MAE_BPS,
    REPLAY_CHOP_MAX_ABS_PNL_BPS,
    REPLAY_CLEAN_MIN_PNL_BPS,
    REPLAY_ENABLE,
    REPLAY_ENTRY_DELAY_SEC,
    REPLAY_FEE_BPS,
    REPLAY_MAX_HOLD_SEC,
    REPLAY_MIN_PRICE_POINTS,
    REPLAY_SLIPPAGE_BPS,
    REPLAY_STOP_LOSS_BPS,
    REPLAY_TAKE_PROFIT_BPS,
    SQLITE_WRITE_REPLAY_EXAMPLES,
    SQLITE_WRITE_REPLAY_PROFILES,
)

REPLAY_LABELS = {
    "clean_followthrough": "clean_followthrough",
    "followthrough_but_bad_entry": "followthrough_but_bad_entry",
    "absorption_reversal": "absorption_reversal",
    "chop_no_edge": "chop_no_edge",
    "data_invalid": "data_invalid",
}

CLOSE_REASONS = {
    "take_profit": "take_profit",
    "stop_loss": "stop_loss",
    "max_hold": "max_hold",
    "data_invalid": "data_invalid",
}

PRICE_SOURCE_PRIORITY = [
    "okx_mark",
    "okx_index",
    "okx_last",
    "market_context_snapshot",
    "outcome_price",
    "pool_quote_proxy",
]


def _text(*values: Any) -> str:
    for value in values:
        if value in (None, "", [], {}, ()):
            continue
        return str(value)
    return ""


def _float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return float(default)


def _int(*values: Any, default: int = 0) -> int:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
    return int(default)


def _stable_replay_id(
    signal_id: str,
    logical_date: str,
    entry_ts: int,
    rules: dict[str, Any],
) -> str:
    payload = f"{signal_id}|{logical_date}|{entry_ts}|{json.dumps(rules, sort_keys=True)}"
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def get_price_at_ts(
    asset: str,
    pair: str,
    ts: int,
    prefer_source: str = "",
) -> float | None:
    """Look up price nearest to `ts` from SQLite market_context_snapshots or outcomes."""
    source = prefer_source or "okx_mark"
    try:
        import sqlite_store

        conn = sqlite_store.get_connection()
        if conn is None:
            return None

        # Try market_context_snapshots first
        cur = conn.execute(
            """SELECT mark_price, index_price, last_price, source_venue
               FROM market_context_snapshots
               WHERE asset = ? AND abs(ts - ?) <= 30
               ORDER BY abs(ts - ?) ASC LIMIT 1""",
            (asset.upper(), ts, ts),
        )
        row = cur.fetchone()
        if row:
            mark, index_p, last_p, venue = row
            if source in ("okx_mark", "any") and mark is not None:
                return float(mark)
            if source in ("okx_index", "any") and index_p is not None:
                return float(index_p)
            if source in ("okx_last", "any") and last_p is not None:
                return float(last_p)
            return float(mark or index_p or last_p or 0)

        # Fallback to outcomes price
        cur = conn.execute(
            """SELECT entry_price, exit_price FROM outcomes
               WHERE signal_id LIKE ?
               ORDER BY outcome_ts DESC LIMIT 1""",
            (f"%{signal_id_prefix(asset)}%",),
        )
        row = cur.fetchone()
        if row:
            entry, exit_p = row
            return float(entry or exit_p or 0)

        return None
    except Exception:
        return None


def signal_id_prefix(asset: str) -> str:
    return asset.upper()[:4]


def compute_replay_pnl(
    entry_price: float,
    exit_price: float,
    side: str,
    fee_bps: float = REPLAY_FEE_BPS,
    slippage_bps: float = REPLAY_SLIPPAGE_BPS,
) -> dict[str, Any]:
    """Compute gross and net PnL in bps."""
    if entry_price <= 0 or exit_price <= 0:
        return {
            "gross_pnl_bps": 0.0,
            "net_pnl_bps": 0.0,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "fee_bps": fee_bps,
            "slippage_bps": slippage_bps,
        }

    side_norm = str(side or "").upper()
    if side_norm == "LONG":
        gross_bps = ((exit_price - entry_price) / entry_price) * 10000
    elif side_norm == "SHORT":
        gross_bps = ((entry_price - exit_price) / entry_price) * 10000
    else:
        gross_bps = 0.0

    total_cost_bps = float(fee_bps) + float(slippage_bps)
    net_bps = round(gross_bps - total_cost_bps, 2)

    return {
        "gross_pnl_bps": round(gross_bps, 2),
        "net_pnl_bps": net_bps,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
    }


def replay_single_signal(
    signal_row: dict[str, Any],
    rules: dict[str, Any],
    price_source_fn=None,
) -> dict[str, Any]:
    """Simulate a single signal as a trade.

    Returns dict with replay_id, label, pnl, etc.
    """
    entry_delay = int(rules.get("entry_delay_sec", REPLAY_ENTRY_DELAY_SEC))
    max_hold = int(rules.get("max_hold_sec", REPLAY_MAX_HOLD_SEC))
    stop_loss = int(rules.get("stop_loss_bps", REPLAY_STOP_LOSS_BPS))
    take_profit = int(rules.get("take_profit_bps", REPLAY_TAKE_PROFIT_BPS))
    fee_bps = int(rules.get("fee_bps", REPLAY_FEE_BPS))
    slippage_bps = int(rules.get("slippage_bps", REPLAY_SLIPPAGE_BPS))
    allow_proxy = bool(rules.get("allow_pool_proxy", False))

    signal_id = str(signal_row.get("signal_id") or "")
    asset = str(signal_row.get("asset") or signal_row.get("asset_symbol") or "").upper()
    pair = str(signal_row.get("pair") or signal_row.get("pair_label") or "")
    side = str(signal_row.get("side") or signal_row.get("direction") or "").upper()
    signal_ts = int(signal_row.get("signal_ts") or signal_row.get("ts") or signal_row.get("event_ts") or 0)
    logical_date = str(signal_row.get("logical_date") or "")

    entry_ts = signal_ts + entry_delay

    if price_source_fn is None:
        price_source_fn = get_price_at_ts

    # Get entry price
    entry_price = None
    price_source = ""
    for src in PRICE_SOURCE_PRIORITY:
        if src == "pool_quote_proxy" and not allow_proxy:
            continue
        entry_price = price_source_fn(asset, pair, entry_ts, prefer_source=src)
        if entry_price is not None and entry_price > 0:
            price_source = src
            break

    if entry_price is None or entry_price <= 0:
        return {
            "signal_id": signal_id,
            "asset": asset,
            "pair": pair,
            "side": side,
            "signal_ts": signal_ts,
            "entry_ts": entry_ts,
            "entry_price": 0,
            "exit_ts": 0,
            "exit_price": 0,
            "gross_pnl_bps": 0,
            "net_pnl_bps": 0,
            "mfe_bps": 0,
            "mae_bps": 0,
            "label": "data_invalid",
            "close_reason": "data_invalid",
            "price_source": "",
            "data_valid": False,
            "invalid_reason": "no_entry_price",
            "entry_delay_sec": entry_delay,
            "max_hold_sec": max_hold,
            "stop_loss_bps": stop_loss,
            "take_profit_bps": take_profit,
            "fee_bps": fee_bps,
            "slippage_bps": slippage_bps,
        }

    # Simulate forward: check stop_loss, take_profit, or max_hold
    exit_ts = entry_ts
    exit_price = entry_price
    mfe_bps = 0.0
    mae_bps = 0.0
    close_reason = "max_hold"

    check_interval = max(1, min(5, max_hold // 12))  # Check every 1-5 seconds
    side_norm = "LONG" if side == "LONG" else "SHORT"

    for offset in range(check_interval, max_hold + check_interval, check_interval):
        check_ts = min(signal_ts + entry_delay + offset, signal_ts + entry_delay + max_hold)
        price = price_source_fn(asset, pair, check_ts, prefer_source=price_source)
        if price is None or price <= 0:
            # Try fallback sources
            for fb_src in PRICE_SOURCE_PRIORITY:
                if fb_src == price_source:
                    continue
                if fb_src == "pool_quote_proxy" and not allow_proxy:
                    continue
                price = price_source_fn(asset, pair, check_ts, prefer_source=fb_src)
                if price is not None and price > 0:
                    break
        if price is None or price <= 0:
            continue

        if side_norm == "LONG":
            move_bps = ((price - entry_price) / entry_price) * 10000
        else:
            move_bps = ((entry_price - price) / entry_price) * 10000

        mfe_bps = max(mfe_bps, move_bps)
        mae_bps = min(mae_bps, move_bps)

        # Check take profit
        if move_bps >= take_profit:
            exit_ts = check_ts
            exit_price = price
            close_reason = "take_profit"
            break

        # Check stop loss
        if move_bps <= -stop_loss:
            exit_ts = check_ts
            exit_price = price
            close_reason = "stop_loss"
            break

        exit_ts = check_ts
        exit_price = price
        close_reason = "max_hold"

    pnl = compute_replay_pnl(entry_price, exit_price, side, fee_bps, slippage_bps)

    result = {
        "signal_id": signal_id,
        "asset": asset,
        "pair": pair,
        "side": side,
        "signal_ts": signal_ts,
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "gross_pnl_bps": pnl["gross_pnl_bps"],
        "net_pnl_bps": pnl["net_pnl_bps"],
        "mfe_bps": round(mfe_bps, 2),
        "mae_bps": round(mae_bps, 2),
        "label": classify_replay_label(pnl["net_pnl_bps"], mfe_bps, mae_bps),
        "close_reason": close_reason,
        "price_source": price_source,
        "data_valid": True,
        "invalid_reason": "",
        "entry_delay_sec": entry_delay,
        "max_hold_sec": max_hold,
        "stop_loss_bps": stop_loss,
        "take_profit_bps": take_profit,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
    }

    replay_id = _stable_replay_id(signal_id, logical_date, entry_ts, rules)
    result["replay_id"] = replay_id

    return result


def classify_replay_label(
    net_pnl_bps: float,
    mfe_bps: float,
    mae_bps: float,
) -> str:
    """Classify replay result into one of five labels."""
    clean_min = float(REPLAY_CLEAN_MIN_PNL_BPS)
    bad_entry_mae = float(REPLAY_BAD_ENTRY_MAE_BPS)
    absorption_adv = float(REPLAY_ABSORPTION_ADVERSE_BPS)
    chop_max = float(REPLAY_CHOP_MAX_ABS_PNL_BPS)

    if net_pnl_bps >= clean_min and abs(mae_bps) < bad_entry_mae and mfe_bps >= clean_min:
        return "clean_followthrough"

    if abs(mae_bps) >= bad_entry_mae and net_pnl_bps >= 0:
        return "followthrough_but_bad_entry"

    if mae_bps <= -absorption_adv:
        return "absorption_reversal"

    if abs(net_pnl_bps) <= chop_max and abs(mfe_bps) <= chop_max * 2 and abs(mae_bps) <= chop_max * 2:
        return "chop_no_edge"

    # Default: check which direction dominates
    if net_pnl_bps > 0:
        return "clean_followthrough" if abs(mae_bps) < bad_entry_mae else "followthrough_but_bad_entry"
    else:
        return "absorption_reversal" if mae_bps <= -absorption_adv else "chop_no_edge"


def _get_signals_for_date(
    date_str: str,
    include_suppressed: bool = False,
    include_blocked: bool = False,
) -> list[dict[str, Any]]:
    """Fetch signals for a given logical date from SQLite."""
    try:
        import sqlite_store

        conn = sqlite_store.get_connection()
        if conn is None:
            return []

        # Get signals for this date window
        # Compute date range from logical_date string (Beijing time: UTC+8)
        from datetime import datetime, timezone, timedelta
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        bj_tz = timezone(timedelta(hours=8))
        start_utc = dt.replace(tzinfo=bj_tz).timestamp()
        end_utc = (dt.replace(hour=23, minute=59, second=59, tzinfo=bj_tz)).timestamp()

        cur = conn.execute(
            """SELECT s.signal_id, s.signal_json, s.timestamp as ts,
                      COALESCE(o.status, 'NONE') as opportunity_status,
                      COALESCE(o.trade_opportunity_id, '') as trade_opportunity_id,
                      COALESCE(o.blockers_json, '{}') as blockers_json,
                      COALESCE(o.shadow_status, 'NONE') as shadow_status,
                      COALESCE(da.delivery_decision, '') as delivery_decision,
                      da.audit_id as delivery_audit_id
               FROM signals s
               LEFT JOIN trade_opportunities o ON s.signal_id = o.signal_id
               LEFT JOIN delivery_audit da ON s.signal_id = da.signal_id
               WHERE s.timestamp >= ? AND s.timestamp <= ?
               ORDER BY s.timestamp ASC""",
            (start_utc, end_utc),
        )
        rows = cur.fetchall()

        signals = []
        for row in rows:
            try:
                signal_json = json.loads(row[1] or "{}")
            except (json.JSONDecodeError, TypeError):
                signal_json = {}

            opportunity_status = str(row[3] or "NONE")
            delivery_decision = str(row[7] or "")

            # Filter based on include flags
            if not include_suppressed and "suppress" in delivery_decision.lower():
                continue
            if not include_blocked and opportunity_status == "BLOCKED":
                continue

            asset = signal_json.get("asset_symbol") or signal_json.get("asset_case_label") or ""
            side = _determine_side(signal_json)
            pair = signal_json.get("pair_label", "")

            signals.append({
                "signal_id": str(row[0] or ""),
                "asset": asset,
                "pair": pair,
                "side": side,
                "signal_ts": int(row[2] or 0),
                "opportunity_status": opportunity_status,
                "trade_opportunity_id": str(row[4] or ""),
                "blockers_json": str(row[5] or "{}"),
                "shadow_status": str(row[6] or "NONE"),
                "delivery_decision": delivery_decision,
                "delivery_audit_id": str(row[8] or ""),
                "logical_date": date_str,
                "lp_stage": signal_json.get("lp_alert_stage", ""),
                "sweep_phase": signal_json.get("lp_sweep_phase", ""),
                "profile_key": _build_profile_key(signal_json, opportunity_status, str(row[6] or "NONE")),
                "features_json": json.dumps(signal_json, default=str),
            })
        return signals
    except Exception:
        return []


def _determine_side(signal_json: dict) -> str:
    direction = str(signal_json.get("direction") or signal_json.get("trade_action_direction") or "")
    if direction.lower() in ("long", "buy_pressure", "buy"):
        return "LONG"
    if direction.lower() in ("short", "sell_pressure", "sell"):
        return "SHORT"
    intent = str(signal_json.get("intent_type") or "")
    if "buy" in intent.lower():
        return "LONG"
    if "sell" in intent.lower():
        return "SHORT"
    return "NONE"


def _build_profile_key(
    signal_json: dict,
    opportunity_status: str,
    shadow_status: str,
) -> str:
    asset = str(signal_json.get("asset_symbol") or signal_json.get("asset_case_label") or "UNKNOWN").upper()
    side = _determine_side(signal_json)
    lp_stage = str(signal_json.get("lp_alert_stage") or "unknown")
    sweep = str(signal_json.get("lp_sweep_phase") or "unknown")
    broader = "true" if str(signal_json.get("lp_confirm_scope") or "") == "broader_confirm" else "false"
    absorption = str(signal_json.get("lp_absorption_context") or "unknown")
    mc_source = str(signal_json.get("market_context_source") or "unavailable")
    basis = _basis_bucket(float(signal_json.get("basis_bps") or 0))
    status = shadow_status if shadow_status != "NONE" else opportunity_status
    blocker = _primary_blocker_family(signal_json)

    return f"{asset}|{side}|{lp_stage}|{sweep}|broader_{broader}|abs_{absorption}|{mc_source}|{basis}|{status}|{blocker}"


def _basis_bucket(basis_bps: float) -> str:
    if basis_bps <= -30:
        return "basis_deep_short"
    if basis_bps <= -10:
        return "basis_short"
    if basis_bps <= 10:
        return "basis_normal"
    if basis_bps <= 30:
        return "basis_long"
    return "basis_deep_long"


def _primary_blocker_family(signal_json: dict) -> str:
    blockers = signal_json.get("blockers", signal_json.get("blocker_type", ""))
    if not blockers:
        return "none"
    blockers_str = str(blockers)
    if "no_trade_lock" in blockers_str:
        return "no_trade_lock"
    if "adverse" in blockers_str:
        return "adverse"
    if "late_or_chase" in blockers_str:
        return "late_or_chase"
    if "low_quality" in blockers_str:
        return "low_quality"
    if "absorption" in blockers_str:
        return "absorption"
    if "sample" in blockers_str or "history" in blockers_str:
        return "insufficient_data"
    return "other"


def _write_replay_to_sqlite(replay_result: dict, rules: dict) -> bool:
    """Write a single replay result to SQLite."""
    if not bool(SQLITE_WRITE_REPLAY_EXAMPLES):
        return False
    try:
        import sqlite_store

        conn = sqlite_store.get_connection()
        if conn is None:
            return False

        conn.execute(
            """INSERT OR REPLACE INTO trade_replay_examples
               (replay_id, logical_date, signal_id, trade_opportunity_id,
                delivery_audit_id, asset, pair, side, opportunity_status,
                shadow_status, signal_stage, lp_stage, sweep_phase,
                profile_key, signal_ts, entry_ts, exit_ts,
                entry_delay_sec, max_hold_sec, entry_price, exit_price,
                stop_loss_bps, take_profit_bps, fee_bps, slippage_bps,
                gross_pnl_bps, net_pnl_bps, mfe_bps, mae_bps,
                label, close_reason, price_source, data_valid,
                invalid_reason, blockers_json, features_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                replay_result.get("replay_id", ""),
                replay_result.get("logical_date", ""),
                replay_result.get("signal_id", ""),
                replay_result.get("trade_opportunity_id", ""),
                replay_result.get("delivery_audit_id", ""),
                replay_result.get("asset", ""),
                replay_result.get("pair", ""),
                replay_result.get("side", ""),
                replay_result.get("opportunity_status", "NONE"),
                replay_result.get("shadow_status", "NONE"),
                replay_result.get("signal_stage", ""),
                replay_result.get("lp_stage", ""),
                replay_result.get("sweep_phase", ""),
                replay_result.get("profile_key", ""),
                int(replay_result.get("signal_ts", 0)),
                int(replay_result.get("entry_ts", 0)),
                int(replay_result.get("exit_ts", 0)),
                int(replay_result.get("entry_delay_sec", rules.get("entry_delay_sec", 5))),
                int(replay_result.get("max_hold_sec", rules.get("max_hold_sec", 60))),
                float(replay_result.get("entry_price", 0)),
                float(replay_result.get("exit_price", 0)),
                int(rules.get("stop_loss_bps", 30)),
                int(rules.get("take_profit_bps", 50)),
                int(rules.get("fee_bps", 6)),
                int(rules.get("slippage_bps", 5)),
                float(replay_result.get("gross_pnl_bps", 0)),
                float(replay_result.get("net_pnl_bps", 0)),
                float(replay_result.get("mfe_bps", 0)),
                float(replay_result.get("mae_bps", 0)),
                replay_result.get("label", ""),
                replay_result.get("close_reason", ""),
                replay_result.get("price_source", ""),
                1 if replay_result.get("data_valid") else 0,
                replay_result.get("invalid_reason", ""),
                replay_result.get("blockers_json", "{}"),
                replay_result.get("features_json", "{}"),
            ),
        )
        conn.commit()
        return True
    except Exception:
        return False


def _update_profile_stats(replay_results: list[dict]) -> dict[str, Any]:
    """Aggregate profile-level statistics from replay results."""
    profiles: dict[str, list[dict]] = defaultdict(list)
    for r in replay_results:
        pk = r.get("profile_key", "")
        if pk:
            profiles[pk].append(r)

    stats = {}
    for pk, results in profiles.items():
        valid = [r for r in results if r.get("data_valid")]
        sample_count = len(results)
        valid_count = len(valid)

        if valid_count == 0:
            stats[pk] = {
                "profile_key": pk,
                "sample_count": sample_count,
                "valid_sample_count": 0,
                "win_rate": 0.0,
                "avg_net_pnl_bps": 0.0,
                "median_net_pnl_bps": 0.0,
                "clean_followthrough_rate": 0.0,
                "bad_entry_rate": 0.0,
                "absorption_reversal_rate": 0.0,
                "chop_rate": 0.0,
                "data_invalid_rate": 1.0 if sample_count > 0 else 0.0,
                "avg_mfe_bps": 0.0,
                "avg_mae_bps": 0.0,
                "recommended_action": "needs_more_samples",
                "confidence_level": "low",
            }
            continue

        wins = [r for r in valid if r.get("net_pnl_bps", 0) > 0]
        pnls = sorted([r.get("net_pnl_bps", 0) for r in valid])
        median_pnl = pnls[len(pnls) // 2] if pnls else 0.0

        labels = [r.get("label", "") for r in valid]
        total = len(valid)

        asset = valid[0].get("asset", "")
        side = valid[0].get("side", "")

        stats[pk] = {
            "profile_key": pk,
            "asset": asset,
            "side": side,
            "sample_count": sample_count,
            "valid_sample_count": valid_count,
            "win_rate": round(len(wins) / total, 4) if total > 0 else 0.0,
            "avg_net_pnl_bps": round(sum(r.get("net_pnl_bps", 0) for r in valid) / total, 2),
            "median_net_pnl_bps": round(median_pnl, 2),
            "clean_followthrough_rate": round(labels.count("clean_followthrough") / total, 4),
            "bad_entry_rate": round(labels.count("followthrough_but_bad_entry") / total, 4),
            "absorption_reversal_rate": round(labels.count("absorption_reversal") / total, 4),
            "chop_rate": round(labels.count("chop_no_edge") / total, 4),
            "data_invalid_rate": round((sample_count - valid_count) / max(sample_count, 1), 4),
            "avg_mfe_bps": round(sum(r.get("mfe_bps", 0) for r in valid) / total, 2),
            "avg_mae_bps": round(sum(r.get("mae_bps", 0) for r in valid) / total, 2),
            "recommended_action": _recommend_profile_action(stats[pk], valid),
            "confidence_level": _confidence_level(valid_count),
        }
    return stats


def _recommend_profile_action(stat: dict, results: list[dict]) -> str:
    """Determine recommended action for a profile."""
    from config import (
        MIN_PROFILE_REPLAY_SAMPLES,
        PROFILE_HARD_BLOCK_ABSORPTION_RATE,
        PROFILE_HARD_BLOCK_AVG_PNL_BPS,
        PROFILE_PROMOTE_AVG_PNL_BPS,
        PROFILE_PROMOTE_WIN_RATE,
    )

    samples = stat.get("valid_sample_count", 0)
    if samples < int(MIN_PROFILE_REPLAY_SAMPLES):
        return "needs_more_samples"

    avg_pnl = stat.get("avg_net_pnl_bps", 0)
    absorption = stat.get("absorption_reversal_rate", 0)
    win_rate = stat.get("win_rate", 0)

    if absorption >= float(PROFILE_HARD_BLOCK_ABSORPTION_RATE) and avg_pnl <= float(PROFILE_HARD_BLOCK_AVG_PNL_BPS):
        return "hard_block"
    if avg_pnl <= -5:
        return "hard_block"
    if abs(avg_pnl) < 3 and stat.get("chop_rate", 0) > 0.4:
        return "keep_observe_only"
    if win_rate >= float(PROFILE_PROMOTE_WIN_RATE) and avg_pnl >= float(PROFILE_PROMOTE_AVG_PNL_BPS):
        return "eligible_verified"
    if avg_pnl > 0 and absorption < 0.25:
        return "promote_candidate"
    return "keep_observe_only"


def _confidence_level(valid_count: int) -> str:
    if valid_count >= 30:
        return "high"
    if valid_count >= 15:
        return "medium"
    return "low"


def _write_profile_stats_to_sqlite(stats: dict[str, Any]) -> int:
    """Write profile stats to SQLite. Returns count of profiles written."""
    if not bool(SQLITE_WRITE_REPLAY_PROFILES):
        return 0
    written = 0
    try:
        import sqlite_store

        conn = sqlite_store.get_connection()
        if conn is None:
            return 0

        for pk, stat in stats.items():
            conn.execute(
                """INSERT OR REPLACE INTO trade_replay_profile_stats
                   (profile_key, asset, side, sample_count, valid_sample_count,
                    win_rate, avg_net_pnl_bps, median_net_pnl_bps,
                    clean_followthrough_rate, bad_entry_rate,
                    absorption_reversal_rate, chop_rate, data_invalid_rate,
                    avg_mfe_bps, avg_mae_bps, recommended_action, confidence_level)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    pk,
                    stat.get("asset", ""),
                    stat.get("side", ""),
                    stat.get("sample_count", 0),
                    stat.get("valid_sample_count", 0),
                    stat.get("win_rate", 0.0),
                    stat.get("avg_net_pnl_bps", 0.0),
                    stat.get("median_net_pnl_bps", 0.0),
                    stat.get("clean_followthrough_rate", 0.0),
                    stat.get("bad_entry_rate", 0.0),
                    stat.get("absorption_reversal_rate", 0.0),
                    stat.get("chop_rate", 0.0),
                    stat.get("data_invalid_rate", 0.0),
                    stat.get("avg_mfe_bps", 0.0),
                    stat.get("avg_mae_bps", 0.0),
                    stat.get("recommended_action", ""),
                    stat.get("confidence_level", ""),
                ),
            )
            written += 1
        conn.commit()
        return written
    except Exception:
        return written


def run_trade_replay(
    date_str: str,
    *,
    rules: dict[str, Any] | None = None,
    dry_run: bool = False,
    include_suppressed: bool = False,
    include_blocked: bool = False,
    output_format: str = "text",
) -> dict[str, Any]:
    """Run trade replay for a given date. Returns summary dict."""
    if not bool(REPLAY_ENABLE):
        return {"status": "disabled", "date": date_str, "results": [], "summary": {}}

    if rules is None:
        rules = {
            "entry_delay_sec": REPLAY_ENTRY_DELAY_SEC,
            "max_hold_sec": REPLAY_MAX_HOLD_SEC,
            "stop_loss_bps": REPLAY_STOP_LOSS_BPS,
            "take_profit_bps": REPLAY_TAKE_PROFIT_BPS,
            "fee_bps": REPLAY_FEE_BPS,
            "slippage_bps": REPLAY_SLIPPAGE_BPS,
        }

    signals = _get_signals_for_date(date_str, include_suppressed, include_blocked)

    if not signals:
        return {
            "status": "no_signals",
            "date": date_str,
            "results": [],
            "summary": {"signal_count": 0, "replay_count": 0},
        }

    results = []
    for sig in signals:
        result = replay_single_signal(sig, rules)
        result["logical_date"] = date_str
        result["trade_opportunity_id"] = sig.get("trade_opportunity_id", "")
        result["delivery_audit_id"] = sig.get("delivery_audit_id", "")
        result["opportunity_status"] = sig.get("opportunity_status", "NONE")
        result["shadow_status"] = sig.get("shadow_status", "NONE")
        result["lp_stage"] = sig.get("lp_stage", "")
        result["sweep_phase"] = sig.get("sweep_phase", "")
        result["profile_key"] = sig.get("profile_key", "")
        result["blockers_json"] = sig.get("blockers_json", "{}")
        result["features_json"] = sig.get("features_json", "{}")

        if not dry_run:
            _write_replay_to_sqlite(result, rules)

        results.append(result)

    # Compute profile stats
    profile_stats = _update_profile_stats(results)
    if not dry_run:
        _write_profile_stats_to_sqlite(profile_stats)

    # Summary
    valid_results = [r for r in results if r.get("data_valid")]
    summary = {
        "date": date_str,
        "signal_count": len(signals),
        "replay_count": len(results),
        "valid_count": len(valid_results),
        "data_invalid_count": len(results) - len(valid_results),
        "win_rate": round(
            len([r for r in valid_results if r.get("net_pnl_bps", 0) > 0]) / max(len(valid_results), 1),
            4,
        ),
        "avg_net_pnl_bps": round(
            sum(r.get("net_pnl_bps", 0) for r in valid_results) / max(len(valid_results), 1),
            2,
        ),
        "clean_followthrough_count": len([r for r in valid_results if r.get("label") == "clean_followthrough"]),
        "bad_entry_count": len([r for r in valid_results if r.get("label") == "followthrough_but_bad_entry"]),
        "absorption_reversal_count": len([r for r in valid_results if r.get("label") == "absorption_reversal"]),
        "chop_count": len([r for r in valid_results if r.get("label") == "chop_no_edge"]),
        "profile_count": len(profile_stats),
        "top_positive_profiles": _top_profiles(profile_stats, "avg_net_pnl_bps", reverse=True, limit=5),
        "top_negative_profiles": _top_profiles(profile_stats, "avg_net_pnl_bps", reverse=False, limit=5),
        "rules": rules,
    }

    # Suppressed/blocked specific metrics
    if include_suppressed:
        suppressed = [r for r in results if "suppress" in str(r.get("delivery_decision", "") or "").lower()]
        suppressed_valid = [r for r in suppressed if r.get("data_valid")]
        summary["suppressed_replay_count"] = len(suppressed)
        summary["suppressed_profitable_rate"] = round(
            len([r for r in suppressed_valid if r.get("net_pnl_bps", 0) > 0]) / max(len(suppressed_valid), 1),
            4,
        )
        summary["suppressed_clean_followthrough_rate"] = round(
            len([r for r in suppressed_valid if r.get("label") == "clean_followthrough"]) / max(len(suppressed_valid), 1),
            4,
        )

    if include_blocked:
        blocked = [r for r in results if r.get("opportunity_status") == "BLOCKED"]
        blocked_valid = [r for r in blocked if r.get("data_valid")]
        profitable_blocked = len([r for r in blocked_valid if r.get("net_pnl_bps", 0) > 0])
        adverse_blocked = len([r for r in blocked_valid if r.get("label") == "absorption_reversal"])
        summary["blocked_replay_count"] = len(blocked)
        summary["blocked_saved_rate_estimate"] = round(
            adverse_blocked / max(len(blocked_valid), 1),
            4,
        )
        summary["blocked_false_block_rate_estimate"] = round(
            profitable_blocked / max(len(blocked_valid), 1),
            4,
        )

    return {
        "status": "ok",
        "date": date_str,
        "results": results,
        "summary": summary,
        "profile_stats": profile_stats,
    }


def _top_profiles(
    stats: dict,
    sort_key: str,
    reverse: bool = True,
    limit: int = 5,
) -> list[dict]:
    items = sorted(
        stats.values(),
        key=lambda x: x.get(sort_key, 0),
        reverse=reverse,
    )
    top = []
    for item in items[:limit]:
        top.append({
            "profile_key": item.get("profile_key", ""),
            "sample_count": item.get("valid_sample_count", 0),
            "avg_net_pnl_bps": item.get("avg_net_pnl_bps", 0),
            "win_rate": item.get("win_rate", 0),
            "recommended_action": item.get("recommended_action", ""),
        })
    return top


def _format_summary_text(summary: dict, profile_stats: dict) -> str:
    lines = []
    lines.append(f"=== Trade Replay Summary: {summary.get('date', 'N/A')} ===")
    lines.append(f"Signals replayed: {summary.get('replay_count', 0)}")
    lines.append(f"Valid replays:    {summary.get('valid_count', 0)}")
    lines.append(f"Invalid replays:  {summary.get('data_invalid_count', 0)}")
    lines.append(f"Win rate:         {summary.get('win_rate', 0):.2%}")
    lines.append(f"Avg net PnL bps:  {summary.get('avg_net_pnl_bps', 0)}")
    lines.append("")
    lines.append("Label distribution:")
    lines.append(f"  clean_followthrough:       {summary.get('clean_followthrough_count', 0)}")
    lines.append(f"  followthrough_but_bad_entry: {summary.get('bad_entry_count', 0)}")
    lines.append(f"  absorption_reversal:       {summary.get('absorption_reversal_count', 0)}")
    lines.append(f"  chop_no_edge:              {summary.get('chop_count', 0)}")
    lines.append(f"  data_invalid:              {summary.get('data_invalid_count', 0)}")
    lines.append("")

    if summary.get("suppressed_replay_count"):
        lines.append("Suppressed signals:")
        lines.append(f"  Count:            {summary.get('suppressed_replay_count', 0)}")
        lines.append(f"  Profitable rate:  {summary.get('suppressed_profitable_rate', 0):.2%}")
        lines.append(f"  Clean FT rate:    {summary.get('suppressed_clean_followthrough_rate', 0):.2%}")

    if summary.get("blocked_replay_count"):
        lines.append("Blocked signals:")
        lines.append(f"  Count:            {summary.get('blocked_replay_count', 0)}")
        lines.append(f"  Saved rate est:   {summary.get('blocked_saved_rate_estimate', 0):.2%}")
        lines.append(f"  False block est:  {summary.get('blocked_false_block_rate_estimate', 0):.2%}")

    lines.append("")
    lines.append(f"Profiles analyzed: {summary.get('profile_count', 0)}")

    top_pos = summary.get("top_positive_profiles", [])
    if top_pos:
        lines.append("Top positive profiles:")
        for p in top_pos:
            lines.append(f"  {p['profile_key'][:60]} | samples={p['sample_count']} | pnl={p['avg_net_pnl_bps']} | wr={p['win_rate']:.2%} | {p['recommended_action']}")

    top_neg = summary.get("top_negative_profiles", [])
    if top_neg:
        lines.append("Top negative profiles:")
        for p in top_neg:
            lines.append(f"  {p['profile_key'][:60]} | samples={p['sample_count']} | pnl={p['avg_net_pnl_bps']} | wr={p['win_rate']:.2%} | {p['recommended_action']}")

    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade Replay — historical signal backtest")
    parser.add_argument("--date", required=True, help="Logical date YYYY-MM-DD")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to SQLite")
    parser.add_argument("--include-suppressed", action="store_true", help="Include suppressed signals")
    parser.add_argument("--include-blocked", action="store_true", help="Include blocked opportunities")
    parser.add_argument("--allow-pool-proxy", action="store_true", help="Allow pool_quote_proxy as price source")
    parser.add_argument("--entry-delay-sec", type=int, default=None)
    parser.add_argument("--max-hold-sec", type=int, default=None)
    parser.add_argument("--stop-loss-bps", type=int, default=None)
    parser.add_argument("--take-profit-bps", type=int, default=None)
    parser.add_argument("--fee-bps", type=int, default=None)
    parser.add_argument("--slippage-bps", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    rules: dict[str, Any] = {
        "entry_delay_sec": args.entry_delay_sec or REPLAY_ENTRY_DELAY_SEC,
        "max_hold_sec": args.max_hold_sec or REPLAY_MAX_HOLD_SEC,
        "stop_loss_bps": args.stop_loss_bps or REPLAY_STOP_LOSS_BPS,
        "take_profit_bps": args.take_profit_bps or REPLAY_TAKE_PROFIT_BPS,
        "fee_bps": args.fee_bps or REPLAY_FEE_BPS,
        "slippage_bps": args.slippage_bps or REPLAY_SLIPPAGE_BPS,
        "allow_pool_proxy": args.allow_pool_proxy,
    }

    result = run_trade_replay(
        args.date,
        rules=rules,
        dry_run=args.dry_run,
        include_suppressed=args.include_suppressed,
        include_blocked=args.include_blocked,
        output_format=args.format,
    )

    if args.format == "json":
        output = {
            "status": result["status"],
            "summary": result["summary"],
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        print(_format_summary_text(result["summary"], result.get("profile_stats", {})))

    if result["status"] != "ok":
        sys.exit(1)


if __name__ == "__main__":
    main()
