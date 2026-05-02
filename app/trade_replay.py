"""Trade replay engine and replay closure helpers.

This module keeps the lightweight per-signal replay logic used by tests,
and adds read-only SQLite helpers for replay closure/report workflows:
- exact signal_id / trade_opportunity_id price lookup
- visible warning / error collection instead of silent failures
- minimal CLI for DATE-scoped replay summaries

It does not change production trade semantics.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import (
    OUTCOME_PREFER_OKX_MARK,
    OUTCOME_USE_MARKET_CONTEXT_PRICE,
    REPLAY_STRATEGY_NAME,
    SHADOW_CANDIDATE_MIN_SCORE,
    SHADOW_OPPORTUNITY_ENABLE,
    SHADOW_REQUIRE_BROADER_CONFIRM,
    SHADOW_REQUIRE_LIVE_CONTEXT,
    SHADOW_VERIFIED_MIN_SCORE,
    SQLITE_DB_PATH,
)
from replay_profile_gate import (
    evaluate_replay_profile_gate,
    profile_payload as replay_profile_payload,
    repair_profile_key,
    replay_profile_summary,
)


# Default replay parameters
DEFAULT_ENTRY_DELAY_SEC = 5
DEFAULT_MAX_HOLD_SEC = 60
DEFAULT_STOP_LOSS_BPS = 30
DEFAULT_TAKE_PROFIT_BPS = 50
DEFAULT_FEE_BPS = 6
DEFAULT_SLIPPAGE_BPS = 5
MIN_PROFILE_REPLAY_SAMPLES = 10
REPLAY_SAMPLE_INSUFFICIENT = 5
BJ_TZ = timezone(timedelta(hours=8))
REPLAY_SCOPES = {"default", "full", "suppressed", "blocked"}

INPUT_SOURCE_KEYS = (
    "signals",
    "trade_opportunities",
    "delivery_audit",
    "telegram_deliveries",
    "shadow_opportunities",
    "suppressed",
    "blocked",
)
REPLAY_ELIGIBLE_STATUSES = {"CANDIDATE", "VERIFIED", "BLOCKED"}
REPLAY_ELIGIBLE_SHADOW_STATUSES = {"SHADOW_CANDIDATE", "SHADOW_VERIFIED"}
SHADOW_HARD_BLOCKERS = {
    "no_trade",
    "no_trade_lock",
    "direction_conflict",
    "replay_profile_negative",
    "sweep_exhaustion_risk",
    "do_not_chase_long",
    "do_not_chase_short",
    "conflict_no_trade",
}
SHADOW_IMMATURITY_BLOCKERS = {
    "outcome_history_insufficient",
    "profile_sample_count_insufficient",
    "history_samples_insufficient",
    "sample_count_insufficient",
    "sample_insufficient",
    "maturity_insufficient",
    "verified_maturity_immature",
    "profile_completion_too_low",
}
REPLAY_ELIGIBLE_ACTIONS = {
    "LONG_CANDIDATE",
    "SHORT_CANDIDATE",
    "LONG_CHASE_ALLOWED",
    "SHORT_CHASE_ALLOWED",
    "DO_NOT_CHASE_LONG",
    "DO_NOT_CHASE_SHORT",
    "LONG_BIAS_OBSERVE",
    "SHORT_BIAS_OBSERVE",
    "NO_TRADE",
    "NO_TRADE_LOCK",
    "CONFLICT_NO_TRADE",
    "WAIT_CONFIRMATION",
    "BLOCKED",
}
REPLAY_ELIGIBLE_PHASES = {"LOCAL_ABSORPTION", "SWEEP_EXHAUSTION_RISK"}
BLOCKED_ACTIONS = {
    "BLOCKED",
    "DO_NOT_CHASE_LONG",
    "DO_NOT_CHASE_SHORT",
    "NO_TRADE",
    "NO_TRADE_LOCK",
    "CONFLICT_NO_TRADE",
}
REPLAY_DIRECTED_ACTIONS = {
    "LONG_CANDIDATE",
    "SHORT_CANDIDATE",
    "LONG_CHASE_ALLOWED",
    "SHORT_CHASE_ALLOWED",
    "DO_NOT_CHASE_LONG",
    "DO_NOT_CHASE_SHORT",
    "LONG_BIAS_OBSERVE",
    "SHORT_BIAS_OBSERVE",
}
REPLAY_AMBIGUOUS_ACTIONS = {
    "WAIT_CONFIRMATION",
    "CONFLICT_NO_TRADE",
    "NO_TRADE_LOCK",
}
LOW_VALUE_AUDIT_REASONS = {
    "stable_non_swap_filtered",
    "below_min_usd",
    "pure_transfer",
    "pricing_unavailable_no_proxy",
    "internal_housekeeping",
    "housekeeping",
    "internal_rebalance",
    "stable_non_swap",
}


class ReplayDiagnostics:
    def __init__(self, external_collector: Any | None = None) -> None:
        self._external_collector = external_collector
        self.warnings: list[str] = []
        self.query_errors: list[str] = []
        self.price_errors: list[str] = []
        self.schema_errors: list[str] = []

    def _emit(self, category: str, message: str) -> None:
        collector = self._external_collector
        if not collector:
            return
        if isinstance(collector, dict):
            values = collector.setdefault(category, [])
            if isinstance(values, list) and message not in values:
                values.append(message)
            if category != "warnings":
                warnings = collector.setdefault("warnings", [])
                if isinstance(warnings, list) and message not in warnings:
                    warnings.append(message)
            return
        if isinstance(collector, list):
            item = f"{category}:{message}"
            if item not in collector:
                collector.append(item)
            return
        if callable(collector):
            try:
                collector(category, message)
            except TypeError:
                collector(message)

    def warning(self, message: str) -> None:
        if message and message not in self.warnings:
            self.warnings.append(message)
            self._emit("warnings", message)

    def query_error(self, message: str) -> None:
        if message and message not in self.query_errors:
            self.query_errors.append(message)
            self._emit("query_errors", message)
            self.warning(message)

    def price_error(self, message: str) -> None:
        if message and message not in self.price_errors:
            self.price_errors.append(message)
            self._emit("price_errors", message)
            self.warning(message)

    def schema_error(self, message: str) -> None:
        if message and message not in self.schema_errors:
            self.schema_errors.append(message)
            self._emit("schema_errors", message)
            self.warning(message)

    def as_dict(self) -> dict[str, list[str]]:
        return {
            "warnings": list(self.warnings),
            "query_errors": list(self.query_errors),
            "price_errors": list(self.price_errors),
            "schema_errors": list(self.schema_errors),
        }


def _coerce_diagnostics(
    error_collector: Any | None = None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> ReplayDiagnostics:
    if replay_diagnostics is not None:
        return replay_diagnostics
    if isinstance(error_collector, ReplayDiagnostics):
        return error_collector
    return ReplayDiagnostics(external_collector=error_collector)


def replay_signal(
    signal_row: dict[str, Any],
    *,
    entry_delay_sec: int = DEFAULT_ENTRY_DELAY_SEC,
    max_hold_sec: int = DEFAULT_MAX_HOLD_SEC,
    stop_loss_bps: float = DEFAULT_STOP_LOSS_BPS,
    take_profit_bps: float = DEFAULT_TAKE_PROFIT_BPS,
    fee_bps: float = DEFAULT_FEE_BPS,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    state_manager=None,
    market_context_adapter=None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    """Replay a single signal with visible invalid reasons."""
    signal_id = str(signal_row.get("signal_id") or "")
    if not signal_id:
        return _invalid_replay("signal_id_missing")

    direction = _normalize_direction(signal_row.get("trade_opportunity_side") or signal_row.get("direction"))
    if direction not in {"LONG", "SHORT"}:
        return _invalid_replay("direction_ambiguous")

    signal_ts = _to_int(signal_row.get("archive_ts") or signal_row.get("timestamp") or signal_row.get("created_at"))
    if not signal_ts:
        return _invalid_replay("timestamp_missing")

    entry_ts = signal_ts + entry_delay_sec
    entry_result = _get_entry_price(
        signal_row,
        entry_ts=entry_ts,
        state_manager=state_manager,
        market_context_adapter=market_context_adapter,
        replay_diagnostics=replay_diagnostics,
    )
    if not entry_result["valid"]:
        reason = _normalize_invalid_reason(entry_result.get("reason"), stage="entry")
        return _invalid_replay(reason, entry_ts=entry_ts, signal_id=signal_id)

    entry_price = entry_result["price"]
    entry_source = entry_result["source"]

    exit_result = _simulate_hold(
        signal_row,
        entry_price=entry_price,
        entry_ts=entry_ts,
        direction=direction,
        max_hold_sec=max_hold_sec,
        stop_loss_bps=stop_loss_bps,
        take_profit_bps=take_profit_bps,
        state_manager=state_manager,
        market_context_adapter=market_context_adapter,
        replay_diagnostics=replay_diagnostics,
    )
    if not exit_result["valid"]:
        reason = _normalize_invalid_reason(exit_result.get("reason"), stage="exit")
        return _invalid_replay(reason, entry_ts=entry_ts, entry_price=entry_price, signal_id=signal_id)

    exit_price = exit_result["price"]
    exit_ts = exit_result["ts"]
    close_reason = exit_result["reason"]

    gross_pnl_bps = _calc_gross_pnl_bps(entry_price, exit_price, direction)
    net_pnl_bps = gross_pnl_bps - (fee_bps * 2) - (slippage_bps * 2)

    mfe_bps, mae_bps = _calc_mfe_mae(
        signal_row,
        entry_price=entry_price,
        entry_ts=entry_ts,
        direction=direction,
        max_hold_sec=max_hold_sec,
        state_manager=state_manager,
        market_context_adapter=market_context_adapter,
    )

    label = _classify_replay_label(
        net_pnl_bps=net_pnl_bps,
        mfe_bps=mfe_bps,
        mae_bps=mae_bps,
        close_reason=close_reason,
        direction=direction,
    )

    hold_duration_sec = exit_ts - entry_ts
    return {
        "replay_id": f"replay_{signal_id}_{int(time.time())}",
        "signal_id": signal_id,
        "trade_opportunity_id": str(signal_row.get("trade_opportunity_id") or ""),
        "asset": str(signal_row.get("asset_symbol") or signal_row.get("asset") or ""),
        "pair": str(signal_row.get("pair_label") or signal_row.get("pair") or ""),
        "direction": direction,
        "signal_ts": signal_ts,
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "entry_price": round(entry_price, 8),
        "exit_price": round(exit_price, 8),
        "entry_source": entry_source,
        "exit_source": exit_result["source"],
        "entry_delay_sec": entry_delay_sec,
        "hold_duration_sec": hold_duration_sec,
        "gross_pnl_bps": round(gross_pnl_bps, 2),
        "net_pnl_bps": round(net_pnl_bps, 2),
        "fee_bps": fee_bps * 2,
        "slippage_bps": slippage_bps * 2,
        "mfe_bps": round(mfe_bps, 2) if mfe_bps is not None else None,
        "mae_bps": round(mae_bps, 2) if mae_bps is not None else None,
        "stop_loss_bps": stop_loss_bps,
        "take_profit_bps": take_profit_bps,
        "close_reason": close_reason,
        "label": label,
        "data_valid": True,
        "invalid_reason": None,
        "profile_key": str(signal_row.get("opportunity_profile_key") or ""),
        "opportunity_status": str(signal_row.get("trade_opportunity_status") or signal_row.get("opportunity_status") or ""),
        "shadow_status": str(signal_row.get("trade_opportunity_shadow_status") or signal_row.get("shadow_status") or "NONE"),
        "created_at": int(time.time()),
    }


def aggregate_profile_stats(replay_examples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Aggregate replay results by profile_key."""
    stats_by_profile: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "profile_key": "",
            "sample_count": 0,
            "valid_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "clean_followthrough_count": 0,
            "bad_entry_count": 0,
            "absorption_reversal_count": 0,
            "chop_count": 0,
            "data_invalid_count": 0,
            "total_net_pnl_bps": 0.0,
            "total_gross_pnl_bps": 0.0,
            "net_pnl_values": [],
            "gross_pnl_values": [],
            "mfe_values": [],
            "mae_values": [],
        }
    )

    for replay in replay_examples:
        profile_key = str(replay.get("profile_key") or "")
        if not profile_key:
            continue

        stats = stats_by_profile[profile_key]
        stats["profile_key"] = profile_key
        stats["sample_count"] += 1

        if replay.get("data_valid"):
            stats["valid_count"] += 1
            net_pnl = float(replay.get("net_pnl_bps") or 0.0)
            gross_pnl = float(replay.get("gross_pnl_bps") or 0.0)

            stats["total_net_pnl_bps"] += net_pnl
            stats["total_gross_pnl_bps"] += gross_pnl
            stats["net_pnl_values"].append(net_pnl)
            stats["gross_pnl_values"].append(gross_pnl)

            if net_pnl > 0:
                stats["win_count"] += 1
            else:
                stats["loss_count"] += 1

            label = str(replay.get("label") or "")
            if label == "clean_followthrough":
                stats["clean_followthrough_count"] += 1
            elif label == "bad_entry":
                stats["bad_entry_count"] += 1
            elif label == "absorption_reversal":
                stats["absorption_reversal_count"] += 1
            elif label == "chop_no_edge":
                stats["chop_count"] += 1

            mfe = replay.get("mfe_bps")
            mae = replay.get("mae_bps")
            if mfe is not None:
                stats["mfe_values"].append(float(mfe))
            if mae is not None:
                stats["mae_values"].append(float(mae))
        else:
            stats["data_invalid_count"] += 1

    for _, stats in stats_by_profile.items():
        valid_count = stats["valid_count"]
        if valid_count > 0:
            stats["win_rate"] = round(stats["win_count"] / valid_count, 4)
            stats["avg_net_pnl_bps"] = round(stats["total_net_pnl_bps"] / valid_count, 2)
            stats["avg_gross_pnl_bps"] = round(stats["total_gross_pnl_bps"] / valid_count, 2)
            stats["clean_rate"] = round(stats["clean_followthrough_count"] / valid_count, 4)
            stats["bad_entry_rate"] = round(stats["bad_entry_count"] / valid_count, 4)
            stats["absorption_rate"] = round(stats["absorption_reversal_count"] / valid_count, 4)
            stats["chop_rate"] = round(stats["chop_count"] / valid_count, 4)
            if stats["mfe_values"]:
                stats["avg_mfe_bps"] = round(sum(stats["mfe_values"]) / len(stats["mfe_values"]), 2)
            if stats["mae_values"]:
                stats["avg_mae_bps"] = round(sum(stats["mae_values"]) / len(stats["mae_values"]), 2)
        else:
            stats["win_rate"] = 0.0
            stats["avg_net_pnl_bps"] = 0.0
            stats["avg_gross_pnl_bps"] = 0.0
            stats["clean_rate"] = 0.0
            stats["bad_entry_rate"] = 0.0
            stats["absorption_rate"] = 0.0
            stats["chop_rate"] = 0.0

        stats["recommended_action"] = _infer_profile_action(stats)
        stats["confidence"] = _infer_profile_confidence(stats)

    return dict(stats_by_profile)


def _recommend_profile_action(stat: dict[str, Any], valid: list[dict[str, Any]] | None = None) -> str:
    """Compatibility shim for persisted replay profile stats."""
    action_input = dict(stat)
    if "valid_count" not in action_input:
        action_input["valid_count"] = int(action_input.get("valid_sample_count") or len(valid or []))
    if "absorption_rate" not in action_input:
        action_input["absorption_rate"] = float(action_input.get("absorption_reversal_rate") or 0.0)
    return _infer_profile_action(action_input)


def _update_profile_stats(
    stats: dict[str, dict[str, Any]],
    replay_results: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Update profile stats without reading partially assigned stats[profile_key]."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for replay in replay_results:
        profile_key = str(replay.get("profile_key") or "")
        if profile_key:
            grouped[profile_key].append(replay)

    for profile_key, samples in grouped.items():
        valid = [item for item in samples if bool(item.get("data_valid"))]
        valid_count = len(valid)
        net_values = [float(item.get("net_pnl_bps") or 0.0) for item in valid]
        gross_values = [float(item.get("gross_pnl_bps") or 0.0) for item in valid]
        win_count = sum(1 for value in net_values if value > 0.0)
        label_counts = Counter(str(item.get("label") or "") for item in valid)
        stat = {
            "profile_key": profile_key,
            "sample_count": len(samples),
            "valid_sample_count": valid_count,
            "valid_count": valid_count,
            "win_count": win_count,
            "loss_count": max(valid_count - win_count, 0),
            "win_rate": round(win_count / valid_count, 4) if valid_count else 0.0,
            "avg_net_pnl_bps": round(sum(net_values) / valid_count, 2) if valid_count else 0.0,
            "avg_gross_pnl_bps": round(sum(gross_values) / valid_count, 2) if valid_count else 0.0,
            "clean_followthrough_rate": round(label_counts.get("clean_followthrough", 0) / valid_count, 4) if valid_count else 0.0,
            "bad_entry_rate": round(label_counts.get("bad_entry", 0) / valid_count, 4) if valid_count else 0.0,
            "absorption_reversal_rate": round(label_counts.get("absorption_reversal", 0) / valid_count, 4) if valid_count else 0.0,
            "absorption_rate": round(label_counts.get("absorption_reversal", 0) / valid_count, 4) if valid_count else 0.0,
            "chop_rate": round(label_counts.get("chop_no_edge", 0) / valid_count, 4) if valid_count else 0.0,
            "data_invalid_rate": round((len(samples) - valid_count) / len(samples), 4) if samples else 0.0,
        }
        stat["recommended_action"] = _recommend_profile_action(stat, valid)
        stats[profile_key] = stat
    return stats


def replay_single_signal(
    signal_row: dict[str, Any],
    *,
    price_source_fn: Any | None = None,
    entry_delay_sec: int = DEFAULT_ENTRY_DELAY_SEC,
    max_hold_sec: int = DEFAULT_MAX_HOLD_SEC,
    stop_loss_bps: float = DEFAULT_STOP_LOSS_BPS,
    take_profit_bps: float = DEFAULT_TAKE_PROFIT_BPS,
    fee_bps: float = DEFAULT_FEE_BPS,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    prefer_source: str = "",
    error_collector: Any | None = None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    """Replay one signal through a pluggable price source.

    The first price_source_fn call uses exact signal/opportunity identifiers;
    legacy custom functions that do not accept those keywords still work.
    """
    diagnostics = _coerce_diagnostics(error_collector, replay_diagnostics)
    signal_id = str(signal_row.get("signal_id") or "")
    trade_opportunity_id = str(signal_row.get("trade_opportunity_id") or "")
    asset = str(signal_row.get("asset_symbol") or signal_row.get("asset") or "")
    pair = str(signal_row.get("pair_label") or signal_row.get("pair") or "")
    direction = _normalize_direction(signal_row.get("trade_opportunity_side") or signal_row.get("direction"))
    signal_ts = _to_int(signal_row.get("archive_ts") or signal_row.get("timestamp") or signal_row.get("created_at"))

    if not signal_id:
        return _invalid_replay("signal_id_missing")
    if direction not in {"LONG", "SHORT"}:
        return _invalid_replay("direction_ambiguous", signal_id=signal_id, trade_opportunity_id=trade_opportunity_id)
    if not signal_ts:
        return _invalid_replay("timestamp_missing", signal_id=signal_id, trade_opportunity_id=trade_opportunity_id)

    price_fn = price_source_fn or get_price_at_ts
    entry_ts = signal_ts + entry_delay_sec
    exit_ts = entry_ts + max_hold_sec
    entry_result = _call_price_source_fn(
        price_fn,
        asset=asset,
        pair=pair,
        ts=entry_ts,
        prefer_source=prefer_source,
        signal_id=signal_id,
        trade_opportunity_id=trade_opportunity_id,
        error_collector=diagnostics,
    )
    entry_price = _price_from_lookup(entry_result)
    if entry_price is None:
        reason = _reason_from_lookup(entry_result, default="no_entry_price")
        return _invalid_replay(
            _normalize_invalid_reason(reason, stage="entry"),
            signal_id=signal_id,
            trade_opportunity_id=trade_opportunity_id,
            entry_ts=entry_ts,
        )

    exit_result = _call_price_source_fn(
        price_fn,
        asset=asset,
        pair=pair,
        ts=exit_ts,
        prefer_source=prefer_source,
        signal_id=signal_id,
        trade_opportunity_id=trade_opportunity_id,
        error_collector=diagnostics,
    )
    exit_price = _price_from_lookup(exit_result)
    if exit_price is None:
        reason = _reason_from_lookup(exit_result, default="no_exit_price")
        return _invalid_replay(
            _normalize_invalid_reason(reason, stage="exit"),
            signal_id=signal_id,
            trade_opportunity_id=trade_opportunity_id,
            entry_ts=entry_ts,
            entry_price=entry_price,
        )

    gross_pnl_bps = _calc_gross_pnl_bps(entry_price, exit_price, direction)
    net_pnl_bps = gross_pnl_bps - (fee_bps * 2) - (slippage_bps * 2)
    label = _classify_replay_label(
        net_pnl_bps=net_pnl_bps,
        mfe_bps=gross_pnl_bps,
        mae_bps=gross_pnl_bps,
        close_reason="max_hold",
        direction=direction,
    )
    if gross_pnl_bps <= -stop_loss_bps:
        close_reason = "stop_loss"
    elif gross_pnl_bps >= take_profit_bps:
        close_reason = "take_profit"
    else:
        close_reason = "max_hold"
    return {
        "replay_id": f"replay_{signal_id}_{int(time.time())}",
        "signal_id": signal_id,
        "trade_opportunity_id": trade_opportunity_id,
        "asset": asset,
        "pair": pair,
        "direction": direction,
        "signal_ts": signal_ts,
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "entry_price": round(entry_price, 8),
        "exit_price": round(exit_price, 8),
        "entry_source": _source_from_lookup(entry_result),
        "exit_source": _source_from_lookup(exit_result),
        "entry_delay_sec": entry_delay_sec,
        "hold_duration_sec": max_hold_sec,
        "gross_pnl_bps": round(gross_pnl_bps, 2),
        "net_pnl_bps": round(net_pnl_bps, 2),
        "fee_bps": fee_bps * 2,
        "slippage_bps": slippage_bps * 2,
        "mfe_bps": round(gross_pnl_bps, 2),
        "mae_bps": round(gross_pnl_bps, 2),
        "stop_loss_bps": stop_loss_bps,
        "take_profit_bps": take_profit_bps,
        "close_reason": close_reason,
        "label": label,
        "data_valid": True,
        "invalid_reason": None,
        "profile_key": str(signal_row.get("opportunity_profile_key") or signal_row.get("profile_key") or ""),
        "opportunity_status": str(signal_row.get("trade_opportunity_status") or signal_row.get("opportunity_status") or ""),
        "shadow_status": str(signal_row.get("trade_opportunity_shadow_status") or signal_row.get("shadow_status") or "NONE"),
        "created_at": int(time.time()),
    }


def _call_price_source_fn(price_source_fn: Any, **kwargs) -> Any:
    try:
        return price_source_fn(**kwargs)
    except TypeError:
        legacy_kwargs = {key: kwargs[key] for key in ("asset", "pair", "ts", "prefer_source")}
        return price_source_fn(**legacy_kwargs)


def _price_from_lookup(result: Any) -> float | None:
    if isinstance(result, dict):
        return _to_float(result.get("price"))
    return _to_float(result)


def _reason_from_lookup(result: Any, *, default: str) -> str:
    if isinstance(result, dict):
        return str(result.get("reason") or default)
    return default


def _source_from_lookup(result: Any) -> str:
    if isinstance(result, dict):
        return str(result.get("source") or "unavailable")
    return "custom_price_source"


def get_price_at_ts(
    asset: str,
    pair: str,
    ts: int,
    prefer_source: str = "",
    signal_id: str = "",
    trade_opportunity_id: str = "",
    error_collector: Any | None = None,
    *,
    conn: sqlite3.Connection | None = None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    """Resolve price with exact-match fallbacks only.

    Order:
    a. market_context_snapshots asset + nearest timestamp
    b. outcomes.signal_id = signal_id
    c. opportunity_outcomes.trade_opportunity_id = trade_opportunity_id
    d. otherwise no_price_for_signal_id
    """
    diagnostics = _coerce_diagnostics(error_collector, replay_diagnostics)
    canonical_asset = str(asset or "").strip().upper()
    canonical_pair = str(pair or "").strip().upper()
    ts_value = _to_int(ts)
    if ts_value <= 0:
        diagnostics.price_error("price_lookup_invalid_ts")
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    if not signal_id:
        diagnostics.warning("outcome_fallback_skipped_missing_signal_id")
    if not trade_opportunity_id:
        diagnostics.warning("opportunity_fallback_skipped_missing_trade_opportunity_id")

    owned_conn = False
    if conn is None:
        db_path = _resolve_db_path()
        if not db_path.exists():
            diagnostics.query_error(f"sqlite_db_missing:{db_path}")
            return {"price": None, "source": "unavailable", "reason": "schema_error"}
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            owned_conn = True
        except sqlite3.Error as exc:
            diagnostics.query_error(f"sqlite_open_failed:{exc}")
            return {"price": None, "source": "unavailable", "reason": "schema_error"}

    try:
        market_result = _lookup_price_from_market_context(
            conn,
            asset=canonical_asset,
            pair=canonical_pair,
            ts=ts_value,
            prefer_source=prefer_source,
            replay_diagnostics=diagnostics,
        )
        if market_result["price"] is not None:
            return market_result

        if signal_id:
            outcome_result = _lookup_price_from_outcomes(
                conn,
                signal_id=str(signal_id),
                asset=canonical_asset,
                pair=canonical_pair,
                ts=ts_value,
                replay_diagnostics=diagnostics,
            )
            if outcome_result["price"] is not None:
                return outcome_result

        if trade_opportunity_id:
            opp_result = _lookup_price_from_opportunity_outcomes(
                conn,
                trade_opportunity_id=str(trade_opportunity_id),
                asset=canonical_asset,
                ts=ts_value,
                replay_diagnostics=diagnostics,
            )
            if opp_result["price"] is not None:
                return opp_result

        if diagnostics.schema_errors:
            return {"price": None, "source": "unavailable", "reason": "schema_error"}
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    except sqlite3.Error as exc:
        diagnostics.query_error(f"price_lookup_failed:{exc}")
        diagnostics.schema_error(f"schema_error:price_lookup_failed:{exc}")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    finally:
        if owned_conn and conn is not None:
            conn.close()


def _fmt_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_bj(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), BJ_TZ).strftime("%Y-%m-%d %H:%M:%S UTC+8")


def _logical_window(logical_date: str) -> dict[str, Any]:
    parsed = date.fromisoformat(logical_date)
    start_bj = datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    end_ts = start_ts + 24 * 3600 - 1
    return {
        "logical_date": logical_date,
        "timezone": "Asia/Shanghai",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "logical_window_start_utc": _fmt_utc(start_ts),
        "logical_window_end_utc": _fmt_utc(end_ts),
        "logical_window_start_beijing": _fmt_bj(start_ts),
        "logical_window_end_beijing": _fmt_bj(end_ts),
    }


def _empty_input_source_counts() -> dict[str, int]:
    return {key: 0 for key in INPUT_SOURCE_KEYS}


def _empty_eligibility_summary() -> dict[str, Any]:
    return {
        "eligible_count": 0,
        "ineligible_count": 0,
        "ineligible_reasons": {},
        "ineligible_reason_by_source": {},
        "ineligible_reason_by_action": {},
        "ineligible_top_examples": {},
        "suppressed_ineligible_reasons": {},
        "eligible_by_action": {},
        "replay_count_by_action": {},
        "ambiguous_by_action": {},
        "action_classification_counts": {},
        "top_ineligible_actions": [],
        "ambiguous_actions": {},
        "replayable_action_coverage": 0.0,
        "raw_audit_universe_count": 0,
        "replay_candidate_universe_count": 0,
        "eligible_replay_universe_count": 0,
        "not_replay_candidate_count": 0,
        "not_replay_candidate_reasons": {},
        "replay_coverage_rate_raw": 0.0,
        "replay_coverage_rate_candidate": 0.0,
        "replay_coverage_rate_eligible": 0.0,
        "replay_coverage_rate": 0.0,
        "replay_coverage_warning": "",
    }


def _normalize_replay_scope(replay_scope: str | None, *, include_suppressed: bool, include_blocked: bool) -> str:
    if replay_scope:
        scope = str(replay_scope).strip().lower()
    elif include_suppressed and include_blocked:
        scope = "full"
    elif include_suppressed:
        scope = "suppressed"
    else:
        scope = "default"
    if scope not in REPLAY_SCOPES:
        return "default"
    return scope


def _strategy_config_payload(
    *,
    replay_scope: str,
    include_suppressed: bool,
    include_blocked: bool,
    replay_strategy_name: str = REPLAY_STRATEGY_NAME,
    entry_delay_sec: int = DEFAULT_ENTRY_DELAY_SEC,
    max_hold_sec: int = DEFAULT_MAX_HOLD_SEC,
    stop_loss_bps: float = DEFAULT_STOP_LOSS_BPS,
    take_profit_bps: float = DEFAULT_TAKE_PROFIT_BPS,
    fee_bps: float = DEFAULT_FEE_BPS,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
) -> dict[str, Any]:
    return {
        "version": 1,
        "replay_strategy_name": str(replay_strategy_name or REPLAY_STRATEGY_NAME),
        "replay_scope": replay_scope,
        "include_suppressed": bool(include_suppressed),
        "include_blocked": bool(include_blocked),
        "entry_delay_sec": int(entry_delay_sec),
        "max_hold_sec": int(max_hold_sec),
        "stop_loss_bps": float(stop_loss_bps),
        "take_profit_bps": float(take_profit_bps),
        "fee_bps": float(fee_bps),
        "slippage_bps": float(slippage_bps),
    }


def _strategy_config_hash(**kwargs: Any) -> str:
    payload = _strategy_config_payload(**kwargs)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def _from_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "sent", "delivered", "completed"}


def _none_if_blank(value: Any) -> Any:
    if value in (None, "", [], {}, ()):
        return None
    return value


def _row_payloads(row: dict[str, Any], json_columns: tuple[str, ...]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for column in json_columns:
        payload = _from_json(row.get(column), {})
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _expanded_payloads(row: dict[str, Any], payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    seen: set[int] = set()

    def add(value: Any) -> None:
        if not isinstance(value, dict):
            return
        identity = id(value)
        if identity not in seen:
            seen.add(identity)
            expanded.append(value)

    add(row)
    for payload in payloads:
        add(payload)
        for nested_key in ("context", "metadata", "signal_context", "signal_metadata", "event_metadata"):
            add(payload.get(nested_key))
        signal = payload.get("signal")
        if isinstance(signal, dict):
            add(signal)
            add(signal.get("context"))
            add(signal.get("metadata"))
        event = payload.get("event")
        if isinstance(event, dict):
            add(event)
            add(event.get("metadata"))
        for nested_key in (
            "trade_action",
            "trade_action_debug",
            "market_context",
            "profile_features",
            "opportunity_profile_features_json",
            "features_json",
        ):
            add(payload.get(nested_key))
    return expanded


def _first_payload_value(row: dict[str, Any], payloads: list[dict[str, Any]], *keys: str) -> Any:
    for key in keys:
        value = _none_if_blank(row.get(key))
        if value is not None:
            return value
    for payload in payloads:
        for key in keys:
            value = _none_if_blank(payload.get(key))
            if value is not None:
                return value
    return None


def _coerce_replay_eligible(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    return _is_truthy(value)


def _coerce_blockers(value: Any) -> list[str]:
    payload = _from_json(value, value)
    if isinstance(payload, dict):
        return [str(key) for key, item in payload.items() if _is_truthy(item) or item not in (None, "", [], {})]
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item or "").strip()]
    text = str(payload or "").strip()
    return [text] if text else []


def _merge_non_empty(target: dict[str, Any], source: dict[str, Any], *, overwrite: bool = False) -> None:
    for key, value in source.items():
        if key == "input_source":
            continue
        if value in (None, "", [], {}):
            continue
        if overwrite or target.get(key) in (None, "", [], {}):
            target[key] = value


def _select_rows_for_window(
    conn: sqlite3.Connection,
    table: str,
    *,
    select_candidates: tuple[str, ...],
    time_candidates: tuple[str, ...],
    start_ts: int,
    end_ts: int,
    replay_diagnostics: ReplayDiagnostics,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not _table_exists(conn, table):
        return [], []
    columns = _table_columns(conn, table, replay_diagnostics)
    time_column = next((column for column in time_candidates if column in columns), "")
    if not time_column:
        replay_diagnostics.warning(f"input_source_missing_time_column:{table}")
        return [], []
    select_columns = list(dict.fromkeys([column for column in (*select_candidates, time_column) if column in columns]))
    if not select_columns:
        select_columns = [time_column]
    select_sql = ", ".join(select_columns)
    try:
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT {select_sql} FROM {table} "
                f"WHERE CAST({time_column} AS REAL) >= ? AND CAST({time_column} AS REAL) <= ? "
                f"ORDER BY CAST({time_column} AS REAL) ASC",
                (start_ts, end_ts),
            ).fetchall()
        ]
        missing_ts_rows: list[dict[str, Any]] = []
        if not rows:
            missing_ts_rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT {select_sql} FROM {table} WHERE {time_column} IS NULL LIMIT 100"
                ).fetchall()
            ]
        return rows, missing_ts_rows
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"input_source_query_failed:{table}:{exc}")
        return [], []


def _canonical_upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _direction_from_values(*values: Any) -> str:
    for value in values:
        raw = _canonical_upper(value)
        if not raw:
            continue
        if raw in {"LONG", "BUY", "BUY_PRESSURE", "LONG_CANDIDATE", "LONG_CHASE_ALLOWED", "LONG_BIAS_OBSERVE", "DO_NOT_CHASE_LONG"}:
            return "LONG"
        if raw in {"SHORT", "SELL", "SELL_PRESSURE", "SHORT_CANDIDATE", "SHORT_CHASE_ALLOWED", "SHORT_BIAS_OBSERVE", "DO_NOT_CHASE_SHORT"}:
            return "SHORT"
        if "LONG" in raw or raw.endswith("_BUY") or raw.startswith("BUY_"):
            return "LONG"
        if "SHORT" in raw or raw.endswith("_SELL") or raw.startswith("SELL_"):
            return "SHORT"
    return ""


def _candidate_action_key(candidate: dict[str, Any]) -> str:
    for value in (
        candidate.get("trade_action_key"),
        candidate.get("trade_action"),
        candidate.get("delivery_decision"),
        candidate.get("final_trading_output_label"),
        candidate.get("opportunity_status"),
        candidate.get("shadow_status"),
    ):
        raw = _canonical_upper(value)
        if not raw:
            continue
        for action in REPLAY_ELIGIBLE_ACTIONS | REPLAY_ELIGIBLE_STATUSES | REPLAY_ELIGIBLE_SHADOW_STATUSES:
            if raw == action or action in raw:
                return action
        return raw[:80]
    return "unknown"


def _candidate_reason_text(candidate: dict[str, Any]) -> str:
    parts = [
        candidate.get("delivery_decision"),
        candidate.get("suppression_reason"),
        candidate.get("blocked_reason"),
        candidate.get("gate_reason"),
        candidate.get("final_trading_output_label"),
        candidate.get("stage"),
    ]
    return " ".join(str(item or "").strip().lower() for item in parts if str(item or "").strip())


def _low_value_audit_reason(candidate: dict[str, Any]) -> str:
    sources = set(str(item) for item in list(candidate.get("input_source") or []))
    if sources and sources - {"delivery_audit"}:
        return ""
    reason_text = _candidate_reason_text(candidate)
    action = _candidate_action_key(candidate)
    has_core_identity = bool(str(candidate.get("signal_id") or "").strip() and str(candidate.get("asset") or "").strip())
    has_action = action != "unknown"
    for reason in sorted(LOW_VALUE_AUDIT_REASONS, key=len, reverse=True):
        if reason in reason_text:
            if reason == "pricing_unavailable_no_proxy" and (has_core_identity or has_action):
                return ""
            return reason
    return ""


def _candidate_action_classification(candidate: dict[str, Any]) -> str:
    low_value_reason = _low_value_audit_reason(candidate)
    if low_value_reason:
        return "not_replay_candidate"
    action = _candidate_action_key(candidate)
    direction = _direction_from_values(
        candidate.get("side"),
        candidate.get("direction"),
        candidate.get("trade_action_key"),
        candidate.get("trade_action"),
        candidate.get("delivery_decision"),
        candidate.get("final_trading_output_label"),
    )
    if action in REPLAY_DIRECTED_ACTIONS or direction in {"LONG", "SHORT"}:
        return "replay_directed"
    if action in REPLAY_AMBIGUOUS_ACTIONS:
        return "replay_ambiguous"
    if _has_replay_eligible_marker(candidate):
        return "replay_ambiguous" if direction not in {"LONG", "SHORT"} else "replay_directed"
    return "not_replay_candidate"


def _is_replay_candidate_universe(candidate: dict[str, Any]) -> bool:
    if _low_value_audit_reason(candidate):
        return False
    sources = set(str(item) for item in list(candidate.get("input_source") or []))
    has_signal_id = bool(str(candidate.get("signal_id") or "").strip())
    has_asset = bool(str(candidate.get("asset") or "").strip())
    action = _candidate_action_key(candidate)
    if "trade_opportunities" in sources:
        return True
    if "signals" in sources and has_asset:
        return True
    if "telegram_deliveries" in sources and has_signal_id:
        return True
    if "delivery_audit" in sources and has_signal_id and (has_asset or action != "unknown"):
        return True
    return _has_replay_eligible_marker(candidate)


def _has_replay_eligible_marker(candidate: dict[str, Any]) -> bool:
    if candidate.get("replay_eligible") is True:
        return True
    status = _canonical_upper(candidate.get("opportunity_status"))
    shadow_status = _canonical_upper(candidate.get("shadow_status"))
    action = _canonical_upper(candidate.get("trade_action_key") or candidate.get("trade_action"))
    delivery_decision = _canonical_upper(candidate.get("delivery_decision"))
    final_label = _canonical_upper(candidate.get("final_trading_output_label"))
    lp_stage = _canonical_upper(candidate.get("lp_stage"))
    sweep_phase = _canonical_upper(candidate.get("sweep_phase"))
    if status in REPLAY_ELIGIBLE_STATUSES:
        return True
    if shadow_status in REPLAY_ELIGIBLE_SHADOW_STATUSES:
        return True
    if action in REPLAY_ELIGIBLE_ACTIONS or delivery_decision in REPLAY_ELIGIBLE_ACTIONS:
        return True
    if lp_stage in REPLAY_ELIGIBLE_PHASES or sweep_phase in REPLAY_ELIGIBLE_PHASES:
        return True
    return any(marker in final_label for marker in REPLAY_ELIGIBLE_ACTIONS | REPLAY_ELIGIBLE_STATUSES | REPLAY_ELIGIBLE_SHADOW_STATUSES | REPLAY_ELIGIBLE_PHASES)


def _is_blocked_candidate(candidate: dict[str, Any]) -> bool:
    status = _canonical_upper(candidate.get("opportunity_status"))
    action = _canonical_upper(candidate.get("trade_action_key") or candidate.get("trade_action"))
    delivery_decision = _canonical_upper(candidate.get("delivery_decision"))
    suppression_reason = _canonical_upper(candidate.get("suppression_reason") or candidate.get("blocked_reason"))
    return (
        status == "BLOCKED"
        or action in BLOCKED_ACTIONS
        or delivery_decision in BLOCKED_ACTIONS
        or "NO_TRADE" in suppression_reason
        or "DO_NOT_CHASE" in suppression_reason
        or "BLOCK" in suppression_reason
    )


def _shadow_score_value(candidate: dict[str, Any]) -> float | None:
    for key in (
        "score",
        "calibrated_score",
        "opportunity_score",
        "opportunity_calibrated_score",
        "trade_opportunity_calibrated_score",
        "trade_opportunity_score",
        "raw_score",
        "opportunity_raw_score",
        "trade_opportunity_raw_score",
        "shadow_score",
        "trade_opportunity_shadow_score",
    ):
        value = _to_float(candidate.get(key))
        if value is not None:
            return max(0.0, min(1.0, float(value)))
    return None


def _shadow_blocker_tokens(candidate: dict[str, Any]) -> list[str]:
    tokens: list[str] = []
    for key in (
        "blockers",
        "blocked_reason",
        "suppression_reason",
        "gate_reason",
        "trade_action_key",
        "trade_action",
        "delivery_decision",
        "final_trading_output_label",
    ):
        for item in _coerce_blockers(candidate.get(key)):
            text = str(item or "").strip()
            if text and text not in tokens:
                tokens.append(text)
    return tokens


def _shadow_blocker_key(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw.startswith("hard_blocker:"):
        raw = raw.split(":", 1)[1]
    if raw in {"no_trade", "no_trade_lock", "direction_conflict", "sweep_exhaustion_risk"}:
        return raw
    if raw in {"do_not_chase_long", "do_not_chase_short", "conflict_no_trade"}:
        return raw
    if "no_trade_lock" in raw:
        return "no_trade_lock"
    if "direction_conflict" in raw or "conflict_no_trade" in raw:
        return "direction_conflict"
    if "sweep_exhaustion_risk" in raw:
        return "sweep_exhaustion_risk"
    return raw


def _shadow_hard_blocker(blockers: list[str]) -> str:
    for blocker in blockers:
        key = _shadow_blocker_key(blocker)
        if key in SHADOW_HARD_BLOCKERS:
            return key
    return ""


def _shadow_immaturity_blocked(blockers: list[str]) -> bool:
    keys = [_shadow_blocker_key(item) for item in blockers if str(item or "").strip()]
    if not keys:
        return False
    non_blocker_labels = {
        str(item).lower()
        for item in (REPLAY_ELIGIBLE_ACTIONS | REPLAY_ELIGIBLE_STATUSES | REPLAY_ELIGIBLE_SHADOW_STATUSES)
    } - SHADOW_HARD_BLOCKERS - {"blocked"}
    keys = [key for key in keys if key not in non_blocker_labels]
    soft = [key for key in keys if key in SHADOW_IMMATURITY_BLOCKERS or "sample" in key or "maturity" in key]
    non_soft = [
        key
        for key in keys
        if key not in set(soft)
        and key not in {"blocked", "production_gate_blocked", "candidate_only", "none"}
    ]
    return bool(soft) and not non_soft


def _shadow_live_context_ready(candidate: dict[str, Any]) -> bool:
    source = str(candidate.get("market_context_source") or "").strip().lower()
    if not source:
        return False
    return source not in {"unavailable", "missing", "none", "fixture_unavailable"}


def _shadow_broader_confirm_ready(candidate: dict[str, Any]) -> bool:
    values = (
        candidate.get("lp_confirm_scope"),
        candidate.get("lp_broader_alignment"),
        candidate.get("profile_key"),
        candidate.get("opportunity_profile_key"),
    )
    text = " ".join(str(value or "") for value in values).lower()
    return "broader_confirm" in text or "broader_alignment" in text or "confirmed" in text


def derive_shadow_evaluation_fields(candidate: dict[str, Any]) -> dict[str, Any]:
    """Derive research-only shadow fields from persisted opportunity features."""
    if not bool(SHADOW_OPPORTUNITY_ENABLE):
        return {
            "shadow_evaluated": False,
            "shadow_status": "NONE",
            "shadow_reason": "shadow_disabled",
            "shadow_score": None,
            "would_have_been_candidate": False,
            "would_have_been_verified": False,
            "missing_reasons": ["shadow_disabled"],
        }

    existing_status = _canonical_upper(candidate.get("shadow_status") or "NONE") or "NONE"
    existing_reason = str(candidate.get("shadow_reason") or "").strip()
    existing_shadow_score = _to_float(candidate.get("shadow_score"))
    score = _shadow_score_value(candidate)
    if existing_status in REPLAY_ELIGIBLE_SHADOW_STATUSES:
        return {
            "shadow_evaluated": True,
            "shadow_status": existing_status,
            "shadow_reason": existing_reason or "persisted_shadow_status",
            "shadow_score": score if score is not None else existing_shadow_score,
            "would_have_been_candidate": True,
            "would_have_been_verified": existing_status == "SHADOW_VERIFIED",
            "missing_reasons": [],
        }
    if existing_reason:
        return {
            "shadow_evaluated": True,
            "shadow_status": "NONE",
            "shadow_reason": existing_reason,
            "shadow_score": score if score is not None else existing_shadow_score,
            "would_have_been_candidate": False,
            "would_have_been_verified": False,
            "missing_reasons": [],
        }

    direction = _direction_from_values(
        candidate.get("side"),
        candidate.get("direction"),
        candidate.get("trade_action_key"),
        candidate.get("trade_action"),
        candidate.get("delivery_decision"),
        candidate.get("final_trading_output_label"),
    )
    missing: list[str] = []
    if direction not in {"LONG", "SHORT"}:
        missing.append("missing_side")
    if score is None:
        missing.append("missing_score")
    if missing:
        return {
            "shadow_evaluated": False,
            "shadow_status": "NONE",
            "shadow_reason": "",
            "shadow_score": score,
            "would_have_been_candidate": False,
            "would_have_been_verified": False,
            "missing_reasons": missing,
        }

    status = _canonical_upper(
        candidate.get("opportunity_status")
        or candidate.get("trade_opportunity_status")
        or candidate.get("status")
        or "NONE"
    ) or "NONE"
    blockers = _shadow_blocker_tokens(candidate)
    hard_blocker = _shadow_hard_blocker(blockers)
    shadow_status = "NONE"
    shadow_reason = "score_below_shadow_candidate"
    would_candidate = False
    would_verified = False

    if status in {"CANDIDATE", "VERIFIED"}:
        shadow_reason = f"production_already_{status.lower()}"
    elif hard_blocker:
        shadow_reason = f"hard_blocker:{hard_blocker}"
    elif bool(SHADOW_REQUIRE_LIVE_CONTEXT) and not _shadow_live_context_ready(candidate):
        shadow_reason = "shadow_require_live_context"
    elif bool(SHADOW_REQUIRE_BROADER_CONFIRM) and not _shadow_broader_confirm_ready(candidate):
        shadow_reason = "shadow_require_broader_confirm"
    elif score >= float(SHADOW_VERIFIED_MIN_SCORE) and _shadow_immaturity_blocked(blockers):
        shadow_status = "SHADOW_VERIFIED"
        shadow_reason = "near_verified_but_immature"
        would_candidate = True
        would_verified = True
    elif score >= float(SHADOW_CANDIDATE_MIN_SCORE):
        shadow_status = "SHADOW_CANDIDATE"
        shadow_reason = "near_candidate_but_blocked" if status in {"BLOCKED", "NONE"} else "near_candidate"
        would_candidate = True

    return {
        "shadow_evaluated": True,
        "shadow_status": shadow_status,
        "shadow_reason": shadow_reason,
        "shadow_score": score,
        "would_have_been_candidate": would_candidate,
        "would_have_been_verified": would_verified,
        "missing_reasons": [],
    }


def _apply_derived_shadow_evaluation(candidate: dict[str, Any]) -> dict[str, Any]:
    derived = derive_shadow_evaluation_fields(candidate)
    candidate["shadow_missing_reasons"] = list(derived.get("missing_reasons") or [])
    if not bool(derived.get("shadow_evaluated")):
        candidate.setdefault("shadow_status", "NONE")
        return candidate
    candidate["shadow_evaluated"] = True
    candidate["shadow_status"] = str(derived.get("shadow_status") or "NONE")
    candidate["shadow_reason"] = str(derived.get("shadow_reason") or "")
    if derived.get("shadow_score") is not None:
        score_value = round(float(derived.get("shadow_score") or 0.0), 4)
        candidate["shadow_score"] = score_value
        candidate.setdefault("score", score_value)
    candidate["would_have_been_candidate"] = bool(derived.get("would_have_been_candidate"))
    candidate["would_have_been_verified"] = bool(derived.get("would_have_been_verified"))
    return candidate


def _shadow_score_distribution(scores: list[float]) -> dict[str, Any]:
    if not scores:
        return {
            "count": 0,
            "min": None,
            "p50": None,
            "p90": None,
            "max": None,
            "avg": None,
            "buckets": {},
        }
    ordered = sorted(float(item) for item in scores)

    def pct(ratio: float) -> float:
        index = int((len(ordered) - 1) * ratio)
        return round(ordered[index], 4)

    buckets = {
        "lt_candidate": sum(1 for item in ordered if item < float(SHADOW_CANDIDATE_MIN_SCORE)),
        "candidate_to_verified": sum(
            1 for item in ordered
            if float(SHADOW_CANDIDATE_MIN_SCORE) <= item < float(SHADOW_VERIFIED_MIN_SCORE)
        ),
        "gte_verified": sum(1 for item in ordered if item >= float(SHADOW_VERIFIED_MIN_SCORE)),
    }
    return {
        "count": len(ordered),
        "min": round(ordered[0], 4),
        "p50": pct(0.50),
        "p90": pct(0.90),
        "max": round(ordered[-1], 4),
        "avg": round(sum(ordered) / len(ordered), 4),
        "buckets": buckets,
    }


def _candidate_timestamp(candidate: dict[str, Any]) -> int:
    return _to_int(
        candidate.get("archive_ts")
        or candidate.get("timestamp")
        or candidate.get("created_at")
        or candidate.get("sent_at")
        or candidate.get("notifier_sent_at")
    )


def _candidate_to_signal_row(candidate: dict[str, Any]) -> dict[str, Any]:
    direction = _direction_from_values(
        candidate.get("side"),
        candidate.get("direction"),
        candidate.get("trade_action_key"),
        candidate.get("trade_action"),
        candidate.get("delivery_decision"),
        candidate.get("final_trading_output_label"),
    )
    signal_ts = _candidate_timestamp(candidate)
    return {
        "signal_id": str(candidate.get("signal_id") or ""),
        "trade_opportunity_id": str(candidate.get("trade_opportunity_id") or ""),
        "asset": str(candidate.get("asset") or ""),
        "asset_symbol": str(candidate.get("asset") or ""),
        "pair": str(candidate.get("pair") or ""),
        "pair_label": str(candidate.get("pair") or ""),
        "direction": direction,
        "trade_opportunity_side": direction,
        "archive_ts": signal_ts,
        "timestamp": signal_ts,
        "created_at": signal_ts,
        "opportunity_profile_key": str(candidate.get("profile_key") or candidate.get("opportunity_profile_key") or ""),
        "trade_opportunity_status": str(candidate.get("opportunity_status") or ""),
        "trade_opportunity_shadow_status": str(candidate.get("shadow_status") or "NONE"),
        "trade_opportunity_shadow_reason": str(candidate.get("shadow_reason") or ""),
        "trade_opportunity_shadow_score": candidate.get("shadow_score"),
        "input_source": list(candidate.get("input_source") or []),
    }


def _source_row_base(source: str, row: dict[str, Any], payloads: list[dict[str, Any]]) -> dict[str, Any]:
    payloads = _expanded_payloads(row, payloads)
    raw_shadow_status = _first_payload_value(row, payloads, "shadow_status", "trade_opportunity_shadow_status")
    shadow_reason = _first_payload_value(row, payloads, "shadow_reason", "trade_opportunity_shadow_reason")
    shadow_score = _first_payload_value(row, payloads, "shadow_score", "trade_opportunity_shadow_score")
    score = _first_payload_value(
        row,
        payloads,
        "calibrated_score",
        "opportunity_calibrated_score",
        "trade_opportunity_calibrated_score",
        "opportunity_score",
        "trade_opportunity_score",
        "score",
        "raw_score",
        "opportunity_raw_score",
        "trade_opportunity_raw_score",
        "shadow_score",
        "trade_opportunity_shadow_score",
    )
    quality_snapshot = _first_payload_value(row, payloads, "quality_snapshot_json", "quality_snapshot")
    profile_features = _first_payload_value(row, payloads, "profile_features_json", "profile_features")
    score_components = _first_payload_value(row, payloads, "score_components_json", "score_components")
    opportunity_features = _first_payload_value(row, payloads, "opportunity_features", "opportunity_json", "features_json")
    feature_fields = {}
    for value in (profile_features, opportunity_features):
        parsed = _from_json(value, {})
        if isinstance(parsed, dict):
            feature_fields.update(parsed)
    candidate = {
        "source": source,
        "signal_id": _first_payload_value(row, payloads, "signal_id"),
        "trade_opportunity_id": _first_payload_value(row, payloads, "trade_opportunity_id"),
        "asset": _first_payload_value(row, payloads, "asset", "asset_symbol", "token_symbol"),
        "pair": _first_payload_value(row, payloads, "pair", "pair_label"),
        "side": _first_payload_value(row, payloads, "side", "opportunity_profile_side", "trade_opportunity_side", "would_have_been_direction"),
        "direction": _first_payload_value(row, payloads, "direction", "trade_opportunity_side", "side", "would_have_been_direction"),
        "trade_action_key": _first_payload_value(row, payloads, "trade_action_key", "trade_action"),
        "trade_action": _first_payload_value(row, payloads, "trade_action", "trade_action_key"),
        "final_trading_output_label": _first_payload_value(row, payloads, "final_trading_output_label", "action_label", "headline"),
        "opportunity_status": _first_payload_value(row, payloads, "status", "trade_opportunity_status", "opportunity_status"),
        "score": score,
        "shadow_status": raw_shadow_status or "NONE",
        "shadow_status_present": raw_shadow_status is not None,
        "shadow_reason": shadow_reason,
        "shadow_score": shadow_score,
        "shadow_evaluated": (
            _canonical_upper(raw_shadow_status) in REPLAY_ELIGIBLE_SHADOW_STATUSES
            or shadow_reason is not None
            or shadow_score is not None
        ),
        "profile_key": _first_payload_value(row, payloads, "profile_key", "opportunity_profile_key"),
        "quality_snapshot": quality_snapshot,
        "profile_features": profile_features,
        "score_components": score_components,
        "opportunity_features": opportunity_features,
        "market_context_source": _first_payload_value(row, payloads, "market_context_source", "market_context_adapter_source", "context_source"),
        "lp_stage": _first_payload_value(row, payloads, "lp_stage", "lp_alert_stage"),
        "lp_alert_stage_candidate": _first_payload_value(row, payloads, "lp_alert_stage_candidate"),
        "sweep_phase": _first_payload_value(row, payloads, "sweep_phase", "lp_sweep_phase"),
        "lp_alert_stage": _first_payload_value(row, payloads, "lp_alert_stage", "lp_stage"),
        "lp_confirm_scope": _first_payload_value(row, payloads, "lp_confirm_scope", "confirm_scope"),
        "trade_action_stage": _first_payload_value(row, payloads, "trade_action_stage", "trade_action_stage_key", "trade_action_stage_label"),
        "lp_absorption_context": _first_payload_value(row, payloads, "lp_absorption_context", "absorption_context"),
        "alert_relative_timing": _first_payload_value(row, payloads, "alert_relative_timing", "market_timing"),
        "subtype": _first_payload_value(row, payloads, "subtype"),
        "basis_bucket": _first_payload_value(row, payloads, "basis_bucket"),
        "quality_bucket": _first_payload_value(row, payloads, "quality_bucket"),
        "pool_quality_score": _first_payload_value(row, payloads, "pool_quality_score"),
        "pair_quality_score": _first_payload_value(row, payloads, "pair_quality_score"),
        "asset_case_quality_score": _first_payload_value(row, payloads, "asset_case_quality_score"),
        "major_asset": _first_payload_value(row, payloads, "major_asset"),
        "delivery_decision": _first_payload_value(row, payloads, "delivery_decision", "delivery_reason", "delivery_class"),
        "stage": _first_payload_value(row, payloads, "stage"),
        "gate_reason": _first_payload_value(row, payloads, "gate_reason", "opportunity_gate_failure_reason"),
        "suppression_reason": _first_payload_value(row, payloads, "suppression_reason", "telegram_suppression_reason", "reason", "gate_reason"),
        "blocked_reason": _first_payload_value(row, payloads, "blocked_reason", "primary_blocker", "primary_hard_blocker"),
        "replay_eligible": _coerce_replay_eligible(_first_payload_value(row, payloads, "replay_eligible", "trade_opportunity_replay_eligible")),
        "archive_ts": _first_payload_value(row, payloads, "archive_ts", "archive_written_at"),
        "timestamp": _first_payload_value(row, payloads, "timestamp"),
        "created_at": _first_payload_value(row, payloads, "created_at"),
        "updated_at": _first_payload_value(row, payloads, "updated_at"),
        "sent_at": _first_payload_value(row, payloads, "sent_at", "notifier_sent_at"),
        "notifier_sent_at": _first_payload_value(row, payloads, "notifier_sent_at"),
        "suppressed": _is_truthy(_first_payload_value(row, payloads, "suppressed")),
        "sent_to_telegram": _is_truthy(_first_payload_value(row, payloads, "sent_to_telegram", "sent", "delivered")),
        "blockers": _coerce_blockers(_first_payload_value(row, payloads, "blockers_json", "hard_blockers_json", "verification_blockers_json", "blockers")),
    }
    repair_fields = dict(feature_fields)
    repair_fields.update({key: value for key, value in candidate.items() if value not in (None, "", [], {})})
    candidate["profile_key"] = repair_profile_key(candidate.get("profile_key"), repair_fields)
    return candidate


def _add_candidate(
    candidates: dict[str, dict[str, Any]],
    candidate: dict[str, Any],
    *,
    source: str,
    fallback_key: str,
) -> None:
    signal_id = str(candidate.get("signal_id") or "").strip()
    key = signal_id or fallback_key
    target = candidates.setdefault(key, {"signal_id": signal_id, "input_source": []})
    if source not in target["input_source"]:
        target["input_source"].append(source)
    overwrite = source == "trade_opportunities"
    _merge_non_empty(target, candidate, overwrite=overwrite)
    target["signal_id"] = target.get("signal_id") or signal_id
    target["suppressed"] = bool(target.get("suppressed")) or bool(candidate.get("suppressed"))
    target["blocked"] = bool(target.get("blocked")) or _is_blocked_candidate(candidate)
    target["shadow_evaluated"] = bool(target.get("shadow_evaluated")) or bool(candidate.get("shadow_evaluated"))
    target["shadow_status_present"] = bool(target.get("shadow_status_present")) or bool(candidate.get("shadow_status_present"))
    if _canonical_upper(candidate.get("shadow_status")) in REPLAY_ELIGIBLE_SHADOW_STATUSES:
        target["shadow_status"] = candidate.get("shadow_status")


def _collect_trade_replay_inputs(
    conn: sqlite3.Connection,
    *,
    window: dict[str, Any],
    include_suppressed: bool,
    include_blocked: bool,
    replay_diagnostics: ReplayDiagnostics,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    del include_suppressed, include_blocked
    start_ts = int(window["start_ts"])
    end_ts = int(window["end_ts"])
    counts = _empty_input_source_counts()
    candidates: dict[str, dict[str, Any]] = {}

    source_specs = (
        (
            "trade_opportunities",
            "trade_opportunities",
            (
                "signal_id",
                "trade_opportunity_id",
                "asset",
                "pair",
                "opportunity_profile_key",
                "side",
                "status",
                "primary_blocker",
                "primary_hard_blocker",
                "primary_verification_blocker",
                "blockers_json",
                "hard_blockers_json",
                "verification_blockers_json",
                "would_have_been_direction",
                "replay_eligible",
                "raw_score",
                "calibrated_score",
                "market_context_source",
                "lp_alert_stage",
                "lp_sweep_phase",
                "lp_confirm_scope",
                "lp_absorption_context",
                "alert_relative_timing",
                "quality_snapshot_json",
                "score_components_json",
                "profile_features_json",
                "created_at",
                "updated_at",
                "shadow_status",
                "shadow_reason",
                "shadow_score",
                "opportunity_json",
            ),
            ("created_at", "updated_at"),
            ("opportunity_json", "blockers_json", "hard_blockers_json", "verification_blockers_json", "quality_snapshot_json", "score_components_json", "profile_features_json"),
        ),
        (
            "signals",
            "signals",
            (
                "signal_id",
                "trade_opportunity_id",
                "asset",
                "pair",
                "timestamp",
                "archive_written_at",
                "created_at",
                "direction",
                "trade_action_key",
                "trade_opportunity_status",
                "trade_opportunity_shadow_status",
                "trade_opportunity_shadow_reason",
                "trade_opportunity_shadow_score",
                "opportunity_profile_key",
                "market_context_source",
                "lp_alert_stage",
                "lp_sweep_phase",
                "lp_confirm_scope",
                "lp_absorption_context",
                "alert_relative_timing",
                "delivery_decision",
                "sent_to_telegram",
                "replay_eligible",
                "signal_json",
            ),
            ("archive_written_at", "timestamp", "created_at"),
            ("signal_json",),
        ),
        (
            "delivery_audit",
            "delivery_audit",
            (
                "audit_id",
                "signal_id",
                "trade_opportunity_id",
                "asset",
                "opportunity_status",
                "shadow_status",
                "blocked_reason",
                "profile_key",
                "replay_eligible",
                "delivery_decision",
                "reason",
                "sent_to_telegram",
                "suppressed",
                "timestamp",
                "stage",
                "gate_reason",
                "final_trading_output_label",
                "opportunity_gate_failure_reason",
                "delivered",
                "notifier_sent_at",
                "suppression_reason",
                "audit_json",
                "archive_written_at",
                "created_at",
            ),
            ("archive_written_at", "timestamp", "created_at"),
            ("audit_json",),
        ),
        (
            "telegram_deliveries",
            "telegram_deliveries",
            (
                "telegram_delivery_id",
                "signal_id",
                "trade_opportunity_id",
                "asset",
                "sent",
                "sent_at",
                "suppressed",
                "suppression_reason",
                "telegram_update_kind",
                "message_json",
                "created_at",
            ),
            ("sent_at", "created_at"),
            ("message_json",),
        ),
    )

    for count_key, table, columns, time_columns, json_columns in source_specs:
        rows, missing_ts_rows = _select_rows_for_window(
            conn,
            table,
            select_candidates=columns,
            time_candidates=time_columns,
            start_ts=start_ts,
            end_ts=end_ts,
            replay_diagnostics=replay_diagnostics,
        )
        counts[count_key] = len(rows)
        for index, row in enumerate([*rows, *missing_ts_rows]):
            payloads = _row_payloads(row, json_columns)
            candidate = _source_row_base(count_key, row, payloads)
            if count_key == "trade_opportunities":
                candidate["opportunity_status"] = _first_payload_value(row, payloads, "status", "trade_opportunity_status", "opportunity_status")
                candidate["profile_key"] = _first_payload_value(row, payloads, "opportunity_profile_key", "profile_key")
            elif count_key == "signals":
                candidate["opportunity_status"] = _first_payload_value(row, payloads, "trade_opportunity_status", "opportunity_status")
                candidate["shadow_status"] = _first_payload_value(row, payloads, "trade_opportunity_shadow_status", "shadow_status") or "NONE"
            elif count_key == "telegram_deliveries":
                candidate["sent_at"] = _first_payload_value(row, payloads, "sent_at", "created_at")
            _add_candidate(candidates, candidate, source=count_key, fallback_key=f"{count_key}:{index}")

    merged = [_apply_derived_shadow_evaluation(item) for item in candidates.values()]
    counts["shadow_opportunities"] = sum(1 for item in merged if _canonical_upper(item.get("shadow_status")) in REPLAY_ELIGIBLE_SHADOW_STATUSES)
    counts["suppressed"] = sum(1 for item in merged if bool(item.get("suppressed")))
    counts["blocked"] = sum(1 for item in merged if bool(item.get("blocked")) or _is_blocked_candidate(item))
    return merged, counts, _empty_eligibility_summary()


def _evaluate_replay_inputs(
    candidates: list[dict[str, Any]],
    *,
    include_suppressed: bool,
    include_blocked: bool,
    raw_audit_universe_count: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    reason_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    reason_by_action: dict[str, Counter[str]] = defaultdict(Counter)
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    suppressed_reasons: Counter[str] = Counter()
    eligible_by_action: Counter[str] = Counter()
    ambiguous_by_action: Counter[str] = Counter()
    action_classification_counts: Counter[str] = Counter()
    not_candidate_reasons: Counter[str] = Counter()
    universe_candidates: list[dict[str, Any]] = []

    for candidate in candidates:
        action = _candidate_action_key(candidate)
        action_class = _candidate_action_classification(candidate)
        action_classification_counts[action_class] += 1
        if not _is_replay_candidate_universe(candidate):
            not_candidate_reasons[_low_value_audit_reason(candidate) or action_class or "not_replay_candidate"] += 1
            continue
        universe_candidates.append(candidate)
        if action_class == "replay_ambiguous":
            ambiguous_by_action[action] += 1

    for candidate in universe_candidates:
        row_reasons: list[str] = []
        action = _candidate_action_key(candidate)
        if not str(candidate.get("signal_id") or "").strip():
            row_reasons.append("signal_id_missing")
        if not str(candidate.get("asset") or "").strip():
            row_reasons.append("asset_missing")
        if not _candidate_timestamp(candidate):
            row_reasons.append("timestamp_missing")
        if bool(candidate.get("suppressed")) and not include_suppressed:
            row_reasons.append("filter_excluded_suppressed")
        blocked = bool(candidate.get("blocked")) or _is_blocked_candidate(candidate)
        if blocked and not include_blocked:
            row_reasons.append("filter_excluded_blocked")
        if not _has_replay_eligible_marker(candidate):
            row_reasons.append("not_replay_eligible")
        direction = _direction_from_values(
            candidate.get("side"),
        candidate.get("direction"),
        candidate.get("trade_action_key"),
        candidate.get("trade_action"),
        candidate.get("delivery_decision"),
        candidate.get("final_trading_output_label"),
    )
        if direction not in {"LONG", "SHORT"}:
            row_reasons.append("direction_ambiguous")
        else:
            candidate["direction"] = direction
            candidate["side"] = direction

        if row_reasons:
            for reason in row_reasons:
                reasons[reason] += 1
                reason_by_action[reason][action] += 1
                for source in list(candidate.get("input_source") or ["unknown"]):
                    reason_by_source[reason][str(source or "unknown")] += 1
                if bool(candidate.get("suppressed")):
                    suppressed_reasons[reason] += 1
                if len(examples[reason]) < 5:
                    examples[reason].append(
                        {
                            "source": list(candidate.get("input_source") or []),
                            "signal_id": str(candidate.get("signal_id") or ""),
                            "asset": str(candidate.get("asset") or ""),
                            "opportunity_status": str(candidate.get("opportunity_status") or ""),
                            "trade_action": str(candidate.get("trade_action_key") or candidate.get("trade_action") or action or ""),
                            "suppression_reason": str(candidate.get("suppression_reason") or "")[:120],
                        }
                    )
            candidate["ineligible_reasons"] = row_reasons
            continue
        eligible_by_action[action] += 1
        eligible.append(candidate)

    candidate_universe_count = len(universe_candidates)
    raw_count = int(raw_audit_universe_count or 0)
    eligible_count = len(eligible)
    raw_rate = round(eligible_count / raw_count, 4) if raw_count else 0.0
    candidate_rate = round(eligible_count / candidate_universe_count, 4) if candidate_universe_count else 0.0
    eligible_rate = 1.0 if eligible_count else 0.0
    coverage_warning = (
        f"replay_coverage_candidate_low:{candidate_rate}"
        if candidate_universe_count >= 10 and candidate_rate < 0.25
        else ""
    )
    top_ineligible_actions = [
        {"action": action, "count": count}
        for action, count in Counter(
            action
            for reason_counts in reason_by_action.values()
            for action, count in reason_counts.items()
            for _ in range(int(count))
        ).most_common(10)
    ]
    return eligible, {
        "eligible_count": len(eligible),
        "ineligible_count": candidate_universe_count - len(eligible),
        "ineligible_reasons": dict(sorted(reasons.items())),
        "ineligible_reason_by_source": {
            reason: dict(sorted(source_counts.items()))
            for reason, source_counts in sorted(reason_by_source.items())
        },
        "ineligible_reason_by_action": {
            reason: dict(sorted(action_counts.items()))
            for reason, action_counts in sorted(reason_by_action.items())
        },
        "ineligible_top_examples": {
            reason: values for reason, values in sorted(examples.items())
        },
        "suppressed_ineligible_reasons": dict(sorted(suppressed_reasons.items())),
        "eligible_by_action": dict(sorted(eligible_by_action.items())),
        "replay_count_by_action": dict(sorted(eligible_by_action.items())),
        "ambiguous_by_action": dict(sorted(ambiguous_by_action.items())),
        "action_classification_counts": dict(sorted(action_classification_counts.items())),
        "top_ineligible_actions": top_ineligible_actions,
        "ambiguous_actions": dict(sorted(ambiguous_by_action.items())),
        "replayable_action_coverage": candidate_rate,
        "raw_audit_universe_count": raw_count,
        "replay_candidate_universe_count": candidate_universe_count,
        "eligible_replay_universe_count": eligible_count,
        "not_replay_candidate_count": len(candidates) - candidate_universe_count,
        "not_replay_candidate_reasons": dict(sorted(not_candidate_reasons.items())),
        "replay_coverage_rate_raw": raw_rate,
        "replay_coverage_rate_candidate": candidate_rate,
        "replay_coverage_rate_eligible": eligible_rate,
        "replay_coverage_rate": candidate_rate,
        "replay_coverage_warning": coverage_warning,
    }


def _trade_replay_missing_reasons(
    *,
    input_source_counts: dict[str, int],
    eligibility_summary: dict[str, Any],
    candidates: list[dict[str, Any]],
    replay_results: list[dict[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if int(input_source_counts.get("signals") or 0) == 0:
        reasons.append("no_signals_in_window")
    if int(input_source_counts.get("trade_opportunities") or 0) == 0:
        reasons.append("no_trade_opportunities_in_window")
    if candidates and int(eligibility_summary.get("eligible_count") or 0) == 0:
        reasons.append("no_replay_eligible_rows")
    ineligible = eligibility_summary.get("ineligible_reasons") if isinstance(eligibility_summary.get("ineligible_reasons"), dict) else {}
    ineligible_count = int(eligibility_summary.get("ineligible_count") or 0)
    if ineligible_count > 0 and int(ineligible.get("direction_ambiguous") or 0) >= ineligible_count:
        reasons.append("all_rows_missing_direction")
    if ineligible_count > 0 and int(ineligible.get("timestamp_missing") or 0) >= ineligible_count:
        reasons.append("all_rows_missing_timestamp")
    if ineligible_count > 0 and int(ineligible.get("signal_id_missing") or 0) >= ineligible_count:
        reasons.append("all_rows_missing_signal_id")
    if int(ineligible.get("filter_excluded_suppressed") or 0) > 0:
        reasons.append("filter_excluded_suppressed")
    if int(ineligible.get("filter_excluded_blocked") or 0) > 0:
        reasons.append("filter_excluded_blocked")
    invalid_reasons = Counter(str(item.get("invalid_reason") or "") for item in replay_results if not item.get("data_valid"))
    if invalid_reasons and sum(invalid_reasons.values()) == len(replay_results) and any(
        key in invalid_reasons for key in ("no_price_for_signal_id", "no_entry_price", "no_exit_price", "market_context_unavailable", "schema_error")
    ):
        reasons.append("no_price_source")
    return sorted(set(reasons or ["no_replay_eligible_rows"]))


def _shadow_funnel_summary(
    *,
    candidates: list[dict[str, Any]],
    replay_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    shadow_candidates = [_apply_derived_shadow_evaluation(dict(item)) for item in candidates]
    status_counts = Counter(_canonical_upper(item.get("shadow_status") or "NONE") for item in shadow_candidates)
    replay_shadow_statuses = Counter(_canonical_upper(item.get("shadow_status") or "NONE") for item in replay_rows)
    shadow_inputs = [
        item for item in shadow_candidates
        if "trade_opportunities" in list(item.get("input_source") or [])
        or bool(item.get("shadow_evaluated"))
        or _canonical_upper(item.get("shadow_status")) in REPLAY_ELIGIBLE_SHADOW_STATUSES
    ]
    evaluated = [
        item for item in shadow_inputs
        if bool(item.get("shadow_evaluated")) or _canonical_upper(item.get("shadow_status")) in REPLAY_ELIGIBLE_SHADOW_STATUSES
    ]
    candidate_count = int(status_counts.get("SHADOW_CANDIDATE") or 0)
    verified_count = int(status_counts.get("SHADOW_VERIFIED") or 0)
    gate_passed = candidate_count + verified_count
    blocked_reasons: Counter[str] = Counter()
    reason_distribution: Counter[str] = Counter()
    evaluated_scores: list[float] = []
    for item in evaluated:
        status = _canonical_upper(item.get("shadow_status") or "NONE")
        score = _to_float(item.get("shadow_score"))
        if score is not None:
            evaluated_scores.append(float(score))
        reason = str(item.get("shadow_reason") or "").strip()
        reason_distribution[reason or status.lower() or "shadow_status_none"] += 1
        if status in REPLAY_ELIGIBLE_SHADOW_STATUSES:
            continue
        if not reason:
            reason = str(item.get("blocked_reason") or item.get("suppression_reason") or "").strip()
        if not reason:
            blockers = item.get("blockers") if isinstance(item.get("blockers"), list) else []
            reason = str(blockers[0]) if blockers else "shadow_status_none"
        blocked_reasons[reason] += 1

    def present(value: Any) -> bool:
        return value not in (None, "", [], {}, ())

    feature_keys = {
        "score": "score",
        "side": "side",
        "market_context": "market_context_source",
        "opportunity_features": "opportunity_features",
        "profile_key": "profile_key",
        "quality_snapshot": "quality_snapshot",
    }
    feature_counts: Counter[str] = Counter()
    missing_reasons: Counter[str] = Counter()
    for item in shadow_inputs:
        if not SHADOW_OPPORTUNITY_ENABLE:
            missing_reasons["shadow_disabled"] += 1
        item_missing: list[str] = []
        if not present(item.get("score")):
            item_missing.append("missing_score")
        if _direction_from_values(item.get("side"), item.get("direction"), item.get("trade_action_key"), item.get("trade_action")) not in {"LONG", "SHORT"}:
            item_missing.append("missing_side")
        for reason in list(item.get("shadow_missing_reasons") or []):
            if reason not in item_missing:
                item_missing.append(str(reason))
        if bool(item.get("shadow_evaluated")) or _canonical_upper(item.get("shadow_status")) in REPLAY_ELIGIBLE_SHADOW_STATUSES:
            item_missing = []
        elif not item_missing:
            item_missing.append("no_shadow_candidates")
        for reason in item_missing:
            missing_reasons[reason] += 1
        for label, key in feature_keys.items():
            if label == "side":
                if _direction_from_values(item.get("side"), item.get("direction"), item.get("trade_action_key"), item.get("trade_action")) in {"LONG", "SHORT"}:
                    feature_counts[label] += 1
            elif label == "opportunity_features":
                if any(present(item.get(candidate_key)) for candidate_key in ("opportunity_features", "profile_features", "score_components")):
                    feature_counts[label] += 1
            elif present(item.get(key)):
                feature_counts[label] += 1

    shadow_input_count = len(shadow_inputs)
    feature_coverage = {
        key: {
            "present_count": int(feature_counts.get(key) or 0),
            "coverage_rate": round(int(feature_counts.get(key) or 0) / max(shadow_input_count, 1), 4) if shadow_input_count else 0.0,
        }
        for key in sorted(feature_keys)
    }

    zero_reasons: list[str] = []
    if not evaluated:
        if not shadow_candidates:
            zero_reasons.append("no_replay_input_rows")
        elif not any("trade_opportunities" in list(item.get("input_source") or []) for item in shadow_candidates):
            zero_reasons.append("no_trade_opportunities_in_window")
        else:
            zero_reasons.extend(str(reason) for reason in missing_reasons)
    if evaluated and gate_passed == 0 and not blocked_reasons:
        zero_reasons.append("shadow_evaluated_but_no_gate_pass")

    return {
        "shadow_input_count": shadow_input_count,
        "shadow_evaluated_count": len(evaluated),
        "shadow_gate_passed_count": gate_passed,
        "shadow_candidate_count": candidate_count,
        "shadow_verified_count": verified_count,
        "shadow_blocked_count": max(len(evaluated) - gate_passed, 0),
        "shadow_blocked_reasons": dict(sorted(blocked_reasons.items())),
        "shadow_missing_field_reasons": dict(sorted(missing_reasons.items())),
        "shadow_reason_distribution": dict(sorted(reason_distribution.items())),
        "shadow_score_distribution": _shadow_score_distribution(evaluated_scores),
        "shadow_feature_coverage": feature_coverage,
        "shadow_replay_count": sum(
            1 for item in replay_rows
            if _canonical_upper(item.get("shadow_status")) not in {"", "NONE"}
        ),
        "shadow_replay_status_counts": dict(sorted(replay_shadow_statuses.items())),
        "zero_shadow_reasons": sorted(set(zero_reasons)),
    }


def _suppressed_replay_zero_reasons(
    *,
    include_suppressed: bool,
    input_source_counts: dict[str, int],
    eligibility_summary: dict[str, Any],
    suppressed_replay_count: int,
) -> list[str]:
    if suppressed_replay_count > 0:
        return []
    suppressed_input_count = int(input_source_counts.get("suppressed") or 0)
    if suppressed_input_count <= 0:
        return ["no_suppressed_rows"]
    if not include_suppressed:
        return ["suppressed_excluded_by_mode"]
    reasons = eligibility_summary.get("suppressed_ineligible_reasons")
    reasons = reasons if isinstance(reasons, dict) else {}
    zero_reasons: list[str] = []
    if int(reasons.get("direction_ambiguous") or 0) > 0:
        zero_reasons.append("suppressed_missing_direction")
    if int(reasons.get("signal_id_missing") or 0) > 0:
        zero_reasons.append("suppressed_missing_signal_id")
    if int(reasons.get("not_replay_eligible") or 0) > 0:
        zero_reasons.append("suppressed_not_replay_eligible")
    return sorted(set(zero_reasons or ["no_suppressed_rows"]))


def _replay_dynamic_inputs(
    conn: sqlite3.Connection,
    eligible_inputs: list[dict[str, Any]],
    *,
    replay_diagnostics: ReplayDiagnostics,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    def price_source_fn(**kwargs) -> dict[str, Any]:
        return get_price_at_ts(
            asset=str(kwargs.get("asset") or ""),
            pair=str(kwargs.get("pair") or ""),
            ts=_to_int(kwargs.get("ts")),
            prefer_source=str(kwargs.get("prefer_source") or ""),
            signal_id=str(kwargs.get("signal_id") or ""),
            trade_opportunity_id=str(kwargs.get("trade_opportunity_id") or ""),
            conn=conn,
            replay_diagnostics=replay_diagnostics,
        )

    for candidate in eligible_inputs:
        signal_row = _candidate_to_signal_row(candidate)
        result = replay_single_signal(signal_row, price_source_fn=price_source_fn, replay_diagnostics=replay_diagnostics)
        result["input_source"] = list(candidate.get("input_source") or [])
        result["opportunity_status"] = str(candidate.get("opportunity_status") or result.get("opportunity_status") or "")
        result["shadow_status"] = str(candidate.get("shadow_status") or result.get("shadow_status") or "NONE")
        result["signal_stage"] = "SUPPRESSED" if bool(candidate.get("suppressed")) else str(candidate.get("signal_stage") or "")
        result["profile_key"] = str(candidate.get("profile_key") or result.get("profile_key") or "")
        result["shadow_reason"] = str(candidate.get("shadow_reason") or "")
        result["trade_action"] = _candidate_action_key(candidate)
        results.append(result)
    return results


def _ensure_columns_for_writer(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table)
    for column, column_type in columns.items():
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
        existing.add(column)


def _ensure_replay_persistence_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS trade_replay_examples (
            replay_id TEXT PRIMARY KEY,
            logical_date TEXT NOT NULL,
            replay_scope TEXT DEFAULT 'default',
            strategy_config_hash TEXT,
            include_suppressed INTEGER DEFAULT 0,
            include_blocked INTEGER DEFAULT 0,
            signal_id TEXT,
            trade_opportunity_id TEXT,
            delivery_audit_id TEXT,
            asset TEXT NOT NULL,
            pair TEXT,
            side TEXT NOT NULL,
            opportunity_status TEXT,
            shadow_status TEXT DEFAULT 'NONE',
            signal_stage TEXT,
            lp_stage TEXT,
            sweep_phase TEXT,
            profile_key TEXT,
            signal_ts INTEGER NOT NULL,
            entry_ts INTEGER,
            exit_ts INTEGER,
            entry_delay_sec INTEGER DEFAULT 5,
            max_hold_sec INTEGER DEFAULT 60,
            entry_price REAL,
            exit_price REAL,
            stop_loss_bps INTEGER DEFAULT 30,
            take_profit_bps INTEGER DEFAULT 50,
            fee_bps INTEGER DEFAULT 6,
            slippage_bps INTEGER DEFAULT 5,
            gross_pnl_bps REAL,
            net_pnl_bps REAL,
            mfe_bps REAL,
            mae_bps REAL,
            label TEXT,
            close_reason TEXT,
            price_source TEXT,
            data_valid INTEGER DEFAULT 1,
            invalid_reason TEXT,
            was_profitable INTEGER,
            was_stopped INTEGER,
            was_late INTEGER,
            price_points_seen INTEGER,
            blockers_json TEXT,
            features_json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS trade_replay_profile_stats (
            profile_key TEXT PRIMARY KEY,
            asset TEXT,
            side TEXT,
            sample_count INTEGER DEFAULT 0,
            valid_sample_count INTEGER DEFAULT 0,
            win_rate REAL,
            avg_net_pnl_bps REAL,
            median_net_pnl_bps REAL,
            clean_followthrough_rate REAL,
            bad_entry_rate REAL,
            absorption_reversal_rate REAL,
            chop_rate REAL,
            data_invalid_rate REAL,
            avg_mfe_bps REAL,
            avg_mae_bps REAL,
            recommended_action TEXT,
            confidence_level TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS trade_replay_profile_daily_stats (
            logical_date TEXT NOT NULL,
            replay_scope TEXT NOT NULL DEFAULT 'default',
            strategy_config_hash TEXT NOT NULL DEFAULT '',
            profile_key TEXT NOT NULL,
            asset TEXT,
            side TEXT,
            sample_count INTEGER DEFAULT 0,
            valid_sample_count INTEGER DEFAULT 0,
            win_rate REAL,
            avg_net_pnl_bps REAL,
            median_net_pnl_bps REAL,
            clean_followthrough_rate REAL,
            bad_entry_rate REAL,
            absorption_reversal_rate REAL,
            chop_rate REAL,
            data_invalid_rate REAL,
            avg_mfe_bps REAL,
            avg_mae_bps REAL,
            recommended_action TEXT,
            confidence_level TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(logical_date, replay_scope, strategy_config_hash, profile_key)
        );
        """
    )
    _ensure_columns_for_writer(
        conn,
        "trade_replay_examples",
        {
            "logical_date": "TEXT",
            "replay_scope": "TEXT DEFAULT 'default'",
            "strategy_config_hash": "TEXT",
            "include_suppressed": "INTEGER DEFAULT 0",
            "include_blocked": "INTEGER DEFAULT 0",
            "signal_id": "TEXT",
            "trade_opportunity_id": "TEXT",
            "delivery_audit_id": "TEXT",
            "asset": "TEXT",
            "pair": "TEXT",
            "side": "TEXT",
            "opportunity_status": "TEXT",
            "shadow_status": "TEXT",
            "signal_stage": "TEXT",
            "lp_stage": "TEXT",
            "sweep_phase": "TEXT",
            "profile_key": "TEXT",
            "signal_ts": "INTEGER",
            "entry_ts": "INTEGER",
            "exit_ts": "INTEGER",
            "entry_delay_sec": "INTEGER",
            "max_hold_sec": "INTEGER",
            "entry_price": "REAL",
            "exit_price": "REAL",
            "stop_loss_bps": "INTEGER",
            "take_profit_bps": "INTEGER",
            "fee_bps": "INTEGER",
            "slippage_bps": "INTEGER",
            "gross_pnl_bps": "REAL",
            "net_pnl_bps": "REAL",
            "mfe_bps": "REAL",
            "mae_bps": "REAL",
            "label": "TEXT",
            "close_reason": "TEXT",
            "price_source": "TEXT",
            "data_valid": "INTEGER",
            "invalid_reason": "TEXT",
            "was_profitable": "INTEGER",
            "was_stopped": "INTEGER",
            "was_late": "INTEGER",
            "price_points_seen": "INTEGER",
            "blockers_json": "TEXT",
            "features_json": "TEXT",
            "created_at": "TEXT",
        },
    )
    _ensure_columns_for_writer(
        conn,
        "trade_replay_profile_stats",
        {
            "asset": "TEXT",
            "side": "TEXT",
            "sample_count": "INTEGER",
            "valid_sample_count": "INTEGER",
            "win_rate": "REAL",
            "avg_net_pnl_bps": "REAL",
            "median_net_pnl_bps": "REAL",
            "clean_followthrough_rate": "REAL",
            "bad_entry_rate": "REAL",
            "absorption_reversal_rate": "REAL",
            "chop_rate": "REAL",
            "data_invalid_rate": "REAL",
            "avg_mfe_bps": "REAL",
            "avg_mae_bps": "REAL",
            "recommended_action": "TEXT",
            "confidence_level": "TEXT",
            "updated_at": "TEXT",
        },
    )
    _ensure_columns_for_writer(
        conn,
        "trade_replay_profile_daily_stats",
        {
            "logical_date": "TEXT",
            "replay_scope": "TEXT DEFAULT 'default'",
            "strategy_config_hash": "TEXT",
            "profile_key": "TEXT",
            "asset": "TEXT",
            "side": "TEXT",
            "sample_count": "INTEGER",
            "valid_sample_count": "INTEGER",
            "win_rate": "REAL",
            "avg_net_pnl_bps": "REAL",
            "median_net_pnl_bps": "REAL",
            "clean_followthrough_rate": "REAL",
            "bad_entry_rate": "REAL",
            "absorption_reversal_rate": "REAL",
            "chop_rate": "REAL",
            "data_invalid_rate": "REAL",
            "avg_mfe_bps": "REAL",
            "avg_mae_bps": "REAL",
            "recommended_action": "TEXT",
            "confidence_level": "TEXT",
            "updated_at": "TEXT",
        },
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_replay_examples_logical_date ON trade_replay_examples(logical_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_replay_examples_scope ON trade_replay_examples(logical_date, replay_scope, strategy_config_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_replay_examples_signal_id ON trade_replay_examples(signal_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_replay_examples_profile_key ON trade_replay_examples(profile_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_replay_profile_daily_date_scope ON trade_replay_profile_daily_stats(logical_date, replay_scope, strategy_config_hash)")


def _deterministic_replay_id(
    logical_date: str,
    row: dict[str, Any],
    index: int,
    *,
    replay_scope: str,
    strategy_config_hash: str,
) -> str:
    signal_id = str(row.get("signal_id") or "").strip()
    opportunity_id = str(row.get("trade_opportunity_id") or "").strip()
    key = signal_id or opportunity_id or f"row_{index}"
    safe_key = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in key)
    safe_scope = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in replay_scope)
    safe_hash = "".join(ch if ch.isalnum() else "_" for ch in strategy_config_hash)
    return f"replay_{logical_date}_{safe_scope}_{safe_key}_{safe_hash}"


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 2)
    return round((ordered[mid - 1] + ordered[mid]) / 2, 2)


def _insert_or_update(conn: sqlite3.Connection, table: str, record: dict[str, Any], key_columns: tuple[str, ...]) -> None:
    columns = [column for column in record.keys() if column in _table_columns(conn, table)]
    if not columns:
        return
    placeholders = ", ".join("?" for _ in columns)
    update_columns = [column for column in columns if column not in key_columns]
    assignments = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
    conflict = ", ".join(key_columns)
    if assignments:
        sql = (
            f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET {assignments}"
        )
    else:
        sql = f"INSERT OR IGNORE INTO {table}({', '.join(columns)}) VALUES({placeholders})"
    conn.execute(sql, tuple(record[column] for column in columns))


def _blocker_json_with_replay_profile_negative(value: Any) -> str:
    blockers = _coerce_blockers(value)
    if "replay_profile_negative" not in blockers:
        blockers.append("replay_profile_negative")
    return json.dumps(blockers, ensure_ascii=False, sort_keys=True)


def _repair_trade_opportunity_replay_profile_blockers(
    conn: sqlite3.Connection,
    profile_rows: dict[str, dict[str, Any]],
    *,
    now_text: str,
) -> int:
    columns = _table_columns(conn, "trade_opportunities", ReplayDiagnostics())
    required = {"trade_opportunity_id", "opportunity_profile_key", "opportunity_json"}
    if not required.issubset(columns):
        return 0
    blocker_profile_keys = sorted(
        profile_key
        for profile_key, row in profile_rows.items()
        if evaluate_replay_profile_gate(row).get("blocker") == "replay_profile_negative"
    )
    if not blocker_profile_keys:
        return 0
    placeholders = ",".join("?" for _ in blocker_profile_keys)
    select_columns = [
        "trade_opportunity_id",
        "opportunity_profile_key",
        "opportunity_json",
    ]
    for optional in ("blockers_json", "hard_blockers_json", "quality_snapshot_json"):
        if optional in columns:
            select_columns.append(optional)
    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT {','.join(select_columns)}
            FROM trade_opportunities
            WHERE opportunity_profile_key IN ({placeholders})
            """,
            blocker_profile_keys,
        ).fetchall()
    ]
    updated = 0
    for row in rows:
        profile_key = str(row.get("opportunity_profile_key") or "")
        gate = evaluate_replay_profile_gate(profile_rows.get(profile_key) or {})
        if gate.get("blocker") != "replay_profile_negative":
            continue
        opportunity_payload = _from_json(row.get("opportunity_json"), {})
        if not isinstance(opportunity_payload, dict):
            opportunity_payload = {}
        blockers = _coerce_blockers(opportunity_payload.get("trade_opportunity_blockers") or row.get("blockers_json"))
        hard_blockers = _coerce_blockers(opportunity_payload.get("trade_opportunity_hard_blockers") or row.get("hard_blockers_json"))
        if "replay_profile_negative" not in blockers:
            blockers.append("replay_profile_negative")
        if "replay_profile_negative" not in hard_blockers:
            hard_blockers.append("replay_profile_negative")
        opportunity_payload["trade_opportunity_blockers"] = blockers
        opportunity_payload["trade_opportunity_hard_blockers"] = hard_blockers
        opportunity_payload["trade_opportunity_replay_profile_gate"] = dict(gate)
        opportunity_payload["trade_opportunity_replay_profile_action"] = str(gate.get("action") or "")
        opportunity_payload["trade_opportunity_replay_profile_research_hint"] = str(gate.get("research_hint") or "")
        quality_snapshot = opportunity_payload.get("trade_opportunity_quality_snapshot")
        if isinstance(quality_snapshot, dict):
            quality_snapshot["replay_profile_gate"] = dict(gate)
        updates = {
            "opportunity_json": json.dumps(opportunity_payload, ensure_ascii=False, sort_keys=True),
        }
        if "blockers_json" in columns:
            updates["blockers_json"] = _blocker_json_with_replay_profile_negative(row.get("blockers_json"))
        if "hard_blockers_json" in columns:
            updates["hard_blockers_json"] = _blocker_json_with_replay_profile_negative(row.get("hard_blockers_json"))
        if "quality_snapshot_json" in columns:
            quality_snapshot_payload = _from_json(row.get("quality_snapshot_json"), {})
            if isinstance(quality_snapshot_payload, dict):
                quality_snapshot_payload["replay_profile_gate"] = dict(gate)
                updates["quality_snapshot_json"] = json.dumps(quality_snapshot_payload, ensure_ascii=False, sort_keys=True)
        if "updated_at" in columns:
            updates["updated_at"] = now_text
        assignments = ", ".join(f"{key}=?" for key in updates)
        params = [*updates.values(), str(row.get("trade_opportunity_id") or "")]
        conn.execute(
            f"UPDATE trade_opportunities SET {assignments} WHERE trade_opportunity_id=?",
            params,
        )
        updated += 1
    return updated


def _persist_trade_replay_rows(
    conn: sqlite3.Connection,
    logical_date: str,
    replay_rows: list[dict[str, Any]],
    replay_diagnostics: ReplayDiagnostics,
    *,
    replay_scope: str,
    strategy_config_hash: str,
    include_suppressed: bool,
    include_blocked: bool,
) -> dict[str, Any]:
    summary = {
        "enabled": True,
        "replay_scope": replay_scope,
        "strategy_config_hash": strategy_config_hash,
        "include_suppressed": bool(include_suppressed),
        "include_blocked": bool(include_blocked),
        "profile_stats_scope": "profile_key_latest_and_daily",
        "idempotent_key": "trade_replay_examples.replay_id=logical_date+replay_scope+signal_id_or_trade_opportunity_id+strategy_config_hash",
        "delete_scope": "logical_date+replay_scope+strategy_config_hash",
        "examples_written": 0,
        "profile_stats_written": 0,
        "profile_daily_stats_written": 0,
        "trade_opportunity_replay_profile_negative_repaired": 0,
    }
    if not replay_rows:
        return summary
    try:
        _ensure_replay_persistence_schema(conn)
        now_text = datetime.now(UTC).isoformat()
        example_columns = _table_columns(conn, "trade_replay_examples")
        if {"logical_date", "replay_scope", "strategy_config_hash"}.issubset(example_columns):
            conn.execute(
                """
                DELETE FROM trade_replay_examples
                WHERE logical_date = ?
                  AND COALESCE(replay_scope, 'default') = ?
                  AND (strategy_config_hash = ? OR strategy_config_hash IS NULL OR strategy_config_hash = '')
                """,
                (logical_date, replay_scope, strategy_config_hash),
            )
        for index, row in enumerate(replay_rows):
            replay_id = _deterministic_replay_id(
                logical_date,
                row,
                index,
                replay_scope=replay_scope,
                strategy_config_hash=strategy_config_hash,
            )
            signal_id = str(row.get("signal_id") or "")
            net_pnl = _to_signed_float(row.get("net_pnl_bps"))
            close_reason = str(row.get("close_reason") or "")
            record = {
                "replay_id": replay_id,
                "logical_date": logical_date,
                "replay_scope": replay_scope,
                "strategy_config_hash": strategy_config_hash,
                "include_suppressed": 1 if include_suppressed else 0,
                "include_blocked": 1 if include_blocked else 0,
                "signal_id": signal_id,
                "trade_opportunity_id": str(row.get("trade_opportunity_id") or ""),
                "delivery_audit_id": str(row.get("delivery_audit_id") or ""),
                "asset": str(row.get("asset") or row.get("asset_symbol") or ""),
                "pair": str(row.get("pair") or row.get("pair_label") or ""),
                "side": str(row.get("direction") or row.get("side") or ""),
                "opportunity_status": str(row.get("opportunity_status") or ""),
                "shadow_status": str(row.get("shadow_status") or "NONE"),
                "signal_stage": str(row.get("signal_stage") or ""),
                "lp_stage": str(row.get("lp_stage") or ""),
                "sweep_phase": str(row.get("sweep_phase") or ""),
                "profile_key": str(row.get("profile_key") or ""),
                "signal_ts": _to_int(row.get("signal_ts") or row.get("archive_ts") or row.get("timestamp")),
                "entry_ts": _to_int(row.get("entry_ts")),
                "exit_ts": _to_int(row.get("exit_ts")),
                "entry_delay_sec": _to_int(row.get("entry_delay_sec")) or DEFAULT_ENTRY_DELAY_SEC,
                "max_hold_sec": _to_int(row.get("hold_duration_sec")) or DEFAULT_MAX_HOLD_SEC,
                "entry_price": _to_float(row.get("entry_price")),
                "exit_price": _to_float(row.get("exit_price")),
                "stop_loss_bps": _to_int(row.get("stop_loss_bps")) or DEFAULT_STOP_LOSS_BPS,
                "take_profit_bps": _to_int(row.get("take_profit_bps")) or DEFAULT_TAKE_PROFIT_BPS,
                "fee_bps": _to_float(row.get("fee_bps")) or DEFAULT_FEE_BPS,
                "slippage_bps": _to_float(row.get("slippage_bps")) or DEFAULT_SLIPPAGE_BPS,
                "gross_pnl_bps": _to_signed_float(row.get("gross_pnl_bps")),
                "net_pnl_bps": net_pnl,
                "mfe_bps": _to_signed_float(row.get("mfe_bps")),
                "mae_bps": _to_signed_float(row.get("mae_bps")),
                "label": str(row.get("label") or ""),
                "close_reason": close_reason,
                "price_source": str(row.get("entry_source") or row.get("exit_source") or row.get("price_source") or ""),
                "data_valid": 1 if _is_truthy(row.get("data_valid", 1)) else 0,
                "invalid_reason": str(row.get("invalid_reason") or ""),
                "was_profitable": 1 if net_pnl is not None and net_pnl > 0.0 else 0,
                "was_stopped": 1 if close_reason == "stop_loss" else 0,
                "was_late": 0,
                "price_points_seen": 2 if row.get("entry_price") is not None and row.get("exit_price") is not None else 0,
                "blockers_json": json.dumps(row.get("blockers") or [], ensure_ascii=False, sort_keys=True),
                "features_json": json.dumps(
                    {
                        "input_source": list(row.get("input_source") or []),
                        "shadow_reason": str(row.get("shadow_reason") or ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "created_at": now_text,
            }
            _insert_or_update(conn, "trade_replay_examples", record, ("replay_id",))
            summary["examples_written"] += 1

        stats: dict[str, dict[str, Any]] = {}
        _update_profile_stats(stats, replay_rows)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in replay_rows:
            profile_key = str(row.get("profile_key") or "")
            if profile_key:
                grouped[profile_key].append(row)
        for profile_key, stat in stats.items():
            samples = grouped.get(profile_key, [])
            valid_samples = [item for item in samples if _is_truthy(item.get("data_valid", 1))]
            net_values = [
                value for value in (_to_signed_float(item.get("net_pnl_bps")) for item in valid_samples)
                if value is not None
            ]
            mfe_values = [
                value for value in (_to_signed_float(item.get("mfe_bps")) for item in valid_samples)
                if value is not None
            ]
            mae_values = [
                value for value in (_to_signed_float(item.get("mae_bps")) for item in valid_samples)
                if value is not None
            ]
            first_sample = samples[0] if samples else {}
            record = {
                "logical_date": logical_date,
                "replay_scope": replay_scope,
                "strategy_config_hash": strategy_config_hash,
                "profile_key": profile_key,
                "asset": str(first_sample.get("asset") or ""),
                "side": str(first_sample.get("direction") or first_sample.get("side") or ""),
                "sample_count": int(stat.get("sample_count") or 0),
                "valid_sample_count": int(stat.get("valid_sample_count") or stat.get("valid_count") or 0),
                "win_rate": float(stat.get("win_rate") or 0.0),
                "avg_net_pnl_bps": float(stat.get("avg_net_pnl_bps") or 0.0),
                "median_net_pnl_bps": _median(net_values),
                "clean_followthrough_rate": float(stat.get("clean_followthrough_rate") or 0.0),
                "bad_entry_rate": float(stat.get("bad_entry_rate") or 0.0),
                "absorption_reversal_rate": float(stat.get("absorption_reversal_rate") or 0.0),
                "chop_rate": float(stat.get("chop_rate") or 0.0),
                "data_invalid_rate": float(stat.get("data_invalid_rate") or 0.0),
                "avg_mfe_bps": round(sum(mfe_values) / len(mfe_values), 2) if mfe_values else None,
                "avg_mae_bps": round(sum(mae_values) / len(mae_values), 2) if mae_values else None,
                "recommended_action": str(stat.get("recommended_action") or ""),
                "confidence_level": str(stat.get("confidence") or ""),
                "updated_at": now_text,
            }
            latest_record = {key: value for key, value in record.items() if key not in {"logical_date", "replay_scope", "strategy_config_hash"}}
            _insert_or_update(conn, "trade_replay_profile_stats", latest_record, ("profile_key",))
            _insert_or_update(
                conn,
                "trade_replay_profile_daily_stats",
                record,
                ("logical_date", "replay_scope", "strategy_config_hash", "profile_key"),
            )
            summary["profile_stats_written"] += 1
            summary["profile_daily_stats_written"] += 1
        summary["trade_opportunity_replay_profile_negative_repaired"] = _repair_trade_opportunity_replay_profile_blockers(
            conn,
            {
                profile_key: {**stat, "profile_key": profile_key}
                for profile_key, stat in stats.items()
                if str(profile_key or "")
            },
            now_text=now_text,
        )
        conn.commit()
        if summary["profile_stats_written"] == 0:
            replay_diagnostics.warning("trade_replay_profile_stats_no_profile_keys")
        return summary
    except sqlite3.Error as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        replay_diagnostics.query_error(f"trade_replay_persist_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:trade_replay_persist_failed:{exc}")
        summary["enabled"] = False
        summary["error"] = str(exc)
        return summary



def _load_persisted_replay_rows(
    conn: sqlite3.Connection,
    logical_date: str,
    replay_diagnostics: ReplayDiagnostics,
    *,
    replay_scope: str = "default",
    strategy_config_hash: str = "",
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "trade_replay_examples"):
        replay_diagnostics.warning("trade_replay_examples_table_missing")
        return []
    columns = _table_columns(conn, "trade_replay_examples", replay_diagnostics)
    required = {"logical_date", "data_valid", "signal_id", "trade_opportunity_id", "asset", "pair", "signal_ts", "entry_ts", "exit_ts", "entry_price", "exit_price", "net_pnl_bps", "label"}
    missing = sorted(required - columns)
    if missing:
        replay_diagnostics.schema_error(f"schema_error:missing_columns:trade_replay_examples:{','.join(missing)}")
        return []
    try:
        if {"replay_scope", "strategy_config_hash"}.issubset(columns):
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT * FROM trade_replay_examples
                    WHERE logical_date = ?
                      AND COALESCE(replay_scope, 'default') = ?
                      AND strategy_config_hash = ?
                    ORDER BY signal_ts ASC
                    """,
                    (logical_date, replay_scope, strategy_config_hash),
                ).fetchall()
            ]
            if rows:
                return rows
            fallback = conn.execute(
                """
                SELECT strategy_config_hash, COUNT(*) AS row_count
                FROM trade_replay_examples
                WHERE logical_date = ?
                  AND COALESCE(replay_scope, 'default') = ?
                GROUP BY strategy_config_hash
                ORDER BY row_count DESC, strategy_config_hash DESC
                LIMIT 1
                """,
                (logical_date, replay_scope),
            ).fetchone()
            fallback_hash = str(fallback["strategy_config_hash"] or "") if fallback else ""
            if fallback_hash:
                replay_diagnostics.warning(f"strategy_config_hash_fallback:{fallback_hash}")
                return [
                    dict(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM trade_replay_examples
                        WHERE logical_date = ?
                          AND COALESCE(replay_scope, 'default') = ?
                          AND strategy_config_hash = ?
                        ORDER BY signal_ts ASC
                        """,
                        (logical_date, replay_scope, fallback_hash),
                    ).fetchall()
                ]
            return rows
        replay_diagnostics.warning("schema_warning:trade_replay_examples.scope_columns_missing")
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM trade_replay_examples WHERE logical_date = ? ORDER BY signal_ts ASC",
                (logical_date,),
            ).fetchall()
        ]
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"trade_replay_query_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:trade_replay_query_failed:{exc}")
        return []


def _profile_rows(
    conn: sqlite3.Connection,
    replay_diagnostics: ReplayDiagnostics,
    *,
    logical_date: str | None = None,
    replay_scope: str = "default",
    strategy_config_hash: str = "",
) -> list[dict[str, Any]]:
    if logical_date and _table_exists(conn, "trade_replay_profile_daily_stats"):
        daily_columns = _table_columns(conn, "trade_replay_profile_daily_stats", replay_diagnostics)
        daily_required = {"logical_date", "replay_scope", "strategy_config_hash", "profile_key", "valid_sample_count", "avg_net_pnl_bps", "win_rate", "recommended_action"}
        daily_missing = sorted(daily_required - daily_columns)
        if not daily_missing:
            try:
                rows = [
                    dict(row)
                    for row in conn.execute(
                        """
                        SELECT * FROM trade_replay_profile_daily_stats
                        WHERE logical_date = ?
                          AND replay_scope = ?
                          AND strategy_config_hash = ?
                        """,
                        (logical_date, replay_scope, strategy_config_hash),
                    ).fetchall()
                ]
                if rows:
                    return rows
            except sqlite3.Error as exc:
                replay_diagnostics.query_error(f"trade_replay_profile_daily_stats_query_failed:{exc}")
        else:
            replay_diagnostics.schema_error(f"schema_error:missing_columns:trade_replay_profile_daily_stats:{','.join(daily_missing)}")
    if not _table_exists(conn, "trade_replay_profile_stats"):
        replay_diagnostics.warning("trade_replay_profile_stats_table_missing")
        return []
    columns = _table_columns(conn, "trade_replay_profile_stats", replay_diagnostics)
    required = {"profile_key", "valid_sample_count", "avg_net_pnl_bps", "win_rate", "recommended_action"}
    missing = sorted(required - columns)
    if missing:
        replay_diagnostics.schema_error(f"schema_error:missing_columns:trade_replay_profile_stats:{','.join(missing)}")
        return []
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM trade_replay_profile_stats").fetchall()]
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"trade_replay_profile_stats_query_failed:{exc}")
        return []


def _profile_payload(row: dict[str, Any]) -> dict[str, Any]:
    return replay_profile_payload(row)


def _augment_summary_from_replays(summary: dict[str, Any], replay_rows: list[dict[str, Any]], profile_rows: list[dict[str, Any]]) -> None:
    total = len(replay_rows)
    valid_rows = [row for row in replay_rows if _is_truthy(row.get("data_valid", 1))]
    valid = len(valid_rows)
    label_counts = Counter(str(row.get("label") or "") for row in replay_rows)
    net_values = [_to_signed_float(row.get("net_pnl_bps")) for row in valid_rows]
    net_values = [value for value in net_values if value is not None]
    wins = sum(1 for value in net_values if value > 0.0)
    valid_den = max(valid, 1)
    suppressed_rows = [row for row in replay_rows if str(row.get("signal_stage") or "") == "SUPPRESSED"]
    suppressed_valid = [row for row in suppressed_rows if _is_truthy(row.get("data_valid", 1))]
    suppressed_net = [_to_signed_float(row.get("net_pnl_bps")) for row in suppressed_valid]
    suppressed_net = [value for value in suppressed_net if value is not None]
    blocked_rows = [row for row in replay_rows if _canonical_upper(row.get("opportunity_status")) == "BLOCKED"]
    blocked_valid = [row for row in blocked_rows if _is_truthy(row.get("data_valid", 1))]
    shadow_count = sum(1 for row in replay_rows if _canonical_upper(row.get("shadow_status")) not in {"", "NONE"})

    if not profile_rows:
        stats = aggregate_profile_stats(replay_rows)
        profile_rows = [
            {
                "profile_key": key,
                "valid_sample_count": value.get("valid_count", 0),
                "avg_net_pnl_bps": value.get("avg_net_pnl_bps", 0.0),
                "win_rate": value.get("win_rate", 0.0),
                "clean_followthrough_rate": value.get("clean_followthrough_rate", value.get("clean_rate", 0.0)),
                "chop_rate": value.get("chop_rate", 0.0),
                "recommended_action": value.get("recommended_action"),
            }
            for key, value in stats.items()
        ]
    positive = [
        _profile_payload(row)
        for row in sorted(profile_rows, key=lambda item: (-float(item.get("avg_net_pnl_bps") or 0.0), -int(item.get("valid_sample_count") or 0), str(item.get("profile_key") or "")))
        if float(row.get("avg_net_pnl_bps") or 0.0) > 0.0
    ][:5]
    negative = [
        _profile_payload(row)
        for row in sorted(profile_rows, key=lambda item: (float(item.get("avg_net_pnl_bps") or 0.0), -int(item.get("valid_sample_count") or 0), str(item.get("profile_key") or "")))
        if float(row.get("avg_net_pnl_bps") or 0.0) < 0.0
    ][:5]

    summary.update(
        {
            "replay_count": total,
            "trade_replay_available": total > 0,
            "valid_replay_count": valid,
            "valid_count": valid,
            "sample_insufficient": valid < REPLAY_SAMPLE_INSUFFICIENT,
            "win_rate": round(wins / valid_den, 4),
            "avg_net_pnl_bps": round(sum(net_values) / len(net_values), 2) if net_values else 0.0,
            "clean_followthrough_rate": round(int(label_counts.get("clean_followthrough") or 0) / valid_den, 4),
            "bad_entry_rate": round((int(label_counts.get("bad_entry") or 0) + int(label_counts.get("followthrough_but_bad_entry") or 0)) / valid_den, 4),
            "absorption_reversal_rate": round(int(label_counts.get("absorption_reversal") or 0) / valid_den, 4),
            "chop_rate": round(int(label_counts.get("chop_no_edge") or 0) / valid_den, 4),
            "data_invalid_rate": round((total - valid) / max(total, 1), 4),
            "suppressed_replay_count": len(suppressed_rows),
            "suppressed_profitable_rate": round(sum(1 for value in suppressed_net if value > 0.0) / max(len(suppressed_net), 1), 4),
            "suppressed_avg_net_pnl_bps": round(sum(suppressed_net) / len(suppressed_net), 2) if suppressed_net else 0.0,
            "suppressed_clean_followthrough_rate": round(sum(1 for row in suppressed_valid if row.get("label") == "clean_followthrough") / max(len(suppressed_valid), 1), 4),
            "suppressed_bad_entry_rate": round(
                sum(1 for row in suppressed_valid if row.get("label") in {"bad_entry", "followthrough_but_bad_entry"})
                / max(len(suppressed_valid), 1),
                4,
            ),
            "suppressed_absorption_reversal_rate": round(sum(1 for row in suppressed_valid if row.get("label") == "absorption_reversal") / max(len(suppressed_valid), 1), 4),
            "blocked_saved_rate_estimate": round(sum(1 for row in blocked_valid if row.get("label") == "absorption_reversal") / max(len(blocked_valid), 1), 4),
            "blocked_false_block_rate_estimate": round(sum(1 for row in blocked_valid if (_to_signed_float(row.get("net_pnl_bps")) or 0.0) > 0.0) / max(len(blocked_valid), 1), 4),
            "shadow_replay_count": shadow_count,
            "top_positive_profiles": positive,
            "top_negative_profiles": negative,
            "recommended_profile_actions": [
                {"profile_key": row.get("profile_key"), "recommended_action": row.get("recommended_action")}
                for row in profile_rows[:10]
            ],
            **replay_profile_summary(profile_rows),
        }
    )


def _finalize_replay_coverage(summary: dict[str, Any], replay_rows: list[dict[str, Any]]) -> None:
    eligibility = summary.get("eligibility_summary")
    if not isinstance(eligibility, dict):
        eligibility = _empty_eligibility_summary()
    replay_count = len(replay_rows)
    raw_count = int(eligibility.get("raw_audit_universe_count") or 0)
    candidate_count = int(eligibility.get("replay_candidate_universe_count") or 0)
    eligible_count = int(eligibility.get("eligible_replay_universe_count") or eligibility.get("eligible_count") or 0)
    raw_rate = round(replay_count / raw_count, 4) if raw_count else 0.0
    candidate_rate = round(replay_count / candidate_count, 4) if candidate_count else 0.0
    eligible_rate = round(replay_count / eligible_count, 4) if eligible_count else 0.0
    warning = ""
    if candidate_count >= 10 and candidate_rate < 0.25:
        warning = f"replay_coverage_candidate_low:{candidate_rate}"
    eligibility.update(
        {
            "raw_audit_universe_count": raw_count,
            "replay_candidate_universe_count": candidate_count,
            "eligible_replay_universe_count": eligible_count,
            "replay_coverage_rate_raw": raw_rate,
            "replay_coverage_rate_candidate": candidate_rate,
            "replay_coverage_rate_eligible": eligible_rate,
            "replay_coverage_rate": candidate_rate,
            "replay_coverage_warning": warning,
        }
    )
    summary["eligibility_summary"] = eligibility
    summary["raw_audit_universe_count"] = raw_count
    summary["replay_candidate_universe_count"] = candidate_count
    summary["eligible_replay_universe_count"] = eligible_count
    summary["replay_coverage_rate_raw"] = raw_rate
    summary["replay_coverage_rate_candidate"] = candidate_rate
    summary["replay_coverage_rate_eligible"] = eligible_rate
    summary["replay_coverage_rate"] = candidate_rate
    summary["replay_coverage_warning"] = warning


def run_trade_replay(
    date_str: str,
    *,
    db_path: str | Path | None = None,
    dry_run: bool = False,
    include_suppressed: bool = False,
    include_blocked: bool = True,
    replay_scope: str | None = None,
) -> dict[str, Any]:
    """Read-only DATE-scoped replay summary for closure validation."""
    diagnostics = ReplayDiagnostics()
    logical_date = str(date_str or "").strip()
    resolved_path = Path(db_path) if db_path else _resolve_db_path()
    normalized_scope = _normalize_replay_scope(
        replay_scope,
        include_suppressed=include_suppressed,
        include_blocked=include_blocked,
    )
    strategy_config = _strategy_config_payload(
        replay_scope=normalized_scope,
        include_suppressed=include_suppressed,
        include_blocked=include_blocked,
    )
    strategy_hash = _strategy_config_hash(
        replay_scope=normalized_scope,
        include_suppressed=include_suppressed,
        include_blocked=include_blocked,
    )
    summary: dict[str, Any] = {
        "logical_date": logical_date,
        "replay_scope": normalized_scope,
        "strategy_config_hash": strategy_hash,
        "current_strategy_config": strategy_config,
        "replay_source": "missing",
        "persisted_rows_found": 0,
        "timezone": "Asia/Shanghai",
        "logical_window_start_utc": "",
        "logical_window_end_utc": "",
        "logical_window_start_beijing": "",
        "logical_window_end_beijing": "",
        "status": "ok",
        "replay_count": 0,
        "valid_replay_count": 0,
        "valid_count": 0,
        "sample_insufficient": True,
        "trade_replay_available": False,
        "warnings": [],
        "query_errors": [],
        "price_errors": [],
        "schema_errors": [],
        "input_source_counts": _empty_input_source_counts(),
        "eligibility_summary": _empty_eligibility_summary(),
        "results": [],
        "dry_run": bool(dry_run),
        "include_suppressed": bool(include_suppressed),
        "include_blocked": bool(include_blocked),
        "db_path": str(resolved_path),
        "win_rate": 0.0,
        "avg_net_pnl_bps": 0.0,
        "clean_followthrough_rate": 0.0,
        "bad_entry_rate": 0.0,
        "absorption_reversal_rate": 0.0,
        "chop_rate": 0.0,
        "data_invalid_rate": 0.0,
        "suppressed_replay_count": 0,
        "suppressed_profitable_rate": 0.0,
        "suppressed_avg_net_pnl_bps": 0.0,
        "suppressed_clean_followthrough_rate": 0.0,
        "suppressed_bad_entry_rate": 0.0,
        "suppressed_absorption_reversal_rate": 0.0,
        "suppressed_replay_zero_reasons": [],
        "blocked_saved_rate_estimate": 0.0,
        "blocked_false_block_rate_estimate": 0.0,
        "shadow_replay_count": 0,
        "shadow_funnel_summary": _shadow_funnel_summary(candidates=[], replay_rows=[]),
        "top_positive_profiles": [],
        "top_negative_profiles": [],
        "recommended_profile_actions": [],
        "replay_profile_count": 0,
        "replay_profile_blocker_count": 0,
        "sampled_negative_profiles": [],
        "blocker_grade_negative_profiles": [],
        "high_confidence_negative_profiles": [],
        "high_confidence_positive_profiles": [],
        "low_sample_positive_profiles": [],
        "low_sample_profiles_count": 0,
        "profile_unknown_diagnostics": {
            "dimension_names": [
                "asset",
                "side",
                "lp_stage",
                "sweep_phase",
                "market_timing",
                "absorption_context",
                "asset_class",
                "basis_bucket",
                "quality_bucket",
            ],
            "example_profile_key_format": "asset|side|lp_stage|sweep_phase|market_timing|absorption_context|asset_class|basis_bucket|quality_bucket",
            "profile_unknown_field_rate": 0.0,
            "profile_unknown_field_count": 0,
            "profile_unknown_profile_count": 0,
            "unknown_by_dimension": {},
            "unknown_missing_sources": {},
            "unknown_rate_by_dimension": {},
            "top_unknown_profiles": [],
        },
        "persistence_summary": {
            "enabled": False,
            "replay_scope": normalized_scope,
            "strategy_config_hash": strategy_hash,
            "profile_stats_scope": "profile_key_latest_and_daily",
            "idempotent_key": "trade_replay_examples.replay_id=logical_date+replay_scope+signal_id_or_trade_opportunity_id+strategy_config_hash",
            "examples_written": 0,
            "profile_stats_written": 0,
            "profile_daily_stats_written": 0,
        },
    }
    if not logical_date:
        diagnostics.query_error("missing_date")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary
    try:
        window = _logical_window(logical_date)
        summary.update(
            {
                "timezone": window["timezone"],
                "logical_window_start_utc": window["logical_window_start_utc"],
                "logical_window_end_utc": window["logical_window_end_utc"],
                "logical_window_start_beijing": window["logical_window_start_beijing"],
                "logical_window_end_beijing": window["logical_window_end_beijing"],
            }
        )
    except ValueError as exc:
        diagnostics.query_error(f"invalid_date:{logical_date}:{exc}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary

    if not resolved_path.exists():
        diagnostics.query_error(f"sqlite_db_missing:{resolved_path}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary

    try:
        if dry_run:
            conn = sqlite3.connect(f"file:{resolved_path}?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(str(resolved_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        diagnostics.query_error(f"sqlite_open_failed:{exc}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary

    try:
        if not dry_run:
            _ensure_replay_persistence_schema(conn)
            conn.commit()
        if dry_run:
            persisted_rows = _load_persisted_replay_rows(
                conn,
                logical_date,
                diagnostics,
                replay_scope=normalized_scope,
                strategy_config_hash=strategy_hash,
            )
            if persisted_rows:
                persisted_hash = str(persisted_rows[0].get("strategy_config_hash") or "")
                if persisted_hash and persisted_hash != strategy_hash:
                    summary["strategy_config_hash"] = persisted_hash
                    strategy_hash = persisted_hash
                summary["persisted_rows_found"] = len(persisted_rows)
                summary["replay_source"] = "persisted"
                profile_rows = _profile_rows(
                    conn,
                    diagnostics,
                    logical_date=logical_date,
                    replay_scope=normalized_scope,
                    strategy_config_hash=strategy_hash,
                )
                _augment_summary_from_replays(summary, persisted_rows, profile_rows)
                _finalize_replay_coverage(summary, persisted_rows)
                summary["shadow_funnel_summary"] = _shadow_funnel_summary(candidates=[], replay_rows=persisted_rows)
                summary["suppressed_replay_zero_reasons"] = _suppressed_replay_zero_reasons(
                    include_suppressed=include_suppressed,
                    input_source_counts=summary["input_source_counts"],
                    eligibility_summary=summary["eligibility_summary"],
                    suppressed_replay_count=int(summary.get("suppressed_replay_count") or 0),
                )
                summary["results"] = [
                    {
                        "signal_id": str(row.get("signal_id") or ""),
                        "trade_opportunity_id": str(row.get("trade_opportunity_id") or ""),
                        "asset": str(row.get("asset") or ""),
                        "pair": str(row.get("pair") or ""),
                        "direction": str(row.get("direction") or row.get("side") or ""),
                        "input_source": list(row.get("input_source") or []),
                        "signal_ts": _to_int(row.get("signal_ts") or row.get("archive_ts") or row.get("timestamp")),
                        "data_valid": _is_truthy(row.get("data_valid", 1)),
                        "invalid_reason": row.get("invalid_reason"),
                        "label": row.get("label"),
                        "entry_price": _to_float(row.get("entry_price")),
                        "exit_price": _to_float(row.get("exit_price")),
                        "net_pnl_bps": _to_signed_float(row.get("net_pnl_bps")),
                    }
                    for row in persisted_rows[:10]
                ]
                if diagnostics.schema_errors or diagnostics.query_errors or diagnostics.price_errors:
                    summary["status"] = "degraded"
                summary.update(diagnostics.as_dict())
                return summary
        candidates, input_source_counts, _ = _collect_trade_replay_inputs(
            conn,
            window=window,
            include_suppressed=include_suppressed,
            include_blocked=include_blocked,
            replay_diagnostics=diagnostics,
        )
        eligible_inputs, eligibility_summary = _evaluate_replay_inputs(
            candidates,
            include_suppressed=include_suppressed,
            include_blocked=include_blocked,
            raw_audit_universe_count=int(input_source_counts.get("delivery_audit") or 0),
        )
        summary["input_source_counts"] = input_source_counts
        summary["eligibility_summary"] = eligibility_summary

        persisted_rows = _load_persisted_replay_rows(
            conn,
            logical_date,
            diagnostics,
            replay_scope=normalized_scope,
            strategy_config_hash=strategy_hash,
        )
        if persisted_rows:
            persisted_hash = str(persisted_rows[0].get("strategy_config_hash") or "")
            if persisted_hash and persisted_hash != strategy_hash:
                summary["strategy_config_hash"] = persisted_hash
                strategy_hash = persisted_hash
        summary["persisted_rows_found"] = len(persisted_rows)
        replay_rows = persisted_rows
        if not dry_run and eligible_inputs:
            replay_rows = _replay_dynamic_inputs(conn, eligible_inputs, replay_diagnostics=diagnostics)
            summary["persistence_summary"] = _persist_trade_replay_rows(
                conn,
                logical_date,
                replay_rows,
                diagnostics,
                replay_scope=normalized_scope,
                strategy_config_hash=strategy_hash,
                include_suppressed=include_suppressed,
                include_blocked=include_blocked,
            )
            summary["persisted_rows_found"] = int(summary["persistence_summary"].get("examples_written") or 0)
            summary["replay_source"] = "persisted" if replay_rows else "missing"
        elif not replay_rows:
            replay_rows = _replay_dynamic_inputs(conn, eligible_inputs, replay_diagnostics=diagnostics)
            summary["replay_source"] = "dry_run" if replay_rows else "missing"
        elif not dry_run:
            summary["persistence_summary"] = _persist_trade_replay_rows(
                conn,
                logical_date,
                replay_rows,
                diagnostics,
                replay_scope=normalized_scope,
                strategy_config_hash=strategy_hash,
                include_suppressed=include_suppressed,
                include_blocked=include_blocked,
            )
            summary["replay_source"] = "persisted"
        else:
            summary["replay_source"] = "persisted"
        profile_rows = _profile_rows(
            conn,
            diagnostics,
            logical_date=logical_date,
            replay_scope=normalized_scope,
            strategy_config_hash=strategy_hash,
        )

        _augment_summary_from_replays(summary, replay_rows, profile_rows)
        _finalize_replay_coverage(summary, replay_rows)
        summary["shadow_funnel_summary"] = _shadow_funnel_summary(candidates=candidates, replay_rows=replay_rows)
        summary["suppressed_replay_zero_reasons"] = _suppressed_replay_zero_reasons(
            include_suppressed=include_suppressed,
            input_source_counts=input_source_counts,
            eligibility_summary=eligibility_summary,
            suppressed_replay_count=int(summary.get("suppressed_replay_count") or 0),
        )

        preview: list[dict[str, Any]] = []
        for row in replay_rows[:10]:
            item = {
                "signal_id": str(row.get("signal_id") or ""),
                "trade_opportunity_id": str(row.get("trade_opportunity_id") or ""),
                "asset": str(row.get("asset") or ""),
                "pair": str(row.get("pair") or ""),
                "direction": str(row.get("direction") or row.get("side") or ""),
                "input_source": list(row.get("input_source") or []),
                "signal_ts": _to_int(row.get("signal_ts") or row.get("archive_ts") or row.get("timestamp")),
                "data_valid": _is_truthy(row.get("data_valid", 1)),
                "invalid_reason": row.get("invalid_reason"),
                "label": row.get("label"),
                "entry_price": _to_float(row.get("entry_price")),
                "exit_price": _to_float(row.get("exit_price")),
                "net_pnl_bps": _to_signed_float(row.get("net_pnl_bps")),
            }
            if not item["data_valid"]:
                item["invalid_reason"] = _normalize_invalid_reason(item.get("invalid_reason"), stage="entry")
            preview.append(item)
        summary["results"] = preview

        if not replay_rows:
            diagnostics.warning("trade_replay_missing")
            for reason in _trade_replay_missing_reasons(
                input_source_counts=input_source_counts,
                eligibility_summary=eligibility_summary,
                candidates=candidates,
                replay_results=replay_rows,
            ):
                diagnostics.warning(f"trade_replay_missing:{reason}")
            summary["status"] = "degraded"
        elif diagnostics.schema_errors or diagnostics.query_errors or diagnostics.price_errors:
            summary["status"] = "degraded"
        summary.update(diagnostics.as_dict())
        return summary
    except sqlite3.Error as exc:
        diagnostics.query_error(f"trade_replay_query_failed:{exc}")
        diagnostics.schema_error(f"schema_error:trade_replay_query_failed:{exc}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary
    finally:
        conn.close()


def _infer_profile_action(stats: dict[str, Any]) -> str:
    if evaluate_replay_profile_gate(stats).get("blocker") == "replay_profile_negative":
        return "block_profile"
    valid_count = int(stats.get("valid_count") or stats.get("valid_sample_count") or 0)
    if valid_count < MIN_PROFILE_REPLAY_SAMPLES:
        return "needs_more_samples"

    win_rate = stats.get("win_rate", 0.0)
    avg_pnl = stats.get("avg_net_pnl_bps", 0.0)
    absorption_rate = stats.get("absorption_rate", 0.0)
    bad_entry_rate = stats.get("bad_entry_rate", 0.0)

    if absorption_rate > 0.50 and avg_pnl < -10:
        return "hard_block"
    if bad_entry_rate > 0.60 and avg_pnl < -5:
        return "hard_block"
    if win_rate >= 0.70 and avg_pnl > 25:
        return "eligible_verified"
    if win_rate >= 0.60 and avg_pnl > 15:
        return "promote_candidate"
    if win_rate >= 0.45 and avg_pnl > -10:
        return "keep_observe_only"
    return "block_profile"


def _infer_profile_confidence(stats: dict[str, Any]) -> float:
    valid_count = int(stats.get("valid_count") or stats.get("valid_sample_count") or 0)
    if valid_count < MIN_PROFILE_REPLAY_SAMPLES:
        return 0.0
    sample_confidence = min(1.0, valid_count / 50.0)
    total_count = int(stats.get("sample_count") or valid_count)
    data_quality = valid_count / total_count if total_count > 0 else 0.0
    return round(sample_confidence * data_quality, 4)


def _get_entry_price(
    signal_row: dict[str, Any],
    *,
    entry_ts: int,
    state_manager=None,
    market_context_adapter=None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    diagnostics = replay_diagnostics or ReplayDiagnostics()
    outcome_price = _to_float(signal_row.get("outcome_price_start"))
    if outcome_price and outcome_price > 0:
        return {"valid": True, "price": outcome_price, "source": "outcome_price_start"}

    if OUTCOME_USE_MARKET_CONTEXT_PRICE and market_context_adapter:
        token_or_pair = str(signal_row.get("pair_label") or signal_row.get("asset_symbol") or "")
        if token_or_pair:
            try:
                context = market_context_adapter.get_market_context(token_or_pair, entry_ts)
                price, source = _select_price_from_context(context)
                if price:
                    return {"valid": True, "price": price, "source": source}
                diagnostics.price_error("market_context_unavailable:entry")
            except Exception as exc:
                diagnostics.price_error(f"market_context_lookup_failed:entry:{exc}")
                return {"valid": False, "price": None, "source": "unavailable", "reason": "market_context_unavailable"}

    if state_manager and hasattr(state_manager, "get_latest_pool_quote_proxy"):
        pool_address = str(signal_row.get("pool_address") or "")
        if pool_address:
            try:
                price = state_manager.get_latest_pool_quote_proxy(pool_address, max_age_sec=300, now_ts=entry_ts)
                if price:
                    return {"valid": True, "price": price, "source": "pool_quote_proxy"}
            except Exception as exc:
                diagnostics.price_error(f"pool_quote_proxy_failed:entry:{exc}")
                return {"valid": False, "price": None, "source": "unavailable", "reason": "no_entry_price"}

    return {"valid": False, "price": None, "source": "unavailable", "reason": "no_entry_price"}


def _simulate_hold(
    signal_row: dict[str, Any],
    *,
    entry_price: float,
    entry_ts: int,
    direction: str,
    max_hold_sec: int,
    stop_loss_bps: float,
    take_profit_bps: float,
    state_manager=None,
    market_context_adapter=None,
    replay_diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    diagnostics = replay_diagnostics or ReplayDiagnostics()
    exit_ts = entry_ts + max_hold_sec

    outcome_60s_price = _to_float(signal_row.get("outcome_60s_price_end"))
    if outcome_60s_price and outcome_60s_price > 0 and max_hold_sec <= 60:
        move_bps = _calc_gross_pnl_bps(entry_price, outcome_60s_price, direction)
        if move_bps <= -stop_loss_bps:
            return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "stop_loss"}
        if move_bps >= take_profit_bps:
            return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "take_profit"}
        return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "max_hold"}

    if market_context_adapter:
        token_or_pair = str(signal_row.get("pair_label") or signal_row.get("asset_symbol") or "")
        if token_or_pair:
            try:
                context = market_context_adapter.get_market_context(token_or_pair, exit_ts)
                price, source = _select_price_from_context(context)
                if price:
                    move_bps = _calc_gross_pnl_bps(entry_price, price, direction)
                    if move_bps <= -stop_loss_bps:
                        reason = "stop_loss"
                    elif move_bps >= take_profit_bps:
                        reason = "take_profit"
                    else:
                        reason = "max_hold"
                    return {"valid": True, "price": price, "ts": exit_ts, "source": source, "reason": reason}
                diagnostics.price_error("market_context_unavailable:exit")
            except Exception as exc:
                diagnostics.price_error(f"market_context_lookup_failed:exit:{exc}")
                return {"valid": False, "price": None, "ts": exit_ts, "source": "unavailable", "reason": "market_context_unavailable"}

    return {"valid": False, "price": None, "ts": exit_ts, "source": "unavailable", "reason": "no_exit_price"}


def _calc_mfe_mae(
    signal_row: dict[str, Any],
    *,
    entry_price: float,
    entry_ts: int,
    direction: str,
    max_hold_sec: int,
    state_manager=None,
    market_context_adapter=None,
) -> tuple[float | None, float | None]:
    prices = []
    if max_hold_sec >= 30:
        price_30s = _to_float(signal_row.get("outcome_30s_price_end"))
        if price_30s:
            prices.append(price_30s)
    if max_hold_sec >= 60:
        price_60s = _to_float(signal_row.get("outcome_60s_price_end"))
        if price_60s:
            prices.append(price_60s)
    if not prices:
        return None, None
    moves = [_calc_gross_pnl_bps(entry_price, p, direction) for p in prices]
    mfe = max(moves) if moves else None
    mae = min(moves) if moves else None
    return mfe, mae


def _calc_gross_pnl_bps(entry_price: float, exit_price: float, direction: str) -> float:
    if direction == "LONG":
        return ((exit_price / entry_price) - 1.0) * 10000
    if direction == "SHORT":
        return ((entry_price / exit_price) - 1.0) * 10000
    return 0.0


def _classify_replay_label(
    *,
    net_pnl_bps: float,
    mfe_bps: float | None,
    mae_bps: float | None,
    close_reason: str,
    direction: str,
) -> str:
    if net_pnl_bps > 15:
        if mfe_bps is not None and mfe_bps > 20 and (mae_bps is None or mae_bps > -10):
            return "clean_followthrough"
    if mae_bps is not None and mae_bps < -20 and net_pnl_bps < 0:
        return "bad_entry"
    if mfe_bps is not None and mfe_bps > 15 and net_pnl_bps < -10:
        return "absorption_reversal"
    if mfe_bps is not None and mae_bps is not None and abs(mfe_bps) < 15 and abs(mae_bps) < 15:
        return "chop_no_edge"
    if net_pnl_bps > 0:
        return "small_win"
    if net_pnl_bps < 0:
        return "small_loss"
    return "neutral"


def _select_price_from_context(context: dict[str, Any]) -> tuple[float | None, str]:
    venue = str(context.get("market_context_venue") or context.get("venue") or "").strip().lower()
    mark_price = _to_float(context.get("perp_mark_price") or context.get("mark_price"))
    index_price = _to_float(context.get("perp_index_price") or context.get("index_price"))
    last_price = _to_float(context.get("perp_last_price") or context.get("last_price") or context.get("price"))
    if venue == "okx_perp" and OUTCOME_PREFER_OKX_MARK and mark_price:
        return mark_price, "okx_mark"
    if index_price:
        return index_price, f"{venue}_index" if venue else "index"
    if mark_price:
        return mark_price, f"{venue}_mark" if venue else "mark"
    if last_price:
        return last_price, f"{venue}_last" if venue else "last"
    return None, "unavailable"


def _lookup_price_from_outcomes(
    conn: sqlite3.Connection,
    *,
    signal_id: str,
    asset: str,
    pair: str,
    ts: int,
    replay_diagnostics: ReplayDiagnostics,
) -> dict[str, Any]:
    if not _table_exists(conn, "outcomes"):
        replay_diagnostics.schema_error("schema_error:missing_table:outcomes")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    columns = _table_columns(conn, "outcomes", replay_diagnostics)
    if "signal_id" not in columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:outcomes:signal_id")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    price_columns = [
        column
        for column in ("start_price", "entry_price", "outcome_price_start", "price_start", "end_price", "exit_price", "outcome_price_end", "price_end")
        if column in columns
    ]
    if not price_columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:outcomes:start_price,entry_price,end_price,exit_price")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    select_columns = ["signal_id", *[column for column in ("asset", "pair") if column in columns], *price_columns]
    time_columns = [column for column in ("completed_at", "created_at", "due_at", "updated_at") if column in columns]
    time_expr = f"COALESCE({', '.join(time_columns)}, 0)" if time_columns else "0"
    try:
        row = conn.execute(
            f"SELECT {', '.join(select_columns)} FROM outcomes "
            f"WHERE signal_id = ? ORDER BY ABS({time_expr} - ?) ASC, {time_expr} ASC LIMIT 1",
            (signal_id, ts),
        ).fetchone()
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"outcomes_lookup_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:outcomes_lookup_failed:{exc}")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    if row is None:
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    row_asset = str(row["asset"] or "").strip().upper() if "asset" in columns else ""
    row_pair = str(row["pair"] or "").strip().upper() if "pair" in columns else ""
    if asset and row_asset and row_asset != asset:
        replay_diagnostics.price_error(f"outcomes_asset_mismatch:{signal_id}:{row_asset}!={asset}")
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    if pair and row_pair and row_pair != pair:
        replay_diagnostics.price_error(f"outcomes_pair_mismatch:{signal_id}:{row_pair}!={pair}")
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    price = _row_first_float(row, ("start_price", "entry_price", "outcome_price_start", "price_start", "end_price", "exit_price", "outcome_price_end", "price_end"))
    if price is None:
        replay_diagnostics.price_error(f"outcomes_price_missing:{signal_id}")
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    return {"price": price, "source": "outcomes.signal_id", "reason": None}


def _lookup_price_from_opportunity_outcomes(
    conn: sqlite3.Connection,
    *,
    trade_opportunity_id: str,
    asset: str,
    ts: int,
    replay_diagnostics: ReplayDiagnostics,
) -> dict[str, Any]:
    if not _table_exists(conn, "opportunity_outcomes"):
        replay_diagnostics.schema_error("schema_error:missing_table:opportunity_outcomes")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    columns = _table_columns(conn, "opportunity_outcomes", replay_diagnostics)
    if "trade_opportunity_id" not in columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:opportunity_outcomes:trade_opportunity_id")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    price_columns = [
        column
        for column in ("start_price", "entry_price", "outcome_price_start", "price_start", "end_price", "exit_price", "outcome_price_end", "price_end")
        if column in columns
    ]
    if not price_columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:opportunity_outcomes:start_price,entry_price,end_price,exit_price")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    select_columns = ["trade_opportunity_id", *price_columns]
    time_columns = [column for column in ("completed_at", "created_at", "due_at", "updated_at") if column in columns]
    time_expr = f"COALESCE({', '.join(time_columns)}, 0)" if time_columns else "0"
    try:
        row = conn.execute(
            f"SELECT {', '.join(select_columns)} FROM opportunity_outcomes "
            f"WHERE trade_opportunity_id = ? ORDER BY ABS({time_expr} - ?) ASC, {time_expr} ASC LIMIT 1",
            (trade_opportunity_id, ts),
        ).fetchone()
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"opportunity_outcomes_lookup_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:opportunity_outcomes_lookup_failed:{exc}")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    if row is None:
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    price = _row_first_float(row, ("start_price", "entry_price", "outcome_price_start", "price_start", "end_price", "exit_price", "outcome_price_end", "price_end"))
    if price is None:
        replay_diagnostics.price_error(f"opportunity_outcomes_price_missing:{trade_opportunity_id}")
        return {"price": None, "source": "unavailable", "reason": "no_price_for_signal_id"}
    return {"price": price, "source": "opportunity_outcomes.trade_opportunity_id", "reason": None}


def _lookup_price_from_market_context(
    conn: sqlite3.Connection,
    *,
    asset: str,
    pair: str,
    ts: int,
    prefer_source: str,
    replay_diagnostics: ReplayDiagnostics,
) -> dict[str, Any]:
    if not _table_exists(conn, "market_context_snapshots"):
        replay_diagnostics.schema_error("schema_error:missing_table:market_context_snapshots")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    columns = _table_columns(conn, "market_context_snapshots", replay_diagnostics)
    if "asset" not in columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:market_context_snapshots:asset")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    time_column = next((column for column in ("created_at", "timestamp", "ts", "archive_ts", "updated_at") if column in columns), "")
    if not time_column:
        replay_diagnostics.schema_error("schema_error:missing_columns:market_context_snapshots:created_at")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    price_columns = [
        column
        for column in ("perp_mark_price", "perp_index_price", "perp_last_price", "mark_price", "index_price", "last_price", "price")
        if column in columns
    ]
    if not price_columns:
        replay_diagnostics.schema_error("schema_error:missing_columns:market_context_snapshots:price")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    select_columns = ["asset", time_column, *[column for column in ("pair", "venue") if column in columns], *price_columns]
    try:
        row = conn.execute(
            f"SELECT {', '.join(select_columns)} "
            f"FROM market_context_snapshots WHERE asset = ? ORDER BY ABS(COALESCE({time_column}, 0) - ?) ASC, COALESCE({time_column}, 0) ASC LIMIT 1",
            (asset, ts),
        ).fetchone()
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"market_context_lookup_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:market_context_lookup_failed:{exc}")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    if row is None:
        replay_diagnostics.price_error(f"market_context_missing:{asset}:{pair}")
        return {"price": None, "source": "unavailable", "reason": "market_context_unavailable"}
    price, source = _select_price_from_context(dict(row))
    if price is None:
        venue = row["venue"] if "venue" in row.keys() else ""
        replay_diagnostics.price_error(f"market_context_price_missing:{asset}:{pair}:{venue}")
        return {"price": None, "source": "unavailable", "reason": "market_context_unavailable"}
    if prefer_source and prefer_source not in source:
        replay_diagnostics.warning(f"prefer_source_unmet:{prefer_source}:{source}")
    return {"price": price, "source": source or "market_context_snapshots", "reason": None}


def _row_first_float(row: sqlite3.Row, columns: tuple[str, ...]) -> float | None:
    keys = set(row.keys())
    for column in columns:
        if column in keys:
            price = _to_float(row[column])
            if price is not None:
                return price
    return None


def _invalid_replay(reason: str, **kwargs) -> dict[str, Any]:
    invalid_reason = reason
    return {
        "replay_id": "",
        "signal_id": kwargs.get("signal_id", ""),
        "trade_opportunity_id": kwargs.get("trade_opportunity_id", ""),
        "data_valid": False,
        "label": "data_invalid",
        "close_reason": invalid_reason,
        "invalid_reason": invalid_reason,
        "net_pnl_bps": 0.0,
        "gross_pnl_bps": 0.0,
        "entry_price": kwargs.get("entry_price"),
        "exit_price": None,
        "mfe_bps": None,
        "mae_bps": None,
        **kwargs,
    }


def _normalize_direction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"LONG", "BUY", "BUY_PRESSURE"}:
        return "LONG"
    if raw in {"SHORT", "SELL", "SELL_PRESSURE"}:
        return "SHORT"
    return raw


def _normalize_invalid_reason(reason: Any, *, stage: str) -> str:
    raw = str(reason or "").strip()
    if raw in {"schema_error", "market_context_unavailable", "no_price_for_signal_id", "no_entry_price", "no_exit_price"}:
        return raw
    if stage == "entry":
        if raw in {"entry_price_unavailable", "outcomes_price_missing"}:
            return "no_entry_price"
        return "no_entry_price"
    if raw in {"exit_price_unavailable"}:
        return "no_exit_price"
    return "no_exit_price"


def _resolve_db_path() -> Path:
    path = Path(str(SQLITE_DB_PATH or "data/chain_monitor.sqlite"))
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[1] / path


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str, replay_diagnostics: ReplayDiagnostics | None = None) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error as exc:
        if replay_diagnostics is not None:
            replay_diagnostics.schema_error(f"schema_error:pragma_failed:{table}:{exc}")
        return set()
    return {str(row[1]) for row in rows}


def _require_table_columns(
    conn: sqlite3.Connection,
    table: str,
    required: set[str],
    replay_diagnostics: ReplayDiagnostics,
) -> bool:
    if not _table_exists(conn, table):
        replay_diagnostics.schema_error(f"schema_error:missing_table:{table}")
        return False
    columns = _table_columns(conn, table, replay_diagnostics)
    missing = sorted(required - columns)
    if missing:
        replay_diagnostics.schema_error(f"schema_error:missing_columns:{table}:{','.join(missing)}")
        return False
    return True


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float | None:
    try:
        result = float(value)
        return result if result > 0 else None
    except (TypeError, ValueError):
        return None


def _to_signed_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trade replay closure summary")
    parser.add_argument("--date", required=True, help="Logical date YYYY-MM-DD")
    parser.add_argument("--format", default="text", choices=("text", "json"), help="Output format")
    parser.add_argument("--dry-run", action="store_true", help="Read summary only")
    parser.add_argument("--replay-scope", choices=tuple(sorted(REPLAY_SCOPES)), help="Replay persistence scope")
    parser.add_argument("--include-suppressed", action="store_true", help="Include suppressed replay-eligible research signals")
    parser.add_argument("--include-blocked", action="store_true", default=True, help="Include blocked/no-trade replay-eligible research signals")
    return parser


def _render_text(summary: dict[str, Any]) -> str:
    lines = [
        f"trade_replay logical_date={summary.get('logical_date')}",
        f"replay_scope={summary.get('replay_scope')}",
        f"replay_source={summary.get('replay_source')}",
        f"strategy_config_hash={summary.get('strategy_config_hash')}",
        f"persisted_rows_found={summary.get('persisted_rows_found')}",
        f"timezone={summary.get('timezone')}",
        f"logical_window_start_utc={summary.get('logical_window_start_utc')}",
        f"logical_window_end_utc={summary.get('logical_window_end_utc')}",
        f"status={summary.get('status')}",
        f"trade_replay_available={summary.get('trade_replay_available')}",
        f"replay_count={summary.get('replay_count')}",
        f"valid_replay_count={summary.get('valid_replay_count')}",
        f"sample_insufficient={summary.get('sample_insufficient')}",
        f"input_source_counts={json.dumps(summary.get('input_source_counts') or {}, ensure_ascii=False, sort_keys=True)}",
        f"eligibility_summary={json.dumps(summary.get('eligibility_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"shadow_funnel_summary={json.dumps(summary.get('shadow_funnel_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"suppressed_replay_zero_reasons={json.dumps(summary.get('suppressed_replay_zero_reasons') or [], ensure_ascii=False)}",
        f"persistence_summary={json.dumps(summary.get('persistence_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"warnings={json.dumps(summary.get('warnings') or [], ensure_ascii=False)}",
        f"query_errors={json.dumps(summary.get('query_errors') or [], ensure_ascii=False)}",
        f"price_errors={json.dumps(summary.get('price_errors') or [], ensure_ascii=False)}",
        f"schema_errors={json.dumps(summary.get('schema_errors') or [], ensure_ascii=False)}",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = run_trade_replay(
        args.date,
        dry_run=bool(args.dry_run),
        include_suppressed=bool(args.include_suppressed),
        include_blocked=bool(args.include_blocked),
        replay_scope=args.replay_scope,
    )
    if args.format == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(_render_text(summary), end="")
    return 0 if summary.get("status") in {"ok", "degraded"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
