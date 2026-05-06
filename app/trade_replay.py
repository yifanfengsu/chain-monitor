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
import os
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
    repair_profile_rows_with_sources,
    replay_profile_summary,
)
from trade_opportunity import BLOCKER_PRIORITY, choose_primary_blocker


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
LP_SUPPRESSION_SAMPLE_SCOPE = "lp_suppression_sample"
LP_SUPPRESSION_SAMPLE_SOURCE = "sampled_from_delivery_audit"
LP_SUPPRESSION_SAMPLE_LIMIT_DEFAULT = 100
LP_SUPPRESSION_SAMPLE_MIN_SAMPLES = 10
LP_SUPPRESSION_SAMPLE_REASONS = {
    "gate/lp_noise_filtered",
    "listener_prefilter/drop",
}
LP_SUPPRESSION_SAMPLE_REVIEW_PROFITABLE_RATE = 0.55

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
REPLAY_PROFILE_NEGATIVE_BLOCKER = "replay_profile_negative"


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
            if str(outcome_result.get("reason") or "") in {"outcome_asset_mismatch", "outcome_pair_mismatch"}:
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
            "lp_stage_context",
            "market_context",
            "profile_features",
            "opportunity_profile_features_json",
            "features_json",
            "signal_json",
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


LP_SAMPLE_QUOTE_SYMBOLS = {
    "USDC",
    "USDT",
    "USD",
    "DAI",
    "USDE",
    "USDS",
    "FRAX",
    "TUSD",
    "USDP",
    "PYUSD",
    "GUSD",
}
LP_SAMPLE_ASSET_ALIASES = {
    "WETH": "ETH",
    "ETH.E": "ETH",
    "WBTC": "BTC",
    "CBBTC": "BTC",
    "BTC.B": "BTC",
    "WSOL": "SOL",
    "SOL.E": "SOL",
    "USDC.E": "USDC",
    "USDT.E": "USDT",
}


def _normalize_lp_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    text = text.replace("$", "").replace(" ", "")
    return LP_SAMPLE_ASSET_ALIASES.get(text, text)


def _is_lp_quote_symbol(value: Any) -> bool:
    return _normalize_lp_symbol(value) in LP_SAMPLE_QUOTE_SYMBOLS


def _lp_asset_aliases(asset: str) -> tuple[str, ...]:
    normalized = _normalize_lp_symbol(asset)
    if normalized == "ETH":
        return ("ETH", "WETH", "ETH.E")
    if normalized == "BTC":
        return ("BTC", "WBTC", "CBBTC", "BTC.B")
    if normalized == "SOL":
        return ("SOL", "WSOL", "SOL.E")
    aliases = {normalized}
    aliases.update(alias for alias, canonical in LP_SAMPLE_ASSET_ALIASES.items() if canonical == normalized)
    return tuple(sorted(alias for alias in aliases if alias))


def _normalize_lp_pair_value(value: Any) -> dict[str, Any]:
    text = str(value or "").strip().upper()
    if not text:
        return {"base": None, "quote": None, "pair": None, "invalid_reason": None}
    text = text.replace("-", "/").replace("_", "/").replace(" ", "")
    if "/" not in text:
        symbol = _normalize_lp_symbol(text)
        return {
            "base": None,
            "quote": symbol if _is_lp_quote_symbol(symbol) else None,
            "pair": None,
            "invalid_reason": "asset_pair_ambiguous",
            "single_asset_pair": symbol or text,
        }
    parts = [part for part in text.split("/") if part]
    if len(parts) != 2:
        return {"base": None, "quote": None, "pair": None, "invalid_reason": "asset_pair_ambiguous"}
    left = _normalize_lp_symbol(parts[0])
    right = _normalize_lp_symbol(parts[1])
    if not left or not right or left == right:
        return {"base": None, "quote": None, "pair": None, "invalid_reason": "asset_pair_ambiguous"}
    if _is_lp_quote_symbol(left) and not _is_lp_quote_symbol(right):
        base, quote = right, left
    else:
        base, quote = left, right
    if _is_lp_quote_symbol(base) and _is_lp_quote_symbol(quote):
        return {"base": None, "quote": None, "pair": None, "invalid_reason": "asset_pair_ambiguous"}
    return {
        "base": base,
        "quote": quote,
        "pair": f"{base}/{quote}",
        "invalid_reason": None,
    }


def _iter_lp_pair_candidates(row: dict[str, Any]) -> list[tuple[str, Any]]:
    payloads = _expanded_payloads(row, [])
    candidates: list[tuple[str, Any]] = []

    def add(source: str, value: Any) -> None:
        if value not in (None, "", [], {}):
            candidates.append((source, value))

    for key in ("drop_pair", "drop_metadata_pair", "pair", "pair_label"):
        add(key, row.get(key))
    for key in ("pool_pair", "pool_pair_label", "lp_pair", "lp_pair_label"):
        add(key, row.get(key))
    for key in ("signal_pair", "signal_pair_label"):
        add(key, row.get(key))
    for payload_key in ("signal_json", "signal_payload"):
        payload = _from_json(row.get(payload_key), row.get(payload_key))
        if isinstance(payload, dict):
            for key in ("pair", "pair_label", "pool_pair", "lp_pair"):
                add(f"{payload_key}.{key}", payload.get(key))
    for key in ("audit_pair", "audit_pair_label", "original_pair"):
        add(key, row.get(key))
    for payload_key in ("audit_json", "audit_payload"):
        payload = _from_json(row.get(payload_key), row.get(payload_key))
        if isinstance(payload, dict):
            for key in ("pair", "pair_label", "pool_pair", "lp_pair"):
                add(f"{payload_key}.{key}", payload.get(key))
    for payload in payloads:
        for key in ("pool_pair", "pool_pair_label", "lp_pair", "lp_pair_label", "pair", "pair_label"):
            add(f"payload.{key}", payload.get(key))
        lp_context = payload.get("lp_context")
        if isinstance(lp_context, dict):
            for key in ("pair_label", "pool_pair", "lp_pair"):
                add(f"lp_context.{key}", lp_context.get(key))
    return candidates


def _iter_lp_asset_candidates(row: dict[str, Any]) -> list[tuple[str, Any]]:
    payloads = _expanded_payloads(row, [])
    candidates: list[tuple[str, Any]] = []

    def add(source: str, value: Any) -> None:
        if value not in (None, "", [], {}):
            candidates.append((source, value))

    for key in (
        "drop_asset",
        "drop_base",
        "drop_base_symbol",
        "asset",
        "asset_symbol",
        "base",
        "base_symbol",
        "base_token_symbol",
    ):
        add(key, row.get(key))
    for key in ("signal_asset", "signal_asset_symbol", "signal_base_token_symbol"):
        add(key, row.get(key))
    for payload_key in ("signal_json", "signal_payload"):
        payload = _from_json(row.get(payload_key), row.get(payload_key))
        if isinstance(payload, dict):
            for key in ("asset", "asset_symbol", "base", "base_symbol", "base_token_symbol"):
                add(f"{payload_key}.{key}", payload.get(key))
    for key in ("audit_asset", "audit_asset_symbol", "original_asset", "token_symbol"):
        add(key, row.get(key))
    for payload_key in ("audit_json", "audit_payload"):
        payload = _from_json(row.get(payload_key), row.get(payload_key))
        if isinstance(payload, dict):
            for key in ("asset", "asset_symbol", "base", "base_symbol", "base_token_symbol", "token_symbol"):
                add(f"{payload_key}.{key}", payload.get(key))
    for payload in payloads:
        for key in ("asset", "asset_symbol", "base", "base_symbol", "base_token_symbol", "token_symbol"):
            add(f"payload.{key}", payload.get(key))
        lp_context = payload.get("lp_context")
        if isinstance(lp_context, dict):
            for key in ("base_token_symbol", "asset", "asset_symbol", "token_symbol"):
                add(f"lp_context.{key}", lp_context.get(key))
    return candidates


def _infer_lp_pair_from_tokens(row: dict[str, Any]) -> dict[str, Any]:
    payloads = _expanded_payloads(row, [])
    containers: list[tuple[str, dict[str, Any]]] = [("row", row)]
    for key in ("signal_json", "signal_payload", "audit_json", "audit_payload", "metadata"):
        payload = _from_json(row.get(key), row.get(key))
        if isinstance(payload, dict):
            containers.append((key, payload))
    for payload in payloads:
        containers.append(("payload", payload))
        lp_context = payload.get("lp_context")
        if isinstance(lp_context, dict):
            containers.append(("lp_context", lp_context))

    token_key_pairs = (
        ("base_token_symbol", "quote_token_symbol"),
        ("base_symbol", "quote_symbol"),
        ("token0_symbol", "token1_symbol"),
        ("token_a_symbol", "token_b_symbol"),
    )
    for source, container in containers:
        for left_key, right_key in token_key_pairs:
            left = _normalize_lp_symbol(container.get(left_key))
            right = _normalize_lp_symbol(container.get(right_key))
            if not left or not right or left == right:
                continue
            if _is_lp_quote_symbol(left) and not _is_lp_quote_symbol(right):
                base, quote = right, left
            elif _is_lp_quote_symbol(right) and not _is_lp_quote_symbol(left):
                base, quote = left, right
            else:
                continue
            return {
                "asset": base,
                "pair": f"{base}/{quote}",
                "quote": quote,
                "base": base,
                "asset_pair_source": f"{source}.{left_key}+{right_key}",
                "invalid_reason": None,
            }
    return {
        "asset": None,
        "pair": None,
        "quote": None,
        "base": None,
        "asset_pair_source": "",
        "invalid_reason": None,
    }


def normalize_lp_sample_asset_pair(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize research-only LP replay asset/pair metadata without guessing."""
    pair_payload: dict[str, Any] | None = None
    pair_source = ""
    first_single_asset_pair = ""
    for source, value in _iter_lp_pair_candidates(row):
        parsed = _normalize_lp_pair_value(value)
        if parsed.get("pair"):
            pair_payload = parsed
            pair_source = source
            break
        if parsed.get("single_asset_pair") and not first_single_asset_pair:
            first_single_asset_pair = str(parsed.get("single_asset_pair") or "")
    if pair_payload is None:
        inferred = _infer_lp_pair_from_tokens(row)
        if inferred.get("pair"):
            pair_payload = {
                "base": inferred.get("base"),
                "quote": inferred.get("quote"),
                "pair": inferred.get("pair"),
                "invalid_reason": None,
            }
            pair_source = str(inferred.get("asset_pair_source") or "token_pair_inference")

    asset_source = ""
    asset_value = ""
    for source, value in _iter_lp_asset_candidates(row):
        normalized = _normalize_lp_symbol(value)
        if normalized:
            asset_source = source
            asset_value = normalized
            break

    if pair_payload and pair_payload.get("pair"):
        base = str(pair_payload.get("base") or "")
        quote = str(pair_payload.get("quote") or "")
        pair = str(pair_payload.get("pair") or "")
        if asset_value and asset_value != base and not _is_lp_quote_symbol(asset_value):
            return {
                "asset": base,
                "pair": pair,
                "quote": quote,
                "base": base,
                "asset_pair_source": f"{pair_source};asset={asset_source}",
                "invalid_reason": "asset_pair_mismatch",
            }
        return {
            "asset": base,
            "pair": pair,
            "quote": quote,
            "base": base,
            "asset_pair_source": f"{pair_source};asset={asset_source}" if asset_source else pair_source,
            "invalid_reason": None,
        }

    if first_single_asset_pair:
        return {
            "asset": None if _is_lp_quote_symbol(first_single_asset_pair) else _normalize_lp_symbol(asset_value or first_single_asset_pair),
            "pair": None,
            "quote": _normalize_lp_symbol(first_single_asset_pair) if _is_lp_quote_symbol(first_single_asset_pair) else None,
            "base": None,
            "asset_pair_source": "single_asset_pair",
            "invalid_reason": "asset_pair_ambiguous",
        }

    if asset_value:
        return {
            "asset": None if _is_lp_quote_symbol(asset_value) else asset_value,
            "pair": None,
            "quote": asset_value if _is_lp_quote_symbol(asset_value) else None,
            "base": None,
            "asset_pair_source": asset_source,
            "invalid_reason": "asset_pair_ambiguous",
        }

    return {
        "asset": None,
        "pair": None,
        "quote": None,
        "base": None,
        "asset_pair_source": "",
        "invalid_reason": "asset_pair_ambiguous",
    }


def _coerce_replay_eligible(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    return _is_truthy(value)


def _coerce_blockers(value: Any) -> list[str]:
    payload = _from_json(value, value)
    if isinstance(payload, dict):
        nested_blockers: list[str] = []
        for key in (
            "trade_opportunity_blockers",
            "trade_opportunity_hard_blockers",
            "trade_opportunity_verification_blockers",
            "blockers",
            "hard_blockers",
            "verification_blockers",
        ):
            for item in _coerce_blockers(payload.get(key)):
                text = str(item or "").strip()
                if text and text not in nested_blockers:
                    nested_blockers.append(text)
        if nested_blockers:
            return nested_blockers
        return [str(key) for key, item in payload.items() if _is_truthy(item) or item not in (None, "", [], {})]
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item or "").strip()]
    text = str(payload or "").strip()
    return [text] if text else []


def _append_unique_blockers(target: list[str], values: Any) -> list[str]:
    for item in _coerce_blockers(values):
        text = str(item or "").strip()
        if text and text not in target:
            target.append(text)
    return target


def _blockers_from_payload(
    row: dict[str, Any],
    opportunity_payload: dict[str, Any],
    *,
    row_keys: tuple[str, ...],
    payload_keys: tuple[str, ...],
) -> list[str]:
    blockers: list[str] = []
    for key in row_keys:
        _append_unique_blockers(blockers, row.get(key))
    for key in payload_keys:
        _append_unique_blockers(blockers, opportunity_payload.get(key))
    return blockers


def _blocker_priority_index(blocker: str) -> int:
    try:
        return BLOCKER_PRIORITY.index(str(blocker or ""))
    except ValueError:
        return len(BLOCKER_PRIORITY) + 1


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


LP_SAMPLE_DIRECTION_FIELDS = {
    "trade_action_key",
    "trade_action",
    "delivery_decision",
    "final_trading_output_label",
    "intent_type",
    "intent",
    "canonical_semantic_key",
    "side",
    "direction",
    "direction_bucket",
    "market_bias",
    "signal_type",
    "stage_reason",
    "stage",
    "lp_stage",
    "lp_alert_stage",
    "reason",
    "suppression_reason",
    "telegram_suppression_reason",
    "gate_reason",
}


LP_SAMPLE_DIRECTION_CONTAINER_KEYS = {
    "audit_json",
    "signal_json",
    "message_json",
    "metadata",
    "context",
    "signal_context",
    "signal_metadata",
    "event_metadata",
    "signal",
    "event",
    "trade_action",
    "trade_action_debug",
    "lp_stage_context",
    "market_context",
    "profile_features",
    "opportunity_profile_features_json",
    "features_json",
    "opportunity_json",
    "quality_snapshot",
}


def _lp_direction_key_name(path: str) -> str:
    key = str(path or "").rsplit(".", 1)[-1].lower()
    for prefix in ("signal_", "audit_", "telegram_", "parsed_", "original_"):
        if key.startswith(prefix):
            key = key[len(prefix):]
    return key


def _lp_sample_direction_values(value: Any, *, path: str = "", depth: int = 0) -> list[tuple[str, str, Any]]:
    if depth > 5:
        return []
    parsed = _from_json(value, value)
    values: list[tuple[str, str, Any]] = []
    if isinstance(parsed, dict):
        for key, item in parsed.items():
            key_text = str(key or "")
            child_path = f"{path}.{key_text}" if path else key_text
            normalized_key = _lp_direction_key_name(child_path)
            if normalized_key in LP_SAMPLE_DIRECTION_FIELDS and not isinstance(item, (dict, list)):
                values.append((child_path, normalized_key, item))
            if (
                isinstance(item, (dict, list))
                or key_text in LP_SAMPLE_DIRECTION_CONTAINER_KEYS
                or (isinstance(item, str) and item[:1] in {"{", "["})
            ):
                values.extend(_lp_sample_direction_values(item, path=child_path, depth=depth + 1))
    elif isinstance(parsed, list):
        for index, item in enumerate(parsed[:20]):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            values.extend(_lp_sample_direction_values(item, path=child_path, depth=depth + 1))
    return values


def _lp_direction_tokens(value: Any) -> set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    lowered = text.lower()
    normalized = "".join(ch.lower() if ch.isalnum() or ch in {"_", "/"} else "_" for ch in text)
    parts = {part for part in normalized.replace("/", "_").split("_") if part}
    tokens = set(parts)
    for marker in (
        "long_bias_observe",
        "short_bias_observe",
        "do_not_chase_long",
        "do_not_chase_short",
        "pool_buy_pressure",
        "pool_sell_pressure",
        "buy_pressure",
        "sell_pressure",
        "lp_noise_filtered",
        "listener_prefilter/drop",
        "listener_prefilter_drop",
        "no_trade_lock",
        "no_trade",
    ):
        if marker in lowered or marker.replace("/", "_") in normalized:
            tokens.add(marker)
    if "买入" in text:
        tokens.add("买入")
    if "卖出" in text:
        tokens.add("卖出")
    return tokens


def _lp_direction_votes_from_value(path: str, field_name: str, value: Any) -> list[dict[str, Any]]:
    tokens = _lp_direction_tokens(value)
    if not tokens:
        return []
    votes: list[dict[str, Any]] = []
    source = str(path or field_name or "unknown")

    def add(side: str, confidence: float, vote_source: str | None = None) -> None:
        votes.append(
            {
                "side": side,
                "direction_source": vote_source or source,
                "direction_confidence": confidence,
            }
        )

    do_not_present = "do_not_chase_long" in tokens or "do_not_chase_short" in tokens
    strong_present = bool(
        tokens
        & {
            "long_bias_observe",
            "short_bias_observe",
            "pool_buy_pressure",
            "pool_sell_pressure",
            "buy_pressure",
            "sell_pressure",
        }
    )
    if "do_not_chase_long" in tokens:
        add("long", 0.55, "do_not_chase_long")
    if "do_not_chase_short" in tokens:
        add("short", 0.55, "do_not_chase_short")
    if do_not_present and not strong_present:
        return votes

    if "long_bias_observe" in tokens:
        add("long", 0.95)
    if "short_bias_observe" in tokens:
        add("short", 0.95)
    if "pool_buy_pressure" in tokens or "buy_pressure" in tokens:
        add("long", 0.9)
    if "pool_sell_pressure" in tokens or "sell_pressure" in tokens:
        add("short", 0.9)

    if field_name in {"side", "direction", "direction_bucket"}:
        if tokens & {"long", "buy", "bullish", "买入"}:
            add("long", 0.9)
        if tokens & {"short", "sell", "bearish", "卖出"}:
            add("short", 0.9)
    elif field_name == "market_bias":
        if tokens & {"long", "bullish"}:
            add("long", 0.8)
        if tokens & {"short", "bearish"}:
            add("short", 0.8)
    elif field_name in {"intent_type", "intent", "canonical_semantic_key", "trade_action_key", "trade_action", "signal_type"}:
        if tokens & {"long", "buy", "bullish"}:
            add("long", 0.85)
        if tokens & {"short", "sell", "bearish"}:
            add("short", 0.85)

    return votes


def infer_lp_sample_replay_side(row: dict[str, Any]) -> dict[str, Any]:
    """Infer research-only replay direction for early LP suppression samples."""
    votes: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for path, field_name, value in _lp_sample_direction_values(row):
        for vote in _lp_direction_votes_from_value(path, field_name, value):
            key = (
                str(vote.get("side") or ""),
                str(vote.get("direction_source") or ""),
                str(vote.get("direction_confidence") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            votes.append(vote)

    sides = {str(item.get("side") or "") for item in votes if item.get("side")}
    if len(sides) > 1:
        return {
            "side": None,
            "direction_source": "direction_conflict",
            "direction_confidence": 0.0,
            "invalid_reason": "direction_conflict",
        }
    if not sides:
        return {
            "side": None,
            "direction_source": "",
            "direction_confidence": 0.0,
            "invalid_reason": "direction_ambiguous",
        }
    side = next(iter(sides))
    strongest = max(votes, key=lambda item: float(item.get("direction_confidence") or 0.0))
    return {
        "side": side,
        "direction_source": str(strongest.get("direction_source") or ""),
        "direction_confidence": round(float(strongest.get("direction_confidence") or 0.0), 4),
        "invalid_reason": None,
    }


def _listener_prefilter_metadata_containers(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    containers: list[dict[str, Any]] = []

    def add(value: Any) -> None:
        parsed = _from_json(value, value)
        if isinstance(parsed, dict):
            nested = _from_json(parsed.get("drop_metadata"), parsed.get("drop_metadata"))
            if isinstance(nested, dict):
                containers.append(nested)
            containers.append(parsed)

    add(candidate)
    for key in ("audit_json", "audit_payload", "metadata", "audit_metadata"):
        add(candidate.get(key))
    return containers


def _listener_prefilter_metadata_value(candidate: dict[str, Any], *keys: str) -> Any:
    for container in _listener_prefilter_metadata_containers(candidate):
        for key in keys:
            value = _none_if_blank(container.get(key))
            if value is not None:
                return value
    return None


def _listener_prefilter_drop_metadata_payload(candidate: dict[str, Any]) -> dict[str, Any]:
    version = _to_int(_listener_prefilter_metadata_value(candidate, "drop_metadata_version"))
    if version != 1:
        return {}
    return {
        "drop_metadata_version": version,
        "event_id": _listener_prefilter_metadata_value(candidate, "event_id"),
        "tx_hash": _listener_prefilter_metadata_value(candidate, "tx_hash"),
        "pool_address": _listener_prefilter_metadata_value(candidate, "pool_address", "lp_pool_address"),
        "pair": _listener_prefilter_metadata_value(candidate, "pair", "pair_label", "lp_pair_label"),
        "asset": _listener_prefilter_metadata_value(candidate, "asset", "asset_symbol", "base", "base_symbol"),
        "base": _listener_prefilter_metadata_value(candidate, "base", "base_symbol", "base_token_symbol", "asset", "asset_symbol"),
        "quote": _listener_prefilter_metadata_value(candidate, "quote", "quote_symbol", "quote_token_symbol"),
        "side": _listener_prefilter_metadata_value(candidate, "side", "direction"),
        "intent_type": _listener_prefilter_metadata_value(candidate, "intent_type", "intent", "trade_action_key", "canonical_semantic_key"),
        "lp_stage": _listener_prefilter_metadata_value(candidate, "lp_stage", "lp_alert_stage", "stage"),
        "event_ts": _listener_prefilter_metadata_value(candidate, "event_ts"),
        "drop_reason": _listener_prefilter_metadata_value(candidate, "drop_reason"),
    }


def _listener_prefilter_metadata_direction_row(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "side": metadata.get("side"),
        "direction": metadata.get("side"),
        "intent_type": metadata.get("intent_type"),
        "intent": metadata.get("intent_type"),
        "trade_action_key": metadata.get("intent_type"),
        "lp_stage": metadata.get("lp_stage"),
    }


def _apply_listener_prefilter_drop_metadata(candidate: dict[str, Any]) -> None:
    if str(candidate.get("suppression_reason") or "") != "listener_prefilter/drop":
        return
    metadata = _listener_prefilter_drop_metadata_payload(candidate)
    candidate["listener_prefilter_has_metadata"] = bool(metadata)
    if not metadata:
        candidate["listener_prefilter_metadata_has_pair"] = False
        candidate["listener_prefilter_metadata_has_direction"] = False
        return

    for key in ("pair", "asset", "base", "quote", "side", "intent_type", "lp_stage", "event_ts", "pool_address"):
        value = metadata.get(key)
        if value in (None, "", [], {}):
            continue
        candidate[f"drop_{key}"] = value
        if candidate.get(key) in (None, "", [], {}):
            candidate[key] = value
    if metadata.get("intent_type") not in (None, "", [], {}):
        candidate["intent"] = metadata.get("intent_type")
        if candidate.get("trade_action_key") in (None, "", [], {}):
            candidate["trade_action_key"] = metadata.get("intent_type")
    if metadata.get("lp_stage") not in (None, "", [], {}) and candidate.get("stage") in (None, "", [], {}):
        candidate["stage"] = metadata.get("lp_stage")
    if metadata.get("event_ts") not in (None, "", [], {}):
        candidate["event_ts"] = metadata.get("event_ts")

    pair_payload = normalize_lp_sample_asset_pair(
        {
            "pair": metadata.get("pair"),
            "asset": metadata.get("asset") or metadata.get("base"),
            "base": metadata.get("base"),
            "quote": metadata.get("quote"),
        }
    )
    direction_payload = infer_lp_sample_replay_side(_listener_prefilter_metadata_direction_row(metadata))
    candidate["listener_prefilter_metadata_has_pair"] = bool(pair_payload.get("pair") and not pair_payload.get("invalid_reason"))
    candidate["listener_prefilter_metadata_has_direction"] = bool(direction_payload.get("side") in {"long", "short"})


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
        candidate.get("event_ts")
        or candidate.get("archive_ts")
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
        "base": str(candidate.get("base") or ""),
        "quote": str(candidate.get("quote") or ""),
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
        "event_id": _first_payload_value(row, payloads, "event_id"),
        "tx_hash": _first_payload_value(row, payloads, "tx_hash"),
        "pool_address": _first_payload_value(row, payloads, "pool_address", "pool", "address"),
        "signal_id": _first_payload_value(row, payloads, "signal_id"),
        "trade_opportunity_id": _first_payload_value(row, payloads, "trade_opportunity_id"),
        "asset": _first_payload_value(row, payloads, "asset", "asset_symbol", "token_symbol"),
        "pair": _first_payload_value(row, payloads, "pair", "pair_label"),
        "base": _first_payload_value(row, payloads, "base", "base_symbol", "base_token_symbol"),
        "quote": _first_payload_value(row, payloads, "quote", "quote_symbol", "quote_token_symbol"),
        "drop_metadata_version": _first_payload_value(row, payloads, "drop_metadata_version"),
        "drop_reason": _first_payload_value(row, payloads, "drop_reason"),
        "intent_type": _first_payload_value(row, payloads, "intent_type", "canonical_semantic_key"),
        "intent": _first_payload_value(row, payloads, "intent", "intent_type", "canonical_semantic_key"),
        "direction_bucket": _first_payload_value(row, payloads, "direction_bucket"),
        "market_bias": _first_payload_value(row, payloads, "market_bias", "bias"),
        "signal_type": _first_payload_value(row, payloads, "signal_type", "type"),
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
        "stage_reason": _first_payload_value(row, payloads, "stage_reason", "stage_detail_reason", "reason"),
        "metadata": _first_payload_value(row, payloads, "metadata", "signal_metadata", "event_metadata"),
        "gate_reason": _first_payload_value(row, payloads, "gate_reason", "opportunity_gate_failure_reason"),
        "suppression_reason": _first_payload_value(row, payloads, "suppression_reason", "telegram_suppression_reason", "reason", "gate_reason"),
        "blocked_reason": _first_payload_value(row, payloads, "blocked_reason", "primary_blocker", "primary_hard_blocker"),
        "replay_eligible": _coerce_replay_eligible(_first_payload_value(row, payloads, "replay_eligible", "trade_opportunity_replay_eligible")),
        "archive_ts": _first_payload_value(row, payloads, "archive_ts", "archive_written_at"),
        "event_ts": _first_payload_value(row, payloads, "event_ts"),
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


def _normalize_lp_suppression_sample_reason(value: Any) -> str:
    text = str(value or "").strip()
    lowered = text.lower().replace(" ", "_")
    if not lowered:
        return "unknown"
    if "lp_noise_filtered" in lowered:
        return "gate/lp_noise_filtered"
    if (
        "listener_prefilter" in lowered
        or "prefilter/drop" in lowered
        or "prefilter_drop" in lowered
        or "lp_adjacent_noise" in lowered
    ):
        return "listener_prefilter/drop"
    return text[:80]


def _lp_suppression_sample_reason(candidate: dict[str, Any]) -> str:
    reason_text = _candidate_reason_text(candidate)
    normalized = _normalize_lp_suppression_sample_reason(reason_text)
    if normalized in LP_SUPPRESSION_SAMPLE_REASONS:
        return normalized
    for key in (
        "suppression_reason",
        "gate_reason",
        "blocked_reason",
        "delivery_decision",
        "stage",
        "final_trading_output_label",
    ):
        normalized = _normalize_lp_suppression_sample_reason(candidate.get(key))
        if normalized in LP_SUPPRESSION_SAMPLE_REASONS:
            return normalized
    return normalized


def _canonical_pair_label(pair: Any, asset: Any = "") -> str:
    text = str(pair or "").strip().upper().replace("-", "/")
    parsed = _normalize_lp_pair_value(text)
    if parsed.get("pair"):
        return str(parsed["pair"])
    if text and "/" in text:
        return text
    return ""


def _lp_suppression_sample_intent(candidate: dict[str, Any]) -> str:
    for key in (
        "signal_trade_action_key",
        "signal_trade_action",
        "signal_intent_type",
        "signal_intent",
        "signal_direction_bucket",
        "audit_trade_action_key",
        "audit_trade_action",
        "audit_intent_type",
        "audit_intent",
        "parsed_trade_action_key",
        "parsed_trade_action",
        "parsed_intent_type",
        "parsed_intent",
        "parsed_direction_bucket",
        "original_intent",
        "trade_action_key",
        "trade_action",
        "intent_type",
        "intent",
        "direction_bucket",
    ):
        value = candidate.get(key)
        raw = _canonical_upper(value)
        if raw in {
            "LONG_BIAS_OBSERVE",
            "SHORT_BIAS_OBSERVE",
            "DO_NOT_CHASE_LONG",
            "DO_NOT_CHASE_SHORT",
        }:
            return raw
        text = str(value or "").strip()
        if text and text.lower() in {"pool_buy_pressure", "pool_sell_pressure", "buy_pressure", "sell_pressure"}:
            return text
    action = _candidate_action_key(candidate)
    if action in {
        "LONG_BIAS_OBSERVE",
        "SHORT_BIAS_OBSERVE",
        "DO_NOT_CHASE_LONG",
        "DO_NOT_CHASE_SHORT",
    }:
        return action
    text = " ".join(
        str(candidate.get(key) or "")
        for key in (
            "stage",
            "lp_stage",
            "lp_alert_stage",
            "sweep_phase",
            "trade_action_stage",
            "suppression_reason",
            "gate_reason",
            "final_trading_output_label",
        )
    ).lower()
    if "exhaustion" in text:
        return "exhaustion_risk"
    if "confirm" in text:
        return "confirm"
    if action and action != "unknown":
        return action
    return "unknown"


def _stable_sample_id(candidate: dict[str, Any], index: int) -> str:
    audit_id = str(candidate.get("audit_id") or candidate.get("delivery_audit_id") or "").strip()
    signal_id = str(candidate.get("signal_id") or "").strip()
    if audit_id:
        return f"lp_sample_{audit_id}"
    if signal_id:
        return f"lp_sample_{signal_id}"
    payload = json.dumps(candidate, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"lp_sample_row_{index}_{digest}"


def _safe_replay_key(value: Any) -> str:
    text = str(value or "").strip()
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in text) or "unknown"


def _select_rows_by_signal_id(
    conn: sqlite3.Connection,
    table: str,
    *,
    signal_ids: set[str],
    select_candidates: tuple[str, ...],
    replay_diagnostics: ReplayDiagnostics,
) -> dict[str, dict[str, Any]]:
    return _select_rows_by_key(
        conn,
        table,
        key_column="signal_id",
        key_values=signal_ids,
        select_candidates=select_candidates,
        replay_diagnostics=replay_diagnostics,
    )


def _select_rows_by_key(
    conn: sqlite3.Connection,
    table: str,
    *,
    key_column: str,
    key_values: set[str],
    select_candidates: tuple[str, ...],
    replay_diagnostics: ReplayDiagnostics,
) -> dict[str, dict[str, Any]]:
    if not key_values or not _table_exists(conn, table):
        return {}
    columns = _table_columns(conn, table, replay_diagnostics)
    if key_column not in columns:
        replay_diagnostics.warning(f"input_source_missing_{key_column}_column:{table}")
        return {}
    select_columns = list(dict.fromkeys([column for column in (key_column, *select_candidates) if column in columns]))
    rows_by_id: dict[str, dict[str, Any]] = {}
    ordered_ids = sorted(key_value for key_value in key_values if key_value)
    for offset in range(0, len(ordered_ids), 500):
        chunk = ordered_ids[offset: offset + 500]
        placeholders = ", ".join("?" for _ in chunk)
        select_sql = ", ".join(select_columns)
        try:
            rows = conn.execute(
                f"SELECT {select_sql} FROM {table} WHERE {key_column} IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
        except sqlite3.Error as exc:
            replay_diagnostics.query_error(f"input_source_join_failed:{table}.{key_column}:{exc}")
            return rows_by_id
        for row in rows:
            key_value = str(row[key_column] or "").strip()
            if key_value and key_value not in rows_by_id:
                rows_by_id[key_value] = dict(row)
    return rows_by_id


def _select_nearest_rows_by_pool_window(
    conn: sqlite3.Connection,
    table: str,
    *,
    candidates: list[dict[str, Any]],
    select_candidates: tuple[str, ...],
    time_candidates: tuple[str, ...],
    replay_diagnostics: ReplayDiagnostics,
    window_sec: int = 120,
) -> dict[str, dict[str, Any]]:
    if not candidates or not _table_exists(conn, table):
        return {}
    columns = _table_columns(conn, table, replay_diagnostics)
    if "pool_address" not in columns:
        return {}
    time_column = next((column for column in time_candidates if column in columns), "")
    if not time_column:
        return {}
    select_columns = list(dict.fromkeys([column for column in ("pool_address", *select_candidates, time_column) if column in columns]))
    if not select_columns:
        return {}
    select_sql = ", ".join(select_columns)
    rows_by_sample: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        sample_id = str(candidate.get("sample_id") or "")
        pool_address = str(candidate.get("pool_address") or "").strip()
        event_ts = _to_int(candidate.get("event_ts") or candidate.get("timestamp") or candidate.get("created_at"))
        if not sample_id or not pool_address or event_ts <= 0:
            continue
        try:
            row = conn.execute(
                f"SELECT {select_sql} FROM {table} "
                f"WHERE pool_address = ? AND CAST({time_column} AS REAL) >= ? AND CAST({time_column} AS REAL) <= ? "
                f"ORDER BY ABS(CAST({time_column} AS REAL) - ?) ASC LIMIT 1",
                (pool_address, event_ts - window_sec, event_ts + window_sec, event_ts),
            ).fetchone()
        except sqlite3.Error as exc:
            replay_diagnostics.query_error(f"input_source_join_failed:{table}.pool_address_window:{exc}")
            continue
        if row is not None:
            rows_by_sample[sample_id] = dict(row)
    return rows_by_sample


def _copy_lp_sample_source_fields(candidate: dict[str, Any], source_candidate: dict[str, Any], *, prefix: str) -> None:
    for key in (
        "trade_action_key",
        "trade_action",
        "intent_type",
        "intent",
        "direction",
        "side",
        "direction_bucket",
        "market_bias",
        "signal_type",
        "stage",
        "stage_reason",
        "lp_stage",
        "lp_alert_stage",
        "sweep_phase",
        "pair",
        "asset",
        "pool_address",
    ):
        value = source_candidate.get(key)
        if value not in (None, "", [], {}):
            candidate[f"{prefix}_{key}"] = value
    candidate[f"{prefix}_payload"] = {
        key: value
        for key, value in source_candidate.items()
        if key in LP_SAMPLE_DIRECTION_FIELDS or key in {"metadata", "signal_json", "audit_json", "message_json"}
    }


def _merge_lp_sample_identity_fields(candidate: dict[str, Any], source_candidate: dict[str, Any]) -> None:
    for key in (
        "asset",
        "pair",
        "trade_opportunity_id",
        "profile_key",
        "lp_stage",
        "lp_alert_stage",
        "sweep_phase",
        "trade_action_stage",
        "market_context_source",
        "archive_ts",
        "timestamp",
        "created_at",
    ):
        if candidate.get(key) in (None, "", [], {}) and source_candidate.get(key) not in (None, "", [], {}):
            candidate[key] = source_candidate.get(key)


def _enrich_lp_suppression_sample_candidates(
    conn: sqlite3.Connection,
    candidates: list[dict[str, Any]],
    *,
    replay_diagnostics: ReplayDiagnostics,
) -> None:
    signal_ids = {
        str(item.get("original_signal_id") or item.get("signal_id") or "").strip()
        for item in candidates
        if str(item.get("original_signal_id") or item.get("signal_id") or "").strip()
    }
    event_ids = {
        str(item.get("event_id") or "").strip()
        for item in candidates
        if str(item.get("event_id") or "").strip()
    }
    signal_rows = _select_rows_by_signal_id(
        conn,
        "signals",
        signal_ids=signal_ids,
        select_candidates=(
            "trade_opportunity_id",
            "event_id",
            "tx_hash",
            "pool_address",
            "asset",
            "pair",
            "timestamp",
            "archive_written_at",
            "created_at",
            "direction",
            "side",
            "trade_action_key",
            "canonical_semantic_key",
            "intent_type",
            "intent",
            "direction_bucket",
            "market_bias",
            "signal_type",
            "lp_alert_stage",
            "lp_sweep_phase",
            "delivery_decision",
            "trade_opportunity_status",
            "trade_opportunity_shadow_status",
            "opportunity_profile_key",
            "market_context_source",
            "alert_relative_timing",
            "signal_json",
            "metadata",
        ),
        replay_diagnostics=replay_diagnostics,
    )
    signal_rows_by_event_id = _select_rows_by_key(
        conn,
        "signals",
        key_column="event_id",
        key_values=event_ids,
        select_candidates=(
            "signal_id",
            "trade_opportunity_id",
            "event_id",
            "tx_hash",
            "pool_address",
            "asset",
            "pair",
            "timestamp",
            "archive_written_at",
            "created_at",
            "direction",
            "side",
            "trade_action_key",
            "canonical_semantic_key",
            "intent_type",
            "intent",
            "direction_bucket",
            "market_bias",
            "signal_type",
            "lp_alert_stage",
            "lp_sweep_phase",
            "delivery_decision",
            "trade_opportunity_status",
            "trade_opportunity_shadow_status",
            "opportunity_profile_key",
            "market_context_source",
            "alert_relative_timing",
            "signal_json",
            "metadata",
        ),
        replay_diagnostics=replay_diagnostics,
    )
    parsed_rows_by_event_id = _select_rows_by_key(
        conn,
        "parsed_events",
        key_column="event_id",
        key_values=event_ids,
        select_candidates=(
            "event_id",
            "tx_hash",
            "pool_address",
            "asset",
            "pair",
            "intent_type",
            "intent",
            "side",
            "direction",
            "direction_bucket",
            "trade_action_key",
            "parsed_at",
            "created_at",
            "parsed_json",
            "metadata",
        ),
        replay_diagnostics=replay_diagnostics,
    )
    parsed_select_candidates = (
        "event_id",
        "tx_hash",
        "pool_address",
        "asset",
        "pair",
        "intent_type",
        "intent",
        "side",
        "direction",
        "direction_bucket",
        "trade_action_key",
        "parsed_at",
        "created_at",
        "parsed_json",
        "metadata",
    )
    parsed_rows_by_tx_hash = _select_rows_by_key(
        conn,
        "parsed_events",
        key_column="tx_hash",
        key_values={
            str(item.get("tx_hash") or "").strip()
            for item in candidates
            if str(item.get("tx_hash") or "").strip()
        },
        select_candidates=parsed_select_candidates,
        replay_diagnostics=replay_diagnostics,
    )
    parsed_rows_by_pool_window = _select_nearest_rows_by_pool_window(
        conn,
        "parsed_events",
        candidates=candidates,
        select_candidates=parsed_select_candidates,
        time_candidates=("parsed_at", "created_at"),
        replay_diagnostics=replay_diagnostics,
    )
    raw_select_candidates = (
        "event_id",
        "tx_hash",
        "pool_address",
        "address",
        "raw_kind",
        "listener_scan_path",
        "captured_at",
        "created_at",
        "raw_json",
        "metadata",
    )
    raw_rows_by_event_id = _select_rows_by_key(
        conn,
        "raw_events",
        key_column="event_id",
        key_values=event_ids,
        select_candidates=raw_select_candidates,
        replay_diagnostics=replay_diagnostics,
    )
    raw_rows_by_tx_hash = _select_rows_by_key(
        conn,
        "raw_events",
        key_column="tx_hash",
        key_values={
            str(item.get("tx_hash") or "").strip()
            for item in candidates
            if str(item.get("tx_hash") or "").strip()
        },
        select_candidates=raw_select_candidates,
        replay_diagnostics=replay_diagnostics,
    )
    raw_rows_by_pool_window = _select_nearest_rows_by_pool_window(
        conn,
        "raw_events",
        candidates=candidates,
        select_candidates=raw_select_candidates,
        time_candidates=("captured_at", "created_at"),
        replay_diagnostics=replay_diagnostics,
    )
    tx_hashes = {
        str(item.get("tx_hash") or "").strip()
        for item in candidates
        if str(item.get("tx_hash") or "").strip()
    }
    tx_hashes.update(
        str(row.get("tx_hash") or "").strip()
        for row in parsed_rows_by_event_id.values()
        if str(row.get("tx_hash") or "").strip()
    )
    tx_hashes.update(
        str(row.get("tx_hash") or "").strip()
        for rows_by_key in (parsed_rows_by_tx_hash, parsed_rows_by_pool_window, raw_rows_by_event_id, raw_rows_by_tx_hash, raw_rows_by_pool_window)
        for row in rows_by_key.values()
        if str(row.get("tx_hash") or "").strip()
    )
    signal_rows_by_tx_hash = _select_rows_by_key(
        conn,
        "signals",
        key_column="tx_hash",
        key_values=tx_hashes,
        select_candidates=(
            "signal_id",
            "trade_opportunity_id",
            "event_id",
            "tx_hash",
            "pool_address",
            "asset",
            "pair",
            "timestamp",
            "archive_written_at",
            "created_at",
            "direction",
            "side",
            "trade_action_key",
            "canonical_semantic_key",
            "intent_type",
            "intent",
            "direction_bucket",
            "market_bias",
            "signal_type",
            "lp_alert_stage",
            "lp_sweep_phase",
            "delivery_decision",
            "trade_opportunity_status",
            "trade_opportunity_shadow_status",
            "opportunity_profile_key",
            "market_context_source",
            "alert_relative_timing",
            "signal_json",
            "metadata",
        ),
        replay_diagnostics=replay_diagnostics,
    )
    joined_signal_ids = {
        str(row.get("signal_id") or "").strip()
        for row in signal_rows_by_event_id.values()
        if str(row.get("signal_id") or "").strip()
    }
    joined_signal_ids.update(
        str(row.get("signal_id") or "").strip()
        for row in signal_rows_by_tx_hash.values()
        if str(row.get("signal_id") or "").strip()
    )
    all_signal_ids = signal_ids | joined_signal_ids
    telegram_rows = _select_rows_by_signal_id(
        conn,
        "telegram_deliveries",
        signal_ids=all_signal_ids,
        select_candidates=(
            "trade_opportunity_id",
            "asset",
            "sent",
            "sent_at",
            "suppressed",
            "suppression_reason",
            "telegram_update_kind",
            "message_json",
            "message_text",
            "created_at",
        ),
        replay_diagnostics=replay_diagnostics,
    )
    for candidate in candidates:
        signal_id = str(candidate.get("original_signal_id") or candidate.get("signal_id") or "").strip()
        event_id = str(candidate.get("event_id") or "").strip()
        sample_id = str(candidate.get("sample_id") or "")
        tx_hash = str(candidate.get("tx_hash") or "").strip()
        raw_row = raw_rows_by_event_id.get(event_id) or raw_rows_by_tx_hash.get(tx_hash) or raw_rows_by_pool_window.get(sample_id)
        if raw_row:
            raw_payloads = _row_payloads(raw_row, ("raw_json", "metadata"))
            raw_candidate = _source_row_base("raw_events", raw_row, raw_payloads)
            _copy_lp_sample_source_fields(candidate, raw_candidate, prefix="raw")
            _merge_lp_sample_identity_fields(candidate, raw_candidate)
            if candidate.get("tx_hash") in (None, "", [], {}) and raw_row.get("tx_hash"):
                candidate["tx_hash"] = raw_row.get("tx_hash")
            if candidate.get("event_id") in (None, "", [], {}) and raw_row.get("event_id"):
                candidate["event_id"] = raw_row.get("event_id")
                event_id = str(raw_row.get("event_id") or "").strip()
            if "raw_events" not in list(candidate.get("input_source") or []):
                candidate["input_source"] = [*list(candidate.get("input_source") or []), "raw_events"]
        tx_hash = str(candidate.get("tx_hash") or "").strip()
        parsed_row = parsed_rows_by_event_id.get(event_id) or parsed_rows_by_tx_hash.get(tx_hash) or parsed_rows_by_pool_window.get(sample_id)
        if parsed_row:
            parsed_payloads = _row_payloads(parsed_row, ("parsed_json", "metadata"))
            parsed_candidate = _source_row_base("parsed_events", parsed_row, parsed_payloads)
            _copy_lp_sample_source_fields(candidate, parsed_candidate, prefix="parsed")
            _merge_lp_sample_identity_fields(candidate, parsed_candidate)
            if candidate.get("tx_hash") in (None, "", [], {}) and parsed_row.get("tx_hash"):
                candidate["tx_hash"] = parsed_row.get("tx_hash")
            if candidate.get("event_id") in (None, "", [], {}) and parsed_row.get("event_id"):
                candidate["event_id"] = parsed_row.get("event_id")
                event_id = str(parsed_row.get("event_id") or "").strip()
            if "parsed_events" not in list(candidate.get("input_source") or []):
                candidate["input_source"] = [*list(candidate.get("input_source") or []), "parsed_events"]
        tx_hash = str(candidate.get("tx_hash") or "").strip()
        signal_row = signal_rows.get(signal_id) or signal_rows_by_event_id.get(event_id) or signal_rows_by_tx_hash.get(tx_hash)
        if signal_row:
            signal_payloads = _row_payloads(signal_row, ("signal_json", "metadata"))
            signal_candidate = _source_row_base("signals", signal_row, signal_payloads)
            joined_signal_id = str(signal_candidate.get("signal_id") or signal_row.get("signal_id") or "").strip()
            if joined_signal_id:
                candidate["signal_id"] = joined_signal_id
                candidate["original_signal_id"] = joined_signal_id
            _copy_lp_sample_source_fields(candidate, signal_candidate, prefix="signal")
            _merge_lp_sample_identity_fields(candidate, signal_candidate)
            candidate["signal_json"] = signal_row.get("signal_json")
            candidate["signal_payload"] = signal_candidate
            if "signals" not in list(candidate.get("input_source") or []):
                candidate["input_source"] = [*list(candidate.get("input_source") or []), "signals"]
        telegram_signal_id = str(candidate.get("original_signal_id") or candidate.get("signal_id") or signal_id).strip()
        telegram_row = telegram_rows.get(telegram_signal_id)
        if telegram_row:
            telegram_payloads = _row_payloads(telegram_row, ("message_json",))
            telegram_candidate = _source_row_base("telegram_deliveries", telegram_row, telegram_payloads)
            _copy_lp_sample_source_fields(candidate, telegram_candidate, prefix="telegram")
            candidate["telegram_message_json"] = telegram_row.get("message_json")
            candidate["telegram_message_text"] = telegram_row.get("message_text")
            candidate["telegram_payload"] = telegram_candidate
            if "telegram_deliveries" not in list(candidate.get("input_source") or []):
                candidate["input_source"] = [*list(candidate.get("input_source") or []), "telegram_deliveries"]


def _apply_lp_sample_direction_inference(candidate: dict[str, Any]) -> None:
    if str(candidate.get("suppression_reason") or "") == "listener_prefilter/drop":
        metadata = _listener_prefilter_drop_metadata_payload(candidate)
        if metadata:
            metadata_inference = infer_lp_sample_replay_side(
                _listener_prefilter_metadata_direction_row(metadata)
            )
            if metadata_inference.get("side") in {"long", "short"}:
                side = str(metadata_inference["side"])
                direction = side.upper()
                candidate["inferred_side"] = side
                candidate["direction_source"] = "listener_prefilter_metadata"
                candidate["direction_confidence"] = float(
                    metadata_inference.get("direction_confidence") or 0.0
                )
                candidate["lp_sample_invalid_reason"] = None
                candidate["direction"] = direction
                candidate["side"] = direction
                candidate["listener_prefilter_metadata_has_direction"] = True
                candidate["listener_prefilter_recovery_mode"] = "metadata"
                return
    inference = infer_lp_sample_replay_side(candidate)
    candidate["inferred_side"] = inference.get("side")
    candidate["direction_source"] = str(inference.get("direction_source") or "")
    candidate["direction_confidence"] = float(inference.get("direction_confidence") or 0.0)
    candidate["lp_sample_invalid_reason"] = inference.get("invalid_reason")
    if inference.get("side") in {"long", "short"}:
        direction = str(inference["side"]).upper()
        candidate["direction"] = direction
        candidate["side"] = direction
    if str(candidate.get("suppression_reason") or "") == "listener_prefilter/drop":
        if bool(candidate.get("listener_prefilter_has_metadata")):
            candidate["listener_prefilter_recovery_mode"] = "metadata"
        elif any(source in list(candidate.get("input_source") or []) for source in ("signals", "parsed_events", "raw_events", "telegram_deliveries")):
            candidate["listener_prefilter_recovery_mode"] = "join"
        else:
            candidate["listener_prefilter_recovery_mode"] = "none"


def _delivery_audit_sample_candidates(
    conn: sqlite3.Connection,
    *,
    window: dict[str, Any],
    replay_diagnostics: ReplayDiagnostics,
) -> list[dict[str, Any]]:
    rows, missing_ts_rows = _select_rows_for_window(
        conn,
        "delivery_audit",
        select_candidates=(
            "audit_id",
            "event_id",
            "tx_hash",
            "pool_address",
            "drop_metadata_version",
            "drop_reason",
            "signal_id",
            "trade_opportunity_id",
            "asset",
            "pair",
            "base",
            "quote",
            "intent_type",
            "intent",
            "side",
            "direction",
            "direction_bucket",
            "market_bias",
            "signal_type",
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
            "event_ts",
            "stage",
            "lp_stage",
            "stage_reason",
            "gate_reason",
            "final_trading_output_label",
            "opportunity_gate_failure_reason",
            "delivered",
            "notifier_sent_at",
            "suppression_reason",
            "telegram_suppression_reason",
            "audit_json",
            "metadata",
            "archive_written_at",
            "created_at",
        ),
        time_candidates=("event_ts", "archive_written_at", "timestamp", "created_at", "notifier_sent_at"),
        start_ts=int(window["start_ts"]),
        end_ts=int(window["end_ts"]),
        replay_diagnostics=replay_diagnostics,
    )
    candidates: list[dict[str, Any]] = []
    for index, row in enumerate([*rows, *missing_ts_rows]):
        payloads = _row_payloads(row, ("audit_json",))
        candidate = _source_row_base("delivery_audit", row, payloads)
        audit_id = str(row.get("audit_id") or _first_payload_value(row, payloads, "audit_id") or "").strip()
        original_signal_id = str(candidate.get("signal_id") or "").strip()
        candidate["audit_id"] = audit_id
        candidate["delivery_audit_id"] = audit_id
        candidate["original_signal_id"] = original_signal_id
        if any(_is_truthy(row.get(key)) for key in ("sent_to_telegram", "delivered", "sent")) or bool(candidate.get("sent_to_telegram")):
            continue
        reason = _lp_suppression_sample_reason(candidate)
        if reason not in LP_SUPPRESSION_SAMPLE_REASONS:
            continue
        sample_id = _stable_sample_id(candidate, index)
        if not original_signal_id:
            candidate["signal_id"] = sample_id
        intent = _lp_suppression_sample_intent(candidate)
        candidate["sample_id"] = sample_id
        candidate["suppression_reason"] = reason
        candidate["source"] = LP_SUPPRESSION_SAMPLE_SOURCE
        candidate["replay_scope"] = LP_SUPPRESSION_SAMPLE_SCOPE
        candidate["event_ts"] = _candidate_timestamp(candidate)
        candidate["suppressed"] = True
        candidate["audit_json"] = row.get("audit_json")
        candidate["audit_metadata"] = row.get("metadata")
        _apply_listener_prefilter_drop_metadata(candidate)
        candidate["original_pair"] = str(candidate.get("pair") or "")
        candidate["original_asset"] = str(candidate.get("asset") or "")
        candidate["original_stage"] = str(candidate.get("stage") or candidate.get("lp_stage") or "")
        if "delivery_audit" not in list(candidate.get("input_source") or []):
            candidate["input_source"] = [*list(candidate.get("input_source") or []), "delivery_audit"]
        candidates.append(candidate)
    _enrich_lp_suppression_sample_candidates(conn, candidates, replay_diagnostics=replay_diagnostics)
    for candidate in candidates:
        _apply_listener_prefilter_drop_metadata(candidate)
        normalized_asset_pair = normalize_lp_sample_asset_pair(candidate)
        original_intent = _lp_suppression_sample_intent(candidate)
        candidate["asset"] = str(normalized_asset_pair.get("asset") or "")
        candidate["asset_symbol"] = str(normalized_asset_pair.get("asset") or "")
        candidate["pair"] = str(normalized_asset_pair.get("pair") or "")
        candidate["pair_label"] = str(normalized_asset_pair.get("pair") or "")
        candidate["base"] = str(normalized_asset_pair.get("base") or "")
        candidate["quote"] = str(normalized_asset_pair.get("quote") or "")
        candidate["asset_pair_source"] = str(normalized_asset_pair.get("asset_pair_source") or "")
        candidate["asset_pair_invalid_reason"] = normalized_asset_pair.get("invalid_reason")
        candidate["original_pair"] = str(candidate.get("original_pair") or candidate.get("pair") or "")
        candidate["original_asset"] = str(candidate.get("original_asset") or candidate.get("asset") or "")
        candidate["original_stage"] = str(candidate.get("original_stage") or candidate.get("stage") or candidate.get("lp_stage") or "")
        candidate["original_intent"] = original_intent
        candidate["intent"] = original_intent
        candidate["event_ts"] = _candidate_timestamp(candidate)
        _apply_lp_sample_direction_inference(candidate)
    return candidates


def _select_lp_suppression_samples(
    candidates: list[dict[str, Any]],
    *,
    limit_per_reason: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    candidate_counts = Counter(str(item.get("suppression_reason") or "unknown") for item in candidates)
    selected: list[dict[str, Any]] = []
    for reason in sorted(LP_SUPPRESSION_SAMPLE_REASONS):
        reason_rows = [
            item
            for item in candidates
            if str(item.get("suppression_reason") or "") == reason
        ]
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for item in sorted(
            reason_rows,
            key=lambda row: (
                _to_int(row.get("event_ts")) or 0,
                str(row.get("sample_id") or ""),
            ),
        ):
            key = (
                str(item.get("pair") or "unknown"),
                str(item.get("intent") or "unknown"),
                str(item.get("stage") or item.get("lp_stage") or "unknown"),
            )
            grouped[key].append(item)
        keys = sorted(grouped.keys())
        while keys and len([item for item in selected if str(item.get("suppression_reason") or "") == reason]) < limit_per_reason:
            progressed = False
            for key in keys:
                bucket = grouped.get(key) or []
                if not bucket:
                    continue
                selected.append(bucket.pop(0))
                progressed = True
                if len([item for item in selected if str(item.get("suppression_reason") or "") == reason]) >= limit_per_reason:
                    break
            keys = [key for key in keys if grouped.get(key)]
            if not progressed:
                break
    return selected, dict(sorted(candidate_counts.items()))


def _lp_sample_recommended_action(sample_count: int, valid_count: int, avg: float | None, profitable_rate: float | None) -> str:
    if sample_count < LP_SUPPRESSION_SAMPLE_MIN_SAMPLES or valid_count < LP_SUPPRESSION_SAMPLE_MIN_SAMPLES:
        return "needs_more_samples"
    if avg is not None and avg > 0 and profitable_rate is not None and profitable_rate >= LP_SUPPRESSION_SAMPLE_REVIEW_PROFITABLE_RATE:
        return "review_threshold"
    return "keep_suppressed"


def _aggregate_lp_suppression_sample_summary(
    *,
    logical_date: str,
    candidates_total: int,
    candidate_counts_by_reason: dict[str, int],
    selected_candidates: list[dict[str, Any]],
    replay_rows: list[dict[str, Any]],
    dry_run: bool,
    limit_per_reason: int,
    persistence_summary: dict[str, Any] | None = None,
    diagnostics: ReplayDiagnostics | None = None,
) -> dict[str, Any]:
    rows_by_sample = {str(row.get("sample_id") or ""): row for row in replay_rows}
    valid_pair_values = [
        str(item.get("pair") or "").strip().upper()
        for item in selected_candidates
        if str(item.get("pair") or "").strip() and "/" in str(item.get("pair") or "")
    ]
    by_pair = Counter(valid_pair_values)
    by_intent = Counter(str(item.get("intent") or "unknown") for item in selected_candidates)
    invalid_reason_counts: Counter[str] = Counter()
    asset_pair_mismatch_examples: list[dict[str, Any]] = []
    direction_source_distribution: Counter[str] = Counter()
    direction_inference_summary: Counter[str] = Counter(
        {
            "long": 0,
            "short": 0,
            "ambiguous": 0,
            "conflict": 0,
            "do_not_chase_long": 0,
            "do_not_chase_short": 0,
        }
    )
    for item in selected_candidates:
        inferred_side = str(item.get("inferred_side") or "")
        invalid_reason = str(item.get("lp_sample_invalid_reason") or "")
        direction_source = str(item.get("direction_source") or "")
        original_intent = _canonical_upper(item.get("original_intent") or item.get("intent"))
        if invalid_reason == "direction_conflict":
            direction_inference_summary["conflict"] += 1
            direction_source_distribution["direction_conflict"] += 1
        elif inferred_side == "long":
            direction_inference_summary["long"] += 1
        elif inferred_side == "short":
            direction_inference_summary["short"] += 1
        else:
            direction_inference_summary["ambiguous"] += 1
        if direction_source:
            direction_source_distribution[direction_source] += 1
            if direction_source == "do_not_chase_long":
                direction_inference_summary["do_not_chase_long"] += 1
            elif direction_source == "do_not_chase_short":
                direction_inference_summary["do_not_chase_short"] += 1
        elif invalid_reason == "direction_ambiguous":
            direction_source_distribution["direction_ambiguous"] += 1
        if original_intent == "DO_NOT_CHASE_LONG" and direction_source != "do_not_chase_long":
            direction_inference_summary["do_not_chase_long"] += 1
        elif original_intent == "DO_NOT_CHASE_SHORT" and direction_source != "do_not_chase_short":
            direction_inference_summary["do_not_chase_short"] += 1
    invalid_single_asset_pair_count = 0
    unknown_pair_count = 0
    for item in selected_candidates:
        pair = str(item.get("pair") or "").strip()
        original_pair = str(item.get("original_pair") or "").strip()
        if not pair:
            unknown_pair_count += 1
        if original_pair and "/" not in original_pair and _normalize_lp_symbol(original_pair):
            invalid_single_asset_pair_count += 1
    listener_candidates = [
        item
        for item in selected_candidates
        if str(item.get("suppression_reason") or "") == "listener_prefilter/drop"
    ]
    listener_metadata_rows = sum(1 for item in listener_candidates if bool(item.get("listener_prefilter_has_metadata")))
    listener_metadata_direction_rows = sum(
        1 for item in listener_candidates if bool(item.get("listener_prefilter_metadata_has_direction"))
    )
    listener_metadata_pair_rows = sum(
        1 for item in listener_candidates if bool(item.get("listener_prefilter_metadata_has_pair"))
    )
    listener_legacy_rows = max(len(listener_candidates) - listener_metadata_rows, 0)
    listener_join_success_count = sum(1 for item in listener_candidates if "signals" in list(item.get("input_source") or []))
    listener_direction_recovered_count = sum(
        1
        for item in listener_candidates
        if str(item.get("inferred_side") or "") in {"long", "short"}
        and (
            bool(item.get("listener_prefilter_metadata_has_direction"))
            or any(source in list(item.get("input_source") or []) for source in ("signals", "parsed_events", "raw_events", "telegram_deliveries"))
        )
    )
    listener_still_ambiguous_count = sum(
        1
        for item in listener_candidates
        if str(item.get("lp_sample_invalid_reason") or "") == "direction_ambiguous"
        or not str(item.get("inferred_side") or "")
    )
    if listener_metadata_rows > 0:
        listener_recovery_mode = "metadata"
    elif listener_join_success_count > 0:
        listener_recovery_mode = "join"
    else:
        listener_recovery_mode = "none"
    by_reason: list[dict[str, Any]] = []
    reason_actions: list[str] = []
    for reason in sorted(LP_SUPPRESSION_SAMPLE_REASONS):
        reason_candidates = [
            item
            for item in selected_candidates
            if str(item.get("suppression_reason") or "") == reason
        ]
        reason_replays = [
            rows_by_sample.get(str(item.get("sample_id") or ""))
            for item in reason_candidates
            if rows_by_sample.get(str(item.get("sample_id") or ""))
        ]
        valid_rows = [row for row in reason_replays if _is_truthy(row.get("data_valid", 1))]
        invalid_rows = [row for row in reason_replays if not _is_truthy(row.get("data_valid", 1))]
        invalid_by_reason = Counter(str(row.get("invalid_reason") or "unknown") for row in invalid_rows)
        invalid_reason_counts.update(invalid_by_reason)
        source_by_reason = Counter(
            str(item.get("direction_source") or item.get("lp_sample_invalid_reason") or "unknown")
            for item in reason_candidates
        )
        net_values = [
            value
            for value in (_to_signed_float(row.get("net_pnl_bps")) for row in valid_rows)
            if value is not None
        ]
        avg = round(sum(net_values) / len(net_values), 2) if net_values else None
        profitable_rate = round(sum(1 for value in net_values if value > 0.0) / len(net_values), 4) if net_values else None
        action = _lp_sample_recommended_action(len(reason_candidates), len(valid_rows), avg, profitable_rate)
        reason_actions.append(action)
        by_reason.append(
            {
                "reason": reason,
                "candidate_count": int(candidate_counts_by_reason.get(reason) or 0),
                "sample_count": len(reason_candidates),
                "replay_count": len(reason_replays),
                "valid_replay_count": len(valid_rows),
                "avg_net_pnl_bps": avg,
                "profitable_rate": profitable_rate,
                "recommended_action": action,
                "invalid_reason_counts": dict(invalid_by_reason.most_common(10)),
                "direction_source_distribution": dict(source_by_reason.most_common(10)),
            }
        )
    if any(action == "review_threshold" for action in reason_actions):
        diagnosis = "possible_over_suppression"
    elif reason_actions and all(action == "keep_suppressed" for action in reason_actions):
        diagnosis = "early_suppression_seems_correct"
    else:
        diagnosis = "needs_more_samples"
    diagnostics = diagnostics or ReplayDiagnostics()
    replay_count = len(replay_rows)
    valid_rows = [row for row in replay_rows if _is_truthy(row.get("data_valid", 1))]
    invalid_rows = [row for row in replay_rows if not _is_truthy(row.get("data_valid", 1))]
    for row in invalid_rows:
        reason = str(row.get("invalid_reason") or "")
        if reason not in {"asset_pair_mismatch", "outcome_asset_mismatch", "outcome_pair_mismatch"}:
            continue
        if len(asset_pair_mismatch_examples) >= 5:
            continue
        asset_pair_mismatch_examples.append(
            {
                "sample_id": str(row.get("sample_id") or "")[:80],
                "asset": str(row.get("asset") or "")[:20],
                "pair": str(row.get("pair") or "")[:40],
                "original_asset": str(row.get("original_asset") or "")[:20],
                "original_pair": str(row.get("original_pair") or "")[:40],
                "asset_pair_source": str(row.get("asset_pair_source") or "")[:160],
                "invalid_reason": reason,
            }
        )
    net_values = [
        value
        for value in (_to_signed_float(row.get("net_pnl_bps")) for row in valid_rows)
        if value is not None
    ]
    avg_net = round(sum(net_values) / len(net_values), 2) if net_values else None
    profitable_rate = round(sum(1 for value in net_values if value > 0.0) / len(net_values), 4) if net_values else None
    if selected_candidates and direction_inference_summary["ambiguous"] >= len(selected_candidates):
        diagnostics.warning("LP sample rows lack signal/audit direction fields; join or archive metadata needs repair.")
    if listener_candidates and listener_still_ambiguous_count >= len(listener_candidates):
        if listener_metadata_rows == 0:
            diagnostics.warning("listener_prefilter/drop old samples lack direction metadata; new samples will be replayable after metadata coverage accumulates.")
        elif listener_metadata_pair_rows > 0 and listener_metadata_direction_rows == 0:
            diagnostics.warning("listener_prefilter/drop metadata has pair but no direction; check intent/side inference.")
    if selected_candidates and len(valid_rows) < LP_SUPPRESSION_SAMPLE_MIN_SAMPLES and invalid_rows:
        diagnostics.warning("LP sample replay degraded: direction/asset pair metadata insufficient.")
    if int(invalid_reason_counts.get("asset_pair_mismatch") or 0) > 0:
        diagnostics.warning("LP sample replay degraded: fix LP sample asset/pair attribution before gate review.")
    if int(invalid_reason_counts.get("market_context_missing") or 0) > 0:
        diagnostics.warning("LP sample replay degraded: market context metadata is missing for valid asset/pair rows.")
    degraded_reasons: list[str] = []
    if diagnostics.schema_errors or diagnostics.query_errors or diagnostics.price_errors:
        degraded_reasons.append("query_schema_or_price_errors")
    if invalid_rows and len(valid_rows) < LP_SUPPRESSION_SAMPLE_MIN_SAMPLES:
        degraded_reasons.append("direction_or_asset_pair_metadata_insufficient")
    if int(invalid_reason_counts.get("asset_pair_mismatch") or 0) > 0:
        degraded_reasons.append("asset_pair_mismatch")
    if int(invalid_reason_counts.get("outcome_asset_mismatch") or 0) > 0 or int(invalid_reason_counts.get("outcome_pair_mismatch") or 0) > 0:
        degraded_reasons.append("outcome_asset_mismatch")
    if int(invalid_reason_counts.get("market_context_missing") or 0) > 0:
        degraded_reasons.append("market_context_missing")
    if listener_candidates and listener_still_ambiguous_count >= len(listener_candidates) and listener_metadata_rows == 0:
        degraded_reasons.append("listener_prefilter_direction_metadata_missing")
    elif listener_candidates and listener_still_ambiguous_count >= len(listener_candidates) and listener_metadata_pair_rows > 0 and listener_metadata_direction_rows == 0:
        degraded_reasons.append("listener_prefilter_direction_metadata_ambiguous")
    would_insert_valid = len(valid_rows)
    invalid_not_inserted = len(invalid_rows)
    return {
        "available": bool(selected_candidates or replay_rows),
        "sampled": True,
        "logical_date": logical_date,
        "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
        "source": LP_SUPPRESSION_SAMPLE_SOURCE,
        "dry_run": bool(dry_run),
        "sample_limit_per_reason": int(limit_per_reason),
        "sample_min_count": LP_SUPPRESSION_SAMPLE_MIN_SAMPLES,
        "candidate_total_count": int(candidates_total),
        "candidate_sample_count": len(selected_candidates),
        "replay_count": replay_count,
        "valid_replay_count": len(valid_rows),
        "invalid_count": len(invalid_rows),
        "avg_net_pnl_bps": avg_net,
        "profitable_rate": profitable_rate,
        "recommended_action": (
            "review_threshold"
            if any(action == "review_threshold" for action in reason_actions)
            else "keep_suppressed"
            if reason_actions and all(action == "keep_suppressed" for action in reason_actions)
            else "needs_more_samples"
        ),
        "candidate_count_by_reason": dict(sorted(candidate_counts_by_reason.items())),
        "by_reason": by_reason,
        "by_pair": dict(by_pair.most_common(20)),
        "by_intent": dict(by_intent.most_common(20)),
        "invalid_reason_counts": dict(invalid_reason_counts.most_common(20)),
        "asset_pair_mismatch_examples": asset_pair_mismatch_examples,
        "asset_pair_summary": {
            "valid_pairs": dict(by_pair.most_common(20)),
            "invalid_single_asset_pair_count": invalid_single_asset_pair_count,
            "unknown_pair_count": unknown_pair_count,
        },
        "direction_inference_summary": dict(direction_inference_summary),
        "direction_source_distribution": dict(direction_source_distribution.most_common(20)),
        "listener_prefilter_join_success_count": listener_join_success_count,
        "listener_prefilter_direction_recovered_count": listener_direction_recovered_count,
        "listener_prefilter_still_ambiguous_count": listener_still_ambiguous_count,
        "listener_prefilter_metadata_rows": listener_metadata_rows,
        "listener_prefilter_metadata_direction_rows": listener_metadata_direction_rows,
        "listener_prefilter_metadata_pair_rows": listener_metadata_pair_rows,
        "listener_prefilter_legacy_rows": listener_legacy_rows,
        "listener_prefilter_recovery_mode": listener_recovery_mode,
        "would_insert_replay_rows": would_insert_valid if dry_run else 0,
        "would_insert_valid_replay_rows": would_insert_valid if dry_run else 0,
        "invalid_rows_not_inserted": invalid_not_inserted,
        "inserted_replay_rows": int((persistence_summary or {}).get("examples_written") or 0) if not dry_run else 0,
        "persistence_summary": persistence_summary or {"enabled": False, "examples_written": 0, "examples_deleted": 0},
        "diagnosis": diagnosis,
        "degraded_reasons": sorted(set(degraded_reasons)),
        **diagnostics.as_dict(),
    }


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
                raw_profile_key = _first_payload_value(row, payloads, "opportunity_profile_key", "profile_key")
                candidate["profile_key"] = repair_profile_key(raw_profile_key, candidate)
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
        key in invalid_reasons
        for key in (
            "no_price_for_signal_id",
            "no_entry_price",
            "no_exit_price",
            "market_context_unavailable",
            "market_context_missing",
            "outcome_asset_mismatch",
            "schema_error",
        )
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


def _lp_sample_pre_replay_invalid_reason(candidate: dict[str, Any]) -> str:
    direction_reason = str(candidate.get("lp_sample_invalid_reason") or "")
    asset_pair_reason = str(candidate.get("asset_pair_invalid_reason") or "")
    if direction_reason == "direction_conflict":
        return direction_reason
    if direction_reason in {"direction_ambiguous", "direction_conflict"}:
        return direction_reason
    if asset_pair_reason in {"asset_pair_ambiguous", "asset_pair_mismatch"}:
        return asset_pair_reason
    direction = _direction_from_values(candidate.get("side"), candidate.get("direction"))
    if direction not in {"LONG", "SHORT"}:
        return "direction_ambiguous"
    if not str(candidate.get("asset") or "").strip() or not str(candidate.get("pair") or "").strip():
        return "asset_pair_ambiguous"
    return ""


def _replay_dynamic_inputs(
    conn: sqlite3.Connection,
    eligible_inputs: list[dict[str, Any]],
    *,
    replay_diagnostics: ReplayDiagnostics,
    require_market_context: bool = False,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    def price_source_fn(**kwargs) -> dict[str, Any]:
        if require_market_context:
            return _lookup_price_from_market_context(
                conn,
                asset=str(kwargs.get("asset") or ""),
                pair=str(kwargs.get("pair") or ""),
                ts=_to_int(kwargs.get("ts")),
                prefer_source=str(kwargs.get("prefer_source") or ""),
                replay_diagnostics=replay_diagnostics,
                strict_pair_match=True,
            )
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
        sample_invalid_reason = _lp_sample_pre_replay_invalid_reason(candidate)
        if sample_invalid_reason:
            result = _invalid_replay(
                sample_invalid_reason,
                signal_id=str(signal_row.get("signal_id") or ""),
                trade_opportunity_id=str(signal_row.get("trade_opportunity_id") or ""),
                signal_ts=_to_int(signal_row.get("timestamp") or signal_row.get("archive_ts")),
                net_pnl_bps=None,
                gross_pnl_bps=None,
            )
            result["asset"] = str(signal_row.get("asset") or "")
            result["pair"] = str(signal_row.get("pair") or "")
            result["direction"] = str(signal_row.get("direction") or "")
        else:
            result = replay_single_signal(signal_row, price_source_fn=price_source_fn, replay_diagnostics=replay_diagnostics)
        result["input_source"] = list(candidate.get("input_source") or [])
        result["opportunity_status"] = str(candidate.get("opportunity_status") or result.get("opportunity_status") or "")
        result["shadow_status"] = str(candidate.get("shadow_status") or result.get("shadow_status") or "NONE")
        result["signal_stage"] = "SUPPRESSED" if bool(candidate.get("suppressed")) else str(candidate.get("signal_stage") or "")
        result["profile_key"] = str(candidate.get("profile_key") or result.get("profile_key") or "")
        result["shadow_reason"] = str(candidate.get("shadow_reason") or "")
        result["trade_action"] = _candidate_action_key(candidate)
        result["lp_stage"] = str(candidate.get("lp_stage") or candidate.get("lp_alert_stage") or "")
        result["sweep_phase"] = str(candidate.get("sweep_phase") or candidate.get("lp_sweep_phase") or "")
        result["trade_action_stage"] = str(candidate.get("trade_action_stage") or candidate.get("stage") or "")
        result["inferred_side"] = candidate.get("inferred_side")
        result["direction_source"] = str(candidate.get("direction_source") or "")
        result["direction_confidence"] = float(candidate.get("direction_confidence") or 0.0)
        result["listener_prefilter_has_metadata"] = bool(candidate.get("listener_prefilter_has_metadata"))
        result["listener_prefilter_metadata_has_direction"] = bool(candidate.get("listener_prefilter_metadata_has_direction"))
        result["listener_prefilter_metadata_has_pair"] = bool(candidate.get("listener_prefilter_metadata_has_pair"))
        result["listener_prefilter_recovery_mode"] = str(candidate.get("listener_prefilter_recovery_mode") or "")
        result["original_intent"] = str(candidate.get("original_intent") or candidate.get("intent") or "")
        result["original_stage"] = str(candidate.get("original_stage") or candidate.get("stage") or candidate.get("lp_stage") or "")
        result["original_pair"] = str(candidate.get("original_pair") or candidate.get("pair") or "")
        result["original_asset"] = str(candidate.get("original_asset") or candidate.get("asset") or "")
        result["base"] = str(candidate.get("base") or "")
        result["quote"] = str(candidate.get("quote") or "")
        result["asset_pair_source"] = str(candidate.get("asset_pair_source") or "")
        result["asset_pair_invalid_reason"] = str(candidate.get("asset_pair_invalid_reason") or "")
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


def _insert_record(conn: sqlite3.Connection, table: str, record: dict[str, Any]) -> None:
    columns = [column for column in record.keys() if column in _table_columns(conn, table)]
    if not columns:
        return
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders})",
        tuple(record[column] for column in columns),
    )


def _delete_replay_scope_rows(conn: sqlite3.Connection, table: str, logical_date: str, replay_scope: str) -> int:
    if not _table_exists(conn, table):
        return 0
    columns = _table_columns(conn, table)
    if {"logical_date", "replay_scope"}.issubset(columns):
        cursor = conn.execute(
            f"""
            DELETE FROM {table}
            WHERE logical_date = ?
              AND COALESCE(replay_scope, 'default') = ?
            """,
            (logical_date, replay_scope),
        )
        return max(int(cursor.rowcount or 0), 0)
    if "logical_date" in columns:
        cursor = conn.execute(f"DELETE FROM {table} WHERE logical_date = ?", (logical_date,))
        return max(int(cursor.rowcount or 0), 0)
    return 0


def _blocker_json_with_replay_profile_negative(value: Any) -> str:
    blockers = _coerce_blockers(value)
    if REPLAY_PROFILE_NEGATIVE_BLOCKER not in blockers:
        blockers.append(REPLAY_PROFILE_NEGATIVE_BLOCKER)
    return json.dumps(blockers, ensure_ascii=False, sort_keys=True)


def _empty_replay_profile_negative_repair_summary() -> dict[str, int]:
    return {
        "candidates_seen": 0,
        "primary_blocker_updated": 0,
        "already_correct": 0,
        "skipped_higher_priority": 0,
        "opportunity_json_primary_updated": 0,
        "rows_updated": 0,
    }


def _repair_trade_opportunity_replay_profile_blockers(
    conn: sqlite3.Connection,
    profile_rows: dict[str, dict[str, Any]],
    *,
    now_text: str,
) -> dict[str, int]:
    summary = _empty_replay_profile_negative_repair_summary()
    columns = _table_columns(conn, "trade_opportunities", ReplayDiagnostics())
    required = {"trade_opportunity_id", "opportunity_profile_key", "opportunity_json"}
    if not required.issubset(columns):
        return summary
    blocker_profile_keys = sorted(
        profile_key
        for profile_key, row in profile_rows.items()
        if evaluate_replay_profile_gate(row).get("blocker") == REPLAY_PROFILE_NEGATIVE_BLOCKER
    )
    if not blocker_profile_keys:
        return summary
    placeholders = ",".join("?" for _ in blocker_profile_keys)
    select_columns = [
        "trade_opportunity_id",
        "opportunity_profile_key",
        "opportunity_json",
    ]
    for optional in (
        "primary_blocker",
        "primary_hard_blocker",
        "primary_verification_blocker",
        "blockers_json",
        "hard_blockers_json",
        "verification_blockers_json",
        "quality_snapshot_json",
    ):
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
    for row in rows:
        profile_key = str(row.get("opportunity_profile_key") or "")
        gate = evaluate_replay_profile_gate(profile_rows.get(profile_key) or {})
        if gate.get("blocker") != REPLAY_PROFILE_NEGATIVE_BLOCKER:
            continue
        summary["candidates_seen"] += 1
        opportunity_payload = _from_json(row.get("opportunity_json"), {})
        if not isinstance(opportunity_payload, dict):
            opportunity_payload = {}
        blockers = _blockers_from_payload(
            row,
            opportunity_payload,
            row_keys=("blockers_json",),
            payload_keys=("trade_opportunity_blockers", "blockers"),
        )
        hard_blockers = _blockers_from_payload(
            row,
            opportunity_payload,
            row_keys=("hard_blockers_json",),
            payload_keys=("trade_opportunity_hard_blockers", "hard_blockers"),
        )
        verification_blockers = _blockers_from_payload(
            row,
            opportunity_payload,
            row_keys=("verification_blockers_json",),
            payload_keys=("trade_opportunity_verification_blockers", "verification_blockers"),
        )
        if REPLAY_PROFILE_NEGATIVE_BLOCKER not in blockers:
            blockers.append(REPLAY_PROFILE_NEGATIVE_BLOCKER)
        if REPLAY_PROFILE_NEGATIVE_BLOCKER not in hard_blockers:
            hard_blockers.append(REPLAY_PROFILE_NEGATIVE_BLOCKER)
        combined_blockers: list[str] = []
        _append_unique_blockers(combined_blockers, blockers)
        _append_unique_blockers(combined_blockers, hard_blockers)
        _append_unique_blockers(combined_blockers, verification_blockers)
        expected_primary = choose_primary_blocker(combined_blockers)
        primary_hard_blocker = choose_primary_blocker(hard_blockers)
        primary_verification_blocker = choose_primary_blocker(verification_blockers)
        current_primary = str(
            row.get("primary_blocker")
            or opportunity_payload.get("trade_opportunity_primary_blocker")
            or opportunity_payload.get("primary_blocker")
            or ""
        ).strip()
        current_json_primary = str(
            opportunity_payload.get("trade_opportunity_primary_blocker")
            or opportunity_payload.get("primary_blocker")
            or ""
        ).strip()
        if expected_primary and _blocker_priority_index(expected_primary) < _blocker_priority_index(REPLAY_PROFILE_NEGATIVE_BLOCKER):
            summary["skipped_higher_priority"] += 1
        elif current_primary == expected_primary and current_json_primary == expected_primary:
            summary["already_correct"] += 1
        if current_primary != expected_primary and "primary_blocker" in columns:
            summary["primary_blocker_updated"] += 1
        if current_json_primary != expected_primary:
            summary["opportunity_json_primary_updated"] += 1
        opportunity_payload["trade_opportunity_blockers"] = blockers
        opportunity_payload["trade_opportunity_hard_blockers"] = hard_blockers
        opportunity_payload["trade_opportunity_verification_blockers"] = verification_blockers
        opportunity_payload["trade_opportunity_primary_blocker"] = expected_primary
        opportunity_payload["primary_blocker"] = expected_primary
        opportunity_payload["trade_opportunity_primary_hard_blocker"] = primary_hard_blocker
        opportunity_payload["primary_hard_blocker"] = primary_hard_blocker
        opportunity_payload["trade_opportunity_primary_verification_blocker"] = primary_verification_blocker
        opportunity_payload["primary_verification_blocker"] = primary_verification_blocker
        opportunity_payload["trade_opportunity_replay_profile_gate"] = dict(gate)
        opportunity_payload["trade_opportunity_replay_profile_action"] = str(gate.get("action") or "")
        opportunity_payload["trade_opportunity_replay_profile_research_hint"] = str(gate.get("research_hint") or "")
        quality_snapshot = opportunity_payload.get("trade_opportunity_quality_snapshot")
        if isinstance(quality_snapshot, dict):
            quality_snapshot["replay_profile_gate"] = dict(gate)
        updates: dict[str, Any] = {}
        opportunity_json = json.dumps(opportunity_payload, ensure_ascii=False, sort_keys=True)
        if str(row.get("opportunity_json") or "") != opportunity_json:
            updates["opportunity_json"] = opportunity_json
        if "primary_blocker" in columns and str(row.get("primary_blocker") or "").strip() != expected_primary:
            updates["primary_blocker"] = expected_primary
        if "primary_hard_blocker" in columns and str(row.get("primary_hard_blocker") or "").strip() != primary_hard_blocker:
            updates["primary_hard_blocker"] = primary_hard_blocker
        if (
            "primary_verification_blocker" in columns
            and str(row.get("primary_verification_blocker") or "").strip() != primary_verification_blocker
        ):
            updates["primary_verification_blocker"] = primary_verification_blocker
        if "blockers_json" in columns:
            blockers_json = json.dumps(blockers, ensure_ascii=False, sort_keys=True)
            if str(row.get("blockers_json") or "") != blockers_json:
                updates["blockers_json"] = blockers_json
        if "hard_blockers_json" in columns:
            hard_blockers_json = json.dumps(hard_blockers, ensure_ascii=False, sort_keys=True)
            if str(row.get("hard_blockers_json") or "") != hard_blockers_json:
                updates["hard_blockers_json"] = hard_blockers_json
        if "verification_blockers_json" in columns:
            verification_blockers_json = json.dumps(verification_blockers, ensure_ascii=False, sort_keys=True)
            if str(row.get("verification_blockers_json") or "") != verification_blockers_json:
                updates["verification_blockers_json"] = verification_blockers_json
        if "quality_snapshot_json" in columns:
            quality_snapshot_payload = _from_json(row.get("quality_snapshot_json"), {})
            if isinstance(quality_snapshot_payload, dict):
                quality_snapshot_payload["replay_profile_gate"] = dict(gate)
                quality_snapshot_json = json.dumps(quality_snapshot_payload, ensure_ascii=False, sort_keys=True)
                if str(row.get("quality_snapshot_json") or "") != quality_snapshot_json:
                    updates["quality_snapshot_json"] = quality_snapshot_json
        if updates and "updated_at" in columns:
            updates["updated_at"] = now_text
        if updates:
            assignments = ", ".join(f"{key}=?" for key in updates)
            params = [*updates.values(), str(row.get("trade_opportunity_id") or "")]
            conn.execute(
                f"UPDATE trade_opportunities SET {assignments} WHERE trade_opportunity_id=?",
                params,
            )
            summary["rows_updated"] += 1
    return summary


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
        "delete_scope": "logical_date+replay_scope",
        "examples_deleted": 0,
        "profile_daily_stats_deleted": 0,
        "examples_written": 0,
        "profile_stats_written": 0,
        "profile_daily_stats_written": 0,
        "trade_opportunity_replay_profile_negative_repaired": 0,
        "trade_opportunity_replay_profile_negative_primary_blocker_updated": 0,
        "trade_opportunity_replay_profile_negative_repair_summary": _empty_replay_profile_negative_repair_summary(),
    }
    try:
        _ensure_replay_persistence_schema(conn)
        now_text = datetime.now(UTC).isoformat()
        summary["examples_deleted"] = _delete_replay_scope_rows(
            conn,
            "trade_replay_examples",
            logical_date,
            replay_scope,
        )
        summary["profile_daily_stats_deleted"] = _delete_replay_scope_rows(
            conn,
            "trade_replay_profile_daily_stats",
            logical_date,
            replay_scope,
        )
        if not replay_rows:
            conn.commit()
            return summary
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
            _insert_record(conn, "trade_replay_profile_daily_stats", record)
            summary["profile_stats_written"] += 1
            summary["profile_daily_stats_written"] += 1
        repair_summary = _repair_trade_opportunity_replay_profile_blockers(
            conn,
            {
                profile_key: {**stat, "profile_key": profile_key}
                for profile_key, stat in stats.items()
                if str(profile_key or "")
            },
            now_text=now_text,
        )
        summary["trade_opportunity_replay_profile_negative_repair_summary"] = repair_summary
        summary["trade_opportunity_replay_profile_negative_repaired"] = int(repair_summary.get("rows_updated") or 0)
        summary["trade_opportunity_replay_profile_negative_primary_blocker_updated"] = int(
            repair_summary.get("primary_blocker_updated") or 0
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


def _persist_lp_suppression_sample_rows(
    conn: sqlite3.Connection,
    logical_date: str,
    replay_rows: list[dict[str, Any]],
    replay_diagnostics: ReplayDiagnostics,
    *,
    strategy_config_hash: str,
    limit_per_reason: int,
) -> dict[str, Any]:
    summary = {
        "enabled": True,
        "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
        "strategy_config_hash": strategy_config_hash,
        "profile_stats_scope": "not_written_for_lp_suppression_sample",
        "idempotent_key": "trade_replay_examples.replay_id=logical_date+lp_suppression_sample+sample_id+strategy_config_hash",
        "delete_scope": "logical_date+lp_suppression_sample",
        "examples_deleted": 0,
        "examples_written": 0,
        "invalid_rows_skipped": 0,
        "profile_stats_written": 0,
        "profile_daily_stats_written": 0,
        "trade_opportunity_replay_profile_negative_repaired": 0,
    }
    try:
        _ensure_replay_persistence_schema(conn)
        now_text = datetime.now(UTC).isoformat()
        summary["examples_deleted"] = _delete_replay_scope_rows(
            conn,
            "trade_replay_examples",
            logical_date,
            LP_SUPPRESSION_SAMPLE_SCOPE,
        )
        valid_replay_rows = [row for row in replay_rows if _is_truthy(row.get("data_valid", 1))]
        summary["invalid_rows_skipped"] = len(replay_rows) - len(valid_replay_rows)
        for index, row in enumerate(valid_replay_rows):
            sample_id = str(row.get("sample_id") or row.get("delivery_audit_id") or row.get("signal_id") or f"row_{index}")
            replay_id = (
                f"replay_{logical_date}_{LP_SUPPRESSION_SAMPLE_SCOPE}_"
                f"{_safe_replay_key(sample_id)}_{_safe_replay_key(strategy_config_hash)}"
            )
            net_pnl = _to_signed_float(row.get("net_pnl_bps"))
            close_reason = str(row.get("close_reason") or "")
            data_valid = _is_truthy(row.get("data_valid", 1))
            features = {
                "input_source": list(row.get("input_source") or []),
                "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
                "sample_id": sample_id,
                "suppression_reason": str(row.get("suppression_reason") or ""),
                "inferred_side": str(row.get("inferred_side") or "").lower(),
                "direction_source": str(row.get("direction_source") or ""),
                "direction_confidence": float(row.get("direction_confidence") or 0.0),
                "listener_prefilter_has_metadata": bool(row.get("listener_prefilter_has_metadata")),
                "listener_prefilter_metadata_has_direction": bool(row.get("listener_prefilter_metadata_has_direction")),
                "listener_prefilter_metadata_has_pair": bool(row.get("listener_prefilter_metadata_has_pair")),
                "listener_prefilter_recovery_mode": str(row.get("listener_prefilter_recovery_mode") or ""),
                "original_intent": str(row.get("original_intent") or row.get("intent") or ""),
                "original_stage": str(row.get("original_stage") or row.get("trade_action_stage") or row.get("lp_stage") or ""),
                "original_pair": str(row.get("original_pair") or row.get("pair") or ""),
                "original_asset": str(row.get("original_asset") or row.get("asset") or ""),
                "base": str(row.get("base") or ""),
                "quote": str(row.get("quote") or ""),
                "asset_pair_source": str(row.get("asset_pair_source") or ""),
                "asset_pair_invalid_reason": str(row.get("asset_pair_invalid_reason") or ""),
                "source": LP_SUPPRESSION_SAMPLE_SOURCE,
                "audit_id": str(row.get("delivery_audit_id") or row.get("audit_id") or ""),
                "original_signal_id": str(row.get("original_signal_id") or ""),
                "event_ts": _to_int(row.get("event_ts") or row.get("signal_ts")),
                "stage": str(row.get("trade_action_stage") or row.get("lp_stage") or ""),
                "intent": str(row.get("intent") or row.get("trade_action") or ""),
                "invalid_reason": str(row.get("invalid_reason") or ""),
                "outcome_status": "completed" if data_valid else "data_invalid",
                "sample_limit_per_reason": int(limit_per_reason),
                "sample_min_count": LP_SUPPRESSION_SAMPLE_MIN_SAMPLES,
                "telegram_realtime_delivery": False,
                "full_replay_isolated": True,
            }
            record = {
                "replay_id": replay_id,
                "logical_date": logical_date,
                "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
                "strategy_config_hash": strategy_config_hash,
                "include_suppressed": 1,
                "include_blocked": 1,
                "signal_id": str(row.get("original_signal_id") or row.get("signal_id") or ""),
                "trade_opportunity_id": str(row.get("trade_opportunity_id") or ""),
                "delivery_audit_id": str(row.get("delivery_audit_id") or row.get("audit_id") or ""),
                "asset": str(row.get("asset") or row.get("asset_symbol") or ""),
                "pair": str(row.get("pair") or row.get("pair_label") or ""),
                "side": str(row.get("direction") or row.get("side") or ""),
                "opportunity_status": str(row.get("opportunity_status") or ""),
                "shadow_status": str(row.get("shadow_status") or "NONE"),
                "signal_stage": "SUPPRESSED",
                "lp_stage": str(row.get("lp_stage") or ""),
                "sweep_phase": str(row.get("sweep_phase") or ""),
                "profile_key": str(row.get("profile_key") or ""),
                "signal_ts": _to_int(row.get("signal_ts") or row.get("event_ts") or row.get("archive_ts") or row.get("timestamp")),
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
                "data_valid": 1 if data_valid else 0,
                "invalid_reason": str(row.get("invalid_reason") or ""),
                "was_profitable": 1 if net_pnl is not None and net_pnl > 0.0 else 0,
                "was_stopped": 1 if close_reason == "stop_loss" else 0,
                "was_late": 0,
                "price_points_seen": 2 if row.get("entry_price") is not None and row.get("exit_price") is not None else 0,
                "blockers_json": json.dumps(row.get("blockers") or [], ensure_ascii=False, sort_keys=True),
                "features_json": json.dumps(features, ensure_ascii=False, sort_keys=True),
                "created_at": now_text,
            }
            _insert_or_update(conn, "trade_replay_examples", record, ("replay_id",))
            summary["examples_written"] += 1
        conn.commit()
        return summary
    except sqlite3.Error as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        replay_diagnostics.query_error(f"lp_suppression_sample_persist_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:lp_suppression_sample_persist_failed:{exc}")
        summary["enabled"] = False
        summary["error"] = str(exc)
        return summary


def run_lp_suppression_sample_replay(
    date_str: str,
    *,
    db_path: str | Path | None = None,
    dry_run: bool = True,
    limit_per_reason: int | None = None,
) -> dict[str, Any]:
    diagnostics = ReplayDiagnostics()
    logical_date = str(date_str or "").strip()
    resolved_path = Path(db_path) if db_path else _resolve_db_path()
    env_limit = _to_int(os.environ.get("LP_SUPPRESSION_SAMPLE_LIMIT"))
    sample_limit = int(limit_per_reason or env_limit or LP_SUPPRESSION_SAMPLE_LIMIT_DEFAULT)
    if sample_limit <= 0:
        sample_limit = LP_SUPPRESSION_SAMPLE_LIMIT_DEFAULT
    strategy_hash = _strategy_config_hash(
        replay_scope=LP_SUPPRESSION_SAMPLE_SCOPE,
        include_suppressed=True,
        include_blocked=True,
    )
    persistence_summary = {
        "enabled": False,
        "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
        "strategy_config_hash": strategy_hash,
        "examples_deleted": 0,
        "examples_written": 0,
        "invalid_rows_skipped": 0,
    }
    base_summary = {
        "available": False,
        "sampled": True,
        "logical_date": logical_date,
        "replay_scope": LP_SUPPRESSION_SAMPLE_SCOPE,
        "source": LP_SUPPRESSION_SAMPLE_SOURCE,
        "dry_run": bool(dry_run),
        "sample_limit_per_reason": sample_limit,
        "sample_min_count": LP_SUPPRESSION_SAMPLE_MIN_SAMPLES,
        "candidate_total_count": 0,
        "candidate_sample_count": 0,
        "replay_count": 0,
        "valid_replay_count": 0,
        "invalid_count": 0,
        "avg_net_pnl_bps": None,
        "profitable_rate": None,
        "recommended_action": "needs_more_samples",
        "candidate_count_by_reason": {},
        "by_reason": [],
        "by_pair": {},
        "by_intent": {},
        "invalid_reason_counts": {},
        "asset_pair_mismatch_examples": [],
        "asset_pair_summary": {
            "valid_pairs": {},
            "invalid_single_asset_pair_count": 0,
            "unknown_pair_count": 0,
        },
        "direction_inference_summary": {
            "long": 0,
            "short": 0,
            "ambiguous": 0,
            "conflict": 0,
            "do_not_chase_long": 0,
            "do_not_chase_short": 0,
        },
        "direction_source_distribution": {},
        "listener_prefilter_join_success_count": 0,
        "listener_prefilter_direction_recovered_count": 0,
        "listener_prefilter_still_ambiguous_count": 0,
        "listener_prefilter_metadata_rows": 0,
        "listener_prefilter_metadata_direction_rows": 0,
        "listener_prefilter_metadata_pair_rows": 0,
        "listener_prefilter_legacy_rows": 0,
        "listener_prefilter_recovery_mode": "none",
        "would_insert_replay_rows": 0,
        "would_insert_valid_replay_rows": 0,
        "invalid_rows_not_inserted": 0,
        "inserted_replay_rows": 0,
        "persistence_summary": persistence_summary,
        "diagnosis": "needs_more_samples",
        "degraded_reasons": [],
        "status": "ok",
        "db_path": str(resolved_path),
    }
    if not logical_date:
        diagnostics.query_error("missing_date")
        base_summary["status"] = "error"
        base_summary.update(diagnostics.as_dict())
        return base_summary
    try:
        window = _logical_window(logical_date)
        base_summary.update(
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
        base_summary["status"] = "error"
        base_summary.update(diagnostics.as_dict())
        return base_summary
    if not resolved_path.exists():
        diagnostics.query_error(f"sqlite_db_missing:{resolved_path}")
        base_summary["status"] = "error"
        base_summary.update(diagnostics.as_dict())
        return base_summary
    try:
        if dry_run:
            conn = sqlite3.connect(f"file:{resolved_path}?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(str(resolved_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        diagnostics.query_error(f"sqlite_open_failed:{exc}")
        base_summary["status"] = "error"
        base_summary.update(diagnostics.as_dict())
        return base_summary
    try:
        candidates = _delivery_audit_sample_candidates(conn, window=window, replay_diagnostics=diagnostics)
        selected, counts_by_reason = _select_lp_suppression_samples(candidates, limit_per_reason=sample_limit)
        replay_rows = _replay_dynamic_inputs(conn, selected, replay_diagnostics=diagnostics, require_market_context=True)
        rows_by_sample: dict[str, dict[str, Any]] = {}
        for candidate, replay in zip(selected, replay_rows, strict=False):
            sample_id = str(candidate.get("sample_id") or "")
            replay["sample_id"] = sample_id
            replay["delivery_audit_id"] = str(candidate.get("delivery_audit_id") or "")
            replay["audit_id"] = str(candidate.get("audit_id") or "")
            replay["original_signal_id"] = str(candidate.get("original_signal_id") or "")
            replay["suppression_reason"] = str(candidate.get("suppression_reason") or "")
            replay["source"] = LP_SUPPRESSION_SAMPLE_SOURCE
            replay["replay_scope"] = LP_SUPPRESSION_SAMPLE_SCOPE
            replay["signal_stage"] = "SUPPRESSED"
            replay["event_ts"] = _to_int(candidate.get("event_ts")) or _to_int(replay.get("signal_ts"))
            replay["signal_ts"] = _to_int(replay.get("signal_ts")) or _to_int(candidate.get("event_ts"))
            replay["pair"] = str(candidate.get("pair") or replay.get("pair") or "")
            replay["asset"] = str(candidate.get("asset") or replay.get("asset") or "")
            replay["intent"] = str(candidate.get("intent") or replay.get("trade_action") or "")
            replay["trade_action"] = str(candidate.get("trade_action") or replay.get("trade_action") or candidate.get("intent") or "")
            replay["lp_stage"] = str(candidate.get("lp_stage") or candidate.get("stage") or replay.get("lp_stage") or "")
            replay["sweep_phase"] = str(candidate.get("sweep_phase") or replay.get("sweep_phase") or "")
            replay["trade_action_stage"] = str(candidate.get("trade_action_stage") or candidate.get("stage") or "")
            replay["inferred_side"] = candidate.get("inferred_side")
            replay["direction_source"] = str(candidate.get("direction_source") or "")
            replay["direction_confidence"] = float(candidate.get("direction_confidence") or 0.0)
            replay["listener_prefilter_has_metadata"] = bool(candidate.get("listener_prefilter_has_metadata"))
            replay["listener_prefilter_metadata_has_direction"] = bool(candidate.get("listener_prefilter_metadata_has_direction"))
            replay["listener_prefilter_metadata_has_pair"] = bool(candidate.get("listener_prefilter_metadata_has_pair"))
            replay["listener_prefilter_recovery_mode"] = (
                "metadata"
                if bool(candidate.get("listener_prefilter_has_metadata"))
                else "join"
                if any(source in list(candidate.get("input_source") or []) for source in ("signals", "parsed_events", "raw_events", "telegram_deliveries"))
                else "none"
            )
            replay["original_intent"] = str(candidate.get("original_intent") or candidate.get("intent") or "")
            replay["original_stage"] = str(candidate.get("original_stage") or candidate.get("stage") or candidate.get("lp_stage") or "")
            replay["original_pair"] = str(candidate.get("original_pair") or candidate.get("pair") or "")
            replay["original_asset"] = str(candidate.get("original_asset") or candidate.get("asset") or "")
            replay["profile_key"] = str(candidate.get("profile_key") or replay.get("profile_key") or "")
            replay["opportunity_status"] = str(candidate.get("opportunity_status") or replay.get("opportunity_status") or "")
            replay["input_source"] = ["delivery_audit", LP_SUPPRESSION_SAMPLE_SCOPE]
            for source_name in ("signals", "telegram_deliveries"):
                if source_name in list(candidate.get("input_source") or []) and source_name not in replay["input_source"]:
                    replay["input_source"].append(source_name)
            replay["blockers"] = list(candidate.get("blockers") or [])
            rows_by_sample[sample_id] = replay
        replay_rows = [rows_by_sample.get(str(candidate.get("sample_id") or ""), {}) for candidate in selected]
        replay_rows = [row for row in replay_rows if row]
        if not dry_run:
            persistence_summary = _persist_lp_suppression_sample_rows(
                conn,
                logical_date,
                replay_rows,
                diagnostics,
                strategy_config_hash=strategy_hash,
                limit_per_reason=sample_limit,
            )
        summary = _aggregate_lp_suppression_sample_summary(
            logical_date=logical_date,
            candidates_total=len(candidates),
            candidate_counts_by_reason=counts_by_reason,
            selected_candidates=selected,
            replay_rows=replay_rows,
            dry_run=dry_run,
            limit_per_reason=sample_limit,
            persistence_summary=persistence_summary,
            diagnostics=diagnostics,
        )
        if diagnostics.schema_errors or diagnostics.query_errors or diagnostics.price_errors or summary.get("degraded_reasons"):
            summary["status"] = "degraded"
        else:
            summary["status"] = "ok"
        summary["db_path"] = str(resolved_path)
        return summary
    except sqlite3.Error as exc:
        diagnostics.query_error(f"lp_suppression_sample_query_failed:{exc}")
        diagnostics.schema_error(f"schema_error:lp_suppression_sample_query_failed:{exc}")
        base_summary["status"] = "error"
        base_summary.update(diagnostics.as_dict())
        return base_summary
    finally:
        conn.close()



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
    profile_rows = repair_profile_rows_with_sources(profile_rows, replay_rows)
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
            "delete_scope": "logical_date+replay_scope",
            "examples_deleted": 0,
            "profile_daily_stats_deleted": 0,
            "examples_written": 0,
            "profile_stats_written": 0,
            "profile_daily_stats_written": 0,
            "trade_opportunity_replay_profile_negative_repaired": 0,
            "trade_opportunity_replay_profile_negative_primary_blocker_updated": 0,
            "trade_opportunity_replay_profile_negative_repair_summary": _empty_replay_profile_negative_repair_summary(),
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
    row_asset = _normalize_lp_symbol(row_asset)
    row_pair = str(_normalize_lp_pair_value(row_pair).get("pair") or row_pair)
    expected_pair = str(_normalize_lp_pair_value(pair).get("pair") or pair)
    if asset and row_asset and row_asset != _normalize_lp_symbol(asset):
        replay_diagnostics.price_error(f"outcomes_asset_mismatch:{signal_id}:{row_asset}!={asset}")
        return {"price": None, "source": "unavailable", "reason": "outcome_asset_mismatch"}
    if expected_pair and row_pair and row_pair != expected_pair:
        replay_diagnostics.price_error(f"outcomes_pair_mismatch:{signal_id}:{row_pair}!={expected_pair}")
        return {"price": None, "source": "unavailable", "reason": "outcome_asset_mismatch"}
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
    strict_pair_match: bool = False,
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
    asset_aliases = _lp_asset_aliases(asset) or (asset,)
    placeholders = ", ".join("?" for _ in asset_aliases)
    limit = 50 if strict_pair_match else 1
    try:
        rows = conn.execute(
            f"SELECT {', '.join(select_columns)} "
            f"FROM market_context_snapshots WHERE asset IN ({placeholders}) "
            f"ORDER BY ABS(COALESCE({time_column}, 0) - ?) ASC, COALESCE({time_column}, 0) ASC LIMIT {limit}",
            (*asset_aliases, ts),
        ).fetchall()
    except sqlite3.Error as exc:
        replay_diagnostics.query_error(f"market_context_lookup_failed:{exc}")
        replay_diagnostics.schema_error(f"schema_error:market_context_lookup_failed:{exc}")
        return {"price": None, "source": "unavailable", "reason": "schema_error"}
    row = None
    expected_pair = str(_normalize_lp_pair_value(pair).get("pair") or "").strip().upper()
    if strict_pair_match and expected_pair:
        if "pair" not in columns:
            replay_diagnostics.price_error(f"market_context_missing:{asset}:{pair}")
            return {"price": None, "source": "unavailable", "reason": "market_context_missing"}
        for candidate_row in rows:
            row_pair = str(_normalize_lp_pair_value(candidate_row["pair"] if "pair" in candidate_row.keys() else "").get("pair") or "").strip().upper()
            if row_pair == expected_pair:
                row = candidate_row
                break
    elif rows:
        row = rows[0]
    if row is None:
        replay_diagnostics.price_error(f"market_context_missing:{asset}:{pair}")
        reason = "market_context_missing" if strict_pair_match else "market_context_unavailable"
        return {"price": None, "source": "unavailable", "reason": reason}
    price, source = _select_price_from_context(dict(row))
    if price is None:
        venue = row["venue"] if "venue" in row.keys() else ""
        replay_diagnostics.price_error(f"market_context_price_missing:{asset}:{pair}:{venue}")
        reason = "market_context_missing" if strict_pair_match else "market_context_unavailable"
        return {"price": None, "source": "unavailable", "reason": reason}
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
    if raw in {
        "schema_error",
        "market_context_unavailable",
        "market_context_missing",
        "no_price_for_signal_id",
        "no_entry_price",
        "no_exit_price",
        "asset_pair_ambiguous",
        "asset_pair_mismatch",
        "outcome_asset_mismatch",
        "outcome_pair_mismatch",
    }:
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
    parser.add_argument("--execute", action="store_true", help="Execute supported write-mode helper")
    parser.add_argument("--lp-suppression-sample", action="store_true", help="Run offline LP early suppression sample replay")
    parser.add_argument("--sample-limit", type=int, help="LP suppression sample limit per reason")
    parser.add_argument("--replay-scope", choices=tuple(sorted(REPLAY_SCOPES)), help="Replay persistence scope")
    parser.add_argument("--include-suppressed", action="store_true", help="Include suppressed replay-eligible research signals")
    parser.add_argument("--include-blocked", action="store_true", default=True, help="Include blocked/no-trade replay-eligible research signals")
    return parser


def _render_lp_suppression_sample_text(summary: dict[str, Any]) -> str:
    lines = [
        f"lp_suppression_sample_replay logical_date={summary.get('logical_date')}",
        f"replay_scope={summary.get('replay_scope')}",
        f"source={summary.get('source')}",
        f"dry_run={summary.get('dry_run')}",
        f"status={summary.get('status')}",
        f"sample_limit_per_reason={summary.get('sample_limit_per_reason')}",
        f"sample_min_count={summary.get('sample_min_count')}",
        f"candidate_total_count={summary.get('candidate_total_count')}",
        f"candidate_sample_count={summary.get('candidate_sample_count')}",
        f"replay_count={summary.get('replay_count')}",
        f"valid_replay_count={summary.get('valid_replay_count')}",
        f"invalid_count={summary.get('invalid_count')}",
        f"avg_net_pnl_bps={summary.get('avg_net_pnl_bps')}",
        f"profitable_rate={summary.get('profitable_rate')}",
        f"recommended_action={summary.get('recommended_action')}",
        f"candidate_count_by_reason={json.dumps(summary.get('candidate_count_by_reason') or {}, ensure_ascii=False, sort_keys=True)}",
        f"by_reason={json.dumps(summary.get('by_reason') or [], ensure_ascii=False, sort_keys=True)}",
        f"by_pair={json.dumps(summary.get('by_pair') or {}, ensure_ascii=False, sort_keys=True)}",
        f"by_intent={json.dumps(summary.get('by_intent') or {}, ensure_ascii=False, sort_keys=True)}",
        f"asset_pair_summary={json.dumps(summary.get('asset_pair_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"asset_pair_mismatch_examples={json.dumps(summary.get('asset_pair_mismatch_examples') or [], ensure_ascii=False, sort_keys=True)}",
        f"direction_inference_summary={json.dumps(summary.get('direction_inference_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"direction_source_distribution={json.dumps(summary.get('direction_source_distribution') or {}, ensure_ascii=False, sort_keys=True)}",
        f"invalid_reason_counts={json.dumps(summary.get('invalid_reason_counts') or {}, ensure_ascii=False, sort_keys=True)}",
        f"listener_prefilter_join_success_count={summary.get('listener_prefilter_join_success_count')}",
        f"listener_prefilter_direction_recovered_count={summary.get('listener_prefilter_direction_recovered_count')}",
        f"listener_prefilter_still_ambiguous_count={summary.get('listener_prefilter_still_ambiguous_count')}",
        f"listener_prefilter_metadata_rows={summary.get('listener_prefilter_metadata_rows')}",
        f"listener_prefilter_metadata_direction_rows={summary.get('listener_prefilter_metadata_direction_rows')}",
        f"listener_prefilter_metadata_pair_rows={summary.get('listener_prefilter_metadata_pair_rows')}",
        f"listener_prefilter_legacy_rows={summary.get('listener_prefilter_legacy_rows')}",
        f"listener_prefilter_recovery_mode={summary.get('listener_prefilter_recovery_mode')}",
        f"would_insert_replay_rows={summary.get('would_insert_replay_rows')}",
        f"would_insert_valid_replay_rows={summary.get('would_insert_valid_replay_rows')}",
        f"invalid_rows_not_inserted={summary.get('invalid_rows_not_inserted')}",
        f"inserted_replay_rows={summary.get('inserted_replay_rows')}",
        f"persistence_summary={json.dumps(summary.get('persistence_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"diagnosis={summary.get('diagnosis')}",
        f"degraded_reasons={json.dumps(summary.get('degraded_reasons') or [], ensure_ascii=False)}",
        f"warnings={json.dumps(summary.get('warnings') or [], ensure_ascii=False)}",
        f"query_errors={json.dumps(summary.get('query_errors') or [], ensure_ascii=False)}",
        f"price_errors={json.dumps(summary.get('price_errors') or [], ensure_ascii=False)}",
        f"schema_errors={json.dumps(summary.get('schema_errors') or [], ensure_ascii=False)}",
    ]
    return "\n".join(lines) + "\n"


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
        "trade_opportunity_replay_profile_negative_repair_summary="
        f"{json.dumps((summary.get('persistence_summary') or {}).get('trade_opportunity_replay_profile_negative_repair_summary') or {}, ensure_ascii=False, sort_keys=True)}",
        f"warnings={json.dumps(summary.get('warnings') or [], ensure_ascii=False)}",
        f"query_errors={json.dumps(summary.get('query_errors') or [], ensure_ascii=False)}",
        f"price_errors={json.dumps(summary.get('price_errors') or [], ensure_ascii=False)}",
        f"schema_errors={json.dumps(summary.get('schema_errors') or [], ensure_ascii=False)}",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.lp_suppression_sample:
        if args.dry_run and args.execute:
            raise SystemExit("--lp-suppression-sample accepts only one of --dry-run or --execute")
        summary = run_lp_suppression_sample_replay(
            args.date,
            dry_run=not bool(args.execute),
            limit_per_reason=args.sample_limit,
        )
        if args.format == "json":
            print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(_render_lp_suppression_sample_text(summary), end="")
        return 0 if summary.get("status") in {"ok", "degraded"} else 1
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
