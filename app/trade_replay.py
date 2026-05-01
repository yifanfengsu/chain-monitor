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
import json
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from config import (
    OUTCOME_PREFER_OKX_MARK,
    OUTCOME_USE_MARKET_CONTEXT_PRICE,
    SQLITE_DB_PATH,
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


def run_trade_replay(
    date_str: str,
    *,
    db_path: str | Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Read-only DATE-scoped replay summary for closure validation."""
    diagnostics = ReplayDiagnostics()
    logical_date = str(date_str or "").strip()
    resolved_path = Path(db_path) if db_path else _resolve_db_path()
    summary: dict[str, Any] = {
        "logical_date": logical_date,
        "status": "ok",
        "replay_count": 0,
        "valid_replay_count": 0,
        "sample_insufficient": True,
        "trade_replay_available": False,
        "warnings": [],
        "query_errors": [],
        "price_errors": [],
        "schema_errors": [],
        "results": [],
        "dry_run": bool(dry_run),
        "db_path": str(resolved_path),
    }
    if not logical_date:
        diagnostics.query_error("missing_date")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary
    if not resolved_path.exists():
        diagnostics.query_error(f"sqlite_db_missing:{resolved_path}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary

    try:
        conn = sqlite3.connect(f"file:{resolved_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        diagnostics.query_error(f"sqlite_open_failed:{exc}")
        summary["status"] = "error"
        summary.update(diagnostics.as_dict())
        return summary

    try:
        required = {
            "trade_replay_examples": {"logical_date", "data_valid", "invalid_reason", "signal_id", "trade_opportunity_id", "asset", "pair", "signal_ts", "entry_ts", "exit_ts", "entry_price", "exit_price", "net_pnl_bps", "label"},
            "trade_replay_profile_stats": {"profile_key", "valid_sample_count", "avg_net_pnl_bps", "win_rate", "recommended_action"},
        }
        for table, columns in required.items():
            if not _table_exists(conn, table):
                diagnostics.schema_error(f"schema_error:missing_table:{table}")
                continue
            missing = sorted(columns - _table_columns(conn, table, diagnostics))
            if missing:
                diagnostics.schema_error(f"schema_error:missing_columns:{table}:{','.join(missing)}")

        if diagnostics.schema_errors:
            summary["status"] = "degraded"
            summary.update(diagnostics.as_dict())
            return summary

        rows = conn.execute(
            "SELECT replay_id, logical_date, signal_id, trade_opportunity_id, asset, pair, signal_ts, entry_ts, exit_ts, "
            "entry_price, exit_price, net_pnl_bps, data_valid, invalid_reason, label "
            "FROM trade_replay_examples WHERE logical_date = ? ORDER BY signal_ts ASC",
            (logical_date,),
        ).fetchall()
        summary["replay_count"] = len(rows)
        summary["trade_replay_available"] = len(rows) > 0
        summary["valid_replay_count"] = sum(1 for row in rows if int(row["data_valid"] or 0) == 1)
        summary["sample_insufficient"] = summary["valid_replay_count"] < REPLAY_SAMPLE_INSUFFICIENT

        preview: list[dict[str, Any]] = []
        for row in rows[:10]:
            item = {
                "signal_id": str(row["signal_id"] or ""),
                "trade_opportunity_id": str(row["trade_opportunity_id"] or ""),
                "asset": str(row["asset"] or ""),
                "pair": str(row["pair"] or ""),
                "signal_ts": _to_int(row["signal_ts"]),
                "data_valid": bool(int(row["data_valid"] or 0)),
                "invalid_reason": row["invalid_reason"],
                "label": row["label"],
                "entry_price": _to_float(row["entry_price"]),
                "exit_price": _to_float(row["exit_price"]),
                "net_pnl_bps": _to_float(row["net_pnl_bps"]),
            }
            if not item["data_valid"]:
                item["invalid_reason"] = _normalize_invalid_reason(item.get("invalid_reason"), stage="entry")
            preview.append(item)
        summary["results"] = preview
        if not rows:
            diagnostics.warning("trade_replay_missing")
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
    valid_count = stats["valid_count"]
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
    valid_count = stats["valid_count"]
    if valid_count < MIN_PROFILE_REPLAY_SAMPLES:
        return 0.0
    sample_confidence = min(1.0, valid_count / 50.0)
    total_count = stats["sample_count"]
    data_quality = stats["valid_count"] / total_count if total_count > 0 else 0.0
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trade replay closure summary")
    parser.add_argument("--date", required=True, help="Logical date YYYY-MM-DD")
    parser.add_argument("--format", default="text", choices=("text", "json"), help="Output format")
    parser.add_argument("--dry-run", action="store_true", help="Read summary only")
    parser.add_argument("--include-suppressed", action="store_true", help="Compatibility flag; no-op in summary mode")
    parser.add_argument("--include-blocked", action="store_true", help="Compatibility flag; no-op in summary mode")
    return parser


def _render_text(summary: dict[str, Any]) -> str:
    lines = [
        f"trade_replay logical_date={summary.get('logical_date')}",
        f"status={summary.get('status')}",
        f"trade_replay_available={summary.get('trade_replay_available')}",
        f"replay_count={summary.get('replay_count')}",
        f"valid_replay_count={summary.get('valid_replay_count')}",
        f"sample_insufficient={summary.get('sample_insufficient')}",
        f"warnings={json.dumps(summary.get('warnings') or [], ensure_ascii=False)}",
        f"query_errors={json.dumps(summary.get('query_errors') or [], ensure_ascii=False)}",
        f"price_errors={json.dumps(summary.get('price_errors') or [], ensure_ascii=False)}",
        f"schema_errors={json.dumps(summary.get('schema_errors') or [], ensure_ascii=False)}",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = run_trade_replay(args.date, dry_run=bool(args.dry_run))
    if args.format == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(_render_text(summary), end="")
    return 0 if summary.get("status") in {"ok", "degraded"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
