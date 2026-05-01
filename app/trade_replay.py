"""Trade replay engine: simulate historical signal trades with realistic entry/exit rules.

This module provides:
1. Entry price resolution (signal_time + delay)
2. Exit simulation (stop loss / take profit / max hold / timeout)
3. PnL calculation (gross + fees + slippage)
4. MFE/MAE tracking
5. Replay label classification
6. Profile stats aggregation
"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from config import (
    OUTCOME_PREFER_OKX_MARK,
    OUTCOME_USE_MARKET_CONTEXT_PRICE,
)


# Default replay parameters
DEFAULT_ENTRY_DELAY_SEC = 5
DEFAULT_MAX_HOLD_SEC = 60
DEFAULT_STOP_LOSS_BPS = 30
DEFAULT_TAKE_PROFIT_BPS = 50
DEFAULT_FEE_BPS = 6
DEFAULT_SLIPPAGE_BPS = 5
MIN_PROFILE_REPLAY_SAMPLES = 10


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
) -> dict[str, Any]:
    """
    Replay a single signal as if it were a real trade.
    
    Args:
        signal_row: Signal record with signal_id, asset, direction, timestamp, etc.
        entry_delay_sec: Delay after signal before entry (simulates reaction time)
        max_hold_sec: Maximum hold duration
        stop_loss_bps: Stop loss threshold in basis points
        take_profit_bps: Take profit threshold in basis points
        fee_bps: Trading fee in basis points (one-way)
        slippage_bps: Slippage in basis points (one-way)
        state_manager: Optional state manager for price lookup
        market_context_adapter: Optional market context adapter
    
    Returns:
        Replay result with PnL, label, MFE/MAE, etc.
    """
    signal_id = str(signal_row.get("signal_id") or "")
    if not signal_id:
        return _invalid_replay("signal_id_missing")
    
    direction = _normalize_direction(signal_row.get("trade_opportunity_side") or signal_row.get("direction"))
    if direction not in {"LONG", "SHORT"}:
        return _invalid_replay("direction_ambiguous")
    
    signal_ts = _to_int(signal_row.get("archive_ts") or signal_row.get("timestamp") or signal_row.get("created_at"))
    if not signal_ts:
        return _invalid_replay("timestamp_missing")
    
    # 1. Get entry price
    entry_ts = signal_ts + entry_delay_sec
    entry_result = _get_entry_price(
        signal_row,
        entry_ts=entry_ts,
        state_manager=state_manager,
        market_context_adapter=market_context_adapter,
    )
    if not entry_result["valid"]:
        return _invalid_replay(entry_result["reason"], entry_ts=entry_ts)
    
    entry_price = entry_result["price"]
    entry_source = entry_result["source"]
    
    # 2. Simulate hold period and find exit
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
    )
    
    if not exit_result["valid"]:
        return _invalid_replay(exit_result["reason"], entry_ts=entry_ts, entry_price=entry_price)
    
    exit_price = exit_result["price"]
    exit_ts = exit_result["ts"]
    close_reason = exit_result["reason"]
    
    # 3. Calculate PnL
    gross_pnl_bps = _calc_gross_pnl_bps(entry_price, exit_price, direction)
    net_pnl_bps = gross_pnl_bps - (fee_bps * 2) - (slippage_bps * 2)
    
    # 4. Calculate MFE/MAE
    mfe_bps, mae_bps = _calc_mfe_mae(
        signal_row,
        entry_price=entry_price,
        entry_ts=entry_ts,
        direction=direction,
        max_hold_sec=max_hold_sec,
        state_manager=state_manager,
        market_context_adapter=market_context_adapter,
    )
    
    # 5. Classify replay label
    label = _classify_replay_label(
        net_pnl_bps=net_pnl_bps,
        mfe_bps=mfe_bps,
        mae_bps=mae_bps,
        close_reason=close_reason,
        direction=direction,
    )
    
    # 6. Build result
    hold_duration_sec = exit_ts - entry_ts
    return {
        "replay_id": f"replay_{signal_id}_{int(time.time())}",
        "signal_id": signal_id,
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
        "profile_key": str(signal_row.get("opportunity_profile_key") or ""),
        "opportunity_status": str(signal_row.get("trade_opportunity_status") or ""),
        "shadow_status": str(signal_row.get("trade_opportunity_shadow_status") or "NONE"),
        "created_at": int(time.time()),
    }


def aggregate_profile_stats(replay_examples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Aggregate replay results by profile_key.
    
    Returns:
        Dict mapping profile_key to aggregated stats with recommended_action.
    """
    stats_by_profile: dict[str, dict[str, Any]] = defaultdict(lambda: {
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
    })
    
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
    
    # Calculate derived metrics and recommended actions
    for profile_key, stats in stats_by_profile.items():
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


def _infer_profile_action(stats: dict[str, Any]) -> str:
    """Infer recommended action based on profile stats."""
    valid_count = stats["valid_count"]
    
    if valid_count < MIN_PROFILE_REPLAY_SAMPLES:
        return "needs_more_samples"
    
    win_rate = stats.get("win_rate", 0.0)
    avg_pnl = stats.get("avg_net_pnl_bps", 0.0)
    absorption_rate = stats.get("absorption_rate", 0.0)
    bad_entry_rate = stats.get("bad_entry_rate", 0.0)
    
    # Hard block: consistently loses money with high absorption
    if absorption_rate > 0.50 and avg_pnl < -10:
        return "hard_block"
    
    # Hard block: consistently bad entries
    if bad_entry_rate > 0.60 and avg_pnl < -5:
        return "hard_block"
    
    # Eligible for verified: strong win rate and positive PnL
    if win_rate >= 0.70 and avg_pnl > 25:
        return "eligible_verified"
    
    # Promote to candidate: decent win rate and positive PnL
    if win_rate >= 0.60 and avg_pnl > 15:
        return "promote_candidate"
    
    # Keep as observe only: neutral or slightly negative
    if win_rate >= 0.45 and avg_pnl > -10:
        return "keep_observe_only"
    
    # Block: consistently loses
    return "block_profile"


def _infer_profile_confidence(stats: dict[str, Any]) -> float:
    """Calculate confidence score for profile recommendation."""
    valid_count = stats["valid_count"]
    if valid_count < MIN_PROFILE_REPLAY_SAMPLES:
        return 0.0
    
    # Confidence increases with sample size
    sample_confidence = min(1.0, valid_count / 50.0)
    
    # Confidence decreases with high data invalid rate
    total_count = stats["sample_count"]
    data_quality = stats["valid_count"] / total_count if total_count > 0 else 0.0
    
    return round(sample_confidence * data_quality, 4)


def _get_entry_price(
    signal_row: dict[str, Any],
    *,
    entry_ts: int,
    state_manager=None,
    market_context_adapter=None,
) -> dict[str, Any]:
    """Get entry price at entry_ts."""
    # Try outcome price first (if available)
    outcome_price = _to_float(signal_row.get("outcome_price_start"))
    if outcome_price and outcome_price > 0:
        return {"valid": True, "price": outcome_price, "source": "outcome_price_start"}
    
    # Try market context if enabled
    if OUTCOME_USE_MARKET_CONTEXT_PRICE and market_context_adapter:
        token_or_pair = str(signal_row.get("pair_label") or signal_row.get("asset_symbol") or "")
        if token_or_pair:
            try:
                context = market_context_adapter.get_market_context(token_or_pair, entry_ts)
                price, source = _select_price_from_context(context)
                if price:
                    return {"valid": True, "price": price, "source": source}
            except Exception:
                pass
    
    # Try pool quote proxy
    if state_manager and hasattr(state_manager, "get_latest_pool_quote_proxy"):
        pool_address = str(signal_row.get("pool_address") or "")
        if pool_address:
            try:
                price = state_manager.get_latest_pool_quote_proxy(pool_address, max_age_sec=300, now_ts=entry_ts)
                if price:
                    return {"valid": True, "price": price, "source": "pool_quote_proxy"}
            except Exception:
                pass
    
    return {"valid": False, "price": None, "source": "unavailable", "reason": "entry_price_unavailable"}


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
) -> dict[str, Any]:
    """Simulate holding period and find exit point."""
    # For now, use simple timeout exit at max_hold_sec
    # In production, this would sample prices during hold period
    exit_ts = entry_ts + max_hold_sec
    
    # Try to get exit price from outcome
    outcome_60s_price = _to_float(signal_row.get("outcome_60s_price_end"))
    if outcome_60s_price and outcome_60s_price > 0 and max_hold_sec <= 60:
        # Check if stop loss or take profit hit
        move_bps = _calc_gross_pnl_bps(entry_price, outcome_60s_price, direction)
        if move_bps <= -stop_loss_bps:
            return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "stop_loss"}
        if move_bps >= take_profit_bps:
            return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "take_profit"}
        return {"valid": True, "price": outcome_60s_price, "ts": exit_ts, "source": "outcome_60s", "reason": "max_hold"}
    
    # Fallback: try market context at exit_ts
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
            except Exception:
                pass
    
    return {"valid": False, "price": None, "ts": exit_ts, "source": "unavailable", "reason": "exit_price_unavailable"}


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
    """Calculate Maximum Favorable Excursion and Maximum Adverse Excursion."""
    # Simplified: use outcome windows if available
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
    """Calculate gross PnL in basis points."""
    if direction == "LONG":
        return ((exit_price / entry_price) - 1.0) * 10000
    elif direction == "SHORT":
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
    """Classify replay result into a label."""
    # Clean followthrough: positive PnL, good MFE, small MAE
    if net_pnl_bps > 15:
        if mfe_bps is not None and mfe_bps > 20 and (mae_bps is None or mae_bps > -10):
            return "clean_followthrough"
    
    # Bad entry: immediate adverse move
    if mae_bps is not None and mae_bps < -20 and net_pnl_bps < 0:
        return "bad_entry"
    
    # Absorption reversal: good MFE but ended negative
    if mfe_bps is not None and mfe_bps > 15 and net_pnl_bps < -10:
        return "absorption_reversal"
    
    # Chop: small moves both ways
    if mfe_bps is not None and mae_bps is not None:
        if abs(mfe_bps) < 15 and abs(mae_bps) < 15:
            return "chop_no_edge"
    
    # Default: neutral
    if net_pnl_bps > 0:
        return "small_win"
    elif net_pnl_bps < 0:
        return "small_loss"
    return "neutral"


def _select_price_from_context(context: dict[str, Any]) -> tuple[float | None, str]:
    """Select best price from market context."""
    venue = str(context.get("market_context_venue") or "").strip().lower()
    mark_price = _to_float(context.get("perp_mark_price"))
    index_price = _to_float(context.get("perp_index_price"))
    last_price = _to_float(context.get("perp_last_price"))
    
    if venue == "okx_perp" and OUTCOME_PREFER_OKX_MARK and mark_price:
        return mark_price, "okx_mark"
    if index_price:
        return index_price, f"{venue}_index"
    if mark_price:
        return mark_price, f"{venue}_mark"
    if last_price:
        return last_price, f"{venue}_last"
    
    return None, "unavailable"


def _invalid_replay(reason: str, **kwargs) -> dict[str, Any]:
    """Return invalid replay result."""
    return {
        "replay_id": "",
        "signal_id": kwargs.get("signal_id", ""),
        "data_valid": False,
        "label": "data_invalid",
        "close_reason": reason,
        "net_pnl_bps": 0.0,
        "gross_pnl_bps": 0.0,
        "entry_price": kwargs.get("entry_price"),
        "exit_price": None,
        "mfe_bps": None,
        "mae_bps": None,
        **kwargs,
    }


def _normalize_direction(value: Any) -> str:
    """Normalize direction to LONG/SHORT."""
    raw = str(value or "").strip().upper()
    if raw in {"LONG", "BUY", "BUY_PRESSURE"}:
        return "LONG"
    if raw in {"SHORT", "SELL", "SELL_PRESSURE"}:
        return "SHORT"
    return raw


def _to_int(value: Any) -> int:
    """Convert to int."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float | None:
    """Convert to float."""
    try:
        f = float(value)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None
