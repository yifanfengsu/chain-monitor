from __future__ import annotations

from constants import ETH_EQUIVALENT_SYMBOLS, QUOTE_TOKEN_SYMBOLS, STABLE_TOKEN_SYMBOLS
from config import (
    LP_MAJOR_ASSETS,
    LP_TREND_BTC_LIKE_SYMBOLS,
    LP_TREND_ETH_LIKE_SYMBOLS,
    LP_TREND_SOL_LIKE_SYMBOLS,
)


LP_STAGE_RANKS = {
    "prealert": 1,
    "confirm": 2,
    "climax": 3,
    "exhaustion_risk": 4,
}


def normalize_symbol(symbol: str | None) -> str:
    return str(symbol or "").strip().upper()


ETH_LIKE_SYMBOLS = {normalize_symbol(item) for item in LP_TREND_ETH_LIKE_SYMBOLS} | ETH_EQUIVALENT_SYMBOLS
BTC_LIKE_SYMBOLS = {normalize_symbol(item) for item in LP_TREND_BTC_LIKE_SYMBOLS} | {"WBTC", "BTC", "CBBTC"}
SOL_LIKE_SYMBOLS = {normalize_symbol(item) for item in LP_TREND_SOL_LIKE_SYMBOLS} | {"SOL", "WSOL"}
MAJOR_ASSET_SYMBOLS = {normalize_symbol(item) for item in LP_MAJOR_ASSETS}


def canonical_asset_symbol(symbol: str | None) -> str:
    normalized = normalize_symbol(symbol)
    if normalized in ETH_LIKE_SYMBOLS:
        return "ETH"
    if normalized in BTC_LIKE_SYMBOLS:
        return "BTC"
    if normalized in SOL_LIKE_SYMBOLS:
        return "SOL"
    return normalized


def is_quote_like_symbol(symbol: str | None) -> bool:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return False
    return normalized in {normalize_symbol(item) for item in QUOTE_TOKEN_SYMBOLS} | ETH_EQUIVALENT_SYMBOLS


def is_stable_symbol(symbol: str | None) -> bool:
    return normalize_symbol(symbol) in {normalize_symbol(item) for item in STABLE_TOKEN_SYMBOLS}


def is_major_asset_symbol(symbol: str | None) -> bool:
    normalized = canonical_asset_symbol(symbol)
    return normalized in {canonical_asset_symbol(item) for item in MAJOR_ASSET_SYMBOLS} or is_stable_symbol(normalized)


def lp_stage_rank(stage: str | None) -> int:
    return int(LP_STAGE_RANKS.get(str(stage or "").strip(), 0))


def stage_label_for_timing(timing: str | None) -> str:
    return {
        "leading": "领先",
        "confirming": "确认",
        "late": "偏晚",
    }.get(str(timing or "").strip(), "")


def lp_direction_bucket_for_event(event) -> str:
    intent_type = str(getattr(event, "intent_type", "") or "")
    if intent_type == "pool_buy_pressure":
        return "buy_pressure"
    if intent_type == "pool_sell_pressure":
        return "sell_pressure"
    if intent_type == "liquidity_addition":
        return "liquidity_addition"
    if intent_type == "liquidity_removal":
        return "liquidity_removal"
    if intent_type == "pool_rebalance":
        return "pool_rebalance"
    return ""


def pair_label_from_event(event) -> str:
    raw = getattr(event, "metadata", {}).get("raw") or {}
    lp_context = raw.get("lp_context") or {}
    return str(
        lp_context.get("pair_label")
        or getattr(event, "metadata", {}).get("pair_label")
        or lp_context.get("pool_label")
        or getattr(event, "metadata", {}).get("pool_label")
        or getattr(event, "address", "")
        or ""
    ).strip()


def pool_label_from_event(event) -> str:
    raw = getattr(event, "metadata", {}).get("raw") or {}
    lp_context = raw.get("lp_context") or {}
    return str(
        lp_context.get("pool_label")
        or lp_context.get("pair_label")
        or getattr(event, "metadata", {}).get("pool_label")
        or getattr(event, "address", "")
        or ""
    ).strip()


def venue_family_from_event(event) -> str:
    raw = getattr(event, "metadata", {}).get("raw") or {}
    lp_context = raw.get("lp_context") or {}
    return str(
        lp_context.get("dex")
        or lp_context.get("protocol")
        or getattr(event, "metadata", {}).get("lp_dex")
        or "onchain_lp"
    ).strip().lower()


def asset_symbol_from_event(event) -> str:
    raw = getattr(event, "metadata", {}).get("raw") or {}
    lp_context = raw.get("lp_context") or {}
    base_symbol = normalize_symbol(lp_context.get("base_token_symbol"))
    quote_symbol = normalize_symbol(lp_context.get("quote_token_symbol"))
    token_symbol = normalize_symbol(getattr(event, "metadata", {}).get("token_symbol") or getattr(event, "token", ""))

    if base_symbol and quote_symbol:
        if is_quote_like_symbol(base_symbol) and not is_quote_like_symbol(quote_symbol):
            return canonical_asset_symbol(quote_symbol)
        return canonical_asset_symbol(base_symbol)

    pair_label = pair_label_from_event(event)
    if "/" in pair_label:
        left, right = [canonical_asset_symbol(part) for part in pair_label.split("/", 1)]
        if is_quote_like_symbol(left) and not is_quote_like_symbol(right):
            return right
        if left:
            return left
        return right
    return canonical_asset_symbol(token_symbol)


def aligned_move(move: float | int | None, direction_bucket: str | None) -> float:
    try:
        numeric_move = float(move or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if str(direction_bucket or "") == "sell_pressure":
        return -numeric_move
    return numeric_move
