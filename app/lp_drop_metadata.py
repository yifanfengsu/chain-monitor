from __future__ import annotations

from typing import Any

from constants import ERC20_TRANSFER_EVENT_SIG
from lp_registry import get_lp_pool_meta


LP_DROP_METADATA_VERSION = 1


LP_DROP_QUOTE_SYMBOLS = {
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


LP_DROP_SYMBOL_ALIASES = {
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


def _first(container: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = container.get(key)
        if value not in (None, "", [], {}, ()):
            return value
    return None


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("$", "").replace(" ", "")
    return LP_DROP_SYMBOL_ALIASES.get(text, text)


def _normalize_stage(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"prealert", "confirm", "exhaustion_risk"}:
        return text
    if "prealert" in text:
        return "prealert"
    if "exhaustion" in text:
        return "exhaustion_risk"
    if "confirm" in text or text == "confirmed":
        return "confirm"
    return "unknown"


def _normalize_pair(base: Any, quote: Any) -> tuple[str, str, str | None]:
    left = _normalize_symbol(base)
    right = _normalize_symbol(quote)
    if not left or not right or left == right:
        return left, right, None
    if left in LP_DROP_QUOTE_SYMBOLS and right not in LP_DROP_QUOTE_SYMBOLS:
        left, right = right, left
    if left in LP_DROP_QUOTE_SYMBOLS and right in LP_DROP_QUOTE_SYMBOLS:
        return left, right, None
    return left, right, f"{left}/{right}"


def _parse_pair(value: Any) -> tuple[str, str, str | None]:
    text = str(value or "").strip().upper().replace("-", "/").replace("_", "/").replace(" ", "")
    if "/" not in text:
        return "", "", None
    parts = [part for part in text.split("/") if part]
    if len(parts) != 2:
        return "", "", None
    return _normalize_pair(parts[0], parts[1])


def _topic_address(topic: Any) -> str:
    text = str(topic or "")
    if not text.startswith("0x"):
        text = f"0x{text}"
    return f"0x{text[-40:].lower()}" if len(text) >= 40 else ""


def _hex_int(value: Any) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    try:
        if text.startswith("0x"):
            return int(text, 16)
        return int(text)
    except ValueError:
        return 0


def _select_pool_address(source: dict[str, Any]) -> str:
    explicit = str(
        _first(source, "pool_address", "lp_pool_address", "address")
        or ""
    ).strip().lower()
    if explicit:
        return explicit
    pools = [
        str(item or "").strip().lower()
        for item in (source.get("touched_lp_pools") or [])
        if str(item or "").strip()
    ]
    if not pools:
        return ""
    counts = source.get("pool_transfer_count_by_pool")
    counts = counts if isinstance(counts, dict) else {}
    return sorted(
        pools,
        key=lambda item: (-int(counts.get(item) or 0), item),
    )[0]


def _intent_from_values(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        upper = text.upper()
        lower = text.lower()
        if upper in {
            "LONG_BIAS_OBSERVE",
            "SHORT_BIAS_OBSERVE",
            "DO_NOT_CHASE_LONG",
            "DO_NOT_CHASE_SHORT",
        }:
            return upper
        if lower in {"pool_buy_pressure", "pool_sell_pressure"}:
            return lower
    return None


def _side_from_values(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        upper = text.upper()
        lower = text.lower()
        if upper in {"LONG", "LONG_BIAS_OBSERVE", "DO_NOT_CHASE_LONG"}:
            return "long"
        if upper in {"SHORT", "SHORT_BIAS_OBSERVE", "DO_NOT_CHASE_SHORT"}:
            return "short"
        if lower in {"pool_buy_pressure", "buy_pressure", "buy", "long"} or "买入" in text:
            return "long"
        if lower in {"pool_sell_pressure", "sell_pressure", "sell", "short"} or "卖出" in text:
            return "short"
    return None


def _direction_from_logs(source: dict[str, Any], pool_address: str, pool_meta: dict[str, Any]) -> dict[str, Any]:
    base_token = str(
        pool_meta.get("base_token_contract")
        or pool_meta.get("base_contract")
        or ""
    ).lower()
    quote_token = str(
        pool_meta.get("quote_token_contract")
        or pool_meta.get("quote_contract")
        or ""
    ).lower()
    if not pool_address or not base_token or not quote_token:
        return {}
    base_net = 0
    quote_net = 0
    for log in source.get("logs") or []:
        if not isinstance(log, dict):
            continue
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        event_sig = str(topics[0] or "").lower()
        if event_sig and event_sig != str(ERC20_TRANSFER_EVENT_SIG).lower():
            continue
        token = str(log.get("address") or "").lower()
        if token not in {base_token, quote_token}:
            continue
        from_addr = _topic_address(topics[1])
        to_addr = _topic_address(topics[2])
        amount = _hex_int(log.get("data"))
        if amount <= 0:
            continue
        delta = 0
        if to_addr == pool_address:
            delta += amount
        if from_addr == pool_address:
            delta -= amount
        if token == base_token:
            base_net += delta
        elif token == quote_token:
            quote_net += delta
    if base_net < 0 and quote_net > 0:
        return {"side": "long", "intent_type": "pool_buy_pressure"}
    if base_net > 0 and quote_net < 0:
        return {"side": "short", "intent_type": "pool_sell_pressure"}
    return {}


def build_lp_prefilter_drop_metadata(
    source: dict[str, Any] | None,
    *,
    event_id: str = "",
    tx_hash: str = "",
    pool_address: str = "",
    drop_reason: str = "listener_prefilter/drop",
    event_ts: int | None = None,
) -> dict[str, Any]:
    """Build minimal replay metadata for LP prefilter drops without changing drop decisions."""
    source = dict(source or {})
    raw = source.get("raw") if isinstance(source.get("raw"), dict) else {}
    lp_context = source.get("lp_context") if isinstance(source.get("lp_context"), dict) else {}
    if not lp_context and isinstance(raw, dict):
        lp_context = raw.get("lp_context") if isinstance(raw.get("lp_context"), dict) else {}

    selected_pool = str(pool_address or _select_pool_address(source) or "").strip().lower()
    pool_meta = get_lp_pool_meta(selected_pool, active_only=False) or {}

    base = _first(
        source,
        "base",
        "base_symbol",
        "base_token_symbol",
        "asset",
        "asset_symbol",
    ) or _first(lp_context, "base", "base_symbol", "base_token_symbol") or _first(
        pool_meta,
        "base_symbol",
        "base_token_symbol",
        "canonical_asset",
    )
    quote = _first(
        source,
        "quote",
        "quote_symbol",
        "quote_token_symbol",
    ) or _first(lp_context, "quote", "quote_symbol", "quote_token_symbol") or _first(
        pool_meta,
        "quote_symbol",
        "quote_token_symbol",
        "canonical_quote_symbol",
    )
    pair_value = _first(
        source,
        "pair",
        "pair_label",
        "lp_pair",
        "lp_pair_label",
    ) or _first(lp_context, "pair", "pair_label", "lp_pair") or _first(
        pool_meta,
        "canonical_pair_label",
        "canonical_pair",
        "pair_label",
    )
    parsed_base, parsed_quote, pair = _parse_pair(pair_value)
    if not pair:
        parsed_base, parsed_quote, pair = _normalize_pair(base, quote)
    base = parsed_base or _normalize_symbol(base)
    quote = parsed_quote or _normalize_symbol(quote)

    intent = _intent_from_values(
        _first(source, "intent_type", "intent", "trade_action_key", "canonical_semantic_key"),
        _first(lp_context, "intent_type", "intent", "action", "direction"),
    )
    side = _side_from_values(
        _first(source, "side", "direction"),
        intent,
        _first(lp_context, "side", "direction", "action"),
    )
    if side is None or intent is None:
        derived = _direction_from_logs(source, selected_pool, pool_meta)
        side = side or derived.get("side")
        intent = intent or derived.get("intent_type")

    resolved_event_ts = event_ts
    if resolved_event_ts is None:
        for value in (
            source.get("event_ts"),
            source.get("block_ts"),
            source.get("timestamp"),
            source.get("ingest_ts"),
            source.get("archive_ts"),
            raw.get("event_ts") if isinstance(raw, dict) else None,
            raw.get("timestamp") if isinstance(raw, dict) else None,
        ):
            try:
                if value not in (None, "", [], {}, ()):
                    resolved_event_ts = int(float(value))
                    break
            except (TypeError, ValueError):
                continue

    return {
        "drop_metadata_version": LP_DROP_METADATA_VERSION,
        "event_id": str(event_id or source.get("event_id") or ""),
        "tx_hash": str(tx_hash or source.get("tx_hash") or source.get("hash") or ""),
        "pool_address": selected_pool,
        "pair": pair,
        "asset": base or None,
        "base": base or None,
        "quote": quote or None,
        "side": side,
        "intent_type": intent,
        "lp_stage": _normalize_stage(_first(source, "lp_stage", "lp_alert_stage", "stage")),
        "event_ts": resolved_event_ts,
        "drop_reason": drop_reason,
    }
