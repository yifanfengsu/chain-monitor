import json
from pathlib import Path

from constants import ETH_EQUIVALENT_CONTRACTS, ETH_EQUIVALENT_SYMBOLS, STABLE_TOKEN_CONTRACTS, STABLE_TOKEN_SYMBOLS


LP_POOLS_PATH = Path(__file__).resolve().parent.parent / "data" / "lp_pools.json"


def _normalize_priority(value) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        priority = 3
    return priority if priority in (1, 2, 3) else 3


def _display_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").upper()
    return "ETH" if normalized == "WETH" else normalized


def _is_stable(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in STABLE_TOKEN_CONTRACTS or symbol in STABLE_TOKEN_SYMBOLS


def _is_eth_like(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in ETH_EQUIVALENT_CONTRACTS or symbol in ETH_EQUIVALENT_SYMBOLS


def _resolve_base_quote(item: dict) -> tuple[str, str, str, str]:
    token0_contract = str(item.get("token0_contract") or "").lower()
    token1_contract = str(item.get("token1_contract") or "").lower()
    token0_symbol = str(item.get("token0_symbol") or "").upper()
    token1_symbol = str(item.get("token1_symbol") or "").upper()

    token0_is_stable = _is_stable(token0_contract, token0_symbol)
    token1_is_stable = _is_stable(token1_contract, token1_symbol)
    token0_is_eth = _is_eth_like(token0_contract, token0_symbol)
    token1_is_eth = _is_eth_like(token1_contract, token1_symbol)

    if token0_is_stable and not token1_is_stable:
        return token1_contract, _display_symbol(token1_symbol), token0_contract, _display_symbol(token0_symbol)
    if token1_is_stable and not token0_is_stable:
        return token0_contract, _display_symbol(token0_symbol), token1_contract, _display_symbol(token1_symbol)
    if token0_is_eth and not token1_is_eth:
        return token0_contract, _display_symbol(token0_symbol), token1_contract, _display_symbol(token1_symbol)
    if token1_is_eth and not token0_is_eth:
        return token1_contract, _display_symbol(token1_symbol), token0_contract, _display_symbol(token0_symbol)
    return token0_contract, _display_symbol(token0_symbol), token1_contract, _display_symbol(token1_symbol)


def _normalize_pool(item: dict) -> dict | None:
    pool_address = str(item.get("pool_address") or "").lower()
    if not pool_address:
        return None

    pair_label = str(item.get("pair_label") or "").strip()
    dex = str(item.get("dex") or "").strip() or "DEX"
    protocol = str(item.get("protocol") or "").strip() or dex.lower().replace(" ", "_")
    base_contract, base_symbol, quote_contract, quote_symbol = _resolve_base_quote(item)

    return {
        "address": pool_address,
        "pool_address": pool_address,
        "pair_label": pair_label or f"{base_symbol}/{quote_symbol}",
        "label": f"{pair_label or f'{base_symbol}/{quote_symbol}'} Pool ({dex})",
        "token0_contract": str(item.get("token0_contract") or "").lower(),
        "token0_symbol": _display_symbol(item.get("token0_symbol")),
        "token1_contract": str(item.get("token1_contract") or "").lower(),
        "token1_symbol": _display_symbol(item.get("token1_symbol")),
        "base_token_contract": base_contract,
        "base_token_symbol": base_symbol,
        "quote_token_contract": quote_contract,
        "quote_token_symbol": quote_symbol,
        "dex": dex,
        "protocol": protocol,
        "priority": _normalize_priority(item.get("priority", 3)),
        "is_active": bool(item.get("is_active", True)),
        "category": "lp_pool",
        "category_label": "流动性池",
        "role": "liquidity_pool",
        "role_label": "流动性池",
        "strategy_role": "lp_pool",
        "strategy_role_label": "主流交易对池",
        "semantic_role": "liquidity_pool",
        "semantic_role_label": "流动性池",
        "display_role_label": "流动性池",
        "role_source": "lp_registry",
        "display": f"{pair_label or f'{base_symbol}/{quote_symbol}'} Pool ({dex})",
        "note": str(item.get("note") or "").strip(),
        "source": "lp_registry",
    }


with LP_POOLS_PATH.open(encoding="utf-8") as fp:
    LP_POOL_BOOK = json.load(fp)


LP_POOLS = {}
for raw_item in LP_POOL_BOOK:
    normalized = _normalize_pool(raw_item)
    if normalized is None:
        continue
    LP_POOLS[normalized["pool_address"]] = normalized

ACTIVE_LP_POOLS = {
    address: meta
    for address, meta in LP_POOLS.items()
    if bool(meta.get("is_active", True))
}
ALL_LP_POOL_ADDRESSES = set(LP_POOLS.keys())
ACTIVE_LP_POOL_ADDRESSES = set(ACTIVE_LP_POOLS.keys())


def is_lp_pool(address: str | None, active_only: bool = False) -> bool:
    normalized = str(address or "").lower()
    if not normalized:
        return False
    if active_only:
        return normalized in ACTIVE_LP_POOL_ADDRESSES
    return normalized in ALL_LP_POOL_ADDRESSES


def get_lp_pool(address: str | None, active_only: bool = False) -> dict | None:
    normalized = str(address or "").lower()
    if not normalized:
        return None
    if active_only:
        return ACTIVE_LP_POOLS.get(normalized)
    return LP_POOLS.get(normalized)


def get_lp_pool_meta(address: str | None, active_only: bool = False) -> dict | None:
    pool = get_lp_pool(address, active_only=active_only)
    if pool is None:
        return None
    return dict(pool)
