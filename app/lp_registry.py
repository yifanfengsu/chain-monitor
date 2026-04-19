import json
from pathlib import Path

from config import (
    LP_MAJOR_ASSETS,
    LP_MAJOR_PRIORITY_SCORE,
    LP_MAJOR_QUOTES,
    LP_TREND_BTC_LIKE_SYMBOLS,
    LP_TREND_ETH_LIKE_SYMBOLS,
    LP_TREND_PRIMARY_PAIR_LABELS,
    LP_TREND_PRIMARY_PAIR_OVERRIDES,
    LP_TREND_SOL_LIKE_SYMBOLS,
    LP_TREND_STABLE_SYMBOLS,
)
from constants import (
    ETH_EQUIVALENT_CONTRACTS,
    ETH_EQUIVALENT_SYMBOLS,
    STABLE_TOKEN_CONTRACTS,
    STABLE_TOKEN_SYMBOLS,
    WBTC_TOKEN_CONTRACT,
)


LP_POOLS_PATH = Path(__file__).resolve().parent.parent / "data" / "lp_pools.json"
PRIMARY_TREND_PAIR_LABELS = {
    str(item).strip().upper().replace(" ", "")
    for item in LP_TREND_PRIMARY_PAIR_LABELS
    if str(item).strip()
}
PRIMARY_TREND_PAIR_OVERRIDES = {
    str(item).strip().upper().replace(" ", "")
    for item in LP_TREND_PRIMARY_PAIR_OVERRIDES
    if str(item).strip()
}


def _normalize_symbol_key(value: str | None) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace(".", "")
    )


def _canonical_asset_symbol(value: str | None) -> str:
    normalized = _normalize_symbol_key(value)
    if normalized in {
        *{
            _normalize_symbol_key(item)
            for item in ETH_EQUIVALENT_SYMBOLS
        },
        *{
            _normalize_symbol_key(item)
            for item in LP_TREND_ETH_LIKE_SYMBOLS
        },
    }:
        return "ETH"
    if normalized in {
        *{
            _normalize_symbol_key(item)
            for item in LP_TREND_BTC_LIKE_SYMBOLS
        },
        "BTC",
        "WBTC",
        "CBBTC",
    }:
        return "BTC"
    if normalized in {
        *{
            _normalize_symbol_key(item)
            for item in LP_TREND_SOL_LIKE_SYMBOLS
        },
        "SOL",
        "WSOL",
    }:
        return "SOL"
    return normalized


MAJOR_BASE_SYMBOLS = {
    _canonical_asset_symbol(item)
    for item in LP_MAJOR_ASSETS
    if str(item).strip()
}
MAJOR_QUOTE_SYMBOLS = {
    _normalize_symbol_key(item)
    for item in LP_MAJOR_QUOTES
    if str(item).strip()
}


TREND_BASE_FAMILY_SYMBOLS = {
    "eth_like": {
        *{
            str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
            for item in ETH_EQUIVALENT_SYMBOLS
        },
        *{
            str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
            for item in LP_TREND_ETH_LIKE_SYMBOLS
        },
    },
    "btc_like": {
        str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
        for item in LP_TREND_BTC_LIKE_SYMBOLS
        if str(item).strip()
    },
    "sol_like": {
        str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
        for item in LP_TREND_SOL_LIKE_SYMBOLS
        if str(item).strip()
    },
}
TREND_BASE_FAMILY_CONTRACTS = {
    "eth_like": set(ETH_EQUIVALENT_CONTRACTS),
    "btc_like": {str(WBTC_TOKEN_CONTRACT or "").lower()} if str(WBTC_TOKEN_CONTRACT or "").strip() else set(),
    "sol_like": set(),
}
TREND_QUOTE_FAMILY_SYMBOLS = {
    "stable": {
        *{
            str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
            for item in STABLE_TOKEN_SYMBOLS
        },
        *{
            str(item).strip().upper().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")
            for item in LP_TREND_STABLE_SYMBOLS
        },
    },
}
TREND_QUOTE_FAMILY_CONTRACTS = {
    "stable": set(STABLE_TOKEN_CONTRACTS),
}


def _normalize_priority(value) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        priority = 3
    return priority if priority in (1, 2, 3) else 3


def _is_hex_address(value: str | None) -> bool:
    raw = str(value or "").strip().lower()
    return bool(raw.startswith("0x") and len(raw) == 42 and all(ch in "0123456789abcdef" for ch in raw[2:]))


def is_placeholder_pool_address(value: str | None) -> bool:
    raw = str(value or "").strip()
    normalized = raw.lower()
    if not raw:
        return True
    if any(marker in normalized for marker in ("placeholder", "todo", "fill_me", "replace_me", "<", ">")):
        return True
    return not _is_hex_address(raw)


def _display_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").upper()
    return "ETH" if normalized == "WETH" else normalized


def normalize_lp_pair_label(value: str | None) -> str:
    normalized = str(value or "").strip().upper().replace(" ", "")
    for sep in ("-", "_", ":"):
        normalized = normalized.replace(sep, "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized

def _is_stable(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in STABLE_TOKEN_CONTRACTS or symbol in STABLE_TOKEN_SYMBOLS


def _is_eth_like(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in ETH_EQUIVALENT_CONTRACTS or symbol in ETH_EQUIVALENT_SYMBOLS


def _resolve_base_quote(item: dict) -> tuple[str, str, str, str]:
    explicit_base_symbol = _display_symbol(item.get("base_symbol"))
    explicit_quote_symbol = _display_symbol(item.get("quote_symbol"))
    explicit_base_contract = str(item.get("base_contract") or item.get("base_token_contract") or "").lower()
    explicit_quote_contract = str(item.get("quote_contract") or item.get("quote_token_contract") or "").lower()
    if explicit_base_symbol and explicit_quote_symbol:
        return explicit_base_contract, explicit_base_symbol, explicit_quote_contract, explicit_quote_symbol

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


def _token_family(
    token_contract: str | None,
    token_symbol: str | None,
    *,
    family_contracts: dict[str, set[str]],
    family_symbols: dict[str, set[str]],
) -> str:
    contract = str(token_contract or "").lower()
    symbol_key = _normalize_symbol_key(token_symbol)
    for family_name, contracts in family_contracts.items():
        if contract and contract in contracts:
            return family_name
    for family_name, symbols in family_symbols.items():
        if symbol_key and symbol_key in symbols:
            return family_name
    return "other"


def classify_trend_pool_meta(meta: dict | None) -> dict:
    meta = meta or {}
    pair_label = normalize_lp_pair_label(meta.get("pair_label"))
    base_symbol = _display_symbol(meta.get("base_token_symbol") or meta.get("token0_symbol"))
    quote_symbol = _display_symbol(meta.get("quote_token_symbol") or meta.get("token1_symbol"))
    derived_pair = normalize_lp_pair_label(
        f"{base_symbol}/{quote_symbol}" if base_symbol and quote_symbol else ""
    )
    candidate_pairs = {item for item in {pair_label, derived_pair} if item}

    base_family = _token_family(
        meta.get("base_token_contract") or meta.get("token0_contract"),
        base_symbol,
        family_contracts=TREND_BASE_FAMILY_CONTRACTS,
        family_symbols=TREND_BASE_FAMILY_SYMBOLS,
    )
    quote_family = _token_family(
        meta.get("quote_token_contract") or meta.get("token1_contract"),
        quote_symbol,
        family_contracts=TREND_QUOTE_FAMILY_CONTRACTS,
        family_symbols=TREND_QUOTE_FAMILY_SYMBOLS,
    )
    major_base_symbol = _canonical_asset_symbol(base_symbol)
    major_quote_symbol = _normalize_symbol_key(quote_symbol)
    is_major_pool = bool(major_base_symbol in MAJOR_BASE_SYMBOLS and major_quote_symbol in MAJOR_QUOTE_SYMBOLS)
    major_match_mode = "major_family_match" if is_major_pool else "non_major_pool"

    match_mode = "non_trend_pool"
    is_primary = False
    if candidate_pairs & PRIMARY_TREND_PAIR_LABELS:
        match_mode = "explicit_whitelist"
        is_primary = True
    elif candidate_pairs & PRIMARY_TREND_PAIR_OVERRIDES:
        match_mode = "override_match"
        is_primary = True
    elif base_family in {"eth_like", "btc_like", "sol_like"} and quote_family == "stable":
        match_mode = "family_match"
        is_primary = True
    elif is_major_pool:
        match_mode = "major_family_match"
        is_primary = True

    trend_pool_family = ""
    if is_primary and base_family in {"eth_like", "btc_like", "sol_like"} and quote_family == "stable":
        trend_pool_family = f"{base_family}_stable"

    return {
        "is_primary_trend_pool": bool(is_primary),
        "is_major_pool": is_major_pool,
        "major_priority_score": float(LP_MAJOR_PRIORITY_SCORE if is_major_pool else 1.0),
        "major_match_mode": major_match_mode,
        "major_base_symbol": major_base_symbol if is_major_pool else "",
        "major_quote_symbol": quote_symbol if is_major_pool else "",
        "trend_pool_family": trend_pool_family,
        "trend_base_family": base_family,
        "trend_quote_family": quote_family,
        "trend_pool_match_mode": match_mode,
    }


def _normalize_pool(item: dict) -> dict | None:
    pool_address = str(item.get("pool_address") or "").lower()
    if not pool_address or is_placeholder_pool_address(pool_address):
        return None

    pair_label = str(item.get("pair_label") or "").strip()
    dex = str(item.get("dex") or "").strip() or "DEX"
    protocol = str(item.get("protocol") or "").strip() or dex.lower().replace(" ", "_")
    chain = str(item.get("chain") or "ethereum").strip().lower() or "ethereum"
    pool_type = str(item.get("pool_type") or "spot_lp").strip().lower() or "spot_lp"
    base_contract, base_symbol, quote_contract, quote_symbol = _resolve_base_quote(item)
    trend_context = classify_trend_pool_meta(
        {
            "pair_label": pair_label or f"{base_symbol}/{quote_symbol}",
            "base_token_contract": base_contract,
            "base_token_symbol": base_symbol,
            "quote_token_contract": quote_contract,
            "quote_token_symbol": quote_symbol,
            "token0_contract": str(item.get("token0_contract") or "").lower(),
            "token0_symbol": _display_symbol(item.get("token0_symbol")),
            "token1_contract": str(item.get("token1_contract") or "").lower(),
            "token1_symbol": _display_symbol(item.get("token1_symbol")),
        }
    )

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
        "chain": chain,
        "pool_type": pool_type,
        "priority": _normalize_priority(item.get("priority", 3)),
        "is_active": bool(item.get("enabled", item.get("is_active", True))),
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
        **trend_context,
    }


def _load_lp_pool_book():
    try:
        with LP_POOLS_PATH.open(encoding="utf-8") as fp:
            return json.load(fp)
    except FileNotFoundError:
        print(f"Warning: {LP_POOLS_PATH.name} missing; using empty LP pool book.")
        return []
    except json.JSONDecodeError as exc:
        print(f"Warning: {LP_POOLS_PATH.name} invalid JSON; using empty LP pool book ({exc.msg}).")
        return []


LP_POOL_BOOK = _load_lp_pool_book()


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
ACTIVE_PRIMARY_TREND_LP_POOLS = {
    address: meta
    for address, meta in ACTIVE_LP_POOLS.items()
    if bool(meta.get("is_primary_trend_pool"))
}
ACTIVE_PRIMARY_TREND_SCAN_LP_POOLS = {
    address: meta
    for address, meta in ACTIVE_PRIMARY_TREND_LP_POOLS.items()
    if str(meta.get("trend_pool_match_mode") or "") in {"explicit_whitelist", "override_match"}
    or int(meta.get("priority") or 3) <= 2
    or bool(meta.get("is_major_pool"))
}
ACTIVE_MAJOR_LP_POOLS = {
    address: meta
    for address, meta in ACTIVE_LP_POOLS.items()
    if bool(meta.get("is_major_pool"))
}
ALL_LP_POOL_ADDRESSES = set(LP_POOLS.keys())
ACTIVE_LP_POOL_ADDRESSES = set(ACTIVE_LP_POOLS.keys())
ACTIVE_PRIMARY_TREND_LP_POOL_ADDRESSES = set(ACTIVE_PRIMARY_TREND_LP_POOLS.keys())
ACTIVE_PRIMARY_TREND_SCAN_LP_POOL_ADDRESSES = set(ACTIVE_PRIMARY_TREND_SCAN_LP_POOLS.keys())
ACTIVE_MAJOR_LP_POOL_ADDRESSES = set(ACTIVE_MAJOR_LP_POOLS.keys())
ACTIVE_EXTENDED_LP_POOL_ADDRESSES = {
    address
    for address in ACTIVE_LP_POOL_ADDRESSES
    if address not in ACTIVE_PRIMARY_TREND_SCAN_LP_POOL_ADDRESSES
}


def normalize_lp_pool_entries(entries: list[dict] | None) -> dict[str, dict]:
    pools: dict[str, dict] = {}
    for raw_item in entries or []:
        normalized = _normalize_pool(raw_item)
        if normalized is None:
            continue
        pools[normalized["pool_address"]] = normalized
    return pools


def validate_lp_pool_book_entries(entries: list[dict] | None = None) -> dict:
    payload = list(entries or LP_POOL_BOOK or [])
    expected_pairs = []
    for asset in sorted(MAJOR_BASE_SYMBOLS):
        if asset not in {"ETH", "BTC", "SOL"}:
            continue
        for quote in sorted(MAJOR_QUOTE_SYMBOLS):
            if quote not in {"USDT", "USDC"}:
                continue
            expected_pairs.append(f"{asset}/{quote}")

    covered_major_pairs: set[str] = set()
    configured_but_disabled_major_pools: list[dict] = []
    malformed_major_pool_entries: list[dict] = []
    recommended_local_config_actions: list[str] = []

    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            malformed_major_pool_entries.append(
                {
                    "index": index,
                    "reason": "entry_not_object",
                    "pair_label": "",
                    "pool_address": "",
                }
            )
            continue
        pool_address = str(item.get("pool_address") or "").strip()
        enabled = bool(item.get("enabled", item.get("is_active", True)))
        placeholder = bool(item.get("placeholder")) or is_placeholder_pool_address(pool_address)
        base_contract, base_symbol, quote_contract, quote_symbol = _resolve_base_quote(item)
        del base_contract, quote_contract
        pair_label = normalize_lp_pair_label(item.get("pair_label") or f"{base_symbol}/{quote_symbol}")
        canonical_pair = ""
        if base_symbol and quote_symbol:
            canonical_pair = f"{_canonical_asset_symbol(base_symbol)}/{_normalize_symbol_key(quote_symbol)}"
        is_major_candidate = canonical_pair in expected_pairs
        dex = str(item.get("dex") or "").strip()
        protocol = str(item.get("protocol") or "").strip()
        chain = str(item.get("chain") or "ethereum").strip().lower() or "ethereum"
        pool_type = str(item.get("pool_type") or "spot_lp").strip().lower() or "spot_lp"
        priority = item.get("priority")
        malformed_reasons = []
        if not pool_address:
            malformed_reasons.append("missing_pool_address")
        elif placeholder and enabled:
            malformed_reasons.append("placeholder_enabled")
        elif not placeholder and not _is_hex_address(pool_address):
            malformed_reasons.append("invalid_pool_address")
        if not pair_label or "/" not in pair_label:
            malformed_reasons.append("missing_pair_label")
        if not base_symbol:
            malformed_reasons.append("missing_base_symbol")
        if not quote_symbol:
            malformed_reasons.append("missing_quote_symbol")
        if not dex:
            malformed_reasons.append("missing_dex")
        if not protocol:
            malformed_reasons.append("missing_protocol")
        if priority in (None, ""):
            malformed_reasons.append("missing_priority")
        if not chain:
            malformed_reasons.append("missing_chain")
        if not pool_type:
            malformed_reasons.append("missing_pool_type")

        if is_major_candidate and malformed_reasons:
            malformed_major_pool_entries.append(
                {
                    "index": index,
                    "pair_label": canonical_pair or pair_label,
                    "pool_address": pool_address,
                    "enabled": enabled,
                    "reasons": malformed_reasons,
                }
            )
            continue
        if not is_major_candidate:
            continue
        if enabled:
            covered_major_pairs.add(canonical_pair)
        else:
            configured_but_disabled_major_pools.append(
                {
                    "index": index,
                    "pair_label": canonical_pair,
                    "pool_address": pool_address,
                    "placeholder": placeholder,
                }
            )

    missing_major_pairs = [pair for pair in expected_pairs if pair not in covered_major_pairs]
    if missing_major_pairs:
        recommended_local_config_actions.append(
            "补齐本地 data/lp_pools.json：优先补 " + ", ".join(missing_major_pairs[:6])
        )
    if configured_but_disabled_major_pools:
        recommended_local_config_actions.append(
            "校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH"
        )
    if malformed_major_pool_entries:
        recommended_local_config_actions.append(
            "修正 malformed major pool entries；placeholder 地址必须保持 disabled"
        )
    return {
        "expected_major_pairs": expected_pairs,
        "covered_major_pairs": sorted(covered_major_pairs),
        "missing_major_pairs": missing_major_pairs,
        "configured_but_disabled_major_pools": configured_but_disabled_major_pools,
        "malformed_major_pool_entries": malformed_major_pool_entries,
        "recommended_local_config_actions": recommended_local_config_actions,
    }


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


def is_primary_trend_pool_meta(meta: dict | None) -> bool:
    return bool(classify_trend_pool_meta(meta).get("is_primary_trend_pool"))
