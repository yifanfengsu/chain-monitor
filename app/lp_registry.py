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
    RPC_URL,
)
from constants import (
    ETH_EQUIVALENT_CONTRACTS,
    ETH_EQUIVALENT_SYMBOLS,
    STABLE_TOKEN_METADATA,
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
RUNTIME_SUPPORTED_CHAIN_KEYS = {"ethereum"}
EVM_CHAIN_KEYS = {"ethereum", "base"}
SOLANA_CHAIN_KEYS = {"solana"}
BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
UNISWAP_V3_POOL_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"internalType": "uint24", "name": "", "type": "uint24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "factory",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]
UNISWAP_V3_ETHEREUM_FACTORY = "0x1f98431c8ad98523631ae4a59f267346ea31f984"


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


def _canonical_quote_symbol(value: str | None) -> str:
    normalized = _normalize_symbol_key(value)
    if normalized in {"USDC", "USDCE"}:
        return "USDC"
    if normalized == "USDT":
        return "USDT"
    return normalized


MAJOR_BASE_SYMBOLS = {
    _canonical_asset_symbol(item)
    for item in LP_MAJOR_ASSETS
    if str(item).strip()
}
MAJOR_QUOTE_SYMBOLS = {
    _canonical_quote_symbol(item)
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
    return priority if priority > 0 else 3


def _coerce_bool(value, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _is_hex_address(value: str | None) -> bool:
    raw = str(value or "").strip().lower()
    return bool(raw.startswith("0x") and len(raw) == 42 and all(ch in "0123456789abcdef" for ch in raw[2:]))


def _is_base58_address(value: str | None) -> bool:
    raw = str(value or "").strip()
    return bool(32 <= len(raw) <= 64 and all(ch in BASE58_ALPHABET for ch in raw))


def _normalize_chain_key(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "eth": "ethereum",
        "ethereum_mainnet": "ethereum",
        "mainnet": "ethereum",
    }
    return aliases.get(normalized, normalized)


def runtime_supported_chains() -> set[str]:
    return set(RUNTIME_SUPPORTED_CHAIN_KEYS)


def _is_runtime_supported_chain(chain: str | None) -> bool:
    return _normalize_chain_key(chain) in runtime_supported_chains()


def _is_pool_address_for_chain(value: str | None, chain: str | None) -> bool:
    normalized_chain = _normalize_chain_key(chain)
    if normalized_chain in EVM_CHAIN_KEYS:
        return _is_hex_address(value)
    if normalized_chain in SOLANA_CHAIN_KEYS:
        return _is_base58_address(value)
    return False


def is_placeholder_pool_address(value: str | None) -> bool:
    raw = str(value or "").strip()
    normalized = raw.lower()
    if not raw:
        return False
    if normalized == "0x0000000000000000000000000000000000000000":
        return True
    if any(marker in normalized for marker in ("placeholder", "todo", "fill_me", "replace_me", "<", ">")):
        return True
    return False


def _looks_placeholder_text(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in ("placeholder", "todo", "fill_me", "replace_me", "<", ">"))


def _display_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").upper()
    return "ETH" if normalized == "WETH" else normalized


def _pool_notes(item: dict) -> str:
    return str(item.get("notes") or item.get("note") or "").strip()


def _expected_major_pairs() -> list[str]:
    pairs = []
    for asset in sorted(MAJOR_BASE_SYMBOLS):
        if asset not in {"ETH", "BTC", "SOL"}:
            continue
        for quote in sorted(MAJOR_QUOTE_SYMBOLS):
            if quote not in {"USDT", "USDC"}:
                continue
            pairs.append(f"{asset}/{quote}")
    return pairs


def flatten_lp_pool_book_payload(payload) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    entries: list[dict] = []
    for section_name, value in payload.items():
        if not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, dict):
                continue
            entry = dict(item)
            entry.setdefault("example_section", str(section_name))
            entries.append(entry)
    return entries


def _normalize_fee_tier(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        fee_tier = int(value)
    except (TypeError, ValueError):
        return None
    return fee_tier if fee_tier >= 0 else None


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


def _pool_book_entry_view(item: dict, *, index: int | None = None) -> dict:
    pool_address = str(item.get("pool_address") or "").strip()
    enabled = _coerce_bool(item.get("enabled", item.get("is_active", True)), default=True)
    placeholder = _coerce_bool(item.get("placeholder"), default=False) or is_placeholder_pool_address(pool_address)
    chain = _normalize_chain_key(item.get("chain") or "ethereum")
    dex = str(item.get("dex") or "").strip()
    protocol = str(item.get("protocol") or "").strip()
    pool_type = str(item.get("pool_type") or "spot_lp").strip().lower()
    base_contract, base_symbol, quote_contract, quote_symbol = _resolve_base_quote(item)
    canonical_asset = _canonical_asset_symbol(item.get("canonical_asset") or base_symbol)
    canonical_quote_symbol = _canonical_quote_symbol(quote_symbol or item.get("quote_symbol"))
    pair_label = normalize_lp_pair_label(item.get("pair_label") or f"{base_symbol}/{quote_symbol}")
    canonical_pair = (
        f"{canonical_asset}/{canonical_quote_symbol}"
        if canonical_asset and canonical_quote_symbol
        else ""
    )
    derived_major_pool = bool(
        canonical_asset in MAJOR_BASE_SYMBOLS and canonical_quote_symbol in MAJOR_QUOTE_SYMBOLS
    )
    explicit_major_pool = item.get("major_pool")
    if explicit_major_pool is None:
        major_pool = derived_major_pool
    else:
        major_pool = _coerce_bool(explicit_major_pool, default=derived_major_pool)
    fee_tier = _normalize_fee_tier(item.get("fee_tier"))
    source_note = str(item.get("source_note") or "").strip()
    major_match_mode = str(
        item.get("major_match_mode")
        or ("major_family_match" if derived_major_pool else "non_major_pool")
    ).strip()
    validation_required = _coerce_bool(
        item.get("validation_required"),
        default=bool(protocol == "uniswap_v3"),
    )
    return {
        "index": index,
        "pool_address": pool_address,
        "enabled": enabled,
        "placeholder": placeholder,
        "chain": chain,
        "dex": dex,
        "protocol": protocol,
        "pool_type": pool_type,
        "base_contract": base_contract,
        "base_symbol": base_symbol,
        "quote_contract": quote_contract,
        "quote_symbol": quote_symbol,
        "canonical_asset": canonical_asset,
        "canonical_quote_symbol": canonical_quote_symbol,
        "quote_canonical": canonical_quote_symbol,
        "pair_label": pair_label,
        "canonical_pair": canonical_pair,
        "priority": _normalize_priority(item.get("priority", 3)),
        "priority_raw": item.get("priority"),
        "fee_tier": fee_tier,
        "major_pool": major_pool,
        "derived_major_pool": derived_major_pool,
        "major_match_mode": major_match_mode,
        "source_note": source_note,
        "validation_required": validation_required,
        "example_section": str(item.get("example_section") or ""),
        "notes": _pool_notes(item),
    }


def _known_symbol_contracts(symbol: str | None) -> set[str]:
    normalized = _normalize_symbol_key(symbol)
    if not normalized:
        return set()
    if normalized in {"USDC", "USDT", "DAI", "BUSD", "FRAX", "TUSD", "USDP"}:
        return {
            address
            for address, meta in STABLE_TOKEN_METADATA.items()
            if _normalize_symbol_key(meta.get("symbol")) == normalized
        }
    if normalized in {"BTC", "WBTC"} and str(WBTC_TOKEN_CONTRACT or "").strip():
        return {str(WBTC_TOKEN_CONTRACT).lower()}
    if normalized in {"ETH", "WETH"} and ETH_EQUIVALENT_CONTRACTS:
        return {str(item).lower() for item in ETH_EQUIVALENT_CONTRACTS}
    return set()


def _expected_contracts_for_entry(resolved: dict, *, role: str) -> set[str]:
    explicit_contract = str(resolved.get(f"{role}_contract") or "").strip().lower()
    if explicit_contract:
        return {explicit_contract}
    symbol = resolved.get(f"{role}_symbol")
    if role == "base":
        symbol = resolved.get("base_symbol") or resolved.get("canonical_asset") or symbol
    if role == "quote":
        symbol = resolved.get("quote_symbol") or resolved.get("canonical_quote_symbol") or symbol
    return _known_symbol_contracts(symbol)


def _default_uniswap_v3_pool_validation(item: dict, resolved: dict) -> dict:
    del item
    if not str(RPC_URL or "").strip():
        return {"status": "unavailable", "reasons": ["rpc_url_missing"]}
    try:
        from web3 import Web3
    except Exception:
        return {"status": "unavailable", "reasons": ["web3_missing"]}

    pool_address = str(resolved.get("pool_address") or "").strip()
    if not _is_hex_address(pool_address):
        return {"status": "failed", "reasons": ["invalid_pool_address"]}
    try:
        provider = Web3.HTTPProvider(str(RPC_URL), request_kwargs={"timeout": 5})
        web3 = Web3(provider)
        contract = web3.eth.contract(
            address=Web3.to_checksum_address(pool_address),
            abi=UNISWAP_V3_POOL_ABI,
        )
        token0 = str(contract.functions.token0().call()).lower()
        token1 = str(contract.functions.token1().call()).lower()
        fee = int(contract.functions.fee().call())
        factory = str(contract.functions.factory().call()).lower()
    except Exception as exc:
        return {
            "status": "unavailable",
            "reasons": [f"rpc_validation_error:{type(exc).__name__}"],
        }
    return {
        "status": "passed",
        "token0": token0,
        "token1": token1,
        "fee": fee,
        "factory": factory,
        "reasons": [],
    }


def _validate_enabled_pool_entry(
    item: dict,
    resolved: dict,
    *,
    onchain_validator=None,
) -> dict:
    chain = _normalize_chain_key(resolved.get("chain"))
    protocol = str(resolved.get("protocol") or "").strip().lower()
    validation_required = bool(resolved.get("validation_required"))
    if not validation_required or chain != "ethereum" or protocol != "uniswap_v3":
        return {"status": "not_required", "reasons": []}

    validator = onchain_validator or _default_uniswap_v3_pool_validation
    try:
        payload = validator(dict(item), dict(resolved)) or {}
    except Exception as exc:
        payload = {
            "status": "unavailable",
            "reasons": [f"validator_exception:{type(exc).__name__}"],
        }

    status = str(payload.get("status") or "").strip().lower() or "unavailable"
    reasons = list(payload.get("reasons") or [])
    token0 = str(payload.get("token0") or "").strip().lower()
    token1 = str(payload.get("token1") or "").strip().lower()
    actual_fee = _normalize_fee_tier(payload.get("fee"))
    actual_factory = str(payload.get("factory") or "").strip().lower()
    if status not in {"passed", "failed", "unavailable"}:
        status = "unavailable"
        if not reasons:
            reasons.append("validation_status_unknown")

    expected_base_contracts = _expected_contracts_for_entry(resolved, role="base")
    expected_quote_contracts = _expected_contracts_for_entry(resolved, role="quote")
    expected_fee_tier = _normalize_fee_tier(resolved.get("fee_tier"))
    expected_factory = UNISWAP_V3_ETHEREUM_FACTORY if chain == "ethereum" else ""

    if status == "passed":
        if not token0 or not token1:
            reasons.append("missing_token0_or_token1")
        elif expected_base_contracts and expected_quote_contracts:
            expected_pairs = {
                (base_contract, quote_contract)
                for base_contract in expected_base_contracts
                for quote_contract in expected_quote_contracts
            }
            if (token0, token1) not in expected_pairs and (token1, token0) not in expected_pairs:
                reasons.append("token_pair_mismatch")
        else:
            if not expected_base_contracts:
                reasons.append("missing_expected_base_contract")
            if not expected_quote_contracts:
                reasons.append("missing_expected_quote_contract")
        if expected_fee_tier is not None and actual_fee != expected_fee_tier:
            reasons.append("fee_tier_mismatch")
        if expected_factory and actual_factory and actual_factory != expected_factory:
            reasons.append("factory_mismatch")
        if reasons:
            status = "failed"

    return {
        "status": status,
        "reasons": reasons,
        "token0": token0,
        "token1": token1,
        "fee": actual_fee,
        "factory": actual_factory,
    }


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
    major_quote_symbol = _canonical_quote_symbol(quote_symbol)
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
        "major_quote_symbol": major_quote_symbol if is_major_pool else "",
        "trend_pool_family": trend_pool_family,
        "trend_base_family": base_family,
        "trend_quote_family": quote_family,
        "trend_pool_match_mode": match_mode,
    }


def _normalize_pool(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    resolved = _pool_book_entry_view(item)
    chain = _normalize_chain_key(resolved.get("chain"))
    pool_address = str(resolved["pool_address"] or "").lower()
    if (
        not pool_address
        or resolved["placeholder"]
        or not _is_runtime_supported_chain(chain)
        or not _is_pool_address_for_chain(pool_address, chain)
    ):
        return None
    if bool(resolved.get("enabled")):
        validation_payload = _validate_enabled_pool_entry(item, resolved)
        if str(validation_payload.get("status") or "") in {"failed", "unavailable"}:
            return None

    pair_label = str(resolved["pair_label"] or "").strip()
    dex = str(resolved["dex"] or "").strip() or "DEX"
    protocol = str(resolved["protocol"] or "").strip() or dex.lower().replace(" ", "_")
    pool_type = str(resolved["pool_type"] or "spot_lp").strip().lower() or "spot_lp"
    base_contract = str(resolved["base_contract"] or "").lower()
    base_symbol = str(resolved["base_symbol"] or "")
    quote_contract = str(resolved["quote_contract"] or "").lower()
    quote_symbol = str(resolved["quote_symbol"] or "")
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
        "priority": int(resolved["priority"] or 3),
        "enabled": bool(resolved["enabled"]),
        "is_active": bool(resolved["enabled"]),
        "canonical_asset": str(resolved["canonical_asset"] or ""),
        "canonical_pair_label": str(resolved["canonical_pair"] or ""),
        "notes": str(resolved["notes"] or ""),
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
        "note": str(resolved["notes"] or ""),
        "source": "lp_registry",
        **trend_context,
    }


def _load_lp_pool_book():
    try:
        with LP_POOLS_PATH.open(encoding="utf-8") as fp:
            return flatten_lp_pool_book_payload(json.load(fp))
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
    for raw_item in flatten_lp_pool_book_payload(entries):
        normalized = _normalize_pool(raw_item)
        if normalized is None:
            continue
        pools[normalized["pool_address"]] = normalized
    return pools


def validate_lp_pool_book_entries(entries=None, *, onchain_validator=None) -> dict:
    payload = flatten_lp_pool_book_payload(entries if entries is not None else LP_POOL_BOOK)
    expected_pairs = _expected_major_pairs()
    covered_major_pairs: set[str] = set()
    configured_major_pairs: set[str] = set()
    configured_but_disabled_major_pools: list[dict] = []
    configured_but_unsupported_chain: list[dict] = []
    configured_but_validation_failed: list[dict] = []
    malformed_major_pool_entries: list[dict] = []
    placeholder_major_pool_entries: list[dict] = []
    enabled_major_pools: list[dict] = []
    duplicate_pool_warnings: list[dict] = []
    recommended_local_config_actions: list[str] = []
    seen_addresses: dict[str, list[int]] = {}
    seen_major_pair_priority: dict[tuple[str, int], list[int]] = {}

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
        resolved = _pool_book_entry_view(item, index=index)
        pool_address = str(resolved["pool_address"] or "")
        enabled = bool(resolved["enabled"])
        placeholder = bool(resolved["placeholder"])
        base_symbol = str(resolved["base_symbol"] or "")
        quote_symbol = str(resolved["quote_symbol"] or "")
        canonical_asset = str(resolved["canonical_asset"] or "")
        canonical_pair = str(resolved["canonical_pair"] or "")
        pair_label = str(resolved["pair_label"] or "")
        dex = str(resolved["dex"] or "")
        protocol = str(resolved["protocol"] or "")
        chain = str(resolved["chain"] or "")
        pool_type = str(resolved["pool_type"] or "")
        priority_raw = resolved["priority_raw"]
        priority = int(resolved["priority"] or 3)
        fee_tier = _normalize_fee_tier(resolved.get("fee_tier"))
        major_match_mode = str(resolved["major_match_mode"] or "")
        source_note = str(resolved.get("source_note") or "")
        example_section = str(resolved.get("example_section") or "")
        notes = str(resolved["notes"] or "")
        canonical_quote_symbol = str(resolved["canonical_quote_symbol"] or "")
        is_major_candidate = bool(
            canonical_pair in expected_pairs
            or (resolved["major_pool"] and canonical_asset in {"ETH", "BTC", "SOL"})
        )
        if is_major_candidate and canonical_pair:
            configured_major_pairs.add(canonical_pair)
        malformed_reasons = []
        if not pool_address:
            malformed_reasons.append("missing_pool_address")
        elif placeholder and enabled:
            malformed_reasons.append("placeholder_enabled")
        elif not placeholder and not _is_pool_address_for_chain(pool_address, chain):
            malformed_reasons.append("invalid_pool_address")
        if not pair_label or "/" not in pair_label:
            malformed_reasons.append("missing_pair_label")
        if not base_symbol:
            malformed_reasons.append("missing_base_symbol")
        if not quote_symbol:
            malformed_reasons.append("missing_quote_symbol")
        if not canonical_asset:
            malformed_reasons.append("missing_canonical_asset")
        elif canonical_asset not in {"ETH", "BTC", "SOL"} and is_major_candidate:
            malformed_reasons.append("unsupported_canonical_asset")
        if not canonical_quote_symbol and is_major_candidate:
            malformed_reasons.append("missing_canonical_quote_symbol")
        elif canonical_quote_symbol not in {"USDT", "USDC"} and is_major_candidate:
            malformed_reasons.append("unsupported_quote_symbol")
        if not dex:
            malformed_reasons.append("missing_dex")
        if not protocol:
            malformed_reasons.append("missing_protocol")
        if priority_raw in (None, ""):
            malformed_reasons.append("missing_priority")
        elif not isinstance(priority_raw, bool):
            try:
                if int(priority_raw) <= 0:
                    malformed_reasons.append("invalid_priority")
            except (TypeError, ValueError):
                malformed_reasons.append("invalid_priority")
        if not chain:
            malformed_reasons.append("missing_chain")
        elif enabled and _looks_placeholder_text(chain):
            malformed_reasons.append("placeholder_chain")
        if not pool_type:
            malformed_reasons.append("missing_pool_type")
        elif enabled and _looks_placeholder_text(pool_type):
            malformed_reasons.append("placeholder_pool_type")
        if enabled and _looks_placeholder_text(dex):
            malformed_reasons.append("placeholder_dex")
        if enabled and _looks_placeholder_text(protocol):
            malformed_reasons.append("placeholder_protocol")
        if not major_match_mode and is_major_candidate:
            malformed_reasons.append("missing_major_match_mode")
        if not notes and not placeholder and is_major_candidate:
            malformed_reasons.append("missing_notes")

        if is_major_candidate and placeholder:
            placeholder_major_pool_entries.append(
                {
                    "index": index,
                    "pair_label": canonical_pair or pair_label,
                    "pool_address": pool_address,
                    "enabled": enabled,
                    "placeholder": True,
                }
            )

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
        if not _is_runtime_supported_chain(chain):
            configured_but_unsupported_chain.append(
                {
                    "index": index,
                    "pair_label": canonical_pair,
                    "configured_pair_label": pair_label,
                    "pool_address": pool_address,
                    "enabled": enabled,
                    "priority": priority,
                    "chain": chain,
                    "dex": dex,
                    "protocol": protocol,
                    "pool_type": pool_type,
                    "quote_canonical": canonical_quote_symbol,
                    "source_note": source_note,
                    "example_section": example_section,
                    "reason": "unsupported_chain",
                }
            )
            continue
        if pool_address:
            seen_addresses.setdefault(pool_address.lower(), []).append(index)
        seen_major_pair_priority.setdefault((canonical_pair, priority), []).append(index)
        validation_payload = _validate_enabled_pool_entry(
            item,
            resolved,
            onchain_validator=onchain_validator,
        ) if enabled else {"status": "not_required", "reasons": []}
        validation_status = str(validation_payload.get("status") or "")
        validation_reasons = list(validation_payload.get("reasons") or [])
        if enabled and validation_status in {"failed", "unavailable"}:
            configured_but_validation_failed.append(
                {
                    "index": index,
                    "pair_label": canonical_pair,
                    "configured_pair_label": pair_label,
                    "pool_address": pool_address,
                    "priority": priority,
                    "chain": chain,
                    "dex": dex,
                    "protocol": protocol,
                    "pool_type": pool_type,
                    "fee_tier": fee_tier,
                    "validation_status": validation_status,
                    "validation_reasons": validation_reasons,
                    "source_note": source_note,
                    "example_section": example_section,
                }
            )
            continue
        if enabled:
            covered_major_pairs.add(canonical_pair)
            enabled_major_pools.append(
                {
                    "index": index,
                    "pair_label": canonical_pair,
                    "configured_pair_label": pair_label,
                    "pool_address": pool_address,
                    "priority": priority,
                    "chain": chain,
                    "dex": dex,
                    "protocol": protocol,
                    "pool_type": pool_type,
                    "quote_canonical": canonical_quote_symbol,
                    "canonical_asset": canonical_asset,
                    "fee_tier": fee_tier,
                    "major_priority_score": float(LP_MAJOR_PRIORITY_SCORE),
                    "major_match_mode": major_match_mode,
                    "validation_status": validation_status,
                    "validation_reasons": validation_reasons,
                    "source_note": source_note,
                    "example_section": example_section,
                    "notes": notes,
                }
            )
        elif not placeholder:
            configured_but_disabled_major_pools.append(
                {
                    "index": index,
                    "pair_label": canonical_pair,
                    "configured_pair_label": pair_label,
                    "pool_address": pool_address,
                    "placeholder": False,
                    "priority": priority,
                    "chain": chain,
                    "dex": dex,
                    "protocol": protocol,
                    "pool_type": pool_type,
                    "fee_tier": fee_tier,
                    "quote_canonical": canonical_quote_symbol,
                    "source_note": source_note,
                    "example_section": example_section,
                }
            )

    for pool_address, indexes in sorted(seen_addresses.items()):
        if len(indexes) < 2:
            continue
        duplicate_pool_warnings.append(
            {
                "warning_type": "duplicate_pool_address",
                "pool_address": pool_address,
                "indexes": indexes,
                "message": f"duplicate pool address {pool_address} 出现多次，建议合并或保留唯一条目",
            }
        )
    for (pair_label, priority), indexes in sorted(seen_major_pair_priority.items()):
        if len(indexes) < 2:
            continue
        duplicate_pool_warnings.append(
            {
                "warning_type": "major_pair_priority_conflict",
                "pair_label": pair_label,
                "priority": priority,
                "indexes": indexes,
                "message": f"{pair_label} 存在多个相同 priority={priority} 的 pool，建议拆分 priority",
            }
        )

    missing_major_pairs = [pair for pair in expected_pairs if pair not in configured_major_pairs]
    if missing_major_pairs:
        recommended_local_config_actions.append(
            "补齐本地 data/lp_pools.json：优先补 " + ", ".join(missing_major_pairs[:6])
        )
    if placeholder_major_pool_entries:
        recommended_local_config_actions.append(
            "placeholder major pools 只能作本地模板；必须填真实 pool_address，且在核对 chain/dex/pool_type 前保持 enabled=false"
        )
    if configured_but_disabled_major_pools:
        recommended_local_config_actions.append(
            "校验并启用已配置但 disabled 的 major pools，避免 majors 覆盖停留在 ETH"
        )
    if configured_but_unsupported_chain:
        recommended_local_config_actions.append(
            "Base/Solana pools 需要对应链 listener/RPC；未支持前不会进入 Ethereum active scan"
        )
    if configured_but_validation_failed:
        recommended_local_config_actions.append(
            "enabled 的 Uniswap v3 major pools 需先通过 token0/token1/fee/factory 校验，再进入 majors 覆盖"
        )
    if malformed_major_pool_entries:
        recommended_local_config_actions.append(
            "修正 malformed major pool entries；placeholder 地址必须保持 disabled"
        )
    if duplicate_pool_warnings:
        recommended_local_config_actions.append(
            "同一 major pair 可以保留多个 pool，但请拆分 priority，并去重 duplicate pool address"
        )
    return {
        "expected_major_pairs": expected_pairs,
        "covered_major_pairs": sorted(covered_major_pairs),
        "missing_major_pairs": missing_major_pairs,
        "configured_but_disabled_major_pools": configured_but_disabled_major_pools,
        "configured_but_unsupported_chain": configured_but_unsupported_chain,
        "configured_but_validation_failed": configured_but_validation_failed,
        "malformed_major_pool_entries": malformed_major_pool_entries,
        "placeholder_major_pool_entries": placeholder_major_pool_entries,
        "enabled_major_pools": enabled_major_pools,
        "duplicate_pool_warnings": duplicate_pool_warnings,
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
