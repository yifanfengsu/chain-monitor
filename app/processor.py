import json
from pathlib import Path
import threading

from web3 import Web3

from constants import (
    DEX_ROUTERS,
    ERC20_TRANSFER_EVENT_SIG,
    ETH_EQUIVALENT_CONTRACTS,
    ETH_EQUIVALENT_SYMBOLS,
    QUOTE_TOKEN_CONTRACTS,
    QUOTE_TOKEN_SYMBOLS,
    STABLE_TOKEN_CONTRACTS,
    STABLE_TOKEN_METADATA,
)
from config import (
    LOW_CU_MODE,
    RPC_URL,
    TOKEN_METADATA_CACHE_ENABLE,
    TOKEN_METADATA_CACHE_PATH,
)
from filter import ALL_WATCH_ADDRESSES, get_address_meta
from liquidation_registry import scan_liquidation_context
from lp_analyzer import LPAnalyzer
from lp_registry import get_lp_pool, is_lp_pool
from rpc_resilience import rpc_call_with_backoff

# ERC20 常见方法签名（函数 selector）。
ERC20_TRANSFER_SIG = "0xa9059cbb"
ERC20_TRANSFER_FROM_SIG = "0x23b872dd"

# 用于链上读取 token 元信息（symbol/decimals）。
w3 = Web3(Web3.HTTPProvider(RPC_URL))
ERC20_METADATA_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# 本地缓存：先放稳定币已知精度/符号，未知 token 再按需查询并写入缓存。
TOKEN_DECIMALS = {
    token: meta["decimals"]
    for token, meta in STABLE_TOKEN_METADATA.items()
}
TOKEN_SYMBOLS = {
    token: meta["symbol"]
    for token, meta in STABLE_TOKEN_METADATA.items()
}
TOKEN_DECIMALS.update({token: 18 for token in ETH_EQUIVALENT_CONTRACTS})
TOKEN_SYMBOLS.update({token: "WETH" for token in ETH_EQUIVALENT_CONTRACTS})
LP_EVENT_ANALYZER = LPAnalyzer()
TOKEN_METADATA_CACHE_FILE = Path(TOKEN_METADATA_CACHE_PATH)
TOKEN_METADATA_CACHE_LOCK = threading.Lock()
TOKEN_METADATA_STATS = {
    "token_metadata_cache_hit": 0,
    "token_metadata_chain_lookup": 0,
    "token_metadata_cache_persisted": 0,
}


def _build_token_metadata_cache_payload() -> dict[str, dict]:
    payload = {}
    for token, symbol in TOKEN_SYMBOLS.items():
        decimals = TOKEN_DECIMALS.get(token)
        if decimals is None:
            continue
        payload[str(token).lower()] = {
            "symbol": str(symbol or ""),
            "decimals": int(decimals),
        }
    return payload


def _load_token_metadata_cache() -> None:
    if not bool(TOKEN_METADATA_CACHE_ENABLE or LOW_CU_MODE):
        return
    try:
        if not TOKEN_METADATA_CACHE_FILE.exists():
            return
        with TOKEN_METADATA_CACHE_FILE.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
        if not isinstance(payload, dict):
            return
        for token, meta in payload.items():
            token_address = str(token or "").lower()
            if not token_address or not isinstance(meta, dict):
                continue
            symbol = str(meta.get("symbol") or "").strip()
            decimals = meta.get("decimals")
            if symbol:
                TOKEN_SYMBOLS[token_address] = symbol
            if decimals is not None:
                try:
                    TOKEN_DECIMALS[token_address] = int(decimals)
                except (TypeError, ValueError):
                    continue
    except Exception as exc:
        print(f"⚠️ token metadata cache 加载失败: {exc}")


def _persist_token_metadata_cache_snapshot() -> None:
    if not bool(TOKEN_METADATA_CACHE_ENABLE or LOW_CU_MODE):
        return
    try:
        payload = _build_token_metadata_cache_payload()
        TOKEN_METADATA_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = TOKEN_METADATA_CACHE_FILE.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, sort_keys=True)
        tmp_path.replace(TOKEN_METADATA_CACHE_FILE)
        with TOKEN_METADATA_CACHE_LOCK:
            TOKEN_METADATA_STATS["token_metadata_cache_persisted"] = int(
                TOKEN_METADATA_STATS.get("token_metadata_cache_persisted") or 0
            ) + 1
    except Exception as exc:
        print(f"⚠️ token metadata cache 落盘失败: {exc}")


def _schedule_token_metadata_cache_persist() -> None:
    if not bool(TOKEN_METADATA_CACHE_ENABLE or LOW_CU_MODE):
        return
    try:
        thread = threading.Thread(
            target=_persist_token_metadata_cache_snapshot,
            name="token-metadata-cache-persist",
            daemon=True,
        )
        thread.start()
    except Exception as exc:
        print(f"⚠️ token metadata cache 异步落盘失败: {exc}")


def get_token_metadata_stats_snapshot() -> dict[str, int]:
    with TOKEN_METADATA_CACHE_LOCK:
        return dict(TOKEN_METADATA_STATS)


_load_token_metadata_cache()


def to_hex(input_data):
    """把不同输入（HexBytes/str/bytes）统一成 0x 前缀十六进制字符串。"""
    if input_data is None:
        return "0x"

    if isinstance(input_data, str):
        return input_data if input_data.startswith("0x") else f"0x{input_data}"

    if hasattr(input_data, "hex"):
        hex_data = input_data.hex()
        return hex_data if hex_data.startswith("0x") else f"0x{hex_data}"

    return str(input_data)


def get_token_metadata(token):
    """获取 token symbol/decimals，优先缓存，缺失时链上查询。"""
    token = token.lower()
    symbol = TOKEN_SYMBOLS.get(token)
    decimals = TOKEN_DECIMALS.get(token)

    if symbol and decimals is not None:
        with TOKEN_METADATA_CACHE_LOCK:
            TOKEN_METADATA_STATS["token_metadata_cache_hit"] = int(
                TOKEN_METADATA_STATS.get("token_metadata_cache_hit") or 0
            ) + 1
        return {"symbol": symbol, "decimals": decimals}

    with TOKEN_METADATA_CACHE_LOCK:
        TOKEN_METADATA_STATS["token_metadata_chain_lookup"] = int(
            TOKEN_METADATA_STATS.get("token_metadata_chain_lookup") or 0
        ) + 1

    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(token),
            abi=ERC20_METADATA_ABI,
        )

        if symbol is None:
            symbol = rpc_call_with_backoff(
                "processor.token_metadata.symbol",
                lambda: contract.functions.symbol().call(),
            )
            if isinstance(symbol, bytes):
                symbol = symbol.decode(errors="ignore").rstrip("\x00")

        if decimals is None:
            decimals = int(
                rpc_call_with_backoff(
                    "processor.token_metadata.decimals",
                    lambda: contract.functions.decimals().call(),
                )
            )

    except Exception:
        symbol = symbol or token[:10]
        decimals = 18 if decimals is None else decimals

    TOKEN_SYMBOLS[token] = symbol
    TOKEN_DECIMALS[token] = decimals
    _schedule_token_metadata_cache_persist()
    return {"symbol": symbol, "decimals": decimals}


def get_log_signature(log):
    """提取日志签名（topics[0]）。"""
    topics = log.get("topics") or []
    if not topics:
        return ""
    return to_hex(topics[0]).lower()


def is_transfer_log(log):
    """判断日志是否是 ERC20 Transfer 事件。"""
    topics = log.get("topics", [])
    return len(topics) >= 3 and get_log_signature(log) == ERC20_TRANSFER_EVENT_SIG


def parse_transfer_log(log):
    """把 ERC20 Transfer 日志解析成统一转账结构。"""
    if not is_transfer_log(log):
        return None

    token = log.get("address")
    if not token:
        return None

    token = token.lower()
    meta = get_token_metadata(token)
    data_hex = to_hex(log.get("data"))
    if data_hex == "0x":
        return None

    raw_value = int(data_hex, 16)

    return {
        "from": "0x" + to_hex(log["topics"][1])[-40:].lower(),
        "to": "0x" + to_hex(log["topics"][2])[-40:].lower(),
        "raw_value": raw_value,
        "value": raw_value / (10 ** meta["decimals"]),
        "token_contract": token,
        "token_symbol": meta["symbol"],
        "log_index": int(log.get("logIndex", 0)) if log.get("logIndex") is not None else 0,
    }


def is_erc20_transfer(tx):
    """判断交易 input 是否是 ERC20 transfer / transferFrom。"""
    input_data = to_hex(tx.get("input"))
    return input_data.startswith(ERC20_TRANSFER_SIG) or input_data.startswith(ERC20_TRANSFER_FROM_SIG)


def is_erc20_transfer_log(item):
    """保留历史入口：结构化日志输入标记。"""
    return item.get("kind") == "erc20_transfer_log"


def is_swap_candidate(item):
    """判断是否是 listener 生成的 swap 候选对象。"""
    return item.get("kind") == "swap_candidate"


def is_token_flow_candidate(item):
    """判断是否是 listener 生成的 token flow 候选对象。"""
    return item.get("kind") == "token_flow_candidate"


def parse_erc20_transfer_log(log):
    """解析单条 ERC20 日志并补上 tx_hash。"""
    transfer = parse_transfer_log(log)
    if not transfer:
        return None

    parsed = {
        "kind": "token_transfer",
        "from": transfer["from"],
        "to": transfer["to"],
        "value": transfer["value"],
        "token_contract": transfer["token_contract"],
        "token_symbol": transfer["token_symbol"],
        "tx_hash": to_hex(log.get("transactionHash")),
    }
    return _attach_context_fields(parsed)


def _is_exchange_meta(meta: dict) -> bool:
    return str(meta.get("role") or "") == "exchange" or str(meta.get("strategy_role") or "").startswith("exchange_")


def _is_router_meta(meta: dict, address: str) -> bool:
    addr = (address or "").lower()
    return (
        addr in DEX_ROUTERS
        or str(meta.get("role") or "") == "router"
        or str(meta.get("strategy_role") or "") == "aggregator_router"
        or str(meta.get("semantic_role") or "") == "router_contract"
    )


def _is_protocol_meta(meta: dict) -> bool:
    return (
        str(meta.get("role") or "") == "protocol"
        or str(meta.get("strategy_role") or "") == "protocol_treasury"
        or str(meta.get("semantic_role") or "") == "protocol_wallet"
    )


def _snapshot_meta(address: str) -> dict:
    meta = get_address_meta(address)
    return {
        "address": (address or "").lower(),
        "label": meta.get("label", ""),
        "role": meta.get("role", "unknown"),
        "strategy_role": meta.get("strategy_role", "unknown"),
        "semantic_role": meta.get("semantic_role", "unknown"),
        "is_watch_address": (address or "").lower() in ALL_WATCH_ADDRESSES,
    }


def _is_inventory_style_internal(from_meta: dict, to_meta: dict) -> bool:
    if not from_meta.get("address") or not to_meta.get("address"):
        return False
    if from_meta["is_watch_address"] and to_meta["is_watch_address"]:
        return True
    # 没有实体级别证据时，不再仅凭“同类角色”把转账解释成内部调拨。
    return False


def _normalize_symbol(symbol: str | None) -> str:
    return str(symbol or "").upper()


def _is_quote_asset(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = (token_contract or "").lower()
    symbol = _normalize_symbol(token_symbol)
    return contract in QUOTE_TOKEN_CONTRACTS or symbol in QUOTE_TOKEN_SYMBOLS


def _is_stable_asset(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = (token_contract or "").lower()
    symbol = _normalize_symbol(token_symbol)
    return contract in STABLE_TOKEN_CONTRACTS or symbol in {"USDT", "USDC", "DAI", "BUSD", "FRAX", "TUSD", "USDP", "STABLE"}


def _is_eth_quote_asset(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = (token_contract or "").lower()
    symbol = _normalize_symbol(token_symbol)
    return contract in ETH_EQUIVALENT_CONTRACTS or symbol in ETH_EQUIVALENT_SYMBOLS


def _build_context_hints(
    parsed: dict,
    watch_address: str = "",
    from_addr: str = "",
    to_addr: str = "",
    counterparty: str = "",
    stable_tokens_touched: list[str] | None = None,
    volatile_tokens_touched: list[str] | None = None,
    extra_addresses: list[str] | None = None,
) -> dict:
    stable_tokens_touched = [str(token).lower() for token in (stable_tokens_touched or []) if token]
    volatile_tokens_touched = [str(token).lower() for token in (volatile_tokens_touched or []) if token]
    participants = []
    for address in [watch_address, from_addr, to_addr, counterparty, *(extra_addresses or [])]:
        address = str(address or "").lower()
        if address and address not in participants:
            participants.append(address)

    participant_meta = {
        address: _snapshot_meta(address)
        for address in participants
    }

    watch_meta = participant_meta.get((watch_address or "").lower(), _snapshot_meta(watch_address))
    counterparty_meta = participant_meta.get((counterparty or "").lower(), _snapshot_meta(counterparty))
    from_meta = participant_meta.get((from_addr or "").lower(), _snapshot_meta(from_addr))
    to_meta = participant_meta.get((to_addr or "").lower(), _snapshot_meta(to_addr))

    is_exchange_related = any(_is_exchange_meta(meta) for meta in participant_meta.values())
    is_router_related = any(_is_router_meta(meta, address) for address, meta in participant_meta.items())
    is_protocol_related = any(_is_protocol_meta(meta) for meta in participant_meta.values())
    is_stablecoin_flow = bool(
        stable_tokens_touched
        or _is_stable_asset(parsed.get("token_contract"), parsed.get("token_symbol"))
        or _is_stable_asset(parsed.get("quote_token_contract"), parsed.get("quote_symbol"))
    )
    stablecoin_dominant = bool(stable_tokens_touched) and len(stable_tokens_touched) >= max(len(volatile_tokens_touched), 1)
    possible_internal_transfer = _is_inventory_style_internal(from_meta, to_meta)
    liquidation_context = scan_liquidation_context(
        addresses=participants,
        participant_meta=participant_meta,
    )

    return {
        "watch_role": watch_meta.get("role", "unknown"),
        "watch_strategy_role": watch_meta.get("strategy_role", "unknown"),
        "watch_semantic_role": watch_meta.get("semantic_role", "unknown"),
        "counterparty_role": counterparty_meta.get("role", "unknown"),
        "counterparty_strategy_role": counterparty_meta.get("strategy_role", "unknown"),
        "counterparty_semantic_role": counterparty_meta.get("semantic_role", "unknown"),
        "is_exchange_related": is_exchange_related,
        "is_router_related": is_router_related,
        "is_protocol_related": is_protocol_related,
        "possible_internal_transfer": possible_internal_transfer,
        "is_stablecoin_flow": is_stablecoin_flow,
        "stablecoin_dominant": stablecoin_dominant,
        "stable_tokens_touched": stable_tokens_touched,
        "volatile_tokens_touched": volatile_tokens_touched,
        "is_liquidation_protocol_related": liquidation_context["is_liquidation_protocol_related"],
        "liquidation_protocols_touched": liquidation_context["protocols"],
        "liquidation_roles_touched": liquidation_context["roles"],
        "liquidation_related_addresses": liquidation_context["related_addresses"],
        "liquidation_address_matches": liquidation_context["matches"],
        "liquidation_relevance_score_max": liquidation_context["max_relevance_score"],
        "possible_keeper_executor": liquidation_context["possible_keeper_executor"],
        "possible_vault_or_auction": liquidation_context["possible_vault_or_auction"],
        "possible_lending_protocol": liquidation_context["possible_lending_protocol"],
        "participants": participant_meta,
    }


def _attach_context_fields(
    parsed: dict,
    watch_address: str = "",
    counterparty: str = "",
    stable_tokens_touched: list[str] | None = None,
    volatile_tokens_touched: list[str] | None = None,
    extra_addresses: list[str] | None = None,
) -> dict:
    from_addr = str(parsed.get("from") or "").lower()
    to_addr = str(parsed.get("to") or "").lower()
    role_hints = _build_context_hints(
        parsed=parsed,
        watch_address=str(watch_address or parsed.get("watch_address") or "").lower(),
        from_addr=from_addr,
        to_addr=to_addr,
        counterparty=str(counterparty or parsed.get("counterparty") or "").lower(),
        stable_tokens_touched=stable_tokens_touched,
        volatile_tokens_touched=volatile_tokens_touched,
        extra_addresses=extra_addresses,
    )

    parsed["role_hints"] = role_hints["participants"]
    parsed["counterparty_role"] = role_hints["counterparty_role"]
    parsed["counterparty_strategy_role"] = role_hints["counterparty_strategy_role"]
    parsed["counterparty_semantic_role"] = role_hints["counterparty_semantic_role"]
    parsed["is_exchange_related"] = role_hints["is_exchange_related"]
    parsed["is_router_related"] = role_hints["is_router_related"]
    parsed["is_protocol_related"] = role_hints["is_protocol_related"]
    parsed["is_stablecoin_flow"] = role_hints["is_stablecoin_flow"]
    parsed["possible_internal_transfer"] = role_hints["possible_internal_transfer"]
    parsed["is_liquidation_protocol_related"] = role_hints["is_liquidation_protocol_related"]
    parsed["liquidation_protocols_touched"] = list(role_hints["liquidation_protocols_touched"])
    parsed["liquidation_roles_touched"] = list(role_hints["liquidation_roles_touched"])
    parsed["liquidation_related_addresses"] = list(role_hints["liquidation_related_addresses"])
    parsed["liquidation_relevance_score_max"] = float(role_hints["liquidation_relevance_score_max"] or 0.0)
    parsed["liquidation_address_matches"] = list(role_hints["liquidation_address_matches"])
    parsed["possible_keeper_executor"] = bool(role_hints["possible_keeper_executor"])
    parsed["possible_vault_or_auction"] = bool(role_hints["possible_vault_or_auction"])
    parsed["possible_lending_protocol"] = bool(role_hints["possible_lending_protocol"])
    parsed["inferred_context"] = role_hints
    return parsed


def _collect_watch_flows(item):
    """提取 watch 地址在各 token 上的 in/out/net 流。"""
    watch_address = (item.get("watch_address") or "").lower()
    if not watch_address:
        return None, [], {}

    transfers = []
    for log in item.get("logs", []):
        transfer = parse_transfer_log(log)
        if transfer:
            transfers.append(transfer)

    if not transfers:
        return watch_address, [], {}

    transfers.sort(key=lambda tx: tx["log_index"])
    flows = {}

    for order, transfer in enumerate(transfers):
        token = transfer["token_contract"]
        flow = flows.setdefault(token, {
            "token_contract": token,
            "token_symbol": transfer["token_symbol"],
            "in": 0.0,
            "out": 0.0,
            "net": 0.0,
            "first_out_order": None,
            "last_in_order": -1,
            "last_out_order": -1,
        })

        if transfer["to"] == watch_address:
            flow["in"] += transfer["value"]
            flow["net"] += transfer["value"]
            flow["last_in_order"] = order

        if transfer["from"] == watch_address:
            flow["out"] += transfer["value"]
            flow["net"] -= transfer["value"]
            if flow["first_out_order"] is None:
                flow["first_out_order"] = order
            flow["last_out_order"] = order

    return watch_address, transfers, flows


def _split_quote_and_target_flows(flows: dict) -> tuple[list[dict], list[dict], list[dict]]:
    stable_flows = []
    eth_flows = []
    other_flows = []
    for flow in flows.values():
        if flow["in"] <= 0 and flow["out"] <= 0:
            continue
        token_contract = flow["token_contract"]
        token_symbol = flow["token_symbol"]
        if _is_stable_asset(token_contract, token_symbol):
            stable_flows.append(flow)
        elif _is_eth_quote_asset(token_contract, token_symbol):
            eth_flows.append(flow)
        else:
            other_flows.append(flow)
    return stable_flows, eth_flows, other_flows


def _family_flow_stats(flows: list[dict]) -> tuple[float, float, float]:
    net = sum(float(flow["net"]) for flow in flows)
    total_in = sum(float(flow["in"]) for flow in flows)
    total_out = sum(float(flow["out"]) for flow in flows)
    return net, total_in, total_out


def _resolve_primary_quote_family(stable_flows: list[dict], eth_flows: list[dict], other_flows: list[dict]):
    if stable_flows:
        target_candidates = other_flows or eth_flows
        if target_candidates:
            return stable_flows, target_candidates, "stable"

    if eth_flows:
        target_candidates = other_flows or stable_flows
        if target_candidates:
            return eth_flows, target_candidates, "eth"

    return [], [], ""


def _pick_primary_quote_token(quote_flows: list[dict], side: str) -> dict:
    if side == "买入":
        return max(quote_flows, key=lambda flow: max(flow["out"] - flow["in"], flow["out"], abs(flow["net"])))
    return max(quote_flows, key=lambda flow: max(flow["in"] - flow["out"], flow["in"], abs(flow["net"])))


def _parse_swap_from_flows(item, watch_address, transfers, flows):
    """
    只要同一 tx 内存在明确报价腿与目标腿的相反净流，就识别为 swap。
    报价腿支持：
    - 稳定币 -> Token
    - ETH/WETH -> Token
    - 稳定币 -> ETH/WETH
    """
    stable_flows, eth_flows, other_flows = _split_quote_and_target_flows(flows)
    quote_flows, target_pool, quote_asset_type = _resolve_primary_quote_family(stable_flows, eth_flows, other_flows)
    if not quote_flows or not target_pool:
        return None

    quote_net, quote_in_total, quote_out_total = _family_flow_stats(quote_flows)
    if quote_net < 0:
        side = "买入"
        quote_amount = -quote_net
    elif quote_net > 0:
        side = "卖出"
        quote_amount = quote_net
    elif quote_out_total > quote_in_total:
        side = "买入"
        quote_amount = quote_out_total - quote_in_total
    elif quote_in_total > quote_out_total:
        side = "卖出"
        quote_amount = quote_in_total - quote_out_total
    else:
        return None

    if quote_amount <= 0:
        return None

    if side == "买入":
        token_candidates = [flow for flow in target_pool if float(flow["net"]) > 0]
    else:
        token_candidates = [flow for flow in target_pool if float(flow["net"]) < 0]
    if not token_candidates:
        return None

    target = max(token_candidates, key=lambda flow: abs(float(flow["net"])))
    token_net = float(target["net"])
    quote_direction_net = quote_net if quote_net != 0 else (-quote_amount if side == "买入" else quote_amount)

    if side == "买入" and not (quote_direction_net < 0 and token_net > 0):
        return None
    if side == "卖出" and not (quote_direction_net > 0 and token_net < 0):
        return None
    if quote_direction_net * token_net > 0:
        return None

    quote_token = _pick_primary_quote_token(quote_flows, side)
    if side == "买入":
        token_amount = float(target["net"])
        token_in_candidates = [
            tx for tx in transfers
            if tx["token_contract"] == target["token_contract"] and tx["to"] == watch_address
        ]
        quote_out_candidates = [
            tx for tx in transfers
            if tx["token_contract"] == quote_token["token_contract"] and tx["from"] == watch_address
        ]

        token_from = max(token_in_candidates, key=lambda x: x["value"])["from"] if token_in_candidates else ""
        quote_to = max(quote_out_candidates, key=lambda x: x["value"])["to"] if quote_out_candidates else ""

        from_addr = token_from
        to_addr = watch_address
        quote_from = watch_address
        counterparty = quote_to or token_from
    else:
        token_amount = abs(float(target["net"]))
        token_out_candidates = [
            tx for tx in transfers
            if tx["token_contract"] == target["token_contract"] and tx["from"] == watch_address
        ]
        quote_in_candidates = [
            tx for tx in transfers
            if tx["token_contract"] == quote_token["token_contract"] and tx["to"] == watch_address
        ]

        token_to = max(token_out_candidates, key=lambda x: x["value"])["to"] if token_out_candidates else ""
        quote_from = max(quote_in_candidates, key=lambda x: x["value"])["from"] if quote_in_candidates else ""

        from_addr = watch_address
        to_addr = token_to
        quote_to = watch_address
        counterparty = quote_from or token_to

    if token_amount <= 0:
        return None

    quote_symbol = quote_token["token_symbol"]
    parsed = {
        "kind": "swap",
        "side": side,
        "watch_address": watch_address,
        "token_contract": target["token_contract"],
        "token_symbol": target["token_symbol"],
        "token_amount": token_amount,
        "quote_symbol": quote_symbol,
        "quote_amount": quote_amount,
        "quote_token_contract": quote_token["token_contract"],
        "quote_asset_type": quote_asset_type,
        "from": from_addr,
        "to": to_addr,
        "counterparty": counterparty,
        "token_from": from_addr,
        "token_to": to_addr,
        "quote_from": quote_from,
        "quote_to": quote_to,
        "tx_hash": to_hex(item.get("tx_hash")),
    }
    return _attach_context_fields(
        parsed,
        watch_address=watch_address,
        counterparty=counterparty,
        stable_tokens_touched=[flow["token_contract"] for flow in stable_flows],
        volatile_tokens_touched=[flow["token_contract"] for flow in other_flows + eth_flows],
        extra_addresses=[quote_from, quote_to, parsed["token_from"], parsed["token_to"]],
    )


def _looks_like_lp_flow(flows):
    stable_flows, eth_flows, other_flows = _split_quote_and_target_flows(flows)
    quote_flows, target_pool, _ = _resolve_primary_quote_family(stable_flows, eth_flows, other_flows)
    if not quote_flows or not target_pool:
        return False

    quote_main = max(quote_flows, key=lambda flow: max(flow["in"], flow["out"]))
    token_main = max(target_pool, key=lambda flow: max(flow["in"], flow["out"]))

    both_out = (
        quote_main["out"] > 0
        and quote_main["in"] == 0
        and token_main["out"] > 0
        and token_main["in"] == 0
    )
    both_in = (
        quote_main["in"] > 0
        and quote_main["out"] == 0
        and token_main["in"] > 0
        and token_main["out"] == 0
    )
    same_direction_net = quote_main["net"] * token_main["net"] > 0
    return both_out or both_in or same_direction_net


def _parse_token_transfer_from_flows(item, watch_address, transfers, flows):
    """非 swap 的 token 资金流，降级为 token_transfer 事件。"""
    watch_related = [flow for flow in flows.values() if (flow["in"] > 0 or flow["out"] > 0)]
    if not watch_related:
        return None

    target = max(watch_related, key=lambda flow: abs(flow["net"]))
    net = float(target["net"])
    if net == 0:
        return None

    direction = "流入" if net > 0 else "流出"
    amount = abs(net)

    counterpart = ""
    if direction == "流入":
        candidates = [
            tx for tx in transfers
            if tx["token_contract"] == target["token_contract"] and tx["to"] == watch_address
        ]
        if candidates:
            counterpart = max(candidates, key=lambda x: x["value"])["from"]
    else:
        candidates = [
            tx for tx in transfers
            if tx["token_contract"] == target["token_contract"] and tx["from"] == watch_address
        ]
        if candidates:
            counterpart = max(candidates, key=lambda x: x["value"])["to"]

    stable_tokens_touched = [
        token for token, flow in flows.items()
        if (flow["in"] > 0 or flow["out"] > 0) and _is_stable_asset(token, flow["token_symbol"])
    ]
    volatile_tokens_touched = [
        token for token, flow in flows.items()
        if (flow["in"] > 0 or flow["out"] > 0) and not _is_stable_asset(token, flow["token_symbol"])
    ]
    parsed = {
        "kind": "token_transfer",
        "watch_address": watch_address,
        "direction": direction,
        "from": counterpart if direction == "流入" else watch_address,
        "to": watch_address if direction == "流入" else counterpart,
        "value": amount,
        "token_contract": target["token_contract"],
        "token_symbol": target["token_symbol"],
        "tx_hash": to_hex(item.get("tx_hash")),
    }
    return _attach_context_fields(
        parsed,
        watch_address=watch_address,
        counterparty=counterpart,
        stable_tokens_touched=stable_tokens_touched,
        volatile_tokens_touched=volatile_tokens_touched,
    )


def _parse_lp_pool_candidate(item, pool_meta: dict, watch_address: str, transfers: list[dict], flows: dict):
    parsed = LP_EVENT_ANALYZER.parse_pool_candidate(
        item=item,
        pool_meta=pool_meta,
        transfers=transfers,
        flows=flows,
        tx_hash=to_hex(item.get("tx_hash")),
    )
    if not parsed:
        return None

    lp_context = parsed.get("lp_context") or {}
    base_token = str(lp_context.get("base_token_contract") or parsed.get("token_contract") or "").lower()
    quote_token = str(lp_context.get("quote_token_contract") or parsed.get("quote_token_contract") or "").lower()
    stable_tokens_touched = [
        token for token in [base_token, quote_token]
        if token and _is_stable_asset(token, TOKEN_SYMBOLS.get(token))
    ]
    volatile_tokens_touched = [
        token for token in [base_token, quote_token]
        if token and not _is_stable_asset(token, TOKEN_SYMBOLS.get(token))
    ]
    extra_addresses = [
        parsed.get("quote_from"),
        parsed.get("quote_to"),
        parsed.get("token_from"),
        parsed.get("token_to"),
    ]
    return _attach_context_fields(
        parsed,
        watch_address=watch_address,
        counterparty=str(parsed.get("counterparty") or "").lower(),
        stable_tokens_touched=stable_tokens_touched,
        volatile_tokens_touched=volatile_tokens_touched,
        extra_addresses=extra_addresses,
    )


def parse_swap_candidate(item):
    """
    解析资金流候选交易：
    - 优先识别为 swap
    - 否则降级为 token_transfer
    """
    watch_address, transfers, flows = _collect_watch_flows(item)
    if not watch_address or not transfers:
        return None

    pool_meta = get_lp_pool(watch_address, active_only=True)
    lp_like_flow = _looks_like_lp_flow(flows)
    if pool_meta is not None:
        parsed = _parse_lp_pool_candidate(item, pool_meta, watch_address, transfers, flows)
        if parsed:
            return parsed
        # LP pool parsing can miss partial-leg samples; fall back instead of dropping the tx.

    if not lp_like_flow:
        swap = _parse_swap_from_flows(item, watch_address, transfers, flows)
        if swap:
            return swap

    return _parse_token_transfer_from_flows(item, watch_address, transfers, flows)


def parse_token_flow_candidate(item):
    """token_flow_candidate 与 swap_candidate 复用同一解析逻辑。"""
    return parse_swap_candidate(item)


def parse_erc20_transfer(tx):
    """解析 ERC20 transfer/transferFrom 交易输入。"""
    input_data = to_hex(tx.get("input"))
    if len(input_data) < 10:
        return None

    sig = input_data[:10]
    data = input_data[10:]
    token = tx.get("to")
    if not token:
        return None

    token = token.lower()
    token_meta = get_token_metadata(token)

    try:
        if sig == ERC20_TRANSFER_SIG:
            if len(data) < 128:
                return None
            to = "0x" + data[24:64]
            raw_value = int(data[64:128], 16)
            from_addr = tx["from"]

        elif sig == ERC20_TRANSFER_FROM_SIG:
            if len(data) < 192:
                return None
            from_addr = "0x" + data[24:64]
            to = "0x" + data[88:128]
            raw_value = int(data[128:192], 16)

        else:
            return None

        parsed = {
            "kind": "token_transfer",
            "from": from_addr.lower(),
            "to": to.lower(),
            "value": raw_value / (10 ** token_meta["decimals"]),
            "token_contract": token,
            "token_symbol": token_meta["symbol"],
            "tx_hash": to_hex(tx.get("hash")),
        }
        return _attach_context_fields(
            parsed,
            stable_tokens_touched=[token] if _is_stable_asset(token, token_meta["symbol"]) else [],
            volatile_tokens_touched=[] if _is_stable_asset(token, token_meta["symbol"]) else [token],
        )

    except Exception as e:
        print("ERC20解析失败:", e)
        return None


def parse_tx(tx):
    """
    统一解析入口：
    - swap_candidate -> swap 结构
    - erc20 输入 -> token 转账结构
    - 其余 -> ETH 转账结构
    """
    if is_swap_candidate(tx):
        return parse_swap_candidate(tx)

    if is_token_flow_candidate(tx):
        return parse_token_flow_candidate(tx)

    if is_erc20_transfer_log(tx):
        return parse_erc20_transfer_log(tx)

    if is_erc20_transfer(tx):
        data = parse_erc20_transfer(tx)
        if data:
            return data

    parsed = {
        "kind": "eth_transfer",
        "from": tx["from"].lower(),
        "to": tx["to"].lower() if tx["to"] else None,
        "value": tx["value"] / 1e18,
        "token_contract": None,
        "token_symbol": "ETH",
        "tx_hash": to_hex(tx.get("hash")),
    }
    return _attach_context_fields(parsed)
