import time

from clmm_position_state import (
    build_position_key,
    get_position_state,
    recent_position_actions,
    record_position_action,
    upsert_position_state,
)
from clmm_registry import detect_clmm_manager_meta


SUPPORTED_CLMM_INTENTS = {
    "clmm_position_open",
    "clmm_position_add_range_liquidity",
    "clmm_position_remove_range_liquidity",
    "clmm_position_collect_fees",
    "clmm_position_close",
    "clmm_range_shift",
    "clmm_jit_liquidity_likely",
    "clmm_inventory_recenter",
    "clmm_passive_fee_harvest",
}


def _normalize_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _normalize_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _item_ts(item: dict) -> int:
    return _normalize_int(item.get("ingest_ts") or item.get("timestamp") or item.get("ts") or int(time.time()))


def _normalized_decoded_events(item: dict) -> list[dict]:
    decoded = item.get("decoded_logs") or item.get("decoded_events") or item.get("logs_decoded") or []
    events = []
    for raw in decoded:
        if not isinstance(raw, dict):
            continue
        args = raw.get("args") or raw.get("decoded_args") or raw.get("data") or {}
        if not isinstance(args, dict):
            args = {}
        events.append({
            "address": str(raw.get("address") or "").strip().lower(),
            "event_name": str(raw.get("event_name") or raw.get("name") or raw.get("event") or "").strip(),
            "args": dict(args),
            "log_index": _normalize_int(raw.get("log_index") or raw.get("logIndex")),
        })
    events.sort(key=lambda item: item["log_index"])
    return events


def _arg(event: dict, *names, default=None):
    args = event.get("args") or {}
    for name in names:
        if name in args and args.get(name) is not None:
            return args.get(name)
    return default


def _zero_address(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"", "0x0000000000000000000000000000000000000000"}


def _event_token_id(event: dict) -> str:
    value = _arg(event, "tokenId", "token_id", default="")
    return str(value or "").strip()


def _is_nft_transfer(event: dict, manager_address: str) -> bool:
    if str(event.get("event_name") or "") != "Transfer":
        return False
    if str(event.get("address") or "").strip().lower() != str(manager_address or "").strip().lower():
        return False
    return bool(_event_token_id(event))


def _first_event(events: list[dict], *event_names: str, address: str | None = None) -> dict | None:
    address_normalized = str(address or "").strip().lower()
    event_name_set = {str(name or "") for name in event_names}
    for event in events:
        if event_name_set and str(event.get("event_name") or "") not in event_name_set:
            continue
        if address_normalized and str(event.get("address") or "").strip().lower() != address_normalized:
            continue
        return event
    return None


def _pool_side_event(events: list[dict], event_name: str, manager_address: str) -> dict | None:
    for event in events:
        if str(event.get("event_name") or "") != event_name:
            continue
        if str(event.get("address") or "").strip().lower() == str(manager_address or "").strip().lower():
            continue
        return event
    return None


def _extract_pool_assets(item: dict, transfers: list[dict] | None, flows: dict | None, prior_state: dict) -> dict:
    item = item or {}
    clmm_context = item.get("clmm_context") or {}
    token0 = str(
        clmm_context.get("token0")
        or item.get("token0_contract")
        or prior_state.get("token0")
        or ""
    ).strip().lower()
    token1 = str(
        clmm_context.get("token1")
        or item.get("token1_contract")
        or prior_state.get("token1")
        or ""
    ).strip().lower()
    token0_symbol = str(
        clmm_context.get("token0_symbol")
        or item.get("token0_symbol")
        or prior_state.get("token0_symbol")
        or ""
    ).strip().upper()
    token1_symbol = str(
        clmm_context.get("token1_symbol")
        or item.get("token1_symbol")
        or prior_state.get("token1_symbol")
        or ""
    ).strip().upper()

    if (not token0 or not token1) and transfers:
        seen = []
        for transfer in transfers:
            token_contract = str(transfer.get("token_contract") or "").strip().lower()
            token_symbol = str(transfer.get("token_symbol") or "").strip().upper()
            if not token_contract or token_contract in seen:
                continue
            seen.append(token_contract)
            if not token0:
                token0 = token_contract
                if not token0_symbol:
                    token0_symbol = token_symbol
            elif not token1:
                token1 = token_contract
                if not token1_symbol:
                    token1_symbol = token_symbol
                break

    if flows:
        for token_contract, flow in flows.items():
            if not token0:
                token0 = str(token_contract or "").strip().lower()
                token0_symbol = token0_symbol or str(flow.get("token_symbol") or "").strip().upper()
            elif not token1 and str(token_contract or "").strip().lower() != token0:
                token1 = str(token_contract or "").strip().lower()
                token1_symbol = token1_symbol or str(flow.get("token_symbol") or "").strip().upper()
                break

    pair_label = str(
        clmm_context.get("pair_label")
        or item.get("pair_label")
        or "/".join([symbol for symbol in [token0_symbol, token1_symbol] if symbol])
    ).strip()

    return {
        "token0": token0,
        "token1": token1,
        "token0_symbol": token0_symbol,
        "token1_symbol": token1_symbol,
        "pair_label": pair_label or "CLMM Position",
    }


def _amount_from_event(event: dict | None, *keys: str) -> float:
    if event is None:
        return 0.0
    return _normalize_float(_arg(event, *keys, default=0.0))


def _range_tuple(tick_lower, tick_upper) -> tuple[int | None, int | None]:
    lower = _arg({"args": {"value": tick_lower}}, "value", default=None)
    upper = _arg({"args": {"value": tick_upper}}, "value", default=None)
    return (
        _normalize_int(lower) if lower is not None else None,
        _normalize_int(upper) if upper is not None else None,
    )


def _position_actor_label(protocol: str, token_id: str, pair_label: str) -> str:
    protocol_label = "Uniswap v3" if protocol == "uniswap_v3" else "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
    if token_id:
        return f"{protocol_label} Position #{token_id}"
    if pair_label:
        return f"{pair_label} CLMM Position"
    return "某 LP 头寸"


def _base_kind(action: str) -> str:
    if action in {"clmm_position_open", "clmm_position_add_range_liquidity"}:
        return "lp_add_liquidity"
    if action in {"clmm_position_remove_range_liquidity", "clmm_position_close"}:
        return "lp_remove_liquidity"
    if action == "clmm_position_collect_fees":
        return "clmm_collect_fees"
    return "lp_rebalance"


def _partial_candidate(
    *,
    manager_meta: dict,
    item: dict,
    reason: str,
    token_id: str = "",
) -> dict:
    protocol = str(manager_meta.get("protocol") or "")
    chain = str(manager_meta.get("chain") or item.get("chain") or "ethereum").strip().lower() or "ethereum"
    position_manager = str(manager_meta.get("manager_address") or "").strip().lower()
    return {
        "parse_status": "candidate_only",
        "partial_support": True,
        "reason": reason,
        "protocol": protocol,
        "chain": chain,
        "position_manager": position_manager,
        "token_id": str(token_id or "").strip(),
        "position_key": build_position_key(protocol, chain, position_manager, token_id),
    }


def _parse_uniswap_v3_candidate(
    item: dict,
    *,
    manager_meta: dict,
    watch_address: str,
    transfers: list[dict] | None,
    flows: dict | None,
) -> dict | None:
    events = _normalized_decoded_events(item)
    if not events:
        return _partial_candidate(
            manager_meta=manager_meta,
            item=item,
            reason="manager_path_without_decoded_events",
        )

    manager_address = str(manager_meta.get("manager_address") or "").strip().lower()
    increase_event = _first_event(events, "IncreaseLiquidity", address=manager_address)
    decrease_event = _first_event(events, "DecreaseLiquidity", address=manager_address)
    collect_event = _first_event(events, "Collect", address=manager_address)
    nft_transfer_event = None
    for event in events:
        if _is_nft_transfer(event, manager_address):
            nft_transfer_event = event
            break
    pool_mint_event = _pool_side_event(events, "Mint", manager_address)
    pool_burn_event = _pool_side_event(events, "Burn", manager_address)
    pool_collect_event = _pool_side_event(events, "Collect", manager_address)

    token_id = ""
    for candidate in [increase_event, decrease_event, collect_event, nft_transfer_event]:
        token_id = _event_token_id(candidate or {})
        if token_id:
            break
    if not token_id:
        token_id = str(item.get("token_id") or item.get("position_token_id") or "").strip()
    if not token_id:
        return _partial_candidate(
            manager_meta=manager_meta,
            item=item,
            reason="decoded_events_missing_token_id",
        )

    prior_state = get_position_state(
        build_position_key(
            manager_meta.get("protocol"),
            manager_meta.get("chain") or item.get("chain"),
            manager_meta.get("manager_address"),
            token_id,
        )
    )

    method_name = str(
        item.get("method_name")
        or item.get("decoded_call", {}).get("function_name")
        or item.get("decoded_call", {}).get("method")
        or item.get("method")
        or ""
    ).strip().lower()
    nft_from = str(_arg(nft_transfer_event or {}, "from", default="") or "").strip().lower()
    nft_to = str(_arg(nft_transfer_event or {}, "to", default="") or "").strip().lower()
    is_nft_mint = bool(nft_transfer_event) and _zero_address(nft_from) and not _zero_address(nft_to)
    is_nft_burn = bool(nft_transfer_event) and not _zero_address(nft_from) and _zero_address(nft_to)

    if decrease_event or pool_burn_event:
        base_action = "clmm_position_close" if is_nft_burn or method_name in {"burn", "close"} else "clmm_position_remove_range_liquidity"
    elif increase_event or pool_mint_event:
        base_action = "clmm_position_open" if is_nft_mint or method_name == "mint" or not prior_state.get("position_key") else "clmm_position_add_range_liquidity"
    elif collect_event or pool_collect_event:
        base_action = "clmm_position_collect_fees"
    else:
        return _partial_candidate(
            manager_meta=manager_meta,
            item=item,
            reason="decoded_events_missing_position_action",
            token_id=token_id,
        )

    owner = str(
        (nft_to if is_nft_mint else nft_from if is_nft_burn else "")
        or _arg(pool_mint_event or pool_burn_event or {}, "owner", default="")
        or item.get("owner")
        or item.get("watch_address")
        or prior_state.get("owner")
        or watch_address
    ).strip().lower()
    pool = str(
        item.get("pool")
        or item.get("clmm_context", {}).get("pool")
        or (pool_mint_event or pool_burn_event or pool_collect_event or {}).get("address")
        or prior_state.get("pool")
        or ""
    ).strip().lower()

    tick_lower = _arg(pool_mint_event or pool_burn_event or pool_collect_event or {}, "tickLower", "tick_lower", default=prior_state.get("tick_lower"))
    tick_upper = _arg(pool_mint_event or pool_burn_event or pool_collect_event or {}, "tickUpper", "tick_upper", default=prior_state.get("tick_upper"))
    liquidity_value = _amount_from_event(increase_event or decrease_event or pool_mint_event or pool_burn_event, "liquidity", "amount")
    amount0 = _amount_from_event(increase_event or decrease_event or pool_mint_event or pool_burn_event, "amount0")
    amount1 = _amount_from_event(increase_event or decrease_event or pool_mint_event or pool_burn_event, "amount1")
    fee_amount0 = _amount_from_event(collect_event or pool_collect_event, "amount0")
    fee_amount1 = _amount_from_event(collect_event or pool_collect_event, "amount1")

    assets = _extract_pool_assets(item, transfers, flows, prior_state)
    pair_label = assets["pair_label"]
    action_ts = _item_ts(item)
    position_key = build_position_key(
        manager_meta.get("protocol"),
        manager_meta.get("chain") or item.get("chain"),
        manager_meta.get("manager_address"),
        token_id,
    )

    if base_action in {"clmm_position_remove_range_liquidity", "clmm_position_close"}:
        from_addr = pool or manager_address
        to_addr = owner
        side = "减少流动性" if base_action == "clmm_position_remove_range_liquidity" else "关闭头寸"
        liquidity_delta = -abs(liquidity_value)
    elif base_action == "clmm_position_collect_fees":
        from_addr = pool or manager_address
        to_addr = owner
        side = "收手续费"
        liquidity_delta = 0.0
    else:
        from_addr = owner
        to_addr = pool or manager_address
        side = "增加流动性" if base_action == "clmm_position_add_range_liquidity" else "开新头寸"
        liquidity_delta = abs(liquidity_value)

    evidence = []
    if increase_event:
        evidence.append("manager:IncreaseLiquidity")
    if decrease_event:
        evidence.append("manager:DecreaseLiquidity")
    if collect_event:
        evidence.append("manager:Collect")
    if pool_mint_event:
        evidence.append("pool:Mint")
    if pool_burn_event:
        evidence.append("pool:Burn")
    if pool_collect_event:
        evidence.append("pool:Collect")
    if is_nft_mint:
        evidence.append("nft:mint")
    if is_nft_burn:
        evidence.append("nft:burn")

    return {
        "parse_status": "parsed",
        "partial_support": False,
        "protocol": str(manager_meta.get("protocol") or "uniswap_v3"),
        "chain": str(manager_meta.get("chain") or item.get("chain") or "ethereum").strip().lower() or "ethereum",
        "position_manager": manager_address,
        "token_id": token_id,
        "position_key": position_key,
        "base_action": base_action,
        "kind": _base_kind(base_action),
        "watch_address": watch_address,
        "from": from_addr,
        "to": to_addr,
        "counterparty": pool or manager_address,
        "owner": owner,
        "pool": pool,
        "token0": assets["token0"],
        "token1": assets["token1"],
        "token0_symbol": assets["token0_symbol"],
        "token1_symbol": assets["token1_symbol"],
        "token_contract": assets["token0"],
        "token_symbol": assets["token0_symbol"],
        "quote_token_contract": assets["token1"],
        "quote_symbol": assets["token1_symbol"],
        "tick_lower": _normalize_int(tick_lower) if tick_lower is not None else None,
        "tick_upper": _normalize_int(tick_upper) if tick_upper is not None else None,
        "liquidity_delta": liquidity_delta,
        "amount0": amount0,
        "amount1": amount1,
        "fee_amount0": fee_amount0,
        "fee_amount1": fee_amount1,
        "value": max(abs(amount0), abs(amount1), abs(fee_amount0), abs(fee_amount1), 0.0),
        "pair_label": pair_label,
        "lp_legs": [
            {
                "token_contract": assets["token0"],
                "token_symbol": assets["token0_symbol"],
                "amount": abs(fee_amount0) if base_action == "clmm_position_collect_fees" else abs(amount0),
                "is_base": True,
                "is_quote": False,
            },
            {
                "token_contract": assets["token1"],
                "token_symbol": assets["token1_symbol"],
                "amount": abs(fee_amount1) if base_action == "clmm_position_collect_fees" else abs(amount1),
                "is_base": False,
                "is_quote": True,
            },
        ],
        "clmm_context": {
            "protocol": str(manager_meta.get("protocol") or "uniswap_v3"),
            "chain": str(manager_meta.get("chain") or item.get("chain") or "ethereum").strip().lower() or "ethereum",
            "position_manager": manager_address,
            "manager_note": str(manager_meta.get("note") or ""),
            "abi_profile": str(manager_meta.get("abi_profile") or ""),
            "token_id": token_id,
            "position_key": position_key,
            "owner": owner,
            "pool": pool,
            "pair_label": pair_label,
            "token0": assets["token0"],
            "token1": assets["token1"],
            "token0_symbol": assets["token0_symbol"],
            "token1_symbol": assets["token1_symbol"],
            "tick_lower": _normalize_int(tick_lower) if tick_lower is not None else None,
            "tick_upper": _normalize_int(tick_upper) if tick_upper is not None else None,
            "base_action": base_action,
            "parse_status": "parsed",
            "parse_evidence": evidence,
            "burst_meta": dict((item.get("clmm_context") or {}).get("burst_meta") or item.get("burst_meta") or {}),
            "position_actor_label": _position_actor_label(
                str(manager_meta.get("protocol") or "uniswap_v3"),
                token_id,
                pair_label,
            ),
        },
        "prior_state": prior_state,
        "timestamp": action_ts,
        "tx_hash": str(item.get("tx_hash") or ""),
    }


def parse_clmm_candidate(
    item: dict,
    *,
    chain: str = "ethereum",
    watch_address: str = "",
    watch_meta: dict | None = None,
    transfers: list[dict] | None = None,
    flows: dict | None = None,
) -> dict | None:
    del watch_meta
    item = dict(item or {})
    manager_meta = detect_clmm_manager_meta(item, chain=chain)
    if manager_meta is None:
        return None

    protocol = str(manager_meta.get("protocol") or "").strip().lower()
    normalized_watch = str(watch_address or item.get("watch_address") or "").strip().lower()
    if protocol == "uniswap_v3":
        return _parse_uniswap_v3_candidate(
            item,
            manager_meta=manager_meta,
            watch_address=normalized_watch,
            transfers=transfers,
            flows=flows,
        )

    token_id = str(item.get("token_id") or item.get("position_token_id") or "").strip()
    return _partial_candidate(
        manager_meta=manager_meta,
        item=item,
        reason="uniswap_v4_partial_support_missing_decode_primitives",
        token_id=token_id,
    )


def update_position_state(candidate: dict) -> dict:
    if candidate is None:
        candidate = {}
    position_key = str(candidate.get("position_key") or "").strip()
    prior_state = get_position_state(position_key)
    action = str(candidate.get("base_action") or "").strip()
    amount0 = abs(_normalize_float(candidate.get("amount0")))
    amount1 = abs(_normalize_float(candidate.get("amount1")))
    liquidity_delta = _normalize_float(candidate.get("liquidity_delta"))
    action_ts = _normalize_int(candidate.get("timestamp"))

    state = dict(prior_state)
    for key in [
        "protocol",
        "chain",
        "position_manager",
        "token_id",
        "owner",
        "pool",
        "token0",
        "token1",
        "token0_symbol",
        "token1_symbol",
        "tick_lower",
        "tick_upper",
    ]:
        if candidate.get(key) not in {None, ""}:
            state[key] = candidate.get(key)

    if not state.get("opened_at") and action in {"clmm_position_open", "clmm_position_add_range_liquidity"}:
        state["opened_at"] = action_ts
    state["last_action_at"] = action_ts

    current_liquidity = _normalize_float(prior_state.get("current_liquidity"))
    if action in {"clmm_position_open", "clmm_position_add_range_liquidity"}:
        current_liquidity += abs(liquidity_delta)
        state["cumulative_added_amount0"] = _normalize_float(prior_state.get("cumulative_added_amount0")) + amount0
        state["cumulative_added_amount1"] = _normalize_float(prior_state.get("cumulative_added_amount1")) + amount1
    elif action in {"clmm_position_remove_range_liquidity", "clmm_position_close"}:
        current_liquidity = max(0.0, current_liquidity - abs(liquidity_delta))
        state["cumulative_removed_amount0"] = _normalize_float(prior_state.get("cumulative_removed_amount0")) + amount0
        state["cumulative_removed_amount1"] = _normalize_float(prior_state.get("cumulative_removed_amount1")) + amount1
        if action == "clmm_position_close":
            current_liquidity = 0.0
    elif action == "clmm_position_collect_fees":
        state["collected_fees_count"] = _normalize_int(prior_state.get("collected_fees_count")) + 1

    state["current_liquidity"] = round(current_liquidity, 8)
    stored_state = upsert_position_state(state)
    action_record = record_position_action({
        "position_key": position_key,
        "protocol": candidate.get("protocol"),
        "chain": candidate.get("chain"),
        "position_manager": candidate.get("position_manager"),
        "token_id": candidate.get("token_id"),
        "owner": stored_state.get("owner"),
        "pool": stored_state.get("pool"),
        "tick_lower": stored_state.get("tick_lower"),
        "tick_upper": stored_state.get("tick_upper"),
        "action": action,
        "liquidity_delta": liquidity_delta,
        "amount0": amount0,
        "amount1": amount1,
        "ts": action_ts,
        "tx_hash": candidate.get("tx_hash"),
        "pair_label": candidate.get("pair_label"),
    })
    candidate["prior_state"] = prior_state
    candidate["position_state"] = stored_state
    candidate["action_record"] = action_record
    return stored_state


def infer_position_intent(candidate: dict) -> dict:
    candidate = dict(candidate or {})
    base_action = str(candidate.get("base_action") or "").strip()
    prior_state = dict(candidate.get("prior_state") or {})
    state = dict(candidate.get("position_state") or {})
    owner = str(state.get("owner") or candidate.get("owner") or "").strip().lower()
    pool = str(state.get("pool") or candidate.get("pool") or "").strip().lower()
    now_ts = _normalize_int(candidate.get("timestamp"))
    position_key = str(candidate.get("position_key") or "").strip()
    recent_actions = recent_position_actions(
        owner=owner,
        pool=pool,
        window_sec=900,
        now_ts=now_ts,
    )
    recent_actions = [
        item
        for item in recent_actions
        if not (
            str(item.get("position_key") or "") == position_key
            and _normalize_int(item.get("ts")) == now_ts
            and str(item.get("action") or "") == base_action
        )
    ]
    prior_range = (prior_state.get("tick_lower"), prior_state.get("tick_upper"))
    current_range = (state.get("tick_lower"), state.get("tick_upper"))
    evidence = list(candidate.get("clmm_context", {}).get("parse_evidence") or [])
    pair_label = str(candidate.get("pair_label") or candidate.get("clmm_context", {}).get("pair_label") or "CLMM Position")

    key = base_action
    confidence = 0.72
    confirmation_score = 0.42
    information_level = "medium"

    if base_action == "clmm_position_collect_fees":
        key = "clmm_passive_fee_harvest"
        confidence = 0.74
        confirmation_score = 0.44
        information_level = "medium"
        evidence.append("collect_only_without_range_change")
    elif base_action == "clmm_position_close":
        key = "clmm_position_close"
        confidence = 0.86
        confirmation_score = 0.56
        information_level = "high"
        evidence.append("burn_or_liquidity_cleared")
    elif base_action == "clmm_position_open":
        key = "clmm_position_open"
        confidence = 0.84
        confirmation_score = 0.50
        information_level = "high"
    elif base_action == "clmm_position_add_range_liquidity":
        key = "clmm_position_add_range_liquidity"
        confidence = 0.80
        confirmation_score = 0.46
    elif base_action == "clmm_position_remove_range_liquidity":
        key = "clmm_position_remove_range_liquidity"
        confidence = 0.82
        confirmation_score = 0.48

    if base_action in {"clmm_position_open", "clmm_position_add_range_liquidity"}:
        recent_reduce = [
            item
            for item in recent_actions
            if str(item.get("action") or "") in {"clmm_position_remove_range_liquidity", "clmm_position_close"}
        ]
        if recent_reduce:
            latest_reduce = recent_reduce[-1]
            old_range = (latest_reduce.get("tick_lower"), latest_reduce.get("tick_upper"))
            range_changed = old_range != current_range and all(value is not None for value in [*old_range, *current_range])
            old_exposure = abs(_normalize_float(latest_reduce.get("amount0"))) + abs(_normalize_float(latest_reduce.get("amount1")))
            new_exposure = abs(_normalize_float(candidate.get("amount0"))) + abs(_normalize_float(candidate.get("amount1")))
            exposure_gap_ratio = abs(new_exposure - old_exposure) / max(old_exposure, 1.0)
            if range_changed:
                if exposure_gap_ratio <= 0.25:
                    key = "clmm_inventory_recenter"
                    confidence = 0.84
                    confirmation_score = 0.52
                    information_level = "high"
                    evidence.append("short_window_remove_then_new_range_with_similar_exposure")
                else:
                    key = "clmm_range_shift"
                    confidence = 0.86
                    confirmation_score = 0.54
                    information_level = "high"
                    evidence.append("short_window_remove_then_new_range_with_shifted_ticks")

    if base_action in {"clmm_position_remove_range_liquidity", "clmm_position_close"}:
        burst_meta = item_burst = candidate.get("burst_meta") or candidate.get("clmm_context", {}).get("burst_meta") or {}
        burst_hits = int(
            burst_meta.get("swap_burst_count")
            or burst_meta.get("burst_swap_count")
            or burst_meta.get("swap_burst_event_count")
            or 0
        )
        impact_window_hit = bool(
            burst_meta.get("impact_window_hit")
            or burst_meta.get("large_swap_window_hit")
            or burst_meta.get("near_swap_burst")
        )
        recent_add = [
            item
            for item in recent_actions
            if str(item.get("action") or "") in {"clmm_position_open", "clmm_position_add_range_liquidity"}
            and _normalize_int(item.get("ts")) >= max(now_ts - 300, 0)
        ]
        if recent_add and (impact_window_hit or burst_hits >= 3):
            key = "clmm_jit_liquidity_likely"
            confidence = 0.62
            confirmation_score = 0.34
            information_level = "medium"
            evidence.append("short_lived_position_around_swap_burst")

    if prior_range != current_range and key == base_action and base_action in {"clmm_position_add_range_liquidity", "clmm_position_remove_range_liquidity"}:
        evidence.append("range_changed")
    if pair_label:
        evidence.append(f"pair={pair_label}")

    return {
        "intent_type": key,
        "intent_confidence": round(confidence, 3),
        "information_level": information_level,
        "confirmation_score": round(confirmation_score, 3),
        "intent_evidence": evidence[:5],
    }
