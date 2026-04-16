def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def normalize_decoder_logs(upstream_logs: list[dict] | None) -> list[dict]:
    normalized = []
    for raw in upstream_logs or []:
        if not isinstance(raw, dict):
            continue
        normalized.append({
            "address": str(raw.get("address") or "").strip().lower(),
            "event_name": str(raw.get("event_name") or raw.get("name") or raw.get("event") or "").strip(),
            "args": dict(raw.get("decoded_args") or raw.get("event_args") or raw.get("args") or {}),
            "log_index": _to_int(raw.get("log_index") or raw.get("logIndex") or raw.get("index")),
        })
    normalized.sort(key=lambda item: item["log_index"])
    return normalized


def normalize_decoder_fixture(fixture: dict, step: dict) -> dict:
    fixture = dict(fixture or {})
    step = dict(step or {})
    payload = dict(step.get("input") or {})
    upstream_context = dict(payload.get("upstream_context") or {})
    token0_meta = dict(payload.get("token0") or {})
    token1_meta = dict(payload.get("token1") or {})
    protocol = str(fixture.get("protocol") or payload.get("protocol") or "").strip()
    chain = str(fixture.get("chain") or payload.get("chain") or "ethereum").strip().lower() or "ethereum"
    manager_address = str(fixture.get("manager_address") or payload.get("manager_address") or "").strip().lower()

    method = payload.get("method") or {}
    method_name = str(
        payload.get("method_name")
        or method.get("function_name")
        or method.get("method")
        or ""
    ).strip()
    tx_hash = str(payload.get("tx_hash") or fixture.get("source_tx_hash") or "").strip().lower()
    watch_address = str(payload.get("watch_address") or fixture.get("watch_address") or "").strip().lower()
    token_id = str(payload.get("position_token_id") or payload.get("token_id") or "").strip()
    amount0 = _to_float(
        payload.get("amount0"),
        _to_float((payload.get("liquidity_summary") or {}).get("amount0")),
    )
    amount1 = _to_float(
        payload.get("amount1"),
        _to_float((payload.get("liquidity_summary") or {}).get("amount1")),
    )

    clmm_context = {
        "pool": str(
            upstream_context.get("pool")
            or upstream_context.get("pool_address")
            or fixture.get("pool")
            or ""
        ).strip().lower(),
        "pair_label": str(upstream_context.get("pair_label") or fixture.get("pair_label") or "").strip(),
        "token0": str(token0_meta.get("contract") or upstream_context.get("token0") or "").strip().lower(),
        "token1": str(token1_meta.get("contract") or upstream_context.get("token1") or "").strip().lower(),
        "token0_symbol": str(token0_meta.get("symbol") or upstream_context.get("token0_symbol") or "").strip().upper(),
        "token1_symbol": str(token1_meta.get("symbol") or upstream_context.get("token1_symbol") or "").strip().upper(),
        "tick_lower": upstream_context.get("tickLower", upstream_context.get("tick_lower")),
        "tick_upper": upstream_context.get("tickUpper", upstream_context.get("tick_upper")),
    }
    if upstream_context.get("burst_meta"):
        clmm_context["burst_meta"] = dict(upstream_context.get("burst_meta") or {})

    return {
        "tx_hash": tx_hash,
        "watch_address": watch_address,
        "chain": chain,
        "ingest_ts": _to_int(payload.get("ingest_ts") or payload.get("timestamp")),
        "method_name": method_name,
        "pair_label": str(clmm_context.get("pair_label") or ""),
        "position_manager_meta": {
            "protocol": protocol,
            "chain": chain,
            "manager_address": manager_address,
            "abi_profile": str(fixture.get("manager_abi_profile") or f"{protocol}_position_manager"),
            "enabled": True,
            "note": f"decoder-fixture:{fixture.get('fixture_name')}",
        },
        "decoded_logs": normalize_decoder_logs(payload.get("upstream_decoded_logs") or payload.get("decoded_logs_snapshot")),
        "clmm_context": clmm_context,
        "token_id": token_id,
        "token0_contract": str(token0_meta.get("contract") or clmm_context.get("token0") or "").strip().lower(),
        "token1_contract": str(token1_meta.get("contract") or clmm_context.get("token1") or "").strip().lower(),
        "token0_symbol": str(token0_meta.get("symbol") or clmm_context.get("token0_symbol") or "").strip().upper(),
        "token1_symbol": str(token1_meta.get("symbol") or clmm_context.get("token1_symbol") or "").strip().upper(),
        "amount0": amount0,
        "amount1": amount1,
        "source_kind": str(fixture.get("source_kind") or payload.get("source_kind") or "").strip(),
        "replay_source": "decoder_aligned_fixture",
    }


def build_parser_input_from_decoder_snapshot(fixture: dict, step: dict) -> dict:
    return normalize_decoder_fixture(fixture, step)
