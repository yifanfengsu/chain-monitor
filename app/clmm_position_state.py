import json
import threading
from pathlib import Path


CLMM_POSITION_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "clmm_positions.cache.json"
_CACHE_LOCK = threading.Lock()


def build_position_key(
    protocol: str | None,
    chain: str | None,
    position_manager: str | None,
    token_id,
) -> str:
    protocol_part = str(protocol or "").strip().lower()
    chain_part = str(chain or "ethereum").strip().lower() or "ethereum"
    manager_part = str(position_manager or "").strip().lower()
    token_part = str(token_id or "").strip()
    if not (protocol_part and chain_part and manager_part and token_part):
        return ""
    return f"{protocol_part}:{chain_part}:{manager_part}:{token_part}"


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


def normalize_position_state(payload: dict | None) -> dict:
    payload = payload or {}
    protocol = str(payload.get("protocol") or "").strip().lower()
    chain = str(payload.get("chain") or "ethereum").strip().lower() or "ethereum"
    position_manager = str(payload.get("position_manager") or "").strip().lower()
    token_id = str(payload.get("token_id") or "").strip()
    normalized = {
        "position_key": build_position_key(protocol, chain, position_manager, token_id),
        "protocol": protocol,
        "chain": chain,
        "position_manager": position_manager,
        "token_id": token_id,
        "owner": str(payload.get("owner") or "").strip().lower(),
        "pool": str(payload.get("pool") or "").strip().lower(),
        "token0": str(payload.get("token0") or "").strip().lower(),
        "token1": str(payload.get("token1") or "").strip().lower(),
        "token0_symbol": str(payload.get("token0_symbol") or "").strip().upper(),
        "token1_symbol": str(payload.get("token1_symbol") or "").strip().upper(),
        "tick_lower": payload.get("tick_lower"),
        "tick_upper": payload.get("tick_upper"),
        "current_liquidity": _normalize_float(payload.get("current_liquidity")),
        "opened_at": _normalize_int(payload.get("opened_at")),
        "last_action_at": _normalize_int(payload.get("last_action_at")),
        "collected_fees_count": _normalize_int(payload.get("collected_fees_count")),
        "cumulative_added_amount0": _normalize_float(payload.get("cumulative_added_amount0")),
        "cumulative_added_amount1": _normalize_float(payload.get("cumulative_added_amount1")),
        "cumulative_removed_amount0": _normalize_float(payload.get("cumulative_removed_amount0")),
        "cumulative_removed_amount1": _normalize_float(payload.get("cumulative_removed_amount1")),
    }
    return normalized


def _load_cache_payload() -> tuple[dict[str, dict], list[dict]]:
    try:
        with CLMM_POSITION_CACHE_PATH.open(encoding="utf-8") as fp:
            payload = json.load(fp)
    except FileNotFoundError:
        return {}, []
    except json.JSONDecodeError as exc:
        print(f"Warning: {CLMM_POSITION_CACHE_PATH.name} invalid JSON; using empty CLMM position cache ({exc.msg}).")
        return {}, []

    if isinstance(payload, dict) and "positions" in payload:
        positions_raw = payload.get("positions") or {}
        history_raw = payload.get("history") or []
    elif isinstance(payload, dict):
        positions_raw = payload
        history_raw = []
    else:
        positions_raw = {}
        history_raw = []

    positions = {}
    if isinstance(positions_raw, dict):
        for _, value in positions_raw.items():
            if not isinstance(value, dict):
                continue
            normalized = normalize_position_state(value)
            if normalized["position_key"]:
                positions[normalized["position_key"]] = normalized

    history = []
    if isinstance(history_raw, list):
        for item in history_raw:
            if isinstance(item, dict):
                history.append(dict(item))
    return positions, history


POSITION_STATES, POSITION_ACTION_HISTORY = _load_cache_payload()


def _persist_cache_snapshot() -> None:
    try:
        CLMM_POSITION_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "positions": POSITION_STATES,
            "history": POSITION_ACTION_HISTORY[-200:],
        }
        tmp_path = CLMM_POSITION_CACHE_PATH.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, sort_keys=True)
        tmp_path.replace(CLMM_POSITION_CACHE_PATH)
    except Exception as exc:
        print(f"Warning: {CLMM_POSITION_CACHE_PATH.name} persist failed ({exc}).")


def get_position_state(position_key: str | None) -> dict:
    normalized_key = str(position_key or "").strip()
    if not normalized_key:
        return normalize_position_state({})
    with _CACHE_LOCK:
        return normalize_position_state(POSITION_STATES.get(normalized_key) or {})


def upsert_position_state(state: dict | None) -> dict:
    normalized = normalize_position_state(state)
    if not normalized["position_key"]:
        return normalized
    with _CACHE_LOCK:
        POSITION_STATES[normalized["position_key"]] = dict(normalized)
        _persist_cache_snapshot()
    return dict(normalized)


def record_position_action(action: dict | None) -> dict:
    payload = dict(action or {})
    payload["position_key"] = str(payload.get("position_key") or "").strip()
    payload["owner"] = str(payload.get("owner") or "").strip().lower()
    payload["pool"] = str(payload.get("pool") or "").strip().lower()
    payload["ts"] = _normalize_int(payload.get("ts"))
    payload["token_id"] = str(payload.get("token_id") or "").strip()
    payload["action"] = str(payload.get("action") or "").strip()
    with _CACHE_LOCK:
        POSITION_ACTION_HISTORY.append(payload)
        del POSITION_ACTION_HISTORY[:-300]
        _persist_cache_snapshot()
    return dict(payload)


def recent_position_actions(
    *,
    owner: str | None = None,
    pool: str | None = None,
    window_sec: int = 900,
    now_ts: int | None = None,
    exclude_position_key: str | None = None,
) -> list[dict]:
    normalized_owner = str(owner or "").strip().lower()
    normalized_pool = str(pool or "").strip().lower()
    normalized_exclude = str(exclude_position_key or "").strip()
    upper_ts = _normalize_int(now_ts)
    lower_ts = upper_ts - int(window_sec or 0) if upper_ts > 0 and window_sec > 0 else None

    with _CACHE_LOCK:
        items = [dict(item) for item in POSITION_ACTION_HISTORY]

    results = []
    for item in items:
        if normalized_owner and str(item.get("owner") or "").strip().lower() != normalized_owner:
            continue
        if normalized_pool and str(item.get("pool") or "").strip().lower() != normalized_pool:
            continue
        if normalized_exclude and str(item.get("position_key") or "").strip() == normalized_exclude:
            continue
        item_ts = _normalize_int(item.get("ts"))
        if lower_ts is not None and item_ts and item_ts < lower_ts:
            continue
        if upper_ts and item_ts and item_ts > upper_ts:
            continue
        results.append(dict(item))
    return results


def reset_position_runtime_state() -> None:
    with _CACHE_LOCK:
        POSITION_STATES.clear()
        POSITION_ACTION_HISTORY.clear()
