import json
from pathlib import Path


CLMM_MANAGERS_PATH = Path(__file__).resolve().parent.parent / "data" / "clmm_managers.json"
SUPPORTED_CLMM_PROTOCOLS = {
    "uniswap_v3",
    "uniswap_v4",
}


def _load_clmm_manager_book():
    try:
        with CLMM_MANAGERS_PATH.open(encoding="utf-8") as fp:
            payload = json.load(fp)
    except FileNotFoundError:
        print(f"Warning: {CLMM_MANAGERS_PATH.name} missing; using empty CLMM manager registry.")
        return []
    except json.JSONDecodeError as exc:
        print(f"Warning: {CLMM_MANAGERS_PATH.name} invalid JSON; using empty CLMM manager registry ({exc.msg}).")
        return []

    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        items = []
        for manager_address, meta in payload.items():
            if not isinstance(meta, dict):
                continue
            item = dict(meta)
            item.setdefault("manager_address", manager_address)
            items.append(item)
        return items
    return []


def _normalize_manager(item: dict) -> dict | None:
    manager_address = str(item.get("manager_address") or item.get("address") or "").lower().strip()
    if not manager_address:
        return None

    protocol = str(item.get("protocol") or "").strip().lower()
    if protocol not in SUPPORTED_CLMM_PROTOCOLS:
        return None

    chain = str(item.get("chain") or "ethereum").strip().lower() or "ethereum"
    abi_profile = str(item.get("abi_profile") or f"{protocol}_position_manager").strip() or f"{protocol}_position_manager"
    return {
        "protocol": protocol,
        "chain": chain,
        "manager_address": manager_address,
        "abi_profile": abi_profile,
        "enabled": bool(item.get("enabled", True)),
        "note": str(item.get("note") or "").strip(),
    }


CLMM_MANAGER_BOOK = _load_clmm_manager_book()
CLMM_MANAGERS_BY_CHAIN: dict[str, dict[str, dict]] = {}
for raw_item in CLMM_MANAGER_BOOK:
    if not isinstance(raw_item, dict):
        continue
    normalized = _normalize_manager(raw_item)
    if normalized is None:
        continue
    chain_bucket = CLMM_MANAGERS_BY_CHAIN.setdefault(normalized["chain"], {})
    chain_bucket[normalized["manager_address"]] = normalized


def all_clmm_managers(*, chain: str | None = None, include_disabled: bool = False) -> list[dict]:
    if chain:
        managers = list(CLMM_MANAGERS_BY_CHAIN.get(str(chain).strip().lower(), {}).values())
    else:
        managers = []
        for bucket in CLMM_MANAGERS_BY_CHAIN.values():
            managers.extend(bucket.values())
    if include_disabled:
        return [dict(item) for item in managers]
    return [dict(item) for item in managers if bool(item.get("enabled", True))]


def get_clmm_manager_meta(manager_address: str | None, *, chain: str = "ethereum", include_disabled: bool = False) -> dict | None:
    normalized_chain = str(chain or "ethereum").strip().lower() or "ethereum"
    normalized_address = str(manager_address or "").strip().lower()
    if not normalized_address:
        return None
    meta = CLMM_MANAGERS_BY_CHAIN.get(normalized_chain, {}).get(normalized_address)
    if meta is None:
        return None
    if not include_disabled and not bool(meta.get("enabled", True)):
        return None
    return dict(meta)


def is_clmm_position_manager(manager_address: str | None, *, chain: str = "ethereum", include_disabled: bool = False) -> bool:
    return get_clmm_manager_meta(manager_address, chain=chain, include_disabled=include_disabled) is not None


def identify_clmm_protocol(manager_address: str | None, *, chain: str = "ethereum") -> str:
    meta = get_clmm_manager_meta(manager_address, chain=chain)
    return str(meta.get("protocol") or "") if meta else ""


def iter_clmm_candidate_addresses(item: dict | None) -> list[str]:
    item = item or {}
    addresses = []
    for value in [
        item.get("watch_address"),
        item.get("position_manager"),
        item.get("manager_address"),
        item.get("clmm_manager"),
        item.get("address"),
        *(item.get("participant_addresses") or []),
        *(item.get("next_hop_addresses") or []),
        *(item.get("touched_watch_addresses") or []),
        *(item.get("touched_lp_pools") or []),
    ]:
        text = str(value or "").strip().lower()
        if text and text not in addresses:
            addresses.append(text)

    for log in item.get("decoded_logs") or item.get("decoded_events") or []:
        if not isinstance(log, dict):
            continue
        text = str(log.get("address") or "").strip().lower()
        if text and text not in addresses:
            addresses.append(text)
    return addresses


def detect_clmm_manager_meta(item: dict | None, *, chain: str = "ethereum") -> dict | None:
    item = item or {}
    explicit_meta = item.get("position_manager_meta") or item.get("clmm_manager_meta") or {}
    if isinstance(explicit_meta, dict) and explicit_meta:
        normalized = _normalize_manager(explicit_meta)
        if normalized is not None:
            return normalized

    explicit_address = (
        item.get("position_manager")
        or item.get("manager_address")
        or item.get("clmm_manager")
    )
    meta = get_clmm_manager_meta(explicit_address, chain=chain)
    if meta is not None:
        return meta

    for address in iter_clmm_candidate_addresses(item):
        meta = get_clmm_manager_meta(address, chain=chain)
        if meta is not None:
            return meta
    return None
