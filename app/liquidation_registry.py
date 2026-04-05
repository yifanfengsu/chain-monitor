from __future__ import annotations

from copy import deepcopy


LIQUIDATION_RELEVANT_ROLE_GROUPS = {
    "lending_protocol",
    "liquidation_engine",
    "auction_house",
    "vault",
    "keeper",
    "executor",
    "protocol_router",
}


LIQUIDATION_PROTOCOL_ADDRESS_BOOK = {
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": {
        "protocol_name": "Aave",
        "protocol_role": "lending_protocol",
        "liquidation_relevance_score": 0.58,
        "label": "Aave V3 Pool",
    },
    "0x2f39d218133afab8f2c766fedfaa2674ca7da90e": {
        "protocol_name": "Aave",
        "protocol_role": "protocol_router",
        "liquidation_relevance_score": 0.34,
        "label": "Aave V3 PoolAddressesProvider",
    },
    "0xc13e21b648a5ee794902342038ff3adab66be987": {
        "protocol_name": "Spark",
        "protocol_role": "lending_protocol",
        "liquidation_relevance_score": 0.62,
        "label": "SparkLend Pool",
    },
    "0xc3d688b66703497daa19211eedff47f25384cdc3": {
        "protocol_name": "Compound",
        "protocol_role": "lending_protocol",
        "liquidation_relevance_score": 0.60,
        "label": "Compound III Comet USDC",
    },
    "0xc67963a226eddd77b91ad8c421630a1b0adff270": {
        "protocol_name": "Maker",
        "protocol_role": "auction_house",
        "liquidation_relevance_score": 0.92,
        "label": "Maker ETH-A Clipper",
    },
    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": {
        "protocol_name": "Morpho",
        "protocol_role": "lending_protocol",
        "liquidation_relevance_score": 0.60,
        "label": "Morpho Blue Core",
    },
}


PROTOCOL_KEYWORD_HINTS = {
    "aave": ("Aave", "lending_protocol", 0.32),
    "spark": ("Spark", "lending_protocol", 0.34),
    "maker": ("Maker", "vault", 0.30),
    "clipper": ("Maker", "auction_house", 0.44),
    "compound": ("Compound", "lending_protocol", 0.32),
    "comet": ("Compound", "lending_protocol", 0.36),
    "morpho": ("Morpho", "lending_protocol", 0.34),
    "vault": ("Maker", "vault", 0.26),
    "auction": ("Maker", "auction_house", 0.30),
    "keeper": ("Generic", "keeper", 0.48),
    "liquidat": ("Generic", "executor", 0.48),
}


KEEPER_EXECUTOR_ROLES = {"keeper", "executor", "protocol_router"}
VAULT_OR_AUCTION_ROLES = {"vault", "auction_house"}
LENDING_PROTOCOL_ROLES = {"lending_protocol"}


def _normalize_address(address: str | None) -> str:
    return str(address or "").strip().lower()


def lookup_liquidation_address(address: str | None) -> dict | None:
    normalized = _normalize_address(address)
    if not normalized:
        return None

    item = LIQUIDATION_PROTOCOL_ADDRESS_BOOK.get(normalized)
    if not item:
        return None

    payload = deepcopy(item)
    payload["address"] = normalized
    return payload


def scan_liquidation_context(
    addresses: list[str] | None,
    participant_meta: dict[str, dict] | None = None,
) -> dict:
    participant_meta = participant_meta or {}
    matches = []
    seen = set()

    for raw_address in addresses or []:
        address = _normalize_address(raw_address)
        if not address or address in seen:
            continue
        seen.add(address)

        match = lookup_liquidation_address(address)
        if match is not None:
            matches.append(match)
            continue

        meta = participant_meta.get(address) or {}
        haystack = " ".join([
            str(meta.get("label") or ""),
            str(meta.get("role") or ""),
            str(meta.get("strategy_role") or ""),
            str(meta.get("semantic_role") or ""),
        ]).lower()
        if not haystack:
            continue

        for keyword, (protocol_name, protocol_role, score) in PROTOCOL_KEYWORD_HINTS.items():
            if keyword not in haystack:
                continue
            matches.append({
                "address": address,
                "protocol_name": protocol_name,
                "protocol_role": protocol_role,
                "liquidation_relevance_score": score,
                "label": str(meta.get("label") or keyword),
                "heuristic": True,
            })
            break

    protocols = []
    roles = []
    related_addresses = []
    max_score = 0.0

    for match in matches:
        protocol_name = str(match.get("protocol_name") or "")
        protocol_role = str(match.get("protocol_role") or "")
        address = _normalize_address(match.get("address"))
        score = float(match.get("liquidation_relevance_score") or 0.0)
        if protocol_name and protocol_name not in protocols:
            protocols.append(protocol_name)
        if protocol_role and protocol_role not in roles:
            roles.append(protocol_role)
        if address and address not in related_addresses:
            related_addresses.append(address)
        max_score = max(max_score, score)

    return {
        "matches": matches,
        "protocols": protocols,
        "roles": roles,
        "related_addresses": related_addresses,
        "is_liquidation_protocol_related": bool(matches),
        "possible_keeper_executor": any(role in KEEPER_EXECUTOR_ROLES for role in roles),
        "possible_vault_or_auction": any(role in VAULT_OR_AUCTION_ROLES for role in roles),
        "possible_lending_protocol": any(role in LENDING_PROTOCOL_ROLES for role in roles),
        "max_relevance_score": round(max_score, 3),
    }
