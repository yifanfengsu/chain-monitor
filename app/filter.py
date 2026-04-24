import json
import os
from pathlib import Path

from lp_registry import get_lp_pool_meta, is_lp_pool
from state_manager import is_runtime_adjacent_watch_active, maybe_get_runtime_adjacent_watch_meta

# 地址簿路径：固定定位到项目根目录 data/addresses.json，避免受 cwd 影响。
ADDRESS_BOOK_PATH = Path(__file__).resolve().parent.parent / "data" / "addresses.json"
ENTITY_BOOK_PATH = Path(__file__).resolve().parent.parent / "data" / "entities.json"
ADDRESS_ALIASES_PATH = Path(__file__).resolve().parent.parent / "data" / "address_aliases.json"
ALLOW_ALIAS_ONLY_ADDRESS_BOOK = str(
    os.getenv("ALLOW_ALIAS_ONLY_ADDRESS_BOOK", "false")
).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

VALID_ENTITY_TYPES = {
    "exchange",
    "market_maker_firm",
    "protocol",
    "treasury",
    "unknown",
}
VALID_WALLET_FUNCTIONS = {
    "exchange_deposit",
    "exchange_hot",
    "exchange_cold",
    "exchange_internal_buffer",
    "exchange_trading",
    "mm_inventory",
    "mm_settlement",
    "protocol_treasury",
    "lp_pool",
    "router",
    "unknown",
}
VALID_ENTITY_SOURCES = {
    "address_book",
    "inferred_likely",
    "adjacent_only",
    "unknown",
}
VALID_ENTITY_ATTRIBUTION_STRENGTHS = {
    "confirmed_entity",
    "likely_entity",
    "adjacent_only",
    "unknown",
}


def _load_json_book_with_status(path: Path, *, fallback):
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f), True, False
    except FileNotFoundError:
        print(f"Warning: {path.name} missing; using fallback.")
        return fallback, False, True
    except json.JSONDecodeError as exc:
        print(f"Warning: {path.name} invalid JSON; using fallback ({exc.msg}).")
        return fallback, False, False


def _load_json_book(path: Path, *, fallback):
    payload, _loaded, _missing = _load_json_book_with_status(path, fallback=fallback)
    return payload


def _load_address_book():
    payload, loaded, missing = _load_json_book_with_status(ADDRESS_BOOK_PATH, fallback={})
    return payload, {
        "address_book_loaded": loaded,
        "address_book_missing": missing,
    }


def _load_entity_book():
    return _load_json_book(ENTITY_BOOK_PATH, fallback={})


def _load_address_aliases():
    return _load_json_book(ADDRESS_ALIASES_PATH, fallback={})


def _normalize_string_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items = []
        seen = set()
        for item in value:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            items.append(text)
        return items
    text = str(value or "").strip()
    return [text] if text else []


def _normalize_entity_type(value) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized if normalized in VALID_ENTITY_TYPES else "unknown"


def _normalize_wallet_function(value) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized if normalized in VALID_WALLET_FUNCTIONS else "unknown"


def _normalize_entity_source(value) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized if normalized in VALID_ENTITY_SOURCES else "unknown"


def _normalize_entity_attribution_strength(value) -> str:
    normalized = str(value or "unknown").strip().lower()
    return normalized if normalized in VALID_ENTITY_ATTRIBUTION_STRENGTHS else "unknown"


def _normalize_confidence(value, default: float = 0.0) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(1.0, normalized))


def _address_book_items(address_book):
    if isinstance(address_book, dict):
        items = []
        for address, payload in address_book.items():
            if isinstance(payload, dict):
                item = dict(payload)
                item.setdefault("address", address)
                items.append(item)
                continue
            if payload:
                items.append({"address": str(address), "label": str(payload)})
        return items
    if isinstance(address_book, list):
        return address_book
    return []


# 公开仓库允许缺失私有地址簿；缺主地址簿时 fail-closed，不让 alias/entity 补丁创建监控集。
RAW_ADDRESS_BOOK, ADDRESS_BOOK_LOAD_STATE = _load_address_book()
ADDRESS_BOOK_LOADED = bool(ADDRESS_BOOK_LOAD_STATE.get("address_book_loaded"))
ADDRESS_BOOK_MISSING = bool(ADDRESS_BOOK_LOAD_STATE.get("address_book_missing"))
ENTITY_BOOK = _load_entity_book() if ADDRESS_BOOK_LOADED else {}
ADDRESS_ALIASES = _load_address_aliases() if ADDRESS_BOOK_LOADED else {}


CATEGORY_LABELS = {
    "exchange_whale": "交易所巨鲸",
    "issuer_whale": "发行方巨鲸",
    "smart_money": "聪明钱",
    "celebrity_whale": "名人鲸鱼",
    "user_added": "自定义地址",
    "lp_pool": "流动性池",
    "unknown": "未分类",
}

ROLE_LABELS = {
    "exchange": "交易所",
    "issuer": "发行方",
    "smart_money": "聪明钱",
    "celebrity": "名人地址",
    "protocol": "协议地址",
    "router": "路由地址",
    "liquidity_provider": "流动性提供者",
    "liquidity_pool": "流动性池",
    "arbitrage_bot": "套利机器人",
    "user_watch": "自定义监控",
    "unknown": "未分类",
}

STRATEGY_ROLE_LABELS = {
    "smart_money_wallet": "聪明钱钱包",
    "market_maker_wallet": "做市钱包",
    "alpha_wallet": "Alpha 钱包",
    "adjacent_watch": "下游观察地址",
    "exchange_hot_wallet": "交易所热钱包",
    "exchange_deposit_wallet": "交易所充值地址",
    "exchange_trading_wallet": "交易所交易席位",
    "treasury_issuer": "发行方金库",
    "celebrity_wallet": "名人钱包",
    "user_watch": "自定义监控",
    "aggregator_router": "聚合路由",
    "protocol_treasury": "协议资金地址",
    "liquidity_provider": "LP 地址",
    "lp_pool": "主流交易对池",
    "arbitrage_bot": "套利机器人",
    "unknown": "未分类",
}

SEMANTIC_ROLE_LABELS = {
    "exchange_wallet": "交易所钱包",
    "exchange_hot_wallet": "交易所热钱包",
    "exchange_deposit_address": "交易所充值地址",
    "router_contract": "路由合约",
    "protocol_wallet": "协议钱包",
    "issuer_treasury": "发行方金库",
    "market_maker_wallet": "做市地址",
    "trader_wallet": "交易地址",
    "investment_wallet": "投资地址",
    "personal_wallet": "个人地址",
    "lp_wallet": "LP 地址",
    "liquidity_pool": "流动性池",
    "bot_wallet": "机器人地址",
    "watched_wallet": "监控地址",
    "unknown": "未分类",
}

ENTITY_TYPE_LABELS = {
    "exchange": "交易所实体",
    "market_maker_firm": "做市机构",
    "protocol": "协议实体",
    "treasury": "资金金库",
    "unknown": "未知实体",
}

WALLET_FUNCTION_LABELS = {
    "exchange_deposit": "Deposit Wallet",
    "exchange_hot": "Hot Wallet",
    "exchange_cold": "Cold Wallet",
    "exchange_internal_buffer": "Internal Buffer",
    "exchange_trading": "Trading Wallet",
    "mm_inventory": "Inventory Wallet",
    "mm_settlement": "Settlement Wallet",
    "protocol_treasury": "Protocol Treasury",
    "lp_pool": "LP Pool",
    "router": "Router",
    "unknown": "Unknown Wallet",
}

CATEGORY_ROLE_DEFAULTS = {
    "smart_money": {
        "role": "smart_money",
        "strategy_role": "smart_money_wallet",
        "semantic_role": "trader_wallet",
    },
    "exchange_whale": {
        "role": "exchange",
        "strategy_role": "exchange_hot_wallet",
        "semantic_role": "exchange_wallet",
    },
    "issuer_whale": {
        "role": "issuer",
        "strategy_role": "treasury_issuer",
        "semantic_role": "issuer_treasury",
    },
    "celebrity_whale": {
        "role": "celebrity",
        "strategy_role": "celebrity_wallet",
        "semantic_role": "personal_wallet",
    },
    "user_added": {
        "role": "user_watch",
        "strategy_role": "user_watch",
        "semantic_role": "watched_wallet",
    },
    "lp_pool": {
        "role": "liquidity_pool",
        "strategy_role": "lp_pool",
        "semantic_role": "liquidity_pool",
    },
    "unknown": {
        "role": "unknown",
        "strategy_role": "unknown",
        "semantic_role": "unknown",
    },
}

# 当前精简地址池实际主要使用：
# - market_maker_wallet
# - treasury_issuer
# - celebrity_wallet
# - exchange_hot_wallet
# 其余角色阈值保留为兼容兜底，不主动依赖关键词推断触发。
STRATEGY_ROLE_THRESHOLDS = {
    "smart_money_wallet": {1: 100, 2: 200, 3: 300},
    "market_maker_wallet": {1: 120, 2: 220, 3: 320},
    "alpha_wallet": {1: 150, 2: 250, 3: 350},
    "adjacent_watch": {1: 30000, 2: 50000, 3: 80000},
    "exchange_hot_wallet": {1: 1000, 2: 1000, 3: 1500},
    "exchange_cold_wallet": {1: 1000, 2: 1000, 3: 1500},
    "exchange_deposit_wallet": {1: 1500, 2: 1500, 3: 2000},
    "exchange_trading_wallet": {1: 800, 2: 1000, 3: 1200},
    "treasury_issuer": {1: 5000, 2: 5000, 3: 6000},
    "celebrity_wallet": {1: 1500, 2: 2000, 3: 2500},
    "user_watch": {1: 300, 2: 500, 3: 800},
    "aggregator_router": {1: 3000, 2: 4000, 3: 5000},
    "protocol_treasury": {1: 1800, 2: 2500, 3: 3200},
    "liquidity_provider": {1: 800, 2: 1200, 3: 1600},
    "lp_pool": {1: 15000, 2: 25000, 3: 40000},
    "arbitrage_bot": {1: 500, 2: 800, 3: 1200},
    "unknown": {1: 500, 2: 500, 3: 500}, 
}

EXCHANGE_STRATEGY_ROLES = {
    "exchange_hot_wallet",
    "exchange_cold_wallet",
    "exchange_deposit_wallet",
    "exchange_trading_wallet",
}
MARKET_MAKER_STRATEGY_ROLES = {
    "market_maker_wallet",
}
SMART_MONEY_STRATEGY_ROLES = {
    "smart_money_wallet",
    "alpha_wallet",
    "celebrity_wallet",
}
PRIORITY_SMART_MONEY_STRATEGY_ROLES = {
    "smart_money_wallet",
    "alpha_wallet",
    "celebrity_wallet",
}
LP_STRATEGY_ROLES = {
    "lp_pool",
}


def _entity_book_item(entity_id: str) -> dict:
    normalized = str(entity_id or "").strip()
    if not normalized:
        return {}
    entity_book = ENTITY_BOOK
    if isinstance(entity_book, dict):
        entry = entity_book.get(normalized) or entity_book.get(normalized.lower()) or {}
        if isinstance(entry, dict):
            payload = dict(entry)
            payload.setdefault("entity_id", normalized)
            return payload
    elif isinstance(entity_book, list):
        for item in entity_book:
            if not isinstance(item, dict):
                continue
            current = str(item.get("entity_id") or "").strip()
            if current and current.lower() == normalized.lower():
                payload = dict(item)
                payload.setdefault("entity_id", current)
                return payload
    return {}


def _address_alias_items(address_aliases):
    if isinstance(address_aliases, dict):
        items = []
        for address, payload in address_aliases.items():
            if isinstance(payload, dict):
                item = dict(payload)
                item.setdefault("address", address)
                items.append(item)
        return items
    if isinstance(address_aliases, list):
        return [item for item in address_aliases if isinstance(item, dict)]
    return []


def _merge_address_aliases(address_book, address_aliases):
    alias_by_address = {}
    for item in _address_alias_items(address_aliases):
        address = str(item.get("address") or "").lower()
        if not address:
            continue
        alias_by_address[address] = dict(item)

    if not alias_by_address:
        return address_book

    address_items = _address_book_items(address_book)
    if not address_items and not ALLOW_ALIAS_ONLY_ADDRESS_BOOK:
        return address_book

    merged_items = []
    for item in address_items:
        if isinstance(item, dict) and item.get("address"):
            address = str(item.get("address") or "").lower()
            alias = alias_by_address.pop(address, {})
            payload = dict(item)
            for key, value in alias.items():
                if key == "address":
                    continue
                payload[key] = value
            merged_items.append(payload)
        else:
            merged_items.append(item)

    if ALLOW_ALIAS_ONLY_ADDRESS_BOOK:
        for address, alias in alias_by_address.items():
            payload = dict(alias)
            payload["address"] = address
            merged_items.append(payload)

    return merged_items


def _wallet_function_from_roles(item: dict) -> str:
    explicit = _normalize_wallet_function(item.get("wallet_function"))
    if explicit != "unknown":
        return explicit

    strategy_role = str(item.get("strategy_role") or "").strip()
    semantic_role = str(item.get("semantic_role") or "").strip()
    role = str(item.get("role") or "").strip()
    category = str(item.get("category") or "").strip()

    if strategy_role == "exchange_deposit_wallet" or semantic_role == "exchange_deposit_address":
        return "exchange_deposit"
    if strategy_role == "exchange_hot_wallet" or semantic_role == "exchange_hot_wallet":
        return "exchange_hot"
    if strategy_role == "exchange_trading_wallet":
        return "exchange_trading"
    if strategy_role == "market_maker_wallet" or semantic_role == "market_maker_wallet":
        return "mm_inventory"
    if strategy_role == "protocol_treasury" or semantic_role == "protocol_wallet":
        return "protocol_treasury"
    if strategy_role == "aggregator_router" or semantic_role == "router_contract" or role == "router":
        return "router"
    if strategy_role == "lp_pool" or semantic_role == "liquidity_pool" or category == "lp_pool":
        return "lp_pool"
    return "unknown"


def _entity_type_from_item(item: dict, entity_entry: dict) -> str:
    explicit = _normalize_entity_type(item.get("entity_type"))
    if explicit != "unknown":
        return explicit

    entity_type = _normalize_entity_type(entity_entry.get("entity_type"))
    if entity_type != "unknown":
        return entity_type

    role = str(item.get("role") or "").strip()
    strategy_role = str(item.get("strategy_role") or "").strip()
    semantic_role = str(item.get("semantic_role") or "").strip()
    category = str(item.get("category") or "").strip()

    if role == "exchange" or strategy_role.startswith("exchange_") or semantic_role.startswith("exchange_"):
        return "exchange"
    if strategy_role == "market_maker_wallet":
        return "market_maker_firm"
    if role == "protocol" or strategy_role == "protocol_treasury":
        return "protocol"
    if role == "issuer" or strategy_role == "treasury_issuer":
        return "treasury"
    if category == "lp_pool":
        return "protocol"
    return "unknown"


def _entity_label_from_item(item: dict, entity_entry: dict) -> str:
    explicit = str(item.get("entity_label") or "").strip()
    if explicit:
        return explicit
    from_book = str(entity_entry.get("entity_label") or entity_entry.get("label") or "").strip()
    if from_book:
        return from_book
    if str(item.get("entity_id") or "").strip():
        return str(item.get("label") or "").strip()
    return ""


def _entity_source_from_item(item: dict, *, has_entity_id: bool) -> str:
    explicit = _normalize_entity_source(item.get("entity_source"))
    if explicit != "unknown":
        return explicit
    if has_entity_id:
        return "address_book"
    return "unknown"


def _entity_attribution_strength_from_item(item: dict, *, has_entity_id: bool, entity_source: str) -> str:
    explicit = _normalize_entity_attribution_strength(item.get("entity_attribution_strength"))
    if explicit != "unknown":
        return explicit
    if has_entity_id:
        return "confirmed_entity"
    if entity_source == "inferred_likely":
        return "likely_entity"
    if entity_source == "adjacent_only":
        return "adjacent_only"
    return "unknown"


def _normalize_priority(value) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        priority = 3
    return priority if priority in (1, 2, 3) else 3


def _default_roles_from_category(category: str) -> dict:
    return CATEGORY_ROLE_DEFAULTS.get(category, CATEGORY_ROLE_DEFAULTS["unknown"]).copy()


def _infer_roles(item: dict) -> dict:
    category = str(item.get("category") or "unknown")
    explicit = {
        "role": str(item.get("role") or "").strip(),
        "strategy_role": str(item.get("strategy_role") or "").strip(),
        "semantic_role": str(item.get("semantic_role") or "").strip(),
    }
    inferred = _default_roles_from_category(category)

    # 地址池已经过提纯：只要显式字段存在，就优先信任地址簿定义。
    if any(explicit.values()):
        return {
            "role": explicit["role"] or inferred["role"],
            "strategy_role": explicit["strategy_role"] or inferred["strategy_role"],
            "semantic_role": explicit["semantic_role"] or inferred["semantic_role"],
        }

    label = str(item.get("label") or "").lower()
    note = str(item.get("note") or "").lower()
    source = str(item.get("source") or "").lower()
    text = " ".join([label, note, source])

    # 仅保留少量高置信兜底，不再依赖地址名称做大量语义推断。
    if "1inch" in text or "router" in text or "aggregator" in text or "聚合" in text:
        return {
            "role": "router",
            "strategy_role": "aggregator_router",
            "semantic_role": "router_contract",
        }

    if category == "exchange_whale" and ("deposit" in text or "充值" in text):
        return {
            "role": "exchange",
            "strategy_role": "exchange_deposit_wallet",
            "semantic_role": "exchange_deposit_address",
        }

    if category == "exchange_whale" and ("hot wallet" in text or "热钱包" in text):
        return {
            "role": "exchange",
            "strategy_role": "exchange_hot_wallet",
            "semantic_role": "exchange_hot_wallet",
        }

    if category == "issuer_whale" or "treasury" in text or "发行方" in text:
        return {
            "role": "issuer",
            "strategy_role": "treasury_issuer",
            "semantic_role": "issuer_treasury",
        }

    if "协议" in text or "protocol" in text:
        return {
            "role": "protocol",
            "strategy_role": "protocol_treasury",
            "semantic_role": "protocol_wallet",
        }

    if "lp" in text or "liquidity" in text or "流动性" in text:
        return {
            "role": "liquidity_provider",
            "strategy_role": "liquidity_provider",
            "semantic_role": "lp_wallet",
        }

    if "arbitrage" in text or "套利" in text:
        return {
            "role": "arbitrage_bot",
            "strategy_role": "arbitrage_bot",
            "semantic_role": "bot_wallet",
        }

    if category == "user_added":
        return {
            "role": "user_watch",
            "strategy_role": "user_watch",
            "semantic_role": "watched_wallet",
        }

    return inferred


def build_address_labels(address_book):
    """构建 address -> label 的快捷索引。"""
    labels = {}

    for item in _address_book_items(address_book):
        if isinstance(item, dict) and item.get("address"):
            labels[item["address"].lower()] = item.get("label", item["address"])

    return labels


def build_address_meta(address_book):
    """构建 address -> 元信息（展示 / 策略 / 解释语义）索引。"""
    meta = {}

    for item in _address_book_items(address_book):
        if not (isinstance(item, dict) and item.get("address")):
            continue

        category = str(item.get("category") or "unknown")
        priority = _normalize_priority(item.get("priority", 3))
        defaults = _default_roles_from_category(category)
        inferred = _infer_roles(item)
        explicit_role = str(item.get("role") or "").strip()
        explicit_strategy_role = str(item.get("strategy_role") or "").strip()
        explicit_semantic_role = str(item.get("semantic_role") or "").strip()

        role = explicit_role or inferred["role"] or defaults["role"]
        strategy_role = explicit_strategy_role or inferred["strategy_role"] or defaults["strategy_role"]
        semantic_role = explicit_semantic_role or inferred["semantic_role"] or defaults["semantic_role"]

        if explicit_role and explicit_strategy_role and explicit_semantic_role:
            role_source = "explicit"
        elif explicit_role or explicit_strategy_role or explicit_semantic_role:
            role_source = "mixed"
        else:
            role_source = "fallback"

        entity_id = str(item.get("entity_id") or "").strip()
        entity_entry = _entity_book_item(entity_id)
        entity_label = _entity_label_from_item(item, entity_entry)
        entity_type = _entity_type_from_item(item, entity_entry)
        wallet_function = _wallet_function_from_roles({
            **dict(item),
            "role": role,
            "strategy_role": strategy_role,
            "semantic_role": semantic_role,
        })
        entity_source = _entity_source_from_item(item, has_entity_id=bool(entity_id))
        entity_attribution_strength = _entity_attribution_strength_from_item(
            item,
            has_entity_id=bool(entity_id),
            entity_source=entity_source,
        )
        cluster_tags = _normalize_string_list(
            item.get("cluster_tags")
            or entity_entry.get("cluster_tags")
        )
        entity_confidence = _normalize_confidence(
            item.get("entity_confidence"),
            default=1.0 if entity_attribution_strength == "confirmed_entity" else 0.0,
        )
        ownership_confidence = _normalize_confidence(
            item.get("ownership_confidence"),
            default=entity_confidence if entity_attribution_strength == "confirmed_entity" else 0.0,
        )
        entity_why = _normalize_string_list(
            item.get("entity_why")
            or entity_entry.get("entity_why")
        )
        wallet_function_confidence = _normalize_confidence(
            item.get("wallet_function_confidence"),
            default=1.0 if wallet_function != "unknown" else 0.0,
        )
        wallet_function_source = str(item.get("wallet_function_source") or ("address_book" if wallet_function != "unknown" else "unknown")).strip()

        meta[item["address"].lower()] = {
            "address": item["address"].lower(),
            "label": item.get("label", item["address"]),
            "category": category,
            "priority": priority,
            "is_active": bool(item.get("is_active", True)),
            "note": item.get("note", ""),
            "source": item.get("source", ""),
            "role": role,
            "strategy_role": strategy_role,
            "semantic_role": semantic_role,
            "role_source": role_source,
            "entity_id": entity_id,
            "entity_label": entity_label,
            "entity_type": entity_type,
            "entity_type_label": ENTITY_TYPE_LABELS.get(entity_type, entity_type),
            "wallet_function": wallet_function,
            "wallet_function_label": WALLET_FUNCTION_LABELS.get(wallet_function, wallet_function),
            "cluster_tags": cluster_tags,
            "ownership_confidence": ownership_confidence,
            "entity_confidence": entity_confidence,
            "entity_source": entity_source,
            "entity_attribution_strength": entity_attribution_strength,
            "entity_why": entity_why,
            "wallet_function_confidence": wallet_function_confidence,
            "wallet_function_source": wallet_function_source,
        }

    return meta


def extract_watch_addresses(address_book, active_only=False):
    """提取监控地址集合，可选择仅提取已启用地址。"""
    addresses = set()

    for item in _address_book_items(address_book):
        if isinstance(item, str):
            if active_only:
                continue
            addresses.add(item.lower())
            continue

        if isinstance(item, dict) and item.get("address"):
            if active_only and not bool(item.get("is_active", True)):
                continue
            addresses.add(item["address"].lower())

    return addresses


# 全量地址（包含禁用地址，用于兜底查 meta）。
ADDRESS_BOOK = (
    _merge_address_aliases(RAW_ADDRESS_BOOK, ADDRESS_ALIASES)
    if ADDRESS_BOOK_LOADED
    else {}
)
ALL_WATCH_ADDRESSES = extract_watch_addresses(ADDRESS_BOOK, active_only=False)
# 实际参与监控的地址（只含 is_active=true）。
WATCH_ADDRESSES = extract_watch_addresses(ADDRESS_BOOK, active_only=True)
ADDRESS_LABELS = build_address_labels(ADDRESS_BOOK)
ADDRESS_META = build_address_meta(ADDRESS_BOOK)
_ADDRESS_INTELLIGENCE_MANAGER = None


def set_address_intelligence_manager(manager) -> None:
    """注册运行时地址情报管理器，供 meta 叠加使用。"""
    global _ADDRESS_INTELLIGENCE_MANAGER
    _ADDRESS_INTELLIGENCE_MANAGER = manager


def get_address_intelligence_manager():
    return _ADDRESS_INTELLIGENCE_MANAGER


def _intelligence_patch(address: str) -> dict:
    manager = get_address_intelligence_manager()
    if manager is None or not address:
        return {}

    try:
        return dict(manager.get_meta_patch(address) or {})
    except Exception:
        return {}


def _runtime_adjacent_patch(address: str) -> dict:
    if not address:
        return {}
    try:
        return dict(maybe_get_runtime_adjacent_watch_meta(address) or {})
    except Exception:
        return {}


def shorten_address(address):
    """把长地址压缩成 0x1234...abcd 便于消息展示。"""
    if not address:
        return ""

    address = address.lower()
    if len(address) < 12:
        return address

    return f"{address[:8]}...{address[-6:]}"


def _format_address_label_from_parts(
    address: str,
    meta: dict | None = None,
    intel_patch: dict | None = None,
    runtime_patch: dict | None = None,
    pool_meta: dict | None = None,
) -> str:
    if not address:
        return ""

    address = address.lower()
    short_address = shorten_address(address)
    pool_meta = pool_meta or get_lp_pool_meta(address)
    if pool_meta:
        return str(pool_meta.get("label") or "").strip() or short_address

    meta = meta or ADDRESS_META.get(address) or {}
    intel_patch = intel_patch or {}
    runtime_patch = runtime_patch or {}

    label = str(meta.get("label") or "").strip()
    if label:
        return f"{label} ({short_address})"

    display_role = _display_role_label(meta)
    if display_role and display_role != "未分类":
        return f"{display_role} ({short_address})"

    suspected_role = str(intel_patch.get("suspected_role") or "").strip()
    if suspected_role and suspected_role != "unknown":
        return f"{suspected_role} ({short_address})"

    display_hint = str(
        runtime_patch.get("display_hint_label")
        or intel_patch.get("display_hint_label")
        or ""
    ).strip()
    if display_hint:
        return f"{display_hint} ({short_address})"

    fallback_label = ADDRESS_LABELS.get(address)
    if fallback_label:
        return f"{fallback_label} ({short_address})"

    return short_address


def _display_role_label(meta: dict) -> str:
    semantic_role = meta.get("semantic_role", "unknown")
    category = meta.get("category", "unknown")
    return SEMANTIC_ROLE_LABELS.get(
        semantic_role,
        CATEGORY_LABELS.get(category, category),
    )


def _entity_defaults() -> dict:
    return {
        "entity_id": "",
        "entity_label": "",
        "entity_type": "unknown",
        "entity_type_label": ENTITY_TYPE_LABELS["unknown"],
        "wallet_function": "unknown",
        "wallet_function_label": WALLET_FUNCTION_LABELS["unknown"],
        "cluster_tags": [],
        "ownership_confidence": 0.0,
        "entity_confidence": 0.0,
        "entity_source": "unknown",
        "entity_attribution_strength": "unknown",
        "entity_why": [],
        "wallet_function_confidence": 0.0,
        "wallet_function_source": "unknown",
    }


def format_address_label(address):
    """格式化地址显示名：优先输出稳定标签，减少调试型冗余信息。"""
    if not address:
        return ""

    address = address.lower()
    pool_meta = get_lp_pool_meta(address)
    meta = ADDRESS_META.get(address)
    intel_patch = _intelligence_patch(address)
    runtime_patch = _runtime_adjacent_patch(address)
    return _format_address_label_from_parts(
        address=address,
        meta=meta,
        intel_patch=intel_patch,
        runtime_patch=runtime_patch,
        pool_meta=pool_meta,
    )


def get_address_meta(address):
    """读取地址元信息，不存在时返回完整默认结构。"""
    if not address:
        defaults = _default_roles_from_category("unknown")
        result = {
            "address": "",
            "label": "",
            "category": "unknown",
            "category_label": CATEGORY_LABELS["unknown"],
            "priority": 3,
            "is_active": True,
            "display": "",
            "note": "",
            "source": "",
            "role": defaults["role"],
            "role_label": ROLE_LABELS.get(defaults["role"], defaults["role"]),
            "strategy_role": defaults["strategy_role"],
            "strategy_role_label": STRATEGY_ROLE_LABELS.get(defaults["strategy_role"], defaults["strategy_role"]),
            "semantic_role": defaults["semantic_role"],
            "semantic_role_label": SEMANTIC_ROLE_LABELS.get(defaults["semantic_role"], defaults["semantic_role"]),
            "role_source": "default",
            "display_role_label": SEMANTIC_ROLE_LABELS.get(defaults["semantic_role"], defaults["semantic_role"]),
            "intelligence_status": "",
            "suspected_role": "",
            "role_confidence": 0.0,
            "candidate_score": 0.0,
            "first_seen_ts": 0,
            "last_seen_ts": 0,
        }
        result.update(_entity_defaults())
        return result

    address = address.lower()
    pool_meta = get_lp_pool_meta(address)
    if pool_meta:
        result = {
            "address": address,
            "label": pool_meta.get("label", address),
            "category": pool_meta.get("category", "lp_pool"),
            "category_label": pool_meta.get("category_label", CATEGORY_LABELS["lp_pool"]),
            "priority": _normalize_priority(pool_meta.get("priority", 2)),
            "is_active": bool(pool_meta.get("is_active", True)),
            "display": pool_meta.get("display", pool_meta.get("label", address)),
            "note": pool_meta.get("note", ""),
            "source": pool_meta.get("source", "lp_registry"),
            "role": pool_meta.get("role", "liquidity_pool"),
            "role_label": pool_meta.get("role_label", ROLE_LABELS.get("liquidity_pool", "流动性池")),
            "strategy_role": pool_meta.get("strategy_role", "lp_pool"),
            "strategy_role_label": pool_meta.get("strategy_role_label", STRATEGY_ROLE_LABELS.get("lp_pool", "主流交易对池")),
            "semantic_role": pool_meta.get("semantic_role", "liquidity_pool"),
            "semantic_role_label": pool_meta.get("semantic_role_label", SEMANTIC_ROLE_LABELS.get("liquidity_pool", "流动性池")),
            "role_source": pool_meta.get("role_source", "lp_registry"),
            "display_role_label": pool_meta.get("display_role_label", "流动性池"),
            "pair_label": pool_meta.get("pair_label", ""),
            "protocol": pool_meta.get("protocol", ""),
            "dex": pool_meta.get("dex", ""),
            "base_token_contract": pool_meta.get("base_token_contract", ""),
            "base_token_symbol": pool_meta.get("base_token_symbol", ""),
            "quote_token_contract": pool_meta.get("quote_token_contract", ""),
            "quote_token_symbol": pool_meta.get("quote_token_symbol", ""),
            "is_primary_trend_pool": bool(pool_meta.get("is_primary_trend_pool")),
            "is_major_pool": bool(pool_meta.get("is_major_pool")),
            "major_priority_score": float(pool_meta.get("major_priority_score") or 1.0),
            "major_match_mode": str(pool_meta.get("major_match_mode") or ""),
            "major_base_symbol": str(pool_meta.get("major_base_symbol") or ""),
            "major_quote_symbol": str(pool_meta.get("major_quote_symbol") or ""),
            "trend_pool_family": str(pool_meta.get("trend_pool_family") or ""),
            "trend_base_family": str(pool_meta.get("trend_base_family") or ""),
            "trend_quote_family": str(pool_meta.get("trend_quote_family") or ""),
            "trend_pool_match_mode": str(pool_meta.get("trend_pool_match_mode") or ""),
        }
        result.update({
            "intelligence_status": "",
            "suspected_role": "",
            "role_confidence": 0.0,
            "candidate_score": 0.0,
            "first_seen_ts": 0,
            "last_seen_ts": 0,
        })
        result.update({
            "entity_id": str(pool_meta.get("entity_id") or ""),
            "entity_label": str(pool_meta.get("entity_label") or pool_meta.get("protocol") or ""),
            "entity_type": _normalize_entity_type(pool_meta.get("entity_type") or ("protocol" if pool_meta.get("protocol") else "unknown")),
            "wallet_function": _normalize_wallet_function(pool_meta.get("wallet_function") or "lp_pool"),
            "cluster_tags": _normalize_string_list(pool_meta.get("cluster_tags")),
            "ownership_confidence": _normalize_confidence(pool_meta.get("ownership_confidence"), default=0.0),
            "entity_confidence": _normalize_confidence(pool_meta.get("entity_confidence"), default=0.0),
            "entity_source": _normalize_entity_source(pool_meta.get("entity_source") or "unknown"),
            "entity_attribution_strength": _normalize_entity_attribution_strength(pool_meta.get("entity_attribution_strength") or "unknown"),
            "entity_why": _normalize_string_list(pool_meta.get("entity_why")),
            "wallet_function_confidence": _normalize_confidence(pool_meta.get("wallet_function_confidence"), default=1.0),
            "wallet_function_source": str(pool_meta.get("wallet_function_source") or "lp_registry"),
        })
        result["entity_type_label"] = ENTITY_TYPE_LABELS.get(result["entity_type"], result["entity_type"])
        result["wallet_function_label"] = WALLET_FUNCTION_LABELS.get(result["wallet_function"], result["wallet_function"])
        return result

    meta = ADDRESS_META.get(address, {})
    category = str(meta.get("category") or "unknown")
    priority = _normalize_priority(meta.get("priority", 3))
    defaults = _default_roles_from_category(category)

    role = str(meta.get("role") or defaults["role"])
    strategy_role = str(meta.get("strategy_role") or defaults["strategy_role"])
    semantic_role = str(meta.get("semantic_role") or defaults["semantic_role"])

    result = {
        "address": address,
        "label": meta.get("label", address),
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
        "priority": priority,
        "is_active": bool(meta.get("is_active", True)),
        "display": format_address_label(address),
        "note": meta.get("note", ""),
        "source": meta.get("source", ""),
        "role": role,
        "role_label": ROLE_LABELS.get(role, role),
        "strategy_role": strategy_role,
        "strategy_role_label": STRATEGY_ROLE_LABELS.get(strategy_role, strategy_role),
        "semantic_role": semantic_role,
        "semantic_role_label": SEMANTIC_ROLE_LABELS.get(semantic_role, semantic_role),
        "role_source": meta.get("role_source", "fallback"),
        "display_role_label": SEMANTIC_ROLE_LABELS.get(semantic_role, semantic_role),
    }
    result.update({
        "intelligence_status": "",
        "suspected_role": "",
        "role_confidence": 0.0,
        "candidate_score": 0.0,
        "first_seen_ts": 0,
        "last_seen_ts": 0,
        "runtime_adjacent_watch": False,
        "watch_meta_source": "",
        "anchor_watch_address": "",
        "anchor_label": "",
        "root_tx_hash": "",
        "opened_at": 0,
        "expire_at": 0,
        "hop": 0,
        "strategy_hint": "",
        "observed_count": 0,
        "display_hint_label": "",
        "display_hint_reason": "",
        "display_hint_anchor_label": "",
        "display_hint_anchor_address": "",
        "display_hint_usd_value": 0.0,
        "display_hint_token_symbol": "",
        "anchor_strategy_role": "",
        "downstream_case_id": "",
    })
    result.update(_entity_defaults())
    if meta:
        result.update({
            "entity_id": str(meta.get("entity_id") or ""),
            "entity_label": str(meta.get("entity_label") or ""),
            "entity_type": _normalize_entity_type(meta.get("entity_type")),
            "wallet_function": _normalize_wallet_function(meta.get("wallet_function")),
            "cluster_tags": _normalize_string_list(meta.get("cluster_tags")),
            "ownership_confidence": _normalize_confidence(meta.get("ownership_confidence")),
            "entity_confidence": _normalize_confidence(meta.get("entity_confidence")),
            "entity_source": _normalize_entity_source(meta.get("entity_source")),
            "entity_attribution_strength": _normalize_entity_attribution_strength(meta.get("entity_attribution_strength")),
            "entity_why": _normalize_string_list(meta.get("entity_why")),
            "wallet_function_confidence": _normalize_confidence(meta.get("wallet_function_confidence")),
            "wallet_function_source": str(meta.get("wallet_function_source") or "unknown"),
        })
    intel_patch = _intelligence_patch(address)
    runtime_patch = _runtime_adjacent_patch(address)
    result.update(intel_patch)
    result.update(runtime_patch)
    result["role"] = str(result.get("role") or defaults["role"])
    result["strategy_role"] = str(result.get("strategy_role") or defaults["strategy_role"])
    result["semantic_role"] = str(result.get("semantic_role") or defaults["semantic_role"])
    result["role_label"] = ROLE_LABELS.get(result["role"], result["role"])
    result["strategy_role_label"] = STRATEGY_ROLE_LABELS.get(result["strategy_role"], result["strategy_role"])
    result["semantic_role_label"] = SEMANTIC_ROLE_LABELS.get(result["semantic_role"], result["semantic_role"])
    result["display_role_label"] = SEMANTIC_ROLE_LABELS.get(result["semantic_role"], result["semantic_role"])
    result["entity_type"] = _normalize_entity_type(result.get("entity_type"))
    result["wallet_function"] = _normalize_wallet_function(result.get("wallet_function"))
    result["entity_source"] = _normalize_entity_source(result.get("entity_source"))
    result["entity_attribution_strength"] = _normalize_entity_attribution_strength(result.get("entity_attribution_strength"))
    result["cluster_tags"] = _normalize_string_list(result.get("cluster_tags"))
    result["entity_why"] = _normalize_string_list(result.get("entity_why"))
    result["ownership_confidence"] = _normalize_confidence(result.get("ownership_confidence"))
    result["entity_confidence"] = _normalize_confidence(result.get("entity_confidence"))
    result["wallet_function_confidence"] = _normalize_confidence(result.get("wallet_function_confidence"))
    result["entity_type_label"] = ENTITY_TYPE_LABELS.get(result["entity_type"], result["entity_type"])
    result["wallet_function_label"] = WALLET_FUNCTION_LABELS.get(result["wallet_function"], result["wallet_function"])
    if not meta:
        result["priority"] = int(runtime_patch.get("priority") or result.get("priority", 3) or 3)
    result["display"] = _format_address_label_from_parts(
        address=address,
        meta=meta,
        intel_patch=intel_patch,
        runtime_patch=runtime_patch,
    )
    return result


def build_flow_description(direction, watch_meta, counterparty_meta):
    """生成资金流向描述文案（当前用于上下文字段）。"""
    watch_label = watch_meta.get("semantic_role_label", watch_meta.get("category_label", "未分类"))
    counterparty_label = counterparty_meta.get("semantic_role_label", counterparty_meta.get("category_label", "未分类"))

    if direction == "流入":
        return f"流入 {watch_label}，来源: {counterparty_label}"

    if direction == "流出":
        return f"从 {watch_label} 流出，去向: {counterparty_label}"

    return f"{watch_label} -> {counterparty_label} 内部划转"


def _select_primary_watch_leg(from_addr: str, to_addr: str, from_meta: dict, to_meta: dict) -> tuple[str, str, str, dict, dict]:
    def _watch_meta_sort_key(meta: dict):
        strategy_role = str(meta.get("strategy_role") or "unknown")
        is_exchange_role = 1 if is_exchange_strategy_role(strategy_role) else 0
        priority = _normalize_priority(meta.get("priority", 3))
        threshold = get_threshold(meta)
        label = str(meta.get("label") or "")
        return (is_exchange_role, priority, threshold, label)

    primary_meta = min([from_meta, to_meta], key=_watch_meta_sort_key)
    if str(primary_meta.get("address") or "").lower() == str(from_addr or "").lower():
        return from_addr, to_addr, "流出", from_meta, to_meta
    return to_addr, from_addr, "流入", to_meta, from_meta


def get_watch_context(data):
    """
    为一笔交易生成“监控上下文”：
    - swap：交易上下文
    - 转账：流入/流出/内部划转
    """
    watch_address = (data.get("watch_address") or "").lower()
    if watch_address and is_runtime_adjacent_watch_active(watch_address):
        from_addr = (data.get("from") or "").lower()
        to_addr = (data.get("to") or "").lower() if data.get("to") else None
        runtime_watch_meta = get_address_meta(watch_address)
        if watch_address == from_addr:
            counterparty = to_addr
            counterparty_meta = get_address_meta(counterparty)
            return {
                "direction": "流出",
                "watch_address": watch_address,
                "counterparty": counterparty,
                "watch_address_label": format_address_label(watch_address),
                "counterparty_label": format_address_label(counterparty),
                "watch_meta": runtime_watch_meta,
                "counterparty_meta": counterparty_meta,
                "flow_label": build_flow_description("流出", runtime_watch_meta, counterparty_meta),
            }
        if watch_address == to_addr:
            counterparty = from_addr
            counterparty_meta = get_address_meta(counterparty)
            return {
                "direction": "流入",
                "watch_address": watch_address,
                "counterparty": counterparty,
                "watch_address_label": format_address_label(watch_address),
                "counterparty_label": format_address_label(counterparty),
                "watch_meta": runtime_watch_meta,
                "counterparty_meta": counterparty_meta,
                "flow_label": build_flow_description("流入", runtime_watch_meta, counterparty_meta),
            }

    if watch_address and is_lp_pool(watch_address, active_only=True):
        watch_meta = get_address_meta(watch_address)
        counterparty = str(
            data.get("counterparty")
            or data.get("router")
            or data.get("from")
            or data.get("to")
            or ""
        ).lower()
        if counterparty == watch_address:
            counterparty = ""
        counterparty_meta = get_address_meta(counterparty)
        source_label = format_address_label(str(data.get("from") or "").lower())
        target_label = format_address_label(str(data.get("to") or "").lower())
        return {
            "direction": str(data.get("side") or data.get("direction") or "池子行为"),
            "watch_address": watch_address,
            "watch_address_label": watch_meta.get("label", format_address_label(watch_address)),
            "watch_meta": watch_meta,
            "counterparty": counterparty,
            "counterparty_label": format_address_label(counterparty) if counterparty else "外部参与者",
            "counterparty_meta": counterparty_meta,
            "flow_label": str(data.get("pair_label") or watch_meta.get("pair_label") or watch_meta.get("label") or ""),
            "flow_source_label": source_label or watch_meta.get("label", "流动性池"),
            "flow_target_label": target_label or watch_meta.get("label", "流动性池"),
        }

    if data.get("kind") == "swap":
        if not watch_address or (watch_address not in WATCH_ADDRESSES and not is_runtime_adjacent_watch_active(watch_address)):
            return None

        watch_meta = get_address_meta(watch_address)
        counterparty = (
            data.get("counterparty")
            or data.get("router")
            or data.get("to")
            or data.get("from")
            or ""
        )
        counterparty = str(counterparty).lower()
        counterparty_meta = get_address_meta(counterparty)
        return {
            "direction": "交易",
            "watch_address": watch_address,
            "watch_address_label": format_address_label(watch_address),
            "watch_meta": watch_meta,
            "counterparty": counterparty,
            "counterparty_label": format_address_label(counterparty),
            "counterparty_meta": counterparty_meta,
        }

    from_addr = (data.get("from") or "").lower()
    to_addr = (data.get("to") or "").lower() if data.get("to") else None

    # 仅 active 地址才算“监控地址”。
    from_watch = from_addr in WATCH_ADDRESSES or is_runtime_adjacent_watch_active(from_addr)
    to_watch = bool(to_addr and (to_addr in WATCH_ADDRESSES or is_runtime_adjacent_watch_active(to_addr)))
    from_meta = get_address_meta(from_addr)
    to_meta = get_address_meta(to_addr)

    if from_watch and to_watch:
        if _intelligence_patch(from_addr).get("entity_attribution_strength") == "confirmed_entity":
            from_meta = get_address_meta(from_addr)
        if _intelligence_patch(to_addr).get("entity_attribution_strength") == "confirmed_entity":
            to_meta = get_address_meta(to_addr)

        if (
            str(from_meta.get("entity_attribution_strength") or "") == "confirmed_entity"
            and str(to_meta.get("entity_attribution_strength") or "") == "confirmed_entity"
            and str(from_meta.get("entity_id") or "")
            and str(from_meta.get("entity_id") or "") == str(to_meta.get("entity_id") or "")
        ):
            return {
                "direction": "内部划转",
                "watch_from": from_addr,
                "watch_to": to_addr,
                "watch_from_label": format_address_label(from_addr),
                "watch_to_label": format_address_label(to_addr),
                "watch_from_meta": from_meta,
                "watch_to_meta": to_meta,
                "flow_label": build_flow_description("内部划转", from_meta, to_meta),
            }

        primary_watch_address, counterparty, direction, watch_meta, counterparty_meta = _select_primary_watch_leg(
            from_addr,
            to_addr,
            from_meta,
            to_meta,
        )
        return {
            "direction": direction,
            "watch_address": primary_watch_address,
            "counterparty": counterparty,
            "watch_address_label": format_address_label(primary_watch_address),
            "counterparty_label": format_address_label(counterparty),
            "watch_meta": watch_meta,
            "counterparty_meta": counterparty_meta,
            "watch_from": from_addr,
            "watch_to": to_addr,
            "watch_from_label": format_address_label(from_addr),
            "watch_to_label": format_address_label(to_addr),
            "watch_from_meta": from_meta,
            "watch_to_meta": to_meta,
            "flow_label": build_flow_description(direction, watch_meta, counterparty_meta),
        }

    if to_watch:
        return {
            "direction": "流入",
            "watch_address": to_addr,
            "counterparty": from_addr,
            "watch_address_label": format_address_label(to_addr),
            "counterparty_label": format_address_label(from_addr),
            "watch_meta": to_meta,
            "counterparty_meta": from_meta,
            "flow_label": build_flow_description("流入", to_meta, from_meta),
        }

    if from_watch:
        return {
            "direction": "流出",
            "watch_address": from_addr,
            "counterparty": to_addr,
            "watch_address_label": format_address_label(from_addr),
            "counterparty_label": format_address_label(to_addr),
            "watch_meta": from_meta,
            "counterparty_meta": to_meta,
            "flow_label": build_flow_description("流出", from_meta, to_meta),
        }

    return None


def get_flow_endpoints(watch_context):
    """从 watch_context 提取消息里展示的 source/target。"""
    if not watch_context:
        return ("", "")

    explicit_source = str(watch_context.get("flow_source_label") or "").strip()
    explicit_target = str(watch_context.get("flow_target_label") or "").strip()
    if explicit_source or explicit_target:
        return (explicit_source, explicit_target)

    if watch_context["direction"] == "内部划转":
        return (
            watch_context["watch_from_label"],
            watch_context["watch_to_label"],
        )

    if watch_context["direction"] == "流入":
        return (
            watch_context["counterparty_label"],
            watch_context["watch_address_label"],
        )

    return (
        watch_context["watch_address_label"],
        watch_context["counterparty_label"],
    )


def get_threshold(meta):
    """按 strategy_role + priority 返回动态阈值，优先使用地址簿显式角色。"""
    strategy_role = str(meta.get("strategy_role") or "unknown")
    priority = _normalize_priority(meta.get("priority", 3))

    by_role = STRATEGY_ROLE_THRESHOLDS.get(strategy_role)
    if by_role:
        return float(by_role.get(priority, by_role[3]))

    category = str(meta.get("category") or "unknown")
    fallback = _default_roles_from_category(category)
    fallback_role = fallback["strategy_role"]
    role_thresholds = STRATEGY_ROLE_THRESHOLDS.get(fallback_role, STRATEGY_ROLE_THRESHOLDS["unknown"])
    return float(role_thresholds.get(priority, role_thresholds[3]))


def is_exchange_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in EXCHANGE_STRATEGY_ROLES


def is_smart_money_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in SMART_MONEY_STRATEGY_ROLES


def is_priority_smart_money_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in PRIORITY_SMART_MONEY_STRATEGY_ROLES


def is_market_maker_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in MARKET_MAKER_STRATEGY_ROLES


def is_lp_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in LP_STRATEGY_ROLES


def strategy_role_group(strategy_role: str | None) -> str:
    normalized = str(strategy_role or "unknown")
    if is_lp_strategy_role(normalized):
        return "lp_pool"
    if is_exchange_strategy_role(normalized):
        return "exchange"
    if is_market_maker_strategy_role(normalized):
        return "market_maker"
    if is_smart_money_strategy_role(normalized):
        return "smart_money"
    return "other"


def _resolve_watch_metas(watch_context, data):
    """
    提取本次交易相关的 watch_meta 列表：
    - 普通场景：watch_meta 或 watch_from/watch_to_meta
    - 兜底场景：仅有 watch_address 时回查地址簿
    """
    metas = []

    if watch_context:
        watch_meta = watch_context.get("watch_meta")
        if watch_meta:
            metas.append(watch_meta)

        watch_from_meta = watch_context.get("watch_from_meta")
        if watch_from_meta:
            metas.append(watch_from_meta)

        watch_to_meta = watch_context.get("watch_to_meta")
        if watch_to_meta:
            metas.append(watch_to_meta)

    if not metas and data.get("watch_address"):
        watch_address = str(data["watch_address"]).lower()
        if watch_address in ALL_WATCH_ADDRESSES or is_lp_pool(watch_address) or is_runtime_adjacent_watch_active(watch_address):
            metas.append(get_address_meta(watch_address))

    return metas


def get_primary_watch_meta(data, watch_context=None):
    """
    返回本事件的主监控地址元信息。
    该函数仅提供元数据，不做策略决策。
    """
    watch_context = watch_context or get_watch_context(data)
    watch_metas = _resolve_watch_metas(watch_context, data)
    if not watch_metas:
        return None

    active_watch_metas = [meta for meta in watch_metas if meta.get("is_active", True)]
    if not active_watch_metas:
        return None

    if len(active_watch_metas) == 1:
        return active_watch_metas[0]

    # 多监控对象场景下，不让少量交易所地址轻易主导主 watch_meta。
    # 优先顺序：
    # 1. 非交易所策略角色
    # 2. 更高 priority（数字更小）
    # 3. 更敏感阈值
    def _watch_meta_sort_key(meta: dict):
        strategy_role = str(meta.get("strategy_role") or "unknown")
        is_exchange_role = 1 if is_exchange_strategy_role(strategy_role) else 0
        priority = _normalize_priority(meta.get("priority", 3))
        threshold = get_threshold(meta)
        label = str(meta.get("label") or "")
        return (is_exchange_role, priority, threshold, label)

    return min(active_watch_metas, key=_watch_meta_sort_key)
