import json
from pathlib import Path

from lp_registry import get_lp_pool_meta, is_lp_pool

# 地址簿路径：固定定位到项目根目录 data/addresses.json，避免受 cwd 影响。
ADDRESS_BOOK_PATH = Path(__file__).resolve().parent.parent / "data" / "addresses.json"
with ADDRESS_BOOK_PATH.open(encoding="utf-8") as f:
    # 地址簿是元信息中心：展示语义、策略语义、解释语义都从这里衍生。
    ADDRESS_BOOK = json.load(f)


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
    "exchange_hot_wallet": {1: 1000, 2: 1000, 3: 1500},
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
    "exchange_deposit_wallet",
    "exchange_trading_wallet",
}
SMART_MONEY_STRATEGY_ROLES = {
    "smart_money_wallet",
    "alpha_wallet",
    "market_maker_wallet",
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

    for item in address_book:
        if isinstance(item, dict) and item.get("address"):
            labels[item["address"].lower()] = item.get("label", item["address"])

    return labels


def build_address_meta(address_book):
    """构建 address -> 元信息（展示 / 策略 / 解释语义）索引。"""
    meta = {}

    for item in address_book:
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
        }

    return meta


def extract_watch_addresses(address_book, active_only=False):
    """提取监控地址集合，可选择仅提取已启用地址。"""
    addresses = set()

    for item in address_book:
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


def shorten_address(address):
    """把长地址压缩成 0x1234...abcd 便于消息展示。"""
    if not address:
        return ""

    address = address.lower()
    if len(address) < 12:
        return address

    return f"{address[:8]}...{address[-6:]}"


def _display_role_label(meta: dict) -> str:
    semantic_role = meta.get("semantic_role", "unknown")
    category = meta.get("category", "unknown")
    return SEMANTIC_ROLE_LABELS.get(
        semantic_role,
        CATEGORY_LABELS.get(category, category),
    )


def format_address_label(address):
    """格式化地址显示名：优先输出稳定标签，减少调试型冗余信息。"""
    if not address:
        return ""

    address = address.lower()
    pool_meta = get_lp_pool_meta(address)
    if pool_meta:
        return str(pool_meta.get("label") or "").strip() or shorten_address(address)

    meta = ADDRESS_META.get(address)
    short_address = shorten_address(address)

    if meta:
        label = str(meta.get("label") or "").strip()
        if label:
            return f"{label} ({short_address})"
        display_role = _display_role_label(meta)
        if display_role and display_role != "未分类":
            return f"{display_role} ({short_address})"

    intel_patch = _intelligence_patch(address)
    suspected_role = str(intel_patch.get("suspected_role") or "").strip()
    if suspected_role and suspected_role != "unknown":
        return f"{suspected_role} ({short_address})"

    label = ADDRESS_LABELS.get(address)
    if label:
        return f"{label} ({short_address})"

    return short_address


def get_address_meta(address):
    """读取地址元信息，不存在时返回完整默认结构。"""
    if not address:
        defaults = _default_roles_from_category("unknown")
        return {
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
        }
        result.update({
            "intelligence_status": "",
            "suspected_role": "",
            "role_confidence": 0.0,
            "candidate_score": 0.0,
            "first_seen_ts": 0,
            "last_seen_ts": 0,
        })
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
    })
    result.update(_intelligence_patch(address))
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


def get_watch_context(data):
    """
    为一笔交易生成“监控上下文”：
    - swap：交易上下文
    - 转账：流入/流出/内部划转
    """
    watch_address = (data.get("watch_address") or "").lower()
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
        if not watch_address or watch_address not in WATCH_ADDRESSES:
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
    from_watch = from_addr in WATCH_ADDRESSES
    to_watch = bool(to_addr and to_addr in WATCH_ADDRESSES)
    from_meta = get_address_meta(from_addr)
    to_meta = get_address_meta(to_addr)

    if from_watch and to_watch:
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
    return str(strategy_role or "unknown") == "market_maker_wallet"


def is_lp_strategy_role(strategy_role: str | None) -> bool:
    return str(strategy_role or "unknown") in LP_STRATEGY_ROLES


def strategy_role_group(strategy_role: str | None) -> str:
    normalized = str(strategy_role or "unknown")
    if is_lp_strategy_role(normalized):
        return "lp_pool"
    if is_exchange_strategy_role(normalized):
        return "exchange"
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
        if watch_address in ALL_WATCH_ADDRESSES or is_lp_pool(watch_address):
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
