import time
from collections import deque

from config import (
    DELIVERY_ALLOW_DOWNSTREAM_OBSERVE,
    DELIVERY_ALLOW_EXCHANGE_OBSERVE,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_ALLOW_LP_OBSERVE,
    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE,
    DELIVERY_OBSERVE_STAGE_BUDGET_TIER1,
    DELIVERY_OBSERVE_STAGE_BUDGET_TIER2,
    DELIVERY_OBSERVE_STAGE_BUDGET_TIER3,
    DELIVERY_OBSERVE_STAGE_BUDGET_TOTAL,
    DELIVERY_PRIMARY_STAGE_BUDGET_TIER1,
    DELIVERY_PRIMARY_STAGE_BUDGET_TIER2,
    DELIVERY_PRIMARY_STAGE_BUDGET_TIER3,
    DELIVERY_PRIMARY_STAGE_BUDGET_TOTAL,
    DELIVERY_STAGE_BUDGET_WINDOW_SEC,
    EXCHANGE_STRONG_OBSERVE_ALLOWED_REASONS,
    EXCHANGE_STRONG_OBSERVE_ENABLE,
    EXCHANGE_STRONG_OBSERVE_MIN_CONFIRMATION,
    EXCHANGE_STRONG_OBSERVE_MIN_PRICING_CONFIDENCE,
    EXCHANGE_STRONG_OBSERVE_MIN_QUALITY,
    EXCHANGE_STRONG_OBSERVE_MIN_RESONANCE,
    EXCHANGE_STRONG_OBSERVE_MIN_USD,
    EXCHANGE_STRONG_OBSERVE_REQUIRE_CONFIRMED_INTENT,
    MARKET_MAKER_NOTIFY_EXECUTION_ONLY,
    SMART_MONEY_NOTIFY_EXECUTION_ONLY,
)
from filter import strategy_role_group


SMART_MONEY_LEGACY_OBSERVE_REASONS = {
    "smart_money_transfer_observe",
    "market_maker_inventory_observe",
    "market_maker_inventory_shift_observe",
    "market_maker_non_execution_observe",
    "smart_money_non_execution_observe",
}
SMART_MONEY_EXECUTION_DELIVERY_REASONS = {
    "market_maker_execution_observe",
    "market_maker_execution_primary",
    "smart_money_execution_observe",
    "smart_money_execution_primary",
    "smart_money_continuous_execution_primary",
}
EXCHANGE_STRONG_OBSERVE_ALLOWED_INTENTS = {
    "exchange_deposit_candidate",
    "exchange_withdraw_candidate",
    "possible_buy_preparation",
    "possible_sell_preparation",
    "swap_execution",
}
EXCHANGE_STRONG_OBSERVE_EXCLUDED_INTENTS = {
    "pure_transfer",
    "unknown_intent",
    "internal_rebalance",
    "market_making_inventory_move",
}
EXCHANGE_STRONG_OBSERVE_EXCLUDED_REASONS = {
    "exchange_inventory_observe",
    "exchange_transfer_observe",
    "exchange_observe",
}
STAGE_BUDGETS = {
    "observe": {
        "total": int(DELIVERY_OBSERVE_STAGE_BUDGET_TOTAL),
        "tier_caps": {
            "tier1": int(DELIVERY_OBSERVE_STAGE_BUDGET_TIER1),
            "tier2": int(DELIVERY_OBSERVE_STAGE_BUDGET_TIER2),
            "tier3": int(DELIVERY_OBSERVE_STAGE_BUDGET_TIER3),
        },
        "reserve": {
            "tier1": min(int(DELIVERY_OBSERVE_STAGE_BUDGET_TIER1), 2),
            "tier2": min(int(DELIVERY_OBSERVE_STAGE_BUDGET_TIER2), 1),
        },
    },
    "primary": {
        "total": int(DELIVERY_PRIMARY_STAGE_BUDGET_TOTAL),
        "tier_caps": {
            "tier1": int(DELIVERY_PRIMARY_STAGE_BUDGET_TIER1),
            "tier2": int(DELIVERY_PRIMARY_STAGE_BUDGET_TIER2),
            "tier3": int(DELIVERY_PRIMARY_STAGE_BUDGET_TIER3),
        },
        "reserve": {
            "tier1": min(int(DELIVERY_PRIMARY_STAGE_BUDGET_TIER1), 1),
            "tier2": min(int(DELIVERY_PRIMARY_STAGE_BUDGET_TIER2), 1),
        },
    },
}
_DELIVERY_HISTORY = deque(maxlen=512)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def _prune_delivery_history(now_ts: int | None = None) -> None:
    reference_ts = int(now_ts or time.time())
    window_sec = max(int(DELIVERY_STAGE_BUDGET_WINDOW_SEC), 1)
    while _DELIVERY_HISTORY and reference_ts - int(_DELIVERY_HISTORY[0].get("ts") or 0) > window_sec:
        _DELIVERY_HISTORY.popleft()


def _stage_budget_payload(
    event,
    signal,
    *,
    allowed: bool,
    reason: str,
    recent_total: int,
    recent_same_tier: int,
    recent_higher_tier: int,
    total_cap: int,
    tier_cap: int,
) -> None:
    delivery_class = str(getattr(signal, "delivery_class", "") or getattr(event, "delivery_class", "") or "")
    role_priority_tier = str(
        getattr(signal, "metadata", {}).get("role_priority_tier")
        or getattr(event, "metadata", {}).get("role_priority_tier")
        or "tier4"
    )
    payload = {
        "stage_budget_evaluated": delivery_class in STAGE_BUDGETS,
        "stage_budget_allowed": bool(allowed),
        "stage_budget_reason": str(reason or ""),
        "stage_budget_stage": delivery_class,
        "stage_budget_window_sec": int(DELIVERY_STAGE_BUDGET_WINDOW_SEC),
        "stage_budget_role_tier": role_priority_tier,
        "stage_budget_recent_total": int(recent_total),
        "stage_budget_recent_same_tier": int(recent_same_tier),
        "stage_budget_recent_higher_tier": int(recent_higher_tier),
        "stage_budget_total_cap": int(total_cap),
        "stage_budget_tier_cap": int(tier_cap),
    }
    getattr(event, "metadata", {}).update(payload)
    getattr(signal, "metadata", {}).update(payload)
    getattr(signal, "context", {}).update(payload)


def _allow_stage_budget(event, signal) -> bool:
    delivery_class = str(getattr(signal, "delivery_class", "") or getattr(event, "delivery_class", "") or "")
    budget = STAGE_BUDGETS.get(delivery_class)
    if budget is None:
        _stage_budget_payload(
            event,
            signal,
            allowed=True,
            reason="stage_budget_not_applicable",
            recent_total=0,
            recent_same_tier=0,
            recent_higher_tier=0,
            total_cap=0,
            tier_cap=0,
        )
        return True

    role_priority_tier = str(
        getattr(signal, "metadata", {}).get("role_priority_tier")
        or getattr(event, "metadata", {}).get("role_priority_tier")
        or "tier4"
    )
    role_priority_rank = _safe_int(
        getattr(signal, "metadata", {}).get("role_priority_rank")
        or getattr(event, "metadata", {}).get("role_priority_rank"),
        4,
    )
    if role_priority_tier not in {"tier1", "tier2", "tier3"}:
        _stage_budget_payload(
            event,
            signal,
            allowed=True,
            reason="stage_budget_other_role_passthrough",
            recent_total=0,
            recent_same_tier=0,
            recent_higher_tier=0,
            total_cap=int(budget["total"]),
            tier_cap=0,
        )
        return True

    now_ts = int(getattr(event, "ts", 0) or time.time())
    _prune_delivery_history(now_ts)
    recent = [item for item in _DELIVERY_HISTORY if item.get("delivery_class") == delivery_class]
    recent_total = len(recent)
    recent_same_tier = sum(1 for item in recent if item.get("role_priority_tier") == role_priority_tier)
    recent_higher_tier = sum(1 for item in recent if _safe_int(item.get("role_priority_rank"), 4) < role_priority_rank)
    total_cap = int(budget["total"])
    tier_cap = int((budget.get("tier_caps") or {}).get(role_priority_tier) or 0)
    reserve = budget.get("reserve") or {}

    allowed = True
    reason = "stage_budget_allowed"
    if recent_total >= total_cap:
        allowed = False
        reason = f"stage_budget_{delivery_class}_window_full"
    elif tier_cap > 0 and recent_same_tier >= tier_cap:
        allowed = False
        reason = f"stage_budget_{delivery_class}_{role_priority_tier}_tier_cap"
    elif role_priority_tier == "tier2" and recent_total >= max(total_cap - int(reserve.get('tier1') or 0), 0):
        allowed = False
        reason = f"stage_budget_{delivery_class}_reserved_for_tier1"
    elif role_priority_tier == "tier3" and recent_total >= max(total_cap - int(reserve.get('tier1') or 0) - int(reserve.get('tier2') or 0), 0):
        allowed = False
        reason = f"stage_budget_{delivery_class}_reserved_for_higher_tiers"
    elif role_priority_tier == "tier3" and recent_higher_tier >= 1 and recent_total >= 1:
        allowed = False
        reason = f"stage_budget_{delivery_class}_higher_tier_pressure"

    _stage_budget_payload(
        event,
        signal,
        allowed=allowed,
        reason=reason,
        recent_total=recent_total,
        recent_same_tier=recent_same_tier,
        recent_higher_tier=recent_higher_tier,
        total_cap=total_cap,
        tier_cap=tier_cap,
    )
    return allowed


def _exchange_strong_observe_thresholds() -> dict:
    return {
        "enabled": bool(EXCHANGE_STRONG_OBSERVE_ENABLE),
        "allowed_reasons": sorted(str(item or "").strip() for item in EXCHANGE_STRONG_OBSERVE_ALLOWED_REASONS if item),
        "allowed_intents": sorted(EXCHANGE_STRONG_OBSERVE_ALLOWED_INTENTS),
        "min_usd": float(EXCHANGE_STRONG_OBSERVE_MIN_USD),
        "min_confirmation": float(EXCHANGE_STRONG_OBSERVE_MIN_CONFIRMATION),
        "min_quality": float(EXCHANGE_STRONG_OBSERVE_MIN_QUALITY),
        "min_resonance": float(EXCHANGE_STRONG_OBSERVE_MIN_RESONANCE),
        "min_pricing_confidence": float(EXCHANGE_STRONG_OBSERVE_MIN_PRICING_CONFIDENCE),
        "require_confirmed_intent": bool(EXCHANGE_STRONG_OBSERVE_REQUIRE_CONFIRMED_INTENT),
    }


def _apply_exchange_strong_observe_metadata(event, signal, allowed: bool, reason: str) -> None:
    payload = {
        "exchange_strong_observe_allowed": bool(allowed),
        "exchange_strong_observe_reason": str(reason or ""),
        "exchange_strong_observe_thresholds": _exchange_strong_observe_thresholds(),
    }
    getattr(event, "metadata", {}).update(payload)
    getattr(signal, "metadata", {}).update(payload)
    getattr(signal, "context", {}).update(payload)


def _is_real_execution(event, signal) -> bool:
    event_metadata = getattr(event, "metadata", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    return bool(
        signal_metadata.get("is_real_execution")
        or event_metadata.get("is_real_execution")
        or getattr(event, "kind", "") == "swap"
        or str(getattr(event, "intent_type", "") or getattr(signal, "intent_type", "") or "") == "swap_execution"
    )


def _smart_money_policy_metadata(
    event,
    signal,
    *,
    allowed: bool,
    reason: str,
    market_maker: bool,
    execution_required_but_missing: bool,
) -> None:
    existing_archive_reason = str(
        getattr(event, "metadata", {}).get("execution_only_archive_reason")
        or getattr(signal, "metadata", {}).get("execution_only_archive_reason")
        or ""
    )
    payload = {
        "smart_money_delivery_policy_allowed": bool(allowed),
        "smart_money_delivery_policy_reason": str(reason or ""),
        "smart_money_delivery_policy_mode": "execution_whitelist_only",
        "smart_money_delivery_policy_hard_whitelist_applied": True,
        "smart_money_allowed_reason_whitelist": sorted(SMART_MONEY_EXECUTION_DELIVERY_REASONS),
        "smart_money_execution_only_mode": bool(SMART_MONEY_NOTIFY_EXECUTION_ONLY),
        "market_maker_execution_only_mode": bool(MARKET_MAKER_NOTIFY_EXECUTION_ONLY),
        "smart_money_legacy_non_exec_branch_disabled": True,
        "execution_required_but_missing": bool(
            execution_required_but_missing
            or getattr(event, "metadata", {}).get("execution_required_but_missing")
            or getattr(signal, "metadata", {}).get("execution_required_but_missing")
        ),
        "execution_only_archive_reason": str(
            existing_archive_reason
            or (reason if (not allowed or execution_required_but_missing) else "")
        ),
    }
    getattr(event, "metadata", {}).update(payload)
    getattr(signal, "metadata", {}).update(payload)
    getattr(signal, "context", {}).update(payload)


def _allow_smart_money_delivery(event, signal, delivery_class: str) -> bool:
    event_metadata = getattr(event, "metadata", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    strategy_role = str(
        signal_metadata.get("strategy_role")
        or event_metadata.get("watch_meta", {}).get("strategy_role")
        or getattr(event, "strategy_role", "")
        or ""
    )
    market_maker = strategy_role == "market_maker_wallet"
    delivery_reason = str(
        getattr(signal, "delivery_reason", "")
        or getattr(event, "delivery_reason", "")
        or ""
    )
    is_execution = _is_real_execution(event, signal)

    if (
        delivery_class == "observe"
        and bool(DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE)
        and delivery_reason in SMART_MONEY_LEGACY_OBSERVE_REASONS
    ):
        _smart_money_policy_metadata(
            event,
            signal,
            allowed=True,
            reason="market_maker_legacy_observe_allowed" if market_maker else "smart_money_legacy_observe_allowed",
            market_maker=market_maker,
            execution_required_but_missing=False,
        )
        return True

    if delivery_reason not in SMART_MONEY_EXECUTION_DELIVERY_REASONS:
        reason = (
            "market_maker_execution_whitelist_reason_not_allowed"
            if market_maker else
            "smart_money_execution_whitelist_reason_not_allowed"
        )
        _smart_money_policy_metadata(
            event,
            signal,
            allowed=False,
            reason=reason,
            market_maker=market_maker,
            execution_required_but_missing=not is_execution,
        )
        return False
    if delivery_class not in {"primary", "observe"}:
        reason = (
            "market_maker_execution_whitelist_non_emittable_delivery_class"
            if market_maker else
            "smart_money_execution_whitelist_non_emittable_delivery_class"
        )
        _smart_money_policy_metadata(
            event,
            signal,
            allowed=False,
            reason=reason,
            market_maker=market_maker,
            execution_required_but_missing=not is_execution,
        )
        return False
    if not is_execution:
        reason = (
            "market_maker_execution_whitelist_requires_execution"
            if market_maker else
            "smart_money_execution_whitelist_requires_execution"
        )
        _smart_money_policy_metadata(
            event,
            signal,
            allowed=False,
            reason=reason,
            market_maker=market_maker,
            execution_required_but_missing=True,
        )
        return False
    _smart_money_policy_metadata(
        event,
        signal,
        allowed=True,
        reason="market_maker_execution_whitelist_allowed" if market_maker else "smart_money_execution_whitelist_allowed",
        market_maker=market_maker,
        execution_required_but_missing=False,
    )
    return True


def _allow_strong_exchange_observe(event, signal) -> bool:
    delivery_class = str(
        getattr(signal, "delivery_class", "")
        or getattr(event, "delivery_class", "")
        or ""
    )
    if delivery_class != "observe":
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_requires_observe_delivery")
        return False

    event_metadata = getattr(event, "metadata", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    role_group = str(
        signal_metadata.get("role_group")
        or event_metadata.get("role_group")
        or strategy_role_group(
            getattr(event, "strategy_role", "")
            or signal_metadata.get("strategy_role")
            or ""
        )
        or ""
    )
    if role_group != "exchange":
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_requires_exchange_role_group")
        return False
    if not bool(EXCHANGE_STRONG_OBSERVE_ENABLE):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_disabled")
        return False

    delivery_reason = str(
        getattr(signal, "delivery_reason", "")
        or getattr(event, "delivery_reason", "")
        or ""
    )
    allowed_reasons = {str(item or "").strip() for item in EXCHANGE_STRONG_OBSERVE_ALLOWED_REASONS if item}
    if delivery_reason in EXCHANGE_STRONG_OBSERVE_EXCLUDED_REASONS:
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_reason_explicitly_excluded")
        return False
    if delivery_reason not in allowed_reasons:
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_reason_not_allowed")
        return False

    intent_type = str(getattr(event, "intent_type", "") or getattr(signal, "intent_type", "") or "")
    if intent_type in EXCHANGE_STRONG_OBSERVE_EXCLUDED_INTENTS:
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_intent_explicitly_excluded")
        return False
    if intent_type not in EXCHANGE_STRONG_OBSERVE_ALLOWED_INTENTS:
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_intent_not_allowed")
        return False

    if bool(EXCHANGE_STRONG_OBSERVE_REQUIRE_CONFIRMED_INTENT):
        intent_stage = str(getattr(event, "intent_stage", "") or getattr(signal, "intent_stage", "") or "")
        if intent_stage != "confirmed":
            _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_requires_confirmed_intent")
            return False

    usd_value = _safe_float(getattr(signal, "usd_value", None), _safe_float(getattr(event, "usd_value", None), 0.0))
    if usd_value < float(EXCHANGE_STRONG_OBSERVE_MIN_USD):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_usd_below_min")
        return False

    confirmation_score = _safe_float(
        getattr(signal, "confirmation_score", None),
        _safe_float(getattr(event, "confirmation_score", None), 0.0),
    )
    if confirmation_score < float(EXCHANGE_STRONG_OBSERVE_MIN_CONFIRMATION):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_confirmation_below_min")
        return False

    quality_score = _safe_float(getattr(signal, "quality_score", None), 0.0)
    if quality_score < float(EXCHANGE_STRONG_OBSERVE_MIN_QUALITY):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_quality_below_min")
        return False

    resonance_score = _safe_float(signal_metadata.get("resonance_score"), 0.0)
    if resonance_score < float(EXCHANGE_STRONG_OBSERVE_MIN_RESONANCE):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_resonance_below_min")
        return False

    pricing_confidence = _safe_float(
        getattr(signal, "pricing_confidence", None),
        _safe_float(getattr(event, "pricing_confidence", None), 0.0),
    )
    if pricing_confidence < float(EXCHANGE_STRONG_OBSERVE_MIN_PRICING_CONFIDENCE):
        _apply_exchange_strong_observe_metadata(event, signal, False, "strong_exchange_observe_pricing_below_min")
        return False

    _apply_exchange_strong_observe_metadata(event, signal, True, "strong_exchange_observe_allowed")
    return True


def can_emit_delivery_notification(event, signal) -> bool:
    delivery_class = str(
        getattr(signal, "delivery_class", "")
        or getattr(event, "delivery_class", "drop")
        or "drop"
    )

    event_metadata = getattr(event, "metadata", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    case_family = str(
        event_metadata.get("case_family")
        or signal_metadata.get("case_family")
        or ""
    )
    if case_family == "downstream_counterparty_followup":
        return bool(DELIVERY_ALLOW_DOWNSTREAM_OBSERVE)

    liquidation_stage = str(
        event_metadata.get("liquidation_stage")
        or signal_metadata.get("liquidation_stage")
        or "none"
    )
    if liquidation_stage in {"risk", "execution"}:
        return bool(DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE)

    role_group = str(
        signal_metadata.get("role_group")
        or strategy_role_group(
            getattr(event, "strategy_role", "")
            or signal_metadata.get("strategy_role")
            or ""
        )
        or ""
    )
    if role_group == "smart_money":
        return _allow_smart_money_delivery(event, signal, delivery_class) and _allow_stage_budget(event, signal)
    if delivery_class == "primary":
        return _allow_stage_budget(event, signal)
    if delivery_class != "observe":
        return False
    if role_group == "lp_pool":
        return bool(DELIVERY_ALLOW_LP_OBSERVE) and _allow_stage_budget(event, signal)
    if role_group == "exchange":
        strong_allowed = _allow_strong_exchange_observe(event, signal)
        if DELIVERY_ALLOW_EXCHANGE_OBSERVE:
            return _allow_stage_budget(event, signal)
        return strong_allowed and _allow_stage_budget(event, signal)
    return False


def record_delivery_notification(event, signal, delivered: bool) -> None:
    if not delivered:
        return
    delivery_class = str(getattr(signal, "delivery_class", "") or getattr(event, "delivery_class", "") or "")
    if delivery_class not in STAGE_BUDGETS:
        return
    role_priority_tier = str(
        getattr(signal, "metadata", {}).get("role_priority_tier")
        or getattr(event, "metadata", {}).get("role_priority_tier")
        or "tier4"
    )
    role_priority_rank = _safe_int(
        getattr(signal, "metadata", {}).get("role_priority_rank")
        or getattr(event, "metadata", {}).get("role_priority_rank"),
        4,
    )
    _prune_delivery_history(int(getattr(event, "ts", 0) or time.time()))
    _DELIVERY_HISTORY.append(
        {
            "ts": int(getattr(event, "ts", 0) or time.time()),
            "delivery_class": delivery_class,
            "role_priority_tier": role_priority_tier,
            "role_priority_rank": role_priority_rank,
            "tx_hash": str(getattr(event, "tx_hash", "") or getattr(signal, "tx_hash", "") or ""),
        }
    )
