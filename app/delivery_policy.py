from config import (
    DELIVERY_ALLOW_DOWNSTREAM_OBSERVE,
    DELIVERY_ALLOW_EXCHANGE_OBSERVE,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_ALLOW_LP_OBSERVE,
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


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
        return _allow_smart_money_delivery(event, signal, delivery_class)
    if delivery_class == "primary":
        return True
    if delivery_class != "observe":
        return False
    if role_group == "lp_pool":
        return bool(DELIVERY_ALLOW_LP_OBSERVE)
    if role_group == "exchange":
        strong_allowed = _allow_strong_exchange_observe(event, signal)
        if DELIVERY_ALLOW_EXCHANGE_OBSERVE:
            return True
        return strong_allowed
    return False
