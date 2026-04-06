from config import (
    DELIVERY_ALLOW_DOWNSTREAM_OBSERVE,
    DELIVERY_ALLOW_EXCHANGE_OBSERVE,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_ALLOW_LP_OBSERVE,
    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE,
)
from filter import strategy_role_group


SMART_MONEY_OBSERVE_REASONS = {
    "smart_money_transfer_observe",
    "market_maker_execution_observe",
    "market_maker_inventory_observe",
    "market_maker_inventory_shift_observe",
    "market_maker_non_execution_observe",
    "smart_money_non_execution_observe",
    "smart_money_execution_observe",
}


def can_emit_delivery_notification(event, signal) -> bool:
    delivery_class = str(
        getattr(signal, "delivery_class", "")
        or getattr(event, "delivery_class", "drop")
        or "drop"
    )
    if delivery_class == "primary":
        return True
    if delivery_class != "observe":
        return False

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

    role_group = strategy_role_group(
        getattr(event, "strategy_role", "")
        or signal_metadata.get("strategy_role")
        or ""
    )
    if role_group == "lp_pool":
        return bool(DELIVERY_ALLOW_LP_OBSERVE)
    if role_group == "exchange":
        return bool(DELIVERY_ALLOW_EXCHANGE_OBSERVE)
    if role_group != "smart_money":
        return False
    if not DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE:
        return False

    delivery_reason = str(
        getattr(signal, "delivery_reason", "")
        or getattr(event, "delivery_reason", "")
        or ""
    )
    return delivery_reason in SMART_MONEY_OBSERVE_REASONS
