import math

from config import (
    DELIVERY_ALLOW_EXCHANGE_ANCHOR_PRIMARY,
    DELIVERY_ALLOW_LIQUIDATION_EXECUTION_PRIMARY,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_LP_WEAK_SIGNAL_ARCHIVE_ONLY,
    DELIVERY_SMART_MONEY_EXECUTION_PRIMARY,
    LIQUIDATION_EXECUTION_MIN_SCORE,
    LIQUIDATION_PRIMARY_MIN_USD,
    LIQUIDATION_RISK_MIN_SCORE,
    MARKET_MAKER_NOTIFY_EXECUTION_ONLY,
    LP_FIRST_HIT_PRIMARY_ENABLE,
    LP_FIRST_HIT_PRIMARY_MIN_ABNORMAL_RATIO,
    LP_FIRST_HIT_PRIMARY_MIN_ACTION_INTENSITY,
    LP_FIRST_HIT_PRIMARY_MIN_CONFIDENCE,
    LP_FIRST_HIT_PRIMARY_MIN_PRICING_CONFIDENCE,
    LP_FIRST_HIT_PRIMARY_MIN_QUALITY,
    LP_FIRST_HIT_PRIMARY_MIN_RESERVE_SKEW,
    LP_FIRST_HIT_PRIMARY_MIN_SURGE_RATIO,
    LP_FIRST_HIT_PRIMARY_MIN_USD,
    LP_FIRST_HIT_PRIMARY_DIRECT_ENABLE,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_ABNORMAL_RATIO,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_ACTION_INTENSITY,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_CONFIDENCE,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_PRICING_CONFIDENCE,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_QUALITY,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_RESERVE_SKEW,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_SURGE_RATIO,
    LP_FIRST_HIT_PRIMARY_DIRECT_MIN_USD,
    LP_BURST_PRIMARY_MIN_ACTION_INTENSITY,
    LP_BURST_PRIMARY_MIN_CONFIRMATION,
    LP_BURST_PRIMARY_MIN_EVENT_COUNT,
    LP_BURST_PRIMARY_MIN_QUALITY,
    LP_BURST_PRIMARY_MIN_TOTAL_USD,
    LP_BURST_PRIMARY_MIN_VOLUME_SURGE_RATIO,
    LP_PREALERT_MIN_PRICING_CONFIDENCE,
    LP_TREND_BURST_PRIMARY_MIN_ACTION_INTENSITY,
    LP_TREND_BURST_PRIMARY_MIN_EVENT_COUNT,
    LP_TREND_BURST_PRIMARY_MIN_TOTAL_USD,
    LP_TREND_BURST_PRIMARY_MIN_VOLUME_SURGE_RATIO,
    LP_OBSERVE_MIN_CONFIDENCE,
    LP_OBSERVE_MIN_USD,
    LP_PRIMARY_MIN_CONFIDENCE,
    LP_PRIMARY_MIN_SURGE_RATIO,
    LP_VOLUME_SURGE_MIN_RATIO,
    MARKET_MAKER_OBSERVE_GATE_FLOOR,
    MARKET_MAKER_OBSERVE_MIN_CONFIRMATION,
    MARKET_MAKER_OBSERVE_MIN_RESONANCE,
    MARKET_MAKER_PRIMARY_STRICT,
    MIN_ADDRESS_SCORE,
    MIN_BEHAVIOR_CONFIDENCE,
    MIN_CONFIDENCE,
    MIN_SIGNAL_USD,
    MIN_TOKEN_SCORE,
    SMART_MONEY_NOTIFY_EXECUTION_ONLY,
    STRATEGY_REQUIRE_NON_NORMAL_BEHAVIOR,
)
from filter import (
    get_threshold,
    is_exchange_strategy_role,
    is_lp_strategy_role,
    is_market_maker_strategy_role,
    is_priority_smart_money_strategy_role,
    is_smart_money_strategy_role,
    strategy_role_group,
)
from models import Event, Signal


EXCHANGE_SENSITIVE_INTENTS = {
    "exchange_deposit_candidate",
    "exchange_withdraw_candidate",
    "possible_sell_preparation",
    "possible_buy_preparation",
}
LP_INTENTS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
    "liquidity_addition",
    "liquidity_removal",
    "pool_rebalance",
    "pool_noise",
}
PRIMARY_LP_INTENTS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
}
LP_PREALERT_INTENTS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
    "liquidity_addition",
    "liquidity_removal",
}
SMART_MONEY_ALLOWED_REASON_WHITELIST = sorted(
    [
        "market_maker_execution_observe",
        "market_maker_execution_primary",
        "smart_money_execution_observe",
        "smart_money_execution_primary",
        "smart_money_continuous_execution_primary",
    ]
)
class StrategyEngine:
    """
    Strategy Layer：
    - 先判断事件是否值得生成统一 signal
    - 再由单一路由函数把 signal 分成 primary / observe / drop
    """

    def __init__(
        self,
        min_confidence: float = MIN_CONFIDENCE,
        min_signal_usd: float = MIN_SIGNAL_USD,
        min_address_score: float = MIN_ADDRESS_SCORE,
        min_token_score: float = MIN_TOKEN_SCORE,
        min_behavior_confidence: float = MIN_BEHAVIOR_CONFIDENCE,
        require_non_normal_behavior: bool = STRATEGY_REQUIRE_NON_NORMAL_BEHAVIOR,
    ) -> None:
        self.min_confidence = float(min_confidence)
        self.min_signal_usd = float(min_signal_usd)
        self.min_address_score = float(min_address_score)
        self.min_token_score = float(min_token_score)
        self.min_behavior_confidence = float(min_behavior_confidence)
        self.require_non_normal_behavior = bool(require_non_normal_behavior)

    def decide(
        self,
        event: Event,
        watch_meta: dict,
        behavior: dict,
        address_score: dict,
        token_score: dict,
        gate_metrics: dict | None = None,
    ) -> Signal | None:
        gate_metrics = gate_metrics or {}
        usd_value = float(event.usd_value or 0.0)
        if usd_value <= 0:
            return None

        cooldown_key = str(gate_metrics.get("cooldown_key") or "")
        if not cooldown_key:
            return None

        role_group = strategy_role_group(watch_meta.get("strategy_role") or event.strategy_role)
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        priority_smart_money = is_priority_smart_money_strategy_role(strategy_role)
        market_maker = is_market_maker_strategy_role(strategy_role)
        behavior_type = str(behavior.get("behavior_type") or "normal")
        behavior_conf = float(behavior.get("confidence") or 0.0)
        address_score_value = float(address_score.get("score") or 0.0)
        token_score_value = float(token_score.get("score") or token_score.get("token_quality_score") or 0.0)
        intent_type = str(event.intent_type or "unknown_intent")
        intent_confidence = float(event.intent_confidence or 0.0)
        intent_stage = str(event.intent_stage or gate_metrics.get("intent_stage") or "preliminary")
        confirmation_score = float(event.confirmation_score or gate_metrics.get("confirmation_score") or 0.0)
        pricing_confidence = float(event.pricing_confidence or 0.0)
        pricing_status = str(event.pricing_status or "unknown")
        relative_address_size = float(gate_metrics.get("relative_address_size") or 1.0)
        raw_quality_score = float(gate_metrics.get("quality_score") or 0.0)
        quality_score = float(gate_metrics.get("adjusted_quality_score") or raw_quality_score or 0.0)
        quality_tier = str(gate_metrics.get("quality_tier") or "Tier 3")
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        multi_address_resonance = bool(gate_metrics.get("multi_address_resonance"))
        exchange_noise_sensitive = bool(gate_metrics.get("exchange_noise_sensitive"))
        is_real_execution = self._is_real_execution(event, intent_type)
        information_level = self._information_level(intent_type, event, gate_metrics, role_group)
        lp_same_pool_continuity = int(gate_metrics.get("lp_same_pool_continuity") or 0)
        lp_multi_pool_resonance = int(gate_metrics.get("lp_multi_pool_resonance") or 0)
        lp_volume_surge_ratio = float(gate_metrics.get("lp_pool_volume_surge_ratio") or 0.0)
        lp_action_intensity = float(gate_metrics.get("lp_action_intensity") or 0.0)
        lp_reserve_skew = float(gate_metrics.get("lp_reserve_skew") or 0.0)
        abnormal_ratio = float(gate_metrics.get("abnormal_ratio") or 0.0)
        lp_observe_exception_applied = bool(gate_metrics.get("lp_observe_exception_applied"))
        lp_observe_exception_reason = str(gate_metrics.get("lp_observe_exception_reason") or "")
        lp_observe_threshold_ratio = float(gate_metrics.get("lp_observe_threshold_ratio") or 0.0)
        lp_observe_below_min_gap = float(gate_metrics.get("lp_observe_below_min_gap") or 0.0)
        lp_prealert_candidate = bool(
            gate_metrics.get("lp_prealert_candidate")
            or event.metadata.get("lp_prealert_candidate")
        )
        lp_prealert_applied = bool(
            gate_metrics.get("lp_prealert_applied")
            or event.metadata.get("lp_prealert_applied")
        )
        lp_fast_exception_applied = bool(gate_metrics.get("lp_fast_exception_applied"))
        lp_fast_exception_reason = str(gate_metrics.get("lp_fast_exception_reason") or "")
        lp_fast_exception_threshold_ratio = float(gate_metrics.get("lp_fast_exception_threshold_ratio") or 0.0)
        lp_fast_exception_usd_gap = float(gate_metrics.get("lp_fast_exception_usd_gap") or 0.0)
        lp_fast_exception_structure_score = float(gate_metrics.get("lp_fast_exception_structure_score") or 0.0)
        lp_fast_exception_gate_version = str(gate_metrics.get("lp_fast_exception_gate_version") or "")
        lp_burst_fastlane_ready = bool(gate_metrics.get("lp_burst_fastlane_ready"))
        lp_burst_fastlane_reason = str(gate_metrics.get("lp_burst_fastlane_reason") or "")
        lp_burst_window_sec = int(gate_metrics.get("lp_burst_window_sec") or 0)
        lp_burst_event_count = int(gate_metrics.get("lp_burst_event_count") or 0)
        lp_burst_total_usd = float(gate_metrics.get("lp_burst_total_usd") or 0.0)
        lp_burst_max_single_usd = float(gate_metrics.get("lp_burst_max_single_usd") or 0.0)
        lp_burst_same_pool_continuity = int(gate_metrics.get("lp_burst_same_pool_continuity") or 0)
        lp_burst_volume_surge_ratio = float(gate_metrics.get("lp_burst_volume_surge_ratio") or 0.0)
        lp_burst_action_intensity = float(gate_metrics.get("lp_burst_action_intensity") or 0.0)
        lp_burst_reserve_skew = float(gate_metrics.get("lp_burst_reserve_skew") or 0.0)
        lp_trend_sensitivity_mode = bool(gate_metrics.get("lp_trend_sensitivity_mode"))
        lp_trend_primary_pool = bool(gate_metrics.get("lp_trend_primary_pool"))
        lp_directional_side = str(gate_metrics.get("lp_directional_side") or "")
        lp_directional_threshold_profile = str(gate_metrics.get("lp_directional_threshold_profile") or "")
        lp_fast_exception_profile_name = str(gate_metrics.get("lp_fast_exception_profile_name") or "")
        lp_fast_exception_structure_passed = bool(gate_metrics.get("lp_fast_exception_structure_passed"))
        lp_burst_trend_mode = bool(gate_metrics.get("lp_burst_trend_mode"))
        lp_burst_event_count_threshold_used = int(gate_metrics.get("lp_burst_event_count_threshold_used") or 0)
        lp_burst_total_usd_threshold_used = float(gate_metrics.get("lp_burst_total_usd_threshold_used") or 0.0)
        lp_burst_trend_profile_name = str(gate_metrics.get("lp_burst_trend_profile_name") or "")
        smart_money_non_exec_exception_applied = bool(gate_metrics.get("smart_money_non_exec_exception_applied"))
        smart_money_non_exec_exception_reason = str(gate_metrics.get("smart_money_non_exec_exception_reason") or "")
        market_maker_observe_exception_applied = bool(gate_metrics.get("market_maker_observe_exception_applied"))
        market_maker_observe_exception_reason = str(gate_metrics.get("market_maker_observe_exception_reason") or "")
        market_maker_threshold_ratio = float(gate_metrics.get("market_maker_threshold_ratio") or 0.0)
        market_maker_quality_gap = float(gate_metrics.get("market_maker_quality_gap") or 0.0)
        is_lp_below_min_usd_exception_candidate = self._allow_lp_below_min_usd_observe_exception(
            role_group=role_group,
            intent_type=intent_type,
            lp_observe_exception_applied=lp_observe_exception_applied,
            lp_prealert_applied=lp_prealert_applied,
        )

        if pricing_status in {"unknown", "unavailable"} and pricing_confidence < 0.35:
            return None

        if self.require_non_normal_behavior and behavior_type == "normal" and intent_type in {"pure_transfer", "unknown_intent"}:
            return None

        if intent_type == "pool_noise":
            return None

        if role_group == "exchange":
            if (
                not is_real_execution
                and intent_type in {"pure_transfer", "unknown_intent"}
                and confirmation_score < 0.42
                and resonance_score < 0.30
                and quality_score < 0.82
            ):
                return None
            if (
                intent_type in EXCHANGE_SENSITIVE_INTENTS
                and intent_stage == "weak"
                and confirmation_score < 0.35
                and resonance_score < 0.24
                and quality_score < 0.80
            ):
                return None

        if priority_smart_money and is_real_execution:
            min_behavior_conf = self.min_behavior_confidence * 0.80
            min_address_score = self.min_address_score * 0.80
            min_token_score = self.min_token_score * 0.88
        elif market_maker and is_real_execution:
            min_behavior_conf = self.min_behavior_confidence * 0.95
            min_address_score = self.min_address_score * 0.96
            min_token_score = self.min_token_score * 0.94
        else:
            min_behavior_conf = self.min_behavior_confidence
            min_address_score = self.min_address_score
            min_token_score = self.min_token_score

        if (
            behavior_conf < min_behavior_conf
            and intent_confidence < 0.58
            and confirmation_score < 0.5
            and quality_score < 0.78
            and not market_maker_observe_exception_applied
        ):
            return None

        if (
            address_score_value < min_address_score
            and quality_score < 0.82
            and relative_address_size < 2.4
            and not (priority_smart_money and is_real_execution)
            and not market_maker_observe_exception_applied
        ):
            return None

        if (
            token_score_value < min_token_score
            and quality_score < 0.8
            and pricing_confidence < 0.75
            and not is_real_execution
            and role_group != "lp_pool"
            and not market_maker_observe_exception_applied
        ):
            return None

        if (
            intent_type in {"pure_transfer", "internal_rebalance", "unknown_intent"}
            and confirmation_score < 0.38
            and resonance_score < 0.35
            and quality_score < 0.8
            and role_group not in {"smart_money", "exchange"}
        ):
            return None

        base_threshold = float(get_threshold(watch_meta))
        effective_threshold = self._effective_threshold(
            base_threshold=base_threshold,
            event=event,
            role_group=role_group,
            strategy_role=strategy_role,
            behavior_type=behavior_type,
            address_score=address_score_value,
            token_score=token_score_value,
            intent_type=intent_type,
            intent_stage=intent_stage,
            confirmation_score=confirmation_score,
            resonance_score=resonance_score,
            pricing_status=pricing_status,
            exchange_noise_sensitive=exchange_noise_sensitive,
        )
        gate_min_usd = float(gate_metrics.get("dynamic_min_usd") or 0.0)
        if not is_lp_below_min_usd_exception_candidate:
            effective_threshold = max(effective_threshold, gate_min_usd)
            if usd_value < max(effective_threshold, self.min_signal_usd):
                return None
        else:
            # gate 已经确认这是“金额略低但结构很强”的 LP observe 例外；
            # 这里不再重复用同一金额门槛拦截，但仍保留二次质量控制。
            if pricing_status in {"unknown", "unavailable"} or pricing_confidence < 0.55:
                return None
            if quality_score < 0.60:
                return None
            if (
                confirmation_score < LP_OBSERVE_MIN_CONFIDENCE
                and resonance_score < 0.34
                and lp_same_pool_continuity < 2
                and lp_multi_pool_resonance < 2
                and lp_volume_surge_ratio < LP_VOLUME_SURGE_MIN_RATIO
                and lp_action_intensity < 0.52
            ):
                return None

        signal_type = self._signal_type(event, behavior_type, intent_type)
        confidence = self._confidence(
            behavior_conf=behavior_conf,
            intent_conf=intent_confidence,
            pricing_conf=pricing_confidence,
            confirmation_score=confirmation_score,
            resonance_score=resonance_score,
            address_score=address_score_value,
            token_score=token_score_value,
            relative_address_size=relative_address_size,
            quality_score=quality_score,
            pricing_status=pricing_status,
            intent_type=intent_type,
            intent_stage=intent_stage,
            exchange_noise_sensitive=exchange_noise_sensitive,
            role_group=role_group,
            strategy_role=strategy_role,
            is_real_execution=is_real_execution,
        )
        min_confidence = self.min_confidence
        if role_group == "lp_pool":
            if intent_type in PRIMARY_LP_INTENTS:
                min_confidence *= 0.90
            elif intent_type in LP_INTENTS:
                min_confidence *= 0.94
        if market_maker_observe_exception_applied:
            min_confidence = min(min_confidence, 0.62)
        if confidence < min_confidence:
            return None

        tier = self._tier(
            confidence=confidence,
            address_score=address_score_value,
            token_score=token_score_value,
            intent_confidence=intent_confidence,
            pricing_confidence=pricing_confidence,
            confirmation_score=confirmation_score,
            resonance_score=resonance_score,
            information_level=information_level,
            quality_tier=quality_tier,
            pricing_status=pricing_status,
            intent_stage=intent_stage,
            exchange_noise_sensitive=exchange_noise_sensitive,
        )
        priority = self._priority(
            tier=tier,
            information_level=information_level,
            intent_confidence=intent_confidence,
            pricing_confidence=pricing_confidence,
            confirmation_score=confirmation_score,
            resonance_score=resonance_score,
            intent_stage=intent_stage,
            role_group=role_group,
            is_real_execution=is_real_execution,
        )

        reason = (
            f"intent={intent_type}:{intent_confidence:.2f}/{intent_stage}; "
            f"confirm={confirmation_score:.2f}; resonance={resonance_score:.2f}; "
            f"pricing={pricing_status}:{pricing_confidence:.2f}; "
            f"relative={relative_address_size:.2f}x; conf={confidence:.2f}; tier={tier}"
        )

        return Signal(
            type=signal_type,
            confidence=round(confidence, 3),
            priority=priority,
            tier=tier,
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=round(usd_value, 2),
            reason=reason,
            behavior_type=behavior_type,
            address_score=address_score_value,
            token_score=token_score_value,
            quality_score=quality_score,
            intent_type=intent_type,
            intent_stage=intent_stage,
            confirmation_score=round(confirmation_score, 3),
            information_level=information_level,
            pricing_confidence=round(pricing_confidence, 3),
            cooldown_key=cooldown_key,
            base_token_score=token_score_value,
            token_context_score=float(gate_metrics.get("token_context_score") or token_score_value),
            effective_threshold_usd=round(effective_threshold, 2),
            metadata={
                "base_threshold_usd": round(base_threshold, 2),
                "address_grade": address_score.get("grade"),
                "token_grade": token_score.get("grade"),
                "behavior_reason": behavior.get("reason"),
                "intent_confidence": round(intent_confidence, 3),
                "intent_evidence": list(event.intent_evidence or []),
                "pricing_status": pricing_status,
                "raw_quality_score": round(raw_quality_score, 3),
                "quality_tier": quality_tier,
                "resonance_score": round(resonance_score, 3),
                "exchange_noise_sensitive": exchange_noise_sensitive,
                "role_group": role_group,
                "strategy_role": strategy_role,
                "is_real_execution": is_real_execution,
                "lp_observe_exception_applied": bool(lp_observe_exception_applied),
                "lp_observe_exception_reason": lp_observe_exception_reason,
                "lp_observe_threshold_ratio": round(lp_observe_threshold_ratio, 3),
                "lp_observe_below_min_gap": round(lp_observe_below_min_gap, 2),
                "lp_observe_delivery_cap": "observe_only" if is_lp_below_min_usd_exception_candidate else "",
                "lp_prealert_candidate": bool(lp_prealert_candidate),
                "lp_prealert_applied": bool(lp_prealert_applied),
                "lp_trend_sensitivity_mode": lp_trend_sensitivity_mode,
                "lp_trend_primary_pool": lp_trend_primary_pool,
                "lp_directional_side": lp_directional_side,
                "lp_directional_threshold_profile": lp_directional_threshold_profile,
                "lp_fast_exception_profile_name": lp_fast_exception_profile_name,
                "lp_fast_exception_applied": lp_fast_exception_applied,
                "lp_fast_exception_reason": lp_fast_exception_reason,
                "lp_fast_exception_threshold_ratio": round(lp_fast_exception_threshold_ratio, 3),
                "lp_fast_exception_usd_gap": round(lp_fast_exception_usd_gap, 2),
                "lp_fast_exception_structure_score": round(lp_fast_exception_structure_score, 3),
                "lp_fast_exception_structure_passed": lp_fast_exception_structure_passed,
                "lp_fast_exception_gate_version": lp_fast_exception_gate_version,
                "lp_burst_trend_mode": lp_burst_trend_mode,
                "lp_burst_event_count_threshold_used": lp_burst_event_count_threshold_used,
                "lp_burst_total_usd_threshold_used": round(lp_burst_total_usd_threshold_used, 2),
                "lp_burst_trend_profile_name": lp_burst_trend_profile_name,
                "lp_burst_fastlane_ready": lp_burst_fastlane_ready,
                "lp_burst_fastlane_reason": lp_burst_fastlane_reason,
                "lp_burst_window_sec": lp_burst_window_sec,
                "lp_burst_event_count": lp_burst_event_count,
                "lp_burst_total_usd": round(lp_burst_total_usd, 2),
                "lp_burst_max_single_usd": round(lp_burst_max_single_usd, 2),
                "lp_burst_same_pool_continuity": lp_burst_same_pool_continuity,
                "lp_burst_volume_surge_ratio": round(lp_burst_volume_surge_ratio, 3),
                "lp_burst_action_intensity": round(lp_burst_action_intensity, 3),
                "lp_burst_reserve_skew": round(lp_burst_reserve_skew, 3),
                "smart_money_non_exec_exception_applied": smart_money_non_exec_exception_applied,
                "smart_money_non_exec_exception_reason": smart_money_non_exec_exception_reason,
                "smart_money_non_exec_delivery_cap": "observe_only" if smart_money_non_exec_exception_applied else "",
                "market_maker_observe_exception_applied": market_maker_observe_exception_applied,
                "market_maker_observe_exception_reason": market_maker_observe_exception_reason,
                "market_maker_threshold_ratio": round(market_maker_threshold_ratio, 3),
                "market_maker_quality_gap": round(market_maker_quality_gap, 3),
                "market_maker_delivery_cap": "observe_only" if market_maker_observe_exception_applied else "",
                "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
                "liquidation_score": round(float(event.metadata.get("liquidation_score") or 0.0), 3),
                "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
                "liquidation_protocols": list(event.metadata.get("liquidation_protocols") or []),
                "gate": gate_metrics,
            },
        )

    def classify_delivery(
        self,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        gate_metrics: dict | None = None,
        behavior_case=None,
    ) -> tuple[str, str]:
        gate_metrics = gate_metrics or {}
        role_group = strategy_role_group(watch_meta.get("strategy_role") or event.strategy_role)
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        priority_smart_money = is_priority_smart_money_strategy_role(strategy_role)
        market_maker = is_market_maker_strategy_role(strategy_role)
        intent_type = str(event.intent_type or signal.intent_type or "unknown_intent")
        confirmation_score = float(event.confirmation_score or signal.confirmation_score or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or signal.metadata.get("resonance_score") or 0.0)
        raw_quality_score = float(signal.metadata.get("raw_quality_score") or gate_metrics.get("quality_score") or signal.quality_score or 0.0)
        quality_score = float(signal.quality_score or gate_metrics.get("adjusted_quality_score") or gate_metrics.get("quality_score") or 0.0)
        multi_address_resonance = bool(gate_metrics.get("multi_address_resonance"))
        same_side_addresses = int(gate_metrics.get("same_side_resonance_addresses") or 0)
        same_side_smart_money_addresses = int(gate_metrics.get("same_side_resonance_smart_money_addresses") or 0)
        is_real_execution = self._is_real_execution(event, intent_type)
        case_family = str((getattr(behavior_case, "metadata", {}) or {}).get("case_family") or event.metadata.get("case_family") or "")
        case_stage = str(getattr(behavior_case, "stage", "") or event.followup_stage or "")
        followup_confirmed = bool(event.metadata.get("followup_confirmed"))
        smart_money_case_confirmed = bool(event.metadata.get("smart_money_case_confirmed"))
        smart_money_execution_count = int(event.metadata.get("smart_money_case_execution_count") or 0)
        smart_money_same_actor_continuation = bool(event.metadata.get("smart_money_same_actor_continuation"))
        smart_money_size_expansion_ratio = float(event.metadata.get("smart_money_size_expansion_ratio") or 1.0)
        intent_confirmed = str(event.intent_stage or signal.intent_stage or "") == "confirmed"
        lp_volume_surge_ratio = float(gate_metrics.get("lp_pool_volume_surge_ratio") or event.metadata.get("lp_analysis", {}).get("pool_volume_surge_ratio") or 0.0)
        lp_same_pool_continuity = int(gate_metrics.get("lp_same_pool_continuity") or event.metadata.get("lp_analysis", {}).get("same_pool_continuity") or 0)
        lp_multi_pool_resonance = int(gate_metrics.get("lp_multi_pool_resonance") or event.metadata.get("lp_analysis", {}).get("multi_pool_resonance") or 0)
        lp_observe_exception_applied = bool(
            signal.metadata.get("lp_observe_exception_applied")
            or gate_metrics.get("lp_observe_exception_applied")
        )
        lp_prealert_applied = bool(
            signal.metadata.get("lp_prealert_applied")
            or gate_metrics.get("lp_prealert_applied")
            or event.metadata.get("lp_prealert_applied")
        )
        lp_observe_exception_reason = str(
            signal.metadata.get("lp_observe_exception_reason")
            or gate_metrics.get("lp_observe_exception_reason")
            or ""
        )
        lp_burst_fastlane_ready = bool(
            gate_metrics.get("lp_burst_fastlane_ready")
            or signal.metadata.get("lp_burst_fastlane_ready")
            or event.metadata.get("lp_burst_fastlane_ready")
        )
        lp_burst_event_count = int(
            gate_metrics.get("lp_burst_event_count")
            or signal.metadata.get("lp_burst_event_count")
            or event.metadata.get("lp_burst_event_count")
            or 0
        )
        lp_burst_total_usd = float(
            gate_metrics.get("lp_burst_total_usd")
            or signal.metadata.get("lp_burst_total_usd")
            or event.metadata.get("lp_burst_total_usd")
            or 0.0
        )
        lp_burst_max_single_usd = float(
            gate_metrics.get("lp_burst_max_single_usd")
            or signal.metadata.get("lp_burst_max_single_usd")
            or event.metadata.get("lp_burst_max_single_usd")
            or 0.0
        )
        lp_burst_same_pool_continuity = int(
            gate_metrics.get("lp_burst_same_pool_continuity")
            or signal.metadata.get("lp_burst_same_pool_continuity")
            or event.metadata.get("lp_burst_same_pool_continuity")
            or 0
        )
        lp_burst_volume_surge_ratio = float(
            gate_metrics.get("lp_burst_volume_surge_ratio")
            or signal.metadata.get("lp_burst_volume_surge_ratio")
            or event.metadata.get("lp_burst_volume_surge_ratio")
            or 0.0
        )
        lp_burst_action_intensity = float(
            gate_metrics.get("lp_burst_action_intensity")
            or signal.metadata.get("lp_burst_action_intensity")
            or event.metadata.get("lp_burst_action_intensity")
            or 0.0
        )
        lp_burst_reserve_skew = float(
            gate_metrics.get("lp_burst_reserve_skew")
            or signal.metadata.get("lp_burst_reserve_skew")
            or event.metadata.get("lp_burst_reserve_skew")
            or 0.0
        )
        lp_trend_primary_pool = bool(
            gate_metrics.get("lp_trend_primary_pool")
            or signal.metadata.get("lp_trend_primary_pool")
            or event.metadata.get("lp_trend_primary_pool")
        )
        liquidation_stage = str(event.metadata.get("liquidation_stage") or signal.metadata.get("liquidation_stage") or "none")
        liquidation_score = float(event.metadata.get("liquidation_score") or signal.metadata.get("liquidation_score") or 0.0)
        liquidation_protocols = list(event.metadata.get("liquidation_protocols") or signal.metadata.get("liquidation_protocols") or [])
        liquidation_primary_candidate = bool(
            event.metadata.get("liquidation_primary_candidate")
            or signal.metadata.get("liquidation_primary_candidate")
        )
        liquidation_case_confirmed = bool(event.metadata.get("liquidation_case_execution_hits") or event.metadata.get("liquidation_case_confirmed"))
        is_liquidation_protocol_related = bool(
            gate_metrics.get("is_liquidation_protocol_related")
            or (event.metadata.get("raw") or {}).get("is_liquidation_protocol_related")
        )
        possible_keeper_executor = bool(gate_metrics.get("possible_keeper_executor"))
        possible_vault_or_auction = bool(gate_metrics.get("possible_vault_or_auction"))
        if case_family == "downstream_counterparty_followup":
            if not bool(event.metadata.get("downstream_followup_anchor_event")) and not bool(event.metadata.get("downstream_followup_confirmed_event")) and case_stage == "followup_opened":
                return self._apply_delivery(
                    event,
                    signal,
                    "drop",
                    "downstream_followup_dropped_as_small_noise",
                    fact_type="downstream_followup",
                )
            if case_stage in {"swap_execution_confirmed", "exchange_arrival_confirmed"}:
                if (
                    confirmation_score >= 0.78
                    and quality_score >= 0.80
                    and is_real_execution
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "downstream_followup_primary",
                        fact_type="downstream_followup",
                    )
                return self._apply_delivery(
                    event,
                    signal,
                    "observe",
                    "downstream_followup_observe",
                    fact_type="downstream_followup",
                )
            return self._apply_delivery(
                event,
                signal,
                "observe",
                "downstream_followup_observe",
                fact_type="downstream_followup",
            )

        if followup_confirmed or case_stage == "followup_confirmed":
            if (
                is_real_execution
                and confirmation_score >= 0.82
                and quality_score >= 0.82
                and (
                    resonance_score >= 0.60
                    or multi_address_resonance
                    or same_side_addresses >= 3
                )
            ):
                return self._apply_delivery(
                    event,
                    signal,
                    "primary",
                    "exchange_followup_execution_primary",
                    fact_type="followup_confirmed",
                )
            return self._apply_delivery(
                event,
                signal,
                "observe",
                "exchange_followup_observe",
                fact_type="followup_confirmed",
            )

        if role_group == "lp_pool":
            if liquidation_stage == "execution":
                if (
                    DELIVERY_ALLOW_LIQUIDATION_EXECUTION_PRIMARY
                    and liquidation_primary_candidate
                    and liquidation_score >= LIQUIDATION_EXECUTION_MIN_SCORE
                    and float(event.usd_value or 0.0) >= LIQUIDATION_PRIMARY_MIN_USD
                    and confirmation_score >= max(LP_PRIMARY_MIN_CONFIDENCE, 0.76)
                    and quality_score >= 0.78
                    and liquidation_protocols
                    and is_liquidation_protocol_related
                    and (possible_keeper_executor or possible_vault_or_auction or liquidation_case_confirmed)
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "liquidation_execution_primary",
                        fact_type="liquidation_execution",
                    )
                return self._apply_delivery(
                    event,
                    signal,
                    "observe",
                    "liquidation_execution_observe",
                    fact_type="liquidation_execution",
                )

            if liquidation_stage == "risk":
                if DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE and (
                    liquidation_score >= LIQUIDATION_RISK_MIN_SCORE
                    or is_liquidation_protocol_related
                    or lp_same_pool_continuity >= 2
                    or lp_multi_pool_resonance >= 2
                    or lp_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "liquidation_risk_observe",
                        fact_type="liquidation_risk",
                    )

            if intent_type in PRIMARY_LP_INTENTS:
                if self._allow_lp_prealert_observe(
                    event=event,
                    lp_trend_primary_pool=lp_trend_primary_pool,
                    lp_prealert_applied=lp_prealert_applied,
                    pricing_confidence=pricing_confidence,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_directional_prealert_observe",
                    )
                if lp_burst_fastlane_ready and self._allow_lp_burst_directional_primary(
                    confirmation_score=confirmation_score,
                    quality_score=quality_score,
                    lp_burst_event_count=lp_burst_event_count,
                    lp_burst_total_usd=lp_burst_total_usd,
                    lp_burst_volume_surge_ratio=lp_burst_volume_surge_ratio,
                    lp_burst_action_intensity=lp_burst_action_intensity,
                    lp_burst_reserve_skew=lp_burst_reserve_skew,
                    lp_trend_primary_pool=lp_trend_primary_pool,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "lp_burst_directional_primary",
                    )
                if lp_burst_fastlane_ready:
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_burst_directional_observe",
                    )
                if self._allow_lp_first_hit_directional_primary_direct(
                    event=event,
                    confirmation_score=confirmation_score,
                    quality_score=quality_score,
                    pricing_confidence=pricing_confidence,
                    lp_action_intensity=lp_action_intensity,
                    lp_reserve_skew=lp_reserve_skew,
                    lp_volume_surge_ratio=lp_volume_surge_ratio,
                    abnormal_ratio=abnormal_ratio,
                    lp_same_pool_continuity=lp_same_pool_continuity,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "lp_first_hit_directional_primary_direct",
                    )
                if self._allow_lp_first_hit_directional_primary(
                    event=event,
                    confirmation_score=confirmation_score,
                    quality_score=quality_score,
                    pricing_confidence=pricing_confidence,
                    lp_action_intensity=lp_action_intensity,
                    lp_reserve_skew=lp_reserve_skew,
                    lp_volume_surge_ratio=lp_volume_surge_ratio,
                    abnormal_ratio=abnormal_ratio,
                    lp_same_pool_continuity=lp_same_pool_continuity,
                    lp_observe_exception_applied=lp_observe_exception_applied,
                    lp_observe_exception_reason=lp_observe_exception_reason,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "lp_first_hit_directional_primary",
                    )
                if lp_observe_exception_applied:
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_observe_exception_capped",
                    )
                if (
                    not lp_observe_exception_applied
                    and
                    confirmation_score >= LP_PRIMARY_MIN_CONFIDENCE
                    and (
                        lp_volume_surge_ratio >= LP_PRIMARY_MIN_SURGE_RATIO
                        or lp_same_pool_continuity >= 2
                        or lp_multi_pool_resonance >= 2
                        or quality_score >= 0.80
                    )
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "lp_directional_pressure_primary",
                    )
                if self._allow_lp_directional_early_observe(
                    event=event,
                    pricing_confidence=pricing_confidence,
                    lp_action_intensity=lp_action_intensity,
                    lp_reserve_skew=lp_reserve_skew,
                    lp_volume_surge_ratio=lp_volume_surge_ratio,
                    abnormal_ratio=abnormal_ratio,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_directional_early_observe",
                    )
                if (
                    confirmation_score >= LP_OBSERVE_MIN_CONFIDENCE
                    or lp_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO
                    or lp_same_pool_continuity >= 2
                    or lp_multi_pool_resonance >= 2
                    or float(event.usd_value or 0.0) >= LP_OBSERVE_MIN_USD
                ):
                    return self._apply_delivery(event, signal, "observe", "lp_directional_pressure_observe")
                return self._apply_delivery(event, signal, "drop", "lp_directional_pressure_drop")

            if intent_type in LP_INTENTS:
                if intent_type == "pool_noise":
                    return self._apply_delivery(event, signal, "drop", "lp_noise_drop")
                if self._allow_lp_prealert_observe(
                    event=event,
                    lp_trend_primary_pool=lp_trend_primary_pool,
                    lp_prealert_applied=lp_prealert_applied,
                    pricing_confidence=pricing_confidence,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_liquidity_prealert_observe",
                    )
                if self._allow_lp_non_directional_structured_observe(
                    event=event,
                    confirmation_score=confirmation_score,
                    quality_score=quality_score,
                    pricing_confidence=pricing_confidence,
                    lp_volume_surge_ratio=lp_volume_surge_ratio,
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "observe",
                        "lp_non_directional_structured_observe",
                    )
                if (
                    float(event.usd_value or 0.0) >= max(LP_OBSERVE_MIN_USD * 1.10, 22_000.0)
                    or lp_same_pool_continuity >= 2
                    or lp_volume_surge_ratio >= max(LP_VOLUME_SURGE_MIN_RATIO, 1.75)
                    or confirmation_score >= max(LP_OBSERVE_MIN_CONFIDENCE + 0.06, 0.64)
                ):
                    return self._apply_delivery(event, signal, "observe", "lp_non_directional_observe")
                if DELIVERY_LP_WEAK_SIGNAL_ARCHIVE_ONLY:
                    return self._apply_delivery(event, signal, "drop", "lp_non_directional_drop")
                return self._apply_delivery(event, signal, "drop", "lp_non_directional_drop")

        if role_group == "exchange":
            if bool(event.metadata.get("exchange_followup_anchor_event")) or case_stage == "anchor_tracking":
                if (
                    DELIVERY_ALLOW_EXCHANGE_ANCHOR_PRIMARY
                    and confirmation_score >= 0.84
                    and (multi_address_resonance or resonance_score >= 0.68)
                ):
                    return self._apply_delivery(event, signal, "primary", "exchange_anchor_exception_primary")
                return self._apply_delivery(event, signal, "observe", "exchange_anchor_observe")

            if is_real_execution:
                if (
                    intent_confirmed
                    and confirmation_score >= 0.88
                    and resonance_score >= 0.68
                    and quality_score >= 0.88
                    and (multi_address_resonance or same_side_addresses >= 3)
                ):
                    return self._apply_delivery(event, signal, "primary", "exchange_execution_primary")
                return self._apply_delivery(event, signal, "observe", "exchange_execution_observe")

            if intent_type in EXCHANGE_SENSITIVE_INTENTS:
                return self._apply_delivery(event, signal, "observe", "exchange_directional_observe")

            if intent_type in {"internal_rebalance", "market_making_inventory_move"}:
                if quality_score >= 0.82 or confirmation_score >= 0.58:
                    return self._apply_delivery(event, signal, "observe", "exchange_inventory_observe")
                return self._apply_delivery(event, signal, "drop", "exchange_inventory_drop")

            if intent_type in {"pure_transfer", "unknown_intent"}:
                if quality_score >= 0.84 and same_side_addresses >= 3 and resonance_score >= 0.6:
                    return self._apply_delivery(event, signal, "observe", "exchange_transfer_observe")
                return self._apply_delivery(event, signal, "drop", "exchange_transfer_drop")

            return self._apply_delivery(event, signal, "observe", "exchange_observe")

        if role_group == "smart_money":
            if is_real_execution:
                if market_maker:
                    strict_market_maker_primary = bool(MARKET_MAKER_PRIMARY_STRICT)
                    if (
                        smart_money_case_confirmed
                        and intent_confirmed
                        and confirmation_score >= (0.76 if strict_market_maker_primary else 0.72)
                        and quality_score >= (0.84 if strict_market_maker_primary else 0.82)
                    ) or (
                        confirmation_score >= (0.86 if strict_market_maker_primary else 0.82)
                        and quality_score >= (0.88 if strict_market_maker_primary else 0.84)
                        and resonance_score >= (0.54 if strict_market_maker_primary else 0.50)
                        and smart_money_execution_count >= 2
                    ):
                        return self._apply_delivery(
                            event,
                            signal,
                            "primary",
                            "market_maker_execution_primary",
                        )
                    if (
                        confirmation_score >= MARKET_MAKER_OBSERVE_MIN_CONFIRMATION
                        or resonance_score >= MARKET_MAKER_OBSERVE_MIN_RESONANCE
                        or quality_score >= max(MARKET_MAKER_OBSERVE_GATE_FLOOR + 0.10, 0.69)
                        or smart_money_case_confirmed
                    ):
                        return self._apply_delivery(event, signal, "observe", "market_maker_execution_observe")
                    return self._apply_delivery(event, signal, "observe", "market_maker_execution_observe")

                if (
                    smart_money_case_confirmed
                    or case_stage == "execution_followup_confirmed"
                    or (
                        priority_smart_money
                        and smart_money_execution_count >= 2
                        and (smart_money_same_actor_continuation or smart_money_size_expansion_ratio >= 1.22)
                    )
                ):
                    return self._apply_delivery(
                        event,
                        signal,
                        "primary",
                        "smart_money_continuous_execution_primary",
                    )
                if DELIVERY_SMART_MONEY_EXECUTION_PRIMARY and priority_smart_money and (
                    confirmation_score >= 0.52
                    or quality_score >= 0.76
                    or smart_money_size_expansion_ratio >= 1.18
                ):
                    return self._apply_delivery(event, signal, "primary", "smart_money_execution_primary")
                if DELIVERY_SMART_MONEY_EXECUTION_PRIMARY and not priority_smart_money and (
                    confirmation_score >= 0.62
                    and (quality_score >= 0.78 or resonance_score >= 0.40 or same_side_smart_money_addresses >= 1)
                ):
                    return self._apply_delivery(event, signal, "primary", "smart_money_execution_primary")
                return self._apply_delivery(event, signal, "observe", "smart_money_execution_observe")
            archive_reason = (
                "market_maker_non_execution_archived"
                if market_maker else
                "smart_money_non_execution_archived"
            )
            return self._drop_smart_money_non_execution(
                event,
                signal,
                market_maker=market_maker,
                reason=archive_reason,
            )

        if is_real_execution:
            if confirmation_score >= 0.78 and (
                multi_address_resonance or resonance_score >= 0.56 or quality_score >= 0.82
            ):
                return self._apply_delivery(event, signal, "primary", "real_execution_primary")
            return self._apply_delivery(event, signal, "observe", "real_execution_observe")

        if intent_type in {"pure_transfer", "unknown_intent", "pool_noise"}:
            return self._apply_delivery(event, signal, "drop", "weak_fact_drop")

        if confirmation_score >= 0.60 or quality_score >= 0.80:
            return self._apply_delivery(event, signal, "observe", "directional_observe")
        return self._apply_delivery(event, signal, "drop", "low_trade_value_drop")

    def _allow_lp_first_hit_directional_primary(
        self,
        event: Event,
        confirmation_score: float,
        quality_score: float,
        pricing_confidence: float,
        lp_action_intensity: float,
        lp_reserve_skew: float,
        lp_volume_surge_ratio: float,
        abnormal_ratio: float,
        lp_same_pool_continuity: int,
        lp_observe_exception_applied: bool,
        lp_observe_exception_reason: str,
    ) -> bool:
        if not bool(LP_FIRST_HIT_PRIMARY_ENABLE):
            return False
        if not lp_observe_exception_applied:
            return False
        if str(lp_observe_exception_reason or "") not in {
            "",
            "lp_fast_exception_structured_directional",
            "lp_first_hit_strong_directional_exception",
        }:
            return False
        if float(event.usd_value or 0.0) < LP_FIRST_HIT_PRIMARY_MIN_USD:
            return False
        if confirmation_score < LP_FIRST_HIT_PRIMARY_MIN_CONFIDENCE:
            return False
        if quality_score < LP_FIRST_HIT_PRIMARY_MIN_QUALITY:
            return False
        if pricing_confidence < LP_FIRST_HIT_PRIMARY_MIN_PRICING_CONFIDENCE:
            return False
        if lp_action_intensity < LP_FIRST_HIT_PRIMARY_MIN_ACTION_INTENSITY:
            return False
        if lp_reserve_skew < LP_FIRST_HIT_PRIMARY_MIN_RESERVE_SKEW:
            return False
        if lp_same_pool_continuity > 1:
            return False
        if (
            lp_volume_surge_ratio < LP_FIRST_HIT_PRIMARY_MIN_SURGE_RATIO
            and abnormal_ratio < LP_FIRST_HIT_PRIMARY_MIN_ABNORMAL_RATIO
        ):
            return False
        return True

    def _allow_lp_first_hit_directional_primary_direct(
        self,
        event: Event,
        confirmation_score: float,
        quality_score: float,
        pricing_confidence: float,
        lp_action_intensity: float,
        lp_reserve_skew: float,
        lp_volume_surge_ratio: float,
        abnormal_ratio: float,
        lp_same_pool_continuity: int,
    ) -> bool:
        if not bool(LP_FIRST_HIT_PRIMARY_DIRECT_ENABLE):
            return False
        if str(event.intent_type or "") not in PRIMARY_LP_INTENTS:
            return False
        if lp_same_pool_continuity > 1:
            return False
        if float(event.usd_value or 0.0) < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_USD:
            return False
        if confirmation_score < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_CONFIDENCE:
            return False
        if quality_score < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_QUALITY:
            return False
        if pricing_confidence < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_PRICING_CONFIDENCE:
            return False
        if lp_action_intensity < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_ACTION_INTENSITY:
            return False
        if lp_reserve_skew < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_RESERVE_SKEW:
            return False
        if (
            lp_volume_surge_ratio < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_SURGE_RATIO
            and abnormal_ratio < LP_FIRST_HIT_PRIMARY_DIRECT_MIN_ABNORMAL_RATIO
        ):
            return False
        return True

    def _allow_lp_burst_directional_primary(
        self,
        *,
        confirmation_score: float,
        quality_score: float,
        lp_burst_event_count: int,
        lp_burst_total_usd: float,
        lp_burst_volume_surge_ratio: float,
        lp_burst_action_intensity: float,
        lp_burst_reserve_skew: float,
        lp_trend_primary_pool: bool,
    ) -> bool:
        min_event_count = LP_TREND_BURST_PRIMARY_MIN_EVENT_COUNT if lp_trend_primary_pool else LP_BURST_PRIMARY_MIN_EVENT_COUNT
        min_total_usd = LP_TREND_BURST_PRIMARY_MIN_TOTAL_USD if lp_trend_primary_pool else LP_BURST_PRIMARY_MIN_TOTAL_USD
        min_volume_surge_ratio = (
            LP_TREND_BURST_PRIMARY_MIN_VOLUME_SURGE_RATIO
            if lp_trend_primary_pool else LP_BURST_PRIMARY_MIN_VOLUME_SURGE_RATIO
        )
        min_action_intensity = (
            LP_TREND_BURST_PRIMARY_MIN_ACTION_INTENSITY
            if lp_trend_primary_pool else LP_BURST_PRIMARY_MIN_ACTION_INTENSITY
        )
        if lp_burst_event_count < min_event_count:
            return False
        if lp_burst_total_usd < min_total_usd:
            return False
        if confirmation_score < LP_BURST_PRIMARY_MIN_CONFIRMATION:
            return False
        if quality_score < LP_BURST_PRIMARY_MIN_QUALITY:
            return False
        if lp_burst_volume_surge_ratio < min_volume_surge_ratio:
            return False
        if lp_burst_action_intensity < min_action_intensity:
            return False
        if lp_burst_reserve_skew < 0.99:
            return False
        return True

    def _allow_lp_directional_early_observe(
        self,
        event: Event,
        pricing_confidence: float,
        lp_action_intensity: float,
        lp_reserve_skew: float,
        lp_volume_surge_ratio: float,
        abnormal_ratio: float,
    ) -> bool:
        if str(event.intent_type or "") not in PRIMARY_LP_INTENTS:
            return False
        if float(event.usd_value or 0.0) < 18_000.0:
            return False
        if pricing_confidence < 0.75:
            return False
        matched_signals = sum(
            1
            for matched in (
                lp_action_intensity >= 0.58,
                lp_reserve_skew >= 0.16,
                lp_volume_surge_ratio >= 1.25,
                abnormal_ratio >= 1.60,
            )
            if matched
        )
        return matched_signals >= 2

    def _allow_lp_prealert_observe(
        self,
        *,
        event: Event,
        lp_trend_primary_pool: bool,
        lp_prealert_applied: bool,
        pricing_confidence: float,
    ) -> bool:
        if not lp_trend_primary_pool or not lp_prealert_applied:
            return False
        if str(event.intent_type or "") not in LP_PREALERT_INTENTS:
            return False
        return pricing_confidence >= LP_PREALERT_MIN_PRICING_CONFIDENCE

    def _allow_lp_below_min_usd_observe_exception(
        self,
        *,
        role_group: str,
        intent_type: str,
        lp_observe_exception_applied: bool,
        lp_prealert_applied: bool,
    ) -> bool:
        if role_group != "lp_pool":
            return False
        if lp_observe_exception_applied and intent_type in PRIMARY_LP_INTENTS:
            return True
        if lp_prealert_applied and intent_type in LP_PREALERT_INTENTS:
            return True
        return False

    def _allow_lp_non_directional_structured_observe(
        self,
        event: Event,
        confirmation_score: float,
        quality_score: float,
        pricing_confidence: float,
        lp_volume_surge_ratio: float,
    ) -> bool:
        if str(event.intent_type or "") not in {"liquidity_addition", "liquidity_removal", "pool_rebalance"}:
            return False
        if float(event.usd_value or 0.0) < 18_000.0:
            return False
        if pricing_confidence < 0.75:
            return False
        if quality_score < 0.72:
            return False
        return confirmation_score >= 0.58 or lp_volume_surge_ratio >= 1.35

    def _apply_delivery(
        self,
        event: Event,
        signal: Signal,
        delivery_class: str,
        reason: str,
        fact_type: str | None = None,
    ) -> tuple[str, str]:
        event.delivery_class = delivery_class
        event.delivery_reason = reason
        signal.delivery_class = delivery_class
        signal.delivery_reason = reason
        payload = {
            "delivery_class": delivery_class,
            "delivery_reason": reason,
            "delivery_fact_type": fact_type or self._delivery_fact_type(event, signal),
        }
        message_variant = self._message_variant_for_delivery(
            event=event,
            signal=signal,
            delivery_class=delivery_class,
            reason=reason,
        )
        if message_variant:
            payload["message_variant"] = message_variant
        lp_route_family, lp_route_priority_source, lp_route_semantics = self._lp_route_metadata(
            event=event,
            reason=reason,
        )
        if lp_route_family or lp_route_priority_source or lp_route_semantics:
            payload.update({
                "lp_route_family": lp_route_family,
                "lp_route_priority_source": lp_route_priority_source,
                "lp_route_semantics": lp_route_semantics,
            })
        if strategy_role_group(event.strategy_role) == "smart_money":
            payload["smart_money_legacy_non_exec_branch_disabled"] = True
        if reason.startswith("lp_burst_directional_"):
            payload.update({
                "lp_burst_fastlane_applied": True,
                "lp_burst_fastlane_reason": reason,
                "lp_burst_delivery_class": delivery_class,
                "lp_fastlane_applied": True,
            })
        if str(event.strategy_role or "") == "lp_pool":
            payload.update({
                "lp_stage_decision": reason,
                "lp_reject_reason": reason if delivery_class == "drop" else "",
                "lp_prealert_applied": reason in {"lp_directional_prealert_observe", "lp_liquidity_prealert_observe"},
                "lp_fastlane_applied": bool(payload.get("lp_fastlane_applied")),
            })
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)
        return delivery_class, reason

    def _lp_route_metadata(
        self,
        *,
        event: Event,
        reason: str,
    ) -> tuple[str, str, str]:
        if str(event.strategy_role or "") != "lp_pool":
            return "", "", ""
        normalized_reason = str(reason or "")
        if normalized_reason.startswith("lp_burst_directional_"):
            return "burst_fastlane", "burst_preferred", "burst_main_entry"
        if normalized_reason in {
            "lp_first_hit_directional_primary",
            "lp_first_hit_directional_primary_direct",
        }:
            return "first_hit_strict", "first_hit_fallback", "single_shot_fallback"
        if normalized_reason == "lp_observe_exception_capped":
            return "directional_exception", "legacy_route", "directional_exception_entry"
        if normalized_reason in {
            "lp_directional_prealert_observe",
            "lp_liquidity_prealert_observe",
            "lp_directional_early_observe",
            "lp_directional_pressure_primary",
            "lp_directional_pressure_observe",
            "lp_directional_pressure_drop",
        }:
            if normalized_reason in {"lp_directional_prealert_observe", "lp_liquidity_prealert_observe"}:
                return "prealert_entry", "primary_trend_pool", "prealert_entry"
            return "directional_standard", "legacy_route", "directional_standard_entry"
        return "", "", ""

    def _apply_execution_only_archive_metadata(
        self,
        event: Event,
        signal: Signal,
        *,
        market_maker: bool,
        reason: str,
    ) -> None:
        payload = {
            "smart_money_execution_only_mode": bool(SMART_MONEY_NOTIFY_EXECUTION_ONLY),
            "market_maker_execution_only_mode": bool(MARKET_MAKER_NOTIFY_EXECUTION_ONLY),
            "execution_required_but_missing": True,
            "execution_only_archive_reason": str(reason or ""),
            "smart_money_legacy_non_exec_branch_disabled": True,
            "smart_money_delivery_policy_mode": "execution_whitelist_only",
            "smart_money_delivery_policy_hard_whitelist_applied": True,
            "smart_money_allowed_reason_whitelist": list(SMART_MONEY_ALLOWED_REASON_WHITELIST),
        }
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)

    def _drop_smart_money_non_execution(
        self,
        event: Event,
        signal: Signal,
        *,
        market_maker: bool,
        reason: str,
    ) -> tuple[str, str]:
        self._apply_execution_only_archive_metadata(
            event,
            signal,
            market_maker=market_maker,
            reason=reason,
        )
        return self._apply_delivery(event, signal, "drop", reason)

    def _message_variant_for_delivery(
        self,
        event: Event,
        signal: Signal,
        delivery_class: str,
        reason: str,
    ) -> str:
        del event
        if reason in {
            "smart_money_execution_primary",
            "smart_money_continuous_execution_primary",
            "market_maker_execution_primary",
        }:
            return "smart_money_primary"
        if reason == "market_maker_execution_observe" and delivery_class == "observe":
            return "market_maker_observe"
        if reason == "smart_money_execution_observe" and delivery_class == "observe":
            return "smart_money_observe"
        return ""

    def _delivery_fact_type(self, event: Event, signal: Signal) -> str:
        if str(event.metadata.get("case_family") or "") == "downstream_counterparty_followup":
            return "downstream_followup"
        if bool(event.metadata.get("followup_confirmed")):
            return "followup_confirmed"
        if str(event.metadata.get("liquidation_stage") or "") == "execution":
            return "liquidation_execution"
        if str(event.metadata.get("liquidation_stage") or "") == "risk":
            return "liquidation_risk"
        if self._is_real_execution(event, event.intent_type):
            return "swap_execution"
        if str(event.intent_type or "") in PRIMARY_LP_INTENTS:
            return str(event.intent_type or "")
        return str(signal.type or event.intent_type or "unknown")

    def _effective_threshold(
        self,
        base_threshold: float,
        event: Event,
        role_group: str,
        strategy_role: str,
        behavior_type: str,
        address_score: float,
        token_score: float,
        intent_type: str,
        intent_stage: str,
        confirmation_score: float,
        resonance_score: float,
        pricing_status: str,
        exchange_noise_sensitive: bool,
    ) -> float:
        factor = 1.0

        if address_score >= 82:
            factor *= 0.82
        elif address_score < 45:
            factor *= 1.18

        if token_score >= 80:
            factor *= 0.92
        elif token_score < 45:
            factor *= 1.10

        if behavior_type == "accumulation":
            factor *= 0.92
        elif behavior_type == "distribution":
            factor *= 0.96
        elif behavior_type == "scalping":
            factor *= 1.12
        elif behavior_type == "inventory_management":
            factor *= 1.05

        intent_factor = {
            "swap_execution": 0.92,
            "exchange_deposit_candidate": 1.00,
            "exchange_withdraw_candidate": 0.98,
            "possible_sell_preparation": 1.02,
            "possible_buy_preparation": 1.00,
            "pool_buy_pressure": 0.94,
            "pool_sell_pressure": 0.94,
            "liquidity_addition": 1.00,
            "liquidity_removal": 0.96,
            "pool_rebalance": 1.04,
            "pool_noise": 1.18,
            "internal_rebalance": 1.10,
            "market_making_inventory_move": 1.06,
            "pure_transfer": 1.08,
            "unknown_intent": 1.14,
        }
        factor *= intent_factor.get(intent_type, 1.0)

        if event.kind == "swap":
            factor *= 0.97

        if intent_stage == "confirmed":
            factor *= 0.96
        elif intent_stage == "weak":
            factor *= 1.04

        if confirmation_score >= 0.8:
            factor *= 0.97
        elif confirmation_score < 0.45:
            factor *= 1.04

        if resonance_score >= 0.65:
            factor *= 0.96
        elif resonance_score < 0.25:
            factor *= 1.02

        if is_priority_smart_money_strategy_role(strategy_role) and self._is_real_execution(event, intent_type):
            factor *= 0.88
        elif is_market_maker_strategy_role(strategy_role) and self._is_real_execution(event, intent_type):
            factor *= 0.96
        elif role_group == "exchange" and event.kind != "swap":
            factor *= 1.06
        elif role_group == "lp_pool" and intent_type not in PRIMARY_LP_INTENTS:
            factor *= 1.08

        if exchange_noise_sensitive and event.kind != "swap":
            factor *= 1.04

        if pricing_status == "estimated":
            factor *= 1.02
        elif pricing_status in {"unknown", "unavailable"}:
            factor *= 1.08

        return base_threshold * factor

    def _signal_type(self, event: Event, behavior_type: str, intent_type: str) -> str:
        intent_map = {
            "swap_execution": "active_trade",
            "exchange_deposit_candidate": "exchange_deposit_flow",
            "exchange_withdraw_candidate": "exchange_withdraw_flow",
            "pool_buy_pressure": "lp_buy_pressure",
            "pool_sell_pressure": "lp_sell_pressure",
            "liquidity_addition": "lp_liquidity_add",
            "liquidity_removal": "lp_liquidity_remove",
            "pool_rebalance": "lp_rebalance",
            "internal_rebalance": "internal_rebalance",
            "market_making_inventory_move": "inventory_rebalance",
            "possible_sell_preparation": "sell_preparation",
            "possible_buy_preparation": "buy_preparation",
            "pure_transfer": "transfer_flow",
        }
        if intent_type in intent_map:
            return intent_map[intent_type]
        if behavior_type in {"accumulation", "distribution", "scalping", "whale_action", "inventory_management"}:
            return behavior_type
        if event.kind == "swap":
            return "swap_flow"
        if event.kind == "token_transfer":
            return "token_flow"
        return "eth_flow"

    def _information_level(self, intent_type: str, event: Event, gate_metrics: dict, role_group: str) -> str:
        confirmation_score = float(gate_metrics.get("confirmation_score") or event.confirmation_score or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        intent_stage = str(gate_metrics.get("intent_stage") or event.intent_stage or "preliminary")
        liquidation_stage = str(event.metadata.get("liquidation_stage") or gate_metrics.get("liquidation_stage") or "none")
        liquidation_score = float(event.metadata.get("liquidation_score") or gate_metrics.get("liquidation_score") or 0.0)

        if role_group == "smart_money" and self._is_real_execution(event, intent_type):
            return "high" if confirmation_score >= 0.56 else "medium"
        if liquidation_stage == "execution":
            return "high" if liquidation_score >= LIQUIDATION_EXECUTION_MIN_SCORE else "medium"
        if liquidation_stage == "risk":
            return "medium" if liquidation_score >= LIQUIDATION_RISK_MIN_SCORE else "low"
        if intent_stage == "weak":
            return "low"
        if intent_type == "swap_execution" and confirmation_score >= 0.75:
            return "high"
        if intent_type in PRIMARY_LP_INTENTS:
            return "high" if confirmation_score >= 0.68 or resonance_score >= 0.45 else "medium"
        if intent_type in {"liquidity_addition", "liquidity_removal"}:
            return "high" if confirmation_score >= 0.76 else "medium"
        if intent_type == "pool_rebalance":
            return "medium" if confirmation_score >= 0.62 else "low"
        if intent_type in {"exchange_deposit_candidate", "exchange_withdraw_candidate", "market_making_inventory_move"}:
            return "high" if confirmation_score >= 0.78 or resonance_score >= 0.65 else "medium"
        if intent_type in {"internal_rebalance", "pure_transfer", "unknown_intent"}:
            return "low"
        if resonance_score >= 0.7 and event.kind == "swap":
            return "high"
        return "medium"

    def _confidence(
        self,
        behavior_conf: float,
        intent_conf: float,
        pricing_conf: float,
        confirmation_score: float,
        resonance_score: float,
        address_score: float,
        token_score: float,
        relative_address_size: float,
        quality_score: float,
        pricing_status: str,
        intent_type: str,
        intent_stage: str,
        exchange_noise_sensitive: bool,
        role_group: str,
        strategy_role: str,
        is_real_execution: bool,
    ) -> float:
        del strategy_role
        behavior_part = self._clamp(behavior_conf, 0.0, 1.0) * 0.12
        intent_part = self._clamp(intent_conf, 0.0, 1.0) * 0.20
        pricing_part = self._clamp(pricing_conf, 0.0, 1.0) * 0.10
        confirmation_part = self._clamp(confirmation_score, 0.0, 1.0) * 0.15
        resonance_part = self._clamp(resonance_score, 0.0, 1.0) * 0.06
        relative_part = self._relative_component(relative_address_size) * 0.12
        token_part = self._clamp(token_score / 100.0, 0.0, 1.0) * 0.04
        address_part = self._clamp(address_score / 100.0, 0.0, 1.0) * 0.06
        quality_part = self._clamp(quality_score, 0.0, 1.0) * 0.15

        conf = (
            behavior_part
            + intent_part
            + pricing_part
            + confirmation_part
            + resonance_part
            + relative_part
            + token_part
            + address_part
            + quality_part
        )

        if intent_type == "swap_execution":
            conf += 0.02
        elif intent_type in {"pool_buy_pressure", "pool_sell_pressure", "liquidity_removal"}:
            conf += 0.02
        elif intent_type == "pool_rebalance":
            conf -= 0.01
        elif intent_type in {"internal_rebalance", "pure_transfer", "unknown_intent"}:
            conf -= 0.03

        if intent_stage == "confirmed":
            conf += 0.02
        elif intent_stage == "weak":
            conf -= 0.04

        if exchange_noise_sensitive and intent_type in EXCHANGE_SENSITIVE_INTENTS and intent_stage != "confirmed":
            conf -= 0.03

        if role_group == "smart_money" and is_real_execution:
            conf += 0.03
        if role_group == "exchange" and not is_real_execution:
            conf -= 0.02

        if pricing_status == "estimated":
            conf -= 0.01
        elif pricing_status in {"unknown", "unavailable"}:
            conf -= 0.06

        return self._clamp(conf, 0.0, 1.0)

    def _relative_component(self, relative_address_size: float) -> float:
        if relative_address_size <= 0:
            return 0.0
        return self._clamp(math.log1p(relative_address_size) / math.log(6.0), 0.0, 1.0)

    def _tier(
        self,
        confidence: float,
        address_score: float,
        token_score: float,
        intent_confidence: float,
        pricing_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        information_level: str,
        quality_tier: str,
        pricing_status: str,
        intent_stage: str,
        exchange_noise_sensitive: bool,
    ) -> str:
        tier_rank = {"Tier 1": 1, "Tier 2": 2, "Tier 3": 3}

        if pricing_status in {"unknown", "unavailable"}:
            tier = "Tier 3"
        elif intent_stage == "weak":
            tier = "Tier 3"
        elif (
            confidence >= 0.87
            and address_score >= 70
            and token_score >= 62
            and intent_confidence >= 0.72
            and pricing_confidence >= 0.78
            and confirmation_score >= 0.74
            and information_level == "high"
        ):
            tier = "Tier 1"
        elif (
            confidence >= 0.76
            and address_score >= 58
            and token_score >= 50
            and intent_confidence >= 0.5
            and pricing_confidence >= 0.58
            and confirmation_score >= 0.48
            and information_level in {"high", "medium"}
        ):
            tier = "Tier 2"
        else:
            tier = "Tier 3"

        if resonance_score >= 0.72 and tier == "Tier 2" and intent_stage != "weak":
            tier = "Tier 1"

        if exchange_noise_sensitive and intent_stage != "confirmed" and tier == "Tier 1":
            tier = "Tier 2"

        return tier if tier_rank[tier] <= tier_rank.get(quality_tier, 3) else quality_tier

    def _priority(
        self,
        tier: str,
        information_level: str,
        intent_confidence: float,
        pricing_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        intent_stage: str,
        role_group: str,
        is_real_execution: bool,
    ) -> int:
        if intent_stage == "weak":
            return 3
        if (
            tier == "Tier 1"
            and information_level == "high"
            and intent_confidence >= 0.72
            and pricing_confidence >= 0.78
            and confirmation_score >= 0.72
        ):
            return 1
        if role_group == "smart_money" and is_real_execution and confirmation_score >= 0.58:
            return 1 if tier == "Tier 1" else 2
        if tier in {"Tier 1", "Tier 2"} and information_level in {"high", "medium"}:
            if (
                confirmation_score >= 0.5
                or resonance_score >= 0.55
                or (intent_stage == "preliminary" and pricing_confidence >= 0.7 and intent_confidence >= 0.58)
            ):
                return 2
        return 3

    def _is_real_execution(self, event: Event, intent_type: str | None) -> bool:
        return event.kind == "swap" or str(intent_type or "") == "swap_execution"

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))
