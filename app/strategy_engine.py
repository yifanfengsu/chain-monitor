import math

from config import (
    DELIVERY_ALLOW_EXCHANGE_ANCHOR_PRIMARY,
    DELIVERY_ALLOW_LIQUIDATION_EXECUTION_PRIMARY,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_LP_WEAK_SIGNAL_ARCHIVE_ONLY,
    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE,
    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_PRIMARY,
    DELIVERY_SMART_MONEY_EXECUTION_PRIMARY,
    LIQUIDATION_EXECUTION_MIN_SCORE,
    LIQUIDATION_PRIMARY_MIN_USD,
    LIQUIDATION_RISK_MIN_SCORE,
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
        lp_observe_exception_applied = bool(gate_metrics.get("lp_observe_exception_applied"))
        lp_observe_exception_reason = str(gate_metrics.get("lp_observe_exception_reason") or "")
        lp_observe_threshold_ratio = float(gate_metrics.get("lp_observe_threshold_ratio") or 0.0)
        lp_observe_below_min_gap = float(gate_metrics.get("lp_observe_below_min_gap") or 0.0)
        smart_money_non_exec_exception_applied = bool(gate_metrics.get("smart_money_non_exec_exception_applied"))
        smart_money_non_exec_exception_reason = str(gate_metrics.get("smart_money_non_exec_exception_reason") or "")
        market_maker_observe_exception_applied = bool(gate_metrics.get("market_maker_observe_exception_applied"))
        market_maker_observe_exception_reason = str(gate_metrics.get("market_maker_observe_exception_reason") or "")
        market_maker_threshold_ratio = float(gate_metrics.get("market_maker_threshold_ratio") or 0.0)
        market_maker_quality_gap = float(gate_metrics.get("market_maker_quality_gap") or 0.0)
        is_lp_directional_exception_candidate = (
            lp_observe_exception_applied
            and role_group == "lp_pool"
            and intent_type in PRIMARY_LP_INTENTS
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
        if not is_lp_directional_exception_candidate:
            effective_threshold = max(effective_threshold, gate_min_usd)
            if usd_value < max(effective_threshold, self.min_signal_usd):
                return None
        else:
            # gate 已经确认这是“金额略低但结构很强”的 directional LP；
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
                "lp_observe_exception_applied": bool(is_lp_directional_exception_candidate),
                "lp_observe_exception_reason": lp_observe_exception_reason,
                "lp_observe_threshold_ratio": round(lp_observe_threshold_ratio, 3),
                "lp_observe_below_min_gap": round(lp_observe_below_min_gap, 2),
                "lp_observe_delivery_cap": "observe_only" if is_lp_directional_exception_candidate else "",
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
        smart_money_non_exec_exception_applied = bool(
            gate_metrics.get("smart_money_non_exec_exception_applied")
            or signal.metadata.get("smart_money_non_exec_exception_applied")
        )
        market_maker_observe_exception_applied = bool(
            gate_metrics.get("market_maker_observe_exception_applied")
            or signal.metadata.get("market_maker_observe_exception_applied")
        )
        market_maker_observe_exception_reason = str(
            gate_metrics.get("market_maker_observe_exception_reason")
            or signal.metadata.get("market_maker_observe_exception_reason")
            or ""
        )
        threshold_ratio = float(
            gate_metrics.get("market_maker_threshold_ratio")
            or gate_metrics.get("smart_money_non_exec_threshold_ratio")
            or (
                (float(event.usd_value or 0.0) / float(gate_metrics.get("dynamic_min_usd") or 1.0))
                if float(gate_metrics.get("dynamic_min_usd") or 0.0) > 0
                else 0.0
            )
        )
        market_maker_non_exec_intent = intent_type in {"pure_transfer", "unknown_intent", "internal_rebalance", "market_making_inventory_move"}
        market_maker_behavior = str(signal.behavior_type or signal.metadata.get("behavior_type") or "")

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

            if market_maker_non_exec_intent:
                if market_maker and market_maker_observe_exception_applied:
                    signal.metadata["smart_money_non_exec_exception_applied"] = True
                    signal.metadata["smart_money_non_exec_delivery_cap"] = "observe_only"
                    signal.metadata["market_maker_observe_exception_applied"] = True
                    signal.metadata["market_maker_observe_exception_reason"] = market_maker_observe_exception_reason
                    signal.metadata["market_maker_delivery_cap"] = "observe_only"
                    event.metadata["smart_money_non_exec_exception_applied"] = True
                    event.metadata["smart_money_non_exec_delivery_cap"] = "observe_only"
                    event.metadata["market_maker_observe_exception_applied"] = True
                    event.metadata["market_maker_observe_exception_reason"] = market_maker_observe_exception_reason
                    event.metadata["market_maker_delivery_cap"] = "observe_only"
                    if market_maker_behavior == "inventory_shift":
                        return self._apply_delivery(event, signal, "observe", "market_maker_inventory_shift_observe")
                    if market_maker_behavior in {"inventory_management", "inventory_expansion", "inventory_distribution"} or intent_type == "market_making_inventory_move":
                        return self._apply_delivery(event, signal, "observe", "market_maker_inventory_observe")
                    return self._apply_delivery(event, signal, "observe", "market_maker_non_execution_observe")
                if smart_money_non_exec_exception_applied:
                    signal.metadata["smart_money_non_exec_exception_applied"] = True
                    signal.metadata["smart_money_non_exec_delivery_cap"] = "observe_only"
                    event.metadata["smart_money_non_exec_exception_applied"] = True
                    event.metadata["smart_money_non_exec_delivery_cap"] = "observe_only"
                    if DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE and (
                        quality_score >= 0.60
                        or confirmation_score >= 0.42
                        or resonance_score >= 0.30
                        or float(event.usd_value or 0.0) >= float(gate_metrics.get("dynamic_min_usd") or 0.0) * 2.0
                    ):
                        return self._apply_delivery(event, signal, "observe", "smart_money_non_execution_observe")
                    return self._apply_delivery(event, signal, "drop", "smart_money_non_execution_exception_drop")
                if (
                    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_PRIMARY
                    and confirmation_score >= 0.86
                    and resonance_score >= 0.72
                    and not market_maker
                ):
                    return self._apply_delivery(event, signal, "primary", "smart_money_transfer_exception_primary")
                if market_maker and DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE and (
                    confirmation_score >= MARKET_MAKER_OBSERVE_MIN_CONFIRMATION
                    or resonance_score >= MARKET_MAKER_OBSERVE_MIN_RESONANCE
                    or quality_score >= max(MARKET_MAKER_OBSERVE_GATE_FLOOR + 0.08, 0.67)
                    or raw_quality_score >= 0.62
                    or threshold_ratio >= 2.0
                ):
                    if market_maker_behavior == "inventory_shift":
                        return self._apply_delivery(event, signal, "observe", "market_maker_inventory_shift_observe")
                    if market_maker_behavior in {"inventory_management", "inventory_expansion", "inventory_distribution"} or intent_type == "market_making_inventory_move":
                        return self._apply_delivery(event, signal, "observe", "market_maker_inventory_observe")
                    return self._apply_delivery(event, signal, "observe", "market_maker_non_execution_observe")
                if DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE and (
                    quality_score >= 0.74 or confirmation_score >= 0.44
                ):
                    reason = "market_maker_non_execution_observe" if market_maker else "smart_money_transfer_observe"
                    return self._apply_delivery(event, signal, "observe", reason)
                return self._apply_delivery(event, signal, "drop", "smart_money_transfer_drop")

            if not market_maker and confirmation_score >= 0.80 and (
                multi_address_resonance or same_side_smart_money_addresses >= 2 or resonance_score >= 0.60
            ):
                return self._apply_delivery(event, signal, "primary", "smart_money_directional_resonance_primary")
            return self._apply_delivery(event, signal, "observe", "smart_money_non_execution_observe")

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
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)
        return delivery_class, reason

    def _delivery_fact_type(self, event: Event, signal: Signal) -> str:
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
