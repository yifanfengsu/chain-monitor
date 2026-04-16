from dataclasses import dataclass

from config import (
    INTERPRETER_ABNORMAL_MULTIPLIER,
    INTERPRETER_CONTINUOUS_WINDOW_SEC,
    QUALITY_GATE_MIN_PRICE_IMPACT_RATIO,
)
from constants import ETH_EQUIVALENT_CONTRACTS, STABLE_TOKEN_CONTRACTS
from filter import WALLET_FUNCTION_LABELS, format_address_label, get_address_meta, shorten_address, strategy_role_group
from lp_analyzer import canonicalize_pool_semantic_key
from models import Event, Signal
from signal_quality_gate import LP_SWEEP_SEMANTIC_SUBTYPES, detect_lp_liquidity_sweep


LP_INTENTS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
    "liquidity_addition",
    "liquidity_removal",
    "pool_rebalance",
    "pool_noise",
}
CLMM_POSITION_INTENTS = {
    "clmm_position_open",
    "clmm_position_add_range_liquidity",
    "clmm_position_remove_range_liquidity",
    "clmm_position_collect_fees",
    "clmm_position_close",
    "clmm_range_shift",
    "clmm_jit_liquidity_likely",
    "clmm_inventory_recenter",
    "clmm_passive_fee_harvest",
}
CLMM_PARTIAL_SUPPORT_INTENT = "clmm_partial_support_observation"


@dataclass
class InterpretationDecision:
    should_notify: bool
    reason: str


class SignalInterpreter:
    """行为解释层，统一生成 notifier 需要的上下文字段。"""

    INTENT_LABELS = {
        "swap_execution": "已执行交易",
        "exchange_deposit_candidate": "交易所方向流动",
        "exchange_withdraw_candidate": "交易所外流方向",
        "internal_rebalance": "内部调拨/归集",
        "market_making_inventory_move": "做市库存调节",
        "possible_sell_preparation": "交易场景方向流动",
        "possible_buy_preparation": "入场方向流动",
        "pure_transfer": "一般资金转移",
        "pool_buy_pressure": "池子买压",
        "pool_sell_pressure": "池子卖压",
        "liquidity_addition": "流动性增加",
        "liquidity_removal": "流动性减少",
        "pool_rebalance": "池子再平衡",
        "pool_noise": "池子噪声",
        "clmm_position_open": "头寸开启",
        "clmm_position_add_range_liquidity": "区间加流动性",
        "clmm_position_remove_range_liquidity": "区间撤流动性",
        "clmm_position_collect_fees": "收取手续费",
        "clmm_position_close": "头寸关闭",
        "clmm_range_shift": "区间迁移",
        "clmm_jit_liquidity_likely": "疑似 JIT 流动性",
        "clmm_inventory_recenter": "库存再居中",
        "clmm_passive_fee_harvest": "被动收手续费",
        CLMM_PARTIAL_SUPPORT_INTENT: "CLMM 部分支持观察",
        "unknown_intent": "意图未明",
    }

    INFO_LABELS = {
        "debug": "调试观察",
        "high": "高信息",
        "medium": "中信息",
        "low": "低信息",
        "normal": "一般",
    }

    PRICING_SOURCE_LABELS = {
        "swap_quote_stable": "稳定币报价",
        "swap_quote_eth": "ETH 报价",
        "swap_quote_market": "报价腿市场价",
        "swap_token_leg_market": "目标腿市场价",
        "native_eth_market": "ETH 市场价",
        "eth_equivalent_market": "ETH 等价资产",
        "stablecoin_parity": "稳定币平价",
        "token_market": "Token 市场价",
        "lp_leg_composite": "LP 双腿估值",
        "none": "无来源",
    }

    SIGNAL_TYPE_LABELS = {
        "exchange_deposit_flow": "Exchange_Deposit_Flow",
        "exchange_withdraw_flow": "Exchange_Withdraw_Flow",
        "buy_preparation": "Buy_Preparation",
        "sell_preparation": "Sell_Preparation",
        "transfer_flow": "Transfer_Flow",
        "inventory_rebalance": "Inventory_Rebalance",
        "internal_rebalance": "Internal_Rebalance",
        "active_trade": "Active_Trade",
        "swap_flow": "Swap_Flow",
        "pool_buy_pressure": "Pool_Buy_Pressure",
        "pool_sell_pressure": "Pool_Sell_Pressure",
        "liquidity_addition": "Liquidity_Addition",
        "liquidity_removal": "Liquidity_Removal",
        "pool_rebalance": "Pool_Rebalance",
        "lp_position_intent": "CLMM_Position_Intent",
        "clmm_partial_support_signal": "CLMM_Partial_Support",
    }

    def __init__(
        self,
        continuous_window_sec: int = INTERPRETER_CONTINUOUS_WINDOW_SEC,
        abnormal_multiplier: float = INTERPRETER_ABNORMAL_MULTIPLIER,
    ) -> None:
        self.continuous_window_sec = int(continuous_window_sec)
        self.abnormal_multiplier = float(abnormal_multiplier)

    def interpret(
        self,
        event: Event,
        signal: Signal,
        behavior: dict,
        watch_meta: dict,
        watch_context: dict,
        address_snapshot: dict,
        token_snapshot: dict,
        gate_metrics: dict | None = None,
    ) -> InterpretationDecision:
        gate_metrics = gate_metrics or {}
        watch_meta = watch_meta or get_address_meta(event.address)
        self._canonicalize_pool_semantics(event, signal)
        raw = event.metadata.get("raw") or {}
        clmm_context = self._clmm_context(raw, event)
        clmm_partial_support = self._is_clmm_partial_support_event(event, raw)
        clmm_event = self._is_clmm_position_event(event, raw)
        lp_event = self._is_lp_event(event, watch_meta, raw)
        counterparty_meta = self._resolve_counterparty_meta(event, watch_context)

        semantic = self._classify_semantic(event, lp_event, clmm_event, clmm_partial_support)
        intent_label = self.INTENT_LABELS.get(str(event.intent_type or "unknown_intent"), "意图未明")
        actor_label = self._actor_label(event.address, watch_meta, lp_event)
        role_display = self._role_display(watch_meta, lp_event)
        action_label = self._action_label(event, signal, semantic, lp_event, clmm_event, clmm_partial_support)
        fact_label = self._fact_label(event, watch_context, lp_event, clmm_event, clmm_partial_support)
        intent_detail = self._intent_detail(event, lp_event, clmm_event, clmm_partial_support)
        continuous_count = self._continuous_count(event, address_snapshot, lp_event)
        continuous_label = self._continuous_label(event, continuous_count, lp_event)
        abnormal_ratio = float(signal.abnormal_ratio or self._abnormal_ratio(event, address_snapshot))
        abnormal_label = self._abnormal_label(event, abnormal_ratio, lp_event)
        pricing_label = self._pricing_label(event)
        information_label = self.INFO_LABELS.get(str(signal.information_level or "normal"), str(signal.information_level or "一般"))
        resonance_label = self._resonance_label(event, gate_metrics, lp_event)
        confirmation_label = self._confirmation_label(event, gate_metrics, lp_event)
        inference_confidence_label = self._inference_confidence_label(event, signal)
        inference_label = self._inference_label(event, intent_label, lp_event)
        evidence_summary = self._evidence_summary(event, gate_metrics, lp_event)
        case_summary = self._case_summary(event, signal)
        next_observation_window = self._next_observation_window(event)
        trade_value_label = self._trade_value_label(event, signal, continuous_count, abnormal_ratio, gate_metrics, lp_event)
        directional_bias = self._directional_bias(event, signal, continuous_count, abnormal_ratio, gate_metrics, lp_event)
        followup_checks = self._followup_checks(event, continuous_count, abnormal_ratio, lp_event)
        followup_summary = self._followup_summary(followup_checks, event.followup_stage, event.followup_status)
        market_implication = self._market_implication(event, directional_bias, continuous_count, abnormal_ratio, lp_event)
        upgrade_conditions = self._upgrade_conditions(event, continuous_count, abnormal_ratio, lp_event)
        failure_conditions = self._failure_conditions(event, lp_event)
        market_context_label = self._market_context_label(event, signal, gate_metrics, token_snapshot, lp_event)
        conclusion_label = f"{directional_bias}｜{trade_value_label}｜{intent_label}"
        followup_confirmed = bool(event.metadata.get("followup_confirmed"))
        followup_assets = list(event.metadata.get("followup_assets") or [])
        followup_label = str(event.metadata.get("followup_label") or "")
        followup_detail = str(event.metadata.get("followup_detail") or "")
        case_notification_stage = event.metadata.get("case_notification_stage")
        case_notification_allowed = bool(event.metadata.get("case_notification_allowed"))
        case_notification_suppressed = bool(event.metadata.get("case_notification_suppressed"))
        case_notification_reason = str(event.metadata.get("case_notification_reason") or "")
        notification_stage_label = str(event.metadata.get("notification_stage_label") or "")

        path_from_label, path_to_label, path_text = self._path_summary(event, watch_context)
        quote_path_text, token_path_text = self._leg_paths(event)
        message_variant = self._message_variant(event, signal, lp_event, watch_meta)
        object_label = actor_label
        pool_label = self._pool_label(event, raw, actor_label)
        pair_label = self._pair_label(event, raw, pool_label)
        headline_label = self._headline_label(directional_bias, trade_value_label, intent_label)
        fact_brief = self._fact_brief(event, fact_label)
        explanation_brief = self._explanation_brief(event, intent_detail, market_implication)
        evidence_brief = self._evidence_brief(
            confirmation_label=confirmation_label,
            abnormal_label=abnormal_label,
            resonance_label=resonance_label,
            continuous_label=continuous_label,
        )
        action_hint = self._action_hint(event, upgrade_conditions, followup_checks, lp_event)
        update_brief = self._update_brief(event)
        lp_action_brief = self._lp_action_brief(event, action_label, fact_label)
        lp_meaning_brief = self._lp_meaning_brief(event, intent_detail, market_implication)
        lp_volume_surge_label = self._lp_volume_surge_label(event)
        same_pool_continuity_label = self._same_pool_continuity_label(event)
        multi_pool_resonance_label = self._multi_pool_resonance_label(event)
        lp_burst_label = self._lp_burst_label(event, signal)
        lp_burst_summary = self._lp_burst_summary(event, signal)
        lp_burst_window_label = self._lp_burst_window_label(event, signal)
        lp_trend_display = self._lp_trend_display(event, signal)
        observe_or_primary_label = "主推送" if str(signal.delivery_class or "") == "primary" else "观察级"
        liquidation_meta = self._liquidation_meta(event)
        sweep_meta = self._lp_sweep_meta(event, gate_metrics)
        if lp_event and sweep_meta["detected"] and not liquidation_meta["active"]:
            semantic = "pool_trade_pressure"
            action_label = self._lp_sweep_action_label(event, sweep_meta)
            fact_label = self._lp_sweep_fact_label(event, pair_label, sweep_meta)
            intent_detail = self._lp_sweep_intent_detail(event, sweep_meta)
            market_implication = self._lp_sweep_market_implication(event, sweep_meta)
            fact_brief = self._fact_brief(event, fact_label)
            explanation_brief = self._explanation_brief(event, intent_detail, market_implication)
            lp_action_brief = action_label
            lp_meaning_brief = self._lp_sweep_meaning_brief(event, sweep_meta)
        smart_money_context = self._smart_money_context(
            event=event,
            signal=signal,
            watch_meta=watch_meta,
            gate_metrics=gate_metrics,
            fact_brief=fact_brief,
            explanation_brief=explanation_brief,
            evidence_brief=evidence_brief,
            action_hint=action_hint,
        )
        market_maker_display_context = self._market_maker_display_context(smart_money_context)
        if smart_money_context["active"] and message_variant == "alert":
            message_variant = smart_money_context["message_variant"]

        downstream_followup_active = bool(event.metadata.get("downstream_followup_active"))
        downstream_followup_stage = str(event.metadata.get("downstream_followup_stage") or "")
        downstream_anchor_usd_value = float(
            event.metadata.get("anchor_usd_value")
            or event.metadata.get("downstream_anchor_usd_value")
            or 0.0
        )
        current_event_is_anchor = bool(event.metadata.get("current_event_is_anchor"))
        downstream_early_warning_candidate = downstream_followup_stage == "followup_opened" and current_event_is_anchor
        if downstream_followup_active:
            message_variant = "downstream_followup"
            object_label = str(event.metadata.get("downstream_object_label") or object_label or actor_label)
            headline_label = str(event.metadata.get("downstream_followup_label") or headline_label)
            if downstream_early_warning_candidate:
                fact_brief = (
                    f"观察开启｜{event.metadata.get('downstream_anchor_label') or '重点地址'} -> "
                    f"{event.metadata.get('downstream_object_label') or object_label}｜"
                    f"{self._trade_value_label(event, signal, continuous_count, abnormal_ratio, gate_metrics, lp_event)}"
                )
            else:
                fact_brief = (
                    f"{event.metadata.get('downstream_anchor_label') or '重点地址'} -> "
                    f"{event.metadata.get('downstream_object_label') or object_label}｜{self._trade_value_label(event, signal, continuous_count, abnormal_ratio, gate_metrics, lp_event)}"
                )
            explanation_brief = str(event.metadata.get("downstream_followup_detail") or explanation_brief)
            evidence_brief = (
                f"{event.metadata.get('downstream_followup_stage_label') or ('首条预警' if downstream_early_warning_candidate else '观察开启')}｜"
                f"anchor ${downstream_anchor_usd_value:,.0f}｜"
                f"{'已有一定确认' if downstream_early_warning_candidate else confirmation_label}"
            )
            action_hint = str(event.metadata.get("downstream_followup_next_hint") or action_hint)
            update_brief = str(
                event.metadata.get("downstream_followup_label")
                or ("超大额下游观察已开启" if downstream_early_warning_candidate else update_brief)
            )
            path_text = str(event.metadata.get("downstream_followup_path_label") or path_text)

        if lp_event and liquidation_meta["active"]:
            semantic = "liquidation_execution" if liquidation_meta["stage"] == "execution" else "liquidation_risk"
            intent_label = "疑似清算执行" if liquidation_meta["stage"] == "execution" else "疑似清算风险"
            action_label = self._liquidation_action_label(liquidation_meta)
            fact_label = self._liquidation_fact_label(event, liquidation_meta)
            intent_detail = self._liquidation_intent_detail(liquidation_meta)
            confirmation_label = self._liquidation_confirmation_label(liquidation_meta)
            inference_label = self._liquidation_inference_label(liquidation_meta)
            evidence_summary = self._liquidation_evidence_summary(liquidation_meta, gate_metrics)
            trade_value_label = "策略级" if liquidation_meta["stage"] == "execution" else "跟踪级"
            directional_bias = self._liquidation_directional_bias(liquidation_meta)
            followup_checks = self._liquidation_followup_checks(liquidation_meta)
            followup_summary = self._followup_summary(followup_checks, event.followup_stage, event.followup_status)
            market_implication = self._liquidation_market_implication(liquidation_meta)
            upgrade_conditions = self._liquidation_upgrade_conditions(liquidation_meta)
            failure_conditions = self._liquidation_failure_conditions(liquidation_meta)
            conclusion_label = f"{directional_bias}｜{trade_value_label}｜{intent_label}"
            message_variant = "liquidation_execution" if liquidation_meta["stage"] == "execution" else "liquidation_risk"
            headline_label = self._headline_label(directional_bias, trade_value_label, intent_label)
            fact_brief = self._fact_brief(event, fact_label)
            explanation_brief = self._explanation_brief(event, intent_detail, market_implication)
            evidence_brief = self._evidence_brief(
                confirmation_label=confirmation_label,
                abnormal_label=abnormal_label,
                resonance_label=resonance_label,
                continuous_label=continuous_label,
            )
            action_hint = self._action_hint(event, upgrade_conditions, followup_checks, lp_event)
            lp_action_brief = action_label
            lp_meaning_brief = market_implication

        market_state_label = headline_label
        if not downstream_followup_active and (lp_event or liquidation_meta["active"]):
            market_state_label = self._market_state_label(event, liquidation_meta, sweep_meta)
            headline_label = market_state_label
            evidence_brief = self._compact_evidence_brief(
                event=event,
                gate_metrics=gate_metrics,
                liquidation_meta=liquidation_meta,
                sweep_meta=sweep_meta,
                confirmation_label=confirmation_label,
                abnormal_label=abnormal_label,
                resonance_label=resonance_label,
                continuous_label=continuous_label,
            )

        operational_intent = self._build_operational_intent(
            event=event,
            signal=signal,
            watch_meta=watch_meta,
            counterparty_meta=counterparty_meta,
            gate_metrics=gate_metrics,
            clmm_event=clmm_event,
            clmm_partial_support=clmm_partial_support,
            clmm_context=clmm_context,
            lp_event=lp_event,
            liquidation_meta=liquidation_meta,
            sweep_meta=sweep_meta,
            actor_label=actor_label,
            object_label=object_label,
            pair_label=pair_label,
        )
        outcome_tracking = self._outcome_tracking_seam(
            event=event,
            pair_label=pair_label,
            clmm_event=clmm_event,
            clmm_context=clmm_context,
            lp_event=lp_event,
            operational_intent=operational_intent,
        )
        role_group = strategy_role_group(watch_meta.get("strategy_role") or event.strategy_role)
        auto_intent_template = ""
        exchange_internal_intent = (
            role_group == "exchange"
            and str(operational_intent.get("operational_intent_strength") or "") in {"confirmed", "likely"}
        )
        if clmm_partial_support and operational_intent.get("operational_intent_key"):
            auto_intent_template = "debug"
        elif (
            operational_intent.get("operational_intent_key")
            and (
                exchange_internal_intent
                or (
                    (role_group in {"exchange", "smart_money", "market_maker"} or clmm_event)
                    and float(operational_intent.get("operational_intent_confidence") or 0.0) >= 0.56
                )
            )
            and not downstream_followup_active
            and not liquidation_meta["active"]
            and not lp_event
        ):
            auto_intent_template = "intent"

        event.metadata.update({
            "semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "lp_semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "sweep_confidence": str(sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_confidence": str(sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_detected": bool(sweep_meta.get("detected")),
            "market_state_label": str(market_state_label or ""),
            "clmm_position_event": clmm_event,
            "clmm_partial_support": clmm_partial_support,
            "clmm_context": dict(clmm_context),
            **operational_intent,
            **outcome_tracking,
        })
        signal.semantic = semantic
        signal.core_action = action_label
        signal.context = {
            "actor_label": actor_label,
            "object_label": object_label,
            "pool_label": pool_label,
            "pair_label": pair_label,
            "role_summary": role_display,
            "role_display": role_display,
            "role_label": watch_meta.get("role_label", "未分类"),
            "strategy_role_label": watch_meta.get("strategy_role_label", "未分类"),
            "semantic_role_label": watch_meta.get("semantic_role_label", "未分类"),
            "entity_id": str(watch_meta.get("entity_id") or ""),
            "entity_label": str(watch_meta.get("entity_label") or ""),
            "entity_type": str(watch_meta.get("entity_type") or "unknown"),
            "wallet_function": str(event.metadata.get("watch_wallet_function") or watch_meta.get("wallet_function") or "unknown"),
            "wallet_function_label": WALLET_FUNCTION_LABELS.get(
                str(event.metadata.get("watch_wallet_function") or watch_meta.get("wallet_function") or "unknown"),
                str(event.metadata.get("watch_wallet_function") or watch_meta.get("wallet_function") or "unknown"),
            ),
            "counterparty_wallet_function": str(event.metadata.get("counterparty_wallet_function") or counterparty_meta.get("wallet_function") or "unknown"),
            "exchange_internality": str(event.metadata.get("exchange_internality") or "no"),
            "exchange_same_entity_strength": str(event.metadata.get("exchange_same_entity_strength") or event.metadata.get("exchange_internality") or "no"),
            "exchange_transfer_purpose": str(event.metadata.get("exchange_transfer_purpose") or "exchange_unknown_flow"),
            "exchange_transfer_purpose_family": str(event.metadata.get("exchange_transfer_purpose_family") or event.metadata.get("exchange_transfer_purpose") or "exchange_unknown_flow"),
            "exchange_transfer_purpose_strength": str(event.metadata.get("exchange_transfer_purpose_strength") or "no"),
            "exchange_transfer_confidence": float(event.metadata.get("exchange_transfer_confidence") or 0.0),
            "exchange_entity_label": str(event.metadata.get("exchange_entity_label") or ""),
            "exchange_transfer_why": list(event.metadata.get("exchange_transfer_why") or []),
            "fact_label": fact_label,
            "inference_label": inference_label,
            "inference_confidence_label": inference_confidence_label,
            "confidence_label": inference_confidence_label,
            "confirmation_label": confirmation_label,
            "evidence_summary": evidence_summary,
            "resonance_label": resonance_label,
            "action_label": action_label,
            "intent_label": intent_label,
            "intent_detail": intent_detail,
            "conclusion_label": conclusion_label,
            "trade_value_label": trade_value_label,
            "directional_bias": directional_bias,
            "market_implication": market_implication,
            "upgrade_conditions": upgrade_conditions,
            "failure_conditions": failure_conditions,
            "information_label": information_label,
            "pricing_label": pricing_label,
            "pricing_source_label": self._pricing_source_label(event.pricing_source),
            "abnormal_ratio": round(abnormal_ratio, 3),
            "abnormal_label": abnormal_label,
            "continuous_count": continuous_count,
            "continuous_label": continuous_label,
            "market_context_label": market_context_label,
            "followup_checks": followup_checks,
            "followup_summary": followup_summary,
            "case_id": event.case_id,
            "followup_stage": event.followup_stage,
            "followup_status": event.followup_status,
            "case_summary": case_summary,
            "next_observation_window": next_observation_window,
            "base_token_score": round(float(signal.base_token_score or 0.0), 1),
            "token_context_score": round(float(signal.token_context_score or 0.0), 1),
            "path_from_label": path_from_label,
            "path_to_label": path_to_label,
            "path_text": path_text,
            "path_label": path_text,
            "path_to_label": path_to_label,
            "quote_path_text": quote_path_text,
            "token_path_text": token_path_text,
            "usd_value": round(float(event.usd_value or 0.0), 2),
            "priority": int(watch_meta.get("priority", 3) or 3),
            "note": str(watch_meta.get("note") or "").strip(),
            "behavior_reason": behavior.get("reason", ""),
            "signal_type_label": self._signal_type_label(signal.type),
            "clmm_position_event": clmm_event,
            "clmm_partial_support": clmm_partial_support,
            "clmm_partial_reason": str(event.metadata.get("clmm_partial_reason") or ""),
            "clmm_candidate_status": str(event.metadata.get("clmm_candidate_status") or ""),
            "clmm_manager_protocol": str(event.metadata.get("clmm_manager_protocol") or ""),
            "clmm_manager_address": str(event.metadata.get("clmm_manager_address") or ""),
            "clmm_context": dict(clmm_context),
            "venue_or_position_context": str(operational_intent.get("venue_or_position_context") or ""),
            "lp_event": lp_event,
            "followup_confirmed": followup_confirmed,
            "followup_assets": followup_assets,
            "followup_label": followup_label,
            "followup_detail": followup_detail,
            "case_notification_stage": case_notification_stage,
            "case_notification_allowed": case_notification_allowed,
            "case_notification_suppressed": case_notification_suppressed,
            "case_notification_reason": case_notification_reason,
            "notification_stage_label": notification_stage_label,
            "message_variant": message_variant,
            "market_state_label": market_state_label,
            "headline_label": headline_label,
            "fact_brief": fact_brief,
            "explanation_brief": explanation_brief,
            "evidence_brief": evidence_brief,
            "action_hint": action_hint,
            "update_brief": update_brief,
            "lp_action_brief": lp_action_brief,
            "lp_meaning_brief": lp_meaning_brief,
            "lp_volume_surge_label": lp_volume_surge_label,
            "same_pool_continuity_label": same_pool_continuity_label,
            "multi_pool_resonance_label": multi_pool_resonance_label,
            "lp_burst_label": lp_burst_label,
            "lp_burst_summary": lp_burst_summary,
            "lp_burst_window_label": lp_burst_window_label,
            "lp_trend_display_label": lp_trend_display.get("lp_trend_display_label", ""),
            "lp_trend_display_profile": lp_trend_display.get("lp_trend_display_profile", ""),
            "lp_trend_display_mode": lp_trend_display.get("lp_trend_display_mode", ""),
            "lp_trend_display_bias": lp_trend_display.get("lp_trend_display_bias", ""),
            "lp_trend_display_state": lp_trend_display.get("lp_trend_display_state", ""),
            "lp_burst_fastlane_applied": bool(event.metadata.get("lp_burst_fastlane_applied") or signal.metadata.get("lp_burst_fastlane_applied")),
            "lp_burst_fastlane_reason": str(event.metadata.get("lp_burst_fastlane_reason") or signal.metadata.get("lp_burst_fastlane_reason") or ""),
            "lp_burst_delivery_class": str(event.metadata.get("lp_burst_delivery_class") or signal.metadata.get("lp_burst_delivery_class") or ""),
            "observe_or_primary_label": observe_or_primary_label,
            "semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "lp_semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "sweep_confidence": str(sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_detected": bool(sweep_meta.get("detected")),
            "liquidation_stage": liquidation_meta["stage"],
            "liquidation_score": liquidation_meta["score"],
            "liquidation_side": liquidation_meta["side"],
            "liquidation_protocols": liquidation_meta["protocols"],
            "liquidation_reason": liquidation_meta["reason"],
            "smart_money_observe_label": smart_money_context["observe_label"],
            "smart_money_fact_brief": smart_money_context["fact_brief"],
            "smart_money_explanation_brief": smart_money_context["explanation_brief"],
            "smart_money_evidence_brief": smart_money_context["evidence_brief"],
            "smart_money_action_hint": smart_money_context["action_hint"],
            "smart_money_style_variant": smart_money_context["style_variant"],
            **market_maker_display_context,
            "downstream_followup_active": downstream_followup_active,
            "downstream_followup_stage": downstream_followup_stage,
            "downstream_anchor_label": str(event.metadata.get("downstream_anchor_label") or ""),
            "downstream_object_label": str(event.metadata.get("downstream_object_label") or ""),
            "downstream_followup_label": str(event.metadata.get("downstream_followup_label") or ""),
            "downstream_followup_detail": str(event.metadata.get("downstream_followup_detail") or ""),
            "downstream_followup_reason": str(event.metadata.get("downstream_followup_reason") or ""),
            "downstream_followup_stage_label": str(event.metadata.get("downstream_followup_stage_label") or ""),
            "downstream_followup_path_label": str(event.metadata.get("downstream_followup_path_label") or ""),
            "downstream_followup_next_hint": str(event.metadata.get("downstream_followup_next_hint") or ""),
            "downstream_early_warning_candidate": downstream_early_warning_candidate,
            "downstream_early_warning_allowed": bool(event.metadata.get("downstream_early_warning_allowed")),
            "downstream_early_warning_reason": str(event.metadata.get("downstream_early_warning_reason") or ""),
            "downstream_early_warning_thresholds": dict(event.metadata.get("downstream_early_warning_thresholds") or {}),
            "downstream_early_warning_emitted": bool(event.metadata.get("downstream_early_warning_emitted")),
            "anchor_usd_value": downstream_anchor_usd_value,
            "current_event_is_anchor": current_event_is_anchor,
            "message_template": auto_intent_template,
            **operational_intent,
            **outcome_tracking,
        }

        event.metadata.update(lp_trend_display)
        signal.metadata.setdefault("summary", {})
        signal.metadata.update({
            "message_variant": message_variant,
            "market_state_label": market_state_label,
            "pair_label": pair_label,
            "semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "lp_semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "sweep_confidence": str(sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_detected": bool(sweep_meta.get("detected")),
            "smart_money_style_variant": smart_money_context["style_variant"],
            "smart_money_observe_label": smart_money_context["observe_label"],
            "message_template": auto_intent_template,
            "clmm_position_event": clmm_event,
            "clmm_partial_support": clmm_partial_support,
            "clmm_partial_reason": str(event.metadata.get("clmm_partial_reason") or ""),
            "clmm_candidate_status": str(event.metadata.get("clmm_candidate_status") or ""),
            "clmm_manager_protocol": str(event.metadata.get("clmm_manager_protocol") or ""),
            "clmm_manager_address": str(event.metadata.get("clmm_manager_address") or ""),
            "clmm_context": dict(clmm_context),
            **operational_intent,
            **outcome_tracking,
            **market_maker_display_context,
            **lp_trend_display,
        })
        signal.metadata["summary"].update({
            "counterparty_role": counterparty_meta.get("role", "unknown"),
            "counterparty_strategy_role": counterparty_meta.get("strategy_role", "unknown"),
            "counterparty_semantic_role": counterparty_meta.get("semantic_role", "unknown"),
            "counterparty_label": counterparty_meta.get("label", ""),
            "pricing_source": event.pricing_source,
            "usd_value_available": bool(event.usd_value_available),
            "usd_value_estimated": bool(event.usd_value_estimated),
            "fact_label": fact_label,
            "inference_label": inference_label,
            "confirmation_label": confirmation_label,
            "confidence_label": inference_confidence_label,
            "inference_confidence_label": inference_confidence_label,
            "path_label": path_text,
            "directional_bias": directional_bias,
            "trade_value_label": trade_value_label,
            "market_implication": market_implication,
            "case_id": event.case_id,
            "followup_stage": event.followup_stage,
            "followup_status": event.followup_status,
            "case_summary": case_summary,
            "next_observation_window": next_observation_window,
            "signal_type_label": self._signal_type_label(signal.type),
            "followup_confirmed": followup_confirmed,
            "followup_assets": followup_assets,
            "followup_label": followup_label,
            "followup_detail": followup_detail,
            "case_notification_stage": case_notification_stage,
            "case_notification_allowed": case_notification_allowed,
            "case_notification_suppressed": case_notification_suppressed,
            "case_notification_reason": case_notification_reason,
            "notification_stage_label": notification_stage_label,
            "message_variant": message_variant,
            "market_state_label": market_state_label,
            "headline_label": headline_label,
            "fact_brief": fact_brief,
            "explanation_brief": explanation_brief,
            "evidence_brief": evidence_brief,
            "action_hint": action_hint,
            "update_brief": update_brief,
            "object_label": object_label,
            "pool_label": pool_label,
            "pair_label": pair_label,
            "lp_action_brief": lp_action_brief,
            "lp_meaning_brief": lp_meaning_brief,
            "lp_volume_surge_label": lp_volume_surge_label,
            "same_pool_continuity_label": same_pool_continuity_label,
            "multi_pool_resonance_label": multi_pool_resonance_label,
            "lp_burst_label": lp_burst_label,
            "lp_burst_summary": lp_burst_summary,
            "lp_burst_window_label": lp_burst_window_label,
            "lp_trend_display_label": lp_trend_display.get("lp_trend_display_label", ""),
            "lp_trend_display_profile": lp_trend_display.get("lp_trend_display_profile", ""),
            "lp_trend_display_mode": lp_trend_display.get("lp_trend_display_mode", ""),
            "lp_trend_display_bias": lp_trend_display.get("lp_trend_display_bias", ""),
            "lp_trend_display_state": lp_trend_display.get("lp_trend_display_state", ""),
            "lp_burst_fastlane_applied": bool(event.metadata.get("lp_burst_fastlane_applied") or signal.metadata.get("lp_burst_fastlane_applied")),
            "lp_burst_fastlane_reason": str(event.metadata.get("lp_burst_fastlane_reason") or signal.metadata.get("lp_burst_fastlane_reason") or ""),
            "lp_burst_delivery_class": str(event.metadata.get("lp_burst_delivery_class") or signal.metadata.get("lp_burst_delivery_class") or ""),
            "observe_or_primary_label": observe_or_primary_label,
            "semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "lp_semantic_subtype": str(sweep_meta.get("semantic_subtype") or ""),
            "sweep_confidence": str(sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_detected": bool(sweep_meta.get("detected")),
            "smart_money_observe_label": smart_money_context["observe_label"],
            "smart_money_fact_brief": smart_money_context["fact_brief"],
            "smart_money_explanation_brief": smart_money_context["explanation_brief"],
            "smart_money_evidence_brief": smart_money_context["evidence_brief"],
            "smart_money_action_hint": smart_money_context["action_hint"],
            "smart_money_style_variant": smart_money_context["style_variant"],
            **market_maker_display_context,
            "downstream_followup_active": downstream_followup_active,
            "downstream_followup_stage": downstream_followup_stage,
            "downstream_anchor_label": str(event.metadata.get("downstream_anchor_label") or ""),
            "downstream_object_label": str(event.metadata.get("downstream_object_label") or ""),
            "downstream_followup_label": str(event.metadata.get("downstream_followup_label") or ""),
            "downstream_followup_detail": str(event.metadata.get("downstream_followup_detail") or ""),
            "downstream_followup_reason": str(event.metadata.get("downstream_followup_reason") or ""),
            "downstream_followup_stage_label": str(event.metadata.get("downstream_followup_stage_label") or ""),
            "downstream_followup_path_label": str(event.metadata.get("downstream_followup_path_label") or ""),
            "downstream_followup_next_hint": str(event.metadata.get("downstream_followup_next_hint") or ""),
            "downstream_early_warning_candidate": downstream_early_warning_candidate,
            "downstream_early_warning_allowed": bool(event.metadata.get("downstream_early_warning_allowed")),
            "downstream_early_warning_reason": str(event.metadata.get("downstream_early_warning_reason") or ""),
            "downstream_early_warning_thresholds": dict(event.metadata.get("downstream_early_warning_thresholds") or {}),
            "downstream_early_warning_emitted": bool(event.metadata.get("downstream_early_warning_emitted")),
            "anchor_usd_value": downstream_anchor_usd_value,
            "current_event_is_anchor": current_event_is_anchor,
            "clmm_position_event": clmm_event,
            "clmm_context": dict(clmm_context),
            "venue_or_position_context": str(operational_intent.get("venue_or_position_context") or ""),
            **outcome_tracking,
        })
        if lp_event:
            signal.metadata["lp"] = {
                "context": dict(raw.get("lp_context") or {}),
                "analysis": dict(event.metadata.get("lp_analysis") or {}),
                "burst": dict(event.metadata.get("lp_burst") or {}),
            }
        if clmm_event or clmm_partial_support:
            signal.metadata["clmm"] = {
                "context": dict(clmm_context),
                "outcome_tracking": dict(outcome_tracking.get("outcome_tracking") or {}),
            }
        return InterpretationDecision(should_notify=True, reason="interpreted")

    def _is_lp_event(self, event: Event, watch_meta: dict, raw: dict) -> bool:
        if str(watch_meta.get("strategy_role") or event.strategy_role or "") == "lp_pool":
            return True
        if str(raw.get("monitor_type") or "") == "lp_pool":
            return True
        return str(event.intent_type or "") in LP_INTENTS or str(event.strategy_role or "") == "lp_pool"

    def _is_clmm_position_event(self, event: Event, raw: dict) -> bool:
        if self._is_clmm_partial_support_event(event, raw):
            return False
        if bool(event.metadata.get("clmm_position_event")):
            return True
        if bool((event.metadata.get("raw") or {}).get("clmm_position_event")):
            return True
        if bool(raw.get("clmm_position_event")):
            return True
        if str(raw.get("monitor_type") or "") == "clmm_position":
            return True
        return str(event.intent_type or "") in CLMM_POSITION_INTENTS

    def _is_clmm_partial_support_event(self, event: Event, raw: dict) -> bool:
        if bool(event.metadata.get("clmm_partial_support")):
            return True
        if bool((event.metadata.get("raw") or {}).get("clmm_partial_support")):
            return True
        if bool(raw.get("clmm_partial_support")):
            return True
        if str(raw.get("monitor_type") or "") == "clmm_partial_support":
            return True
        return str(event.intent_type or "") == CLMM_PARTIAL_SUPPORT_INTENT

    def _clmm_context(self, raw: dict, event: Event) -> dict:
        context = {}
        raw_context = raw.get("clmm_context") or {}
        metadata_context = event.metadata.get("clmm_context") or {}
        if isinstance(raw_context, dict):
            context.update(raw_context)
        if isinstance(metadata_context, dict):
            context.update(metadata_context)
        return context

    def _actor_label(self, address: str, watch_meta: dict, lp_event: bool) -> str:
        label = str(watch_meta.get("label") or "").strip()
        if lp_event and label:
            return label
        short = shorten_address(address)
        if label:
            return f"{label} ({short})"
        return format_address_label(address)

    def _role_display(self, watch_meta: dict, lp_event: bool) -> str:
        if lp_event:
            return str(watch_meta.get("semantic_role_label") or watch_meta.get("role_label") or "流动性池")
        semantic_role_label = str(watch_meta.get("semantic_role_label") or "").strip()
        role_label = str(watch_meta.get("role_label") or "").strip()
        strategy_role_label = str(watch_meta.get("strategy_role_label") or "").strip()
        return semantic_role_label or role_label or strategy_role_label or "未分类"

    def _resolve_counterparty_meta(self, event: Event, watch_context: dict | None) -> dict:
        if watch_context:
            meta = watch_context.get("counterparty_meta")
            if isinstance(meta, dict) and meta.get("role"):
                return meta
            counterparty = str(watch_context.get("counterparty") or "").lower()
            if counterparty:
                return get_address_meta(counterparty)

        raw = event.metadata.get("raw") or {}
        counterparty = str(raw.get("counterparty") or "").lower()
        if counterparty:
            return get_address_meta(counterparty)
        return get_address_meta("")

    def _classify_semantic(self, event: Event, lp_event: bool, clmm_event: bool, clmm_partial_support: bool) -> str:
        if lp_event:
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                return "pool_trade_pressure"
            if event.intent_type in {"liquidity_addition", "liquidity_removal"}:
                return "pool_liquidity_shift"
            if event.intent_type == "pool_rebalance":
                return "pool_rebalance"
            return "pool_noise"
        if clmm_event:
            return "clmm_position"
        if clmm_partial_support:
            return "clmm_partial_support"

        semantic_map = {
            "swap_execution": "active_trade",
            "exchange_deposit_candidate": "deposit_preparation",
            "exchange_withdraw_candidate": "withdraw_exit",
            "internal_rebalance": "internal_rebalance",
            "market_making_inventory_move": "inventory_rebalance",
            "possible_sell_preparation": "deposit_preparation",
            "possible_buy_preparation": "buy_preparation",
            "pure_transfer": "transfer_only",
            "unknown_intent": "unknown_flow",
        }
        return semantic_map.get(str(event.intent_type or "unknown_intent"), "unknown_flow")

    def _action_label(self, event: Event, signal: Signal, semantic: str, lp_event: bool, clmm_event: bool, clmm_partial_support: bool) -> str:
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        clmm_context = self._clmm_context(raw, event)
        token_symbol = str(lp_context.get("base_token_symbol") or raw.get("token_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        quote_symbol = str(lp_context.get("quote_token_symbol") or raw.get("quote_symbol") or event.metadata.get("quote_symbol") or "稳定币")
        pair_label = str(clmm_context.get("pair_label") or lp_context.get("pair_label") or "")

        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return f"池子承接 {quote_symbol} 流入，{token_symbol} 流出"
            if event.intent_type == "pool_sell_pressure":
                return f"池子承接 {token_symbol} 流入，{quote_symbol} 流出"
            if event.intent_type == "liquidity_addition":
                return f"{pair_label or f'{token_symbol}/{quote_symbol}'} 大额加池"
            if event.intent_type == "liquidity_removal":
                return f"{pair_label or f'{token_symbol}/{quote_symbol}'} 大额撤池"
            if event.intent_type == "pool_rebalance":
                return f"{pair_label or f'{token_symbol}/{quote_symbol}'} 池子再平衡"
            return f"{pair_label or f'{token_symbol}/{quote_symbol}'} 池子噪声流"
        if clmm_event:
            if event.intent_type == "clmm_position_open":
                return f"{pair_label or 'CLMM'} 开新头寸"
            if event.intent_type == "clmm_position_add_range_liquidity":
                return f"{pair_label or 'CLMM'} 区间加流动性"
            if event.intent_type == "clmm_position_remove_range_liquidity":
                return f"{pair_label or 'CLMM'} 区间撤流动性"
            if event.intent_type == "clmm_position_close":
                return f"{pair_label or 'CLMM'} 头寸退出"
            if event.intent_type == "clmm_passive_fee_harvest":
                return f"{pair_label or 'CLMM'} 被动收手续费"
            if event.intent_type == "clmm_jit_liquidity_likely":
                return f"{pair_label or 'CLMM'} 疑似 JIT 抽费"
            return f"{pair_label or 'CLMM'} 区间重设"
        if clmm_partial_support:
            protocol = str(clmm_context.get("protocol") or event.metadata.get("clmm_manager_protocol") or "clmm").strip()
            protocol_label = "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
            return f"{protocol_label} PositionManager 路径命中（部分支持）"

        if semantic == "active_trade":
            if event.side == "买入":
                return f"买入 {token_symbol}"
            if event.side == "卖出":
                return f"卖出 {token_symbol}"
            return f"交易 {token_symbol}"
        if semantic == "deposit_preparation":
            return f"{token_symbol} 向交易场景移动"
        if semantic == "withdraw_exit":
            return f"{token_symbol} 从交易场景转出"
        if semantic == "internal_rebalance":
            return f"{token_symbol} 在重点地址体系内转移"
        if semantic == "inventory_rebalance":
            return f"{token_symbol} 做市库存调节"
        if semantic == "buy_preparation":
            return f"{token_symbol} 向入场方向移动"
        if event.side == "流入":
            return f"{token_symbol} 流入"
        if event.side == "流出":
            return f"{token_symbol} 流出"
        return signal.type or f"{token_symbol} 转移"

    def _intent_detail(self, event: Event, lp_event: bool, clmm_event: bool, clmm_partial_support: bool) -> str:
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return "池子表现为稳定币流入、标的流出，更接近市场主动买入。"
            if event.intent_type == "pool_sell_pressure":
                return "池子表现为标的流入、稳定币流出，更接近市场主动卖出。"
            if event.intent_type == "liquidity_addition":
                return "池子两侧资产同步增加，更接近流动性补充。"
            if event.intent_type == "liquidity_removal":
                return "池子两侧资产同步减少，更接近流动性撤离。"
            if event.intent_type == "pool_rebalance":
                return "池子更像在做库存或区间再平衡，方向性较弱。"
            return "当前更像普通池子噪声，不宜直接下强结论。"
        if clmm_event:
            if event.intent_type == "clmm_passive_fee_harvest":
                return "当前更像头寸在不明显改动区间和仓位的前提下收取手续费，不宜解读为方向表达。"
            if event.intent_type == "clmm_jit_liquidity_likely":
                return "当前只可写成疑似 JIT fee extraction，不能写 confirmed，也不能直接写成方向性看多/看空。"
            if event.intent_type == "clmm_position_close":
                return "当前更像该头寸退出对应区间流动性，但不自动等于方向性看空。"
            if event.intent_type in {"clmm_range_shift", "clmm_inventory_recenter"}:
                return "当前更像 LP 在短窗内迁移或再居中重设价区，不直接等于主观方向判断。"
            if event.intent_type == "clmm_position_remove_range_liquidity":
                return "当前更像 LP 正在撤离对应价区流动性，需继续看是否还有后续撤离或重设。"
            return "当前更像 LP 头寸层的区间加流动性动作，不直接等于方向表达。"
        if clmm_partial_support:
            return "当前只确认命中了已知 CLMM PositionManager 路径，但还缺 decode primitives，不能写成已解析的 LP 头寸动作。"

        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            return "前序交易场景流入后，又出现同地址主流资产后续流入，当前更像结构延续观察，不等于已确认卖出。"
        if self._is_exchange_followup_case(event):
            return "当前已建立交易场景观察锚点，等待同地址后续资产流继续出现，或转成真实执行。"
        if event.intent_type == "swap_execution":
            return "这是重点地址已经执行完成的真实链上交易动作。"
        if event.intent_type in {"exchange_deposit_candidate", "possible_sell_preparation"}:
            return "路径指向交易场景，但当前仍以观察性解释为主。"
        if event.intent_type == "exchange_withdraw_candidate":
            return "路径显示资金正在离开交易场景，后续去向更关键。"
        if event.intent_type == "internal_rebalance":
            return "更接近重点地址体系内的调拨或归集。"
        if event.intent_type == "market_making_inventory_move":
            return "更接近做市库存调节，而不是主观建仓。"
        if event.intent_type == "possible_buy_preparation":
            return "更接近入场前的资金准备，尚未看到真实买入。"
        return "当前更像重点地址的一般资金动作，暂不下更强结论。"

    def _fact_label(self, event: Event, watch_context: dict | None, lp_event: bool, clmm_event: bool, clmm_partial_support: bool) -> str:
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        clmm_context = self._clmm_context(raw, event)
        token_symbol = str(lp_context.get("base_token_symbol") or raw.get("token_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        quote_symbol = str(lp_context.get("quote_token_symbol") or raw.get("quote_symbol") or event.metadata.get("quote_symbol") or "报价资产")
        pair_label = str(clmm_context.get("pair_label") or lp_context.get("pair_label") or f"{token_symbol}/{quote_symbol}")

        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return f"{pair_label} 池子出现买压：{quote_symbol} 流入池子，{token_symbol} 流出池子"
            if event.intent_type == "pool_sell_pressure":
                return f"{pair_label} 池子出现卖压：{token_symbol} 流入池子，{quote_symbol} 流出池子"
            if event.intent_type == "liquidity_addition":
                return f"{pair_label} 池子出现加池：双边流动性同步增加"
            if event.intent_type == "liquidity_removal":
                return f"{pair_label} 池子出现撤池：双边流动性同步减少"
            if event.intent_type == "pool_rebalance":
                return f"{pair_label} 池子发生再平衡：双边资金结构出现混合调整"
            return f"{pair_label} 池子出现弱资金扰动"
        if clmm_event:
            position_label = str(clmm_context.get("position_actor_label") or pair_label or "某 LP 头寸")
            if event.intent_type == "clmm_position_open":
                return f"{position_label} 在 {pair_label} 开出新价区头寸"
            if event.intent_type == "clmm_position_add_range_liquidity":
                return f"{position_label} 在 {pair_label} 对当前区间继续加流动性"
            if event.intent_type == "clmm_position_remove_range_liquidity":
                return f"{position_label} 在 {pair_label} 从当前区间撤出部分流动性"
            if event.intent_type == "clmm_position_close":
                return f"{position_label} 在 {pair_label} 退出该区间头寸"
            if event.intent_type == "clmm_passive_fee_harvest":
                return f"{position_label} 在 {pair_label} 更像收手续费而非改方向"
            if event.intent_type == "clmm_jit_liquidity_likely":
                return f"{position_label} 在 {pair_label} 出现短持有围绕冲击窗口的疑似 JIT 行为"
            return f"{position_label} 在 {pair_label} 出现区间迁移/再居中调整"
        if clmm_partial_support:
            protocol = str(clmm_context.get("protocol") or event.metadata.get("clmm_manager_protocol") or "clmm").strip()
            protocol_label = "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
            manager_address = str(
                event.metadata.get("clmm_manager_address")
                or clmm_context.get("position_manager")
                or ""
            ).strip()
            manager_label = format_address_label(manager_address) if manager_address else "未知 manager"
            return f"{protocol_label} PositionManager 路径命中：{manager_label}，当前仅到部分支持观察"

        flow_direction = (watch_context or {}).get("direction") or event.metadata.get("flow_direction") or event.side or ""
        if event.kind == "swap":
            if event.side == "买入":
                return f"链上已发生真实 swap：使用 {quote_symbol} 买入 {token_symbol}"
            if event.side == "卖出":
                return f"链上已发生真实 swap：卖出 {token_symbol} 换回 {quote_symbol}"
            return f"链上已发生真实 swap：{quote_symbol} <-> {token_symbol}"
        if event.kind == "token_transfer":
            if flow_direction == "流入" or event.side == "流入":
                return f"链上发生 token 转移：{token_symbol} 流入监控地址"
            if flow_direction == "流出" or event.side == "流出":
                return f"链上发生 token 转移：{token_symbol} 从监控地址流出"
            if flow_direction == "内部划转":
                return f"链上发生 token 内部划转：{token_symbol} 在监控体系内转移"
            return f"链上发生 token 转移：{token_symbol} 出现资金流动"
        if flow_direction == "流入" or event.side == "流入":
            return "链上发生原生资产转移：ETH 流入监控地址"
        if flow_direction == "流出" or event.side == "流出":
            return "链上发生原生资产转移：ETH 从监控地址流出"
        return "链上发生原生资产转移"

    def _inference_label(self, event: Event, intent_label: str, lp_event: bool) -> str:
        if lp_event:
            prefix = "池子当前更接近解释为"
        else:
            prefix = "系统当前更倾向解释为"
        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            return f"{prefix}：{intent_label}（已获结构延续型后续确认）"
        if event.intent_stage == "confirmed":
            return f"{prefix}：{intent_label}"
        if event.intent_stage == "preliminary":
            return f"{prefix}：{intent_label}"
        return f"{prefix}：{intent_label}（弱解释）"

    def _inference_confidence_label(self, event: Event, signal: Signal) -> str:
        composite = (
            0.45 * float(event.intent_confidence or 0.0)
            + 0.35 * float(event.confirmation_score or 0.0)
            + 0.20 * float(signal.confidence or 0.0)
        )
        if composite >= 0.84:
            return "高把握"
        if composite >= 0.70:
            return "中高把握"
        if composite >= 0.56:
            return "中等把握"
        if composite >= 0.42:
            return "低把握"
        return "很低把握"

    def _confirmation_label(self, event: Event, gate_metrics: dict, lp_event: bool) -> str:
        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            return "结构延续确认"
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        confirmation_score = float(event.confirmation_score or 0.0)
        if lp_event:
            if event.intent_stage == "confirmed" and confirmation_score >= 0.80:
                return "已看到同池/跨池确认"
            if event.intent_stage == "confirmed":
                return "已有池子确认"
            if resonance_score >= 0.45:
                return "有池间共振"
            return "规则型初判"
        if event.intent_stage == "confirmed" and confirmation_score >= 0.82:
            return "已看到短时确认"
        if event.intent_stage == "confirmed":
            return "已有一定确认"
        if event.intent_stage == "preliminary" and resonance_score >= 0.6:
            return "有辅助确认"
        if event.intent_stage == "preliminary":
            return "规则型初判"
        return "弱证据"

    def _evidence_summary(self, event: Event, gate_metrics: dict, lp_event: bool) -> str:
        evidence = list(event.intent_evidence or [])
        if self._is_exchange_followup_case(event):
            followup_detail = str(event.metadata.get("followup_detail") or "").strip()
            if bool(event.metadata.get("exchange_followup_anchor_event")):
                evidence.append("观察锚点已建立")
            if bool(event.metadata.get("followup_confirmed")):
                evidence.append("followup 已成立")
            if followup_detail:
                evidence.append(followup_detail)
            if bool(event.metadata.get("execution_required_but_missing")):
                evidence.append("当前仍缺少真实执行")
        if lp_event:
            lp_analysis = event.metadata.get("lp_analysis") or {}
            same_pool_continuity = int(lp_analysis.get("same_pool_continuity") or 0)
            multi_pool_resonance = int(lp_analysis.get("multi_pool_resonance") or 0)
            surge_ratio = float(lp_analysis.get("pool_volume_surge_ratio") or 0.0)
            if same_pool_continuity >= 1:
                evidence.append(f"同池连续 {same_pool_continuity + 1} 笔")
            if multi_pool_resonance >= 2:
                evidence.append(f"{multi_pool_resonance} 个主流池短时同向")
            if surge_ratio >= 1.2:
                evidence.append(f"量能放大 {surge_ratio:.2f}x")
            evidence.append(f"确认度 {float(event.confirmation_score or 0.0):.2f}")
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        same_side_addresses = int(gate_metrics.get("same_side_resonance_addresses") or 0)
        if not lp_event and resonance_score >= 0.6 and same_side_addresses >= 2:
            evidence.append(f"{same_side_addresses} 个地址短时同向共振")
        if not evidence:
            return "当前主要来自单笔规则判断，尚缺更多连续确认。"
        unique = []
        for item in evidence:
            if item not in unique:
                unique.append(item)
        return "；".join(unique[:3])

    def _continuous_count(self, event: Event, address_snapshot: dict, lp_event: bool) -> int:
        recent = address_snapshot.get("windows", {}).get("5m", {}).get("recent") or address_snapshot.get("recent") or []
        if not recent:
            return 1
        now_ts = int(event.ts or 0)
        count = 0
        for item in recent:
            if item.tx_hash == event.tx_hash:
                continue
            if now_ts - int(item.ts or now_ts) > self.continuous_window_sec:
                continue
            if lp_event:
                if item.intent_type == event.intent_type or str(item.side or "") == str(event.side or ""):
                    count += 1
                continue
            if (item.token or "").lower() != (event.token or "").lower():
                continue
            if (item.side or "") != (event.side or ""):
                continue
            if item.kind != event.kind:
                continue
            count += 1
        return max(count + 1, 1)

    def _continuous_label(self, event: Event, count: int, lp_event: bool) -> str:
        window_min = max(int(self.continuous_window_sec / 60), 1)
        if count <= 1:
            return "单笔"
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return f"同池连续买压（{count}笔 / {window_min}分钟）"
            if event.intent_type == "pool_sell_pressure":
                return f"同池连续卖压（{count}笔 / {window_min}分钟）"
            if event.intent_type == "liquidity_addition":
                return f"同池连续加池（{count}笔 / {window_min}分钟）"
            if event.intent_type == "liquidity_removal":
                return f"同池连续撤池（{count}笔 / {window_min}分钟）"
            return f"同池连续结构变化（{count}笔 / {window_min}分钟）"
        if event.side == "买入":
            return f"连续买入（{count}笔 / {window_min}分钟）"
        if event.side == "卖出":
            return f"连续卖出（{count}笔 / {window_min}分钟）"
        return f"连续同向（{count}笔 / {window_min}分钟）"

    def _abnormal_ratio(self, event: Event, address_snapshot: dict) -> float:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        if float(lp_analysis.get("abnormal_ratio") or 0.0) > 0:
            return float(lp_analysis.get("abnormal_ratio") or 0.0)
        avg_usd_24h = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        usd_value = float(event.usd_value or 0.0)
        if avg_usd_24h <= 0:
            return 0.0
        return usd_value / avg_usd_24h

    def _abnormal_label(self, event: Event, abnormal_ratio: float, lp_event: bool) -> str:
        if abnormal_ratio <= 0:
            return "无历史基线"
        if lp_event:
            return f"池子基线 {abnormal_ratio:.2f}x"
        if abnormal_ratio >= self.abnormal_multiplier:
            return f"异常放大 {abnormal_ratio:.2f}x"
        return f"历史均值 {abnormal_ratio:.2f}x"

    def _pricing_label(self, event: Event) -> str:
        status = str(event.pricing_status or "unknown")
        confidence = float(event.pricing_confidence or 0.0)
        source_label = self._pricing_source_label(event.pricing_source)
        if not event.usd_value_available:
            return f"未定价｜{source_label} ({confidence:.2f})"
        if status == "exact":
            return f"精确定价｜{source_label} ({confidence:.2f})"
        if status == "estimated" or event.usd_value_estimated:
            return f"估算定价｜{source_label} ({confidence:.2f})"
        return f"{status}｜{source_label} ({confidence:.2f})"

    def _pricing_source_label(self, pricing_source: str | None) -> str:
        source = str(pricing_source or "none")
        return self.PRICING_SOURCE_LABELS.get(source, source)

    def _trade_value_label(
        self,
        event: Event,
        signal: Signal,
        continuous_count: int,
        abnormal_ratio: float,
        gate_metrics: dict,
        lp_event: bool,
    ) -> str:
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        if lp_event:
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                if event.intent_stage == "confirmed" and (continuous_count >= 2 or resonance_score >= 0.55 or abnormal_ratio >= 1.8):
                    return "策略级"
                if continuous_count >= 2 or abnormal_ratio >= 1.4:
                    return "跟踪级"
                return "观察级"
            if event.intent_type == "liquidity_removal":
                if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.6):
                    return "跟踪级"
                return "观察级"
            if event.intent_type == "liquidity_addition":
                return "观察级" if event.intent_stage != "weak" else "噪声级"
            if event.intent_type == "pool_rebalance":
                return "观察级" if event.intent_stage == "confirmed" else "噪声级"
            return "噪声级"

        if event.intent_stage == "weak":
            return "噪声级" if event.intent_type in {"pure_transfer", "unknown_intent", "internal_rebalance"} else "观察级"
        if event.intent_type == "swap_execution":
            if event.intent_stage == "confirmed" and str(signal.tier or "") == "Tier 1" and float(signal.confidence or 0.0) >= 0.86:
                return "执行级"
            if event.intent_stage == "confirmed" and (str(signal.tier or "") in {"Tier 1", "Tier 2"} or resonance_score >= 0.65):
                return "策略级"
            if continuous_count >= 2 or abnormal_ratio >= 1.8:
                return "跟踪级"
            return "观察级"
        if event.intent_type in {"exchange_deposit_candidate", "exchange_withdraw_candidate", "possible_buy_preparation", "possible_sell_preparation"}:
            if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8):
                return "跟踪级"
            return "观察级"
        if event.intent_type == "market_making_inventory_move":
            return "跟踪级" if event.intent_stage == "confirmed" and abnormal_ratio >= 2.5 else "观察级"
        return "观察级" if event.intent_stage == "confirmed" else "噪声级"

    def _directional_bias(
        self,
        event: Event,
        signal: Signal,
        continuous_count: int,
        abnormal_ratio: float,
        gate_metrics: dict,
        lp_event: bool,
    ) -> str:
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return "偏多" if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8 or resonance_score >= 0.55) else "中偏多"
            if event.intent_type == "pool_sell_pressure":
                return "偏空" if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8 or resonance_score >= 0.55) else "中偏空"
            if event.intent_type == "liquidity_addition":
                return "中性"
            if event.intent_type == "liquidity_removal":
                return "中偏空" if event.intent_stage != "weak" else "中性"
            if event.intent_type == "pool_rebalance":
                return "中性"
            return "不明"

        if event.intent_stage == "weak":
            if event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
                return "中性"
            return "不明"
        if event.intent_type == "swap_execution":
            if event.side == "买入":
                return "偏多" if event.intent_stage == "confirmed" and float(signal.confidence or 0.0) >= 0.84 else "中偏多"
            if event.side == "卖出":
                return "偏空" if event.intent_stage == "confirmed" and float(signal.confidence or 0.0) >= 0.84 else "中偏空"
            return "不明"
        if self._is_exchange_followup_case(event) and not bool(event.metadata.get("followup_confirmed")):
            return "中性观察"
        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            if str(event.metadata.get("followup_semantic") or "") == "execution_confirmed" and event.kind == "swap":
                return "中偏空"
            return "中性观察"
        if event.intent_type in {"exchange_deposit_candidate", "possible_sell_preparation"}:
            return "中偏空" if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8) else "中性"
        if event.intent_type == "exchange_withdraw_candidate":
            return "中偏多" if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8) else "中性"
        if event.intent_type == "possible_buy_preparation":
            return "中偏多" if event.intent_stage == "confirmed" and (continuous_count >= 2 or abnormal_ratio >= 1.8) else "中性"
        if event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
            return "中性"
        return "不明"

    def _followup_checks(self, event: Event, continuous_count: int, abnormal_ratio: float, lp_event: bool) -> list[str]:
        checks = []
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                checks.extend([
                    "观察同池后续是否继续出现稳定币流入、标的流出",
                    "观察是否有第二个主流池同步出现买压",
                    "观察短线净流与价格是否继续共振",
                ])
            elif event.intent_type == "pool_sell_pressure":
                checks.extend([
                    "观察同池后续是否继续出现标的流入、稳定币流出",
                    "观察是否有第二个主流池同步出现卖压",
                    "观察短线净流与价格是否继续偏空共振",
                ])
            elif event.intent_type == "liquidity_addition":
                checks.extend([
                    "观察是否继续有大额加池补充深度",
                    "观察后续 swap 冲击是否被明显吸收",
                    "观察是否仅为单次补池而非连续结构增强",
                ])
            elif event.intent_type == "liquidity_removal":
                checks.extend([
                    "观察是否继续有同池或跨池撤池动作",
                    "观察后续 swap 冲击是否放大",
                    "观察波动是否随深度下降而抬升",
                ])
            else:
                checks.extend([
                    "观察后续是否转成更明确的买压或卖压",
                    "观察同池是否继续出现结构调整",
                    "观察是否有其他主流池出现共振",
                ])
        elif self._is_exchange_followup_case(event):
            if bool(event.metadata.get("followup_confirmed")):
                checks.extend([
                    "观察同一交易所地址是否继续出现更多主流资产流入",
                    "观察是否很快出现反向流出，削弱该连续确认",
                    "观察该连续流是否最终转成更明确交易执行",
                ])
            else:
                checks.extend([
                    "观察 5-15 分钟内同一交易所地址是否再出现 WETH/WBTC/ETH-equivalent 流入",
                    "观察是否形成稳定币先行、主流资产跟随的连续路径",
                    "观察是否出现反向流出或内部划转，削弱该锚点解释",
                ])
        else:
            checks.extend([
                "观察后续 5-15 分钟内是否继续出现同地址连续动作",
                "观察是否出现第二跳进入交易场景",
                "观察是否有其他重点地址同步动作",
            ])

        if continuous_count >= 3 or abnormal_ratio >= 2.5:
            checks.insert(0, "观察该行为是否在短时间内继续放大")

        unique = []
        for item in checks:
            if item not in unique:
                unique.append(item)
        return unique[:3]

    def _followup_summary(self, followup_checks: list[str], followup_stage: str, followup_status: str) -> str:
        prefix = ""
        if followup_status == "confirmed":
            prefix = "已确认案例："
        elif followup_status == "developing":
            prefix = "跟踪中："
        elif followup_status == "cooled":
            prefix = "冷却观察："
        elif followup_stage == "anchor_tracking":
            prefix = "锚点已建立："
        elif followup_stage == "initial":
            prefix = "新开案例："
        if not followup_checks:
            return f"{prefix}观察后续是否形成连续确认".strip()
        return f"{prefix}{'；'.join(followup_checks[:2])}".strip()

    def _market_implication(self, event: Event, directional_bias: str, continuous_count: int, abnormal_ratio: float, lp_event: bool) -> str:
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                return "主流池出现买压，若同池连续或跨池共振继续增强，短线更偏多。"
            if event.intent_type == "pool_sell_pressure":
                return "主流池出现卖压，若同池连续或跨池共振继续增强，短线更偏空。"
            if event.intent_type == "liquidity_addition":
                return "池子深度增强，方向性偏中性，但后续大额冲击的吸收能力会提高。"
            if event.intent_type == "liquidity_removal":
                return "池子深度下降，若继续撤池，后续价格冲击和波动风险可能放大。"
            if event.intent_type == "pool_rebalance":
                return "当前更像池子库存再平衡，方向性不强，先观察是否转成明确压力。"
            return "当前更像普通池子噪声，市场方向暂不明确。"

        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            return "交易场景前序流入后，同地址又出现主流资产继续流入，当前更像结构延续观察；仍需继续看是否出现真实执行、更强共振或连续同向放大。"
        if event.intent_type == "swap_execution":
            if event.side == "买入":
                return "已出现真实买入执行，若后续继续同向放大，短线更偏多。"
            if event.side == "卖出":
                return "已出现真实卖出执行，若后续继续同向放大，短线更偏空。"
        if event.intent_type in {"exchange_deposit_candidate", "possible_sell_preparation"}:
            return "更接近资金进入交易场景，当前偏空观察；只有继续连续放大，才值得进一步升级。"
        if event.intent_type == "exchange_withdraw_candidate":
            return "更接近标的离开交易场景，当前中偏多观察；需看是否继续连续外流。"
        if event.intent_type == "possible_buy_preparation":
            return "更接近入场前资金准备，当前偏多观察；只有后续转成真实买入，才具备更强交易价值。"
        if event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
            return "当前更像内部调拨或库存调节，市场方向偏中性，不宜直接下交易结论。"
        if directional_bias == "不明":
            return "当前主要具备行为情报价值，市场方向仍不明确。"
        return f"当前更偏{directional_bias}观察，但还需要连续性和后续路径进一步确认。"

    def _upgrade_conditions(self, event: Event, continuous_count: int, abnormal_ratio: float, lp_event: bool) -> list[str]:
        if lp_event:
            if event.intent_type == "pool_buy_pressure":
                conditions = [
                    "同池继续出现稳定币流入、标的流出",
                    "更多主流池短时出现同向买压",
                    "短线净流和价格冲击继续扩大",
                ]
            elif event.intent_type == "pool_sell_pressure":
                conditions = [
                    "同池继续出现标的流入、稳定币流出",
                    "更多主流池短时出现同向卖压",
                    "短线净流和价格冲击继续扩大",
                ]
            elif event.intent_type == "liquidity_addition":
                conditions = [
                    "同池继续有连续大额加池",
                    "加池后成交冲击明显被吸收",
                    "更多主流池同步补充深度",
                ]
            elif event.intent_type == "liquidity_removal":
                conditions = [
                    "同池继续有连续大额撤池",
                    "更多主流池同步出现撤池",
                    "撤池后价格冲击和波动同步放大",
                ]
            else:
                conditions = [
                    "后续转成更明确的池子买压或卖压",
                    "同池继续出现连续结构变化",
                    "更多主流池同步共振",
                ]
        elif self._is_exchange_followup_case(event):
            if bool(event.metadata.get("followup_confirmed")):
                conditions = [
                    "同一交易所地址继续出现更多主流资产流入",
                    "后续继续转成真实卖出或更明确交易执行",
                    "更多交易所地址同步出现类似连续流入",
                ]
            else:
                conditions = [
                    "5-15 分钟内同一交易所地址出现 WETH/WBTC/ETH-equivalent 流入",
                    "形成稳定币先行、主流资产跟随的连续路径",
                    "更多高质量地址出现同向共振",
                ]
        else:
            conditions = [
                "后续继续出现同向资金流",
                "出现第二跳进入交易场景",
                "更多重点地址同步跟进",
            ]
        if continuous_count >= 3 or abnormal_ratio >= 2.5:
            conditions.insert(0, "短时间内继续同向放大")
        return conditions[:3]

    def _failure_conditions(self, event: Event, lp_event: bool) -> list[str]:
        if lp_event:
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                return [
                    "后续没有连续同向池子冲击",
                    "很快出现反向压力或反向池间共振",
                    "价格和净流没有继续确认",
                ]
            if event.intent_type == "liquidity_removal":
                return [
                    "后续没有继续撤池",
                    "很快有大额补池对冲",
                    "波动和冲击没有继续放大",
                ]
            if event.intent_type == "liquidity_addition":
                return [
                    "后续没有继续补池",
                    "深度增强没有体现到成交承接",
                    "只是单笔孤立加池",
                ]
            return [
                "后续没有连续动作",
                "没有跨池共振",
                "很快回到普通噪声水平",
            ]
        if self._is_exchange_followup_case(event):
            return [
                "超过短窗口仍未出现主流资产 followup",
                "很快出现反向流出或更像内部划转",
                "后续只剩零散噪声，没有形成连续确认",
            ]
        return [
            "后续没有连续动作",
            "缺少第二跳或共振确认",
            "很快出现反向路径",
        ]

    def _case_summary(self, event: Event, signal: Signal) -> str:
        metadata_case = event.metadata.get("case") or signal.metadata.get("case") or {}
        summary = str(metadata_case.get("summary") or event.metadata.get("case_summary") or "").strip()
        if summary:
            return summary
        followup_label = str(event.metadata.get("followup_label") or "").strip()
        if followup_label:
            return followup_label
        token_symbol = str(event.metadata.get("token_symbol") or event.token or "资产")
        if event.case_id:
            return f"{token_symbol} {event.intent_type} 跟踪案例"
        return ""

    def _next_observation_window(self, event: Event) -> str:
        if self._is_exchange_followup_case(event):
            return "30-60 分钟" if bool(event.metadata.get("followup_confirmed")) else "5-15 分钟"
        if event.followup_status == "confirmed":
            return "30-60 分钟"
        if event.followup_status == "developing":
            return "15-30 分钟"
        if event.followup_status == "cooled":
            return "1-4 小时"
        return "5-15 分钟"

    def _resonance_label(self, event: Event, gate_metrics: dict, lp_event: bool) -> str:
        if lp_event:
            lp_analysis = event.metadata.get("lp_analysis") or {}
            multi_pool_resonance = int(lp_analysis.get("multi_pool_resonance") or gate_metrics.get("lp_multi_pool_resonance") or 0)
            same_pool_continuity = int(lp_analysis.get("same_pool_continuity") or 0)
            if multi_pool_resonance >= 2:
                return f"{multi_pool_resonance} 池同向"
            if same_pool_continuity >= 1:
                return "同池连续"
            return "单池孤立"
        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            assets = list(event.metadata.get("followup_assets") or [])
            if assets:
                return f"同地址跨 token 确认（{'/'.join(assets[:3])})"
            return "同地址跨 token 确认"

        same_side_addresses = int(gate_metrics.get("same_side_resonance_addresses") or 0)
        high_quality = int(gate_metrics.get("same_side_resonance_high_quality_addresses") or 0)
        smart_money = int(gate_metrics.get("same_side_resonance_smart_money_addresses") or 0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        if same_side_addresses < 2:
            return "单地址孤立"
        label = f"{same_side_addresses} 地址同向"
        if high_quality >= 2:
            label += f"，{high_quality} 高质量"
        if smart_money >= 2:
            label += f"，{smart_money} 聪明钱"
        if resonance_score >= 0.65:
            label += "，共振较强"
        return label

    def _is_exchange_followup_case(self, event: Event) -> bool:
        return str(event.metadata.get("case_family") or "") == "exchange_cross_token_followup"

    def _is_downstream_followup_case(self, event: Event) -> bool:
        return str(event.metadata.get("case_family") or "") == "downstream_counterparty_followup"

    def _lp_message_variant(self, event: Event) -> str:
        if str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"}:
            return "lp_directional"
        if str(event.intent_type or "") in {"liquidity_addition", "liquidity_removal", "pool_rebalance"}:
            return "lp_liquidity"
        return "alert"

    def _message_variant(self, event: Event, signal: Signal, lp_event: bool, watch_meta: dict) -> str:
        if self._is_downstream_followup_case(event):
            return "downstream_followup"
        liquidation_stage = str(event.metadata.get("liquidation_stage") or "none")
        if liquidation_stage == "execution":
            return "liquidation_execution"
        if liquidation_stage == "risk":
            return "liquidation_risk"
        if bool(event.metadata.get("followup_confirmed")):
            return "followup"
        if lp_event:
            return self._lp_message_variant(event)
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "")
        is_execution = (
            bool(signal.metadata.get("is_real_execution"))
            or event.kind == "swap"
            or str(event.intent_type or "") == "swap_execution"
        )
        if strategy_role in {"smart_money_wallet", "alpha_wallet", "market_maker_wallet", "celebrity_wallet"}:
            if not is_execution:
                return "alert"
            if strategy_role == "market_maker_wallet":
                if str(signal.delivery_class or "") == "primary":
                    return "market_maker_primary"
                return "market_maker_observe"
            if str(signal.delivery_class or "") == "primary":
                return "smart_money_primary"
            return "smart_money_observe"
        return "alert"

    def _smart_money_context(
        self,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        gate_metrics: dict,
        fact_brief: str,
        explanation_brief: str,
        evidence_brief: str,
        action_hint: str,
    ) -> dict:
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        style_variant = "market_maker" if strategy_role == "market_maker_wallet" else "smart_money"
        if strategy_role not in {"smart_money_wallet", "alpha_wallet", "market_maker_wallet", "celebrity_wallet"}:
            return {
                "active": False,
                "message_variant": "alert",
                "observe_label": "",
                "fact_brief": "",
                "explanation_brief": "",
                "evidence_brief": "",
                "action_hint": "",
                "style_variant": "",
            }

        delivery_class = str(signal.delivery_class or "")
        is_execution = (
            bool(signal.metadata.get("is_real_execution"))
            or event.kind == "swap"
            or str(event.intent_type or "") == "swap_execution"
        )
        if not is_execution:
            return {
                "active": False,
                "message_variant": "alert",
                "observe_label": "",
                "fact_brief": "",
                "explanation_brief": "",
                "evidence_brief": "",
                "action_hint": "",
                "style_variant": style_variant,
            }

        threshold_ratio = float(
            gate_metrics.get("market_maker_threshold_ratio")
            or gate_metrics.get("smart_money_non_exec_threshold_ratio")
            or (
                (float(event.usd_value or 0.0) / float(gate_metrics.get("dynamic_min_usd") or 1.0))
                if float(gate_metrics.get("dynamic_min_usd") or 0.0) > 0
                else 0.0
            )
        )
        quality_score = float(gate_metrics.get("adjusted_quality_score") or signal.quality_score or 0.0)
        confirmation_score = float(event.confirmation_score or gate_metrics.get("confirmation_score") or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)
        inventory_context = bool(
            gate_metrics.get("market_maker_inventory_context")
            or signal.metadata.get("market_maker_inventory_context")
            or event.metadata.get("market_maker_inventory_context")
        )
        context_confirmed = bool(
            gate_metrics.get("market_maker_context_confirmed")
            or signal.metadata.get("market_maker_context_confirmed")
            or event.metadata.get("market_maker_context_confirmed")
        )
        exception_applied = bool(
            gate_metrics.get("market_maker_observe_exception_applied")
            or gate_metrics.get("smart_money_non_exec_exception_applied")
            or signal.metadata.get("market_maker_observe_exception_applied")
            or signal.metadata.get("smart_money_non_exec_exception_applied")
        )
        observe_label = "Smart Money 执行观察雷达"
        message_variant = "smart_money_primary" if delivery_class == "primary" else "smart_money_observe"
        if style_variant == "market_maker":
            observe_label = "Market Maker 库存观察雷达"
            message_variant = "market_maker_primary" if delivery_class == "primary" else "market_maker_observe"
            sm_explanation = (
                "已看到做市地址真实执行，当前先按库存管理 / 流动性供给语境跟踪。"
                if delivery_class == "primary"
                else "已看到做市地址真实执行，但当前更像库存管理 / 流动性供给链路，先继续观察确认。"
            )
        else:
            sm_explanation = (
                "已看到更强的聪明钱真实执行，当前优先按执行路径跟踪。"
                if delivery_class == "primary"
                else "已看到真实执行，但当前更适合继续观察确认，不急于放大解读。"
            )

        evidence_items = []
        if threshold_ratio > 0:
            evidence_items.append(f"金额 {threshold_ratio:.2f}x 门槛")
        if confirmation_score >= 0.46:
            evidence_items.append(f"确认 {confirmation_score:.2f}")
        if resonance_score >= 0.32:
            evidence_items.append(f"共振 {resonance_score:.2f}")
        if quality_score >= 0.60:
            evidence_items.append(f"质量 {quality_score:.2f}")
        if style_variant == "market_maker" and inventory_context:
            evidence_items.append("库存语境")
        if style_variant == "market_maker" and context_confirmed:
            evidence_items.append("做市确认")
        if exception_applied:
            evidence_items.append("gate 例外保留")

        sm_action_hint = (
            "继续看同 token 是否出现连续库存回补、报价切换或跨地址共振。"
            if style_variant == "market_maker"
            else "继续看是否出现连续执行、同地址放量或多聪明钱共振。"
        )

        return {
            "active": True,
            "message_variant": message_variant,
            "observe_label": observe_label,
            "fact_brief": fact_brief,
            "explanation_brief": sm_explanation or explanation_brief,
            "evidence_brief": "｜".join(evidence_items[:4]) if evidence_items else evidence_brief,
            "action_hint": sm_action_hint,
            "style_variant": style_variant,
        }

    def _market_maker_display_context(self, smart_money_context: dict) -> dict:
        if str(smart_money_context.get("style_variant") or "") != "market_maker":
            return {
                "market_maker_observe_label": "",
                "market_maker_fact_brief": "",
                "market_maker_explanation_brief": "",
                "market_maker_evidence_brief": "",
                "market_maker_action_hint": "",
            }
        return {
            "market_maker_observe_label": smart_money_context.get("observe_label", ""),
            "market_maker_fact_brief": smart_money_context.get("fact_brief", ""),
            "market_maker_explanation_brief": smart_money_context.get("explanation_brief", ""),
            "market_maker_evidence_brief": smart_money_context.get("evidence_brief", ""),
            "market_maker_action_hint": smart_money_context.get("action_hint", ""),
        }

    def _build_operational_intent(
        self,
        *,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        counterparty_meta: dict,
        gate_metrics: dict,
        clmm_event: bool,
        clmm_partial_support: bool,
        clmm_context: dict,
        lp_event: bool,
        liquidation_meta: dict,
        sweep_meta: dict,
        actor_label: str,
        object_label: str,
        pair_label: str,
    ) -> dict:
        role_group = strategy_role_group(watch_meta.get("strategy_role") or event.strategy_role)
        if clmm_partial_support:
            payload = self._clmm_partial_support_operational_intent(
                event=event,
                clmm_context=clmm_context,
                pair_label=pair_label,
            )
        elif clmm_event:
            payload = self._clmm_operational_intent(
                event=event,
                clmm_context=clmm_context,
                gate_metrics=gate_metrics,
                pair_label=pair_label,
            )
        elif liquidation_meta["active"]:
            payload = self._liquidation_operational_intent(
                event=event,
                liquidation_meta=liquidation_meta,
                pair_label=pair_label,
            )
        elif lp_event:
            payload = self._lp_operational_intent(
                event=event,
                sweep_meta=sweep_meta,
                pair_label=pair_label,
            )
        elif role_group == "exchange":
            payload = self._exchange_operational_intent(
                event=event,
                watch_meta=watch_meta,
                counterparty_meta=counterparty_meta,
                actor_label=actor_label,
                object_label=object_label,
            )
        elif role_group == "market_maker":
            payload = self._market_maker_operational_intent(
                event=event,
                signal=signal,
                watch_meta=watch_meta,
                counterparty_meta=counterparty_meta,
                gate_metrics=gate_metrics,
                actor_label=actor_label,
                object_label=object_label,
            )
        elif role_group == "smart_money":
            payload = self._smart_money_operational_intent(
                event=event,
                signal=signal,
                watch_meta=watch_meta,
                counterparty_meta=counterparty_meta,
                gate_metrics=gate_metrics,
                actor_label=actor_label,
                object_label=object_label,
            )
        else:
            payload = {}

        normalized = self._empty_operational_intent()
        normalized.update(payload)
        confidence = float(normalized.get("operational_intent_confidence") or 0.0)
        normalized["operational_intent_confidence"] = round(max(0.0, min(1.0, confidence)), 3)
        normalized["operational_intent_confidence_label"] = self._operational_confidence_label(confidence)
        why_value = normalized.get("operational_intent_why")
        if isinstance(why_value, str):
            normalized["operational_intent_why"] = why_value.strip()
        else:
            normalized["operational_intent_why"] = self._join_operational_evidence(why_value)
        normalized["operational_intent_not_yet"] = str(normalized.get("operational_intent_not_yet") or "").strip()
        normalized["operational_intent_next_check"] = str(normalized.get("operational_intent_next_check") or "").strip()
        normalized["operational_intent_market_implication"] = str(normalized.get("operational_intent_market_implication") or "").strip()
        normalized["operational_intent_time_horizon"] = str(normalized.get("operational_intent_time_horizon") or "").strip()
        normalized["operational_intent_invalidation"] = str(normalized.get("operational_intent_invalidation") or "").strip()
        normalized["venue_or_position_context"] = str(normalized.get("venue_or_position_context") or "").strip()
        return normalized

    def _exchange_operational_intent(
        self,
        *,
        event: Event,
        watch_meta: dict,
        counterparty_meta: dict,
        actor_label: str,
        object_label: str,
    ) -> dict:
        raw = event.metadata.get("raw") or {}
        purpose = str(event.metadata.get("exchange_transfer_purpose_family") or event.metadata.get("exchange_transfer_purpose") or raw.get("exchange_transfer_purpose_family") or raw.get("exchange_transfer_purpose") or "exchange_unknown_flow")
        internality = str(event.metadata.get("exchange_internality") or raw.get("exchange_internality") or "no")
        same_entity_strength = str(
            event.metadata.get("exchange_same_entity_strength")
            or raw.get("exchange_same_entity_strength")
            or internality
            or "no"
        )
        purpose_strength = str(
            event.metadata.get("exchange_transfer_purpose_strength")
            or raw.get("exchange_transfer_purpose_strength")
            or (
                same_entity_strength
                if purpose in {
                    "exchange_user_deposit_consolidation",
                    "exchange_hot_wallet_overflow_to_cold",
                    "exchange_hot_wallet_cold_wallet_topup",
                    "exchange_trading_desk_funding",
                    "exchange_trading_desk_return",
                    "exchange_internal_rebalance",
                }
                else "no"
            )
        )
        transfer_confidence = float(event.metadata.get("exchange_transfer_confidence") or raw.get("exchange_transfer_confidence") or 0.0)
        entity_label = str(event.metadata.get("exchange_entity_label") or raw.get("exchange_entity_label") or watch_meta.get("entity_label") or "").strip()
        wallet_function = str(event.metadata.get("watch_wallet_function") or watch_meta.get("wallet_function") or "unknown")
        counterparty_role_group = strategy_role_group(counterparty_meta.get("strategy_role") or "")
        known_followup_counterparty = counterparty_role_group in {"smart_money", "market_maker"} or bool(counterparty_meta.get("entity_label"))
        likely_internal = purpose_strength == "likely" or same_entity_strength == "likely"
        confirmed_internal = purpose_strength == "confirmed" or same_entity_strength == "confirmed"

        key = ""
        label = ""
        market_implication = ""
        next_check = ""
        invalidation = ""

        if purpose in {"exchange_user_deposit_consolidation", "exchange_internal_rebalance"}:
            key = "exchange_internal_consolidation"
            label = "疑似内部归集" if likely_internal else "内部归集"
            market_implication = (
                "当前更像交易所内部库存/用户资金归集，但 same-entity 仍待确认。"
                if likely_internal
                else "市场含义偏中性，更像交易所内部库存/用户资金归集。"
            )
            next_check = "是否继续回流同实体 hot/trading，或出现对外转出。"
            invalidation = "短窗内改为流向外部对手方，且不再命中 same-entity。"
        elif purpose in {"exchange_hot_wallet_overflow_to_cold", "exchange_hot_wallet_cold_wallet_topup"}:
            key = "exchange_hot_cold_rebalance"
            label = "疑似热冷调拨" if likely_internal else "热冷调拨"
            market_implication = (
                "更像交易所热冷钱包之间的疑似调拨，不直接等于外部执行。"
                if likely_internal
                else "更像交易所热冷钱包之间的内部调拨，不直接等于外部执行。"
            )
            next_check = "继续看是否回到 hot/trading 端，或后续进入外部路径。"
            invalidation = "后续直接流向外部对手方，且不再命中 same-entity。"
        elif purpose in {
            "exchange_trading_desk_funding",
            "exchange_trading_desk_return",
            "exchange_external_inflow",
        }:
            key = "exchange_liquidity_preparation"
            label = "疑似流动性准备" if likely_internal else "流动性准备"
            market_implication = (
                "更像交易所为后续流动性/交易席位做疑似准备，不直接等于方向执行。"
                if likely_internal
                else "更像交易所为后续流动性/交易席位做准备，不直接等于方向执行。"
            )
            next_check = "是否继续进入 trading/hot 路径，或短时出现成交延续。"
            invalidation = "后续没有形成同实体接力，且转而快速回流外部。"
        elif purpose == "exchange_hot_wallet_withdrawal_outflow":
            if known_followup_counterparty:
                key = "exchange_external_distribution_risk"
                label = "外部迁移/分发风险"
                market_implication = "对市场偏风险，但当前更像外部分发/迁移风险上升，不可直接写成已卖出。"
                next_check = "盯 smart money / maker / bridge / OTC / 新簇的后续 swap 或二跳分发。"
                invalidation = "资金回流同实体钱包，或后续没有外部延续。"
            else:
                key = "exchange_user_withdrawal_servicing"
                label = "外部出金服务"
                market_implication = "更像交易所服务用户出金，市场含义偏中性。"
                next_check = "继续看是否进入路由/桥/场外地址，或是否出现大额连续出金。"
                invalidation = "后续很快回流同实体内部，或缺少外部延续。"
        elif purpose == "exchange_external_outflow":
            key = "exchange_external_distribution_risk"
            label = "外部迁移/分发风险"
            market_implication = "偏风险，但仍只是外部迁移/分发风险升高，不应直接解读为已卖出。"
            next_check = "继续盯外部地址是否执行 swap、进桥、进 OTC 或分发新簇。"
            invalidation = "后续没有执行/分发确认，或资金很快回流同实体。"
        else:
            key = "exchange_liquidity_preparation" if wallet_function in {"exchange_hot", "exchange_trading"} else "exchange_internal_consolidation"
            label = "待继续确认"
            market_implication = "当前仍是交易所运营动作线索，方向性含义有限。"
            next_check = "继续看 same-entity 接力、外部二跳、是否命中执行地址。"
            invalidation = "观察窗内没有新的路径确认。"

        confidence = max(
            float(event.intent_confidence or 0.0) * 0.45
            + float(event.confirmation_score or 0.0) * 0.25
            + transfer_confidence * 0.30,
            transfer_confidence,
        )
        if confirmed_internal or internality == "confirmed":
            confidence = min(0.92, confidence + 0.10)
        elif likely_internal or internality == "likely":
            confidence = min(0.66, max(confidence, 0.44))

        actor = self._operational_actor_label(
            watch_meta,
            fallback=actor_label,
            default_unknown="交易所地址",
        )
        why_items = list(event.metadata.get("exchange_transfer_why") or raw.get("exchange_transfer_why") or [])
        why_items.append(f"purpose={purpose}")
        why_items.append(f"internality={internality}")
        why_items.append(f"purpose_strength={purpose_strength}")
        if entity_label:
            why_items.append(f"entity={entity_label}")
        return {
            "operational_intent_key": key,
            "operational_intent_family": key,
            "operational_intent_strength": "likely" if likely_internal else "confirmed" if confirmed_internal else "no",
            "operational_intent_label": label,
            "operational_intent_confidence": confidence,
            "operational_intent_why": why_items,
            "operational_intent_not_yet": (
                "same-entity 仍未完全确认，还不能把它写成确认过的内部业务动作。"
                if likely_internal
                else "还没看到明确外部执行或成交落点，不能写成市场方向执行。"
            ),
            "operational_intent_next_check": next_check,
            "operational_intent_market_implication": market_implication,
            "operational_intent_time_horizon": "短线到数小时",
            "operational_intent_invalidation": invalidation,
            "operational_actor_label": actor,
            "operational_object_label": self._operational_object_label(event, object_label),
        }

    def _clmm_partial_support_operational_intent(
        self,
        *,
        event: Event,
        clmm_context: dict,
        pair_label: str,
    ) -> dict:
        protocol = str(
            clmm_context.get("protocol")
            or event.metadata.get("clmm_manager_protocol")
            or "clmm"
        ).strip()
        protocol_label = "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
        manager_address = str(
            event.metadata.get("clmm_manager_address")
            or clmm_context.get("position_manager")
            or ""
        ).strip().lower()
        reason = str(
            event.metadata.get("clmm_partial_reason")
            or clmm_context.get("partial_reason")
            or ""
        ).strip()
        candidate_status = str(
            event.metadata.get("clmm_candidate_status")
            or clmm_context.get("parse_status")
            or "candidate_only"
        ).strip()
        venue_context = f"{protocol_label} · manager {format_address_label(manager_address) if manager_address else 'unknown'}"
        why_items = list(event.intent_evidence or [])
        if protocol:
            why_items.append(f"protocol={protocol}")
        if candidate_status:
            why_items.append(f"status={candidate_status}")
        if reason:
            why_items.append(f"reason={reason}")
        if manager_address:
            why_items.append(f"manager={manager_address}")
        return {
            "operational_intent_key": "clmm_partial_support_observation",
            "operational_intent_family": "clmm_partial_support_observation",
            "operational_intent_strength": "partial_support",
            "operational_intent_label": "V4 PositionManager 命中（部分支持）" if protocol == "uniswap_v4" else "CLMM manager path 命中（部分支持）",
            "operational_intent_confidence": 0.44,
            "operational_intent_why": why_items,
            "operational_intent_not_yet": "尚未完整解析 position action；不能写成开仓、加仓、减仓、收手续费或平仓。",
            "operational_intent_next_check": "补齐 decode primitives 后再看是否能升级为 parsed CLMM position event。",
            "operational_intent_market_implication": "这只是技术观察：命中了已知 manager path，不构成 LP 主体意图，也不构成市场方向结论。",
            "operational_intent_time_horizon": "工程观察",
            "operational_intent_invalidation": "后续证明该路径并非已登记 PositionManager，或补码后发现并非 position action。",
            "operational_actor_label": f"{protocol_label} PositionManager",
            "operational_object_label": pair_label or format_address_label(manager_address) or "CLMM manager path",
            "venue_or_position_context": venue_context,
        }

    def _smart_money_operational_intent(
        self,
        *,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        counterparty_meta: dict,
        gate_metrics: dict,
        actor_label: str,
        object_label: str,
    ) -> dict:
        is_execution = bool(signal.metadata.get("is_real_execution") or event.kind == "swap" or str(event.intent_type or "") == "swap_execution")
        if is_execution:
            key = "smart_money_entry_execution" if str(event.side or "") == "买入" else "smart_money_exit_execution"
            label = "已执行建仓" if key == "smart_money_entry_execution" else "已执行退出"
            market_implication = "已出现真实执行，短线方向性更强。"
            not_yet = ""
            next_check = "同地址是否继续同向执行，或短时进入交易所反向回吐。"
            invalidation = "短窗内被同地址明显反向成交覆盖。"
            confidence = min(
                0.96,
                0.56 * float(event.intent_confidence or 0.0)
                + 0.34 * float(event.confirmation_score or 0.0)
                + 0.10 * max(float(gate_metrics.get("resonance_score") or 0.0), 0.30),
            )
        else:
            key = "smart_money_preparation_only"
            label = "仅交易准备"
            market_implication = "目前仍停留在准备阶段，方向性证据有限。"
            not_yet = "还没看到 router + token + 短窗延续 + executed swap 的执行闭环。"
            next_check = "继续看 router 命中、swap_execution、是否进入交易所/桥/新簇。"
            invalidation = "观察窗结束仍无执行，或资金反向回流。"
            confidence = min(
                0.72,
                0.48 * float(event.intent_confidence or 0.0)
                + 0.26 * float(event.confirmation_score or 0.0)
                + 0.12 * max(float(gate_metrics.get("resonance_score") or 0.0), 0.10),
            )
            if str(event.intent_type or "") in {"exchange_deposit_candidate", "possible_sell_preparation"}:
                market_implication = "更像卖出准备 / 交易准备，不能写成已卖出。"

        actor = self._operational_actor_label(
            watch_meta,
            fallback=actor_label,
            default_unknown="某聪明钱地址",
        )
        why_items = list(event.intent_evidence or [])
        if is_execution:
            why_items.append("链上已出现 executed swap")
        else:
            why_items.append(f"intent={event.intent_type}")
        return {
            "operational_intent_key": key,
            "operational_intent_label": label,
            "operational_intent_confidence": confidence,
            "operational_intent_why": why_items,
            "operational_intent_not_yet": not_yet,
            "operational_intent_next_check": next_check,
            "operational_intent_market_implication": market_implication,
            "operational_intent_time_horizon": "分钟到数小时",
            "operational_intent_invalidation": invalidation,
            "operational_actor_label": actor,
            "operational_object_label": self._operational_object_label(event, object_label),
        }

    def _market_maker_operational_intent(
        self,
        *,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        counterparty_meta: dict,
        gate_metrics: dict,
        actor_label: str,
        object_label: str,
    ) -> dict:
        behavior_type = str(signal.behavior_type or event.metadata.get("behavior_type") or "")
        counterparty_wallet_function = str(event.metadata.get("counterparty_wallet_function") or counterparty_meta.get("wallet_function") or "unknown")
        settlement_like = counterparty_wallet_function in {"exchange_trading", "exchange_internal_buffer", "mm_settlement"}
        if settlement_like:
            key = "market_maker_settlement_move"
            label = "结算/轮转"
            market_implication = "更像做市结算或库存轮转，不应轻易给方向性 alpha。"
        elif behavior_type == "inventory_distribution" or str(event.side or "") in {"卖出", "流出"}:
            key = "market_maker_inventory_distribute"
            label = "库存分发"
            market_implication = "更像做市库存分发，不等于主观看空。"
        elif behavior_type == "inventory_expansion" or str(event.side or "") in {"买入", "流入"}:
            key = "market_maker_inventory_expand"
            label = "库存扩张"
            market_implication = "更像库存回补/扩张，不等于主观看多。"
        elif behavior_type in {"inventory_shift", "inventory_management"} or str(event.intent_type or "") == "market_making_inventory_move":
            key = "market_maker_inventory_recenter"
            label = "库存轮转"
            market_implication = "更像库存再居中/库存轮转，方向性有限。"
        else:
            key = "market_maker_inventory_recenter"
            label = "库存轮转"
            market_implication = "更像库存管理动作。"

        confidence = min(
            0.86,
            0.42 * float(event.intent_confidence or 0.0)
            + 0.28 * float(event.confirmation_score or 0.0)
            + 0.18 * max(float(gate_metrics.get("resonance_score") or 0.0), 0.12)
            + 0.12 * (1.0 if behavior_type in {"inventory_management", "inventory_shift", "inventory_expansion", "inventory_distribution"} else 0.0),
        )
        actor = self._operational_actor_label(
            watch_meta,
            fallback=actor_label,
            default_unknown="某做市地址",
        )
        why_items = list(event.intent_evidence or [])
        if settlement_like:
            why_items.append(f"counterparty_wallet_function={counterparty_wallet_function}")
        if behavior_type:
            why_items.append(f"behavior={behavior_type}")
        return {
            "operational_intent_key": key,
            "operational_intent_label": label,
            "operational_intent_confidence": confidence,
            "operational_intent_why": why_items,
            "operational_intent_not_yet": "当前仍以库存/结算语境优先，不把它直接写成主观看多看空。",
            "operational_intent_next_check": "继续看是否进入交易所 trading/settlement 路径，或是否出现连续同 token 轮转。",
            "operational_intent_market_implication": market_implication,
            "operational_intent_time_horizon": "分钟到数小时",
            "operational_intent_invalidation": "短窗内出现强执行闭环且脱离库存/结算语境。",
            "operational_actor_label": actor,
            "operational_object_label": self._operational_object_label(event, object_label),
        }

    def _clmm_operational_intent(
        self,
        *,
        event: Event,
        clmm_context: dict,
        gate_metrics: dict,
        pair_label: str,
    ) -> dict:
        raw_key = str(event.intent_type or clmm_context.get("intent_type") or "").strip()
        protocol = str(clmm_context.get("protocol") or "clmm").strip()
        token_id = str(clmm_context.get("token_id") or "").strip()
        tick_lower = clmm_context.get("tick_lower")
        tick_upper = clmm_context.get("tick_upper")
        position_key = str(clmm_context.get("position_key") or "").strip()
        actor = str(
            clmm_context.get("position_actor_label")
            or self._clmm_actor_label(protocol=protocol, token_id=token_id, pair_label=pair_label)
        )
        venue_context = self._clmm_position_context_brief(clmm_context, pair_label)

        if raw_key in {"clmm_range_shift", "clmm_inventory_recenter"}:
            key = "clmm_range_recenter"
            label = "区间重设"
            implication = "更像 LP 在追价/再居中重设区间，不直接等于主观看多看空。"
            not_yet = "还没看到该头寸完全退出流动性，也不能把它写成主观方向表达。"
            next_check = "继续看新价区是否保留、是否继续加减仓，以及近价深度是否迁移。"
            invalidation = "短窗内并未形成新区间承接，或后续只是单边退出而非重设。"
        elif raw_key == "clmm_jit_liquidity_likely":
            key = "clmm_jit_fee_extraction_likely"
            label = "疑似 JIT 抽费"
            implication = "更像围绕冲击窗口的疑似抽费行为，只能写 likely，不能写 confirmed。"
            not_yet = "缺少更长样本验证，不能把它写成 confirmed JIT，也不能推演主观方向。"
            next_check = "继续看持有期是否极短、是否紧贴大额 swap burst、是否快速撤出。"
            invalidation = "持有期明显拉长，或没有命中冲击窗口的前后夹层。"
        elif raw_key in {"clmm_passive_fee_harvest", "clmm_position_collect_fees"}:
            key = "clmm_passive_fee_harvest"
            label = "被动收手续费"
            implication = "更像在不明显改区间和仓位的情况下收手续费，不宜解读为方向性表态。"
            not_yet = "还没看到明确区间迁移或仓位撤离，不应写成 bullish / bearish。"
            next_check = "继续看 range/liquidity 是否基本不变，还是随后转成移仓/撤仓。"
            invalidation = "collect 后紧接着明显改区间或大幅改流动性。"
        elif raw_key == "clmm_position_remove_range_liquidity":
            key = "clmm_range_liquidity_remove"
            label = "区间撤流动性"
            implication = "该价区深度更可能变薄；若继续撤离近价流动性，短线冲击风险会上升。"
            not_yet = "当前只是头寸层撤流动性，不自动等于方向性卖出。"
            next_check = "继续看是否还有同池同 owner 的连续撤离，或是否转为新区间重设。"
            invalidation = "后续快速回补流动性，或只是单次微调并未持续。"
        elif raw_key == "clmm_position_close":
            key = "clmm_position_exit"
            label = "头寸退出"
            implication = "该头寸退出该区间流动性，但这仍不自动等于方向性看空。"
            not_yet = "还不能仅凭 close 推断其整体仓位方向或现货观点。"
            next_check = "继续看是否在邻近区间重开，还是整体撤离该池。"
            invalidation = "短窗内在邻近区间迅速重开并维持相近暴露。"
        else:
            key = "clmm_range_liquidity_add"
            label = "区间加流动性"
            implication = "更像 LP 在对应价区补充深度 / 建立头寸，不直接等于方向判断。"
            not_yet = "当前只是 position-level 加流动性，不应替 LP 推演心理状态。"
            next_check = "继续看是否保留该区间、是否扩展仓位、以及后续成交是否围绕该价区发生。"
            invalidation = "加池后很快撤回，或并未形成持续的区间驻留。"

        confidence = min(
            0.92,
            max(
                float(event.intent_confidence or 0.0),
                0.52 * float(event.intent_confidence or 0.0)
                + 0.30 * float(event.confirmation_score or 0.0)
                + 0.18 * max(float(gate_metrics.get("resonance_score") or 0.0), 0.10),
            ),
        )
        if key == "clmm_jit_fee_extraction_likely":
            confidence = min(confidence, 0.68)

        why_items = list(event.intent_evidence or [])
        if position_key:
            why_items.append(f"position={position_key}")
        if tick_lower is not None and tick_upper is not None:
            why_items.append(f"range=[{tick_lower},{tick_upper}]")
        if protocol:
            why_items.append(f"protocol={protocol}")

        return {
            "operational_intent_key": key,
            "operational_intent_label": label,
            "operational_intent_confidence": confidence,
            "operational_intent_why": why_items,
            "operational_intent_not_yet": not_yet,
            "operational_intent_next_check": next_check,
            "operational_intent_market_implication": implication,
            "operational_intent_time_horizon": "分钟到数小时",
            "operational_intent_invalidation": invalidation,
            "operational_actor_label": actor,
            "operational_object_label": pair_label or self._operational_object_label(event, pair_label),
            "venue_or_position_context": venue_context,
        }

    def _lp_operational_intent(
        self,
        *,
        event: Event,
        sweep_meta: dict,
        pair_label: str,
    ) -> dict:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            key = "lp_pool_buy_pressure"
            label = "买方清扫"
            implication = "短线偏多，但这是池子成交冲击，不是 LP 主体方向。"
        elif sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            key = "lp_pool_sell_pressure"
            label = "卖方清扫"
            implication = "短线偏空，但这是池子成交冲击，不是 LP 主体方向。"
        elif str(event.intent_type or "") == "pool_buy_pressure":
            key = "lp_pool_buy_pressure"
            label = "池子买压"
            implication = "更像池子层面的买压延续，不替 LP 主体发言。"
        elif str(event.intent_type or "") == "pool_sell_pressure":
            key = "lp_pool_sell_pressure"
            label = "池子卖压"
            implication = "更像池子层面的卖压延续，不替 LP 主体发言。"
        elif str(event.intent_type or "") == "liquidity_removal":
            key = "lp_liquidity_withdrawal_risk"
            label = "流动性抽离风险"
            implication = "更像池子深度下降与冲击风险抬升，不替 LP 主体发言。"
        else:
            key = "lp_liquidity_reinforcement"
            label = "流动性补充"
            implication = "更像池子深度补充，不替 LP 主体发言。"

        return {
            "operational_intent_key": key,
            "operational_intent_label": label,
            "operational_intent_confidence": min(0.92, max(float(event.intent_confidence or 0.0), float(event.confirmation_score or 0.0))),
            "operational_intent_why": list(event.intent_evidence or []) + [f"intent={event.intent_type}"],
            "operational_intent_not_yet": "不能替 LP 主体表态，只能描述池子层结构。 ",
            "operational_intent_next_check": "继续看同池连续、跨池共振、以及是否被清算事件接管。",
            "operational_intent_market_implication": implication,
            "operational_intent_time_horizon": "分钟级",
            "operational_intent_invalidation": "后续不再出现同向压力/结构延续。",
            "operational_actor_label": pair_label or "LP Pool",
            "operational_object_label": pair_label or "LP Pool",
            "venue_or_position_context": f"Pool Layer · {pair_label or 'LP Pool'}",
        }

    def _liquidation_operational_intent(
        self,
        *,
        event: Event,
        liquidation_meta: dict,
        pair_label: str,
    ) -> dict:
        if liquidation_meta["stage"] == "execution":
            key = "liquidation_sell_pressure_release"
            label = "清算压力释放"
            implication = "短线冲击已落地，优先盯是否继续连锁触发。"
            not_yet = ""
            invalidation = "后续没有继续连锁，且成交冲击快速衰减。"
        else:
            key = "liquidation_risk_building"
            label = "清算风险累积"
            implication = "风险在积累，但尚未等于已发生大规模清算。"
            not_yet = "还没看到更强执行级清算链条。"
            invalidation = "风险指标回落且未进入执行阶段。"
        return {
            "operational_intent_key": key,
            "operational_intent_label": label,
            "operational_intent_confidence": min(0.94, max(float(liquidation_meta.get("score") or 0.0), float(event.intent_confidence or 0.0))),
            "operational_intent_why": self._join_operational_evidence([
                f"stage={liquidation_meta['stage']}",
                *(list(liquidation_meta.get("protocols") or [])[:2]),
                str(liquidation_meta.get("reason") or ""),
            ]),
            "operational_intent_not_yet": not_yet,
            "operational_intent_next_check": "继续看协议命中扩散、同池连续冲击、是否升级到执行级。",
            "operational_intent_market_implication": implication,
            "operational_intent_time_horizon": "分钟级",
            "operational_intent_invalidation": invalidation,
            "operational_actor_label": pair_label or "清算相关池",
            "operational_object_label": pair_label or "清算相关池",
            "venue_or_position_context": f"Pool Layer · {pair_label or '清算相关池'}",
        }

    def _empty_operational_intent(self) -> dict:
        return {
            "operational_intent_key": "",
            "operational_intent_family": "",
            "operational_intent_strength": "",
            "operational_intent_label": "",
            "operational_intent_confidence": 0.0,
            "operational_intent_confidence_label": "低",
            "operational_intent_why": "",
            "operational_intent_not_yet": "",
            "operational_intent_next_check": "",
            "operational_intent_market_implication": "",
            "operational_intent_time_horizon": "",
            "operational_intent_invalidation": "",
            "operational_actor_label": "",
            "operational_object_label": "",
            "venue_or_position_context": "",
        }

    def _operational_confidence_label(self, confidence: float) -> str:
        if confidence >= 0.78:
            return "高"
        if confidence >= 0.56:
            return "中"
        return "低"

    def _join_operational_evidence(self, items) -> str:
        return "｜".join(self._dedup_text(items)[:4])

    def _clmm_actor_label(self, *, protocol: str, token_id: str, pair_label: str) -> str:
        protocol_label = "Uniswap v3" if protocol == "uniswap_v3" else "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
        if token_id:
            return f"{protocol_label} Position #{token_id}"
        if pair_label:
            return f"{pair_label} CLMM Position"
        return "某 LP 头寸"

    def _clmm_position_context_brief(self, clmm_context: dict, pair_label: str) -> str:
        protocol = str(clmm_context.get("protocol") or "clmm").strip()
        protocol_label = "Uniswap v3" if protocol == "uniswap_v3" else "Uniswap v4" if protocol == "uniswap_v4" else "CLMM"
        lower = clmm_context.get("tick_lower")
        upper = clmm_context.get("tick_upper")
        parts = [protocol_label]
        if pair_label:
            parts.append(pair_label)
        if lower is not None and upper is not None:
            parts.append(f"tick [{lower},{upper}]")
        return " · ".join([item for item in parts if item]) or "CLMM Position"

    def _outcome_tracking_seam(
        self,
        *,
        event: Event,
        pair_label: str,
        clmm_event: bool,
        clmm_context: dict,
        lp_event: bool,
        operational_intent: dict,
    ) -> dict:
        if not clmm_event and not lp_event:
            return {}
        windows = {
            window: {
                "price_follow_through": None,
                "additional_same_direction_flow": None,
                "pool_depth_change_followup": None,
                "position_continuation": None,
                "invalidation": None,
            }
            for window in ("5m", "15m", "1h")
        }
        return {
            "outcome_tracking_enabled": True,
            "outcome_tracking": {
                "schema_version": "clmm_lp_v1",
                "actor_type": "clmm_position" if clmm_event else "lp_pool",
                "pair_label": pair_label,
                "operational_intent_key": str(operational_intent.get("operational_intent_key") or ""),
                "anchor_tx_hash": str(event.tx_hash or ""),
                "position_key": str(clmm_context.get("position_key") or ""),
                "windows": windows,
            },
        }

    def _operational_actor_label(self, meta: dict, *, fallback: str, default_unknown: str) -> str:
        entity_label = str(meta.get("entity_label") or "").strip()
        wallet_function = str(meta.get("wallet_function") or "unknown")
        if entity_label and wallet_function != "unknown":
            return f"{entity_label} {WALLET_FUNCTION_LABELS.get(wallet_function, wallet_function)}"
        if entity_label:
            return entity_label
        label = str(meta.get("label") or "").strip()
        return label or fallback or default_unknown

    def _operational_object_label(self, event: Event, fallback: str) -> str:
        clmm_context = self._clmm_context(event.metadata.get("raw") or {}, event)
        if str(clmm_context.get("pair_label") or "").strip():
            return str(clmm_context.get("pair_label") or "").strip()
        token_symbol = str(event.metadata.get("token_symbol") or event.token or "").strip()
        return token_symbol or fallback or "资产"

    def _canonicalize_pool_semantics(self, event: Event, signal: Signal) -> None:
        canonical_intent = canonicalize_pool_semantic_key(event.intent_type)
        if canonical_intent:
            event.intent_type = canonical_intent
        canonical_signal_type = canonicalize_pool_semantic_key(signal.type)
        if canonical_signal_type:
            signal.type = canonical_signal_type
        canonical_signal_intent = canonicalize_pool_semantic_key(signal.intent_type)
        if canonical_signal_intent:
            signal.intent_type = canonical_signal_intent

    def _liquidation_meta(self, event: Event) -> dict:
        return {
            "active": str(event.metadata.get("liquidation_stage") or "none") in {"risk", "execution"},
            "stage": str(event.metadata.get("liquidation_stage") or "none"),
            "score": round(float(event.metadata.get("liquidation_score") or 0.0), 3),
            "side": str(event.metadata.get("liquidation_side") or "unknown"),
            "protocols": list(event.metadata.get("liquidation_protocols") or []),
            "reason": str(event.metadata.get("liquidation_reason") or ""),
            "evidence": list(event.metadata.get("liquidation_evidence") or []),
        }

    def _liquidation_action_label(self, liquidation_meta: dict) -> str:
        stage = liquidation_meta["stage"]
        side = liquidation_meta["side"]
        if stage == "execution":
            if side == "long_flush":
                return "疑似清算执行卖压"
            if side == "short_squeeze":
                return "疑似清算执行买压"
            return "疑似清算执行"
        if side == "long_flush":
            return "疑似清算风险卖压"
        if side == "short_squeeze":
            return "疑似清算风险买压"
        return "疑似清算风险"

    def _liquidation_fact_label(self, event: Event, liquidation_meta: dict) -> str:
        pool_label = self._pool_label(event, event.metadata.get("raw") or {}, self._actor_label(event.address, {}, True))
        if liquidation_meta["stage"] == "execution":
            return f"{pool_label} 已出现更强协议处置路径，当前较接近清算执行"
        if liquidation_meta["side"] == "long_flush":
            return f"{pool_label} 出现偏单边快速卖压，当前更像清算环境信号"
        if liquidation_meta["side"] == "short_squeeze":
            return f"{pool_label} 出现偏单边快速买压，当前更像空头回补/清算环境"
        return f"{pool_label} 出现疑似清算相关池子压力"

    def _liquidation_intent_detail(self, liquidation_meta: dict) -> str:
        protocols = "/".join(liquidation_meta["protocols"][:2]) if liquidation_meta["protocols"] else "协议"
        if liquidation_meta["stage"] == "execution":
            return f"已看到更强的 {protocols} 协议/处置证据，当前较接近清算执行，但仍保持疑似表述。"
        if liquidation_meta["side"] == "long_flush":
            return f"当前更像 {protocols} 相关的多头被动出清风险环境，不等于已确认强平。"
        if liquidation_meta["side"] == "short_squeeze":
            return f"当前更像 {protocols} 相关的空头回补/反向清算环境，不等于已确认强平。"
        return "当前更像清算风险环境信号，仍需后续协议/处置证据。"

    def _liquidation_confirmation_label(self, liquidation_meta: dict) -> str:
        if liquidation_meta["stage"] == "execution":
            return "协议/处置证据增强"
        if liquidation_meta["score"] >= 0.78:
            return "清算风险较强"
        return "清算风险初判"

    def _liquidation_inference_label(self, liquidation_meta: dict) -> str:
        if liquidation_meta["stage"] == "execution":
            return "系统当前更倾向解释为：疑似清算执行"
        return "系统当前更倾向解释为：疑似清算风险"

    def _liquidation_evidence_summary(self, liquidation_meta: dict, gate_metrics: dict) -> str:
        parts = []
        protocols = liquidation_meta["protocols"]
        if protocols:
            parts.append(f"协议 {('/'.join(protocols[:3]))}")
        if liquidation_meta["evidence"]:
            parts.extend(liquidation_meta["evidence"][:3])
        if not protocols and liquidation_meta["score"] > 0:
            parts.append(f"liquidation_score {liquidation_meta['score']:.2f}")
        return "；".join(self._dedup_text(parts)[:4]) or "当前仅有弱结构证据，仍需继续确认。"

    def _liquidation_directional_bias(self, liquidation_meta: dict) -> str:
        if liquidation_meta["side"] == "long_flush":
            return "偏空" if liquidation_meta["stage"] == "execution" else "中偏空"
        if liquidation_meta["side"] == "short_squeeze":
            return "偏多" if liquidation_meta["stage"] == "execution" else "中偏多"
        return "中性观察"

    def _liquidation_followup_checks(self, liquidation_meta: dict) -> list[str]:
        if liquidation_meta["stage"] == "execution":
            return [
                "观察同协议地址是否继续触发更多处置卖出/买入",
                "观察更多 ETH 主流池是否同步进入同方向 cluster",
                "观察是否快速被反向承接，削弱执行延续性",
            ]
        return [
            "观察是否新增 keeper / vault / auction 证据",
            "观察同池连续性是否升级为跨池 cluster",
            "观察风险信号是否在短窗内升级为执行级别",
        ]

    def _liquidation_market_implication(self, liquidation_meta: dict) -> str:
        if liquidation_meta["stage"] == "execution":
            if liquidation_meta["side"] == "long_flush":
                return "当前较接近协议清算执行卖出，短线偏空，但仍需留意是否出现被动抛压后的反抽。"
            if liquidation_meta["side"] == "short_squeeze":
                return "当前较接近协议清算/回补执行买入，短线偏多，但仍需留意是否快速回落。"
            return "当前较接近清算执行，需观察是否继续扩展。"
        if liquidation_meta["side"] == "long_flush":
            return "当前更像多头被动出清风险环境，偏空观察，不等于已确认强平。"
        if liquidation_meta["side"] == "short_squeeze":
            return "当前更像空头回补/反向清算环境，偏多观察，不等于已确认强平。"
        return "当前更像清算环境信号，先中性观察。"

    def _liquidation_upgrade_conditions(self, liquidation_meta: dict) -> list[str]:
        if liquidation_meta["stage"] == "execution":
            return [
                "同协议地址继续处置更多仓位",
                "更多主流池同步出现同方向协议处置",
                "protocol -> executor -> pool 路径继续重复出现",
            ]
        return [
            "命中明确 keeper / vault / auction 地址",
            "同协议短窗内重复触发类似处置路径",
            "同方向压力从单池扩展到跨池共振",
        ]

    def _liquidation_failure_conditions(self, liquidation_meta: dict) -> list[str]:
        if liquidation_meta["stage"] == "execution":
            return [
                "后续没有继续出现协议处置路径",
                "很快被反向流承接并失去连续性",
                "协议证据只剩单点命中，未继续扩展",
            ]
        return [
            "后续没有新增协议/keeper 证据",
            "同池连续与跨池共振迅速消退",
            "结构回到普通 LP 买压/卖压",
        ]

    def _dedup_text(self, values: list[str]) -> list[str]:
        unique = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in unique:
                unique.append(text)
        return unique

    def _first_present(self, *values):
        for value in values:
            if value is not None:
                return value
        return None

    def _pair_label(self, event: Event, raw: dict, pool_label: str) -> str:
        lp_context = raw.get("lp_context") or {}
        clmm_context = raw.get("clmm_context") or event.metadata.get("clmm_context") or {}
        return str(
            clmm_context.get("pair_label")
            or lp_context.get("pair_label")
            or event.metadata.get("pair_label")
            or pool_label
            or "LP Pool"
        )

    def _lp_sweep_meta(self, event: Event, gate_metrics: dict) -> dict:
        semantic_subtype = str(
            self._first_present(
                gate_metrics.get("lp_semantic_subtype"),
                gate_metrics.get("semantic_subtype"),
                event.metadata.get("lp_semantic_subtype"),
                event.metadata.get("semantic_subtype"),
                "",
            )
            or ""
        )
        sweep_confidence = str(
            self._first_present(
                gate_metrics.get("lp_sweep_confidence"),
                event.metadata.get("lp_sweep_confidence"),
                event.metadata.get("sweep_confidence"),
                "",
            )
            or ""
        )
        detected_value = self._first_present(
            gate_metrics.get("lp_sweep_detected"),
            event.metadata.get("lp_sweep_detected"),
            None,
        )
        if semantic_subtype or detected_value is not None:
            detected = bool(detected_value) or semantic_subtype in LP_SWEEP_SEMANTIC_SUBTYPES
            display_label = "买方清扫" if semantic_subtype == "buy_side_liquidity_sweep" else "卖方清扫" if semantic_subtype == "sell_side_liquidity_sweep" else ""
            return {
                "detected": detected,
                "semantic_subtype": semantic_subtype,
                "sweep_confidence": sweep_confidence or ("likely" if detected else ""),
                "display_label": display_label,
            }

        lp_analysis = event.metadata.get("lp_analysis") or {}
        lp_burst = event.metadata.get("lp_burst") or {}
        detected = detect_lp_liquidity_sweep(
            event=event,
            reserve_skew=float(self._first_present(gate_metrics.get("lp_reserve_skew"), lp_analysis.get("reserve_skew"), 0.0) or 0.0),
            action_intensity=float(self._first_present(gate_metrics.get("lp_action_intensity"), lp_analysis.get("action_intensity"), 0.0) or 0.0),
            same_pool_continuity=int(self._first_present(gate_metrics.get("lp_same_pool_continuity"), lp_analysis.get("same_pool_continuity"), 0) or 0),
            multi_pool_resonance=int(self._first_present(gate_metrics.get("lp_multi_pool_resonance"), lp_analysis.get("multi_pool_resonance"), 0) or 0),
            pool_volume_surge_ratio=float(self._first_present(gate_metrics.get("lp_pool_volume_surge_ratio"), lp_analysis.get("pool_volume_surge_ratio"), 0.0) or 0.0),
            lp_burst_event_count=int(self._first_present(gate_metrics.get("lp_burst_event_count"), lp_burst.get("lp_burst_event_count"), 0) or 0),
            lp_burst_window_sec=int(self._first_present(gate_metrics.get("lp_burst_window_sec"), lp_burst.get("lp_burst_window_sec"), 0) or 0),
            price_impact_ratio=float(self._first_present(gate_metrics.get("price_impact_ratio"), event.metadata.get("price_impact_ratio"), 0.0) or 0.0),
            min_price_impact_ratio=float(self._first_present(gate_metrics.get("lp_sweep_min_price_impact_ratio"), QUALITY_GATE_MIN_PRICE_IMPACT_RATIO) or QUALITY_GATE_MIN_PRICE_IMPACT_RATIO),
            liquidation_stage=str(self._first_present(gate_metrics.get("liquidation_stage"), event.metadata.get("liquidation_stage"), "none") or "none"),
        )
        return {
            "detected": bool(detected.get("detected")),
            "semantic_subtype": str(detected.get("semantic_subtype") or ""),
            "sweep_confidence": str(detected.get("sweep_confidence") or ""),
            "display_label": str(detected.get("display_label") or ""),
        }

    def _lp_sweep_action_label(self, event: Event, sweep_meta: dict) -> str:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return "买方清扫｜主动 swap 快速吃掉近价卖盘深度"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return "卖方清扫｜主动 swap 快速吃掉近价买盘深度"
        return self._lp_action_brief(event, "", "")

    def _lp_sweep_fact_label(self, event: Event, pair_label: str, sweep_meta: dict) -> str:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return f"{pair_label} 出现推断型买方清扫：主动 swap 在短时快速吃掉近价卖盘流动性"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return f"{pair_label} 出现推断型卖方清扫：主动 swap 在短时快速吃掉近价买盘流动性"
        return pair_label

    def _lp_sweep_intent_detail(self, event: Event, sweep_meta: dict) -> str:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return "当前更像主动 swap 在短时间内快速吃掉近价可用流动性，属于推断型买方清扫；这不是撤池，也不等于 LP 主体看多。"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return "当前更像主动 swap 在短时间内快速吃掉近价可用流动性，属于推断型卖方清扫；这不是撤池，也不等于 LP 主体看空。"
        return self._intent_detail(event, True, False)

    def _lp_sweep_market_implication(self, event: Event, sweep_meta: dict) -> str:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return "更接近短线主动买盘清扫近价深度并带来明显冲击，短线偏多；本质是成交冲击，不是 LP 主体方向。"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return "更接近短线主动卖盘清扫近价深度并带来明显冲击，短线偏空；本质是成交冲击，不是 LP 主体方向。"
        return self._market_implication(event, "", 1, 0.0, True)

    def _lp_sweep_meaning_brief(self, event: Event, sweep_meta: dict) -> str:
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return "推断型｜主动买盘快速吃掉近价流动性并带来冲击，不等于 LP 主体看多"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return "推断型｜主动卖盘快速吃掉近价流动性并带来冲击，不等于 LP 主体看空"
        return self._lp_meaning_brief(event, "", "")

    def _market_state_label(self, event: Event, liquidation_meta: dict, sweep_meta: dict) -> str:
        if liquidation_meta["stage"] == "execution":
            return "疑似清算执行"
        if liquidation_meta["stage"] == "risk":
            return "疑似清算风险"
        if sweep_meta["semantic_subtype"] == "buy_side_liquidity_sweep":
            return "买方清扫"
        if sweep_meta["semantic_subtype"] == "sell_side_liquidity_sweep":
            return "卖方清扫"
        if event.intent_type == "pool_buy_pressure":
            return "持续买压"
        if event.intent_type == "pool_sell_pressure":
            return "持续卖压"
        if event.intent_type == "liquidity_addition":
            return "流动性补充"
        if event.intent_type == "liquidity_removal":
            return "流动性抽离"
        if event.intent_type == "pool_rebalance":
            return "池子再平衡"
        return self.INTENT_LABELS.get(str(event.intent_type or "unknown_intent"), "意图未明")

    def _compact_evidence_brief(
        self,
        *,
        event: Event,
        gate_metrics: dict,
        liquidation_meta: dict,
        sweep_meta: dict,
        confirmation_label: str,
        abnormal_label: str,
        resonance_label: str,
        continuous_label: str,
    ) -> str:
        if liquidation_meta["active"]:
            return self._liquidation_evidence_pack(liquidation_meta)
        if str(event.intent_type or "") in LP_INTENTS:
            return self._lp_evidence_pack(event, gate_metrics, sweep_meta)
        return self._evidence_brief(
            confirmation_label=confirmation_label,
            abnormal_label=abnormal_label,
            resonance_label=resonance_label,
            continuous_label=continuous_label,
        )

    def _lp_evidence_pack(self, event: Event, gate_metrics: dict, sweep_meta: dict) -> str:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        lp_burst = event.metadata.get("lp_burst") or {}
        same_pool_continuity = int(self._first_present(gate_metrics.get("lp_same_pool_continuity"), lp_analysis.get("same_pool_continuity"), 0) or 0)
        multi_pool_resonance = int(self._first_present(gate_metrics.get("lp_multi_pool_resonance"), lp_analysis.get("multi_pool_resonance"), 0) or 0)
        pool_volume_surge_ratio = float(self._first_present(gate_metrics.get("lp_pool_volume_surge_ratio"), lp_analysis.get("pool_volume_surge_ratio"), 0.0) or 0.0)
        lp_burst_event_count = int(self._first_present(gate_metrics.get("lp_burst_event_count"), lp_burst.get("lp_burst_event_count"), 0) or 0)
        lp_burst_window_sec = int(self._first_present(gate_metrics.get("lp_burst_window_sec"), lp_burst.get("lp_burst_window_sec"), 0) or 0)
        price_impact_ratio = float(self._first_present(gate_metrics.get("price_impact_ratio"), event.metadata.get("price_impact_ratio"), 0.0) or 0.0)
        primary_pool = bool(self._first_present(gate_metrics.get("lp_trend_primary_pool"), event.metadata.get("lp_trend_primary_pool"), False))

        tokens = []
        if primary_pool and event.intent_type in {"liquidity_addition", "liquidity_removal"}:
            tokens.append("主池")
        if event.intent_type == "liquidity_removal":
            tokens.extend(["深度下降", "风险升高"])
        elif event.intent_type == "liquidity_addition":
            tokens.append("深度补充")
        if lp_burst_event_count >= 3 and lp_burst_window_sec > 0:
            tokens.append(f"{lp_burst_window_sec}s {lp_burst_event_count}笔")
        if same_pool_continuity >= 1:
            tokens.append(f"同池连续{same_pool_continuity + 1}")
        if multi_pool_resonance >= 2:
            tokens.append(f"跨池{multi_pool_resonance}")
        if pool_volume_surge_ratio >= 1.2:
            tokens.append(f"放量{pool_volume_surge_ratio:.1f}x")
        if sweep_meta["detected"] and price_impact_ratio > 0:
            tokens.append(f"冲击{price_impact_ratio:.2%}")

        deduped = self._dedup_text(tokens)
        if deduped:
            return "｜".join(deduped[:4])
        return self._evidence_brief("规则型初判", "无历史基线", "单池孤立", "单笔")

    def _liquidation_evidence_pack(self, liquidation_meta: dict) -> str:
        tokens = []
        protocols = list(liquidation_meta.get("protocols") or [])
        if protocols:
            tokens.append(f"{protocols[0]} 命中")
        side = str(liquidation_meta.get("side") or "")
        if side == "long_flush":
            tokens.append("卖压释放")
            tokens.append("短线偏空")
        elif side == "short_squeeze":
            tokens.append("买压释放")
            tokens.append("短线偏多")
        if liquidation_meta.get("stage") == "execution":
            tokens.insert(1 if tokens else 0, "执行级")
        else:
            tokens.append("风险升高")
        deduped = self._dedup_text(tokens)
        return "｜".join(deduped[:4]) or "协议线索｜风险升高"

    def _headline_label(self, directional_bias: str, trade_value_label: str, intent_label: str) -> str:
        parts = [str(trade_value_label or "").strip(), str(directional_bias or "").strip(), str(intent_label or "").strip()]
        return "｜".join([part for part in parts if part])

    def _fact_brief(self, event: Event, fact_label: str) -> str:
        amount_text = f"${float(event.usd_value or 0.0):,.2f}"
        return f"{fact_label}｜{amount_text}"

    def _explanation_brief(self, event: Event, intent_detail: str, market_implication: str) -> str:
        if self._is_exchange_followup_case(event) and bool(event.metadata.get("followup_confirmed")):
            return "结构延续观察升级，仍缺少更强交易证据"
        if event.intent_type == "swap_execution":
            return intent_detail
        return market_implication or intent_detail

    def _evidence_brief(
        self,
        confirmation_label: str,
        abnormal_label: str,
        resonance_label: str,
        continuous_label: str,
    ) -> str:
        parts = []
        for item in [confirmation_label, abnormal_label, resonance_label, continuous_label]:
            text = str(item or "").strip()
            if text and text not in parts:
                parts.append(text)
            if len(parts) >= 3:
                break
        return "｜".join(parts)

    def _action_hint(
        self,
        event: Event,
        upgrade_conditions: list[str],
        followup_checks: list[str],
        lp_event: bool,
    ) -> str:
        picks = []
        for item in list(upgrade_conditions or []) + list(followup_checks or []):
            text = str(item or "").strip()
            if text and text not in picks:
                picks.append(text)
            if len(picks) >= 2:
                break

        if picks:
            return " / ".join(picks)
        if lp_event:
            return "观察同池是否继续放大 / 是否出现跨池共振"
        if self._is_exchange_followup_case(event):
            return "观察是否出现真实执行 / 更强共振 / 连续同向放大"
        return "观察是否继续同向放大 / 是否出现第二跳确认"

    def _update_brief(self, event: Event) -> str:
        detail = str(event.metadata.get("followup_detail") or "").strip()
        if detail:
            return detail
        anchor_symbol = str((event.metadata.get("case") or {}).get("anchor_symbol") or "稳定币").strip() or "稳定币"
        assets = list(event.metadata.get("followup_assets") or [])
        if assets:
            return f"前序 {anchor_symbol} 进入交易场景后，{'/'.join(assets[:3])} 继续流入同一地址"
        return f"前序 {anchor_symbol} 进入交易场景后，同地址出现后续确认"

    def _lp_volume_surge_label(self, event: Event) -> str:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        surge_ratio = float(lp_analysis.get("pool_volume_surge_ratio") or 0.0)
        if surge_ratio <= 0:
            return "无放量基线"
        if surge_ratio >= 2.5:
            return f"量能显著放大 {surge_ratio:.2f}x"
        if surge_ratio >= 1.5:
            return f"量能放大 {surge_ratio:.2f}x"
        return f"量能平稳 {surge_ratio:.2f}x"

    def _same_pool_continuity_label(self, event: Event) -> str:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        continuity = int(lp_analysis.get("same_pool_continuity") or 0) + 1
        if continuity <= 1:
            return "单池单笔"
        return f"同池连续 {continuity} 笔"

    def _multi_pool_resonance_label(self, event: Event) -> str:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        resonance = int(lp_analysis.get("multi_pool_resonance") or 0)
        if resonance >= 2:
            return f"跨池同向 {resonance} 池"
        return "无跨池共振"

    def _lp_burst_state(self, event: Event, signal: Signal) -> dict:
        event_meta = event.metadata or {}
        signal_meta = signal.metadata or {}
        burst_meta = event_meta.get("lp_burst") or {}
        return {
            "applied": bool(
                event_meta.get("lp_burst_fastlane_applied")
                or signal_meta.get("lp_burst_fastlane_applied")
            ),
            "delivery_class": str(
                event_meta.get("lp_burst_delivery_class")
                or signal_meta.get("lp_burst_delivery_class")
                or ""
            ),
            "window_sec": int(
                event_meta.get("lp_burst_window_sec")
                or signal_meta.get("lp_burst_window_sec")
                or burst_meta.get("lp_burst_window_sec")
                or 0
            ),
            "event_count": int(
                event_meta.get("lp_burst_event_count")
                or signal_meta.get("lp_burst_event_count")
                or burst_meta.get("lp_burst_event_count")
                or 0
            ),
            "total_usd": float(
                event_meta.get("lp_burst_total_usd")
                or signal_meta.get("lp_burst_total_usd")
                or burst_meta.get("lp_burst_total_usd")
                or 0.0
            ),
            "max_single_usd": float(
                event_meta.get("lp_burst_max_single_usd")
                or signal_meta.get("lp_burst_max_single_usd")
                or burst_meta.get("lp_burst_max_single_usd")
                or 0.0
            ),
            "same_pool_continuity": int(
                event_meta.get("lp_burst_same_pool_continuity")
                or signal_meta.get("lp_burst_same_pool_continuity")
                or burst_meta.get("lp_burst_same_pool_continuity")
                or 0
            ),
            "volume_surge_ratio": float(
                event_meta.get("lp_burst_volume_surge_ratio")
                or signal_meta.get("lp_burst_volume_surge_ratio")
                or burst_meta.get("lp_burst_volume_surge_ratio")
                or 0.0
            ),
        }

    def _lp_burst_label(self, event: Event, signal: Signal) -> str:
        burst = self._lp_burst_state(event, signal)
        if burst["event_count"] <= 0:
            return ""
        if burst["applied"] and burst["delivery_class"] == "primary":
            return "同池同向 burst 主推送"
        if burst["applied"]:
            return "同池同向 burst 快车道"
        return "同池同向 burst 候选"

    def _lp_burst_window_label(self, event: Event, signal: Signal) -> str:
        window_sec = int(self._lp_burst_state(event, signal).get("window_sec") or 0)
        if window_sec <= 0:
            return ""
        return f"{window_sec} 秒窗口"

    def _lp_burst_summary(self, event: Event, signal: Signal) -> str:
        burst = self._lp_burst_state(event, signal)
        event_count = int(burst.get("event_count") or 0)
        if event_count <= 0:
            return ""
        window_sec = int(burst.get("window_sec") or 0)
        total_usd = float(burst.get("total_usd") or 0.0)
        max_single_usd = float(burst.get("max_single_usd") or 0.0)
        continuity = int(burst.get("same_pool_continuity") or event_count or 0)
        volume_surge_ratio = float(burst.get("volume_surge_ratio") or 0.0)
        direction = "买压" if str(event.intent_type or "") == "pool_buy_pressure" else "卖压"
        return (
            f"{direction} burst｜{window_sec} 秒内连续 {max(event_count, continuity)} 笔｜"
            f"合计 ${total_usd:,.0f}｜峰值 ${max_single_usd:,.0f}｜放量 {volume_surge_ratio:.1f}x"
        )

    def _lp_trend_display(self, event: Event, signal: Signal) -> dict:
        if str(event.strategy_role or "") != "lp_pool" and str(event.intent_type or "") not in LP_INTENTS:
            return {}

        event_meta = event.metadata or {}
        signal_meta = signal.metadata or {}
        raw_profile = str(
            event_meta.get("lp_directional_threshold_profile")
            or signal_meta.get("lp_directional_threshold_profile")
            or "standard"
        )
        if raw_profile.startswith("buy_bias"):
            profile = "buy_bias"
        elif raw_profile == "sell_bias":
            profile = "sell_bias"
        else:
            profile = "standard"

        primary_pool = bool(
            event_meta.get("lp_trend_primary_pool")
            or signal_meta.get("lp_trend_primary_pool")
        )
        trend_state = str(
            event_meta.get("lp_trend_state")
            or signal_meta.get("lp_trend_state")
            or "trend_neutral"
        )
        burst_trend_mode = bool(
            event_meta.get("lp_burst_trend_mode")
            or signal_meta.get("lp_burst_trend_mode")
        )
        route_semantics = str(
            event_meta.get("lp_route_semantics")
            or signal_meta.get("lp_route_semantics")
            or ""
        )

        if trend_state in {"trend_continuation_sell", "trend_continuation_buy"}:
            state_label = "趋势延续"
        elif trend_state in {"trend_reversal_to_buy", "trend_reversal_to_sell"}:
            state_label = "趋势反转"
        else:
            state_label = "常规 directional"

        if profile == "sell_bias":
            bias_label = "偏空趋势敏感化"
        elif raw_profile in {"buy_bias_trend_continuation", "buy_bias_trend_reversal"}:
            bias_label = "偏多趋势校准"
        elif profile == "buy_bias":
            bias_label = "温和偏多校准"
        else:
            bias_label = "标准 directional"

        if route_semantics == "single_shot_fallback":
            mode_label = "single-shot fallback"
        elif route_semantics == "burst_main_entry":
            mode_label = "trend burst｜主入口" if burst_trend_mode else "standard burst｜主入口"
        elif route_semantics == "directional_exception_entry":
            mode_label = "trend directional｜例外入口" if profile != "standard" or primary_pool else "standard directional｜例外入口"
        elif route_semantics == "directional_standard_entry":
            mode_label = "trend directional｜常规入口" if profile != "standard" or primary_pool else "standard directional｜常规入口"
        elif burst_trend_mode:
            mode_label = "trend burst"
        elif str(signal.delivery_reason or "").startswith("lp_burst_directional_"):
            mode_label = "standard burst"
        else:
            mode_label = "trend directional" if profile != "standard" or primary_pool else "standard directional"

        return {
            "lp_trend_display_label": "主流趋势池" if primary_pool else "普通池",
            "lp_trend_display_profile": profile,
            "lp_trend_display_mode": mode_label,
            "lp_trend_display_bias": bias_label,
            "lp_trend_display_state": state_label,
        }

    def _pool_label(self, event: Event, raw: dict, actor_label: str) -> str:
        lp_context = raw.get("lp_context") or {}
        clmm_context = raw.get("clmm_context") or event.metadata.get("clmm_context") or {}
        return str(
            clmm_context.get("pair_label")
            or lp_context.get("pool_label")
            or lp_context.get("pair_label")
            or event.metadata.get("watch_address_label")
            or actor_label
            or "LP Pool"
        )

    def _lp_action_brief(self, event: Event, action_label: str, fact_label: str) -> str:
        if event.intent_type == "pool_buy_pressure":
            return "持续买压｜稳定币流入、标的流出"
        if event.intent_type == "pool_sell_pressure":
            return "持续卖压｜标的流入、稳定币流出"
        if event.intent_type == "liquidity_addition":
            return "流动性补充｜双边资产同步增加"
        if event.intent_type == "liquidity_removal":
            return "流动性抽离｜双边资产同步减少"
        if event.intent_type == "pool_rebalance":
            return "池子再平衡｜库存结构调整"
        return action_label or fact_label

    def _lp_meaning_brief(self, event: Event, intent_detail: str, market_implication: str) -> str:
        if event.intent_type == "pool_buy_pressure":
            return "更接近市场主动买入延续，不等于 LP 主体看多"
        if event.intent_type == "pool_sell_pressure":
            return "更接近市场主动卖出延续，不等于 LP 主体看空"
        if event.intent_type == "liquidity_addition":
            return "更接近池子补充深度"
        if event.intent_type == "liquidity_removal":
            return "更接近池子撤出深度，后续冲击风险可能抬升"
        if event.intent_type == "pool_rebalance":
            return "更接近池子库存再平衡"
        return intent_detail or market_implication

    def _market_context_label(self, event: Event, signal: Signal, gate_metrics: dict, token_snapshot: dict, lp_event: bool) -> str:
        if lp_event:
            lp_analysis = event.metadata.get("lp_analysis") or {}
            reserve_skew = float(lp_analysis.get("reserve_skew") or 0.0)
            action_intensity = float(lp_analysis.get("action_intensity") or 0.0)
            impact_hint = str(lp_analysis.get("market_impact_hint") or "低冲击")
            multi_pool_resonance = int(lp_analysis.get("multi_pool_resonance") or 0)
            continuity = int(lp_analysis.get("same_pool_continuity") or 0) + 1
            surge_ratio = float(lp_analysis.get("pool_volume_surge_ratio") or 0.0)
            resonance_text = f"{multi_pool_resonance} 池共振" if multi_pool_resonance >= 2 else "单池观察"
            skew_text = f"偏斜 {reserve_skew:.2f}" if reserve_skew > 0 else "偏斜轻微"
            intensity_text = f"强度 {action_intensity:.2f}"
            continuity_text = f"连续 {continuity} 笔" if continuity > 1 else "单笔"
            surge_text = f"放量 {surge_ratio:.2f}x" if surge_ratio > 0 else "无放量基线"
            return f"{skew_text}｜{continuity_text}｜{resonance_text}｜{surge_text}｜{impact_hint}｜{intensity_text}"

        quality_score = float(gate_metrics.get("quality_score") or 0.0)
        volume_ratio = float(gate_metrics.get("token_volume_ratio") or 0.0)
        impact_ratio = float(gate_metrics.get("price_impact_ratio") or 0.0)
        cluster = int(gate_metrics.get("buy_cluster_5m") or 0) if event.side == "买入" else int(gate_metrics.get("sell_cluster_5m") or 0)
        net_flow_5m = float(gate_metrics.get("token_net_flow_usd_5m") or token_snapshot.get("token_net_flow_usd_5m") or 0.0)
        base_token_score = float(signal.base_token_score or 0.0)
        token_context_score = float(signal.token_context_score or base_token_score)
        liquidity_proxy_usd = float(gate_metrics.get("liquidity_proxy_usd") or token_snapshot.get("liquidity_proxy_usd") or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)

        if quality_score >= 0.86 or impact_ratio >= 0.01 or volume_ratio >= 0.01 or cluster >= 3:
            represent = "高代表性"
        elif quality_score >= 0.75 or impact_ratio >= 0.004 or volume_ratio >= 0.003 or cluster >= 2:
            represent = "中代表性"
        else:
            represent = "低代表性"

        if event.side == "买入":
            trend = "短线买盘占优" if net_flow_5m > 0 else "短线中性"
        elif event.side == "卖出":
            trend = "短线卖压占优" if net_flow_5m < 0 else "短线中性"
        else:
            trend = "短线中性"

        if liquidity_proxy_usd >= 10_000_000:
            liquidity_label = "高流动"
        elif liquidity_proxy_usd >= 1_000_000:
            liquidity_label = "中流动"
        else:
            liquidity_label = "低流动"

        token_context = f"Token {base_token_score:.0f}->{token_context_score:.0f}"
        resonance_text = "强共振" if resonance_score >= 0.65 else "弱共振" if resonance_score >= 0.35 else "无共振"
        return f"{represent}｜{trend}｜{liquidity_label}｜{resonance_text}｜{token_context}"

    def _path_summary(self, event: Event, watch_context: dict | None) -> tuple[str, str, str]:
        raw = event.metadata.get("raw") or {}
        src_addr = str(raw.get("from") or "").lower()
        dst_addr = str(raw.get("to") or "").lower()
        src_label = format_address_label(src_addr) if src_addr else ""
        dst_label = format_address_label(dst_addr) if dst_addr else ""
        if not src_label and watch_context:
            src_label = str(watch_context.get("flow_source_label") or watch_context.get("counterparty_label") or "未知来源")
        if not dst_label and watch_context:
            dst_label = str(watch_context.get("flow_target_label") or watch_context.get("watch_address_label") or "未知去向")
        src_label = src_label or "未知来源"
        dst_label = dst_label or "未知去向"
        return src_label, dst_label, f"{src_label} -> {dst_label}"

    def _leg_paths(self, event: Event) -> tuple[str, str]:
        raw = event.metadata.get("raw") or {}
        quote_from = str(raw.get("quote_from") or "").lower()
        quote_to = str(raw.get("quote_to") or "").lower()
        token_from = str(raw.get("token_from") or "").lower()
        token_to = str(raw.get("token_to") or "").lower()
        quote_text = ""
        token_text = ""
        if quote_from or quote_to:
            quote_text = f"{format_address_label(quote_from) if quote_from else '未知来源'} -> {format_address_label(quote_to) if quote_to else '未知去向'}"
        if token_from or token_to:
            token_text = f"{format_address_label(token_from) if token_from else '未知来源'} -> {format_address_label(token_to) if token_to else '未知去向'}"
        return quote_text, token_text

    def _signal_type_label(self, signal_type: str | None) -> str:
        normalized = canonicalize_pool_semantic_key(signal_type)
        return self.SIGNAL_TYPE_LABELS.get(normalized, normalized or "Unknown_Signal")
