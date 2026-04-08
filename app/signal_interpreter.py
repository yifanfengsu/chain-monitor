from dataclasses import dataclass

from config import (
    INTERPRETER_ABNORMAL_MULTIPLIER,
    INTERPRETER_CONTINUOUS_WINDOW_SEC,
)
from constants import ETH_EQUIVALENT_CONTRACTS, STABLE_TOKEN_CONTRACTS
from filter import format_address_label, get_address_meta, shorten_address
from models import Event, Signal


LP_INTENTS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
    "liquidity_addition",
    "liquidity_removal",
    "pool_rebalance",
    "pool_noise",
}


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
        "unknown_intent": "意图未明",
    }

    INFO_LABELS = {
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
        "lp_buy_pressure": "LP_Buy_Pressure",
        "lp_sell_pressure": "LP_Sell_Pressure",
        "lp_liquidity_add": "LP_Liquidity_Add",
        "lp_liquidity_remove": "LP_Liquidity_Remove",
        "lp_rebalance": "LP_Rebalance",
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
        raw = event.metadata.get("raw") or {}
        lp_event = self._is_lp_event(event, watch_meta, raw)
        counterparty_meta = self._resolve_counterparty_meta(event, watch_context)

        semantic = self._classify_semantic(event, lp_event)
        intent_label = self.INTENT_LABELS.get(str(event.intent_type or "unknown_intent"), "意图未明")
        actor_label = self._actor_label(event.address, watch_meta, lp_event)
        role_display = self._role_display(watch_meta, lp_event)
        action_label = self._action_label(event, signal, semantic, lp_event)
        fact_label = self._fact_label(event, watch_context, lp_event)
        intent_detail = self._intent_detail(event, lp_event)
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
        observe_or_primary_label = "主推送" if str(signal.delivery_class or "") == "primary" else "观察级"
        liquidation_meta = self._liquidation_meta(event)
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
        if smart_money_context["active"] and message_variant == "alert":
            message_variant = smart_money_context["message_variant"]

        downstream_followup_active = bool(event.metadata.get("downstream_followup_active"))
        if downstream_followup_active:
            message_variant = "downstream_followup"
            object_label = str(event.metadata.get("downstream_object_label") or object_label or actor_label)
            headline_label = str(event.metadata.get("downstream_followup_label") or headline_label)
            fact_brief = (
                f"{event.metadata.get('downstream_anchor_label') or '重点地址'} -> "
                f"{event.metadata.get('downstream_object_label') or object_label}｜{self._trade_value_label(event, signal, continuous_count, abnormal_ratio, gate_metrics, lp_event)}"
            )
            explanation_brief = str(event.metadata.get("downstream_followup_detail") or explanation_brief)
            evidence_brief = (
                f"{event.metadata.get('downstream_followup_stage_label') or '观察开启'}｜"
                f"anchor ${float(event.metadata.get('downstream_anchor_usd_value') or 0.0):,.0f}｜"
                f"{confirmation_label}"
            )
            action_hint = str(event.metadata.get("downstream_followup_next_hint") or action_hint)
            update_brief = str(event.metadata.get("downstream_followup_label") or update_brief)
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

        signal.semantic = semantic
        signal.core_action = action_label
        signal.context = {
            "actor_label": actor_label,
            "object_label": object_label,
            "pool_label": pool_label,
            "role_summary": role_display,
            "role_display": role_display,
            "role_label": watch_meta.get("role_label", "未分类"),
            "strategy_role_label": watch_meta.get("strategy_role_label", "未分类"),
            "semantic_role_label": watch_meta.get("semantic_role_label", "未分类"),
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
            "observe_or_primary_label": observe_or_primary_label,
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
            "downstream_followup_active": downstream_followup_active,
            "downstream_anchor_label": str(event.metadata.get("downstream_anchor_label") or ""),
            "downstream_object_label": str(event.metadata.get("downstream_object_label") or ""),
            "downstream_followup_label": str(event.metadata.get("downstream_followup_label") or ""),
            "downstream_followup_detail": str(event.metadata.get("downstream_followup_detail") or ""),
            "downstream_followup_reason": str(event.metadata.get("downstream_followup_reason") or ""),
            "downstream_followup_stage_label": str(event.metadata.get("downstream_followup_stage_label") or ""),
            "downstream_followup_path_label": str(event.metadata.get("downstream_followup_path_label") or ""),
            "downstream_followup_next_hint": str(event.metadata.get("downstream_followup_next_hint") or ""),
        }

        signal.metadata.setdefault("summary", {})
        signal.metadata.update({
            "message_variant": message_variant,
            "smart_money_style_variant": smart_money_context["style_variant"],
            "smart_money_observe_label": smart_money_context["observe_label"],
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
            "headline_label": headline_label,
            "fact_brief": fact_brief,
            "explanation_brief": explanation_brief,
            "evidence_brief": evidence_brief,
            "action_hint": action_hint,
            "update_brief": update_brief,
            "object_label": object_label,
            "pool_label": pool_label,
            "lp_action_brief": lp_action_brief,
            "lp_meaning_brief": lp_meaning_brief,
            "lp_volume_surge_label": lp_volume_surge_label,
            "same_pool_continuity_label": same_pool_continuity_label,
            "multi_pool_resonance_label": multi_pool_resonance_label,
            "observe_or_primary_label": observe_or_primary_label,
            "smart_money_observe_label": smart_money_context["observe_label"],
            "smart_money_fact_brief": smart_money_context["fact_brief"],
            "smart_money_explanation_brief": smart_money_context["explanation_brief"],
            "smart_money_evidence_brief": smart_money_context["evidence_brief"],
            "smart_money_action_hint": smart_money_context["action_hint"],
            "smart_money_style_variant": smart_money_context["style_variant"],
            "downstream_followup_active": downstream_followup_active,
            "downstream_anchor_label": str(event.metadata.get("downstream_anchor_label") or ""),
            "downstream_object_label": str(event.metadata.get("downstream_object_label") or ""),
            "downstream_followup_label": str(event.metadata.get("downstream_followup_label") or ""),
            "downstream_followup_detail": str(event.metadata.get("downstream_followup_detail") or ""),
            "downstream_followup_reason": str(event.metadata.get("downstream_followup_reason") or ""),
            "downstream_followup_stage_label": str(event.metadata.get("downstream_followup_stage_label") or ""),
            "downstream_followup_path_label": str(event.metadata.get("downstream_followup_path_label") or ""),
            "downstream_followup_next_hint": str(event.metadata.get("downstream_followup_next_hint") or ""),
        })
        if lp_event:
            signal.metadata["lp"] = {
                "context": dict(raw.get("lp_context") or {}),
                "analysis": dict(event.metadata.get("lp_analysis") or {}),
            }
        return InterpretationDecision(should_notify=True, reason="interpreted")

    def _is_lp_event(self, event: Event, watch_meta: dict, raw: dict) -> bool:
        if str(watch_meta.get("strategy_role") or event.strategy_role or "") == "lp_pool":
            return True
        if str(raw.get("monitor_type") or "") == "lp_pool":
            return True
        return str(event.intent_type or "") in LP_INTENTS or str(event.strategy_role or "") == "lp_pool"

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

    def _classify_semantic(self, event: Event, lp_event: bool) -> str:
        if lp_event:
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                return "pool_trade_pressure"
            if event.intent_type in {"liquidity_addition", "liquidity_removal"}:
                return "pool_liquidity_shift"
            if event.intent_type == "pool_rebalance":
                return "pool_rebalance"
            return "pool_noise"

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

    def _action_label(self, event: Event, signal: Signal, semantic: str, lp_event: bool) -> str:
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        token_symbol = str(lp_context.get("base_token_symbol") or raw.get("token_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        quote_symbol = str(lp_context.get("quote_token_symbol") or raw.get("quote_symbol") or event.metadata.get("quote_symbol") or "稳定币")
        pair_label = str(lp_context.get("pair_label") or "")

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

    def _intent_detail(self, event: Event, lp_event: bool) -> str:
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

    def _fact_label(self, event: Event, watch_context: dict | None, lp_event: bool) -> str:
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        token_symbol = str(lp_context.get("base_token_symbol") or raw.get("token_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        quote_symbol = str(lp_context.get("quote_token_symbol") or raw.get("quote_symbol") or event.metadata.get("quote_symbol") or "报价资产")
        pair_label = str(lp_context.get("pair_label") or f"{token_symbol}/{quote_symbol}")

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
        is_execution = str(event.intent_type or "") == "swap_execution" or bool(signal.metadata.get("is_real_execution"))
        if strategy_role == "market_maker_wallet" and str(signal.delivery_class or "") == "observe":
            return "market_maker_observe"
        if strategy_role in {"smart_money_wallet", "alpha_wallet", "market_maker_wallet", "celebrity_wallet"}:
            if str(signal.delivery_class or "") == "primary" and is_execution:
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
        delivery_reason = str(signal.delivery_reason or event.delivery_reason or "")
        is_execution = str(event.intent_type or "") == "swap_execution" or bool(signal.metadata.get("is_real_execution"))
        style_variant = "market_maker" if strategy_role == "market_maker_wallet" else "smart_money"
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
        exception_applied = bool(
            gate_metrics.get("market_maker_observe_exception_applied")
            or gate_metrics.get("smart_money_non_exec_exception_applied")
            or signal.metadata.get("market_maker_observe_exception_applied")
            or signal.metadata.get("smart_money_non_exec_exception_applied")
        )
        behavior_type = str(signal.behavior_type or "")
        behavior_label_map = {
            "inventory_management": "库存管理",
            "inventory_shift": "库存切换",
            "inventory_expansion": "库存扩张",
            "inventory_distribution": "库存分配",
            "whale_action": "大额库存动作",
        }

        observe_label = "Smart Money 观察级雷达"
        message_variant = "smart_money_primary" if delivery_class == "primary" and is_execution else "smart_money_observe"
        if style_variant == "market_maker":
            if is_execution:
                observe_label = "Market Maker 执行观察雷达"
            elif behavior_type == "inventory_shift" or delivery_reason == "market_maker_inventory_shift_observe":
                observe_label = "Market Maker 库存切换观察雷达"
            else:
                observe_label = "Market Maker 库存观察雷达"
            if delivery_class == "observe":
                message_variant = "market_maker_observe"
        elif is_execution:
            observe_label = "Smart Money 执行观察雷达"
        elif delivery_class == "primary":
            observe_label = "Smart Money 执行雷达"

        if style_variant == "market_maker":
            if is_execution:
                sm_explanation = "已看到做市地址真实执行，但当前更适合继续看是否形成连续执行或方向切换。"
            elif behavior_type == "inventory_shift" or delivery_reason == "market_maker_inventory_shift_observe":
                sm_explanation = "当前更像做市库存切换/方向调整，不直接等同于主观建仓或出货。"
            elif behavior_type in {"inventory_management", "inventory_expansion", "inventory_distribution"} or delivery_reason == "market_maker_inventory_observe":
                sm_explanation = "当前更像做市库存调节或换手动作，金额与结构值得继续观察。"
            else:
                sm_explanation = "当前更像做市库存动作，后续是否转成真实执行更关键。"
        else:
            if delivery_class == "primary" and is_execution:
                sm_explanation = "已看到更强的聪明钱真实执行，当前优先按执行路径跟踪。"
            elif is_execution:
                sm_explanation = "已看到真实执行，但当前更适合继续观察确认，不急于放大解读。"
            else:
                sm_explanation = "金额与地址质量较强，但当前仍属非执行观察，后续是否转成真实执行更关键。"

        evidence_items = []
        if threshold_ratio > 0:
            evidence_items.append(f"金额 {threshold_ratio:.2f}x 门槛")
        if confirmation_score >= 0.46:
            evidence_items.append(f"确认 {confirmation_score:.2f}")
        if resonance_score >= 0.32:
            evidence_items.append(f"共振 {resonance_score:.2f}")
        if quality_score >= 0.60:
            evidence_items.append(f"质量 {quality_score:.2f}")
        if exception_applied:
            evidence_items.append("gate 例外保留")
        if behavior_type in {"inventory_management", "inventory_shift", "inventory_expansion", "inventory_distribution"}:
            evidence_items.append(behavior_label_map.get(behavior_type, behavior_type))

        sm_action_hint = action_hint
        if style_variant == "market_maker":
            sm_action_hint = "继续看同 token 是否连续换手、是否出现跨地址共振，或进一步转成真实执行。"
        elif is_execution:
            sm_action_hint = "继续看是否出现连续执行、同地址放量或多聪明钱共振。"
        else:
            sm_action_hint = "继续看是否从资金转移升级为真实执行，或形成更多地址共振。"

        return {
            "active": delivery_class in {"primary", "observe"},
            "message_variant": message_variant,
            "observe_label": observe_label,
            "fact_brief": fact_brief,
            "explanation_brief": sm_explanation or explanation_brief,
            "evidence_brief": "｜".join(evidence_items[:4]) if evidence_items else evidence_brief,
            "action_hint": sm_action_hint,
            "style_variant": style_variant,
        }

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

    def _pool_label(self, event: Event, raw: dict, actor_label: str) -> str:
        lp_context = raw.get("lp_context") or {}
        return str(
            lp_context.get("pool_label")
            or lp_context.get("pair_label")
            or event.metadata.get("watch_address_label")
            or actor_label
            or "LP Pool"
        )

    def _lp_action_brief(self, event: Event, action_label: str, fact_label: str) -> str:
        if event.intent_type == "pool_buy_pressure":
            return "池子买压｜稳定币流入、标的流出"
        if event.intent_type == "pool_sell_pressure":
            return "池子卖压｜标的流入、稳定币流出"
        if event.intent_type == "liquidity_addition":
            return "流动性增加｜双边资产同步增加"
        if event.intent_type == "liquidity_removal":
            return "流动性减少｜双边资产同步减少"
        if event.intent_type == "pool_rebalance":
            return "池子再平衡｜库存结构调整"
        return action_label or fact_label

    def _lp_meaning_brief(self, event: Event, intent_detail: str, market_implication: str) -> str:
        if event.intent_type == "pool_buy_pressure":
            return "更接近市场主动买入"
        if event.intent_type == "pool_sell_pressure":
            return "更接近市场主动卖出"
        if event.intent_type == "liquidity_addition":
            return "更接近池子补充深度"
        if event.intent_type == "liquidity_removal":
            return "更接近池子撤出深度"
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
        normalized = str(signal_type or "")
        return self.SIGNAL_TYPE_LABELS.get(normalized, normalized or "Unknown_Signal")
