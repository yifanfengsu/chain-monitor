import asyncio
import os
import time
from telegram import Bot

from config import (
    CHAT_ID,
    TELEGRAM_BOT_TOKEN,
)
from filter import format_address_label, strategy_role_group
from lp_analyzer import canonicalize_pool_semantic_key
from models import Event, Signal

bot = Bot(token=TELEGRAM_BOT_TOKEN)
NOTIFIER_RUNTIME_STATS = {
    "signals_attempted_send": 0,
    "signals_sent_ok": 0,
    "signals_sent_failed": 0,
}
_NOTIFIER_STATS_LOG_INTERVAL_SEC = 300
_last_notifier_stats_log_ts = 0.0
DEFAULT_MESSAGE_TEMPLATE = os.getenv("WHALE_MESSAGE_TEMPLATE", "long").strip().lower()
MESSAGE_VARIANTS = {
    "downstream_followup",
    "followup",
    "liquidation_risk",
    "liquidation_execution",
    "market_maker_primary",
    "market_maker_observe",
    "smart_money_primary",
    "smart_money_observe",
    "lp_directional",
    "lp_liquidity",
    "alert",
}
BRIEF_BY_DEFAULT_VARIANTS = {
    "lp_directional",
    "lp_liquidity",
    "liquidation_risk",
    "liquidation_execution",
}


async def send(msg) -> bool:
    """发送告警消息到 Telegram，失败时做短重试。"""
    for i in range(3):
        try:
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            return True
        except Exception as e:
            print(f"发送失败，第{i + 1}次重试:", e)
            if i < 2:
                await asyncio.sleep(2)
    return False


def _log_notifier_stats_if_needed(force: bool = False) -> None:
    global _last_notifier_stats_log_ts
    now = time.time()
    if not force and (now - _last_notifier_stats_log_ts) < _NOTIFIER_STATS_LOG_INTERVAL_SEC:
        return
    _last_notifier_stats_log_ts = now
    print(
        "📣 notifier funnel:",
        f"attempted={NOTIFIER_RUNTIME_STATS['signals_attempted_send']}",
        f"sent_ok={NOTIFIER_RUNTIME_STATS['signals_sent_ok']}",
        f"sent_failed={NOTIFIER_RUNTIME_STATS['signals_sent_failed']}",
    )


def _bump_notifier_stat(stat_key: str, *, force_log: bool = False) -> None:
    NOTIFIER_RUNTIME_STATS[stat_key] = int(NOTIFIER_RUNTIME_STATS.get(stat_key) or 0) + 1
    _log_notifier_stats_if_needed(force=force_log)


def _format_path_line(prefix: str, from_addr: str | None, to_addr: str | None) -> str | None:
    src = (from_addr or "").lower()
    dst = (to_addr or "").lower()
    if not src and not dst:
        return None

    src_label = format_address_label(src) if src else "未知来源"
    dst_label = format_address_label(dst) if dst else "未知去向"
    return f"{prefix}: {src_label} -> {dst_label}"


def _resolve_template(signal: Signal) -> str:
    context = signal.context or {}
    metadata = signal.metadata or {}
    template = context.get("message_template") or metadata.get("message_template") or DEFAULT_MESSAGE_TEMPLATE
    normalized = str(template).strip().lower()
    if normalized == "intent":
        return "intent"
    if normalized in {"headline", "brief"}:
        return "brief"
    if normalized == "short":
        return "short"
    if normalized == "debug":
        return "debug"
    return "long"


def _message_template_overridden(signal: Signal) -> bool:
    context = signal.context or {}
    metadata = signal.metadata or {}
    return bool(context.get("message_template") or metadata.get("message_template"))


def _priority_text(context: dict) -> str:
    priority = context.get("priority")
    if isinstance(priority, int):
        return f"P{priority}"
    return "P3"


def _stage_role_line(signal: Signal, context: dict) -> str:
    delivery_class = str(signal.delivery_class or context.get("delivery_class") or "").strip().lower()
    stage_label = "Primary" if delivery_class == "primary" else "Observe" if delivery_class == "observe" else ""
    role_tier = str(
        context.get("role_priority_tier")
        or signal.metadata.get("role_priority_tier")
        or ""
    ).strip()
    strategy_role = str(
        context.get("strategy_role")
        or signal.metadata.get("strategy_role")
        or ""
    ).strip()
    role_group = strategy_role_group(strategy_role)
    role_label = {
        "tier1": "T1 Market Maker" if role_group == "market_maker" else "T1 Smart Money",
        "tier2": "T2 LP",
        "tier3": "T3 Exchange",
        "tier4": "T4 Other",
    }.get(role_tier, "")
    parts = [item for item in [stage_label, role_label, _priority_text(context)] if item]
    return f"阶段：{'｜'.join(parts)}" if parts else ""


def _short_tx_hash(tx_hash: str) -> str:
    if not tx_hash:
        return ""
    if len(tx_hash) <= 14:
        return tx_hash
    return f"{tx_hash[:10]}...{tx_hash[-6:]}"


def _path_bundle(context: dict, raw: dict) -> tuple[str, str, str]:
    path_text = context.get("path_text") or ""
    if not path_text:
        path = _format_path_line("路径", raw.get("from"), raw.get("to"))
        path_text = path.replace("路径: ", "", 1) if path else "未知来源 -> 未知去向"

    quote_path_text = context.get("quote_path_text") or ""
    token_path_text = context.get("token_path_text") or ""
    return path_text, quote_path_text, token_path_text


def _select_message_variant(signal: Signal, event: Event, context: dict) -> str:
    variant = str(
        context.get("message_variant")
        or signal.metadata.get("message_variant")
        or ""
    ).strip()
    smart_money_variant = _smart_money_variant(signal, event, context)
    if variant in {"smart_money_primary", "smart_money_observe", "market_maker_primary", "market_maker_observe"}:
        return smart_money_variant or "alert"
    if variant in MESSAGE_VARIANTS:
        return variant
    if bool(context.get("downstream_followup_active")) or str(context.get("case_family") or "") == "downstream_counterparty_followup":
        return "downstream_followup"
    liquidation_stage = str(context.get("liquidation_stage") or event.metadata.get("liquidation_stage") or "none")
    if liquidation_stage == "execution":
        return "liquidation_execution"
    if liquidation_stage == "risk":
        return "liquidation_risk"
    if bool(context.get("followup_confirmed")) or str(context.get("case_family") or "") == "exchange_cross_token_followup":
        return "followup"
    if bool(context.get("lp_event")):
        if str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"}:
            return "lp_directional"
        return "lp_liquidity"
    if smart_money_variant:
        return smart_money_variant
    return "alert"


def _is_smart_money_primary(signal: Signal, event: Event) -> bool:
    if str(signal.delivery_class or "") != "primary":
        return False
    if bool(signal.metadata.get("smart_money_case")):
        return True
    return (
        strategy_role_group(event.strategy_role) in {"smart_money", "market_maker"}
        and str(event.intent_type or "") == "swap_execution"
    )


def _smart_money_variant(signal: Signal, event: Event, context: dict) -> str:
    delivery_class = str(signal.delivery_class or "")
    if delivery_class not in {"primary", "observe"}:
        return ""
    role_group = strategy_role_group(
        context.get("strategy_role")
        or signal.metadata.get("strategy_role")
        or event.strategy_role
    )
    if role_group not in {"smart_money", "market_maker"}:
        return ""

    delivery_reason = str(signal.delivery_reason or event.delivery_reason or "")
    smart_money_reasons = {
        "market_maker_execution_observe",
        "market_maker_execution_primary",
        "smart_money_execution_observe",
        "smart_money_execution_primary",
        "smart_money_continuous_execution_primary",
    }
    is_execution = (
        bool(signal.metadata.get("is_real_execution"))
        or event.kind == "swap"
        or str(event.intent_type or "") == "swap_execution"
    )
    if delivery_reason not in smart_money_reasons or not is_execution:
        return ""

    style_variant = str(
        context.get("smart_money_style_variant")
        or signal.metadata.get("smart_money_style_variant")
        or ("market_maker" if role_group == "market_maker" else "smart_money" if role_group == "smart_money" else "")
    ).strip()
    if style_variant == "market_maker" and delivery_class == "primary":
        return "market_maker_primary"
    if delivery_class == "primary" and is_execution:
        return "smart_money_primary"
    if style_variant == "market_maker":
        return "market_maker_observe"
    return "smart_money_observe"


def _join_lines(lines: list[str]) -> str:
    return "\n".join([line for line in lines if str(line).strip()]) + "\n"


def _usd_value_text(value: float) -> str:
    usd_value = float(value or 0.0)
    if usd_value >= 100_000:
        return f"${usd_value:,.0f}"
    return f"${usd_value:,.2f}"


def _tx_line(signal: Signal, compact: bool = False) -> str:
    tx_value = _short_tx_hash(signal.tx_hash) if compact else _short_tx_hash(signal.tx_hash)
    return f"Tx: {tx_value}"


def _object_label(signal: Signal, context: dict) -> str:
    return str(context.get("object_label") or context.get("actor_label") or format_address_label(signal.address) or signal.address)


def _pool_label(signal: Signal, context: dict) -> str:
    return str(context.get("pool_label") or context.get("object_label") or context.get("actor_label") or signal.address)


def _pair_label(signal: Signal, context: dict) -> str:
    return str(context.get("pair_label") or context.get("pool_label") or _pool_label(signal, context))


def _headline_label(context: dict, event: Event) -> str:
    return str(context.get("headline_label") or context.get("conclusion_label") or event.intent_type or "意图未明")


def _exchange_transfer_purpose_label(purpose: str, strength: str = "no") -> str:
    mapping = {
        "exchange_user_deposit_consolidation": "用户入金归集",
        "exchange_hot_wallet_withdrawal_outflow": "热钱包出金",
        "exchange_hot_wallet_cold_wallet_topup": "冷转热补给",
        "exchange_hot_wallet_overflow_to_cold": "热转冷归仓",
        "exchange_internal_rebalance": "内部再平衡",
        "exchange_trading_desk_funding": "交易席位注资",
        "exchange_trading_desk_return": "交易席位回流",
        "exchange_external_outflow": "外部转出",
        "exchange_external_inflow": "外部流入",
        "exchange_unknown_flow": "待继续确认",
    }
    label = mapping.get(str(purpose or ""), str(purpose or "待继续确认"))
    if str(strength or "") == "likely" and str(purpose or "") in {
        "exchange_user_deposit_consolidation",
        "exchange_hot_wallet_overflow_to_cold",
        "exchange_hot_wallet_cold_wallet_topup",
        "exchange_trading_desk_funding",
        "exchange_trading_desk_return",
        "exchange_internal_rebalance",
    }:
        return f"疑似{label}"
    return label


def _operational_intent_label(context: dict, event: Event) -> str:
    return str(context.get("operational_intent_label") or _headline_label(context, event))


def _confidence_badge(context: dict) -> str:
    return str(
        context.get("operational_intent_confidence_label")
        or context.get("confidence_label")
        or "低"
    )


def _entity_context_brief(context: dict) -> str:
    venue_or_position_context = str(context.get("venue_or_position_context") or "").strip()
    if venue_or_position_context:
        return venue_or_position_context
    parts = []
    entity_label = str(context.get("exchange_entity_label") or context.get("entity_label") or "").strip()
    wallet_label = str(context.get("wallet_function_label") or "").strip()
    if entity_label and wallet_label and wallet_label != "Unknown Wallet":
        parts.append(f"{entity_label} {wallet_label}")
    elif entity_label:
        parts.append(entity_label)
    elif wallet_label and wallet_label != "Unknown Wallet":
        parts.append(wallet_label)

    purpose = str(context.get("exchange_transfer_purpose") or "").strip()
    purpose_strength = str(
        context.get("exchange_transfer_purpose_strength")
        or context.get("exchange_same_entity_strength")
        or "no"
    ).strip()
    if purpose:
        parts.append(_exchange_transfer_purpose_label(purpose, purpose_strength))

    internality = str(context.get("exchange_internality") or "").strip()
    if internality == "confirmed":
        parts.append("同实体内部")
    elif internality == "likely":
        parts.append("同实体待确认")

    if not parts:
        return str(context.get("market_context_label") or context.get("role_summary") or "场景待确认")
    return " · ".join(parts[:3])


def _invalidation_brief(context: dict) -> str:
    return str(
        context.get("operational_intent_invalidation")
        or context.get("failure_conditions")
        or "等待更多失效条件"
    )


def _next_check_brief(context: dict) -> str:
    return str(
        context.get("operational_intent_next_check")
        or context.get("action_hint")
        or "观察后续路径"
    )


def _market_implication_short(context: dict, event: Event) -> str:
    return str(
        context.get("operational_intent_market_implication")
        or context.get("lp_meaning_brief")
        or _explanation_brief(context, event)
    )


def _intent_message(signal: Signal, event: Event, context: dict, raw: dict, *, debug: bool = False) -> str:
    del raw
    actor = str(context.get("operational_actor_label") or _object_label(signal, context))
    object_or_pair = str(
        context.get("operational_object_label")
        or context.get("pair_label")
        or context.get("pool_label")
        or _object_label(signal, context)
    )
    confidence = _confidence_badge(context)
    evidence_pack = str(context.get("operational_intent_why") or _evidence_brief(context))
    lines = [
        f"{actor}｜{_operational_intent_label(context, event)}｜{confidence}",
        f"{object_or_pair}｜{_usd_value_text(signal.usd_value)}｜{_entity_context_brief(context)}",
        evidence_pack,
        _market_implication_short(context, event),
    ]
    if debug or str(signal.delivery_class or "") == "primary" or confidence == "高":
        lines.append(f"失效：{_invalidation_brief(context)}｜继续看：{_next_check_brief(context)}")
    if debug or str(signal.delivery_class or "") == "primary":
        lines.append(_tx_line(signal, compact=True))
    return _join_lines(lines)


def _market_state_label(context: dict, event: Event) -> str:
    return str(context.get("market_state_label") or _headline_label(context, event))


def _fact_brief(signal: Signal, context: dict, event: Event) -> str:
    return str(
        context.get("fact_brief")
        or f"{context.get('fact_label') or context.get('action_label') or signal.core_action or signal.type}｜{_usd_value_text(signal.usd_value)}"
    )


def _explanation_brief(context: dict, event: Event) -> str:
    return str(
        context.get("explanation_brief")
        or context.get("market_implication")
        or context.get("intent_detail")
        or context.get("inference_label")
        or event.intent_type
    )


def _evidence_brief(context: dict) -> str:
    return str(context.get("evidence_brief") or context.get("evidence_summary") or context.get("confirmation_label") or "弱证据")


def _action_hint(context: dict) -> str:
    return str(context.get("action_hint") or context.get("followup_summary") or "观察后续是否继续确认")


def _market_maker_fact_brief(signal: Signal, context: dict, event: Event) -> str:
    return str(context.get("market_maker_fact_brief") or _fact_brief(signal, context, event))


def _market_maker_explanation_brief(context: dict, event: Event) -> str:
    return str(context.get("market_maker_explanation_brief") or _explanation_brief(context, event))


def _market_maker_evidence_brief(context: dict) -> str:
    return str(context.get("market_maker_evidence_brief") or _evidence_brief(context))


def _market_maker_action_hint(context: dict) -> str:
    return str(context.get("market_maker_action_hint") or _action_hint(context))


def _update_brief(context: dict) -> str:
    return str(context.get("update_brief") or context.get("followup_detail") or context.get("followup_label") or "出现新的后续确认")


def _lp_burst_summary(context: dict) -> str:
    return str(context.get("lp_burst_summary") or "").strip()


def _lp_burst_line(signal: Signal, context: dict) -> str:
    burst_applied = bool(
        context.get("lp_burst_fastlane_applied")
        or signal.metadata.get("lp_burst_fastlane_applied")
        or str(signal.delivery_reason or "").startswith("lp_burst_directional_")
    )
    if not burst_applied:
        return ""
    summary = _lp_burst_summary(context)
    if summary:
        return f"Burst：{summary}"
    label = str(context.get("lp_burst_label") or "同池同向 burst").strip()
    window_label = str(context.get("lp_burst_window_label") or "").strip()
    return f"Burst：{label}{('｜' + window_label) if window_label else ''}"


def _lp_route_semantics_line(signal: Signal, context: dict) -> str:
    semantics = str(
        context.get("lp_route_semantics")
        or signal.metadata.get("lp_route_semantics")
        or ""
    ).strip()
    if semantics == "burst_main_entry":
        return "入口：LP burst 主入口｜短窗口连续爆发"
    if semantics == "single_shot_fallback":
        return "入口：LP 首击兜底｜单笔/首击型稀有样本"
    if semantics == "directional_exception_entry":
        return "入口：LP directional 例外入口｜结构强但单笔略低"
    if semantics == "directional_standard_entry":
        return "入口：LP directional 常规入口"
    return ""


def _path_line(context: dict, raw: dict) -> str:
    path_text, _, _ = _path_bundle(context, raw)
    return str(context.get("path_label") or path_text)


def _lp_trend_slot_line(context: dict) -> str:
    label = str(context.get("lp_trend_display_label") or "").strip()
    profile = str(context.get("lp_trend_display_profile") or "").strip()
    state = str(context.get("lp_trend_display_state") or "").strip()
    if not (label or profile or state):
        return ""
    parts = [item for item in [label, profile, state] if item]
    return f"趋势档位：{'｜'.join(parts)}"


def _lp_trend_mode_line(context: dict) -> str:
    mode = str(context.get("lp_trend_display_mode") or "").strip()
    if not mode:
        return ""
    return f"趋势模式：{mode}"


def _followup_explanation_prefix(signal: Signal, event: Event, context: dict) -> str:
    semantic = str(
        context.get("followup_semantic")
        or signal.metadata.get("followup_semantic")
        or event.metadata.get("followup_semantic")
        or ""
    ).strip()
    is_execution_confirmed = (
        semantic == "execution_confirmed"
        or str(event.kind or "") == "swap"
        or bool(signal.metadata.get("is_real_execution"))
        or bool(event.metadata.get("execution_confirmed"))
    )
    return "当前结构" if is_execution_confirmed else "当前解释"


def _lp_direction_field(signal: Signal, context: dict) -> str:
    intent_type = str(getattr(signal, "intent_type", "") or "")
    if intent_type == "pool_buy_pressure":
        return str(context.get("lp_action_brief") or "持续买压｜稳定币流入、标的流出")
    if intent_type == "pool_sell_pressure":
        return str(context.get("lp_action_brief") or "持续卖压｜标的流入、稳定币流出")
    return str(context.get("lp_action_brief") or context.get("action_label") or signal.type)


def _lp_liquidity_structure(context: dict) -> str:
    continuity = str(context.get("same_pool_continuity_label") or "单池单笔")
    resonance = str(context.get("multi_pool_resonance_label") or "无跨池共振")
    return f"{continuity}｜{resonance}"


def _is_downstream_early_warning(context: dict) -> bool:
    stage = str(
        context.get("case_notification_stage")
        or context.get("downstream_followup_stage")
        or ""
    ).strip()
    if stage == "followup_opened":
        return True
    return bool(
        context.get("downstream_early_warning_allowed")
        or context.get("downstream_early_warning_emitted")
        or context.get("downstream_early_warning_candidate")
    )


def _header_for_variant(signal: Signal, context: dict, variant: str, compact: bool = False) -> str:
    if variant == "downstream_followup":
        if _is_downstream_early_warning(context):
            return f"📡 下游地址后续观察 | {str(signal.tier or 'Tier 3')} | 首条预警"
        return "🛰️ 下游地址后续观察"
    if variant == "followup":
        return "🔁 Followup 观察升级"
    if variant == "liquidation_risk":
        return "⚠️ 疑似清算风险"
    if variant == "liquidation_execution":
        return "🚨 疑似清算执行"
    if variant == "smart_money_primary":
        return "🎯 Smart Money 执行"
    if variant == "smart_money_observe":
        return "🧠 Smart Money 执行观察"
    if variant == "market_maker_primary":
        return "🏦 Market Maker 库存执行"
    if variant == "market_maker_observe":
        return "🏦 Market Maker 库存观察"
    if variant == "lp_directional":
        intent_type = str(getattr(signal, "intent_type", "") or "")
        if intent_type == "pool_buy_pressure":
            return "🌊 LP 买压雷达"
        if intent_type == "pool_sell_pressure":
            return "🌊 LP 卖压雷达"
        return "🌊 LP 买卖压力雷达"
    if variant == "lp_liquidity":
        return "🧱 LP 流动性雷达"
    return "📡 重点地址行为雷达"


def _alert_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "alert"),
        _stage_role_line(signal, context),
        _headline_label(context, event),
        f"对象：{_object_label(signal, context)}",
        f"事实：{_fact_brief(signal, context, event)}",
        f"解释：{_explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"路径：{_path_line(context, raw)}",
        f"继续看：{_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _followup_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "followup"),
        _stage_role_line(signal, context),
        f"对象：{_object_label(signal, context)}",
        f"更新：{_update_brief(context)}",
        f"当前解释：{_explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"下一步：{_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _downstream_followup_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    early_warning = _is_downstream_early_warning(context)
    anchor_label = str(context.get("downstream_anchor_label") or "重点地址")
    anchor_usd_value = float(signal.metadata.get("downstream_anchor_usd_value") or event.metadata.get("downstream_anchor_usd_value") or 0.0)
    anchor_token = str(event.metadata.get("downstream_anchor_token") or event.metadata.get("token_symbol") or event.token or "资产")
    lines = [
        _header_for_variant(signal, context, "downstream_followup"),
        _stage_role_line(signal, context),
        f"对象：{context.get('downstream_object_label') or _object_label(signal, context)}",
        f"来源锚点：{anchor_label}",
        f"首次大额转移：{_usd_value_text(anchor_usd_value)} {anchor_token}",
        f"后续动作：{context.get('downstream_followup_label') or context.get('update_brief') or ('超大额下游观察已开启' if early_warning else '观察窗口已开启')}",
        f"当前更像：{context.get('downstream_followup_detail') or _explanation_brief(context, event)}",
        f"证据：{context.get('evidence_brief') or _evidence_brief(context)}",
        f"路径：{context.get('downstream_followup_path_label') or _path_line(context, raw)}",
        f"继续看：{context.get('downstream_followup_next_hint') or ('进入交易所 / 命中 swap_execution / 出现分发' if early_warning else _action_hint(context))}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _lp_directional_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    observe = str(signal.delivery_class or "") == "observe"
    lines = [
        _header_for_variant(signal, context, "lp_directional"),
        _stage_role_line(signal, context),
        f"池子：{_pool_label(signal, context)}",
        f"方向：{_lp_direction_field(signal, context)}",
        _lp_trend_slot_line(context),
        _lp_trend_mode_line(context),
        f"金额：{_usd_value_text(signal.usd_value)}",
        _lp_burst_line(signal, context),
        _lp_route_semantics_line(signal, context),
        f"放量：{context.get('lp_volume_surge_label') or '无明显放量'}",
        f"连续：{context.get('same_pool_continuity_label') or '单池单笔'}",
        f"共振：{context.get('multi_pool_resonance_label') or '无跨池共振'}",
        f"含义：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"继续看：{_action_hint(context) if observe else context.get('action_hint') or '观察是否继续同向放大'}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _lp_liquidity_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    observe = str(signal.delivery_class or "") == "observe"
    lines = [
        _header_for_variant(signal, context, "lp_liquidity"),
        _stage_role_line(signal, context),
        f"池子：{_pool_label(signal, context)}",
        f"动作：{context.get('lp_action_brief') or context.get('action_label') or context.get('fact_label') or signal.type}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"放量：{context.get('lp_volume_surge_label') or '无明显放量'}",
        f"结构：{_lp_liquidity_structure(context)}",
        f"含义：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"继续看：{_action_hint(context) if observe else context.get('action_hint') or '观察是否继续结构放大'}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _liquidation_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    protocols = "/".join(list(context.get("liquidation_protocols") or [])[:3]) or "未命中"
    variant = _select_message_variant(signal, event, context)
    lines = [
        _header_for_variant(signal, context, variant),
        _stage_role_line(signal, context),
        f"池子：{_pool_label(signal, context)}",
        f"动作：{context.get('lp_action_brief') or context.get('action_label') or _headline_label(context, event)}",
        f"命中协议：{protocols}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"放量：{context.get('lp_volume_surge_label') or '无明显放量'}",
        f"连续：{context.get('same_pool_continuity_label') or '单池单笔'}",
        f"共振：{context.get('multi_pool_resonance_label') or '无跨池共振'}",
        f"当前解释：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"继续看：{_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _smart_money_primary_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    header = _header_for_variant(signal, context, "smart_money_primary")
    execution_brief = context.get("smart_money_fact_brief") or context.get("fact_brief") or f"真实执行｜{_usd_value_text(signal.usd_value)}"
    continuation = str(signal.metadata.get("smart_money_case_detail") or context.get("update_brief") or "").strip()
    lines = [
        header,
        _stage_role_line(signal, context),
        f"对象：{_object_label(signal, context)}",
        f"执行：{execution_brief}",
        f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
        f"下一步：{continuation or context.get('smart_money_action_hint') or _action_hint(context)}",
        f"当前解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
        f"方向：{context.get('directional_bias') or _headline_label(context, event)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _smart_money_observe_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "smart_money_observe"),
        _stage_role_line(signal, context),
        f"对象：{_object_label(signal, context)}",
        f"执行：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"路径：{_path_line(context, raw)}",
        f"当前解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
        f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
        f"连续/共振：{context.get('continuous_label') or '单笔'}｜{context.get('resonance_label') or '单地址孤立'}",
        f"为什么值得观察：{context.get('smart_money_observe_label') or '聪明钱执行观察路径'}",
        f"继续看：{context.get('smart_money_action_hint') or _action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _market_maker_primary_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "market_maker_primary"),
        _stage_role_line(signal, context),
        f"对象：{_object_label(signal, context)}",
        f"库存动作：{_market_maker_fact_brief(signal, context, event)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"路径：{_path_line(context, raw)}",
        f"库存语境：{_market_maker_explanation_brief(context, event)}",
        f"证据：{_market_maker_evidence_brief(context)}",
        f"继续看：{_market_maker_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _market_maker_observe_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "market_maker_observe"),
        _stage_role_line(signal, context),
        f"对象：{_object_label(signal, context)}",
        f"库存动作：{_market_maker_fact_brief(signal, context, event)}",
        f"当前更像：{_market_maker_explanation_brief(context, event)}",
        f"证据：{_market_maker_evidence_brief(context)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"路径：{_path_line(context, raw)}",
        f"共振/确认：{context.get('resonance_label') or '单地址孤立'}｜{context.get('confirmation_label') or '弱证据'}",
        f"为什么值得观察：{context.get('market_maker_observe_label') or '库存管理 / 流动性供给观察路径'}",
        f"继续看：{_market_maker_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _brief_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    del raw
    variant = _select_message_variant(signal, event, context)
    label = _pair_label(signal, context) if variant in BRIEF_BY_DEFAULT_VARIANTS else _object_label(signal, context)
    line1 = f"{label}｜{_market_state_label(context, event)}｜{_usd_value_text(signal.usd_value)}"
    line2 = str(
        context.get("evidence_brief")
        or context.get("fact_brief")
        or context.get("lp_meaning_brief")
        or _evidence_brief(context)
    )
    lines = [line1, line2]
    if str(signal.delivery_class or "").strip().lower() == "primary":
        lines.append(_tx_line(signal, compact=True))
    return _join_lines(lines)


def _short_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    variant = _select_message_variant(signal, event, context)
    if variant == "downstream_followup":
        early_warning = _is_downstream_early_warning(context)
        anchor_usd_value = float(signal.metadata.get("downstream_anchor_usd_value") or event.metadata.get("downstream_anchor_usd_value") or 0.0)
        anchor_token = str(event.metadata.get("downstream_anchor_token") or event.metadata.get("token_symbol") or event.token or "资产")
        lines = [
            _header_for_variant(signal, context, "downstream_followup", compact=True),
            _stage_role_line(signal, context),
            f"对象：{context.get('downstream_object_label') or _object_label(signal, context)}",
            f"来源锚点：{context.get('downstream_anchor_label') or '重点地址'}",
            f"首次大额转移：{_usd_value_text(anchor_usd_value)} {anchor_token}",
            f"后续动作：{context.get('downstream_followup_label') or context.get('update_brief') or ('超大额下游观察已开启' if early_warning else '观察窗口已开启')}",
            f"当前更像：{context.get('downstream_followup_detail') or _explanation_brief(context, event)}",
            f"证据：{context.get('evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "followup":
        lines = [
            _header_for_variant(signal, context, "followup", compact=True),
            _stage_role_line(signal, context),
            f"对象：{_object_label(signal, context)}",
            f"更新：{_update_brief(context)}",
            f"{_followup_explanation_prefix(signal, event, context)}：{_explanation_brief(context, event)}",
            f"证据：{_evidence_brief(context)}",
            f"路径：{_path_line(context, raw)}",
            f"继续看：{_action_hint(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "lp_directional":
        return _brief_message(signal, event, context, raw)
    if variant == "lp_liquidity":
        return _brief_message(signal, event, context, raw)
    if variant in {"liquidation_risk", "liquidation_execution"}:
        return _brief_message(signal, event, context, raw)
    if variant == "smart_money_primary":
        lines = [
            _header_for_variant(signal, context, "smart_money_primary", compact=True),
            _stage_role_line(signal, context),
            f"对象：{_object_label(signal, context)}",
            f"执行：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
            f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "market_maker_primary":
        lines = [
            _header_for_variant(signal, context, "market_maker_primary", compact=True),
            _stage_role_line(signal, context),
            f"对象：{_object_label(signal, context)}",
            f"库存动作：{_market_maker_fact_brief(signal, context, event)}",
            f"证据：{_market_maker_evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "smart_money_observe":
        lines = [
            _header_for_variant(signal, context, "smart_money_observe", compact=True),
            _stage_role_line(signal, context),
            f"执行：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
            f"解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
            f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "market_maker_observe":
        lines = [
            _header_for_variant(signal, context, "market_maker_observe", compact=True),
            _stage_role_line(signal, context),
            f"对象：{_object_label(signal, context)}",
            f"库存动作：{_market_maker_fact_brief(signal, context, event)}",
            f"当前更像：{_market_maker_explanation_brief(context, event)}",
            f"证据：{_market_maker_evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)

    lines = [
        _header_for_variant(signal, context, "alert", compact=True),
        _stage_role_line(signal, context),
        _headline_label(context, event),
        f"对象：{_object_label(signal, context)}",
        f"事实：{_fact_brief(signal, context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"继续看：{_action_hint(context)}",
        _tx_line(signal, compact=True),
    ]
    return _join_lines(lines)


def _long_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    variant = _select_message_variant(signal, event, context)
    if variant == "downstream_followup":
        return _downstream_followup_message(signal, event, context, raw)
    if variant == "followup":
        return _followup_message(signal, event, context, raw)
    if variant == "lp_directional":
        return _lp_directional_message(signal, event, context, raw)
    if variant == "lp_liquidity":
        return _lp_liquidity_message(signal, event, context, raw)
    if variant in {"liquidation_risk", "liquidation_execution"}:
        return _liquidation_message(signal, event, context, raw)
    if variant == "smart_money_primary":
        return _smart_money_primary_message(signal, event, context, raw)
    if variant == "market_maker_primary":
        return _market_maker_primary_message(signal, event, context, raw)
    if variant == "smart_money_observe":
        return _smart_money_observe_message(signal, event, context, raw)
    if variant == "market_maker_observe":
        return _market_maker_observe_message(signal, event, context, raw)
    return _alert_message(signal, event, context, raw)


def format_signal_message(signal: Signal, event: Event) -> str:
    raw = event.metadata.get("raw") or {}
    context = signal.context or {}
    template = _resolve_template(signal)
    variant = _select_message_variant(signal, event, context)

    if (
        template == "long"
        and not _message_template_overridden(signal)
        and variant in BRIEF_BY_DEFAULT_VARIANTS
    ):
        template = "brief"

    if template == "brief":
        return _brief_message(signal, event, context, raw)
    if template == "short":
        return _short_message(signal, event, context, raw)
    if template == "intent":
        return _intent_message(signal, event, context, raw)
    if template == "debug" and context.get("operational_intent_key"):
        return _intent_message(signal, event, context, raw, debug=True)
    return _long_message(signal, event, context, raw)

def _signal_type_display(signal: Signal) -> str:
    normalized = canonicalize_pool_semantic_key(signal.type)
    mapping = {
        "exchange_deposit_flow": "Exchange_Inflow",
        "exchange_withdraw_flow": "Exchange_Outflow",
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
        "clmm_partial_support_signal": "CLMM_Partial_Support",
    }
    return mapping.get(normalized, normalized or "Unknown_Signal")


def _direction_display(event: Event, context: dict, raw: dict) -> str:
    role_display = context.get("role_display") or context.get("role_summary") or "Unknown"
    role_display = (
        str(role_display)
        .replace("交易所热钱包", "Exchange HotWallet")
        .replace("交易所钱包", "Exchange Wallet")
        .replace("做市地址", "Market Maker")
        .replace("做市钱包", "Market Maker")
        .replace("名人钱包", "Celebrity Wallet")
        .replace("发行方金库", "Treasury")
        .replace("个人地址", "Personal Wallet")
        .replace("监控地址", "Watched Wallet")
        .replace("流动性池", "Liquidity Pool")
    )

    signal_type = str(context.get("signal_type_label") or "")
    side = str(event.side or raw.get("direction") or "")
    if signal_type in {"Pool_Buy_Pressure", "LP_Buy_Pressure"} or side == "池子买压":
        return f"BUY PRESSURE → {role_display}"
    if signal_type in {"Pool_Sell_Pressure", "LP_Sell_Pressure"} or side == "池子卖压":
        return f"SELL PRESSURE → {role_display}"
    if signal_type in {"Liquidity_Addition", "LP_Liquidity_Add"} or side == "增加流动性":
        return f"LIQUIDITY ADD → {role_display}"
    if signal_type in {"Liquidity_Removal", "LP_Liquidity_Remove"} or side == "减少流动性":
        return f"LIQUIDITY REMOVE → {role_display}"
    if signal_type in {"Pool_Rebalance", "LP_Rebalance"} or side == "池子再平衡":
        return f"REBALANCE → {role_display}"
    if side in {"流入", "买入"}:
        return f"IN → {role_display}"
    if side in {"流出", "卖出"}:
        return f"OUT → {role_display}"
    if side == "内部划转":
        return f"INTERNAL → {role_display}"
    return f"FLOW → {role_display}"


async def send_signal(signal: Signal, event: Event) -> bool:
    delivery_class = str(signal.delivery_class or event.delivery_class or "")
    if delivery_class == "drop":
        return False
    pending_case_notification = bool(
        signal.metadata.get("pending_case_notification")
        or signal.context.get("pending_case_notification")
        or event.metadata.get("pending_case_notification")
    )
    if (
        str(signal.metadata.get("case_family") or event.metadata.get("case_family") or "").strip()
        and signal.metadata.get("case_notification_allowed") is False
        and not pending_case_notification
    ):
        return False
    try:
        msg = format_signal_message(signal, event)
        _bump_notifier_stat("signals_attempted_send")
        delivered = await send(msg)
        if delivered:
            _bump_notifier_stat("signals_sent_ok")
        else:
            _bump_notifier_stat("signals_sent_failed", force_log=True)
        return delivered
    except Exception as e:
        _bump_notifier_stat("signals_sent_failed", force_log=True)
        print(f"信号发送失败: {e}")
        return False
