import asyncio
import os
from telegram import Bot

from config import (
    CHAT_ID,
    TELEGRAM_BOT_TOKEN,
)
from delivery_policy import can_emit_delivery_notification
from filter import format_address_label, is_smart_money_strategy_role, strategy_role_group
from models import Event, Signal

bot = Bot(token=TELEGRAM_BOT_TOKEN)
DEFAULT_MESSAGE_TEMPLATE = os.getenv("WHALE_MESSAGE_TEMPLATE", "long").strip().lower()


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
    return "short" if str(template).lower() == "short" else "long"


def _priority_text(context: dict) -> str:
    priority = context.get("priority")
    if isinstance(priority, int):
        return f"P{priority}"
    return "P3"


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
    if bool(context.get("downstream_followup_active")) or str(context.get("case_family") or "") == "downstream_counterparty_followup":
        return "downstream_followup"
    if str(context.get("liquidation_stage") or event.metadata.get("liquidation_stage") or "none") in {"risk", "execution"}:
        return "liquidation"
    if bool(context.get("followup_confirmed")) or str(context.get("case_family") or "") == "exchange_cross_token_followup":
        return "followup"
    if bool(context.get("lp_event")):
        return "lp"
    smart_money_variant = _smart_money_variant(signal, event, context)
    if smart_money_variant:
        return smart_money_variant
    return "alert"


def _is_smart_money_primary(signal: Signal, event: Event) -> bool:
    if str(signal.delivery_class or "") != "primary":
        return False
    if bool(signal.metadata.get("smart_money_case")):
        return True
    return is_smart_money_strategy_role(event.strategy_role) and str(event.intent_type or "") == "swap_execution"


def _smart_money_variant(signal: Signal, event: Event, context: dict) -> str:
    delivery_class = str(signal.delivery_class or "")
    if delivery_class not in {"primary", "observe"}:
        return ""
    role_group = strategy_role_group(
        context.get("strategy_role")
        or signal.metadata.get("strategy_role")
        or event.strategy_role
    )
    if role_group != "smart_money":
        return ""

    delivery_reason = str(signal.delivery_reason or event.delivery_reason or "")
    smart_money_reasons = {
        "smart_money_transfer_observe",
        "market_maker_execution_observe",
        "market_maker_inventory_observe",
        "market_maker_inventory_shift_observe",
        "market_maker_non_execution_observe",
        "smart_money_non_execution_observe",
        "smart_money_execution_observe",
        "smart_money_execution_primary",
        "smart_money_continuous_execution_primary",
        "market_maker_execution_primary",
    }
    if not (
        delivery_reason in smart_money_reasons
        or bool(signal.metadata.get("smart_money_case"))
        or is_smart_money_strategy_role(event.strategy_role)
    ):
        return ""

    style_variant = str(context.get("smart_money_style_variant") or signal.metadata.get("smart_money_style_variant") or "").strip()
    is_execution = bool(signal.metadata.get("is_real_execution")) or str(event.intent_type or "") == "swap_execution"
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


def _headline_label(context: dict, event: Event) -> str:
    return str(context.get("headline_label") or context.get("conclusion_label") or event.intent_type or "意图未明")


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


def _update_brief(context: dict) -> str:
    return str(context.get("update_brief") or context.get("followup_detail") or context.get("followup_label") or "出现新的后续确认")


def _path_line(context: dict, raw: dict) -> str:
    path_text, _, _ = _path_bundle(context, raw)
    return str(context.get("path_label") or path_text)


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


def _header_for_variant(signal: Signal, context: dict, variant: str, compact: bool = False) -> str:
    notification_stage_label = str(context.get("notification_stage_label") or "").strip()
    if variant == "downstream_followup":
        stage_label = str(
            context.get("downstream_followup_stage_label")
            or notification_stage_label
            or "观察开启"
        ).strip()
        return f"📡 下游地址后续观察 | {signal.tier} | {stage_label}"
    if variant == "followup":
        followup_label = str(context.get("followup_label") or context.get("intent_label") or "交易所方向流动").strip()
        stage_label = notification_stage_label or "观察升级"
        return f"📡 {followup_label}｜{stage_label}"
    if variant == "lp":
        if str(signal.delivery_class or "") == "observe":
            return f"📡 LP 观察级雷达 | {signal.tier}"
        return f"📡 LP 行为雷达 | {signal.tier}"
    if variant == "liquidation":
        stage = str(context.get("liquidation_stage") or "risk")
        if stage == "execution":
            return "📡 疑似清算执行雷达"
        return "📡 疑似清算风险雷达"
    if variant == "smart_money_primary":
        return f"📡 Smart Money 执行雷达 | {signal.tier}"
    if variant == "smart_money_observe":
        return f"📡 Smart Money 观察级雷达 | {signal.tier}"
    if variant == "market_maker_observe":
        return f"📡 Market Maker 观察级雷达 | {signal.tier}"

    header = f"📡 重点地址行为雷达 | {signal.tier} | {_priority_text(context)}"
    if notification_stage_label and not compact:
        return f"{header} | {notification_stage_label}"
    return header


def _alert_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "alert"),
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
        f"对象：{_object_label(signal, context)}",
        f"更新：{_update_brief(context)}",
        f"当前解释：{_explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"下一步：{_action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _downstream_followup_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    anchor_label = str(context.get("downstream_anchor_label") or "重点地址")
    anchor_usd_value = float(signal.metadata.get("downstream_anchor_usd_value") or event.metadata.get("downstream_anchor_usd_value") or 0.0)
    anchor_token = str(event.metadata.get("downstream_anchor_token") or event.metadata.get("token_symbol") or event.token or "资产")
    lines = [
        _header_for_variant(signal, context, "downstream_followup"),
        f"对象：{context.get('downstream_object_label') or _object_label(signal, context)}",
        f"来源锚点：{anchor_label}",
        f"首次大额转移：{_usd_value_text(anchor_usd_value)} {anchor_token}",
        f"后续动作：{context.get('downstream_followup_label') or context.get('update_brief') or '观察窗口已开启'}",
        f"当前更像：{context.get('downstream_followup_detail') or _explanation_brief(context, event)}",
        f"证据：{context.get('evidence_brief') or _evidence_brief(context)}",
        f"路径：{context.get('downstream_followup_path_label') or _path_line(context, raw)}",
        f"继续看：{context.get('downstream_followup_next_hint') or _action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _lp_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    observe = str(signal.delivery_class or "") == "observe"
    lines = [
        _header_for_variant(signal, context, "lp"),
        f"池子：{_pool_label(signal, context)}",
        f"动作：{context.get('lp_action_brief') or context.get('action_label') or context.get('fact_label') or signal.type}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"放量：{context.get('lp_volume_surge_label') or '无明显放量'}",
        f"连续：{context.get('same_pool_continuity_label') or '单池单笔'}",
        f"共振：{context.get('multi_pool_resonance_label') or '无跨池共振'}",
        f"含义：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
        f"证据：{_evidence_brief(context)}",
        f"继续看：{_action_hint(context) if observe else context.get('action_hint') or '观察是否继续同向放大'}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _liquidation_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    protocols = "/".join(list(context.get("liquidation_protocols") or [])[:3]) or "未命中"
    lines = [
        _header_for_variant(signal, context, "liquidation"),
        f"池子：{_pool_label(signal, context)}",
        f"动作：{context.get('lp_action_brief') or context.get('action_label') or _headline_label(context, event)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"命中协议：{protocols}",
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
    stage_label = str(context.get("notification_stage_label") or "").strip()
    header = _header_for_variant(signal, context, "smart_money_primary")
    if stage_label:
        header = f"{header} | {stage_label}"

    execution_brief = context.get("smart_money_fact_brief") or context.get("fact_brief") or f"真实执行｜{_usd_value_text(signal.usd_value)}"
    continuation = str(signal.metadata.get("smart_money_case_detail") or context.get("update_brief") or "").strip()
    lines = [
        header,
        f"对象：{_object_label(signal, context)}",
        f"执行：{execution_brief}",
        f"方向：{context.get('directional_bias') or _headline_label(context, event)}",
        f"当前解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
        f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
        f"继续看：{continuation or context.get('smart_money_action_hint') or _action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _smart_money_observe_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "smart_money_observe"),
        f"对象：{_object_label(signal, context)}",
        f"动作：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"路径：{_path_line(context, raw)}",
        f"当前解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
        f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
        f"连续/共振：{context.get('continuous_label') or '单笔'}｜{context.get('resonance_label') or '单地址孤立'}",
        f"为什么值得观察：{context.get('smart_money_observe_label') or '聪明钱观察路径'}",
        f"继续看：{context.get('smart_money_action_hint') or _action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _market_maker_observe_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    lines = [
        _header_for_variant(signal, context, "market_maker_observe"),
        f"对象：{_object_label(signal, context)}",
        f"动作：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
        f"金额：{_usd_value_text(signal.usd_value)}",
        f"路径：{_path_line(context, raw)}",
        f"当前更像：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
        f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
        f"共振/确认：{context.get('resonance_label') or '单地址孤立'}｜{context.get('confirmation_label') or '弱证据'}",
        f"为什么值得观察：{context.get('smart_money_observe_label') or '做市库存观察路径'}",
        f"继续看：{context.get('smart_money_action_hint') or _action_hint(context)}",
        _tx_line(signal),
    ]
    return _join_lines(lines)


def _short_message(signal: Signal, event: Event, context: dict, raw: dict) -> str:
    variant = _select_message_variant(signal, event, context)
    if variant == "downstream_followup":
        lines = [
            _header_for_variant(signal, context, "downstream_followup", compact=True),
            f"对象：{context.get('downstream_object_label') or _object_label(signal, context)}",
            f"锚点：{context.get('downstream_anchor_label') or '重点地址'}",
            f"动作：{context.get('downstream_followup_label') or context.get('update_brief') or '观察窗口已开启'}",
            f"解释：{context.get('downstream_followup_detail') or _explanation_brief(context, event)}",
            f"证据：{context.get('evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "followup":
        lines = [
            _header_for_variant(signal, context, "followup", compact=True),
            f"对象：{_object_label(signal, context)}",
            f"更新：{_update_brief(context)}",
            f"{_followup_explanation_prefix(signal, event, context)}：{_explanation_brief(context, event)}",
            f"证据：{_evidence_brief(context)}",
            f"路径：{_path_line(context, raw)}",
            f"继续看：{_action_hint(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "lp":
        lines = [
            _header_for_variant(signal, context, "lp", compact=True),
            f"池子：{_pool_label(signal, context)}",
            f"动作：{context.get('lp_action_brief') or signal.type}",
            f"含义：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
            f"证据：{_evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "liquidation":
        lines = [
            _header_for_variant(signal, context, "liquidation", compact=True),
            f"池子：{_pool_label(signal, context)}",
            f"动作：{context.get('lp_action_brief') or _headline_label(context, event)}",
            f"解释：{context.get('lp_meaning_brief') or _explanation_brief(context, event)}",
            f"证据：{_evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "smart_money_primary":
        lines = [
            _header_for_variant(signal, context, "smart_money_primary", compact=True),
            f"对象：{_object_label(signal, context)}",
            f"执行：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
            f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "smart_money_observe":
        lines = [
            _header_for_variant(signal, context, "smart_money_observe", compact=True),
            f"动作：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
            f"解释：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
            f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)
    if variant == "market_maker_observe":
        lines = [
            _header_for_variant(signal, context, "market_maker_observe", compact=True),
            f"动作：{context.get('smart_money_fact_brief') or _fact_brief(signal, context, event)}",
            f"当前更像：{context.get('smart_money_explanation_brief') or _explanation_brief(context, event)}",
            f"证据：{context.get('smart_money_evidence_brief') or _evidence_brief(context)}",
            _tx_line(signal, compact=True),
        ]
        return _join_lines(lines)

    lines = [
        _header_for_variant(signal, context, "alert", compact=True),
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
    if variant == "lp":
        return _lp_message(signal, event, context, raw)
    if variant == "liquidation":
        return _liquidation_message(signal, event, context, raw)
    if variant == "smart_money_primary":
        return _smart_money_primary_message(signal, event, context, raw)
    if variant == "smart_money_observe":
        return _smart_money_observe_message(signal, event, context, raw)
    if variant == "market_maker_observe":
        return _market_maker_observe_message(signal, event, context, raw)
    return _alert_message(signal, event, context, raw)


def format_signal_message(signal: Signal, event: Event) -> str:
    raw = event.metadata.get("raw") or {}
    context = signal.context or {}
    template = _resolve_template(signal)

    if template == "short":
        return _short_message(signal, event, context, raw)
    return _long_message(signal, event, context, raw)

def _signal_type_display(signal: Signal) -> str:
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
        "lp_buy_pressure": "LP_Buy_Pressure",
        "lp_sell_pressure": "LP_Sell_Pressure",
        "lp_liquidity_add": "LP_Liquidity_Add",
        "lp_liquidity_remove": "LP_Liquidity_Remove",
        "lp_rebalance": "LP_Rebalance",
    }
    return mapping.get(str(signal.type or ""), str(signal.type or "Unknown_Signal"))


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
    if signal_type == "LP_Buy_Pressure" or side == "池子买压":
        return f"BUY PRESSURE → {role_display}"
    if signal_type == "LP_Sell_Pressure" or side == "池子卖压":
        return f"SELL PRESSURE → {role_display}"
    if signal_type == "LP_Liquidity_Add" or side == "增加流动性":
        return f"LIQUIDITY ADD → {role_display}"
    if signal_type == "LP_Liquidity_Remove" or side == "减少流动性":
        return f"LIQUIDITY REMOVE → {role_display}"
    if signal_type == "LP_Rebalance" or side == "池子再平衡":
        return f"REBALANCE → {role_display}"
    if side in {"流入", "买入"}:
        return f"IN → {role_display}"
    if side in {"流出", "卖出"}:
        return f"OUT → {role_display}"
    if side == "内部划转":
        return f"INTERNAL → {role_display}"
    return f"FLOW → {role_display}"


async def send_signal(signal: Signal, event: Event) -> bool:
    if not can_emit_delivery_notification(event, signal):
        return False
    try:
        msg = format_signal_message(signal, event)
        return await send(msg)
    except Exception as e:
        print(f"信号发送失败: {e}")
        return False
