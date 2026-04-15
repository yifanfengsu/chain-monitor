from dataclasses import dataclass, field
import math

from config import (
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    EXCHANGE_NOISE_CONFIRMATION_MIN,
    EXCHANGE_DIRECTIONAL_VALUE_BONUS_MAX,
    EXCHANGE_DIRECTIONAL_VALUE_THRESHOLD_RATIO,
    LIQUIDATION_EXECUTION_MIN_SCORE,
    LIQUIDATION_MIN_USD,
    LIQUIDATION_PRIMARY_MIN_USD,
    LIQUIDATION_RISK_MIN_SCORE,
    MARKET_MAKER_OBSERVE_GATE_FLOOR,
    MARKET_MAKER_OBSERVE_MIN_CONFIRMATION,
    MARKET_MAKER_OBSERVE_MIN_RESONANCE,
    MARKET_MAKER_OBSERVE_VALUE_BONUS_MAX,
    LP_VALUE_BONUS_MAX,
    LP_BUY_FAST_EXCEPTION_MIN_ACTION_INTENSITY,
    LP_BUY_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO,
    LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY,
    LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO,
    LP_PREALERT_LIQUIDITY_ADDITION_MIN_ACTION_INTENSITY,
    LP_PREALERT_LIQUIDITY_ADDITION_MIN_VOLUME_SURGE_RATIO,
    LP_PREALERT_LIQUIDITY_REMOVAL_MIN_ACTION_INTENSITY,
    LP_PREALERT_LIQUIDITY_REMOVAL_MIN_VOLUME_SURGE_RATIO,
    LP_PREALERT_MIN_CONFIRMATION,
    LP_PREALERT_PRIMARY_TREND_MIN_MATCHES,
    LP_PREALERT_MIN_PRICING_CONFIDENCE,
    LP_PREALERT_MIN_RESERVE_SKEW,
    LP_SWEEP_MAX_BURST_WINDOW_SEC,
    LP_SWEEP_MIN_ACTION_INTENSITY,
    LP_SWEEP_MIN_BURST_EVENT_COUNT,
    LP_SWEEP_MIN_RESERVE_SKEW,
    LP_SWEEP_MIN_SAME_POOL_CONTINUITY,
    LP_SWEEP_MIN_VOLUME_SURGE_RATIO,
    LP_BUY_OBSERVE_THRESHOLD_RATIO_FLOOR,
    LP_BUY_TREND_BURST_MIN_ACTION_INTENSITY,
    LP_BUY_TREND_BURST_MIN_VOLUME_SURGE_RATIO,
    LP_BUY_TREND_CONTINUATION_MIN_ACTION_INTENSITY,
    LP_BUY_TREND_CONTINUATION_MIN_VOLUME_SURGE_RATIO,
    LP_BUY_TREND_CONTINUATION_THRESHOLD_RATIO_FLOOR,
    LP_BUY_TREND_REVERSAL_MIN_ACTION_INTENSITY,
    LP_BUY_TREND_REVERSAL_MIN_VOLUME_SURGE_RATIO,
    LP_BUY_TREND_REVERSAL_THRESHOLD_RATIO_FLOOR,
    LP_BURST_FASTLANE_ENABLE,
    LP_BURST_MIN_ACTION_INTENSITY,
    LP_BURST_MIN_EVENT_COUNT,
    LP_BURST_MIN_MAX_SINGLE_USD,
    LP_BURST_MIN_PRICING_CONFIDENCE,
    LP_BURST_MIN_RESERVE_SKEW,
    LP_BURST_MIN_TOTAL_USD,
    LP_BURST_MIN_VOLUME_SURGE_RATIO,
    LP_DIRECTIONAL_COOLDOWN_SEC,
    LP_SELL_FAST_EXCEPTION_MIN_ACTION_INTENSITY,
    LP_SELL_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO,
    LP_SELL_OBSERVE_THRESHOLD_RATIO_FLOOR,
    LP_FAST_EXCEPTION_MIN_ACTION_INTENSITY,
    LP_FAST_EXCEPTION_MIN_PRICING_CONFIDENCE,
    LP_FAST_EXCEPTION_MIN_RESERVE_SKEW,
    LP_FAST_EXCEPTION_MIN_SAME_POOL_CONTINUITY,
    LP_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO,
    LP_OBSERVE_MIN_CONFIDENCE,
    LP_OBSERVE_MAX_USD_GAP,
    LP_OBSERVE_MIN_USD,
    LP_NOTIFY_HARD_MIN_USD,
    LP_OBSERVE_THRESHOLD_RATIO_FLOOR,
    LP_PRIMARY_MIN_CONFIDENCE,
    LP_PRIMARY_MIN_SURGE_RATIO,
    LP_STRUCTURE_MIN_USD_PER_EVENT,
    LP_TREND_BURST_MIN_ACTION_INTENSITY,
    LP_TREND_BURST_MIN_EVENT_COUNT,
    LP_TREND_BURST_MIN_MAX_SINGLE_USD,
    LP_TREND_BURST_MIN_PRICING_CONFIDENCE,
    LP_TREND_BURST_MIN_RESERVE_SKEW,
    LP_TREND_BURST_MIN_TOTAL_USD,
    LP_TREND_BURST_MIN_VOLUME_SURGE_RATIO,
    LP_VOLUME_SURGE_MIN_RATIO,
    QUALITY_GATE_COOLDOWN_SEC,
    QUALITY_GATE_MIN_GLOBAL_USD,
    QUALITY_GATE_MIN_PRICE_IMPACT_RATIO,
    QUALITY_GATE_MIN_RELATIVE_ADDRESS_SIZE,
    QUALITY_GATE_MIN_TOKEN_LIQUIDITY_USD,
    QUALITY_GATE_MIN_TOKEN_VOLUME_RATIO,
    QUALITY_GATE_REQUIRE_NON_NORMAL_BEHAVIOR,
    QUALITY_GATE_SCORE_THRESHOLD,
    QUALITY_GATE_STABLE_NON_SWAP_HARD_FILTER_USD,
    QUALITY_GATE_STABLE_TRANSFER_MIN_USD,
    QUALITY_GATE_TIER1_THRESHOLD,
    QUALITY_GATE_TIER2_THRESHOLD,
    SMART_MONEY_HIGH_VALUE_GATE_MAX_QUALITY_GAP,
    SMART_MONEY_HIGH_VALUE_GATE_MIN_USD,
    SMART_MONEY_HIGH_VALUE_GATE_THRESHOLD_RATIO,
    SMART_MONEY_NON_EXEC_GATE_FLOOR,
    SMART_MONEY_VALUE_BONUS_MAX,
    SMART_MONEY_VALUE_THRESHOLD_RATIO,
    TOKEN_RESONANCE_MIN_ADDRESSES,
    TOKEN_RESONANCE_WINDOW_SEC,
    MARKET_MAKER_HIGH_VALUE_GATE_THRESHOLD_RATIO,
)
from constants import STABLE_TOKEN_CONTRACTS
from filter import (
    get_threshold,
    is_exchange_strategy_role,
    is_market_maker_strategy_role,
    is_priority_smart_money_strategy_role,
    is_smart_money_strategy_role,
    strategy_role_group,
)
from lp_registry import classify_trend_pool_meta
from models import Event
from state_manager import StateManager


EXCHANGE_STRICT_ROLES = {
    "exchange_hot_wallet",
    "exchange_deposit_wallet",
    "exchange_trading_wallet",
}
LP_FAST_EXCEPTION_GATE_VERSION = "lp_directional_trend_state_v4"
LP_SWEEP_SEMANTIC_SUBTYPES = {
    "buy_side_liquidity_sweep",
    "sell_side_liquidity_sweep",
}


def detect_lp_liquidity_sweep(
    *,
    event: Event,
    reserve_skew: float,
    action_intensity: float,
    same_pool_continuity: int,
    multi_pool_resonance: int,
    pool_volume_surge_ratio: float,
    lp_burst_event_count: int,
    lp_burst_window_sec: int,
    price_impact_ratio: float,
    min_price_impact_ratio: float,
    liquidation_stage: str,
) -> dict:
    intent_type = str(event.intent_type or "")
    if event.kind != "swap":
        return {
            "detected": False,
            "semantic_subtype": "",
            "sweep_confidence": "",
            "display_label": "",
            "reason": "non_swap_event",
        }
    if intent_type not in {"pool_buy_pressure", "pool_sell_pressure"}:
        return {
            "detected": False,
            "semantic_subtype": "",
            "sweep_confidence": "",
            "display_label": "",
            "reason": "non_directional_lp_intent",
        }
    if str(liquidation_stage or "none") == "execution":
        return {
            "detected": False,
            "semantic_subtype": "",
            "sweep_confidence": "",
            "display_label": "",
            "reason": "liquidation_execution_priority",
        }

    burst_like = (
        pool_volume_surge_ratio >= LP_SWEEP_MIN_VOLUME_SURGE_RATIO
        or same_pool_continuity >= LP_SWEEP_MIN_SAME_POOL_CONTINUITY
        or (
            lp_burst_event_count >= LP_SWEEP_MIN_BURST_EVENT_COUNT
            and lp_burst_window_sec > 0
            and lp_burst_window_sec <= LP_SWEEP_MAX_BURST_WINDOW_SEC
        )
    )
    structure_ready = (
        reserve_skew >= LP_SWEEP_MIN_RESERVE_SKEW
        and action_intensity >= LP_SWEEP_MIN_ACTION_INTENSITY
        and burst_like
        and price_impact_ratio >= float(min_price_impact_ratio or 0.0)
    )
    if not structure_ready:
        return {
            "detected": False,
            "semantic_subtype": "",
            "sweep_confidence": "",
            "display_label": "",
            "reason": "lp_sweep_structure_not_met",
        }

    del multi_pool_resonance
    semantic_subtype = (
        "buy_side_liquidity_sweep"
        if intent_type == "pool_buy_pressure"
        else "sell_side_liquidity_sweep"
    )
    return {
        "detected": True,
        "semantic_subtype": semantic_subtype,
        "sweep_confidence": "likely",
        "display_label": "买方清扫" if semantic_subtype == "buy_side_liquidity_sweep" else "卖方清扫",
        "reason": "swap_short_window_liquidity_consumption",
    }


@dataclass
class GateDecision:
    passed: bool
    reason: str
    quality_score: float
    quality_tier: str
    quality_threshold: float
    metrics: dict = field(default_factory=dict)


class SignalQualityGate:
    """
    信号质量门控：
    - 先执行必要硬过滤（极小额、低可信定价、低信息稳定币 transfer）
    - 再执行 quality score 评分
    - 冷却以 cooldown_key 为准，而不是 address 级别
    - 对交易所热钱包相关路径施加更强确认要求，抑制单跳噪声
    """

    def __init__(
        self,
        state_manager: StateManager,
        min_global_usd: float = QUALITY_GATE_MIN_GLOBAL_USD,
        stable_transfer_min_usd: float = QUALITY_GATE_STABLE_TRANSFER_MIN_USD,
        stable_non_swap_hard_filter_usd: float = QUALITY_GATE_STABLE_NON_SWAP_HARD_FILTER_USD,
        min_token_liquidity_usd: float = QUALITY_GATE_MIN_TOKEN_LIQUIDITY_USD,
        min_price_impact_ratio: float = QUALITY_GATE_MIN_PRICE_IMPACT_RATIO,
        min_relative_address_size: float = QUALITY_GATE_MIN_RELATIVE_ADDRESS_SIZE,
        min_token_volume_ratio: float = QUALITY_GATE_MIN_TOKEN_VOLUME_RATIO,
        cooldown_sec: int = QUALITY_GATE_COOLDOWN_SEC,
        require_non_normal_behavior: bool = QUALITY_GATE_REQUIRE_NON_NORMAL_BEHAVIOR,
        score_threshold: float = QUALITY_GATE_SCORE_THRESHOLD,
        tier1_threshold: float = QUALITY_GATE_TIER1_THRESHOLD,
        tier2_threshold: float = QUALITY_GATE_TIER2_THRESHOLD,
        exchange_noise_confirmation_min: float = EXCHANGE_NOISE_CONFIRMATION_MIN,
        token_resonance_min_addresses: int = TOKEN_RESONANCE_MIN_ADDRESSES,
        token_resonance_window_sec: int = TOKEN_RESONANCE_WINDOW_SEC,
    ) -> None:
        self.state_manager = state_manager
        self.min_global_usd = float(min_global_usd)
        self.stable_transfer_min_usd = float(stable_transfer_min_usd)
        self.stable_non_swap_hard_filter_usd = float(stable_non_swap_hard_filter_usd)
        self.min_token_liquidity_usd = float(min_token_liquidity_usd)
        self.min_price_impact_ratio = float(min_price_impact_ratio)
        self.min_relative_address_size = float(min_relative_address_size)
        self.min_token_volume_ratio = float(min_token_volume_ratio)
        self.cooldown_sec = int(cooldown_sec)
        self.lp_directional_cooldown_sec = int(LP_DIRECTIONAL_COOLDOWN_SEC)
        self.require_non_normal_behavior = bool(require_non_normal_behavior)
        self.score_threshold = float(score_threshold)
        self.tier1_threshold = float(tier1_threshold)
        self.tier2_threshold = float(tier2_threshold)
        self.exchange_noise_confirmation_min = float(exchange_noise_confirmation_min)
        self.token_resonance_min_addresses = int(token_resonance_min_addresses)
        self.token_resonance_window_sec = int(token_resonance_window_sec)
        self.lp_observe_threshold_ratio_floor = float(LP_OBSERVE_THRESHOLD_RATIO_FLOOR)
        self.lp_observe_max_usd_gap = float(LP_OBSERVE_MAX_USD_GAP)
        self.lp_notify_hard_min_usd = float(LP_NOTIFY_HARD_MIN_USD)
        self.lp_structure_min_usd_per_event = float(LP_STRUCTURE_MIN_USD_PER_EVENT)
        self.lp_fast_exception_min_same_pool_continuity = int(LP_FAST_EXCEPTION_MIN_SAME_POOL_CONTINUITY)
        self.lp_fast_exception_min_volume_surge_ratio = float(LP_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO)
        self.lp_fast_exception_min_action_intensity = float(LP_FAST_EXCEPTION_MIN_ACTION_INTENSITY)
        self.lp_fast_exception_min_reserve_skew = float(LP_FAST_EXCEPTION_MIN_RESERVE_SKEW)
        self.lp_fast_exception_min_pricing_confidence = float(LP_FAST_EXCEPTION_MIN_PRICING_CONFIDENCE)
        self.lp_sell_observe_threshold_ratio_floor = float(LP_SELL_OBSERVE_THRESHOLD_RATIO_FLOOR)
        self.lp_sell_fast_exception_min_volume_surge_ratio = float(LP_SELL_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO)
        self.lp_sell_fast_exception_min_action_intensity = float(LP_SELL_FAST_EXCEPTION_MIN_ACTION_INTENSITY)
        self.lp_buy_observe_threshold_ratio_floor = float(LP_BUY_OBSERVE_THRESHOLD_RATIO_FLOOR)
        self.lp_buy_fast_exception_min_volume_surge_ratio = float(LP_BUY_FAST_EXCEPTION_MIN_VOLUME_SURGE_RATIO)
        self.lp_buy_fast_exception_min_action_intensity = float(LP_BUY_FAST_EXCEPTION_MIN_ACTION_INTENSITY)
        self.lp_buy_trend_continuation_threshold_ratio_floor = float(LP_BUY_TREND_CONTINUATION_THRESHOLD_RATIO_FLOOR)
        self.lp_buy_trend_continuation_min_volume_surge_ratio = float(LP_BUY_TREND_CONTINUATION_MIN_VOLUME_SURGE_RATIO)
        self.lp_buy_trend_continuation_min_action_intensity = float(LP_BUY_TREND_CONTINUATION_MIN_ACTION_INTENSITY)
        self.lp_buy_trend_reversal_threshold_ratio_floor = float(LP_BUY_TREND_REVERSAL_THRESHOLD_RATIO_FLOOR)
        self.lp_buy_trend_reversal_min_volume_surge_ratio = float(LP_BUY_TREND_REVERSAL_MIN_VOLUME_SURGE_RATIO)
        self.lp_buy_trend_reversal_min_action_intensity = float(LP_BUY_TREND_REVERSAL_MIN_ACTION_INTENSITY)
        self.lp_burst_fastlane_enable = bool(LP_BURST_FASTLANE_ENABLE)
        self.lp_burst_min_event_count = int(LP_BURST_MIN_EVENT_COUNT)
        self.lp_burst_min_total_usd = float(LP_BURST_MIN_TOTAL_USD)
        self.lp_burst_min_max_single_usd = float(LP_BURST_MIN_MAX_SINGLE_USD)
        self.lp_burst_min_volume_surge_ratio = float(LP_BURST_MIN_VOLUME_SURGE_RATIO)
        self.lp_burst_min_action_intensity = float(LP_BURST_MIN_ACTION_INTENSITY)
        self.lp_burst_min_reserve_skew = float(LP_BURST_MIN_RESERVE_SKEW)
        self.lp_burst_min_pricing_confidence = float(LP_BURST_MIN_PRICING_CONFIDENCE)
        self.lp_prealert_min_pricing_confidence = float(LP_PREALERT_MIN_PRICING_CONFIDENCE)
        self.lp_prealert_min_reserve_skew = float(LP_PREALERT_MIN_RESERVE_SKEW)
        self.lp_prealert_min_confirmation = float(LP_PREALERT_MIN_CONFIRMATION)
        self.lp_prealert_primary_trend_min_matches = int(LP_PREALERT_PRIMARY_TREND_MIN_MATCHES)
        self.lp_prealert_directional_min_action_intensity = float(LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY)
        self.lp_prealert_directional_min_volume_surge_ratio = float(LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO)
        self.lp_prealert_liquidity_removal_min_action_intensity = float(LP_PREALERT_LIQUIDITY_REMOVAL_MIN_ACTION_INTENSITY)
        self.lp_prealert_liquidity_removal_min_volume_surge_ratio = float(LP_PREALERT_LIQUIDITY_REMOVAL_MIN_VOLUME_SURGE_RATIO)
        self.lp_prealert_liquidity_addition_min_action_intensity = float(LP_PREALERT_LIQUIDITY_ADDITION_MIN_ACTION_INTENSITY)
        self.lp_prealert_liquidity_addition_min_volume_surge_ratio = float(LP_PREALERT_LIQUIDITY_ADDITION_MIN_VOLUME_SURGE_RATIO)
        self.lp_trend_burst_min_event_count = int(LP_TREND_BURST_MIN_EVENT_COUNT)
        self.lp_trend_burst_min_total_usd = float(LP_TREND_BURST_MIN_TOTAL_USD)
        self.lp_trend_burst_min_max_single_usd = float(LP_TREND_BURST_MIN_MAX_SINGLE_USD)
        self.lp_trend_burst_min_volume_surge_ratio = float(LP_TREND_BURST_MIN_VOLUME_SURGE_RATIO)
        self.lp_trend_burst_min_action_intensity = float(LP_TREND_BURST_MIN_ACTION_INTENSITY)
        self.lp_trend_burst_min_reserve_skew = float(LP_TREND_BURST_MIN_RESERVE_SKEW)
        self.lp_trend_burst_min_pricing_confidence = float(LP_TREND_BURST_MIN_PRICING_CONFIDENCE)
        self.lp_buy_trend_burst_min_volume_surge_ratio = float(LP_BUY_TREND_BURST_MIN_VOLUME_SURGE_RATIO)
        self.lp_buy_trend_burst_min_action_intensity = float(LP_BUY_TREND_BURST_MIN_ACTION_INTENSITY)
        self.liquidation_min_usd = float(LIQUIDATION_MIN_USD)
        self.liquidation_primary_min_usd = float(LIQUIDATION_PRIMARY_MIN_USD)
        self.liquidation_risk_min_score = float(LIQUIDATION_RISK_MIN_SCORE)
        self.liquidation_execution_min_score = float(LIQUIDATION_EXECUTION_MIN_SCORE)
        self.allow_liquidation_risk_observe = bool(DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE)
        self.smart_money_value_bonus_max = float(SMART_MONEY_VALUE_BONUS_MAX)
        self.smart_money_value_threshold_ratio = float(SMART_MONEY_VALUE_THRESHOLD_RATIO)
        self.smart_money_non_exec_gate_floor = float(SMART_MONEY_NON_EXEC_GATE_FLOOR)
        self.smart_money_high_value_gate_min_usd = float(SMART_MONEY_HIGH_VALUE_GATE_MIN_USD)
        self.smart_money_high_value_gate_threshold_ratio = float(SMART_MONEY_HIGH_VALUE_GATE_THRESHOLD_RATIO)
        self.smart_money_high_value_gate_max_quality_gap = float(SMART_MONEY_HIGH_VALUE_GATE_MAX_QUALITY_GAP)
        self.market_maker_observe_gate_floor = float(MARKET_MAKER_OBSERVE_GATE_FLOOR)
        self.market_maker_observe_min_confirmation = float(MARKET_MAKER_OBSERVE_MIN_CONFIRMATION)
        self.market_maker_observe_min_resonance = float(MARKET_MAKER_OBSERVE_MIN_RESONANCE)
        self.market_maker_observe_value_bonus_max = float(MARKET_MAKER_OBSERVE_VALUE_BONUS_MAX)
        self.market_maker_high_value_gate_threshold_ratio = float(MARKET_MAKER_HIGH_VALUE_GATE_THRESHOLD_RATIO)
        self.lp_value_bonus_max = float(LP_VALUE_BONUS_MAX)
        self.exchange_directional_value_bonus_max = float(EXCHANGE_DIRECTIONAL_VALUE_BONUS_MAX)
        self.exchange_directional_value_threshold_ratio = float(EXCHANGE_DIRECTIONAL_VALUE_THRESHOLD_RATIO)

    def evaluate(
        self,
        event: Event,
        watch_meta: dict,
        behavior: dict,
        address_score: dict,
        token_score: dict,
        address_snapshot: dict,
        token_snapshot: dict,
    ) -> GateDecision:
        usd_value = float(event.usd_value or 0.0)
        token = (event.token or "").lower()
        behavior_type = behavior.get("behavior_type", "normal")
        behavior_conf = float(behavior.get("confidence") or 0.0)
        address_score_value = float(address_score.get("alpha_score") or address_score.get("score") or 0.0)
        address_structure_score = float(address_score.get("structure_score") or address_score.get("score") or 0.0)
        token_score_value = float(token_score.get("score") or token_score.get("token_quality_score") or 0.0)
        pricing_status = str(event.pricing_status or "unknown")
        pricing_confidence = float(event.pricing_confidence or 0.0)
        cooldown_key = self.state_manager.get_cooldown_key(event, intent_type=event.intent_type)
        cooldown_sec = self._cooldown_sec(event)
        base_threshold_usd = float(get_threshold(watch_meta))
        dynamic_min_usd = self._dynamic_min_usd(event=event, watch_meta=watch_meta)
        lp_event = self._is_lp_event(event=event, watch_meta=watch_meta)
        role_group = strategy_role_group(watch_meta.get("strategy_role") or event.strategy_role)
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        priority_smart_money = is_priority_smart_money_strategy_role(strategy_role)
        market_maker = is_market_maker_strategy_role(strategy_role)
        is_real_execution = event.kind == "swap" or str(event.intent_type or "") == "swap_execution"
        lp_analysis = event.metadata.get("lp_analysis") or {}
        reserve_skew = float(lp_analysis.get("reserve_skew") or 0.0)
        action_intensity = float(lp_analysis.get("action_intensity") or 0.0)
        same_pool_continuity = int(lp_analysis.get("same_pool_continuity") or 0)
        multi_pool_resonance = int(lp_analysis.get("multi_pool_resonance") or 0)
        pool_volume_surge_ratio = float(lp_analysis.get("pool_volume_surge_ratio") or 0.0)
        pool_window_trade_count = int(lp_analysis.get("pool_window_trade_count") or 0)
        pool_window_usd_total = float(lp_analysis.get("pool_window_usd_total") or 0.0)
        burst_state = event.metadata.get("lp_burst") or {}
        lp_burst_event_count = int(burst_state.get("lp_burst_event_count") or 0)
        lp_burst_total_usd = float(burst_state.get("lp_burst_total_usd") or 0.0)
        lp_burst_max_single_usd = float(burst_state.get("lp_burst_max_single_usd") or 0.0)
        lp_burst_same_pool_continuity = int(burst_state.get("lp_burst_same_pool_continuity") or 0)
        lp_burst_volume_surge_ratio = float(burst_state.get("lp_burst_volume_surge_ratio") or 0.0)
        lp_burst_action_intensity = float(burst_state.get("lp_burst_action_intensity") or 0.0)
        lp_burst_reserve_skew = float(burst_state.get("lp_burst_reserve_skew") or 0.0)
        lp_burst_first_ts = int(burst_state.get("lp_burst_first_ts") or 0)
        lp_burst_last_ts = int(burst_state.get("lp_burst_last_ts") or 0)
        lp_burst_window_sec = int(burst_state.get("lp_burst_window_sec") or 0)

        address_avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        address_max_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("max_usd")
            or address_snapshot.get("max_usd")
            or 0.0
        )
        abnormal_ratio = usd_value / address_avg_usd if address_avg_usd > 0 else 0.0
        relative_address_size = usd_value / address_avg_usd if address_avg_usd > 0 else 2.0
        relative_address_peak = usd_value / address_max_usd if address_max_usd > 0 else 1.0

        liquidity_proxy_usd = float(
            token_snapshot.get("liquidity_proxy_usd")
            or token_snapshot.get("liquidity_usd")
            or 0.0
        )
        price_impact_ratio = usd_value / max(liquidity_proxy_usd, 1.0)
        volume_24h_proxy_usd = float(
            token_snapshot.get("volume_24h_proxy_usd")
            or token_snapshot.get("volume_24h_usd")
            or token_snapshot.get("volume_usd")
            or 0.0
        )
        token_volume_ratio = usd_value / volume_24h_proxy_usd if volume_24h_proxy_usd > 0 else 0.0
        net_flow_5m = float(token_snapshot.get("token_net_flow_usd_5m") or 0.0)
        net_flow_1h = float(token_snapshot.get("token_net_flow_usd_1h") or 0.0)

        buy_cluster_5m = int(token_snapshot.get("buy_cluster_5m") or 0)
        sell_cluster_5m = int(token_snapshot.get("sell_cluster_5m") or 0)
        cluster_boost = self._cluster_boost(event.side, buy_cluster_5m, sell_cluster_5m)

        side_resonance = self._side_resonance(token_snapshot, event.side)
        resonance_score = float(side_resonance.get("resonance_score") or 0.0)
        same_side_addresses = int(side_resonance.get("same_side_unique_addresses") or 0)
        same_side_high_quality_addresses = int(side_resonance.get("same_side_high_quality_addresses") or 0)
        same_side_smart_money_addresses = int(side_resonance.get("same_side_smart_money_addresses") or 0)
        leader_follow = bool(side_resonance.get("leader_follow"))

        exchange_noise_sensitive = self._exchange_noise_sensitive(event, watch_meta)
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        lp_directional_side = self._lp_directional_side(event)
        lp_trend_pool_context = self._lp_trend_pool_context(
            event=event,
            watch_meta=watch_meta,
            lp_context=lp_context,
        )
        lp_trend_primary_pool = bool(lp_trend_pool_context.get("is_primary_trend_pool"))
        lp_trend_snapshot = self.state_manager.get_lp_trend_snapshot(
            pool_address=str(event.address or "").lower(),
            now_ts=int(event.ts or 0),
            trend_pool_context=lp_trend_pool_context,
        )
        lp_directional_profile = self._lp_directional_profile(
            event=event,
            lp_trend_pool_context=lp_trend_pool_context,
            lp_trend_snapshot=lp_trend_snapshot,
            same_pool_continuity=same_pool_continuity,
            multi_pool_resonance=multi_pool_resonance,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
            action_intensity=action_intensity,
            reserve_skew=reserve_skew,
            lp_burst_event_count=lp_burst_event_count,
            lp_burst_volume_surge_ratio=lp_burst_volume_surge_ratio,
            lp_burst_action_intensity=lp_burst_action_intensity,
        )
        lp_burst_profile = self._lp_burst_profile(
            lp_trend_pool_context=lp_trend_pool_context,
            lp_trend_snapshot=lp_trend_snapshot,
            directional_side=lp_directional_side,
        )
        is_stablecoin_flow = bool(raw.get("is_stablecoin_flow") or token in STABLE_TOKEN_CONTRACTS)
        is_exchange_related = bool(raw.get("is_exchange_related"))
        possible_internal_transfer = bool(raw.get("possible_internal_transfer"))
        followup_strength = str(event.metadata.get("followup_strength") or "")
        followup_semantic = str(event.metadata.get("followup_semantic") or "")
        execution_required_but_missing = bool(event.metadata.get("execution_required_but_missing"))
        liquidation = event.metadata.get("liquidation") or {}
        liquidation_stage = str(event.metadata.get("liquidation_stage") or liquidation.get("liquidation_stage") or "none")
        liquidation_score = float(event.metadata.get("liquidation_score") or liquidation.get("liquidation_score") or 0.0)
        liquidation_side = str(event.metadata.get("liquidation_side") or liquidation.get("liquidation_side") or "unknown")
        liquidation_protocols = list(event.metadata.get("liquidation_protocols") or liquidation.get("liquidation_protocols") or [])
        liquidation_evidence = list(event.metadata.get("liquidation_evidence") or liquidation.get("liquidation_evidence") or [])
        is_liquidation_protocol_related = bool(
            raw.get("is_liquidation_protocol_related")
            or (raw.get("inferred_context") or {}).get("is_liquidation_protocol_related")
        )
        possible_keeper_executor = bool(
            raw.get("possible_keeper_executor")
            or (raw.get("inferred_context") or {}).get("possible_keeper_executor")
        )
        possible_vault_or_auction = bool(
            raw.get("possible_vault_or_auction")
            or (raw.get("inferred_context") or {}).get("possible_vault_or_auction")
        )
        possible_lending_protocol = bool(
            raw.get("possible_lending_protocol")
            or (raw.get("inferred_context") or {}).get("possible_lending_protocol")
        )
        threshold_ratio = (usd_value / dynamic_min_usd) if dynamic_min_usd > 0 else 0.0
        lp_notify_hard_min_usd_not_met = bool(lp_event and usd_value < self.lp_notify_hard_min_usd)
        metrics = {
            "usd_value": usd_value,
            "dynamic_min_usd": dynamic_min_usd,
            "base_threshold_usd": base_threshold_usd,
            "effective_threshold_usd": dynamic_min_usd,
            "behavior_type": behavior_type,
            "behavior_confidence": behavior_conf,
            "address_score": address_score_value,
            "address_alpha_score": address_score_value,
            "address_structure_score": address_structure_score,
            "token_score": token_score_value,
            "pricing_status": pricing_status,
            "pricing_source": event.pricing_source,
            "pricing_confidence": pricing_confidence,
            "usd_value_available": bool(event.usd_value_available),
            "usd_value_estimated": bool(event.usd_value_estimated),
            "cooldown_key": cooldown_key,
            "cooldown_sec": cooldown_sec,
            "intent_type": event.intent_type,
            "intent_confidence": float(event.intent_confidence or 0.0),
            "intent_stage": str(event.intent_stage or "preliminary"),
            "information_level": str((event.metadata.get("intent") or {}).get("information_level") or "low"),
            "confirmation_score": float(event.confirmation_score or 0.0),
            "confirmation_evidence_count": len(event.intent_evidence or []),
            "intent_evidence": list(event.intent_evidence or []),
            "address_avg_usd_24h": address_avg_usd,
            "address_max_usd_24h": address_max_usd,
            "abnormal_ratio": abnormal_ratio,
            "relative_address_size": relative_address_size,
            "relative_address_peak": relative_address_peak,
            "liquidity_proxy_usd": liquidity_proxy_usd,
            "volume_24h_proxy_usd": volume_24h_proxy_usd,
            "liquidity_usd": liquidity_proxy_usd,
            "volume_24h_usd": volume_24h_proxy_usd,
            "price_impact_ratio": price_impact_ratio,
            "token_volume_ratio": token_volume_ratio,
            "token_net_flow_usd_5m": net_flow_5m,
            "token_net_flow_usd_1h": net_flow_1h,
            "buy_cluster_5m": buy_cluster_5m,
            "sell_cluster_5m": sell_cluster_5m,
            "cluster_boost": cluster_boost,
            "resonance_score": resonance_score,
            "resonance_window_sec": self.token_resonance_window_sec,
            "same_side_resonance_addresses": same_side_addresses,
            "same_side_resonance_high_quality_addresses": same_side_high_quality_addresses,
            "same_side_resonance_smart_money_addresses": same_side_smart_money_addresses,
            "multi_address_resonance": bool(side_resonance.get("multi_address_resonance")),
            "leader_follow_resonance": leader_follow,
            "exchange_noise_sensitive": exchange_noise_sensitive,
            "lp_event": lp_event,
            "role_group": role_group,
            "strategy_role": strategy_role,
            "semantic_role": str(watch_meta.get("semantic_role") or event.semantic_role or "unknown"),
            "is_real_execution": is_real_execution,
            "lp_reserve_skew": reserve_skew,
            "lp_action_intensity": action_intensity,
            "lp_same_pool_continuity": same_pool_continuity,
            "lp_multi_pool_resonance": multi_pool_resonance,
            "lp_pool_volume_surge_ratio": pool_volume_surge_ratio,
            "lp_pool_window_trade_count": pool_window_trade_count,
            "lp_pool_window_usd_total": pool_window_usd_total,
            "lp_pair_label": str(lp_context.get("pair_label") or ""),
            "lp_dex": str(lp_context.get("dex") or ""),
            "lp_protocol": str(lp_context.get("protocol") or ""),
            "lp_action": str(lp_context.get("action") or ""),
            "lp_direction": str(lp_context.get("direction") or ""),
            "lp_trend_sensitivity_mode": bool(lp_directional_profile.get("trend_sensitivity_mode")),
            "lp_trend_primary_pool": bool(lp_trend_primary_pool),
            "lp_trend_pool_family": str(lp_trend_pool_context.get("trend_pool_family") or ""),
            "lp_trend_base_family": str(lp_trend_pool_context.get("trend_base_family") or ""),
            "lp_trend_quote_family": str(lp_trend_pool_context.get("trend_quote_family") or ""),
            "lp_trend_pool_match_mode": str(lp_trend_pool_context.get("trend_pool_match_mode") or "non_trend_pool"),
            "lp_trend_state": str(lp_trend_snapshot.get("lp_trend_state") or "trend_neutral"),
            "lp_trend_side_bias": str(lp_trend_snapshot.get("lp_trend_side_bias") or "neutral"),
            "lp_trend_continuation_score": float(lp_trend_snapshot.get("lp_trend_continuation_score") or 0.0),
            "lp_trend_reversal_score": float(lp_trend_snapshot.get("lp_trend_reversal_score") or 0.0),
            "lp_trend_state_source": str(lp_trend_snapshot.get("lp_trend_state_source") or "state_manager_window"),
            "lp_trend_state_window_sec": int(lp_trend_snapshot.get("lp_trend_state_window_sec") or 0),
            "lp_trend_buy_pressure_count": int(lp_trend_snapshot.get("lp_trend_buy_pressure_count") or 0),
            "lp_trend_sell_pressure_count": int(lp_trend_snapshot.get("lp_trend_sell_pressure_count") or 0),
            "lp_trend_window_total_usd": float(lp_trend_snapshot.get("lp_trend_window_total_usd") or 0.0),
            "lp_trend_last_shift_ts": int(lp_trend_snapshot.get("lp_trend_last_shift_ts") or 0),
            "lp_directional_side": lp_directional_side,
            "lp_directional_threshold_profile": str(lp_directional_profile.get("threshold_profile") or ""),
            "lp_fast_exception_profile_name": str(lp_directional_profile.get("profile_name") or ""),
            "lp_buy_trend_profile_active": bool(lp_directional_profile.get("buy_trend_profile_active")),
            "lp_buy_trend_profile_name": str(lp_directional_profile.get("buy_trend_profile_name") or ""),
            "lp_buy_trend_profile_reason": str(lp_directional_profile.get("buy_trend_profile_reason") or ""),
            "is_exchange_related": is_exchange_related,
            "is_stablecoin_flow": is_stablecoin_flow,
            "stablecoin_dominant": is_stablecoin_flow,
            "possible_internal_transfer": possible_internal_transfer,
            "followup_strength": followup_strength,
            "followup_semantic": followup_semantic,
            "followup_confirmed": bool(event.metadata.get("followup_confirmed")),
            "liquidation_stage": liquidation_stage,
            "liquidation_score": liquidation_score,
            "liquidation_side": liquidation_side,
            "liquidation_protocols": liquidation_protocols,
            "liquidation_evidence_count": len(liquidation_evidence),
            "is_liquidation_protocol_related": is_liquidation_protocol_related,
            "possible_keeper_executor": possible_keeper_executor,
            "possible_vault_or_auction": possible_vault_or_auction,
            "possible_lending_protocol": possible_lending_protocol,
            "execution_required_but_missing": execution_required_but_missing,
            "stable_non_swap_hard_filter_usd": float(self.stable_non_swap_hard_filter_usd),
            "stable_transfer_min_usd": float(self.stable_transfer_min_usd),
            "stable_non_swap_filtered_flag": False,
            "lp_observe_exception_applied": False,
            "lp_observe_exception_reason": "",
            "lp_fast_exception_applied": False,
            "lp_fast_exception_reason": "",
            "lp_fast_exception_threshold_ratio": round(threshold_ratio, 3),
            "lp_fast_exception_usd_gap": round(max(dynamic_min_usd - usd_value, 0.0), 2),
            "lp_fast_exception_structure_score": 0.0,
            "lp_fast_exception_structure_passed": False,
            "lp_fast_exception_gate_version": LP_FAST_EXCEPTION_GATE_VERSION,
            "lp_burst_fastlane_applied": False,
            "lp_burst_fastlane_reason": "",
            "lp_burst_window_sec": lp_burst_window_sec,
            "lp_burst_event_count": lp_burst_event_count,
            "lp_burst_total_usd": round(lp_burst_total_usd, 2),
            "lp_burst_max_single_usd": round(lp_burst_max_single_usd, 2),
            "lp_burst_same_pool_continuity": lp_burst_same_pool_continuity,
            "lp_burst_volume_surge_ratio": round(lp_burst_volume_surge_ratio, 3),
            "lp_burst_action_intensity": round(lp_burst_action_intensity, 3),
            "lp_burst_reserve_skew": round(lp_burst_reserve_skew, 3),
            "lp_burst_first_ts": lp_burst_first_ts,
            "lp_burst_last_ts": lp_burst_last_ts,
            "lp_burst_delivery_class": "",
            "lp_burst_trend_mode": bool(lp_burst_profile.get("trend_mode")),
            "lp_burst_event_count_threshold_used": int(lp_burst_profile.get("min_event_count") or 0) if lp_directional_side else 0,
            "lp_burst_total_usd_threshold_used": float(lp_burst_profile.get("min_total_usd") or 0.0) if lp_directional_side else 0.0,
            "lp_burst_trend_profile_name": str(lp_burst_profile.get("profile_name") or "") if lp_directional_side else "",
            "lp_fastlane_ready": False,
            "lp_fastlane_applied": False,
            "lp_directional_cooldown_key": cooldown_key if lp_event and str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"} else "",
            "lp_directional_cooldown_sec": cooldown_sec if lp_event and str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"} else 0,
            "lp_directional_cooldown_allowed": True if lp_event and str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"} else None,
            "lp_stage_decision": "gate_pending" if lp_event else "",
            "lp_reject_reason": "",
            "lp_prealert_candidate": False,
            "lp_prealert_applied": False,
            "lp_structure_score": 0.0,
            "lp_structure_components": {},
            "lp_pool_priority_class": "primary_trend_pool" if lp_trend_primary_pool else "standard_pool",
            "liquidation_observe_exception_applied": False,
            "liquidation_observe_exception_reason": "",
            "value_weight_multiplier": 1.0,
            "role_group_value_bonus": 0.0,
            "smart_money_value_bonus": 0.0,
            "smart_money_non_exec_value_bonus": 0.0,
            "market_maker_value_bonus": 0.0,
            "market_maker_non_exec_value_bonus": 0.0,
            "quality_score": 0.0,
            "adjusted_quality_score": 0.0,
            "smart_money_non_exec_exception_applied": False,
            "smart_money_non_exec_exception_reason": "",
            "smart_money_non_exec_threshold_ratio": round(threshold_ratio, 3),
            "smart_money_non_exec_quality_gap": 0.0,
            "market_maker_observe_exception_applied": False,
            "market_maker_observe_exception_reason": "",
            "market_maker_threshold_ratio": round(threshold_ratio, 3),
            "market_maker_quality_gap": 0.0,
            "gate_exception_passed": False,
            "gate_relaxed_by_role": "",
            "lp_observe_threshold_ratio": round(threshold_ratio, 3),
            "lp_observe_below_min_gap": round(max(dynamic_min_usd - usd_value, 0.0), 2),
            "lp_notify_hard_min_usd": float(self.lp_notify_hard_min_usd),
            "lp_notify_hard_min_usd_not_met": bool(lp_notify_hard_min_usd_not_met),
            "lp_structure_min_usd_per_event": float(lp_analysis.get("lp_structure_min_usd_per_event") or self.lp_structure_min_usd_per_event),
            "lp_continuity_eligible": bool(lp_analysis.get("lp_continuity_eligible")),
            "lp_resonance_eligible": bool(lp_analysis.get("lp_resonance_eligible")),
            "lp_continuity_filtered_by_min_usd": int(lp_analysis.get("lp_continuity_filtered_by_min_usd") or 0),
            "lp_resonance_filtered_by_min_usd": int(lp_analysis.get("lp_resonance_filtered_by_min_usd") or 0),
        }
        lp_sweep_meta = detect_lp_liquidity_sweep(
            event=event,
            reserve_skew=reserve_skew,
            action_intensity=action_intensity,
            same_pool_continuity=same_pool_continuity,
            multi_pool_resonance=multi_pool_resonance,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
            lp_burst_event_count=lp_burst_event_count,
            lp_burst_window_sec=lp_burst_window_sec,
            price_impact_ratio=price_impact_ratio,
            min_price_impact_ratio=self.min_price_impact_ratio,
            liquidation_stage=liquidation_stage,
        )
        metrics.update({
            "lp_semantic_subtype": str(lp_sweep_meta.get("semantic_subtype") or ""),
            "semantic_subtype": str(lp_sweep_meta.get("semantic_subtype") or ""),
            "lp_sweep_detected": bool(lp_sweep_meta.get("detected")),
            "lp_sweep_confidence": str(lp_sweep_meta.get("sweep_confidence") or ""),
            "lp_sweep_display_label": str(lp_sweep_meta.get("display_label") or ""),
            "lp_sweep_reason": str(lp_sweep_meta.get("reason") or ""),
            "lp_sweep_min_price_impact_ratio": float(self.min_price_impact_ratio),
        })
        lp_burst_fastlane_ready, lp_burst_fastlane_reason = self._allow_lp_burst_fastlane_exception(
            event=event,
            lp_event=lp_event,
            pricing_confidence=pricing_confidence,
            burst_profile=lp_burst_profile,
        )
        metrics["lp_burst_fastlane_ready"] = bool(lp_burst_fastlane_ready)
        metrics["lp_fastlane_ready"] = bool(lp_burst_fastlane_ready)
        if lp_burst_fastlane_reason:
            metrics["lp_burst_fastlane_reason"] = str(lp_burst_fastlane_reason)

        if lp_event and event.intent_type == "pool_noise":
            metrics["lp_stage_decision"] = "gate_rejected"
            metrics["lp_reject_reason"] = "lp_noise_filtered"
            return GateDecision(False, "lp_noise_filtered", 0.0, "DROP", self.score_threshold, metrics)

        if (
            event.kind != "swap"
            and token in STABLE_TOKEN_CONTRACTS
            and usd_value < self.stable_non_swap_hard_filter_usd
        ):
            metrics["stable_non_swap_filtered_flag"] = True
            return GateDecision(False, "stable_non_swap_filtered", 0.0, "DROP", self.score_threshold, metrics)

        if role_group == "smart_money" and not is_real_execution:
            if (
                event.intent_type in {"pure_transfer", "unknown_intent", "internal_rebalance"}
                and float(event.confirmation_score or 0.0) < (0.38 if market_maker else 0.30)
                and resonance_score < 0.24
                and relative_address_size < (1.18 if market_maker else 1.08)
            ):
                return GateDecision(False, "smart_money_non_execution_weak", 0.0, "DROP", self.score_threshold, metrics)
        if role_group == "market_maker" and not is_real_execution:
            if (
                event.intent_type in {"pure_transfer", "unknown_intent", "internal_rebalance"}
                and float(event.confirmation_score or 0.0) < 0.38
                and resonance_score < 0.24
                and relative_address_size < 1.18
                and behavior_type not in {
                    "inventory_management",
                    "inventory_shift",
                    "inventory_expansion",
                    "inventory_distribution",
                }
            ):
                return GateDecision(False, "market_maker_non_execution_weak", 0.0, "DROP", self.score_threshold, metrics)

        if role_group == "exchange" and not is_real_execution:
            if (
                event.intent_type in {
                    "exchange_deposit_candidate",
                    "exchange_withdraw_candidate",
                    "possible_buy_preparation",
                    "possible_sell_preparation",
                }
                and float(event.confirmation_score or 0.0) < 0.24
                and resonance_score < 0.20
                and relative_address_size < 1.10
            ):
                return GateDecision(False, "exchange_candidate_too_weak", 0.0, "DROP", self.score_threshold, metrics)

        if pricing_status in {"unknown", "unavailable"} and (not event.usd_value_available or usd_value <= 0):
            return GateDecision(False, "pricing_unavailable_no_proxy", 0.0, "DROP", self.score_threshold, metrics)

        if pricing_confidence < 0.35 and event.kind != "swap":
            return GateDecision(False, "pricing_low_confidence_non_swap", 0.0, "DROP", self.score_threshold, metrics)

        if usd_value < dynamic_min_usd:
            allow_lp_prealert, lp_prealert_reason, lp_prealert_score, lp_prealert_components = self._allow_lp_prealert_exception(
                event=event,
                lp_trend_primary_pool=lp_trend_primary_pool,
                pricing_status=pricing_status,
                pricing_confidence=pricing_confidence,
                confirmation_score=float(event.confirmation_score or 0.0),
                action_intensity=action_intensity,
                reserve_skew=reserve_skew,
                pool_volume_surge_ratio=pool_volume_surge_ratio,
            )
            metrics["lp_prealert_candidate"] = bool(allow_lp_prealert)
            metrics["lp_structure_score"] = round(lp_prealert_score, 3)
            metrics["lp_structure_components"] = dict(lp_prealert_components or {})
            if allow_lp_prealert:
                metrics["lp_prealert_applied"] = True
                metrics["lp_stage_decision"] = "gate_prealert_pass"
                metrics["gate_relaxed_by_role"] = "tier2_lp_prealert"
            else:
                metrics["lp_stage_decision"] = "gate_under_threshold"

            allow_lp_observe_exception, lp_exception_reason, lp_exception_structure_score, lp_exception_structure_passed = self._allow_lp_directional_observe_exception(
                event=event,
                lp_event=lp_event,
                usd_value=usd_value,
                dynamic_min_usd=dynamic_min_usd,
                pricing_status=pricing_status,
                pricing_confidence=pricing_confidence,
                confirmation_score=float(event.confirmation_score or 0.0),
                resonance_score=resonance_score,
                abnormal_ratio=abnormal_ratio,
                relative_address_size=relative_address_size,
                action_intensity=action_intensity,
                reserve_skew=reserve_skew,
                same_pool_continuity=same_pool_continuity,
                multi_pool_resonance=multi_pool_resonance,
                pool_volume_surge_ratio=pool_volume_surge_ratio,
                directional_profile=lp_directional_profile,
            )
            metrics["lp_fast_exception_structure_score"] = round(lp_exception_structure_score, 3)
            metrics["lp_fast_exception_structure_passed"] = bool(lp_exception_structure_passed)
            metrics["lp_fast_exception_reason"] = str(lp_exception_reason or "")
            if allow_lp_prealert:
                pass
            elif allow_lp_observe_exception:
                metrics["lp_observe_exception_applied"] = True
                metrics["lp_observe_exception_reason"] = lp_exception_reason
                metrics["lp_fast_exception_applied"] = True
                metrics["lp_fast_exception_reason"] = lp_exception_reason
                metrics["lp_stage_decision"] = "gate_fast_exception_pass"
                metrics["gate_relaxed_by_role"] = "tier2_lp_directional_exception"
            else:
                if not lp_burst_fastlane_ready:
                    allow_liq, liq_reason = self._allow_liquidation_observe_exception(
                        event=event,
                        usd_value=usd_value,
                        pricing_status=pricing_status,
                        pricing_confidence=pricing_confidence,
                        confirmation_score=float(event.confirmation_score or 0.0),
                        quality_score_hint=0.0,
                        liquidation_stage=liquidation_stage,
                        liquidation_score=liquidation_score,
                        is_liquidation_protocol_related=is_liquidation_protocol_related,
                        possible_keeper_executor=possible_keeper_executor,
                        possible_vault_or_auction=possible_vault_or_auction,
                        same_pool_continuity=same_pool_continuity,
                        multi_pool_resonance=multi_pool_resonance,
                        pool_volume_surge_ratio=pool_volume_surge_ratio,
                    )
                    if allow_liq:
                        metrics["liquidation_observe_exception_applied"] = True
                        metrics["liquidation_observe_exception_reason"] = liq_reason
                    else:
                        metrics["lp_stage_decision"] = "gate_rejected"
                        metrics["lp_reject_reason"] = "below_min_usd"
                        return GateDecision(False, "below_min_usd", 0.0, "DROP", self.score_threshold, metrics)

        if lp_event and event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
            if (
                usd_value < LP_OBSERVE_MIN_USD
                and float(event.confirmation_score or 0.0) < LP_OBSERVE_MIN_CONFIDENCE
                and pool_volume_surge_ratio < LP_VOLUME_SURGE_MIN_RATIO
                and same_pool_continuity < 2
                and multi_pool_resonance < 2
                and reserve_skew < 0.10
                and action_intensity < 0.32
                and abnormal_ratio < 1.35
                and relative_address_size < 1.14
            ):
                metrics["lp_stage_decision"] = "gate_rejected"
                metrics["lp_reject_reason"] = "lp_swap_low_confirmation"
                return GateDecision(False, "lp_swap_low_confirmation", 0.0, "DROP", self.score_threshold, metrics)

        if liquidation_stage == "risk":
            if (
                not self.allow_liquidation_risk_observe
                or usd_value < self.liquidation_min_usd
                or (
                    liquidation_score < self.liquidation_risk_min_score
                    and not is_liquidation_protocol_related
                    and same_pool_continuity < 2
                    and multi_pool_resonance < 2
                    and pool_volume_surge_ratio < LP_VOLUME_SURGE_MIN_RATIO
                )
            ):
                return GateDecision(False, "liquidation_risk_too_weak", 0.0, "DROP", self.score_threshold, metrics)

        if liquidation_stage == "execution":
            if usd_value < self.liquidation_min_usd:
                return GateDecision(False, "liquidation_execution_too_small", 0.0, "DROP", self.score_threshold, metrics)
            if not (is_liquidation_protocol_related and (possible_keeper_executor or possible_vault_or_auction or possible_lending_protocol)):
                return GateDecision(False, "liquidation_execution_missing_protocol", 0.0, "DROP", self.score_threshold, metrics)
            if liquidation_score < self.liquidation_execution_min_score:
                return GateDecision(False, "liquidation_execution_score_low", 0.0, "DROP", self.score_threshold, metrics)

        if lp_event and event.intent_type in {"liquidity_addition", "liquidity_removal"}:
            if (
                usd_value < max(LP_OBSERVE_MIN_USD * 1.10, dynamic_min_usd)
                and same_pool_continuity == 0
                and pool_volume_surge_ratio < max(LP_VOLUME_SURGE_MIN_RATIO - 0.15, 1.15)
                and action_intensity < 0.34
                and float(event.confirmation_score or 0.0) < max(LP_OBSERVE_MIN_CONFIDENCE - 0.08, 0.42)
            ):
                metrics["lp_stage_decision"] = "gate_rejected"
                metrics["lp_reject_reason"] = "lp_liquidity_low_signal"
                return GateDecision(False, "lp_liquidity_low_signal", 0.0, "DROP", self.score_threshold, metrics)

        if lp_event and event.intent_type == "pool_rebalance":
            if reserve_skew < 0.12 and action_intensity < 0.46 and same_pool_continuity == 0:
                metrics["lp_stage_decision"] = "gate_rejected"
                metrics["lp_reject_reason"] = "lp_rebalance_noise"
                return GateDecision(False, "lp_rebalance_noise", 0.0, "DROP", self.score_threshold, metrics)

        if self.require_non_normal_behavior and behavior_type == "normal" and not (priority_smart_money and is_real_execution):
            return GateDecision(False, "normal_behavior_blocked", 0.0, "DROP", self.score_threshold, metrics)

        allow_lp_prealert_observe_gate_exception, lp_prealert_observe_gate_reason = self._allow_lp_prealert_observe_gate_exception(
            event=event,
            lp_prealert_applied=bool(metrics.get("lp_prealert_applied")),
            lp_trend_primary_pool=lp_trend_primary_pool,
            pricing_status=pricing_status,
            pricing_confidence=pricing_confidence,
            confirmation_score=float(event.confirmation_score or 0.0),
            action_intensity=action_intensity,
            reserve_skew=reserve_skew,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
            adjusted_quality_score=None,
            quality_threshold=self.score_threshold,
        )

        if (
            liquidity_proxy_usd < self.min_token_liquidity_usd
            and price_impact_ratio < self.min_price_impact_ratio
            and token_volume_ratio < self.min_token_volume_ratio
            and not (priority_smart_money and is_real_execution)
            and not allow_lp_prealert_observe_gate_exception
        ):
            return GateDecision(False, "low_liquidity_no_tradeable_impact", 0.0, "DROP", self.score_threshold, metrics)
        if allow_lp_prealert_observe_gate_exception:
            metrics["gate_exception_passed"] = True
            metrics["gate_relaxed_by_role"] = "tier2_lp_prealert"
            metrics["lp_stage_decision"] = "gate_prealert_observe_pass"
            metrics["lp_observe_exception_applied"] = True
            metrics["lp_observe_exception_reason"] = lp_prealert_observe_gate_reason

        exchange_noise_reason = self._exchange_noise_reason(
            event=event,
            exchange_noise_sensitive=exchange_noise_sensitive,
            abnormal_ratio=abnormal_ratio,
            relative_address_size=relative_address_size,
            same_side_addresses=same_side_addresses,
            resonance_score=resonance_score,
        )
        if exchange_noise_reason:
            return GateDecision(False, exchange_noise_reason, 0.0, "DROP", self.score_threshold, metrics)

        base_quality_score = self._quality_score(
            usd_value=usd_value,
            relative_address_size=relative_address_size,
            relative_address_peak=relative_address_peak,
            token_score=token_score_value,
            address_score=address_score_value,
            behavior_confidence=behavior_conf,
            pricing_confidence=pricing_confidence,
            confirmation_score=float(event.confirmation_score or 0.0),
            resonance_score=resonance_score,
            price_impact_ratio=price_impact_ratio,
            token_volume_ratio=token_volume_ratio,
            cluster_boost=cluster_boost,
            side=event.side,
            token_net_flow_5m=net_flow_5m,
            token_net_flow_1h=net_flow_1h,
        )
        role_group_value_bonus, role_specific_value_bonus, value_weight_multiplier = self._role_group_value_bonus(
            event=event,
            role_group=role_group,
            strategy_role=strategy_role,
            usd_value=usd_value,
            dynamic_min_usd=dynamic_min_usd,
            threshold_ratio=threshold_ratio,
            token=token,
            token_score=token_score_value,
            pricing_status=pricing_status,
            pricing_confidence=pricing_confidence,
            behavior_confidence=behavior_conf,
            confirmation_score=float(event.confirmation_score or 0.0),
            resonance_score=resonance_score,
            intent_confidence=float(event.intent_confidence or 0.0),
            is_real_execution=is_real_execution,
            is_stablecoin_flow=is_stablecoin_flow,
            stable_non_swap_filtered=False,
            same_pool_continuity=same_pool_continuity,
            multi_pool_resonance=multi_pool_resonance,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
        )
        quality_score = base_quality_score
        if lp_event:
            lp_quality_boost = 0.0
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                if pool_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO:
                    lp_quality_boost += 0.05
                if same_pool_continuity >= 2:
                    lp_quality_boost += 0.06
                if multi_pool_resonance >= 2:
                    lp_quality_boost += 0.06
                if float(event.confirmation_score or 0.0) >= LP_PRIMARY_MIN_CONFIDENCE:
                    lp_quality_boost += 0.04
                if pool_volume_surge_ratio >= LP_PRIMARY_MIN_SURGE_RATIO:
                    lp_quality_boost += 0.03
            elif event.intent_type in {"liquidity_addition", "liquidity_removal"}:
                if same_pool_continuity >= 2 or pool_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO:
                    lp_quality_boost += 0.04
            quality_score = self._clamp(quality_score + lp_quality_boost, 0.0, 1.0)
        if liquidation_stage == "risk":
            liquidation_quality_boost = 0.0
            if liquidation_score >= self.liquidation_risk_min_score:
                liquidation_quality_boost += 0.04
            if is_liquidation_protocol_related:
                liquidation_quality_boost += 0.05
            if same_pool_continuity >= 2 or multi_pool_resonance >= 2:
                liquidation_quality_boost += 0.03
            quality_score = self._clamp(quality_score + liquidation_quality_boost, 0.0, 1.0)
        elif liquidation_stage == "execution":
            liquidation_quality_boost = 0.08
            if possible_vault_or_auction or possible_keeper_executor:
                liquidation_quality_boost += 0.04
            quality_score = self._clamp(quality_score + liquidation_quality_boost, 0.0, 1.0)

        raw_quality_score = quality_score
        adjusted_quality_score = self._clamp(quality_score + role_group_value_bonus, 0.0, 1.0)

        quality_threshold = self._quality_threshold(
            base_threshold=self.score_threshold,
            event=event,
            behavior_type=behavior_type,
            pricing_status=pricing_status,
            pricing_confidence=pricing_confidence,
            confirmation_score=float(event.confirmation_score or 0.0),
            resonance_score=resonance_score,
            price_impact_ratio=price_impact_ratio,
            token_volume_ratio=token_volume_ratio,
            relative_address_size=relative_address_size,
            exchange_noise_sensitive=exchange_noise_sensitive,
        )
        quality_tier = self._quality_tier(adjusted_quality_score, quality_threshold)
        metrics.update({
            "quality_score": raw_quality_score,
            "adjusted_quality_score": adjusted_quality_score,
            "quality_threshold": quality_threshold,
            "quality_tier": quality_tier,
            "value_weight_multiplier": round(value_weight_multiplier, 3),
            "role_group_value_bonus": round(role_group_value_bonus, 3),
            "smart_money_value_bonus": round(role_specific_value_bonus if role_group == "smart_money" else 0.0, 3),
            "smart_money_non_exec_value_bonus": round(role_specific_value_bonus if role_group == "smart_money" else 0.0, 3),
            "market_maker_value_bonus": round(role_specific_value_bonus if role_group == "market_maker" else 0.0, 3),
            "market_maker_non_exec_value_bonus": round(role_specific_value_bonus if role_group == "market_maker" else 0.0, 3),
        })

        if not self.state_manager.can_emit_signal_by_key(cooldown_key, int(event.ts), cooldown_sec):
            metrics["cooldown_sec"] = cooldown_sec
            metrics["lp_directional_cooldown_allowed"] = False if metrics.get("lp_directional_cooldown_key") else None
            metrics["last_signal_ts"] = getattr(self.state_manager, "get_last_signal_ts_by_key", lambda key: None)(cooldown_key)
            if lp_event:
                metrics["lp_stage_decision"] = "cooldown_blocked"
                metrics["lp_reject_reason"] = "cooldown_active"
            return GateDecision(False, "cooldown_active", adjusted_quality_score, "DROP", quality_threshold, metrics)
        metrics["lp_directional_cooldown_allowed"] = True if metrics.get("lp_directional_cooldown_key") else None
        if lp_event and metrics.get("lp_stage_decision") in {"gate_pending", "gate_under_threshold"}:
            metrics["lp_stage_decision"] = "gate_passed"

        exception_applied = False
        exception_reason = ""
        quality_gap = quality_threshold - adjusted_quality_score
        if adjusted_quality_score < quality_threshold:
            allow_market_maker_exception, market_maker_exception_reason = self._allow_market_maker_observe_exception(
                event=event,
                strategy_role=strategy_role,
                usd_value=usd_value,
                dynamic_min_usd=dynamic_min_usd,
                threshold_ratio=threshold_ratio,
                address_score=address_score_value,
                token_score=token_score_value,
                behavior_type=behavior_type,
                behavior_confidence=behavior_conf,
                intent_confidence=float(event.intent_confidence or 0.0),
                confirmation_score=float(event.confirmation_score or 0.0),
                resonance_score=resonance_score,
                pricing_status=pricing_status,
                pricing_confidence=pricing_confidence,
                is_stablecoin_flow=is_stablecoin_flow,
                possible_internal_transfer=possible_internal_transfer,
                touched_watch_addresses_count=len(raw.get("touched_watch_addresses") or event.metadata.get("touched_watch_addresses") or []),
                adjusted_quality_score=adjusted_quality_score,
                quality_threshold=quality_threshold,
                market_maker_value_bonus=role_specific_value_bonus if role_group == "market_maker" else 0.0,
            )
            allow_smart_money_exception, exception_reason = self._allow_smart_money_non_exec_exception(
                event=event,
                role_group=role_group,
                strategy_role=strategy_role,
                usd_value=usd_value,
                dynamic_min_usd=dynamic_min_usd,
                threshold_ratio=threshold_ratio,
                address_score=address_score_value,
                token_score=token_score_value,
                behavior_confidence=behavior_conf,
                intent_confidence=float(event.intent_confidence or 0.0),
                confirmation_score=float(event.confirmation_score or 0.0),
                resonance_score=resonance_score,
                pricing_status=pricing_status,
                pricing_confidence=pricing_confidence,
                exchange_noise_sensitive=exchange_noise_sensitive,
                is_stablecoin_flow=is_stablecoin_flow,
                stable_non_swap_filtered=bool(metrics.get("stable_non_swap_filtered_flag")),
                adjusted_quality_score=adjusted_quality_score,
                quality_threshold=quality_threshold,
                smart_money_value_bonus=role_specific_value_bonus if role_group == "smart_money" else 0.0,
            )
            allow_lp_quality_exception, lp_quality_exception_reason = self._allow_lp_prealert_observe_gate_exception(
                event=event,
                lp_prealert_applied=bool(metrics.get("lp_prealert_applied")),
                lp_trend_primary_pool=lp_trend_primary_pool,
                pricing_status=pricing_status,
                pricing_confidence=pricing_confidence,
                confirmation_score=float(event.confirmation_score or 0.0),
                action_intensity=action_intensity,
                reserve_skew=reserve_skew,
                pool_volume_surge_ratio=pool_volume_surge_ratio,
                adjusted_quality_score=adjusted_quality_score,
                quality_threshold=quality_threshold,
            )
            metrics["smart_money_non_exec_quality_gap"] = round(max(quality_gap, 0.0), 3)
            metrics["market_maker_quality_gap"] = round(max(quality_gap, 0.0), 3)
            if allow_market_maker_exception:
                exception_applied = True
                exception_reason = market_maker_exception_reason
                metrics["gate_exception_passed"] = True
                metrics["gate_relaxed_by_role"] = "tier1_market_maker_high_value"
                metrics["market_maker_observe_exception_applied"] = True
                metrics["market_maker_observe_exception_reason"] = market_maker_exception_reason
                adjusted_quality_score = self._clamp(
                    max(adjusted_quality_score, self.market_maker_observe_gate_floor),
                    0.0,
                    1.0,
                )
                metrics["adjusted_quality_score"] = round(adjusted_quality_score, 3)
                quality_tier = self._quality_tier(adjusted_quality_score, quality_threshold)
                metrics["quality_tier"] = quality_tier
            elif allow_smart_money_exception:
                exception_applied = True
                metrics["gate_exception_passed"] = True
                metrics["gate_relaxed_by_role"] = "tier1_smart_money_high_value"
                metrics["smart_money_non_exec_exception_applied"] = True
                metrics["smart_money_non_exec_exception_reason"] = exception_reason
                adjusted_quality_score = self._clamp(
                    max(adjusted_quality_score, self.smart_money_non_exec_gate_floor),
                    0.0,
                    1.0,
                )
                metrics["adjusted_quality_score"] = round(adjusted_quality_score, 3)
                quality_tier = self._quality_tier(adjusted_quality_score, quality_threshold)
                metrics["quality_tier"] = quality_tier
            elif allow_lp_quality_exception:
                exception_applied = True
                metrics["gate_exception_passed"] = True
                metrics["gate_relaxed_by_role"] = "tier2_lp_prealert"
                metrics["lp_stage_decision"] = "gate_prealert_observe_pass"
                metrics["lp_observe_exception_applied"] = True
                metrics["lp_observe_exception_reason"] = lp_quality_exception_reason
                adjusted_quality_score = self._clamp(
                    max(adjusted_quality_score, max(0.58, quality_threshold - 0.10)),
                    0.0,
                    1.0,
                )
                metrics["adjusted_quality_score"] = round(adjusted_quality_score, 3)
                quality_tier = self._quality_tier(adjusted_quality_score, quality_threshold)
                metrics["quality_tier"] = quality_tier
            else:
                return GateDecision(False, "quality_score_below_threshold", adjusted_quality_score, "DROP", quality_threshold, metrics)
        else:
            metrics["smart_money_non_exec_quality_gap"] = round(max(quality_gap, 0.0), 3)
            metrics["market_maker_quality_gap"] = round(max(quality_gap, 0.0), 3)

        if lp_notify_hard_min_usd_not_met:
            notify_hard_min_reason = "lp_notify_hard_min_usd_not_met"
            if bool(metrics.get("lp_prealert_applied")):
                notify_hard_min_reason = "lp_prealert_notify_hard_min_usd_not_met"
            elif bool(metrics.get("lp_observe_exception_applied")):
                notify_hard_min_reason = "lp_observe_gate_blocked_by_notify_hard_min"
            metrics["lp_stage_decision"] = "gate_rejected"
            metrics["lp_reject_reason"] = notify_hard_min_reason
            return GateDecision(False, notify_hard_min_reason, adjusted_quality_score, "DROP", quality_threshold, metrics)

        return GateDecision(
            True,
            "gate_exception_passed" if exception_applied else "passed",
            adjusted_quality_score,
            quality_tier,
            quality_threshold,
            metrics,
        )

    def mark_emitted(self, event: Event) -> None:
        cooldown_key = self.state_manager.get_cooldown_key(event, intent_type=event.intent_type)
        self.state_manager.mark_signal_emitted_by_key(cooldown_key, int(event.ts))

    def _cooldown_sec(self, event: Event) -> int:
        if self._is_lp_event(event=event) and str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"}:
            return self.lp_directional_cooldown_sec
        return self.cooldown_sec

    def _dynamic_min_usd(self, event: Event, watch_meta: dict) -> float:
        category_floor = {
            "smart_money": 1100.0,
            "exchange_whale": 1600.0,
            "issuer_whale": 7000.0,
            "celebrity_whale": 1800.0,
            "user_added": 1400.0,
            "lp_pool": 15000.0,
            "unknown": 1400.0,
        }

        category = watch_meta.get("category", "unknown")
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        try:
            priority = int(watch_meta.get("priority", 3) or 3)
        except (TypeError, ValueError):
            priority = 3

        category_min = float(category_floor.get(category, category_floor["unknown"]))
        threshold_min = float(get_threshold(watch_meta))
        min_usd = max(self.min_global_usd, category_min, threshold_min)

        if strategy_role == "lp_pool":
            if str(event.intent_type or "") == "liquidity_removal":
                min_usd *= 0.90
            elif str(event.intent_type or "") == "liquidity_addition":
                min_usd *= 0.96
            elif str(event.intent_type or "") == "pool_rebalance":
                min_usd *= 1.08
            elif str(event.intent_type or "") == "pool_noise":
                min_usd *= 1.18

        if priority == 1:
            min_usd *= 0.90
        elif priority == 3:
            min_usd *= 1.08

        if event.kind == "token_transfer" and (event.token or "").lower() in STABLE_TOKEN_CONTRACTS:
            min_usd = max(min_usd, self.stable_transfer_min_usd)

        if str(event.pricing_status or "") in {"unknown", "unavailable"}:
            min_usd *= 1.10
        elif bool(event.usd_value_estimated):
            min_usd *= 1.03

        if str(event.intent_stage or "preliminary") == "weak":
            min_usd *= 1.03
        elif str(event.intent_stage or "preliminary") == "confirmed":
            min_usd *= 0.98

        intent_type = str(event.intent_type or "")
        if is_priority_smart_money_strategy_role(strategy_role) and intent_type == "swap_execution":
            min_usd *= 0.72
        elif is_market_maker_strategy_role(strategy_role) and intent_type == "swap_execution":
            min_usd *= 0.94
        elif is_market_maker_strategy_role(strategy_role) and intent_type in {"pure_transfer", "unknown_intent"}:
            min_usd *= 1.08
        elif is_smart_money_strategy_role(strategy_role) and intent_type in {"pure_transfer", "unknown_intent"}:
            min_usd *= 1.12

        if intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        }:
            min_usd *= 1.08

        if strategy_role == "lp_pool" and intent_type not in {"pool_buy_pressure", "pool_sell_pressure"}:
            min_usd *= 1.06

        return float(min_usd)

    def _exchange_noise_reason(
        self,
        event: Event,
        exchange_noise_sensitive: bool,
        abnormal_ratio: float,
        relative_address_size: float,
        same_side_addresses: int,
        resonance_score: float,
    ) -> str | None:
        if not exchange_noise_sensitive or event.kind == "swap":
            return None

        confirmation_score = float(event.confirmation_score or 0.0)

        if event.intent_stage == "weak":
            # 对 pure_transfer 继续严格，但不再把所有 weak 交易所事件一刀切拦掉
            if event.intent_type in {"pure_transfer", "unknown_intent"}:
                if (
                    same_side_addresses <= 1
                    and abnormal_ratio < 1.22
                    and relative_address_size < 1.12
                    and resonance_score < 0.22
                ):
                    return "exchange_flow_weak_confirmation"
                return None

            # 对交易所方向型意图，只在“很弱 + 很普通 + 没异常”时拦
            if event.intent_type in {
                "exchange_deposit_candidate",
                "exchange_withdraw_candidate",
                "possible_buy_preparation",
                "possible_sell_preparation",
            }:
                if (
                    same_side_addresses == 0
                    and abnormal_ratio < 1.15
                    and relative_address_size < 1.10
                    and resonance_score < 0.18
                    and confirmation_score < 0.26
                ):
                    return "exchange_flow_weak_confirmation"
                return None

            return None

        if event.intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        }:
            if (
                confirmation_score < max(0.34, self.exchange_noise_confirmation_min - 0.16)
                and same_side_addresses < self.token_resonance_min_addresses
                and abnormal_ratio < 1.45
                and relative_address_size < 1.28
                and resonance_score < 0.28
            ):
                return "exchange_flow_low_confirmation"

        if event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
            if abnormal_ratio < 1.45 and relative_address_size < 1.22 and resonance_score < 0.24:
                return "exchange_internal_inventory_noise"

        return None

    def _lp_directional_side(self, event: Event) -> str:
        intent_type = str(event.intent_type or "")
        if intent_type == "pool_buy_pressure":
            return "buy_pressure"
        if intent_type == "pool_sell_pressure":
            return "sell_pressure"
        return ""

    def _lp_trend_pool_context(
        self,
        event: Event,
        watch_meta: dict,
        lp_context: dict,
    ) -> dict:
        if not self._is_lp_event(event=event, watch_meta=watch_meta):
            return {
                "is_primary_trend_pool": False,
                "trend_pool_family": "",
                "trend_base_family": "other",
                "trend_quote_family": "other",
                "trend_pool_match_mode": "non_trend_pool",
            }
        meta = {
            "pair_label": lp_context.get("pair_label") or watch_meta.get("pair_label") or "",
            "base_token_contract": lp_context.get("base_token_contract") or watch_meta.get("base_token_contract") or "",
            "base_token_symbol": lp_context.get("base_token_symbol") or watch_meta.get("base_token_symbol") or "",
            "quote_token_contract": lp_context.get("quote_token_contract") or watch_meta.get("quote_token_contract") or "",
            "quote_token_symbol": lp_context.get("quote_token_symbol") or watch_meta.get("quote_token_symbol") or "",
            "token0_contract": lp_context.get("token0_contract") or watch_meta.get("token0_contract") or "",
            "token0_symbol": lp_context.get("token0_symbol") or watch_meta.get("token0_symbol") or "",
            "token1_contract": lp_context.get("token1_contract") or watch_meta.get("token1_contract") or "",
            "token1_symbol": lp_context.get("token1_symbol") or watch_meta.get("token1_symbol") or "",
        }
        return classify_trend_pool_meta(meta)

    def _lp_trend_primary_pool(
        self,
        event: Event,
        watch_meta: dict,
        lp_context: dict,
    ) -> bool:
        return bool(self._lp_trend_pool_context(event=event, watch_meta=watch_meta, lp_context=lp_context).get("is_primary_trend_pool"))

    def _lp_directional_profile(
        self,
        *,
        event: Event,
        lp_trend_pool_context: dict,
        lp_trend_snapshot: dict,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
        action_intensity: float,
        reserve_skew: float,
        lp_burst_event_count: int,
        lp_burst_volume_surge_ratio: float,
        lp_burst_action_intensity: float,
    ) -> dict:
        side = self._lp_directional_side(event)
        profile = {
            "trend_sensitivity_mode": False,
            "threshold_profile": "standard" if side else "",
            "profile_name": "lp_directional_standard_v1" if side else "",
            "threshold_ratio_floor": self.lp_observe_threshold_ratio_floor,
            "min_same_pool_continuity": self.lp_fast_exception_min_same_pool_continuity,
            "min_volume_surge_ratio": self.lp_fast_exception_min_volume_surge_ratio,
            "min_action_intensity": self.lp_fast_exception_min_action_intensity,
            "min_reserve_skew": self.lp_fast_exception_min_reserve_skew,
            "min_pricing_confidence": self.lp_fast_exception_min_pricing_confidence,
            "buy_trend_profile_active": False,
            "buy_trend_profile_name": "",
            "buy_trend_profile_reason": "",
        }
        if not bool(lp_trend_pool_context.get("is_primary_trend_pool")) or side not in {"buy_pressure", "sell_pressure"}:
            return profile

        trend_state = str(lp_trend_snapshot.get("lp_trend_state") or "trend_neutral")
        trend_side_bias = str(lp_trend_snapshot.get("lp_trend_side_bias") or "neutral")
        if side == "sell_pressure":
            if trend_state in {"trend_continuation_sell", "trend_reversal_to_sell"} or trend_side_bias == "sell_pressure":
                profile.update({
                    "trend_sensitivity_mode": True,
                    "threshold_profile": "sell_bias",
                    "profile_name": "lp_directional_sell_bias_v2",
                    "threshold_ratio_floor": self.lp_sell_observe_threshold_ratio_floor,
                    "min_volume_surge_ratio": self.lp_sell_fast_exception_min_volume_surge_ratio,
                    "min_action_intensity": self.lp_sell_fast_exception_min_action_intensity,
                })
            return profile

        if trend_side_bias == "buy_pressure":
            profile.update({
                "trend_sensitivity_mode": True,
                "threshold_profile": "buy_bias",
                "profile_name": "lp_directional_buy_bias_standard_v1",
                "threshold_ratio_floor": self.lp_buy_observe_threshold_ratio_floor,
                "min_volume_surge_ratio": self.lp_buy_fast_exception_min_volume_surge_ratio,
                "min_action_intensity": self.lp_buy_fast_exception_min_action_intensity,
            })

        buy_trend_ready, buy_trend_reason = self._buy_trend_structure_ready(
            trend_state=trend_state,
            same_pool_continuity=same_pool_continuity,
            multi_pool_resonance=multi_pool_resonance,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
            action_intensity=action_intensity,
            reserve_skew=reserve_skew,
            lp_burst_event_count=lp_burst_event_count,
            lp_burst_volume_surge_ratio=lp_burst_volume_surge_ratio,
            lp_burst_action_intensity=lp_burst_action_intensity,
        )
        if buy_trend_ready and trend_state == "trend_continuation_buy":
            profile.update({
                "trend_sensitivity_mode": True,
                "threshold_profile": "buy_bias_trend_continuation",
                "profile_name": "lp_directional_buy_bias_trend_v1",
                "threshold_ratio_floor": self.lp_buy_trend_continuation_threshold_ratio_floor,
                "min_volume_surge_ratio": self.lp_buy_trend_continuation_min_volume_surge_ratio,
                "min_action_intensity": self.lp_buy_trend_continuation_min_action_intensity,
                "buy_trend_profile_active": True,
                "buy_trend_profile_name": "lp_directional_buy_bias_trend_v1",
                "buy_trend_profile_reason": buy_trend_reason,
            })
        elif buy_trend_ready and trend_state == "trend_reversal_to_buy":
            profile.update({
                "trend_sensitivity_mode": True,
                "threshold_profile": "buy_bias_trend_reversal",
                "profile_name": "lp_directional_buy_bias_trend_v1",
                "threshold_ratio_floor": self.lp_buy_trend_reversal_threshold_ratio_floor,
                "min_volume_surge_ratio": self.lp_buy_trend_reversal_min_volume_surge_ratio,
                "min_action_intensity": self.lp_buy_trend_reversal_min_action_intensity,
                "buy_trend_profile_active": True,
                "buy_trend_profile_name": "lp_directional_buy_bias_trend_v1",
                "buy_trend_profile_reason": buy_trend_reason,
            })
        return profile

    def _lp_burst_profile(self, *, lp_trend_pool_context: dict, lp_trend_snapshot: dict, directional_side: str) -> dict:
        profile = {
            "trend_mode": False,
            "profile_name": "lp_burst_standard_v1" if directional_side else "",
            "min_event_count": self.lp_burst_min_event_count,
            "min_total_usd": self.lp_burst_min_total_usd,
            "min_max_single_usd": self.lp_burst_min_max_single_usd,
            "min_volume_surge_ratio": self.lp_burst_min_volume_surge_ratio,
            "min_action_intensity": self.lp_burst_min_action_intensity,
            "min_reserve_skew": self.lp_burst_min_reserve_skew,
            "min_pricing_confidence": self.lp_burst_min_pricing_confidence,
        }
        if not bool(lp_trend_pool_context.get("is_primary_trend_pool")) or directional_side not in {"buy_pressure", "sell_pressure"}:
            return profile
        trend_state = str(lp_trend_snapshot.get("lp_trend_state") or "trend_neutral")
        trend_side_bias = str(lp_trend_snapshot.get("lp_trend_side_bias") or "neutral")
        if directional_side == "sell_pressure" and trend_side_bias == "sell_pressure":
            profile.update({
                "trend_mode": True,
                "profile_name": "lp_burst_trend_primary_v2",
                "min_event_count": self.lp_trend_burst_min_event_count,
                "min_total_usd": self.lp_trend_burst_min_total_usd,
                "min_max_single_usd": self.lp_trend_burst_min_max_single_usd,
                "min_volume_surge_ratio": self.lp_trend_burst_min_volume_surge_ratio,
                "min_action_intensity": self.lp_trend_burst_min_action_intensity,
                "min_reserve_skew": self.lp_trend_burst_min_reserve_skew,
                "min_pricing_confidence": self.lp_trend_burst_min_pricing_confidence,
            })
            return profile
        if directional_side == "buy_pressure" and trend_state in {"trend_continuation_buy", "trend_reversal_to_buy"}:
            profile.update({
                "trend_mode": True,
                "profile_name": "lp_burst_buy_trend_v1",
                "min_event_count": self.lp_trend_burst_min_event_count,
                "min_total_usd": self.lp_trend_burst_min_total_usd,
                "min_max_single_usd": self.lp_trend_burst_min_max_single_usd,
                "min_volume_surge_ratio": self.lp_buy_trend_burst_min_volume_surge_ratio,
                "min_action_intensity": self.lp_buy_trend_burst_min_action_intensity,
                "min_reserve_skew": self.lp_trend_burst_min_reserve_skew,
                "min_pricing_confidence": self.lp_trend_burst_min_pricing_confidence,
            })
        return profile

    def _buy_trend_structure_ready(
        self,
        *,
        trend_state: str,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
        action_intensity: float,
        reserve_skew: float,
        lp_burst_event_count: int,
        lp_burst_volume_surge_ratio: float,
        lp_burst_action_intensity: float,
    ) -> tuple[bool, str]:
        score = 0
        if same_pool_continuity >= 2:
            score += 1
        if multi_pool_resonance >= 2:
            score += 1
        if pool_volume_surge_ratio >= 3.4:
            score += 1
        if reserve_skew >= 0.99:
            score += 1
        if action_intensity >= 0.57:
            score += 1
        if lp_burst_event_count >= 4 or lp_burst_volume_surge_ratio >= 4.0 or lp_burst_action_intensity >= 0.57:
            score += 1
        if trend_state == "trend_reversal_to_buy":
            return bool(score >= 2 and (lp_burst_event_count >= 3 or same_pool_continuity >= 2 or pool_volume_surge_ratio >= 3.4)), "trend_reversal_to_buy"
        if trend_state == "trend_continuation_buy":
            return bool(score >= 2 and (same_pool_continuity >= 2 or lp_burst_event_count >= 3 or multi_pool_resonance >= 2)), "trend_continuation_buy"
        return False, ""

    def _allow_lp_directional_observe_exception(
        self,
        event: Event,
        lp_event: bool,
        usd_value: float,
        dynamic_min_usd: float,
        pricing_status: str,
        pricing_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        abnormal_ratio: float,
        relative_address_size: float,
        action_intensity: float,
        reserve_skew: float,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
        directional_profile: dict,
    ) -> tuple[bool, str, float, bool]:
        if not lp_event:
            return False, "", 0.0, False
        if str(event.intent_type or "") not in {"pool_buy_pressure", "pool_sell_pressure"}:
            return False, "", 0.0, False
        if str(event.intent_type or "") in {"pool_noise", "liquidity_addition", "liquidity_removal", "pool_rebalance"}:
            return False, "not_directional_lp_pressure", 0.0, False

        min_same_pool_continuity = int(directional_profile.get("min_same_pool_continuity") or self.lp_fast_exception_min_same_pool_continuity)
        min_volume_surge_ratio = float(directional_profile.get("min_volume_surge_ratio") or self.lp_fast_exception_min_volume_surge_ratio)
        min_action_intensity = float(directional_profile.get("min_action_intensity") or self.lp_fast_exception_min_action_intensity)
        min_reserve_skew = float(directional_profile.get("min_reserve_skew") or self.lp_fast_exception_min_reserve_skew)
        min_pricing_confidence = float(directional_profile.get("min_pricing_confidence") or self.lp_fast_exception_min_pricing_confidence)
        threshold_ratio_floor = float(directional_profile.get("threshold_ratio_floor") or self.lp_observe_threshold_ratio_floor)

        structure_score = self._lp_fast_exception_structure_score(
            pricing_confidence=pricing_confidence,
            same_pool_continuity=same_pool_continuity,
            pool_volume_surge_ratio=pool_volume_surge_ratio,
            action_intensity=action_intensity,
            reserve_skew=reserve_skew,
            min_same_pool_continuity=min_same_pool_continuity,
            min_volume_surge_ratio=min_volume_surge_ratio,
            min_action_intensity=min_action_intensity,
            min_reserve_skew=min_reserve_skew,
            min_pricing_confidence=min_pricing_confidence,
        )
        structure_passed = (
            same_pool_continuity >= min_same_pool_continuity
            and pool_volume_surge_ratio >= min_volume_surge_ratio
            and action_intensity >= min_action_intensity
            and reserve_skew >= min_reserve_skew
            and pricing_confidence >= min_pricing_confidence
        )

        if pricing_status in {"unknown", "unavailable"}:
            return False, "pricing_unavailable_for_lp_fast_exception", 0.0, False
        if pricing_confidence < min_pricing_confidence:
            return False, "lp_fast_exception_pricing_confidence_too_low", structure_score, structure_passed
        if dynamic_min_usd <= 0:
            return False, "invalid_dynamic_min_usd", 0.0, structure_passed

        threshold_ratio = usd_value / dynamic_min_usd if dynamic_min_usd > 0 else 0.0
        below_min_gap = max(dynamic_min_usd - usd_value, 0.0)
        if threshold_ratio < threshold_ratio_floor:
            return False, "lp_fast_exception_threshold_ratio_too_low", structure_score, structure_passed
        if below_min_gap > self.lp_observe_max_usd_gap:
            return False, "lp_fast_exception_usd_gap_too_wide", structure_score, structure_passed
        if same_pool_continuity < min_same_pool_continuity:
            return False, "lp_fast_exception_same_pool_continuity_too_low", structure_score, structure_passed
        if pool_volume_surge_ratio < min_volume_surge_ratio:
            return False, "lp_fast_exception_volume_surge_too_low", structure_score, structure_passed
        if action_intensity < min_action_intensity:
            return False, "lp_fast_exception_action_intensity_too_low", structure_score, structure_passed
        if reserve_skew < min_reserve_skew:
            return False, "lp_fast_exception_reserve_skew_too_low", structure_score, structure_passed

        del confirmation_score, resonance_score, abnormal_ratio, relative_address_size, multi_pool_resonance
        return True, "lp_fast_exception_structured_directional", structure_score, structure_passed

    def _allow_lp_prealert_exception(
        self,
        *,
        event: Event,
        lp_trend_primary_pool: bool,
        pricing_status: str,
        pricing_confidence: float,
        confirmation_score: float,
        action_intensity: float,
        reserve_skew: float,
        pool_volume_surge_ratio: float,
    ) -> tuple[bool, str, float, dict]:
        if not lp_trend_primary_pool:
            return False, "", 0.0, {}
        intent_type = str(event.intent_type or "")
        if intent_type not in {
            "pool_buy_pressure",
            "pool_sell_pressure",
            "liquidity_removal",
            "liquidity_addition",
        }:
            return False, "", 0.0, {}
        if pricing_status in {"unknown", "unavailable"}:
            return False, "lp_prealert_pricing_unavailable", 0.0, {}
        if pricing_confidence < self.lp_prealert_min_pricing_confidence:
            return False, "lp_prealert_pricing_confidence_too_low", 0.0, {}
        if confirmation_score < self.lp_prealert_min_confirmation:
            return False, "lp_prealert_confirmation_too_low", 0.0, {}

        min_action_intensity = self.lp_prealert_directional_min_action_intensity
        min_volume_surge_ratio = self.lp_prealert_directional_min_volume_surge_ratio
        min_matches = self.lp_prealert_primary_trend_min_matches
        if intent_type == "liquidity_removal":
            min_action_intensity = self.lp_prealert_liquidity_removal_min_action_intensity
            min_volume_surge_ratio = self.lp_prealert_liquidity_removal_min_volume_surge_ratio
        elif intent_type == "liquidity_addition":
            min_action_intensity = self.lp_prealert_liquidity_addition_min_action_intensity
            min_volume_surge_ratio = self.lp_prealert_liquidity_addition_min_volume_surge_ratio
            min_matches = max(self.lp_prealert_primary_trend_min_matches + 1, 3)

        components = {
            "pricing_confidence": bool(pricing_confidence >= self.lp_prealert_min_pricing_confidence),
            "action_intensity": bool(action_intensity >= min_action_intensity),
            "reserve_skew": bool(reserve_skew >= self.lp_prealert_min_reserve_skew),
            "volume_surge_ratio": bool(pool_volume_surge_ratio >= min_volume_surge_ratio),
        }
        matched = sum(1 for matched in components.values() if matched)
        structure_score = round(matched / max(len(components), 1), 3)
        if matched < min_matches:
            return False, "lp_prealert_structure_too_weak", structure_score, components
        return True, "lp_prealert_structured_mainstream_pool", structure_score, components

    def _lp_fast_exception_structure_score(
        self,
        *,
        pricing_confidence: float,
        same_pool_continuity: int,
        pool_volume_surge_ratio: float,
        action_intensity: float,
        reserve_skew: float,
        min_same_pool_continuity: int | None = None,
        min_volume_surge_ratio: float | None = None,
        min_action_intensity: float | None = None,
        min_reserve_skew: float | None = None,
        min_pricing_confidence: float | None = None,
    ) -> float:
        min_same_pool_continuity = int(min_same_pool_continuity or self.lp_fast_exception_min_same_pool_continuity)
        min_volume_surge_ratio = float(min_volume_surge_ratio or self.lp_fast_exception_min_volume_surge_ratio)
        min_action_intensity = float(min_action_intensity or self.lp_fast_exception_min_action_intensity)
        min_reserve_skew = float(min_reserve_skew or self.lp_fast_exception_min_reserve_skew)
        min_pricing_confidence = float(min_pricing_confidence or self.lp_fast_exception_min_pricing_confidence)
        checks = (
            same_pool_continuity >= min_same_pool_continuity,
            pool_volume_surge_ratio >= min_volume_surge_ratio,
            action_intensity >= min_action_intensity,
            reserve_skew >= min_reserve_skew,
            pricing_confidence >= min_pricing_confidence,
        )
        return round(sum(1 for matched in checks if matched) / len(checks), 3)

    def _allow_lp_burst_fastlane_exception(
        self,
        event: Event,
        lp_event: bool,
        pricing_confidence: float,
        burst_profile: dict,
    ) -> tuple[bool, str]:
        if not lp_event or str(event.intent_type or "") not in {"pool_buy_pressure", "pool_sell_pressure"}:
            return False, ""
        if not self.lp_burst_fastlane_enable:
            return False, "lp_burst_fastlane_disabled"

        min_event_count = int(burst_profile.get("min_event_count") or self.lp_burst_min_event_count)
        min_total_usd = float(burst_profile.get("min_total_usd") or self.lp_burst_min_total_usd)
        min_max_single_usd = float(burst_profile.get("min_max_single_usd") or self.lp_burst_min_max_single_usd)
        min_volume_surge_ratio = float(burst_profile.get("min_volume_surge_ratio") or self.lp_burst_min_volume_surge_ratio)
        min_action_intensity = float(burst_profile.get("min_action_intensity") or self.lp_burst_min_action_intensity)
        min_reserve_skew = float(burst_profile.get("min_reserve_skew") or self.lp_burst_min_reserve_skew)
        min_pricing_confidence = float(burst_profile.get("min_pricing_confidence") or self.lp_burst_min_pricing_confidence)

        burst_state = event.metadata.get("lp_burst") or {}
        if int(burst_state.get("lp_burst_event_count") or 0) < min_event_count:
            return False, "lp_burst_event_count_too_low"
        if float(burst_state.get("lp_burst_total_usd") or 0.0) < min_total_usd:
            return False, "lp_burst_total_usd_too_low"
        if float(burst_state.get("lp_burst_max_single_usd") or 0.0) < min_max_single_usd:
            return False, "lp_burst_max_single_usd_too_low"
        if float(burst_state.get("lp_burst_volume_surge_ratio") or 0.0) < min_volume_surge_ratio:
            return False, "lp_burst_volume_surge_too_low"
        if float(burst_state.get("lp_burst_action_intensity") or 0.0) < min_action_intensity:
            return False, "lp_burst_action_intensity_too_low"
        if float(burst_state.get("lp_burst_reserve_skew") or 0.0) < min_reserve_skew:
            return False, "lp_burst_reserve_skew_too_low"
        if pricing_confidence < min_pricing_confidence:
            return False, "lp_burst_pricing_confidence_too_low"
        return True, "lp_burst_fastlane_ready"

    def _allow_liquidation_observe_exception(
        self,
        event: Event,
        usd_value: float,
        pricing_status: str,
        pricing_confidence: float,
        confirmation_score: float,
        quality_score_hint: float,
        liquidation_stage: str,
        liquidation_score: float,
        is_liquidation_protocol_related: bool,
        possible_keeper_executor: bool,
        possible_vault_or_auction: bool,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
    ) -> tuple[bool, str]:
        del quality_score_hint
        if not self.allow_liquidation_risk_observe:
            return False, ""
        if str(event.intent_type or "") not in {"pool_buy_pressure", "pool_sell_pressure"}:
            return False, ""
        if liquidation_stage not in {"risk", "execution"}:
            return False, ""
        if pricing_status in {"unknown", "unavailable"} or pricing_confidence < 0.55:
            return False, ""
        if usd_value < self.liquidation_min_usd:
            return False, ""
        if liquidation_stage == "execution":
            if liquidation_score >= self.liquidation_execution_min_score and (
                is_liquidation_protocol_related and (possible_keeper_executor or possible_vault_or_auction)
            ):
                return True, "liquidation_execution_exception"
            return False, ""

        strong_structure = (
            same_pool_continuity >= 2
            or multi_pool_resonance >= 2
            or pool_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO
        )
        if liquidation_score >= self.liquidation_risk_min_score and (
            is_liquidation_protocol_related or strong_structure or confirmation_score >= LP_OBSERVE_MIN_CONFIDENCE
        ):
            return True, "liquidation_risk_exception"
        return False, ""

    def _role_group_value_bonus(
        self,
        event: Event,
        role_group: str,
        strategy_role: str,
        usd_value: float,
        dynamic_min_usd: float,
        threshold_ratio: float,
        token: str,
        token_score: float,
        pricing_status: str,
        pricing_confidence: float,
        behavior_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        intent_confidence: float,
        is_real_execution: bool,
        is_stablecoin_flow: bool,
        stable_non_swap_filtered: bool,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
    ) -> tuple[float, float, float]:
        del token
        if dynamic_min_usd <= 0:
            return 0.0, 0.0, 1.0
        if pricing_status in {"unknown", "unavailable"} or pricing_confidence < 0.45:
            return 0.0, 0.0, 1.0

        structure_ok = (
            confirmation_score >= 0.40
            or resonance_score >= 0.30
            or intent_confidence >= 0.52
            or behavior_confidence >= 0.46
        )
        if not structure_ok:
            return 0.0, 0.0, 1.0

        if role_group == "smart_money":
            if (
                str(event.intent_type or "") in {"pure_transfer", "internal_rebalance", "possible_buy_preparation", "possible_sell_preparation"}
                and strategy_role in {"smart_money_wallet", "alpha_wallet", "celebrity_wallet"}
                and threshold_ratio >= self.smart_money_value_threshold_ratio
                and usd_value >= dynamic_min_usd * self.smart_money_value_threshold_ratio
                and token_score >= 28.0
                and behavior_confidence >= 0.38
            ):
                strength = self._clamp((threshold_ratio - self.smart_money_value_threshold_ratio) / 2.5, 0.0, 1.0)
                bonus = self.smart_money_value_bonus_max * (0.45 + 0.55 * strength)
                return round(bonus, 3), round(bonus, 3), round(1.0 + bonus * 4.0, 3)
            if is_real_execution and threshold_ratio >= 1.40:
                bonus = min(0.03, self.smart_money_value_bonus_max * 0.40)
                return round(bonus, 3), 0.0, round(1.0 + bonus * 4.0, 3)

        if role_group == "market_maker":
            if (
                str(event.intent_type or "") in {
                    "market_making_inventory_move",
                    "internal_rebalance",
                    "pure_transfer",
                    "possible_buy_preparation",
                    "possible_sell_preparation",
                }
                and strategy_role == "market_maker_wallet"
                and threshold_ratio >= self.market_maker_high_value_gate_threshold_ratio
                and usd_value >= dynamic_min_usd * self.market_maker_high_value_gate_threshold_ratio
                and token_score >= 24.0
                and (
                    behavior_confidence >= 0.34
                    or confirmation_score >= self.market_maker_observe_min_confirmation
                    or resonance_score >= self.market_maker_observe_min_resonance
                )
            ):
                strength = self._clamp(
                    (threshold_ratio - self.market_maker_high_value_gate_threshold_ratio) / 2.2,
                    0.0,
                    1.0,
                )
                bonus = self.market_maker_observe_value_bonus_max * (0.45 + 0.55 * strength)
                return round(bonus, 3), round(bonus, 3), round(1.0 + bonus * 4.0, 3)
            if is_real_execution and threshold_ratio >= 1.25:
                bonus = min(0.02, self.market_maker_observe_value_bonus_max * 0.30)
                return round(bonus, 3), 0.0, round(1.0 + bonus * 4.0, 3)

        if role_group == "lp_pool":
            if (
                str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure", "liquidity_removal"}
                and threshold_ratio >= 1.15
                and (
                    same_pool_continuity >= 2
                    or multi_pool_resonance >= 2
                    or pool_volume_surge_ratio >= LP_VOLUME_SURGE_MIN_RATIO
                )
            ):
                strength = self._clamp((threshold_ratio - 1.15) / 2.0, 0.0, 1.0)
                bonus = self.lp_value_bonus_max * (0.35 + 0.65 * strength)
                return round(bonus, 3), 0.0, round(1.0 + bonus * 4.0, 3)

        if role_group == "exchange":
            if stable_non_swap_filtered:
                return 0.0, 0.0, 1.0
            if (
                (
                    str(event.intent_type or "") in {
                        "exchange_deposit_candidate",
                        "exchange_withdraw_candidate",
                        "possible_sell_preparation",
                        "possible_buy_preparation",
                    }
                    or is_real_execution
                )
                and not (event.kind != "swap" and is_stablecoin_flow and str(event.intent_type or "") in {"pure_transfer", "unknown_intent"})
                and threshold_ratio >= self.exchange_directional_value_threshold_ratio
                and token_score >= 32.0
            ):
                strength = self._clamp(
                    (threshold_ratio - self.exchange_directional_value_threshold_ratio) / 2.0,
                    0.0,
                    1.0,
                )
                bonus = self.exchange_directional_value_bonus_max * (0.40 + 0.60 * strength)
                return round(bonus, 3), 0.0, round(1.0 + bonus * 4.0, 3)

        return 0.0, 0.0, 1.0

    def _allow_smart_money_non_exec_exception(
        self,
        event: Event,
        role_group: str,
        strategy_role: str,
        usd_value: float,
        dynamic_min_usd: float,
        threshold_ratio: float,
        address_score: float,
        token_score: float,
        behavior_confidence: float,
        intent_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        pricing_status: str,
        pricing_confidence: float,
        exchange_noise_sensitive: bool,
        is_stablecoin_flow: bool,
        stable_non_swap_filtered: bool,
        adjusted_quality_score: float,
        quality_threshold: float,
        smart_money_value_bonus: float,
    ) -> tuple[bool, str]:
        if role_group != "smart_money":
            return False, ""
        if strategy_role not in {"smart_money_wallet", "alpha_wallet", "celebrity_wallet"}:
            return False, ""
        if str(event.intent_type or "") not in {
            "pure_transfer",
            "internal_rebalance",
            "possible_buy_preparation",
            "possible_sell_preparation",
        }:
            return False, ""
        if stable_non_swap_filtered or exchange_noise_sensitive and is_stablecoin_flow and event.kind != "swap":
            return False, ""
        if pricing_status in {"unknown", "unavailable"} or pricing_confidence < 0.52:
            return False, ""
        if usd_value < self.smart_money_high_value_gate_min_usd:
            return False, ""
        if dynamic_min_usd <= 0 or usd_value < dynamic_min_usd * self.smart_money_high_value_gate_threshold_ratio:
            return False, ""
        if threshold_ratio < self.smart_money_high_value_gate_threshold_ratio:
            return False, ""
        if address_score < 55.0:
            return False, ""
        if token_score < 18.0:
            return False, ""
        if behavior_confidence < 0.26:
            return False, ""
        if adjusted_quality_score < self.smart_money_non_exec_gate_floor - 0.12:
            return False, ""
        if quality_threshold - adjusted_quality_score > self.smart_money_high_value_gate_max_quality_gap:
            return False, ""
        if max(confirmation_score, resonance_score, intent_confidence) < 0.30:
            return False, ""

        if (
            confirmation_score >= 0.34
            or resonance_score >= 0.22
            or intent_confidence >= 0.40
            or smart_money_value_bonus > 0
        ):
            return True, "smart_money_non_exec_value_exception"
        return False, ""

    def _allow_market_maker_observe_exception(
        self,
        event: Event,
        strategy_role: str,
        usd_value: float,
        dynamic_min_usd: float,
        threshold_ratio: float,
        address_score: float,
        token_score: float,
        behavior_type: str,
        behavior_confidence: float,
        intent_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        pricing_status: str,
        pricing_confidence: float,
        is_stablecoin_flow: bool,
        possible_internal_transfer: bool,
        touched_watch_addresses_count: int,
        adjusted_quality_score: float,
        quality_threshold: float,
        market_maker_value_bonus: float,
    ) -> tuple[bool, str]:
        if strategy_role != "market_maker_wallet":
            return False, ""
        if str(event.intent_type or "") not in {
            "pure_transfer",
            "internal_rebalance",
            "market_making_inventory_move",
            "possible_buy_preparation",
            "possible_sell_preparation",
        }:
            return False, ""
        if pricing_status in {"unknown", "unavailable"} or pricing_confidence < 0.52:
            return False, ""
        if is_stablecoin_flow and event.kind != "swap" and not possible_internal_transfer:
            if confirmation_score < self.market_maker_observe_min_confirmation and resonance_score < self.market_maker_observe_min_resonance:
                return False, ""
        if usd_value < self.smart_money_high_value_gate_min_usd:
            return False, ""
        if dynamic_min_usd <= 0 or usd_value < dynamic_min_usd * self.market_maker_high_value_gate_threshold_ratio:
            return False, ""
        if threshold_ratio < self.market_maker_high_value_gate_threshold_ratio:
            return False, ""
        if address_score < 52.0 or token_score < 18.0:
            return False, ""
        if behavior_confidence < 0.28 and behavior_type not in {
            "inventory_management",
            "inventory_shift",
            "inventory_expansion",
            "inventory_distribution",
            "whale_action",
        }:
            return False, ""
        if adjusted_quality_score < self.market_maker_observe_gate_floor - 0.12:
            return False, ""
        if quality_threshold - adjusted_quality_score > self.smart_money_high_value_gate_max_quality_gap:
            return False, ""

        strength_hits = 0
        if confirmation_score >= self.market_maker_observe_min_confirmation:
            strength_hits += 1
        if resonance_score >= self.market_maker_observe_min_resonance:
            strength_hits += 1
        if threshold_ratio >= max(1.15, self.smart_money_value_threshold_ratio):
            strength_hits += 1
        if possible_internal_transfer:
            strength_hits += 1
        if touched_watch_addresses_count >= 2:
            strength_hits += 1
        if behavior_type in {"inventory_management", "inventory_shift", "inventory_expansion", "inventory_distribution"}:
            strength_hits += 1
        if intent_confidence >= 0.56:
            strength_hits += 1
        if market_maker_value_bonus > 0:
            strength_hits += 1

        if strength_hits < 1:
            return False, ""
        if behavior_type == "inventory_shift":
            return True, "market_maker_inventory_shift_exception"
        if behavior_type in {"inventory_management", "inventory_expansion", "inventory_distribution"} or possible_internal_transfer:
            return True, "market_maker_inventory_management_exception"
        return True, "market_maker_observe_value_exception"

    def _allow_lp_prealert_observe_gate_exception(
        self,
        *,
        event: Event,
        lp_prealert_applied: bool,
        lp_trend_primary_pool: bool,
        pricing_status: str,
        pricing_confidence: float,
        confirmation_score: float,
        action_intensity: float,
        reserve_skew: float,
        pool_volume_surge_ratio: float,
        adjusted_quality_score: float | None,
        quality_threshold: float,
    ) -> tuple[bool, str]:
        if not lp_prealert_applied:
            return False, ""
        if str(event.intent_type or "") not in {
            "pool_buy_pressure",
            "pool_sell_pressure",
            "liquidity_addition",
            "liquidity_removal",
        }:
            return False, ""
        if pricing_status in {"unknown", "unavailable"} or pricing_confidence < max(self.lp_prealert_min_pricing_confidence - 0.08, 0.56):
            return False, ""
        if adjusted_quality_score is not None:
            if adjusted_quality_score < 0.48:
                return False, ""
            if quality_threshold - adjusted_quality_score > 0.22:
                return False, ""
        strength_hits = 0
        if lp_trend_primary_pool:
            strength_hits += 1
        if confirmation_score >= max(self.lp_prealert_min_confirmation - 0.08, 0.30):
            strength_hits += 1
        if action_intensity >= max(self.lp_prealert_directional_min_action_intensity - 0.08, 0.18):
            strength_hits += 1
        if reserve_skew >= max(self.lp_prealert_min_reserve_skew, 0.08):
            strength_hits += 1
        if pool_volume_surge_ratio >= max(self.lp_prealert_directional_min_volume_surge_ratio - 0.10, 1.10):
            strength_hits += 1
        if strength_hits >= 2:
            return True, "lp_prealert_observe_gate_exception"
        return False, ""

    def _quality_score(
        self,
        usd_value: float,
        relative_address_size: float,
        relative_address_peak: float,
        token_score: float,
        address_score: float,
        behavior_confidence: float,
        pricing_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        price_impact_ratio: float,
        token_volume_ratio: float,
        cluster_boost: float,
        side: str | None,
        token_net_flow_5m: float,
        token_net_flow_1h: float,
    ) -> float:
        log_usd = math.log(max(usd_value, 1.0))
        usd_component = self._clamp((log_usd - 6.5) / 4.5, 0.0, 1.0)
        relative_component = self._clamp(relative_address_size / max(self.min_relative_address_size * 2.2, 1.0), 0.0, 1.0)
        relative_peak_component = self._clamp(relative_address_peak / 1.2, 0.0, 1.0)
        token_component = self._clamp(token_score / 100.0, 0.0, 1.0)
        address_component = self._clamp(address_score / 100.0, 0.0, 1.0)
        behavior_component = self._clamp(behavior_confidence, 0.0, 1.0)
        pricing_component = self._clamp(pricing_confidence, 0.0, 1.0)
        confirmation_component = self._clamp(confirmation_score, 0.0, 1.0)
        resonance_component = self._clamp(resonance_score, 0.0, 1.0)
        impact_component = self._clamp(price_impact_ratio / max(self.min_price_impact_ratio * 8.0, 1e-6), 0.0, 1.0)
        volume_component = self._clamp(token_volume_ratio / max(self.min_token_volume_ratio * 8.0, 1e-9), 0.0, 1.0)

        score = (
            0.18 * usd_component
            + 0.12 * relative_component
            + 0.05 * relative_peak_component
            + 0.09 * token_component
            + 0.08 * address_component
            + 0.12 * behavior_component
            + 0.09 * pricing_component
            + 0.12 * confirmation_component
            + 0.09 * resonance_component
            + 0.03 * impact_component
            + 0.03 * volume_component
        )

        market_boost = 0.0
        if side == "买入":
            if token_net_flow_5m > 0:
                market_boost += 0.04
            elif token_net_flow_5m < 0:
                market_boost -= 0.04
        elif side == "卖出":
            if token_net_flow_5m < 0:
                market_boost += 0.04
            elif token_net_flow_5m > 0:
                market_boost -= 0.04

        if side == "买入" and token_net_flow_1h > 0:
            market_boost += 0.02
        if side == "卖出" and token_net_flow_1h < 0:
            market_boost += 0.02

        score = score + cluster_boost + market_boost
        return self._clamp(score, 0.0, 1.0)

    def _quality_threshold(
        self,
        base_threshold: float,
        event: Event,
        behavior_type: str,
        pricing_status: str,
        pricing_confidence: float,
        confirmation_score: float,
        resonance_score: float,
        price_impact_ratio: float,
        token_volume_ratio: float,
        relative_address_size: float,
        exchange_noise_sensitive: bool,
    ) -> float:
        threshold = float(base_threshold)
        lp_event = self._is_lp_event(event=event)

        if behavior_type in {"accumulation", "distribution", "whale_action"}:
            threshold -= 0.03

        if event.kind == "swap":
            threshold -= 0.02

        if price_impact_ratio < self.min_price_impact_ratio:
            threshold += 0.02

        if token_volume_ratio < self.min_token_volume_ratio:
            threshold += 0.02

        if relative_address_size < self.min_relative_address_size:
            threshold += 0.03

        if pricing_status in {"unknown", "unavailable"}:
            threshold += 0.06
        elif pricing_status == "estimated":
            threshold += 0.02

        if pricing_confidence < 0.55:
            threshold += 0.02

        # 对 weak 的惩罚再平滑一点，不再过重
        intent_stage = str(event.intent_stage or "preliminary")
        if intent_stage == "weak":
            threshold += 0.015
        elif intent_stage == "preliminary":
            threshold -= 0.003
        elif intent_stage == "confirmed":
            threshold -= 0.015

        if confirmation_score < 0.45:
            threshold += 0.02
        elif confirmation_score < 0.58:
            threshold += 0.01
        elif confirmation_score >= 0.78:
            threshold -= 0.02

        if resonance_score >= 0.65:
            threshold -= 0.02
        elif resonance_score >= 0.40:
            threshold -= 0.01
        elif resonance_score < 0.20:
            threshold += 0.01

        # 交易所噪声仍保守，但不再额外压得太狠
        if exchange_noise_sensitive and event.kind != "swap":
            if event.intent_type in {
                "exchange_deposit_candidate",
                "exchange_withdraw_candidate",
                "possible_buy_preparation",
                "possible_sell_preparation",
            }:
                if intent_stage == "weak" and confirmation_score < 0.35 and resonance_score < 0.25:
                    threshold += 0.015
                elif intent_stage == "preliminary":
                    threshold += 0.005
                else:
                    threshold += 0.008
            else:
                threshold += 0.01

        if lp_event:
            if event.intent_type in {"pool_buy_pressure", "pool_sell_pressure"}:
                threshold -= 0.03
            elif event.intent_type == "liquidity_removal":
                threshold -= 0.015
            elif event.intent_type == "pool_rebalance":
                threshold += 0.01

        return self._clamp(threshold, 0.53, 0.90)

    def _quality_tier(self, score: float, threshold: float) -> str:
        if score >= max(self.tier1_threshold, threshold + 0.12):
            return "Tier 1"
        if score >= max(self.tier2_threshold, threshold + 0.04):
            return "Tier 2"
        return "Tier 3"

    def _cluster_boost(self, side: str | None, buy_cluster_5m: int, sell_cluster_5m: int) -> float:
        buy_boost = min(0.12, max(buy_cluster_5m - 1, 0) * 0.03)
        sell_boost = min(0.12, max(sell_cluster_5m - 1, 0) * 0.03)
        if side == "买入":
            return buy_boost - (sell_boost * 0.6)
        if side == "卖出":
            return sell_boost - (buy_boost * 0.6)
        return max(buy_boost, sell_boost) * 0.4

    def _side_resonance(self, token_snapshot: dict, side: str | None) -> dict:
        resonance_5m = token_snapshot.get("resonance_5m") or {}
        if side in {"买入", "流入"}:
            same_side_addresses = int(resonance_5m.get("buy_unique_addresses") or 0)
            same_side_high_quality_addresses = int(resonance_5m.get("buy_high_quality_addresses") or 0)
            same_side_smart_money_addresses = int(resonance_5m.get("buy_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("buy_leader_follow"))
        elif side in {"卖出", "流出"}:
            same_side_addresses = int(resonance_5m.get("sell_unique_addresses") or 0)
            same_side_high_quality_addresses = int(resonance_5m.get("sell_high_quality_addresses") or 0)
            same_side_smart_money_addresses = int(resonance_5m.get("sell_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("sell_leader_follow"))
        else:
            same_side_addresses = max(int(resonance_5m.get("buy_unique_addresses") or 0), int(resonance_5m.get("sell_unique_addresses") or 0))
            same_side_high_quality_addresses = max(int(resonance_5m.get("buy_high_quality_addresses") or 0), int(resonance_5m.get("sell_high_quality_addresses") or 0))
            same_side_smart_money_addresses = max(int(resonance_5m.get("buy_smart_money_addresses") or 0), int(resonance_5m.get("sell_smart_money_addresses") or 0))
            leader_follow = bool(resonance_5m.get("leader_follow_resonance"))

        return {
            "same_side_unique_addresses": same_side_addresses,
            "same_side_high_quality_addresses": same_side_high_quality_addresses,
            "same_side_smart_money_addresses": same_side_smart_money_addresses,
            "multi_address_resonance": same_side_addresses >= self.token_resonance_min_addresses,
            "leader_follow": leader_follow,
            "resonance_score": float(token_snapshot.get("resonance_score_5m") or 0.0),
        }

    def _exchange_noise_sensitive(self, event: Event, watch_meta: dict) -> bool:
        watch_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        if watch_role == "lp_pool":
            return False
        counterparty_role = str(
            (event.metadata.get("raw") or {}).get("counterparty_strategy_role")
            or event.metadata.get("counterparty_strategy_role")
            or "unknown"
        )
        return is_exchange_strategy_role(watch_role) or is_exchange_strategy_role(counterparty_role)

    def _is_lp_event(self, event: Event, watch_meta: dict | None = None) -> bool:
        strategy_role = str((watch_meta or {}).get("strategy_role") or event.strategy_role or "unknown")
        return strategy_role == "lp_pool"

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))
