import time
import hashlib
from collections import defaultdict, deque

from analyzer import BehaviorAnalyzer
from asset_market_state import AssetMarketStateManager
from asset_case_manager import AssetCaseManager
from trade_opportunity import (
    LEGACY_CHASE_ACTION_KEYS,
    TradeOpportunityManager,
    canonical_final_trading_output_label,
    validate_final_trading_output_gate,
)
from config import (
    ADJACENT_WATCH_NOTIFY_ALLOWED_STAGES,
    ADJACENT_WATCH_NOTIFY_MIN_ABNORMAL_RATIO,
    ADJACENT_WATCH_NOTIFY_MIN_CONFIRMATION,
    ADJACENT_WATCH_NOTIFY_MIN_FOLLOWUP_COUNT,
    ADJACENT_WATCH_NOTIFY_MIN_PRICING_CONFIDENCE,
    ADJACENT_WATCH_NOTIFY_MIN_QUALITY,
    ADJACENT_WATCH_NOTIFY_MIN_RESONANCE,
    ADJACENT_WATCH_RUNTIME_MIN_USD,
    ADJACENT_WATCH_RUNTIME_PRIORITY,
    ADJACENT_WATCH_RUNTIME_STRATEGY_ROLE,
    DOWNSTREAM_EARLY_WARNING_ENABLE,
    DOWNSTREAM_EARLY_WARNING_MAX_PER_CASE,
    DOWNSTREAM_EARLY_WARNING_MIN_ABNORMAL_RATIO,
    DOWNSTREAM_EARLY_WARNING_MIN_ANCHOR_USD,
    DOWNSTREAM_EARLY_WARNING_MIN_CONFIRMATION,
    DOWNSTREAM_EARLY_WARNING_MIN_EVENT_USD,
    DOWNSTREAM_EARLY_WARNING_MIN_PRICING_CONFIDENCE,
    DOWNSTREAM_EARLY_WARNING_MIN_QUALITY,
    DOWNSTREAM_EARLY_WARNING_MIN_RESONANCE,
    LP_FASTLANE_PROMOTION_ENABLE,
    LP_FASTLANE_PROMOTION_TTL_SEC,
    LP_QUALITY_MIN_FASTLANE_ROI_SCORE,
    MARKET_CONTEXT_PRIMARY_VENUE,
    PERSISTED_EXCHANGE_ADJACENT_EXCHANGE_RELATED_MIN_PRICING_CONFIDENCE,
    PERSISTED_EXCHANGE_ADJACENT_EXCHANGE_RELATED_MIN_USD,
    TRADE_ACTION_CONFLICT_WINDOW_SEC,
)
from constants import STABLE_TOKEN_CONTRACTS
from delivery_policy import can_emit_delivery_notification, record_delivery_notification
from filter import (
    WATCH_ADDRESSES,
    get_address_meta,
    get_flow_endpoints,
    get_primary_watch_meta,
    get_threshold,
    get_watch_context,
    is_smart_money_strategy_role,
    strategy_role_group,
)
from lp_analyzer import LP_ALL_INTENTS, LPAnalyzer
from lp_analyzer import canonicalize_pool_semantic_key
from lp_noise_rules import (
    LP_ADJACENT_NOISE_STAGE_PIPELINE,
    lp_adjacent_noise_core_decision,
)
from lp_product_helpers import stage_label_for_timing
from liquidation_detector import LiquidationDetector
from market_context_adapter import build_market_context_adapter
from models import Event
from outcome_scheduler import OutcomeScheduler
from price_service import PriceService
from processor import parse_tx
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from signal_interpreter import SignalInterpreter
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer
from trade_action import apply_trade_action, build_trade_action_signal_summary
from user_tiers import apply_user_tier_context


class SignalPipeline:
    """
    主处理管道：
    Data -> Parsing -> Pricing -> Intent -> State -> Intelligence -> Strategy -> Output
    本阶段增强：
    - 先给出初步意图
    - 再基于短时序列与 token 共振做确认/降级
    - 把确认结构贯穿到 gate / strategy / interpreter
    """

    def __init__(
        self,
        price_service: PriceService,
        state_manager: StateManager,
        behavior_analyzer: BehaviorAnalyzer,
        address_scorer: AddressScorer,
        token_scorer: TokenScorer,
        strategy_engine: StrategyEngine,
        quality_gate: SignalQualityGate | None = None,
        signal_interpreter: SignalInterpreter | None = None,
        lp_analyzer: LPAnalyzer | None = None,
        liquidation_detector: LiquidationDetector | None = None,
        address_intelligence=None,
        archive_store=None,
        followup_tracker=None,
        asset_case_manager: AssetCaseManager | None = None,
        asset_market_state_manager: AssetMarketStateManager | None = None,
        trade_opportunity_manager: TradeOpportunityManager | None = None,
        market_context_adapter=None,
        quality_manager: QualityManager | None = None,
        outcome_scheduler: OutcomeScheduler | None = None,
    ) -> None:
        self.price_service = price_service
        self.state_manager = state_manager
        self.behavior_analyzer = behavior_analyzer
        self.address_scorer = address_scorer
        self.token_scorer = token_scorer
        self.quality_gate = quality_gate or SignalQualityGate(state_manager=state_manager)
        self.strategy_engine = strategy_engine
        self.signal_interpreter = signal_interpreter or SignalInterpreter()
        self.lp_analyzer = lp_analyzer or LPAnalyzer()
        self.liquidation_detector = liquidation_detector or LiquidationDetector()
        self.asset_case_manager = asset_case_manager or AssetCaseManager()
        self.asset_market_state_manager = asset_market_state_manager or AssetMarketStateManager(
            state_manager=state_manager,
        )
        self.trade_opportunity_manager = trade_opportunity_manager or TradeOpportunityManager(
            state_manager=state_manager,
        )
        self.market_context_adapter = market_context_adapter or build_market_context_adapter()
        self.quality_manager = quality_manager or QualityManager(state_manager=state_manager)
        self.outcome_scheduler = outcome_scheduler or OutcomeScheduler(
            state_manager=state_manager,
            market_context_adapter=self.market_context_adapter,
            quality_manager=self.quality_manager,
            trade_opportunity_manager=self.trade_opportunity_manager,
        )
        self.address_intelligence = address_intelligence
        self.archive_store = archive_store
        self.followup_tracker = followup_tracker
        self.runtime_stats = {
            "signals_entered_notifier": 0,
            "pricing_unavailable_no_proxy_count": 0,
            "low_pricing_confidence_count": 0,
        }
        self._runtime_log_interval_sec = 300
        self._last_notifier_stats_log_ts = 0.0
        self._last_pricing_stats_log_ts = 0.0
        self._pricing_unavailable_by_token_contract = defaultdict(int)
        self._pricing_unavailable_by_token_symbol = defaultdict(int)
        self._pricing_unavailable_by_strategy_role = defaultdict(int)
        self._pricing_unavailable_by_watch_address = defaultdict(int)
        self._pricing_unavailable_by_monitor_type = defaultdict(int)
        self._pricing_unavailable_by_anchor_watch = defaultdict(int)
        self._pricing_unavailable_by_watch_meta_source = defaultdict(int)
        self._low_pricing_confidence_by_bucket = defaultdict(int)
        self._low_pricing_confidence_by_token_contract = defaultdict(int)
        self._low_pricing_confidence_by_token_symbol = defaultdict(int)
        self._low_pricing_confidence_by_strategy_role = defaultdict(int)
        self._low_pricing_confidence_by_watch_address = defaultdict(int)
        self._lp_trade_action_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=96))

    def _top_counter_items(self, counter: dict[str, int], limit: int = 3) -> list[tuple[str, int]]:
        return sorted(
            (
                (str(key), int(value))
                for key, value in (counter or {}).items()
                if key and int(value) > 0
            ),
            key=lambda item: (-item[1], item[0]),
        )[:limit]

    def _recent_trade_action_signals(
        self,
        asset_symbol: str,
        *,
        now_ts: int,
        window_sec: int | None = None,
    ) -> list[dict]:
        symbol = str(asset_symbol or "").strip().upper()
        if not symbol:
            return []
        effective_window = max(int(window_sec or TRADE_ACTION_CONFLICT_WINDOW_SEC), 1)
        history = self._lp_trade_action_history.get(symbol)
        if history is None:
            return []
        recent: list[dict] = []
        while history and now_ts - int(history[0].get("ts") or 0) > effective_window:
            history.popleft()
        for item in history:
            if now_ts - int(item.get("ts") or 0) <= effective_window:
                recent.append(dict(item))
        return recent

    def _register_trade_action_signal(self, event: Event, signal) -> dict:
        summary = build_trade_action_signal_summary(event, signal)
        asset_symbol = str(summary.get("asset_symbol") or "").strip().upper()
        direction = str(summary.get("direction") or "")
        if not asset_symbol or direction not in {"long", "short"}:
            return summary
        self._lp_trade_action_history[asset_symbol].append(summary)
        return summary

    def _carry_runtime_candidate_fields(self, parsed: dict, raw_item: dict) -> dict:
        carried = dict(parsed or {})
        source = dict(raw_item or {})
        passthrough_fields = (
            "ingest_ts",
            "listener_rpc_mode",
            "listener_block_fetch_mode",
            "listener_block_fetch_reason",
            "listener_block_get_logs_request_count",
            "listener_block_topic_chunk_count",
            "listener_block_monitored_address_count",
            "listener_block_lp_secondary_scan_used",
            "listener_block_bloom_prefilter_used",
            "listener_block_bloom_skipped_get_logs_count",
            "listener_block_bloom_transfer_possible",
            "listener_block_bloom_address_possible_count",
            "listener_runtime_adjacent_core_count",
            "listener_runtime_adjacent_secondary_count",
            "listener_runtime_adjacent_secondary_scan_used",
            "listener_runtime_adjacent_secondary_skipped_count",
            "listener_block_lp_primary_trend_scan_used",
            "listener_block_lp_extended_scan_used",
            "listener_block_lp_promoted_main_scan_used",
            "listener_block_lp_primary_trend_pool_count",
            "listener_block_lp_extended_pool_count",
            "listener_block_lp_promoted_pool_count",
            "listener_block_get_logs_primary_side_count",
            "listener_block_get_logs_secondary_side_count",
            "listener_block_get_logs_secondary_side_skipped_count",
            "listener_block_get_logs_empty_response_count",
            "low_cu_mode_enabled",
            "low_cu_mode_lp_secondary_only",
            "low_cu_mode_poll_interval_sec",
            "lp_scan_path",
            "lp_scan_paths_by_pool",
            "lp_promoted_fastlane",
            "lp_promote_reason",
            "tx_pool_hit_count",
            "touched_lp_pool_count",
            "pool_transfer_count_by_pool",
            "pool_candidate_weight",
            "participant_addresses",
            "next_hop_addresses",
            "raw_log_count",
            "replay_source",
        )
        for field in passthrough_fields:
            if field in source:
                carried[field] = source.get(field)
        carried["first_chain_seen_at"] = int(
            source.get("first_chain_seen_at")
            or source.get("ingest_ts")
            or carried.get("first_chain_seen_at")
            or 0
        )
        carried["parsed_at"] = int(time.time())
        return carried

    def _lp_price_proxy(self, event: Event) -> float:
        raw = event.metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        base_amount = float(lp_context.get("base_amount") or event.amount or 0.0)
        quote_amount = float(lp_context.get("quote_amount") or event.metadata.get("quote_amount") or 0.0)
        if base_amount <= 0 or quote_amount <= 0:
            return 0.0
        return quote_amount / max(base_amount, 1e-12)

    def _annotate_lp_market_timing(self, event: Event) -> dict:
        if not self._is_lp_event(event=event):
            return {}
        first_chain_seen_at = int(event.metadata.get("first_chain_seen_at") or 0)
        parsed_at = int(event.metadata.get("parsed_at") or time.time())
        block_age_s = max(parsed_at - first_chain_seen_at, 0) if first_chain_seen_at > 0 else 0
        detect_latency_ms = int(block_age_s * 1000)
        price_proxy = self._lp_price_proxy(event)
        pool_address = str(event.address or "").lower()
        move_before_30s = 0.0
        move_before_60s = 0.0
        if price_proxy > 0 and hasattr(self.state_manager, "_lp_price_move_from_history"):
            move_before_30s = float(
                self.state_manager._lp_price_move_from_history(pool_address, price_proxy, int(event.ts or time.time()), 30) or 0.0
            )
            move_before_60s = float(
                self.state_manager._lp_price_move_from_history(pool_address, price_proxy, int(event.ts or time.time()), 60) or 0.0
            )
        payload = {
            "first_chain_seen_at": first_chain_seen_at,
            "parsed_at": parsed_at,
            "lp_block_age_s": round(float(block_age_s), 3),
            "lp_detect_latency_ms": detect_latency_ms,
            "pool_price_move_before_alert_30s": round(float(move_before_30s), 6),
            "pool_price_move_before_alert_60s": round(float(move_before_60s), 6),
            "pool_price_move_after_alert_60s": None,
            "pool_price_move_after_alert_300s": None,
        }
        event.metadata.update(payload)
        return payload

    def _promote_lp_fastlane_if_needed(self, event: Event, signal, gate_metrics: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if not bool(LP_FASTLANE_PROMOTION_ENABLE):
            return {}
        if not self._is_lp_event(event=event):
            return {}
        pool_address = str(event.address or "").lower()
        if not pool_address:
            return {}
        stage = str(signal.context.get("lp_alert_stage") or event.metadata.get("lp_alert_stage") or "")
        multi_pool_resonance = int(gate_metrics.get("lp_multi_pool_resonance") or 0)
        structure_score = float(gate_metrics.get("lp_structure_score") or 0.0)
        lp_trend_primary_pool = bool(gate_metrics.get("lp_trend_primary_pool") or event.metadata.get("lp_trend_primary_pool"))
        lp_major_pool = bool(
            signal.context.get("lp_major_pool")
            or signal.metadata.get("lp_major_pool")
            or event.metadata.get("lp_major_pool")
            or gate_metrics.get("lp_major_pool")
        )
        major_priority_score = float(
            signal.context.get("lp_major_priority_score")
            or signal.metadata.get("lp_major_priority_score")
            or event.metadata.get("lp_major_priority_score")
            or gate_metrics.get("lp_major_priority_score")
            or 1.0
        )
        fastlane_roi_score = float(
            signal.context.get("fastlane_roi_score")
            or signal.metadata.get("fastlane_roi_score")
            or event.metadata.get("fastlane_roi_score")
            or 0.55
        )
        asset_case_quality_score = float(
            signal.context.get("asset_case_quality_score")
            or signal.metadata.get("asset_case_quality_score")
            or event.metadata.get("asset_case_quality_score")
            or 0.58
        )
        reason = ""
        priority_score = 0.0
        if stage == "prealert":
            if fastlane_roi_score < float(LP_QUALITY_MIN_FASTLANE_ROI_SCORE):
                return {}
            reason = str(event.metadata.get("lp_prealert_reason") or "lp_prealert_fastlane")
            priority_score = max(0.44, min(0.72, 0.52 + fastlane_roi_score * 0.20 + asset_case_quality_score * 0.10))
        elif multi_pool_resonance >= 2:
            reason = "lp_multi_pool_resonance_fastlane"
            priority_score = max(0.58, min(0.84, 0.58 + fastlane_roi_score * 0.16 + asset_case_quality_score * 0.12))
        elif lp_trend_primary_pool:
            reason = "lp_primary_trend_pool_fastlane"
            priority_score = max(0.40, min(0.70, 0.40 + fastlane_roi_score * 0.10 + asset_case_quality_score * 0.10))
        elif structure_score >= 0.5:
            reason = "lp_structure_hot_pool_fastlane"
            priority_score = max(0.36, min(0.60, 0.34 + fastlane_roi_score * 0.08 + asset_case_quality_score * 0.08))
        if not reason:
            return {}
        if lp_major_pool:
            reason = f"{reason}_major_pool"
            priority_score = min(0.94, priority_score * max(major_priority_score, 1.0))
        payload = self.state_manager.register_lp_fastlane_pool(
            pool_address,
            now_ts=int(event.ts or time.time()),
            ttl_sec=int(LP_FASTLANE_PROMOTION_TTL_SEC or 0),
            reason=reason,
            alert_stage=stage,
            signal_id=str(signal.signal_id or ""),
            priority_score=priority_score,
        )
        if payload:
            event.metadata["lp_promote_reason"] = str(payload.get("promote_reason") or reason)
            signal.metadata["lp_promote_reason"] = str(payload.get("promote_reason") or reason)
            signal.context["lp_promote_reason"] = str(payload.get("promote_reason") or reason)
        return payload

    def _record_lp_outcome_runtime(self, event: Event, signal) -> dict:
        payload = self.state_manager.record_lp_outcome_alert(event, signal)
        if not payload:
            return {}
        event.metadata["lp_outcome_record"] = dict(payload)
        signal.metadata["lp_outcome_record"] = dict(payload)
        signal.context["lp_outcome_record"] = dict(payload)
        signal.context["pool_price_move_before_alert_30s"] = float(payload.get("move_before_alert_30s") or 0.0)
        signal.context["pool_price_move_before_alert_60s"] = float(payload.get("move_before_alert_60s") or 0.0)
        signal.context["pool_price_move_after_alert_30s"] = payload.get("move_after_alert_30s")
        signal.context["pool_price_move_after_alert_60s"] = payload.get("move_after_alert_60s")
        signal.context["pool_price_move_after_alert_300s"] = payload.get("move_after_alert_300s")
        event.metadata["pool_price_move_after_alert_30s"] = payload.get("move_after_alert_30s")
        event.metadata["pool_price_move_after_alert_60s"] = payload.get("move_after_alert_60s")
        event.metadata["pool_price_move_after_alert_300s"] = payload.get("move_after_alert_300s")
        outcome_tracking = dict(signal.context.get("outcome_tracking") or {})
        if outcome_tracking:
            outcome_tracking["move_before_alert"] = payload.get("move_before_alert")
            outcome_tracking["move_before_alert_30s"] = payload.get("move_before_alert_30s")
            outcome_tracking["move_before_alert_60s"] = payload.get("move_before_alert_60s")
            outcome_tracking["move_after_alert"] = payload.get("move_after_alert")
            outcome_tracking["time_to_confirm"] = payload.get("time_to_confirm")
            outcome_tracking["outcome_price_source"] = payload.get("outcome_price_source")
            outcome_tracking["outcome_price_start"] = payload.get("outcome_price_start")
            outcome_tracking["outcome_price_end"] = payload.get("outcome_price_end")
            outcome_tracking["outcome_window_status"] = payload.get("outcome_window_status")
            outcome_tracking["outcome_failure_reason"] = payload.get("outcome_failure_reason")
            windows = dict(outcome_tracking.get("windows") or {})
            window_30s = dict(windows.get("30s") or {})
            window_30s.update({
                "status": ((payload.get("outcome_windows") or {}).get("30s") or {}).get("status"),
                "move_after_alert": payload.get("move_after_alert_30s"),
                "raw_move_after": payload.get("raw_move_after_30s"),
                "direction_adjusted_move_after": payload.get("direction_adjusted_move_after_30s"),
                "adverse_by_direction": payload.get("adverse_by_direction_30s"),
                "price_source": ((payload.get("outcome_windows") or {}).get("30s") or {}).get("price_source"),
                "completed_at": ((payload.get("outcome_windows") or {}).get("30s") or {}).get("completed_at"),
                "failure_reason": ((payload.get("outcome_windows") or {}).get("30s") or {}).get("failure_reason"),
                "followthrough_positive": payload.get("followthrough_positive"),
                "followthrough_negative": payload.get("followthrough_negative"),
                "confirm_after_prealert": payload.get("confirm_after_prealert"),
                "reversal_after_climax": payload.get("reversal_after_climax"),
            })
            window_60s = dict(windows.get("60s") or {})
            window_60s.update({
                "status": ((payload.get("outcome_windows") or {}).get("60s") or {}).get("status"),
                "move_after_alert": payload.get("move_after_alert_60s"),
                "raw_move_after": payload.get("raw_move_after_60s"),
                "direction_adjusted_move_after": payload.get("direction_adjusted_move_after_60s"),
                "adverse_by_direction": payload.get("adverse_by_direction_60s"),
                "price_source": ((payload.get("outcome_windows") or {}).get("60s") or {}).get("price_source"),
                "completed_at": ((payload.get("outcome_windows") or {}).get("60s") or {}).get("completed_at"),
                "failure_reason": ((payload.get("outcome_windows") or {}).get("60s") or {}).get("failure_reason"),
                "confirm_after_prealert": payload.get("confirm_after_prealert"),
                "false_prealert": payload.get("false_prealert"),
                "reversal_after_climax": payload.get("reversal_after_climax"),
            })
            window_300s = dict(windows.get("300s") or {})
            window_300s.update({
                "status": ((payload.get("outcome_windows") or {}).get("300s") or {}).get("status"),
                "move_after_alert": payload.get("move_after_alert_300s"),
                "raw_move_after": payload.get("raw_move_after_300s"),
                "direction_adjusted_move_after": payload.get("direction_adjusted_move_after_300s"),
                "adverse_by_direction": payload.get("adverse_by_direction_300s"),
                "price_source": ((payload.get("outcome_windows") or {}).get("300s") or {}).get("price_source"),
                "completed_at": ((payload.get("outcome_windows") or {}).get("300s") or {}).get("completed_at"),
                "failure_reason": ((payload.get("outcome_windows") or {}).get("300s") or {}).get("failure_reason"),
                "confirm_after_prealert": payload.get("confirm_after_prealert"),
                "false_prealert": payload.get("false_prealert"),
                "reversal_after_climax": payload.get("reversal_after_climax"),
            })
            outcome_tracking["windows"] = {"30s": window_30s, "60s": window_60s, "300s": window_300s}
            signal.context["outcome_tracking"] = outcome_tracking
            event.metadata["outcome_tracking"] = outcome_tracking
            signal.metadata["outcome_tracking"] = outcome_tracking
        if self.outcome_scheduler is not None:
            self.outcome_scheduler.register_from_lp_record(payload)
        self.quality_manager.sync_from_state_manager(force=True)
        return payload

    def _market_context_brief(self, context_payload: dict) -> str:
        source = str(context_payload.get("market_context_source") or "unavailable")
        if source == "unavailable":
            return ""
        timing_label = stage_label_for_timing(context_payload.get("alert_relative_timing"))
        if not timing_label:
            return ""
        parts = [f"合约视角：{timing_label}"]
        basis_bps = context_payload.get("basis_bps")
        if basis_bps is not None:
            parts.append(f"基差 {float(basis_bps):+.1f}bp")
        mark_index_spread_bps = context_payload.get("mark_index_spread_bps")
        if mark_index_spread_bps is not None:
            parts.append(f"M/I {float(mark_index_spread_bps):+.1f}bp")
        elif context_payload.get("last_mark_spread_bps") is not None:
            parts.append(f"L/M {float(context_payload.get('last_mark_spread_bps') or 0.0):+.1f}bp")
        return "｜".join(parts[:3])

    def _annotate_market_context(self, event: Event, signal) -> dict:
        if not self._is_lp_event(event=event):
            return {}
        token_or_pair = (
            str(signal.context.get("pair_label") or "")
            or str(signal.metadata.get("pair_label") or "")
            or str(((event.metadata.get("raw") or {}).get("lp_context") or {}).get("pair_label") or "")
            or str(signal.context.get("asset_case_label") or "")
            or str(event.metadata.get("asset_case_label") or "")
            or str(event.metadata.get("token_symbol") or event.token or "")
        )
        context_payload = dict(
            self.market_context_adapter.get_market_context(
                token_or_pair,
                int(event.ts or 0),
                venue=MARKET_CONTEXT_PRIMARY_VENUE,
            )
            or {}
        )
        source = str(context_payload.get("market_context_source") or "unavailable")
        if source != "unavailable" and (source == "live_public" or not context_payload.get("alert_relative_timing")):
            context_payload["alert_relative_timing"] = self.market_context_adapter.classify_alert_relative_timing(
                context_payload,
                stage=str(signal.context.get("lp_alert_stage") or event.metadata.get("lp_alert_stage") or ""),
            )
        timing = str(context_payload.get("alert_relative_timing") or "")
        update_payload = {
            **context_payload,
            "market_context_available": source != "unavailable",
            "market_timing_label": stage_label_for_timing(timing),
            "market_context_brief": self._market_context_brief(context_payload),
        }
        event.metadata.update(update_payload)
        signal.metadata.update(update_payload)
        signal.context.update(update_payload)
        return update_payload

    def _lp_direction_sign(self, intent_type: str) -> int:
        normalized = str(intent_type or "")
        if normalized == "pool_buy_pressure":
            return 1
        if normalized == "pool_sell_pressure":
            return -1
        return 0

    def _lp_direction_labels(self, intent_type: str) -> tuple[str, str]:
        if str(intent_type or "") == "pool_sell_pressure":
            return "卖压", "承接"
        return "买压", "吸收"

    def _signed_market_alignment(self, intent_type: str, *moves) -> float:
        direction = self._lp_direction_sign(intent_type)
        if direction == 0:
            return 0.0
        scored = []
        for move in moves:
            if move in {None, ""}:
                continue
            try:
                scored.append(direction * float(move))
            except (TypeError, ValueError):
                continue
        if not scored:
            return 0.0
        return max(scored)

    def _apply_lp_signal_corrections(self, event: Event, signal, gate_metrics: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if not self._is_lp_event(event=event):
            return {}
        stage = str(
            signal.context.get("lp_alert_stage")
            or signal.metadata.get("lp_alert_stage")
            or event.metadata.get("lp_alert_stage")
            or ""
        ).strip()
        if stage not in {"prealert", "confirm", "climax", "exhaustion_risk"}:
            return {}

        intent_type = str(event.intent_type or "")
        move_label, absorption_verb = self._lp_direction_labels(intent_type)
        market_source = str(signal.context.get("market_context_source") or event.metadata.get("market_context_source") or "unavailable")
        alert_timing = str(signal.context.get("alert_relative_timing") or event.metadata.get("alert_relative_timing") or "")
        same_pool_continuity = int(
            signal.context.get("lp_same_pool_continuity")
            or signal.metadata.get("lp_same_pool_continuity")
            or gate_metrics.get("lp_same_pool_continuity")
            or (event.metadata.get("lp_analysis") or {}).get("same_pool_continuity")
            or 0
        )
        multi_pool_resonance = int(
            signal.context.get("lp_multi_pool_resonance")
            or signal.metadata.get("lp_multi_pool_resonance")
            or gate_metrics.get("lp_multi_pool_resonance")
            or (event.metadata.get("lp_analysis") or {}).get("multi_pool_resonance")
            or 0
        )
        asset_case_supporting_pairs = int(
            signal.context.get("asset_case_supporting_pair_count")
            or signal.metadata.get("asset_case_supporting_pair_count")
            or event.metadata.get("asset_case_supporting_pair_count")
            or 1
        )
        pool_move_before_abs = max(
            abs(float(signal.context.get("pool_price_move_before_alert_30s") or event.metadata.get("pool_price_move_before_alert_30s") or 0.0)),
            abs(float(signal.context.get("pool_price_move_before_alert_60s") or event.metadata.get("pool_price_move_before_alert_60s") or 0.0)),
        )
        market_move_before_abs = max(
            abs(float(signal.context.get("market_move_before_alert_30s") or event.metadata.get("market_move_before_alert_30s") or 0.0)),
            abs(float(signal.context.get("market_move_before_alert_60s") or event.metadata.get("market_move_before_alert_60s") or 0.0)),
        )
        signed_market_alignment = self._signed_market_alignment(
            intent_type,
            signal.context.get("market_move_before_alert_30s"),
            signal.context.get("market_move_before_alert_60s"),
            signal.context.get("market_move_after_alert_60s"),
        )
        detect_latency_ms = int(
            signal.context.get("lp_detect_latency_ms")
            or signal.metadata.get("lp_detect_latency_ms")
            or event.metadata.get("lp_detect_latency_ms")
            or 0
        )
        case_started_at = int(
            signal.context.get("asset_case_started_at")
            or signal.metadata.get("asset_case_started_at")
            or event.metadata.get("asset_case_started_at")
            or 0
        )
        case_age_sec = int(max(int(event.ts or 0) - case_started_at, 0)) if case_started_at > 0 else 0
        climax_reversal_score = float(
            signal.context.get("climax_reversal_score")
            or signal.metadata.get("climax_reversal_score")
            or event.metadata.get("climax_reversal_score")
            or 0.0
        )
        pool_quality_score = float(
            signal.context.get("pool_quality_score")
            or signal.metadata.get("pool_quality_score")
            or event.metadata.get("pool_quality_score")
            or 0.58
        )
        pair_quality_score = float(
            signal.context.get("pair_quality_score")
            or signal.metadata.get("pair_quality_score")
            or event.metadata.get("pair_quality_score")
            or 0.58
        )
        asset_case_quality_score = float(
            signal.context.get("asset_case_quality_score")
            or signal.metadata.get("asset_case_quality_score")
            or event.metadata.get("asset_case_quality_score")
            or max(pool_quality_score, pair_quality_score, 0.58)
        )
        quality_floor = min(pool_quality_score, pair_quality_score, asset_case_quality_score)
        quality_gap = max(pool_quality_score, pair_quality_score, asset_case_quality_score) - quality_floor
        sweep_phase = str(
            signal.context.get("lp_sweep_phase")
            or signal.metadata.get("lp_sweep_phase")
            or event.metadata.get("lp_sweep_phase")
            or ""
        )
        sweep_detected = bool(
            signal.context.get("lp_sweep_detected")
            or signal.metadata.get("lp_sweep_detected")
            or event.metadata.get("lp_sweep_detected")
            or sweep_phase
        )
        single_pool_dominant = multi_pool_resonance <= 1 and asset_case_supporting_pairs <= 1

        broader_alignment_confirmed = (
            market_source == "live_public"
            and signed_market_alignment >= 0.0015
            and alert_timing in {"leading", "confirming"}
        )
        broader_alignment = (
            "confirmed"
            if broader_alignment_confirmed
            else "unavailable"
            if market_source == "unavailable"
            else "weak_or_missing"
        )

        absorption_context = "pool_only_unconfirmed_pressure"
        absorption_confidence = 0.35
        local_vs_broad_reason = "缺 broader perp/spot 同向确认"
        if broader_alignment_confirmed and (multi_pool_resonance >= 2 or asset_case_supporting_pairs >= 2):
            absorption_context = (
                "broader_buy_pressure_confirmed"
                if intent_type == "pool_buy_pressure"
                else "broader_sell_pressure_confirmed"
            )
            absorption_confidence = min(
                0.95,
                0.56
                + min(multi_pool_resonance / 4.0, 1.0) * 0.18
                + min(asset_case_supporting_pairs / 3.0, 1.0) * 0.12
                + min(max(signed_market_alignment, 0.0) / 0.006, 1.0) * 0.14,
            )
            local_vs_broad_reason = "多池同向，且 broader perp/spot 同向"
        elif stage == "confirm" and (
            single_pool_dominant
            or market_source == "unavailable"
            or climax_reversal_score >= 0.60
        ):
            absorption_context = (
                "local_buy_pressure_absorption"
                if intent_type == "pool_buy_pressure"
                else "local_sell_pressure_absorption"
            )
            absorption_confidence = min(
                0.90,
                0.42
                + (0.16 if single_pool_dominant else 0.0)
                + (0.10 if market_source == "unavailable" else 0.0)
                + min(climax_reversal_score / 0.8, 1.0) * 0.14,
            )
            if market_source == "unavailable":
                local_vs_broad_reason = f"市场上下文不可用，单池主导，{move_label}缺 broader 同向确认"
            elif market_source == "live_public":
                local_vs_broad_reason = f"单池主导，perp/spot 未见更广泛同向确认"
            else:
                local_vs_broad_reason = f"单池主导，{move_label}缺 broader 同向确认"
        else:
            absorption_confidence = min(
                0.78,
                0.32
                + (0.12 if multi_pool_resonance <= 1 else 0.0)
                + (0.10 if asset_case_supporting_pairs <= 1 else 0.0)
                + (0.08 if market_source == "unavailable" else 0.0),
            )
            local_vs_broad_reason = (
                "市场上下文不可用，局部池子信号存在，但仍待 broader perp/spot 确认"
                if market_source == "unavailable"
                else "局部池子信号存在，但仍待 broader perp/spot 确认"
            )

        confirm_scope = ""
        confirm_quality = ""
        confirm_reason = ""
        confirm_alignment_score = 0.0
        chase_risk_score = 0.0
        confirm_timing_bucket = ""
        stage_badge = str(signal.context.get("lp_stage_badge") or "")
        state_label = str(signal.context.get("lp_state_label") or signal.context.get("market_state_label") or "")
        market_read = str(signal.context.get("lp_market_read") or "")

        def _base_absorption_label() -> str:
            if absorption_context == "broader_buy_pressure_confirmed":
                return "更广泛买压确认"
            if absorption_context == "broader_sell_pressure_confirmed":
                return "更广泛卖压确认"
            if absorption_context == "local_sell_pressure_absorption":
                return "局部卖压，可能被承接"
            if absorption_context == "local_buy_pressure_absorption":
                return "局部买压，仍待 broader 确认"
            return f"局部{move_label}，仍待 broader 确认"

        def _building_label() -> str:
            if intent_type == "pool_sell_pressure":
                return "局部卖压建立中，待确认"
            if intent_type == "pool_buy_pressure":
                return "局部买压建立中，待确认"
            if intent_type == "liquidity_removal":
                return "局部深度抽离建立中，待确认"
            if intent_type == "liquidity_addition":
                return "局部深度补充建立中，待确认"
            return "局部结构建立中，待确认"

        def _late_label() -> str:
            if intent_type == "pool_sell_pressure":
                return "持续卖压（偏晚）"
            if intent_type == "pool_buy_pressure":
                return "持续买压（偏晚）"
            return f"局部{move_label}（偏晚）"

        def _chase_label() -> str:
            if intent_type == "pool_sell_pressure":
                return "持续卖压（追空风险）"
            if intent_type == "pool_buy_pressure":
                return "持续买压（追涨风险）"
            return f"局部{move_label}（追单风险）"

        if stage == "confirm" and not sweep_detected:
            broader_scope_confirmed = broader_alignment_confirmed and (multi_pool_resonance >= 2 or asset_case_supporting_pairs >= 2)
            confirm_scope = "broader_confirm" if broader_scope_confirmed else "local_confirm"
            confirm_alignment_score = min(
                1.0,
                (0.30 if market_source == "live_public" else 0.02)
                + (0.16 if alert_timing in {"leading", "confirming"} else 0.0)
                + (0.22 if broader_alignment_confirmed else 0.0)
                + min(multi_pool_resonance / 3.0, 1.0) * 0.14
                + min(max(same_pool_continuity, 0) / 3.0, 1.0) * 0.08
                + min(asset_case_supporting_pairs / 3.0, 1.0) * 0.06
                + min(quality_floor / 0.75, 1.0) * 0.06
            )
            chase_risk_score = min(
                1.0,
                min(pool_move_before_abs / 0.014, 1.0) * 0.34
                + min(market_move_before_abs / 0.015, 1.0) * 0.12
                + (0.18 if alert_timing == "late" else 0.0)
                + (0.20 if market_source == "unavailable" else 0.0)
                + min(climax_reversal_score / 0.8, 1.0) * 0.12
                + (0.12 if single_pool_dominant else 0.0)
                + (0.10 if not broader_alignment_confirmed else 0.0)
                + (0.10 if case_age_sec >= 180 else 0.0)
                + (0.08 if detect_latency_ms >= 5_500 else 0.0)
                + (0.10 if quality_floor < 0.56 or quality_gap >= 0.18 else 0.0)
            )
            clean_confirm = (
                market_source == "live_public"
                and broader_alignment_confirmed
                and pool_move_before_abs < 0.006
                and market_move_before_abs < 0.006
                and detect_latency_ms < 4_000
                and case_age_sec < 120
                and quality_floor >= 0.56
            )
            late_confirm = (
                alert_timing == "late"
                or (market_source == "unavailable" and pool_move_before_abs >= 0.007)
                or pool_move_before_abs >= 0.009
                or market_move_before_abs >= 0.008
                or detect_latency_ms >= 4_500
                or case_age_sec >= 150
                or (climax_reversal_score >= 0.62 and quality_floor < 0.62 and not broader_alignment_confirmed)
                or quality_gap >= 0.18
            )
            chase_risk = (
                chase_risk_score >= 0.58
                and (
                    pool_move_before_abs >= 0.010
                    or market_move_before_abs >= 0.010
                    or (alert_timing == "late" and not broader_alignment_confirmed)
                    or (market_source == "unavailable" and pool_move_before_abs >= 0.008 and single_pool_dominant)
                    or detect_latency_ms >= 8_000
                    or case_age_sec >= 240
                    or (climax_reversal_score >= 0.70 and not broader_alignment_confirmed)
                )
            )
            if chase_risk:
                confirm_timing_bucket = "chase_window"
            elif late_confirm:
                confirm_timing_bucket = "late_window"
            elif clean_confirm:
                confirm_timing_bucket = "clean_window"
            else:
                confirm_timing_bucket = "stretched_window"
            if chase_risk:
                confirm_quality = "chase_risk"
                confirm_reason = "确认已明显偏后，且 broader confirmation 不足，当前更像追单风险"
                stage_badge = "风险"
                state_label = _chase_label()
                market_read = (
                    "已出现明显预走或局部冲击，当前更像追单风险而不是 clean confirm｜"
                    f"{local_vs_broad_reason}"
                )
            elif late_confirm:
                confirm_quality = "late_confirm"
                confirm_reason = "确认成立，但节奏偏晚、单池主导或 broader confirmation 不足"
                state_label = _late_label()
                market_read = (
                    "确认已成立，但更像中后段确认，不应按 ultra-early 先手理解｜"
                    f"{local_vs_broad_reason}"
                )
            elif clean_confirm:
                confirm_quality = "clean_confirm"
                confirm_reason = "多池/更广泛盘口同向，且预走不大"
                state_label = _base_absorption_label()
                market_read = "更像 clean confirm：已有 broader perp/spot 同向确认，但仍不是首发先手"
            else:
                confirm_quality = "unconfirmed_confirm"
                if broader_scope_confirmed:
                    confirm_reason = "已见多池与 live public 同向，但预走/质量一般，仍不能当成下一根 K 线预测"
                    state_label = _base_absorption_label()
                    market_read = (
                        "已见 broader perp/spot 同向，但这仍只是结构确认，不是下一根 K 线预测｜"
                        f"{local_vs_broad_reason}"
                    )
                else:
                    confirm_reason = "确认成立，但缺更广泛盘口同向确认，不能当作继续追击的保证"
                    state_label = _base_absorption_label()
                    market_read = (
                        "确认已出现，但缺 broader perp/spot 同向确认，延续性仍待跟踪｜"
                        f"{local_vs_broad_reason}"
                    )

        elif stage == "prealert":
            if not sweep_detected:
                state_label = _building_label()
            if not sweep_detected:
                if market_source == "unavailable":
                    market_read = "先手观察｜市场上下文不可用，先看 30-90s 是否续单 / 共振"
                else:
                    market_read = "先手观察｜结构建立中，先看 30-90s 是否续单 / 跨池共振"

        payload = {
            "lp_confirm_scope": confirm_scope,
            "lp_confirm_quality": confirm_quality,
            "lp_confirm_reason": confirm_reason,
            "lp_confirm_alignment_score": round(float(confirm_alignment_score), 3),
            "lp_chase_risk_score": round(float(chase_risk_score), 3),
            "lp_confirm_timing_bucket": confirm_timing_bucket,
            "lp_absorption_context": absorption_context,
            "lp_absorption_confidence": round(float(absorption_confidence), 3),
            "lp_broader_alignment": broader_alignment,
            "lp_local_vs_broad_reason": local_vs_broad_reason,
            "lp_stage_badge": stage_badge,
            "lp_state_label": state_label,
            "market_state_label": state_label,
            "headline_label": state_label,
            "lp_market_read": market_read,
            "lp_meaning_brief": market_read,
        }
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)
        return payload

    def _apply_lp_prealert_diagnostics(
        self,
        event: Event,
        signal,
        *,
        gate_metrics: dict | None = None,
        asset_case_payload: dict | None = None,
        delivery_allowed: bool | None = None,
        delivery_block_reason: str | None = None,
    ) -> dict:
        gate_metrics = gate_metrics or {}
        if not self._is_lp_event(event=event):
            return {}
        existing = getattr(signal, "context", {}) or getattr(event, "metadata", {}) or {}
        final_stage = str(
            getattr(signal, "context", {}).get("lp_alert_stage")
            or getattr(signal, "metadata", {}).get("lp_alert_stage")
            or event.metadata.get("lp_alert_stage")
            or ""
        ).strip()
        candidate = bool(
            gate_metrics.get("lp_prealert_candidate")
            or existing.get("lp_prealert_candidate")
        )
        gate_passed = bool(
            gate_metrics.get("lp_prealert_gate_passed")
            or gate_metrics.get("lp_prealert_applied")
            or existing.get("lp_prealert_gate_passed")
            or existing.get("lp_prealert_applied")
        )
        stage_overwritten = bool(candidate and gate_passed and final_stage and final_stage != "prealert")
        if asset_case_payload is None:
            asset_case_payload = {}
        asset_case_preserved = bool(
            asset_case_payload.get("asset_case_had_prealert")
            or asset_case_payload.get("asset_case_stage") == "prealert"
            or existing.get("asset_case_had_prealert")
            or existing.get("asset_case_stage") == "prealert"
        )
        resolved_delivery_allowed = delivery_allowed
        resolved_delivery_block_reason = str(delivery_block_reason or "").strip()
        if resolved_delivery_allowed is None:
            if not candidate:
                resolved_delivery_allowed = False
                resolved_delivery_block_reason = resolved_delivery_block_reason or str(
                    gate_metrics.get("lp_prealert_candidate_reason")
                    or existing.get("lp_prealert_candidate_reason")
                    or "lp_prealert_not_candidate"
                )
            elif not gate_passed:
                resolved_delivery_allowed = False
                resolved_delivery_block_reason = resolved_delivery_block_reason or str(
                    gate_metrics.get("lp_prealert_gate_fail_reason")
                    or existing.get("lp_prealert_gate_fail_reason")
                    or "lp_prealert_gate_failed"
                )
            elif stage_overwritten:
                resolved_delivery_allowed = False
                resolved_delivery_block_reason = resolved_delivery_block_reason or "lp_prealert_stage_overwritten"
        payload = {
            "lp_prealert_candidate": bool(candidate),
            "lp_prealert_candidate_reason": str(
                gate_metrics.get("lp_prealert_candidate_reason")
                or existing.get("lp_prealert_candidate_reason")
                or gate_metrics.get("lp_prealert_reason")
                or existing.get("lp_prealert_reason")
                or ""
            ),
            "lp_prealert_gate_passed": bool(gate_passed),
            "lp_prealert_gate_fail_reason": str(
                gate_metrics.get("lp_prealert_gate_fail_reason")
                or existing.get("lp_prealert_gate_fail_reason")
                or ""
            ),
            "lp_prealert_delivery_allowed": resolved_delivery_allowed,
            "lp_prealert_delivery_block_reason": resolved_delivery_block_reason,
            "lp_prealert_asset_case_preserved": bool(asset_case_preserved),
            "lp_prealert_stage_overwritten": bool(stage_overwritten),
            "lp_prealert_first_leg": bool(
                gate_metrics.get("lp_prealert_first_leg")
                or existing.get("lp_prealert_first_leg")
            ),
            "lp_prealert_major_override_used": bool(
                gate_metrics.get("lp_prealert_major_override_used")
                or existing.get("lp_prealert_major_override_used")
            ),
        }
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)
        return payload

    def _apply_lp_productization_context(self, event: Event, signal, gate_metrics: dict | None = None, watch_meta: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if not self._is_lp_event(event=event):
            return {}
        asset_case_payload = self.asset_case_manager.merge_lp_signal(event, signal, gate_metrics=gate_metrics)
        market_context_payload = self._annotate_market_context(event, signal)
        quality_payload = self.quality_manager.annotate_lp_signal(event, signal, gate_metrics=gate_metrics)
        correction_payload = self._apply_lp_signal_corrections(event, signal, gate_metrics=gate_metrics)
        prealert_payload = self._apply_lp_prealert_diagnostics(
            event,
            signal,
            gate_metrics=gate_metrics,
            asset_case_payload=asset_case_payload,
        )
        recent_trade_signals = self._recent_trade_action_signals(
            str(
                getattr(signal, "context", {}).get("asset_symbol")
                or getattr(signal, "context", {}).get("asset_case_label")
                or getattr(event, "metadata", {}).get("asset_symbol")
                or getattr(event, "metadata", {}).get("asset_case_label")
                or getattr(event, "metadata", {}).get("token_symbol")
                or getattr(event, "token", "")
                or ""
            ),
            now_ts=int(getattr(event, "ts", 0) or 0),
            window_sec=int(TRADE_ACTION_CONFLICT_WINDOW_SEC),
        )
        trade_action_payload = apply_trade_action(
            event,
            signal,
            recent_signals=recent_trade_signals,
            conflict_window_sec=int(TRADE_ACTION_CONFLICT_WINDOW_SEC),
        )
        tier_payload = apply_user_tier_context(event=event, signal=signal, watch_meta=watch_meta)
        asset_market_state_payload = self.asset_market_state_manager.apply_lp_signal(event, signal)
        trade_opportunity_payload = self.trade_opportunity_manager.apply_lp_signal(event, signal)
        self.asset_case_manager.attach_runtime_context(
            event,
            signal,
            gate_metrics=gate_metrics,
        )
        self._register_trade_action_signal(event, signal)
        return {
            "asset_case": asset_case_payload,
            "market_context": market_context_payload,
            "quality": quality_payload,
            "lp_corrections": correction_payload,
            "prealert_diagnostics": prealert_payload,
            "trade_action": trade_action_payload,
            "asset_market_state": asset_market_state_payload,
            "trade_opportunity": trade_opportunity_payload,
            "user_tier": tier_payload,
        }

    def _log_runtime_notifier_stats_if_needed(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_notifier_stats_log_ts) < self._runtime_log_interval_sec:
            return
        self._last_notifier_stats_log_ts = now
        print(
            "🧭 pipeline funnel:",
            f"entered_notifier={int(self.runtime_stats.get('signals_entered_notifier') or 0)}",
        )

    def _mark_entered_notifier(self) -> None:
        self.runtime_stats["signals_entered_notifier"] = int(
            self.runtime_stats.get("signals_entered_notifier") or 0
        ) + 1
        self._log_runtime_notifier_stats_if_needed()

    def _pricing_confidence_bucket(self, pricing_confidence: float) -> str:
        if 0.0 < pricing_confidence < 0.5:
            return "(0,0.5)"
        if 0.5 <= pricing_confidence < 0.8:
            return "[0.5,0.8)"
        return ""

    def _log_pricing_stats_if_needed(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._last_pricing_stats_log_ts) < self._runtime_log_interval_sec:
            return
        self._last_pricing_stats_log_ts = now
        print(
            "💲 pricing stats:",
            f"unavailable_no_proxy={int(self.runtime_stats.get('pricing_unavailable_no_proxy_count') or 0)}",
            f"low_confidence={int(self.runtime_stats.get('low_pricing_confidence_count') or 0)}",
            f"top_unavailable_token={self._top_counter_items(self._pricing_unavailable_by_token_symbol)}",
            f"top_unavailable_role={self._top_counter_items(self._pricing_unavailable_by_strategy_role)}",
            f"top_unavailable_watch={self._top_counter_items(self._pricing_unavailable_by_watch_address, limit=2)}",
            f"top_unavailable_anchor={self._top_counter_items(self._pricing_unavailable_by_anchor_watch, limit=2)}",
            f"top_low_conf_token={self._top_counter_items(self._low_pricing_confidence_by_token_symbol)}",
            f"top_low_conf_role={self._top_counter_items(self._low_pricing_confidence_by_strategy_role, limit=2)}",
            f"top_low_conf_watch={self._top_counter_items(self._low_pricing_confidence_by_watch_address, limit=2)}",
            f"low_conf_bucket={self._top_counter_items(self._low_pricing_confidence_by_bucket)}",
        )

    def _record_pricing_runtime_stats(self, event: Event, watch_meta: dict, gate_decision) -> None:
        pricing_confidence = float(event.pricing_confidence or 0.0)
        token_contract = str(event.token or "").lower()
        token_symbol = str(event.metadata.get("token_symbol") or "")
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "")
        watch_address = str(event.address or "").lower()
        monitor_type = str(event.metadata.get("monitor_type") or "")
        anchor_watch_address = str(event.metadata.get("anchor_watch_address") or "")
        watch_meta_source = str(event.metadata.get("watch_meta_source") or "")

        if str(gate_decision.reason or "") == "pricing_unavailable_no_proxy":
            self.runtime_stats["pricing_unavailable_no_proxy_count"] = int(
                self.runtime_stats.get("pricing_unavailable_no_proxy_count") or 0
            ) + 1
            if token_contract:
                self._pricing_unavailable_by_token_contract[token_contract] += 1
            if token_symbol:
                self._pricing_unavailable_by_token_symbol[token_symbol] += 1
            if strategy_role:
                self._pricing_unavailable_by_strategy_role[strategy_role] += 1
            if watch_address:
                self._pricing_unavailable_by_watch_address[watch_address] += 1
            if monitor_type:
                self._pricing_unavailable_by_monitor_type[monitor_type] += 1
            if anchor_watch_address:
                self._pricing_unavailable_by_anchor_watch[anchor_watch_address] += 1
            if watch_meta_source:
                self._pricing_unavailable_by_watch_meta_source[watch_meta_source] += 1

        bucket = self._pricing_confidence_bucket(pricing_confidence)
        if bucket:
            self.runtime_stats["low_pricing_confidence_count"] = int(
                self.runtime_stats.get("low_pricing_confidence_count") or 0
            ) + 1
            self._low_pricing_confidence_by_bucket[bucket] += 1
            if token_contract:
                self._low_pricing_confidence_by_token_contract[token_contract] += 1
            if token_symbol:
                self._low_pricing_confidence_by_token_symbol[token_symbol] += 1
            if strategy_role:
                self._low_pricing_confidence_by_strategy_role[strategy_role] += 1
            if watch_address:
                self._low_pricing_confidence_by_watch_address[watch_address] += 1

        if str(gate_decision.reason or "") == "pricing_unavailable_no_proxy" or bucket:
            self._log_pricing_stats_if_needed()

    async def process(self, raw_item: dict):
        archive_status = {
            "raw_event": False,
            "parsed_event": False,
            "signal": False,
            "delivery_audit": False,
            "case_update": False,
            "case_followup": False,
        }
        archive_ts = int(time.time())
        self._archive_raw_event(raw_item, archive_status, archive_ts)

        if self._is_lp_adjacent_noise_candidate(raw_item=raw_item):
            self._archive_lp_adjacent_noise_audit(
                raw_item=raw_item,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        parsed = parse_tx(raw_item)
        if not parsed:
            self._archive_parse_failure(raw_item, archive_status, archive_ts)
            return None

        watch_context = get_watch_context(parsed)
        if not watch_context:
            return None

        parsed = self._carry_runtime_candidate_fields(parsed, raw_item)
        parsed = self._normalize_parsed(parsed, watch_context, raw_item=raw_item)
        watch_meta = (
            get_primary_watch_meta(parsed, watch_context=watch_context)
            or watch_context.get("watch_meta")
            or get_address_meta(parsed.get("watch_address"))
        )
        lp_adjacent_decision = self._lp_adjacent_noise_fallback_decision(
            parsed=parsed,
            watch_meta=watch_meta,
        )
        parsed.update(self._lp_adjacent_noise_metadata_from_decision(lp_adjacent_decision))
        if lp_adjacent_decision.get("is_noise"):
            self._archive_lp_adjacent_noise_audit(
                raw_item=raw_item,
                parsed=parsed,
                watch_meta=watch_meta,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        pricing = await self._evaluate_pricing(parsed)
        parsed["usd_value"] = float(
            pricing.get("usd_value")
            or parsed.get("usd_value")
            or parsed.get("value")
            or 0.0
        )
        parsed["pricing_confidence"] = float(pricing.get("pricing_confidence") or parsed.get("pricing_confidence") or 0.0)
        if self._is_adjacent_watch_meta_missing(parsed=parsed, watch_meta=watch_meta):
            self._archive_adjacent_watch_meta_missing_audit(
                parsed=parsed,
                watch_context=watch_context,
                watch_meta=watch_meta,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None
        if not self._allow_persisted_exchange_adjacent_flow(
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
        ):
            self._archive_persisted_exchange_adjacent_prefilter_audit(
                parsed=parsed,
                watch_context=watch_context,
                watch_meta=watch_meta,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        event = self._to_event(parsed, watch_context=watch_context, watch_meta=watch_meta, pricing=pricing)
        event.archive_ts = archive_ts
        event.event_id = self._build_event_id(event)
        parsed["event_id"] = event.event_id
        self._annotate_lp_market_timing(event)

        preliminary_intent = self._classify_intent(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
        )
        event.intent_type = preliminary_intent["intent_type"]
        event.intent_confidence = float(preliminary_intent["intent_confidence"])
        event.intent_stage = "preliminary"
        event.confirmation_score = float(preliminary_intent.get("confirmation_score") or 0.0)
        event.intent_evidence = list(preliminary_intent.get("intent_evidence") or [])
        event.metadata["watch_meta"] = {
            "role": watch_meta.get("role", "unknown"),
            "strategy_role": watch_meta.get("strategy_role", "unknown"),
            "semantic_role": watch_meta.get("semantic_role", "unknown"),
            "priority": int(watch_meta.get("priority", 3) or 3),
            "label": watch_meta.get("label", ""),
            "category": watch_meta.get("category", "unknown"),
            "intelligence_status": watch_meta.get("intelligence_status", ""),
            "suspected_role": watch_meta.get("suspected_role", ""),
            "candidate_score": float(watch_meta.get("candidate_score") or 0.0),
            "runtime_adjacent_watch": bool(watch_meta.get("runtime_adjacent_watch")),
            "watch_meta_source": str(watch_meta.get("watch_meta_source") or ""),
            "strategy_hint": str(watch_meta.get("strategy_hint") or ""),
            "runtime_state": str(watch_meta.get("runtime_state") or ""),
            "anchor_watch_address": str(watch_meta.get("anchor_watch_address") or ""),
            "anchor_label": str(watch_meta.get("anchor_label") or ""),
            "downstream_case_id": str(watch_meta.get("downstream_case_id") or ""),
        }
        event.metadata["intent"] = preliminary_intent

        self.state_manager.apply_event(event)
        self._touch_runtime_adjacent_watch(event, watch_meta)
        self._record_counterparty(event, watch_context, parsed)

        address_snapshot = self.state_manager.get_address_snapshot(
            event.address,
            window_sec=self.behavior_analyzer.window_sec,
            now_ts=event.ts,
        )
        token_snapshot = self.state_manager.get_token_snapshot(
            event.token,
            window_sec=max(self.behavior_analyzer.window_sec, 86400),
            now_ts=event.ts,
        )
        pool_snapshot = self.state_manager.get_pool_snapshot(
            event.address,
            window_sec=900,
            now_ts=event.ts,
        ) if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed) else {}

        behavior = self.behavior_analyzer.detect(event, address_snapshot)
        confirmed_intent = self._confirm_intent(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
            behavior=behavior,
            preliminary_intent=preliminary_intent,
        )
        self._apply_confirmed_intent(event, confirmed_intent)
        self._apply_lp_burst_state(event)
        self._apply_liquidation_overlay(
            event=event,
            parsed=parsed,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
        )

        address_intel_update = self._observe_address_intelligence(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            raw_item=raw_item,
            token_snapshot=token_snapshot,
        )

        case_result = self._match_or_open_case(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
        )
        behavior_case = case_result.get("case") if case_result else None
        for stale_case in (case_result or {}).get("stale_updates") or []:
            self.state_manager.update_case_pointer(
                case_id=stale_case.case_id,
                watch_address=stale_case.watch_address,
                token=stale_case.token,
                is_open=False,
            )
            self._expire_adjacent_watch_for_case(stale_case)
        for invalid_case in (case_result or {}).get("invalidated_cases") or []:
            self.state_manager.update_case_pointer(
                case_id=invalid_case.case_id,
                watch_address=invalid_case.watch_address,
                token=invalid_case.token,
                is_open=False,
            )
            self._expire_adjacent_watch_for_case(invalid_case)
        if behavior_case is not None:
            self._bind_case_to_event(event, behavior_case, case_result)
            self._register_adjacent_watch_from_case(event, behavior_case, case_result, watch_meta)
            self.state_manager.register_case(
                case_id=behavior_case.case_id,
                watch_address=behavior_case.watch_address,
                token=behavior_case.token,
            )
            if behavior_case.status in {"cooled", "closed", "invalidated"}:
                self.state_manager.update_case_pointer(
                    case_id=behavior_case.case_id,
                    watch_address=behavior_case.watch_address,
                    token=behavior_case.token,
                    is_open=False,
                )
            event.metadata["case_summary"] = behavior_case.summary
            event.metadata["case_followup_steps"] = list(behavior_case.followup_steps or [])
            event.metadata["case"] = {
                "case_id": behavior_case.case_id,
                "status": behavior_case.status,
                "stage": behavior_case.stage,
                "summary": behavior_case.summary,
            }
            self._apply_exchange_followup_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )
            self._apply_downstream_followup_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )
            self._apply_smart_money_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )
            self._apply_liquidation_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )

        self._archive_parsed_event(
            parsed=self._parsed_archive_payload(
                parsed=parsed,
                event=event,
                watch_meta=watch_meta,
                behavior=behavior,
                address_snapshot=address_snapshot,
                pool_snapshot=pool_snapshot,
                token_snapshot=token_snapshot,
            ),
            archive_status=archive_status,
            archive_ts=archive_ts,
        )
        self._archive_case_update(case_result, event=event, archive_status=archive_status, archive_ts=archive_ts)

        address_score = self.address_scorer.score(watch_meta, address_snapshot, behavior)
        token_score = self.token_scorer.score(event, token_snapshot)

        gate_decision = self.quality_gate.evaluate(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
            address_score=address_score,
            token_score=token_score,
            address_snapshot=address_snapshot,
            token_snapshot=token_snapshot,
        )
        self._record_pricing_runtime_stats(event, watch_meta, gate_decision)
        self._apply_lp_gate_runtime_metadata(
            event=event,
            gate_metrics=gate_decision.metrics,
        )
        if not gate_decision.passed:
            event.delivery_class = "drop"
            event.delivery_reason = gate_decision.reason
            self._apply_silent_reason(
                event=event,
                stage="gate",
                reason_code=gate_decision.reason,
                reason_detail=gate_decision.reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
            )
            self._archive_delivery_audit(
                event=event,
                signal=None,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="gate",
                gate_reason=gate_decision.reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        gate_decision.metrics["token_context_score"] = self._token_context_score(
            base_token_score=float(token_score.get("score") or 0.0),
            gate_metrics=gate_decision.metrics,
        )
        if self._is_lp_event(event=event):
            event.metadata["gate_passed_at"] = int(time.time())
        runtime_adjacent_allowed, runtime_adjacent_gate_result = self._passes_runtime_adjacent_execution_gate(
            event=event,
            watch_meta=watch_meta,
            behavior_case=behavior_case,
            gate_metrics=gate_decision.metrics,
        )
        if not runtime_adjacent_allowed:
            event.delivery_class = "drop"
            event.delivery_reason = "runtime_adjacent_execution_below_threshold"
            self._apply_silent_reason(
                event=event,
                stage="adjacent_watch_gate",
                reason_code="runtime_adjacent_execution_below_threshold",
                reason_detail="runtime_adjacent_execution_below_threshold",
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
            )
            self._archive_delivery_audit(
                event=event,
                signal=None,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="adjacent_watch_gate",
                gate_reason="runtime_adjacent_execution_below_threshold",
                archive_status=archive_status,
                archive_ts=archive_ts,
                audit_extras=runtime_adjacent_gate_result,
            )
            return None

        signal = self.strategy_engine.decide(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
            address_score=address_score,
            token_score=token_score,
            gate_metrics=gate_decision.metrics,
        )
        if not signal:
            strategy_reject_reason = str(
                event.metadata.get("strategy_reject_reason")
                or "strategy_rejected"
            )
            event.delivery_class = "drop"
            event.delivery_reason = strategy_reject_reason
            self._apply_silent_reason(
                event=event,
                stage="strategy",
                reason_code=strategy_reject_reason,
                reason_detail=strategy_reject_reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
            )
            self._archive_delivery_audit(
                event=event,
                signal=None,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="strategy",
                gate_reason=strategy_reject_reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        cooldown_key = str(
            gate_decision.metrics.get("cooldown_key")
            or self.state_manager.get_cooldown_key(event, intent_type=event.intent_type)
        )
        cooldown_sec = int(gate_decision.metrics.get("cooldown_sec") or self.quality_gate.cooldown_sec)
        now_ts = int(event.ts)
        if not self.state_manager.can_emit_signal_by_key(cooldown_key, now_ts, cooldown_sec):
            last_signal_ts = self.state_manager.get_last_signal_ts_by_key(cooldown_key)
            event.delivery_class = "drop"
            event.delivery_reason = "cooldown_suppressed"
            signal.delivery_class = "drop"
            signal.delivery_reason = "cooldown_suppressed"
            self._apply_cooldown_state(
                event=event,
                signal=signal,
                allowed=False,
                reason="cooldown_suppressed",
            )
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="cooldown",
                reason_code="cooldown_suppressed",
                reason_detail="cooldown_suppressed",
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                cooldown_allowed=False,
            )
            self._archive_signal_state(
                signal,
                event,
                archive_status,
                archive_ts,
                sent_to_telegram=False,
                delivery_decision="cooldown_suppressed",
            )
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="cooldown",
                gate_reason="cooldown_suppressed",
                archive_status=archive_status,
                archive_ts=archive_ts,
                audit_extras={
                    "cooldown_key": cooldown_key,
                    "now_ts": now_ts,
                    "last_signal_ts": int(last_signal_ts) if last_signal_ts is not None else None,
                    "cooldown_sec": cooldown_sec,
                },
            )
            return None
        self._apply_cooldown_state(
            event=event,
            signal=signal,
            allowed=True,
            reason="cooldown_allowed",
        )

        signal.archive_ts = archive_ts
        signal.signal_id = self._build_signal_id(signal, event)
        signal.event_id = event.event_id
        signal.case_id = event.case_id
        signal.parent_case_id = event.parent_case_id
        signal.followup_stage = event.followup_stage
        signal.followup_status = event.followup_status
        signal.intent_type = event.intent_type
        signal.intent_stage = event.intent_stage
        signal.confirmation_score = float(event.confirmation_score or 0.0)
        signal.information_level = confirmed_intent["information_level"]
        signal.abnormal_ratio = self._abnormal_ratio(address_snapshot, event)
        signal.pricing_confidence = float(event.pricing_confidence or 0.0)
        signal.cooldown_key = cooldown_key
        signal.base_token_score = float(token_score.get("score") or 0.0)
        signal.token_context_score = float(gate_decision.metrics.get("token_context_score") or signal.base_token_score)
        signal.metadata.setdefault("pricing", pricing)
        signal.metadata["intent"] = confirmed_intent
        signal.metadata["watch_meta"] = event.metadata.get("watch_meta", {})
        signal.metadata["confirmation"] = {
            "intent_stage": event.intent_stage,
            "confirmation_score": float(event.confirmation_score or 0.0),
            "intent_evidence": list(event.intent_evidence or []),
        }
        signal.metadata.update({
            "listener_rpc_mode": str(event.metadata.get("listener_rpc_mode") or ""),
            "listener_block_fetch_mode": str(event.metadata.get("listener_block_fetch_mode") or ""),
            "listener_block_fetch_reason": str(event.metadata.get("listener_block_fetch_reason") or ""),
            "listener_block_get_logs_request_count": int(
                event.metadata.get("listener_block_get_logs_request_count") or 0
            ),
            "listener_block_topic_chunk_count": int(
                event.metadata.get("listener_block_topic_chunk_count") or 0
            ),
            "listener_block_monitored_address_count": int(
                event.metadata.get("listener_block_monitored_address_count") or 0
            ),
            "listener_block_lp_secondary_scan_used": bool(
                event.metadata.get("listener_block_lp_secondary_scan_used")
            ),
            "listener_block_bloom_prefilter_used": bool(
                event.metadata.get("listener_block_bloom_prefilter_used")
            ),
            "listener_block_bloom_skipped_get_logs_count": int(
                event.metadata.get("listener_block_bloom_skipped_get_logs_count") or 0
            ),
            "listener_block_bloom_transfer_possible": bool(
                event.metadata.get("listener_block_bloom_transfer_possible")
                if "listener_block_bloom_transfer_possible" in event.metadata
                else True
            ),
            "listener_block_bloom_address_possible_count": int(
                event.metadata.get("listener_block_bloom_address_possible_count") or 0
            ),
            "listener_runtime_adjacent_core_count": int(
                event.metadata.get("listener_runtime_adjacent_core_count") or 0
            ),
            "listener_runtime_adjacent_secondary_count": int(
                event.metadata.get("listener_runtime_adjacent_secondary_count") or 0
            ),
            "listener_runtime_adjacent_secondary_scan_used": bool(
                event.metadata.get("listener_runtime_adjacent_secondary_scan_used")
            ),
            "listener_runtime_adjacent_secondary_skipped_count": int(
                event.metadata.get("listener_runtime_adjacent_secondary_skipped_count") or 0
            ),
            "listener_block_lp_primary_trend_scan_used": bool(
                event.metadata.get("listener_block_lp_primary_trend_scan_used")
            ),
            "listener_block_lp_extended_scan_used": bool(
                event.metadata.get("listener_block_lp_extended_scan_used")
            ),
            "listener_block_lp_primary_trend_pool_count": int(
                event.metadata.get("listener_block_lp_primary_trend_pool_count") or 0
            ),
            "listener_block_lp_extended_pool_count": int(
                event.metadata.get("listener_block_lp_extended_pool_count") or 0
            ),
            "listener_block_get_logs_primary_side_count": int(
                event.metadata.get("listener_block_get_logs_primary_side_count") or 0
            ),
            "listener_block_get_logs_secondary_side_count": int(
                event.metadata.get("listener_block_get_logs_secondary_side_count") or 0
            ),
            "listener_block_get_logs_secondary_side_skipped_count": int(
                event.metadata.get("listener_block_get_logs_secondary_side_skipped_count") or 0
            ),
            "listener_block_get_logs_empty_response_count": int(
                event.metadata.get("listener_block_get_logs_empty_response_count") or 0
            ),
            "low_cu_mode_enabled": bool(event.metadata.get("low_cu_mode_enabled")),
            "low_cu_mode_lp_secondary_only": bool(
                event.metadata.get("low_cu_mode_lp_secondary_only")
            ),
            "low_cu_mode_poll_interval_sec": float(
                event.metadata.get("low_cu_mode_poll_interval_sec") or 0.0
            ),
            "first_chain_seen_at": int(event.metadata.get("first_chain_seen_at") or 0),
            "parsed_at": int(event.metadata.get("parsed_at") or 0),
            "gate_passed_at": int(event.metadata.get("gate_passed_at") or 0),
            "lp_scan_path": str(event.metadata.get("lp_scan_path") or ""),
            "lp_promoted_fastlane": bool(event.metadata.get("lp_promoted_fastlane")),
            "lp_promote_reason": str(event.metadata.get("lp_promote_reason") or ""),
            "lp_detect_latency_ms": int(event.metadata.get("lp_detect_latency_ms") or 0),
            "lp_block_age_s": float(event.metadata.get("lp_block_age_s") or 0.0),
            "pool_price_move_before_alert_30s": float(event.metadata.get("pool_price_move_before_alert_30s") or 0.0),
            "pool_price_move_before_alert_60s": float(event.metadata.get("pool_price_move_before_alert_60s") or 0.0),
            "lp_adjacent_noise_skipped_in_listener": bool(event.metadata.get("lp_adjacent_noise_skipped_in_listener")),
            "lp_adjacent_noise_listener_reason": str(event.metadata.get("lp_adjacent_noise_listener_reason") or ""),
            "lp_adjacent_noise_listener_confidence": float(event.metadata.get("lp_adjacent_noise_listener_confidence") or 0.0),
            "lp_adjacent_noise_listener_source_signals": list(event.metadata.get("lp_adjacent_noise_listener_source_signals") or []),
            "lp_adjacent_noise_rule_version": str(event.metadata.get("lp_adjacent_noise_rule_version") or ""),
            "lp_adjacent_noise_decision_stage": str(event.metadata.get("lp_adjacent_noise_decision_stage") or ""),
            "lp_adjacent_noise_filtered": bool(event.metadata.get("lp_adjacent_noise_filtered")),
            "lp_adjacent_noise_reason": str(event.metadata.get("lp_adjacent_noise_reason") or ""),
            "lp_adjacent_noise_confidence": float(event.metadata.get("lp_adjacent_noise_confidence") or 0.0),
            "lp_adjacent_noise_source_signals": list(event.metadata.get("lp_adjacent_noise_source_signals") or []),
            "lp_adjacent_noise_context_used": list(event.metadata.get("lp_adjacent_noise_context_used") or []),
            "lp_adjacent_noise_runtime_context_present": bool(
                event.metadata.get("lp_adjacent_noise_runtime_context_present")
            ),
            "lp_adjacent_noise_downstream_context_present": bool(
                event.metadata.get("lp_adjacent_noise_downstream_context_present")
            ),
        })
        self._apply_lp_gate_runtime_metadata(
            event=event,
            signal=signal,
            gate_metrics=gate_decision.metrics,
        )
        if behavior_case is not None:
            updated_case = self._attach_signal_to_case(behavior_case, signal)
            if updated_case is not None:
                behavior_case = updated_case
                event.followup_stage = behavior_case.stage
                event.followup_status = behavior_case.status
                event.metadata["case_summary"] = behavior_case.summary
                event.metadata["case"] = {
                    "case_id": behavior_case.case_id,
                    "status": behavior_case.status,
                    "stage": behavior_case.stage,
                    "summary": behavior_case.summary,
                }
                signal.followup_stage = behavior_case.stage
                signal.followup_status = behavior_case.status
                signal.metadata["case"] = {
                    "case_id": behavior_case.case_id,
                    "status": behavior_case.status,
                    "stage": behavior_case.stage,
                    "summary": behavior_case.summary,
                }
                self._apply_exchange_followup_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )
                self._apply_downstream_followup_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )
                self._apply_smart_money_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )
                self._apply_liquidation_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )

        self._sync_exchange_followup_to_signal(event, signal, behavior_case)
        self._sync_downstream_followup_to_signal(event, signal, behavior_case)
        self._sync_smart_money_case_to_signal(event, signal, behavior_case)
        self._sync_liquidation_to_signal(event, signal, behavior_case)

        interpretation = self.signal_interpreter.interpret(
            event=event,
            signal=signal,
            behavior=behavior,
            watch_meta=watch_meta,
            watch_context=watch_context,
            address_snapshot=address_snapshot,
            token_snapshot=token_snapshot,
            gate_metrics=gate_decision.metrics,
        )
        self._apply_lp_prealert_diagnostics(
            event,
            signal,
            gate_metrics=gate_decision.metrics,
        )
        if not interpretation.should_notify:
            event.delivery_class = "drop"
            event.delivery_reason = interpretation.reason
            signal.delivery_class = "drop"
            signal.delivery_reason = interpretation.reason
            self._apply_lp_prealert_diagnostics(
                event,
                signal,
                gate_metrics=gate_decision.metrics,
                delivery_allowed=False,
                delivery_block_reason=str(interpretation.reason or "interpreter_suppressed"),
            )
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="interpreter",
                reason_code=interpretation.reason,
                reason_detail=interpretation.reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                cooldown_allowed=True,
            )
            self._archive_signal_state(
                signal,
                event,
                archive_status,
                archive_ts,
                sent_to_telegram=False,
                delivery_decision=str(interpretation.reason or "interpreter_suppressed"),
            )
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="interpreter",
                gate_reason=interpretation.reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        self._apply_lp_productization_context(
            event=event,
            signal=signal,
            gate_metrics=gate_decision.metrics,
            watch_meta=watch_meta,
        )

        delivery_class, delivery_reason = self.strategy_engine.classify_delivery(
            event=event,
            signal=signal,
            watch_meta=watch_meta,
            gate_metrics=gate_decision.metrics,
            behavior_case=behavior_case,
        )
        self._apply_lp_prealert_diagnostics(
            event,
            signal,
            gate_metrics=gate_decision.metrics,
            delivery_allowed=bool(delivery_class != "drop"),
            delivery_block_reason="" if delivery_class != "drop" else str(delivery_reason or "strategy_blocked"),
        )
        self._promote_lp_fastlane_if_needed(event, signal, gate_decision.metrics)
        self._record_lp_outcome_runtime(event, signal)
        if delivery_class == "drop":
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="strategy",
                reason_code=delivery_reason,
                reason_detail=delivery_reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                cooldown_allowed=True,
            )
            self._archive_non_primary_signal(
                event=event,
                signal=signal,
                reason=delivery_reason,
                gate_metrics=gate_decision.metrics,
                behavior=behavior,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None
        should_send = self._should_emit_delivery_notification(event, signal)
        if not should_send:
            delivery_policy_reason = str(
                event.metadata.get("delivery_policy_reason")
                or signal.metadata.get("delivery_policy_reason")
                or delivery_reason
                or "delivery_policy_blocked"
            )
            event.delivery_class = "drop"
            event.delivery_reason = delivery_policy_reason
            signal.delivery_class = "drop"
            signal.delivery_reason = delivery_policy_reason
            self._apply_lp_prealert_diagnostics(
                event,
                signal,
                gate_metrics=gate_decision.metrics,
                delivery_allowed=False,
                delivery_block_reason=delivery_policy_reason,
            )
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="delivery_policy",
                reason_code=delivery_policy_reason,
                reason_detail=delivery_policy_reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                delivery_policy_allowed=False,
                cooldown_allowed=True,
                would_have_been_delivery_class=delivery_class,
            )
            self._archive_non_primary_signal(
                event=event,
                signal=signal,
                reason=delivery_policy_reason,
                gate_metrics=gate_decision.metrics,
                behavior=behavior,
                archive_status=archive_status,
                archive_ts=archive_ts,
                stage="delivery_policy",
            )
            return None

        if not self._apply_case_notification_control(event, signal, behavior_case):
            event.delivery_class = "drop"
            event.delivery_reason = str(event.metadata.get("case_notification_reason") or "case_notification_suppressed")
            signal.delivery_class = "drop"
            signal.delivery_reason = event.delivery_reason
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="case_notification",
                reason_code=event.delivery_reason,
                reason_detail=event.delivery_reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                delivery_policy_allowed=bool(event.metadata.get("delivery_policy_allowed")),
                cooldown_allowed=True,
            )
            self._archive_signal_state(
                signal,
                event,
                archive_status,
                archive_ts,
                sent_to_telegram=False,
                delivery_decision=str(event.delivery_reason or "case_notification_suppressed"),
            )
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="case_notification",
                gate_reason=event.delivery_reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            self._archive_case_decision_followup(
                behavior_case=behavior_case,
                event=event,
                signal=signal,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        downstream_impact_allowed, downstream_impact_reason = self._passes_downstream_impact_gate(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
            gate_metrics=gate_decision.metrics,
        )
        self._apply_downstream_impact_gate_state(
            event=event,
            signal=signal,
            allowed=bool(downstream_impact_allowed),
            reason=str(downstream_impact_reason or ""),
        )
        if not downstream_impact_allowed:
            impact_reason = str(downstream_impact_reason or "downstream_impact_gate_rejected")
            event.delivery_class = "drop"
            event.delivery_reason = impact_reason
            signal.delivery_class = "drop"
            signal.delivery_reason = impact_reason
            rejection_payload = {
                "pending_case_notification": False,
                "pending_case_notification_stage": "",
                "pending_case_notification_reason": "",
                "pending_case_notification_case_id": "",
                "pending_case_notification_case_family": "",
                "downstream_impact_gate_reason": impact_reason,
                "downstream_observation_reason": impact_reason,
            }
            event.metadata.update(rejection_payload)
            signal.metadata.update(rejection_payload)
            signal.context.update(rejection_payload)
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="impact_gate",
                reason_code=impact_reason,
                reason_detail=impact_reason,
                behavior_case=behavior_case,
                gate_metrics=gate_decision.metrics,
                delivery_policy_allowed=bool(event.metadata.get("delivery_policy_allowed")),
                impact_gate_allowed=False,
                cooldown_allowed=True,
            )
            self._archive_signal_state(
                signal,
                event,
                archive_status,
                archive_ts,
                sent_to_telegram=False,
                delivery_decision=str(impact_reason or "downstream_impact_gate_rejected"),
            )
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="impact_gate",
                gate_reason=impact_reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
                audit_extras={
                    "downstream_impact_gate_reason": impact_reason,
                },
            )
            self._archive_case_decision_followup(
                behavior_case=behavior_case,
                event=event,
                signal=signal,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        if self._is_lp_event(event=event):
            final_output_allowed = (
                event.metadata.get("final_trading_output_allowed")
                if event.metadata.get("final_trading_output_allowed") is not None
                else signal.metadata.get("final_trading_output_allowed")
                if signal.metadata.get("final_trading_output_allowed") is not None
                else signal.context.get("final_trading_output_allowed")
            )
            telegram_should_send = bool(
                final_output_allowed
                if final_output_allowed is not None
                else event.metadata.get("telegram_should_send")
                if event.metadata.get("telegram_should_send") is not None
                else signal.metadata.get("telegram_should_send")
                if signal.metadata.get("telegram_should_send") is not None
                else signal.context.get("telegram_should_send")
            )
            gate_allowed, gate_reason = self._enforce_lp_final_output_gate(
                event,
                signal,
                telegram_should_send=telegram_should_send,
            )
            if telegram_should_send and not gate_allowed:
                telegram_should_send = False
                if gate_reason:
                    event.metadata["delivery_block_reason"] = gate_reason
                    signal.metadata["delivery_block_reason"] = gate_reason
                    signal.context["delivery_block_reason"] = gate_reason
            if not telegram_should_send:
                suppression_reason = str(
                    event.metadata.get("opportunity_gate_failure_reason")
                    or signal.metadata.get("opportunity_gate_failure_reason")
                    or signal.context.get("opportunity_gate_failure_reason")
                    or event.metadata.get("telegram_suppression_reason")
                    or signal.metadata.get("telegram_suppression_reason")
                    or signal.context.get("telegram_suppression_reason")
                    or "asset_market_state_suppressed"
                )
                event.delivery_class = "drop"
                event.delivery_reason = suppression_reason
                signal.delivery_class = "drop"
                signal.delivery_reason = suppression_reason
                self._apply_lp_prealert_diagnostics(
                    event,
                    signal,
                    gate_metrics=gate_decision.metrics,
                    delivery_allowed=False,
                    delivery_block_reason=suppression_reason,
                )
                self._apply_silent_reason(
                    event=event,
                    signal=signal,
                    stage="asset_market_state",
                    reason_code=suppression_reason,
                    reason_detail=suppression_reason,
                    behavior_case=behavior_case,
                    gate_metrics=gate_decision.metrics,
                    delivery_policy_allowed=bool(event.metadata.get("delivery_policy_allowed")),
                    impact_gate_allowed=event.metadata.get("downstream_impact_gate_allowed"),
                    cooldown_allowed=True,
                    would_have_been_delivery_class=delivery_class,
                )
                self._archive_signal_state(
                    signal,
                    event,
                    archive_status,
                    archive_ts,
                    sent_to_telegram=False,
                    delivery_decision=suppression_reason,
                )
                self._archive_delivery_audit(
                    event=event,
                    signal=signal,
                    behavior=behavior,
                    gate_metrics=gate_decision.metrics,
                    stage="asset_market_state",
                    gate_reason=suppression_reason,
                    archive_status=archive_status,
                    archive_ts=archive_ts,
                    audit_extras={
                        "telegram_should_send": False,
                        "telegram_suppression_reason": suppression_reason,
                        "telegram_update_kind": str(
                            event.metadata.get("telegram_update_kind")
                            or signal.metadata.get("telegram_update_kind")
                            or signal.context.get("telegram_update_kind")
                            or "suppressed"
                        ),
                    },
                )
                if behavior_case is not None:
                    self._archive_case_decision_followup(
                        behavior_case=behavior_case,
                        event=event,
                        signal=signal,
                        archive_status=archive_status,
                        archive_ts=archive_ts,
                    )
                return None

        self._mark_entered_notifier()
        self._archive_delivery_audit(
            event=event,
            signal=signal,
            behavior=behavior,
            gate_metrics=gate_decision.metrics,
            stage="notifier",
            gate_reason="passed",
            archive_status=archive_status,
            archive_ts=archive_ts,
        )
        if behavior_case is not None:
            self._archive_case_followup(behavior_case, signal, archive_status, archive_ts)

        return {
            "event": event,
            "signal": signal,
            "behavior": behavior,
            "intent": confirmed_intent,
            "pricing": pricing,
            "address_score": address_score,
            "token_score": token_score,
            "gate": gate_decision,
            "watch_meta": watch_meta,
            "watch_context": watch_context,
            "address_snapshot": address_snapshot,
            "token_snapshot": token_snapshot,
            "interpretation": interpretation,
            "case": behavior_case,
            "address_intel_update": address_intel_update,
            "archive_status": archive_status,
        }

    def _normalize_parsed(self, parsed: dict, watch_context: dict, raw_item: dict | None = None) -> dict:
        raw_item = raw_item or {}
        if parsed.get("watch_address"):
            parsed["watch_address"] = str(parsed["watch_address"]).lower()
        elif watch_context.get("watch_address"):
            parsed["watch_address"] = str(watch_context["watch_address"]).lower()
        elif watch_context.get("watch_from"):
            parsed["watch_address"] = str(watch_context["watch_from"]).lower()
        else:
            parsed["watch_address"] = ""

        if not parsed.get("direction") and watch_context.get("direction"):
            parsed["direction"] = watch_context["direction"]

        parsed["timestamp"] = int(raw_item.get("ingest_ts") or time.time())
        parsed["ingest_ts"] = int(raw_item.get("ingest_ts") or parsed["timestamp"])
        parsed["source_kind"] = str(raw_item.get("source_kind") or "")
        parsed["touched_watch_addresses"] = list(raw_item.get("touched_watch_addresses") or [])
        parsed["touched_lp_pools"] = list(raw_item.get("touched_lp_pools") or [])
        parsed["raw_log_count"] = int(raw_item.get("raw_log_count") or 0)
        parsed["participant_addresses"] = list(raw_item.get("participant_addresses") or [])
        parsed["next_hop_addresses"] = list(raw_item.get("next_hop_addresses") or [])
        parsed["token_addresses"] = list(raw_item.get("token_addresses") or [])
        parsed["monitor_type"] = str(raw_item.get("monitor_type") or parsed.get("monitor_type") or "watch_address")
        parsed["tx_pool_hit_count"] = int(raw_item.get("tx_pool_hit_count") or 0)
        parsed["touched_lp_pool_count"] = int(raw_item.get("touched_lp_pool_count") or 0)
        parsed["pool_transfer_count_by_pool"] = dict(raw_item.get("pool_transfer_count_by_pool") or {})
        parsed["pool_candidate_weight"] = float(raw_item.get("pool_candidate_weight") or 0.0)
        parsed["replay_source"] = str(raw_item.get("replay_source") or "")
        parsed["listener_rpc_mode"] = str(raw_item.get("listener_rpc_mode") or "")
        parsed["listener_block_fetch_mode"] = str(raw_item.get("listener_block_fetch_mode") or "")
        parsed["listener_block_fetch_reason"] = str(raw_item.get("listener_block_fetch_reason") or "")
        parsed["listener_block_get_logs_request_count"] = int(
            raw_item.get("listener_block_get_logs_request_count") or 0
        )
        parsed["listener_block_topic_chunk_count"] = int(
            raw_item.get("listener_block_topic_chunk_count") or 0
        )
        parsed["listener_block_monitored_address_count"] = int(
            raw_item.get("listener_block_monitored_address_count") or 0
        )
        parsed["listener_block_lp_secondary_scan_used"] = bool(
            raw_item.get("listener_block_lp_secondary_scan_used")
        )
        parsed["listener_block_bloom_prefilter_used"] = bool(
            raw_item.get("listener_block_bloom_prefilter_used")
        )
        parsed["listener_block_bloom_skipped_get_logs_count"] = int(
            raw_item.get("listener_block_bloom_skipped_get_logs_count") or 0
        )
        parsed["listener_block_bloom_transfer_possible"] = bool(
            raw_item.get("listener_block_bloom_transfer_possible")
            if "listener_block_bloom_transfer_possible" in raw_item
            else True
        )
        parsed["listener_block_bloom_address_possible_count"] = int(
            raw_item.get("listener_block_bloom_address_possible_count") or 0
        )
        parsed["listener_runtime_adjacent_core_count"] = int(
            raw_item.get("listener_runtime_adjacent_core_count") or 0
        )
        parsed["listener_runtime_adjacent_secondary_count"] = int(
            raw_item.get("listener_runtime_adjacent_secondary_count") or 0
        )
        parsed["listener_runtime_adjacent_secondary_scan_used"] = bool(
            raw_item.get("listener_runtime_adjacent_secondary_scan_used")
        )
        parsed["listener_runtime_adjacent_secondary_skipped_count"] = int(
            raw_item.get("listener_runtime_adjacent_secondary_skipped_count") or 0
        )
        parsed["listener_block_lp_primary_trend_scan_used"] = bool(
            raw_item.get("listener_block_lp_primary_trend_scan_used")
        )
        parsed["listener_block_lp_extended_scan_used"] = bool(
            raw_item.get("listener_block_lp_extended_scan_used")
        )
        parsed["listener_block_lp_primary_trend_pool_count"] = int(
            raw_item.get("listener_block_lp_primary_trend_pool_count") or 0
        )
        parsed["listener_block_lp_extended_pool_count"] = int(
            raw_item.get("listener_block_lp_extended_pool_count") or 0
        )
        parsed["listener_block_get_logs_primary_side_count"] = int(
            raw_item.get("listener_block_get_logs_primary_side_count") or 0
        )
        parsed["listener_block_get_logs_secondary_side_count"] = int(
            raw_item.get("listener_block_get_logs_secondary_side_count") or 0
        )
        parsed["listener_block_get_logs_secondary_side_skipped_count"] = int(
            raw_item.get("listener_block_get_logs_secondary_side_skipped_count") or 0
        )
        parsed["listener_block_get_logs_empty_response_count"] = int(
            raw_item.get("listener_block_get_logs_empty_response_count") or 0
        )
        parsed["low_cu_mode_enabled"] = bool(raw_item.get("low_cu_mode_enabled"))
        parsed["low_cu_mode_lp_secondary_only"] = bool(
            raw_item.get("low_cu_mode_lp_secondary_only")
        )
        parsed["low_cu_mode_poll_interval_sec"] = float(
            raw_item.get("low_cu_mode_poll_interval_sec") or 0.0
        )
        parsed["lp_adjacent_noise_skipped_in_listener"] = bool(raw_item.get("lp_adjacent_noise_skipped_in_listener"))
        parsed["lp_adjacent_noise_listener_reason"] = str(raw_item.get("lp_adjacent_noise_listener_reason") or "")
        parsed["lp_adjacent_noise_listener_confidence"] = float(raw_item.get("lp_adjacent_noise_listener_confidence") or 0.0)
        parsed["lp_adjacent_noise_listener_source_signals"] = list(raw_item.get("lp_adjacent_noise_listener_source_signals") or [])
        parsed["lp_adjacent_noise_rule_version"] = str(raw_item.get("lp_adjacent_noise_rule_version") or "")
        parsed["lp_adjacent_noise_decision_stage"] = str(raw_item.get("lp_adjacent_noise_decision_stage") or "")
        parsed["lp_adjacent_noise_filtered"] = bool(raw_item.get("lp_adjacent_noise_filtered"))
        parsed["lp_adjacent_noise_reason"] = str(raw_item.get("lp_adjacent_noise_reason") or "")
        parsed["lp_adjacent_noise_confidence"] = float(raw_item.get("lp_adjacent_noise_confidence") or 0.0)
        parsed["lp_adjacent_noise_source_signals"] = list(raw_item.get("lp_adjacent_noise_source_signals") or [])
        parsed["lp_adjacent_noise_context_used"] = list(raw_item.get("lp_adjacent_noise_context_used") or [])
        parsed["lp_adjacent_noise_runtime_context_present"] = bool(
            raw_item.get("lp_adjacent_noise_runtime_context_present")
        )
        parsed["lp_adjacent_noise_downstream_context_present"] = bool(
            raw_item.get("lp_adjacent_noise_downstream_context_present")
        )
        return parsed

    def _lp_adjacent_noise_fallback_decision(
        self,
        *,
        raw_item: dict | None = None,
        parsed: dict | None = None,
        watch_meta: dict | None = None,
    ) -> dict:
        payload = dict(parsed or raw_item or {})
        watch_address = str(payload.get("watch_address") or "").lower()
        resolved_meta = watch_meta or get_address_meta(watch_address)
        payload["watch_address"] = watch_address
        payload["strategy_role"] = str(
            (resolved_meta or {}).get("strategy_role") or payload.get("strategy_role") or ""
        )
        payload["watch_meta_source"] = str(
            (resolved_meta or {}).get("watch_meta_source")
            or payload.get("watch_meta_source")
            or ""
        )
        payload["strategy_hint"] = str(
            (resolved_meta or {}).get("strategy_hint")
            or payload.get("strategy_hint")
            or ""
        )
        payload["runtime_adjacent_watch"] = bool(
            (resolved_meta or {}).get("runtime_adjacent_watch")
            or payload.get("runtime_adjacent_watch")
        )
        payload["runtime_state"] = str(
            (resolved_meta or {}).get("runtime_state")
            or payload.get("runtime_state")
            or ""
        )
        payload["anchor_watch_address"] = str(
            (resolved_meta or {}).get("anchor_watch_address")
            or payload.get("anchor_watch_address")
            or ""
        )
        payload["downstream_case_id"] = str(
            (resolved_meta or {}).get("downstream_case_id")
            or payload.get("downstream_case_id")
            or ""
        )
        decision = lp_adjacent_noise_core_decision(
            payload,
            stage=LP_ADJACENT_NOISE_STAGE_PIPELINE,
            watch_addresses=WATCH_ADDRESSES,
        )
        if raw_item is not None and parsed is None and watch_meta is None and decision.get("is_noise"):
            early_strong_reasons = {
                "adjacent_watch_overlaps_active_lp_pool",
                "adjacent_watch_watch_meta_lp_pool",
                "adjacent_watch_listener_high_confidence_lp_noise",
            }
            if str(decision.get("reason") or "") not in early_strong_reasons:
                decision["is_noise"] = False
        return decision

    def _lp_adjacent_noise_metadata_from_decision(self, decision: dict) -> dict:
        return {
            "lp_adjacent_noise_rule_version": str(decision.get("rule_version") or ""),
            "lp_adjacent_noise_decision_stage": str(decision.get("decision_stage") or ""),
            "lp_adjacent_noise_filtered": bool(decision.get("is_noise")),
            "lp_adjacent_noise_reason": str(decision.get("reason") or ""),
            "lp_adjacent_noise_confidence": round(float(decision.get("confidence") or 0.0), 3),
            "lp_adjacent_noise_source_signals": list(decision.get("source_signals") or []),
            "lp_adjacent_noise_context_used": list(decision.get("context_used") or []),
            "lp_adjacent_noise_runtime_context_present": bool(
                decision.get("runtime_context_present")
            ),
            "lp_adjacent_noise_downstream_context_present": bool(
                decision.get("downstream_context_present")
            ),
        }

    def _is_lp_adjacent_noise_candidate(
        self,
        raw_item: dict | None = None,
        parsed: dict | None = None,
        watch_meta: dict | None = None,
    ) -> bool:
        decision = self._lp_adjacent_noise_fallback_decision(
            raw_item=raw_item,
            parsed=parsed,
            watch_meta=watch_meta,
        )
        return bool(decision.get("is_noise"))

    def _archive_lp_adjacent_noise_audit(
        self,
        raw_item: dict | None = None,
        parsed: dict | None = None,
        watch_meta: dict | None = None,
        archive_status: dict | None = None,
        archive_ts: int | None = None,
    ) -> None:
        archive_status = archive_status or {}
        payload = dict(parsed or raw_item or {})
        watch_address = str(payload.get("watch_address") or "").lower()
        resolved_meta = watch_meta or get_address_meta(watch_address)
        decision = self._lp_adjacent_noise_fallback_decision(
            raw_item=raw_item,
            parsed=parsed,
            watch_meta=resolved_meta,
        )
        decision_metadata = self._lp_adjacent_noise_metadata_from_decision(decision)
        self._archive_prefilter_delivery_audit(
            parsed=payload,
            watch_context=None,
            watch_meta=resolved_meta,
            archive_status=archive_status,
            archive_ts=int(archive_ts or time.time()),
            gate_reason="lp_adjacent_noise_filtered",
            audit_extras={
                **decision_metadata,
                "lp_adjacent_noise_filtered": True,
                "lp_adjacent_noise_reason": str(
                    decision.get("reason") or "lp_adjacent_noise_filtered"
                ),
                "lp_adjacent_noise_skipped_in_listener": bool(payload.get("lp_adjacent_noise_skipped_in_listener")),
                "lp_adjacent_noise_listener_reason": str(payload.get("lp_adjacent_noise_listener_reason") or ""),
                "lp_adjacent_noise_listener_confidence": float(payload.get("lp_adjacent_noise_listener_confidence") or 0.0),
                "lp_adjacent_noise_listener_source_signals": list(payload.get("lp_adjacent_noise_listener_source_signals") or []),
                "monitor_type": str(payload.get("monitor_type") or "adjacent_watch"),
                "strategy_role": str((resolved_meta or {}).get("strategy_role") or "lp_pool"),
                "role_group": str(strategy_role_group((resolved_meta or {}).get("strategy_role") or "lp_pool")),
                "watch_address": watch_address,
            },
        )

    def _lp_burst_direction(self, event: Event) -> str:
        if str(event.strategy_role or "") != "lp_pool":
            return ""
        intent_type = str(event.intent_type or "")
        if intent_type == "pool_buy_pressure":
            return "buy_pressure"
        if intent_type == "pool_sell_pressure":
            return "sell_pressure"
        return ""

    def _apply_lp_burst_state(self, event: Event) -> dict:
        direction = self._lp_burst_direction(event)
        if not direction:
            return {}

        burst_state = self.state_manager.get_lp_burst_snapshot(
            pool_address=str(event.address or "").lower(),
            direction=direction,
            now_ts=int(event.ts or time.time()),
        )
        payload = {
            **burst_state,
            "lp_burst_fastlane_applied": bool(event.metadata.get("lp_burst_fastlane_applied")),
            "lp_burst_fastlane_reason": str(event.metadata.get("lp_burst_fastlane_reason") or ""),
            "lp_burst_delivery_class": str(event.metadata.get("lp_burst_delivery_class") or ""),
        }
        event.metadata["lp_burst"] = dict(burst_state)
        event.metadata.update(payload)
        return payload

    def _apply_lp_gate_runtime_metadata(
        self,
        event: Event,
        signal=None,
        gate_metrics: dict | None = None,
    ) -> dict:
        gate_metrics = gate_metrics or {}
        if not self._is_lp_event(event=event):
            return {}

        existing = event.metadata or {}

        def _first_value(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        payload = {
            "lp_observe_exception_applied": bool(_first_value(gate_metrics.get("lp_observe_exception_applied"), existing.get("lp_observe_exception_applied"), False)),
            "lp_observe_exception_reason": str(_first_value(gate_metrics.get("lp_observe_exception_reason"), existing.get("lp_observe_exception_reason"), "") or ""),
            "lp_observe_threshold_ratio": float(_first_value(gate_metrics.get("lp_observe_threshold_ratio"), existing.get("lp_observe_threshold_ratio"), 0.0) or 0.0),
            "lp_observe_below_min_gap": float(_first_value(gate_metrics.get("lp_observe_below_min_gap"), existing.get("lp_observe_below_min_gap"), 0.0) or 0.0),
            "lp_stage_decision": str(_first_value(gate_metrics.get("lp_stage_decision"), existing.get("lp_stage_decision"), "") or ""),
            "lp_reject_reason": str(_first_value(gate_metrics.get("lp_reject_reason"), existing.get("lp_reject_reason"), "") or ""),
            "lp_prealert_candidate": bool(_first_value(gate_metrics.get("lp_prealert_candidate"), existing.get("lp_prealert_candidate"), False)),
            "lp_prealert_applied": bool(_first_value(gate_metrics.get("lp_prealert_applied"), existing.get("lp_prealert_applied"), False)),
            "lp_prealert_reason": str(_first_value(gate_metrics.get("lp_prealert_reason"), existing.get("lp_prealert_reason"), "") or ""),
            "lp_prealert_candidate_reason": str(_first_value(gate_metrics.get("lp_prealert_candidate_reason"), existing.get("lp_prealert_candidate_reason"), "") or ""),
            "lp_prealert_gate_passed": bool(_first_value(gate_metrics.get("lp_prealert_gate_passed"), existing.get("lp_prealert_gate_passed"), False)),
            "lp_prealert_gate_fail_reason": str(_first_value(gate_metrics.get("lp_prealert_gate_fail_reason"), existing.get("lp_prealert_gate_fail_reason"), "") or ""),
            "lp_prealert_delivery_allowed": _first_value(gate_metrics.get("lp_prealert_delivery_allowed"), existing.get("lp_prealert_delivery_allowed"), None),
            "lp_prealert_delivery_block_reason": str(_first_value(gate_metrics.get("lp_prealert_delivery_block_reason"), existing.get("lp_prealert_delivery_block_reason"), "") or ""),
            "lp_prealert_asset_case_preserved": _first_value(gate_metrics.get("lp_prealert_asset_case_preserved"), existing.get("lp_prealert_asset_case_preserved"), None),
            "lp_prealert_stage_overwritten": _first_value(gate_metrics.get("lp_prealert_stage_overwritten"), existing.get("lp_prealert_stage_overwritten"), None),
            "lp_prealert_first_leg": bool(_first_value(gate_metrics.get("lp_prealert_first_leg"), existing.get("lp_prealert_first_leg"), False)),
            "lp_prealert_major_override_used": bool(_first_value(gate_metrics.get("lp_prealert_major_override_used"), existing.get("lp_prealert_major_override_used"), False)),
            "lp_structure_score": float(_first_value(gate_metrics.get("lp_structure_score"), existing.get("lp_structure_score"), 0.0) or 0.0),
            "lp_structure_components": _first_value(gate_metrics.get("lp_structure_components"), existing.get("lp_structure_components"), {}) or {},
            "lp_fastlane_ready": bool(_first_value(gate_metrics.get("lp_fastlane_ready"), existing.get("lp_fastlane_ready"), gate_metrics.get("lp_burst_fastlane_ready"), False)),
            "lp_fastlane_applied": bool(_first_value(gate_metrics.get("lp_fastlane_applied"), existing.get("lp_fastlane_applied"), False)),
            "lp_pool_priority_class": str(_first_value(gate_metrics.get("lp_pool_priority_class"), existing.get("lp_pool_priority_class"), "") or ""),
            "lp_trend_sensitivity_mode": bool(_first_value(gate_metrics.get("lp_trend_sensitivity_mode"), existing.get("lp_trend_sensitivity_mode"), False)),
            "lp_trend_primary_pool": bool(_first_value(gate_metrics.get("lp_trend_primary_pool"), existing.get("lp_trend_primary_pool"), False)),
            "lp_trend_pool_family": str(_first_value(gate_metrics.get("lp_trend_pool_family"), existing.get("lp_trend_pool_family"), "") or ""),
            "lp_trend_base_family": str(_first_value(gate_metrics.get("lp_trend_base_family"), existing.get("lp_trend_base_family"), "") or ""),
            "lp_trend_quote_family": str(_first_value(gate_metrics.get("lp_trend_quote_family"), existing.get("lp_trend_quote_family"), "") or ""),
            "lp_trend_pool_match_mode": str(_first_value(gate_metrics.get("lp_trend_pool_match_mode"), existing.get("lp_trend_pool_match_mode"), "non_trend_pool") or "non_trend_pool"),
            "lp_trend_state": str(_first_value(gate_metrics.get("lp_trend_state"), existing.get("lp_trend_state"), "trend_neutral") or "trend_neutral"),
            "lp_trend_side_bias": str(_first_value(gate_metrics.get("lp_trend_side_bias"), existing.get("lp_trend_side_bias"), "neutral") or "neutral"),
            "lp_trend_continuation_score": float(_first_value(gate_metrics.get("lp_trend_continuation_score"), existing.get("lp_trend_continuation_score"), 0.0) or 0.0),
            "lp_trend_reversal_score": float(_first_value(gate_metrics.get("lp_trend_reversal_score"), existing.get("lp_trend_reversal_score"), 0.0) or 0.0),
            "lp_trend_state_source": str(_first_value(gate_metrics.get("lp_trend_state_source"), existing.get("lp_trend_state_source"), "") or ""),
            "lp_trend_state_window_sec": int(_first_value(gate_metrics.get("lp_trend_state_window_sec"), existing.get("lp_trend_state_window_sec"), 0) or 0),
            "lp_trend_buy_pressure_count": int(_first_value(gate_metrics.get("lp_trend_buy_pressure_count"), existing.get("lp_trend_buy_pressure_count"), 0) or 0),
            "lp_trend_sell_pressure_count": int(_first_value(gate_metrics.get("lp_trend_sell_pressure_count"), existing.get("lp_trend_sell_pressure_count"), 0) or 0),
            "lp_trend_window_total_usd": float(_first_value(gate_metrics.get("lp_trend_window_total_usd"), existing.get("lp_trend_window_total_usd"), 0.0) or 0.0),
            "lp_trend_last_shift_ts": int(_first_value(gate_metrics.get("lp_trend_last_shift_ts"), existing.get("lp_trend_last_shift_ts"), 0) or 0),
            "lp_directional_side": str(_first_value(gate_metrics.get("lp_directional_side"), existing.get("lp_directional_side"), "") or ""),
            "lp_directional_threshold_profile": str(_first_value(gate_metrics.get("lp_directional_threshold_profile"), existing.get("lp_directional_threshold_profile"), "") or ""),
            "lp_fast_exception_profile_name": str(_first_value(gate_metrics.get("lp_fast_exception_profile_name"), existing.get("lp_fast_exception_profile_name"), "") or ""),
            "lp_buy_trend_profile_active": bool(_first_value(gate_metrics.get("lp_buy_trend_profile_active"), existing.get("lp_buy_trend_profile_active"), False)),
            "lp_buy_trend_profile_name": str(_first_value(gate_metrics.get("lp_buy_trend_profile_name"), existing.get("lp_buy_trend_profile_name"), "") or ""),
            "lp_buy_trend_profile_reason": str(_first_value(gate_metrics.get("lp_buy_trend_profile_reason"), existing.get("lp_buy_trend_profile_reason"), "") or ""),
            "lp_fast_exception_applied": bool(_first_value(gate_metrics.get("lp_fast_exception_applied"), existing.get("lp_fast_exception_applied"), False)),
            "lp_fast_exception_reason": str(_first_value(gate_metrics.get("lp_fast_exception_reason"), existing.get("lp_fast_exception_reason"), "") or ""),
            "lp_fast_exception_threshold_ratio": float(_first_value(gate_metrics.get("lp_fast_exception_threshold_ratio"), existing.get("lp_fast_exception_threshold_ratio"), 0.0) or 0.0),
            "lp_fast_exception_usd_gap": float(_first_value(gate_metrics.get("lp_fast_exception_usd_gap"), existing.get("lp_fast_exception_usd_gap"), 0.0) or 0.0),
            "lp_fast_exception_structure_score": float(_first_value(gate_metrics.get("lp_fast_exception_structure_score"), existing.get("lp_fast_exception_structure_score"), 0.0) or 0.0),
            "lp_fast_exception_structure_passed": bool(_first_value(gate_metrics.get("lp_fast_exception_structure_passed"), existing.get("lp_fast_exception_structure_passed"), False)),
            "lp_fast_exception_gate_version": str(_first_value(gate_metrics.get("lp_fast_exception_gate_version"), existing.get("lp_fast_exception_gate_version"), "") or ""),
            "lp_burst_trend_mode": bool(_first_value(gate_metrics.get("lp_burst_trend_mode"), existing.get("lp_burst_trend_mode"), False)),
            "lp_burst_event_count_threshold_used": int(_first_value(gate_metrics.get("lp_burst_event_count_threshold_used"), existing.get("lp_burst_event_count_threshold_used"), 0) or 0),
            "lp_burst_total_usd_threshold_used": float(_first_value(gate_metrics.get("lp_burst_total_usd_threshold_used"), existing.get("lp_burst_total_usd_threshold_used"), 0.0) or 0.0),
            "lp_burst_trend_profile_name": str(_first_value(gate_metrics.get("lp_burst_trend_profile_name"), existing.get("lp_burst_trend_profile_name"), "") or ""),
            "lp_burst_fastlane_applied": bool(_first_value(gate_metrics.get("lp_burst_fastlane_applied"), existing.get("lp_burst_fastlane_applied"), False)),
            "lp_burst_fastlane_reason": str(_first_value(gate_metrics.get("lp_burst_fastlane_reason"), existing.get("lp_burst_fastlane_reason"), "") or ""),
            "lp_burst_window_sec": int(_first_value(gate_metrics.get("lp_burst_window_sec"), existing.get("lp_burst_window_sec"), 0) or 0),
            "lp_burst_event_count": int(_first_value(gate_metrics.get("lp_burst_event_count"), existing.get("lp_burst_event_count"), 0) or 0),
            "lp_burst_total_usd": float(_first_value(gate_metrics.get("lp_burst_total_usd"), existing.get("lp_burst_total_usd"), 0.0) or 0.0),
            "lp_burst_max_single_usd": float(_first_value(gate_metrics.get("lp_burst_max_single_usd"), existing.get("lp_burst_max_single_usd"), 0.0) or 0.0),
            "lp_burst_same_pool_continuity": int(_first_value(gate_metrics.get("lp_burst_same_pool_continuity"), existing.get("lp_burst_same_pool_continuity"), 0) or 0),
            "lp_burst_volume_surge_ratio": float(_first_value(gate_metrics.get("lp_burst_volume_surge_ratio"), existing.get("lp_burst_volume_surge_ratio"), 0.0) or 0.0),
            "lp_burst_action_intensity": float(_first_value(gate_metrics.get("lp_burst_action_intensity"), existing.get("lp_burst_action_intensity"), 0.0) or 0.0),
            "lp_burst_reserve_skew": float(_first_value(gate_metrics.get("lp_burst_reserve_skew"), existing.get("lp_burst_reserve_skew"), 0.0) or 0.0),
            "lp_burst_first_ts": int(_first_value(gate_metrics.get("lp_burst_first_ts"), existing.get("lp_burst_first_ts"), 0) or 0),
            "lp_burst_last_ts": int(_first_value(gate_metrics.get("lp_burst_last_ts"), existing.get("lp_burst_last_ts"), 0) or 0),
            "lp_burst_delivery_class": str(_first_value(gate_metrics.get("lp_burst_delivery_class"), existing.get("lp_burst_delivery_class"), "") or ""),
            "lp_directional_cooldown_key": str(_first_value(gate_metrics.get("lp_directional_cooldown_key"), existing.get("lp_directional_cooldown_key"), "") or ""),
            "lp_directional_cooldown_sec": int(_first_value(gate_metrics.get("lp_directional_cooldown_sec"), existing.get("lp_directional_cooldown_sec"), 0) or 0),
            "lp_directional_cooldown_allowed": bool(_first_value(gate_metrics.get("lp_directional_cooldown_allowed"), existing.get("lp_directional_cooldown_allowed"), False)),
            "lp_notify_hard_min_usd": float(_first_value(gate_metrics.get("lp_notify_hard_min_usd"), existing.get("lp_notify_hard_min_usd"), 0.0) or 0.0),
            "lp_notify_hard_min_usd_not_met": bool(_first_value(gate_metrics.get("lp_notify_hard_min_usd_not_met"), existing.get("lp_notify_hard_min_usd_not_met"), False)),
            "lp_structure_min_usd_per_event": float(_first_value(gate_metrics.get("lp_structure_min_usd_per_event"), existing.get("lp_structure_min_usd_per_event"), 0.0) or 0.0),
            "lp_continuity_eligible": bool(_first_value(gate_metrics.get("lp_continuity_eligible"), existing.get("lp_continuity_eligible"), False)),
            "lp_resonance_eligible": bool(_first_value(gate_metrics.get("lp_resonance_eligible"), existing.get("lp_resonance_eligible"), False)),
            "lp_continuity_filtered_by_min_usd": int(_first_value(gate_metrics.get("lp_continuity_filtered_by_min_usd"), existing.get("lp_continuity_filtered_by_min_usd"), 0) or 0),
            "lp_resonance_filtered_by_min_usd": int(_first_value(gate_metrics.get("lp_resonance_filtered_by_min_usd"), existing.get("lp_resonance_filtered_by_min_usd"), 0) or 0),
            "lp_sweep_phase": str(_first_value(gate_metrics.get("lp_sweep_phase"), existing.get("lp_sweep_phase"), "") or ""),
            "lp_sweep_followthrough_score": float(_first_value(gate_metrics.get("lp_sweep_followthrough_score"), existing.get("lp_sweep_followthrough_score"), 0.0) or 0.0),
            "lp_sweep_exhaustion_score": float(_first_value(gate_metrics.get("lp_sweep_exhaustion_score"), existing.get("lp_sweep_exhaustion_score"), 0.0) or 0.0),
            "lp_sweep_continuation_score": float(_first_value(gate_metrics.get("lp_sweep_continuation_score"), existing.get("lp_sweep_continuation_score"), 0.0) or 0.0),
            "lp_impact_to_size_ratio": float(_first_value(gate_metrics.get("lp_impact_to_size_ratio"), existing.get("lp_impact_to_size_ratio"), 0.0) or 0.0),
        }
        event.metadata["lp_burst"] = {
            "lp_burst_window_sec": payload["lp_burst_window_sec"],
            "lp_burst_pool_address": str(event.address or "").lower(),
            "lp_burst_direction": self._lp_burst_direction(event),
            "lp_burst_event_count": payload["lp_burst_event_count"],
            "lp_burst_total_usd": payload["lp_burst_total_usd"],
            "lp_burst_max_single_usd": payload["lp_burst_max_single_usd"],
            "lp_burst_same_pool_continuity": payload["lp_burst_same_pool_continuity"],
            "lp_burst_volume_surge_ratio": payload["lp_burst_volume_surge_ratio"],
            "lp_burst_action_intensity": payload["lp_burst_action_intensity"],
            "lp_burst_reserve_skew": payload["lp_burst_reserve_skew"],
            "lp_burst_first_ts": payload["lp_burst_first_ts"],
            "lp_burst_last_ts": payload["lp_burst_last_ts"],
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _is_persisted_exchange_adjacent_runtime_watch(self, watch_meta: dict | None) -> bool:
        watch_meta = watch_meta or {}
        return (
            bool(watch_meta.get("runtime_adjacent_watch"))
            and str(watch_meta.get("watch_meta_source") or "") == "runtime_adjacent_watch"
            and str(watch_meta.get("strategy_hint") or "") == "persisted_exchange_adjacent"
        )

    def _is_adjacent_watch_meta_missing(self, parsed: dict | None, watch_meta: dict | None) -> bool:
        parsed = parsed or {}
        if str(parsed.get("monitor_type") or "") != "adjacent_watch":
            return False
        watch_meta = watch_meta or {}
        return not watch_meta or str(watch_meta.get("watch_meta_source") or "") != "runtime_adjacent_watch"

    def _allow_persisted_exchange_adjacent_flow(
        self,
        parsed: dict | None,
        watch_context: dict | None,
        watch_meta: dict | None,
    ) -> bool:
        if not self._is_persisted_exchange_adjacent_runtime_watch(watch_meta):
            return True

        counterparty = str((watch_context or {}).get("counterparty") or "").lower()
        if counterparty in WATCH_ADDRESSES:
            return True
        if counterparty and counterparty in self._restored_top_counterparty_addresses(watch_meta, limit=3):
            return True
        if self._allow_persisted_exchange_related_flow(parsed):
            return True
        return False

    def _allow_persisted_exchange_related_flow(self, parsed: dict | None) -> bool:
        parsed = parsed or {}
        if not bool(parsed.get("is_exchange_related")):
            return False
        usd_value = float(parsed.get("usd_value") or parsed.get("value") or 0.0)
        pricing_confidence = float(parsed.get("pricing_confidence") or 0.0)
        return (
            usd_value >= PERSISTED_EXCHANGE_ADJACENT_EXCHANGE_RELATED_MIN_USD
            and pricing_confidence >= PERSISTED_EXCHANGE_ADJACENT_EXCHANGE_RELATED_MIN_PRICING_CONFIDENCE
        )

    def _archive_prefilter_delivery_audit(
        self,
        parsed: dict | None,
        watch_context: dict | None,
        watch_meta: dict | None,
        archive_status: dict,
        archive_ts: int,
        gate_reason: str,
        audit_extras: dict | None = None,
    ) -> None:
        if self.archive_store is None:
            return

        parsed = parsed or {}
        watch_context = watch_context or {}
        watch_meta = watch_meta or {}
        tx_hash = str(parsed.get("tx_hash") or parsed.get("hash") or "")
        watch_address = str(
            parsed.get("watch_address")
            or watch_context.get("watch_address")
            or watch_meta.get("address")
            or ""
        ).lower()
        counterparty = str(
            watch_context.get("counterparty")
            or parsed.get("counterparty")
            or parsed.get("to")
            or parsed.get("from")
            or ""
        ).lower()
        strategy_role = str(
            watch_meta.get("strategy_role")
            or parsed.get("strategy_role")
            or "unknown"
        )
        event_id_seed = "|".join(
            [
                tx_hash,
                watch_address or "runtime_adjacent_watch",
                counterparty or "counterparty_unknown",
                gate_reason,
            ]
        )
        record = {
            "event_id": f"evt_{hashlib.sha1(event_id_seed.encode('utf-8')).hexdigest()[:16]}",
            "tx_hash": tx_hash,
            "watch_address": watch_address,
            "monitor_type": str(parsed.get("monitor_type") or ""),
            "strategy_role": strategy_role,
            "role_group": str(strategy_role_group(strategy_role)),
            "intent_type": str(parsed.get("intent_type") or ""),
            "behavior_type": "unknown",
            "gate_reason": gate_reason,
            "delivery_class": "drop",
            "stage": "prefilter",
            "counterparty": counterparty,
            "watch_meta_source": str(watch_meta.get("watch_meta_source") or ""),
            "strategy_hint": str(watch_meta.get("strategy_hint") or ""),
            "runtime_adjacent_watch": bool(watch_meta.get("runtime_adjacent_watch")),
            "runtime_state": str(watch_meta.get("runtime_state") or ""),
            "anchor_watch_address": str(watch_meta.get("anchor_watch_address") or ""),
            "downstream_case_id": str(watch_meta.get("downstream_case_id") or ""),
            "touched_watch_addresses": list(parsed.get("touched_watch_addresses") or []),
            "touched_lp_pools": list(parsed.get("touched_lp_pools") or []),
            "touched_lp_pool_count": int(parsed.get("touched_lp_pool_count") or 0),
            "tx_pool_hit_count": int(parsed.get("tx_pool_hit_count") or 0),
            "pool_transfer_count_by_pool": dict(parsed.get("pool_transfer_count_by_pool") or {}),
            "pool_candidate_weight": round(float(parsed.get("pool_candidate_weight") or 0.0), 3),
            "participant_addresses": list(parsed.get("participant_addresses") or []),
            "listener_rpc_mode": str(parsed.get("listener_rpc_mode") or ""),
            "listener_block_fetch_mode": str(parsed.get("listener_block_fetch_mode") or ""),
            "listener_block_fetch_reason": str(parsed.get("listener_block_fetch_reason") or ""),
            "listener_block_get_logs_request_count": int(
                parsed.get("listener_block_get_logs_request_count") or 0
            ),
            "listener_block_topic_chunk_count": int(
                parsed.get("listener_block_topic_chunk_count") or 0
            ),
            "listener_block_monitored_address_count": int(
                parsed.get("listener_block_monitored_address_count") or 0
            ),
            "listener_block_lp_secondary_scan_used": bool(
                parsed.get("listener_block_lp_secondary_scan_used")
            ),
            "listener_block_bloom_prefilter_used": bool(
                parsed.get("listener_block_bloom_prefilter_used")
            ),
            "listener_block_bloom_skipped_get_logs_count": int(
                parsed.get("listener_block_bloom_skipped_get_logs_count") or 0
            ),
            "listener_block_bloom_transfer_possible": bool(
                parsed.get("listener_block_bloom_transfer_possible")
                if "listener_block_bloom_transfer_possible" in parsed
                else True
            ),
            "listener_block_bloom_address_possible_count": int(
                parsed.get("listener_block_bloom_address_possible_count") or 0
            ),
            "listener_runtime_adjacent_core_count": int(
                parsed.get("listener_runtime_adjacent_core_count") or 0
            ),
            "listener_runtime_adjacent_secondary_count": int(
                parsed.get("listener_runtime_adjacent_secondary_count") or 0
            ),
            "listener_runtime_adjacent_secondary_scan_used": bool(
                parsed.get("listener_runtime_adjacent_secondary_scan_used")
            ),
            "listener_runtime_adjacent_secondary_skipped_count": int(
                parsed.get("listener_runtime_adjacent_secondary_skipped_count") or 0
            ),
            "listener_block_lp_primary_trend_scan_used": bool(
                parsed.get("listener_block_lp_primary_trend_scan_used")
            ),
            "listener_block_lp_extended_scan_used": bool(
                parsed.get("listener_block_lp_extended_scan_used")
            ),
            "listener_block_lp_primary_trend_pool_count": int(
                parsed.get("listener_block_lp_primary_trend_pool_count") or 0
            ),
            "listener_block_lp_extended_pool_count": int(
                parsed.get("listener_block_lp_extended_pool_count") or 0
            ),
            "listener_block_get_logs_primary_side_count": int(
                parsed.get("listener_block_get_logs_primary_side_count") or 0
            ),
            "listener_block_get_logs_secondary_side_count": int(
                parsed.get("listener_block_get_logs_secondary_side_count") or 0
            ),
            "listener_block_get_logs_secondary_side_skipped_count": int(
                parsed.get("listener_block_get_logs_secondary_side_skipped_count") or 0
            ),
            "listener_block_get_logs_empty_response_count": int(
                parsed.get("listener_block_get_logs_empty_response_count") or 0
            ),
            "low_cu_mode_enabled": bool(parsed.get("low_cu_mode_enabled")),
            "low_cu_mode_lp_secondary_only": bool(
                parsed.get("low_cu_mode_lp_secondary_only")
            ),
            "low_cu_mode_poll_interval_sec": round(
                float(parsed.get("low_cu_mode_poll_interval_sec") or 0.0), 3
            ),
            "lp_adjacent_noise_skipped_in_listener": bool(parsed.get("lp_adjacent_noise_skipped_in_listener")),
            "lp_adjacent_noise_listener_reason": str(parsed.get("lp_adjacent_noise_listener_reason") or ""),
            "lp_adjacent_noise_listener_confidence": round(float(parsed.get("lp_adjacent_noise_listener_confidence") or 0.0), 3),
            "lp_adjacent_noise_listener_source_signals": list(parsed.get("lp_adjacent_noise_listener_source_signals") or []),
            "lp_adjacent_noise_rule_version": str(parsed.get("lp_adjacent_noise_rule_version") or ""),
            "lp_adjacent_noise_decision_stage": str(parsed.get("lp_adjacent_noise_decision_stage") or ""),
            "lp_adjacent_noise_filtered": bool(parsed.get("lp_adjacent_noise_filtered")),
            "lp_adjacent_noise_reason": str(parsed.get("lp_adjacent_noise_reason") or ""),
            "lp_adjacent_noise_confidence": round(float(parsed.get("lp_adjacent_noise_confidence") or 0.0), 3),
            "lp_adjacent_noise_source_signals": list(parsed.get("lp_adjacent_noise_source_signals") or []),
            "lp_adjacent_noise_context_used": list(parsed.get("lp_adjacent_noise_context_used") or []),
            "lp_adjacent_noise_runtime_context_present": bool(
                parsed.get("lp_adjacent_noise_runtime_context_present")
            ),
            "lp_adjacent_noise_downstream_context_present": bool(
                parsed.get("lp_adjacent_noise_downstream_context_present")
            ),
            "usd_value": round(float(parsed.get("usd_value") or parsed.get("value") or 0.0), 2),
            "pricing_confidence": round(float(parsed.get("pricing_confidence") or 0.0), 3),
            "archive_ts": int(archive_ts),
        }
        silent_reason = self._build_silent_reason(
            stage="prefilter",
            reason_code=gate_reason,
            reason_detail=gate_reason,
            reason_bucket="prefilter_blocked",
            parsed=parsed,
            watch_meta=watch_meta,
        )
        record.update({
            "silent_reason": silent_reason,
            "silent_reason_bucket": str(silent_reason.get("reason_bucket") or "prefilter_blocked"),
            "shadow_high_value_candidate": False,
            "shadow_candidate_reason": "",
            "shadow_candidate_class": "",
        })
        if audit_extras:
            record.update(audit_extras)
        try:
            archive_status["delivery_audit"] = bool(
                self.archive_store.write_delivery_audit(record, archive_ts=archive_ts)
            ) or bool(archive_status.get("delivery_audit"))
        except Exception as e:
            print(f"prefilter delivery audit 归档失败: {e}")

    def _archive_adjacent_watch_meta_missing_audit(
        self,
        parsed: dict | None,
        watch_context: dict | None,
        watch_meta: dict | None,
        archive_status: dict,
        archive_ts: int,
    ) -> None:
        self._archive_prefilter_delivery_audit(
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
            archive_status=archive_status,
            archive_ts=archive_ts,
            gate_reason="adjacent_watch_meta_missing",
            audit_extras={
                "strategy_role": str(
                    (watch_meta or {}).get("strategy_role")
                    or (parsed or {}).get("strategy_role")
                    or "unknown"
                ),
            },
        )

    def _archive_persisted_exchange_adjacent_prefilter_audit(
        self,
        parsed: dict | None,
        watch_context: dict | None,
        watch_meta: dict | None,
        archive_status: dict,
        archive_ts: int,
    ) -> None:
        self._archive_prefilter_delivery_audit(
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
            archive_status=archive_status,
            archive_ts=archive_ts,
            gate_reason="persisted_exchange_adjacent_filtered",
            audit_extras={
                "is_exchange_related": bool((parsed or {}).get("is_exchange_related")),
            },
        )

    def _raw_archive_payload(self, raw_item: dict, *, archive_ts: int) -> dict:
        lp_debug = dict(raw_item.get("lp_parse_debug") or {})
        watched_pool = str(
            raw_item.get("address")
            or raw_item.get("watch_address")
            or (raw_item.get("touched_lp_pools") or [""])[0]
            or ""
        ).lower()
        tx_hash = str(raw_item.get("tx_hash") or "")
        raw_kind = str(
            raw_item.get("kind")
            or raw_item.get("raw_kind")
            or raw_item.get("event_type")
            or "raw_event"
        )
        captured_at = int(
            raw_item.get("captured_at")
            or raw_item.get("block_ts")
            or raw_item.get("ts")
            or archive_ts
        )
        event_id_seed = "|".join([tx_hash, watched_pool or "unknown", raw_kind, str(captured_at)])
        event_id = str(raw_item.get("event_id") or f"raw_{hashlib.sha1(event_id_seed.encode('utf-8')).hexdigest()[:16]}")
        return {
            **raw_item,
            "event_id": event_id,
            "tx_hash": tx_hash,
            "block_number": raw_item.get("block_number"),
            "watch_address": watched_pool,
            "pool_address": watched_pool,
            "raw_kind": raw_kind,
            "captured_at": captured_at,
            "listener_scan_path": str(
                raw_item.get("listener_scan_path")
                or raw_item.get("lp_scan_path")
                or lp_debug.get("scan_path")
                or ""
            ),
        }

    def _restored_top_counterparty_addresses(self, watch_meta: dict | None, limit: int = 3) -> set[str]:
        watch_meta = watch_meta or {}
        entries = list(watch_meta.get("restored_top_counterparties") or [])[: max(int(limit), 0)]
        addresses = set()
        for entry in entries:
            if isinstance(entry, dict):
                address = str(entry.get("address") or "").lower()
            elif isinstance(entry, (list, tuple)) and entry:
                address = str(entry[0] or "").lower()
            else:
                address = ""
            if address:
                addresses.add(address)
        return addresses

    def _archive_raw_event(self, raw_item: dict, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["raw_event"] = bool(
                self.archive_store.write_raw_event(
                    self._raw_archive_payload(raw_item, archive_ts=archive_ts),
                    archive_ts=archive_ts,
                )
            )
        except Exception as e:
            print(f"raw event 归档失败: {e}")

    def _archive_parse_failure(self, raw_item: dict, archive_status: dict, archive_ts: int) -> None:
        lp_debug = dict(raw_item.get("lp_parse_debug") or {})
        touched_lp_pools = list(raw_item.get("touched_lp_pools") or [])
        if not lp_debug and not touched_lp_pools:
            return

        watch_address = str(lp_debug.get("watch_address") or (touched_lp_pools[0] if touched_lp_pools else "")).lower()
        tx_hash = str(raw_item.get("tx_hash") or "")
        reason = str(lp_debug.get("reason") or "lp_parse_failed")
        event_id_seed = "|".join([tx_hash, watch_address or "lp_pool", reason])
        event_id = f"evt_{hashlib.sha1(event_id_seed.encode('utf-8')).hexdigest()[:16]}"
        record = {
            "event_id": event_id,
            "tx_hash": tx_hash,
            "watch_address": watch_address,
            "strategy_role": "lp_pool" if watch_address else "unknown",
            "role_group": "lp_pool" if watch_address else "other",
            "intent_type": "lp_parse_failed",
            "behavior_type": "unknown",
            "gate_reason": reason,
            "delivery_class": "drop",
            "stage": "strategy",
            "lp_parse_status": str(lp_debug.get("status") or "failed"),
            "lp_parse_missing_legs": list(lp_debug.get("missing_legs") or []),
            "replay_source": str(raw_item.get("replay_source") or ""),
        }
        try:
            archive_status["delivery_audit"] = bool(
                self.archive_store.write_delivery_audit(record, archive_ts=archive_ts)
            ) or bool(archive_status.get("delivery_audit"))
        except Exception as e:
            print(f"lp parse failure audit 归档失败: {e}")

    def _archive_parsed_event(self, parsed: dict, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["parsed_event"] = bool(self.archive_store.write_parsed_event(parsed, archive_ts=archive_ts))
        except Exception as e:
            print(f"parsed event 归档失败: {e}")

    def _signal_archive_key(self, signal, event: Event | None = None) -> str:
        signal_id = str(getattr(signal, "signal_id", "") or "")
        if signal_id:
            return signal_id
        tx_hash = str(getattr(signal, "tx_hash", "") or getattr(event, "tx_hash", "") or "")
        stage = str(
            getattr(signal, "context", {}).get("lp_alert_stage")
            or getattr(signal, "metadata", {}).get("lp_alert_stage")
            or getattr(event, "metadata", {}).get("lp_alert_stage")
            or getattr(signal, "delivery_class", "")
            or "signal"
        )
        return f"{tx_hash}:{stage}"

    def _build_signal_archive_record(
        self,
        signal,
        event: Event,
        *,
        archive_ts: int,
        sent_to_telegram: bool | None = None,
        delivery_decision: str | None = None,
    ) -> dict:
        signal_metadata = getattr(signal, "metadata", {}) or {}
        signal_context = getattr(signal, "context", {}) or {}
        event_metadata = getattr(event, "metadata", {}) or {}
        lp_outcome_record = dict(
            signal_metadata.get("lp_outcome_record")
            or signal_context.get("lp_outcome_record")
            or event_metadata.get("lp_outcome_record")
            or {}
        )
        outcome_tracking = dict(
            signal_context.get("outcome_tracking")
            or signal_metadata.get("outcome_tracking")
            or event_metadata.get("outcome_tracking")
            or {}
        )
        message_variant = str(
            signal_context.get("message_variant")
            or signal_metadata.get("message_variant")
            or event_metadata.get("message_variant")
            or ""
        )
        message_template = str(
            signal_context.get("message_template")
            or signal_metadata.get("message_template")
            or event_metadata.get("message_template")
            or ""
        )
        notifier_sent_at = (
            signal_context.get("notifier_sent_at")
            or signal_metadata.get("notifier_sent_at")
            or event_metadata.get("notifier_sent_at")
        )
        delivery_class = str(getattr(signal, "delivery_class", "") or getattr(event, "delivery_class", "") or "")
        delivery_reason = str(getattr(signal, "delivery_reason", "") or getattr(event, "delivery_reason", "") or "")
        resolved_delivery_decision = str(delivery_decision or delivery_reason or delivery_class or "").strip()
        signal_archive_key = self._signal_archive_key(signal, event)
        record = {
            "signal_archive_key": signal_archive_key,
            "signal_id": str(getattr(signal, "signal_id", "") or ""),
            "event_id": str(getattr(signal, "event_id", "") or getattr(event, "event_id", "") or ""),
            "asset_case_id": str(
                signal_context.get("asset_case_id")
                or signal_metadata.get("asset_case_id")
                or event_metadata.get("asset_case_id")
                or ""
            ),
            "tx_hash": str(getattr(signal, "tx_hash", "") or getattr(event, "tx_hash", "") or ""),
            "stage": str(
                signal_context.get("lp_alert_stage")
                or signal_metadata.get("lp_alert_stage")
                or event_metadata.get("lp_alert_stage")
                or signal_context.get("stage")
                or signal_metadata.get("stage")
                or ""
            ),
            "canonical_semantic_key": str(
                canonicalize_pool_semantic_key(
                    getattr(signal, "type", "")
                    or getattr(signal, "intent_type", "")
                    or getattr(event, "intent_type", "")
                )
            ),
            "operational_intent_key": str(
                signal_context.get("operational_intent_key")
                or signal_metadata.get("operational_intent_key")
                or event_metadata.get("operational_intent_key")
                or ""
            ),
            "notifier_variant": message_variant,
            "notifier_template": message_template,
            "delivery_decision": resolved_delivery_decision,
            "delivery_class": delivery_class,
            "delivery_reason": delivery_reason,
            "sent_to_telegram": bool(sent_to_telegram) if sent_to_telegram is not None else bool(notifier_sent_at),
            "notifier_sent_at": int(notifier_sent_at) if notifier_sent_at not in {None, ""} else None,
            "market_context_source": str(
                signal_context.get("market_context_source")
                or signal_metadata.get("market_context_source")
                or event_metadata.get("market_context_source")
                or "unavailable"
            ),
            "market_context_venue": str(
                signal_context.get("market_context_venue")
                or signal_metadata.get("market_context_venue")
                or event_metadata.get("market_context_venue")
                or ""
            ),
            "market_context_requested_symbol": str(
                signal_context.get("market_context_requested_symbol")
                or signal_metadata.get("market_context_requested_symbol")
                or event_metadata.get("market_context_requested_symbol")
                or ""
            ),
            "market_context_resolved_symbol": str(
                signal_context.get("market_context_resolved_symbol")
                or signal_metadata.get("market_context_resolved_symbol")
                or event_metadata.get("market_context_resolved_symbol")
                or ""
            ),
            "market_context_failure_reason": str(
                signal_context.get("market_context_failure_reason")
                or signal_metadata.get("market_context_failure_reason")
                or event_metadata.get("market_context_failure_reason")
                or ""
            ),
            "market_context_endpoint": str(
                signal_context.get("market_context_endpoint")
                or signal_metadata.get("market_context_endpoint")
                or event_metadata.get("market_context_endpoint")
                or ""
            ),
            "market_context_http_status": (
                signal_context.get("market_context_http_status")
                if signal_context.get("market_context_http_status") not in {None, ""}
                else signal_metadata.get("market_context_http_status")
                if signal_metadata.get("market_context_http_status") not in {None, ""}
                else event_metadata.get("market_context_http_status")
            ),
            "market_context_latency_ms": (
                signal_context.get("market_context_latency_ms")
                if signal_context.get("market_context_latency_ms") not in {None, ""}
                else signal_metadata.get("market_context_latency_ms")
                if signal_metadata.get("market_context_latency_ms") not in {None, ""}
                else event_metadata.get("market_context_latency_ms")
            ),
            "alert_relative_timing": str(
                signal_context.get("alert_relative_timing")
                or signal_metadata.get("alert_relative_timing")
                or event_metadata.get("alert_relative_timing")
                or ""
            ),
            "lp_alert_stage": str(
                signal_context.get("lp_alert_stage")
                or signal_metadata.get("lp_alert_stage")
                or event_metadata.get("lp_alert_stage")
                or ""
            ),
            "lp_prealert_candidate": bool(
                signal_context.get("lp_prealert_candidate")
                or signal_metadata.get("lp_prealert_candidate")
                or event_metadata.get("lp_prealert_candidate")
            ),
            "lp_prealert_candidate_reason": str(
                signal_context.get("lp_prealert_candidate_reason")
                or signal_metadata.get("lp_prealert_candidate_reason")
                or event_metadata.get("lp_prealert_candidate_reason")
                or ""
            ),
            "lp_prealert_gate_passed": bool(
                signal_context.get("lp_prealert_gate_passed")
                or signal_metadata.get("lp_prealert_gate_passed")
                or event_metadata.get("lp_prealert_gate_passed")
            ),
            "lp_prealert_gate_fail_reason": str(
                signal_context.get("lp_prealert_gate_fail_reason")
                or signal_metadata.get("lp_prealert_gate_fail_reason")
                or event_metadata.get("lp_prealert_gate_fail_reason")
                or ""
            ),
            "lp_prealert_delivery_allowed": (
                signal_context.get("lp_prealert_delivery_allowed")
                if signal_context.get("lp_prealert_delivery_allowed") is not None
                else signal_metadata.get("lp_prealert_delivery_allowed")
                if signal_metadata.get("lp_prealert_delivery_allowed") is not None
                else event_metadata.get("lp_prealert_delivery_allowed")
            ),
            "lp_prealert_delivery_block_reason": str(
                signal_context.get("lp_prealert_delivery_block_reason")
                or signal_metadata.get("lp_prealert_delivery_block_reason")
                or event_metadata.get("lp_prealert_delivery_block_reason")
                or ""
            ),
            "lp_prealert_asset_case_preserved": (
                signal_context.get("lp_prealert_asset_case_preserved")
                if signal_context.get("lp_prealert_asset_case_preserved") is not None
                else signal_metadata.get("lp_prealert_asset_case_preserved")
                if signal_metadata.get("lp_prealert_asset_case_preserved") is not None
                else event_metadata.get("lp_prealert_asset_case_preserved")
            ),
            "lp_prealert_stage_overwritten": (
                signal_context.get("lp_prealert_stage_overwritten")
                if signal_context.get("lp_prealert_stage_overwritten") is not None
                else signal_metadata.get("lp_prealert_stage_overwritten")
                if signal_metadata.get("lp_prealert_stage_overwritten") is not None
                else event_metadata.get("lp_prealert_stage_overwritten")
            ),
            "lp_prealert_first_leg": bool(
                signal_context.get("lp_prealert_first_leg")
                or signal_metadata.get("lp_prealert_first_leg")
                or event_metadata.get("lp_prealert_first_leg")
            ),
            "lp_prealert_major_override_used": bool(
                signal_context.get("lp_prealert_major_override_used")
                or signal_metadata.get("lp_prealert_major_override_used")
                or event_metadata.get("lp_prealert_major_override_used")
            ),
            "outcome_tracking_key": str(
                outcome_tracking.get("record_id")
                or lp_outcome_record.get("record_id")
                or outcome_tracking.get("outcome_tracking_key")
                or ""
            ),
            "move_after_alert_30s": (
                lp_outcome_record.get("move_after_alert_30s")
                if lp_outcome_record.get("move_after_alert_30s") is not None
                else outcome_tracking.get("move_after_alert_30s")
            ),
            "move_after_alert_60s": (
                lp_outcome_record.get("move_after_alert_60s")
                if lp_outcome_record.get("move_after_alert_60s") is not None
                else outcome_tracking.get("move_after_alert_60s")
            ),
            "move_after_alert_300s": (
                lp_outcome_record.get("move_after_alert_300s")
                if lp_outcome_record.get("move_after_alert_300s") is not None
                else outcome_tracking.get("move_after_alert_300s")
            ),
            "raw_move_after_30s": lp_outcome_record.get("raw_move_after_30s"),
            "raw_move_after_60s": lp_outcome_record.get("raw_move_after_60s"),
            "raw_move_after_300s": lp_outcome_record.get("raw_move_after_300s"),
            "direction_adjusted_move_after_30s": lp_outcome_record.get("direction_adjusted_move_after_30s"),
            "direction_adjusted_move_after_60s": lp_outcome_record.get("direction_adjusted_move_after_60s"),
            "direction_adjusted_move_after_300s": lp_outcome_record.get("direction_adjusted_move_after_300s"),
            "adverse_by_direction_30s": lp_outcome_record.get("adverse_by_direction_30s"),
            "adverse_by_direction_60s": lp_outcome_record.get("adverse_by_direction_60s"),
            "adverse_by_direction_300s": lp_outcome_record.get("adverse_by_direction_300s"),
            "outcome_price_source": str(
                lp_outcome_record.get("outcome_price_source")
                or outcome_tracking.get("outcome_price_source")
                or ""
            ),
            "outcome_price_start": (
                lp_outcome_record.get("outcome_price_start")
                if lp_outcome_record.get("outcome_price_start") not in {None, ""}
                else outcome_tracking.get("outcome_price_start")
            ),
            "outcome_price_end": (
                lp_outcome_record.get("outcome_price_end")
                if lp_outcome_record.get("outcome_price_end") not in {None, ""}
                else outcome_tracking.get("outcome_price_end")
            ),
            "outcome_window_status": str(
                lp_outcome_record.get("outcome_window_status")
                or outcome_tracking.get("outcome_window_status")
                or ""
            ),
            "outcome_failure_reason": str(
                lp_outcome_record.get("outcome_failure_reason")
                or outcome_tracking.get("outcome_failure_reason")
                or ""
            ),
            "outcome_windows": dict(lp_outcome_record.get("outcome_windows") or {}),
            "archive_written_at": int(archive_ts),
            "asset_case_key": str(
                signal_context.get("asset_case_key")
                or signal_metadata.get("asset_case_key")
                or event_metadata.get("asset_case_key")
                or ""
            ),
            "asset_case_had_prealert": bool(
                signal_context.get("asset_case_had_prealert")
                or signal_metadata.get("asset_case_had_prealert")
                or event_metadata.get("asset_case_had_prealert")
            ),
            "asset_case_prealert_to_confirm_sec": (
                signal_context.get("asset_case_prealert_to_confirm_sec")
                if signal_context.get("asset_case_prealert_to_confirm_sec") not in {None, ""}
                else signal_metadata.get("asset_case_prealert_to_confirm_sec")
                if signal_metadata.get("asset_case_prealert_to_confirm_sec") not in {None, ""}
                else event_metadata.get("asset_case_prealert_to_confirm_sec")
            ),
            "pair_label": str(
                signal_context.get("pair_label")
                or signal_metadata.get("pair_label")
                or event_metadata.get("pair_label")
                or ""
            ),
            "lp_confirm_quality": str(
                signal_context.get("lp_confirm_quality")
                or signal_metadata.get("lp_confirm_quality")
                or event_metadata.get("lp_confirm_quality")
                or ""
            ),
            "lp_confirm_scope": str(
                signal_context.get("lp_confirm_scope")
                or signal_metadata.get("lp_confirm_scope")
                or event_metadata.get("lp_confirm_scope")
                or ""
            ),
            "lp_absorption_context": str(
                signal_context.get("lp_absorption_context")
                or signal_metadata.get("lp_absorption_context")
                or event_metadata.get("lp_absorption_context")
                or ""
            ),
            "lp_absorption_confidence": float(
                signal_context.get("lp_absorption_confidence")
                or signal_metadata.get("lp_absorption_confidence")
                or event_metadata.get("lp_absorption_confidence")
                or 0.0
            ),
            "lp_broader_alignment": str(
                signal_context.get("lp_broader_alignment")
                or signal_metadata.get("lp_broader_alignment")
                or event_metadata.get("lp_broader_alignment")
                or ""
            ),
            "lp_local_vs_broad_reason": str(
                signal_context.get("lp_local_vs_broad_reason")
                or signal_metadata.get("lp_local_vs_broad_reason")
                or event_metadata.get("lp_local_vs_broad_reason")
                or ""
            ),
            "lp_confirm_reason": str(
                signal_context.get("lp_confirm_reason")
                or signal_metadata.get("lp_confirm_reason")
                or event_metadata.get("lp_confirm_reason")
                or ""
            ),
            "lp_confirm_alignment_score": float(
                signal_context.get("lp_confirm_alignment_score")
                or signal_metadata.get("lp_confirm_alignment_score")
                or event_metadata.get("lp_confirm_alignment_score")
                or 0.0
            ),
            "lp_chase_risk_score": float(
                signal_context.get("lp_chase_risk_score")
                or signal_metadata.get("lp_chase_risk_score")
                or event_metadata.get("lp_chase_risk_score")
                or 0.0
            ),
            "lp_confirm_timing_bucket": str(
                signal_context.get("lp_confirm_timing_bucket")
                or signal_metadata.get("lp_confirm_timing_bucket")
                or event_metadata.get("lp_confirm_timing_bucket")
                or ""
            ),
            "trade_action_key": str(
                signal_context.get("trade_action_key")
                or signal_metadata.get("trade_action_key")
                or event_metadata.get("trade_action_key")
                or ""
            ),
            "trade_action_label": str(
                signal_context.get("trade_action_label")
                or signal_metadata.get("trade_action_label")
                or event_metadata.get("trade_action_label")
                or ""
            ),
            "trade_action_direction": str(
                signal_context.get("trade_action_direction")
                or signal_metadata.get("trade_action_direction")
                or event_metadata.get("trade_action_direction")
                or ""
            ),
            "trade_action_confidence": float(
                signal_context.get("trade_action_confidence")
                or signal_metadata.get("trade_action_confidence")
                or event_metadata.get("trade_action_confidence")
                or 0.0
            ),
            "trade_action_reason": str(
                signal_context.get("trade_action_reason")
                or signal_metadata.get("trade_action_reason")
                or event_metadata.get("trade_action_reason")
                or ""
            ),
            "trade_action_blockers": list(
                signal_context.get("trade_action_blockers")
                or signal_metadata.get("trade_action_blockers")
                or event_metadata.get("trade_action_blockers")
                or []
            ),
            "trade_action_required_confirmation": str(
                signal_context.get("trade_action_required_confirmation")
                or signal_metadata.get("trade_action_required_confirmation")
                or event_metadata.get("trade_action_required_confirmation")
                or ""
            ),
            "trade_action_invalidated_by": str(
                signal_context.get("trade_action_invalidated_by")
                or signal_metadata.get("trade_action_invalidated_by")
                or event_metadata.get("trade_action_invalidated_by")
                or ""
            ),
            "trade_action_time_horizon_sec": int(
                signal_context.get("trade_action_time_horizon_sec")
                or signal_metadata.get("trade_action_time_horizon_sec")
                or event_metadata.get("trade_action_time_horizon_sec")
                or 0
            ),
            "trade_action_source": str(
                signal_context.get("trade_action_source")
                or signal_metadata.get("trade_action_source")
                or event_metadata.get("trade_action_source")
                or ""
            ),
            "trade_action_is_instruction": bool(
                signal_context.get("trade_action_is_instruction")
                if signal_context.get("trade_action_is_instruction") is not None
                else signal_metadata.get("trade_action_is_instruction")
                if signal_metadata.get("trade_action_is_instruction") is not None
                else event_metadata.get("trade_action_is_instruction")
            ),
            "trade_action_requires_user_confirmation": bool(
                signal_context.get("trade_action_requires_user_confirmation")
                if signal_context.get("trade_action_requires_user_confirmation") is not None
                else signal_metadata.get("trade_action_requires_user_confirmation")
                if signal_metadata.get("trade_action_requires_user_confirmation") is not None
                else event_metadata.get("trade_action_requires_user_confirmation")
            ),
            "asset_market_state_key": str(
                signal_context.get("asset_market_state_key")
                or signal_metadata.get("asset_market_state_key")
                or event_metadata.get("asset_market_state_key")
                or ""
            ),
            "asset_market_state_label": str(
                signal_context.get("asset_market_state_label")
                or signal_metadata.get("asset_market_state_label")
                or event_metadata.get("asset_market_state_label")
                or ""
            ),
            "asset_market_state_reason": str(
                signal_context.get("asset_market_state_reason")
                or signal_metadata.get("asset_market_state_reason")
                or event_metadata.get("asset_market_state_reason")
                or ""
            ),
            "asset_market_state_confidence": float(
                signal_context.get("asset_market_state_confidence")
                or signal_metadata.get("asset_market_state_confidence")
                or event_metadata.get("asset_market_state_confidence")
                or 0.0
            ),
            "asset_market_state_changed": bool(
                signal_context.get("asset_market_state_changed")
                if signal_context.get("asset_market_state_changed") is not None
                else signal_metadata.get("asset_market_state_changed")
                if signal_metadata.get("asset_market_state_changed") is not None
                else event_metadata.get("asset_market_state_changed")
            ),
            "previous_asset_market_state_key": str(
                signal_context.get("previous_asset_market_state_key")
                or signal_metadata.get("previous_asset_market_state_key")
                or event_metadata.get("previous_asset_market_state_key")
                or ""
            ),
            "asset_market_state_started_at": (
                signal_context.get("asset_market_state_started_at")
                if signal_context.get("asset_market_state_started_at") not in {None, ""}
                else signal_metadata.get("asset_market_state_started_at")
                if signal_metadata.get("asset_market_state_started_at") not in {None, ""}
                else event_metadata.get("asset_market_state_started_at")
            ),
            "asset_market_state_updated_at": (
                signal_context.get("asset_market_state_updated_at")
                if signal_context.get("asset_market_state_updated_at") not in {None, ""}
                else signal_metadata.get("asset_market_state_updated_at")
                if signal_metadata.get("asset_market_state_updated_at") not in {None, ""}
                else event_metadata.get("asset_market_state_updated_at")
            ),
            "asset_market_state_ttl_sec": (
                signal_context.get("asset_market_state_ttl_sec")
                if signal_context.get("asset_market_state_ttl_sec") not in {None, ""}
                else signal_metadata.get("asset_market_state_ttl_sec")
                if signal_metadata.get("asset_market_state_ttl_sec") not in {None, ""}
                else event_metadata.get("asset_market_state_ttl_sec")
            ),
            "asset_market_state_evidence_pack": str(
                signal_context.get("asset_market_state_evidence_pack")
                or signal_metadata.get("asset_market_state_evidence_pack")
                or event_metadata.get("asset_market_state_evidence_pack")
                or ""
            ),
            "asset_market_state_required_confirmation": str(
                signal_context.get("asset_market_state_required_confirmation")
                or signal_metadata.get("asset_market_state_required_confirmation")
                or event_metadata.get("asset_market_state_required_confirmation")
                or ""
            ),
            "asset_market_state_invalidated_by": str(
                signal_context.get("asset_market_state_invalidated_by")
                or signal_metadata.get("asset_market_state_invalidated_by")
                or event_metadata.get("asset_market_state_invalidated_by")
                or ""
            ),
            "trade_opportunity_id": str(
                signal_context.get("trade_opportunity_id")
                or signal_metadata.get("trade_opportunity_id")
                or event_metadata.get("trade_opportunity_id")
                or ""
            ),
            "trade_opportunity_key": str(
                signal_context.get("trade_opportunity_key")
                or signal_metadata.get("trade_opportunity_key")
                or event_metadata.get("trade_opportunity_key")
                or ""
            ),
            "trade_opportunity_side": str(
                signal_context.get("trade_opportunity_side")
                or signal_metadata.get("trade_opportunity_side")
                or event_metadata.get("trade_opportunity_side")
                or ""
            ),
            "trade_opportunity_status": str(
                signal_context.get("trade_opportunity_status")
                or signal_metadata.get("trade_opportunity_status")
                or event_metadata.get("trade_opportunity_status")
                or ""
            ),
            "trade_opportunity_label": str(
                signal_context.get("trade_opportunity_label")
                or signal_metadata.get("trade_opportunity_label")
                or event_metadata.get("trade_opportunity_label")
                or ""
            ),
            "trade_opportunity_score": float(
                signal_context.get("trade_opportunity_score")
                or signal_metadata.get("trade_opportunity_score")
                or event_metadata.get("trade_opportunity_score")
                or 0.0
            ),
            "trade_opportunity_confidence": str(
                signal_context.get("trade_opportunity_confidence")
                or signal_metadata.get("trade_opportunity_confidence")
                or event_metadata.get("trade_opportunity_confidence")
                or ""
            ),
            "trade_opportunity_time_horizon": str(
                signal_context.get("trade_opportunity_time_horizon")
                or signal_metadata.get("trade_opportunity_time_horizon")
                or event_metadata.get("trade_opportunity_time_horizon")
                or ""
            ),
            "trade_opportunity_reason": str(
                signal_context.get("trade_opportunity_reason")
                or signal_metadata.get("trade_opportunity_reason")
                or event_metadata.get("trade_opportunity_reason")
                or ""
            ),
            "trade_opportunity_evidence": list(
                signal_context.get("trade_opportunity_evidence")
                or signal_metadata.get("trade_opportunity_evidence")
                or event_metadata.get("trade_opportunity_evidence")
                or []
            ),
            "trade_opportunity_blockers": list(
                signal_context.get("trade_opportunity_blockers")
                or signal_metadata.get("trade_opportunity_blockers")
                or event_metadata.get("trade_opportunity_blockers")
                or []
            ),
            "trade_opportunity_required_confirmation": str(
                signal_context.get("trade_opportunity_required_confirmation")
                or signal_metadata.get("trade_opportunity_required_confirmation")
                or event_metadata.get("trade_opportunity_required_confirmation")
                or ""
            ),
            "trade_opportunity_invalidated_by": str(
                signal_context.get("trade_opportunity_invalidated_by")
                or signal_metadata.get("trade_opportunity_invalidated_by")
                or event_metadata.get("trade_opportunity_invalidated_by")
                or ""
            ),
            "trade_opportunity_risk_flags": list(
                signal_context.get("trade_opportunity_risk_flags")
                or signal_metadata.get("trade_opportunity_risk_flags")
                or event_metadata.get("trade_opportunity_risk_flags")
                or []
            ),
            "trade_opportunity_quality_snapshot": dict(
                signal_context.get("trade_opportunity_quality_snapshot")
                or signal_metadata.get("trade_opportunity_quality_snapshot")
                or event_metadata.get("trade_opportunity_quality_snapshot")
                or {}
            ),
            "trade_opportunity_outcome_policy": dict(
                signal_context.get("trade_opportunity_outcome_policy")
                or signal_metadata.get("trade_opportunity_outcome_policy")
                or event_metadata.get("trade_opportunity_outcome_policy")
                or {}
            ),
            "trade_opportunity_created_at": (
                signal_context.get("trade_opportunity_created_at")
                if signal_context.get("trade_opportunity_created_at") not in {None, ""}
                else signal_metadata.get("trade_opportunity_created_at")
                if signal_metadata.get("trade_opportunity_created_at") not in {None, ""}
                else event_metadata.get("trade_opportunity_created_at")
            ),
            "trade_opportunity_expires_at": (
                signal_context.get("trade_opportunity_expires_at")
                if signal_context.get("trade_opportunity_expires_at") not in {None, ""}
                else signal_metadata.get("trade_opportunity_expires_at")
                if signal_metadata.get("trade_opportunity_expires_at") not in {None, ""}
                else event_metadata.get("trade_opportunity_expires_at")
            ),
            "trade_opportunity_source": str(
                signal_context.get("trade_opportunity_source")
                or signal_metadata.get("trade_opportunity_source")
                or event_metadata.get("trade_opportunity_source")
                or ""
            ),
            "trade_opportunity_primary_blocker": str(
                signal_context.get("trade_opportunity_primary_blocker")
                or signal_metadata.get("trade_opportunity_primary_blocker")
                or event_metadata.get("trade_opportunity_primary_blocker")
                or ""
            ),
            "trade_opportunity_score_components": dict(
                signal_context.get("trade_opportunity_score_components")
                or signal_metadata.get("trade_opportunity_score_components")
                or event_metadata.get("trade_opportunity_score_components")
                or {}
            ),
            "trade_opportunity_history_snapshot": dict(
                signal_context.get("trade_opportunity_history_snapshot")
                or signal_metadata.get("trade_opportunity_history_snapshot")
                or event_metadata.get("trade_opportunity_history_snapshot")
                or {}
            ),
            "trade_opportunity_status_at_creation": str(
                signal_context.get("trade_opportunity_status_at_creation")
                or signal_metadata.get("trade_opportunity_status_at_creation")
                or event_metadata.get("trade_opportunity_status_at_creation")
                or ""
            ),
            "opportunity_outcome_source": str(
                signal_context.get("opportunity_outcome_source")
                or signal_metadata.get("opportunity_outcome_source")
                or event_metadata.get("opportunity_outcome_source")
                or ""
            ),
            "opportunity_outcome_30s": str(
                signal_context.get("opportunity_outcome_30s")
                or signal_metadata.get("opportunity_outcome_30s")
                or event_metadata.get("opportunity_outcome_30s")
                or ""
            ),
            "opportunity_outcome_60s": str(
                signal_context.get("opportunity_outcome_60s")
                or signal_metadata.get("opportunity_outcome_60s")
                or event_metadata.get("opportunity_outcome_60s")
                or ""
            ),
            "opportunity_outcome_300s": str(
                signal_context.get("opportunity_outcome_300s")
                or signal_metadata.get("opportunity_outcome_300s")
                or event_metadata.get("opportunity_outcome_300s")
                or ""
            ),
            "opportunity_followthrough_30s": (
                signal_context.get("opportunity_followthrough_30s")
                if signal_context.get("opportunity_followthrough_30s") is not None
                else signal_metadata.get("opportunity_followthrough_30s")
                if signal_metadata.get("opportunity_followthrough_30s") is not None
                else event_metadata.get("opportunity_followthrough_30s")
            ),
            "opportunity_followthrough_60s": (
                signal_context.get("opportunity_followthrough_60s")
                if signal_context.get("opportunity_followthrough_60s") is not None
                else signal_metadata.get("opportunity_followthrough_60s")
                if signal_metadata.get("opportunity_followthrough_60s") is not None
                else event_metadata.get("opportunity_followthrough_60s")
            ),
            "opportunity_followthrough_300s": (
                signal_context.get("opportunity_followthrough_300s")
                if signal_context.get("opportunity_followthrough_300s") is not None
                else signal_metadata.get("opportunity_followthrough_300s")
                if signal_metadata.get("opportunity_followthrough_300s") is not None
                else event_metadata.get("opportunity_followthrough_300s")
            ),
            "opportunity_adverse_30s": (
                signal_context.get("opportunity_adverse_30s")
                if signal_context.get("opportunity_adverse_30s") is not None
                else signal_metadata.get("opportunity_adverse_30s")
                if signal_metadata.get("opportunity_adverse_30s") is not None
                else event_metadata.get("opportunity_adverse_30s")
            ),
            "opportunity_adverse_60s": (
                signal_context.get("opportunity_adverse_60s")
                if signal_context.get("opportunity_adverse_60s") is not None
                else signal_metadata.get("opportunity_adverse_60s")
                if signal_metadata.get("opportunity_adverse_60s") is not None
                else event_metadata.get("opportunity_adverse_60s")
            ),
            "opportunity_adverse_300s": (
                signal_context.get("opportunity_adverse_300s")
                if signal_context.get("opportunity_adverse_300s") is not None
                else signal_metadata.get("opportunity_adverse_300s")
                if signal_metadata.get("opportunity_adverse_300s") is not None
                else event_metadata.get("opportunity_adverse_300s")
            ),
            "opportunity_invalidated_at": (
                signal_context.get("opportunity_invalidated_at")
                if signal_context.get("opportunity_invalidated_at") not in {None, ""}
                else signal_metadata.get("opportunity_invalidated_at")
                if signal_metadata.get("opportunity_invalidated_at") not in {None, ""}
                else event_metadata.get("opportunity_invalidated_at")
            ),
            "opportunity_invalidated_reason": str(
                signal_context.get("opportunity_invalidated_reason")
                or signal_metadata.get("opportunity_invalidated_reason")
                or event_metadata.get("opportunity_invalidated_reason")
                or ""
            ),
            "opportunity_result_label": str(
                signal_context.get("opportunity_result_label")
                or signal_metadata.get("opportunity_result_label")
                or event_metadata.get("opportunity_result_label")
                or ""
            ),
            "trade_opportunity_notifier_sent_at": (
                signal_context.get("trade_opportunity_notifier_sent_at")
                if signal_context.get("trade_opportunity_notifier_sent_at") not in {None, ""}
                else signal_metadata.get("trade_opportunity_notifier_sent_at")
                if signal_metadata.get("trade_opportunity_notifier_sent_at") not in {None, ""}
                else event_metadata.get("trade_opportunity_notifier_sent_at")
            ),
            "trade_opportunity_delivered_notification": bool(
                signal_context.get("trade_opportunity_delivered_notification")
                if signal_context.get("trade_opportunity_delivered_notification") is not None
                else signal_metadata.get("trade_opportunity_delivered_notification")
                if signal_metadata.get("trade_opportunity_delivered_notification") is not None
                else event_metadata.get("trade_opportunity_delivered_notification")
            ),
            "final_trading_output_source": str(
                signal_context.get("final_trading_output_source")
                or signal_metadata.get("final_trading_output_source")
                or event_metadata.get("final_trading_output_source")
                or ""
            ),
            "final_trading_output_label": str(
                signal_context.get("final_trading_output_label")
                or signal_metadata.get("final_trading_output_label")
                or event_metadata.get("final_trading_output_label")
                or ""
            ),
            "final_trading_output_allowed": bool(
                signal_context.get("final_trading_output_allowed")
                if signal_context.get("final_trading_output_allowed") is not None
                else signal_metadata.get("final_trading_output_allowed")
                if signal_metadata.get("final_trading_output_allowed") is not None
                else event_metadata.get("final_trading_output_allowed")
            ),
            "legacy_chase_downgraded": bool(
                signal_context.get("legacy_chase_downgraded")
                if signal_context.get("legacy_chase_downgraded") is not None
                else signal_metadata.get("legacy_chase_downgraded")
                if signal_metadata.get("legacy_chase_downgraded") is not None
                else event_metadata.get("legacy_chase_downgraded")
            ),
            "legacy_chase_downgrade_reason": str(
                signal_context.get("legacy_chase_downgrade_reason")
                or signal_metadata.get("legacy_chase_downgrade_reason")
                or event_metadata.get("legacy_chase_downgrade_reason")
                or ""
            ),
            "opportunity_gate_required": bool(
                signal_context.get("opportunity_gate_required")
                if signal_context.get("opportunity_gate_required") is not None
                else signal_metadata.get("opportunity_gate_required")
                if signal_metadata.get("opportunity_gate_required") is not None
                else event_metadata.get("opportunity_gate_required")
            ),
            "opportunity_gate_passed": bool(
                signal_context.get("opportunity_gate_passed")
                if signal_context.get("opportunity_gate_passed") is not None
                else signal_metadata.get("opportunity_gate_passed")
                if signal_metadata.get("opportunity_gate_passed") is not None
                else event_metadata.get("opportunity_gate_passed")
            ),
            "opportunity_gate_failure_reason": str(
                signal_context.get("opportunity_gate_failure_reason")
                or signal_metadata.get("opportunity_gate_failure_reason")
                or event_metadata.get("opportunity_gate_failure_reason")
                or ""
            ),
            "first_seen_stage": str(
                signal_context.get("first_seen_stage")
                or signal_metadata.get("first_seen_stage")
                or event_metadata.get("first_seen_stage")
                or ""
            ),
            "first_seen_at": (
                signal_context.get("first_seen_at")
                if signal_context.get("first_seen_at") not in {None, ""}
                else signal_metadata.get("first_seen_at")
                if signal_metadata.get("first_seen_at") not in {None, ""}
                else event_metadata.get("first_seen_at")
            ),
            "prealert_lifecycle_state": str(
                signal_context.get("prealert_lifecycle_state")
                or signal_metadata.get("prealert_lifecycle_state")
                or event_metadata.get("prealert_lifecycle_state")
                or ""
            ),
            "prealert_expires_at": (
                signal_context.get("prealert_expires_at")
                if signal_context.get("prealert_expires_at") not in {None, ""}
                else signal_metadata.get("prealert_expires_at")
                if signal_metadata.get("prealert_expires_at") not in {None, ""}
                else event_metadata.get("prealert_expires_at")
            ),
            "prealert_to_confirm_sec": (
                signal_context.get("prealert_to_confirm_sec")
                if signal_context.get("prealert_to_confirm_sec") not in {None, ""}
                else signal_metadata.get("prealert_to_confirm_sec")
                if signal_metadata.get("prealert_to_confirm_sec") not in {None, ""}
                else event_metadata.get("prealert_to_confirm_sec")
            ),
            "prealert_visible_to_user": bool(
                signal_context.get("prealert_visible_to_user")
                if signal_context.get("prealert_visible_to_user") is not None
                else signal_metadata.get("prealert_visible_to_user")
                if signal_metadata.get("prealert_visible_to_user") is not None
                else event_metadata.get("prealert_visible_to_user")
            ),
            "no_trade_lock_active": bool(
                signal_context.get("no_trade_lock_active")
                if signal_context.get("no_trade_lock_active") is not None
                else signal_metadata.get("no_trade_lock_active")
                if signal_metadata.get("no_trade_lock_active") is not None
                else event_metadata.get("no_trade_lock_active")
            ),
            "no_trade_lock_reason": str(
                signal_context.get("no_trade_lock_reason")
                or signal_metadata.get("no_trade_lock_reason")
                or event_metadata.get("no_trade_lock_reason")
                or ""
            ),
            "no_trade_lock_started_at": (
                signal_context.get("no_trade_lock_started_at")
                if signal_context.get("no_trade_lock_started_at") not in {None, ""}
                else signal_metadata.get("no_trade_lock_started_at")
                if signal_metadata.get("no_trade_lock_started_at") not in {None, ""}
                else event_metadata.get("no_trade_lock_started_at")
            ),
            "no_trade_lock_until": (
                signal_context.get("no_trade_lock_until")
                if signal_context.get("no_trade_lock_until") not in {None, ""}
                else signal_metadata.get("no_trade_lock_until")
                if signal_metadata.get("no_trade_lock_until") not in {None, ""}
                else event_metadata.get("no_trade_lock_until")
            ),
            "no_trade_lock_conflict_score": float(
                signal_context.get("no_trade_lock_conflict_score")
                or signal_metadata.get("no_trade_lock_conflict_score")
                or event_metadata.get("no_trade_lock_conflict_score")
                or 0.0
            ),
            "no_trade_lock_conflicting_signals": list(
                signal_context.get("no_trade_lock_conflicting_signals")
                or signal_metadata.get("no_trade_lock_conflicting_signals")
                or event_metadata.get("no_trade_lock_conflicting_signals")
                or []
            ),
            "no_trade_lock_release_condition": str(
                signal_context.get("no_trade_lock_release_condition")
                or signal_metadata.get("no_trade_lock_release_condition")
                or event_metadata.get("no_trade_lock_release_condition")
                or ""
            ),
            "no_trade_lock_released_by": str(
                signal_context.get("no_trade_lock_released_by")
                or signal_metadata.get("no_trade_lock_released_by")
                or event_metadata.get("no_trade_lock_released_by")
                or ""
            ),
            "telegram_should_send": bool(
                signal_context.get("telegram_should_send")
                if signal_context.get("telegram_should_send") is not None
                else signal_metadata.get("telegram_should_send")
                if signal_metadata.get("telegram_should_send") is not None
                else event_metadata.get("telegram_should_send")
            ),
            "telegram_suppression_reason": str(
                signal_context.get("telegram_suppression_reason")
                or signal_metadata.get("telegram_suppression_reason")
                or event_metadata.get("telegram_suppression_reason")
                or ""
            ),
            "telegram_state_change_reason": str(
                signal_context.get("telegram_state_change_reason")
                or signal_metadata.get("telegram_state_change_reason")
                or event_metadata.get("telegram_state_change_reason")
                or ""
            ),
            "telegram_update_kind": str(
                signal_context.get("telegram_update_kind")
                or signal_metadata.get("telegram_update_kind")
                or event_metadata.get("telegram_update_kind")
                or ""
            ),
            "suppressed_signal_count_in_state": int(
                signal_context.get("suppressed_signal_count_in_state")
                or signal_metadata.get("suppressed_signal_count_in_state")
                or event_metadata.get("suppressed_signal_count_in_state")
                or 0
            ),
            "last_telegram_state_key": str(
                signal_context.get("last_telegram_state_key")
                or signal_metadata.get("last_telegram_state_key")
                or event_metadata.get("last_telegram_state_key")
                or ""
            ),
            "lp_sweep_phase": str(
                signal_context.get("lp_sweep_phase")
                or signal_metadata.get("lp_sweep_phase")
                or event_metadata.get("lp_sweep_phase")
                or ""
            ),
            "lp_sweep_display_stage": str(
                signal_context.get("lp_sweep_display_stage")
                or signal_metadata.get("lp_sweep_display_stage")
                or event_metadata.get("lp_sweep_display_stage")
                or ""
            ),
            "lp_conflict_context": str(
                signal_context.get("lp_conflict_context")
                or signal_metadata.get("lp_conflict_context")
                or event_metadata.get("lp_conflict_context")
                or ""
            ),
            "lp_conflict_score": float(
                signal_context.get("lp_conflict_score")
                or signal_metadata.get("lp_conflict_score")
                or event_metadata.get("lp_conflict_score")
                or 0.0
            ),
            "lp_conflict_window_sec": int(
                signal_context.get("lp_conflict_window_sec")
                or signal_metadata.get("lp_conflict_window_sec")
                or event_metadata.get("lp_conflict_window_sec")
                or 0
            ),
            "lp_conflicting_signals": list(
                signal_context.get("lp_conflicting_signals")
                or signal_metadata.get("lp_conflicting_signals")
                or event_metadata.get("lp_conflicting_signals")
                or []
            ),
            "lp_conflict_resolution": str(
                signal_context.get("lp_conflict_resolution")
                or signal_metadata.get("lp_conflict_resolution")
                or event_metadata.get("lp_conflict_resolution")
                or ""
            ),
            "market_context_attempts": list(
                signal_context.get("market_context_attempts")
                or signal_metadata.get("market_context_attempts")
                or event_metadata.get("market_context_attempts")
                or []
            ),
            "market_context_attempted_venues": list(
                signal_context.get("market_context_attempted_venues")
                or signal_metadata.get("market_context_attempted_venues")
                or event_metadata.get("market_context_attempted_venues")
                or []
            ),
            "market_context_fallback_chain": list(
                signal_context.get("market_context_fallback_chain")
                or signal_metadata.get("market_context_fallback_chain")
                or event_metadata.get("market_context_fallback_chain")
                or []
            ),
            "signal": self.archive_store._serialize(signal),
            "event": self.archive_store._serialize(event),
        }
        return record

    def _archive_signal_state(
        self,
        signal,
        event: Event,
        archive_status: dict,
        archive_ts: int,
        *,
        sent_to_telegram: bool | None = None,
        delivery_decision: str | None = None,
    ) -> None:
        if self.archive_store is None:
            return
        try:
            record = self._build_signal_archive_record(
                signal,
                event,
                archive_ts=archive_ts,
                sent_to_telegram=sent_to_telegram,
                delivery_decision=delivery_decision,
            )
            archive_status["signal"] = bool(
                self.archive_store.write_signal(
                    record,
                    archive_ts=archive_ts,
                    dedupe_key=str(record.get("signal_archive_key") or ""),
                )
            ) or bool(archive_status.get("signal"))
        except Exception as e:
            print(f"signal 归档失败: {e}")

    def _archive_delivery_audit(
        self,
        event: Event,
        signal,
        behavior: dict | None,
        stage: str,
        gate_reason: str,
        archive_status: dict,
        archive_ts: int,
        gate_metrics: dict | None = None,
        audit_extras: dict | None = None,
    ) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["delivery_audit"] = bool(
                self.archive_store.write_delivery_audit(
                    self._delivery_audit_record(
                        event=event,
                        signal=signal,
                        behavior=behavior,
                        gate_metrics=gate_metrics,
                        stage=stage,
                        gate_reason=gate_reason,
                        archive_ts=archive_ts,
                        audit_extras=audit_extras,
                    ),
                    archive_ts=archive_ts,
                )
            )
        except Exception as e:
            print(f"delivery audit 归档失败: {e}")

    def _archive_case_update(self, case_result: dict | None, event: Event, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None or not case_result:
            return

        wrote = False
        try:
            for stale_case in case_result.get("stale_updates") or []:
                action = "expired" if self._is_downstream_followup_case(stale_case) else "stale"
                wrote = bool(
                    self.archive_store.write_case_update(
                        stale_case,
                        event=event,
                        action=action,
                        archive_ts=archive_ts,
                    )
                ) or wrote
                self._archive_downstream_case_followup(
                    behavior_case=stale_case,
                    archive_status=archive_status,
                    archive_ts=archive_ts,
                    status=str(getattr(stale_case, "status", "") or ""),
                    stage=str(getattr(stale_case, "stage", "") or ""),
                    should_notify=False,
                    reason=str((getattr(stale_case, "metadata", {}) or {}).get("lifecycle_reason") or "downstream_window_expired"),
                    event=event,
                )

            for invalid_case in case_result.get("invalidated_cases") or []:
                wrote = bool(
                    self.archive_store.write_case_update(
                        invalid_case,
                        event=event,
                        action="invalidated",
                        archive_ts=archive_ts,
                    )
                ) or wrote
                self._archive_downstream_case_followup(
                    behavior_case=invalid_case,
                    archive_status=archive_status,
                    archive_ts=archive_ts,
                    status="invalidated",
                    stage=str(getattr(invalid_case, "stage", "") or "invalidated"),
                    should_notify=False,
                    reason="downstream_followup_redundant",
                    event=event,
                )

            behavior_case = case_result.get("case")
            if behavior_case is not None:
                action = "opened" if bool(case_result.get("created")) else "updated"
                wrote = bool(
                    self.archive_store.write_case_update(
                        behavior_case,
                        event=event,
                        action=action,
                        archive_ts=archive_ts,
                    )
                ) or wrote
                self._archive_downstream_case_followup(
                    behavior_case=behavior_case,
                    archive_status=archive_status,
                    archive_ts=archive_ts,
                    status=str(getattr(behavior_case, "status", "") or ""),
                    stage=str(getattr(behavior_case, "stage", "") or ""),
                    should_notify=False,
                    reason=str((event.metadata or {}).get("downstream_observation_reason") or (getattr(behavior_case, "metadata", {}) or {}).get("current_followup_reason") or ("downstream_anchor_opened" if bool(case_result.get("created")) else "downstream_case_updated")),
                    event=event,
                )
        except Exception as e:
            print(f"case update 归档失败: {e}")

        archive_status["case_update"] = bool(archive_status.get("case_update") or wrote)

    def _archive_case_followup(self, behavior_case, signal, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None or behavior_case is None:
            return

        wrote_followup = False
        try:
            signal_followup = {
                "status": behavior_case.status,
                "stage": behavior_case.stage,
            }
            signal_id = str(getattr(signal, "signal_id", "") or "")
            signal_type = str(getattr(signal, "type", "") or "")
            signal_confidence = getattr(signal, "confidence", None)
            if signal_id:
                signal_followup["signal_id"] = signal_id
            if signal_type:
                signal_followup["signal_type"] = signal_type
            if signal_confidence not in (None, ""):
                signal_followup["confidence"] = float(signal_confidence or 0.0)
            self.archive_store.write_case_update(
                behavior_case,
                signal=signal,
                action="signal_attached",
                archive_ts=archive_ts,
            )
            if self._is_downstream_followup_case(behavior_case):
                self._archive_downstream_case_followup(
                    behavior_case=behavior_case,
                    archive_status=archive_status,
                    archive_ts=archive_ts,
                    status=str(getattr(behavior_case, "status", "") or ""),
                    stage=str(getattr(behavior_case, "stage", "") or ""),
                    should_notify=True,
                    reason="signal_attached",
                    signal=signal,
                )
                wrote_followup = True
            else:
                wrote_followup = bool(
                    self.archive_store.write_case_followup(
                        behavior_case.case_id,
                        signal_followup,
                        archive_ts=archive_ts,
                    )
                )
        except Exception as e:
            print(f"case followup 归档失败: {e}")

        archive_status["case_followup"] = bool(archive_status.get("case_followup") or wrote_followup)

    def _archive_case_decision_followup(
        self,
        behavior_case,
        event: Event,
        signal,
        archive_status: dict,
        archive_ts: int,
    ) -> None:
        if behavior_case is None:
            return
        reason = str(
            (event.metadata or {}).get("case_notification_reason")
            or getattr(signal, "delivery_reason", "")
            or getattr(event, "delivery_reason", "")
            or ""
        )
        self._archive_downstream_case_followup(
            behavior_case=behavior_case,
            archive_status=archive_status,
            archive_ts=archive_ts,
            status=str(getattr(behavior_case, "status", "") or ""),
            stage=str((event.metadata or {}).get("downstream_followup_stage") or getattr(behavior_case, "stage", "") or ""),
            should_notify=False,
            reason=reason,
            event=event,
            signal=signal,
        )

    def _archive_downstream_case_followup(
        self,
        behavior_case,
        archive_status: dict,
        archive_ts: int,
        status: str,
        stage: str,
        should_notify: bool,
        reason: str,
        event: Event | None = None,
        signal=None,
    ) -> None:
        if self.archive_store is None or behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return
        metadata = getattr(behavior_case, "metadata", {}) or {}
        followup = {
            "case_family": str(metadata.get("case_family") or ""),
            "stage": str(stage or getattr(behavior_case, "stage", "") or ""),
            "status": str(status or getattr(behavior_case, "status", "") or ""),
            "anchor_watch_address": str(metadata.get("anchor_watch_address") or ""),
            "downstream_bridge_candidate": bool(metadata.get("downstream_bridge_candidate")),
            "downstream_bridge_applied": bool(metadata.get("downstream_bridge_applied")),
            "downstream_bridge_reason": str(metadata.get("downstream_bridge_reason") or ""),
            "downstream_bridge_anchor_source": str(metadata.get("downstream_bridge_anchor_source") or ""),
            "downstream_bridge_counterparty_role": str(metadata.get("downstream_bridge_counterparty_role") or ""),
            "downstream_bridge_counterparty_label": str(metadata.get("downstream_bridge_counterparty_label") or ""),
            "downstream_case_anchor_watch_address": str(metadata.get("downstream_case_anchor_watch_address") or ""),
            "downstream_case_match_mode": str(metadata.get("downstream_case_match_mode") or ""),
            "downstream_case_reused": metadata.get("downstream_case_reused"),
            "anchor_label": str(metadata.get("anchor_label") or ""),
            "downstream_address": str(metadata.get("downstream_address") or getattr(behavior_case, "watch_address", "") or ""),
            "downstream_label": str(metadata.get("downstream_label") or ""),
            "anchor_tx_hash": str(metadata.get("root_tx_hash") or getattr(behavior_case, "root_tx_hash", "") or ""),
            "followup_type": str(
                metadata.get("current_followup_type")
                or metadata.get("last_followup_type")
                or (event.metadata.get("downstream_followup_type") if event is not None else "")
                or ""
            ),
            "usd_value": round(
                float(
                    (event.usd_value if event is not None and event.usd_value is not None else 0.0)
                    or metadata.get("last_followup_usd")
                    or metadata.get("anchor_usd_value")
                    or 0.0
                ),
                2,
            ),
            "should_notify": bool(should_notify),
            "reason": str(reason or metadata.get("current_followup_reason") or metadata.get("lifecycle_reason") or ""),
            "hop": int(metadata.get("hop") or 1),
            "window_sec": int(metadata.get("window_sec") or 0),
            "downstream_notification_cap": metadata.get("downstream_notification_cap"),
            "downstream_notification_count": metadata.get("downstream_notification_count"),
            "downstream_notification_cap_source": str(metadata.get("downstream_notification_cap_source") or ""),
            "delivery_class": str(getattr(signal, "delivery_class", "") or (event.delivery_class if event is not None else "") or ""),
        }
        signal_id = str(getattr(signal, "signal_id", "") or "")
        if signal_id:
            followup["signal_id"] = signal_id
        try:
            wrote = bool(
                self.archive_store.write_case_followup(
                    str(getattr(behavior_case, "case_id", "") or ""),
                    followup,
                    archive_ts=archive_ts,
                )
            )
            archive_status["case_followup"] = bool(archive_status.get("case_followup") or wrote)
        except Exception as e:
            print(f"downstream case followup 归档失败: {e}")

    def _archive_non_primary_signal(
        self,
        event: Event,
        signal,
        reason: str,
        gate_metrics: dict,
        behavior: dict | None,
        archive_status: dict,
        archive_ts: int,
        stage: str = "strategy",
        ) -> None:
        self._archive_signal_state(
            signal,
            event,
            archive_status,
            archive_ts,
            sent_to_telegram=False,
            delivery_decision=str(reason or getattr(signal, "delivery_reason", "") or getattr(signal, "delivery_class", "") or ""),
        )
        self._archive_delivery_audit(
            event=event,
            signal=signal,
            behavior=behavior,
            gate_metrics=gate_metrics,
            stage=stage,
            gate_reason=reason,
            archive_status=archive_status,
            archive_ts=archive_ts,
        )
        self._archive_case_decision_followup(
            behavior_case=self.followup_tracker.get_case(getattr(signal, "case_id", "")) if self.followup_tracker and getattr(signal, "case_id", "") else None,
            event=event,
            signal=signal,
            archive_status=archive_status,
            archive_ts=archive_ts,
        )

    def _delivery_policy_reason(self, event: Event, signal, allowed: bool) -> str:
        event_metadata = getattr(event, "metadata", {}) or {}
        signal_metadata = getattr(signal, "metadata", {}) or {}
        signal_context = getattr(signal, "context", {}) or {}
        case_family = str(
            event_metadata.get("case_family")
            or signal_metadata.get("case_family")
            or signal_context.get("case_family")
            or ""
        ).strip()
        if case_family == "downstream_counterparty_followup":
            return "delivery_policy_downstream_observe_allowed" if allowed else "delivery_policy_downstream_observe_disabled"

        liquidation_stage = str(
            event_metadata.get("liquidation_stage")
            or signal_metadata.get("liquidation_stage")
            or signal_context.get("liquidation_stage")
            or ""
        ).strip()
        if liquidation_stage in {"risk", "execution"}:
            return "delivery_policy_liquidation_observe_allowed" if allowed else "delivery_policy_liquidation_observe_disabled"

        role_group = str(
            signal_metadata.get("role_group")
            or event_metadata.get("role_group")
            or signal_context.get("role_group")
            or strategy_role_group(
                getattr(event, "strategy_role", "")
                or signal_metadata.get("strategy_role")
                or ""
            )
            or ""
        ).strip()
        if role_group == "smart_money":
            return str(
                event_metadata.get("smart_money_delivery_policy_reason")
                or signal_metadata.get("smart_money_delivery_policy_reason")
                or signal_context.get("smart_money_delivery_policy_reason")
                or ("smart_money_execution_only_allowed" if allowed else "smart_money_execution_only_blocked")
            )
        if role_group == "market_maker":
            return str(
                event_metadata.get("market_maker_delivery_policy_reason")
                or signal_metadata.get("market_maker_delivery_policy_reason")
                or signal_context.get("market_maker_delivery_policy_reason")
                or ("market_maker_execution_only_allowed" if allowed else "market_maker_execution_only_blocked")
            )
        user_tier_reason = str(
            event_metadata.get("user_tier_delivery_reason")
            or signal_metadata.get("user_tier_delivery_reason")
            or signal_context.get("user_tier_delivery_reason")
            or ""
        ).strip()
        if user_tier_reason and not bool(
            event_metadata.get("user_tier_delivery_allowed")
            or signal_metadata.get("user_tier_delivery_allowed")
            or signal_context.get("user_tier_delivery_allowed")
        ):
            return user_tier_reason
        stage_budget_reason = str(
            event_metadata.get("stage_budget_reason")
            or signal_metadata.get("stage_budget_reason")
            or signal_context.get("stage_budget_reason")
            or ""
        ).strip()
        if stage_budget_reason:
            return stage_budget_reason

        delivery_class = str(
            getattr(signal, "delivery_class", "")
            or getattr(event, "delivery_class", "")
            or ""
        ).strip()
        if delivery_class == "primary":
            return "delivery_policy_primary_allowed" if allowed else "delivery_policy_primary_blocked"
        if delivery_class != "observe":
            return "delivery_policy_non_emittable_delivery_class"
        if role_group == "lp_pool":
            return "delivery_policy_lp_observe_allowed" if allowed else "delivery_policy_lp_observe_disabled"
        if role_group == "exchange":
            strong_allowed = bool(
                event_metadata.get("exchange_strong_observe_allowed")
                or signal_metadata.get("exchange_strong_observe_allowed")
                or signal_context.get("exchange_strong_observe_allowed")
            )
            strong_reason = str(
                event_metadata.get("exchange_strong_observe_reason")
                or signal_metadata.get("exchange_strong_observe_reason")
                or signal_context.get("exchange_strong_observe_reason")
                or ""
            ).strip()
            if allowed and strong_allowed:
                return strong_reason or "strong_exchange_observe_allowed"
            if allowed:
                return "delivery_policy_exchange_observe_allowed"
            return strong_reason or "delivery_policy_exchange_observe_disabled"
        return "delivery_policy_observe_not_supported"

    def _apply_delivery_policy_state(
        self,
        event: Event,
        signal,
        allowed: bool,
        reason: str,
        evaluated: bool = True,
        evaluated_at_stage: str = "pipeline_pre_send",
    ) -> dict:
        payload = {
            "delivery_policy_evaluated": bool(evaluated),
            "delivery_policy_allowed": bool(allowed),
            "delivery_policy_reason": str(reason or ""),
            "delivery_policy_evaluated_at_stage": str(evaluated_at_stage or ""),
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _apply_cooldown_state(
        self,
        event: Event,
        signal=None,
        allowed: bool = True,
        reason: str = "",
    ) -> dict:
        payload = {
            "cooldown_allowed": bool(allowed),
            "cooldown_reason": str(reason or ""),
        }
        if str(event.strategy_role or "") == "lp_pool" and str(event.intent_type or "") in {"pool_buy_pressure", "pool_sell_pressure"}:
            existing = event.metadata or {}
            payload.update({
                "lp_directional_cooldown_key": str(
                    existing.get("lp_directional_cooldown_key")
                    or getattr(signal, "metadata", {}).get("lp_directional_cooldown_key")
                    or ""
                ),
                "lp_directional_cooldown_sec": int(
                    existing.get("lp_directional_cooldown_sec")
                    or getattr(signal, "metadata", {}).get("lp_directional_cooldown_sec")
                    or 0
                ),
                "lp_directional_cooldown_allowed": bool(allowed),
            })
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _apply_downstream_impact_gate_state(
        self,
        event: Event,
        signal=None,
        allowed: bool = True,
        reason: str = "",
    ) -> dict:
        payload = {
            "downstream_impact_gate_allowed": bool(allowed),
            "downstream_impact_gate_reason": str(reason or ""),
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _apply_downstream_case_history_metadata(
        self,
        event: Event | None = None,
        signal=None,
        behavior_case=None,
    ) -> dict:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return {}
        metadata = getattr(behavior_case, "metadata", {}) or {}
        emitted_stages = list(metadata.get("emitted_notification_stages") or [])
        payload = {
            "emitted_stages": emitted_stages,
            "emitted_notification_stages": emitted_stages,
            "emitted_notification_count": int(metadata.get("emitted_notification_count") or len(emitted_stages)),
            "downstream_notification_cap": int(metadata.get("downstream_notification_cap") or metadata.get("max_notifications") or 0),
            "downstream_notification_count": int(
                metadata.get("downstream_notification_count")
                or sum(1 for emitted_stage in emitted_stages if emitted_stage != "followup_opened")
            ),
            "downstream_notification_cap_source": str(
                metadata.get("downstream_notification_cap_source") or "DOWNSTREAM_FOLLOWUP_MAX_NOTIFICATIONS"
            ),
            "last_notification_stage": str(metadata.get("last_notification_stage") or ""),
            "last_notification_signal_id": str(metadata.get("last_notification_signal_id") or ""),
            "downstream_early_warning_emitted": bool(metadata.get("downstream_early_warning_emitted")),
            "downstream_early_warning_emitted_count": int(metadata.get("downstream_early_warning_emitted_count") or 0),
            "downstream_early_warning_signal_id": str(metadata.get("downstream_early_warning_signal_id") or ""),
            "downstream_early_warning_stage_recorded": bool(
                metadata.get("downstream_early_warning_stage_recorded")
                or "followup_opened" in emitted_stages
            ),
            "emitted_notification_history_version": int(metadata.get("emitted_notification_history_version") or 2),
            "emitted_notification_stage_source": str(metadata.get("emitted_notification_stage_source") or "unified_case_history"),
        }
        metadata.update({
            "downstream_early_warning_stage_recorded": payload["downstream_early_warning_stage_recorded"],
            "emitted_notification_count": payload["emitted_notification_count"],
            "downstream_notification_cap": payload["downstream_notification_cap"],
            "downstream_notification_count": payload["downstream_notification_count"],
            "downstream_notification_cap_source": payload["downstream_notification_cap_source"],
            "emitted_notification_history_version": payload["emitted_notification_history_version"],
            "emitted_notification_stage_source": payload["emitted_notification_stage_source"],
        })
        if event is not None:
            event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _normalize_downstream_current_event_state(
        self,
        state: dict | None,
        *,
        fallback_stage: str = "",
    ) -> dict:
        state = state or {}
        current_event_is_anchor = bool(state.get("current_event_is_anchor"))
        current_event_is_followup = bool(state.get("current_event_is_followup"))
        state_missing = bool(state.get("downstream_current_event_state_missing"))
        semantics = str(state.get("downstream_current_event_state_semantics") or "")
        if not semantics:
            if state_missing:
                semantics = "missing_fail_closed"
            elif current_event_is_anchor:
                semantics = "explicit_anchor"
            elif current_event_is_followup:
                semantics = "explicit_followup"
            else:
                semantics = "explicit_non_anchor_non_followup"
        state_missing = bool(state_missing or semantics == "missing_fail_closed")
        state_known = bool(
            state.get("downstream_current_event_state_known")
            if "downstream_current_event_state_known" in state
            else semantics != "missing_fail_closed"
        )
        compat_bool_source = str(
            state.get("downstream_current_event_state_compat_bool_source")
            or ("semantic_overlay" if semantics == "missing_fail_closed" else "legacy_bool_fields")
        )
        effective_label = str(
            state.get("downstream_current_event_state_effective_label")
            or (
                "unknown_fail_closed"
                if semantics == "missing_fail_closed"
                else "anchor"
                if semantics == "explicit_anchor"
                else "followup"
                if semantics == "explicit_followup"
                else "non_anchor_non_followup"
            )
        )
        effective_bool_safe = state.get("downstream_current_event_state_effective_bool_safe")
        if effective_bool_safe in (None, ""):
            effective_bool_safe = "unknown" if semantics == "missing_fail_closed" else bool(
                current_event_is_anchor or current_event_is_followup
            )
        return {
            "current_event_is_anchor": current_event_is_anchor,
            "current_event_is_followup": current_event_is_followup,
            "current_followup_type": str(state.get("current_followup_type") or ""),
            "current_followup_reason": str(state.get("current_followup_reason") or ""),
            "current_stage": str(state.get("current_stage") or fallback_stage or ""),
            "downstream_current_event_state_semantics": semantics,
            "downstream_current_event_state_known": state_known,
            "downstream_current_event_state_missing": state_missing,
            "downstream_current_event_state_fail_closed": bool(
                state.get("downstream_current_event_state_fail_closed")
                or semantics == "missing_fail_closed"
            ),
            "downstream_current_event_state_reason": str(
                state.get("downstream_current_event_state_reason")
                or state.get("current_followup_reason")
                or ""
            ),
            "downstream_current_event_state_compat_bool_source": compat_bool_source,
            "downstream_current_event_state_effective_label": effective_label,
            "downstream_current_event_state_effective_bool_safe": effective_bool_safe,
        }

    def _apply_downstream_current_event_state_metadata(
        self,
        event: Event,
        signal=None,
        behavior_case=None,
        case_result: dict | None = None,
    ) -> dict:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return {}

        metadata = getattr(behavior_case, "metadata", {}) or {}
        fallback_stage = str(metadata.get("current_stage") or getattr(behavior_case, "stage", "") or "")
        case_state = self._normalize_downstream_current_event_state(
            metadata,
            fallback_stage=fallback_stage,
        )
        for key in (
            "downstream_current_event_state_semantics",
            "downstream_current_event_state_known",
            "downstream_current_event_state_missing",
            "downstream_current_event_state_fail_closed",
            "downstream_current_event_state_reason",
            "downstream_current_event_state_compat_bool_source",
            "downstream_current_event_state_effective_label",
            "downstream_current_event_state_effective_bool_safe",
        ):
            if metadata.get(key) != case_state.get(key):
                metadata[key] = case_state.get(key)
        matched_state_raw = (case_result or {}).get("downstream_event_state")
        matched_state = None
        state_source = str(metadata.get("downstream_current_event_state_source") or "case_metadata")
        if matched_state_raw:
            matched_state = self._normalize_downstream_current_event_state(
                matched_state_raw,
                fallback_stage=fallback_stage,
            )
            if matched_state != case_state:
                metadata.update(matched_state)
                metadata["downstream_current_event_state_source"] = "pipeline_synced_case_state"
                metadata["downstream_current_event_state_missing"] = False
                metadata["downstream_current_event_state_fail_closed"] = False
                metadata["downstream_current_event_state_reason"] = str(
                    matched_state.get("current_followup_reason") or "pipeline_synced_case_state"
                )
                case_state = self._normalize_downstream_current_event_state(
                    metadata,
                    fallback_stage=fallback_stage,
                )
                state_source = "pipeline_synced_case_state"

        event_anchor_flag = bool(case_state.get("current_event_is_anchor"))
        event_followup_flag = bool(case_state.get("current_event_is_followup"))
        payload = {
            "current_event_is_anchor": event_anchor_flag,
            "current_event_is_followup": event_followup_flag,
            "current_followup_type": str(case_state.get("current_followup_type") or ""),
            "current_followup_reason": str(case_state.get("current_followup_reason") or ""),
            "current_stage": str(case_state.get("current_stage") or fallback_stage or ""),
            "downstream_current_event_state_source": state_source,
            "downstream_current_event_state_semantics": str(case_state.get("downstream_current_event_state_semantics") or ""),
            "downstream_current_event_state_known": bool(case_state.get("downstream_current_event_state_known")),
            "downstream_current_event_state_missing": bool(case_state.get("downstream_current_event_state_missing")),
            "downstream_current_event_state_fail_closed": bool(case_state.get("downstream_current_event_state_fail_closed")),
            "downstream_current_event_state_reason": str(case_state.get("downstream_current_event_state_reason") or ""),
            "downstream_current_event_state_compat_bool_source": str(
                case_state.get("downstream_current_event_state_compat_bool_source") or ""
            ),
            "downstream_current_event_state_effective_label": str(
                case_state.get("downstream_current_event_state_effective_label") or ""
            ),
            "downstream_current_event_state_effective_bool_safe": case_state.get(
                "downstream_current_event_state_effective_bool_safe"
            ),
            "downstream_case_anchor_flag": bool(case_state.get("current_event_is_anchor")),
            "downstream_event_anchor_flag": event_anchor_flag,
            "downstream_case_followup_flag": bool(case_state.get("current_event_is_followup")),
            "downstream_event_followup_flag": event_followup_flag,
            "downstream_case_state_consistent": (
                event_anchor_flag == bool(case_state.get("current_event_is_anchor"))
                and event_followup_flag == bool(case_state.get("current_event_is_followup"))
            ),
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _resolve_runtime_adjacent_anchor_state(
        self,
        behavior_case,
        case_result: dict | None = None,
    ) -> dict:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        case_anchor_flag = None
        if "current_event_is_anchor" in metadata:
            case_anchor_flag = bool(metadata.get("current_event_is_anchor"))
        elif "downstream_case_anchor_flag" in metadata:
            case_anchor_flag = bool(metadata.get("downstream_case_anchor_flag"))

        source = "case_metadata"
        anchor_flag = bool(case_anchor_flag) if case_anchor_flag is not None else False
        if case_anchor_flag is None and "downstream_followup_anchor" in (case_result or {}):
            anchor_flag = bool((case_result or {}).get("downstream_followup_anchor"))
            source = "case_result_fallback"

        return {
            "runtime_adjacent_anchor_source": source,
            "runtime_adjacent_anchor_flag": bool(anchor_flag),
            "runtime_adjacent_watch_consistent": (
                bool(anchor_flag) == bool(case_anchor_flag)
                if case_anchor_flag is not None else False
            ),
            "runtime_adjacent_case_id": str(getattr(behavior_case, "case_id", "") or ""),
        }

    def _silent_reason_bucket(self, stage: str, reason_code: str) -> str:
        normalized_stage = str(stage or "").strip().lower()
        normalized_reason = str(reason_code or "").strip().lower()
        if normalized_reason in {
            "smart_money_wait_execution_only",
            "market_maker_wait_execution_only",
        }:
            return "no_chain_evidence"
        if normalized_stage == "prefilter" or normalized_reason in {
            "adjacent_watch_meta_missing",
            "persisted_exchange_adjacent_filtered",
        }:
            return "prefilter_blocked"
        if normalized_stage == "gate":
            return "quality_gate_blocked"
        if normalized_stage in {"adjacent_watch_gate", "impact_gate"} or normalized_reason in {
            "runtime_adjacent_execution_below_threshold",
            "downstream_impact_gate_rejected",
        }:
            return "impact_gate_blocked"
        if normalized_stage == "cooldown" or normalized_reason == "cooldown_suppressed":
            return "cooldown_blocked"
        if normalized_stage == "delivery_policy" or normalized_reason.startswith("delivery_policy_"):
            return "delivery_policy_blocked"
        if normalized_stage == "case_notification":
            return "case_stage_blocked"
        if normalized_stage == "notifier_delivery" or normalized_reason == "notifier_send_failed":
            return "send_failed"
        if normalized_stage in {"interpreter", "strategy"}:
            if normalized_reason == "strategy_rejected":
                return "strategy_blocked"
            if (
                normalized_reason.endswith("_drop")
                or normalized_reason.startswith("low_")
                or normalized_reason.startswith("weak_")
                or normalized_reason.endswith("_wait_execution_only")
                or normalized_reason.startswith("downstream_followup_dropped")
            ):
                return "no_chain_evidence"
            return "strategy_blocked"
        return "strategy_blocked"

    def _default_silent_reason_detail(self, reason_code: str) -> str:
        details = {
            "smart_money_wait_execution_only": "smart money 已识别为非执行事件，execution_only 模式下仅归档，不发送 Telegram。",
            "market_maker_wait_execution_only": "market maker 已识别为非执行事件，execution_only 模式下仅归档，不发送 Telegram。",
            "smart_money_execution_only_requires_execution": "delivery policy 识别到 smart money 缺少真实执行证据，execution_only 模式下不发送。",
            "market_maker_execution_only_requires_execution": "delivery policy 识别到 market maker 缺少真实执行证据，execution_only 模式下不发送。",
            "smart_money_execution_only_reason_not_allowed": "delivery policy 仅允许 smart money / market maker 的真实执行类 reason 进入发送。",
            "market_maker_execution_only_reason_not_allowed": "delivery policy 仅允许 market maker 的真实执行类 reason 进入发送。",
            "smart_money_execution_whitelist_reason_not_allowed": "delivery policy 最终只允许 smart money 执行白名单 reason 进入发送。",
            "market_maker_execution_whitelist_reason_not_allowed": "delivery policy 最终只允许 market maker 执行白名单 reason 进入发送。",
            "smart_money_execution_whitelist_non_emittable_delivery_class": "smart money 已命中执行白名单 reason，但当前 delivery_class 不可发送，因此拦截。",
            "market_maker_execution_whitelist_non_emittable_delivery_class": "market maker 已命中执行白名单 reason，但当前 delivery_class 不可发送，因此拦截。",
            "smart_money_execution_whitelist_requires_execution": "delivery policy 已命中 smart money reason 白名单，但事件缺少真实执行证据，因此不发送。",
            "market_maker_execution_whitelist_requires_execution": "delivery policy 已命中 market maker reason 白名单，但事件缺少真实执行证据，因此不发送。",
            "downstream_event_state_missing": "downstream 当前事件态缺失，本次事件按 fail-closed 处理，不沿用旧的 anchor/followup 状态。",
        }
        return details.get(str(reason_code or ""), str(reason_code or ""))

    def _chain_evidence_strength(
        self,
        event: Event | None = None,
        signal=None,
        gate_metrics: dict | None = None,
        parsed: dict | None = None,
    ) -> str:
        gate_metrics = gate_metrics or {}
        parsed = parsed or {}
        signal_metadata = getattr(signal, "metadata", {}) or {}
        confirmation_score = float(
            (event.confirmation_score if event is not None else 0.0)
            or getattr(signal, "confirmation_score", 0.0)
            or gate_metrics.get("confirmation_score")
            or parsed.get("confirmation_score")
            or 0.0
        )
        quality_score = float(
            getattr(signal, "quality_score", 0.0)
            or gate_metrics.get("adjusted_quality_score")
            or gate_metrics.get("quality_score")
            or parsed.get("quality_score")
            or 0.0
        )
        pricing_confidence = float(
            getattr(signal, "pricing_confidence", 0.0)
            or (event.pricing_confidence if event is not None else 0.0)
            or gate_metrics.get("pricing_confidence")
            or parsed.get("pricing_confidence")
            or 0.0
        )
        resonance_score = float(
            signal_metadata.get("resonance_score")
            or gate_metrics.get("resonance_score")
            or parsed.get("resonance_score")
            or 0.0
        )
        abnormal_ratio = float(
            getattr(signal, "abnormal_ratio", 0.0)
            or gate_metrics.get("abnormal_ratio")
            or parsed.get("abnormal_ratio")
            or 0.0
        )
        strong_hits = sum(
            1
            for matched in (
                confirmation_score >= 0.72,
                quality_score >= 0.84,
                pricing_confidence >= 0.80,
                resonance_score >= 0.45 or abnormal_ratio >= 2.0,
            )
            if matched
        )
        medium_hits = sum(
            1
            for matched in (
                confirmation_score >= 0.58,
                quality_score >= 0.76,
                pricing_confidence >= 0.72,
                resonance_score >= 0.32 or abnormal_ratio >= 1.6,
            )
            if matched
        )
        if strong_hits >= 3 or (
            confirmation_score >= 0.72
            and quality_score >= 0.84
            and pricing_confidence >= 0.80
        ):
            return "strong"
        if medium_hits >= 2:
            return "medium"
        return "weak"

    def _build_silent_reason(
        self,
        *,
        stage: str,
        reason_code: str,
        reason_detail: str | None = None,
        reason_bucket: str | None = None,
        event: Event | None = None,
        signal=None,
        behavior_case=None,
        gate_metrics: dict | None = None,
        watch_meta: dict | None = None,
        parsed: dict | None = None,
        delivery_policy_allowed: bool | None = None,
        impact_gate_allowed: bool | None = None,
        cooldown_allowed: bool | None = None,
        would_have_been_delivery_class: str | None = None,
        would_have_been_message_variant: str | None = None,
    ) -> dict:
        gate_metrics = gate_metrics or {}
        parsed = parsed or {}
        event_metadata = getattr(event, "metadata", {}) or {}
        signal_metadata = getattr(signal, "metadata", {}) or {}
        signal_context = getattr(signal, "context", {}) or {}
        watch_meta = watch_meta or event_metadata.get("watch_meta") or signal_metadata.get("watch_meta") or {}
        case_metadata = getattr(behavior_case, "metadata", {}) or {}

        def _first_non_none(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        bucket = str(reason_bucket or self._silent_reason_bucket(stage, reason_code))
        case_family = str(
            _first_non_none(
                case_metadata.get("case_family"),
                event_metadata.get("case_family"),
                signal_metadata.get("case_family"),
                watch_meta.get("case_family"),
                "",
            )
            or ""
        )
        case_stage = str(
            _first_non_none(
                getattr(behavior_case, "stage", None),
                event_metadata.get("downstream_followup_stage"),
                event_metadata.get("case_notification_stage"),
                signal_metadata.get("case_notification_stage"),
                getattr(event, "followup_stage", None) if event is not None else None,
                watch_meta.get("runtime_state"),
                "",
            )
            or ""
        )
        resolved_delivery_policy_allowed = _first_non_none(
            delivery_policy_allowed,
            event_metadata.get("delivery_policy_allowed"),
            signal_metadata.get("delivery_policy_allowed"),
            signal_context.get("delivery_policy_allowed"),
        )
        resolved_impact_gate_allowed = _first_non_none(
            impact_gate_allowed,
            event_metadata.get("downstream_impact_gate_allowed"),
            signal_metadata.get("downstream_impact_gate_allowed"),
            signal_context.get("downstream_impact_gate_allowed"),
        )
        resolved_cooldown_allowed = _first_non_none(
            cooldown_allowed,
            event_metadata.get("cooldown_allowed"),
            signal_metadata.get("cooldown_allowed"),
            signal_context.get("cooldown_allowed"),
        )
        resolved_delivery_class = str(
            would_have_been_delivery_class
            or getattr(signal, "delivery_class", "")
            or getattr(event, "delivery_class", "") if event is not None else ""
        ).strip()
        resolved_message_variant = str(
            would_have_been_message_variant
            or signal_context.get("message_variant")
            or signal_metadata.get("message_variant")
            or event_metadata.get("message_variant")
            or ""
        ).strip()
        resolved_reason_detail = str(reason_detail or reason_code or "")
        if resolved_reason_detail == str(reason_code or ""):
            resolved_reason_detail = self._default_silent_reason_detail(str(reason_code or ""))
        return {
            "stage": str(stage or ""),
            "reason_code": str(reason_code or ""),
            "reason_detail": resolved_reason_detail,
            "reason_bucket": bucket,
            "would_have_been_delivery_class": resolved_delivery_class,
            "would_have_been_message_variant": resolved_message_variant,
            "chain_evidence_strength": self._chain_evidence_strength(
                event=event,
                signal=signal,
                gate_metrics=gate_metrics,
                parsed=parsed,
            ),
            "case_stage": case_stage,
            "case_family": case_family,
            "delivery_policy_allowed": resolved_delivery_policy_allowed,
            "impact_gate_allowed": resolved_impact_gate_allowed,
            "cooldown_allowed": resolved_cooldown_allowed,
        }

    def _is_shadow_high_value_candidate(
        self,
        silent_reason: dict,
        event: Event | None = None,
        signal=None,
        gate_metrics: dict | None = None,
    ) -> bool:
        if str(silent_reason.get("reason_bucket") or "") not in {
            "delivery_policy_blocked",
            "case_stage_blocked",
            "impact_gate_blocked",
            "cooldown_blocked",
        }:
            return False
        gate_metrics = gate_metrics or {}
        usd_value = float(
            getattr(signal, "usd_value", 0.0)
            or gate_metrics.get("usd_value")
            or (event.usd_value if event is not None else 0.0)
            or 0.0
        )
        dynamic_min_usd = float(
            gate_metrics.get("dynamic_min_usd")
            or getattr(signal, "effective_threshold_usd", 0.0)
            or getattr(signal, "metadata", {}).get("dynamic_min_usd", 0.0)
            or 0.0
        )
        if usd_value < max(dynamic_min_usd * 3.0, 300_000.0):
            return False
        signal_metadata = getattr(signal, "metadata", {}) or {}
        confirmation_score = float(
            (event.confirmation_score if event is not None else 0.0)
            or getattr(signal, "confirmation_score", 0.0)
            or gate_metrics.get("confirmation_score")
            or 0.0
        )
        quality_score = float(
            getattr(signal, "quality_score", 0.0)
            or gate_metrics.get("adjusted_quality_score")
            or gate_metrics.get("quality_score")
            or 0.0
        )
        pricing_confidence = float(
            getattr(signal, "pricing_confidence", 0.0)
            or (event.pricing_confidence if event is not None else 0.0)
            or gate_metrics.get("pricing_confidence")
            or 0.0
        )
        resonance_score = float(signal_metadata.get("resonance_score") or gate_metrics.get("resonance_score") or 0.0)
        abnormal_ratio = float(getattr(signal, "abnormal_ratio", 0.0) or gate_metrics.get("abnormal_ratio") or 0.0)
        strong_hits = sum(
            1
            for matched in (
                confirmation_score >= 0.62,
                quality_score >= 0.80,
                pricing_confidence >= 0.78,
                resonance_score >= 0.40 or abnormal_ratio >= 2.0,
            )
            if matched
        )
        return strong_hits >= 2 or str(silent_reason.get("chain_evidence_strength") or "") == "strong"

    def _apply_silent_reason(
        self,
        *,
        event: Event,
        signal=None,
        stage: str,
        reason_code: str,
        reason_detail: str | None = None,
        reason_bucket: str | None = None,
        behavior_case=None,
        gate_metrics: dict | None = None,
        delivery_policy_allowed: bool | None = None,
        impact_gate_allowed: bool | None = None,
        cooldown_allowed: bool | None = None,
        would_have_been_delivery_class: str | None = None,
        would_have_been_message_variant: str | None = None,
    ) -> dict:
        silent_reason = self._build_silent_reason(
            stage=stage,
            reason_code=reason_code,
            reason_detail=reason_detail,
            reason_bucket=reason_bucket,
            event=event,
            signal=signal,
            behavior_case=behavior_case,
            gate_metrics=gate_metrics,
            delivery_policy_allowed=delivery_policy_allowed,
            impact_gate_allowed=impact_gate_allowed,
            cooldown_allowed=cooldown_allowed,
            would_have_been_delivery_class=would_have_been_delivery_class,
            would_have_been_message_variant=would_have_been_message_variant,
        )
        shadow_high_value_candidate = self._is_shadow_high_value_candidate(
            silent_reason=silent_reason,
            event=event,
            signal=signal,
            gate_metrics=gate_metrics,
        )
        payload = {
            "silent_reason": silent_reason,
            "silent_reason_bucket": str(silent_reason.get("reason_bucket") or ""),
            "shadow_high_value_candidate": bool(shadow_high_value_candidate),
            "shadow_candidate_reason": (
                f"{silent_reason['reason_bucket']}:{silent_reason['reason_code']}"
                if shadow_high_value_candidate else ""
            ),
            "shadow_candidate_class": (
                str(silent_reason.get("would_have_been_delivery_class") or "unknown")
                if shadow_high_value_candidate else ""
            ),
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _delivery_audit_record(
        self,
        event: Event,
        signal=None,
        behavior: dict | None = None,
        gate_metrics: dict | None = None,
        stage: str = "strategy",
        gate_reason: str = "",
        archive_ts: int | None = None,
        audit_extras: dict | None = None,
    ) -> dict:
        behavior = behavior or {}
        gate_metrics = gate_metrics or dict(getattr(signal, "metadata", {}).get("gate") or {})
        signal_metadata = getattr(signal, "metadata", {}) or {}
        signal_context = getattr(signal, "context", {}) or {}
        event_metadata = event.metadata or {}
        raw = event_metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        lp_analysis = event_metadata.get("lp_analysis") or {}
        watch_meta = event_metadata.get("watch_meta") or {}
        pricing = event_metadata.get("pricing") or {}
        liquidation = event_metadata.get("liquidation") or {}
        behavior_case = None
        if self.followup_tracker is not None:
            signal_case_id = str(getattr(signal, "case_id", "") or "")
            event_case_id = str(event_metadata.get("downstream_case_id") or event.case_id or "")
            for candidate_case_id in (signal_case_id, event_case_id):
                if not candidate_case_id:
                    continue
                behavior_case = self.followup_tracker.get_case(candidate_case_id)
                if behavior_case is not None:
                    break
        case_metadata = getattr(behavior_case, "metadata", {}) or {}

        def _first_value(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        def _num(*values, digits: int = 3):
            value = _first_value(*values)
            if value is None or value == "":
                return None
            try:
                return round(float(value), digits)
            except (TypeError, ValueError):
                return None

        def _int_value(*values):
            value = _first_value(*values)
            if value is None or value == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return None

        def _text(*values):
            value = _first_value(*values)
            if value is None:
                return ""
            return str(value)

        def _bool_value(*values):
            value = _first_value(*values)
            if value is None:
                return False
            return bool(value)

        def _bool_or_none(*values):
            value = _first_value(*values)
            if value is None or value == "":
                return None
            return bool(value)

        usd_value = _num(
            getattr(signal, "usd_value", None),
            gate_metrics.get("usd_value"),
            event.usd_value,
            digits=2,
        )
        dynamic_min_usd = _num(
            gate_metrics.get("dynamic_min_usd"),
            signal_metadata.get("dynamic_min_usd"),
            digits=2,
        )
        base_threshold_usd = _num(
            signal_metadata.get("base_threshold_usd"),
            gate_metrics.get("base_threshold_usd"),
            digits=2,
        )
        effective_threshold_usd = _num(
            getattr(signal, "effective_threshold_usd", None),
            signal_metadata.get("effective_threshold_usd"),
            gate_metrics.get("effective_threshold_usd"),
            dynamic_min_usd,
            digits=2,
        )
        below_min_usd_gap = None
        threshold_ratio = None
        if usd_value is not None and dynamic_min_usd is not None:
            below_min_usd_gap = round(max(dynamic_min_usd - usd_value, 0.0), 2)
            threshold_ratio = round((usd_value / dynamic_min_usd) if dynamic_min_usd > 0 else 0.0, 3)

        lp_event = bool(_bool_value(
            gate_metrics.get("lp_event"),
            signal_context.get("lp_event"),
            self._is_lp_event(event=event),
        ))
        counterparty = _text(
            raw.get("counterparty"),
            event_metadata.get("counterparty"),
        )
        payload = {
            "signal_id": _text(getattr(signal, "signal_id", "")),
            "event_id": _text(event.event_id),
            "tx_hash": _text(event.tx_hash),
            "asset_case_id": _text(
                signal_context.get("asset_case_id"),
                signal_metadata.get("asset_case_id"),
                event_metadata.get("asset_case_id"),
            ),
            "watch_address": _text(event.address),
            "strategy_role": _text(
                signal_metadata.get("strategy_role"),
                gate_metrics.get("strategy_role"),
                event.strategy_role,
            ),
            "role_group": _text(
                signal_metadata.get("role_group"),
                gate_metrics.get("role_group"),
                strategy_role_group(event.strategy_role),
            ),
            "intent_type": _text(getattr(signal, "intent_type", None), gate_metrics.get("intent_type"), event.intent_type),
            "behavior_type": _text(
                getattr(signal, "behavior_type", None),
                gate_metrics.get("behavior_type"),
                behavior.get("behavior_type"),
                "unknown",
            ),
            "gate_reason": _text(gate_reason, getattr(signal, "delivery_reason", None), event.delivery_reason),
            "delivery_class": _text(getattr(signal, "delivery_class", None), event.delivery_class, "drop"),
            "delivery_reason": _text(getattr(signal, "delivery_reason", None), event.delivery_reason),
            "message_variant": _text(
                signal_context.get("message_variant"),
                signal_metadata.get("message_variant"),
                event_metadata.get("message_variant"),
            ),
            "message_template": _text(
                signal_context.get("message_template"),
                signal_metadata.get("message_template"),
                event_metadata.get("message_template"),
            ),
            "notifier_sent_at": _int_value(
                signal_context.get("notifier_sent_at"),
                signal_metadata.get("notifier_sent_at"),
                event_metadata.get("notifier_sent_at"),
            ),
            "market_context_requested_symbol": _text(
                signal_context.get("market_context_requested_symbol"),
                signal_metadata.get("market_context_requested_symbol"),
                event_metadata.get("market_context_requested_symbol"),
            ),
            "market_context_resolved_symbol": _text(
                signal_context.get("market_context_resolved_symbol"),
                signal_metadata.get("market_context_resolved_symbol"),
                event_metadata.get("market_context_resolved_symbol"),
            ),
            "market_context_failure_reason": _text(
                signal_context.get("market_context_failure_reason"),
                signal_metadata.get("market_context_failure_reason"),
                event_metadata.get("market_context_failure_reason"),
            ),
            "market_context_endpoint": _text(
                signal_context.get("market_context_endpoint"),
                signal_metadata.get("market_context_endpoint"),
                event_metadata.get("market_context_endpoint"),
            ),
            "market_context_http_status": _int_value(
                signal_context.get("market_context_http_status"),
                signal_metadata.get("market_context_http_status"),
                event_metadata.get("market_context_http_status"),
            ),
            "market_context_latency_ms": _int_value(
                signal_context.get("market_context_latency_ms"),
                signal_metadata.get("market_context_latency_ms"),
                event_metadata.get("market_context_latency_ms"),
            ),
            "asset_market_state_key": _text(
                signal_context.get("asset_market_state_key"),
                signal_metadata.get("asset_market_state_key"),
                event_metadata.get("asset_market_state_key"),
            ),
            "asset_market_state_label": _text(
                signal_context.get("asset_market_state_label"),
                signal_metadata.get("asset_market_state_label"),
                event_metadata.get("asset_market_state_label"),
            ),
            "asset_market_state_changed": _bool_value(
                signal_context.get("asset_market_state_changed"),
                signal_metadata.get("asset_market_state_changed"),
                event_metadata.get("asset_market_state_changed"),
            ),
            "previous_asset_market_state_key": _text(
                signal_context.get("previous_asset_market_state_key"),
                signal_metadata.get("previous_asset_market_state_key"),
                event_metadata.get("previous_asset_market_state_key"),
            ),
            "no_trade_lock_active": _bool_value(
                signal_context.get("no_trade_lock_active"),
                signal_metadata.get("no_trade_lock_active"),
                event_metadata.get("no_trade_lock_active"),
            ),
            "no_trade_lock_reason": _text(
                signal_context.get("no_trade_lock_reason"),
                signal_metadata.get("no_trade_lock_reason"),
                event_metadata.get("no_trade_lock_reason"),
            ),
            "no_trade_lock_conflict_score": _num(
                signal_context.get("no_trade_lock_conflict_score"),
                signal_metadata.get("no_trade_lock_conflict_score"),
                event_metadata.get("no_trade_lock_conflict_score"),
            ),
            "prealert_lifecycle_state": _text(
                signal_context.get("prealert_lifecycle_state"),
                signal_metadata.get("prealert_lifecycle_state"),
                event_metadata.get("prealert_lifecycle_state"),
            ),
            "prealert_to_confirm_sec": _int_value(
                signal_context.get("prealert_to_confirm_sec"),
                signal_metadata.get("prealert_to_confirm_sec"),
                event_metadata.get("prealert_to_confirm_sec"),
            ),
            "telegram_should_send": _bool_value(
                signal_context.get("telegram_should_send"),
                signal_metadata.get("telegram_should_send"),
                event_metadata.get("telegram_should_send"),
            ),
            "telegram_suppression_reason": _text(
                signal_context.get("telegram_suppression_reason"),
                signal_metadata.get("telegram_suppression_reason"),
                event_metadata.get("telegram_suppression_reason"),
            ),
            "telegram_update_kind": _text(
                signal_context.get("telegram_update_kind"),
                signal_metadata.get("telegram_update_kind"),
                event_metadata.get("telegram_update_kind"),
            ),
            "final_trading_output_source": _text(
                signal_context.get("final_trading_output_source"),
                signal_metadata.get("final_trading_output_source"),
                event_metadata.get("final_trading_output_source"),
            ),
            "final_trading_output_label": _text(
                signal_context.get("final_trading_output_label"),
                signal_metadata.get("final_trading_output_label"),
                event_metadata.get("final_trading_output_label"),
            ),
            "final_trading_output_allowed": _bool_value(
                signal_context.get("final_trading_output_allowed"),
                signal_metadata.get("final_trading_output_allowed"),
                event_metadata.get("final_trading_output_allowed"),
            ),
            "legacy_chase_downgraded": _bool_value(
                signal_context.get("legacy_chase_downgraded"),
                signal_metadata.get("legacy_chase_downgraded"),
                event_metadata.get("legacy_chase_downgraded"),
            ),
            "legacy_chase_downgrade_reason": _text(
                signal_context.get("legacy_chase_downgrade_reason"),
                signal_metadata.get("legacy_chase_downgrade_reason"),
                event_metadata.get("legacy_chase_downgrade_reason"),
            ),
            "opportunity_gate_required": _bool_value(
                signal_context.get("opportunity_gate_required"),
                signal_metadata.get("opportunity_gate_required"),
                event_metadata.get("opportunity_gate_required"),
            ),
            "opportunity_gate_passed": _bool_value(
                signal_context.get("opportunity_gate_passed"),
                signal_metadata.get("opportunity_gate_passed"),
                event_metadata.get("opportunity_gate_passed"),
            ),
            "opportunity_gate_failure_reason": _text(
                signal_context.get("opportunity_gate_failure_reason"),
                signal_metadata.get("opportunity_gate_failure_reason"),
                event_metadata.get("opportunity_gate_failure_reason"),
            ),
            "lp_confirm_scope": _text(
                signal_context.get("lp_confirm_scope"),
                signal_metadata.get("lp_confirm_scope"),
                event_metadata.get("lp_confirm_scope"),
            ),
            "delivery_policy_evaluated": _bool_value(
                event_metadata.get("delivery_policy_evaluated"),
                signal_metadata.get("delivery_policy_evaluated"),
                signal_context.get("delivery_policy_evaluated"),
            ),
            "delivery_policy_allowed": _bool_or_none(
                event_metadata.get("delivery_policy_allowed"),
                signal_metadata.get("delivery_policy_allowed"),
                signal_context.get("delivery_policy_allowed"),
            ),
            "delivery_policy_reason": _text(
                event_metadata.get("delivery_policy_reason"),
                signal_metadata.get("delivery_policy_reason"),
                signal_context.get("delivery_policy_reason"),
            ),
            "delivery_policy_evaluated_at_stage": _text(
                event_metadata.get("delivery_policy_evaluated_at_stage"),
                signal_metadata.get("delivery_policy_evaluated_at_stage"),
                signal_context.get("delivery_policy_evaluated_at_stage"),
            ),
            "role_priority_tier": _text(
                event_metadata.get("role_priority_tier"),
                signal_metadata.get("role_priority_tier"),
                signal_context.get("role_priority_tier"),
            ),
            "role_priority_rank": _int_value(
                event_metadata.get("role_priority_rank"),
                signal_metadata.get("role_priority_rank"),
                signal_context.get("role_priority_rank"),
            ),
            "role_priority_label": _text(
                event_metadata.get("role_priority_label"),
                signal_metadata.get("role_priority_label"),
                signal_context.get("role_priority_label"),
            ),
            "stage_tier": _text(
                event_metadata.get("stage_tier"),
                signal_metadata.get("stage_tier"),
                signal_context.get("stage_tier"),
            ),
            "routing_reason": _text(
                event_metadata.get("routing_reason"),
                signal_metadata.get("routing_reason"),
                signal_context.get("routing_reason"),
            ),
            "observe_route_reason": _text(
                event_metadata.get("observe_route_reason"),
                signal_metadata.get("observe_route_reason"),
                signal_context.get("observe_route_reason"),
            ),
            "primary_route_reason": _text(
                event_metadata.get("primary_route_reason"),
                signal_metadata.get("primary_route_reason"),
                signal_context.get("primary_route_reason"),
            ),
            "archive_only_reason": _text(
                event_metadata.get("archive_only_reason"),
                signal_metadata.get("archive_only_reason"),
                signal_context.get("archive_only_reason"),
            ),
            "strategy_reject_reason": _text(
                event_metadata.get("strategy_reject_reason"),
                signal_metadata.get("strategy_reject_reason"),
                signal_context.get("strategy_reject_reason"),
            ),
            "observe_candidate_reason": _text(
                event_metadata.get("observe_candidate_reason"),
                signal_metadata.get("observe_candidate_reason"),
                signal_context.get("observe_candidate_reason"),
            ),
            "observe_relaxed_by_role": _text(
                event_metadata.get("observe_relaxed_by_role"),
                signal_metadata.get("observe_relaxed_by_role"),
                signal_context.get("observe_relaxed_by_role"),
            ),
            "gate_relaxed_by_role": _text(
                event_metadata.get("gate_relaxed_by_role"),
                signal_metadata.get("gate_relaxed_by_role"),
                signal_context.get("gate_relaxed_by_role"),
                gate_metrics.get("gate_relaxed_by_role"),
            ),
            "promotion_path": _text(
                event_metadata.get("promotion_path"),
                signal_metadata.get("promotion_path"),
                signal_context.get("promotion_path"),
            ),
            "relaxed_thresholds_applied": _bool_value(
                event_metadata.get("relaxed_thresholds_applied"),
                signal_metadata.get("relaxed_thresholds_applied"),
                signal_context.get("relaxed_thresholds_applied"),
            ),
            "relaxed_threshold_details": _first_value(
                event_metadata.get("relaxed_threshold_details"),
                signal_metadata.get("relaxed_threshold_details"),
                signal_context.get("relaxed_threshold_details"),
            ) or {},
            "observe_threshold_profile": _first_value(
                event_metadata.get("observe_threshold_profile"),
                signal_metadata.get("observe_threshold_profile"),
                signal_context.get("observe_threshold_profile"),
            ) or {},
            "primary_threshold_profile": _first_value(
                event_metadata.get("primary_threshold_profile"),
                signal_metadata.get("primary_threshold_profile"),
                signal_context.get("primary_threshold_profile"),
            ) or {},
            "stage_budget_evaluated": _bool_value(
                event_metadata.get("stage_budget_evaluated"),
                signal_metadata.get("stage_budget_evaluated"),
                signal_context.get("stage_budget_evaluated"),
            ),
            "stage_budget_allowed": _bool_or_none(
                event_metadata.get("stage_budget_allowed"),
                signal_metadata.get("stage_budget_allowed"),
                signal_context.get("stage_budget_allowed"),
            ),
            "stage_budget_reason": _text(
                event_metadata.get("stage_budget_reason"),
                signal_metadata.get("stage_budget_reason"),
                signal_context.get("stage_budget_reason"),
            ),
            "stage_budget_stage": _text(
                event_metadata.get("stage_budget_stage"),
                signal_metadata.get("stage_budget_stage"),
                signal_context.get("stage_budget_stage"),
            ),
            "stage_budget_window_sec": _int_value(
                event_metadata.get("stage_budget_window_sec"),
                signal_metadata.get("stage_budget_window_sec"),
                signal_context.get("stage_budget_window_sec"),
            ),
            "stage_budget_role_tier": _text(
                event_metadata.get("stage_budget_role_tier"),
                signal_metadata.get("stage_budget_role_tier"),
                signal_context.get("stage_budget_role_tier"),
            ),
            "stage_budget_recent_total": _int_value(
                event_metadata.get("stage_budget_recent_total"),
                signal_metadata.get("stage_budget_recent_total"),
                signal_context.get("stage_budget_recent_total"),
            ),
            "stage_budget_recent_same_tier": _int_value(
                event_metadata.get("stage_budget_recent_same_tier"),
                signal_metadata.get("stage_budget_recent_same_tier"),
                signal_context.get("stage_budget_recent_same_tier"),
            ),
            "stage_budget_recent_higher_tier": _int_value(
                event_metadata.get("stage_budget_recent_higher_tier"),
                signal_metadata.get("stage_budget_recent_higher_tier"),
                signal_context.get("stage_budget_recent_higher_tier"),
            ),
            "stage_budget_total_cap": _int_value(
                event_metadata.get("stage_budget_total_cap"),
                signal_metadata.get("stage_budget_total_cap"),
                signal_context.get("stage_budget_total_cap"),
            ),
            "stage_budget_tier_cap": _int_value(
                event_metadata.get("stage_budget_tier_cap"),
                signal_metadata.get("stage_budget_tier_cap"),
                signal_context.get("stage_budget_tier_cap"),
            ),
            "stage": _text(stage or "strategy"),
            "chain": _text(event.chain, "ethereum"),
            "event_kind": _text(event.kind),
            "side": _text(event.side),
            "token": _text(event.token),
            "counterparty": counterparty,
            "counterparty_label": _text(event_metadata.get("counterparty_label"), raw.get("counterparty_label")),
            "monitor_type": _text(raw.get("monitor_type"), event_metadata.get("monitor_type")),
            "watch_meta_source": _text(
                signal_metadata.get("watch_meta_source"),
                signal_context.get("watch_meta_source"),
                watch_meta.get("watch_meta_source"),
            ),
            "strategy_hint": _text(
                signal_metadata.get("strategy_hint"),
                signal_context.get("strategy_hint"),
                watch_meta.get("strategy_hint"),
            ),
            "runtime_adjacent_watch": _bool_value(
                signal_metadata.get("runtime_adjacent_watch"),
                signal_context.get("runtime_adjacent_watch"),
                watch_meta.get("runtime_adjacent_watch"),
            ),
            "listener_rpc_mode": _text(
                event_metadata.get("listener_rpc_mode"),
                signal_metadata.get("listener_rpc_mode"),
                signal_context.get("listener_rpc_mode"),
                raw.get("listener_rpc_mode"),
            ),
            "listener_block_fetch_mode": _text(
                event_metadata.get("listener_block_fetch_mode"),
                signal_metadata.get("listener_block_fetch_mode"),
                signal_context.get("listener_block_fetch_mode"),
                raw.get("listener_block_fetch_mode"),
            ),
            "listener_block_fetch_reason": _text(
                event_metadata.get("listener_block_fetch_reason"),
                signal_metadata.get("listener_block_fetch_reason"),
                signal_context.get("listener_block_fetch_reason"),
                raw.get("listener_block_fetch_reason"),
            ),
            "listener_block_get_logs_request_count": _int_value(
                event_metadata.get("listener_block_get_logs_request_count"),
                signal_metadata.get("listener_block_get_logs_request_count"),
                signal_context.get("listener_block_get_logs_request_count"),
                raw.get("listener_block_get_logs_request_count"),
            ),
            "listener_block_topic_chunk_count": _int_value(
                event_metadata.get("listener_block_topic_chunk_count"),
                signal_metadata.get("listener_block_topic_chunk_count"),
                signal_context.get("listener_block_topic_chunk_count"),
                raw.get("listener_block_topic_chunk_count"),
            ),
            "listener_block_monitored_address_count": _int_value(
                event_metadata.get("listener_block_monitored_address_count"),
                signal_metadata.get("listener_block_monitored_address_count"),
                signal_context.get("listener_block_monitored_address_count"),
                raw.get("listener_block_monitored_address_count"),
            ),
            "listener_block_lp_secondary_scan_used": _bool_value(
                event_metadata.get("listener_block_lp_secondary_scan_used"),
                signal_metadata.get("listener_block_lp_secondary_scan_used"),
                signal_context.get("listener_block_lp_secondary_scan_used"),
                raw.get("listener_block_lp_secondary_scan_used"),
            ),
            "listener_block_bloom_prefilter_used": _bool_value(
                event_metadata.get("listener_block_bloom_prefilter_used"),
                signal_metadata.get("listener_block_bloom_prefilter_used"),
                signal_context.get("listener_block_bloom_prefilter_used"),
                raw.get("listener_block_bloom_prefilter_used"),
            ),
            "listener_block_bloom_skipped_get_logs_count": _int_value(
                event_metadata.get("listener_block_bloom_skipped_get_logs_count"),
                signal_metadata.get("listener_block_bloom_skipped_get_logs_count"),
                signal_context.get("listener_block_bloom_skipped_get_logs_count"),
                raw.get("listener_block_bloom_skipped_get_logs_count"),
            ),
            "listener_block_bloom_transfer_possible": _bool_value(
                event_metadata.get("listener_block_bloom_transfer_possible"),
                signal_metadata.get("listener_block_bloom_transfer_possible"),
                signal_context.get("listener_block_bloom_transfer_possible"),
                raw.get("listener_block_bloom_transfer_possible"),
            ),
            "listener_block_bloom_address_possible_count": _int_value(
                event_metadata.get("listener_block_bloom_address_possible_count"),
                signal_metadata.get("listener_block_bloom_address_possible_count"),
                signal_context.get("listener_block_bloom_address_possible_count"),
                raw.get("listener_block_bloom_address_possible_count"),
            ),
            "listener_runtime_adjacent_core_count": _int_value(
                event_metadata.get("listener_runtime_adjacent_core_count"),
                signal_metadata.get("listener_runtime_adjacent_core_count"),
                signal_context.get("listener_runtime_adjacent_core_count"),
                raw.get("listener_runtime_adjacent_core_count"),
            ),
            "listener_runtime_adjacent_secondary_count": _int_value(
                event_metadata.get("listener_runtime_adjacent_secondary_count"),
                signal_metadata.get("listener_runtime_adjacent_secondary_count"),
                signal_context.get("listener_runtime_adjacent_secondary_count"),
                raw.get("listener_runtime_adjacent_secondary_count"),
            ),
            "listener_runtime_adjacent_secondary_scan_used": _bool_value(
                event_metadata.get("listener_runtime_adjacent_secondary_scan_used"),
                signal_metadata.get("listener_runtime_adjacent_secondary_scan_used"),
                signal_context.get("listener_runtime_adjacent_secondary_scan_used"),
                raw.get("listener_runtime_adjacent_secondary_scan_used"),
            ),
            "listener_runtime_adjacent_secondary_skipped_count": _int_value(
                event_metadata.get("listener_runtime_adjacent_secondary_skipped_count"),
                signal_metadata.get("listener_runtime_adjacent_secondary_skipped_count"),
                signal_context.get("listener_runtime_adjacent_secondary_skipped_count"),
                raw.get("listener_runtime_adjacent_secondary_skipped_count"),
            ),
            "listener_block_lp_primary_trend_scan_used": _bool_value(
                event_metadata.get("listener_block_lp_primary_trend_scan_used"),
                signal_metadata.get("listener_block_lp_primary_trend_scan_used"),
                signal_context.get("listener_block_lp_primary_trend_scan_used"),
                raw.get("listener_block_lp_primary_trend_scan_used"),
            ),
            "listener_block_lp_extended_scan_used": _bool_value(
                event_metadata.get("listener_block_lp_extended_scan_used"),
                signal_metadata.get("listener_block_lp_extended_scan_used"),
                signal_context.get("listener_block_lp_extended_scan_used"),
                raw.get("listener_block_lp_extended_scan_used"),
            ),
            "listener_block_lp_primary_trend_pool_count": _int_value(
                event_metadata.get("listener_block_lp_primary_trend_pool_count"),
                signal_metadata.get("listener_block_lp_primary_trend_pool_count"),
                signal_context.get("listener_block_lp_primary_trend_pool_count"),
                raw.get("listener_block_lp_primary_trend_pool_count"),
            ),
            "listener_block_lp_extended_pool_count": _int_value(
                event_metadata.get("listener_block_lp_extended_pool_count"),
                signal_metadata.get("listener_block_lp_extended_pool_count"),
                signal_context.get("listener_block_lp_extended_pool_count"),
                raw.get("listener_block_lp_extended_pool_count"),
            ),
            "listener_block_get_logs_primary_side_count": _int_value(
                event_metadata.get("listener_block_get_logs_primary_side_count"),
                signal_metadata.get("listener_block_get_logs_primary_side_count"),
                signal_context.get("listener_block_get_logs_primary_side_count"),
                raw.get("listener_block_get_logs_primary_side_count"),
            ),
            "listener_block_get_logs_secondary_side_count": _int_value(
                event_metadata.get("listener_block_get_logs_secondary_side_count"),
                signal_metadata.get("listener_block_get_logs_secondary_side_count"),
                signal_context.get("listener_block_get_logs_secondary_side_count"),
                raw.get("listener_block_get_logs_secondary_side_count"),
            ),
            "listener_block_get_logs_secondary_side_skipped_count": _int_value(
                event_metadata.get("listener_block_get_logs_secondary_side_skipped_count"),
                signal_metadata.get("listener_block_get_logs_secondary_side_skipped_count"),
                signal_context.get("listener_block_get_logs_secondary_side_skipped_count"),
                raw.get("listener_block_get_logs_secondary_side_skipped_count"),
            ),
            "listener_block_get_logs_empty_response_count": _int_value(
                event_metadata.get("listener_block_get_logs_empty_response_count"),
                signal_metadata.get("listener_block_get_logs_empty_response_count"),
                signal_context.get("listener_block_get_logs_empty_response_count"),
                raw.get("listener_block_get_logs_empty_response_count"),
            ),
            "low_cu_mode_enabled": _bool_value(
                event_metadata.get("low_cu_mode_enabled"),
                signal_metadata.get("low_cu_mode_enabled"),
                signal_context.get("low_cu_mode_enabled"),
                raw.get("low_cu_mode_enabled"),
            ),
            "low_cu_mode_lp_secondary_only": _bool_value(
                event_metadata.get("low_cu_mode_lp_secondary_only"),
                signal_metadata.get("low_cu_mode_lp_secondary_only"),
                signal_context.get("low_cu_mode_lp_secondary_only"),
                raw.get("low_cu_mode_lp_secondary_only"),
            ),
            "low_cu_mode_poll_interval_sec": _num(
                event_metadata.get("low_cu_mode_poll_interval_sec"),
                signal_metadata.get("low_cu_mode_poll_interval_sec"),
                signal_context.get("low_cu_mode_poll_interval_sec"),
                raw.get("low_cu_mode_poll_interval_sec"),
            ),
            "source_kind": _text(raw.get("source_kind"), event_metadata.get("source_kind")),
            "raw_log_count": _int_value(raw.get("raw_log_count"), event_metadata.get("raw_log_count"), 0),
            "touched_watch_addresses_count": _int_value(len(raw.get("touched_watch_addresses") or event_metadata.get("touched_watch_addresses") or []), 0),
            "touched_lp_pools_count": _int_value(len(raw.get("touched_lp_pools") or event_metadata.get("touched_lp_pools") or []), 0),
            "usd_value": usd_value,
            "usd_value_available": _bool_value(event.usd_value_available, pricing.get("usd_value_available")),
            "usd_value_estimated": _bool_value(event.usd_value_estimated, pricing.get("usd_value_estimated")),
            "pricing_status": _text(getattr(signal, "metadata", {}).get("pricing_status"), gate_metrics.get("pricing_status"), event.pricing_status),
            "pricing_source": _text(gate_metrics.get("pricing_source"), event.pricing_source, pricing.get("pricing_source")),
            "pricing_confidence": _num(getattr(signal, "pricing_confidence", None), gate_metrics.get("pricing_confidence"), event.pricing_confidence),
            "dynamic_min_usd": dynamic_min_usd,
            "base_threshold_usd": base_threshold_usd,
            "effective_threshold_usd": effective_threshold_usd,
            "quality_threshold": _num(gate_metrics.get("quality_threshold")),
            "below_min_usd_gap": below_min_usd_gap,
            "threshold_ratio": threshold_ratio,
            "quality_score": _num(getattr(signal, "quality_score", None), gate_metrics.get("quality_score")),
            "adjusted_quality_score": _num(gate_metrics.get("adjusted_quality_score"), getattr(signal, "quality_score", None)),
            "quality_tier": _text(gate_metrics.get("quality_tier"), signal_metadata.get("quality_tier")),
            "address_score": _num(getattr(signal, "address_score", None), gate_metrics.get("address_score")),
            "token_score": _num(getattr(signal, "token_score", None), gate_metrics.get("token_score")),
            "token_context_score": _num(getattr(signal, "token_context_score", None), gate_metrics.get("token_context_score")),
            "behavior_confidence": _num(gate_metrics.get("behavior_confidence"), behavior.get("confidence")),
            "intent_confidence": _num(signal_metadata.get("intent_confidence"), gate_metrics.get("intent_confidence"), event.intent_confidence),
            "confirmation_score": _num(getattr(signal, "confirmation_score", None), gate_metrics.get("confirmation_score"), event.confirmation_score),
            "confirmation_evidence_count": _int_value(gate_metrics.get("confirmation_evidence_count"), len(event.intent_evidence or []), 0),
            "resonance_score": _num(gate_metrics.get("resonance_score"), signal_metadata.get("resonance_score")),
            "intent_stage": _text(getattr(signal, "intent_stage", None), gate_metrics.get("intent_stage"), event.intent_stage),
            "information_level": _text(getattr(signal, "information_level", None), signal_context.get("information_label"), event_metadata.get("intent", {}).get("information_level")),
            "semantic_role": _text(gate_metrics.get("semantic_role"), event.semantic_role, watch_meta.get("semantic_role")),
            "cooldown_key": _text(getattr(signal, "cooldown_key", None), gate_metrics.get("cooldown_key")),
            "last_signal_ts": _int_value(gate_metrics.get("last_signal_ts")),
            "cooldown_sec": _int_value(gate_metrics.get("cooldown_sec")),
            "lp_event": lp_event,
            "lp_pool_address": _text(event.address if lp_event else "", raw.get("watch_address")),
            "lp_pair_label": _text(gate_metrics.get("lp_pair_label"), lp_context.get("pair_label")),
            "lp_dex": _text(gate_metrics.get("lp_dex"), lp_context.get("dex")),
            "lp_protocol": _text(gate_metrics.get("lp_protocol"), lp_context.get("protocol")),
            "lp_action": _text(gate_metrics.get("lp_action"), lp_context.get("action")),
            "lp_direction": _text(gate_metrics.get("lp_direction"), lp_context.get("direction")),
            "lp_trend_sensitivity_mode": _bool_value(
                event_metadata.get("lp_trend_sensitivity_mode"),
                signal_metadata.get("lp_trend_sensitivity_mode"),
                signal_context.get("lp_trend_sensitivity_mode"),
                gate_metrics.get("lp_trend_sensitivity_mode"),
            ),
            "lp_trend_primary_pool": _bool_value(
                event_metadata.get("lp_trend_primary_pool"),
                signal_metadata.get("lp_trend_primary_pool"),
                signal_context.get("lp_trend_primary_pool"),
                gate_metrics.get("lp_trend_primary_pool"),
            ),
            "lp_trend_pool_family": _text(
                event_metadata.get("lp_trend_pool_family"),
                signal_metadata.get("lp_trend_pool_family"),
                signal_context.get("lp_trend_pool_family"),
                gate_metrics.get("lp_trend_pool_family"),
            ),
            "lp_trend_base_family": _text(
                event_metadata.get("lp_trend_base_family"),
                signal_metadata.get("lp_trend_base_family"),
                signal_context.get("lp_trend_base_family"),
                gate_metrics.get("lp_trend_base_family"),
            ),
            "lp_trend_quote_family": _text(
                event_metadata.get("lp_trend_quote_family"),
                signal_metadata.get("lp_trend_quote_family"),
                signal_context.get("lp_trend_quote_family"),
                gate_metrics.get("lp_trend_quote_family"),
            ),
            "lp_trend_pool_match_mode": _text(
                event_metadata.get("lp_trend_pool_match_mode"),
                signal_metadata.get("lp_trend_pool_match_mode"),
                signal_context.get("lp_trend_pool_match_mode"),
                gate_metrics.get("lp_trend_pool_match_mode"),
            ),
            "lp_trend_state": _text(
                event_metadata.get("lp_trend_state"),
                signal_metadata.get("lp_trend_state"),
                signal_context.get("lp_trend_state"),
                gate_metrics.get("lp_trend_state"),
            ),
            "lp_trend_side_bias": _text(
                event_metadata.get("lp_trend_side_bias"),
                signal_metadata.get("lp_trend_side_bias"),
                signal_context.get("lp_trend_side_bias"),
                gate_metrics.get("lp_trend_side_bias"),
            ),
            "lp_trend_continuation_score": _num(
                event_metadata.get("lp_trend_continuation_score"),
                signal_metadata.get("lp_trend_continuation_score"),
                signal_context.get("lp_trend_continuation_score"),
                gate_metrics.get("lp_trend_continuation_score"),
            ),
            "lp_trend_reversal_score": _num(
                event_metadata.get("lp_trend_reversal_score"),
                signal_metadata.get("lp_trend_reversal_score"),
                signal_context.get("lp_trend_reversal_score"),
                gate_metrics.get("lp_trend_reversal_score"),
            ),
            "lp_trend_state_source": _text(
                event_metadata.get("lp_trend_state_source"),
                signal_metadata.get("lp_trend_state_source"),
                signal_context.get("lp_trend_state_source"),
                gate_metrics.get("lp_trend_state_source"),
            ),
            "lp_trend_state_window_sec": _int_value(
                event_metadata.get("lp_trend_state_window_sec"),
                signal_metadata.get("lp_trend_state_window_sec"),
                signal_context.get("lp_trend_state_window_sec"),
                gate_metrics.get("lp_trend_state_window_sec"),
            ),
            "lp_directional_side": _text(
                event_metadata.get("lp_directional_side"),
                signal_metadata.get("lp_directional_side"),
                signal_context.get("lp_directional_side"),
                gate_metrics.get("lp_directional_side"),
            ),
            "lp_directional_threshold_profile": _text(
                event_metadata.get("lp_directional_threshold_profile"),
                signal_metadata.get("lp_directional_threshold_profile"),
                signal_context.get("lp_directional_threshold_profile"),
                gate_metrics.get("lp_directional_threshold_profile"),
            ),
            "lp_fast_exception_profile_name": _text(
                event_metadata.get("lp_fast_exception_profile_name"),
                signal_metadata.get("lp_fast_exception_profile_name"),
                signal_context.get("lp_fast_exception_profile_name"),
                gate_metrics.get("lp_fast_exception_profile_name"),
            ),
            "lp_buy_trend_profile_active": _bool_value(
                event_metadata.get("lp_buy_trend_profile_active"),
                signal_metadata.get("lp_buy_trend_profile_active"),
                signal_context.get("lp_buy_trend_profile_active"),
                gate_metrics.get("lp_buy_trend_profile_active"),
            ),
            "lp_buy_trend_profile_name": _text(
                event_metadata.get("lp_buy_trend_profile_name"),
                signal_metadata.get("lp_buy_trend_profile_name"),
                signal_context.get("lp_buy_trend_profile_name"),
                gate_metrics.get("lp_buy_trend_profile_name"),
            ),
            "lp_buy_trend_profile_reason": _text(
                event_metadata.get("lp_buy_trend_profile_reason"),
                signal_metadata.get("lp_buy_trend_profile_reason"),
                signal_context.get("lp_buy_trend_profile_reason"),
                gate_metrics.get("lp_buy_trend_profile_reason"),
            ),
            "lp_same_pool_continuity": _int_value(gate_metrics.get("lp_same_pool_continuity"), lp_analysis.get("same_pool_continuity")),
            "lp_multi_pool_resonance": _int_value(gate_metrics.get("lp_multi_pool_resonance"), lp_analysis.get("multi_pool_resonance")),
            "lp_action_intensity": _num(gate_metrics.get("lp_action_intensity"), lp_analysis.get("action_intensity")),
            "lp_reserve_skew": _num(gate_metrics.get("lp_reserve_skew"), lp_analysis.get("reserve_skew")),
            "lp_pool_volume_surge_ratio": _num(gate_metrics.get("lp_pool_volume_surge_ratio"), lp_analysis.get("pool_volume_surge_ratio")),
            "lp_pool_window_trade_count": _int_value(gate_metrics.get("lp_pool_window_trade_count"), lp_analysis.get("pool_window_trade_count")),
            "lp_pool_window_usd_total": _num(gate_metrics.get("lp_pool_window_usd_total"), lp_analysis.get("pool_window_usd_total"), digits=2),
            "lp_observe_exception_applied": _bool_value(gate_metrics.get("lp_observe_exception_applied")),
            "lp_observe_exception_reason": _text(gate_metrics.get("lp_observe_exception_reason")),
            "lp_observe_threshold_ratio": _num(gate_metrics.get("lp_observe_threshold_ratio")),
            "lp_observe_below_min_gap": _num(gate_metrics.get("lp_observe_below_min_gap"), digits=2),
            "lp_notify_hard_min_usd": _num(
                gate_metrics.get("lp_notify_hard_min_usd"),
                event_metadata.get("lp_notify_hard_min_usd"),
                signal_metadata.get("lp_notify_hard_min_usd"),
                signal_context.get("lp_notify_hard_min_usd"),
                digits=2,
            ),
            "lp_notify_hard_min_usd_not_met": _bool_value(
                gate_metrics.get("lp_notify_hard_min_usd_not_met"),
                event_metadata.get("lp_notify_hard_min_usd_not_met"),
                signal_metadata.get("lp_notify_hard_min_usd_not_met"),
                signal_context.get("lp_notify_hard_min_usd_not_met"),
            ),
            "lp_structure_min_usd_per_event": _num(
                gate_metrics.get("lp_structure_min_usd_per_event"),
                lp_analysis.get("lp_structure_min_usd_per_event"),
                event_metadata.get("lp_structure_min_usd_per_event"),
                signal_metadata.get("lp_structure_min_usd_per_event"),
                digits=2,
            ),
            "lp_continuity_eligible": _bool_value(
                gate_metrics.get("lp_continuity_eligible"),
                lp_analysis.get("lp_continuity_eligible"),
                event_metadata.get("lp_continuity_eligible"),
            ),
            "lp_resonance_eligible": _bool_value(
                gate_metrics.get("lp_resonance_eligible"),
                lp_analysis.get("lp_resonance_eligible"),
                event_metadata.get("lp_resonance_eligible"),
            ),
            "lp_continuity_filtered_by_min_usd": _int_value(
                gate_metrics.get("lp_continuity_filtered_by_min_usd"),
                lp_analysis.get("lp_continuity_filtered_by_min_usd"),
                event_metadata.get("lp_continuity_filtered_by_min_usd"),
            ),
            "lp_resonance_filtered_by_min_usd": _int_value(
                gate_metrics.get("lp_resonance_filtered_by_min_usd"),
                lp_analysis.get("lp_resonance_filtered_by_min_usd"),
                event_metadata.get("lp_resonance_filtered_by_min_usd"),
            ),
            "lp_fast_exception_applied": _bool_value(
                gate_metrics.get("lp_fast_exception_applied"),
                event_metadata.get("lp_fast_exception_applied"),
                signal_metadata.get("lp_fast_exception_applied"),
                signal_context.get("lp_fast_exception_applied"),
            ),
            "lp_fast_exception_reason": _text(
                gate_metrics.get("lp_fast_exception_reason"),
                event_metadata.get("lp_fast_exception_reason"),
                signal_metadata.get("lp_fast_exception_reason"),
                signal_context.get("lp_fast_exception_reason"),
            ),
            "lp_fast_exception_threshold_ratio": _num(
                gate_metrics.get("lp_fast_exception_threshold_ratio"),
                event_metadata.get("lp_fast_exception_threshold_ratio"),
                signal_metadata.get("lp_fast_exception_threshold_ratio"),
            ),
            "lp_fast_exception_usd_gap": _num(
                gate_metrics.get("lp_fast_exception_usd_gap"),
                event_metadata.get("lp_fast_exception_usd_gap"),
                signal_metadata.get("lp_fast_exception_usd_gap"),
                digits=2,
            ),
            "lp_fast_exception_structure_score": _num(
                gate_metrics.get("lp_fast_exception_structure_score"),
                event_metadata.get("lp_fast_exception_structure_score"),
                signal_metadata.get("lp_fast_exception_structure_score"),
            ),
            "lp_fast_exception_gate_version": _text(
                gate_metrics.get("lp_fast_exception_gate_version"),
                event_metadata.get("lp_fast_exception_gate_version"),
                signal_metadata.get("lp_fast_exception_gate_version"),
                signal_context.get("lp_fast_exception_gate_version"),
            ),
            "lp_fast_exception_structure_passed": _bool_value(
                gate_metrics.get("lp_fast_exception_structure_passed"),
                event_metadata.get("lp_fast_exception_structure_passed"),
                signal_metadata.get("lp_fast_exception_structure_passed"),
                signal_context.get("lp_fast_exception_structure_passed"),
            ),
            "lp_burst_trend_mode": _bool_value(
                event_metadata.get("lp_burst_trend_mode"),
                signal_metadata.get("lp_burst_trend_mode"),
                signal_context.get("lp_burst_trend_mode"),
                gate_metrics.get("lp_burst_trend_mode"),
            ),
            "lp_burst_fastlane_applied": _bool_value(
                event_metadata.get("lp_burst_fastlane_applied"),
                signal_metadata.get("lp_burst_fastlane_applied"),
                signal_context.get("lp_burst_fastlane_applied"),
                gate_metrics.get("lp_burst_fastlane_applied"),
            ),
            "lp_stage_decision": _text(
                event_metadata.get("lp_stage_decision"),
                signal_metadata.get("lp_stage_decision"),
                signal_context.get("lp_stage_decision"),
                gate_metrics.get("lp_stage_decision"),
            ),
            "lp_reject_reason": _text(
                event_metadata.get("lp_reject_reason"),
                signal_metadata.get("lp_reject_reason"),
                signal_context.get("lp_reject_reason"),
                gate_metrics.get("lp_reject_reason"),
            ),
            "lp_fastlane_ready": _bool_value(
                event_metadata.get("lp_fastlane_ready"),
                signal_metadata.get("lp_fastlane_ready"),
                signal_context.get("lp_fastlane_ready"),
                gate_metrics.get("lp_fastlane_ready"),
                gate_metrics.get("lp_burst_fastlane_ready"),
            ),
            "lp_fastlane_applied": _bool_value(
                event_metadata.get("lp_fastlane_applied"),
                signal_metadata.get("lp_fastlane_applied"),
                signal_context.get("lp_fastlane_applied"),
                gate_metrics.get("lp_fastlane_applied"),
                event_metadata.get("lp_burst_fastlane_applied"),
            ),
            "lp_prealert_candidate": _bool_value(
                event_metadata.get("lp_prealert_candidate"),
                signal_metadata.get("lp_prealert_candidate"),
                signal_context.get("lp_prealert_candidate"),
                gate_metrics.get("lp_prealert_candidate"),
            ),
            "lp_prealert_applied": _bool_value(
                event_metadata.get("lp_prealert_applied"),
                signal_metadata.get("lp_prealert_applied"),
                signal_context.get("lp_prealert_applied"),
                gate_metrics.get("lp_prealert_applied"),
            ),
            "lp_structure_score": _num(
                event_metadata.get("lp_structure_score"),
                signal_metadata.get("lp_structure_score"),
                signal_context.get("lp_structure_score"),
                gate_metrics.get("lp_structure_score"),
                gate_metrics.get("lp_fast_exception_structure_score"),
            ),
            "lp_structure_components": _first_value(
                event_metadata.get("lp_structure_components"),
                signal_metadata.get("lp_structure_components"),
                signal_context.get("lp_structure_components"),
                gate_metrics.get("lp_structure_components"),
            ) or {},
            "lp_pool_priority_class": _text(
                event_metadata.get("lp_pool_priority_class"),
                signal_metadata.get("lp_pool_priority_class"),
                signal_context.get("lp_pool_priority_class"),
                gate_metrics.get("lp_pool_priority_class"),
            ),
            "market_context_source": _text(
                event_metadata.get("market_context_source"),
                signal_metadata.get("market_context_source"),
                signal_context.get("market_context_source"),
            ),
            "alert_relative_timing": _text(
                event_metadata.get("alert_relative_timing"),
                signal_metadata.get("alert_relative_timing"),
                signal_context.get("alert_relative_timing"),
            ),
            "lp_alert_stage": _text(
                event_metadata.get("lp_alert_stage"),
                signal_metadata.get("lp_alert_stage"),
                signal_context.get("lp_alert_stage"),
            ),
            "lp_burst_fastlane_reason": _text(
                event_metadata.get("lp_burst_fastlane_reason"),
                signal_metadata.get("lp_burst_fastlane_reason"),
                signal_context.get("lp_burst_fastlane_reason"),
                gate_metrics.get("lp_burst_fastlane_reason"),
            ),
            "lp_burst_window_sec": _int_value(
                event_metadata.get("lp_burst_window_sec"),
                signal_metadata.get("lp_burst_window_sec"),
                signal_context.get("lp_burst_window_sec"),
                gate_metrics.get("lp_burst_window_sec"),
            ),
            "lp_burst_event_count": _int_value(
                event_metadata.get("lp_burst_event_count"),
                signal_metadata.get("lp_burst_event_count"),
                signal_context.get("lp_burst_event_count"),
                gate_metrics.get("lp_burst_event_count"),
            ),
            "lp_burst_total_usd": _num(
                event_metadata.get("lp_burst_total_usd"),
                signal_metadata.get("lp_burst_total_usd"),
                signal_context.get("lp_burst_total_usd"),
                gate_metrics.get("lp_burst_total_usd"),
                digits=2,
            ),
            "lp_burst_event_count_threshold_used": _int_value(
                event_metadata.get("lp_burst_event_count_threshold_used"),
                signal_metadata.get("lp_burst_event_count_threshold_used"),
                signal_context.get("lp_burst_event_count_threshold_used"),
                gate_metrics.get("lp_burst_event_count_threshold_used"),
            ),
            "lp_burst_total_usd_threshold_used": _num(
                event_metadata.get("lp_burst_total_usd_threshold_used"),
                signal_metadata.get("lp_burst_total_usd_threshold_used"),
                signal_context.get("lp_burst_total_usd_threshold_used"),
                gate_metrics.get("lp_burst_total_usd_threshold_used"),
                digits=2,
            ),
            "lp_burst_trend_profile_name": _text(
                event_metadata.get("lp_burst_trend_profile_name"),
                signal_metadata.get("lp_burst_trend_profile_name"),
                signal_context.get("lp_burst_trend_profile_name"),
                gate_metrics.get("lp_burst_trend_profile_name"),
            ),
            "lp_burst_max_single_usd": _num(
                event_metadata.get("lp_burst_max_single_usd"),
                signal_metadata.get("lp_burst_max_single_usd"),
                signal_context.get("lp_burst_max_single_usd"),
                gate_metrics.get("lp_burst_max_single_usd"),
                digits=2,
            ),
            "lp_burst_delivery_class": _text(
                event_metadata.get("lp_burst_delivery_class"),
                signal_metadata.get("lp_burst_delivery_class"),
                signal_context.get("lp_burst_delivery_class"),
                gate_metrics.get("lp_burst_delivery_class"),
            ),
            "lp_directional_cooldown_key": _text(
                event_metadata.get("lp_directional_cooldown_key"),
                signal_metadata.get("lp_directional_cooldown_key"),
                signal_context.get("lp_directional_cooldown_key"),
                gate_metrics.get("lp_directional_cooldown_key"),
            ),
            "lp_directional_cooldown_sec": _int_value(
                event_metadata.get("lp_directional_cooldown_sec"),
                signal_metadata.get("lp_directional_cooldown_sec"),
                signal_context.get("lp_directional_cooldown_sec"),
                gate_metrics.get("lp_directional_cooldown_sec"),
            ),
            "lp_directional_cooldown_allowed": _bool_or_none(
                event_metadata.get("lp_directional_cooldown_allowed"),
                signal_metadata.get("lp_directional_cooldown_allowed"),
                signal_context.get("lp_directional_cooldown_allowed"),
                gate_metrics.get("lp_directional_cooldown_allowed"),
            ),
            "lp_route_family": _text(
                event_metadata.get("lp_route_family"),
                signal_metadata.get("lp_route_family"),
                signal_context.get("lp_route_family"),
            ),
            "lp_route_priority_source": _text(
                event_metadata.get("lp_route_priority_source"),
                signal_metadata.get("lp_route_priority_source"),
                signal_context.get("lp_route_priority_source"),
            ),
            "lp_route_semantics": _text(
                event_metadata.get("lp_route_semantics"),
                signal_metadata.get("lp_route_semantics"),
                signal_context.get("lp_route_semantics"),
            ),
            "lp_trend_display_label": _text(
                event_metadata.get("lp_trend_display_label"),
                signal_metadata.get("lp_trend_display_label"),
                signal_context.get("lp_trend_display_label"),
            ),
            "lp_trend_display_profile": _text(
                event_metadata.get("lp_trend_display_profile"),
                signal_metadata.get("lp_trend_display_profile"),
                signal_context.get("lp_trend_display_profile"),
            ),
            "lp_trend_display_mode": _text(
                event_metadata.get("lp_trend_display_mode"),
                signal_metadata.get("lp_trend_display_mode"),
                signal_context.get("lp_trend_display_mode"),
            ),
            "lp_trend_display_bias": _text(
                event_metadata.get("lp_trend_display_bias"),
                signal_metadata.get("lp_trend_display_bias"),
                signal_context.get("lp_trend_display_bias"),
            ),
            "lp_trend_display_state": _text(
                event_metadata.get("lp_trend_display_state"),
                signal_metadata.get("lp_trend_display_state"),
                signal_context.get("lp_trend_display_state"),
            ),
            "lp_adjacent_noise_decision_stage": _text(
                event_metadata.get("lp_adjacent_noise_decision_stage"),
                signal_metadata.get("lp_adjacent_noise_decision_stage"),
                signal_context.get("lp_adjacent_noise_decision_stage"),
            ),
            "lp_adjacent_noise_rule_version": _text(
                event_metadata.get("lp_adjacent_noise_rule_version"),
                signal_metadata.get("lp_adjacent_noise_rule_version"),
                signal_context.get("lp_adjacent_noise_rule_version"),
            ),
            "lp_adjacent_noise_filtered": _bool_value(
                event_metadata.get("lp_adjacent_noise_filtered"),
                signal_metadata.get("lp_adjacent_noise_filtered"),
                signal_context.get("lp_adjacent_noise_filtered"),
            ),
            "lp_adjacent_noise_reason": _text(
                event_metadata.get("lp_adjacent_noise_reason"),
                signal_metadata.get("lp_adjacent_noise_reason"),
                signal_context.get("lp_adjacent_noise_reason"),
            ),
            "lp_adjacent_noise_confidence": _num(
                event_metadata.get("lp_adjacent_noise_confidence"),
                signal_metadata.get("lp_adjacent_noise_confidence"),
                signal_context.get("lp_adjacent_noise_confidence"),
                digits=3,
            ),
            "lp_adjacent_noise_source_signals": _first_value(
                event_metadata.get("lp_adjacent_noise_source_signals"),
                signal_metadata.get("lp_adjacent_noise_source_signals"),
                signal_context.get("lp_adjacent_noise_source_signals"),
            ) or [],
            "lp_adjacent_noise_context_used": _first_value(
                event_metadata.get("lp_adjacent_noise_context_used"),
                signal_metadata.get("lp_adjacent_noise_context_used"),
                signal_context.get("lp_adjacent_noise_context_used"),
            ) or [],
            "lp_adjacent_noise_runtime_context_present": _bool_value(
                event_metadata.get("lp_adjacent_noise_runtime_context_present"),
                signal_metadata.get("lp_adjacent_noise_runtime_context_present"),
                signal_context.get("lp_adjacent_noise_runtime_context_present"),
            ),
            "lp_adjacent_noise_downstream_context_present": _bool_value(
                event_metadata.get("lp_adjacent_noise_downstream_context_present"),
                signal_metadata.get("lp_adjacent_noise_downstream_context_present"),
                signal_context.get("lp_adjacent_noise_downstream_context_present"),
            ),
            "lp_adjacent_noise_skipped_in_listener": _bool_value(
                event_metadata.get("lp_adjacent_noise_skipped_in_listener"),
                signal_metadata.get("lp_adjacent_noise_skipped_in_listener"),
            ),
            "lp_adjacent_noise_listener_reason": _text(
                event_metadata.get("lp_adjacent_noise_listener_reason"),
                signal_metadata.get("lp_adjacent_noise_listener_reason"),
            ),
            "lp_adjacent_noise_listener_confidence": _num(
                event_metadata.get("lp_adjacent_noise_listener_confidence"),
                signal_metadata.get("lp_adjacent_noise_listener_confidence"),
                digits=3,
            ),
            "lp_adjacent_noise_listener_source_signals": _first_value(
                event_metadata.get("lp_adjacent_noise_listener_source_signals"),
                signal_metadata.get("lp_adjacent_noise_listener_source_signals"),
            ) or [],
            "value_weight_multiplier": _num(gate_metrics.get("value_weight_multiplier")),
            "role_group_value_bonus": _num(gate_metrics.get("role_group_value_bonus")),
            "smart_money_value_bonus": _num(gate_metrics.get("smart_money_value_bonus")),
            "market_maker_value_bonus": _num(gate_metrics.get("market_maker_value_bonus")),
            "smart_money_non_exec_exception_applied": _bool_value(gate_metrics.get("smart_money_non_exec_exception_applied")),
            "smart_money_non_exec_exception_reason": _text(gate_metrics.get("smart_money_non_exec_exception_reason")),
            "smart_money_non_exec_threshold_ratio": _num(gate_metrics.get("smart_money_non_exec_threshold_ratio")),
            "smart_money_non_exec_quality_gap": _num(gate_metrics.get("smart_money_non_exec_quality_gap")),
            "smart_money_non_exec_value_bonus": _num(gate_metrics.get("smart_money_non_exec_value_bonus")),
            "market_maker_non_exec_value_bonus": _num(gate_metrics.get("market_maker_non_exec_value_bonus")),
            "smart_money_execution_only_mode": _bool_value(
                event_metadata.get("smart_money_execution_only_mode"),
                signal_metadata.get("smart_money_execution_only_mode"),
                signal_context.get("smart_money_execution_only_mode"),
            ),
            "smart_money_legacy_non_exec_branch_disabled": _bool_value(
                event_metadata.get("smart_money_legacy_non_exec_branch_disabled"),
                signal_metadata.get("smart_money_legacy_non_exec_branch_disabled"),
                signal_context.get("smart_money_legacy_non_exec_branch_disabled"),
            ),
            "execution_only_archive_reason": _text(
                event_metadata.get("execution_only_archive_reason"),
                signal_metadata.get("execution_only_archive_reason"),
                signal_context.get("execution_only_archive_reason"),
            ),
            "smart_money_delivery_policy_mode": _text(
                event_metadata.get("smart_money_delivery_policy_mode"),
                signal_metadata.get("smart_money_delivery_policy_mode"),
                signal_context.get("smart_money_delivery_policy_mode"),
            ),
            "smart_money_delivery_policy_reason": _text(
                event_metadata.get("smart_money_delivery_policy_reason"),
                signal_metadata.get("smart_money_delivery_policy_reason"),
                signal_context.get("smart_money_delivery_policy_reason"),
            ),
            "smart_money_delivery_policy_hard_whitelist_applied": _bool_value(
                event_metadata.get("smart_money_delivery_policy_hard_whitelist_applied"),
                signal_metadata.get("smart_money_delivery_policy_hard_whitelist_applied"),
                signal_context.get("smart_money_delivery_policy_hard_whitelist_applied"),
            ),
            "smart_money_allowed_reason_whitelist": _first_value(
                event_metadata.get("smart_money_allowed_reason_whitelist"),
                signal_metadata.get("smart_money_allowed_reason_whitelist"),
                signal_context.get("smart_money_allowed_reason_whitelist"),
            ) or [],
            "market_maker_execution_only_mode": _bool_value(
                event_metadata.get("market_maker_execution_only_mode"),
                signal_metadata.get("market_maker_execution_only_mode"),
                signal_context.get("market_maker_execution_only_mode"),
            ),
            "market_maker_legacy_inventory_branch_disabled": _bool_value(
                event_metadata.get("market_maker_legacy_inventory_branch_disabled"),
                signal_metadata.get("market_maker_legacy_inventory_branch_disabled"),
                signal_context.get("market_maker_legacy_inventory_branch_disabled"),
            ),
            "market_maker_delivery_policy_mode": _text(
                event_metadata.get("market_maker_delivery_policy_mode"),
                signal_metadata.get("market_maker_delivery_policy_mode"),
                signal_context.get("market_maker_delivery_policy_mode"),
            ),
            "market_maker_delivery_policy_reason": _text(
                event_metadata.get("market_maker_delivery_policy_reason"),
                signal_metadata.get("market_maker_delivery_policy_reason"),
                signal_context.get("market_maker_delivery_policy_reason"),
            ),
            "market_maker_delivery_policy_hard_whitelist_applied": _bool_value(
                event_metadata.get("market_maker_delivery_policy_hard_whitelist_applied"),
                signal_metadata.get("market_maker_delivery_policy_hard_whitelist_applied"),
                signal_context.get("market_maker_delivery_policy_hard_whitelist_applied"),
            ),
            "market_maker_allowed_reason_whitelist": _first_value(
                event_metadata.get("market_maker_allowed_reason_whitelist"),
                signal_metadata.get("market_maker_allowed_reason_whitelist"),
                signal_context.get("market_maker_allowed_reason_whitelist"),
            ) or [],
            "market_maker_observe_exception_applied": _bool_value(
                gate_metrics.get("market_maker_observe_exception_applied"),
                signal_metadata.get("market_maker_observe_exception_applied"),
            ),
            "market_maker_observe_exception_reason": _text(
                gate_metrics.get("market_maker_observe_exception_reason"),
                signal_metadata.get("market_maker_observe_exception_reason"),
            ),
            "market_maker_threshold_ratio": _num(
                gate_metrics.get("market_maker_threshold_ratio"),
                signal_metadata.get("market_maker_threshold_ratio"),
            ),
            "market_maker_quality_gap": _num(
                gate_metrics.get("market_maker_quality_gap"),
                signal_metadata.get("market_maker_quality_gap"),
            ),
            "market_maker_context_confirmed": _bool_value(
                event_metadata.get("market_maker_context_confirmed"),
                signal_metadata.get("market_maker_context_confirmed"),
            ),
            "market_maker_context_strength": _int_value(
                event_metadata.get("market_maker_context_strength"),
                signal_metadata.get("market_maker_context_strength"),
            ),
            "market_maker_inventory_context": _bool_value(
                event_metadata.get("market_maker_inventory_context"),
                signal_metadata.get("market_maker_inventory_context"),
            ),
            "market_maker_fact_brief": _text(
                signal_context.get("market_maker_fact_brief"),
                signal_metadata.get("market_maker_fact_brief"),
            ),
            "market_maker_explanation_brief": _text(
                signal_context.get("market_maker_explanation_brief"),
                signal_metadata.get("market_maker_explanation_brief"),
            ),
            "market_maker_evidence_brief": _text(
                signal_context.get("market_maker_evidence_brief"),
                signal_metadata.get("market_maker_evidence_brief"),
            ),
            "market_maker_action_hint": _text(
                signal_context.get("market_maker_action_hint"),
                signal_metadata.get("market_maker_action_hint"),
            ),
            "gate_exception_passed": _bool_value(gate_metrics.get("gate_exception_passed")),
            "smart_money_style_variant": _text(
                signal_context.get("smart_money_style_variant"),
                signal_metadata.get("smart_money_style_variant"),
            ),
            "lp_buy_cluster_5m": _int_value(gate_metrics.get("buy_cluster_5m")),
            "lp_sell_cluster_5m": _int_value(gate_metrics.get("sell_cluster_5m")),
            "lp_liquidity_proxy_usd": _num(gate_metrics.get("liquidity_proxy_usd"), digits=2),
            "lp_volume_24h_proxy_usd": _num(gate_metrics.get("volume_24h_proxy_usd"), digits=2),
            "lp_price_impact_ratio": _num(gate_metrics.get("price_impact_ratio")),
            "lp_token_volume_ratio": _num(gate_metrics.get("token_volume_ratio")),
            "is_exchange_related": _bool_value(gate_metrics.get("is_exchange_related"), raw.get("is_exchange_related")),
            "is_stablecoin_flow": _bool_value(gate_metrics.get("is_stablecoin_flow"), raw.get("is_stablecoin_flow")),
            "stablecoin_dominant": _bool_value(gate_metrics.get("stablecoin_dominant"), raw.get("is_stablecoin_flow")),
            "possible_internal_transfer": _bool_value(gate_metrics.get("possible_internal_transfer"), raw.get("possible_internal_transfer")),
            "exchange_noise_sensitive": _bool_value(gate_metrics.get("exchange_noise_sensitive")),
            "exchange_strong_observe_allowed": _bool_value(
                event_metadata.get("exchange_strong_observe_allowed"),
                signal_metadata.get("exchange_strong_observe_allowed"),
                signal_context.get("exchange_strong_observe_allowed"),
            ),
            "exchange_strong_observe_reason": _text(
                event_metadata.get("exchange_strong_observe_reason"),
                signal_metadata.get("exchange_strong_observe_reason"),
                signal_context.get("exchange_strong_observe_reason"),
            ),
            "exchange_strong_observe_thresholds": _first_value(
                event_metadata.get("exchange_strong_observe_thresholds"),
                signal_metadata.get("exchange_strong_observe_thresholds"),
                signal_context.get("exchange_strong_observe_thresholds"),
            ) or {},
            "followup_strength": _text(gate_metrics.get("followup_strength"), event_metadata.get("followup_strength")),
            "followup_semantic": _text(gate_metrics.get("followup_semantic"), event_metadata.get("followup_semantic")),
            "followup_confirmed": _bool_value(gate_metrics.get("followup_confirmed"), event_metadata.get("followup_confirmed")),
            "case_family": _text(event_metadata.get("case_family"), signal_metadata.get("case_family")),
            "downstream_followup": _bool_value(event_metadata.get("downstream_followup"), signal_metadata.get("downstream_followup")),
            "notification_stage": _text(
                event_metadata.get("delivered_notification_stage"),
                signal_metadata.get("delivered_notification_stage"),
                event_metadata.get("pending_case_notification_stage"),
                signal_metadata.get("pending_case_notification_stage"),
                event_metadata.get("case_notification_stage"),
                signal_metadata.get("case_notification_stage"),
            ),
            "pending_case_notification": _bool_value(
                event_metadata.get("pending_case_notification"),
                signal_metadata.get("pending_case_notification"),
            ),
            "pending_notification_stage": _text(
                event_metadata.get("pending_case_notification_stage"),
                signal_metadata.get("pending_case_notification_stage"),
            ),
            "pending_notification_reason": _text(
                event_metadata.get("pending_case_notification_reason"),
                signal_metadata.get("pending_case_notification_reason"),
            ),
            "delivered_notification": _bool_value(
                (audit_extras or {}).get("delivered"),
                event_metadata.get("delivered_notification"),
                signal_metadata.get("delivered_notification"),
            ),
            "delivered_notification_stage": _text(
                event_metadata.get("delivered_notification_stage"),
                signal_metadata.get("delivered_notification_stage"),
            ),
            "delivered_notification_reason": _text(
                event_metadata.get("delivered_notification_reason"),
                signal_metadata.get("delivered_notification_reason"),
            ),
            "cooldown_allowed": _bool_or_none(
                event_metadata.get("cooldown_allowed"),
                signal_metadata.get("cooldown_allowed"),
                signal_context.get("cooldown_allowed"),
            ),
            "cooldown_reason": _text(
                event_metadata.get("cooldown_reason"),
                signal_metadata.get("cooldown_reason"),
                signal_context.get("cooldown_reason"),
            ),
            "anchor_tx_hash": _text(event_metadata.get("anchor_tx_hash")),
            "anchor_usd_value": _num(
                event_metadata.get("anchor_usd_value"),
                event_metadata.get("downstream_anchor_usd_value"),
                signal_metadata.get("anchor_usd_value"),
                signal_context.get("anchor_usd_value"),
                digits=2,
            ),
            "anchor_watch_address": _text(
                event_metadata.get("anchor_watch_address"),
                event_metadata.get("downstream_anchor_address"),
                signal_metadata.get("anchor_watch_address"),
                signal_context.get("anchor_watch_address"),
                watch_meta.get("anchor_watch_address"),
            ),
            "anchor_label": _text(event_metadata.get("anchor_label"), event_metadata.get("downstream_anchor_label")),
            "downstream_address": _text(event_metadata.get("downstream_address")),
            "downstream_label": _text(event_metadata.get("downstream_label"), event_metadata.get("downstream_object_label")),
            "downstream_bridge_candidate": _bool_value(
                event_metadata.get("downstream_bridge_candidate"),
                signal_metadata.get("downstream_bridge_candidate"),
                signal_context.get("downstream_bridge_candidate"),
                case_metadata.get("downstream_bridge_candidate"),
            ),
            "downstream_bridge_applied": _bool_value(
                event_metadata.get("downstream_bridge_applied"),
                signal_metadata.get("downstream_bridge_applied"),
                signal_context.get("downstream_bridge_applied"),
                case_metadata.get("downstream_bridge_applied"),
            ),
            "downstream_bridge_reason": _text(
                event_metadata.get("downstream_bridge_reason"),
                signal_metadata.get("downstream_bridge_reason"),
                signal_context.get("downstream_bridge_reason"),
                case_metadata.get("downstream_bridge_reason"),
            ),
            "downstream_bridge_anchor_source": _text(
                event_metadata.get("downstream_bridge_anchor_source"),
                signal_metadata.get("downstream_bridge_anchor_source"),
                signal_context.get("downstream_bridge_anchor_source"),
                case_metadata.get("downstream_bridge_anchor_source"),
            ),
            "downstream_bridge_counterparty_role": _text(
                event_metadata.get("downstream_bridge_counterparty_role"),
                signal_metadata.get("downstream_bridge_counterparty_role"),
                signal_context.get("downstream_bridge_counterparty_role"),
                case_metadata.get("downstream_bridge_counterparty_role"),
            ),
            "downstream_bridge_counterparty_label": _text(
                event_metadata.get("downstream_bridge_counterparty_label"),
                signal_metadata.get("downstream_bridge_counterparty_label"),
                signal_context.get("downstream_bridge_counterparty_label"),
                case_metadata.get("downstream_bridge_counterparty_label"),
            ),
            "downstream_case_id": _text(
                event_metadata.get("downstream_case_id"),
                signal_metadata.get("downstream_case_id"),
                signal_context.get("downstream_case_id"),
                watch_meta.get("downstream_case_id"),
            ),
            "downstream_case_anchor_watch_address": _text(
                event_metadata.get("downstream_case_anchor_watch_address"),
                signal_metadata.get("downstream_case_anchor_watch_address"),
                signal_context.get("downstream_case_anchor_watch_address"),
                case_metadata.get("downstream_case_anchor_watch_address"),
            ),
            "downstream_case_match_mode": _text(
                event_metadata.get("downstream_case_match_mode"),
                signal_metadata.get("downstream_case_match_mode"),
                signal_context.get("downstream_case_match_mode"),
                case_metadata.get("downstream_case_match_mode"),
            ),
            "downstream_case_reused": _bool_or_none(
                event_metadata.get("downstream_case_reused"),
                signal_metadata.get("downstream_case_reused"),
                signal_context.get("downstream_case_reused"),
                case_metadata.get("downstream_case_reused"),
            ),
            "downstream_followup_type": _text(event_metadata.get("downstream_followup_type")),
            "downstream_followup_label": _text(event_metadata.get("downstream_followup_label")),
            "downstream_followup_stage": _text(event_metadata.get("downstream_followup_stage")),
            "downstream_followup_reason": _text(event_metadata.get("downstream_followup_reason")),
            "downstream_followup_audit_reason": _text(
                event_metadata.get("downstream_observation_reason"),
                event_metadata.get("case_notification_reason"),
                gate_reason,
            ),
            "downstream_early_warning_allowed": _bool_value(
                event_metadata.get("downstream_early_warning_allowed"),
                signal_metadata.get("downstream_early_warning_allowed"),
                signal_context.get("downstream_early_warning_allowed"),
            ),
            "downstream_early_warning_reason": _text(
                event_metadata.get("downstream_early_warning_reason"),
                signal_metadata.get("downstream_early_warning_reason"),
                signal_context.get("downstream_early_warning_reason"),
            ),
            "downstream_early_warning_thresholds": _first_value(
                event_metadata.get("downstream_early_warning_thresholds"),
                signal_metadata.get("downstream_early_warning_thresholds"),
                signal_context.get("downstream_early_warning_thresholds"),
            ) or {},
            "downstream_early_warning_emitted": _bool_value(
                event_metadata.get("downstream_early_warning_emitted"),
                signal_metadata.get("downstream_early_warning_emitted"),
                signal_context.get("downstream_early_warning_emitted"),
            ),
            "downstream_early_warning_emitted_count": _int_value(
                event_metadata.get("downstream_early_warning_emitted_count"),
                signal_metadata.get("downstream_early_warning_emitted_count"),
                signal_context.get("downstream_early_warning_emitted_count"),
            ),
            "downstream_early_warning_signal_id": _text(
                event_metadata.get("downstream_early_warning_signal_id"),
                signal_metadata.get("downstream_early_warning_signal_id"),
                signal_context.get("downstream_early_warning_signal_id"),
            ),
            "downstream_early_warning_stage_recorded": _bool_value(
                event_metadata.get("downstream_early_warning_stage_recorded"),
                signal_metadata.get("downstream_early_warning_stage_recorded"),
                signal_context.get("downstream_early_warning_stage_recorded"),
            ),
            "followup_stage": _text(event_metadata.get("followup_stage"), event_metadata.get("downstream_followup_stage")),
            "followup_type": _text(event_metadata.get("followup_type"), event_metadata.get("downstream_followup_type")),
            "current_event_is_anchor": _bool_value(
                event_metadata.get("current_event_is_anchor"),
                signal_metadata.get("current_event_is_anchor"),
                signal_context.get("current_event_is_anchor"),
            ),
            "current_event_is_followup": _bool_value(
                event_metadata.get("current_event_is_followup"),
                signal_metadata.get("current_event_is_followup"),
                signal_context.get("current_event_is_followup"),
            ),
            "downstream_current_event_state_source": _text(
                event_metadata.get("downstream_current_event_state_source"),
                signal_metadata.get("downstream_current_event_state_source"),
                signal_context.get("downstream_current_event_state_source"),
            ),
            "downstream_current_event_state_semantics": _text(
                event_metadata.get("downstream_current_event_state_semantics"),
                signal_metadata.get("downstream_current_event_state_semantics"),
                signal_context.get("downstream_current_event_state_semantics"),
            ),
            "downstream_current_event_state_compat_bool_source": _text(
                event_metadata.get("downstream_current_event_state_compat_bool_source"),
                signal_metadata.get("downstream_current_event_state_compat_bool_source"),
                signal_context.get("downstream_current_event_state_compat_bool_source"),
            ),
            "downstream_current_event_state_effective_label": _text(
                event_metadata.get("downstream_current_event_state_effective_label"),
                signal_metadata.get("downstream_current_event_state_effective_label"),
                signal_context.get("downstream_current_event_state_effective_label"),
            ),
            "downstream_current_event_state_effective_bool_safe": _first_value(
                event_metadata.get("downstream_current_event_state_effective_bool_safe"),
                signal_metadata.get("downstream_current_event_state_effective_bool_safe"),
                signal_context.get("downstream_current_event_state_effective_bool_safe"),
            ),
            "downstream_current_event_state_known": _bool_value(
                event_metadata.get("downstream_current_event_state_known"),
                signal_metadata.get("downstream_current_event_state_known"),
                signal_context.get("downstream_current_event_state_known"),
            ),
            "downstream_current_event_state_missing": _bool_value(
                event_metadata.get("downstream_current_event_state_missing"),
                signal_metadata.get("downstream_current_event_state_missing"),
                signal_context.get("downstream_current_event_state_missing"),
            ),
            "downstream_current_event_state_fail_closed": _bool_value(
                event_metadata.get("downstream_current_event_state_fail_closed"),
                signal_metadata.get("downstream_current_event_state_fail_closed"),
                signal_context.get("downstream_current_event_state_fail_closed"),
            ),
            "downstream_current_event_state_reason": _text(
                event_metadata.get("downstream_current_event_state_reason"),
                signal_metadata.get("downstream_current_event_state_reason"),
                signal_context.get("downstream_current_event_state_reason"),
            ),
            "downstream_case_state_consistent": _bool_value(
                event_metadata.get("downstream_case_state_consistent"),
                signal_metadata.get("downstream_case_state_consistent"),
                signal_context.get("downstream_case_state_consistent"),
            ),
            "downstream_case_anchor_flag": _bool_value(
                event_metadata.get("downstream_case_anchor_flag"),
                signal_metadata.get("downstream_case_anchor_flag"),
                signal_context.get("downstream_case_anchor_flag"),
                event_metadata.get("current_event_is_anchor"),
            ),
            "downstream_event_anchor_flag": _bool_value(
                event_metadata.get("downstream_event_anchor_flag"),
                signal_metadata.get("downstream_event_anchor_flag"),
                signal_context.get("downstream_event_anchor_flag"),
                event_metadata.get("current_event_is_anchor"),
            ),
            "downstream_case_followup_flag": _bool_value(
                event_metadata.get("downstream_case_followup_flag"),
                signal_metadata.get("downstream_case_followup_flag"),
                signal_context.get("downstream_case_followup_flag"),
                event_metadata.get("current_event_is_followup"),
            ),
            "downstream_event_followup_flag": _bool_value(
                event_metadata.get("downstream_event_followup_flag"),
                signal_metadata.get("downstream_event_followup_flag"),
                signal_context.get("downstream_event_followup_flag"),
                event_metadata.get("current_event_is_followup"),
            ),
            "runtime_adjacent_anchor_source": _text(
                event_metadata.get("runtime_adjacent_anchor_source"),
                signal_metadata.get("runtime_adjacent_anchor_source"),
                signal_context.get("runtime_adjacent_anchor_source"),
            ),
            "runtime_adjacent_anchor_flag": _bool_value(
                event_metadata.get("runtime_adjacent_anchor_flag"),
                signal_metadata.get("runtime_adjacent_anchor_flag"),
                signal_context.get("runtime_adjacent_anchor_flag"),
            ),
            "runtime_adjacent_watch_consistent": _bool_value(
                event_metadata.get("runtime_adjacent_watch_consistent"),
                signal_metadata.get("runtime_adjacent_watch_consistent"),
                signal_context.get("runtime_adjacent_watch_consistent"),
            ),
            "runtime_adjacent_case_id": _text(
                event_metadata.get("runtime_adjacent_case_id"),
                signal_metadata.get("runtime_adjacent_case_id"),
                signal_context.get("runtime_adjacent_case_id"),
            ),
            "hop": _int_value(event_metadata.get("hop")),
            "window_sec": _int_value(event_metadata.get("window_sec")),
            "runtime_state": _text(
                event_metadata.get("downstream_runtime_state"),
                signal_metadata.get("downstream_runtime_state"),
                signal_context.get("downstream_runtime_state"),
                watch_meta.get("runtime_state"),
            ),
            "execution_required_but_missing": _bool_value(gate_metrics.get("execution_required_but_missing"), event_metadata.get("execution_required_but_missing")),
            "downstream_impact_gate_allowed": _bool_or_none(
                event_metadata.get("downstream_impact_gate_allowed"),
                signal_metadata.get("downstream_impact_gate_allowed"),
                signal_context.get("downstream_impact_gate_allowed"),
            ),
            "downstream_impact_gate_reason": _text(
                event_metadata.get("downstream_impact_gate_reason"),
                signal_metadata.get("downstream_impact_gate_reason"),
                signal_context.get("downstream_impact_gate_reason"),
            ),
            "emitted_stages": list(
                _first_value(
                    event_metadata.get("emitted_stages"),
                    signal_metadata.get("emitted_stages"),
                    signal_context.get("emitted_stages"),
                    event_metadata.get("emitted_notification_stages"),
                    signal_metadata.get("emitted_notification_stages"),
                )
                or []
            ),
            "emitted_notification_count": _int_value(
                event_metadata.get("emitted_notification_count"),
                signal_metadata.get("emitted_notification_count"),
                signal_context.get("emitted_notification_count"),
            ),
            "downstream_notification_cap": _int_value(
                event_metadata.get("downstream_notification_cap"),
                signal_metadata.get("downstream_notification_cap"),
                signal_context.get("downstream_notification_cap"),
                case_metadata.get("downstream_notification_cap"),
            ),
            "downstream_notification_count": _int_value(
                event_metadata.get("downstream_notification_count"),
                signal_metadata.get("downstream_notification_count"),
                signal_context.get("downstream_notification_count"),
                case_metadata.get("downstream_notification_count"),
            ),
            "downstream_notification_cap_source": _text(
                event_metadata.get("downstream_notification_cap_source"),
                signal_metadata.get("downstream_notification_cap_source"),
                signal_context.get("downstream_notification_cap_source"),
                case_metadata.get("downstream_notification_cap_source"),
            ),
            "last_notification_stage": _text(
                event_metadata.get("last_notification_stage"),
                signal_metadata.get("last_notification_stage"),
                signal_context.get("last_notification_stage"),
            ),
            "last_notification_signal_id": _text(
                event_metadata.get("last_notification_signal_id"),
                signal_metadata.get("last_notification_signal_id"),
                signal_context.get("last_notification_signal_id"),
            ),
            "emitted_notification_history_version": _int_value(
                event_metadata.get("emitted_notification_history_version"),
                signal_metadata.get("emitted_notification_history_version"),
                signal_context.get("emitted_notification_history_version"),
            ),
            "emitted_notification_stage_source": _text(
                event_metadata.get("emitted_notification_stage_source"),
                signal_metadata.get("emitted_notification_stage_source"),
                signal_context.get("emitted_notification_stage_source"),
            ),
            "stable_non_swap_filtered_flag": _bool_value(gate_metrics.get("stable_non_swap_filtered_flag"), gate_reason == "stable_non_swap_filtered"),
            "stable_non_swap_hard_filter_usd": _num(gate_metrics.get("stable_non_swap_hard_filter_usd"), digits=2),
            "stable_transfer_min_usd": _num(gate_metrics.get("stable_transfer_min_usd"), digits=2),
            "liquidation_stage": _text(event_metadata.get("liquidation_stage"), liquidation.get("liquidation_stage")),
            "liquidation_score": _num(event_metadata.get("liquidation_score"), liquidation.get("liquidation_score")),
            "liquidation_side": _text(event_metadata.get("liquidation_side"), liquidation.get("liquidation_side")),
            "liquidation_reason": _text(event_metadata.get("liquidation_reason"), liquidation.get("liquidation_reason")),
            "liquidation_protocols": list(event_metadata.get("liquidation_protocols") or liquidation.get("liquidation_protocols") or []),
            "liquidation_evidence_count": _int_value(
                len(event_metadata.get("liquidation_evidence") or liquidation.get("liquidation_evidence") or []),
                liquidation.get("liquidation_evidence_count"),
            ),
            "is_liquidation_protocol_related": _bool_value(
                raw.get("is_liquidation_protocol_related"),
                (raw.get("inferred_context") or {}).get("is_liquidation_protocol_related"),
            ),
            "possible_keeper_executor": _bool_value(
                raw.get("possible_keeper_executor"),
                (raw.get("inferred_context") or {}).get("possible_keeper_executor"),
            ),
            "possible_vault_or_auction": _bool_value(
                raw.get("possible_vault_or_auction"),
                (raw.get("inferred_context") or {}).get("possible_vault_or_auction"),
            ),
            "replay_source": _text(event_metadata.get("replay_source"), raw.get("replay_source")),
            "metadata_summary": _text(
                signal_context.get("headline_label"),
                signal_context.get("fact_brief"),
                event_metadata.get("followup_label"),
            ),
            "silent_reason": _first_value(
                event_metadata.get("silent_reason"),
                signal_metadata.get("silent_reason"),
                signal_context.get("silent_reason"),
            ) or {},
            "silent_reason_bucket": _text(
                event_metadata.get("silent_reason_bucket"),
                signal_metadata.get("silent_reason_bucket"),
                signal_context.get("silent_reason_bucket"),
            ),
            "shadow_high_value_candidate": _bool_value(
                event_metadata.get("shadow_high_value_candidate"),
                signal_metadata.get("shadow_high_value_candidate"),
                signal_context.get("shadow_high_value_candidate"),
            ),
            "shadow_candidate_reason": _text(
                event_metadata.get("shadow_candidate_reason"),
                signal_metadata.get("shadow_candidate_reason"),
                signal_context.get("shadow_candidate_reason"),
            ),
            "shadow_candidate_class": _text(
                event_metadata.get("shadow_candidate_class"),
                signal_metadata.get("shadow_candidate_class"),
                signal_context.get("shadow_candidate_class"),
            ),
        }
        if audit_extras:
            payload.update(audit_extras)
        return self._compact_delivery_audit_payload(payload)

    def _compact_delivery_audit_payload(self, payload: dict) -> dict:
        compact = dict(payload)

        # Prefer a single canonical field for duplicated followup / notification semantics.
        for field in {
            "downstream_followup_audit_reason",
            "followup_type",
            "followup_stage",
            "notification_stage",
            "silent_reason_bucket",
        }:
            compact.pop(field, None)

        empty_skip_fields = {
            "primary_threshold_profile",
            "relaxed_threshold_details",
            "observe_threshold_profile",
            "exchange_strong_observe_thresholds",
            "lp_adjacent_noise_listener_reason",
            "lp_adjacent_noise_listener_source_signals",
            "shadow_candidate_reason",
            "shadow_candidate_class",
            "replay_source",
            "smart_money_allowed_reason_whitelist",
        }
        for field in empty_skip_fields:
            if compact.get(field) in (None, "", [], {}):
                compact.pop(field, None)

        conditional_nonempty_fields = {
            "role_priority_tier",
            "role_priority_label",
            "stage_tier",
            "delivery_policy_reason",
            "delivery_policy_evaluated_at_stage",
        }
        for field in conditional_nonempty_fields:
            if compact.get(field) == "":
                compact.pop(field, None)

        conditional_none_fields = {
            "stage_budget_allowed",
            "stage_budget_window_sec",
            "stage_budget_recent_total",
            "stage_budget_recent_same_tier",
            "stage_budget_recent_higher_tier",
            "stage_budget_total_cap",
            "stage_budget_tier_cap",
            "role_priority_rank",
            "delivery_policy_allowed",
            "cooldown_allowed",
            "last_signal_ts",
            "window_sec",
            "anchor_usd_value",
        }
        for field in conditional_none_fields:
            if compact.get(field) is None:
                compact.pop(field, None)

        if compact.get("stage_budget_reason") == "":
            compact.pop("stage_budget_reason", None)
        if compact.get("stage_budget_stage") == "":
            compact.pop("stage_budget_stage", None)
        if compact.get("stage_budget_role_tier") == "":
            compact.pop("stage_budget_role_tier", None)

        silent_reason = compact.get("silent_reason")
        if isinstance(silent_reason, dict):
            silent_reason = dict(silent_reason)
            if silent_reason.get("reason_detail") == silent_reason.get("reason_code"):
                silent_reason.pop("reason_detail", None)
            compact["silent_reason"] = silent_reason

        return compact

    def _observe_address_intelligence(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict | None,
        raw_item: dict,
        token_snapshot: dict,
    ) -> dict | None:
        if self.address_intelligence is None:
            return None
        if self._is_lp_event(event=event):
            return None
        try:
            return self.address_intelligence.observe_event(
                event=event,
                parsed=parsed,
                watch_context=watch_context,
                raw_item=raw_item,
                token_snapshot=token_snapshot,
            )
        except Exception as e:
            print(f"地址情报采集失败: {e}")
            return None

    def _touch_runtime_adjacent_watch(self, event: Event, watch_meta: dict | None) -> None:
        if not bool((watch_meta or {}).get("runtime_adjacent_watch")):
            return
        try:
            self.state_manager.touch_adjacent_watch(
                event.address,
                ts=int(event.ts or 0),
                event_type=str(event.intent_type or event.kind or ""),
            )
        except Exception as e:
            print(f"runtime adjacent watch touch 失败: {e}")

    def _expire_adjacent_watch_for_case(self, behavior_case) -> None:
        if not self._is_downstream_followup_case(behavior_case):
            return
        try:
            status = str(getattr(behavior_case, "status", "") or "")
            if status in {"invalidated", "closed"}:
                self.state_manager.close_adjacent_watch(
                    behavior_case.watch_address,
                    ts=int(time.time()),
                    reason=status or "closed",
                )
            else:
                self.state_manager.expire_adjacent_watch(behavior_case.watch_address, now_ts=int(time.time()))
        except Exception as e:
            print(f"runtime adjacent watch 过期失败: {e}")

    def _register_adjacent_watch_from_case(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None,
    ) -> None:
        if not self._is_downstream_followup_case(behavior_case):
            return
        metadata = getattr(behavior_case, "metadata", {}) or {}
        runtime_adjacent_state = self._resolve_runtime_adjacent_anchor_state(
            behavior_case=behavior_case,
            case_result=case_result,
        )
        event.metadata.update(runtime_adjacent_state)
        if not bool(runtime_adjacent_state.get("runtime_adjacent_anchor_flag")):
            return
        if int(metadata.get("hop") or 1) > 1:
            return
        try:
            registered = self.state_manager.register_adjacent_watch(
                address=str(metadata.get("downstream_address") or behavior_case.watch_address or "").lower(),
                anchor_watch_address=str(metadata.get("anchor_watch_address") or event.address or "").lower(),
                anchor_label=str(metadata.get("anchor_label") or (watch_meta or {}).get("label") or ""),
                root_tx_hash=str(metadata.get("root_tx_hash") or event.tx_hash or ""),
                token=event.token,
                anchor_usd_value=float(metadata.get("anchor_usd_value") or event.usd_value or 0.0),
                opened_at=int(metadata.get("opened_at") or event.ts or time.time()),
                active_until=int(metadata.get("active_until") or metadata.get("expire_at") or event.ts or time.time()),
                cooling_until=int(metadata.get("cooling_until") or event.ts or time.time()),
                closing_until=int(metadata.get("closing_until") or event.ts or time.time()),
                hop=int(metadata.get("hop") or 1),
                reason="new_large_counterparty",
                strategy_hint="runtime_adjacent_watch",
                runtime_label_hint="new_large_counterparty",
                metadata={
                    "priority": int(ADJACENT_WATCH_RUNTIME_PRIORITY or 3),
                    "strategy_role": str(ADJACENT_WATCH_RUNTIME_STRATEGY_ROLE or "adjacent_watch"),
                    "semantic_role": "watched_wallet",
                    "role": "user_watch",
                    "token_symbol": str(event.metadata.get("token_symbol") or ""),
                    "anchor_strategy_role": str(metadata.get("anchor_strategy_role") or (watch_meta or {}).get("strategy_role") or event.strategy_role or "unknown"),
                    "display_hint_label": "new_large_counterparty",
                    "display_hint_reason": "anchor_super_large_outflow",
                    "case_id": str(behavior_case.case_id or ""),
                    "window_sec": int(metadata.get("window_sec") or 0),
                    "runtime_adjacent_anchor_source": str(runtime_adjacent_state.get("runtime_adjacent_anchor_source") or ""),
                    "runtime_adjacent_anchor_flag": bool(runtime_adjacent_state.get("runtime_adjacent_anchor_flag")),
                    "runtime_adjacent_watch_consistent": bool(runtime_adjacent_state.get("runtime_adjacent_watch_consistent")),
                },
            )
            if registered and self.address_intelligence is not None:
                self.address_intelligence.mark_display_hint(
                    address=str(metadata.get("downstream_address") or behavior_case.watch_address or "").lower(),
                    display_hint_label="new_large_counterparty",
                    expire_at=int(metadata.get("active_until") or metadata.get("expire_at") or event.ts or time.time()),
                    display_hint_reason="anchor_super_large_outflow",
                    display_hint_anchor_label=str(metadata.get("anchor_label") or (watch_meta or {}).get("label") or ""),
                    display_hint_anchor_address=str(metadata.get("anchor_watch_address") or event.address or "").lower(),
                    display_hint_usd_value=float(metadata.get("anchor_usd_value") or event.usd_value or 0.0),
                    display_hint_token_symbol=str(event.metadata.get("token_symbol") or ""),
                )
        except Exception as e:
            print(f"runtime adjacent watch 注册失败: {e}")

    def _match_or_open_case(self, event: Event, watch_meta: dict, behavior: dict) -> dict | None:
        if self.followup_tracker is None:
            return None
        try:
            return self.followup_tracker.match_or_open_case(
                event=event,
                watch_meta=watch_meta,
                behavior=behavior,
            )
        except Exception as e:
            print(f"followup case 处理失败: {e}")
            return None

    def _attach_signal_to_case(self, behavior_case, signal):
        if self.followup_tracker is None or behavior_case is None:
            return behavior_case
        try:
            return self.followup_tracker.attach_signal(behavior_case, signal)
        except Exception as e:
            print(f"case 绑定 signal 失败: {e}")
            return behavior_case

    def _bind_case_to_event(self, event: Event, behavior_case, case_result: dict | None) -> None:
        if behavior_case is None:
            return
        event.case_id = behavior_case.case_id
        event.followup_stage = behavior_case.stage
        event.followup_status = behavior_case.status
        invalidated_cases = (case_result or {}).get("invalidated_cases") or []
        if invalidated_cases and not event.parent_case_id:
            event.parent_case_id = invalidated_cases[0].case_id

    def _apply_exchange_followup_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        if not self._is_exchange_followup_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        followup_assets = list(metadata.get("followup_tokens_seen") or [])
        followup_confirmed = bool(metadata.get("followup_confirmed"))
        anchor_symbol = str(metadata.get("anchor_symbol") or event.metadata.get("token_symbol") or "稳定币")
        has_execution_evidence = bool(event.kind == "swap" or str(event.intent_type or "") == "swap_execution")
        followup_strength = "weak"
        followup_semantic = "observation_upgrade"
        execution_required_but_missing = False
        label = "结构延续观察升级" if followup_confirmed else "交易场景观察建立"
        if followup_confirmed and followup_assets:
            assets_text = "/".join(followup_assets[:3])
            detail = f"前序{anchor_symbol}流入后，{assets_text} 又流入同一交易所地址，当前更像结构延续观察"
        else:
            detail = f"{anchor_symbol} 已流入该交易所地址，先建立观察锚点，继续看是否出现真实执行或更强共振"

        if followup_confirmed:
            if has_execution_evidence or float(event.confirmation_score or 0.0) >= 0.82:
                followup_strength = "strong"
                followup_semantic = "execution_confirmed" if has_execution_evidence else "structure_continuation"
            else:
                followup_strength = "medium"
                followup_semantic = "structure_continuation"
                execution_required_but_missing = not has_execution_evidence

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "followup_confirmed": followup_confirmed,
            "followup_assets": followup_assets,
        }
        event.metadata["case_summary"] = behavior_case.summary
        event.metadata["case_followup_steps"] = list(behavior_case.followup_steps or [])
        event.metadata["followup_confirmed"] = followup_confirmed
        event.metadata["followup_assets"] = followup_assets
        event.metadata["followup_label"] = label
        event.metadata["followup_detail"] = detail
        event.metadata["followup_strength"] = followup_strength
        event.metadata["followup_semantic"] = followup_semantic
        event.metadata["execution_required_but_missing"] = execution_required_but_missing
        event.metadata["exchange_followup_case"] = True
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["exchange_followup_anchor_event"] = bool((case_result or {}).get("exchange_followup_anchor"))
        event.metadata["exchange_followup_confirmed_event"] = bool((case_result or {}).get("exchange_followup_confirmed"))

        if followup_confirmed and not bool(event.metadata.get("exchange_followup_confirmation_applied")):
            boost = 0.14 if has_execution_evidence else 0.08
            event.confirmation_score = self._clamp(max(float(event.confirmation_score or 0.0), 0.60) + boost, 0.0, 0.90)
            for evidence in [
                "前序稳定币已进入交易场景",
                "同一交易所地址后续出现主流资产继续流入",
                "形成跨 token 结构延续观察",
            ]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if has_execution_evidence and event.intent_stage != "confirmed":
                event.intent_stage = "confirmed"
            elif event.intent_stage == "weak":
                event.intent_stage = "preliminary"
            intent_meta = dict(event.metadata.get("intent") or {})
            intent_meta["intent_stage"] = event.intent_stage
            intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
            intent_meta["intent_evidence"] = list(event.intent_evidence or [])
            intent_meta["exchange_cross_token_followup"] = True
            intent_meta["followup_strength"] = followup_strength
            intent_meta["followup_semantic"] = followup_semantic
            intent_meta["execution_required_but_missing"] = execution_required_but_missing
            event.metadata["intent"] = intent_meta
            event.metadata["exchange_followup_confirmation_applied"] = True

    def _apply_downstream_followup_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        if not self._is_downstream_followup_case(behavior_case):
            return

        watch_meta = watch_meta or {}
        metadata = getattr(behavior_case, "metadata", {}) or {}
        state_payload = self._apply_downstream_current_event_state_metadata(
            event=event,
            behavior_case=behavior_case,
            case_result=case_result,
        )
        runtime_adjacent_state = self._resolve_runtime_adjacent_anchor_state(
            behavior_case=behavior_case,
            case_result=case_result,
        )
        stage = str(state_payload.get("current_stage") or metadata.get("current_stage") or behavior_case.stage or "followup_opened")
        followup_type = str(state_payload.get("current_followup_type") or metadata.get("last_followup_type") or "")
        downstream_address = str(metadata.get("downstream_address") or behavior_case.watch_address or "").lower()
        downstream_label = str(metadata.get("downstream_label") or get_address_meta(downstream_address).get("display") or downstream_address)
        anchor_label = str(metadata.get("anchor_label") or watch_meta.get("label") or metadata.get("anchor_watch_address") or "")
        anchor_symbol = str(metadata.get("anchor_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        anchor_usd_value = float(metadata.get("anchor_usd_value") or 0.0)
        active_until = int(metadata.get("active_until") or metadata.get("expire_at") or 0)
        cooling_until = int(metadata.get("cooling_until") or 0)
        closing_until = int(metadata.get("closing_until") or 0)
        last_counterparty_label = str(metadata.get("last_counterparty_label") or event.metadata.get("counterparty_label") or "")
        notification_stage = stage
        event_is_anchor = bool(state_payload.get("current_event_is_anchor"))
        event_is_followup = bool(state_payload.get("current_event_is_followup"))
        followup_reason = str(state_payload.get("current_followup_reason") or metadata.get("lifecycle_reason") or "")
        runtime_state = "active"
        now_ts = int(event.ts or time.time())
        if closing_until and now_ts >= closing_until:
            runtime_state = "closed"
        elif cooling_until and now_ts >= cooling_until:
            runtime_state = "closing"
        elif active_until and now_ts >= active_until:
            runtime_state = "cooling"

        label = "超大额下游观察已开启"
        detail = f"{anchor_label} 刚向 {downstream_label} 转出 {_usd_text(anchor_usd_value)} {anchor_symbol}，已开启短时下游观察窗口。"
        reason = "当前更像重点地址刚把大额资金送往下游地址，优先看是否继续进入交易所、命中执行或出现分发。"
        path_label = f"{anchor_label} -> {downstream_label}"
        next_hint = "进入交易所 / 命中 swap_execution / 出现分发"

        if stage == "exchange_arrival_confirmed":
            label = "下游资金进入交易场景"
            detail = f"{downstream_label} 随后又把大额 {anchor_symbol} 转向 {last_counterparty_label or '交易所地址'}，更像进入交易场景。"
            reason = "这笔资金与前序锚点大额转移处于同一观察窗口内，已从单次转账升级为交易场景 follow-up。"
            path_label = f"{anchor_label} -> {downstream_label} -> {last_counterparty_label or '交易所地址'}"
        elif stage == "swap_execution_confirmed":
            label = "下游已升级为真实执行"
            detail = f"{downstream_label} 在观察窗口内出现大额执行路径，当前已从库存转移升级为更明确的执行行为。"
            reason = "后续动作直接命中 swap/协议路径，说明这笔资金已不只是临时库存停留。"
            path_label = f"{anchor_label} -> {downstream_label} -> {last_counterparty_label or '协议路径'}"
        elif stage == "distribution_confirmed":
            label = "下游开始大额分发"
            detail = f"{downstream_label} 已向多个地址拆分大额资金，当前更像进入分发/分仓阶段。"
            reason = "观察窗口内出现第二个高价值下游目的地，已超过单一去向噪声。"
            path_label = f"{anchor_label} -> {downstream_label} -> 多地址分发"
        elif stage == "downstream_seen":
            label = "下游继续观察中"
            detail = f"{downstream_label} 在观察窗口内再次出现后续动作，但当前仍只作为 case 延续观察。"
            reason = "已看到后续动作，但还不足以按强影响 follow-up 进入通知。"
            path_label = f"{anchor_label} -> {downstream_label} -> {last_counterparty_label or '后续去向'}"

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "anchor_label": anchor_label,
            "downstream_address": downstream_address,
            "downstream_label": downstream_label,
        }
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["case_summary"] = behavior_case.summary
        event.metadata["case_followup_steps"] = list(behavior_case.followup_steps or [])
        event.metadata["downstream_followup"] = True
        event.metadata["downstream_followup_active"] = True
        event.metadata["downstream_followup_anchor_event"] = event_is_anchor
        event.metadata["downstream_followup_confirmed_event"] = event_is_followup
        event.metadata["downstream_followup_type"] = followup_type
        event.metadata["downstream_followup_stage"] = stage
        event.metadata["followup_stage"] = stage
        event.metadata["followup_type"] = followup_type
        event.metadata["downstream_anchor_label"] = anchor_label
        event.metadata["downstream_anchor_address"] = str(metadata.get("anchor_watch_address") or "")
        event.metadata["downstream_anchor_usd_value"] = anchor_usd_value
        event.metadata["downstream_anchor_token"] = str(metadata.get("anchor_token") or event.token or "")
        event.metadata["downstream_object_label"] = downstream_label
        event.metadata["downstream_followup_label"] = label
        event.metadata["downstream_followup_detail"] = detail
        event.metadata["downstream_followup_reason"] = reason
        event.metadata["downstream_followup_stage_label"] = self._notification_stage_label(notification_stage or stage)
        event.metadata["downstream_followup_path_label"] = path_label
        event.metadata["downstream_followup_next_hint"] = next_hint
        event.metadata["anchor_tx_hash"] = str(metadata.get("root_tx_hash") or behavior_case.root_tx_hash or "")
        event.metadata["anchor_watch_address"] = str(metadata.get("anchor_watch_address") or "")
        event.metadata["downstream_bridge_candidate"] = bool(metadata.get("downstream_bridge_candidate"))
        event.metadata["downstream_bridge_applied"] = bool(metadata.get("downstream_bridge_applied"))
        event.metadata["downstream_bridge_reason"] = str(metadata.get("downstream_bridge_reason") or "")
        event.metadata["downstream_bridge_anchor_source"] = str(metadata.get("downstream_bridge_anchor_source") or "")
        event.metadata["downstream_bridge_counterparty_role"] = str(metadata.get("downstream_bridge_counterparty_role") or "")
        event.metadata["downstream_bridge_counterparty_label"] = str(metadata.get("downstream_bridge_counterparty_label") or "")
        event.metadata["downstream_case_anchor_watch_address"] = str(
            metadata.get("downstream_case_anchor_watch_address") or metadata.get("anchor_watch_address") or ""
        )
        event.metadata["downstream_case_match_mode"] = str(metadata.get("downstream_case_match_mode") or "")
        event.metadata["downstream_case_reused"] = bool(metadata.get("downstream_case_reused"))
        event.metadata["anchor_label"] = anchor_label
        event.metadata["downstream_address"] = downstream_address
        event.metadata["downstream_label"] = downstream_label
        event.metadata["downstream_followup_type_label"] = followup_type
        event.metadata["hop"] = int(metadata.get("hop") or 1)
        event.metadata["window_sec"] = int(metadata.get("window_sec") or 0)
        event.metadata["active_until"] = active_until
        event.metadata["cooling_until"] = cooling_until
        event.metadata["closing_until"] = closing_until
        event.metadata["downstream_runtime_state"] = runtime_state
        event.metadata["downstream_observation_reason"] = followup_reason
        event.metadata["anchor_usd_value"] = anchor_usd_value
        event.metadata["current_event_is_followup"] = event_is_followup
        event.metadata["downstream_case_id"] = behavior_case.case_id
        event.metadata.update(runtime_adjacent_state)
        self._apply_downstream_early_warning_state(
            event=event,
            behavior_case=behavior_case,
        )
        self._apply_downstream_case_history_metadata(
            event=event,
            behavior_case=behavior_case,
        )

        if stage in {"exchange_arrival_confirmed", "swap_execution_confirmed", "distribution_confirmed"} and not bool(event.metadata.get("downstream_followup_confirmation_applied")):
            boost = 0.12
            if stage == "swap_execution_confirmed":
                boost = 0.20
            elif stage == "exchange_arrival_confirmed":
                boost = 0.16
            event.confirmation_score = self._clamp(max(float(event.confirmation_score or 0.0), 0.58) + boost, 0.0, 0.95)
            if stage in {"exchange_arrival_confirmed", "swap_execution_confirmed"}:
                event.intent_stage = "confirmed"
            for evidence in [
                f"{anchor_label} 的超大额下游观察窗口已建立",
                f"{downstream_label} 在窗口内出现新的高价值后续动作",
            ]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if stage == "exchange_arrival_confirmed":
                evidence = "下游资金已进入交易所相关地址"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if stage == "swap_execution_confirmed":
                evidence = "下游地址已出现真实执行/协议路径"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            intent_meta = dict(event.metadata.get("intent") or {})
            intent_meta["intent_stage"] = event.intent_stage
            intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
            intent_meta["intent_evidence"] = list(event.intent_evidence or [])
            intent_meta["downstream_counterparty_followup"] = True
            event.metadata["intent"] = intent_meta
            event.metadata["downstream_followup_confirmation_applied"] = True

    def _downstream_early_warning_thresholds(self) -> dict:
        return {
            "enabled": bool(DOWNSTREAM_EARLY_WARNING_ENABLE),
            "min_anchor_usd": float(DOWNSTREAM_EARLY_WARNING_MIN_ANCHOR_USD),
            "min_event_usd": float(DOWNSTREAM_EARLY_WARNING_MIN_EVENT_USD),
            "min_confirmation": float(DOWNSTREAM_EARLY_WARNING_MIN_CONFIRMATION),
            "min_quality": float(DOWNSTREAM_EARLY_WARNING_MIN_QUALITY),
            "min_pricing_confidence": float(DOWNSTREAM_EARLY_WARNING_MIN_PRICING_CONFIDENCE),
            "min_resonance": float(DOWNSTREAM_EARLY_WARNING_MIN_RESONANCE),
            "min_abnormal_ratio": float(DOWNSTREAM_EARLY_WARNING_MIN_ABNORMAL_RATIO),
            "max_per_case": int(DOWNSTREAM_EARLY_WARNING_MAX_PER_CASE),
        }

    def _apply_downstream_early_warning_state(
        self,
        event: Event,
        signal=None,
        behavior_case=None,
        allowed: bool | None = None,
        reason: str | None = None,
        emitted: bool | None = None,
    ) -> dict:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        state_payload = {}
        if behavior_case is not None and self._is_downstream_followup_case(behavior_case):
            state_payload = self._apply_downstream_current_event_state_metadata(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
            )
        existing = event.metadata or {}
        emitted_stages = list(metadata.get("emitted_notification_stages") or [])
        anchor_usd_value = float(
            existing.get("anchor_usd_value")
            or existing.get("downstream_anchor_usd_value")
            or metadata.get("anchor_usd_value")
            or 0.0
        )
        current_event_is_anchor = (
            bool(state_payload.get("current_event_is_anchor"))
            if state_payload else
            (bool(existing.get("current_event_is_anchor")) if "current_event_is_anchor" in existing else bool(metadata.get("current_event_is_anchor")))
        )
        current_event_is_followup = (
            bool(state_payload.get("current_event_is_followup"))
            if state_payload else
            (bool(existing.get("current_event_is_followup")) if "current_event_is_followup" in existing else bool(metadata.get("current_event_is_followup")))
        )
        payload = {
            "downstream_early_warning_allowed": bool(existing.get("downstream_early_warning_allowed")) if allowed is None else bool(allowed),
            "downstream_early_warning_reason": (
                str(existing.get("downstream_early_warning_reason") or "")
                if reason is None else str(reason or "")
            ),
            "downstream_early_warning_thresholds": self._downstream_early_warning_thresholds(),
            "downstream_early_warning_emitted": (
                bool(existing.get("downstream_early_warning_emitted") or metadata.get("downstream_early_warning_emitted"))
                if emitted is None else bool(emitted)
            ),
            "downstream_early_warning_emitted_count": int(
                existing.get("downstream_early_warning_emitted_count")
                or metadata.get("downstream_early_warning_emitted_count")
                or 0
            ),
            "downstream_early_warning_signal_id": str(
                existing.get("downstream_early_warning_signal_id")
                or metadata.get("downstream_early_warning_signal_id")
                or ""
            ),
            "downstream_early_warning_stage_recorded": bool(
                existing.get("downstream_early_warning_stage_recorded")
                or metadata.get("downstream_early_warning_stage_recorded")
                or "followup_opened" in emitted_stages
            ),
            "anchor_usd_value": anchor_usd_value,
            "current_event_is_anchor": current_event_is_anchor,
            "current_event_is_followup": current_event_is_followup,
            "downstream_current_event_state_source": str(
                existing.get("downstream_current_event_state_source")
                or state_payload.get("downstream_current_event_state_source")
                or "case_metadata"
            ),
            "downstream_current_event_state_semantics": str(
                existing.get("downstream_current_event_state_semantics")
                or state_payload.get("downstream_current_event_state_semantics")
                or ""
            ),
            "downstream_current_event_state_compat_bool_source": str(
                existing.get("downstream_current_event_state_compat_bool_source")
                or state_payload.get("downstream_current_event_state_compat_bool_source")
                or ""
            ),
            "downstream_current_event_state_known": bool(
                existing.get("downstream_current_event_state_known")
                if "downstream_current_event_state_known" in existing
                else state_payload.get("downstream_current_event_state_known", True)
            ),
            "downstream_current_event_state_missing": bool(
                existing.get("downstream_current_event_state_missing")
                if "downstream_current_event_state_missing" in existing
                else state_payload.get("downstream_current_event_state_missing", False)
            ),
            "downstream_current_event_state_fail_closed": bool(
                existing.get("downstream_current_event_state_fail_closed")
                if "downstream_current_event_state_fail_closed" in existing
                else state_payload.get("downstream_current_event_state_fail_closed", False)
            ),
            "downstream_current_event_state_reason": str(
                existing.get("downstream_current_event_state_reason")
                or state_payload.get("downstream_current_event_state_reason")
                or ""
            ),
            "downstream_current_event_state_effective_label": str(
                existing.get("downstream_current_event_state_effective_label")
                or state_payload.get("downstream_current_event_state_effective_label")
                or ""
            ),
            "downstream_current_event_state_effective_bool_safe": (
                existing.get("downstream_current_event_state_effective_bool_safe")
                if "downstream_current_event_state_effective_bool_safe" in existing
                else state_payload.get("downstream_current_event_state_effective_bool_safe")
            ),
            "downstream_case_state_consistent": bool(
                existing.get("downstream_case_state_consistent")
                if "downstream_case_state_consistent" in existing
                else state_payload.get("downstream_case_state_consistent", True)
            ),
            "downstream_case_anchor_flag": bool(
                existing.get("downstream_case_anchor_flag")
                if "downstream_case_anchor_flag" in existing
                else state_payload.get("downstream_case_anchor_flag", current_event_is_anchor)
            ),
            "downstream_event_anchor_flag": bool(
                existing.get("downstream_event_anchor_flag")
                if "downstream_event_anchor_flag" in existing
                else current_event_is_anchor
            ),
            "downstream_case_followup_flag": bool(
                existing.get("downstream_case_followup_flag")
                if "downstream_case_followup_flag" in existing
                else state_payload.get("downstream_case_followup_flag", current_event_is_followup)
            ),
            "downstream_event_followup_flag": bool(
                existing.get("downstream_event_followup_flag")
                if "downstream_event_followup_flag" in existing
                else current_event_is_followup
            ),
        }
        event.metadata.update(payload)
        if signal is not None:
            signal.metadata.update(payload)
            signal.context.update(payload)
        return payload

    def _passes_downstream_impact_gate(
        self,
        event: Event,
        signal,
        behavior_case,
        gate_metrics: dict | None,
    ) -> tuple[bool, str]:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return True, "not_downstream_followup_case"

        gate_metrics = gate_metrics or {}
        metadata = getattr(behavior_case, "metadata", {}) or {}
        state_payload = {}
        if behavior_case is not None and self._is_downstream_followup_case(behavior_case):
            state_payload = self._apply_downstream_current_event_state_metadata(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
            )
        allowed_stages = {str(item or "").strip() for item in ADJACENT_WATCH_NOTIFY_ALLOWED_STAGES if item}
        stage = str(
            event.metadata.get("downstream_followup_stage")
            or metadata.get("current_stage")
            or getattr(behavior_case, "stage", "")
            or ""
        )
        followup_events = list(metadata.get("followup_events") or [])
        confirmation_score = float(
            event.confirmation_score
            or getattr(signal, "confirmation_score", 0.0)
            or gate_metrics.get("confirmation_score")
            or 0.0
        )
        quality_score = float(
            gate_metrics.get("adjusted_quality_score")
            or gate_metrics.get("quality_score")
            or getattr(signal, "quality_score", 0.0)
            or 0.0
        )
        pricing_confidence = float(
            getattr(signal, "pricing_confidence", 0.0)
            or gate_metrics.get("pricing_confidence")
            or event.pricing_confidence
            or 0.0
        )
        resonance_score = float(
            gate_metrics.get("resonance_score")
            or getattr(signal, "metadata", {}).get("resonance_score")
            or 0.0
        )
        abnormal_ratio = float(
            getattr(signal, "abnormal_ratio", 0.0)
            or gate_metrics.get("abnormal_ratio")
            or 0.0
        )
        anchor_usd_value = float(
            event.metadata.get("anchor_usd_value")
            or event.metadata.get("downstream_anchor_usd_value")
            or metadata.get("anchor_usd_value")
            or 0.0
        )
        current_event_is_anchor = bool(state_payload.get("current_event_is_anchor")) if state_payload else bool(
            event.metadata.get("current_event_is_anchor")
            or metadata.get("current_event_is_anchor")
        )

        if stage == "followup_opened":
            allowed = True
            reason = "downstream_early_warning_allowed"
            if not bool(DOWNSTREAM_EARLY_WARNING_ENABLE):
                allowed = False
                reason = "downstream_early_warning_disabled"
            elif not current_event_is_anchor:
                allowed = False
                reason = "downstream_early_warning_requires_anchor_event"
            elif float(event.usd_value or 0.0) < float(DOWNSTREAM_EARLY_WARNING_MIN_EVENT_USD or 0.0):
                allowed = False
                reason = "downstream_early_warning_event_usd_below_min"
            elif anchor_usd_value < float(DOWNSTREAM_EARLY_WARNING_MIN_ANCHOR_USD or 0.0):
                allowed = False
                reason = "downstream_early_warning_anchor_usd_below_min"
            elif confirmation_score < float(DOWNSTREAM_EARLY_WARNING_MIN_CONFIRMATION or 0.0):
                allowed = False
                reason = "downstream_early_warning_confirmation_below_min"
            elif quality_score < float(DOWNSTREAM_EARLY_WARNING_MIN_QUALITY or 0.0):
                allowed = False
                reason = "downstream_early_warning_quality_below_min"
            elif pricing_confidence < float(DOWNSTREAM_EARLY_WARNING_MIN_PRICING_CONFIDENCE or 0.0):
                allowed = False
                reason = "downstream_early_warning_pricing_below_min"
            elif not (
                resonance_score >= float(DOWNSTREAM_EARLY_WARNING_MIN_RESONANCE or 0.0)
                or abnormal_ratio >= float(DOWNSTREAM_EARLY_WARNING_MIN_ABNORMAL_RATIO or 0.0)
            ):
                allowed = False
                reason = "downstream_early_warning_impact_signal_below_min"
            event.metadata["downstream_observation_reason"] = str(reason or "")
            signal.metadata["downstream_observation_reason"] = str(reason or "")
            signal.context["downstream_observation_reason"] = str(reason or "")
            self._apply_downstream_early_warning_state(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
                allowed=allowed,
                reason=reason,
            )
            return allowed, reason

        if stage not in allowed_stages:
            return False, "stage_not_allowed"
        if len(followup_events) < max(int(ADJACENT_WATCH_NOTIFY_MIN_FOLLOWUP_COUNT or 1), 1):
            return False, "followup_count_below_notify_min"
        if float(event.usd_value or 0.0) < float(ADJACENT_WATCH_RUNTIME_MIN_USD or 0.0):
            return False, "runtime_min_usd_not_met"
        if confirmation_score < float(ADJACENT_WATCH_NOTIFY_MIN_CONFIRMATION or 0.0):
            return False, "confirmation_below_notify_min"
        if quality_score < float(ADJACENT_WATCH_NOTIFY_MIN_QUALITY or 0.0):
            return False, "quality_below_notify_min"
        if pricing_confidence < float(ADJACENT_WATCH_NOTIFY_MIN_PRICING_CONFIDENCE or 0.0):
            return False, "pricing_confidence_below_notify_min"
        if not (
            resonance_score >= float(ADJACENT_WATCH_NOTIFY_MIN_RESONANCE or 0.0)
            or abnormal_ratio >= float(ADJACENT_WATCH_NOTIFY_MIN_ABNORMAL_RATIO or 0.0)
        ):
            return False, "impact_signal_below_notify_min"
        return True, "allowed"

    def _passes_runtime_adjacent_execution_gate(
        self,
        event: Event,
        watch_meta: dict | None,
        behavior_case,
        gate_metrics: dict | None,
    ) -> tuple[bool, dict]:
        watch_meta = watch_meta or {}
        gate_metrics = gate_metrics or {}
        if str(watch_meta.get("watch_meta_source") or "") != "runtime_adjacent_watch":
            return True, {}
        if str((event.metadata or {}).get("monitor_type") or "") != "adjacent_watch":
            return True, {}
        if str(watch_meta.get("strategy_hint") or "") == "persisted_exchange_adjacent":
            return True, {}
        if self._is_downstream_confirmed_runtime_adjacent_event(event, behavior_case):
            return True, {}
        if event.kind not in {"swap", "token_transfer", "eth_transfer"}:
            return True, {}

        usd_value = float(event.usd_value or gate_metrics.get("usd_value") or 0.0)
        pricing_confidence = float(
            event.pricing_confidence
            or gate_metrics.get("pricing_confidence")
            or 0.0
        )
        quality_score = float(
            gate_metrics.get("adjusted_quality_score")
            or gate_metrics.get("quality_score")
            or 0.0
        )
        confirmation_score = float(event.confirmation_score or gate_metrics.get("confirmation_score") or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)

        failures = []
        if usd_value < float(ADJACENT_WATCH_RUNTIME_MIN_USD or 0.0):
            failures.append("usd_value_below_min")
        if pricing_confidence < float(ADJACENT_WATCH_NOTIFY_MIN_PRICING_CONFIDENCE or 0.0):
            failures.append("pricing_confidence_below_min")
        if quality_score < float(ADJACENT_WATCH_NOTIFY_MIN_QUALITY or 0.0):
            failures.append("quality_score_below_min")
        if not (
            confirmation_score >= float(ADJACENT_WATCH_NOTIFY_MIN_CONFIRMATION or 0.0)
            or resonance_score >= float(ADJACENT_WATCH_NOTIFY_MIN_RESONANCE or 0.0)
        ):
            failures.append("confirmation_or_resonance_below_min")

        if failures:
            return False, {
                "runtime_adjacent_gate_failures": failures,
            }
        return True, {}

    def _is_downstream_confirmed_runtime_adjacent_event(self, event: Event, behavior_case) -> bool:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return False
        stage = str(
            (event.metadata or {}).get("downstream_followup_stage")
            or getattr(behavior_case, "stage", "")
            or ""
        )
        allowed_stages = {str(item or "").strip() for item in ADJACENT_WATCH_NOTIFY_ALLOWED_STAGES if item}
        return stage in allowed_stages

    def _apply_smart_money_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        if not self._is_smart_money_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        execution_count = int(metadata.get("execution_count") or 0)
        cohort_addresses = list(metadata.get("cohort_addresses") or [])
        continuation_confirmed = bool(metadata.get("continuation_confirmed"))
        latest_symbol = str(metadata.get("latest_symbol") or event.metadata.get("token_symbol") or event.token or "")
        same_actor_continuation = bool(metadata.get("same_actor_continuation"))
        size_expansion_ratio = float(metadata.get("size_expansion_ratio") or 1.0)
        first_execution_usd = float(metadata.get("first_execution_usd") or 0.0)
        max_execution_usd = float(metadata.get("max_execution_usd") or 0.0)
        watch_strategy_role = str(metadata.get("watch_strategy_role") or event.strategy_role or "unknown")

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "execution_count": execution_count,
            "cohort_addresses": cohort_addresses,
            "continuation_confirmed": continuation_confirmed,
            "same_actor_continuation": same_actor_continuation,
            "size_expansion_ratio": size_expansion_ratio,
        }
        event.metadata["smart_money_case"] = True
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["smart_money_case_confirmed"] = continuation_confirmed
        event.metadata["smart_money_case_execution_count"] = execution_count
        event.metadata["smart_money_case_addresses"] = cohort_addresses
        event.metadata["smart_money_same_actor_continuation"] = same_actor_continuation
        event.metadata["smart_money_size_expansion_ratio"] = size_expansion_ratio
        event.metadata["smart_money_case_first_execution_usd"] = first_execution_usd
        event.metadata["smart_money_case_max_execution_usd"] = max_execution_usd
        event.metadata["smart_money_case_role"] = watch_strategy_role
        event.metadata["smart_money_case_label"] = f"{latest_symbol} 聪明钱连续执行"
        event.metadata["smart_money_case_detail"] = (
            f"{execution_count} 次同向真实执行，{len(cohort_addresses)} 个地址参与，放大量 {size_expansion_ratio:.2f}x"
            if execution_count > 1 else f"{latest_symbol} 首次真实执行，等待后续连续确认"
        )

        if continuation_confirmed and not bool(event.metadata.get("smart_money_case_confirmation_applied")):
            event.confirmation_score = self._clamp(max(float(event.confirmation_score or 0.0), 0.72) + 0.18, 0.0, 0.98)
            event.intent_stage = "confirmed"
            event.intent_confidence = self._clamp(max(float(event.intent_confidence or 0.0), 0.74) + 0.10, 0.0, 0.98)
            for evidence in [
                "短窗口内再次出现同向真实执行",
                "聪明钱执行连续性得到确认",
            ]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if same_actor_continuation:
                evidence = "同地址连续两次同向真实执行"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if size_expansion_ratio >= 1.22:
                evidence = f"执行金额相对首次放大 {size_expansion_ratio:.2f}x"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if len(cohort_addresses) >= 2:
                evidence = "多聪明钱地址出现同向真实执行"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            intent_meta = dict(event.metadata.get("intent") or {})
            intent_meta["intent_stage"] = event.intent_stage
            intent_meta["intent_confidence"] = round(float(event.intent_confidence or 0.0), 3)
            intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
            intent_meta["intent_evidence"] = list(event.intent_evidence or [])
            intent_meta["smart_money_execution_case"] = True
            event.metadata["intent"] = intent_meta
            event.metadata["smart_money_case_confirmation_applied"] = True

    def _apply_liquidation_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        del watch_meta, case_result
        if not self._is_liquidation_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        protocols = list(metadata.get("protocols_seen") or event.metadata.get("liquidation_protocols") or [])
        pools = list(metadata.get("pools_seen") or [event.address])
        stage = str(metadata.get("case_stage") or behavior_case.stage or "")
        risk_hits = int(metadata.get("risk_hits") or 0)
        execution_hits = int(metadata.get("execution_hits") or 0)
        max_score = float(metadata.get("max_liquidation_score") or event.metadata.get("liquidation_score") or 0.0)

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "protocols_seen": protocols,
            "pools_seen": pools,
            "risk_hits": risk_hits,
            "execution_hits": execution_hits,
            "max_liquidation_score": max_score,
        }
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["liquidation_case"] = True
        event.metadata["liquidation_case_protocols"] = protocols
        event.metadata["liquidation_case_pools"] = pools
        event.metadata["liquidation_case_risk_hits"] = risk_hits
        event.metadata["liquidation_case_execution_hits"] = execution_hits
        event.metadata["liquidation_case_stage"] = stage
        event.metadata["liquidation_case_confirmed"] = execution_hits >= 1
        event.metadata["liquidation_case_label"] = "Liquidation 风险簇" if stage != "execution_confirmed" else "Liquidation 执行簇"
        event.metadata["liquidation_case_detail"] = (
            f"{len(pools)} 个池子，{risk_hits} 次风险，{execution_hits} 次执行，协议 {('/'.join(protocols[:3]) or 'unknown')}"
        )

    def _sync_exchange_followup_to_signal(self, event: Event, signal, behavior_case) -> None:
        if behavior_case is None or not self._is_exchange_followup_case(behavior_case):
            return

        followup_fields = {
            "case_family": event.metadata.get("case_family", ""),
            "case_notification_stage": event.metadata.get("case_notification_stage"),
            "case_notification_allowed": event.metadata.get("case_notification_allowed"),
            "case_notification_suppressed": event.metadata.get("case_notification_suppressed"),
            "case_notification_reason": event.metadata.get("case_notification_reason"),
            "followup_confirmed": bool(event.metadata.get("followup_confirmed")),
            "followup_assets": list(event.metadata.get("followup_assets") or []),
            "followup_label": event.metadata.get("followup_label", ""),
            "followup_detail": event.metadata.get("followup_detail", ""),
            "followup_strength": event.metadata.get("followup_strength", ""),
            "followup_semantic": event.metadata.get("followup_semantic", ""),
            "execution_required_but_missing": bool(event.metadata.get("execution_required_but_missing")),
            "notification_stage_label": event.metadata.get("notification_stage_label", ""),
        }
        signal.metadata.update(followup_fields)
        signal.metadata["case"] = {
            **dict(signal.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": followup_fields["case_family"],
            "followup_confirmed": followup_fields["followup_confirmed"],
            "followup_assets": followup_fields["followup_assets"],
        }

    def _sync_downstream_followup_to_signal(self, event: Event, signal, behavior_case) -> None:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return

        self._apply_downstream_current_event_state_metadata(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
        )
        payload = {
            "case_family": str(event.metadata.get("case_family") or ""),
            "downstream_followup": bool(event.metadata.get("downstream_followup")),
            "downstream_followup_active": bool(event.metadata.get("downstream_followup_active")),
            "downstream_followup_anchor_event": bool(event.metadata.get("downstream_followup_anchor_event")),
            "downstream_followup_confirmed_event": bool(event.metadata.get("downstream_followup_confirmed_event")),
            "downstream_followup_type": str(event.metadata.get("downstream_followup_type") or ""),
            "downstream_followup_stage": str(event.metadata.get("downstream_followup_stage") or ""),
            "followup_stage": str(event.metadata.get("followup_stage") or event.metadata.get("downstream_followup_stage") or ""),
            "followup_type": str(event.metadata.get("followup_type") or event.metadata.get("downstream_followup_type") or ""),
            "downstream_anchor_label": str(event.metadata.get("downstream_anchor_label") or ""),
            "downstream_anchor_address": str(event.metadata.get("downstream_anchor_address") or ""),
            "downstream_anchor_usd_value": float(event.metadata.get("downstream_anchor_usd_value") or 0.0),
            "anchor_usd_value": float(event.metadata.get("anchor_usd_value") or event.metadata.get("downstream_anchor_usd_value") or 0.0),
            "downstream_object_label": str(event.metadata.get("downstream_object_label") or ""),
            "downstream_followup_label": str(event.metadata.get("downstream_followup_label") or ""),
            "downstream_followup_detail": str(event.metadata.get("downstream_followup_detail") or ""),
            "downstream_followup_reason": str(event.metadata.get("downstream_followup_reason") or ""),
            "downstream_followup_audit_reason": str(event.metadata.get("downstream_observation_reason") or ""),
            "downstream_followup_stage_label": str(event.metadata.get("downstream_followup_stage_label") or ""),
            "downstream_followup_path_label": str(event.metadata.get("downstream_followup_path_label") or ""),
            "downstream_followup_next_hint": str(event.metadata.get("downstream_followup_next_hint") or ""),
            "downstream_early_warning_allowed": bool(event.metadata.get("downstream_early_warning_allowed")),
            "downstream_early_warning_reason": str(event.metadata.get("downstream_early_warning_reason") or ""),
            "downstream_early_warning_thresholds": dict(event.metadata.get("downstream_early_warning_thresholds") or {}),
            "downstream_early_warning_emitted": bool(event.metadata.get("downstream_early_warning_emitted")),
            "downstream_early_warning_emitted_count": int(event.metadata.get("downstream_early_warning_emitted_count") or 0),
            "downstream_early_warning_signal_id": str(event.metadata.get("downstream_early_warning_signal_id") or ""),
            "downstream_early_warning_stage_recorded": bool(event.metadata.get("downstream_early_warning_stage_recorded")),
            "downstream_address": str(event.metadata.get("downstream_address") or ""),
            "downstream_label": str(event.metadata.get("downstream_label") or event.metadata.get("downstream_object_label") or ""),
            "anchor_watch_address": str(event.metadata.get("anchor_watch_address") or event.metadata.get("downstream_anchor_address") or ""),
            "anchor_label": str(event.metadata.get("anchor_label") or event.metadata.get("downstream_anchor_label") or ""),
            "anchor_tx_hash": str(event.metadata.get("anchor_tx_hash") or ""),
            "downstream_bridge_candidate": bool(event.metadata.get("downstream_bridge_candidate")),
            "downstream_bridge_applied": bool(event.metadata.get("downstream_bridge_applied")),
            "downstream_bridge_reason": str(event.metadata.get("downstream_bridge_reason") or ""),
            "downstream_bridge_anchor_source": str(event.metadata.get("downstream_bridge_anchor_source") or ""),
            "downstream_bridge_counterparty_role": str(event.metadata.get("downstream_bridge_counterparty_role") or ""),
            "downstream_bridge_counterparty_label": str(event.metadata.get("downstream_bridge_counterparty_label") or ""),
            "downstream_case_anchor_watch_address": str(event.metadata.get("downstream_case_anchor_watch_address") or ""),
            "downstream_case_match_mode": str(event.metadata.get("downstream_case_match_mode") or ""),
            "downstream_case_reused": bool(event.metadata.get("downstream_case_reused")),
            "current_event_is_anchor": bool(event.metadata.get("current_event_is_anchor")),
            "current_event_is_followup": bool(event.metadata.get("current_event_is_followup")),
            "current_followup_type": str(event.metadata.get("current_followup_type") or ""),
            "current_followup_reason": str(event.metadata.get("current_followup_reason") or ""),
            "downstream_current_event_state_source": str(event.metadata.get("downstream_current_event_state_source") or ""),
            "downstream_current_event_state_semantics": str(event.metadata.get("downstream_current_event_state_semantics") or ""),
            "downstream_current_event_state_compat_bool_source": str(event.metadata.get("downstream_current_event_state_compat_bool_source") or ""),
            "downstream_current_event_state_effective_label": str(
                event.metadata.get("downstream_current_event_state_effective_label") or ""
            ),
            "downstream_current_event_state_effective_bool_safe": event.metadata.get(
                "downstream_current_event_state_effective_bool_safe"
            ),
            "downstream_current_event_state_known": bool(event.metadata.get("downstream_current_event_state_known")),
            "downstream_current_event_state_missing": bool(event.metadata.get("downstream_current_event_state_missing")),
            "downstream_current_event_state_fail_closed": bool(event.metadata.get("downstream_current_event_state_fail_closed")),
            "downstream_current_event_state_reason": str(event.metadata.get("downstream_current_event_state_reason") or ""),
            "downstream_case_state_consistent": bool(event.metadata.get("downstream_case_state_consistent")),
            "downstream_case_anchor_flag": bool(event.metadata.get("downstream_case_anchor_flag")),
            "downstream_event_anchor_flag": bool(event.metadata.get("downstream_event_anchor_flag")),
            "downstream_case_followup_flag": bool(event.metadata.get("downstream_case_followup_flag")),
            "downstream_event_followup_flag": bool(event.metadata.get("downstream_event_followup_flag")),
            "runtime_adjacent_anchor_source": str(event.metadata.get("runtime_adjacent_anchor_source") or ""),
            "runtime_adjacent_anchor_flag": bool(event.metadata.get("runtime_adjacent_anchor_flag")),
            "runtime_adjacent_watch_consistent": bool(event.metadata.get("runtime_adjacent_watch_consistent")),
            "runtime_adjacent_case_id": str(event.metadata.get("runtime_adjacent_case_id") or ""),
            "hop": int(event.metadata.get("hop") or 1),
            "window_sec": int(event.metadata.get("window_sec") or 0),
            "downstream_runtime_state": str(event.metadata.get("downstream_runtime_state") or ""),
            "notification_stage_label": str(event.metadata.get("notification_stage_label") or ""),
            "emitted_stages": list(event.metadata.get("emitted_stages") or event.metadata.get("emitted_notification_stages") or []),
            "emitted_notification_count": int(event.metadata.get("emitted_notification_count") or 0),
            "downstream_notification_cap": int(event.metadata.get("downstream_notification_cap") or 0),
            "downstream_notification_count": int(event.metadata.get("downstream_notification_count") or 0),
            "downstream_notification_cap_source": str(event.metadata.get("downstream_notification_cap_source") or ""),
            "last_notification_stage": str(event.metadata.get("last_notification_stage") or ""),
            "last_notification_signal_id": str(event.metadata.get("last_notification_signal_id") or ""),
            "emitted_notification_history_version": int(event.metadata.get("emitted_notification_history_version") or 2),
            "emitted_notification_stage_source": str(event.metadata.get("emitted_notification_stage_source") or "unified_case_history"),
        }
        signal.metadata.update(payload)
        signal.context.update(payload)
        signal.metadata["case"] = {
            **dict(signal.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": payload["case_family"],
            "anchor_label": payload["downstream_anchor_label"],
            "downstream_address": payload["downstream_address"],
        }

    def _sync_smart_money_case_to_signal(self, event: Event, signal, behavior_case) -> None:
        if behavior_case is None or not self._is_smart_money_case(behavior_case):
            return

        payload = {
            "smart_money_case": True,
            "smart_money_case_confirmed": bool(event.metadata.get("smart_money_case_confirmed")),
            "smart_money_case_execution_count": int(event.metadata.get("smart_money_case_execution_count") or 0),
            "smart_money_case_addresses": list(event.metadata.get("smart_money_case_addresses") or []),
            "smart_money_same_actor_continuation": bool(event.metadata.get("smart_money_same_actor_continuation")),
            "smart_money_size_expansion_ratio": float(event.metadata.get("smart_money_size_expansion_ratio") or 1.0),
            "smart_money_case_label": event.metadata.get("smart_money_case_label", ""),
            "smart_money_case_detail": event.metadata.get("smart_money_case_detail", ""),
            "case_family": event.metadata.get("case_family", "") or getattr(behavior_case, "metadata", {}).get("case_family", ""),
        }
        signal.metadata.update(payload)
        signal.context.update(payload)
        signal.metadata["case"] = {
            **dict(signal.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "execution_count": payload["smart_money_case_execution_count"],
            "cohort_addresses": payload["smart_money_case_addresses"],
            "continuation_confirmed": payload["smart_money_case_confirmed"],
        }

    def _sync_liquidation_to_signal(self, event: Event, signal, behavior_case) -> None:
        payload = {
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "liquidation_score": float(event.metadata.get("liquidation_score") or 0.0),
            "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
            "liquidation_reason": str(event.metadata.get("liquidation_reason") or ""),
            "liquidation_evidence": list(event.metadata.get("liquidation_evidence") or []),
            "liquidation_protocols": list(event.metadata.get("liquidation_protocols") or []),
            "liquidation_roles": list(event.metadata.get("liquidation_roles") or []),
            "semantic_overlay": str(event.metadata.get("semantic_overlay") or ""),
            "liquidation_primary_candidate": bool(event.metadata.get("liquidation_primary_candidate")),
            "liquidation_case": bool(event.metadata.get("liquidation_case")),
            "liquidation_case_confirmed": bool(event.metadata.get("liquidation_case_confirmed")),
            "liquidation_case_label": str(event.metadata.get("liquidation_case_label") or ""),
            "liquidation_case_detail": str(event.metadata.get("liquidation_case_detail") or ""),
            "case_family": str(event.metadata.get("case_family") or ""),
        }
        signal.metadata.update(payload)
        signal.context.update(payload)
        if behavior_case is not None and self._is_liquidation_case(behavior_case):
            signal.metadata["case"] = {
                **dict(signal.metadata.get("case") or {}),
                "case_id": behavior_case.case_id,
                "status": behavior_case.status,
                "stage": behavior_case.stage,
                "summary": behavior_case.summary,
                "protocols_seen": list((behavior_case.metadata or {}).get("protocols_seen") or []),
                "pools_seen": list((behavior_case.metadata or {}).get("pools_seen") or []),
            }

    def _apply_case_notification_control(self, event: Event, signal, behavior_case) -> bool:
        if behavior_case is None or not (
            self._is_downstream_followup_case(behavior_case)
            or
            self._is_exchange_followup_case(behavior_case)
            or self._is_smart_money_case(behavior_case)
            or self._is_liquidation_case(behavior_case)
        ):
            return True

        stage = self._resolve_case_notification_stage(event, signal, behavior_case)
        allowed, reason = self._should_emit_case_signal(behavior_case, stage)
        self._apply_case_notification_metadata(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
            stage=stage,
            allowed=allowed,
            reason=reason,
        )
        if not allowed:
            return False

        return True

    def _resolve_case_notification_stage(self, event: Event, signal, behavior_case) -> str | None:
        if self._is_downstream_followup_case(behavior_case):
            return str(
                event.metadata.get("downstream_followup_stage")
                or getattr(behavior_case, "stage", "")
                or ""
            ) or None
        if self._is_exchange_followup_case(behavior_case):
            if bool(event.metadata.get("exchange_followup_confirmed_event")) and bool(event.metadata.get("followup_confirmed")):
                return "followup_confirmed"
            if bool(event.metadata.get("exchange_followup_anchor_event")) and not bool(event.metadata.get("followup_confirmed")):
                return "anchor_opened"
            return None

        if self._is_smart_money_case(behavior_case):
            if bool(event.metadata.get("smart_money_case_confirmed")) or str(getattr(behavior_case, "stage", "") or "") == "execution_followup_confirmed":
                return "continuation_confirmed"
            if str(getattr(behavior_case, "stage", "") or "") in {"execution_opened", "execution_tracking"}:
                return "execution_opened"
        if self._is_liquidation_case(behavior_case):
            stage = str(getattr(behavior_case, "stage", "") or event.metadata.get("liquidation_case_stage") or "")
            if stage == "execution_confirmed":
                return "execution_confirmed"
            if stage == "risk_escalating":
                return "risk_escalating"
            if stage == "risk_opened":
                return "risk_opened"
        return None

    def _should_emit_case_signal(self, behavior_case, stage: str | None) -> tuple[bool, str]:
        if self.followup_tracker is None:
            return True, "no_followup_tracker"
        if not (
            self._is_downstream_followup_case(behavior_case)
            or
            self._is_exchange_followup_case(behavior_case)
            or self._is_smart_money_case(behavior_case)
            or self._is_liquidation_case(behavior_case)
        ):
            return True, "not_exchange_followup_case"
        try:
            return self.followup_tracker.case_notification_decision(behavior_case, stage)
        except Exception as e:
            print(f"case 通知判断失败: {e}")
            return False, "case_notification_decision_error"

    def _mark_case_signal_emitted(self, behavior_case, stage: str | None, signal) -> None:
        if self.followup_tracker is None or behavior_case is None or not stage:
            return
        try:
            self.followup_tracker.mark_case_notification_emitted(behavior_case, stage, signal)
            if self._is_downstream_followup_case(behavior_case):
                self.state_manager.mark_adjacent_watch_notification(
                    getattr(behavior_case, "watch_address", ""),
                    ts=int(time.time()),
                    stage=stage,
                )
        except Exception as e:
            print(f"case 通知状态标记失败: {e}")

    def _apply_case_notification_metadata(
        self,
        event: Event,
        signal,
        behavior_case,
        stage: str | None,
        allowed: bool,
        reason: str,
    ) -> None:
        metadata = behavior_case.metadata if behavior_case is not None else {}
        case_id = str(getattr(behavior_case, "case_id", "") or "")
        case_family = str(metadata.get("case_family") or event.metadata.get("case_family") or "")
        if self._is_downstream_followup_case(behavior_case):
            self._apply_downstream_current_event_state_metadata(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
            )
        case_history_payload = self._apply_downstream_case_history_metadata(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
        )
        payload = {
            "case_notification_stage": stage,
            "case_notification_allowed": bool(allowed),
            "case_notification_suppressed": not bool(allowed),
            "case_notification_reason": reason,
            "pending_case_notification": bool(allowed),
            "pending_case_notification_stage": stage if allowed else "",
            "pending_case_notification_reason": reason if allowed else "",
            "pending_case_notification_case_id": case_id if allowed else "",
            "pending_case_notification_case_family": case_family if allowed else "",
            "delivered_notification": False,
            "delivered_notification_stage": "",
            "delivered_notification_reason": "",
            "downstream_observation_reason": event.metadata.get("downstream_observation_reason", "") or reason,
            "followup_confirmed": bool(metadata.get("followup_confirmed")),
            "followup_assets": list(metadata.get("followup_tokens_seen") or []),
            "followup_label": event.metadata.get("followup_label", ""),
            "followup_detail": event.metadata.get("followup_detail", ""),
            "downstream_followup_label": event.metadata.get("downstream_followup_label", ""),
            "downstream_followup_detail": event.metadata.get("downstream_followup_detail", ""),
            "liquidation_case_label": event.metadata.get("liquidation_case_label", ""),
            "liquidation_case_detail": event.metadata.get("liquidation_case_detail", ""),
            "notification_stage_label": self._notification_stage_label(stage),
            "emitted_stages": list(case_history_payload.get("emitted_stages") or metadata.get("emitted_notification_stages") or []),
            "last_notification_stage": str(case_history_payload.get("last_notification_stage") or metadata.get("last_notification_stage") or ""),
            "last_notification_signal_id": str(case_history_payload.get("last_notification_signal_id") or metadata.get("last_notification_signal_id") or ""),
            "downstream_early_warning_emitted": bool(
                case_history_payload.get("downstream_early_warning_emitted")
                or metadata.get("downstream_early_warning_emitted")
            ),
            "downstream_early_warning_emitted_count": int(
                case_history_payload.get("downstream_early_warning_emitted_count")
                or metadata.get("downstream_early_warning_emitted_count")
                or 0
            ),
            "downstream_early_warning_signal_id": str(
                case_history_payload.get("downstream_early_warning_signal_id")
                or metadata.get("downstream_early_warning_signal_id")
                or ""
            ),
            "downstream_early_warning_stage_recorded": bool(
                case_history_payload.get("downstream_early_warning_stage_recorded")
                or metadata.get("downstream_early_warning_stage_recorded")
            ),
            "emitted_notification_history_version": int(
                case_history_payload.get("emitted_notification_history_version")
                or metadata.get("emitted_notification_history_version")
                or 2
            ),
            "emitted_notification_stage_source": str(
                case_history_payload.get("emitted_notification_stage_source")
                or metadata.get("emitted_notification_stage_source")
                or "unified_case_history"
            ),
        }
        event.metadata.update(payload)
        if self._is_downstream_followup_case(behavior_case):
            if stage == "followup_opened":
                event.metadata["downstream_observation_reason"] = str(reason or "")
                signal.metadata["downstream_observation_reason"] = str(reason or "")
                signal.context["downstream_observation_reason"] = str(reason or "")
            self._apply_downstream_early_warning_state(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
                allowed=bool(allowed) if stage == "followup_opened" else None,
                reason=reason if stage == "followup_opened" else None,
            )
        signal.context.update(payload)
        signal.metadata.update(payload)
        decision = {
            "stage": stage,
            "allowed": bool(allowed),
            "reason": reason,
            "signal_id": str(getattr(signal, "signal_id", "") or getattr(signal, "event_id", "") or ""),
            "emitted_stages": list(payload.get("emitted_stages") or []),
            "last_notification_stage": str(payload.get("last_notification_stage") or ""),
            "last_notification_signal_id": str(payload.get("last_notification_signal_id") or ""),
            "downstream_early_warning_emitted": bool(payload.get("downstream_early_warning_emitted")),
            "downstream_early_warning_emitted_count": int(payload.get("downstream_early_warning_emitted_count") or 0),
            "downstream_early_warning_signal_id": str(payload.get("downstream_early_warning_signal_id") or ""),
            "downstream_early_warning_stage_recorded": bool(payload.get("downstream_early_warning_stage_recorded")),
            "emitted_notification_history_version": int(payload.get("emitted_notification_history_version") or 2),
            "emitted_notification_stage_source": str(payload.get("emitted_notification_stage_source") or "unified_case_history"),
        }
        if behavior_case is not None:
            behavior_case.metadata["last_notification_decision"] = decision

    def _apply_notification_delivery_metadata(
        self,
        event: Event,
        signal,
        delivered: bool,
        reason: str,
    ) -> None:
        stage = str(
            event.metadata.get("pending_case_notification_stage")
            or signal.metadata.get("pending_case_notification_stage")
            or ""
        )
        payload = {
            "delivered_notification": bool(delivered),
            "delivered_notification_stage": stage if delivered else "",
            "delivered_notification_reason": str(reason or ""),
            "pending_case_notification": False,
        }
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)

    def _notification_stage_label(self, stage: str | None) -> str:
        if stage == "followup_opened":
            return "首条预警"
        if stage == "downstream_seen":
            return "后续动作"
        if stage == "exchange_arrival_confirmed":
            return "进入交易场景"
        if stage == "swap_execution_confirmed":
            return "执行确认"
        if stage == "distribution_confirmed":
            return "分发确认"
        if stage == "cooling":
            return "观察降温"
        if stage == "closing":
            return "窗口关闭中"
        if stage == "expired":
            return "观察过期"
        if stage == "anchor_opened":
            return "观察建立"
        if stage == "followup_confirmed":
            return "观察升级"
        if stage == "execution_opened":
            return "首次执行"
        if stage == "continuation_confirmed":
            return "连续确认"
        if stage == "risk_opened":
            return "风险开启"
        if stage == "risk_escalating":
            return "风险升级"
        if stage == "execution_confirmed":
            return "执行确认"
        return ""

    def _should_emit_delivery_notification(self, event: Event, signal) -> bool:
        allowed = bool(can_emit_delivery_notification(event, signal))
        reason = self._delivery_policy_reason(event, signal, allowed)
        self._apply_delivery_policy_state(
            event=event,
            signal=signal,
            allowed=allowed,
            reason=reason,
            evaluated=True,
            evaluated_at_stage="pipeline_pre_send",
        )
        return allowed

    def _lp_final_output_gate_state(self, event: Event, signal) -> dict[str, object]:
        signal_context = getattr(signal, "context", {}) or {}
        signal_metadata = getattr(signal, "metadata", {}) or {}
        event_metadata = event.metadata or {}

        def _first(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        return {
            "status": str(
                _first(
                    signal_context.get("trade_opportunity_status"),
                    signal_metadata.get("trade_opportunity_status"),
                    event_metadata.get("trade_opportunity_status"),
                    "NONE",
                )
                or "NONE"
            ).strip().upper(),
            "side": str(
                _first(
                    signal_context.get("trade_opportunity_side"),
                    signal_metadata.get("trade_opportunity_side"),
                    event_metadata.get("trade_opportunity_side"),
                    "NONE",
                )
                or "NONE"
            ).strip().upper(),
            "primary_blocker": str(
                _first(
                    signal_context.get("trade_opportunity_primary_blocker"),
                    signal_metadata.get("trade_opportunity_primary_blocker"),
                    event_metadata.get("trade_opportunity_primary_blocker"),
                    "",
                )
                or ""
            ),
            "final_source": str(
                _first(
                    signal_context.get("final_trading_output_source"),
                    signal_metadata.get("final_trading_output_source"),
                    event_metadata.get("final_trading_output_source"),
                    "",
                )
                or ""
            ).strip().lower(),
            "final_label": str(
                _first(
                    signal_context.get("final_trading_output_label"),
                    signal_metadata.get("final_trading_output_label"),
                    event_metadata.get("final_trading_output_label"),
                    "",
                )
                or ""
            ),
            "asset_market_state_key": str(
                _first(
                    signal_context.get("asset_market_state_key"),
                    signal_metadata.get("asset_market_state_key"),
                    event_metadata.get("asset_market_state_key"),
                    "",
                )
                or ""
            ).strip().upper(),
            "opportunity_gate_passed": bool(
                _first(
                    signal_context.get("opportunity_gate_passed"),
                    signal_metadata.get("opportunity_gate_passed"),
                    event_metadata.get("opportunity_gate_passed"),
                    False,
                )
            ),
            "opportunity_gate_required": bool(
                _first(
                    signal_context.get("opportunity_gate_required"),
                    signal_metadata.get("opportunity_gate_required"),
                    event_metadata.get("opportunity_gate_required"),
                    False,
                )
            ),
            "opportunity_gate_failure_reason": str(
                _first(
                    signal_context.get("opportunity_gate_failure_reason"),
                    signal_metadata.get("opportunity_gate_failure_reason"),
                    event_metadata.get("opportunity_gate_failure_reason"),
                    "",
                )
                or ""
            ),
            "trade_action_key": str(
                _first(
                    signal_context.get("trade_action_key"),
                    signal_metadata.get("trade_action_key"),
                    event_metadata.get("trade_action_key"),
                    "",
                )
                or ""
            ).strip().upper(),
            "legacy_chase_downgrade_reason": str(
                _first(
                    signal_context.get("legacy_chase_downgrade_reason"),
                    signal_metadata.get("legacy_chase_downgrade_reason"),
                    event_metadata.get("legacy_chase_downgrade_reason"),
                    "",
                )
                or ""
            ),
        }

    def _apply_lp_final_output_gate_payload(self, event: Event, signal, payload: dict[str, object]) -> None:
        event.metadata.update(payload)
        signal.metadata.update(payload)
        signal.context.update(payload)

    def _enforce_lp_final_output_gate(self, event: Event, signal, *, telegram_should_send: bool) -> tuple[bool, str]:
        if not telegram_should_send:
            return False, ""

        gate_state = self._lp_final_output_gate_state(event, signal)
        status = str(gate_state["status"])
        trade_action_key = str(gate_state["trade_action_key"])
        asset_market_state_key = str(gate_state["asset_market_state_key"])
        forced_gate_reason = ""
        if trade_action_key in LEGACY_CHASE_ACTION_KEYS and status != "VERIFIED":
            forced_gate_reason = "legacy_chase_requires_verified_opportunity"
        elif asset_market_state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE", "TRADEABLE_LONG", "TRADEABLE_SHORT"} and status not in {"CANDIDATE", "VERIFIED", "BLOCKED"}:
            forced_gate_reason = "asset_market_state_candidate_without_opportunity"
        gate_allowed, gate_reason = validate_final_trading_output_gate(
            trade_opportunity_status=status,
            final_trading_output_source=str(gate_state["final_source"]),
            final_trading_output_label=str(gate_state["final_label"]),
            opportunity_gate_passed=bool(gate_state["opportunity_gate_passed"]),
            asset_market_state_key=asset_market_state_key,
            delivered_to_trader=True,
        )
        if forced_gate_reason:
            gate_allowed = False
            gate_reason = gate_reason or forced_gate_reason
        if gate_allowed:
            return True, ""

        side = str(gate_state["side"])
        primary_blocker = str(gate_state["primary_blocker"])
        legacy_downgrade_reason = str(gate_state["legacy_chase_downgrade_reason"])

        if status in {"VERIFIED", "CANDIDATE", "BLOCKED"}:
            canonical_label = canonical_final_trading_output_label(status, side, primary_blocker)
            canonical_gate_passed = bool(gate_state["opportunity_gate_passed"]) or status in {"CANDIDATE", "BLOCKED"}
            canonical_allowed, _ = validate_final_trading_output_gate(
                trade_opportunity_status=status,
                final_trading_output_source="trade_opportunity",
                final_trading_output_label=canonical_label,
                opportunity_gate_passed=canonical_gate_passed,
                asset_market_state_key=str(gate_state["asset_market_state_key"]),
                delivered_to_trader=True,
            )
            if canonical_allowed:
                payload: dict[str, object] = {
                    "final_trading_output_source": "trade_opportunity",
                    "final_trading_output_label": canonical_label,
                    "final_trading_output_allowed": True,
                    "telegram_should_send": True,
                    "telegram_suppression_reason": "",
                    "telegram_update_kind": {
                        "VERIFIED": "opportunity",
                        "CANDIDATE": "candidate",
                        "BLOCKED": "risk_blocker",
                    }.get(status, "opportunity"),
                    "opportunity_gate_required": True,
                    "opportunity_gate_passed": True,
                    "opportunity_gate_failure_reason": "",
                }
                if trade_action_key in LEGACY_CHASE_ACTION_KEYS and status != "VERIFIED":
                    payload["legacy_chase_downgraded"] = True
                    payload["legacy_chase_downgrade_reason"] = legacy_downgrade_reason or (
                        "candidate_only_not_verified"
                        if status == "CANDIDATE"
                        else f"blocked:{primary_blocker or 'blocked'}"
                    )
                self._apply_lp_final_output_gate_payload(event, signal, payload)
                return True, ""

        suppression_reason = gate_reason or str(gate_state["opportunity_gate_failure_reason"] or "") or "opportunity_gate_rejected"
        payload = {
            "final_trading_output_source": "suppressed",
            "final_trading_output_label": "",
            "final_trading_output_allowed": False,
            "telegram_should_send": False,
            "telegram_suppression_reason": suppression_reason,
            "telegram_update_kind": "suppressed",
            "opportunity_gate_required": True,
            "opportunity_gate_passed": False,
            "opportunity_gate_failure_reason": suppression_reason,
        }
        if trade_action_key in LEGACY_CHASE_ACTION_KEYS:
            payload["legacy_chase_downgraded"] = True
            payload["legacy_chase_downgrade_reason"] = legacy_downgrade_reason or suppression_reason
        self._apply_lp_final_output_gate_payload(event, signal, payload)
        return False, suppression_reason

    def _resolve_behavior_case_for_signal(self, signal):
        if self.followup_tracker is None:
            return None
        return self.followup_tracker.get_case(getattr(signal, "case_id", ""))

    def finalize_notification_delivery(
        self,
        event: Event,
        signal,
        delivered: bool,
        behavior_case=None,
        behavior: dict | None = None,
        gate_metrics: dict | None = None,
        archive_status: dict | None = None,
    ) -> None:
        behavior_case = behavior_case or self._resolve_behavior_case_for_signal(signal)
        delivery_reason = "notifier_delivered" if delivered else "notifier_send_failed"
        notifier_sent_at = int(time.time())
        archive_ts = int(getattr(signal, "archive_ts", 0) or event.archive_ts or time.time())
        if self._is_lp_event(event=event):
            first_chain_seen_at = int(
                signal.context.get("first_chain_seen_at")
                or signal.metadata.get("first_chain_seen_at")
                or event.metadata.get("first_chain_seen_at")
                or 0
            )
            end_to_end_latency_ms = int(max(notifier_sent_at - first_chain_seen_at, 0) * 1000) if first_chain_seen_at > 0 else 0
            lp_latency_payload = {
                "notifier_sent_at": notifier_sent_at,
                "lp_end_to_end_latency_ms": end_to_end_latency_ms,
            }
            event.metadata.update(lp_latency_payload)
            signal.metadata.update(lp_latency_payload)
            signal.context.update(lp_latency_payload)
            outcome_record = dict(signal.metadata.get("lp_outcome_record") or event.metadata.get("lp_outcome_record") or {})
            if outcome_record.get("record_id"):
                updated_record = self.state_manager.mark_lp_outcome_notification(
                    str(outcome_record.get("record_id") or ""),
                    notifier_sent_at=notifier_sent_at,
                    delivered=bool(delivered),
                )
                if updated_record:
                    event.metadata["lp_outcome_record"] = updated_record
                    signal.metadata["lp_outcome_record"] = updated_record
                    signal.context["lp_outcome_record"] = updated_record
                    self.quality_manager.sync_from_state_manager(force=True)
            opportunity_id = str(
                signal.context.get("trade_opportunity_id")
                or signal.metadata.get("trade_opportunity_id")
                or event.metadata.get("trade_opportunity_id")
                or ""
            )
            if opportunity_id:
                updated_opportunity = self.trade_opportunity_manager.mark_notification(
                    opportunity_id,
                    notifier_sent_at=notifier_sent_at,
                    delivered=bool(delivered),
                )
                if updated_opportunity:
                    event.metadata.update(updated_opportunity)
                    signal.metadata.update(updated_opportunity)
                    signal.context.update(updated_opportunity)
            self._apply_lp_prealert_diagnostics(
                event,
                signal,
                delivery_allowed=bool(delivered),
                delivery_block_reason="" if delivered else "notifier_send_failed",
            )
        self._apply_notification_delivery_metadata(
            event=event,
            signal=signal,
            delivered=delivered,
            reason=delivery_reason,
        )
        record_delivery_notification(event, signal, delivered)
        if delivered:
            stage = str(
                event.metadata.get("pending_case_notification_stage")
                or signal.metadata.get("pending_case_notification_stage")
                or ""
            ) or None
            self._mark_case_signal_emitted(behavior_case, stage, signal)
            if self._is_downstream_followup_case(behavior_case) and stage == "followup_opened":
                self._apply_downstream_early_warning_state(
                    event=event,
                    signal=signal,
                    behavior_case=behavior_case,
                    emitted=True,
                )
            self._apply_downstream_case_history_metadata(
                event=event,
                signal=signal,
                behavior_case=behavior_case,
            )
            self.quality_gate.mark_emitted(event)
        else:
            self._apply_silent_reason(
                event=event,
                signal=signal,
                stage="notifier_delivery",
                reason_code="notifier_send_failed",
                reason_detail="notifier_send_failed",
                behavior_case=behavior_case,
                gate_metrics=gate_metrics,
                delivery_policy_allowed=bool(event.metadata.get("delivery_policy_allowed")),
                impact_gate_allowed=event.metadata.get("downstream_impact_gate_allowed"),
                cooldown_allowed=event.metadata.get("cooldown_allowed"),
            )

        self._archive_signal_state(
            signal,
            event,
            archive_status or {},
            archive_ts,
            sent_to_telegram=bool(delivered),
            delivery_decision=delivery_reason,
        )
        self._archive_delivery_audit(
            event=event,
            signal=signal,
            behavior=behavior,
            gate_metrics=gate_metrics,
            stage="notifier_delivery",
            gate_reason=delivery_reason,
            archive_status=archive_status or {},
            archive_ts=archive_ts,
            audit_extras={
                "delivered": bool(delivered),
                "pending_notification_stage": str(event.metadata.get("pending_case_notification_stage") or ""),
                "pending_notification_reason": str(event.metadata.get("pending_case_notification_reason") or ""),
                "delivered_notification_stage": str(event.metadata.get("delivered_notification_stage") or ""),
            },
        )

    def _is_exchange_followup_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "exchange_cross_token_followup"

    def _is_downstream_followup_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "downstream_counterparty_followup"

    def _is_smart_money_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "smart_money_execution_case"

    def _is_liquidation_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "liquidation_case_family"

    def _record_counterparty(self, event: Event, watch_context: dict | None, parsed: dict) -> None:
        counterparty = ""
        if watch_context:
            counterparty = str(watch_context.get("counterparty") or "").lower()
        if not counterparty:
            counterparty = str(parsed.get("counterparty") or "").lower()
        if not counterparty:
            return

        counterparty_meta = (watch_context or {}).get("counterparty_meta") or get_address_meta(counterparty)
        self.state_manager.record_counterparty(
            address=event.address,
            counterparty=counterparty,
            usd_value=float(event.usd_value or 0.0),
            ts=int(event.ts or 0),
            metadata={
                "role": counterparty_meta.get("role", "unknown"),
                "strategy_role": counterparty_meta.get("strategy_role", "unknown"),
                "semantic_role": counterparty_meta.get("semantic_role", "unknown"),
                "intent_type": event.intent_type,
            },
        )

    def _parsed_archive_payload(
        self,
        parsed: dict,
        event: Event,
        watch_meta: dict,
        behavior: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
    ) -> dict:
        return {
            **parsed,
            "event_id": event.event_id,
            "tx_hash": str(event.tx_hash or parsed.get("tx_hash") or ""),
            "parsed_kind": str(parsed.get("parsed_kind") or event.kind or "parsed_event"),
            "role_group": str(parsed.get("role_group") or event.metadata.get("role_group") or ""),
            "parse_status": str(parsed.get("parse_status") or parsed.get("status") or "parsed"),
            "parsed_at": int(parsed.get("parsed_at") or event.metadata.get("parsed_at") or event.ts or 0),
            "case_id": event.case_id,
            "followup_stage": event.followup_stage,
            "followup_status": event.followup_status,
            "intent_type": event.intent_type,
            "intent_confidence": float(event.intent_confidence or 0.0),
            "intent_stage": event.intent_stage,
            "confirmation_score": float(event.confirmation_score or 0.0),
            "watch_meta": event.metadata.get("watch_meta", {}),
            "behavior": behavior,
            "recent_counterparties": address_snapshot.get("recent_counterparties", []),
            "token_open_case_ids": token_snapshot.get("open_case_ids", []),
            "watch_label": watch_meta.get("label", ""),
            "pool_snapshot": pool_snapshot,
            "case_family": str(event.metadata.get("case_family") or ""),
            "downstream_followup": bool(event.metadata.get("downstream_followup")),
            "notification_stage": str(
                event.metadata.get("delivered_notification_stage")
                or event.metadata.get("pending_case_notification_stage")
                or event.metadata.get("case_notification_stage")
                or ""
            ),
            "lp_prealert_candidate": bool(event.metadata.get("lp_prealert_candidate")),
            "lp_prealert_candidate_reason": str(event.metadata.get("lp_prealert_candidate_reason") or ""),
            "lp_prealert_gate_passed": bool(event.metadata.get("lp_prealert_gate_passed")),
            "lp_prealert_gate_fail_reason": str(event.metadata.get("lp_prealert_gate_fail_reason") or ""),
            "lp_prealert_first_leg": bool(event.metadata.get("lp_prealert_first_leg")),
            "lp_prealert_major_override_used": bool(event.metadata.get("lp_prealert_major_override_used")),
            "lp_alert_stage_candidate": str(
                event.metadata.get("lp_alert_stage")
                or event.metadata.get("lp_stage_decision")
                or ""
            ),
            "pending_notification_stage": str(event.metadata.get("pending_case_notification_stage") or ""),
            "delivered_notification_stage": str(event.metadata.get("delivered_notification_stage") or ""),
            "anchor_tx_hash": str(event.metadata.get("anchor_tx_hash") or ""),
            "anchor_watch_address": str(event.metadata.get("anchor_watch_address") or ""),
            "anchor_label": str(event.metadata.get("anchor_label") or ""),
            "downstream_address": str(event.metadata.get("downstream_address") or ""),
            "downstream_label": str(event.metadata.get("downstream_label") or ""),
            "downstream_followup_type": str(event.metadata.get("downstream_followup_type") or ""),
            "followup_stage": str(event.metadata.get("followup_stage") or ""),
            "followup_type": str(event.metadata.get("followup_type") or ""),
            "hop": int(event.metadata.get("hop") or 0),
            "window_sec": int(event.metadata.get("window_sec") or 0),
            "runtime_state": str(event.metadata.get("downstream_runtime_state") or ""),
            "lp_analysis": dict(event.metadata.get("lp_analysis") or {}),
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "liquidation_score": float(event.metadata.get("liquidation_score") or 0.0),
            "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
            "liquidation_protocols": list(event.metadata.get("liquidation_protocols") or []),
            "liquidation_evidence_count": len(event.metadata.get("liquidation_evidence") or []),
            "liquidation_reason": str(event.metadata.get("liquidation_reason") or ""),
            "is_liquidation_protocol_related": bool(parsed.get("is_liquidation_protocol_related")),
            "possible_keeper_executor": bool(parsed.get("possible_keeper_executor")),
            "possible_vault_or_auction": bool(parsed.get("possible_vault_or_auction")),
            "replay_source": str(event.metadata.get("replay_source") or ""),
        }

    def _build_event_id(self, event: Event) -> str:
        raw = "|".join([
            str(event.tx_hash or ""),
            str(event.address or ""),
            str(event.token or "native"),
            str(event.kind or ""),
            str(event.side or ""),
            str(int(event.ts or 0)),
        ])
        return "evt_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def _build_signal_id(self, signal, event: Event) -> str:
        raw = "|".join([
            str(event.event_id or ""),
            str(getattr(signal, "type", "")),
            str(getattr(signal, "tier", "")),
            str(round(float(getattr(signal, "confidence", 0.0) or 0.0), 4)),
        ])
        return "sig_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    async def _evaluate_pricing(self, parsed: dict) -> dict:
        return await self.price_service.evaluate_event_pricing(parsed)

    def _classify_intent(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict,
        watch_meta: dict,
    ) -> dict:
        if self._is_clmm_partial_support_event(event=event, parsed=parsed):
            return {
                "intent_type": str(parsed.get("intent_type") or "clmm_partial_support_observation"),
                "intent_confidence": float(parsed.get("intent_confidence") or 0.42),
                "information_level": str(parsed.get("information_level") or "low"),
                "confirmation_score": float(parsed.get("confirmation_score") or 0.18),
                "intent_evidence": list(parsed.get("intent_evidence") or []),
                "basis": {
                    "monitor_type": str(parsed.get("monitor_type") or ""),
                    "candidate_status": str(parsed.get("clmm_candidate_status") or ""),
                    "manager_protocol": str(parsed.get("clmm_manager_protocol") or ""),
                    "manager_address": str(parsed.get("clmm_manager_address") or ""),
                    "partial_reason": str(parsed.get("clmm_partial_reason") or ""),
                },
            }
        if self._is_clmm_position_event(event=event, parsed=parsed):
            return {
                "intent_type": str(parsed.get("intent_type") or "clmm_position_open"),
                "intent_confidence": float(parsed.get("intent_confidence") or 0.68),
                "information_level": str(parsed.get("information_level") or "medium"),
                "confirmation_score": float(parsed.get("confirmation_score") or 0.42),
                "intent_evidence": list(parsed.get("intent_evidence") or []),
                "basis": {
                    "monitor_type": str(parsed.get("monitor_type") or ""),
                    "position_key": str((parsed.get("clmm_context") or {}).get("position_key") or ""),
                    "base_action": str((parsed.get("clmm_context") or {}).get("base_action") or ""),
                },
            }
        if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed):
            return self.lp_analyzer.preliminary_intent(event, parsed)

        inferred = parsed.get("inferred_context") or {}
        counterparty = str(watch_context.get("counterparty") or parsed.get("counterparty") or "").lower()
        counterparty_meta = watch_context.get("counterparty_meta") or get_address_meta(counterparty)

        watch_strategy_role = str(watch_meta.get("strategy_role") or "unknown")
        watch_semantic_role = str(watch_meta.get("semantic_role") or "unknown")
        counterparty_strategy_role = str(counterparty_meta.get("strategy_role") or "unknown")
        counterparty_role = str(counterparty_meta.get("role") or "unknown")

        is_stable = bool(parsed.get("is_stablecoin_flow") or (event.token or "").lower() in STABLE_TOKEN_CONTRACTS)
        is_exchange_related = bool(parsed.get("is_exchange_related"))
        is_router_related = bool(parsed.get("is_router_related"))
        is_protocol_related = bool(parsed.get("is_protocol_related"))
        possible_internal_transfer = bool(
            parsed.get("possible_internal_transfer")
            or watch_context.get("direction") == "内部划转"
        )
        exchange_internality = str(parsed.get("exchange_internality") or "no")
        exchange_transfer_purpose = str(parsed.get("exchange_transfer_purpose") or "exchange_unknown_flow")
        exchange_transfer_confidence = float(parsed.get("exchange_transfer_confidence") or 0.0)
        direction = str(event.side or watch_context.get("direction") or "")
        watch_is_exchange = watch_strategy_role.startswith("exchange_")
        counterparty_is_exchange = counterparty_strategy_role.startswith("exchange_")
        confirmed_exchange_internal = exchange_internality == "confirmed"
        likely_exchange_internal = exchange_internality == "likely"

        usd_value = float(event.usd_value or 0.0)
        participant_count = len(parsed.get("participant_addresses") or [])
        next_hop_count = len(parsed.get("next_hop_addresses") or [])
        single_hop = participant_count <= 2 and next_hop_count <= 1
        counterparty_unknown = (
            counterparty_role == "unknown"
            and counterparty_strategy_role == "unknown"
            and not counterparty_is_exchange
        )
        route_like = is_router_related or is_protocol_related
        external_non_exchange = not counterparty_is_exchange and not possible_internal_transfer

        if usd_value < 1500:
            size_bucket = "small"
        elif usd_value < 10000:
            size_bucket = "medium"
        else:
            size_bucket = "large"

        if event.kind == "swap":
            return {
                "intent_type": "swap_execution",
                "intent_confidence": 0.98,
                "information_level": "high",
                "confirmation_score": 0.58,
                "intent_evidence": ["当前事件本身是链上实际 swap"],
                "basis": {
                    "kind": event.kind,
                    "side": direction,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "is_router_related": is_router_related,
                },
            }

        if possible_internal_transfer or confirmed_exchange_internal:
            if watch_strategy_role == "market_maker_wallet" or counterparty_strategy_role == "market_maker_wallet":
                return {
                    "intent_type": "market_making_inventory_move",
                    "intent_confidence": 0.68,
                    "information_level": "medium",
                    "confirmation_score": 0.36,
                    "intent_evidence": ["路径存在 confirmed internal / 库存迁移特征，更像库存迁移"],
                    "basis": {
                        "possible_internal_transfer": True,
                        "watch_strategy_role": watch_strategy_role,
                        "counterparty_strategy_role": counterparty_strategy_role,
                        "exchange_internality": exchange_internality,
                        "exchange_transfer_purpose": exchange_transfer_purpose,
                    },
                }

            return {
                "intent_type": "internal_rebalance",
                "intent_confidence": 0.64,
                "information_level": "low",
                "confirmation_score": 0.34,
                "intent_evidence": ["路径更像同实体内部归集/调拨"],
                "basis": {
                    "possible_internal_transfer": True,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "exchange_internality": exchange_internality,
                    "exchange_transfer_purpose": exchange_transfer_purpose,
                },
            }

        if likely_exchange_internal:
            return {
                "intent_type": "internal_rebalance",
                "intent_confidence": max(0.52, min(exchange_transfer_confidence, 0.62)),
                "information_level": "low",
                "confirmation_score": 0.26,
                "intent_evidence": ["存在 likely same-entity 证据，更像交易所内部流转"],
                "basis": {
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "exchange_internality": exchange_internality,
                    "exchange_transfer_purpose": exchange_transfer_purpose,
                },
            }

        if event.kind != "swap" and watch_strategy_role == "market_maker_wallet" and route_like:
            return {
                "intent_type": "market_making_inventory_move",
                "intent_confidence": 0.64,
                "information_level": "medium",
                "confirmation_score": 0.32,
                "intent_evidence": ["做市地址与路由/协议发生非 swap 资金变动，更像库存调节"],
                "basis": {
                    "watch_strategy_role": watch_strategy_role,
                    "is_router_related": is_router_related,
                    "is_protocol_related": is_protocol_related,
                    "size_bucket": size_bucket,
                },
            }

        # 重点修正：交易所热钱包非 swap 事件不再一律 pure_transfer
        if watch_is_exchange and event.kind != "swap":
            if direction == "流入":
                if size_bucket == "large" and external_non_exchange:
                    return {
                        "intent_type": "exchange_deposit_candidate",
                        "intent_confidence": 0.56,
                        "information_level": "medium",
                        "confirmation_score": 0.32,
                        "intent_evidence": ["大额资产正在流入交易所热钱包，具备进入交易场景特征"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_strategy_role": counterparty_strategy_role,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

                if size_bucket == "medium" and external_non_exchange and (counterparty_unknown or is_stable):
                    return {
                        "intent_type": "exchange_deposit_candidate",
                        "intent_confidence": 0.51,
                        "information_level": "low",
                        "confirmation_score": 0.26,
                        "intent_evidence": ["中等规模资金正流入交易所热钱包，先保留为交易场景线索"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

            if direction == "流出":
                if is_stable and external_non_exchange and counterparty_unknown and size_bucket == "large":
                    return {
                        "intent_type": "possible_buy_preparation",
                        "intent_confidence": 0.58,
                        "information_level": "medium",
                        "confirmation_score": 0.34,
                        "intent_evidence": ["大额稳定币正从交易所热钱包流向未知外部地址，更像入场前资金准备"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                            "counterparty_strategy_role": counterparty_strategy_role,
                        },
                    }

                if external_non_exchange and size_bucket == "large":
                    return {
                        "intent_type": "exchange_withdraw_candidate",
                        "intent_confidence": 0.56,
                        "information_level": "medium",
                        "confirmation_score": 0.30,
                        "intent_evidence": ["大额资产正在离开交易所热钱包，当前先按外流方向线索处理"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                            "counterparty_strategy_role": counterparty_strategy_role,
                        },
                    }

                if is_stable and external_non_exchange and size_bucket == "medium":
                    return {
                        "intent_type": "exchange_withdraw_candidate",
                        "intent_confidence": 0.52,
                        "information_level": "low",
                        "confirmation_score": 0.28,
                        "intent_evidence": ["中等规模稳定币正离开交易所热钱包，先保留为外流观察线索"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.40 if size_bucket == "small" and single_hop else 0.44,
                "information_level": "low",
                "confirmation_score": 0.20 if size_bucket == "small" else 0.24,
                "intent_evidence": ["交易所热钱包发生非 swap 资金流，当前先按行为通知保守处理"],
                "basis": {
                    "watch_role": watch_strategy_role,
                    "counterparty_role": counterparty_strategy_role,
                    "direction": direction,
                    "size_bucket": size_bucket,
                    "single_hop": single_hop,
                },
            }

        if is_exchange_related and (counterparty_is_exchange or watch_is_exchange):
            if not watch_is_exchange and counterparty_is_exchange and direction == "流出":
                if is_stable:
                    return {
                        "intent_type": "possible_buy_preparation",
                        "intent_confidence": 0.56,
                        "information_level": "low",
                        "confirmation_score": 0.26,
                        "intent_evidence": ["稳定资产正在流向交易场景，先记为行为线索"],
                        "basis": {"exchange_leg": "outbound_stable", "counterparty": counterparty_strategy_role},
                    }
                return {
                    "intent_type": "exchange_deposit_candidate",
                    "intent_confidence": 0.60,
                    "information_level": "low",
                    "confirmation_score": 0.28,
                    "intent_evidence": ["token 正在流向交易所相关地址，当前仅保留观察性判断"],
                    "basis": {"exchange_leg": "outbound_token", "counterparty": counterparty_strategy_role},
                }

            if not watch_is_exchange and counterparty_is_exchange and direction == "流入":
                return {
                    "intent_type": "exchange_withdraw_candidate",
                    "intent_confidence": 0.58,
                    "information_level": "low",
                    "confirmation_score": 0.26,
                    "intent_evidence": ["资金正在离开交易场景，仍需后续动作确认"],
                    "basis": {"exchange_leg": "inbound_from_exchange", "counterparty": counterparty_strategy_role},
                }

        if event.kind != "swap" and direction in {"流入", "流出"}:
            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.44,
                "information_level": "low",
                "confirmation_score": 0.20,
                "intent_evidence": ["当前仅观察到单笔资金动作，先按重点地址行为通知处理"],
                "basis": {
                    "direction": direction,
                    "watch_semantic_role": watch_semantic_role,
                    "is_stablecoin_flow": is_stable,
                    "size_bucket": size_bucket,
                },
            }

        if event.kind in {"token_transfer", "eth_transfer"}:
            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.46,
                "information_level": "low",
                "confirmation_score": 0.20,
                "intent_evidence": ["当前仅观察到单笔转移，未看到交易执行"],
                "basis": {
                    "kind": event.kind,
                    "direction": direction,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "inferred": inferred,
                    "size_bucket": size_bucket,
                },
            }

        return {
            "intent_type": "unknown_intent",
            "intent_confidence": 0.2,
            "information_level": "low",
            "confirmation_score": 0.1,
            "intent_evidence": ["缺少足够路径信息"],
            "basis": {"kind": event.kind, "direction": direction},
        }
    def _confirm_intent(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict,
        watch_meta: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
        behavior: dict,
        preliminary_intent: dict,
    ) -> dict:
        if self._is_clmm_partial_support_event(event=event, parsed=parsed):
            return {
                "intent_type": str(preliminary_intent.get("intent_type") or parsed.get("intent_type") or "clmm_partial_support_observation"),
                "intent_confidence": min(
                    0.48,
                    max(
                        float(preliminary_intent.get("intent_confidence") or 0.0),
                        float(parsed.get("intent_confidence") or 0.0),
                        0.42,
                    ),
                ),
                "information_level": str(preliminary_intent.get("information_level") or parsed.get("information_level") or "low"),
                "confirmation_score": min(
                    0.24,
                    max(
                        float(preliminary_intent.get("confirmation_score") or 0.0),
                        float(parsed.get("confirmation_score") or 0.0),
                        0.18,
                    ),
                ),
                "intent_evidence": list(preliminary_intent.get("intent_evidence") or [])[:4],
                "basis": {
                    "monitor_type": str(parsed.get("monitor_type") or ""),
                    "candidate_status": str(parsed.get("clmm_candidate_status") or ""),
                    "manager_protocol": str(parsed.get("clmm_manager_protocol") or ""),
                    "manager_address": str(parsed.get("clmm_manager_address") or ""),
                    "partial_reason": str(parsed.get("clmm_partial_reason") or ""),
                },
            }
        if self._is_clmm_position_event(event=event, parsed=parsed):
            clmm_context = parsed.get("clmm_context") or {}
            confirmation_score = max(
                float(preliminary_intent.get("confirmation_score") or 0.0),
                float(parsed.get("confirmation_score") or 0.0),
            )
            if str(preliminary_intent.get("intent_type") or "") in {"clmm_range_shift", "clmm_inventory_recenter"}:
                confirmation_score = max(confirmation_score, 0.52)
            if str(preliminary_intent.get("intent_type") or "") == "clmm_position_close":
                confirmation_score = max(confirmation_score, 0.56)
            return {
                "intent_type": str(preliminary_intent.get("intent_type") or parsed.get("intent_type") or "clmm_position_open"),
                "intent_confidence": max(
                    float(preliminary_intent.get("intent_confidence") or 0.0),
                    float(parsed.get("intent_confidence") or 0.0),
                ),
                "information_level": str(preliminary_intent.get("information_level") or parsed.get("information_level") or "medium"),
                "confirmation_score": min(0.92, confirmation_score),
                "intent_evidence": list(preliminary_intent.get("intent_evidence") or [])[:4] + [
                    item
                    for item in [
                        f"position={clmm_context.get('position_key')}" if clmm_context.get("position_key") else "",
                        f"base_action={clmm_context.get('base_action')}" if clmm_context.get("base_action") else "",
                    ]
                    if item
                ],
                "basis": {
                    "monitor_type": str(parsed.get("monitor_type") or ""),
                    "position_key": str(clmm_context.get("position_key") or ""),
                    "base_action": str(clmm_context.get("base_action") or ""),
                },
            }
        if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed):
            return self.lp_analyzer.confirm_intent(
                event=event,
                parsed=parsed,
                address_snapshot=address_snapshot,
                pool_snapshot=pool_snapshot,
                token_snapshot=token_snapshot,
                behavior=behavior,
                preliminary_intent=preliminary_intent,
            )

        counterparty_meta = watch_context.get("counterparty_meta") or get_address_meta(
            str(watch_context.get("counterparty") or parsed.get("counterparty") or "").lower()
        )
        watch_strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        counterparty_strategy_role = str(counterparty_meta.get("strategy_role") or parsed.get("counterparty_strategy_role") or "unknown")
        exchange_sensitive = self._is_exchange_sensitive_role(watch_strategy_role) or self._is_exchange_sensitive_role(counterparty_strategy_role)
        watch_is_exchange = self._is_exchange_sensitive_role(watch_strategy_role)
        exchange_internality = str(parsed.get("exchange_internality") or "no")
        confirmed_internal_like = exchange_internality == "confirmed" or bool(parsed.get("possible_internal_transfer"))
        likely_internal_like = exchange_internality == "likely"

        recent_window = address_snapshot.get("windows", {}).get("15m", {}).get("recent") or address_snapshot.get("recent") or []
        prior_events = [item for item in recent_window if item.tx_hash != event.tx_hash]
        direction_bucket = self._direction_bucket(event.side)
        same_direction_prior = [item for item in prior_events if self._direction_bucket(item.side) == direction_bucket]
        same_token_prior = [
            item for item in same_direction_prior
            if (item.token or "").lower() == (event.token or "").lower()
        ]
        same_intent_prior = [item for item in same_direction_prior if item.intent_type == event.intent_type]
        prior_prep_count = self._prior_preparation_count(event, prior_events)
        recent_internal_count = sum(
            1 for item in prior_events if item.intent_type in {"internal_rebalance", "market_making_inventory_move"}
        )

        abnormal_ratio = self._abnormal_ratio(address_snapshot, event)
        relative_address_size = self._relative_address_size(address_snapshot, event)
        resonance = self._side_resonance(token_snapshot, event.side)
        resonance_score = float(resonance.get("resonance_score") or 0.0)
        behavior_type = str(behavior.get("behavior_type") or "normal")

        score = self._clamp(0.16 + 0.45 * float(preliminary_intent.get("intent_confidence") or 0.0), 0.0, 1.0)
        evidence = []

        if event.kind == "swap":
            score += 0.12
            evidence.append("当前事件本身是链上真实 swap")

        if len(same_token_prior) >= 1:
            score += 0.12
            evidence.append("同地址 15 分钟内重复出现同 token 同向路径")
        elif len(same_intent_prior) >= 1:
            score += 0.08
            evidence.append("同地址 15 分钟内重复出现同类资金路径")
        elif len(same_direction_prior) >= 2:
            score += 0.06
            evidence.append("同地址 15 分钟内出现连续同向资金流")

        if event.intent_type == "swap_execution" and prior_prep_count >= 1:
            score += 0.16
            evidence.append("前序准备动作已在短时间内转为实际交易执行")
        elif event.intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        } and len(same_direction_prior) >= 1:
            score += 0.10
            evidence.append("交易场景相关路径出现短时重复")

        if int(resonance.get("same_side_high_quality_addresses") or 0) >= 2:
            score += 0.16
            evidence.append("短窗口内多个高质量地址同向触发")
        elif int(resonance.get("same_side_unique_addresses") or 0) >= 3:
            score += 0.10
            evidence.append("短窗口内多地址同向共振")

        if int(resonance.get("same_side_smart_money_addresses") or 0) >= 2:
            score += 0.10
            evidence.append("短窗口内多个聪明钱地址同向")

        if bool(resonance.get("leader_follow")):
            score += 0.06
            evidence.append("存在高质量地址带动其他地址跟随")

        if event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
            if confirmed_internal_like:
                score += 0.18
                evidence.append("路径更像已确认的同体系地址调拨或库存迁移")
            elif likely_internal_like:
                score += 0.08
                evidence.append("存在 likely same-entity 线索，当前仅按疑似内部流转处理")

        if event.intent_type == "market_making_inventory_move" and behavior_type == "inventory_management":
            score += 0.12
            evidence.append("做市地址短时出现双向换手，更像库存管理")

        if exchange_sensitive and event.kind != "swap":
            if confirmed_internal_like:
                if event.intent_type not in {"internal_rebalance", "market_making_inventory_move"}:
                    score -= 0.16
                    evidence.append("存在交易所内部划转特征，削弱外部交易推断")
            elif likely_internal_like:
                if event.intent_type not in {"internal_rebalance", "market_making_inventory_move"}:
                    score -= 0.08
                    evidence.append("存在 likely same-entity 线索，先压低外部交易推断")

            if (
                len(same_direction_prior) == 0
                and abnormal_ratio < 1.6
                and relative_address_size < 1.4
                and resonance_score < 0.45
            ):
                score -= 0.18
                evidence.append("单笔交易所路径，缺少连续性和共振确认")

            if recent_internal_count >= 1 and event.intent_type in {
                "exchange_deposit_candidate",
                "exchange_withdraw_candidate",
                "possible_buy_preparation",
                "possible_sell_preparation",
            }:
                score -= 0.08
                evidence.append("同地址近期已出现内部调拨样式")

        if event.pricing_status in {"unknown", "unavailable"}:
            score -= 0.08
            evidence.append("定价可信度较弱")
        elif event.usd_value_estimated:
            score -= 0.03

        if event.intent_type in {"pure_transfer", "unknown_intent"} and resonance_score < 0.4 and not same_intent_prior:
            score -= 0.08

        score = self._clamp(score, 0.0, 1.0)
        if likely_internal_like and event.intent_type in {"internal_rebalance", "market_making_inventory_move"}:
            score = min(score, 0.66)
        if score >= 0.78:
            stage = "confirmed"
        elif score >= 0.52:
            stage = "preliminary"
        else:
            stage = "weak"

        intent_confidence = self._clamp(
            0.55 * float(preliminary_intent.get("intent_confidence") or 0.0) + 0.45 * score,
            0.0,
            1.0,
        )
        if exchange_sensitive and event.kind != "swap" and stage != "confirmed":
            intent_confidence = min(intent_confidence, 0.74)
        if stage == "weak":
            intent_confidence = min(intent_confidence, 0.62)

        information_level = str(preliminary_intent.get("information_level") or "low")
        if stage == "confirmed":
            if event.kind == "swap" or resonance_score >= 0.6 or prior_prep_count >= 1:
                information_level = "high"
            else:
                information_level = "medium"
        elif stage == "weak":
            information_level = "low"
        elif exchange_sensitive and event.kind != "swap":
            information_level = "low"

        if not evidence:
            evidence.append("当前主要来自单跳路径规则，尚缺连续确认")

        sequence_confirmed = (
            len(same_token_prior) >= 2
            or prior_prep_count >= 1
            or resonance_score >= 0.7
        )
        exchange_behavior_supported = (
            sequence_confirmed
            or len(same_direction_prior) >= 1
            or abnormal_ratio >= 1.9
            or relative_address_size >= 1.75
            or resonance_score >= 0.55
        )

        # 交易所相关非 swap 事件默认更保守：
        # 更偏“重点地址行为通知”，只有出现明显短时重复路径才允许提升。
        if watch_is_exchange and event.kind != "swap":
            if event.intent_type not in {"internal_rebalance", "market_making_inventory_move"} and not exchange_behavior_supported:
                stage = "weak"
                score = min(score, 0.48)
                intent_confidence = min(intent_confidence, 0.52)
                information_level = "low"
                evidence.append("监控对象为交易所侧重点地址，当前优先按行为通知保守处理")
            elif stage == "confirmed" and not sequence_confirmed:
                stage = "preliminary"
                intent_confidence = min(intent_confidence, 0.68)
                information_level = "medium" if abnormal_ratio >= 2.2 or resonance_score >= 0.6 else "low"
                evidence.append("交易所侧事件当前具备规模或异常特征，但仍等待连续路径确认")

        if event.intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        } and event.kind != "swap":
            if not exchange_behavior_supported:
                if watch_is_exchange:
                    stage = "weak"
                    intent_confidence = min(intent_confidence, 0.52)
                else:
                    if stage == "confirmed":
                        stage = "preliminary"
                    intent_confidence = min(intent_confidence, 0.58)
                information_level = "low"
                evidence.append("尚未看到足够的连续路径确认，当前仅保留为观察性推断")
            elif stage == "confirmed" and not sequence_confirmed:
                stage = "preliminary"
                intent_confidence = min(intent_confidence, 0.66 if watch_is_exchange else 0.62)
                information_level = "medium" if abnormal_ratio >= 2.0 or resonance_score >= 0.55 else "low"
                evidence.append("当前已具备一定规模或异常特征，但仍优先等待连续路径确认")

        unique_evidence = []
        for item in evidence:
            if item not in unique_evidence:
                unique_evidence.append(item)

        return {
            **preliminary_intent,
            "intent_type": event.intent_type,
            "intent_confidence": round(intent_confidence, 3),
            "information_level": information_level,
            "intent_stage": stage,
            "confirmation_score": round(score, 3),
            "intent_evidence": unique_evidence[:4],
            "sequence": {
                "same_direction_prior_count": len(same_direction_prior),
                "same_token_prior_count": len(same_token_prior),
                "same_intent_prior_count": len(same_intent_prior),
                "prior_preparation_count": prior_prep_count,
            },
            "resonance": resonance,
            "exchange_noise_sensitive": exchange_sensitive,
            "abnormal_ratio": round(abnormal_ratio, 3),
            "relative_address_size": round(relative_address_size, 3),
        }

    def _apply_confirmed_intent(self, event: Event, confirmed_intent: dict) -> None:
        event.intent_type = str(confirmed_intent.get("intent_type") or event.intent_type)
        event.intent_confidence = float(confirmed_intent.get("intent_confidence") or event.intent_confidence)
        event.intent_stage = str(confirmed_intent.get("intent_stage") or "preliminary")
        event.confirmation_score = float(confirmed_intent.get("confirmation_score") or 0.0)
        event.intent_evidence = list(confirmed_intent.get("intent_evidence") or [])
        event.metadata["intent"] = confirmed_intent
        if self._is_lp_event(event=event):
            event.metadata["lp_analysis"] = {
                "same_pool_continuity": int(confirmed_intent.get("same_pool_continuity") or 0),
                "multi_pool_resonance": int(confirmed_intent.get("multi_pool_resonance") or 0),
                "reserve_skew": float(confirmed_intent.get("reserve_skew") or 0.0),
                "action_intensity": float(confirmed_intent.get("action_intensity") or 0.0),
                "pool_volume_surge_ratio": float(confirmed_intent.get("pool_volume_surge_ratio") or 0.0),
                "pool_window_trade_count": int(confirmed_intent.get("pool_window_trade_count") or 0),
                "pool_window_usd_total": float(confirmed_intent.get("pool_window_usd_total") or 0.0),
                "abnormal_ratio": float(confirmed_intent.get("abnormal_ratio") or 0.0),
                "market_impact_hint": str(confirmed_intent.get("market_impact_hint") or ""),
                "lp_structure_min_usd_per_event": float(confirmed_intent.get("lp_structure_min_usd_per_event") or 0.0),
                "lp_continuity_eligible": bool(confirmed_intent.get("lp_continuity_eligible")),
                "lp_resonance_eligible": bool(confirmed_intent.get("lp_resonance_eligible")),
                "lp_continuity_filtered_by_min_usd": int(confirmed_intent.get("lp_continuity_filtered_by_min_usd") or 0),
                "lp_resonance_filtered_by_min_usd": int(confirmed_intent.get("lp_resonance_filtered_by_min_usd") or 0),
            }

    def _apply_liquidation_overlay(
        self,
        event: Event,
        parsed: dict,
        watch_meta: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
    ) -> None:
        detection = self.liquidation_detector.detect(
            event=event,
            parsed=parsed,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
        )
        event.metadata["liquidation"] = detection
        event.metadata["liquidation_score"] = float(detection.get("liquidation_score") or 0.0)
        event.metadata["liquidation_stage"] = str(detection.get("liquidation_stage") or "none")
        event.metadata["liquidation_side"] = str(detection.get("liquidation_side") or "unknown")
        event.metadata["liquidation_reason"] = str(detection.get("liquidation_reason") or "")
        event.metadata["liquidation_evidence"] = list(detection.get("liquidation_evidence") or [])
        event.metadata["liquidation_protocols"] = list(detection.get("liquidation_protocols") or [])
        event.metadata["liquidation_roles"] = list(detection.get("liquidation_roles") or [])
        event.metadata["semantic_overlay"] = str(detection.get("semantic_overlay") or "")
        event.metadata["liquidation_confidence"] = float(detection.get("liquidation_confidence") or 0.0)
        event.metadata["liquidation_primary_candidate"] = bool(detection.get("primary_candidate"))

        if event.metadata["liquidation_stage"] in {"risk", "execution"}:
            for evidence in list(event.metadata.get("liquidation_evidence") or [])[:3]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if event.metadata["liquidation_stage"] == "risk":
                event.confirmation_score = self._clamp(float(event.confirmation_score or 0.0) + 0.04, 0.0, 0.96)
            elif event.metadata["liquidation_stage"] == "execution":
                event.confirmation_score = self._clamp(float(event.confirmation_score or 0.0) + 0.10, 0.0, 0.98)
                if event.intent_stage == "weak":
                    event.intent_stage = "preliminary"

        intent_meta = dict(event.metadata.get("intent") or {})
        intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
        intent_meta["intent_evidence"] = list(event.intent_evidence or [])
        intent_meta["liquidation_stage"] = event.metadata["liquidation_stage"]
        intent_meta["liquidation_score"] = event.metadata["liquidation_score"]
        intent_meta["liquidation_side"] = event.metadata["liquidation_side"]
        intent_meta["liquidation_protocols"] = list(event.metadata.get("liquidation_protocols") or [])
        intent_meta["semantic_overlay"] = str(event.metadata.get("semantic_overlay") or "")
        event.metadata["intent"] = intent_meta

    def _prior_preparation_count(self, event: Event, prior_events: list[Event]) -> int:
        if event.side == "买入":
            expected = {"possible_buy_preparation", "exchange_withdraw_candidate"}
        elif event.side == "卖出":
            expected = {"possible_sell_preparation", "exchange_deposit_candidate"}
        elif event.side == "流入":
            expected = {"exchange_withdraw_candidate"}
        elif event.side == "流出":
            expected = {"exchange_deposit_candidate", "possible_buy_preparation", "possible_sell_preparation"}
        else:
            expected = {
                "possible_buy_preparation",
                "possible_sell_preparation",
                "exchange_withdraw_candidate",
                "exchange_deposit_candidate",
            }
        return sum(1 for item in prior_events if item.intent_type in expected)

    def _side_resonance(self, token_snapshot: dict, side: str | None) -> dict:
        bucket = self._direction_bucket(side)
        resonance_5m = token_snapshot.get("resonance_5m") or {}
        resonance_15m = token_snapshot.get("resonance_15m") or {}

        if bucket == "buy":
            same_side_unique_5m = int(resonance_5m.get("buy_unique_addresses") or 0)
            same_side_high_quality_5m = int(resonance_5m.get("buy_high_quality_addresses") or 0)
            same_side_smart_money_5m = int(resonance_5m.get("buy_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("buy_leader_follow"))
        elif bucket == "sell":
            same_side_unique_5m = int(resonance_5m.get("sell_unique_addresses") or 0)
            same_side_high_quality_5m = int(resonance_5m.get("sell_high_quality_addresses") or 0)
            same_side_smart_money_5m = int(resonance_5m.get("sell_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("sell_leader_follow"))
        else:
            same_side_unique_5m = max(int(resonance_5m.get("buy_unique_addresses") or 0), int(resonance_5m.get("sell_unique_addresses") or 0))
            same_side_high_quality_5m = max(int(resonance_5m.get("buy_high_quality_addresses") or 0), int(resonance_5m.get("sell_high_quality_addresses") or 0))
            same_side_smart_money_5m = max(int(resonance_5m.get("buy_smart_money_addresses") or 0), int(resonance_5m.get("sell_smart_money_addresses") or 0))
            leader_follow = bool(resonance_5m.get("leader_follow_resonance"))

        if bucket == "buy":
            same_side_unique_15m = int(resonance_15m.get("buy_unique_addresses") or 0)
        elif bucket == "sell":
            same_side_unique_15m = int(resonance_15m.get("sell_unique_addresses") or 0)
        else:
            same_side_unique_15m = max(int(resonance_15m.get("buy_unique_addresses") or 0), int(resonance_15m.get("sell_unique_addresses") or 0))

        resonance_score = max(
            float(resonance_5m.get("resonance_score") or 0.0),
            min(1.0, float(resonance_15m.get("resonance_score") or 0.0) * 0.9),
        )
        return {
            "same_side_unique_addresses": same_side_unique_5m,
            "same_side_high_quality_addresses": same_side_high_quality_5m,
            "same_side_smart_money_addresses": same_side_smart_money_5m,
            "same_side_unique_addresses_15m": same_side_unique_15m,
            "leader_follow": leader_follow,
            "multi_address_resonance": same_side_unique_5m >= 2,
            "resonance_score": round(resonance_score, 3),
        }

    def _is_exchange_sensitive_role(self, strategy_role: str) -> bool:
        return str(strategy_role or "unknown").startswith("exchange_")

    def _direction_bucket(self, side: str | None) -> str:
        normalized = str(side or "")
        if normalized in {"买入", "流入"}:
            return "buy"
        if normalized in {"卖出", "流出"}:
            return "sell"
        return "other"

    def _abnormal_ratio(self, address_snapshot: dict, event: Event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 0.0
        return float(event.usd_value or 0.0) / avg_usd

    def _relative_address_size(self, address_snapshot: dict, event: Event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 2.0
        return float(event.usd_value or 0.0) / avg_usd

    def _token_context_score(self, base_token_score: float, gate_metrics: dict) -> float:
        volume_ratio = float(gate_metrics.get("token_volume_ratio") or 0.0)
        price_impact_ratio = float(gate_metrics.get("price_impact_ratio") or 0.0)
        cluster_boost = float(gate_metrics.get("cluster_boost") or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)

        boost = (
            min(12.0, volume_ratio * 1000.0)
            + min(8.0, price_impact_ratio * 1000.0)
            + (cluster_boost * 20.0)
            + (resonance_score * 10.0)
        )
        score = base_token_score + boost
        return max(0.0, min(100.0, score))

    def _to_event(self, parsed: dict, watch_context: dict | None, watch_meta: dict, pricing: dict) -> Event:
        kind = parsed.get("kind", "unknown")
        side = parsed.get("side") or parsed.get("direction")
        if kind == "swap":
            amount = float(parsed.get("token_amount") or 0.0)
        else:
            amount = float(parsed.get("value") or 0.0)

        token_contract = parsed.get("token_contract")
        token = str(token_contract).lower() if token_contract else None
        watch_address = str(parsed.get("watch_address") or "").lower()
        flow_source_label, flow_target_label = get_flow_endpoints(watch_context) if watch_context else ("", "")

        return Event(
            tx_hash=str(parsed.get("tx_hash") or ""),
            address=watch_address,
            token=token,
            amount=amount,
            side=side,
            usd_value=float(pricing["usd_value"] or 0.0),
            kind=kind,
            ts=int(parsed.get("timestamp") or int(time.time())),
            pricing_status=str(pricing.get("pricing_status") or "unknown"),
            pricing_source=str(pricing.get("pricing_source") or "none"),
            pricing_confidence=float(pricing.get("pricing_confidence") or 0.0),
            usd_value_available=bool(pricing.get("usd_value_available", False)),
            usd_value_estimated=bool(pricing.get("usd_value_estimated", False)),
            semantic_role=str(watch_meta.get("semantic_role") or "unknown"),
            strategy_role=str(watch_meta.get("strategy_role") or "unknown"),
            metadata={
                "token_symbol": parsed.get("token_symbol"),
                "quote_symbol": parsed.get("quote_symbol"),
                "quote_amount": parsed.get("quote_amount"),
                "quote_token_contract": parsed.get("quote_token_contract"),
                "flow_direction": watch_context.get("direction") if watch_context else "",
                "flow_label": watch_context.get("flow_label") if watch_context else "",
                "flow_source_label": flow_source_label,
                "flow_target_label": flow_target_label,
                "watch_address_label": watch_context.get("watch_address_label") if watch_context else "",
                "counterparty_label": watch_context.get("counterparty_label") if watch_context else "",
                "counterparty_role": parsed.get("counterparty_role", "unknown"),
                "counterparty_strategy_role": parsed.get("counterparty_strategy_role", "unknown"),
                "counterparty_semantic_role": parsed.get("counterparty_semantic_role", "unknown"),
                "wallet_function": parsed.get("wallet_function", watch_meta.get("wallet_function", "unknown")),
                "watch_wallet_function": parsed.get("watch_wallet_function", watch_meta.get("wallet_function", "unknown")),
                "counterparty_wallet_function": parsed.get("counterparty_wallet_function", "unknown"),
                "exchange_internality": parsed.get("exchange_internality", "no"),
                "exchange_same_entity_strength": parsed.get("exchange_same_entity_strength", "no"),
                "exchange_transfer_purpose": parsed.get("exchange_transfer_purpose", "exchange_unknown_flow"),
                "exchange_transfer_purpose_family": parsed.get("exchange_transfer_purpose_family", parsed.get("exchange_transfer_purpose", "exchange_unknown_flow")),
                "exchange_transfer_purpose_strength": parsed.get("exchange_transfer_purpose_strength", "no"),
                "exchange_inflow_context_strength": parsed.get("exchange_inflow_context_strength", "no"),
                "exchange_followup_ready": bool(parsed.get("exchange_followup_ready")),
                "exchange_external_counterparty_risk_class": str(parsed.get("exchange_external_counterparty_risk_class") or "none"),
                "exchange_distribution_risk_target_eligible": bool(parsed.get("exchange_distribution_risk_target_eligible")),
                "exchange_transfer_confidence": float(parsed.get("exchange_transfer_confidence") or 0.0),
                "exchange_entity_label": str(parsed.get("exchange_entity_label") or ""),
                "exchange_transfer_why": list(parsed.get("exchange_transfer_why") or []),
                "clmm_position_event": bool(parsed.get("clmm_position_event")),
                "clmm_partial_candidate": bool(parsed.get("clmm_partial_candidate")),
                "clmm_partial_support": bool(parsed.get("clmm_partial_support")),
                "clmm_partial_reason": str(parsed.get("clmm_partial_reason") or ""),
                "clmm_manager_protocol": str(parsed.get("clmm_manager_protocol") or ""),
                "clmm_manager_address": str(parsed.get("clmm_manager_address") or ""),
                "clmm_candidate_status": str(parsed.get("clmm_candidate_status") or ""),
                "clmm_context": dict(parsed.get("clmm_context") or {}),
                "inferred_context": parsed.get("inferred_context", {}),
                "is_liquidation_protocol_related": bool(parsed.get("is_liquidation_protocol_related")),
                "liquidation_protocols_touched": list(parsed.get("liquidation_protocols_touched") or []),
                "liquidation_roles_touched": list(parsed.get("liquidation_roles_touched") or []),
                "liquidation_related_addresses": list(parsed.get("liquidation_related_addresses") or []),
                "possible_keeper_executor": bool(parsed.get("possible_keeper_executor")),
                "possible_vault_or_auction": bool(parsed.get("possible_vault_or_auction")),
                "possible_lending_protocol": bool(parsed.get("possible_lending_protocol")),
                "ingest_ts": int(parsed.get("ingest_ts") or 0),
                "first_chain_seen_at": int(parsed.get("first_chain_seen_at") or parsed.get("ingest_ts") or 0),
                "parsed_at": int(parsed.get("parsed_at") or 0),
                "source_kind": parsed.get("source_kind", ""),
                "touched_watch_addresses": list(parsed.get("touched_watch_addresses") or []),
                "touched_lp_pools": list(parsed.get("touched_lp_pools") or []),
                "participant_addresses": list(parsed.get("participant_addresses") or []),
                "next_hop_addresses": list(parsed.get("next_hop_addresses") or []),
                "raw_log_count": int(parsed.get("raw_log_count") or 0),
                "monitor_type": parsed.get("monitor_type", "watch_address"),
                "tx_pool_hit_count": int(parsed.get("tx_pool_hit_count") or 0),
                "touched_lp_pool_count": int(parsed.get("touched_lp_pool_count") or 0),
                "pool_transfer_count_by_pool": dict(parsed.get("pool_transfer_count_by_pool") or {}),
                "pool_candidate_weight": float(parsed.get("pool_candidate_weight") or 0.0),
                "lp_scan_path": str(parsed.get("lp_scan_path") or ""),
                "lp_scan_paths_by_pool": dict(parsed.get("lp_scan_paths_by_pool") or {}),
                "lp_promoted_fastlane": bool(parsed.get("lp_promoted_fastlane")),
                "lp_promote_reason": str(parsed.get("lp_promote_reason") or ""),
                "replay_source": str(parsed.get("replay_source") or ""),
                "listener_rpc_mode": str(parsed.get("listener_rpc_mode") or ""),
                "listener_block_fetch_mode": str(parsed.get("listener_block_fetch_mode") or ""),
                "listener_block_fetch_reason": str(parsed.get("listener_block_fetch_reason") or ""),
                "listener_block_get_logs_request_count": int(
                    parsed.get("listener_block_get_logs_request_count") or 0
                ),
                "listener_block_topic_chunk_count": int(
                    parsed.get("listener_block_topic_chunk_count") or 0
                ),
                "listener_block_monitored_address_count": int(
                    parsed.get("listener_block_monitored_address_count") or 0
                ),
                "listener_block_lp_secondary_scan_used": bool(
                    parsed.get("listener_block_lp_secondary_scan_used")
                ),
                "listener_block_bloom_prefilter_used": bool(
                    parsed.get("listener_block_bloom_prefilter_used")
                ),
                "listener_block_bloom_skipped_get_logs_count": int(
                    parsed.get("listener_block_bloom_skipped_get_logs_count") or 0
                ),
                "listener_block_bloom_transfer_possible": bool(
                    parsed.get("listener_block_bloom_transfer_possible")
                    if "listener_block_bloom_transfer_possible" in parsed
                    else True
                ),
                "listener_block_bloom_address_possible_count": int(
                    parsed.get("listener_block_bloom_address_possible_count") or 0
                ),
                "listener_runtime_adjacent_core_count": int(
                    parsed.get("listener_runtime_adjacent_core_count") or 0
                ),
                "listener_runtime_adjacent_secondary_count": int(
                    parsed.get("listener_runtime_adjacent_secondary_count") or 0
                ),
                "listener_runtime_adjacent_secondary_scan_used": bool(
                    parsed.get("listener_runtime_adjacent_secondary_scan_used")
                ),
                "listener_runtime_adjacent_secondary_skipped_count": int(
                    parsed.get("listener_runtime_adjacent_secondary_skipped_count") or 0
                ),
                "listener_block_lp_primary_trend_scan_used": bool(
                    parsed.get("listener_block_lp_primary_trend_scan_used")
                ),
                "listener_block_lp_extended_scan_used": bool(
                    parsed.get("listener_block_lp_extended_scan_used")
                ),
                "listener_block_lp_primary_trend_pool_count": int(
                    parsed.get("listener_block_lp_primary_trend_pool_count") or 0
                ),
                "listener_block_lp_extended_pool_count": int(
                    parsed.get("listener_block_lp_extended_pool_count") or 0
                ),
                "listener_block_get_logs_primary_side_count": int(
                    parsed.get("listener_block_get_logs_primary_side_count") or 0
                ),
                "listener_block_get_logs_secondary_side_count": int(
                    parsed.get("listener_block_get_logs_secondary_side_count") or 0
                ),
                "listener_block_get_logs_secondary_side_skipped_count": int(
                    parsed.get("listener_block_get_logs_secondary_side_skipped_count") or 0
                ),
                "listener_block_get_logs_empty_response_count": int(
                    parsed.get("listener_block_get_logs_empty_response_count") or 0
                ),
                "low_cu_mode_enabled": bool(parsed.get("low_cu_mode_enabled")),
                "low_cu_mode_lp_secondary_only": bool(
                    parsed.get("low_cu_mode_lp_secondary_only")
                ),
                "low_cu_mode_poll_interval_sec": float(
                    parsed.get("low_cu_mode_poll_interval_sec") or 0.0
                ),
                "lp_adjacent_noise_skipped_in_listener": bool(parsed.get("lp_adjacent_noise_skipped_in_listener")),
                "lp_adjacent_noise_listener_reason": str(parsed.get("lp_adjacent_noise_listener_reason") or ""),
                "lp_adjacent_noise_listener_confidence": float(parsed.get("lp_adjacent_noise_listener_confidence") or 0.0),
                "lp_adjacent_noise_listener_source_signals": list(parsed.get("lp_adjacent_noise_listener_source_signals") or []),
                "lp_adjacent_noise_rule_version": str(parsed.get("lp_adjacent_noise_rule_version") or ""),
                "lp_adjacent_noise_decision_stage": str(parsed.get("lp_adjacent_noise_decision_stage") or ""),
                "lp_adjacent_noise_filtered": bool(parsed.get("lp_adjacent_noise_filtered")),
                "lp_adjacent_noise_reason": str(parsed.get("lp_adjacent_noise_reason") or ""),
                "lp_adjacent_noise_confidence": float(parsed.get("lp_adjacent_noise_confidence") or 0.0),
                "lp_adjacent_noise_source_signals": list(parsed.get("lp_adjacent_noise_source_signals") or []),
                "lp_adjacent_noise_context_used": list(parsed.get("lp_adjacent_noise_context_used") or []),
                "lp_adjacent_noise_runtime_context_present": bool(
                    parsed.get("lp_adjacent_noise_runtime_context_present")
                ),
                "lp_adjacent_noise_downstream_context_present": bool(
                    parsed.get("lp_adjacent_noise_downstream_context_present")
                ),
                "pricing": pricing,
                "raw": parsed,
            },
        )

    def _is_lp_event(
        self,
        event: Event | None = None,
        watch_meta: dict | None = None,
        parsed: dict | None = None,
    ) -> bool:
        if watch_meta and str(watch_meta.get("strategy_role") or "") == "lp_pool":
            return True
        if event and str(event.strategy_role or "") == "lp_pool":
            return True
        if parsed and str(parsed.get("monitor_type") or "") == "lp_pool":
            return True
        if event and str(event.intent_type or "") in LP_ALL_INTENTS:
            return True
        return False

    def _is_clmm_position_event(
        self,
        event: Event | None = None,
        parsed: dict | None = None,
    ) -> bool:
        if self._is_clmm_partial_support_event(event=event, parsed=parsed):
            return False
        if parsed and str(parsed.get("monitor_type") or "") == "clmm_position":
            return True
        if parsed and bool(parsed.get("clmm_position_event")):
            return True
        if event and bool(event.metadata.get("clmm_position_event")):
            return True
        if event and bool((event.metadata.get("raw") or {}).get("clmm_position_event")):
            return True
        if event and str(event.intent_type or "").startswith("clmm_"):
            return True
        return False

    def _is_clmm_partial_support_event(
        self,
        event: Event | None = None,
        parsed: dict | None = None,
    ) -> bool:
        if parsed and bool(parsed.get("clmm_partial_support")):
            return True
        if parsed and str(parsed.get("monitor_type") or "") == "clmm_partial_support":
            return True
        if event and bool(event.metadata.get("clmm_partial_support")):
            return True
        if event and bool((event.metadata.get("raw") or {}).get("clmm_partial_support")):
            return True
        return str(event.intent_type or "") == "clmm_partial_support_observation" if event else False

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))


def _usd_text(value: float) -> str:
    usd_value = float(value or 0.0)
    if usd_value >= 100_000:
        return f"${usd_value:,.0f}"
    return f"${usd_value:,.2f}"
