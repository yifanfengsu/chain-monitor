from __future__ import annotations

import atexit
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
import hashlib
import time
from typing import Any

from config import (
    OPPORTUNITY_CACHE_PATH,
    OPPORTUNITY_COOLDOWN_SEC,
    OPPORTUNITY_ENABLE,
    OPPORTUNITY_FLUSH_INTERVAL_SEC,
    OPPORTUNITY_HISTORY_LIMIT,
    OPPORTUNITY_MAX_60S_ADVERSE_RATE,
    OPPORTUNITY_MAX_PER_ASSET_PER_HOUR,
    OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE,
    OPPORTUNITY_MIN_CANDIDATE_SCORE,
    OPPORTUNITY_MIN_HISTORY_SAMPLES,
    OPPORTUNITY_NON_LP_EVIDENCE_ENABLE,
    OPPORTUNITY_NON_LP_EVIDENCE_WINDOW_SEC,
    OPPORTUNITY_NON_LP_OBSERVE_WEIGHT,
    OPPORTUNITY_NON_LP_SCORE_WEIGHT,
    OPPORTUNITY_NON_LP_STRONG_BLOCKER_ENABLE,
    OPPORTUNITY_NON_LP_TENTATIVE_WEIGHT,
    OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE,
    OPPORTUNITY_MIN_VERIFIED_SCORE,
    OPPORTUNITY_PERSIST_ENABLE,
    OPPORTUNITY_RECENT_OPPOSITE_SIGNAL_WINDOW_SEC,
    OPPORTUNITY_RECOVER_ON_START,
    OPPORTUNITY_REPEAT_CANDIDATE_SUPPRESS_SEC,
    OPPORTUNITY_REQUIRE_BROADER_CONFIRM,
    OPPORTUNITY_REQUIRE_LIVE_CONTEXT,
    OPPORTUNITY_REQUIRE_OUTCOME_HISTORY,
    OPPORTUNITY_SCHEMA_VERSION,
    TELEGRAM_SUPPRESS_REPEAT_STATE_SEC,
    TRADE_ACTION_MAX_LONG_BASIS_BPS,
    TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE,
    TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE,
    TRADE_ACTION_MIN_SHORT_BASIS_BPS,
)
from opportunity_calibration import build_calibration_quality_stats, calibrate_opportunity_score
from lp_product_helpers import asset_symbol_from_event, is_major_asset_symbol
from persistence_utils import read_json_file, resolve_persistence_path, write_json_file


ACTIVE_STATUSES = {"CANDIDATE", "VERIFIED", "BLOCKED"}
OPPORTUNITY_TERMINAL_STATUSES = {"EXPIRED", "INVALIDATED"}
HIGH_VALUE_STATUSES = ACTIVE_STATUSES | OPPORTUNITY_TERMINAL_STATUSES
LONG_KEYS = {"LONG_CHASE_ALLOWED", "DO_NOT_CHASE_LONG", "LONG_CANDIDATE", "TRADEABLE_LONG"}
SHORT_KEYS = {"SHORT_CHASE_ALLOWED", "DO_NOT_CHASE_SHORT", "SHORT_CANDIDATE", "TRADEABLE_SHORT"}
LEGACY_CHASE_ACTION_KEYS = {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
ASSET_STATE_OPPORTUNITY_KEYS = {"LONG_CANDIDATE", "SHORT_CANDIDATE", "TRADEABLE_LONG", "TRADEABLE_SHORT"}
ASSET_STATE_RISK_BLOCKER_KEYS = {"NO_TRADE_LOCK", "DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT", "DATA_GAP_NO_TRADE"}
LEGACY_CHASE_LABELS = {"可追多", "可追空", "可顺势追多", "可顺势追空"}
VERIFIED_OUTPUT_TERMS = tuple(sorted(LEGACY_CHASE_LABELS | {"多头机会", "空头机会"}))
CANDIDATE_OUTPUT_TERMS = ("多头候选", "空头候选")
BLOCKED_OUTPUT_TERMS = ("机会被阻止", "不追多", "不追空", "不交易")
_OUTCOME_WINDOWS = (30, 60, 300)
_FOLLOWTHROUGH_MIN_MOVE = 0.002
_STRONG_SIGNAL_MIN = 0.60
_PROFILE_VERSION = "v1"
_NON_LP_STRONG_CONFIDENCE = 0.72
_NON_LP_TENTATIVE_CONFIDENCE = 0.45
_NON_LP_SUPPORT_BASELINE = 0.50

_NON_LP_FAMILY_LABELS = {
    "smart_money": "smart money",
    "maker": "maker",
    "exchange": "exchange",
    "clmm": "CLMM",
    "liquidation": "liquidation",
}

_NON_LP_FAMILY_BASE_WEIGHTS = {
    "smart_money": 0.42,
    "maker": 0.38,
    "exchange": 0.34,
    "clmm": 0.30,
    "liquidation": 0.24,
}

_NON_LP_DIRECTIONAL_MAP = {
    "smart_money_entry_execution": {"family": "smart_money", "support": "LONG", "risk": "SHORT"},
    "smart_money_exit_execution": {"family": "smart_money", "support": "SHORT", "risk": "LONG"},
    "market_maker_inventory_expand": {"family": "maker", "support": "LONG", "risk": "SHORT"},
    "market_maker_inventory_distribute": {"family": "maker", "support": "SHORT", "risk": "LONG"},
    "market_maker_inventory_recenter": {"family": "maker", "support": "LONG", "risk": "SHORT"},
    "exchange_liquidity_preparation": {"family": "exchange", "support": "LONG", "risk": "SHORT"},
    "exchange_external_distribution_risk": {"family": "exchange", "support": "SHORT", "risk": "LONG"},
    "clmm_range_liquidity_add": {"family": "clmm", "support": "LONG", "risk": "SHORT"},
    "clmm_range_recenter": {"family": "clmm", "support": "LONG", "risk": "SHORT"},
    "clmm_range_liquidity_remove": {"family": "clmm", "support": "SHORT", "risk": "LONG"},
    "clmm_position_exit": {"family": "clmm", "support": "SHORT", "risk": "LONG"},
    "liquidation_sell_pressure_release": {"family": "liquidation", "support": "LONG", "risk": "SHORT"},
}

_NON_LP_CONTEXT_ONLY_KEYS = {
    "smart_money_preparation_only",
    "exchange_external_inflow_observation",
    "exchange_external_outflow_observation",
    "clmm_partial_support_observation",
    "clmm_jit_fee_extraction_likely",
    "market_maker_settlement_move",
}

OPPORTUNITY_LABELS = {
    ("NONE", "NONE"): "无机会",
    ("CANDIDATE", "LONG"): "多头机会候选",
    ("CANDIDATE", "SHORT"): "空头机会候选",
    ("VERIFIED", "LONG"): "多头机会",
    ("VERIFIED", "SHORT"): "空头机会",
    ("BLOCKED", "LONG"): "机会被阻止",
    ("BLOCKED", "SHORT"): "机会被阻止",
    ("BLOCKED", "NONE"): "机会被阻止",
    ("EXPIRED", "LONG"): "多头机会过期",
    ("EXPIRED", "SHORT"): "空头机会过期",
    ("INVALIDATED", "LONG"): "多头机会失效",
    ("INVALIDATED", "SHORT"): "空头机会失效",
}

BLOCKER_PRIORITY = [
    "no_trade_lock",
    "direction_conflict",
    "data_gap",
    "sweep_exhaustion_risk",
    "late_or_chase",
    "crowded_basis",
    "local_absorption",
    "strong_opposite_signal",
    "non_lp_evidence_conflict",
    "non_lp_opposite_smart_money",
    "non_lp_maker_against",
    "non_lp_exchange_flow_against",
    "non_lp_clmm_position_against",
    "non_lp_liquidation_against",
    "low_quality",
    "profile_adverse_too_high",
    "profile_completion_too_low",
    "profile_sample_count_insufficient",
    "outcome_history_insufficient",
    "profile_followthrough_too_low",
]

BLOCKER_LABELS = {
    "no_trade_lock": "不交易锁定",
    "direction_conflict": "方向冲突锁定",
    "data_gap": "数据缺口",
    "sweep_exhaustion_risk": "清扫后回吐/反抽风险",
    "late_or_chase": "确认偏晚 / 追单风险",
    "crowded_basis": "basis / Mark-Index 拥挤",
    "local_absorption": "局部压力被吸收/承接",
    "strong_opposite_signal": "近期反向强信号",
    "recent_opposite_strong_signal": "近期反向强信号",
    "non_lp_evidence_conflict": "非 LP 证据冲突",
    "non_lp_opposite_smart_money": "smart money 反向执行",
    "non_lp_maker_against": "maker inventory 反向",
    "non_lp_exchange_flow_against": "exchange flow 反向",
    "non_lp_clmm_position_against": "CLMM position 反向",
    "non_lp_liquidation_against": "liquidation 反向风险",
    "low_quality": "质量不足",
    "profile_adverse_too_high": "profile adverse 偏高",
    "profile_completion_too_low": "profile completion 偏低",
    "profile_sample_count_insufficient": "profile 样本不足",
    "profile_followthrough_too_low": "profile followthrough 未稳定",
    "outcome_history_insufficient": "后验历史不足",
    "history_adverse_too_high": "历史 adverse 偏高",
    "history_completion_too_low": "历史 completion 偏低",
    "history_samples_insufficient": "历史样本不足",
    "history_followthrough_not_ready": "历史 followthrough 未稳定",
}


def canonical_final_trading_output_label(
    status: str,
    side: str,
    primary_blocker: str = "",
) -> str:
    normalized_status = str(status or "").strip().upper()
    normalized_side = str(side or "").strip().upper()
    if normalized_status == "VERIFIED":
        return "多头机会" if normalized_side == "LONG" else "空头机会" if normalized_side == "SHORT" else "机会"
    if normalized_status == "CANDIDATE":
        return "多头候选" if normalized_side == "LONG" else "空头候选" if normalized_side == "SHORT" else "候选"
    if normalized_status == "BLOCKED":
        return _blocked_output_label(normalized_side, str(primary_blocker or ""))
    if normalized_status in OPPORTUNITY_TERMINAL_STATUSES:
        return "状态变化"
    return ""


def classify_trading_output_label(label: str | None) -> str:
    normalized = str(label or "").strip()
    if not normalized:
        return "empty"
    if normalized == "状态变化":
        return "state_change"
    if any(term in normalized for term in VERIFIED_OUTPUT_TERMS):
        return "verified_like"
    if any(term in normalized for term in CANDIDATE_OUTPUT_TERMS):
        return "candidate_like"
    if any(term in normalized for term in BLOCKED_OUTPUT_TERMS):
        return "blocked_like"
    return "informational"


def legacy_output_requires_opportunity_gate(
    *,
    trade_action_key: str | None = None,
    asset_market_state_key: str | None = None,
    labels: list[str] | tuple[str, ...] | None = None,
) -> bool:
    if str(trade_action_key or "").strip().upper() in LEGACY_CHASE_ACTION_KEYS:
        return True
    if str(asset_market_state_key or "").strip().upper() in ASSET_STATE_OPPORTUNITY_KEYS:
        return True
    for label in labels or ():
        if classify_trading_output_label(label) in {"verified_like", "candidate_like"}:
            return True
    return False


def validate_final_trading_output_gate(
    *,
    trade_opportunity_status: str | None,
    final_trading_output_source: str | None,
    final_trading_output_label: str | None,
    opportunity_gate_passed: bool | None,
    asset_market_state_key: str | None = None,
    delivered_to_trader: bool = False,
) -> tuple[bool, str]:
    status = str(trade_opportunity_status or "NONE").strip().upper()
    source = str(final_trading_output_source or "").strip().lower()
    label = str(final_trading_output_label or "").strip()
    asset_state_key = str(asset_market_state_key or "").strip().upper()
    label_kind = classify_trading_output_label(label)

    if delivered_to_trader and source == "trade_action_legacy":
        return False, "legacy_trade_action_not_deliverable"

    if label_kind == "verified_like":
        if status != "VERIFIED":
            return False, "verified_output_requires_verified"
        if source != "trade_opportunity":
            return False, "verified_output_requires_trade_opportunity"
        if not bool(opportunity_gate_passed):
            return False, "verified_output_requires_gate_pass"
        return True, ""

    if label_kind == "candidate_like":
        if status != "CANDIDATE":
            return False, "candidate_output_requires_candidate"
        if source != "trade_opportunity":
            return False, "candidate_output_requires_trade_opportunity"
        return True, ""

    if label_kind == "blocked_like":
        if status == "BLOCKED" and source == "trade_opportunity":
            return True, ""
        if source == "asset_market_state" and asset_state_key in ASSET_STATE_RISK_BLOCKER_KEYS:
            return True, ""
        return False, "blocked_output_requires_blocked_or_risk_state"

    if label_kind == "state_change":
        if source == "trade_opportunity" and status in OPPORTUNITY_TERMINAL_STATUSES:
            return True, ""
        if source == "asset_market_state":
            return True, ""
        return False, "state_change_requires_state_source"

    return True, ""


def _text(*values: Any) -> str:
    for value in values:
        if value in (None, "", [], {}, ()):
            continue
        return str(value)
    return ""


def _float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return float(default)


def _int(*values: Any, default: int = 0) -> int:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
    return int(default)


def _bool(*values: Any) -> bool:
    for value in values:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            continue
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return False


def _direction_from_values(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip().lower()
        if normalized in {"buy_pressure", "buy", "upward", "long"}:
            return "long"
        if normalized in {"sell_pressure", "sell", "downward", "short"}:
            return "short"
    return "neutral"


def _side_from_direction(direction: str) -> str:
    if direction == "long":
        return "LONG"
    if direction == "short":
        return "SHORT"
    return "NONE"


def _side_cn(side: str) -> str:
    if side == "LONG":
        return "多头"
    if side == "SHORT":
        return "空头"
    return "无方向"


def _label_for(status: str, side: str) -> str:
    return OPPORTUNITY_LABELS.get((status, side), OPPORTUNITY_LABELS.get((status, "NONE"), "无机会"))


def _blocked_output_label(side: str, blocker: str) -> str:
    if blocker in {"sweep_exhaustion_risk", "late_or_chase", "crowded_basis", "local_absorption"}:
        return "不追多" if side == "LONG" else "不追空" if side == "SHORT" else "不交易"
    return "不交易"


def _window_key(window_sec: int) -> str:
    return f"{int(window_sec)}s"


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def _clamp(value: Any, minimum: float = 0.0, maximum: float = 1.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(minimum, min(maximum, numeric))


def _status_bucket(score: float) -> str:
    if score >= 0.82:
        return "high"
    if score >= 0.70:
        return "medium"
    return "low"


def _market_alignment_positive(direction: str, *moves: Any) -> bool:
    values: list[float] = []
    for move in moves:
        if move in (None, ""):
            continue
        try:
            values.append(float(move))
        except (TypeError, ValueError):
            continue
    if not values:
        return False
    if direction == "long":
        return max(values) >= 0.0010 and min(values) > -0.0015
    if direction == "short":
        return min(values) <= -0.0010 and max(values) < 0.0015
    return False


def _has_crowding(direction: str, basis_bps: float | None, mark_index_spread_bps: float | None) -> bool:
    if direction == "long":
        return (
            basis_bps is not None and basis_bps > float(TRADE_ACTION_MAX_LONG_BASIS_BPS)
        ) or (
            mark_index_spread_bps is not None and mark_index_spread_bps > float(TRADE_ACTION_MAX_LONG_BASIS_BPS)
        )
    if direction == "short":
        return (
            basis_bps is not None and basis_bps < float(TRADE_ACTION_MIN_SHORT_BASIS_BPS)
        ) or (
            mark_index_spread_bps is not None and mark_index_spread_bps < float(TRADE_ACTION_MIN_SHORT_BASIS_BPS)
        )
    return False


def _score_component(weight: float, score: float, reason: str) -> dict[str, Any]:
    normalized = max(0.0, min(1.0, round(float(score), 4)))
    return {
        "weight": round(float(weight), 4),
        "score": normalized,
        "weighted_score": round(normalized * float(weight), 4),
        "reason": str(reason or ""),
    }


def _major_market_venue(venue: str) -> str:
    normalized = str(venue or "").strip().lower()
    if normalized.startswith("okx"):
        return "OKX"
    if normalized.startswith("kraken"):
        return "Kraken"
    if normalized.startswith("binance"):
        return "Binance"
    if normalized.startswith("bybit"):
        return "Bybit"
    return str(venue or "").strip().upper()


def _list_dedup(items: list[str], *, limit: int = 6) -> list[str]:
    result: list[str] = []
    for item in items:
        normalized = str(item or "").strip()
        if normalized and normalized not in result:
            result.append(normalized)
        if len(result) >= limit:
            break
    return result


def _result_label_from_outcome(status: str, followthrough: Any, adverse: Any) -> str:
    normalized = str(status or "")
    if normalized == "completed":
        if adverse is True:
            return "adverse"
        if followthrough is True:
            return "success"
        return "neutral"
    if normalized == "expired":
        return "expired"
    if normalized == "unavailable":
        return "unavailable"
    return "neutral"


@dataclass
class TradeOpportunityAssetState:
    asset_symbol: str
    active_opportunity_id: str = ""
    active_opportunity_key: str = ""
    active_opportunity_status: str = "NONE"
    active_opportunity_side: str = "NONE"
    active_created_at: int = 0
    last_sent_key: str = ""
    last_sent_status: str = ""
    last_sent_side: str = "NONE"
    last_sent_at: int = 0
    last_candidate_sent_at: int = 0
    opportunity_cooldown_until: int = 0
    opportunity_sent_count_by_hour: dict[str, int] = field(default_factory=dict)
    repeated_opportunity_suppressed: int = 0
    opportunity_budget_exhausted: int = 0
    restored_from_cache: bool = False


class TradeOpportunityManager:
    def __init__(
        self,
        *,
        state_manager=None,
        enabled: bool = OPPORTUNITY_ENABLE,
        persistence_enabled: bool = OPPORTUNITY_PERSIST_ENABLE,
        cache_path: str | None = OPPORTUNITY_CACHE_PATH,
        flush_interval_sec: float = OPPORTUNITY_FLUSH_INTERVAL_SEC,
        recover_on_start: bool = OPPORTUNITY_RECOVER_ON_START,
        schema_version: str = OPPORTUNITY_SCHEMA_VERSION,
        history_limit: int = OPPORTUNITY_HISTORY_LIMIT,
    ) -> None:
        self.state_manager = state_manager
        self.enabled = bool(enabled)
        self.persistence_enabled = bool(persistence_enabled)
        self.schema_version = str(schema_version or OPPORTUNITY_SCHEMA_VERSION)
        self.flush_interval_sec = max(float(flush_interval_sec or 0.0), 0.2)
        self.history_limit = max(int(history_limit or 0), 200)
        self.cache_path = resolve_persistence_path(cache_path, namespace="trade-opportunity") if self.persistence_enabled else None
        self._opportunities: list[dict[str, Any]] = []
        self._opportunity_index: dict[str, dict[str, Any]] = {}
        self._asset_states: dict[str, TradeOpportunityAssetState] = {}
        self._profile_stats: dict[str, dict[str, Any]] = {}
        self._recent_direction_signals: dict[str, deque] = defaultdict(lambda: deque(maxlen=12))
        self._dirty = False
        self._last_flush_monotonic = 0.0
        self._last_load_status = "not_loaded"
        if self.enabled and self.persistence_enabled and bool(recover_on_start):
            self.load_from_disk()
        if self.enabled and self.persistence_enabled:
            atexit.register(self.flush, True)

    def apply_lp_signal(self, event, signal) -> dict[str, Any]:
        if not self.enabled:
            return {}
        context = getattr(signal, "context", {}) or {}
        metadata = getattr(signal, "metadata", {}) or {}
        event_metadata = getattr(event, "metadata", {}) or {}
        if str(getattr(event, "strategy_role", "") or "") != "lp_pool" and not _bool(
            context.get("lp_event"),
            metadata.get("lp_event"),
            event_metadata.get("lp_event"),
        ):
            return {}

        self._refresh_recent_outcomes()
        summary = self._summary(event, signal)
        asset_symbol = str(summary.get("asset_symbol") or "").strip().upper()
        if not asset_symbol:
            return {}
        summary.update(self._build_profile(summary=summary, side=str(summary.get("trade_opportunity_side") or "NONE")))
        summary.update(
            self.build_non_lp_evidence_context(
                asset_symbol,
                _int(summary.get("event_ts"), default=int(time.time())),
                window_sec=int(OPPORTUNITY_NON_LP_EVIDENCE_WINDOW_SEC),
                side=str(summary.get("trade_opportunity_side") or "NONE"),
                pair_label=str(summary.get("pair_label") or ""),
                summary=summary,
            )
        )
        now_ts = _int(summary.get("event_ts"), default=int(time.time()))
        asset_state = self._asset_states.get(asset_symbol) or TradeOpportunityAssetState(asset_symbol=asset_symbol)
        previous = self._opportunity_index.get(str(asset_state.active_opportunity_id or "")) if asset_state.active_opportunity_id else None
        previous_status = str(previous.get("trade_opportunity_status") or "") if previous else ""
        self._prune_hour_buckets(asset_state, now_ts=now_ts)

        history = self._history_snapshot(summary=summary)
        score_payload = self._score(summary=summary, history=history)
        evaluation = self._evaluate(summary=summary, history=history, score_payload=score_payload)

        if previous and str(previous.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED"}:
            current_status = str(evaluation.get("trade_opportunity_status") or "")
            current_key = str(evaluation.get("trade_opportunity_key") or "")
            if current_status not in {"CANDIDATE", "VERIFIED"} or current_key != str(previous.get("trade_opportunity_key") or ""):
                self._invalidate_active(previous, now_ts=now_ts, reason=str(evaluation.get("trade_opportunity_reason") or "alignment_lost"))
                if current_status == "NONE" and previous_status == "VERIFIED":
                    evaluation = self._build_state_change(previous, now_ts=now_ts)

        record = self._register_opportunity(summary=summary, history=history, evaluation=evaluation, now_ts=now_ts)
        decision = self._telegram_decision(record=record, asset_state=asset_state, previous=previous, now_ts=now_ts)
        record.update(decision)
        record.update(self._final_trading_output(summary=summary, record=record))
        if record["telegram_should_send"]:
            asset_state.last_sent_key = str(record.get("trade_opportunity_key") or "")
            asset_state.last_sent_status = str(record.get("trade_opportunity_status") or "")
            asset_state.last_sent_side = str(record.get("trade_opportunity_side") or "NONE")
            asset_state.last_sent_at = now_ts
            if str(record.get("trade_opportunity_status") or "") == "CANDIDATE":
                asset_state.last_candidate_sent_at = now_ts
            if str(record.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED"}:
                hour_key = self._hour_bucket(now_ts)
                asset_state.opportunity_sent_count_by_hour[hour_key] = int(asset_state.opportunity_sent_count_by_hour.get(hour_key) or 0) + 1
        else:
            if str(record.get("telegram_suppression_reason") or "") in {
                "same_opportunity_repeat",
                "same_blocker_repeat",
                "candidate_refresh_within_hold_window",
            }:
                asset_state.repeated_opportunity_suppressed = int(asset_state.repeated_opportunity_suppressed or 0) + 1
            if str(record.get("telegram_suppression_reason") or "") == "opportunity_budget_exhausted":
                asset_state.opportunity_budget_exhausted = int(asset_state.opportunity_budget_exhausted or 0) + 1

        if str(record.get("trade_opportunity_status") or "") in ACTIVE_STATUSES:
            asset_state.active_opportunity_id = str(record.get("trade_opportunity_id") or "")
            asset_state.active_opportunity_key = str(record.get("trade_opportunity_key") or "")
            asset_state.active_opportunity_status = str(record.get("trade_opportunity_status") or "NONE")
            asset_state.active_opportunity_side = str(record.get("trade_opportunity_side") or "NONE")
            asset_state.active_created_at = now_ts
        elif str(record.get("trade_opportunity_status") or "") in OPPORTUNITY_TERMINAL_STATUSES or (
            str(record.get("trade_opportunity_status") or "") == "NONE"
            and previous_status in {"CANDIDATE", "VERIFIED"}
        ):
            asset_state.active_opportunity_id = ""
            asset_state.active_opportunity_key = ""
            asset_state.active_opportunity_status = "NONE"
            asset_state.active_opportunity_side = "NONE"
            asset_state.active_created_at = 0

        asset_state.restored_from_cache = False
        self._asset_states[asset_symbol] = asset_state
        self._append_recent_direction_signal(summary=summary)
        self._trim_history()
        self._mark_dirty()
        self.flush()

        payload = dict(record)
        payload["trade_opportunity_sent_count_by_asset_hour"] = int(self._sent_count_this_hour(asset_state, now_ts))
        payload["trade_opportunity_cooldown_until"] = int(asset_state.opportunity_cooldown_until or 0)
        payload["last_opportunity_key"] = str(asset_state.last_sent_key or "")
        payload["repeated_opportunity_suppressed"] = int(asset_state.repeated_opportunity_suppressed or 0)
        payload["opportunity_budget_exhausted"] = int(asset_state.opportunity_budget_exhausted or 0)
        event_metadata.update(payload)
        metadata.update(payload)
        context.update(payload)
        self._mirror_sqlite_opportunity(payload)
        return payload

    def mark_notification(
        self,
        opportunity_id: str,
        *,
        notifier_sent_at: int | None = None,
        delivered: bool = False,
    ) -> dict[str, Any]:
        record = self._opportunity_index.get(str(opportunity_id or ""))
        if record is None:
            return {}
        record["trade_opportunity_notifier_sent_at"] = int(notifier_sent_at or time.time())
        record["trade_opportunity_delivered_notification"] = bool(delivered)
        self._mark_dirty()
        self.flush()
        self._mirror_sqlite_opportunity(record)
        return dict(record)

    def flush(self, force: bool = False) -> None:
        if not self.persistence_enabled or self.cache_path is None or not self._dirty:
            return
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_flush_monotonic) < self.flush_interval_sec:
            return
        payload = {
            "schema_version": self.schema_version,
            "generated_at": int(time.time()),
            "opportunities": list(self._opportunities[-self.history_limit :]),
            "opportunity_profile_stats": [
                item
                for key in sorted(self._profile_stats)
                if (item := self._profile_stat_payload(key))
            ],
            "rolling_stats": self._rolling_stats(),
            "per_asset_budget": {
                asset: self._serialize_asset_state(record)
                for asset, record in sorted(self._asset_states.items())
            },
            "candidate_history": self._status_history("CANDIDATE"),
            "verified_history": self._status_history("VERIFIED"),
            "blocker_history": self._status_history("BLOCKED"),
        }
        write_json_file(self.cache_path, payload)
        self._dirty = False
        self._last_flush_monotonic = now_monotonic

    def load_from_disk(self) -> int:
        payload = read_json_file(self.cache_path)
        if not payload:
            self._last_load_status = "empty_or_missing"
            return 0
        if not isinstance(payload, dict):
            self._last_load_status = "malformed_payload"
            return 0
        if str(payload.get("schema_version") or "") != self.schema_version:
            self._last_load_status = f"schema_mismatch:{payload.get('schema_version') or 'unknown'}"
            return 0
        raw_opportunities = payload.get("opportunities") or []
        raw_profile_stats = payload.get("opportunity_profile_stats") or []
        raw_budget = payload.get("per_asset_budget") or {}
        self._opportunities = []
        self._opportunity_index = {}
        self._profile_stats = {}
        for item in raw_opportunities:
            if not isinstance(item, dict):
                continue
            record = dict(item)
            opportunity_id = str(record.get("trade_opportunity_id") or "")
            if not opportunity_id:
                continue
            self._opportunities.append(record)
            self._opportunity_index[opportunity_id] = record
        for item in raw_profile_stats:
            if not isinstance(item, dict):
                continue
            stats_json = item.get("stats_json") if isinstance(item.get("stats_json"), dict) else {}
            profile_key = str(item.get("scope_key") or stats_json.get("profile_key") or "").strip()
            if not profile_key:
                continue
            merged = dict(stats_json or {})
            merged.setdefault("profile_key", profile_key)
            merged.setdefault("profile_version", _PROFILE_VERSION)
            merged.setdefault("asset", str(item.get("asset") or ""))
            merged.setdefault("pair_family", str(item.get("pair") or ""))
            self._profile_stats[profile_key] = merged
        self._asset_states = {}
        if isinstance(raw_budget, dict):
            for asset, item in raw_budget.items():
                record = self._deserialize_asset_state(asset_symbol=asset, payload=item)
                if record is not None:
                    self._asset_states[asset] = record
        self._trim_history()
        self._last_load_status = "loaded"
        return len(self._opportunities)

    def _serialize_asset_state(self, record: TradeOpportunityAssetState) -> dict[str, Any]:
        payload = asdict(record)
        payload["restored_from_cache"] = False
        return payload

    def _deserialize_asset_state(self, *, asset_symbol: str, payload: dict[str, Any]) -> TradeOpportunityAssetState | None:
        if not isinstance(payload, dict):
            return None
        asset = str(asset_symbol or payload.get("asset_symbol") or "").strip().upper()
        if not asset:
            return None
        record = TradeOpportunityAssetState(asset_symbol=asset)
        for field_name in record.__dataclass_fields__:
            if field_name == "asset_symbol":
                continue
            if field_name not in payload:
                continue
            setattr(record, field_name, payload.get(field_name))
        record.asset_symbol = asset
        record.restored_from_cache = True
        return record

    def _summary(self, event, signal) -> dict[str, Any]:
        context = getattr(signal, "context", {}) or {}
        metadata = getattr(signal, "metadata", {}) or {}
        event_metadata = getattr(event, "metadata", {}) or {}
        trade_action_debug = context.get("trade_action_debug") or metadata.get("trade_action_debug") or event_metadata.get("trade_action_debug") or {}
        outcome_tracking = context.get("lp_outcome_record") or metadata.get("lp_outcome_record") or event_metadata.get("lp_outcome_record") or {}
        outcome_record_id = _text(
            outcome_tracking.get("record_id"),
            context.get("outcome_tracking_key"),
            metadata.get("outcome_tracking_key"),
            event_metadata.get("outcome_tracking_key"),
        )
        direction = _text(
            context.get("trade_action_direction"),
            metadata.get("trade_action_direction"),
        ) or _direction_from_values(
            context.get("asset_case_direction"),
            metadata.get("asset_case_direction"),
            event_metadata.get("asset_case_direction"),
            context.get("direction_bucket"),
            metadata.get("direction_bucket"),
            event_metadata.get("direction_bucket"),
        )
        side = _side_from_direction(direction)
        basis_bps = None
        if _text(context.get("basis_bps"), metadata.get("basis_bps"), event_metadata.get("basis_bps")):
            basis_bps = _float(context.get("basis_bps"), metadata.get("basis_bps"), event_metadata.get("basis_bps"))
        mark_index_spread_bps = None
        if _text(
            context.get("mark_index_spread_bps"),
            metadata.get("mark_index_spread_bps"),
            event_metadata.get("mark_index_spread_bps"),
        ):
            mark_index_spread_bps = _float(
                context.get("mark_index_spread_bps"),
                metadata.get("mark_index_spread_bps"),
                event_metadata.get("mark_index_spread_bps"),
            )
        return {
            "event_ts": _int(getattr(event, "ts", 0), default=int(time.time())),
            "signal_id": _text(getattr(signal, "signal_id", "")),
            "event_id": _text(getattr(event, "event_id", "")),
            "asset_symbol": _text(
                context.get("asset_symbol"),
                context.get("asset_case_label"),
                metadata.get("asset_symbol"),
                metadata.get("asset_case_label"),
                event_metadata.get("asset_symbol"),
                event_metadata.get("asset_case_label"),
                event_metadata.get("token_symbol"),
                asset_symbol_from_event(event),
                getattr(event, "token", ""),
            ).upper(),
            "pair_label": _text(context.get("pair_label"), metadata.get("pair_label"), event_metadata.get("pair_label")),
            "direction": direction,
            "trade_opportunity_side": side,
            "trade_action_key": _text(context.get("trade_action_key"), metadata.get("trade_action_key"), event_metadata.get("trade_action_key")),
            "trade_action_label": _text(context.get("trade_action_label"), metadata.get("trade_action_label"), event_metadata.get("trade_action_label")),
            "trade_action_reason": _text(context.get("trade_action_reason"), metadata.get("trade_action_reason"), event_metadata.get("trade_action_reason")),
            "trade_action_required_confirmation": _text(
                context.get("trade_action_required_confirmation"),
                metadata.get("trade_action_required_confirmation"),
                event_metadata.get("trade_action_required_confirmation"),
            ),
            "trade_action_invalidated_by": _text(
                context.get("trade_action_invalidated_by"),
                metadata.get("trade_action_invalidated_by"),
                event_metadata.get("trade_action_invalidated_by"),
            ),
            "trade_action_confidence": _float(context.get("trade_action_confidence"), metadata.get("trade_action_confidence"), event_metadata.get("trade_action_confidence")),
            "strength_score": _float(trade_action_debug.get("strength_score"), default=0.0),
            "asset_market_state_key": _text(context.get("asset_market_state_key"), metadata.get("asset_market_state_key"), event_metadata.get("asset_market_state_key")),
            "asset_market_state_label": _text(context.get("asset_market_state_label"), metadata.get("asset_market_state_label"), event_metadata.get("asset_market_state_label")),
            "asset_market_state_reason": _text(context.get("asset_market_state_reason"), metadata.get("asset_market_state_reason"), event_metadata.get("asset_market_state_reason")),
            "asset_market_state_required_confirmation": _text(
                context.get("asset_market_state_required_confirmation"),
                metadata.get("asset_market_state_required_confirmation"),
                event_metadata.get("asset_market_state_required_confirmation"),
            ),
            "asset_market_state_invalidated_by": _text(
                context.get("asset_market_state_invalidated_by"),
                metadata.get("asset_market_state_invalidated_by"),
                event_metadata.get("asset_market_state_invalidated_by"),
            ),
            "market_context_source": _text(context.get("market_context_source"), metadata.get("market_context_source"), event_metadata.get("market_context_source"), "unavailable"),
            "market_context_venue": _text(context.get("market_context_venue"), metadata.get("market_context_venue"), event_metadata.get("market_context_venue")),
            "market_context_failure_reason": _text(
                context.get("market_context_failure_reason"),
                metadata.get("market_context_failure_reason"),
                event_metadata.get("market_context_failure_reason"),
            ),
            "alert_relative_timing": _text(context.get("alert_relative_timing"), metadata.get("alert_relative_timing"), event_metadata.get("alert_relative_timing")),
            "market_move_before_alert_30s": context.get("market_move_before_alert_30s", metadata.get("market_move_before_alert_30s", event_metadata.get("market_move_before_alert_30s"))),
            "market_move_before_alert_60s": context.get("market_move_before_alert_60s", metadata.get("market_move_before_alert_60s", event_metadata.get("market_move_before_alert_60s"))),
            "market_move_after_alert_60s": context.get("market_move_after_alert_60s", metadata.get("market_move_after_alert_60s", event_metadata.get("market_move_after_alert_60s"))),
            "basis_bps": basis_bps,
            "mark_index_spread_bps": mark_index_spread_bps,
            "last_mark_spread_bps": context.get("last_mark_spread_bps", metadata.get("last_mark_spread_bps", event_metadata.get("last_mark_spread_bps"))),
            "perp_last_price": context.get("perp_last_price", metadata.get("perp_last_price", event_metadata.get("perp_last_price"))),
            "perp_mark_price": context.get("perp_mark_price", metadata.get("perp_mark_price", event_metadata.get("perp_mark_price"))),
            "perp_index_price": context.get("perp_index_price", metadata.get("perp_index_price", event_metadata.get("perp_index_price"))),
            "spot_reference_price": context.get("spot_reference_price", metadata.get("spot_reference_price", event_metadata.get("spot_reference_price"))),
            "lp_alert_stage": _text(context.get("lp_alert_stage"), metadata.get("lp_alert_stage"), event_metadata.get("lp_alert_stage")),
            "lp_sweep_phase": _text(context.get("lp_sweep_phase"), metadata.get("lp_sweep_phase"), event_metadata.get("lp_sweep_phase")),
            "lp_confirm_scope": _text(context.get("lp_confirm_scope"), metadata.get("lp_confirm_scope"), event_metadata.get("lp_confirm_scope")),
            "lp_confirm_quality": _text(context.get("lp_confirm_quality"), metadata.get("lp_confirm_quality"), event_metadata.get("lp_confirm_quality")),
            "lp_absorption_context": _text(context.get("lp_absorption_context"), metadata.get("lp_absorption_context"), event_metadata.get("lp_absorption_context")),
            "lp_broader_alignment": _text(context.get("lp_broader_alignment"), metadata.get("lp_broader_alignment"), event_metadata.get("lp_broader_alignment")),
            "lp_sweep_continuation_score": _float(context.get("lp_sweep_continuation_score"), metadata.get("lp_sweep_continuation_score"), event_metadata.get("lp_sweep_continuation_score")),
            "lp_sweep_followthrough_score": _float(context.get("lp_sweep_followthrough_score"), metadata.get("lp_sweep_followthrough_score"), event_metadata.get("lp_sweep_followthrough_score")),
            "lp_chase_risk_score": _float(context.get("lp_chase_risk_score"), metadata.get("lp_chase_risk_score"), event_metadata.get("lp_chase_risk_score")),
            "pool_quality_score": _float(context.get("pool_quality_score"), metadata.get("pool_quality_score"), event_metadata.get("pool_quality_score")),
            "pair_quality_score": _float(context.get("pair_quality_score"), metadata.get("pair_quality_score"), event_metadata.get("pair_quality_score")),
            "asset_case_quality_score": _float(context.get("asset_case_quality_score"), metadata.get("asset_case_quality_score"), event_metadata.get("asset_case_quality_score")),
            "quality_score_brief": _text(context.get("quality_score_brief"), metadata.get("quality_score_brief"), event_metadata.get("quality_score_brief")),
            "asset_case_supporting_pair_count": _int(
                context.get("asset_case_supporting_pair_count"),
                metadata.get("asset_case_supporting_pair_count"),
                event_metadata.get("asset_case_supporting_pair_count"),
            ),
            "asset_case_multi_pool": _bool(context.get("asset_case_multi_pool"), metadata.get("asset_case_multi_pool"), event_metadata.get("asset_case_multi_pool")),
            "lp_multi_pool_resonance": _int(
                context.get("lp_multi_pool_resonance"),
                metadata.get("lp_multi_pool_resonance"),
                event_metadata.get("lp_multi_pool_resonance"),
            ),
            "lp_same_pool_continuity": _int(
                context.get("lp_same_pool_continuity"),
                metadata.get("lp_same_pool_continuity"),
                event_metadata.get("lp_same_pool_continuity"),
            ),
            "prealert_lifecycle_state": _text(context.get("prealert_lifecycle_state"), metadata.get("prealert_lifecycle_state"), event_metadata.get("prealert_lifecycle_state")),
            "prealert_to_confirm_sec": _int(context.get("prealert_to_confirm_sec"), metadata.get("prealert_to_confirm_sec"), event_metadata.get("prealert_to_confirm_sec")),
            "no_trade_lock_active": _bool(context.get("no_trade_lock_active"), metadata.get("no_trade_lock_active"), event_metadata.get("no_trade_lock_active")),
            "no_trade_lock_reason": _text(context.get("no_trade_lock_reason"), metadata.get("no_trade_lock_reason"), event_metadata.get("no_trade_lock_reason")),
            "lp_conflict_context": _text(context.get("lp_conflict_context"), metadata.get("lp_conflict_context"), event_metadata.get("lp_conflict_context")),
            "lp_conflict_resolution": _text(context.get("lp_conflict_resolution"), metadata.get("lp_conflict_resolution"), event_metadata.get("lp_conflict_resolution")),
            "asset_state_telegram_should_send": _bool(
                context.get("telegram_should_send"),
                metadata.get("telegram_should_send"),
                event_metadata.get("telegram_should_send"),
            ),
            "asset_state_telegram_suppression_reason": _text(
                context.get("telegram_suppression_reason"),
                metadata.get("telegram_suppression_reason"),
                event_metadata.get("telegram_suppression_reason"),
            ),
            "asset_state_telegram_update_kind": _text(
                context.get("telegram_update_kind"),
                metadata.get("telegram_update_kind"),
                event_metadata.get("telegram_update_kind"),
            ),
            "asset_state_telegram_state_change_reason": _text(
                context.get("telegram_state_change_reason"),
                metadata.get("telegram_state_change_reason"),
                event_metadata.get("telegram_state_change_reason"),
            ),
            "headline_label": _text(context.get("headline_label"), metadata.get("headline_label"), event_metadata.get("headline_label")),
            "market_state_label": _text(context.get("market_state_label"), metadata.get("market_state_label"), event_metadata.get("market_state_label")),
            "message_template": _text(context.get("message_template"), metadata.get("message_template"), event_metadata.get("message_template")),
            "user_tier": _text(context.get("user_tier"), metadata.get("user_tier"), event_metadata.get("user_tier")),
            "operational_intent_key": _text(
                context.get("operational_intent_key"),
                metadata.get("operational_intent_key"),
                event_metadata.get("operational_intent_key"),
            ),
            "operational_intent_confidence": _float(
                context.get("operational_intent_confidence"),
                metadata.get("operational_intent_confidence"),
                event_metadata.get("operational_intent_confidence"),
                getattr(event, "intent_confidence", None),
            ),
            "liquidation_stage": _text(
                context.get("liquidation_stage"),
                metadata.get("liquidation_stage"),
                event_metadata.get("liquidation_stage"),
                "none",
            ),
            "liquidation_side": _text(
                context.get("liquidation_side"),
                metadata.get("liquidation_side"),
                event_metadata.get("liquidation_side"),
            ),
            "outcome_tracking_key": outcome_record_id,
            "outcome_price_source": _text(
                outcome_tracking.get("outcome_price_source"),
                context.get("outcome_price_source"),
                metadata.get("outcome_price_source"),
                event_metadata.get("outcome_price_source"),
            ),
            "major_asset": bool(
                context.get("major_asset")
                if context.get("major_asset") is not None
                else metadata.get("major_asset")
                if metadata.get("major_asset") is not None
                else is_major_asset_symbol(
                    _text(
                        context.get("asset_symbol"),
                        metadata.get("asset_symbol"),
                        event_metadata.get("asset_symbol"),
                        event_metadata.get("token_symbol"),
                        asset_symbol_from_event(event),
                    )
                )
            ),
        }

    def _pair_family(self, pair_label: str) -> str:
        raw = str(pair_label or "").strip().upper()
        if "/" in raw:
            _, quote = raw.split("/", 1)
            return quote or "UNKNOWN"
        return raw or "UNKNOWN"

    def _basis_bucket(self, *, direction: str, basis_bps: Any, mark_index_spread_bps: Any) -> str:
        if _has_crowding(direction, _float(basis_bps, default=0.0), _float(mark_index_spread_bps, default=0.0)):
            return "basis_crowded"
        return "basis_normal"

    def _quality_bucket(self, summary: dict[str, Any]) -> str:
        quality_floor = min(
            _float(summary.get("pool_quality_score")),
            _float(summary.get("pair_quality_score")),
            _float(summary.get("asset_case_quality_score")),
        )
        if quality_floor >= 0.82:
            return "quality_high"
        if quality_floor >= max(float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE), 0.60):
            return "quality_medium"
        return "quality_low"

    def _absorption_bucket(self, absorption: str) -> str:
        normalized = str(absorption or "").strip()
        if normalized.startswith("local_") or normalized == "pool_only_unconfirmed_pressure":
            return "local_absorption"
        if normalized.startswith("broader_"):
            return "broader_absorption"
        return "no_absorption"

    def _stage_bucket(self, summary: dict[str, Any]) -> str:
        sweep_phase = str(summary.get("lp_sweep_phase") or "")
        alert_stage = str(summary.get("lp_alert_stage") or "")
        confirm_scope = str(summary.get("lp_confirm_scope") or "")
        if sweep_phase == "sweep_confirmed":
            return "sweep_confirmed"
        if sweep_phase == "sweep_exhaustion_risk" or alert_stage == "exhaustion_risk":
            return "exhaustion_risk"
        if confirm_scope == "broader_confirm":
            return "broader_confirm"
        if alert_stage == "confirm":
            return "confirm"
        if alert_stage == "prealert":
            return "candidate"
        return alert_stage or "candidate"

    def _strategy_bucket(self, summary: dict[str, Any], stage_bucket: str) -> str:
        if stage_bucket in {"sweep_confirmed", "broader_confirm", "confirm"}:
            return "lp_continuation"
        if str(summary.get("lp_alert_stage") or "") == "exhaustion_risk":
            return "lp_risk_blocker"
        return "lp_candidate"

    def _build_profile(self, *, summary: dict[str, Any], side: str) -> dict[str, Any]:
        asset = str(summary.get("asset_symbol") or "").strip().upper() or "UNKNOWN"
        confirm_scope = str(summary.get("lp_confirm_scope") or "unknown").strip() or "unknown"
        timing = str(summary.get("alert_relative_timing") or "unknown").strip() or "unknown"
        absorption_bucket = self._absorption_bucket(str(summary.get("lp_absorption_context") or ""))
        stage_bucket = self._stage_bucket(summary)
        basis_bucket = self._basis_bucket(
            direction=str(summary.get("direction") or ""),
            basis_bps=summary.get("basis_bps"),
            mark_index_spread_bps=summary.get("mark_index_spread_bps"),
        )
        quality_bucket = self._quality_bucket(summary)
        pair_family = self._pair_family(str(summary.get("pair_label") or ""))
        strategy = self._strategy_bucket(summary, stage_bucket)
        major_bucket = "major" if bool(summary.get("major_asset")) else "minor"
        features = {
            "confirm_scope": confirm_scope,
            "stage_bucket": stage_bucket,
            "market_timing": timing,
            "sweep_phase": str(summary.get("lp_sweep_phase") or ""),
            "absorption_context": str(summary.get("lp_absorption_context") or ""),
            "absorption_bucket": absorption_bucket,
            "no_trade_lock_active": bool(summary.get("no_trade_lock_active")),
            "major_asset": bool(summary.get("major_asset")),
            "basis_bucket": basis_bucket,
            "quality_bucket": quality_bucket,
            "pair_family": pair_family,
            "market_context_source": str(summary.get("market_context_source") or ""),
        }
        profile_key = "|".join(
            [
                asset,
                side or "NONE",
                confirm_scope,
                stage_bucket,
                timing,
                absorption_bucket,
                major_bucket,
                basis_bucket,
                quality_bucket,
            ]
        )
        return {
            "opportunity_profile_key": profile_key,
            "opportunity_profile_version": _PROFILE_VERSION,
            "opportunity_profile_side": side or "NONE",
            "opportunity_profile_asset": asset,
            "opportunity_profile_pair_family": pair_family,
            "opportunity_profile_strategy": strategy,
            "opportunity_profile_features_json": features,
        }

    def _signal_lookup(self, row: dict[str, Any], *keys: str) -> Any:
        signal = row.get("signal") if isinstance(row.get("signal"), dict) else {}
        event = row.get("event") if isinstance(row.get("event"), dict) else {}
        containers = [
            row,
            signal.get("context") if isinstance(signal.get("context"), dict) else {},
            signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {},
            signal,
            event.get("metadata") if isinstance(event.get("metadata"), dict) else {},
            event,
        ]
        for key in keys:
            for container in containers:
                if not isinstance(container, dict):
                    continue
                value = container.get(key)
                if value not in (None, "", [], {}, ()):
                    return value
        return None

    def _recent_non_lp_signal_rows(self, *, asset_symbol: str, ts: int, window_sec: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        since_ts = int(ts) - int(max(window_sec, 0))
        if self.state_manager is not None and hasattr(self.state_manager, "get_recent_signal_records"):
            try:
                rows = list(
                    self.state_manager.get_recent_signal_records(
                        asset_symbol=asset_symbol,
                        since_ts=since_ts,
                        until_ts=int(ts),
                        limit=120,
                    )
                    or []
                )
            except Exception:
                rows = []
        if rows:
            return rows
        try:
            import sqlite_store

            return list(
                sqlite_store.load_recent_non_lp_signal_rows(
                    asset_symbol=asset_symbol,
                    start_ts=since_ts,
                    end_ts=int(ts),
                    limit=120,
                )
                or []
            )
        except Exception:
            return []

    def _non_lp_family(self, key: str) -> str:
        normalized = str(key or "").strip()
        if normalized in _NON_LP_DIRECTIONAL_MAP:
            return str(_NON_LP_DIRECTIONAL_MAP[normalized]["family"])
        if normalized.startswith("smart_money_"):
            return "smart_money"
        if normalized.startswith("market_maker_"):
            return "maker"
        if normalized.startswith("exchange_"):
            return "exchange"
        if normalized.startswith("clmm_"):
            return "clmm"
        if normalized.startswith("liquidation_"):
            return "liquidation"
        return ""

    def _current_non_lp_row(self, summary: dict[str, Any]) -> dict[str, Any] | None:
        key = str(summary.get("operational_intent_key") or "")
        liquidation_stage = str(summary.get("liquidation_stage") or "none")
        if not key and liquidation_stage == "execution":
            key = "liquidation_sell_pressure_release"
        elif not key and liquidation_stage == "risk":
            key = "liquidation_risk_building"
        if not key:
            return None
        return {
            "signal_id": str(summary.get("signal_id") or ""),
            "asset_symbol": str(summary.get("asset_symbol") or ""),
            "pair_label": str(summary.get("pair_label") or ""),
            "timestamp": _int(summary.get("event_ts"), default=int(time.time())),
            "archive_ts": _int(summary.get("event_ts"), default=int(time.time())),
            "operational_intent_key": key,
            "operational_intent_confidence": _float(summary.get("operational_intent_confidence")),
            "confirmation_score": max(_float(summary.get("operational_intent_confidence")), _float(summary.get("strength_score"))),
            "intent_stage": "confirmed",
            "liquidation_stage": liquidation_stage,
            "liquidation_side": str(summary.get("liquidation_side") or ""),
        }

    def _classify_non_lp_item(self, *, row: dict[str, Any], side: str) -> dict[str, Any] | None:
        key = str(
            self._signal_lookup(
                row,
                "operational_intent_key",
                "intent_type",
                "canonical_semantic_key",
            )
            or ""
        ).strip()
        if not key:
            return None
        family = self._non_lp_family(key)
        if not family:
            return None
        if key.startswith("lp_") or key in {"pool_buy_pressure", "pool_sell_pressure"}:
            return None

        confidence = max(
            _float(self._signal_lookup(row, "operational_intent_confidence")),
            _float(self._signal_lookup(row, "intent_confidence")),
            _float(self._signal_lookup(row, "confirmation_score")),
        )
        confirmation_score = _float(self._signal_lookup(row, "confirmation_score"))
        stage = str(self._signal_lookup(row, "operational_intent_stage", "intent_stage") or "").strip().lower()
        liquidation_side = str(self._signal_lookup(row, "liquidation_side") or "").strip().lower()
        observe_only = bool(
            key in _NON_LP_CONTEXT_ONLY_KEYS
            or key.endswith("_observation")
            or stage in {"observe", "observation"}
            or key == "clmm_partial_support_observation"
        )
        tentative = bool(
            not observe_only
            and (
                "likely" in key
                or "tentative" in key
                or stage in {"candidate", "tentative", "preliminary", "watch"}
                or max(confidence, confirmation_score) < _NON_LP_STRONG_CONFIDENCE
            )
        )
        strength = "strong"
        weight_multiplier = 1.0
        if observe_only:
            strength = "observe"
            weight_multiplier = float(OPPORTUNITY_NON_LP_OBSERVE_WEIGHT)
        elif tentative or max(confidence, confirmation_score) < _NON_LP_TENTATIVE_CONFIDENCE:
            strength = "tentative"
            weight_multiplier = float(OPPORTUNITY_NON_LP_TENTATIVE_WEIGHT)

        category = "context"
        support_side = ""
        risk_side = ""
        mapping = _NON_LP_DIRECTIONAL_MAP.get(key)
        if mapping:
            support_side = str(mapping.get("support") or "")
            risk_side = str(mapping.get("risk") or "")
        elif key == "liquidation_risk_building":
            if liquidation_side == "long_flush":
                support_side = "SHORT"
                risk_side = "LONG"
            elif liquidation_side == "short_squeeze":
                support_side = "LONG"
                risk_side = "SHORT"
        if side == support_side:
            category = "support"
        elif side == risk_side:
            category = "risk"
        if key == "clmm_partial_support_observation":
            category = "context"

        base_weight = float(_NON_LP_FAMILY_BASE_WEIGHTS.get(family, 0.0))
        item_weight = round(base_weight * max(weight_multiplier, 0.0), 4)
        return {
            "signal_id": str(self._signal_lookup(row, "signal_id") or ""),
            "ts": _int(self._signal_lookup(row, "archive_ts", "timestamp", "ts"), default=0),
            "intent_key": key,
            "family": family,
            "family_label": _NON_LP_FAMILY_LABELS.get(family, family),
            "effect": category,
            "strength": strength,
            "weight": item_weight,
            "confidence": round(confidence, 4),
            "confirmation_score": round(confirmation_score, 4),
            "liquidation_side": liquidation_side,
            "summary": f"{_NON_LP_FAMILY_LABELS.get(family, family)}:{key}:{category}:{strength}",
        }

    def build_non_lp_evidence_context(
        self,
        asset: str,
        ts: int,
        *,
        window_sec: int = OPPORTUNITY_NON_LP_EVIDENCE_WINDOW_SEC,
        side: str = "NONE",
        pair_label: str = "",
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        empty = {
            "non_lp_evidence_available": False,
            "non_lp_evidence_window_sec": int(window_sec),
            "smart_money_same_direction_count": 0,
            "smart_money_opposite_count": 0,
            "smart_money_entry_support": 0.0,
            "smart_money_exit_risk": 0.0,
            "exchange_flow_support": 0.0,
            "exchange_flow_risk": 0.0,
            "maker_inventory_support": 0.0,
            "maker_inventory_risk": 0.0,
            "clmm_position_support": 0.0,
            "clmm_position_risk": 0.0,
            "liquidation_support": 0.0,
            "liquidation_risk": 0.0,
            "non_lp_support_score": 0.0,
            "non_lp_risk_score": 0.0,
            "non_lp_evidence_items": [],
            "non_lp_evidence_summary": "",
            "non_lp_support_families": [],
            "non_lp_risk_families": [],
            "non_lp_strong_support_families": [],
            "non_lp_strong_risk_families": [],
            "non_lp_strong_conflict": False,
            "trade_opportunity_non_lp_component_score": _NON_LP_SUPPORT_BASELINE,
        }
        if not bool(OPPORTUNITY_NON_LP_EVIDENCE_ENABLE):
            return empty
        side_key = str(side or "NONE")
        asset_symbol = str(asset or "").strip().upper()
        if side_key not in {"LONG", "SHORT"} or not asset_symbol:
            return empty

        rows = self._recent_non_lp_signal_rows(asset_symbol=asset_symbol, ts=int(ts), window_sec=int(window_sec))
        current_row = self._current_non_lp_row(summary or {})
        if current_row:
            rows = [current_row, *rows]

        support_scores: dict[str, float] = defaultdict(float)
        risk_scores: dict[str, float] = defaultdict(float)
        evidence_items: list[dict[str, Any]] = []
        smart_money_same_direction_count = 0
        smart_money_opposite_count = 0
        smart_money_entry_support = 0.0
        smart_money_exit_risk = 0.0
        strong_support_families: set[str] = set()
        strong_risk_families: set[str] = set()

        for row in rows:
            item = self._classify_non_lp_item(row=dict(row or {}), side=side_key)
            if item is None:
                continue
            evidence_items.append(item)
            family = str(item.get("family") or "")
            effect = str(item.get("effect") or "context")
            weight = _float(item.get("weight"))
            intent_key = str(item.get("intent_key") or "")
            strength = str(item.get("strength") or "observe")
            if effect == "support" and weight > 0:
                support_scores[family] = round(min(1.0, float(support_scores.get(family) or 0.0) + weight), 4)
                if strength == "strong":
                    strong_support_families.add(family)
                if family == "smart_money":
                    smart_money_same_direction_count += 1
                if intent_key == "smart_money_entry_execution":
                    smart_money_entry_support = round(min(1.0, smart_money_entry_support + weight), 4)
            elif effect == "risk" and weight > 0:
                risk_scores[family] = round(min(1.0, float(risk_scores.get(family) or 0.0) + weight), 4)
                if strength == "strong":
                    strong_risk_families.add(family)
                if family == "smart_money":
                    smart_money_opposite_count += 1
                if intent_key == "smart_money_exit_execution":
                    smart_money_exit_risk = round(min(1.0, smart_money_exit_risk + weight), 4)

        support_score = round(min(1.0, sum(float(value or 0.0) for value in support_scores.values())), 4)
        risk_score = round(min(1.0, sum(float(value or 0.0) for value in risk_scores.values())), 4)
        component_score = round(_clamp(_NON_LP_SUPPORT_BASELINE + 0.4 * (support_score - risk_score)), 4)
        top_items = sorted(
            evidence_items,
            key=lambda item: (-float(item.get("weight") or 0.0), str(item.get("intent_key") or "")),
        )[:4]
        if top_items:
            summary_text = "; ".join(str(item.get("summary") or "") for item in top_items)
        else:
            summary_text = f"最近{int(window_sec)}s无已确认 non-LP 证据"
        strong_conflict = bool(strong_support_families and strong_risk_families)
        return {
            "non_lp_evidence_available": bool(evidence_items),
            "non_lp_evidence_window_sec": int(window_sec),
            "smart_money_same_direction_count": int(smart_money_same_direction_count),
            "smart_money_opposite_count": int(smart_money_opposite_count),
            "smart_money_entry_support": round(smart_money_entry_support, 4),
            "smart_money_exit_risk": round(smart_money_exit_risk, 4),
            "exchange_flow_support": round(float(support_scores.get("exchange") or 0.0), 4),
            "exchange_flow_risk": round(float(risk_scores.get("exchange") or 0.0), 4),
            "maker_inventory_support": round(float(support_scores.get("maker") or 0.0), 4),
            "maker_inventory_risk": round(float(risk_scores.get("maker") or 0.0), 4),
            "clmm_position_support": round(float(support_scores.get("clmm") or 0.0), 4),
            "clmm_position_risk": round(float(risk_scores.get("clmm") or 0.0), 4),
            "liquidation_support": round(float(support_scores.get("liquidation") or 0.0), 4),
            "liquidation_risk": round(float(risk_scores.get("liquidation") or 0.0), 4),
            "non_lp_support_score": support_score,
            "non_lp_risk_score": risk_score,
            "non_lp_evidence_items": evidence_items[:12],
            "non_lp_evidence_summary": summary_text,
            "non_lp_support_families": sorted(family for family, value in support_scores.items() if value > 0.0),
            "non_lp_risk_families": sorted(family for family, value in risk_scores.items() if value > 0.0),
            "non_lp_strong_support_families": sorted(strong_support_families),
            "non_lp_strong_risk_families": sorted(strong_risk_families),
            "non_lp_strong_conflict": bool(strong_conflict),
            "trade_opportunity_non_lp_component_score": component_score,
        }

    def _empty_history_metrics(self, history_source: str = "none") -> dict[str, Any]:
        return {
            "sample_size": 0,
            "resolved_count": 0,
            "completion_rate": 0.0,
            "followthrough_rate": 0.0,
            "adverse_rate": 1.0,
            "outcome_source_distribution": {},
            "history_source": history_source,
        }

    def _empty_profile_stats(self, *, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "profile_key": str(record.get("opportunity_profile_key") or ""),
            "profile_version": str(record.get("opportunity_profile_version") or _PROFILE_VERSION),
            "side": str(record.get("opportunity_profile_side") or record.get("trade_opportunity_side") or "NONE"),
            "asset": str(record.get("opportunity_profile_asset") or record.get("asset_symbol") or ""),
            "pair_family": str(record.get("opportunity_profile_pair_family") or self._pair_family(str(record.get("pair_label") or ""))),
            "strategy": str(record.get("opportunity_profile_strategy") or "lp_candidate"),
            "features_json": dict(record.get("opportunity_profile_features_json") or {}),
            "sample_count": 0,
            "candidate_count": 0,
            "verified_count": 0,
            "blocked_count": 0,
            "completed_30s": 0,
            "completed_60s": 0,
            "completed_300s": 0,
            "followthrough_30s_count": 0,
            "followthrough_60s_count": 0,
            "followthrough_300s_count": 0,
            "adverse_30s_count": 0,
            "adverse_60s_count": 0,
            "adverse_300s_count": 0,
            "blocked_completed_30s": 0,
            "blocked_completed_60s": 0,
            "blocked_completed_300s": 0,
            "blocker_saved_trade_30s_count": 0,
            "blocker_saved_trade_60s_count": 0,
            "blocker_saved_trade_300s_count": 0,
            "blocker_false_block_30s_count": 0,
            "blocker_false_block_60s_count": 0,
            "blocker_false_block_300s_count": 0,
            "last_updated": int(time.time()),
        }

    def _profile_stat_entry(self, record: dict[str, Any]) -> dict[str, Any]:
        profile_key = str(record.get("opportunity_profile_key") or "")
        if not profile_key:
            return {}
        entry = self._profile_stats.get(profile_key)
        if entry is None:
            entry = self._empty_profile_stats(record=record)
            self._profile_stats[profile_key] = entry
        return entry

    def _history_metrics_from_profile_stats(
        self,
        rows: list[dict[str, Any]],
        *,
        history_source: str,
        history_scope_type: str,
        history_scope_key: str,
    ) -> dict[str, Any]:
        if not rows:
            payload = self._empty_history_metrics(history_source)
            payload.update(
                {
                    "history_scope_type": history_scope_type,
                    "history_scope_key": history_scope_key,
                }
            )
            return payload
        sample_size = sum(int(row.get("sample_count") or 0) for row in rows)
        resolved = sum(int(row.get("completed_60s") or 0) for row in rows)
        followthrough = sum(int(row.get("followthrough_60s_count") or 0) for row in rows)
        adverse = sum(int(row.get("adverse_60s_count") or 0) for row in rows)
        source_counter: Counter = Counter()
        for row in rows:
            for key, value in dict((row.get("stats_json") or {}).get("outcome_source_distribution_60s") or {}).items():
                source_counter[str(key)] += int(value or 0)
        return {
            "sample_size": int(sample_size),
            "resolved_count": int(resolved),
            "completion_rate": _rate(resolved, sample_size),
            "followthrough_rate": _rate(followthrough, resolved),
            "adverse_rate": _rate(adverse, resolved) if resolved else 1.0,
            "outcome_source_distribution": dict(sorted(source_counter.items())),
            "history_source": history_source,
            "history_scope_type": history_scope_type,
            "history_scope_key": history_scope_key,
        }

    def _profile_scope_rows(self, *, summary: dict[str, Any], scope_type: str) -> list[dict[str, Any]]:
        side = str(summary.get("opportunity_profile_side") or summary.get("trade_opportunity_side") or "NONE")
        asset = str(summary.get("opportunity_profile_asset") or summary.get("asset_symbol") or "")
        pair_family = str(summary.get("opportunity_profile_pair_family") or self._pair_family(str(summary.get("pair_label") or "")))
        profile_key = str(summary.get("opportunity_profile_key") or "")
        rows = list(self._profile_stats.values())
        if scope_type == "profile":
            return [row for row in rows if str(row.get("profile_key") or "") == profile_key]
        if scope_type == "asset_side":
            return [
                row
                for row in rows
                if str(row.get("asset") or "") == asset and str(row.get("side") or "") == side
            ]
        if scope_type == "pair_family_side":
            return [
                row
                for row in rows
                if str(row.get("pair_family") or "") == pair_family and str(row.get("side") or "") == side
            ]
        if scope_type == "global_side":
            return [row for row in rows if str(row.get("side") or "") == side]
        return []

    def _history_snapshot(self, *, summary: dict[str, Any]) -> dict[str, Any]:
        asset_symbol = str(summary.get("asset_symbol") or "").upper()
        side = str(summary.get("trade_opportunity_side") or "NONE")
        direction = "long" if side == "LONG" else "short" if side == "SHORT" else "neutral"
        scope_chain = [
            ("profile", str(summary.get("opportunity_profile_key") or "")),
            ("asset_side", f"{asset_symbol}|{side}"),
            ("pair_family_side", f"{str(summary.get('opportunity_profile_pair_family') or '')}|{side}"),
            ("global_side", side),
        ]
        fallback_chain: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None
        for scope_type, scope_key in scope_chain:
            scope_rows = self._profile_scope_rows(summary=summary, scope_type=scope_type)
            stats = self._history_metrics_from_profile_stats(
                scope_rows,
                history_source="opportunity_profile" if scope_type == "profile" else f"opportunity_profile_fallback:{scope_type}",
                history_scope_type=scope_type,
                history_scope_key=scope_key,
            )
            fallback_chain.append(
                {
                    "scope_type": scope_type,
                    "scope_key": scope_key,
                    "sample_size": int(stats.get("sample_size") or 0),
                    "completion_rate": float(stats.get("completion_rate") or 0.0),
                    "followthrough_rate": float(stats.get("followthrough_rate") or 0.0),
                    "adverse_rate": float(stats.get("adverse_rate") or 1.0),
                    "history_source": str(stats.get("history_source") or ""),
                }
            )
            if selected is None:
                selected = stats
            if int(stats.get("sample_size") or 0) >= int(OPPORTUNITY_MIN_HISTORY_SAMPLES):
                selected = stats
                break

        opportunity_rows = [
            row
            for row in self._opportunities
            if str(row.get("asset_symbol") or "").upper() == asset_symbol
            and str(row.get("trade_opportunity_side") or "NONE") == side
            and str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED"}
        ][-self.history_limit :]
        opportunity_snapshot = self._history_metrics_from_records(opportunity_rows, history_source="opportunity_asset_legacy")
        lp_fallback = self._lp_outcome_fallback(direction=direction)
        selected = dict(selected or self._empty_history_metrics())
        if int(selected.get("sample_size") or 0) == 0 and opportunity_snapshot["sample_size"] > 0:
            selected = dict(opportunity_snapshot)
            selected["history_scope_type"] = "legacy_asset_side"
            selected["history_scope_key"] = f"{asset_symbol}|{side}"
        if int(selected.get("sample_size") or 0) < int(OPPORTUNITY_MIN_HISTORY_SAMPLES) and lp_fallback["sample_size"] > int(selected.get("sample_size") or 0):
            selected = dict(lp_fallback)
            selected["history_source"] = "lp_outcome_fallback"
            selected["history_scope_type"] = "lp_outcome_directional"
            selected["history_scope_key"] = direction
        selected["fallback_chain"] = fallback_chain
        selected["fallback_used"] = str(selected.get("history_scope_type") or "profile") != "profile"
        selected["profile_sample_size"] = int((fallback_chain[0] or {}).get("sample_size") if fallback_chain else 0)
        selected["asset_side_sample_size"] = int((fallback_chain[1] or {}).get("sample_size") if len(fallback_chain) > 1 else 0)
        selected["pair_family_side_sample_size"] = int((fallback_chain[2] or {}).get("sample_size") if len(fallback_chain) > 2 else 0)
        selected["global_side_sample_size"] = int((fallback_chain[3] or {}).get("sample_size") if len(fallback_chain) > 3 else 0)
        selected["opportunity_sample_size"] = int(opportunity_snapshot["sample_size"])
        selected["lp_sample_size"] = int(lp_fallback["sample_size"])
        selected["opportunity_followthrough_rate"] = float(opportunity_snapshot["followthrough_rate"])
        selected["opportunity_adverse_rate"] = float(opportunity_snapshot["adverse_rate"])
        selected["opportunity_completion_rate"] = float(opportunity_snapshot["completion_rate"])
        selected["lp_followthrough_rate"] = float(lp_fallback["followthrough_rate"])
        selected["lp_adverse_rate"] = float(lp_fallback["adverse_rate"])
        selected["lp_completion_rate"] = float(lp_fallback["completion_rate"])
        return selected

    def _history_metrics_from_records(self, rows: list[dict[str, Any]], *, history_source: str) -> dict[str, Any]:
        sample_size = len(rows)
        completed = 0
        followthrough = 0
        adverse = 0
        source_counter: Counter = Counter()
        for row in rows:
            status = str(row.get("opportunity_outcome_60s") or "")
            source_counter[str(row.get("opportunity_outcome_source") or row.get("outcome_price_source") or "unavailable")] += 1
            if status != "completed":
                continue
            completed += 1
            if row.get("opportunity_followthrough_60s") is True:
                followthrough += 1
            if row.get("opportunity_adverse_60s") is True:
                adverse += 1
        return {
            "sample_size": int(sample_size),
            "resolved_count": int(completed),
            "completion_rate": round((completed / sample_size) if sample_size else 0.0, 4),
            "followthrough_rate": round((followthrough / completed) if completed else 0.0, 4),
            "adverse_rate": round((adverse / completed) if completed else 1.0, 4),
            "outcome_source_distribution": dict(sorted(source_counter.items())),
            "history_source": history_source,
        }

    def _lp_outcome_fallback(self, *, direction: str) -> dict[str, Any]:
        if self.state_manager is None or not hasattr(self.state_manager, "get_recent_lp_outcome_records"):
            return {
                "sample_size": 0,
                "resolved_count": 0,
                "completion_rate": 0.0,
                "followthrough_rate": 0.0,
                "adverse_rate": 1.0,
                "outcome_source_distribution": {},
                "history_source": "none",
            }
        direction_bucket = "buy_pressure" if direction == "long" else "sell_pressure" if direction == "short" else ""
        rows = [
            row
            for row in list(self.state_manager.get_recent_lp_outcome_records(limit=self.history_limit))
            if str(row.get("direction_bucket") or "") == direction_bucket
            and str(row.get("lp_alert_stage") or "") in {"confirm", "climax", "exhaustion_risk"}
        ]
        sample_size = len(rows)
        completed = 0
        followthrough = 0
        adverse = 0
        source_counter: Counter = Counter()
        for row in rows:
            windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
            window = dict(windows.get("60s") or {})
            status = str(window.get("status") or "")
            source_counter[str(window.get("price_source") or row.get("outcome_price_source") or "unavailable")] += 1
            if status != "completed":
                continue
            completed += 1
            adjusted_move = window.get("direction_adjusted_move_after")
            if adjusted_move is None:
                adjusted_move = row.get("direction_adjusted_move_after_60s")
            if adjusted_move is not None and float(adjusted_move) > _FOLLOWTHROUGH_MIN_MOVE:
                followthrough += 1
            adverse_flag = window.get("adverse_by_direction")
            if adverse_flag is None:
                adverse_flag = row.get("adverse_by_direction_60s")
            if adverse_flag is True:
                adverse += 1
        return {
            "sample_size": int(sample_size),
            "resolved_count": int(completed),
            "completion_rate": round((completed / sample_size) if sample_size else 0.0, 4),
            "followthrough_rate": round((followthrough / completed) if completed else 0.0, 4),
            "adverse_rate": round((adverse / completed) if completed else 1.0, 4),
            "outcome_source_distribution": dict(sorted(source_counter.items())),
            "history_source": "lp_outcome_fallback",
        }

    def _score(self, *, summary: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
        direction = str(summary.get("direction") or "neutral")
        lp_confirm_scope = str(summary.get("lp_confirm_scope") or "")
        broader_alignment = str(summary.get("lp_broader_alignment") or "")
        market_source = str(summary.get("market_context_source") or "unavailable")
        timing = str(summary.get("alert_relative_timing") or "")
        sweep_phase = str(summary.get("lp_sweep_phase") or "")
        confirm_quality = str(summary.get("lp_confirm_quality") or "")
        multi_pool_resonance = max(
            _int(summary.get("lp_multi_pool_resonance")),
            _int(summary.get("asset_case_supporting_pair_count")),
        )
        same_pool_continuity = _int(summary.get("lp_same_pool_continuity"))
        aligned = _market_alignment_positive(
            direction,
            summary.get("market_move_before_alert_30s"),
            summary.get("market_move_before_alert_60s"),
            summary.get("market_move_after_alert_60s"),
        )
        crowded = _has_crowding(direction, summary.get("basis_bps"), summary.get("mark_index_spread_bps"))
        quality_floor = min(
            _float(summary.get("pool_quality_score")),
            _float(summary.get("pair_quality_score")),
            _float(summary.get("asset_case_quality_score")),
        )

        direction_score = 0.0
        direction_reasons: list[str] = []
        if lp_confirm_scope == "broader_confirm":
            direction_score += 0.46
            direction_reasons.append("broader_confirm")
        elif lp_confirm_scope == "local_confirm":
            direction_score += 0.18
            direction_reasons.append("local_confirm_only")
        if broader_alignment == "confirmed":
            direction_score += 0.24
            direction_reasons.append("broader_alignment")
        if aligned:
            direction_score += 0.20
            direction_reasons.append("live_direction_aligned")
        if str(summary.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}:
            direction_score += 0.10
            direction_reasons.append("trade_action_candidate")
        direction_component = _score_component(0.22, direction_score, " + ".join(direction_reasons) or "direction_unclear")

        market_score = 0.0
        market_reasons: list[str] = []
        if market_source == "live_public":
            market_score += 0.46
            market_reasons.append("live_public")
        if timing in {"leading", "confirming"}:
            market_score += 0.22
            market_reasons.append(f"timing={timing}")
        if not crowded:
            market_score += 0.20
            market_reasons.append("basis_clean")
        if _text(summary.get("perp_mark_price")) and _text(summary.get("perp_index_price")):
            market_score += 0.12
            market_reasons.append("mark_index_present")
        if aligned:
            market_score += 0.08
            market_reasons.append("moves_aligned")
        market_component = _score_component(0.18, market_score, " + ".join(market_reasons) or "market_context_weak")

        structure_score = 0.0
        structure_reasons: list[str] = []
        if multi_pool_resonance >= 3:
            structure_score += 0.36
            structure_reasons.append(f"cross_pool={multi_pool_resonance}")
        elif multi_pool_resonance >= 2:
            structure_score += 0.28
            structure_reasons.append(f"cross_pool={multi_pool_resonance}")
        elif same_pool_continuity >= 2:
            structure_score += 0.16
            structure_reasons.append(f"same_pool={same_pool_continuity}")
        if confirm_quality == "clean_confirm":
            structure_score += 0.28
            structure_reasons.append("clean_confirm")
        if sweep_phase == "sweep_confirmed" and _float(summary.get("lp_sweep_continuation_score")) >= 0.72:
            structure_score += 0.18
            structure_reasons.append("strong_sweep")
        if str(summary.get("prealert_lifecycle_state") or "") in {"upgraded_to_confirm", "merged"}:
            structure_score += 0.08
            structure_reasons.append("prealert_to_confirm")
        if str(summary.get("lp_alert_stage") or "") == "confirm":
            structure_score += 0.10
            structure_reasons.append("confirm_stage")
        if str(summary.get("lp_alert_stage") or "") == "exhaustion_risk" or sweep_phase == "sweep_exhaustion_risk":
            structure_score -= 0.22
            structure_reasons.append("exhaustion_penalty")
        structure_component = _score_component(0.18, structure_score, " + ".join(structure_reasons) or "structure_weak")

        history_quality_score = (
            quality_floor * 0.55
            + float(history.get("followthrough_rate") or 0.0) * 0.25
            + float(history.get("completion_rate") or 0.0) * 0.10
            + max(0.0, 1.0 - _float(history.get("adverse_rate"), default=1.0)) * 0.10
        )
        quality_reason = (
            f"quality_floor={round(quality_floor, 3)} "
            f"history={history.get('history_source')} "
            f"ft={history.get('followthrough_rate')} "
            f"adv={history.get('adverse_rate')}"
        )
        quality_component = _score_component(0.20, history_quality_score, quality_reason)

        risk_score = 1.0
        risk_reasons: list[str] = ["clean"]
        if crowded:
            risk_score -= 0.18
            risk_reasons.append("crowded")
        if str(summary.get("lp_absorption_context") or "").startswith("local_") or str(summary.get("lp_absorption_context") or "") == "pool_only_unconfirmed_pressure":
            risk_score -= 0.22
            risk_reasons.append("local_absorption")
        if timing == "late" or confirm_quality in {"late_confirm", "chase_risk"}:
            risk_score -= 0.22
            risk_reasons.append("late_or_chase")
        if str(summary.get("lp_conflict_resolution") or "") not in {"", "no_conflict", "strong_side_override"}:
            risk_score -= 0.28
            risk_reasons.append("conflict")
        if market_source != "live_public":
            risk_score -= 0.32
            risk_reasons.append("data_gap")
        risk_component = _score_component(0.12, risk_score, " + ".join(risk_reasons))

        non_lp_support_score = _float(summary.get("non_lp_support_score"))
        non_lp_risk_score = _float(summary.get("non_lp_risk_score"))
        non_lp_component_score = _float(summary.get("trade_opportunity_non_lp_component_score"), default=_NON_LP_SUPPORT_BASELINE)
        non_lp_reason = (
            f"support={round(non_lp_support_score, 3)} "
            f"risk={round(non_lp_risk_score, 3)} "
            f"summary={str(summary.get('non_lp_evidence_summary') or 'none')}"
        )
        non_lp_component = _score_component(float(OPPORTUNITY_NON_LP_SCORE_WEIGHT), non_lp_component_score, non_lp_reason)

        subtotal_without_non_lp = round(
            direction_component["weighted_score"]
            + market_component["weighted_score"]
            + structure_component["weighted_score"]
            + quality_component["weighted_score"]
            + risk_component["weighted_score"],
            4,
        )
        raw_score = round(subtotal_without_non_lp + non_lp_component["weighted_score"], 4)
        score_components = {
            "direction_confirmation": direction_component,
            "market_context": market_component,
            "lp_structure": structure_component,
            "quality_outcome": quality_component,
            "risk_cleanliness": risk_component,
            "non_lp_evidence": non_lp_component,
        }
        calibration_snapshot = calibrate_opportunity_score(
            raw_score=max(0.0, min(1.0, raw_score)),
            profile_key=str(summary.get("opportunity_profile_key") or ""),
            asset=str(summary.get("asset_symbol") or ""),
            side=str(summary.get("trade_opportunity_side") or "NONE"),
            components=score_components,
            pair_family=str(summary.get("opportunity_profile_pair_family") or ""),
            profile_rows=list(self._profile_stats.values()),
        )
        calibrated_score = float(calibration_snapshot.get("opportunity_calibrated_score") or 0.0)
        calibration_adjustment = float(calibration_snapshot.get("opportunity_calibration_adjustment") or 0.0)
        score_components["database_calibration"] = {
            "weight": 1.0,
            "score": round(calibrated_score, 4),
            "weighted_score": round(calibration_adjustment, 4),
            "reason": str(calibration_snapshot.get("opportunity_calibration_reason") or ""),
            "source": str(calibration_snapshot.get("opportunity_calibration_source") or "none"),
            "sample_count": int(calibration_snapshot.get("opportunity_calibration_sample_count") or 0),
            "confidence": round(_float(calibration_snapshot.get("opportunity_calibration_confidence")), 4),
        }
        return {
            "trade_opportunity_score": round(max(0.0, min(1.0, calibrated_score)), 4),
            "trade_opportunity_raw_score": round(max(0.0, min(1.0, raw_score)), 4),
            "trade_opportunity_calibrated_score": round(max(0.0, min(1.0, calibrated_score)), 4),
            "trade_opportunity_score_without_non_lp": max(0.0, min(1.0, subtotal_without_non_lp)),
            "trade_opportunity_non_lp_score_delta": round(non_lp_component["weighted_score"], 4),
            "trade_opportunity_non_lp_component_score": round(non_lp_component_score, 4),
            "opportunity_raw_score": round(max(0.0, min(1.0, raw_score)), 4),
            "opportunity_calibrated_score": round(max(0.0, min(1.0, calibrated_score)), 4),
            "opportunity_calibration_adjustment": round(calibration_adjustment, 4),
            "opportunity_calibration_reason": str(calibration_snapshot.get("opportunity_calibration_reason") or ""),
            "opportunity_calibration_sample_count": int(calibration_snapshot.get("opportunity_calibration_sample_count") or 0),
            "opportunity_calibration_confidence": round(_float(calibration_snapshot.get("opportunity_calibration_confidence")), 4),
            "opportunity_calibration_source": str(calibration_snapshot.get("opportunity_calibration_source") or "none"),
            "trade_opportunity_score_components": score_components,
            "trade_opportunity_calibration_snapshot": calibration_snapshot,
        }

    def _evaluate(self, *, summary: dict[str, Any], history: dict[str, Any], score_payload: dict[str, Any]) -> dict[str, Any]:
        status = "NONE"
        side = str(summary.get("trade_opportunity_side") or "NONE")
        raw_score = float(score_payload.get("opportunity_raw_score") or score_payload.get("trade_opportunity_raw_score") or 0.0)
        calibrated_score = float(
            score_payload.get("opportunity_calibrated_score")
            or score_payload.get("trade_opportunity_calibrated_score")
            or score_payload.get("trade_opportunity_score")
            or 0.0
        )
        quality_floor = min(
            _float(summary.get("pool_quality_score")),
            _float(summary.get("pair_quality_score")),
            _float(summary.get("asset_case_quality_score")),
        )
        preliminary_quality_pass = bool(
            _float(summary.get("asset_case_quality_score")) >= float(TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE)
            and min(_float(summary.get("pair_quality_score")), _float(summary.get("pool_quality_score"))) >= float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE)
        )
        live_context = str(summary.get("market_context_source") or "unavailable") == "live_public"
        broader_confirm = str(summary.get("lp_confirm_scope") or "") == "broader_confirm"
        clean_or_continuation = bool(
            str(summary.get("lp_confirm_quality") or "") == "clean_confirm"
            or (
                str(summary.get("lp_sweep_phase") or "") == "sweep_confirmed"
                and _float(summary.get("lp_sweep_continuation_score")) >= 0.72
            )
        )
        strong_directional = bool(
            side in {"LONG", "SHORT"}
            and (
                broader_confirm
                or str(summary.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
                or _float(summary.get("strength_score")) >= _STRONG_SIGNAL_MIN
            )
        )
        hard_blockers, risk_flags = self._hard_blockers(
            summary=summary,
            preliminary_quality_pass=preliminary_quality_pass,
        )
        verification_gate = self._history_gate(history)
        verification_blockers = list(verification_gate.get("blockers") or [])
        risk_flags.extend(verification_blockers)
        if "profile_sample_count_insufficient" in verification_blockers or "outcome_history_insufficient" in verification_blockers:
            risk_flags.append("history_samples_insufficient")

        if side == "NONE":
            status = "NONE"
        elif hard_blockers and strong_directional:
            status = "BLOCKED"
        else:
            candidate_ready = bool(
                strong_directional
                and calibrated_score >= float(OPPORTUNITY_MIN_CANDIDATE_SCORE)
                and preliminary_quality_pass
                and (live_context or not bool(OPPORTUNITY_REQUIRE_LIVE_CONTEXT))
                and (broader_confirm or not bool(OPPORTUNITY_REQUIRE_BROADER_CONFIRM))
            )
            verified_ready = bool(
                candidate_ready
                and clean_or_continuation
                and calibrated_score >= float(OPPORTUNITY_MIN_VERIFIED_SCORE)
                and verification_gate["status"] == "verified_ready"
            )
            if verified_ready:
                status = "VERIFIED"
            elif candidate_ready:
                status = "CANDIDATE"
            elif strong_directional and not preliminary_quality_pass:
                status = "BLOCKED"
                hard_blockers.append("low_quality")
                risk_flags.append("low_quality")
            else:
                status = "NONE"

        primary_hard_blocker = self._primary_blocker(hard_blockers)
        primary_verification_blocker = self._primary_blocker(verification_blockers)
        primary_blocker = primary_hard_blocker if status == "BLOCKED" else primary_verification_blocker
        evidence = self._build_evidence(summary=summary, status=status, quality_floor=quality_floor, history=history)
        reason = self._reason(
            summary=summary,
            status=status,
            side=side,
            primary_blocker=primary_blocker,
            verification_gate=verification_gate,
            score=calibrated_score,
        )
        required_confirmation = self._required_confirmation(
            summary=summary,
            status=status,
            primary_hard_blocker=primary_hard_blocker,
            primary_verification_blocker=primary_verification_blocker,
            verification_gate=verification_gate,
        )
        invalidated_by = self._invalidated_by(summary=summary, status=status, primary_blocker=primary_blocker)
        source = self._source(summary=summary)
        created_at = _int(summary.get("event_ts"), default=int(time.time()))
        time_horizon = self._time_horizon(summary=summary, status=status)
        expires_at = created_at + {"30s": 30, "60s": 60, "300s": 300}.get(time_horizon, 60)
        confidence = self._confidence(status=status, score=calibrated_score, primary_blocker=primary_blocker)
        opportunity_key = self._key(
            summary=summary,
            side=side,
            status=status,
            primary_blocker=primary_blocker,
        )
        opportunity_id = self._id(opportunity_key=opportunity_key, signal_id=str(summary.get("signal_id") or ""), event_ts=created_at)
        calibration_snapshot = dict(score_payload.get("trade_opportunity_calibration_snapshot") or {})
        return {
            "trade_opportunity_id": opportunity_id,
            "trade_opportunity_key": opportunity_key,
            "trade_opportunity_side": side,
            "trade_opportunity_status": status,
            "trade_opportunity_label": _label_for(status, side),
            "trade_opportunity_score": round(calibrated_score, 4),
            "trade_opportunity_raw_score": round(raw_score, 4),
            "trade_opportunity_calibrated_score": round(calibrated_score, 4),
            "trade_opportunity_score_without_non_lp": round(float(score_payload.get("trade_opportunity_score_without_non_lp") or 0.0), 4),
            "trade_opportunity_non_lp_score_delta": round(float(score_payload.get("trade_opportunity_non_lp_score_delta") or 0.0), 4),
            "trade_opportunity_non_lp_component_score": round(float(score_payload.get("trade_opportunity_non_lp_component_score") or _NON_LP_SUPPORT_BASELINE), 4),
            "opportunity_raw_score": round(raw_score, 4),
            "opportunity_calibrated_score": round(calibrated_score, 4),
            "opportunity_calibration_adjustment": round(float(score_payload.get("opportunity_calibration_adjustment") or 0.0), 4),
            "opportunity_calibration_reason": str(score_payload.get("opportunity_calibration_reason") or ""),
            "opportunity_calibration_sample_count": int(score_payload.get("opportunity_calibration_sample_count") or 0),
            "opportunity_calibration_confidence": round(_float(score_payload.get("opportunity_calibration_confidence")), 4),
            "opportunity_calibration_source": str(score_payload.get("opportunity_calibration_source") or "none"),
            "trade_opportunity_confidence": confidence,
            "trade_opportunity_time_horizon": time_horizon,
            "trade_opportunity_reason": reason,
            "trade_opportunity_evidence": evidence,
            "trade_opportunity_blockers": _list_dedup(hard_blockers + verification_blockers, limit=8),
            "trade_opportunity_hard_blockers": _list_dedup(hard_blockers, limit=8),
            "trade_opportunity_verification_blockers": _list_dedup(verification_blockers, limit=8),
            "trade_opportunity_required_confirmation": required_confirmation,
            "trade_opportunity_invalidated_by": invalidated_by,
            "trade_opportunity_risk_flags": _list_dedup(risk_flags, limit=8),
            "trade_opportunity_primary_hard_blocker": primary_hard_blocker,
            "trade_opportunity_primary_verification_blocker": primary_verification_blocker,
            "trade_opportunity_quality_snapshot": {
                "pool_quality_score": round(_float(summary.get("pool_quality_score")), 4),
                "pair_quality_score": round(_float(summary.get("pair_quality_score")), 4),
                "asset_case_quality_score": round(_float(summary.get("asset_case_quality_score")), 4),
                "quality_floor": round(quality_floor, 4),
                "history": history,
                "trade_action_key": str(summary.get("trade_action_key") or ""),
                "asset_market_state_key": str(summary.get("asset_market_state_key") or ""),
                "calibration": calibration_snapshot,
                "non_lp_evidence_context": {
                    "support_score": round(_float(summary.get("non_lp_support_score")), 4),
                    "risk_score": round(_float(summary.get("non_lp_risk_score")), 4),
                    "summary": str(summary.get("non_lp_evidence_summary") or ""),
                    "support_families": list(summary.get("non_lp_support_families") or []),
                    "risk_families": list(summary.get("non_lp_risk_families") or []),
                },
            },
            "trade_opportunity_outcome_policy": {
                "windows": ["30s", "60s", "300s"],
                "require_outcome_history": bool(OPPORTUNITY_REQUIRE_OUTCOME_HISTORY),
                "price_source_priority": ["okx_mark", "okx_index", "okx_last", "kraken_mark", "kraken_last"],
                "history_source": str(history.get("history_source") or "none"),
            },
            "trade_opportunity_calibration_snapshot": calibration_snapshot,
            "trade_opportunity_created_at": created_at,
            "trade_opportunity_expires_at": expires_at,
            "trade_opportunity_source": source,
            "trade_opportunity_score_components": dict(score_payload.get("trade_opportunity_score_components") or {}),
            "trade_opportunity_history_snapshot": dict(history),
            "trade_opportunity_non_lp_evidence_context": {
                "non_lp_evidence_available": bool(summary.get("non_lp_evidence_available")),
                "non_lp_evidence_window_sec": int(summary.get("non_lp_evidence_window_sec") or 0),
                "smart_money_same_direction_count": int(summary.get("smart_money_same_direction_count") or 0),
                "smart_money_opposite_count": int(summary.get("smart_money_opposite_count") or 0),
                "smart_money_entry_support": round(_float(summary.get("smart_money_entry_support")), 4),
                "smart_money_exit_risk": round(_float(summary.get("smart_money_exit_risk")), 4),
                "exchange_flow_support": round(_float(summary.get("exchange_flow_support")), 4),
                "exchange_flow_risk": round(_float(summary.get("exchange_flow_risk")), 4),
                "maker_inventory_support": round(_float(summary.get("maker_inventory_support")), 4),
                "maker_inventory_risk": round(_float(summary.get("maker_inventory_risk")), 4),
                "clmm_position_support": round(_float(summary.get("clmm_position_support")), 4),
                "clmm_position_risk": round(_float(summary.get("clmm_position_risk")), 4),
                "liquidation_support": round(_float(summary.get("liquidation_support")), 4),
                "liquidation_risk": round(_float(summary.get("liquidation_risk")), 4),
                "non_lp_support_score": round(_float(summary.get("non_lp_support_score")), 4),
                "non_lp_risk_score": round(_float(summary.get("non_lp_risk_score")), 4),
                "non_lp_support_families": list(summary.get("non_lp_support_families") or []),
                "non_lp_risk_families": list(summary.get("non_lp_risk_families") or []),
                "non_lp_strong_support_families": list(summary.get("non_lp_strong_support_families") or []),
                "non_lp_strong_risk_families": list(summary.get("non_lp_strong_risk_families") or []),
                "non_lp_strong_conflict": bool(summary.get("non_lp_strong_conflict")),
                "non_lp_evidence_items": list(summary.get("non_lp_evidence_items") or []),
                "non_lp_evidence_summary": str(summary.get("non_lp_evidence_summary") or ""),
            },
            "trade_opportunity_non_lp_evidence_summary": str(summary.get("non_lp_evidence_summary") or ""),
            "trade_opportunity_primary_blocker": primary_blocker,
            "trade_opportunity_status_at_creation": status,
            "asset_symbol": str(summary.get("asset_symbol") or ""),
            "pair_label": str(summary.get("pair_label") or ""),
            "opportunity_profile_key": str(summary.get("opportunity_profile_key") or ""),
            "opportunity_profile_version": str(summary.get("opportunity_profile_version") or _PROFILE_VERSION),
            "opportunity_profile_side": str(summary.get("opportunity_profile_side") or side),
            "opportunity_profile_asset": str(summary.get("opportunity_profile_asset") or summary.get("asset_symbol") or ""),
            "opportunity_profile_pair_family": str(summary.get("opportunity_profile_pair_family") or ""),
            "opportunity_profile_strategy": str(summary.get("opportunity_profile_strategy") or ""),
            "opportunity_profile_features_json": dict(summary.get("opportunity_profile_features_json") or {}),
            "outcome_tracking_key": str(summary.get("outcome_tracking_key") or ""),
            "opportunity_outcome_source": str(summary.get("outcome_price_source") or "unavailable"),
            "opportunity_outcome_30s": "pending",
            "opportunity_outcome_60s": "pending",
            "opportunity_outcome_300s": "pending",
            "opportunity_followthrough_30s": None,
            "opportunity_followthrough_60s": None,
            "opportunity_followthrough_300s": None,
            "opportunity_adverse_30s": None,
            "opportunity_adverse_60s": None,
            "opportunity_adverse_300s": None,
            "opportunity_invalidated_at": None,
            "opportunity_invalidated_reason": "",
            "opportunity_result_label": "neutral",
            "trade_opportunity_notifier_sent_at": None,
            "trade_opportunity_delivered_notification": False,
            "blocker_type": primary_hard_blocker,
            "would_have_been_direction": side,
            "blocker_effectiveness_window_sec": 60,
            "adverse_after_block": None,
            "blocker_saved_trade": None,
            "blocker_false_block_possible": None,
        }

    def _hard_blockers(
        self,
        *,
        summary: dict[str, Any],
        preliminary_quality_pass: bool,
    ) -> tuple[list[str], list[str]]:
        blockers: list[str] = []
        risk_flags: list[str] = []
        market_source = str(summary.get("market_context_source") or "unavailable")
        timing = str(summary.get("alert_relative_timing") or "")
        confirm_quality = str(summary.get("lp_confirm_quality") or "")
        absorption = str(summary.get("lp_absorption_context") or "")
        trade_action_key = str(summary.get("trade_action_key") or "")
        asset_market_state_key = str(summary.get("asset_market_state_key") or "")
        crowded = _has_crowding(str(summary.get("direction") or ""), summary.get("basis_bps"), summary.get("mark_index_spread_bps"))
        strong_non_lp_risk = list(summary.get("non_lp_strong_risk_families") or [])
        strong_non_lp_support = list(summary.get("non_lp_strong_support_families") or [])

        if _bool(summary.get("no_trade_lock_active")) or asset_market_state_key == "NO_TRADE_LOCK":
            blockers.append("no_trade_lock")
        if trade_action_key == "CONFLICT_NO_TRADE" or str(summary.get("lp_conflict_resolution") or "") == "wait_for_direction_unification":
            blockers.append("direction_conflict")
        if bool(OPPORTUNITY_REQUIRE_LIVE_CONTEXT) and market_source != "live_public":
            blockers.append("data_gap")
        if trade_action_key in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}:
            if str(summary.get("lp_alert_stage") or "") == "exhaustion_risk" or str(summary.get("lp_sweep_phase") or "") == "sweep_exhaustion_risk":
                blockers.append("sweep_exhaustion_risk")
            elif timing == "late" or confirm_quality in {"late_confirm", "chase_risk"}:
                blockers.append("late_or_chase")
        if str(summary.get("lp_alert_stage") or "") == "exhaustion_risk" or str(summary.get("lp_sweep_phase") or "") == "sweep_exhaustion_risk":
            blockers.append("sweep_exhaustion_risk")
        if timing == "late" or confirm_quality in {"late_confirm", "chase_risk"} or _float(summary.get("lp_chase_risk_score")) >= 0.58:
            blockers.append("late_or_chase")
        if absorption.startswith("local_") or absorption == "pool_only_unconfirmed_pressure":
            blockers.append("local_absorption")
        if crowded:
            blockers.append("crowded_basis")
        if not preliminary_quality_pass:
            blockers.append("low_quality")
        if self._has_recent_opposite_signal(summary=summary):
            blockers.append("strong_opposite_signal")
        if bool(OPPORTUNITY_NON_LP_STRONG_BLOCKER_ENABLE):
            if bool(summary.get("non_lp_strong_conflict")) and strong_non_lp_risk and strong_non_lp_support:
                blockers.append("non_lp_evidence_conflict")
            elif "smart_money" in strong_non_lp_risk:
                blockers.append("non_lp_opposite_smart_money")
            elif "maker" in strong_non_lp_risk:
                blockers.append("non_lp_maker_against")
            elif "exchange" in strong_non_lp_risk:
                blockers.append("non_lp_exchange_flow_against")
            elif "clmm" in strong_non_lp_risk:
                blockers.append("non_lp_clmm_position_against")
            elif "liquidation" in strong_non_lp_risk:
                blockers.append("non_lp_liquidation_against")
        risk_flags.extend(blockers)
        return _list_dedup(blockers, limit=8), _list_dedup(risk_flags, limit=8)

    def _history_gate(self, history: dict[str, Any]) -> dict[str, Any]:
        blockers: list[str] = []
        sample_size = int(history.get("sample_size") or 0)
        completion_rate = float(history.get("completion_rate") or 0.0)
        followthrough_rate = float(history.get("followthrough_rate") or 0.0)
        adverse_rate = _float(history.get("adverse_rate"), default=1.0)
        if not bool(OPPORTUNITY_REQUIRE_OUTCOME_HISTORY):
            return {"status": "verified_ready", "blockers": blockers}
        if sample_size <= 0:
            blockers.append("outcome_history_insufficient")
            return {"status": "candidate_only", "blockers": blockers}
        if sample_size < int(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            blockers.append("profile_sample_count_insufficient")
            return {"status": "candidate_only", "blockers": blockers}
        if completion_rate < float(OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE):
            blockers.append("profile_completion_too_low")
            return {"status": "candidate_only", "blockers": blockers}
        if adverse_rate > float(OPPORTUNITY_MAX_60S_ADVERSE_RATE):
            blockers.append("profile_adverse_too_high")
            return {"status": "candidate_only", "blockers": blockers}
        if followthrough_rate < float(OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE):
            blockers.append("profile_followthrough_too_low")
            return {"status": "candidate_only", "blockers": blockers}
        return {"status": "verified_ready", "blockers": blockers}

    def _reason(
        self,
        *,
        summary: dict[str, Any],
        status: str,
        side: str,
        primary_blocker: str,
        verification_gate: dict[str, Any],
        score: float,
    ) -> str:
        direction_text = "买压" if side == "LONG" else "卖压" if side == "SHORT" else "方向"
        if status == "VERIFIED":
            return (
                f"链上{direction_text}扩散，合约同向确认，机会分 {round(score, 2)}，"
                f"且 60s followthrough / adverse / completion 已达到验证门槛。"
            )
        if status == "CANDIDATE":
            blockers = list(verification_gate.get("blockers") or [])
            if _float(summary.get("non_lp_support_score")) > _float(summary.get("non_lp_risk_score")) and bool(summary.get("non_lp_evidence_available")):
                return "LP 结构已过 candidate gate，且最近 non-LP 同向证据在增强，但 VERIFIED 后验尚未完全打开，先列为候选，不可盲追。"
            if "outcome_history_insufficient" in blockers or "profile_sample_count_insufficient" in blockers:
                return "当前结构接近机会，但 profile 后验样本不足，只能列为候选，不可盲追。"
            if "profile_completion_too_low" in blockers:
                return "当前结构接近机会，但 profile completion 仍偏低，只能列为候选，不可盲追。"
            if "profile_followthrough_too_low" in blockers:
                return "当前结构接近机会，但 profile followthrough 还不稳定，只能列为候选，不可盲追。"
            if "profile_adverse_too_high" in blockers:
                return "当前结构接近机会，但 profile adverse 仍偏高，只能列为候选，不可盲追。"
            return "当前结构接近机会，但后验 followthrough / completion 尚未稳定，先列为候选，不可盲追。"
        if status == "BLOCKED":
            if primary_blocker == "no_trade_lock":
                return "当前存在不交易锁定，冲突尚未解除，不应把它当作交易机会。"
            if primary_blocker == "direction_conflict":
                return "近期双向强信号冲突，方向未统一，追单风险高。"
            if primary_blocker == "data_gap":
                return "缺 live market context，无法把链上结构升级成交易机会。"
            if primary_blocker == "sweep_exhaustion_risk":
                return "当前更像清扫后回吐/反抽风险，而不是干净延续。"
            if primary_blocker == "late_or_chase":
                return "确认节奏已偏晚，当前更像追单风险而不是机会。"
            if primary_blocker == "crowded_basis":
                return "基差或 Mark-Index 已拥挤，风险收益比不适合提示为机会。"
            if primary_blocker == "local_absorption":
                return "局部压力被吸收/承接，不能把 pool-only 结构误写成真实交易机会。"
            if primary_blocker == "low_quality":
                return "当前质量分不足，后验基础不够稳，禁止升级为机会。"
            if primary_blocker == "strong_opposite_signal":
                return "近期已经出现反向强信号，当前不应把它继续升级为机会。"
            if primary_blocker == "non_lp_evidence_conflict":
                return "最近窗口内 non-LP 证据出现强冲突，不能把 LP 候选直接写成交易机会。"
            if primary_blocker == "non_lp_opposite_smart_money":
                return "近期 smart money 已执行反向动作，当前不应把 LP 候选升级成机会。"
            if primary_blocker == "non_lp_maker_against":
                return "maker inventory 明显站在反方向，当前更像被对手盘吸收而不是干净机会。"
            if primary_blocker == "non_lp_exchange_flow_against":
                return "exchange flow 最近偏反向分发/卖压风险，当前不适合继续升级为机会。"
            if primary_blocker == "non_lp_clmm_position_against":
                return "CLMM position 最近在反向撤流/离场，当前不应把它写成机会。"
            if primary_blocker == "non_lp_liquidation_against":
                return "liquidation 风险仍在反方向累积，当前更像风险提示而不是机会。"
            return "当前不是机会，追单风险高。"
        if status == "INVALIDATED":
            return "此前的有效条件已经消失，当前不再保留原判断。"
        if status == "EXPIRED":
            return "原观察窗口已过，且没有延续到足以继续保留。"
        return "当前不满足多源确认、后验准入或风险过滤，保持无机会。"

    def _required_confirmation(
        self,
        *,
        summary: dict[str, Any],
        status: str,
        primary_hard_blocker: str,
        primary_verification_blocker: str,
        verification_gate: dict[str, Any],
    ) -> str:
        direction = str(summary.get("trade_opportunity_side") or "NONE")
        if status == "VERIFIED":
            return "不是自动下单；仅在 broader alignment 仍成立且无反向强信号时继续保留。"
        if status == "CANDIDATE":
            blockers = list(verification_gate.get("blockers") or [])
            if bool(summary.get("non_lp_evidence_available")) and _float(summary.get("non_lp_support_score")) > _float(summary.get("non_lp_risk_score")):
                return "等待后验达标，同时确认 non-LP 同向证据没有转弱或翻反。"
            if "outcome_history_insufficient" in blockers or "profile_sample_count_insufficient" in blockers:
                return "候选 profile 样本补足后，才可评估 VERIFIED。"
            if primary_verification_blocker == "profile_completion_too_low":
                return "等待 profile completion 达标后，再评估 VERIFIED。"
            if primary_verification_blocker == "profile_followthrough_too_low":
                return "等待 profile followthrough 达标后，再评估 VERIFIED。"
            if primary_verification_blocker == "profile_adverse_too_high":
                return "等待 profile adverse 回落后，再评估 VERIFIED。"
            return "候选后验达标 + 无反向强信号 + clean continuation 保留，才可升级为机会。"
        if status == "BLOCKED":
            if primary_hard_blocker == "no_trade_lock":
                return "等待锁定解除 + broader_confirm + quality pass + 120s 无反向强信号。"
            if primary_hard_blocker == "direction_conflict":
                return "等待一方形成更广方向确认并清掉冲突后再评估。"
            if primary_hard_blocker == "data_gap":
                return "等待 live_public 恢复并给出同向 context 后再评估。"
            if primary_hard_blocker == "crowded_basis":
                return "等待 basis / Mark-Index 拥挤回落且方向仍成立后再评估。"
            if primary_hard_blocker == "local_absorption":
                return "等待结构扩散成 broader_confirm，而不是局部承接。"
            if primary_hard_blocker in {
                "non_lp_evidence_conflict",
                "non_lp_opposite_smart_money",
                "non_lp_maker_against",
                "non_lp_exchange_flow_against",
                "non_lp_clmm_position_against",
                "non_lp_liquidation_against",
            }:
                return "等待反向 non-LP 证据消退，并重新出现同向 broader confirm 后再评估。"
            return "等待更干净的 broader_confirm + live context + quality pass 后再评估。"
        if direction == "LONG":
            return "等待更广买压确认和 clean continuation。"
        if direction == "SHORT":
            return "等待更广卖压确认和 clean continuation。"
        return "等待更清晰的单方向结构。"

    def _invalidated_by(self, *, summary: dict[str, Any], status: str, primary_blocker: str) -> str:
        if status in {"VERIFIED", "CANDIDATE"}:
            return _text(
                summary.get("asset_market_state_invalidated_by"),
                summary.get("trade_action_invalidated_by"),
                "broader alignment 消失 / 反向清扫 / Mark-Index 反向",
            )
        if status in {"INVALIDATED", "EXPIRED"}:
            return "机会已失效"
        if primary_blocker == "direction_conflict":
            return "一方形成稳定单边优势"
        return _text(summary.get("trade_action_invalidated_by"), "等待下次独立机会")

    def _source(self, *, summary: dict[str, Any]) -> str:
        if str(summary.get("asset_market_state_key") or ""):
            return "asset_state"
        if str(summary.get("trade_action_key") or ""):
            return "trade_action"
        if str(summary.get("asset_case_supporting_pair_count") or ""):
            return "asset_case"
        return "lp_signal"

    def _time_horizon(self, *, summary: dict[str, Any], status: str) -> str:
        if status in {"BLOCKED", "INVALIDATED", "EXPIRED"}:
            return "30s"
        if str(summary.get("lp_alert_stage") or "") == "prealert" or str(summary.get("lp_sweep_phase") or "") == "sweep_building":
            return "300s"
        return "60s"

    def _confidence(self, *, status: str, score: float, primary_blocker: str) -> str:
        if status == "VERIFIED":
            return "high" if score >= 0.82 else "medium"
        if status == "CANDIDATE":
            return _status_bucket(score)
        if status == "BLOCKED":
            if primary_blocker in {
                "no_trade_lock",
                "direction_conflict",
                "data_gap",
                "sweep_exhaustion_risk",
                "non_lp_evidence_conflict",
                "non_lp_opposite_smart_money",
                "non_lp_maker_against",
                "non_lp_exchange_flow_against",
                "non_lp_clmm_position_against",
                "non_lp_liquidation_against",
            }:
                return "high"
            return "medium"
        return _status_bucket(score)

    def _build_evidence(self, *, summary: dict[str, Any], status: str, quality_floor: float, history: dict[str, Any]) -> list[str]:
        evidence: list[str] = []
        multi_pool = max(_int(summary.get("lp_multi_pool_resonance")), _int(summary.get("asset_case_supporting_pair_count")))
        same_pool = _int(summary.get("lp_same_pool_continuity"))
        if multi_pool >= 2:
            evidence.append(f"跨池{multi_pool}")
        elif same_pool >= 2:
            evidence.append(f"同池连续{same_pool}")
        venue = _major_market_venue(str(summary.get("market_context_venue") or ""))
        if str(summary.get("market_context_source") or "") == "live_public":
            evidence.append(f"{venue or 'Live'} 同向")
        if str(summary.get("lp_confirm_quality") or "") == "clean_confirm":
            evidence.append("clean confirm")
        if str(summary.get("lp_sweep_phase") or "") == "sweep_confirmed" and _float(summary.get("lp_sweep_continuation_score")) >= 0.72:
            evidence.append("强延续")
        if quality_floor >= float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE):
            evidence.append("质量通过")
        support_families = list(summary.get("non_lp_support_families") or [])
        risk_families = list(summary.get("non_lp_risk_families") or [])
        if support_families:
            evidence.append("non-LP支持:" + "/".join(support_families[:2]))
        elif risk_families and status == "BLOCKED":
            evidence.append("non-LP反向:" + "/".join(risk_families[:2]))
        if status == "CANDIDATE":
            evidence.append("等待后验证明")
        if status == "VERIFIED":
            evidence.append(f"history={int(history.get('sample_size') or 0)}")
        return _list_dedup(evidence, limit=5)

    def _primary_blocker(self, blockers: list[str]) -> str:
        for item in BLOCKER_PRIORITY:
            if item in blockers:
                return item
        return str(blockers[0]) if blockers else ""

    def _key(self, *, summary: dict[str, Any], side: str, status: str, primary_blocker: str) -> str:
        if status == "BLOCKED":
            detail = f"{primary_blocker or 'blocked'}:{str(summary.get('opportunity_profile_key') or '')}"
        else:
            detail = str(summary.get("opportunity_profile_key") or "") or ":".join(
                [
                    str(summary.get("lp_confirm_scope") or ""),
                    str(summary.get("lp_confirm_quality") or ""),
                    str(summary.get("lp_sweep_phase") or summary.get("lp_alert_stage") or ""),
                    str(max(_int(summary.get("lp_multi_pool_resonance")), _int(summary.get("asset_case_supporting_pair_count")))),
                ]
            )
        digest = hashlib.sha1(detail.encode("utf-8")).hexdigest()[:10]
        return f"{str(summary.get('asset_symbol') or '')}:{side}:{status}:{digest}"

    def _id(self, *, opportunity_key: str, signal_id: str, event_ts: int) -> str:
        seed = f"{opportunity_key}:{signal_id}:{event_ts}"
        return f"opp_{hashlib.sha1(seed.encode('utf-8')).hexdigest()[:16]}"

    def _register_opportunity(
        self,
        *,
        summary: dict[str, Any],
        history: dict[str, Any],
        evaluation: dict[str, Any],
        now_ts: int,
    ) -> dict[str, Any]:
        opportunity_id = str(evaluation.get("trade_opportunity_id") or "")
        existing = self._opportunity_index.get(opportunity_id)
        if existing is None:
            record = dict(evaluation)
            record["created_at"] = int(now_ts)
            record["signal_id"] = str(summary.get("signal_id") or "")
            record["event_id"] = str(summary.get("event_id") or "")
            record["trade_opportunity_history_snapshot"] = dict(history)
            self._register_profile_creation(record=record, now_ts=now_ts)
            self._apply_outcome_snapshot(record=record)
            self._opportunities.append(record)
            self._opportunity_index[opportunity_id] = record
            return record
        existing.update(evaluation)
        existing["trade_opportunity_history_snapshot"] = dict(history)
        self._register_profile_creation(record=existing, now_ts=now_ts)
        self._apply_outcome_snapshot(record=existing)
        return existing

    def _register_profile_creation(self, *, record: dict[str, Any], now_ts: int) -> None:
        entry = self._profile_stat_entry(record)
        if not entry or _bool(record.get("opportunity_profile_registered")):
            return
        created_status = str(record.get("trade_opportunity_status_at_creation") or record.get("trade_opportunity_status") or "NONE")
        if created_status in {"CANDIDATE", "VERIFIED"}:
            entry["sample_count"] = int(entry.get("sample_count") or 0) + 1
        if created_status == "CANDIDATE":
            entry["candidate_count"] = int(entry.get("candidate_count") or 0) + 1
        elif created_status == "VERIFIED":
            entry["verified_count"] = int(entry.get("verified_count") or 0) + 1
        elif created_status == "BLOCKED":
            entry["blocked_count"] = int(entry.get("blocked_count") or 0) + 1
        entry["last_updated"] = int(now_ts)
        record["opportunity_profile_registered"] = True
        self._mark_dirty()

    def _update_blocker_effectiveness_flags(self, *, record: dict[str, Any]) -> None:
        if str(record.get("trade_opportunity_status_at_creation") or record.get("trade_opportunity_status") or "") != "BLOCKED":
            return
        record["blocker_type"] = str(record.get("trade_opportunity_primary_hard_blocker") or record.get("trade_opportunity_primary_blocker") or "")
        record["would_have_been_direction"] = str(record.get("trade_opportunity_side") or "NONE")
        record["blocker_effectiveness_window_sec"] = 60
        for window_sec in _OUTCOME_WINDOWS:
            key = _window_key(window_sec)
            status = str(record.get(f"opportunity_outcome_{key}") or "")
            adverse = record.get(f"opportunity_adverse_{key}")
            followthrough = record.get(f"opportunity_followthrough_{key}")
            adjusted_move = record.get(f"direction_adjusted_move_after_{key}")
            saved_trade = None
            false_block = None
            if status == "completed":
                saved_trade = adverse is True
                favorable = followthrough is True
                if adjusted_move is not None:
                    try:
                        favorable = favorable or float(adjusted_move) > _FOLLOWTHROUGH_MIN_MOVE
                    except (TypeError, ValueError):
                        pass
                false_block = (adverse is False) and bool(favorable)
            record[f"adverse_after_block_{key}"] = adverse if status == "completed" else None
            record[f"blocker_saved_trade_{key}"] = saved_trade
            record[f"blocker_false_block_possible_{key}"] = false_block
        record["adverse_after_block"] = record.get("adverse_after_block_60s")
        record["blocker_saved_trade"] = record.get("blocker_saved_trade_60s")
        record["blocker_false_block_possible"] = record.get("blocker_false_block_possible_60s")

    def _accumulate_profile_outcomes(self, *, record: dict[str, Any]) -> None:
        entry = self._profile_stat_entry(record)
        if not entry:
            return
        status_at_creation = str(record.get("trade_opportunity_status_at_creation") or record.get("trade_opportunity_status") or "NONE")
        accumulated = dict(record.get("opportunity_profile_accumulated_windows") or {})
        updated = False
        for window_sec in _OUTCOME_WINDOWS:
            key = _window_key(window_sec)
            status = str(record.get(f"opportunity_outcome_{key}") or "")
            if status not in {"completed", "unavailable", "expired"}:
                continue
            if str(accumulated.get(key) or "") == status:
                continue
            if status_at_creation in {"CANDIDATE", "VERIFIED"} and status == "completed":
                entry[f"completed_{key}"] = int(entry.get(f"completed_{key}") or 0) + 1
                if record.get(f"opportunity_followthrough_{key}") is True:
                    entry[f"followthrough_{key}_count"] = int(entry.get(f"followthrough_{key}_count") or 0) + 1
                if record.get(f"opportunity_adverse_{key}") is True:
                    entry[f"adverse_{key}_count"] = int(entry.get(f"adverse_{key}_count") or 0) + 1
            if status_at_creation == "BLOCKED" and status == "completed":
                entry[f"blocked_completed_{key}"] = int(entry.get(f"blocked_completed_{key}") or 0) + 1
                if record.get(f"blocker_saved_trade_{key}") is True:
                    entry[f"blocker_saved_trade_{key}_count"] = int(entry.get(f"blocker_saved_trade_{key}_count") or 0) + 1
                if record.get(f"blocker_false_block_possible_{key}") is True:
                    entry[f"blocker_false_block_{key}_count"] = int(entry.get(f"blocker_false_block_{key}_count") or 0) + 1
            accumulated[key] = status
            updated = True
        if updated:
            entry["last_updated"] = int(time.time())
            record["opportunity_profile_accumulated_windows"] = accumulated
            self._mark_dirty()

    def _apply_outcome_snapshot(self, *, record: dict[str, Any]) -> None:
        record_id = str(record.get("outcome_tracking_key") or "")
        outcome_row = {}
        if record_id and self.state_manager is not None and hasattr(self.state_manager, "get_lp_outcome_record"):
            outcome_row = dict(self.state_manager.get_lp_outcome_record(record_id) or {})
        if not outcome_row:
            return
        windows = outcome_row.get("outcome_windows") if isinstance(outcome_row.get("outcome_windows"), dict) else {}
        record["opportunity_outcome_source"] = str(outcome_row.get("outcome_price_source") or record.get("opportunity_outcome_source") or "unavailable")
        for window_sec in _OUTCOME_WINDOWS:
            key = _window_key(window_sec)
            window = dict(windows.get(key) or {})
            status = str(window.get("status") or "")
            followthrough = window.get("followthrough_positive")
            adverse = window.get("adverse_by_direction")
            record[f"opportunity_outcome_{key}"] = status or record.get(f"opportunity_outcome_{key}") or "pending"
            record[f"opportunity_followthrough_{key}"] = followthrough if followthrough in {True, False} else record.get(f"opportunity_followthrough_{key}")
            record[f"opportunity_adverse_{key}"] = adverse if adverse in {True, False} else record.get(f"opportunity_adverse_{key}")
            record[f"raw_move_after_{key}"] = window.get("raw_move_after", record.get(f"raw_move_after_{key}"))
            record[f"direction_adjusted_move_after_{key}"] = window.get(
                "direction_adjusted_move_after",
                record.get(f"direction_adjusted_move_after_{key}"),
            )
            record[f"due_at_{key}"] = window.get("due_at", record.get(f"due_at_{key}"))
            record[f"outcome_price_start_{key}"] = window.get("price_start", record.get(f"outcome_price_start_{key}"))
            record[f"outcome_price_end_{key}"] = window.get("price_end", record.get(f"outcome_price_end_{key}"))
            record[f"start_price_source_{key}"] = window.get("start_price_source", record.get(f"start_price_source_{key}"))
            record[f"end_price_source_{key}"] = window.get("end_price_source", record.get(f"end_price_source_{key}"))
            record[f"outcome_price_source_{key}"] = window.get("outcome_price_source") or window.get("price_source") or record.get(f"outcome_price_source_{key}")
            record[f"outcome_failure_reason_{key}"] = window.get("failure_reason", record.get(f"outcome_failure_reason_{key}"))
            record[f"outcome_completed_at_{key}"] = window.get("completed_at", record.get(f"outcome_completed_at_{key}"))
            record[f"opportunity_reversal_{key}"] = window.get("reversal", record.get(f"opportunity_reversal_{key}"))
            record["settled_by"] = window.get("settled_by", record.get("settled_by"))
            record["catchup"] = window.get("catchup", record.get("catchup"))
        sixty_status = str(record.get("opportunity_outcome_60s") or "")
        record["opportunity_result_label"] = _result_label_from_outcome(
            sixty_status,
            record.get("opportunity_followthrough_60s"),
            record.get("opportunity_adverse_60s"),
        )
        self._update_blocker_effectiveness_flags(record=record)
        self._accumulate_profile_outcomes(record=record)

    def _refresh_recent_outcomes(self) -> None:
        pending = [
            row
            for row in self._opportunities[-min(self.history_limit, 200) :]
            if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED", "BLOCKED"}
        ]
        for row in pending:
            self._apply_outcome_snapshot(record=row)
            self._mirror_sqlite_opportunity(row)

    def _profile_stat_payload(self, profile_key: str) -> dict[str, Any]:
        row = dict(self._profile_stats.get(str(profile_key) or "") or {})
        if not row:
            return {}
        sample_count = int(row.get("sample_count") or 0)
        completed_30s = int(row.get("completed_30s") or 0)
        completed_60s = int(row.get("completed_60s") or 0)
        completed_300s = int(row.get("completed_300s") or 0)
        blocked_completed_60s = int(row.get("blocked_completed_60s") or 0)
        stats_json = {
            "profile_key": str(row.get("profile_key") or ""),
            "profile_version": str(row.get("profile_version") or _PROFILE_VERSION),
            "sample_count": sample_count,
            "candidate_count": int(row.get("candidate_count") or 0),
            "verified_count": int(row.get("verified_count") or 0),
            "blocked_count": int(row.get("blocked_count") or 0),
            "completed_30s": completed_30s,
            "completed_60s": completed_60s,
            "completed_300s": completed_300s,
            "followthrough_30s_count": int(row.get("followthrough_30s_count") or 0),
            "followthrough_60s_count": int(row.get("followthrough_60s_count") or 0),
            "followthrough_300s_count": int(row.get("followthrough_300s_count") or 0),
            "adverse_30s_count": int(row.get("adverse_30s_count") or 0),
            "adverse_60s_count": int(row.get("adverse_60s_count") or 0),
            "adverse_300s_count": int(row.get("adverse_300s_count") or 0),
            "blocked_completed_30s": int(row.get("blocked_completed_30s") or 0),
            "blocked_completed_60s": blocked_completed_60s,
            "blocked_completed_300s": int(row.get("blocked_completed_300s") or 0),
            "blocker_saved_trade_30s_count": int(row.get("blocker_saved_trade_30s_count") or 0),
            "blocker_saved_trade_60s_count": int(row.get("blocker_saved_trade_60s_count") or 0),
            "blocker_saved_trade_300s_count": int(row.get("blocker_saved_trade_300s_count") or 0),
            "blocker_false_block_30s_count": int(row.get("blocker_false_block_30s_count") or 0),
            "blocker_false_block_60s_count": int(row.get("blocker_false_block_60s_count") or 0),
            "blocker_false_block_300s_count": int(row.get("blocker_false_block_300s_count") or 0),
            "completion_30s_rate": _rate(completed_30s, sample_count),
            "completion_60s_rate": _rate(completed_60s, sample_count),
            "completion_300s_rate": _rate(completed_300s, sample_count),
            "followthrough_30s_rate": _rate(int(row.get("followthrough_30s_count") or 0), completed_30s),
            "followthrough_60s_rate": _rate(int(row.get("followthrough_60s_count") or 0), completed_60s),
            "followthrough_300s_rate": _rate(int(row.get("followthrough_300s_count") or 0), completed_300s),
            "adverse_30s_rate": _rate(int(row.get("adverse_30s_count") or 0), completed_30s) if completed_30s else 1.0,
            "adverse_60s_rate": _rate(int(row.get("adverse_60s_count") or 0), completed_60s) if completed_60s else 1.0,
            "adverse_300s_rate": _rate(int(row.get("adverse_300s_count") or 0), completed_300s) if completed_300s else 1.0,
            "blocker_saved_rate": _rate(int(row.get("blocker_saved_trade_60s_count") or 0), blocked_completed_60s),
            "blocker_false_block_rate": _rate(int(row.get("blocker_false_block_60s_count") or 0), blocked_completed_60s),
            "blocker_avoided_adverse_rate": _rate(int(row.get("blocker_saved_trade_60s_count") or 0), blocked_completed_60s),
            "last_updated": int(row.get("last_updated") or 0),
            "features_json": dict(row.get("features_json") or {}),
            "outcome_source_distribution_60s": {},
        }
        return {
            "scope_type": "opportunity_profile",
            "scope_key": str(row.get("profile_key") or ""),
            "asset": str(row.get("asset") or ""),
            "pair": str(row.get("pair_family") or ""),
            "stage": "all",
            "sample_count": sample_count,
            "candidate_followthrough_rate": stats_json["followthrough_60s_rate"],
            "candidate_adverse_rate": stats_json["adverse_60s_rate"],
            "verified_followthrough_rate": stats_json["followthrough_60s_rate"],
            "verified_adverse_rate": stats_json["adverse_60s_rate"],
            "outcome_completion_rate": stats_json["completion_60s_rate"],
            "updated_at": stats_json["last_updated"],
            "stats_json": stats_json,
        }

    def _mirror_sqlite_opportunity(self, payload: dict[str, Any]) -> None:
        if not payload:
            return
        try:
            import sqlite_store

            sqlite_store.upsert_trade_opportunity(payload)
            profile_payload = self._profile_stat_payload(str(payload.get("opportunity_profile_key") or ""))
            if profile_payload:
                sqlite_store.upsert_quality_stat(profile_payload)
            calibration_rows = build_calibration_quality_stats(
                list(self._profile_stats.values()),
                profile_key=str(payload.get("opportunity_profile_key") or ""),
                asset=str(payload.get("opportunity_profile_asset") or payload.get("asset_symbol") or ""),
                side=str(payload.get("opportunity_profile_side") or payload.get("trade_opportunity_side") or "NONE"),
                pair_family=str(payload.get("opportunity_profile_pair_family") or ""),
                updated_at=int(time.time()),
            )
            for row in calibration_rows:
                sqlite_store.upsert_quality_stat(row)
        except Exception as exc:
            print(f"sqlite opportunity mirror failed: {exc}")

    def _invalidate_active(self, record: dict[str, Any], *, now_ts: int, reason: str) -> None:
        previous_status = str(record.get("trade_opportunity_status") or "")
        if previous_status not in {"CANDIDATE", "VERIFIED"}:
            return
        record["opportunity_invalidated_at"] = int(now_ts)
        record["opportunity_invalidated_reason"] = str(reason or "alignment_lost")
        record["trade_opportunity_status"] = "INVALIDATED"
        asset_symbol = str(record.get("asset_symbol") or "").upper()
        asset_state = self._asset_states.get(asset_symbol)
        if asset_state is not None and previous_status == "VERIFIED":
            asset_state.opportunity_cooldown_until = max(int(asset_state.opportunity_cooldown_until or 0), now_ts + int(OPPORTUNITY_COOLDOWN_SEC))

    def _build_state_change(self, previous: dict[str, Any], *, now_ts: int) -> dict[str, Any]:
        status = "EXPIRED" if now_ts >= _int(previous.get("trade_opportunity_expires_at"), default=0) > 0 else "INVALIDATED"
        side = str(previous.get("trade_opportunity_side") or "NONE")
        opportunity_key = f"{str(previous.get('trade_opportunity_key') or '')}:{status.lower()}"
        return {
            "trade_opportunity_id": self._id(opportunity_key=opportunity_key, signal_id=str(previous.get("signal_id") or ""), event_ts=now_ts),
            "trade_opportunity_key": opportunity_key,
            "trade_opportunity_side": side,
            "trade_opportunity_status": status,
            "trade_opportunity_label": _label_for(status, side),
            "trade_opportunity_score": float(previous.get("trade_opportunity_score") or 0.0),
            "trade_opportunity_raw_score": float(previous.get("trade_opportunity_raw_score") or previous.get("opportunity_raw_score") or previous.get("trade_opportunity_score") or 0.0),
            "trade_opportunity_calibrated_score": float(previous.get("trade_opportunity_calibrated_score") or previous.get("opportunity_calibrated_score") or previous.get("trade_opportunity_score") or 0.0),
            "trade_opportunity_score_without_non_lp": float(previous.get("trade_opportunity_score_without_non_lp") or 0.0),
            "trade_opportunity_non_lp_score_delta": float(previous.get("trade_opportunity_non_lp_score_delta") or 0.0),
            "trade_opportunity_non_lp_component_score": float(previous.get("trade_opportunity_non_lp_component_score") or _NON_LP_SUPPORT_BASELINE),
            "opportunity_raw_score": float(previous.get("opportunity_raw_score") or previous.get("trade_opportunity_raw_score") or previous.get("trade_opportunity_score") or 0.0),
            "opportunity_calibrated_score": float(previous.get("opportunity_calibrated_score") or previous.get("trade_opportunity_calibrated_score") or previous.get("trade_opportunity_score") or 0.0),
            "opportunity_calibration_adjustment": float(previous.get("opportunity_calibration_adjustment") or 0.0),
            "opportunity_calibration_reason": str(previous.get("opportunity_calibration_reason") or ""),
            "opportunity_calibration_sample_count": int(previous.get("opportunity_calibration_sample_count") or 0),
            "opportunity_calibration_confidence": float(previous.get("opportunity_calibration_confidence") or 0.0),
            "opportunity_calibration_source": str(previous.get("opportunity_calibration_source") or "none"),
            "trade_opportunity_confidence": "medium",
            "trade_opportunity_time_horizon": "30s",
            "trade_opportunity_reason": "此前的有效条件已经消失，当前不再保留原判断。",
            "trade_opportunity_evidence": list(previous.get("trade_opportunity_evidence") or []),
            "trade_opportunity_blockers": ["alignment_lost"],
            "trade_opportunity_hard_blockers": [],
            "trade_opportunity_verification_blockers": [],
            "trade_opportunity_required_confirmation": "等待下一次独立有效条件重新建立。",
            "trade_opportunity_invalidated_by": "broader alignment 消失 / 反向结构出现",
            "trade_opportunity_risk_flags": ["alignment_lost"],
            "trade_opportunity_primary_hard_blocker": "",
            "trade_opportunity_primary_verification_blocker": "",
            "trade_opportunity_quality_snapshot": dict(previous.get("trade_opportunity_quality_snapshot") or {}),
            "trade_opportunity_outcome_policy": dict(previous.get("trade_opportunity_outcome_policy") or {}),
            "trade_opportunity_calibration_snapshot": dict(previous.get("trade_opportunity_calibration_snapshot") or {}),
            "trade_opportunity_created_at": int(now_ts),
            "trade_opportunity_expires_at": int(now_ts + 30),
            "trade_opportunity_source": str(previous.get("trade_opportunity_source") or "asset_state"),
            "trade_opportunity_score_components": dict(previous.get("trade_opportunity_score_components") or {}),
            "trade_opportunity_history_snapshot": dict(previous.get("trade_opportunity_history_snapshot") or {}),
            "trade_opportunity_non_lp_evidence_context": dict(previous.get("trade_opportunity_non_lp_evidence_context") or {}),
            "trade_opportunity_non_lp_evidence_summary": str(previous.get("trade_opportunity_non_lp_evidence_summary") or ""),
            "trade_opportunity_primary_blocker": "alignment_lost",
            "trade_opportunity_status_at_creation": status,
            "asset_symbol": str(previous.get("asset_symbol") or ""),
            "pair_label": str(previous.get("pair_label") or ""),
            "opportunity_profile_key": str(previous.get("opportunity_profile_key") or ""),
            "opportunity_profile_version": str(previous.get("opportunity_profile_version") or _PROFILE_VERSION),
            "opportunity_profile_side": str(previous.get("opportunity_profile_side") or side),
            "opportunity_profile_asset": str(previous.get("opportunity_profile_asset") or previous.get("asset_symbol") or ""),
            "opportunity_profile_pair_family": str(previous.get("opportunity_profile_pair_family") or ""),
            "opportunity_profile_strategy": str(previous.get("opportunity_profile_strategy") or ""),
            "opportunity_profile_features_json": dict(previous.get("opportunity_profile_features_json") or {}),
            "outcome_tracking_key": str(previous.get("outcome_tracking_key") or ""),
            "opportunity_outcome_source": str(previous.get("opportunity_outcome_source") or "unavailable"),
            "opportunity_outcome_30s": str(previous.get("opportunity_outcome_30s") or "pending"),
            "opportunity_outcome_60s": str(previous.get("opportunity_outcome_60s") or "pending"),
            "opportunity_outcome_300s": str(previous.get("opportunity_outcome_300s") or "pending"),
            "opportunity_followthrough_30s": previous.get("opportunity_followthrough_30s"),
            "opportunity_followthrough_60s": previous.get("opportunity_followthrough_60s"),
            "opportunity_followthrough_300s": previous.get("opportunity_followthrough_300s"),
            "opportunity_adverse_30s": previous.get("opportunity_adverse_30s"),
            "opportunity_adverse_60s": previous.get("opportunity_adverse_60s"),
            "opportunity_adverse_300s": previous.get("opportunity_adverse_300s"),
            "opportunity_invalidated_at": int(now_ts),
            "opportunity_invalidated_reason": "alignment_lost",
            "opportunity_result_label": str(previous.get("opportunity_result_label") or "neutral"),
            "trade_opportunity_notifier_sent_at": None,
            "trade_opportunity_delivered_notification": False,
            "blocker_type": "",
            "would_have_been_direction": str(previous.get("trade_opportunity_side") or side),
            "blocker_effectiveness_window_sec": 60,
            "adverse_after_block": previous.get("adverse_after_block"),
            "blocker_saved_trade": previous.get("blocker_saved_trade"),
            "blocker_false_block_possible": previous.get("blocker_false_block_possible"),
        }

    def _telegram_decision(
        self,
        *,
        record: dict[str, Any],
        asset_state: TradeOpportunityAssetState,
        previous: dict[str, Any] | None,
        now_ts: int,
    ) -> dict[str, Any]:
        status = str(record.get("trade_opportunity_status") or "NONE")
        suppression_reason = ""
        update_kind = "suppressed"
        should_send = False
        state_change_reason = ""
        if status == "VERIFIED":
            update_kind = "opportunity"
            state_change_reason = f"{str(previous.get('trade_opportunity_status') or 'none') if previous else 'none'}->{status}"
            if now_ts < int(asset_state.opportunity_cooldown_until or 0):
                suppression_reason = "opportunity_cooldown_active"
            elif self._sent_count_this_hour(asset_state, now_ts) >= int(OPPORTUNITY_MAX_PER_ASSET_PER_HOUR):
                suppression_reason = "opportunity_budget_exhausted"
            elif asset_state.last_sent_key == str(record.get("trade_opportunity_key") or "") and asset_state.last_sent_status == status:
                suppression_reason = "same_opportunity_repeat"
            else:
                should_send = True
        elif status == "CANDIDATE":
            update_kind = "candidate"
            state_change_reason = f"{str(previous.get('trade_opportunity_status') or 'none') if previous else 'none'}->{status}"
            if now_ts < int(asset_state.opportunity_cooldown_until or 0):
                suppression_reason = "opportunity_cooldown_active"
            elif self._sent_count_this_hour(asset_state, now_ts) >= int(OPPORTUNITY_MAX_PER_ASSET_PER_HOUR):
                suppression_reason = "opportunity_budget_exhausted"
            elif asset_state.last_sent_key == str(record.get("trade_opportunity_key") or "") and asset_state.last_sent_status == status:
                suppression_reason = "same_opportunity_repeat"
            elif (
                asset_state.last_sent_status == "CANDIDATE"
                and now_ts - int(asset_state.last_candidate_sent_at or 0) < int(OPPORTUNITY_REPEAT_CANDIDATE_SUPPRESS_SEC)
            ):
                suppression_reason = "candidate_refresh_within_hold_window"
            else:
                should_send = True
        elif status == "BLOCKED":
            update_kind = "risk_blocker"
            state_change_reason = f"{str(previous.get('trade_opportunity_status') or 'none') if previous else 'none'}->{status}"
            if (
                asset_state.last_sent_key == str(record.get("trade_opportunity_key") or "")
                and now_ts - int(asset_state.last_sent_at or 0) < int(TELEGRAM_SUPPRESS_REPEAT_STATE_SEC)
            ):
                suppression_reason = "same_blocker_repeat"
            else:
                should_send = True
        elif status in OPPORTUNITY_TERMINAL_STATUSES:
            update_kind = "state_change"
            state_change_reason = f"{str(previous.get('trade_opportunity_status') or 'none') if previous else 'none'}->{status}"
            if str(previous.get("trade_opportunity_status") or "") != "VERIFIED":
                suppression_reason = "non_verified_opportunity_state_change_suppressed"
            elif (
                asset_state.last_sent_key == str(record.get("trade_opportunity_key") or "")
                and now_ts - int(asset_state.last_sent_at or 0) < int(TELEGRAM_SUPPRESS_REPEAT_STATE_SEC)
            ):
                suppression_reason = "same_opportunity_repeat"
            else:
                should_send = True
        else:
            suppression_reason = "no_trade_opportunity"

        return {
            "telegram_should_send": bool(should_send),
            "telegram_suppression_reason": suppression_reason,
            "telegram_update_kind": update_kind if should_send else "suppressed",
            "telegram_state_change_reason": state_change_reason,
            "trade_opportunity_should_send": bool(should_send),
            "trade_opportunity_suppression_reason": suppression_reason,
            "trade_opportunity_update_kind": update_kind if should_send else "suppressed",
        }

    def _final_trading_output(
        self,
        *,
        summary: dict[str, Any],
        record: dict[str, Any],
    ) -> dict[str, Any]:
        status = str(record.get("trade_opportunity_status") or "NONE")
        side = str(record.get("trade_opportunity_side") or "NONE")
        primary_blocker = str(record.get("trade_opportunity_primary_blocker") or "")
        trade_action_key = str(summary.get("trade_action_key") or "")
        trade_action_label = str(summary.get("trade_action_label") or "")
        asset_state_key = str(summary.get("asset_market_state_key") or "")
        asset_state_label = str(summary.get("asset_market_state_label") or "")
        asset_state_should_send = bool(summary.get("asset_state_telegram_should_send"))
        asset_state_update_kind = str(summary.get("asset_state_telegram_update_kind") or "")
        asset_state_change_reason = str(summary.get("asset_state_telegram_state_change_reason") or "")
        chase_allowed = trade_action_key in LEGACY_CHASE_ACTION_KEYS
        opportunity_gate_required = bool(
            chase_allowed
            or status in ACTIVE_STATUSES
            or asset_state_key in ASSET_STATE_OPPORTUNITY_KEYS
        )
        final_source = "suppressed"
        final_label = ""
        final_allowed = False
        final_update_kind = "suppressed"
        final_suppression_reason = str(
            record.get("trade_opportunity_suppression_reason")
            or record.get("telegram_suppression_reason")
            or ""
        )
        final_state_change_reason = str(record.get("telegram_state_change_reason") or "")
        opportunity_gate_failure_reason = ""

        if status == "VERIFIED":
            final_source = "trade_opportunity"
            final_label = "多头机会" if side == "LONG" else "空头机会"
            final_allowed = bool(record.get("trade_opportunity_should_send"))
            final_update_kind = str(record.get("trade_opportunity_update_kind") or "opportunity")
        elif status == "CANDIDATE":
            final_source = "trade_opportunity"
            final_label = "多头候选" if side == "LONG" else "空头候选"
            final_allowed = bool(record.get("trade_opportunity_should_send"))
            final_update_kind = str(record.get("trade_opportunity_update_kind") or "candidate")
        elif status == "BLOCKED":
            final_source = "trade_opportunity"
            final_label = _blocked_output_label(side, primary_blocker)
            final_allowed = bool(record.get("trade_opportunity_should_send"))
            final_update_kind = str(record.get("trade_opportunity_update_kind") or "risk_blocker")
        elif status in OPPORTUNITY_TERMINAL_STATUSES:
            if bool(record.get("trade_opportunity_should_send")):
                final_source = "trade_opportunity"
                final_label = "状态变化"
                final_allowed = True
                final_update_kind = "state_change"
            else:
                final_suppression_reason = final_suppression_reason or "no_trade_opportunity"
        elif chase_allowed:
            final_suppression_reason = final_suppression_reason or "no_trade_opportunity"
            opportunity_gate_failure_reason = "no_trade_opportunity"
        elif asset_state_should_send and asset_state_key not in ASSET_STATE_OPPORTUNITY_KEYS:
            final_source = "asset_market_state"
            final_label = asset_state_label or "状态变化"
            final_allowed = True
            final_update_kind = asset_state_update_kind or "state_change"
            final_state_change_reason = asset_state_change_reason
            final_suppression_reason = ""
        elif asset_state_key in ASSET_STATE_OPPORTUNITY_KEYS:
            final_suppression_reason = final_suppression_reason or "asset_market_state_candidate_without_opportunity"
            opportunity_gate_failure_reason = "asset_market_state_candidate_without_opportunity"
        elif trade_action_key and not chase_allowed:
            final_source = "trade_action_legacy"
            final_label = trade_action_label or trade_action_key
            final_allowed = False
            final_update_kind = "suppressed"
            final_suppression_reason = final_suppression_reason or "trade_action_informational_only"
        else:
            final_suppression_reason = final_suppression_reason or "no_trade_opportunity"

        base_opportunity_gate_passed = bool(
            not opportunity_gate_required
            or (
                final_source == "trade_opportunity"
                and status in ACTIVE_STATUSES
            )
            or (
                final_source == "trade_opportunity"
                and status in OPPORTUNITY_TERMINAL_STATUSES
                and final_label == "状态变化"
            )
            or (
                final_source == "asset_market_state"
                and asset_state_key not in ASSET_STATE_OPPORTUNITY_KEYS
                and final_label not in {"可顺势追多", "可顺势追空", "偏多候选", "偏空候选"}
            )
        )
        label_gate_passed, label_gate_failure_reason = validate_final_trading_output_gate(
            trade_opportunity_status=status,
            final_trading_output_source=final_source,
            final_trading_output_label=final_label,
            opportunity_gate_passed=base_opportunity_gate_passed,
            asset_market_state_key=asset_state_key,
            delivered_to_trader=bool(final_allowed),
        )
        opportunity_gate_passed = bool(base_opportunity_gate_passed and label_gate_passed)
        if opportunity_gate_required and not opportunity_gate_passed and not opportunity_gate_failure_reason:
            opportunity_gate_failure_reason = label_gate_failure_reason or final_suppression_reason or "opportunity_gate_rejected"
        elif not opportunity_gate_required and not label_gate_passed and not opportunity_gate_failure_reason:
            opportunity_gate_failure_reason = label_gate_failure_reason
        if bool(final_allowed) and not label_gate_passed:
            final_allowed = False
            final_suppression_reason = label_gate_failure_reason or final_suppression_reason or "opportunity_gate_rejected"
            final_update_kind = "suppressed"
        if opportunity_gate_required and not opportunity_gate_passed and not opportunity_gate_failure_reason:
            opportunity_gate_failure_reason = final_suppression_reason or "opportunity_gate_rejected"

        legacy_chase_downgraded = bool(chase_allowed and not (status == "VERIFIED" and final_source == "trade_opportunity"))
        legacy_chase_downgrade_reason = ""
        if legacy_chase_downgraded:
            if status == "CANDIDATE":
                legacy_chase_downgrade_reason = "candidate_only_not_verified"
            elif status == "BLOCKED":
                legacy_chase_downgrade_reason = f"blocked:{primary_blocker or 'blocked'}"
            elif status == "EXPIRED":
                legacy_chase_downgrade_reason = "trade_opportunity_expired"
            elif status == "INVALIDATED":
                legacy_chase_downgrade_reason = "trade_opportunity_invalidated"
            else:
                legacy_chase_downgrade_reason = opportunity_gate_failure_reason or "no_trade_opportunity"

        return {
            "final_trading_output_source": final_source,
            "final_trading_output_label": final_label,
            "final_trading_output_allowed": bool(final_allowed),
            "legacy_chase_downgraded": bool(legacy_chase_downgraded),
            "legacy_chase_downgrade_reason": legacy_chase_downgrade_reason,
            "opportunity_gate_required": bool(opportunity_gate_required),
            "opportunity_gate_passed": bool(opportunity_gate_passed),
            "opportunity_gate_failure_reason": opportunity_gate_failure_reason,
            "telegram_should_send": bool(final_allowed),
            "telegram_suppression_reason": "" if final_allowed else final_suppression_reason,
            "telegram_update_kind": final_update_kind if final_allowed else "suppressed",
            "telegram_state_change_reason": final_state_change_reason,
            "headline_label": final_label or str(summary.get("headline_label") or ""),
            "market_state_label": final_label or str(summary.get("market_state_label") or ""),
        }

    def _append_recent_direction_signal(self, *, summary: dict[str, Any]) -> None:
        asset = str(summary.get("asset_symbol") or "").upper()
        direction = str(summary.get("direction") or "neutral")
        if not asset or direction not in {"long", "short"}:
            return
        strong = bool(
            _float(summary.get("strength_score")) >= _STRONG_SIGNAL_MIN
            or str(summary.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED", "DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}
            or str(summary.get("lp_confirm_scope") or "") in {"local_confirm", "broader_confirm"}
        )
        self._recent_direction_signals[asset].append(
            {
                "ts": _int(summary.get("event_ts"), default=int(time.time())),
                "direction": direction,
                "strong": strong,
                "trade_action_key": str(summary.get("trade_action_key") or ""),
            }
        )

    def _has_recent_opposite_signal(self, *, summary: dict[str, Any]) -> bool:
        asset = str(summary.get("asset_symbol") or "").upper()
        direction = str(summary.get("direction") or "neutral")
        if asset == "" or direction not in {"long", "short"}:
            return False
        queue = self._recent_direction_signals.get(asset)
        if queue is None:
            return False
        now_ts = _int(summary.get("event_ts"), default=int(time.time()))
        while queue and now_ts - int(queue[0].get("ts") or 0) > int(OPPORTUNITY_RECENT_OPPOSITE_SIGNAL_WINDOW_SEC):
            queue.popleft()
        for item in queue:
            if not bool(item.get("strong")):
                continue
            item_direction = str(item.get("direction") or "")
            if item_direction and item_direction != direction:
                return True
        return False

    def _hour_bucket(self, ts: int) -> str:
        return time.strftime("%Y%m%d%H", time.gmtime(int(ts)))

    def _prune_hour_buckets(self, asset_state: TradeOpportunityAssetState, *, now_ts: int) -> None:
        current_hour = int(self._hour_bucket(now_ts))
        pruned: dict[str, int] = {}
        for key, value in dict(asset_state.opportunity_sent_count_by_hour or {}).items():
            try:
                if current_hour - int(key) <= 2:
                    pruned[str(key)] = int(value)
            except ValueError:
                continue
        asset_state.opportunity_sent_count_by_hour = pruned

    def _sent_count_this_hour(self, asset_state: TradeOpportunityAssetState, now_ts: int) -> int:
        self._prune_hour_buckets(asset_state, now_ts=now_ts)
        return int(asset_state.opportunity_sent_count_by_hour.get(self._hour_bucket(now_ts)) or 0)

    def _trim_history(self) -> None:
        if len(self._opportunities) <= self.history_limit:
            return
        overflow = len(self._opportunities) - self.history_limit
        removed = self._opportunities[:overflow]
        self._opportunities = self._opportunities[overflow:]
        for item in removed:
            self._opportunity_index.pop(str(item.get("trade_opportunity_id") or ""), None)

    def _status_history(self, status: str) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self._opportunities
            if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") == status
        ]
        return rows[-min(len(rows), 300) :]

    def _rolling_stats(self) -> dict[str, Any]:
        rows = list(self._opportunities[-self.history_limit :])
        status_counter = Counter(str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "NONE") for row in rows)
        score_values = [float(row.get("trade_opportunity_score") or 0.0) for row in rows if row.get("trade_opportunity_score") not in {None, ""}]
        candidate_rows = [row for row in rows if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") == "CANDIDATE"]
        verified_rows = [row for row in rows if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") == "VERIFIED"]
        blocker_rows = [row for row in rows if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") == "BLOCKED"]
        non_lp_supported_rows = [
            row
            for row in rows
            if not bool((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_strong_conflict"))
            if _float((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_support_score"))
            > _float((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_risk_score"))
        ]
        non_lp_risk_rows = [
            row
            for row in rows
            if not bool((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_strong_conflict"))
            if _float((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_risk_score"))
            > _float((row.get("trade_opportunity_non_lp_evidence_context") or {}).get("non_lp_support_score"))
        ]
        history_sufficient = sum(
            1 for row in rows
            if int((row.get("trade_opportunity_history_snapshot") or {}).get("sample_size") or 0) >= int(OPPORTUNITY_MIN_HISTORY_SAMPLES)
        )
        blocker_distribution = Counter(
            str(row.get("trade_opportunity_primary_hard_blocker") or row.get("trade_opportunity_primary_blocker") or "")
            for row in blocker_rows
            if str(row.get("trade_opportunity_primary_hard_blocker") or row.get("trade_opportunity_primary_blocker") or "").strip()
        )
        verification_distribution = Counter(
            str(row.get("trade_opportunity_primary_verification_blocker") or "")
            for row in candidate_rows + verified_rows
            if str(row.get("trade_opportunity_primary_verification_blocker") or "").strip()
        )
        candidate_completed = [row for row in candidate_rows if str(row.get("opportunity_outcome_60s") or "") == "completed"]
        verified_completed = [row for row in verified_rows if str(row.get("opportunity_outcome_60s") or "") == "completed"]
        blocker_saved = [row for row in blocker_rows if row.get("blocker_saved_trade") is True]
        blocker_false = [row for row in blocker_rows if row.get("blocker_false_block_possible") is True]
        return {
            "opportunity_candidate_count": int(status_counter.get("CANDIDATE", 0)),
            "opportunity_verified_count": int(status_counter.get("VERIFIED", 0)),
            "opportunity_blocked_count": int(status_counter.get("BLOCKED", 0)),
            "opportunity_none_count": int(status_counter.get("NONE", 0)),
            "opportunity_candidate_to_verified_rate": round((len(verified_rows) / len(candidate_rows)) if candidate_rows else 0.0, 4),
            "opportunity_score_median": self._median(score_values),
            "opportunity_score_p90": self._percentile(score_values, 0.90),
            "opportunity_history_sample_sufficiency_rate": round((history_sufficient / len(rows)) if rows else 0.0, 4),
            "opportunity_candidate_followthrough_60s_rate": round(
                (sum(1 for row in candidate_completed if row.get("opportunity_followthrough_60s") is True) / len(candidate_completed))
                if candidate_completed
                else 0.0,
                4,
            ),
            "opportunity_candidate_adverse_60s_rate": round(
                (sum(1 for row in candidate_completed if row.get("opportunity_adverse_60s") is True) / len(candidate_completed))
                if candidate_completed
                else 0.0,
                4,
            ),
            "opportunity_verified_followthrough_60s_rate": round(
                (sum(1 for row in verified_completed if row.get("opportunity_followthrough_60s") is True) / len(verified_completed))
                if verified_completed
                else 0.0,
                4,
            ),
            "opportunity_verified_adverse_60s_rate": round(
                (sum(1 for row in verified_completed if row.get("opportunity_adverse_60s") is True) / len(verified_completed))
                if verified_completed
                else 0.0,
                4,
            ),
            "opportunity_profile_count": len(self._profile_stats),
            "opportunity_blocker_avoided_adverse_rate": round((len(blocker_saved) / len(blocker_rows)) if blocker_rows else 0.0, 4),
            "opportunity_blocker_false_block_rate": round((len(blocker_false) / len(blocker_rows)) if blocker_rows else 0.0, 4),
            "opportunity_non_lp_support_count": len(non_lp_supported_rows),
            "opportunity_non_lp_risk_count": len(non_lp_risk_rows),
            "opportunities_blocked_by_non_lp": sum(
                1
                for row in blocker_rows
                if str(row.get("trade_opportunity_primary_hard_blocker") or row.get("trade_opportunity_primary_blocker") or "").startswith("non_lp_")
            ),
            "opportunity_budget_suppressed_count": sum(int(record.opportunity_budget_exhausted or 0) for record in self._asset_states.values()),
            "opportunity_cooldown_suppressed_count": sum(
                1 for row in rows if str(row.get("telegram_suppression_reason") or "") == "opportunity_cooldown_active"
            ),
            "opportunity_hard_blocker_distribution": dict(sorted(blocker_distribution.items())),
            "opportunity_verification_blocker_distribution": dict(sorted(verification_distribution.items())),
            "candidate_result_distribution": dict(sorted(Counter(str(row.get("opportunity_result_label") or "") for row in candidate_rows).items())),
            "verified_result_distribution": dict(sorted(Counter(str(row.get("opportunity_result_label") or "") for row in verified_rows).items())),
            "blocker_result_distribution": dict(sorted(Counter(str(row.get("opportunity_result_label") or "") for row in blocker_rows).items())),
        }

    def _median(self, values: list[float]) -> float:
        cleaned = sorted(float(item) for item in values if item is not None)
        if not cleaned:
            return 0.0
        length = len(cleaned)
        mid = length // 2
        if length % 2 == 1:
            return round(cleaned[mid], 4)
        return round((cleaned[mid - 1] + cleaned[mid]) / 2.0, 4)

    def _percentile(self, values: list[float], ratio: float) -> float:
        cleaned = sorted(float(item) for item in values if item is not None)
        if not cleaned:
            return 0.0
        index = max(0, min(len(cleaned) - 1, int((len(cleaned) - 1) * float(ratio))))
        return round(cleaned[index], 4)

    def _mark_dirty(self) -> None:
        self._dirty = True
