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
from lp_product_helpers import asset_symbol_from_event, is_major_asset_symbol
from persistence_utils import read_json_file, resolve_persistence_path, write_json_file


ACTIVE_STATUSES = {"CANDIDATE", "VERIFIED", "BLOCKED"}
OPPORTUNITY_TERMINAL_STATUSES = {"EXPIRED", "INVALIDATED"}
HIGH_VALUE_STATUSES = ACTIVE_STATUSES | OPPORTUNITY_TERMINAL_STATUSES
LONG_KEYS = {"LONG_CHASE_ALLOWED", "DO_NOT_CHASE_LONG", "LONG_CANDIDATE", "TRADEABLE_LONG"}
SHORT_KEYS = {"SHORT_CHASE_ALLOWED", "DO_NOT_CHASE_SHORT", "SHORT_CANDIDATE", "TRADEABLE_SHORT"}
_OUTCOME_WINDOWS = (30, 60, 300)
_FOLLOWTHROUGH_MIN_MOVE = 0.002
_STRONG_SIGNAL_MIN = 0.60

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
    "recent_opposite_strong_signal",
    "low_quality",
    "history_adverse_too_high",
    "history_completion_too_low",
]

BLOCKER_LABELS = {
    "no_trade_lock": "不交易锁定",
    "direction_conflict": "方向冲突锁定",
    "data_gap": "数据缺口",
    "sweep_exhaustion_risk": "清扫后回吐/反抽风险",
    "late_or_chase": "确认偏晚 / 追单风险",
    "crowded_basis": "basis / Mark-Index 拥挤",
    "local_absorption": "局部压力被吸收/承接",
    "recent_opposite_strong_signal": "近期反向强信号",
    "low_quality": "质量不足",
    "history_adverse_too_high": "历史 adverse 偏高",
    "history_completion_too_low": "历史 completion 偏低",
    "history_samples_insufficient": "历史样本不足",
    "history_followthrough_not_ready": "历史 followthrough 未稳定",
}


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


def _window_key(window_sec: int) -> str:
    return f"{int(window_sec)}s"


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
        now_ts = _int(summary.get("event_ts"), default=int(time.time()))
        asset_state = self._asset_states.get(asset_symbol) or TradeOpportunityAssetState(asset_symbol=asset_symbol)
        previous = self._opportunity_index.get(str(asset_state.active_opportunity_id or "")) if asset_state.active_opportunity_id else None
        previous_status = str(previous.get("trade_opportunity_status") or "") if previous else ""
        self._prune_hour_buckets(asset_state, now_ts=now_ts)

        history = self._history_snapshot(asset_symbol=asset_symbol, side=str(summary.get("trade_opportunity_side") or "NONE"))
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
        raw_budget = payload.get("per_asset_budget") or {}
        self._opportunities = []
        self._opportunity_index = {}
        for item in raw_opportunities:
            if not isinstance(item, dict):
                continue
            record = dict(item)
            opportunity_id = str(record.get("trade_opportunity_id") or "")
            if not opportunity_id:
                continue
            self._opportunities.append(record)
            self._opportunity_index[opportunity_id] = record
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

    def _history_snapshot(self, *, asset_symbol: str, side: str) -> dict[str, Any]:
        direction = "long" if side == "LONG" else "short" if side == "SHORT" else "neutral"
        opportunity_rows = [
            row
            for row in self._opportunities
            if str(row.get("asset_symbol") or "").upper() == asset_symbol
            and str(row.get("trade_opportunity_side") or "NONE") == side
            and str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED"}
        ][-self.history_limit :]
        opportunity_snapshot = self._history_metrics_from_records(opportunity_rows, history_source="opportunity_asset")
        lp_fallback = self._lp_outcome_fallback(direction=direction)
        selected = dict(opportunity_snapshot)
        if opportunity_snapshot["sample_size"] < int(OPPORTUNITY_MIN_HISTORY_SAMPLES) and lp_fallback["sample_size"] > opportunity_snapshot["sample_size"]:
            selected = dict(lp_fallback)
            selected["history_source"] = "lp_outcome_fallback"
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
        direction_component = _score_component(0.25, direction_score, " + ".join(direction_reasons) or "direction_unclear")

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
        market_component = _score_component(0.20, market_score, " + ".join(market_reasons) or "market_context_weak")

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
        structure_component = _score_component(0.20, structure_score, " + ".join(structure_reasons) or "structure_weak")

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
        risk_component = _score_component(0.15, risk_score, " + ".join(risk_reasons))

        total_score = round(
            direction_component["weighted_score"]
            + market_component["weighted_score"]
            + structure_component["weighted_score"]
            + quality_component["weighted_score"]
            + risk_component["weighted_score"],
            4,
        )
        return {
            "trade_opportunity_score": max(0.0, min(1.0, total_score)),
            "trade_opportunity_score_components": {
                "direction_confirmation": direction_component,
                "market_context": market_component,
                "lp_structure": structure_component,
                "quality_outcome": quality_component,
                "risk_cleanliness": risk_component,
            },
        }

    def _evaluate(self, *, summary: dict[str, Any], history: dict[str, Any], score_payload: dict[str, Any]) -> dict[str, Any]:
        status = "NONE"
        side = str(summary.get("trade_opportunity_side") or "NONE")
        score = float(score_payload.get("trade_opportunity_score") or 0.0)
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
            history=history,
            preliminary_quality_pass=preliminary_quality_pass,
        )
        history_gate = self._history_gate(history)
        if history_gate["status"] == "blocked":
            hard_blockers.extend(history_gate["blockers"])
            risk_flags.extend(history_gate["blockers"])

        if side == "NONE":
            status = "NONE"
        elif hard_blockers and strong_directional:
            status = "BLOCKED"
        else:
            candidate_ready = bool(
                strong_directional
                and score >= float(OPPORTUNITY_MIN_CANDIDATE_SCORE)
                and preliminary_quality_pass
                and (live_context or not bool(OPPORTUNITY_REQUIRE_LIVE_CONTEXT))
                and (broader_confirm or not bool(OPPORTUNITY_REQUIRE_BROADER_CONFIRM))
            )
            verified_ready = bool(
                candidate_ready
                and clean_or_continuation
                and score >= float(OPPORTUNITY_MIN_VERIFIED_SCORE)
                and history_gate["status"] == "verified_ready"
            )
            if verified_ready:
                status = "VERIFIED"
            elif candidate_ready:
                status = "CANDIDATE"
                if history_gate["status"] == "candidate_only":
                    risk_flags.extend(history_gate["blockers"])
            elif strong_directional and not preliminary_quality_pass:
                status = "BLOCKED"
                hard_blockers.append("low_quality")
                risk_flags.append("low_quality")
            else:
                status = "NONE"

        primary_blocker = self._primary_blocker(hard_blockers)
        evidence = self._build_evidence(summary=summary, status=status, quality_floor=quality_floor, history=history)
        reason = self._reason(
            summary=summary,
            status=status,
            side=side,
            primary_blocker=primary_blocker,
            history_gate=history_gate,
            score=score,
        )
        required_confirmation = self._required_confirmation(
            summary=summary,
            status=status,
            primary_blocker=primary_blocker,
            history_gate=history_gate,
        )
        invalidated_by = self._invalidated_by(summary=summary, status=status, primary_blocker=primary_blocker)
        source = self._source(summary=summary)
        created_at = _int(summary.get("event_ts"), default=int(time.time()))
        time_horizon = self._time_horizon(summary=summary, status=status)
        expires_at = created_at + {"30s": 30, "60s": 60, "300s": 300}.get(time_horizon, 60)
        confidence = self._confidence(status=status, score=score, primary_blocker=primary_blocker)
        opportunity_key = self._key(
            summary=summary,
            side=side,
            status=status,
            primary_blocker=primary_blocker,
        )
        opportunity_id = self._id(opportunity_key=opportunity_key, signal_id=str(summary.get("signal_id") or ""), event_ts=created_at)
        return {
            "trade_opportunity_id": opportunity_id,
            "trade_opportunity_key": opportunity_key,
            "trade_opportunity_side": side,
            "trade_opportunity_status": status,
            "trade_opportunity_label": _label_for(status, side),
            "trade_opportunity_score": round(score, 4),
            "trade_opportunity_confidence": confidence,
            "trade_opportunity_time_horizon": time_horizon,
            "trade_opportunity_reason": reason,
            "trade_opportunity_evidence": evidence,
            "trade_opportunity_blockers": _list_dedup(hard_blockers, limit=8),
            "trade_opportunity_required_confirmation": required_confirmation,
            "trade_opportunity_invalidated_by": invalidated_by,
            "trade_opportunity_risk_flags": _list_dedup(risk_flags, limit=8),
            "trade_opportunity_quality_snapshot": {
                "pool_quality_score": round(_float(summary.get("pool_quality_score")), 4),
                "pair_quality_score": round(_float(summary.get("pair_quality_score")), 4),
                "asset_case_quality_score": round(_float(summary.get("asset_case_quality_score")), 4),
                "quality_floor": round(quality_floor, 4),
                "history": history,
                "trade_action_key": str(summary.get("trade_action_key") or ""),
                "asset_market_state_key": str(summary.get("asset_market_state_key") or ""),
            },
            "trade_opportunity_outcome_policy": {
                "windows": ["30s", "60s", "300s"],
                "require_outcome_history": bool(OPPORTUNITY_REQUIRE_OUTCOME_HISTORY),
                "price_source_priority": ["okx_mark", "okx_index", "okx_last", "kraken_mark", "kraken_last"],
                "history_source": str(history.get("history_source") or "none"),
            },
            "trade_opportunity_created_at": created_at,
            "trade_opportunity_expires_at": expires_at,
            "trade_opportunity_source": source,
            "trade_opportunity_score_components": dict(score_payload.get("trade_opportunity_score_components") or {}),
            "trade_opportunity_history_snapshot": dict(history),
            "trade_opportunity_primary_blocker": primary_blocker,
            "trade_opportunity_status_at_creation": status,
            "asset_symbol": str(summary.get("asset_symbol") or ""),
            "pair_label": str(summary.get("pair_label") or ""),
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
        }

    def _hard_blockers(
        self,
        *,
        summary: dict[str, Any],
        history: dict[str, Any],
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
            blockers.append("recent_opposite_strong_signal")

        risk_flags.extend(blockers)
        if int(history.get("sample_size") or 0) < int(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            risk_flags.append("history_samples_insufficient")
        return _list_dedup(blockers, limit=8), _list_dedup(risk_flags, limit=8)

    def _history_gate(self, history: dict[str, Any]) -> dict[str, Any]:
        blockers: list[str] = []
        sample_size = int(history.get("sample_size") or 0)
        completion_rate = float(history.get("completion_rate") or 0.0)
        followthrough_rate = float(history.get("followthrough_rate") or 0.0)
        adverse_rate = _float(history.get("adverse_rate"), default=1.0)
        if not bool(OPPORTUNITY_REQUIRE_OUTCOME_HISTORY):
            return {"status": "verified_ready", "blockers": blockers}
        if sample_size < int(OPPORTUNITY_MIN_HISTORY_SAMPLES):
            blockers.append("history_samples_insufficient")
            return {"status": "candidate_only", "blockers": blockers}
        if completion_rate < float(OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE) - 0.10:
            blockers.append("history_completion_too_low")
            return {"status": "blocked", "blockers": blockers}
        if adverse_rate > float(OPPORTUNITY_MAX_60S_ADVERSE_RATE) + 0.08:
            blockers.append("history_adverse_too_high")
            return {"status": "blocked", "blockers": blockers}
        if completion_rate < float(OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE):
            blockers.append("history_completion_not_ready")
            return {"status": "candidate_only", "blockers": blockers}
        if followthrough_rate < float(OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE):
            blockers.append("history_followthrough_not_ready")
            return {"status": "candidate_only", "blockers": blockers}
        if adverse_rate > float(OPPORTUNITY_MAX_60S_ADVERSE_RATE):
            blockers.append("history_adverse_not_ready")
            return {"status": "candidate_only", "blockers": blockers}
        return {"status": "verified_ready", "blockers": blockers}

    def _reason(
        self,
        *,
        summary: dict[str, Any],
        status: str,
        side: str,
        primary_blocker: str,
        history_gate: dict[str, Any],
        score: float,
    ) -> str:
        direction_text = "买压" if side == "LONG" else "卖压" if side == "SHORT" else "方向"
        if status == "VERIFIED":
            return (
                f"链上{direction_text}扩散，合约同向确认，机会分 {round(score, 2)}，"
                f"且 60s followthrough / adverse / completion 已达到验证门槛。"
            )
        if status == "CANDIDATE":
            if "history_samples_insufficient" in history_gate.get("blockers", []):
                return "当前结构接近机会，但历史样本不足，只能列为候选，不可盲追。"
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
            if primary_blocker in {"history_adverse_too_high", "history_completion_too_low"}:
                return "后验统计明显不健康，继续提示机会只会增加错误交易诱导。"
            return "当前不是机会，追单风险高。"
        if status == "INVALIDATED":
            return "此前的机会条件已经消失，当前不再保留机会判断。"
        if status == "EXPIRED":
            return "机会窗口已过，且没有延续到足以继续保留。"
        return "当前不满足多源确认、后验准入或风险过滤，保持无机会。"

    def _required_confirmation(
        self,
        *,
        summary: dict[str, Any],
        status: str,
        primary_blocker: str,
        history_gate: dict[str, Any],
    ) -> str:
        direction = str(summary.get("trade_opportunity_side") or "NONE")
        if status == "VERIFIED":
            return "不是自动下单；仅在 broader alignment 仍成立且无反向强信号时继续保留。"
        if status == "CANDIDATE":
            if "history_samples_insufficient" in history_gate.get("blockers", []):
                return "候选后验样本补足 + 无反向强信号，才可升级为机会。"
            return "候选后验达标 + 无反向强信号 + clean continuation 保留，才可升级为机会。"
        if status == "BLOCKED":
            if primary_blocker == "no_trade_lock":
                return "等待锁定解除 + broader_confirm + quality pass + 120s 无反向强信号。"
            if primary_blocker == "direction_conflict":
                return "等待一方形成更广方向确认并清掉冲突后再评估。"
            if primary_blocker == "data_gap":
                return "等待 live_public 恢复并给出同向 context 后再评估。"
            if primary_blocker == "crowded_basis":
                return "等待 basis / Mark-Index 拥挤回落且方向仍成立后再评估。"
            if primary_blocker == "local_absorption":
                return "等待结构扩散成 broader_confirm，而不是局部承接。"
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
            if primary_blocker in {"no_trade_lock", "direction_conflict", "data_gap", "sweep_exhaustion_risk"}:
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
            detail = primary_blocker or "blocked"
        else:
            detail = ":".join(
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
            self._apply_outcome_snapshot(record=record)
            self._opportunities.append(record)
            self._opportunity_index[opportunity_id] = record
            return record
        existing.update(evaluation)
        existing["trade_opportunity_history_snapshot"] = dict(history)
        self._apply_outcome_snapshot(record=existing)
        return existing

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
        sixty_status = str(record.get("opportunity_outcome_60s") or "")
        record["opportunity_result_label"] = _result_label_from_outcome(
            sixty_status,
            record.get("opportunity_followthrough_60s"),
            record.get("opportunity_adverse_60s"),
        )

    def _refresh_recent_outcomes(self) -> None:
        pending = [
            row
            for row in self._opportunities[-min(self.history_limit, 200) :]
            if str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "") in {"CANDIDATE", "VERIFIED", "BLOCKED"}
        ]
        for row in pending:
            self._apply_outcome_snapshot(record=row)

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
            "trade_opportunity_confidence": "medium",
            "trade_opportunity_time_horizon": "30s",
            "trade_opportunity_reason": "此前的 verified 机会条件已经消失，当前不再保留机会判断。",
            "trade_opportunity_evidence": list(previous.get("trade_opportunity_evidence") or []),
            "trade_opportunity_blockers": ["alignment_lost"],
            "trade_opportunity_required_confirmation": "等待下一次独立 verified 机会。",
            "trade_opportunity_invalidated_by": "broader alignment 消失 / 反向结构出现",
            "trade_opportunity_risk_flags": ["alignment_lost"],
            "trade_opportunity_quality_snapshot": dict(previous.get("trade_opportunity_quality_snapshot") or {}),
            "trade_opportunity_outcome_policy": dict(previous.get("trade_opportunity_outcome_policy") or {}),
            "trade_opportunity_created_at": int(now_ts),
            "trade_opportunity_expires_at": int(now_ts + 30),
            "trade_opportunity_source": str(previous.get("trade_opportunity_source") or "asset_state"),
            "trade_opportunity_score_components": dict(previous.get("trade_opportunity_score_components") or {}),
            "trade_opportunity_history_snapshot": dict(previous.get("trade_opportunity_history_snapshot") or {}),
            "trade_opportunity_primary_blocker": "alignment_lost",
            "trade_opportunity_status_at_creation": status,
            "asset_symbol": str(previous.get("asset_symbol") or ""),
            "pair_label": str(previous.get("pair_label") or ""),
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
        history_sufficient = sum(
            1 for row in rows
            if int((row.get("trade_opportunity_history_snapshot") or {}).get("sample_size") or 0) >= int(OPPORTUNITY_MIN_HISTORY_SAMPLES)
        )
        blocker_distribution = Counter(str(row.get("trade_opportunity_primary_blocker") or "") for row in blocker_rows if str(row.get("trade_opportunity_primary_blocker") or "").strip())
        candidate_completed = [row for row in candidate_rows if str(row.get("opportunity_outcome_60s") or "") == "completed"]
        verified_completed = [row for row in verified_rows if str(row.get("opportunity_outcome_60s") or "") == "completed"]
        blocker_adverse = [row for row in blocker_rows if row.get("opportunity_adverse_60s") is True]
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
            "opportunity_blocker_avoided_adverse_rate": round((len(blocker_adverse) / len(blocker_rows)) if blocker_rows else 0.0, 4),
            "opportunity_budget_suppressed_count": sum(int(record.opportunity_budget_exhausted or 0) for record in self._asset_states.values()),
            "opportunity_cooldown_suppressed_count": sum(
                1 for row in rows if str(row.get("telegram_suppression_reason") or "") == "opportunity_cooldown_active"
            ),
            "opportunity_hard_blocker_distribution": dict(sorted(blocker_distribution.items())),
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
