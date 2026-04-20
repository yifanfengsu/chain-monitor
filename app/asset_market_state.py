from __future__ import annotations

import atexit
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import time
from typing import Any

from config import (
    ASSET_MARKET_STATE_CACHE_PATH,
    ASSET_MARKET_STATE_CANDIDATE_TTL_SEC,
    ASSET_MARKET_STATE_DATA_GAP_TTL_SEC,
    ASSET_MARKET_STATE_ENABLE,
    ASSET_MARKET_STATE_FLUSH_INTERVAL_SEC,
    ASSET_MARKET_STATE_MAX_TRACKED,
    ASSET_MARKET_STATE_OBSERVE_TTL_SEC,
    ASSET_MARKET_STATE_PERSIST_ENABLE,
    ASSET_MARKET_STATE_RECENT_SIGNAL_MAXLEN,
    ASSET_MARKET_STATE_RECOVER_ON_START,
    ASSET_MARKET_STATE_RISK_TTL_SEC,
    ASSET_MARKET_STATE_SCHEMA_VERSION,
    ASSET_MARKET_STATE_WAIT_TTL_SEC,
    CHASE_ENABLE_AFTER_MIN_SAMPLES,
    CHASE_MAX_ADVERSE_60S_RATE,
    CHASE_MIN_FOLLOWTHROUGH_60S_RATE,
    CHASE_REQUIRE_OUTCOME_COMPLETION_RATE,
    NO_TRADE_LOCK_ENABLE,
    NO_TRADE_LOCK_MIN_CONFLICT_SCORE,
    NO_TRADE_LOCK_SUPPRESS_LOCAL_SIGNALS,
    NO_TRADE_LOCK_TTL_SEC,
    NO_TRADE_LOCK_WINDOW_SEC,
    PREALERT_MIN_LIFETIME_SEC,
    TELEGRAM_ALLOW_CANDIDATES,
    TELEGRAM_ALLOW_RISK_BLOCKERS,
    TELEGRAM_SEND_ONLY_STATE_CHANGES,
    TELEGRAM_SUPPRESS_REPEAT_STATE_SEC,
    TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE,
    TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE,
)
from lp_product_helpers import asset_symbol_from_event
from persistence_utils import read_json_file, resolve_persistence_path, write_json_file


ASSET_MARKET_STATE_LABELS = {
    "NO_TRADE_CHOP": "不交易",
    "NO_TRADE_LOCK": "不交易锁定",
    "WAIT_CONFIRMATION": "等待确认",
    "OBSERVE_LONG": "偏多观察",
    "OBSERVE_SHORT": "偏空观察",
    "DO_NOT_CHASE_LONG": "不追多",
    "DO_NOT_CHASE_SHORT": "不追空",
    "LONG_CANDIDATE": "偏多候选",
    "SHORT_CANDIDATE": "偏空候选",
    "TRADEABLE_LONG": "可顺势追多",
    "TRADEABLE_SHORT": "可顺势追空",
    "DATA_GAP_NO_TRADE": "数据缺口，不交易",
}
RISK_BLOCKER_STATES = {
    "NO_TRADE_LOCK",
    "DO_NOT_CHASE_LONG",
    "DO_NOT_CHASE_SHORT",
    "DATA_GAP_NO_TRADE",
}
CANDIDATE_STATES = {
    "LONG_CANDIDATE",
    "SHORT_CANDIDATE",
    "TRADEABLE_LONG",
    "TRADEABLE_SHORT",
}
OBSERVE_STATES = {"WAIT_CONFIRMATION", "OBSERVE_LONG", "OBSERVE_SHORT", "NO_TRADE_CHOP"}
LONG_STATES = {"OBSERVE_LONG", "DO_NOT_CHASE_LONG", "LONG_CANDIDATE", "TRADEABLE_LONG"}
SHORT_STATES = {"OBSERVE_SHORT", "DO_NOT_CHASE_SHORT", "SHORT_CANDIDATE", "TRADEABLE_SHORT"}
STATE_PRIORITY = {
    "NO_TRADE_LOCK": 100,
    "DATA_GAP_NO_TRADE": 90,
    "DO_NOT_CHASE_LONG": 80,
    "DO_NOT_CHASE_SHORT": 80,
    "TRADEABLE_LONG": 72,
    "TRADEABLE_SHORT": 72,
    "LONG_CANDIDATE": 70,
    "SHORT_CANDIDATE": 70,
    "OBSERVE_LONG": 55,
    "OBSERVE_SHORT": 55,
    "WAIT_CONFIRMATION": 50,
    "NO_TRADE_CHOP": 40,
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


def _state_ttl_sec(state_key: str) -> int:
    if state_key == "NO_TRADE_LOCK":
        return int(NO_TRADE_LOCK_TTL_SEC)
    if state_key in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}:
        return int(ASSET_MARKET_STATE_RISK_TTL_SEC)
    if state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE", "TRADEABLE_LONG", "TRADEABLE_SHORT"}:
        return int(ASSET_MARKET_STATE_CANDIDATE_TTL_SEC)
    if state_key == "DATA_GAP_NO_TRADE":
        return int(ASSET_MARKET_STATE_DATA_GAP_TTL_SEC)
    if state_key in {"OBSERVE_LONG", "OBSERVE_SHORT"}:
        return int(ASSET_MARKET_STATE_OBSERVE_TTL_SEC)
    return int(ASSET_MARKET_STATE_WAIT_TTL_SEC)


@dataclass
class AssetMarketStateRecord:
    asset_symbol: str
    asset_market_state_key: str = "WAIT_CONFIRMATION"
    asset_market_state_label: str = "等待确认"
    asset_market_state_reason: str = ""
    asset_market_state_confidence: float = 0.0
    asset_market_state_started_at: int = 0
    asset_market_state_updated_at: int = 0
    asset_market_state_ttl_sec: int = 0
    asset_market_state_evidence_pack: str = ""
    asset_market_state_required_confirmation: str = ""
    asset_market_state_invalidated_by: str = ""
    asset_market_state_debug: dict[str, Any] = field(default_factory=dict)
    previous_asset_market_state_key: str = ""
    last_telegram_state_key: str = ""
    last_telegram_sent_at: int = 0
    suppressed_signal_count_in_state: int = 0
    first_seen_stage: str = ""
    first_seen_at: int = 0
    prealert_lifecycle_state: str = ""
    prealert_min_lifetime_sec: int = PREALERT_MIN_LIFETIME_SEC
    prealert_expires_at: int = 0
    prealert_to_confirm_sec: int | None = None
    prealert_visible_to_user: bool = False
    prealert_notification_policy: str = "research_high_quality_only"
    no_trade_lock_active: bool = False
    no_trade_lock_reason: str = ""
    no_trade_lock_started_at: int = 0
    no_trade_lock_until: int = 0
    no_trade_lock_conflict_score: float = 0.0
    no_trade_lock_conflicting_signals: list[dict[str, Any]] = field(default_factory=list)
    no_trade_lock_release_condition: str = ""
    no_trade_lock_released_by: str = ""
    transition_count: int = 0
    state_history: list[dict[str, Any]] = field(default_factory=list)
    restored_from_cache: bool = False


class AssetMarketStateManager:
    def __init__(
        self,
        *,
        state_manager=None,
        enabled: bool = ASSET_MARKET_STATE_ENABLE,
        persistence_enabled: bool = ASSET_MARKET_STATE_PERSIST_ENABLE,
        cache_path: str | None = ASSET_MARKET_STATE_CACHE_PATH,
        flush_interval_sec: float = ASSET_MARKET_STATE_FLUSH_INTERVAL_SEC,
        recover_on_start: bool = ASSET_MARKET_STATE_RECOVER_ON_START,
        schema_version: str = ASSET_MARKET_STATE_SCHEMA_VERSION,
        max_tracked: int = ASSET_MARKET_STATE_MAX_TRACKED,
        recent_signal_maxlen: int = ASSET_MARKET_STATE_RECENT_SIGNAL_MAXLEN,
    ) -> None:
        self.state_manager = state_manager
        self.enabled = bool(enabled)
        self.persistence_enabled = bool(persistence_enabled)
        self.schema_version = str(schema_version or ASSET_MARKET_STATE_SCHEMA_VERSION)
        self.flush_interval_sec = max(float(flush_interval_sec or 0.0), 0.2)
        self.max_tracked = max(int(max_tracked or 0), 32)
        self.recent_signal_maxlen = max(int(recent_signal_maxlen or 0), 8)
        self.cache_path = resolve_persistence_path(cache_path, namespace="asset-market-state") if self.persistence_enabled else None
        self._records: dict[str, AssetMarketStateRecord] = {}
        self._recent_signals: dict[str, deque] = defaultdict(lambda: deque(maxlen=self.recent_signal_maxlen))
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

        summary = self._signal_summary(event, signal)
        asset_symbol = str(summary.get("asset_symbol") or "").strip().upper()
        if not asset_symbol:
            return {}

        now_ts = _int(summary.get("event_ts"), default=int(time.time()))
        self._prune_recent_signals(asset_symbol=asset_symbol, now_ts=now_ts)
        record = self._records.get(asset_symbol)
        if record is None:
            record = AssetMarketStateRecord(asset_symbol=asset_symbol)
        previous_key = str(record.asset_market_state_key or "")

        self._expire_prealert_lifecycle(record=record, now_ts=now_ts)
        self._expire_lock(record=record, now_ts=now_ts)
        self._update_prealert_lifecycle(record=record, summary=summary, now_ts=now_ts)

        base_state_key, candidate_validation = self._derive_base_state(summary=summary)
        lock_decision = self._evaluate_no_trade_lock(
            record=record,
            summary=summary,
            now_ts=now_ts,
            base_state_key=base_state_key,
        )

        state_key = base_state_key
        state_reason = self._base_reason(summary=summary, state_key=base_state_key, candidate_validation=candidate_validation)
        required_confirmation = self._base_required_confirmation(
            summary=summary,
            state_key=base_state_key,
            candidate_validation=candidate_validation,
        )
        invalidated_by = self._base_invalidated_by(summary=summary, state_key=base_state_key)
        evidence_pack = self._base_evidence_pack(summary=summary, state_key=base_state_key, record=record)
        confidence = self._base_confidence(summary=summary, state_key=base_state_key, candidate_validation=candidate_validation)

        if lock_decision.get("state_key") == "NO_TRADE_LOCK":
            state_key = "NO_TRADE_LOCK"
            state_reason = str(lock_decision.get("reason") or "近期冲突严重，进入冷却锁，暂停追单。")
            required_confirmation = str(
                lock_decision.get("release_condition")
                or "一方形成 broader_confirm + clean + 90s 无反向强信号"
            )
            invalidated_by = str(lock_decision.get("release_condition") or "锁到期或强侧完成 clean dominance")
            evidence_pack = str(lock_decision.get("evidence_pack") or evidence_pack)
            confidence = max(float(lock_decision.get("conflict_score") or 0.0), confidence)
            if bool(summary.get("lp_prealert_gate_passed")) and str(summary.get("lp_alert_stage") or "") == "prealert":
                record.prealert_lifecycle_state = "suppressed_by_lock"

        changed = state_key != previous_key
        if changed:
            record.previous_asset_market_state_key = previous_key
            record.asset_market_state_started_at = now_ts
            record.transition_count = int(record.transition_count or 0) + 1
            record.state_history = list(record.state_history or [])[-15:]
            record.state_history.append(
                {
                    "ts": now_ts,
                    "from": previous_key,
                    "to": state_key,
                    "reason": state_reason,
                }
            )
        elif not int(record.asset_market_state_started_at or 0):
            record.asset_market_state_started_at = now_ts

        record.asset_market_state_key = state_key
        record.asset_market_state_label = ASSET_MARKET_STATE_LABELS.get(state_key, state_key)
        record.asset_market_state_reason = state_reason
        record.asset_market_state_confidence = round(float(confidence), 3)
        record.asset_market_state_updated_at = now_ts
        record.asset_market_state_ttl_sec = _state_ttl_sec(state_key)
        record.asset_market_state_evidence_pack = evidence_pack
        record.asset_market_state_required_confirmation = required_confirmation
        record.asset_market_state_invalidated_by = invalidated_by
        record.asset_market_state_debug = {
            "trade_action_key": str(summary.get("trade_action_key") or ""),
            "trade_action_reason": str(summary.get("trade_action_reason") or ""),
            "base_state_key": base_state_key,
            "candidate_validation": candidate_validation,
            "lock_decision": lock_decision,
            "market_context_source": str(summary.get("market_context_source") or "unavailable"),
            "lp_alert_stage": str(summary.get("lp_alert_stage") or ""),
            "lp_sweep_phase": str(summary.get("lp_sweep_phase") or ""),
            "lp_confirm_scope": str(summary.get("lp_confirm_scope") or ""),
            "lp_confirm_quality": str(summary.get("lp_confirm_quality") or ""),
            "lp_absorption_context": str(summary.get("lp_absorption_context") or ""),
            "restored_from_cache": bool(record.restored_from_cache),
        }
        record.restored_from_cache = False

        telegram_payload = self._telegram_decision(
            record=record,
            summary=summary,
            now_ts=now_ts,
            state_changed=changed,
            candidate_validation=candidate_validation,
            lock_decision=lock_decision,
        )
        if telegram_payload["telegram_should_send"]:
            record.last_telegram_state_key = state_key
            record.last_telegram_sent_at = now_ts
            record.suppressed_signal_count_in_state = 0
        elif not changed:
            record.suppressed_signal_count_in_state = int(record.suppressed_signal_count_in_state or 0) + 1

        self._records[asset_symbol] = record
        self._append_recent_signal(asset_symbol=asset_symbol, summary=summary, now_ts=now_ts)
        self._trim_records()
        self._mark_dirty()
        self.flush()

        payload = {
            "asset_market_state_key": record.asset_market_state_key,
            "asset_market_state_label": record.asset_market_state_label,
            "asset_market_state_reason": record.asset_market_state_reason,
            "asset_market_state_confidence": record.asset_market_state_confidence,
            "asset_market_state_changed": bool(changed),
            "previous_asset_market_state_key": record.previous_asset_market_state_key,
            "asset_market_state_started_at": int(record.asset_market_state_started_at or 0),
            "asset_market_state_updated_at": int(record.asset_market_state_updated_at or 0),
            "asset_market_state_ttl_sec": int(record.asset_market_state_ttl_sec or 0),
            "asset_market_state_evidence_pack": record.asset_market_state_evidence_pack,
            "asset_market_state_required_confirmation": record.asset_market_state_required_confirmation,
            "asset_market_state_invalidated_by": record.asset_market_state_invalidated_by,
            "asset_market_state_debug": dict(record.asset_market_state_debug or {}),
            "first_seen_stage": record.first_seen_stage,
            "first_seen_at": int(record.first_seen_at or 0),
            "prealert_lifecycle_state": record.prealert_lifecycle_state,
            "prealert_min_lifetime_sec": int(record.prealert_min_lifetime_sec or 0),
            "prealert_expires_at": int(record.prealert_expires_at or 0),
            "prealert_to_confirm_sec": record.prealert_to_confirm_sec,
            "prealert_visible_to_user": bool(record.prealert_visible_to_user),
            "prealert_notification_policy": record.prealert_notification_policy,
            "no_trade_lock_active": bool(record.no_trade_lock_active),
            "no_trade_lock_reason": record.no_trade_lock_reason,
            "no_trade_lock_started_at": int(record.no_trade_lock_started_at or 0),
            "no_trade_lock_until": int(record.no_trade_lock_until or 0),
            "no_trade_lock_conflict_score": round(float(record.no_trade_lock_conflict_score or 0.0), 3),
            "no_trade_lock_conflicting_signals": list(record.no_trade_lock_conflicting_signals or []),
            "no_trade_lock_release_condition": record.no_trade_lock_release_condition,
            "no_trade_lock_released_by": record.no_trade_lock_released_by,
            "telegram_should_send": bool(telegram_payload["telegram_should_send"]),
            "telegram_suppression_reason": str(telegram_payload["telegram_suppression_reason"] or ""),
            "telegram_state_change_reason": str(telegram_payload["telegram_state_change_reason"] or ""),
            "telegram_update_kind": str(telegram_payload["telegram_update_kind"] or "suppressed"),
            "suppressed_signal_count_in_state": int(record.suppressed_signal_count_in_state or 0),
            "last_telegram_state_key": record.last_telegram_state_key,
            "market_state_label": record.asset_market_state_label,
            "headline_label": record.asset_market_state_label,
        }
        event_metadata.update(payload)
        metadata.update(payload)
        context.update(payload)
        return payload

    def get_current_state(self, asset_symbol: str) -> dict[str, Any]:
        record = self._records.get(str(asset_symbol or "").strip().upper())
        return asdict(record) if record is not None else {}

    def snapshot(self) -> list[dict[str, Any]]:
        return [asdict(item) for item in self.snapshot_records()]

    def snapshot_records(self) -> list[AssetMarketStateRecord]:
        return sorted(
            self._records.values(),
            key=lambda item: (
                -int(item.asset_market_state_updated_at or 0),
                -int(item.transition_count or 0),
                str(item.asset_symbol or ""),
            ),
        )

    def flush(self, force: bool = False) -> None:
        if not self.persistence_enabled or self.cache_path is None or not self._dirty:
            return
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_flush_monotonic) < self.flush_interval_sec:
            return
        payload = {
            "schema_version": self.schema_version,
            "generated_at": int(time.time()),
            "records": [self._serialize_record(record) for record in self.snapshot_records()],
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
        rows = payload.get("records") or []
        if not isinstance(rows, list):
            self._last_load_status = "records_invalid"
            return 0
        self._records.clear()
        loaded = 0
        for row in rows:
            record = self._deserialize_record(row)
            if record is None:
                continue
            self._records[record.asset_symbol] = record
            loaded += 1
        self._last_load_status = "loaded"
        return loaded

    def _signal_summary(self, event, signal) -> dict[str, Any]:
        context = getattr(signal, "context", {}) or {}
        metadata = getattr(signal, "metadata", {}) or {}
        event_metadata = getattr(event, "metadata", {}) or {}
        return {
            "event_ts": _int(getattr(event, "ts", 0), default=int(time.time())),
            "signal_id": _text(getattr(signal, "signal_id", "")),
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
            "direction": _direction_from_values(
                context.get("trade_action_direction"),
                context.get("asset_case_direction"),
                metadata.get("trade_action_direction"),
                metadata.get("asset_case_direction"),
                event_metadata.get("trade_action_direction"),
                event_metadata.get("asset_case_direction"),
                "buy_pressure" if str(getattr(event, "intent_type", "") or "") == "pool_buy_pressure" else "sell_pressure" if str(getattr(event, "intent_type", "") or "") == "pool_sell_pressure" else "",
            ),
            "trade_action_key": _text(
                context.get("trade_action_key"),
                metadata.get("trade_action_key"),
                event_metadata.get("trade_action_key"),
            ),
            "trade_action_label": _text(
                context.get("trade_action_label"),
                metadata.get("trade_action_label"),
                event_metadata.get("trade_action_label"),
            ),
            "trade_action_reason": _text(
                context.get("trade_action_reason"),
                metadata.get("trade_action_reason"),
                event_metadata.get("trade_action_reason"),
            ),
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
            "trade_action_evidence_pack": _text(
                context.get("trade_action_evidence_pack"),
                metadata.get("trade_action_evidence_pack"),
                event_metadata.get("trade_action_evidence_pack"),
                context.get("asset_case_evidence_pack"),
                metadata.get("asset_case_evidence_pack"),
                event_metadata.get("asset_case_evidence_pack"),
            ),
            "trade_action_confidence": _float(
                context.get("trade_action_confidence"),
                metadata.get("trade_action_confidence"),
                event_metadata.get("trade_action_confidence"),
            ),
            "market_context_source": _text(
                context.get("market_context_source"),
                metadata.get("market_context_source"),
                event_metadata.get("market_context_source"),
                "unavailable",
            ),
            "lp_alert_stage": _text(
                context.get("lp_alert_stage"),
                metadata.get("lp_alert_stage"),
                event_metadata.get("lp_alert_stage"),
            ),
            "lp_sweep_phase": _text(
                context.get("lp_sweep_phase"),
                metadata.get("lp_sweep_phase"),
                event_metadata.get("lp_sweep_phase"),
            ),
            "lp_confirm_scope": _text(
                context.get("lp_confirm_scope"),
                metadata.get("lp_confirm_scope"),
                event_metadata.get("lp_confirm_scope"),
            ),
            "lp_confirm_quality": _text(
                context.get("lp_confirm_quality"),
                metadata.get("lp_confirm_quality"),
                event_metadata.get("lp_confirm_quality"),
            ),
            "lp_absorption_context": _text(
                context.get("lp_absorption_context"),
                metadata.get("lp_absorption_context"),
                event_metadata.get("lp_absorption_context"),
            ),
            "lp_broader_alignment": _text(
                context.get("lp_broader_alignment"),
                metadata.get("lp_broader_alignment"),
                event_metadata.get("lp_broader_alignment"),
            ),
            "asset_case_quality_score": _float(
                context.get("asset_case_quality_score"),
                metadata.get("asset_case_quality_score"),
                event_metadata.get("asset_case_quality_score"),
            ),
            "pair_quality_score": _float(
                context.get("pair_quality_score"),
                metadata.get("pair_quality_score"),
                event_metadata.get("pair_quality_score"),
            ),
            "pool_quality_score": _float(
                context.get("pool_quality_score"),
                metadata.get("pool_quality_score"),
                event_metadata.get("pool_quality_score"),
            ),
            "strength_score": _float(
                (context.get("trade_action_debug") or {}).get("strength_score"),
                (metadata.get("trade_action_debug") or {}).get("strength_score"),
            ),
            "alert_relative_timing": _text(
                context.get("alert_relative_timing"),
                metadata.get("alert_relative_timing"),
                event_metadata.get("alert_relative_timing"),
            ),
            "user_tier": _text(
                context.get("user_tier"),
                metadata.get("user_tier"),
                event_metadata.get("user_tier"),
                "research",
            ).lower(),
            "lp_prealert_gate_passed": _bool(
                context.get("lp_prealert_gate_passed"),
                metadata.get("lp_prealert_gate_passed"),
                event_metadata.get("lp_prealert_gate_passed"),
            ),
            "lp_prealert_candidate": _bool(
                context.get("lp_prealert_candidate"),
                metadata.get("lp_prealert_candidate"),
                event_metadata.get("lp_prealert_candidate"),
            ),
            "lp_prealert_delivery_allowed": context.get(
                "lp_prealert_delivery_allowed",
                metadata.get(
                    "lp_prealert_delivery_allowed",
                    event_metadata.get("lp_prealert_delivery_allowed"),
                ),
            ),
            "prealert_precision_score": _float(
                context.get("prealert_precision_score"),
                metadata.get("prealert_precision_score"),
                event_metadata.get("prealert_precision_score"),
            ),
            "asset_case_supporting_pair_count": _int(
                context.get("asset_case_supporting_pair_count"),
                metadata.get("asset_case_supporting_pair_count"),
                event_metadata.get("asset_case_supporting_pair_count"),
            ),
            "quality_scope_asset_size": _int(
                context.get("quality_scope_asset_size"),
                metadata.get("quality_scope_asset_size"),
                event_metadata.get("quality_scope_asset_size"),
            ),
            "quality_actionable_sample_size": _int(
                context.get("quality_actionable_sample_size"),
                metadata.get("quality_actionable_sample_size"),
                event_metadata.get("quality_actionable_sample_size"),
            ),
        }

    def _derive_base_state(self, *, summary: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        trade_action_key = str(summary.get("trade_action_key") or "")
        direction = str(summary.get("direction") or "neutral")
        validation = self._candidate_validation(direction=direction)
        if trade_action_key == "DATA_GAP_NO_TRADE":
            return "DATA_GAP_NO_TRADE", validation
        if trade_action_key in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}:
            return trade_action_key, validation
        if trade_action_key in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}:
            if validation["sample_size"] < int(CHASE_ENABLE_AFTER_MIN_SAMPLES):
                return "LONG_CANDIDATE" if direction == "long" else "SHORT_CANDIDATE", validation
            if validation["completion_rate"] < float(CHASE_REQUIRE_OUTCOME_COMPLETION_RATE):
                return "LONG_CANDIDATE" if direction == "long" else "SHORT_CANDIDATE", validation
            if validation["followthrough_rate"] >= float(CHASE_MIN_FOLLOWTHROUGH_60S_RATE) and validation["adverse_rate"] <= float(CHASE_MAX_ADVERSE_60S_RATE):
                return "TRADEABLE_LONG" if direction == "long" else "TRADEABLE_SHORT", validation
            return "OBSERVE_LONG" if direction == "long" else "OBSERVE_SHORT", validation
        if trade_action_key == "LONG_BIAS_OBSERVE":
            return "OBSERVE_LONG", validation
        if trade_action_key == "SHORT_BIAS_OBSERVE":
            return "OBSERVE_SHORT", validation
        if trade_action_key == "WAIT_CONFIRMATION":
            return "WAIT_CONFIRMATION", validation
        if trade_action_key in {"NO_TRADE", "CONFLICT_NO_TRADE"}:
            return "NO_TRADE_CHOP", validation
        if direction == "long":
            return "OBSERVE_LONG", validation
        if direction == "short":
            return "OBSERVE_SHORT", validation
        return "NO_TRADE_CHOP", validation

    def _candidate_validation(self, *, direction: str) -> dict[str, Any]:
        if self.state_manager is None or direction not in {"long", "short"}:
            return {
                "sample_size": 0,
                "resolved_count": 0,
                "completion_rate": 0.0,
                "followthrough_rate": 0.0,
                "adverse_rate": 1.0,
                "outcome_source_distribution": {},
            }
        records = list(self.state_manager.get_recent_lp_outcome_records(limit=500))
        direction_bucket = "buy_pressure" if direction == "long" else "sell_pressure"
        scoped = [
            row
            for row in records
            if str(row.get("direction_bucket") or "") == direction_bucket
            and str(row.get("lp_alert_stage") or "") in {"confirm", "climax", "exhaustion_risk"}
        ]
        sample_size = len(scoped)
        resolved = []
        followthrough = 0
        adverse = 0
        source_counter: dict[str, int] = defaultdict(int)
        for row in scoped:
            windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
            window = dict(windows.get("60s") or {})
            status = str(window.get("status") or "")
            price_source = str(window.get("price_source") or row.get("outcome_price_source") or "unavailable")
            source_counter[price_source] += 1
            if status != "completed":
                continue
            resolved.append(row)
            adjusted = window.get("direction_adjusted_move_after")
            if adjusted is None:
                adjusted = row.get("direction_adjusted_move_after_60s")
            if adjusted is not None and float(adjusted) > 0.002:
                followthrough += 1
            adverse_flag = window.get("adverse_by_direction")
            if adverse_flag is None:
                adverse_flag = row.get("adverse_by_direction_60s")
            if adverse_flag is True:
                adverse += 1
        resolved_count = len(resolved)
        return {
            "sample_size": sample_size,
            "resolved_count": resolved_count,
            "completion_rate": round((resolved_count / sample_size) if sample_size > 0 else 0.0, 3),
            "followthrough_rate": round((followthrough / resolved_count) if resolved_count > 0 else 0.0, 3),
            "adverse_rate": round((adverse / resolved_count) if resolved_count > 0 else 1.0, 3),
            "outcome_source_distribution": dict(sorted(source_counter.items())),
        }

    def _evaluate_no_trade_lock(
        self,
        *,
        record: AssetMarketStateRecord,
        summary: dict[str, Any],
        now_ts: int,
        base_state_key: str,
    ) -> dict[str, Any]:
        if not bool(NO_TRADE_LOCK_ENABLE):
            return {"state_key": base_state_key}

        current_is_clean_release = self._is_clean_release_signal(summary)
        current_is_strong = self._is_strong_signal(summary)
        direction = str(summary.get("direction") or "neutral")
        asset_symbol = str(summary.get("asset_symbol") or "")

        recent = list(self._recent_signals.get(asset_symbol) or [])
        opposite = [
            dict(item)
            for item in recent
            if str(item.get("direction") or "") in {"long", "short"}
            and str(item.get("direction") or "") != direction
            and now_ts - int(item.get("ts") or 0) <= int(NO_TRADE_LOCK_WINDOW_SEC)
            and bool(item.get("strong"))
        ]
        strongest_opposite = max((_float(item.get("strength_score")) for item in opposite), default=0.0)
        conflict_score = round(
            0.45
            + max(_float(summary.get("strength_score")), strongest_opposite) * 0.28
            + min(len(opposite) / 2.0, 1.0) * 0.16,
            3,
        )
        lock_evidence = []
        if opposite:
            lock_evidence.append(f"{int(NO_TRADE_LOCK_WINDOW_SEC)}s 内买卖清扫交替")
        if current_is_strong and opposite:
            lock_evidence.append("局部买卖压冲突")
        if not current_is_clean_release:
            lock_evidence.append("无稳定 broader 确认")
        evidence_pack = "｜".join(lock_evidence[:4]) if lock_evidence else ""
        conflicting_signals = [
            {
                "signal_id": str(item.get("signal_id") or ""),
                "direction": str(item.get("direction") or ""),
                "trade_action_key": str(item.get("trade_action_key") or ""),
                "ts": int(item.get("ts") or 0),
            }
            for item in opposite[:4]
        ]

        if record.no_trade_lock_active and int(record.no_trade_lock_until or 0) > now_ts:
            if current_is_clean_release:
                record.no_trade_lock_active = False
                record.no_trade_lock_released_by = "broader_clean_dominance"
                record.no_trade_lock_reason = ""
                record.no_trade_lock_conflicting_signals = []
                record.no_trade_lock_conflict_score = 0.0
                return {
                    "state_key": base_state_key,
                    "released": True,
                    "release_condition": "强侧形成 broader_confirm + clean + 90s 无反向强信号",
                }
            if opposite and current_is_strong:
                record.no_trade_lock_until = max(int(record.no_trade_lock_until or 0), now_ts + int(NO_TRADE_LOCK_TTL_SEC))
                record.no_trade_lock_conflict_score = max(float(record.no_trade_lock_conflict_score or 0.0), conflict_score)
                record.no_trade_lock_conflicting_signals = conflicting_signals or list(record.no_trade_lock_conflicting_signals or [])
            return {
                "state_key": "NO_TRADE_LOCK",
                "active": True,
                "reason": record.no_trade_lock_reason or "近期双向强信号冲突，进入冷却锁，暂停追单。",
                "conflict_score": record.no_trade_lock_conflict_score,
                "release_condition": record.no_trade_lock_release_condition or "一方形成 broader_confirm + clean + 90s 无反向强信号",
                "evidence_pack": evidence_pack or record.asset_market_state_evidence_pack,
            }

        if not current_is_strong or not opposite:
            return {"state_key": base_state_key}

        if current_is_clean_release and strongest_opposite <= 0.55:
            return {"state_key": base_state_key}

        if conflict_score < float(NO_TRADE_LOCK_MIN_CONFLICT_SCORE):
            return {"state_key": base_state_key}

        record.no_trade_lock_active = True
        record.no_trade_lock_reason = "双向扫流动性，暂停追单。"
        record.no_trade_lock_started_at = now_ts
        record.no_trade_lock_until = now_ts + int(NO_TRADE_LOCK_TTL_SEC)
        record.no_trade_lock_conflict_score = conflict_score
        record.no_trade_lock_conflicting_signals = conflicting_signals
        record.no_trade_lock_release_condition = "一方形成 broader_confirm + clean + 90s 无反向强信号"
        record.no_trade_lock_released_by = ""
        return {
            "state_key": "NO_TRADE_LOCK",
            "entered": True,
            "reason": "近期冲突严重，进入冷却锁，暂停追单。",
            "conflict_score": conflict_score,
            "release_condition": record.no_trade_lock_release_condition,
            "evidence_pack": evidence_pack,
        }

    def _is_strong_signal(self, summary: dict[str, Any]) -> bool:
        if _float(summary.get("strength_score")) >= 0.55:
            return True
        if str(summary.get("lp_sweep_phase") or "") in {"sweep_confirmed", "sweep_exhaustion_risk"}:
            return True
        if str(summary.get("lp_confirm_scope") or "") in {"local_confirm", "broader_confirm"}:
            return True
        if str(summary.get("trade_action_key") or "") in {
            "LONG_CHASE_ALLOWED",
            "SHORT_CHASE_ALLOWED",
            "DO_NOT_CHASE_LONG",
            "DO_NOT_CHASE_SHORT",
        }:
            return True
        return False

    def _is_clean_release_signal(self, summary: dict[str, Any]) -> bool:
        return bool(
            str(summary.get("market_context_source") or "") == "live_public"
            and str(summary.get("lp_confirm_scope") or "") == "broader_confirm"
            and str(summary.get("lp_confirm_quality") or "") == "clean_confirm"
            and _float(summary.get("asset_case_quality_score")) >= float(TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE)
            and _float(summary.get("pair_quality_score")) >= float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE)
        )

    def _update_prealert_lifecycle(
        self,
        *,
        record: AssetMarketStateRecord,
        summary: dict[str, Any],
        now_ts: int,
    ) -> None:
        stage = str(summary.get("lp_alert_stage") or "")
        gate_passed = bool(summary.get("lp_prealert_gate_passed"))
        user_tier = str(summary.get("user_tier") or "research")
        major_like = _bool(summary.get("asset_case_supporting_pair_count")) or str(summary.get("asset_symbol") or "") in {"ETH", "BTC", "SOL"}
        if gate_passed and stage == "prealert":
            if not record.first_seen_stage:
                record.first_seen_stage = "prealert"
                record.first_seen_at = now_ts
            if record.prealert_lifecycle_state not in {"delivered", "upgraded_to_confirm"}:
                record.prealert_lifecycle_state = "active"
            record.prealert_min_lifetime_sec = int(PREALERT_MIN_LIFETIME_SEC)
            record.prealert_expires_at = max(int(record.prealert_expires_at or 0), now_ts + int(PREALERT_MIN_LIFETIME_SEC))
            record.prealert_visible_to_user = bool(
                user_tier == "research"
                and major_like
                and _float(summary.get("prealert_precision_score")) >= 0.50
            )
        if stage in {"confirm", "climax", "exhaustion_risk"} and record.first_seen_stage == "prealert" and int(record.first_seen_at or 0) > 0:
            if record.prealert_to_confirm_sec is None:
                record.prealert_to_confirm_sec = max(now_ts - int(record.first_seen_at or 0), 0)
            record.prealert_lifecycle_state = "upgraded_to_confirm" if stage == "confirm" else "merged"

    def _expire_prealert_lifecycle(self, *, record: AssetMarketStateRecord, now_ts: int) -> None:
        if record.prealert_lifecycle_state in {"", "upgraded_to_confirm", "merged", "suppressed_by_lock", "delivered"}:
            return
        if int(record.prealert_expires_at or 0) > 0 and now_ts >= int(record.prealert_expires_at or 0):
            record.prealert_lifecycle_state = "expired"

    def _expire_lock(self, *, record: AssetMarketStateRecord, now_ts: int) -> None:
        if not record.no_trade_lock_active:
            return
        if int(record.no_trade_lock_until or 0) > now_ts:
            return
        record.no_trade_lock_active = False
        record.no_trade_lock_reason = ""
        record.no_trade_lock_released_by = "ttl_expired"
        record.no_trade_lock_conflict_score = 0.0
        record.no_trade_lock_conflicting_signals = []

    def _base_reason(
        self,
        *,
        summary: dict[str, Any],
        state_key: str,
        candidate_validation: dict[str, Any],
    ) -> str:
        reason = str(summary.get("trade_action_reason") or "")
        if state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
            if candidate_validation["sample_size"] < int(CHASE_ENABLE_AFTER_MIN_SAMPLES):
                return "更广方向确认已出现，但 chase 后验样本不足，先列为候选。"
            return "更广方向确认已出现，但后验 completion/followthrough 仍未稳定达标，先列为候选。"
        if state_key in {"TRADEABLE_LONG", "TRADEABLE_SHORT"}:
            return "更广方向确认已出现，且 rolling 60s 后验通过，才恢复为可顺势状态。"
        if state_key == "NO_TRADE_CHOP":
            return reason or "双向信号或结构噪音偏高，当前更适合不交易。"
        if state_key == "DATA_GAP_NO_TRADE":
            return reason or "缺 market context / outcome / evidence，不应交易。"
        return reason or "当前更适合等待更清晰的单方向结构。"

    def _base_required_confirmation(
        self,
        *,
        summary: dict[str, Any],
        state_key: str,
        candidate_validation: dict[str, Any],
    ) -> str:
        if state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
            if candidate_validation["sample_size"] < int(CHASE_ENABLE_AFTER_MIN_SAMPLES):
                return "补足 rolling 60s 样本并通过 followthrough/adverse 门槛后，才允许升级。"
            return "rolling 60s followthrough 达标且 adverse 不超标后，才允许升级。"
        if state_key in {"TRADEABLE_LONG", "TRADEABLE_SHORT"}:
            return "仅在 broader confirm 仍成立且无反向强信号时继续保留。"
        if state_key == "NO_TRADE_CHOP":
            return "等待一方形成 broader_confirm + clean dominance。"
        return str(summary.get("trade_action_required_confirmation") or "等待更清晰的单方向确认")

    def _base_invalidated_by(self, *, summary: dict[str, Any], state_key: str) -> str:
        if state_key == "NO_TRADE_CHOP":
            return "一方形成稳定 broader dominance"
        return str(summary.get("trade_action_invalidated_by") or "无")

    def _base_evidence_pack(
        self,
        *,
        summary: dict[str, Any],
        state_key: str,
        record: AssetMarketStateRecord,
    ) -> str:
        parts = [part.strip() for part in str(summary.get("trade_action_evidence_pack") or "").split("｜") if part.strip()]
        if state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
            parts.append("等待后验证明")
        if state_key == "NO_TRADE_CHOP":
            parts.append("方向混乱/高噪音")
        if record.first_seen_stage == "prealert" and record.prealert_lifecycle_state in {"active", "delivered", "upgraded_to_confirm", "merged"}:
            parts.append("first_seen=prealert")
        deduped = []
        for part in parts:
            if part not in deduped:
                deduped.append(part)
        return "｜".join(deduped[:4]) if deduped else "结构证据有限"

    def _base_confidence(
        self,
        *,
        summary: dict[str, Any],
        state_key: str,
        candidate_validation: dict[str, Any],
    ) -> float:
        base = _float(summary.get("trade_action_confidence"), default=0.55)
        if state_key in {"LONG_CANDIDATE", "SHORT_CANDIDATE"}:
            if candidate_validation["sample_size"] < int(CHASE_ENABLE_AFTER_MIN_SAMPLES):
                return round(min(base, 0.78), 3)
            return round(min(base, 0.82), 3)
        if state_key in {"TRADEABLE_LONG", "TRADEABLE_SHORT"}:
            return round(max(base, 0.84), 3)
        if state_key in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT", "DATA_GAP_NO_TRADE"}:
            return round(max(base, 0.80), 3)
        return round(base, 3)

    def _telegram_decision(
        self,
        *,
        record: AssetMarketStateRecord,
        summary: dict[str, Any],
        now_ts: int,
        state_changed: bool,
        candidate_validation: dict[str, Any],
        lock_decision: dict[str, Any],
    ) -> dict[str, Any]:
        state_key = str(record.asset_market_state_key or "")
        previous_key = str(record.previous_asset_market_state_key or "")
        suppression_reason = ""
        update_kind = "suppressed"
        state_change_reason = ""
        should_send = False

        if bool(TELEGRAM_SEND_ONLY_STATE_CHANGES):
            if not state_changed:
                suppression_reason = "same_asset_state_repeat"
            elif state_key in RISK_BLOCKER_STATES and bool(TELEGRAM_ALLOW_RISK_BLOCKERS):
                should_send = True
                update_kind = "risk_blocker"
                state_change_reason = f"{previous_key or 'none'} -> {state_key}"
            elif state_key in CANDIDATE_STATES and bool(TELEGRAM_ALLOW_CANDIDATES):
                should_send = True
                update_kind = "candidate"
                state_change_reason = f"{previous_key or 'none'} -> {state_key}"
            elif state_changed:
                should_send = True
                update_kind = "state_change"
                state_change_reason = f"{previous_key or 'none'} -> {state_key}"
        else:
            should_send = True
            update_kind = "risk_blocker" if state_key in RISK_BLOCKER_STATES else "candidate" if state_key in CANDIDATE_STATES else "state_change"
            state_change_reason = f"{previous_key or 'none'} -> {state_key}" if state_changed else "stream_passthrough"

        if should_send and record.last_telegram_state_key == state_key:
            if now_ts - int(record.last_telegram_sent_at or 0) < int(TELEGRAM_SUPPRESS_REPEAT_STATE_SEC):
                should_send = False
                suppression_reason = "repeat_state_within_suppression_window"

        if record.no_trade_lock_active and bool(NO_TRADE_LOCK_SUPPRESS_LOCAL_SIGNALS):
            if not state_changed and state_key == "NO_TRADE_LOCK":
                suppression_reason = "no_trade_lock_local_signal_suppressed"

        if state_key in OBSERVE_STATES and str(summary.get("lp_sweep_phase") or "") == "sweep_building" and not state_changed:
            suppression_reason = suppression_reason or "sweep_building_repeat_suppressed"

        if not should_send and not update_kind:
            update_kind = "suppressed"
        if not should_send and not suppression_reason:
            suppression_reason = "non_high_value_intermediate_state"
        if not should_send:
            update_kind = "suppressed"
        return {
            "telegram_should_send": bool(should_send),
            "telegram_suppression_reason": suppression_reason,
            "telegram_state_change_reason": state_change_reason,
            "telegram_update_kind": update_kind,
            "candidate_validation": candidate_validation,
            "lock_decision": lock_decision,
        }

    def _append_recent_signal(self, *, asset_symbol: str, summary: dict[str, Any], now_ts: int) -> None:
        self._recent_signals[asset_symbol].append(
            {
                "ts": now_ts,
                "signal_id": str(summary.get("signal_id") or ""),
                "direction": str(summary.get("direction") or ""),
                "trade_action_key": str(summary.get("trade_action_key") or ""),
                "strength_score": _float(summary.get("strength_score")),
                "strong": bool(self._is_strong_signal(summary)),
            }
        )

    def _prune_recent_signals(self, *, asset_symbol: str, now_ts: int) -> None:
        queue = self._recent_signals.get(asset_symbol)
        if queue is None:
            return
        window_sec = max(int(NO_TRADE_LOCK_WINDOW_SEC), 1)
        while queue and now_ts - int(queue[0].get("ts") or 0) > window_sec:
            queue.popleft()

    def _trim_records(self) -> None:
        if len(self._records) <= self.max_tracked:
            return
        ordered = sorted(
            self._records,
            key=lambda key: int(self._records[key].asset_market_state_updated_at or 0),
        )
        for key in ordered[: max(len(self._records) - self.max_tracked, 0)]:
            self._records.pop(key, None)

    def _mark_dirty(self) -> None:
        self._dirty = True

    def _serialize_record(self, record: AssetMarketStateRecord) -> dict[str, Any]:
        payload = asdict(record)
        payload["restored_from_cache"] = False
        return payload

    def _deserialize_record(self, payload: dict[str, Any]) -> AssetMarketStateRecord | None:
        if not isinstance(payload, dict):
            return None
        asset_symbol = str(payload.get("asset_symbol") or "").strip().upper()
        if not asset_symbol:
            return None
        record = AssetMarketStateRecord(asset_symbol=asset_symbol)
        for field_name in record.__dataclass_fields__:
            if field_name not in payload or field_name == "asset_symbol":
                continue
            setattr(record, field_name, payload.get(field_name))
        record.asset_symbol = asset_symbol
        record.restored_from_cache = True
        return record
