from __future__ import annotations

from collections import Counter
import math
import time
from typing import Any

from config import (
    MARKET_CONTEXT_PRIMARY_VENUE,
    OUTCOME_ALLOW_CATCHUP_WITH_LATEST_MARK,
    OUTCOME_CATCHUP_MAX_SEC,
    OUTCOME_EXPIRE_AFTER_SEC,
    OUTCOME_MARKET_CONTEXT_REFRESH_ON_DUE,
    OUTCOME_PREFER_OKX_MARK,
    OUTCOME_SCHEDULER_ENABLE,
    OUTCOME_SETTLE_BATCH_SIZE,
    OUTCOME_TICK_INTERVAL_SEC,
    OUTCOME_USE_MARKET_CONTEXT_PRICE,
    OUTCOME_WINDOW_GRACE_SEC,
)
from market_context_adapter import build_market_context_adapter


OUTCOME_WINDOWS = (30, 60, 300)
TERMINAL_STATUSES = {"completed", "unavailable", "expired"}


def _coerce_price(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0.0 or math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _direction_from_bucket(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"buy_pressure", "long", "buy", "upward"}:
        return "long"
    if raw in {"sell_pressure", "short", "sell", "downward"}:
        return "short"
    return raw or "unknown"


def _bucket_from_direction(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"long", "buy", "buy_pressure", "upward"}:
        return "buy_pressure"
    if raw in {"short", "sell", "sell_pressure", "downward"}:
        return "sell_pressure"
    return raw


def _direction_adjusted_move(raw_move: float | None, direction: str | None) -> float | None:
    if raw_move is None:
        return None
    normalized = str(direction or "").strip().lower()
    if normalized in {"short", "sell", "sell_pressure", "downward"}:
        return -float(raw_move)
    if normalized in {"long", "buy", "buy_pressure", "upward"}:
        return float(raw_move)
    return None


def _adverse_by_direction(raw_move: float | None, direction: str | None) -> bool | None:
    adjusted = _direction_adjusted_move(raw_move, direction)
    if adjusted is None:
        return None
    return bool(adjusted < 0.0)


def _result_label(status: str, followthrough: Any, adverse: Any) -> str:
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


class OutcomeScheduler:
    def __init__(
        self,
        *,
        state_manager=None,
        market_context_adapter=None,
        quality_manager=None,
        trade_opportunity_manager=None,
        enabled: bool = OUTCOME_SCHEDULER_ENABLE,
        tick_interval_sec: int = OUTCOME_TICK_INTERVAL_SEC,
        grace_sec: int = OUTCOME_WINDOW_GRACE_SEC,
        catchup_max_sec: int = OUTCOME_CATCHUP_MAX_SEC,
        settle_batch_size: int = OUTCOME_SETTLE_BATCH_SIZE,
        use_market_context_price: bool = OUTCOME_USE_MARKET_CONTEXT_PRICE,
        refresh_on_due: bool = OUTCOME_MARKET_CONTEXT_REFRESH_ON_DUE,
        prefer_okx_mark: bool = OUTCOME_PREFER_OKX_MARK,
        allow_catchup_with_latest_mark: bool = OUTCOME_ALLOW_CATCHUP_WITH_LATEST_MARK,
        expire_after_sec: int = OUTCOME_EXPIRE_AFTER_SEC,
    ) -> None:
        self.state_manager = state_manager
        self.market_context_adapter = market_context_adapter or build_market_context_adapter()
        self.quality_manager = quality_manager
        self.trade_opportunity_manager = trade_opportunity_manager
        self.enabled = bool(enabled)
        self.tick_interval_sec = max(int(tick_interval_sec or 0), 1)
        self.grace_sec = max(int(grace_sec or 0), 0)
        self.catchup_max_sec = max(int(catchup_max_sec or 0), 0)
        self.settle_batch_size = max(int(settle_batch_size or 0), 1)
        self.use_market_context_price = bool(use_market_context_price)
        self.refresh_on_due = bool(refresh_on_due)
        self.prefer_okx_mark = bool(prefer_okx_mark)
        self.allow_catchup_with_latest_mark = bool(allow_catchup_with_latest_mark)
        self.expire_after_sec = max(int(expire_after_sec or 0), 1)
        self._pending: dict[str, dict[str, Any]] = {}
        self._stats: Counter = Counter()
        self._last_tick_at: float | None = None
        self._last_error: str = ""

    def register_from_lp_record(self, record: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not self.enabled or not isinstance(record, dict) or not record:
            return []
        created: list[dict[str, Any]] = []
        signal_id = str(record.get("signal_id") or record.get("record_id") or "").strip()
        if not signal_id:
            return []
        windows = record.get("outcome_windows") if isinstance(record.get("outcome_windows"), dict) else {}
        created_at = int(float(record.get("created_at") or time.time()))
        for window_sec in OUTCOME_WINDOWS:
            window = dict(windows.get(f"{window_sec}s") or {})
            created.append(
                self.register_pending_outcome(
                    outcome_id=str(window.get("outcome_id") or f"outcome_{signal_id}_{window_sec}"),
                    signal_id=signal_id,
                    trade_opportunity_id=str(record.get("trade_opportunity_id") or ""),
                    asset=str(record.get("asset_symbol") or record.get("asset") or ""),
                    pair=str(record.get("pair_label") or record.get("pair") or ""),
                    direction=_direction_from_bucket(str(record.get("direction_bucket") or record.get("direction") or "")),
                    window_sec=window_sec,
                    due_at=int(float(window.get("due_at") or (created_at + window_sec))),
                    status=str(window.get("status") or "pending"),
                    start_price=_coerce_price(window.get("price_start") if window.get("price_start") is not None else record.get("outcome_price_start")),
                    start_price_source=str(window.get("start_price_source") or window.get("price_source") or record.get("outcome_price_source") or "unavailable"),
                    created_at=created_at,
                    pool_address=str(record.get("pool_address") or ""),
                    lp_record=record,
                )
            )
        return [item for item in created if item]

    def register_pending_outcome(
        self,
        *,
        outcome_id: str,
        signal_id: str,
        asset: str,
        pair: str,
        direction: str,
        window_sec: int,
        due_at: int,
        start_price: float | None,
        start_price_source: str,
        created_at: int | float | None = None,
        trade_opportunity_id: str | None = None,
        status: str = "pending",
        pool_address: str = "",
        lp_record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {}
        normalized_signal_id = str(signal_id or "").strip()
        if not normalized_signal_id:
            return {}
        normalized_window = int(window_sec)
        normalized_outcome_id = str(outcome_id or f"outcome_{normalized_signal_id}_{normalized_window}").strip()
        created_ts = int(float(created_at or time.time()))
        row = {
            "outcome_id": normalized_outcome_id,
            "signal_id": normalized_signal_id,
            "trade_opportunity_id": str(trade_opportunity_id or ""),
            "asset": str(asset or ""),
            "pair": str(pair or ""),
            "direction": _direction_from_bucket(direction),
            "window_sec": normalized_window,
            "due_at": int(due_at),
            "status": str(status or "pending"),
            "start_price": _coerce_price(start_price),
            "start_price_source": str(start_price_source or "unavailable"),
            "created_at": created_ts,
            "pool_address": str(pool_address or ""),
            "price_source": str(start_price_source or "unavailable"),
            "outcome_price_source": str(start_price_source or "unavailable"),
            "failure_reason": "",
        }
        if row["start_price"] is None:
            row["status"] = "unavailable"
            row["failure_reason"] = "market_price_unavailable"
            row["completed_at"] = created_ts
        if row["status"] == "pending":
            self._pending[normalized_outcome_id] = dict(row)
            self._stats["registered_pending_count"] += 1
        self._sync_state_pending(row, lp_record=lp_record)
        self._write_sqlite_outcome(row)
        if row.get("trade_opportunity_id"):
            self._write_sqlite_opportunity_outcome(row)
        return dict(row)

    def list_due_outcomes(self, now: int | float | None = None) -> list[dict[str, Any]]:
        reference = int(now or time.time())
        pending = {
            str(row.get("outcome_id") or ""): dict(row)
            for row in self._pending.values()
            if str(row.get("status") or "") == "pending"
        }
        for row in self._load_pending_from_sqlite(limit=self.settle_batch_size * 4):
            outcome_id = str(row.get("outcome_id") or "")
            if outcome_id and outcome_id not in pending:
                pending[outcome_id] = dict(row)
        due = [
            row
            for row in pending.values()
            if int(float(row.get("due_at") or 0)) <= reference
        ]
        return sorted(
            due,
            key=lambda item: (
                int(float(item.get("due_at") or 0)),
                str(item.get("signal_id") or ""),
                int(float(item.get("window_sec") or 0)),
            ),
        )

    def settle_due_outcomes(self, now: int | float | None = None, limit: int | None = None) -> dict[str, Any]:
        if not self.enabled:
            return self.outcome_scheduler_health_summary(now=now)
        reference = int(now or time.time())
        self._last_tick_at = float(reference)
        due = self.list_due_outcomes(reference)[: max(int(limit or self.settle_batch_size), 1)]
        settled: list[dict[str, Any]] = []
        for row in due:
            settled.append(self.settle_one_outcome(row, now=reference))
        return {
            "processed_count": len(settled),
            "completed_count": sum(1 for item in settled if item.get("status") == "completed"),
            "unavailable_count": sum(1 for item in settled if item.get("status") == "unavailable"),
            "expired_count": sum(1 for item in settled if item.get("status") == "expired"),
            "pending_count": self.pending_count(),
        }

    def settle_one_outcome(
        self,
        outcome: dict[str, Any],
        *,
        now: int | float | None = None,
        catchup: bool = False,
    ) -> dict[str, Any]:
        reference = int(now or time.time())
        row = self._normalize_outcome_row(outcome)
        outcome_id = str(row.get("outcome_id") or "")
        if not outcome_id:
            return {}
        if str(row.get("status") or "pending") != "pending":
            self._pending.pop(outcome_id, None)
            return dict(row)
        due_at = int(float(row.get("due_at") or 0))
        age_after_due = max(reference - due_at, 0)
        if catchup and age_after_due > self.catchup_max_sec:
            return self._expire_row(row, reference, "catchup_window_exceeded", catchup=True)
        if not catchup and age_after_due > self.expire_after_sec:
            return self._expire_row(row, reference, "pending_not_processed", catchup=False)

        price, source, failure_reason = self._resolve_end_price(row, now=reference, catchup=catchup)
        if price is None:
            return self._mark_unavailable_row(
                row,
                reference,
                failure_reason or "market_price_unavailable",
                price_source=source or "unavailable",
                catchup=catchup,
            )
        return self._complete_row(row, price, source, reference, catchup=catchup)

    def catchup_due_outcomes(self, now: int | float | None = None, limit: int | None = None) -> dict[str, Any]:
        if not self.enabled:
            return self.outcome_scheduler_health_summary(now=now)
        reference = int(now or time.time())
        pending = [
            row
            for row in self._load_pending_from_sqlite(limit=max(int(limit or self.settle_batch_size), 1) * 4)
            if int(float(row.get("due_at") or 0)) <= reference
        ]
        settled = []
        for row in pending[: max(int(limit or self.settle_batch_size), 1)]:
            due_at = int(float(row.get("due_at") or 0))
            if reference - due_at > self.catchup_max_sec:
                settled.append(self._expire_row(self._normalize_outcome_row(row), reference, "catchup_window_exceeded", catchup=True))
                continue
            settled.append(self.settle_one_outcome(row, now=reference, catchup=True))
        return {
            "processed_count": len(settled),
            "catchup_completed_count": sum(1 for item in settled if item.get("status") == "completed"),
            "catchup_expired_count": sum(1 for item in settled if item.get("status") == "expired"),
            "catchup_unavailable_count": sum(1 for item in settled if item.get("status") == "unavailable"),
            "pending_count": self.pending_count(),
        }

    def restore_pending_outcomes_from_sqlite(
        self,
        *,
        now: int | float | None = None,
        catchup: bool = True,
        limit: int | None = None,
    ) -> dict[str, Any]:
        rows = self._load_pending_from_sqlite(limit=limit)
        for row in rows:
            normalized = self._normalize_outcome_row(row)
            if str(normalized.get("status") or "") == "pending":
                self._pending[str(normalized.get("outcome_id") or "")] = normalized
        restored_records = self._restore_state_records_from_rows(rows)
        result = {
            "restored_pending_count": len(rows),
            "restored_state_record_count": restored_records,
            "catchup": {},
        }
        if catchup:
            result["catchup"] = self.catchup_due_outcomes(now=now, limit=limit or self.settle_batch_size)
        return result

    def outcome_scheduler_health_summary(self, now: int | float | None = None) -> dict[str, Any]:
        reference = int(now or time.time())
        status_counts = Counter()
        due_count = 0
        for row in self._pending.values():
            status_counts[str(row.get("status") or "pending")] += 1
            if str(row.get("status") or "") == "pending" and int(float(row.get("due_at") or 0)) <= reference:
                due_count += 1
        return {
            "enabled": bool(self.enabled),
            "tick_interval_sec": int(self.tick_interval_sec),
            "grace_sec": int(self.grace_sec),
            "catchup_max_sec": int(self.catchup_max_sec),
            "settle_batch_size": int(self.settle_batch_size),
            "pending_count": int(status_counts.get("pending", 0)),
            "due_count": int(due_count),
            "registered_pending_count": int(self._stats.get("registered_pending_count", 0)),
            "completed_count": int(self._stats.get("completed_count", 0)),
            "unavailable_count": int(self._stats.get("unavailable_count", 0)),
            "expired_count": int(self._stats.get("expired_count", 0)),
            "catchup_completed_count": int(self._stats.get("catchup_completed_count", 0)),
            "catchup_expired_count": int(self._stats.get("catchup_expired_count", 0)),
            "sqlite_write_failed_count": int(self._stats.get("sqlite_write_failed_count", 0)),
            "last_tick_at": self._last_tick_at,
            "last_error": self._last_error,
        }

    def pending_count(self) -> int:
        return sum(1 for row in self._pending.values() if str(row.get("status") or "") == "pending")

    def _normalize_outcome_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row or {})
        normalized["outcome_id"] = str(
            normalized.get("outcome_id")
            or f"outcome_{normalized.get('signal_id')}_{int(float(normalized.get('window_sec') or 0))}"
        )
        normalized["signal_id"] = str(normalized.get("signal_id") or normalized.get("record_id") or "")
        normalized["trade_opportunity_id"] = str(normalized.get("trade_opportunity_id") or "")
        normalized["asset"] = str(normalized.get("asset") or normalized.get("asset_symbol") or "")
        normalized["pair"] = str(normalized.get("pair") or normalized.get("pair_label") or "")
        normalized["direction"] = _direction_from_bucket(str(normalized.get("direction") or normalized.get("direction_bucket") or ""))
        normalized["window_sec"] = int(float(normalized.get("window_sec") or 0))
        normalized["due_at"] = int(float(normalized.get("due_at") or (float(normalized.get("created_at") or time.time()) + normalized["window_sec"])))
        normalized["created_at"] = int(float(normalized.get("created_at") or (normalized["due_at"] - normalized["window_sec"])))
        normalized["start_price"] = _coerce_price(normalized.get("start_price") if normalized.get("start_price") is not None else normalized.get("price_start"))
        normalized["start_price_source"] = str(normalized.get("start_price_source") or normalized.get("price_source") or normalized.get("outcome_price_source") or "unavailable")
        normalized["pool_address"] = str(normalized.get("pool_address") or "")
        normalized["status"] = str(normalized.get("status") or "pending")
        return normalized

    def _resolve_end_price(self, row: dict[str, Any], *, now: int, catchup: bool) -> tuple[float | None, str, str]:
        if self.use_market_context_price and (self.refresh_on_due or catchup):
            if catchup and not self.allow_catchup_with_latest_mark:
                return None, "unavailable", "pending_not_processed"
            token_or_pair = str(row.get("pair") or row.get("asset") or "")
            if token_or_pair:
                try:
                    context = self.market_context_adapter.get_market_context(
                        token_or_pair,
                        now,
                        venue=MARKET_CONTEXT_PRIMARY_VENUE,
                    )
                except Exception as exc:
                    self._last_error = f"market_context_error:{exc.__class__.__name__}"
                    return None, "unavailable", "market_context_error"
                price, source = self._select_price_from_market_context(context)
                if price is not None:
                    return price, source, ""
                failure_reason = self._normalize_market_failure(context)
                if failure_reason == "symbol_unavailable":
                    return None, "unavailable", "symbol_unavailable"
        pool_price = self._latest_pool_quote_proxy(row, now=now)
        if pool_price is not None:
            return pool_price, "pool_quote_proxy", ""
        return None, "unavailable", "market_price_unavailable"

    def _select_price_from_market_context(self, context: dict[str, Any]) -> tuple[float | None, str]:
        venue = str(context.get("market_context_venue") or "").strip().lower()
        mark_price = _coerce_price(context.get("perp_mark_price"))
        index_price = _coerce_price(context.get("perp_index_price"))
        last_price = _coerce_price(context.get("perp_last_price"))
        spot_price = _coerce_price(context.get("spot_reference_price"))
        if venue == "okx_perp":
            if self.prefer_okx_mark and mark_price is not None:
                return mark_price, "okx_mark"
            if index_price is not None:
                return index_price, "okx_index"
            if last_price is not None:
                return last_price, "okx_last"
            if mark_price is not None:
                return mark_price, "okx_mark"
        if venue == "kraken_futures":
            if mark_price is not None:
                return mark_price, "kraken_mark"
            if index_price is not None:
                return index_price, "kraken_index"
            if last_price is not None:
                return last_price, "kraken_last"
        if spot_price is not None:
            return spot_price, "spot_reference_price"
        if mark_price is not None:
            return mark_price, "market_context_mark"
        if index_price is not None:
            return index_price, "market_context_index"
        if last_price is not None:
            return last_price, "market_context_last"
        return None, "unavailable"

    def _normalize_market_failure(self, context: dict[str, Any]) -> str:
        reason = str(context.get("market_context_failure_reason") or "").strip().lower()
        if reason in {"no_symbol", "market_symbol_unavailable", "symbol_mismatch"}:
            return "symbol_unavailable"
        if reason:
            return "market_context_error"
        source = str(context.get("market_context_source") or "").strip().lower()
        if source == "unavailable":
            return "market_price_unavailable"
        return "unknown"

    def _latest_pool_quote_proxy(self, row: dict[str, Any], *, now: int) -> float | None:
        if self.state_manager is None or not hasattr(self.state_manager, "get_latest_pool_quote_proxy"):
            return None
        pool_address = str(row.get("pool_address") or "").strip()
        if not pool_address:
            return None
        try:
            return self.state_manager.get_latest_pool_quote_proxy(
                pool_address,
                max_age_sec=self.expire_after_sec,
                now_ts=now,
            )
        except Exception:
            return None

    def _complete_row(self, row: dict[str, Any], end_price: float, source: str, completed_at: int, *, catchup: bool) -> dict[str, Any]:
        start_price = _coerce_price(row.get("start_price"))
        if start_price is None:
            return self._mark_unavailable_row(row, completed_at, "market_price_unavailable", price_source=source, catchup=catchup)
        raw_move = (float(end_price) / float(start_price)) - 1.0
        adjusted = _direction_adjusted_move(raw_move, str(row.get("direction") or ""))
        adverse = _adverse_by_direction(raw_move, str(row.get("direction") or ""))
        followthrough = bool(adjusted is not None and adjusted > 0.002)
        reversal = bool(adjusted is not None and adjusted < -0.002)
        result = {
            **row,
            "status": "completed",
            "end_price": round(float(end_price), 8),
            "price_end": round(float(end_price), 8),
            "raw_move": round(float(raw_move), 6),
            "direction_adjusted_move": round(float(adjusted), 6) if adjusted is not None else None,
            "followthrough": followthrough,
            "adverse": adverse,
            "reversal": reversal,
            "price_source": str(source or "unavailable"),
            "end_price_source": str(source or "unavailable"),
            "outcome_price_source": str(source or "unavailable"),
            "completed_at": int(completed_at),
            "failure_reason": "",
            "settled_by": "outcome_scheduler_catchup" if catchup else "outcome_scheduler",
            "catchup": bool(catchup),
        }
        self._sync_terminal_state(result)
        self._stats["completed_count"] += 1
        if catchup:
            self._stats["catchup_completed_count"] += 1
        return result

    def _mark_unavailable_row(
        self,
        row: dict[str, Any],
        completed_at: int,
        reason: str,
        *,
        price_source: str = "unavailable",
        catchup: bool,
    ) -> dict[str, Any]:
        normalized_reason = self._normalize_failure_reason(reason)
        result = {
            **row,
            "status": "unavailable",
            "price_source": str(price_source or "unavailable"),
            "end_price_source": str(price_source or "unavailable"),
            "outcome_price_source": str(price_source or "unavailable"),
            "completed_at": int(completed_at),
            "failure_reason": normalized_reason,
            "settled_by": "outcome_scheduler_catchup" if catchup else "outcome_scheduler",
            "catchup": bool(catchup),
        }
        self._sync_terminal_state(result)
        self._stats["unavailable_count"] += 1
        return result

    def _expire_row(self, row: dict[str, Any], completed_at: int, reason: str, *, catchup: bool) -> dict[str, Any]:
        normalized_reason = self._normalize_failure_reason(reason)
        result = {
            **row,
            "status": "expired",
            "completed_at": int(completed_at),
            "failure_reason": normalized_reason,
            "settled_by": "outcome_scheduler_catchup" if catchup else "outcome_scheduler",
            "catchup": bool(catchup),
        }
        self._sync_terminal_state(result)
        self._stats["expired_count"] += 1
        if catchup:
            self._stats["catchup_expired_count"] += 1
        return result

    def _sync_terminal_state(self, row: dict[str, Any]) -> None:
        outcome_id = str(row.get("outcome_id") or "")
        self._pending.pop(outcome_id, None)
        state_payload = {}
        record_id = str(row.get("signal_id") or "")
        if self.state_manager is not None:
            try:
                if row.get("status") == "completed" and hasattr(self.state_manager, "complete_lp_outcome_window"):
                    state_payload = self.state_manager.complete_lp_outcome_window(
                        record_id,
                        window_sec=int(row.get("window_sec") or 0),
                        end_price=float(row.get("end_price") or row.get("price_end") or 0.0),
                        price_source=str(row.get("outcome_price_source") or row.get("price_source") or "unavailable"),
                        completed_at=int(row.get("completed_at") or time.time()),
                        settled_by=str(row.get("settled_by") or "outcome_scheduler"),
                        catchup=bool(row.get("catchup")),
                    )
                elif row.get("status") == "expired" and hasattr(self.state_manager, "expire_lp_outcome_window"):
                    state_payload = self.state_manager.expire_lp_outcome_window(
                        record_id,
                        window_sec=int(row.get("window_sec") or 0),
                        completed_at=int(row.get("completed_at") or time.time()),
                        reason=str(row.get("failure_reason") or "pending_not_processed"),
                        settled_by=str(row.get("settled_by") or "outcome_scheduler"),
                        catchup=bool(row.get("catchup")),
                    )
                elif hasattr(self.state_manager, "mark_lp_outcome_window_unavailable"):
                    state_payload = self.state_manager.mark_lp_outcome_window_unavailable(
                        record_id,
                        window_sec=int(row.get("window_sec") or 0),
                        completed_at=int(row.get("completed_at") or time.time()),
                        reason=str(row.get("failure_reason") or "market_price_unavailable"),
                        price_source=str(row.get("outcome_price_source") or row.get("price_source") or "unavailable"),
                        settled_by=str(row.get("settled_by") or "outcome_scheduler"),
                        catchup=bool(row.get("catchup")),
                    )
            except Exception as exc:
                self._last_error = f"state_sync_error:{exc.__class__.__name__}"
        if not state_payload:
            self._write_sqlite_outcome(row)
        if row.get("trade_opportunity_id"):
            self._write_sqlite_opportunity_outcome(row)
        self._sync_quality_and_opportunity_cache()

    def _sync_state_pending(self, row: dict[str, Any], *, lp_record: dict[str, Any] | None = None) -> None:
        if self.state_manager is None:
            return
        try:
            if lp_record and hasattr(self.state_manager, "register_lp_outcome_pending_window"):
                self.state_manager.register_lp_outcome_pending_window(
                    str(row.get("signal_id") or ""),
                    window_sec=int(row.get("window_sec") or 0),
                    outcome_id=str(row.get("outcome_id") or ""),
                    due_at=int(row.get("due_at") or 0),
                    trade_opportunity_id=str(row.get("trade_opportunity_id") or ""),
                    start_price_source=str(row.get("start_price_source") or ""),
                )
        except Exception as exc:
            self._last_error = f"state_pending_sync_error:{exc.__class__.__name__}"

    def _sync_quality_and_opportunity_cache(self) -> None:
        if self.quality_manager is not None and hasattr(self.quality_manager, "sync_from_state_manager"):
            try:
                self.quality_manager.sync_from_state_manager(force=True)
            except Exception as exc:
                self._last_error = f"quality_sync_error:{exc.__class__.__name__}"
        if self.trade_opportunity_manager is not None and hasattr(self.trade_opportunity_manager, "_refresh_recent_outcomes"):
            try:
                self.trade_opportunity_manager._refresh_recent_outcomes()
                if hasattr(self.trade_opportunity_manager, "flush"):
                    self.trade_opportunity_manager.flush(force=True)
            except Exception as exc:
                self._last_error = f"opportunity_sync_error:{exc.__class__.__name__}"

    def _write_sqlite_outcome(self, row: dict[str, Any]) -> bool:
        try:
            import sqlite_store

            ok = bool(sqlite_store.upsert_outcome_window(row))
            if not ok and bool(getattr(sqlite_store, "SQLITE_ENABLE", False)) and bool(getattr(sqlite_store, "SQLITE_WRITE_OUTCOMES", False)):
                self._stats["sqlite_write_failed_count"] += 1
            return ok
        except Exception as exc:
            self._stats["sqlite_write_failed_count"] += 1
            self._last_error = f"sqlite_write_failed:{exc.__class__.__name__}"
            return False

    def _write_sqlite_opportunity_outcome(self, row: dict[str, Any]) -> bool:
        try:
            import sqlite_store

            payload = self._opportunity_payload(row)
            ok = bool(sqlite_store.upsert_opportunity_outcome(payload))
            if not ok and bool(getattr(sqlite_store, "SQLITE_ENABLE", False)):
                self._stats["sqlite_write_failed_count"] += 1
            return ok
        except Exception as exc:
            self._stats["sqlite_write_failed_count"] += 1
            self._last_error = f"sqlite_opportunity_write_failed:{exc.__class__.__name__}"
            return False

    def _opportunity_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        followthrough = row.get("followthrough")
        adverse = row.get("adverse")
        return {
            "trade_opportunity_id": str(row.get("trade_opportunity_id") or ""),
            "window_sec": int(row.get("window_sec") or 0),
            "due_at": row.get("due_at"),
            "start_price": row.get("start_price"),
            "end_price": row.get("end_price") or row.get("price_end"),
            "start_price_source": row.get("start_price_source"),
            "end_price_source": row.get("end_price_source"),
            "outcome_price_source": row.get("outcome_price_source") or row.get("price_source"),
            "price_source": row.get("price_source") or row.get("outcome_price_source"),
            "raw_move": row.get("raw_move"),
            "direction_adjusted_move": row.get("direction_adjusted_move"),
            "followthrough": followthrough,
            "adverse": adverse,
            "reversal": row.get("reversal"),
            "result_label": _result_label(str(row.get("status") or ""), followthrough, adverse),
            "status": row.get("status"),
            "failure_reason": row.get("failure_reason"),
            "completed_at": row.get("completed_at"),
            "created_at": row.get("created_at"),
            "settled_by": row.get("settled_by"),
            "catchup": row.get("catchup"),
        }

    def _load_pending_from_sqlite(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        try:
            import sqlite_store

            return [dict(row) for row in sqlite_store.load_pending_outcome_rows(limit=limit)]
        except Exception as exc:
            self._last_error = f"sqlite_pending_load_failed:{exc.__class__.__name__}"
            return []

    def _restore_state_records_from_rows(self, rows: list[dict[str, Any]]) -> int:
        if self.state_manager is None or not hasattr(self.state_manager, "restore_lp_outcome_records"):
            return 0
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            normalized = self._normalize_outcome_row(row)
            signal_id = str(normalized.get("signal_id") or "")
            if not signal_id:
                continue
            record = grouped.setdefault(
                signal_id,
                {
                    "schema_version": "lp_alert_outcome_v1",
                    "record_id": signal_id,
                    "signal_id": signal_id,
                    "trade_opportunity_id": str(normalized.get("trade_opportunity_id") or ""),
                    "asset_symbol": str(normalized.get("asset") or ""),
                    "pair_label": str(normalized.get("pair") or ""),
                    "direction_bucket": _bucket_from_direction(str(normalized.get("direction") or "")),
                    "created_at": int(normalized.get("created_at") or 0),
                    "outcome_price_source": str(normalized.get("start_price_source") or normalized.get("outcome_price_source") or "unavailable"),
                    "outcome_price_start": normalized.get("start_price"),
                    "outcome_price_end": None,
                    "outcome_window_status": "pending",
                    "outcome_failure_reason": "",
                    "outcome_windows": {},
                },
            )
            record["outcome_windows"][f"{int(normalized.get('window_sec') or 0)}s"] = {
                "outcome_id": str(normalized.get("outcome_id") or ""),
                "window_sec": int(normalized.get("window_sec") or 0),
                "due_at": int(normalized.get("due_at") or 0),
                "status": str(normalized.get("status") or "pending"),
                "price_source": str(normalized.get("price_source") or normalized.get("outcome_price_source") or normalized.get("start_price_source") or "unavailable"),
                "start_price_source": str(normalized.get("start_price_source") or "unavailable"),
                "end_price_source": str(normalized.get("end_price_source") or ""),
                "price_start": normalized.get("start_price"),
                "price_end": normalized.get("end_price"),
                "raw_move_after": normalized.get("raw_move"),
                "direction_adjusted_move_after": normalized.get("direction_adjusted_move"),
                "followthrough_positive": normalized.get("followthrough"),
                "adverse_by_direction": normalized.get("adverse"),
                "reversal": normalized.get("reversal"),
                "completed_at": normalized.get("completed_at"),
                "failure_reason": str(normalized.get("failure_reason") or ""),
                "settled_by": str(normalized.get("settled_by") or ""),
                "catchup": bool(normalized.get("catchup")),
            }
        try:
            return int(self.state_manager.restore_lp_outcome_records(list(grouped.values())) or 0)
        except Exception as exc:
            self._last_error = f"state_restore_error:{exc.__class__.__name__}"
            return 0

    def _normalize_failure_reason(self, reason: str | None) -> str:
        normalized = str(reason or "").strip()
        if normalized in {
            "market_price_unavailable",
            "pending_not_processed",
            "catchup_window_exceeded",
            "symbol_unavailable",
            "market_context_error",
            "sqlite_write_failed",
            "window_elapsed_without_price_update",
        }:
            return normalized
        if not normalized:
            return "unknown"
        if normalized in {"price_unavailable", "live_market_price_unavailable", "market_context_and_pool_price_unavailable"}:
            return "market_price_unavailable"
        if normalized in {"no_symbol", "market_symbol_unavailable", "symbol_mismatch"}:
            return "symbol_unavailable"
        if normalized.startswith("market_context"):
            return "market_context_error"
        return normalized or "unknown"


_DEFAULT_SCHEDULER: OutcomeScheduler | None = None


def get_default_scheduler(**kwargs) -> OutcomeScheduler:
    global _DEFAULT_SCHEDULER
    if _DEFAULT_SCHEDULER is None:
        _DEFAULT_SCHEDULER = OutcomeScheduler(**kwargs)
    return _DEFAULT_SCHEDULER


def register_pending_outcome(*args, **kwargs) -> dict[str, Any]:
    return get_default_scheduler().register_pending_outcome(*args, **kwargs)


def list_due_outcomes(now: int | float | None = None) -> list[dict[str, Any]]:
    return get_default_scheduler().list_due_outcomes(now=now)


def settle_due_outcomes(now: int | float | None = None, limit: int | None = None) -> dict[str, Any]:
    return get_default_scheduler().settle_due_outcomes(now=now, limit=limit)


def settle_one_outcome(outcome: dict[str, Any], *, now: int | float | None = None) -> dict[str, Any]:
    return get_default_scheduler().settle_one_outcome(outcome, now=now)


def catchup_due_outcomes(now: int | float | None = None, limit: int | None = None) -> dict[str, Any]:
    return get_default_scheduler().catchup_due_outcomes(now=now, limit=limit)


def restore_pending_outcomes_from_sqlite(now: int | float | None = None) -> dict[str, Any]:
    return get_default_scheduler().restore_pending_outcomes_from_sqlite(now=now)


def outcome_scheduler_health_summary(now: int | float | None = None) -> dict[str, Any]:
    return get_default_scheduler().outcome_scheduler_health_summary(now=now)
