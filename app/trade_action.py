from __future__ import annotations

from typing import Any

from config import (
    TRADE_ACTION_CONFLICT_WINDOW_SEC,
    TRADE_ACTION_ENABLE,
    TRADE_ACTION_MAX_LONG_BASIS_BPS,
    TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE,
    TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE,
    TRADE_ACTION_MIN_SHORT_BASIS_BPS,
    TRADE_ACTION_REQUIRE_BROADER_CONFIRM_FOR_CHASE,
    TRADE_ACTION_REQUIRE_LIVE_CONTEXT_FOR_CHASE,
)
from lp_product_helpers import asset_symbol_from_event, is_major_asset_symbol


TRADE_ACTION_SOURCE = "trade_action_layer.lp_v1"
LP_TRADE_ACTION_STAGES = {"prealert", "confirm", "climax", "exhaustion_risk"}
TRADE_ACTION_LABELS = {
    "NO_TRADE": "不交易",
    "WAIT_CONFIRMATION": "等待确认",
    "DATA_GAP_NO_TRADE": "数据缺口，不交易",
    "CONFLICT_NO_TRADE": "不交易",
    "DO_NOT_CHASE_LONG": "不追多",
    "DO_NOT_CHASE_SHORT": "不追空",
    "LONG_BIAS_OBSERVE": "偏多观察",
    "SHORT_BIAS_OBSERVE": "偏空观察",
    "LONG_CHASE_ALLOWED": "可顺势追多",
    "SHORT_CHASE_ALLOWED": "可顺势追空",
    "REVERSAL_WATCH_LONG": "观察反抽",
    "REVERSAL_WATCH_SHORT": "观察回吐",
}
TRADE_ACTION_DIRECTIONS = {
    "NO_TRADE": "neutral",
    "WAIT_CONFIRMATION": "neutral",
    "DATA_GAP_NO_TRADE": "neutral",
    "CONFLICT_NO_TRADE": "neutral",
    "DO_NOT_CHASE_LONG": "long",
    "DO_NOT_CHASE_SHORT": "short",
    "LONG_BIAS_OBSERVE": "long",
    "SHORT_BIAS_OBSERVE": "short",
    "LONG_CHASE_ALLOWED": "long",
    "SHORT_CHASE_ALLOWED": "short",
    "REVERSAL_WATCH_LONG": "long",
    "REVERSAL_WATCH_SHORT": "short",
}
CHASE_ALLOWED_KEYS = {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
NO_TRADE_KEYS = {"NO_TRADE", "DATA_GAP_NO_TRADE", "CONFLICT_NO_TRADE"}
_HIGH_SWEEP_CONTINUATION_SCORE = 0.72
_CONFLICT_SIGNAL_STRENGTH_MIN = 0.45
_STRONG_SIDE_OVERRIDE_STRENGTH = 0.78
_WEAK_SIDE_STRENGTH_MAX = 0.55


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


def _split_segments(text: str) -> list[str]:
    return [part.strip() for part in str(text or "").split("｜") if part.strip()]


def _direction_from_values(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip().lower()
        if normalized in {"buy_pressure", "buy", "upward", "long"}:
            return "long"
        if normalized in {"sell_pressure", "sell", "downward", "short"}:
            return "short"
    return "neutral"


def _expected_broader_absorption(direction: str) -> str:
    if direction == "long":
        return "broader_buy_pressure_confirmed"
    if direction == "short":
        return "broader_sell_pressure_confirmed"
    return ""


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


def _quality_samples_insufficient(context: dict[str, Any]) -> bool:
    actionable_size = _int(context.get("quality_actionable_sample_size"), default=0)
    if actionable_size <= 0:
        return False
    scope_sizes = [
        _int(context.get("quality_scope_pool_size"), default=0),
        _int(context.get("quality_scope_pair_size"), default=0),
        _int(context.get("quality_scope_asset_size"), default=0),
    ]
    known_sizes = [size for size in scope_sizes if size > 0]
    return bool(known_sizes) and min(known_sizes) < actionable_size


def _trade_action_strength(context: dict[str, Any]) -> float:
    direction = str(context.get("direction") or "neutral")
    if direction not in {"long", "short"}:
        return 0.0
    strength = 0.18
    if context.get("market_context_source") == "live_public":
        strength += 0.12
    if context.get("lp_confirm_scope") == "broader_confirm":
        strength += 0.18
    if context.get("lp_confirm_quality") == "clean_confirm":
        strength += 0.16
    if context.get("lp_sweep_phase") == "sweep_confirmed":
        strength += min(_float(context.get("lp_sweep_continuation_score")) / 0.9, 1.0) * 0.16
    if context.get("lp_absorption_context") == _expected_broader_absorption(direction):
        strength += 0.12
    if _float(context.get("asset_case_quality_score")) >= float(TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE):
        strength += 0.08
    if _float(context.get("pair_quality_score")) >= float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE):
        strength += 0.06
    if _bool(context.get("major_asset")):
        strength += 0.05
    if _text(context.get("lp_alert_stage")) == "prealert" or _text(context.get("lp_sweep_phase")) == "sweep_building":
        strength -= 0.18
    if _text(context.get("lp_alert_stage")) == "exhaustion_risk" or _text(context.get("lp_sweep_phase")) == "sweep_exhaustion_risk":
        strength -= 0.22
    if _text(context.get("lp_confirm_quality")) in {"late_confirm", "chase_risk"}:
        strength -= 0.16
    if _text(context.get("alert_relative_timing")) == "late":
        strength -= 0.10
    if _text(context.get("lp_absorption_context")).startswith("local_"):
        strength -= 0.12
    if _text(context.get("lp_absorption_context")) == "pool_only_unconfirmed_pressure":
        strength -= 0.10
    if _has_crowding(
        direction,
        context.get("basis_bps"),
        context.get("mark_index_spread_bps"),
    ):
        strength -= 0.08
    return max(0.0, min(1.0, round(strength, 3)))


def _conflict_signal_qualifies(summary: dict[str, Any]) -> bool:
    return bool(
        summary.get("strength_score", 0.0) >= _CONFLICT_SIGNAL_STRENGTH_MIN
        or summary.get("lp_sweep_phase") in {"sweep_building", "sweep_confirmed", "sweep_exhaustion_risk"}
        or summary.get("lp_confirm_scope") in {"local_confirm", "broader_confirm"}
        or str(summary.get("lp_absorption_context") or "").startswith(("local_", "broader_"))
    )


def _detect_conflict(context: dict[str, Any], recent_signals: list[dict[str, Any]] | None) -> dict[str, Any]:
    direction = str(context.get("direction") or "neutral")
    asset_symbol = str(context.get("asset_symbol") or "")
    now_ts = _int(context.get("event_ts"), default=0)
    window_sec = _int(context.get("lp_conflict_window_sec"), default=TRADE_ACTION_CONFLICT_WINDOW_SEC)
    current_strength = _trade_action_strength(context)
    opposite_signals: list[dict[str, Any]] = []
    for item in recent_signals or []:
        if str(item.get("asset_symbol") or "") != asset_symbol:
            continue
        item_direction = str(item.get("direction") or "neutral")
        if item_direction not in {"long", "short"} or item_direction == direction:
            continue
        if now_ts > 0 and abs(now_ts - _int(item.get("ts"), default=0)) > window_sec:
            continue
        if not _conflict_signal_qualifies(item):
            continue
        opposite_signals.append(dict(item))
    if not opposite_signals:
        return {
            "lp_conflict_context": "",
            "lp_conflict_score": 0.0,
            "lp_conflict_window_sec": window_sec,
            "lp_conflicting_signals": [],
            "lp_conflict_resolution": "no_conflict",
        }

    strongest_opposite = max(_float(item.get("strength_score")) for item in opposite_signals)
    current_override = bool(
        current_strength >= _STRONG_SIDE_OVERRIDE_STRENGTH
        and _text(context.get("market_context_source")) == "live_public"
        and _text(context.get("lp_confirm_scope")) == "broader_confirm"
        and _float(context.get("asset_case_quality_score")) >= float(TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE)
        and strongest_opposite <= _WEAK_SIDE_STRENGTH_MAX
    )
    if current_override:
        return {
            "lp_conflict_context": "weak_opposite_signal_overridden",
            "lp_conflict_score": round(strongest_opposite, 3),
            "lp_conflict_window_sec": window_sec,
            "lp_conflicting_signals": [
                {
                    "signal_id": str(item.get("signal_id") or ""),
                    "direction": str(item.get("direction") or ""),
                    "stage": str(item.get("lp_alert_stage") or ""),
                    "trade_action_key": str(item.get("trade_action_key") or ""),
                }
                for item in opposite_signals[:4]
            ],
            "lp_conflict_resolution": "strong_side_override",
        }

    conflict_score = min(
        0.98,
        0.52 + max(current_strength, strongest_opposite) * 0.30 + min(len(opposite_signals) / 3.0, 1.0) * 0.12,
    )
    return {
        "lp_conflict_context": "opposite_lp_signals_within_window",
        "lp_conflict_score": round(conflict_score, 3),
        "lp_conflict_window_sec": window_sec,
        "lp_conflicting_signals": [
            {
                "signal_id": str(item.get("signal_id") or ""),
                "direction": str(item.get("direction") or ""),
                "stage": str(item.get("lp_alert_stage") or ""),
                "trade_action_key": str(item.get("trade_action_key") or ""),
            }
            for item in opposite_signals[:4]
        ],
        "lp_conflict_resolution": "wait_for_direction_unification",
    }


def build_trade_action_context(
    event,
    signal,
    *,
    recent_signals: list[dict[str, Any]] | None = None,
    conflict_window_sec: int | None = None,
) -> dict[str, Any]:
    signal_context = getattr(signal, "context", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    event_metadata = getattr(event, "metadata", {}) or {}
    direction_bucket = _text(
        signal_context.get("asset_case_direction"),
        signal_metadata.get("asset_case_direction"),
        event_metadata.get("asset_case_direction"),
        signal_context.get("direction_bucket"),
        signal_metadata.get("direction_bucket"),
        event_metadata.get("direction_bucket"),
        "buy_pressure" if str(getattr(event, "intent_type", "") or "") == "pool_buy_pressure" else "sell_pressure" if str(getattr(event, "intent_type", "") or "") == "pool_sell_pressure" else "",
    )
    direction = _direction_from_values(direction_bucket)
    basis_bps = None
    if _text(signal_context.get("basis_bps"), signal_metadata.get("basis_bps"), event_metadata.get("basis_bps")):
        basis_bps = _float(signal_context.get("basis_bps"), signal_metadata.get("basis_bps"), event_metadata.get("basis_bps"))
    mark_index_spread_bps = None
    if _text(
        signal_context.get("mark_index_spread_bps"),
        signal_metadata.get("mark_index_spread_bps"),
        event_metadata.get("mark_index_spread_bps"),
    ):
        mark_index_spread_bps = _float(
            signal_context.get("mark_index_spread_bps"),
            signal_metadata.get("mark_index_spread_bps"),
            event_metadata.get("mark_index_spread_bps"),
        )
    context = {
        "event_ts": _int(getattr(event, "ts", 0), default=0),
        "signal_id": _text(getattr(signal, "signal_id", "")),
        "asset_symbol": _text(
            signal_context.get("asset_symbol"),
            signal_context.get("asset_case_label"),
            signal_metadata.get("asset_symbol"),
            signal_metadata.get("asset_case_label"),
            event_metadata.get("asset_symbol"),
            event_metadata.get("asset_case_label"),
            event_metadata.get("token_symbol"),
            asset_symbol_from_event(event),
            getattr(event, "token", ""),
        ).upper(),
        "pair_label": _text(signal_context.get("pair_label"), signal_metadata.get("pair_label"), event_metadata.get("pair_label")),
        "direction_bucket": direction_bucket,
        "direction": direction,
        "intent_type": _text(getattr(signal, "intent_type", None), getattr(event, "intent_type", None)),
        "lp_alert_stage": _text(signal_context.get("lp_alert_stage"), signal_metadata.get("lp_alert_stage"), event_metadata.get("lp_alert_stage")),
        "lp_sweep_phase": _text(signal_context.get("lp_sweep_phase"), signal_metadata.get("lp_sweep_phase"), event_metadata.get("lp_sweep_phase")),
        "lp_confirm_scope": _text(signal_context.get("lp_confirm_scope"), signal_metadata.get("lp_confirm_scope"), event_metadata.get("lp_confirm_scope")),
        "lp_confirm_quality": _text(signal_context.get("lp_confirm_quality"), signal_metadata.get("lp_confirm_quality"), event_metadata.get("lp_confirm_quality")),
        "lp_absorption_context": _text(signal_context.get("lp_absorption_context"), signal_metadata.get("lp_absorption_context"), event_metadata.get("lp_absorption_context")),
        "lp_broader_alignment": _text(signal_context.get("lp_broader_alignment"), signal_metadata.get("lp_broader_alignment"), event_metadata.get("lp_broader_alignment")),
        "lp_sweep_continuation_score": _float(signal_context.get("lp_sweep_continuation_score"), signal_metadata.get("lp_sweep_continuation_score"), event_metadata.get("lp_sweep_continuation_score")),
        "lp_sweep_followthrough_score": _float(signal_context.get("lp_sweep_followthrough_score"), signal_metadata.get("lp_sweep_followthrough_score"), event_metadata.get("lp_sweep_followthrough_score")),
        "lp_sweep_exhaustion_score": _float(signal_context.get("lp_sweep_exhaustion_score"), signal_metadata.get("lp_sweep_exhaustion_score"), event_metadata.get("lp_sweep_exhaustion_score")),
        "lp_chase_risk_score": _float(signal_context.get("lp_chase_risk_score"), signal_metadata.get("lp_chase_risk_score"), event_metadata.get("lp_chase_risk_score")),
        "market_context_source": _text(signal_context.get("market_context_source"), signal_metadata.get("market_context_source"), event_metadata.get("market_context_source"), "unavailable"),
        "market_context_failure_reason": _text(
            signal_context.get("market_context_failure_reason"),
            signal_metadata.get("market_context_failure_reason"),
            event_metadata.get("market_context_failure_reason"),
        ),
        "alert_relative_timing": _text(signal_context.get("alert_relative_timing"), signal_metadata.get("alert_relative_timing"), event_metadata.get("alert_relative_timing")),
        "market_move_before_alert_30s": signal_context.get("market_move_before_alert_30s", signal_metadata.get("market_move_before_alert_30s", event_metadata.get("market_move_before_alert_30s"))),
        "market_move_before_alert_60s": signal_context.get("market_move_before_alert_60s", signal_metadata.get("market_move_before_alert_60s", event_metadata.get("market_move_before_alert_60s"))),
        "market_move_after_alert_60s": signal_context.get("market_move_after_alert_60s", signal_metadata.get("market_move_after_alert_60s", event_metadata.get("market_move_after_alert_60s"))),
        "basis_bps": basis_bps,
        "mark_index_spread_bps": mark_index_spread_bps,
        "pool_quality_score": _float(signal_context.get("pool_quality_score"), signal_metadata.get("pool_quality_score"), event_metadata.get("pool_quality_score"), default=0.0),
        "pair_quality_score": _float(signal_context.get("pair_quality_score"), signal_metadata.get("pair_quality_score"), event_metadata.get("pair_quality_score"), default=0.0),
        "asset_case_quality_score": _float(signal_context.get("asset_case_quality_score"), signal_metadata.get("asset_case_quality_score"), event_metadata.get("asset_case_quality_score"), default=0.0),
        "quality_scope_pool_size": _int(signal_context.get("quality_scope_pool_size"), signal_metadata.get("quality_scope_pool_size"), event_metadata.get("quality_scope_pool_size"), default=0),
        "quality_scope_pair_size": _int(signal_context.get("quality_scope_pair_size"), signal_metadata.get("quality_scope_pair_size"), event_metadata.get("quality_scope_pair_size"), default=0),
        "quality_scope_asset_size": _int(signal_context.get("quality_scope_asset_size"), signal_metadata.get("quality_scope_asset_size"), event_metadata.get("quality_scope_asset_size"), default=0),
        "quality_actionable_sample_size": _int(signal_context.get("quality_actionable_sample_size"), signal_metadata.get("quality_actionable_sample_size"), event_metadata.get("quality_actionable_sample_size"), default=0),
        "quality_score_brief": _text(signal_context.get("quality_score_brief"), signal_metadata.get("quality_score_brief"), event_metadata.get("quality_score_brief")),
        "asset_case_supporting_pair_count": _int(
            signal_context.get("asset_case_supporting_pair_count"),
            signal_metadata.get("asset_case_supporting_pair_count"),
            event_metadata.get("asset_case_supporting_pair_count"),
            default=0,
        ),
        "evidence_brief": _text(signal_context.get("evidence_brief"), signal_metadata.get("evidence_brief"), event_metadata.get("evidence_brief")),
        "asset_case_evidence_pack": _text(signal_context.get("asset_case_evidence_pack"), signal_metadata.get("asset_case_evidence_pack"), event_metadata.get("asset_case_evidence_pack")),
        "asset_case_evidence_summary": _text(signal_context.get("asset_case_evidence_summary"), signal_metadata.get("asset_case_evidence_summary"), event_metadata.get("asset_case_evidence_summary")),
        "lp_followup_check": _text(signal_context.get("lp_followup_check"), signal_metadata.get("lp_followup_check"), event_metadata.get("lp_followup_check")),
        "lp_invalidation": _text(signal_context.get("lp_invalidation"), signal_metadata.get("lp_invalidation"), event_metadata.get("lp_invalidation")),
        "lp_followup_window_sec": _int(signal_context.get("lp_followup_window_sec"), signal_metadata.get("lp_followup_window_sec"), event_metadata.get("lp_followup_window_sec"), default=90),
        "major_asset": bool(is_major_asset_symbol(_text(
            signal_context.get("asset_case_label"),
            signal_metadata.get("asset_case_label"),
            event_metadata.get("asset_case_label"),
            event_metadata.get("token_symbol"),
            asset_symbol_from_event(event),
        ))),
        "lp_conflict_window_sec": _int(conflict_window_sec, default=TRADE_ACTION_CONFLICT_WINDOW_SEC),
    }
    context["samples_insufficient"] = _quality_samples_insufficient(context)
    context["market_direction_aligned"] = _market_alignment_positive(
        direction,
        context.get("market_move_before_alert_30s"),
        context.get("market_move_before_alert_60s"),
        context.get("market_move_after_alert_60s"),
    )
    context["crowded"] = _has_crowding(direction, basis_bps, mark_index_spread_bps)
    context["strength_score"] = _trade_action_strength(context)
    context.update(_detect_conflict(context, recent_signals))
    return context


def _hard_chase_allowed(context: dict[str, Any]) -> tuple[bool, list[str]]:
    direction = str(context.get("direction") or "neutral")
    blockers: list[str] = []
    if direction not in {"long", "short"}:
        blockers.append("direction_ambiguous")
    if bool(TRADE_ACTION_REQUIRE_BROADER_CONFIRM_FOR_CHASE) and _text(context.get("lp_confirm_scope")) != "broader_confirm":
        blockers.append("broader_confirm_required")
    if bool(TRADE_ACTION_REQUIRE_LIVE_CONTEXT_FOR_CHASE) and _text(context.get("market_context_source")) != "live_public":
        blockers.append("live_context_required")
    if not bool(context.get("market_direction_aligned")):
        blockers.append("broader_market_not_aligned")
    expected_absorption = _expected_broader_absorption(direction)
    if not (
        _text(context.get("lp_absorption_context")) == expected_absorption
        or (
            _text(context.get("lp_broader_alignment")) == "confirmed"
            and _text(context.get("lp_confirm_scope")) == "broader_confirm"
        )
    ):
        blockers.append("directional_broader_alignment_missing")
    clean_or_continuation = bool(
        _text(context.get("lp_confirm_quality")) == "clean_confirm"
        or (
            _text(context.get("lp_sweep_phase")) == "sweep_confirmed"
            and _float(context.get("lp_sweep_continuation_score")) >= _HIGH_SWEEP_CONTINUATION_SCORE
        )
    )
    if not clean_or_continuation:
        blockers.append("clean_confirm_or_strong_sweep_required")
    if _text(context.get("alert_relative_timing")) == "late" or _text(context.get("lp_confirm_quality")) == "late_confirm":
        blockers.append("late_confirm")
    if _text(context.get("lp_confirm_quality")) == "chase_risk" or _float(context.get("lp_chase_risk_score")) >= 0.58:
        blockers.append("chase_risk")
    if _text(context.get("lp_alert_stage")) == "exhaustion_risk" or _text(context.get("lp_sweep_phase")) == "sweep_exhaustion_risk":
        blockers.append("exhaustion_risk")
    if _text(context.get("lp_absorption_context")) in {
        "local_buy_pressure_absorption",
        "local_sell_pressure_absorption",
        "pool_only_unconfirmed_pressure",
    }:
        blockers.append(_text(context.get("lp_absorption_context")))
    if _float(context.get("asset_case_quality_score")) < float(TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE):
        blockers.append("asset_quality_below_min")
    if min(
        _float(context.get("pair_quality_score")),
        _float(context.get("pool_quality_score")),
    ) < float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE):
        blockers.append("pair_or_pool_quality_below_min")
    if bool(context.get("samples_insufficient")):
        blockers.append("samples_insufficient")
    if bool(context.get("crowded")):
        blockers.append("crowded_basis_or_mark")
    if not _bool(context.get("major_asset")):
        blockers.append("non_major_asset")
    if _text(context.get("lp_conflict_resolution")) != "no_conflict" and _text(context.get("lp_conflict_resolution")) != "strong_side_override":
        blockers.append("conflict_no_trade")
    return not blockers, blockers


def _long_required_confirmation(direction: str) -> str:
    if direction == "long":
        return "90s 内跨池续买 + live_public broader confirm，才考虑顺势"
    if direction == "short":
        return "90s 内跨池续卖 + live_public broader confirm，才考虑顺势"
    return "等待更清晰的单方向确认"


def _long_invalidation(direction: str) -> str:
    if direction == "long":
        return "连续性中断 / 反向卖压出现"
    if direction == "short":
        return "连续性中断 / 反向买压出现"
    return "无"


def _build_evidence_pack(context: dict[str, Any], key: str) -> str:
    segments = _split_segments(
        _text(
            context.get("asset_case_evidence_pack"),
            context.get("asset_case_evidence_summary"),
            context.get("evidence_brief"),
        )
    )
    stage = _text(context.get("lp_alert_stage"))
    confirm_quality = _text(context.get("lp_confirm_quality"))
    direction = _text(context.get("direction"))
    if key == "DATA_GAP_NO_TRADE":
        segments.append("无合约同向确认")
    elif key == "CONFLICT_NO_TRADE":
        conflicts = context.get("lp_conflicting_signals") or []
        if conflicts:
            directions = []
            for item in conflicts[:2]:
                item_direction = str(item.get("direction") or "")
                if item_direction == "long":
                    directions.append("买方信号")
                elif item_direction == "short":
                    directions.append("卖方信号")
            if directions:
                segments.append(" + ".join(directions))
        segments.append("缺稳定 broader 确认")
    elif key in CHASE_ALLOWED_KEYS:
        segments.append("合约同步上行" if direction == "long" else "合约同步走弱")
        if confirm_quality == "clean_confirm":
            segments.append("clean confirm")
    elif key in {"DO_NOT_CHASE_LONG", "DO_NOT_CHASE_SHORT"}:
        if _text(context.get("lp_sweep_phase")) == "sweep_exhaustion_risk" or stage == "exhaustion_risk":
            segments.append("局部高潮风险")
        if _text(context.get("lp_absorption_context")).startswith("local_"):
            segments.append("局部压力被吸收")
        if bool(context.get("crowded")):
            basis_bps = context.get("basis_bps")
            if basis_bps is not None:
                segments.append(f"基差{float(basis_bps):+.1f}bp")
    elif key in {"LONG_BIAS_OBSERVE", "SHORT_BIAS_OBSERVE", "WAIT_CONFIRMATION"}:
        if _text(context.get("market_context_source")) != "live_public":
            segments.append("缺 live public")
        elif _text(context.get("lp_confirm_scope")) != "broader_confirm":
            segments.append("无 broader confirm")
    if not segments:
        segments = ["结构证据有限"]
    deduped: list[str] = []
    for segment in segments:
        if segment and segment not in deduped:
            deduped.append(segment)
    return "｜".join(deduped[:4])


def _build_payload(
    *,
    context: dict[str, Any],
    key: str,
    confidence: float,
    reason: str,
    conclusion: str,
    required_confirmation: str,
    invalidated_by: str,
    blockers: list[str],
) -> dict[str, Any]:
    direction = TRADE_ACTION_DIRECTIONS.get(key, _text(context.get("direction")) or "neutral")
    payload = {
        "trade_action_key": key,
        "trade_action_label": TRADE_ACTION_LABELS[key],
        "trade_action_direction": direction,
        "trade_action_confidence": round(float(confidence), 3),
        "trade_action_reason": reason,
        "trade_action_blockers": list(blockers),
        "trade_action_required_confirmation": required_confirmation,
        "trade_action_invalidated_by": invalidated_by or "无",
        "trade_action_time_horizon_sec": _int(context.get("lp_followup_window_sec"), default=90),
        "trade_action_source": TRADE_ACTION_SOURCE,
        "trade_action_debug": {
            "stage": _text(context.get("lp_alert_stage")),
            "sweep_phase": _text(context.get("lp_sweep_phase")),
            "confirm_scope": _text(context.get("lp_confirm_scope")),
            "confirm_quality": _text(context.get("lp_confirm_quality")),
            "absorption_context": _text(context.get("lp_absorption_context")),
            "broader_alignment": _text(context.get("lp_broader_alignment")),
            "market_context_source": _text(context.get("market_context_source")),
            "alert_relative_timing": _text(context.get("alert_relative_timing")),
            "basis_bps": context.get("basis_bps"),
            "mark_index_spread_bps": context.get("mark_index_spread_bps"),
            "asset_case_quality_score": _float(context.get("asset_case_quality_score")),
            "pair_quality_score": _float(context.get("pair_quality_score")),
            "pool_quality_score": _float(context.get("pool_quality_score")),
            "samples_insufficient": bool(context.get("samples_insufficient")),
            "lp_conflict_context": _text(context.get("lp_conflict_context")),
            "lp_conflict_score": _float(context.get("lp_conflict_score")),
            "strength_score": _trade_action_strength(context),
        },
        "trade_action_is_instruction": False,
        "trade_action_requires_user_confirmation": True,
        "trade_action_conclusion": conclusion,
        "trade_action_evidence_pack": _build_evidence_pack(context, key),
        "lp_conflict_context": _text(context.get("lp_conflict_context")),
        "lp_conflict_score": round(_float(context.get("lp_conflict_score")), 3),
        "lp_conflict_window_sec": _int(context.get("lp_conflict_window_sec"), default=TRADE_ACTION_CONFLICT_WINDOW_SEC),
        "lp_conflicting_signals": list(context.get("lp_conflicting_signals") or []),
        "lp_conflict_resolution": _text(context.get("lp_conflict_resolution"), "no_conflict"),
    }
    return payload


def infer_trade_action(context: dict[str, Any]) -> dict[str, Any]:
    direction = _text(context.get("direction")) or "neutral"
    stage = _text(context.get("lp_alert_stage"))
    sweep_phase = _text(context.get("lp_sweep_phase"))
    confirm_quality = _text(context.get("lp_confirm_quality"))
    absorption_context = _text(context.get("lp_absorption_context"))
    market_source = _text(context.get("market_context_source"), "unavailable")
    conflict_context = _text(context.get("lp_conflict_context"))
    conflict_resolution = _text(context.get("lp_conflict_resolution"))

    if direction == "neutral":
        return _build_payload(
            context=context,
            key="NO_TRADE",
            confidence=0.78,
            reason="当前不是明确的多空方向性 LP 结构，不能直接转成交易动作。",
            conclusion="结构偏非方向性，暂不具备交易条件",
            required_confirmation="等待出现更清晰的单方向跨池扩散",
            invalidated_by="无",
            blockers=["direction_ambiguous"],
        )

    if conflict_context and conflict_resolution == "wait_for_direction_unification":
        return _build_payload(
            context=context,
            key="CONFLICT_NO_TRADE",
            confidence=max(0.78, _float(context.get("lp_conflict_score"))),
            reason="买卖清扫/买卖压同时出现，缺稳定 broader 确认，追多追空都容易被反扫。",
            conclusion="双向扫流动性，方向冲突",
            required_confirmation="等待一方形成 broader_confirm",
            invalidated_by="无",
            blockers=["opposite_signal_within_window"],
        )

    if stage == "exhaustion_risk" or sweep_phase == "sweep_exhaustion_risk":
        if direction == "long":
            return _build_payload(
                context=context,
                key="DO_NOT_CHASE_LONG",
                confidence=0.84,
                reason="买方清扫更像局部高潮，回吐风险抬升，不宜继续追多。",
                conclusion="买方清扫后回吐风险",
                required_confirmation="更多池续买 + 合约同步上行后，再重新评估",
                invalidated_by="续买消失 / 出现卖方回补",
                blockers=["sweep_exhaustion_risk"],
            )
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_SHORT",
            confidence=0.84,
            reason="卖方清扫更像局部高潮，反抽风险抬升，不宜继续追空。",
            conclusion="卖方清扫后反抽风险",
            required_confirmation="更多池续卖 + 合约同步走弱后，再重新评估",
            invalidated_by="续卖消失 / 出现买方回补",
            blockers=["sweep_exhaustion_risk"],
        )

    if confirm_quality == "chase_risk" or _float(context.get("lp_chase_risk_score")) >= 0.58:
        if direction == "long":
            return _build_payload(
                context=context,
                key="DO_NOT_CHASE_LONG",
                confidence=0.82,
                reason="确认已偏后或预走过多，当前更像追涨风险而不是 clean 延续。",
                conclusion="确认偏晚，追多风险高",
                required_confirmation="等待回撤后重新形成 broader buy confirm",
                invalidated_by=_long_invalidation(direction),
                blockers=["chase_risk"],
            )
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_SHORT",
            confidence=0.82,
            reason="确认已偏后或预走过多，当前更像追空风险而不是 clean 延续。",
            conclusion="确认偏晚，追空风险高",
            required_confirmation="等待反弹后重新形成 broader sell confirm",
            invalidated_by=_long_invalidation(direction),
            blockers=["chase_risk"],
        )

    if confirm_quality == "late_confirm" or _text(context.get("alert_relative_timing")) == "late":
        if direction == "long":
            return _build_payload(
                context=context,
                key="DO_NOT_CHASE_LONG",
                confidence=0.76,
                reason="确认虽然成立，但节奏已偏晚，不能把它当成继续追多的许可。",
                conclusion="确认偏晚，先别追多",
                required_confirmation="等待更干净的 broader buy confirm",
                invalidated_by=_long_invalidation(direction),
                blockers=["late_confirm"],
            )
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_SHORT",
            confidence=0.76,
            reason="确认虽然成立，但节奏已偏晚，不能把它当成继续追空的许可。",
            conclusion="确认偏晚，先别追空",
            required_confirmation="等待更干净的 broader sell confirm",
            invalidated_by=_long_invalidation(direction),
            blockers=["late_confirm"],
        )

    if absorption_context == "local_buy_pressure_absorption":
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_LONG",
            confidence=0.82,
            reason="局部买压可能被吸收，不宜把单池压力误读成可以追多。",
            conclusion="局部买压可能被吸收",
            required_confirmation="继续跨池续买 + live_public broader confirm 后再评估",
            invalidated_by="续买消失 / 出现卖方回补",
            blockers=["local_buy_pressure_absorption"],
        )

    if absorption_context == "local_sell_pressure_absorption":
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_SHORT",
            confidence=0.82,
            reason="局部卖压可能被承接，不宜把单池卖压误读成可以追空。",
            conclusion="局部卖压可能被承接",
            required_confirmation="继续跨池续卖 + live_public broader confirm 后再评估",
            invalidated_by="续卖消失 / 出现买方回补",
            blockers=["local_sell_pressure_absorption"],
        )

    if market_source != "live_public":
        return _build_payload(
            context=context,
            key="DATA_GAP_NO_TRADE",
            confidence=0.90,
            reason="缺 live public broader context，无法确认链上方向是否真的扩散到更广盘口。",
            conclusion="缺 broader live context",
            required_confirmation="恢复 live_public broader context 后再评估",
            invalidated_by=_text(context.get("market_context_failure_reason"), "无"),
            blockers=["market_context_unavailable"],
        )

    chase_allowed, chase_blockers = _hard_chase_allowed(context)
    if chase_allowed and direction == "long":
        return _build_payload(
            context=context,
            key="LONG_CHASE_ALLOWED",
            confidence=max(0.84, _trade_action_strength(context)),
            reason="链上买压扩散且合约未偏晚，broader confirmation 与质量门槛都已通过。",
            conclusion="更广买压确认",
            required_confirmation="已触发；只在回撤不破确认区时考虑顺势",
            invalidated_by="broader alignment 消失 / Mark 跌回 Index 下方 / 反向清扫出现",
            blockers=[],
        )
    if chase_allowed and direction == "short":
        return _build_payload(
            context=context,
            key="SHORT_CHASE_ALLOWED",
            confidence=max(0.84, _trade_action_strength(context)),
            reason="链上卖压扩散且合约未偏晚，broader confirmation 与质量门槛都已通过。",
            conclusion="更广卖压确认",
            required_confirmation="已触发；只在反弹不破确认区时考虑顺势",
            invalidated_by="broader alignment 消失 / Mark 回到 Index 上方 / 反向买方清扫出现",
            blockers=[],
        )

    if context.get("crowded"):
        if direction == "long":
            return _build_payload(
                context=context,
                key="DO_NOT_CHASE_LONG",
                confidence=0.80,
                reason="合约基差或 Mark/Index 溢价偏高，当前更像多头拥挤，不宜追多。",
                conclusion="多头拥挤，先别追多",
                required_confirmation="拥挤度回落后再看是否仍有 broader buy confirm",
                invalidated_by=_long_invalidation(direction),
                blockers=["crowded_basis_or_mark"],
            )
        return _build_payload(
            context=context,
            key="DO_NOT_CHASE_SHORT",
            confidence=0.80,
            reason="合约基差或 Mark/Index 贴水偏深，当前更像空头拥挤，不宜追空。",
            conclusion="空头拥挤，先别追空",
            required_confirmation="拥挤度回落后再看是否仍有 broader sell confirm",
            invalidated_by=_long_invalidation(direction),
            blockers=["crowded_basis_or_mark"],
        )

    if stage == "prealert" or sweep_phase == "sweep_building":
        if direction == "long":
            return _build_payload(
                context=context,
                key="LONG_BIAS_OBSERVE",
                confidence=0.62,
                reason="局部买压/买方清扫建立中，但还没到可以追多的严格条件。",
                conclusion="局部买压仍待 broader 确认",
                required_confirmation=_long_required_confirmation(direction),
                invalidated_by="连续性中断 / 反向卖压出现",
                blockers=chase_blockers or ["building_stage"],
            )
        return _build_payload(
            context=context,
            key="SHORT_BIAS_OBSERVE",
            confidence=0.62,
            reason="局部卖压/卖方清扫建立中，但还没到可以追空的严格条件。",
            conclusion="局部卖压仍待 broader 确认",
            required_confirmation=_long_required_confirmation(direction),
            invalidated_by="连续性中断 / 反向买压出现",
            blockers=chase_blockers or ["building_stage"],
        )

    if absorption_context == "pool_only_unconfirmed_pressure" or _text(context.get("lp_confirm_scope")) != "broader_confirm":
        if direction == "long":
            return _build_payload(
                context=context,
                key="LONG_BIAS_OBSERVE",
                confidence=0.66,
                reason="方向偏多，但目前仍更像局部池子压力，缺 broader confirm，不能直接追多。",
                conclusion="方向偏多，但未满足追多条件",
                required_confirmation=_long_required_confirmation(direction),
                invalidated_by="连续性中断 / 反向卖压出现",
                blockers=chase_blockers or ["broader_confirm_missing"],
            )
        return _build_payload(
            context=context,
            key="SHORT_BIAS_OBSERVE",
            confidence=0.66,
            reason="方向偏空，但目前仍更像局部池子压力，缺 broader confirm，不能直接追空。",
            conclusion="方向偏空，但未满足追空条件",
            required_confirmation=_long_required_confirmation(direction),
            invalidated_by="连续性中断 / 反向买压出现",
            blockers=chase_blockers or ["broader_confirm_missing"],
        )

    if context.get("samples_insufficient"):
        if direction == "long":
            return _build_payload(
                context=context,
                key="LONG_BIAS_OBSERVE",
                confidence=0.68,
                reason="方向证据不差，但历史样本仍不足，只能先列为偏多观察。",
                conclusion="样本不足，先偏多观察",
                required_confirmation=_long_required_confirmation(direction),
                invalidated_by=_long_invalidation(direction),
                blockers=chase_blockers or ["samples_insufficient"],
            )
        return _build_payload(
            context=context,
            key="SHORT_BIAS_OBSERVE",
            confidence=0.68,
            reason="方向证据不差，但历史样本仍不足，只能先列为偏空观察。",
            conclusion="样本不足，先偏空观察",
            required_confirmation=_long_required_confirmation(direction),
            invalidated_by=_long_invalidation(direction),
            blockers=chase_blockers or ["samples_insufficient"],
        )

    if direction == "long":
        return _build_payload(
            context=context,
            key="WAIT_CONFIRMATION",
            confidence=0.64,
            reason="局部结构已经出现，但仍缺足够 clean/broader 证据，先等确认而不是直接追多。",
            conclusion="局部买压仍待确认",
            required_confirmation=_long_required_confirmation(direction),
            invalidated_by="连续性中断 / 反向卖压出现",
            blockers=chase_blockers or ["confirmation_pending"],
        )
    return _build_payload(
        context=context,
        key="WAIT_CONFIRMATION",
        confidence=0.64,
        reason="局部结构已经出现，但仍缺足够 clean/broader 证据，先等确认而不是直接追空。",
        conclusion="局部卖压仍待确认",
        required_confirmation=_long_required_confirmation(direction),
        invalidated_by="连续性中断 / 反向买压出现",
        blockers=chase_blockers or ["confirmation_pending"],
    )


def apply_trade_action(
    event,
    signal,
    *,
    recent_signals: list[dict[str, Any]] | None = None,
    conflict_window_sec: int | None = None,
) -> dict[str, Any]:
    if not bool(TRADE_ACTION_ENABLE):
        return {}
    signal_context = getattr(signal, "context", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    event_metadata = getattr(event, "metadata", {}) or {}
    stage = _text(signal_context.get("lp_alert_stage"), signal_metadata.get("lp_alert_stage"), event_metadata.get("lp_alert_stage"))
    if not (
        str(getattr(event, "strategy_role", "") or "") == "lp_pool"
        or stage in LP_TRADE_ACTION_STAGES
        or _bool(signal_context.get("lp_event"), signal_metadata.get("lp_event"), event_metadata.get("lp_event"))
    ):
        return {}
    context = build_trade_action_context(
        event,
        signal,
        recent_signals=recent_signals,
        conflict_window_sec=conflict_window_sec,
    )
    payload = infer_trade_action(context)
    event_metadata.update(payload)
    signal_metadata.update(payload)
    signal_context.update(payload)
    return payload


def build_trade_action_signal_summary(event, signal) -> dict[str, Any]:
    signal_context = getattr(signal, "context", {}) or {}
    signal_metadata = getattr(signal, "metadata", {}) or {}
    event_metadata = getattr(event, "metadata", {}) or {}
    direction = _text(
        signal_context.get("trade_action_direction"),
        signal_metadata.get("trade_action_direction"),
    ) or _direction_from_values(
        signal_context.get("asset_case_direction"),
        signal_metadata.get("asset_case_direction"),
        event_metadata.get("asset_case_direction"),
        signal_context.get("direction_bucket"),
        signal_metadata.get("direction_bucket"),
        event_metadata.get("direction_bucket"),
    )
    summary = {
        "ts": _int(getattr(event, "ts", 0), default=0),
        "signal_id": _text(getattr(signal, "signal_id", "")),
        "asset_symbol": _text(
            signal_context.get("asset_symbol"),
            signal_context.get("asset_case_label"),
            signal_metadata.get("asset_symbol"),
            signal_metadata.get("asset_case_label"),
            event_metadata.get("asset_symbol"),
            event_metadata.get("asset_case_label"),
            event_metadata.get("token_symbol"),
            asset_symbol_from_event(event),
            getattr(event, "token", ""),
        ).upper(),
        "direction": direction,
        "lp_alert_stage": _text(signal_context.get("lp_alert_stage"), signal_metadata.get("lp_alert_stage"), event_metadata.get("lp_alert_stage")),
        "lp_sweep_phase": _text(signal_context.get("lp_sweep_phase"), signal_metadata.get("lp_sweep_phase"), event_metadata.get("lp_sweep_phase")),
        "lp_confirm_scope": _text(signal_context.get("lp_confirm_scope"), signal_metadata.get("lp_confirm_scope"), event_metadata.get("lp_confirm_scope")),
        "lp_confirm_quality": _text(signal_context.get("lp_confirm_quality"), signal_metadata.get("lp_confirm_quality"), event_metadata.get("lp_confirm_quality")),
        "lp_absorption_context": _text(signal_context.get("lp_absorption_context"), signal_metadata.get("lp_absorption_context"), event_metadata.get("lp_absorption_context")),
        "market_context_source": _text(signal_context.get("market_context_source"), signal_metadata.get("market_context_source"), event_metadata.get("market_context_source")),
        "asset_case_quality_score": _float(signal_context.get("asset_case_quality_score"), signal_metadata.get("asset_case_quality_score"), event_metadata.get("asset_case_quality_score")),
        "pair_quality_score": _float(signal_context.get("pair_quality_score"), signal_metadata.get("pair_quality_score"), event_metadata.get("pair_quality_score")),
        "trade_action_key": _text(signal_context.get("trade_action_key"), signal_metadata.get("trade_action_key")),
        "strength_score": _float(
            ((signal_context.get("trade_action_debug") or {}).get("strength_score")),
            ((signal_metadata.get("trade_action_debug") or {}).get("strength_score")),
            default=0.0,
        ),
    }
    if summary["strength_score"] <= 0.0:
        summary["strength_score"] = _trade_action_strength(
            build_trade_action_context(event, signal)
        )
    return summary
