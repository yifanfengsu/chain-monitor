from __future__ import annotations

from collections import Counter
from typing import Any

from config import (
    REPLAY_PROFILE_BLOCK_MAX_AVG_PNL_BPS,
    REPLAY_PROFILE_BLOCK_MAX_CLEAN_RATE,
    REPLAY_PROFILE_BLOCK_MIN_CHOP_RATE,
    REPLAY_PROFILE_BLOCK_MIN_SAMPLES,
    REPLAY_PROFILE_POSITIVE_MIN_AVG_PNL_BPS,
    REPLAY_PROFILE_POSITIVE_MIN_SAMPLES,
    TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE,
)


PROFILE_KEY_DIMENSIONS = (
    "asset",
    "side",
    "lp_stage",
    "sweep_phase",
    "market_timing",
    "absorption_context",
    "asset_class",
    "basis_bucket",
    "quality_bucket",
)
UNKNOWN_VALUES = {"", "unknown", "UNKNOWN", "none", "NONE", "null", "NULL"}
LOW_SAMPLE_POSITIVE_MAX_SAMPLES = 5
HIGH_CONFIDENCE_POSITIVE_MIN_CLEAN_RATE = 0.50


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _unknown(value: Any) -> bool:
    return _text(value) in UNKNOWN_VALUES


def _profile_valid_count(row: dict[str, Any]) -> int:
    return _to_int(row.get("valid_sample_count"), _to_int(row.get("valid_count")))


def _clean_followthrough_rate(row: dict[str, Any]) -> float:
    return _to_float(row.get("clean_followthrough_rate"), _to_float(row.get("clean_rate")))


def profile_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "profile_key": row.get("profile_key"),
        "valid_sample_count": _profile_valid_count(row),
        "avg_net_pnl_bps": round(_to_float(row.get("avg_net_pnl_bps")), 2),
        "win_rate": round(_to_float(row.get("win_rate")), 4),
        "clean_followthrough_rate": round(_clean_followthrough_rate(row), 4),
        "chop_rate": round(_to_float(row.get("chop_rate")), 4),
        "recommended_action": row.get("recommended_action"),
    }


def is_high_confidence_negative_profile(row: dict[str, Any]) -> bool:
    return bool(
        _profile_valid_count(row) >= int(REPLAY_PROFILE_BLOCK_MIN_SAMPLES)
        and _to_float(row.get("avg_net_pnl_bps")) < 0.0
    )


def _recommended_profile_blocker(row: dict[str, Any]) -> bool:
    return str(row.get("recommended_action") or "").strip().lower() == "block_profile"


def is_replay_profile_negative_blocker(row: dict[str, Any]) -> bool:
    strict_negative = bool(
        _profile_valid_count(row) >= int(REPLAY_PROFILE_BLOCK_MIN_SAMPLES)
        and _to_float(row.get("avg_net_pnl_bps")) <= float(REPLAY_PROFILE_BLOCK_MAX_AVG_PNL_BPS)
        and _clean_followthrough_rate(row) <= float(REPLAY_PROFILE_BLOCK_MAX_CLEAN_RATE)
        and _to_float(row.get("chop_rate")) >= float(REPLAY_PROFILE_BLOCK_MIN_CHOP_RATE)
    )
    replay_recommended_negative = _recommended_profile_blocker(row) and is_high_confidence_negative_profile(row)
    return strict_negative or replay_recommended_negative


def is_high_confidence_positive_profile(row: dict[str, Any]) -> bool:
    return bool(
        _profile_valid_count(row) >= int(REPLAY_PROFILE_POSITIVE_MIN_SAMPLES)
        and _to_float(row.get("avg_net_pnl_bps")) > float(REPLAY_PROFILE_POSITIVE_MIN_AVG_PNL_BPS)
        and _clean_followthrough_rate(row) >= HIGH_CONFIDENCE_POSITIVE_MIN_CLEAN_RATE
    )


def is_low_sample_positive_profile(row: dict[str, Any]) -> bool:
    return bool(
        _to_float(row.get("avg_net_pnl_bps")) > 0.0
        and _profile_valid_count(row) < LOW_SAMPLE_POSITIVE_MAX_SAMPLES
    )


def evaluate_replay_profile_gate(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {
            "matched": False,
            "action": "missing_profile_stats",
            "blocker": "",
            "research_hint": "",
            "profile_stats": {},
        }
    payload = profile_payload(row)
    if is_replay_profile_negative_blocker(row):
        return {
            "matched": True,
            "action": "block_profile",
            "blocker": "replay_profile_negative",
            "research_hint": "",
            "profile_stats": payload,
        }
    if is_high_confidence_positive_profile(row):
        return {
            "matched": True,
            "action": "positive_research_only",
            "blocker": "",
            "research_hint": "positive_profile_research_only",
            "profile_stats": payload,
        }
    if _to_float(row.get("avg_net_pnl_bps")) > 0.0:
        return {
            "matched": True,
            "action": "needs_more_samples",
            "blocker": "",
            "research_hint": "positive_profile_needs_more_samples",
            "profile_stats": payload,
        }
    return {
        "matched": True,
        "action": str(row.get("recommended_action") or "observe_only"),
        "blocker": "",
        "research_hint": "",
        "profile_stats": payload,
    }


def profile_key_parts(profile_key: Any) -> list[str]:
    parts = [_text(item) for item in _text(profile_key).split("|")]
    if len(parts) < len(PROFILE_KEY_DIMENSIONS):
        parts.extend(["unknown"] * (len(PROFILE_KEY_DIMENSIONS) - len(parts)))
    return parts[: len(PROFILE_KEY_DIMENSIONS)]


def profile_key_unknown_dimensions(profile_key: Any) -> list[str]:
    parts = profile_key_parts(profile_key)
    return [
        dimension
        for dimension, value in zip(PROFILE_KEY_DIMENSIONS, parts, strict=False)
        if _unknown(value)
    ]


def profile_unknown_diagnostics(profile_rows: list[dict[str, Any]]) -> dict[str, Any]:
    unknown_by_dimension: Counter[str] = Counter()
    top_unknown_profiles: list[dict[str, Any]] = []
    total_fields = len(profile_rows) * len(PROFILE_KEY_DIMENSIONS)
    unknown_fields = 0
    for row in profile_rows:
        profile_key = row.get("profile_key")
        unknown_dimensions = profile_key_unknown_dimensions(profile_key)
        if not unknown_dimensions:
            continue
        unknown_fields += len(unknown_dimensions)
        for dimension in unknown_dimensions:
            unknown_by_dimension[dimension] += 1
        payload = profile_payload(row)
        payload["unknown_dimensions"] = unknown_dimensions
        payload["missing_sources"] = [f"profile_key.{dimension}" for dimension in unknown_dimensions]
        top_unknown_profiles.append(payload)
    top_unknown_profiles.sort(
        key=lambda item: (
            -len(item.get("unknown_dimensions") or []),
            -int(item.get("valid_sample_count") or 0),
            str(item.get("profile_key") or ""),
        )
    )
    return {
        "profile_unknown_field_rate": round(unknown_fields / total_fields, 4) if total_fields else 0.0,
        "profile_unknown_field_count": int(unknown_fields),
        "profile_unknown_profile_count": len(top_unknown_profiles),
        "unknown_by_dimension": dict(sorted(unknown_by_dimension.items())),
        "top_unknown_profiles": top_unknown_profiles[:10],
    }


def replay_profile_summary(profile_rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = [profile_payload(row) for row in profile_rows]
    high_negative = [
        profile_payload(row)
        for row in sorted(
            profile_rows,
            key=lambda item: (
                _to_float(item.get("avg_net_pnl_bps")),
                -_profile_valid_count(item),
                str(item.get("profile_key") or ""),
            ),
        )
        if is_high_confidence_negative_profile(row)
    ][:10]
    high_positive = [
        profile_payload(row)
        for row in sorted(
            profile_rows,
            key=lambda item: (
                -_to_float(item.get("avg_net_pnl_bps")),
                -_profile_valid_count(item),
                str(item.get("profile_key") or ""),
            ),
        )
        if is_high_confidence_positive_profile(row)
    ][:10]
    low_positive = [
        profile_payload(row)
        for row in sorted(
            profile_rows,
            key=lambda item: (
                -_to_float(item.get("avg_net_pnl_bps")),
                _profile_valid_count(item),
                str(item.get("profile_key") or ""),
            ),
        )
        if is_low_sample_positive_profile(row)
    ][:10]
    return {
        "replay_profile_count": len(normalized),
        "replay_profile_blocker_count": sum(1 for row in profile_rows if is_replay_profile_negative_blocker(row)),
        "high_confidence_negative_profiles": high_negative,
        "high_confidence_positive_profiles": high_positive,
        "low_sample_positive_profiles": low_positive,
        "low_sample_profiles_count": sum(
            1
            for row in profile_rows
            if _profile_valid_count(row) < int(REPLAY_PROFILE_BLOCK_MIN_SAMPLES)
        ),
        "profile_unknown_diagnostics": profile_unknown_diagnostics(profile_rows),
    }


def _bucket_absorption(value: Any) -> str:
    normalized = _text(value)
    if normalized.startswith("local_") or normalized == "pool_only_unconfirmed_pressure":
        return "local_absorption"
    if normalized.startswith("broader_"):
        return "broader_absorption"
    if normalized and not _unknown(normalized):
        return normalized
    return "unknown"


def _bucket_quality(fields: dict[str, Any]) -> str:
    existing = _text(fields.get("quality_bucket"))
    if existing and not _unknown(existing):
        return existing
    scores = [
        _to_float(fields.get("pool_quality_score"), -1.0),
        _to_float(fields.get("pair_quality_score"), -1.0),
        _to_float(fields.get("asset_case_quality_score"), -1.0),
    ]
    known = [score for score in scores if score >= 0.0]
    if not known:
        return "unknown"
    quality_floor = min(known)
    if quality_floor >= 0.82:
        return "quality_high"
    if quality_floor >= max(float(TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE), 0.60):
        return "quality_medium"
    return "quality_low"


def _side_from_fields(fields: dict[str, Any]) -> str:
    for key in ("side", "trade_opportunity_side", "opportunity_profile_side", "direction"):
        raw = _text(fields.get(key)).upper()
        if raw in {"LONG", "BUY", "BUY_PRESSURE"}:
            return "LONG"
        if raw in {"SHORT", "SELL", "SELL_PRESSURE"}:
            return "SHORT"
        if "LONG" in raw:
            return "LONG"
        if "SHORT" in raw:
            return "SHORT"
    return "unknown"


def repair_profile_key(profile_key: Any, fields: dict[str, Any]) -> str:
    raw_key = _text(profile_key)
    if raw_key and len(raw_key.split("|")) != len(PROFILE_KEY_DIMENSIONS):
        return raw_key
    parts = profile_key_parts(profile_key)
    replacements = {
        "asset": _text(fields.get("asset") or fields.get("asset_symbol") or fields.get("opportunity_profile_asset")).upper(),
        "side": _side_from_fields(fields),
        "lp_stage": _text(fields.get("confirm_scope") or fields.get("lp_confirm_scope")),
        "sweep_phase": _text(fields.get("stage_bucket") or fields.get("lp_alert_stage") or fields.get("sweep_phase") or fields.get("lp_sweep_phase")),
        "market_timing": _text(fields.get("market_timing") or fields.get("alert_relative_timing")),
        "absorption_context": _bucket_absorption(fields.get("absorption_bucket") or fields.get("lp_absorption_context") or fields.get("absorption_context")),
        "asset_class": "major" if fields.get("major_asset") is True else "minor" if fields.get("major_asset") is False else _text(fields.get("asset_class")),
        "basis_bucket": _text(fields.get("basis_bucket")),
        "quality_bucket": _bucket_quality(fields),
    }
    repaired: list[str] = []
    for dimension, current in zip(PROFILE_KEY_DIMENSIONS, parts, strict=False):
        replacement = _text(replacements.get(dimension))
        repaired.append(replacement if _unknown(current) and replacement and not _unknown(replacement) else current)
    return "|".join(repaired)


def local_absorption_quality_low(fields: dict[str, Any]) -> bool:
    absorption = _bucket_absorption(fields.get("lp_absorption_context") or fields.get("absorption_context"))
    quality = _bucket_quality(fields)
    return absorption == "local_absorption" and quality == "quality_low"
