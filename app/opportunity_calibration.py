from __future__ import annotations

from collections import Counter
from typing import Any

from config import (
    OPPORTUNITY_CALIBRATION_ADVERSE_MAX,
    OPPORTUNITY_CALIBRATION_COMPLETION_MIN,
    OPPORTUNITY_CALIBRATION_ENABLE,
    OPPORTUNITY_CALIBRATION_FOLLOWTHROUGH_MIN,
    OPPORTUNITY_CALIBRATION_MAX_NEGATIVE_ADJUSTMENT,
    OPPORTUNITY_CALIBRATION_MAX_POSITIVE_ADJUSTMENT,
    OPPORTUNITY_CALIBRATION_MIN_SAMPLES,
    OPPORTUNITY_CALIBRATION_STRONG_SAMPLES,
)


_SOURCE_ORDER = ("profile", "asset_side", "pair_side", "global_side")
_SOURCE_CONFIDENCE_BASE = {
    "profile": 1.0,
    "asset_side": 0.86,
    "pair_side": 0.74,
    "global_side": 0.60,
    "none": 0.0,
}


def _float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return int(default)


def _clamp(value: Any, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, _float(value)))


def _source_scope_key(*, source: str, profile_key: str, asset: str, side: str, pair_family: str) -> str:
    normalized_source = str(source or "").strip()
    if normalized_source == "profile":
        return f"profile:{profile_key}"
    if normalized_source == "asset_side":
        return f"asset_side:{asset}|{side}"
    if normalized_source == "pair_side":
        return f"pair_side:{pair_family}|{side}"
    if normalized_source == "global_side":
        return f"global_side:{side}"
    return ""


def _decode_scope_source(scope_key: str) -> str:
    raw = str(scope_key or "")
    if raw.startswith("profile:"):
        return "profile"
    if raw.startswith("asset_side:"):
        return "asset_side"
    if raw.startswith("pair_side:"):
        return "pair_side"
    if raw.startswith("global_side:"):
        return "global_side"
    return "none"


def _scope_value(scope_key: str) -> str:
    raw = str(scope_key or "")
    if ":" not in raw:
        return raw
    return raw.split(":", 1)[1]


def _completed_count(row: dict[str, Any]) -> int:
    if row.get("completed_60s") not in (None, ""):
        return _int(row.get("completed_60s"))
    sample_count = _int(row.get("sample_count"))
    completion_rate = _float(row.get("completion_60s_rate"), _float(row.get("outcome_completion_rate")))
    return int(round(sample_count * completion_rate))


def _weighted_count(row: dict[str, Any], count_key: str, rate_key: str, denominator: int) -> int:
    if row.get(count_key) not in (None, ""):
        return _int(row.get(count_key))
    rate = _float(row.get(rate_key))
    return int(round(denominator * rate))


def _normalize_profile_row(row: dict[str, Any]) -> dict[str, Any]:
    stats = row.get("stats_json") if isinstance(row.get("stats_json"), dict) else {}
    nested_stats = stats.get("stats_json") if isinstance(stats.get("stats_json"), dict) else {}
    base = dict(nested_stats or {})
    for key, value in stats.items():
        if key == "stats_json" or key in base:
            continue
        base[key] = value
    base.update({key: value for key, value in row.items() if key != "stats_json"})
    profile_key = str(base.get("profile_key") or base.get("scope_key") or "").strip()
    sample_count = _int(base.get("sample_count"))
    completed_60s = _completed_count(base)
    followthrough_60s_count = _weighted_count(base, "followthrough_60s_count", "followthrough_60s_rate", completed_60s)
    adverse_60s_count = _weighted_count(base, "adverse_60s_count", "adverse_60s_rate", completed_60s)
    blocked_completed_60s = _int(base.get("blocked_completed_60s"))
    blocker_saved_trade_60s_count = _weighted_count(
        base,
        "blocker_saved_trade_60s_count",
        "blocker_saved_rate",
        blocked_completed_60s,
    )
    blocker_false_block_60s_count = _weighted_count(
        base,
        "blocker_false_block_60s_count",
        "blocker_false_block_rate",
        blocked_completed_60s,
    )
    return {
        "profile_key": profile_key,
        "asset": str(base.get("asset") or "").strip().upper(),
        "pair_family": str(base.get("pair_family") or base.get("pair") or "").strip().upper(),
        "side": str(base.get("side") or "").strip().upper(),
        "sample_count": sample_count,
        "completed_60s": completed_60s,
        "followthrough_60s_count": followthrough_60s_count,
        "adverse_60s_count": adverse_60s_count,
        "blocked_completed_60s": blocked_completed_60s,
        "blocker_saved_trade_60s_count": blocker_saved_trade_60s_count,
        "blocker_false_block_60s_count": blocker_false_block_60s_count,
    }


def _normalize_calibration_row(row: dict[str, Any]) -> dict[str, Any]:
    stats = row.get("stats_json") if isinstance(row.get("stats_json"), dict) else {}
    nested_stats = stats.get("stats_json") if isinstance(stats.get("stats_json"), dict) else {}
    base = dict(nested_stats or {})
    for key, value in stats.items():
        if key == "stats_json" or key in base:
            continue
        base[key] = value
    base.update({key: value for key, value in row.items() if key != "stats_json"})
    scope_key = str(base.get("scope_key") or row.get("scope_key") or "").strip()
    source = str(base.get("scope_type") or base.get("scope_source") or "").strip()
    if source not in _SOURCE_ORDER:
        source = _decode_scope_source(scope_key)
    return {
        "scope_key": scope_key,
        "source": source or "none",
        "sample_count": _int(base.get("sample_count")),
        "completion_rate_60s": round(_clamp(base.get("completion_rate_60s"), 0.0, 1.0), 4),
        "followthrough_rate_60s": round(_clamp(base.get("followthrough_rate_60s"), 0.0, 1.0), 4),
        "adverse_rate_60s": round(_clamp(base.get("adverse_rate_60s"), 0.0, 1.0), 4),
        "blocker_saved_rate": round(_clamp(base.get("blocker_saved_rate"), 0.0, 1.0), 4),
        "blocker_false_block_rate": round(_clamp(base.get("blocker_false_block_rate"), 0.0, 1.0), 4),
        "adjustment": round(_float(base.get("adjustment")), 4),
        "confidence": round(_clamp(base.get("confidence"), 0.0, 1.0), 4),
        "reason": str(base.get("reason") or "").strip(),
        "updated_at": _float(base.get("updated_at")),
    }


def _aggregate_profile_scope(
    profile_rows: list[dict[str, Any]],
    *,
    source: str,
    profile_key: str,
    asset: str,
    side: str,
    pair_family: str,
) -> dict[str, Any]:
    normalized_rows = [_normalize_profile_row(row) for row in profile_rows if isinstance(row, dict)]
    selected_rows: list[dict[str, Any]] = []
    if source == "profile":
        selected_rows = [row for row in normalized_rows if str(row.get("profile_key") or "") == str(profile_key or "")]
    elif source == "asset_side":
        selected_rows = [
            row
            for row in normalized_rows
            if str(row.get("asset") or "") == str(asset or "").upper() and str(row.get("side") or "") == str(side or "").upper()
        ]
    elif source == "pair_side":
        selected_rows = [
            row
            for row in normalized_rows
            if str(row.get("pair_family") or "") == str(pair_family or "").upper() and str(row.get("side") or "") == str(side or "").upper()
        ]
    elif source == "global_side":
        selected_rows = [row for row in normalized_rows if str(row.get("side") or "") == str(side or "").upper()]
    sample_count = sum(_int(row.get("sample_count")) for row in selected_rows)
    completed_60s = sum(_int(row.get("completed_60s")) for row in selected_rows)
    followthrough_60s_count = sum(_int(row.get("followthrough_60s_count")) for row in selected_rows)
    adverse_60s_count = sum(_int(row.get("adverse_60s_count")) for row in selected_rows)
    blocked_completed_60s = sum(_int(row.get("blocked_completed_60s")) for row in selected_rows)
    blocker_saved_trade_60s_count = sum(_int(row.get("blocker_saved_trade_60s_count")) for row in selected_rows)
    blocker_false_block_60s_count = sum(_int(row.get("blocker_false_block_60s_count")) for row in selected_rows)
    return {
        "scope_type": source,
        "scope_key": _source_scope_key(
            source=source,
            profile_key=profile_key,
            asset=asset,
            side=side,
            pair_family=pair_family,
        ),
        "sample_count": sample_count,
        "completion_rate_60s": round((completed_60s / sample_count) if sample_count else 0.0, 4),
        "followthrough_rate_60s": round((followthrough_60s_count / completed_60s) if completed_60s else 0.0, 4),
        "adverse_rate_60s": round((adverse_60s_count / completed_60s) if completed_60s else 1.0, 4),
        "blocker_saved_rate": round((blocker_saved_trade_60s_count / blocked_completed_60s) if blocked_completed_60s else 0.0, 4),
        "blocker_false_block_rate": round((blocker_false_block_60s_count / blocked_completed_60s) if blocked_completed_60s else 0.0, 4),
    }


def _sample_factors(sample_count: int) -> tuple[float, float]:
    minimum = int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES)
    strong = max(int(OPPORTUNITY_CALIBRATION_STRONG_SAMPLES), minimum + 1)
    if sample_count <= 0:
        return 0.0, 0.0
    if sample_count < minimum:
        ratio = max(0.0, min(1.0, sample_count / float(minimum)))
        return 0.0, round(0.25 + ratio * 0.35, 4)
    if sample_count >= strong:
        return 1.0, 1.0
    ratio = (sample_count - minimum) / float(strong - minimum)
    return round(0.35 + ratio * 0.65, 4), round(0.45 + ratio * 0.55, 4)


def get_calibration_adjustment(
    raw_score: float,
    profile_key: str,
    asset: str,
    side: str,
    components: dict[str, Any] | None = None,
    calibration_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del raw_score, profile_key, asset, side, components
    selected = dict(calibration_stats or {})
    sample_count = _int(selected.get("sample_count"))
    completion_rate = _float(selected.get("completion_rate_60s"))
    followthrough_rate = _float(selected.get("followthrough_rate_60s"))
    adverse_rate = _float(selected.get("adverse_rate_60s"), 1.0)
    blocker_saved_rate = _float(selected.get("blocker_saved_rate"))
    blocker_false_block_rate = _float(selected.get("blocker_false_block_rate"))
    positive_factor, negative_factor = _sample_factors(sample_count)
    adjustment = 0.0
    reasons: list[str] = []

    if completion_rate < 0.50:
        delta = -0.050 * max(negative_factor, 0.30)
        adjustment += delta
        reasons.append(f"completion<{0.50:.2f}")
    elif completion_rate < 0.70:
        delta = -0.024 * max(negative_factor, 0.25)
        adjustment += delta
        reasons.append(f"completion<{float(OPPORTUNITY_CALIBRATION_COMPLETION_MIN):.2f}")

    if adverse_rate > 0.45:
        delta = -0.070 * max(negative_factor, 0.35)
        adjustment += delta
        reasons.append("adverse>0.45")
    elif adverse_rate > 0.35:
        delta = -0.036 * max(negative_factor, 0.30)
        adjustment += delta
        reasons.append(f"adverse>{float(OPPORTUNITY_CALIBRATION_ADVERSE_MAX):.2f}")
    elif adverse_rate < 0.25 and positive_factor > 0.0:
        delta = 0.014 * positive_factor
        adjustment += delta
        reasons.append("adverse<0.25")

    if followthrough_rate > 0.65 and positive_factor > 0.0:
        delta = 0.026 * positive_factor
        adjustment += delta
        reasons.append("followthrough>0.65")
    elif followthrough_rate >= 0.58 and positive_factor > 0.0:
        delta = 0.012 * positive_factor
        adjustment += delta
        reasons.append(f"followthrough>={float(OPPORTUNITY_CALIBRATION_FOLLOWTHROUGH_MIN):.2f}")
    elif followthrough_rate < 0.50 and sample_count > 0:
        delta = -0.020 * max(negative_factor, 0.25)
        adjustment += delta
        reasons.append("followthrough<0.50")

    if blocker_saved_rate >= 0.65 and sample_count >= int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES):
        adjustment -= 0.012 * max(negative_factor, 0.45)
        reasons.append("blocker_saved_rate_high")
    if blocker_false_block_rate >= 0.55 and sample_count >= int(OPPORTUNITY_CALIBRATION_STRONG_SAMPLES):
        adjustment += 0.006 * positive_factor
        reasons.append("blocker_false_block_rate_high")

    if sample_count < int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES) and adjustment > 0.0:
        adjustment = 0.0
        reasons.append("sample_guard_no_positive")
    if sample_count < int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES):
        adjustment = max(adjustment, -0.030)

    max_positive = float(OPPORTUNITY_CALIBRATION_MAX_POSITIVE_ADJUSTMENT)
    max_negative = abs(float(OPPORTUNITY_CALIBRATION_MAX_NEGATIVE_ADJUSTMENT))
    adjustment = max(-max_negative, min(max_positive, adjustment))

    confidence_base = float(_SOURCE_CONFIDENCE_BASE.get(str(selected.get("scope_type") or "none"), 0.0))
    confidence_factor = max(positive_factor, negative_factor)
    confidence = round(_clamp(confidence_base * confidence_factor, 0.0, 1.0), 4)
    reason = "neutral_calibration" if not reasons else "; ".join(reasons)
    return {
        "adjustment": round(adjustment, 4),
        "reason": reason,
        "sample_count": sample_count,
        "confidence": confidence,
        "source": str(selected.get("scope_type") or "none"),
        "completion_rate_60s": round(completion_rate, 4),
        "followthrough_rate_60s": round(followthrough_rate, 4),
        "adverse_rate_60s": round(adverse_rate, 4),
        "blocker_saved_rate": round(blocker_saved_rate, 4),
        "blocker_false_block_rate": round(blocker_false_block_rate, 4),
    }


def _load_quality_stats(scope_type: str, scope_keys: list[str] | None = None, *, stage: str | None = None) -> list[dict[str, Any]]:
    try:
        import sqlite_store

        conn = sqlite_store.get_connection()
    except Exception:
        conn = None
    if conn is None:
        return []
    clauses = ["scope_type = ?"]
    params: list[Any] = [scope_type]
    if stage is not None:
        clauses.append("stage = ?")
        params.append(str(stage))
    if scope_keys:
        placeholders = ", ".join("?" for _ in scope_keys)
        clauses.append(f"scope_key IN ({placeholders})")
        params.extend(scope_keys)
    sql = (
        "SELECT scope_type, scope_key, asset, pair, stage, sample_count, "
        "candidate_followthrough_rate, candidate_adverse_rate, verified_followthrough_rate, "
        "verified_adverse_rate, outcome_completion_rate, stats_json, updated_at "
        f"FROM quality_stats WHERE {' AND '.join(clauses)}"
    )
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
    except Exception:
        return []
    result: list[dict[str, Any]] = []
    for row in rows:
        stats_json = row["stats_json"]
        if isinstance(stats_json, str):
            try:
                import json

                stats_json = json.loads(stats_json)
            except Exception:
                stats_json = {}
        result.append(
            {
                "scope_type": row["scope_type"],
                "scope_key": row["scope_key"],
                "asset": row["asset"],
                "pair": row["pair"],
                "stage": row["stage"],
                "sample_count": row["sample_count"],
                "candidate_followthrough_rate": row["candidate_followthrough_rate"],
                "candidate_adverse_rate": row["candidate_adverse_rate"],
                "verified_followthrough_rate": row["verified_followthrough_rate"],
                "verified_adverse_rate": row["verified_adverse_rate"],
                "outcome_completion_rate": row["outcome_completion_rate"],
                "stats_json": stats_json if isinstance(stats_json, dict) else {},
                "updated_at": row["updated_at"],
            }
        )
    return result


def load_calibration_stats(
    profile_key: str,
    asset: str,
    side: str,
    components: dict[str, Any] | None = None,
    *,
    pair_family: str = "",
    calibration_rows: list[dict[str, Any]] | None = None,
    profile_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    del components
    scope_keys = [
        _source_scope_key(source=source, profile_key=profile_key, asset=asset, side=side, pair_family=pair_family)
        for source in _SOURCE_ORDER
    ]
    calibration_rows = (
        list(calibration_rows)
        if calibration_rows is not None
        else _load_quality_stats("opportunity_calibration", scope_keys, stage="60s")
    )
    by_source: dict[str, dict[str, Any]] = {}
    for row in calibration_rows:
        normalized = _normalize_calibration_row(row)
        source = str(normalized.get("source") or "none")
        if source in _SOURCE_ORDER:
            by_source[source] = normalized

    if not by_source:
        profile_rows = (
            list(profile_rows)
            if profile_rows is not None
            else _load_quality_stats("opportunity_profile", stage="all")
        )
        for source in _SOURCE_ORDER:
            by_source[source] = _aggregate_profile_scope(
                profile_rows,
                source=source,
                profile_key=profile_key,
                asset=asset,
                side=side,
                pair_family=pair_family,
            )

    attempted_scopes: list[dict[str, Any]] = []
    selected: dict[str, Any] | None = None
    for source in _SOURCE_ORDER:
        current = dict(by_source.get(source) or {})
        if not current:
            current = {
                "scope_type": source,
                "scope_key": _source_scope_key(
                    source=source,
                    profile_key=profile_key,
                    asset=asset,
                    side=side,
                    pair_family=pair_family,
                ),
                "sample_count": 0,
                "completion_rate_60s": 0.0,
                "followthrough_rate_60s": 0.0,
                "adverse_rate_60s": 1.0,
                "blocker_saved_rate": 0.0,
                "blocker_false_block_rate": 0.0,
                "reason": "",
            }
        current["scope_type"] = source
        attempted_scopes.append(
            {
                "scope_type": source,
                "scope_key": str(current.get("scope_key") or ""),
                "sample_count": _int(current.get("sample_count")),
                "completion_rate_60s": round(_float(current.get("completion_rate_60s")), 4),
                "followthrough_rate_60s": round(_float(current.get("followthrough_rate_60s")), 4),
                "adverse_rate_60s": round(_float(current.get("adverse_rate_60s"), 1.0), 4),
            }
        )
        if selected is None and _int(current.get("sample_count")) >= int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES):
            selected = current
    if selected is None:
        for source in _SOURCE_ORDER:
            current = dict(by_source.get(source) or {})
            if _int(current.get("sample_count")) > 0:
                selected = current
                break
    if selected is None:
        selected = {
            "scope_type": "none",
            "scope_key": "",
            "sample_count": 0,
            "completion_rate_60s": 0.0,
            "followthrough_rate_60s": 0.0,
            "adverse_rate_60s": 1.0,
            "blocker_saved_rate": 0.0,
            "blocker_false_block_rate": 0.0,
            "reason": "no_calibration_rows",
        }
    selected = dict(selected)
    selected.setdefault("scope_type", "none")
    selected.setdefault("scope_key", "")
    selected.setdefault("sample_count", 0)
    selected.setdefault("completion_rate_60s", 0.0)
    selected.setdefault("followthrough_rate_60s", 0.0)
    selected.setdefault("adverse_rate_60s", 1.0)
    selected.setdefault("blocker_saved_rate", 0.0)
    selected.setdefault("blocker_false_block_rate", 0.0)
    return {
        "selected": selected,
        "attempted_scopes": attempted_scopes,
        "fallback_used": str(selected.get("scope_type") or "none") not in {"profile", "none"},
        "source": str(selected.get("scope_type") or "none"),
    }


def build_calibration_snapshot(
    *,
    raw_score: float,
    profile_key: str,
    asset: str,
    side: str,
    pair_family: str,
    selected_stats: dict[str, Any],
    adjustment_payload: dict[str, Any],
    attempted_scopes: list[dict[str, Any]],
) -> dict[str, Any]:
    calibrated_score = round(_clamp(raw_score + _float(adjustment_payload.get("adjustment"))), 4)
    selected_source = str(selected_stats.get("scope_type") or "none")
    sample_count = _int(adjustment_payload.get("sample_count"))
    reason = str(adjustment_payload.get("reason") or "")
    if selected_source != "none" and selected_source != "profile":
        reason = f"fallback_to_{selected_source}; {reason}"
    return {
        "opportunity_raw_score": round(_clamp(raw_score), 4),
        "opportunity_calibrated_score": calibrated_score,
        "opportunity_calibration_adjustment": round(_float(adjustment_payload.get("adjustment")), 4),
        "opportunity_calibration_reason": reason,
        "opportunity_calibration_sample_count": sample_count,
        "opportunity_calibration_confidence": round(_clamp(adjustment_payload.get("confidence")), 4),
        "opportunity_calibration_source": selected_source,
        "opportunity_calibration_scope_key": str(selected_stats.get("scope_key") or ""),
        "opportunity_calibration_completion_rate_60s": round(_float(adjustment_payload.get("completion_rate_60s")), 4),
        "opportunity_calibration_followthrough_rate_60s": round(_float(adjustment_payload.get("followthrough_rate_60s")), 4),
        "opportunity_calibration_adverse_rate_60s": round(_float(adjustment_payload.get("adverse_rate_60s"), 1.0), 4),
        "opportunity_calibration_blocker_saved_rate": round(_float(adjustment_payload.get("blocker_saved_rate")), 4),
        "opportunity_calibration_blocker_false_block_rate": round(_float(adjustment_payload.get("blocker_false_block_rate")), 4),
        "opportunity_calibration_attempted_scopes": attempted_scopes,
        "calibration_profile_key": str(profile_key or ""),
        "calibration_asset": str(asset or "").upper(),
        "calibration_side": str(side or "").upper(),
        "calibration_pair_family": str(pair_family or "").upper(),
    }


def calibrate_opportunity_score(
    raw_score: float,
    profile_key: str,
    asset: str,
    side: str,
    components: dict[str, Any] | None,
    *,
    pair_family: str = "",
    calibration_rows: list[dict[str, Any]] | None = None,
    profile_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not bool(OPPORTUNITY_CALIBRATION_ENABLE):
        return build_calibration_snapshot(
            raw_score=raw_score,
            profile_key=profile_key,
            asset=asset,
            side=side,
            pair_family=pair_family,
            selected_stats={"scope_type": "none", "scope_key": ""},
            adjustment_payload={
                "adjustment": 0.0,
                "reason": "calibration_disabled",
                "sample_count": 0,
                "confidence": 0.0,
                "completion_rate_60s": 0.0,
                "followthrough_rate_60s": 0.0,
                "adverse_rate_60s": 1.0,
                "blocker_saved_rate": 0.0,
                "blocker_false_block_rate": 0.0,
            },
            attempted_scopes=[],
        )
    stats_payload = load_calibration_stats(
        profile_key=profile_key,
        asset=asset,
        side=side,
        components=components,
        pair_family=pair_family,
        calibration_rows=calibration_rows,
        profile_rows=profile_rows,
    )
    selected = dict(stats_payload.get("selected") or {})
    adjustment_payload = get_calibration_adjustment(
        raw_score=raw_score,
        profile_key=profile_key,
        asset=asset,
        side=side,
        components=components,
        calibration_stats=selected,
    )
    return build_calibration_snapshot(
        raw_score=raw_score,
        profile_key=profile_key,
        asset=asset,
        side=side,
        pair_family=pair_family,
        selected_stats=selected,
        adjustment_payload=adjustment_payload,
        attempted_scopes=list(stats_payload.get("attempted_scopes") or []),
    )


def build_calibration_quality_stats(
    profile_rows: list[dict[str, Any]],
    *,
    profile_key: str,
    asset: str,
    side: str,
    pair_family: str,
    updated_at: float | int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in _SOURCE_ORDER:
        aggregated = _aggregate_profile_scope(
            profile_rows,
            source=source,
            profile_key=profile_key,
            asset=asset,
            side=side,
            pair_family=pair_family,
        )
        adjustment_payload = get_calibration_adjustment(
            raw_score=0.0,
            profile_key=profile_key,
            asset=asset,
            side=side,
            components=None,
            calibration_stats=aggregated,
        )
        stats_json = {
            "calibration_key": str(aggregated.get("scope_key") or ""),
            "scope_type": source,
            "scope_key": str(aggregated.get("scope_key") or ""),
            "sample_count": _int(aggregated.get("sample_count")),
            "completion_rate_60s": round(_float(aggregated.get("completion_rate_60s")), 4),
            "followthrough_rate_60s": round(_float(aggregated.get("followthrough_rate_60s")), 4),
            "adverse_rate_60s": round(_float(aggregated.get("adverse_rate_60s"), 1.0), 4),
            "blocker_saved_rate": round(_float(aggregated.get("blocker_saved_rate")), 4),
            "blocker_false_block_rate": round(_float(aggregated.get("blocker_false_block_rate")), 4),
            "adjustment": round(_float(adjustment_payload.get("adjustment")), 4),
            "confidence": round(_float(adjustment_payload.get("confidence")), 4),
            "reason": str(adjustment_payload.get("reason") or ""),
            "updated_at": _float(updated_at),
        }
        rows.append(
            {
                "scope_type": "opportunity_calibration",
                "scope_key": str(aggregated.get("scope_key") or ""),
                "asset": str(asset or "").upper() if source in {"profile", "asset_side"} else (str(asset or "").upper() if source == "pair_side" else ""),
                "pair": str(pair_family or "").upper() if source == "pair_side" else "",
                "stage": "60s",
                "sample_count": stats_json["sample_count"],
                "candidate_followthrough_rate": stats_json["followthrough_rate_60s"],
                "candidate_adverse_rate": stats_json["adverse_rate_60s"],
                "verified_followthrough_rate": stats_json["followthrough_rate_60s"],
                "verified_adverse_rate": stats_json["adverse_rate_60s"],
                "outcome_completion_rate": stats_json["completion_rate_60s"],
                "updated_at": _float(updated_at),
                "stats_json": stats_json,
            }
        )
    return rows


def calibration_health_summary(
    calibration_rows: list[dict[str, Any]] | None = None,
    *,
    profile_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = list(calibration_rows or _load_quality_stats("opportunity_calibration", stage="60s"))
    normalized: list[dict[str, Any]] = []
    if rows:
        normalized = [_normalize_calibration_row(row) for row in rows if isinstance(row, dict)]
    if not normalized:
        profile_rows = list(profile_rows or _load_quality_stats("opportunity_profile", stage="all"))
        profile_stats = [_normalize_profile_row(row) for row in profile_rows if isinstance(row, dict)]
        scope_rows: list[dict[str, Any]] = []
        for row in profile_stats:
            profile_key = str(row.get("profile_key") or "")
            if not profile_key:
                continue
            scope_rows.extend(
                build_calibration_quality_stats(
                    profile_stats,
                    profile_key=profile_key,
                    asset=str(row.get("asset") or ""),
                    side=str(row.get("side") or ""),
                    pair_family=str(row.get("pair_family") or ""),
                    updated_at=0,
                )[:1]
            )
        normalized = [_normalize_calibration_row(row) for row in scope_rows]

    profile_scope_rows = [row for row in normalized if str(row.get("source") or "") == "profile"]
    positive_rows = [row for row in profile_scope_rows if _float(row.get("adjustment")) > 0.0]
    negative_rows = [row for row in profile_scope_rows if _float(row.get("adjustment")) < 0.0]
    confidence_distribution = Counter()
    for row in profile_scope_rows:
        confidence = _float(row.get("confidence"))
        if confidence >= 0.75:
            confidence_distribution["high"] += 1
        elif confidence >= 0.45:
            confidence_distribution["medium"] += 1
        else:
            confidence_distribution["low"] += 1

    recommended_threshold_review: list[str] = []
    if any(_float(row.get("adverse_rate_60s"), 1.0) > 0.45 for row in profile_scope_rows):
        recommended_threshold_review.append("高 adverse profile 偏多，先复查 risk_cleanliness 与 verified gate。")
    if any(_float(row.get("completion_rate_60s")) < 0.50 for row in profile_scope_rows):
        recommended_threshold_review.append("completion 偏低的 profile 偏多，优先提升 outcome 完整率而不是放松阈值。")
    if not recommended_threshold_review:
        recommended_threshold_review.append("当前 calibration 偏保守，无需直接改动 runtime 阈值。")

    def _scope_label(row: dict[str, Any]) -> str:
        return _scope_value(str(row.get("scope_key") or ""))

    return {
        "profiles_calibrated_count": len(profile_scope_rows),
        "profiles_sample_insufficient_count": sum(
            1 for row in profile_scope_rows if _int(row.get("sample_count")) < int(OPPORTUNITY_CALIBRATION_MIN_SAMPLES)
        ),
        "top_positive_adjustments": [
            {
                "profile_key": _scope_label(row),
                "sample_count": _int(row.get("sample_count")),
                "adjustment": round(_float(row.get("adjustment")), 4),
                "reason": str(row.get("reason") or ""),
            }
            for row in sorted(
                positive_rows,
                key=lambda item: (-_float(item.get("adjustment")), -_int(item.get("sample_count")), _scope_label(item)),
            )[:10]
        ],
        "top_negative_adjustments": [
            {
                "profile_key": _scope_label(row),
                "sample_count": _int(row.get("sample_count")),
                "adjustment": round(_float(row.get("adjustment")), 4),
                "reason": str(row.get("reason") or ""),
            }
            for row in sorted(
                negative_rows,
                key=lambda item: (_float(item.get("adjustment")), -_int(item.get("sample_count")), _scope_label(item)),
            )[:10]
        ],
        "high_adverse_profiles": [
            {
                "profile_key": _scope_label(row),
                "sample_count": _int(row.get("sample_count")),
                "adverse_rate_60s": round(_float(row.get("adverse_rate_60s"), 1.0), 4),
            }
            for row in sorted(
                [item for item in profile_scope_rows if _float(item.get("adverse_rate_60s"), 1.0) > 0.35],
                key=lambda item: (-_float(item.get("adverse_rate_60s"), 1.0), -_int(item.get("sample_count")), _scope_label(item)),
            )[:10]
        ],
        "high_followthrough_profiles": [
            {
                "profile_key": _scope_label(row),
                "sample_count": _int(row.get("sample_count")),
                "followthrough_rate_60s": round(_float(row.get("followthrough_rate_60s")), 4),
            }
            for row in sorted(
                [item for item in profile_scope_rows if _float(item.get("followthrough_rate_60s")) >= 0.58],
                key=lambda item: (-_float(item.get("followthrough_rate_60s")), -_int(item.get("sample_count")), _scope_label(item)),
            )[:10]
        ],
        "low_completion_profiles": [
            {
                "profile_key": _scope_label(row),
                "sample_count": _int(row.get("sample_count")),
                "completion_rate_60s": round(_float(row.get("completion_rate_60s")), 4),
            }
            for row in sorted(
                [item for item in profile_scope_rows if _float(item.get("completion_rate_60s")) < float(OPPORTUNITY_CALIBRATION_COMPLETION_MIN)],
                key=lambda item: (_float(item.get("completion_rate_60s")), -_int(item.get("sample_count")), _scope_label(item)),
            )[:10]
        ],
        "recommended_threshold_review": recommended_threshold_review,
        "calibration_confidence_distribution": dict(sorted(confidence_distribution.items())),
    }
