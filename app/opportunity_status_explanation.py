from __future__ import annotations

import json
from collections import Counter
from typing import Any


RAW_STATUS_KEYS = (
    "trade_opportunity_status_at_creation",
    "status_at_creation",
    "trade_opportunity_status",
    "status",
)

PRIMARY_BLOCKER_KEYS = (
    "trade_opportunity_primary_blocker",
    "primary_blocker",
    "trade_opportunity_primary_hard_blocker",
    "primary_hard_blocker",
    "trade_opportunity_primary_verification_blocker",
    "primary_verification_blocker",
    "blocked_reason",
    "blocker_type",
)

HARD_BLOCKER_KEYS = (
    "trade_opportunity_primary_hard_blocker",
    "primary_hard_blocker",
    "trade_opportunity_hard_blockers",
    "hard_blockers",
    "hard_blockers_json",
)

BLOCKER_LIST_KEYS = (
    "trade_opportunity_blockers",
    "blockers",
    "blockers_json",
    "trade_opportunity_hard_blockers",
    "hard_blockers",
    "hard_blockers_json",
    "trade_opportunity_verification_blockers",
    "verification_blockers",
    "verification_blockers_json",
)

GATE_FAILURE_KEYS = (
    "opportunity_gate_failure_reason",
    "gate_failure_reason",
    "gate_reason",
    "blocked_reason",
)

SHADOW_REASON_KEYS = (
    "trade_opportunity_shadow_reason",
    "shadow_reason",
)

DERIVED_KEEP_STATUSES = {"CANDIDATE", "VERIFIED", "INVALIDATED", "EXPIRED"}
RAW_STATUS_ORDER = ("NONE", "CANDIDATE", "VERIFIED", "BLOCKED", "INVALIDATED", "EXPIRED")
DERIVED_STATUS_ORDER = (
    "TRUE_NONE",
    "BLOCKED_LIKE",
    "GATE_FAILED",
    "LOW_QUALITY",
    "REPLAY_NEGATIVE",
    "DO_NOT_CHASE",
    "LOCAL_ABSORPTION",
    "NEAR_CANDIDATE",
    "CANDIDATE",
    "VERIFIED",
    "BLOCKED",
    "INVALIDATED",
    "EXPIRED",
)

DO_NOT_CHASE_KEYS = {
    "no_trade",
    "no_trade_lock",
    "do_not_chase",
    "do_not_chase_long",
    "do_not_chase_short",
    "late_or_chase",
}

REPLAY_NEGATIVE_KEYS = {"replay_profile_negative"}
LOW_QUALITY_KEYS = {"low_quality", "quality_floor", "quality_below_candidate"}
LOCAL_ABSORPTION_KEYS = {"local_absorption", "local_absorption_quality_low"}
NEAR_CANDIDATE_KEYS = {
    "score_below_candidate",
    "score_below_shadow_candidate",
    "near_candidate_but_blocked",
    "near_verified_but_immature",
}

BLOCKED_LIKE_REASON_KEYS = {
    *DO_NOT_CHASE_KEYS,
    *REPLAY_NEGATIVE_KEYS,
    *LOW_QUALITY_KEYS,
    *LOCAL_ABSORPTION_KEYS,
    "blocked",
    "blocked_by_gate",
    "blocked_like",
    "crowded_basis",
    "data_gap",
    "direction_conflict",
    "non_lp_clmm_position_against",
    "non_lp_evidence_conflict",
    "non_lp_exchange_flow_against",
    "non_lp_liquidation_against",
    "non_lp_maker_against",
    "non_lp_opposite_smart_money",
    "profile_adverse_too_high",
    "profile_completion_too_low",
    "profile_followthrough_too_low",
    "profile_sample_count_insufficient",
    "strong_opposite_signal",
    "sweep_exhaustion_risk",
}


def normalize_status(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"", "NONE", "NULL", "N/A", "NA"}:
        return "NONE"
    return raw


def normalize_reason(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    for prefix in ("hard_blocker:", "blocked:", "gate:", "primary_blocker:"):
        if raw.startswith(prefix):
            raw = raw.split(":", 1)[1]
    if raw in {"no_trade_lock_active", "no_trade_locked"}:
        return "no_trade_lock"
    if raw == "do_not_chase_long_side":
        return "do_not_chase_long"
    if raw == "do_not_chase_short_side":
        return "do_not_chase_short"
    return raw


def is_status_blocking_reason(value: Any) -> bool:
    key = normalize_reason(value)
    return bool(key and key in BLOCKED_LIKE_REASON_KEYS)


def _from_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _containers(row: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [row]
    for key in ("opportunity_json", "opportunity", "metadata", "context", "signal", "event", "data"):
        value = row.get(key)
        if isinstance(value, str) and value[:1] in {"{", "["}:
            value = _from_json(value, {})
        if isinstance(value, dict):
            containers.append(value)
            nested = value.get("metadata")
            if isinstance(nested, dict):
                containers.append(nested)
            nested_context = value.get("context")
            if isinstance(nested_context, dict):
                containers.append(nested_context)
    return containers


def _first(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for container in _containers(row):
        for key in keys:
            value = container.get(key)
            if value not in (None, "", [], {}, ()):
                return value
    return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "passed", "enabled"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float)):
        return float(value) == 0.0
    return str(value or "").strip().lower() in {"0", "false", "no", "n", "failed", "rejected"}


def _append_unique(items: list[str], value: Any) -> None:
    key = normalize_reason(value)
    if not key or key in {"none", "null", "n/a", "na"}:
        return
    if key not in items:
        items.append(key)


def _string_list(value: Any) -> list[str]:
    parsed = _from_json(value, value)
    if isinstance(parsed, list):
        return [str(item or "").strip() for item in parsed if str(item or "").strip()]
    if isinstance(parsed, dict):
        items: list[str] = []
        for key in BLOCKER_LIST_KEYS:
            nested = parsed.get(key)
            if isinstance(nested, list):
                items.extend(str(item or "").strip() for item in nested if str(item or "").strip())
        for key in PRIMARY_BLOCKER_KEYS + GATE_FAILURE_KEYS + SHADOW_REASON_KEYS:
            nested_value = parsed.get(key)
            if nested_value not in (None, "", [], {}, ()):
                items.append(str(nested_value))
        return items
    if isinstance(parsed, str):
        stripped = parsed.strip()
        return [stripped] if stripped else []
    return []


def _collect_reasons(row: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    reasons: list[str] = []
    for container in _containers(row):
        for key in keys:
            if key not in container:
                continue
            for item in _string_list(container.get(key)):
                _append_unique(reasons, item)
    return reasons


def _raw_status(row: dict[str, Any]) -> str:
    return normalize_status(_first(row, RAW_STATUS_KEYS))


def _primary_blocker(row: dict[str, Any]) -> str:
    return normalize_reason(_first(row, PRIMARY_BLOCKER_KEYS))


def _hard_blockers(row: dict[str, Any]) -> list[str]:
    blockers = _collect_reasons(row, HARD_BLOCKER_KEYS)
    primary_hard = normalize_reason(_first(row, ("trade_opportunity_primary_hard_blocker", "primary_hard_blocker")))
    if primary_hard:
        _append_unique(blockers, primary_hard)
    return blockers


def _all_blockers(row: dict[str, Any]) -> list[str]:
    blockers = _collect_reasons(row, BLOCKER_LIST_KEYS)
    primary = _primary_blocker(row)
    if primary:
        _append_unique(blockers, primary)
    for blocker in _hard_blockers(row):
        _append_unique(blockers, blocker)
    return blockers


def _gate_failure_reason(row: dict[str, Any]) -> str:
    return normalize_reason(_first(row, GATE_FAILURE_KEYS))


def _shadow_reasons(row: dict[str, Any]) -> list[str]:
    return _collect_reasons(row, SHADOW_REASON_KEYS)


def _has_any(reasons: list[str], keys: set[str]) -> bool:
    return any(normalize_reason(item) in keys for item in reasons)


def _first_matching(reasons: list[str], keys: set[str]) -> str:
    for item in reasons:
        key = normalize_reason(item)
        if key in keys:
            return key
    return ""


def _is_gate_failed(row: dict[str, Any], gate_failure_reason: str) -> bool:
    required = _first(row, ("opportunity_gate_required",))
    passed = _first(row, ("opportunity_gate_passed",))
    if _boolish(required) and _explicit_false(passed):
        return True
    return bool(gate_failure_reason and gate_failure_reason not in NEAR_CANDIDATE_KEYS)


def _is_near_candidate(row: dict[str, Any], shadow_reasons: list[str], all_reasons: list[str]) -> bool:
    if _boolish(_first(row, ("would_have_been_candidate", "trade_opportunity_would_have_been_candidate"))):
        return True
    shadow_status = normalize_status(_first(row, ("trade_opportunity_shadow_status", "shadow_status")))
    if shadow_status not in {"", "NONE"}:
        return True
    return _has_any(shadow_reasons + all_reasons, NEAR_CANDIDATE_KEYS)


def derive_opportunity_status(row: dict[str, Any]) -> dict[str, Any]:
    raw_status = _raw_status(row)
    hard_blockers = _hard_blockers(row)
    blockers = _all_blockers(row)
    primary = _primary_blocker(row)
    gate_failure_reason = _gate_failure_reason(row)
    shadow_reasons = _shadow_reasons(row)
    all_reasons = list(dict.fromkeys([*blockers, gate_failure_reason, *shadow_reasons]))
    has_blocker = bool(blockers or primary)
    has_hard_blocker = bool(hard_blockers)
    has_gate_failure = _is_gate_failed(row, gate_failure_reason)
    is_near_candidate = _is_near_candidate(row, shadow_reasons, all_reasons)

    derived_status = "TRUE_NONE"
    derived_reason = "true_none"
    primary_explanation = "no blocker, gate failure, or near-candidate evidence"

    if raw_status in DERIVED_KEEP_STATUSES:
        derived_status = raw_status
        derived_reason = raw_status.lower()
        primary_explanation = f"raw status is {raw_status}"
    elif raw_status == "BLOCKED":
        derived_status = "BLOCKED"
        derived_reason = primary or gate_failure_reason or "blocked"
        primary_explanation = f"raw status is BLOCKED: {derived_reason}"
    elif has_hard_blocker:
        derived_status = "BLOCKED_LIKE"
        derived_reason = _first_matching(hard_blockers, BLOCKED_LIKE_REASON_KEYS) or hard_blockers[0]
        primary_explanation = f"hard blocker present: {derived_reason}"
    elif _has_any([primary, *blockers], REPLAY_NEGATIVE_KEYS):
        derived_status = "REPLAY_NEGATIVE"
        derived_reason = "replay_profile_negative"
        primary_explanation = "replay profile negative blocker present"
    elif _has_any([primary, *blockers], LOW_QUALITY_KEYS):
        derived_status = "LOW_QUALITY"
        derived_reason = _first_matching([primary, *blockers], LOW_QUALITY_KEYS) or "low_quality"
        primary_explanation = "quality blocker present"
    elif _has_any([primary, *blockers], DO_NOT_CHASE_KEYS):
        derived_status = "DO_NOT_CHASE"
        derived_reason = _first_matching([primary, *blockers], DO_NOT_CHASE_KEYS) or "do_not_chase"
        primary_explanation = "do-not-chase blocker present"
    elif _has_any([primary, *blockers], LOCAL_ABSORPTION_KEYS):
        derived_status = "LOCAL_ABSORPTION"
        derived_reason = _first_matching([primary, *blockers], LOCAL_ABSORPTION_KEYS) or "local_absorption"
        primary_explanation = "local absorption blocker present"
    elif has_gate_failure:
        derived_status = "GATE_FAILED"
        derived_reason = gate_failure_reason or "opportunity_gate_rejected"
        primary_explanation = f"opportunity gate failed: {derived_reason}"
    elif is_near_candidate:
        derived_status = "NEAR_CANDIDATE"
        derived_reason = _first_matching([*shadow_reasons, *all_reasons], NEAR_CANDIDATE_KEYS) or "would_have_been_candidate"
        primary_explanation = f"near-candidate evidence present: {derived_reason}"

    return {
        "raw_status": raw_status,
        "derived_status": derived_status,
        "derived_reason": derived_reason,
        "status_reason": derived_reason,
        "primary_explanation": primary_explanation,
        "has_blocker": has_blocker,
        "has_hard_blocker": has_hard_blocker,
        "has_gate_failure": has_gate_failure,
        "is_near_candidate": is_near_candidate,
        "blocked_like": derived_status in {
            "BLOCKED",
            "BLOCKED_LIKE",
            "GATE_FAILED",
            "LOW_QUALITY",
            "REPLAY_NEGATIVE",
            "DO_NOT_CHASE",
            "LOCAL_ABSORPTION",
        },
        "primary_blocker": primary,
        "hard_blockers": hard_blockers,
        "blockers": blockers,
        "gate_failure_reason": gate_failure_reason,
    }


def explain_opportunity_status(row: dict[str, Any]) -> dict[str, Any]:
    return derive_opportunity_status(row)


def _ordered_counter_payload(counter: Counter[str], order: tuple[str, ...] = ()) -> dict[str, int]:
    payload: dict[str, int] = {}
    for key in order:
        payload[key] = int(counter.get(key, 0))
    for key, value in sorted(counter.items()):
        if key not in payload:
            payload[key] = int(value)
    return payload


def explain_opportunity_status_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_status = Counter()
    derived_status = Counter()
    top_derived_reasons = Counter()
    top_none_blockers = Counter()
    top_gate_failure_reasons = Counter()
    none_total = 0
    true_none_count = 0
    blocked_like_none_count = 0
    gate_failed_none_count = 0
    low_quality_none_count = 0
    replay_negative_none_count = 0
    do_not_chase_none_count = 0
    local_absorption_none_count = 0
    near_candidate_none_count = 0
    none_with_blockers_count = 0
    none_with_hard_blockers_count = 0
    none_with_primary_blocker_count = 0

    for row in rows:
        derived = derive_opportunity_status(row)
        raw = str(derived["raw_status"])
        status = str(derived["derived_status"])
        reason = str(derived["derived_reason"])
        raw_status[raw] += 1
        derived_status[status] += 1
        top_derived_reasons[reason] += 1
        if derived.get("gate_failure_reason"):
            top_gate_failure_reasons[str(derived["gate_failure_reason"])] += 1
        if raw != "NONE":
            continue

        none_total += 1
        blockers = list(derived.get("blockers") or [])
        if blockers:
            none_with_blockers_count += 1
            for blocker in blockers:
                top_none_blockers[str(blocker)] += 1
        if derived.get("has_hard_blocker"):
            none_with_hard_blockers_count += 1
        if derived.get("primary_blocker"):
            none_with_primary_blocker_count += 1
        if status == "TRUE_NONE":
            true_none_count += 1
        if status in {"BLOCKED", "BLOCKED_LIKE", "LOW_QUALITY", "REPLAY_NEGATIVE", "DO_NOT_CHASE", "LOCAL_ABSORPTION"}:
            blocked_like_none_count += 1
        if status == "GATE_FAILED":
            gate_failed_none_count += 1
        if status == "LOW_QUALITY" or _has_any(blockers, LOW_QUALITY_KEYS):
            low_quality_none_count += 1
        if status == "REPLAY_NEGATIVE" or _has_any(blockers, REPLAY_NEGATIVE_KEYS):
            replay_negative_none_count += 1
        if status == "DO_NOT_CHASE" or _has_any(blockers, DO_NOT_CHASE_KEYS):
            do_not_chase_none_count += 1
        if status == "LOCAL_ABSORPTION" or _has_any(blockers, LOCAL_ABSORPTION_KEYS):
            local_absorption_none_count += 1
        if status == "NEAR_CANDIDATE" or bool(derived.get("is_near_candidate")):
            near_candidate_none_count += 1

    raw_all_none = bool(rows) and int(raw_status.get("NONE", 0)) == len(rows)
    warning = (
        "raw_status_all_none_but_blocked_like_present"
        if raw_all_none and (blocked_like_none_count or gate_failed_none_count or near_candidate_none_count)
        else ""
    )
    if warning:
        diagnosis = "raw_none_contains_explainable_opportunities"
    elif none_total and true_none_count == none_total:
        diagnosis = "raw_none_is_true_none"
    elif rows:
        diagnosis = "status_assignment_ok"
    else:
        diagnosis = "no_trade_opportunities"

    return {
        "total": len(rows),
        "raw_status_distribution": _ordered_counter_payload(raw_status, RAW_STATUS_ORDER),
        "derived_status_distribution": _ordered_counter_payload(derived_status, DERIVED_STATUS_ORDER),
        "none_count": none_total,
        "true_none_count": true_none_count,
        "blocked_like_none_count": blocked_like_none_count,
        "gate_failed_none_count": gate_failed_none_count,
        "low_quality_none_count": low_quality_none_count,
        "replay_negative_none_count": replay_negative_none_count,
        "do_not_chase_none_count": do_not_chase_none_count,
        "local_absorption_none_count": local_absorption_none_count,
        "near_candidate_none_count": near_candidate_none_count,
        "near_candidate_count": near_candidate_none_count,
        "none_with_blockers_count": none_with_blockers_count,
        "none_with_hard_blockers_count": none_with_hard_blockers_count,
        "none_with_primary_blocker_count": none_with_primary_blocker_count,
        "top_derived_reasons": dict(top_derived_reasons.most_common(10)),
        "top_none_reasons": dict(top_derived_reasons.most_common(10)),
        "top_none_blockers": dict(top_none_blockers.most_common(10)),
        "top_blocker_like_reasons": dict(top_derived_reasons.most_common(10)),
        "top_gate_failure_reasons": dict(top_gate_failure_reasons.most_common(10)),
        "status_reason_distribution": dict(top_derived_reasons.most_common(20)),
        "status_assignment_warning": warning,
        "status_assignment_diagnosis": diagnosis,
    }
