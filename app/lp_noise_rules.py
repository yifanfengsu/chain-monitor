from lp_registry import ACTIVE_LP_POOL_ADDRESSES


LP_ADJACENT_NOISE_RULE_VERSION = "lp_adjacent_core_v1"
LP_ADJACENT_NOISE_STAGE_LISTENER = "listener_prefilter"
LP_ADJACENT_NOISE_STAGE_PIPELINE = "pipeline_prefilter"


def _dedupe_texts(values) -> list[str]:
    items: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text and text not in items:
            items.append(text)
    return items


def _normalized_addresses(values) -> set[str]:
    return {
        str(value or "").lower()
        for value in (values or [])
        if str(value or "").strip()
    }


def lp_adjacent_noise_confidence_bucket(confidence: float) -> str:
    score = float(confidence or 0.0)
    if score >= 0.95:
        return ">=0.95"
    if score >= 0.85:
        return "0.85~0.95"
    return "<0.85"


def _base_decision(
    *,
    stage: str,
    context_used: list[str],
    runtime_context_present: bool,
    downstream_context_present: bool,
    reason: str = "",
    confidence: float = 0.0,
    source_signals: list[str] | None = None,
    is_noise: bool = False,
) -> dict:
    return {
        "is_noise": bool(is_noise),
        "reason": str(reason or ""),
        "confidence": round(float(confidence or 0.0), 3),
        "source_signals": _dedupe_texts(source_signals or []),
        "rule_version": LP_ADJACENT_NOISE_RULE_VERSION,
        "decision_stage": str(stage or ""),
        "context_used": _dedupe_texts(context_used),
        "runtime_context_present": bool(runtime_context_present),
        "downstream_context_present": bool(downstream_context_present),
    }


def lp_adjacent_noise_core_decision(
    payload: dict | None,
    *,
    stage: str,
    watch_addresses: set[str] | None = None,
) -> dict:
    payload = dict(payload or {})
    monitor_type = str(payload.get("monitor_type") or "adjacent_watch").strip()
    watch_address = str(payload.get("watch_address") or "").lower()
    normalized_watch_addresses = _normalized_addresses(watch_addresses)
    strategy_role = str(payload.get("strategy_role") or "").lower()
    strategy_hint = str(payload.get("strategy_hint") or "").lower()
    runtime_adjacent_watch = bool(payload.get("runtime_adjacent_watch"))
    runtime_state = str(payload.get("runtime_state") or "").lower()
    anchor_watch_address = str(payload.get("anchor_watch_address") or "").lower()
    downstream_case_id = str(payload.get("downstream_case_id") or "")
    listener_confidence = float(payload.get("lp_adjacent_noise_listener_confidence") or 0.0)
    listener_source_signals = list(payload.get("lp_adjacent_noise_listener_source_signals") or [])

    touched_lp_pools = _normalized_addresses(payload.get("touched_lp_pools"))
    touched_lp_pool_count = int(payload.get("touched_lp_pool_count") or len(touched_lp_pools))
    tx_pool_hit_count = int(payload.get("tx_pool_hit_count") or 0)
    pool_candidate_weight = float(payload.get("pool_candidate_weight") or 0.0)
    participant_addresses = _normalized_addresses(payload.get("participant_addresses"))
    pool_transfer_count_by_pool = {
        str(address or "").lower(): int(value or 0)
        for address, value in dict(payload.get("pool_transfer_count_by_pool") or {}).items()
        if str(address or "").strip()
    }
    max_pool_transfer_count = max(pool_transfer_count_by_pool.values(), default=0)

    context_used: list[str] = []
    if touched_lp_pool_count:
        context_used.append("touched_lp_pool_count")
    if tx_pool_hit_count:
        context_used.append("tx_pool_hit_count")
    if payload.get("pool_candidate_weight") is not None:
        context_used.append("pool_candidate_weight")
    if pool_transfer_count_by_pool:
        context_used.append("pool_transfer_count_by_pool")
    if participant_addresses:
        context_used.append("participant_addresses")
    if strategy_role:
        context_used.append("watch_meta.strategy_role")
    if listener_confidence or listener_source_signals:
        context_used.append("listener_prefilter")
    if strategy_hint:
        context_used.append("strategy_hint")
    if runtime_adjacent_watch:
        context_used.append("runtime_adjacent_watch")
    if runtime_state:
        context_used.append("runtime_state")
    if anchor_watch_address:
        context_used.append("anchor_watch_address")
    if downstream_case_id:
        context_used.append("downstream_case_id")

    runtime_context_present = bool(
        runtime_adjacent_watch
        or runtime_state
        or "runtime_adjacent" in strategy_hint
        or "runtime_watch" in strategy_hint
    )
    downstream_context_present = bool(
        anchor_watch_address
        or downstream_case_id
        or "downstream" in strategy_hint
    )

    if monitor_type != "adjacent_watch" or not watch_address or watch_address in normalized_watch_addresses:
        return _base_decision(
            stage=stage,
            context_used=context_used,
            runtime_context_present=runtime_context_present,
            downstream_context_present=downstream_context_present,
        )

    watch_in_participants = watch_address in participant_addresses
    non_watch_participants = {
        address for address in participant_addresses
        if address and address != watch_address
    }
    lp_reference_pools = set(ACTIVE_LP_POOL_ADDRESSES) | touched_lp_pools
    lp_only_counterparties = bool(non_watch_participants) and non_watch_participants.issubset(lp_reference_pools)

    candidate_reason = ""
    candidate_confidence = 0.0
    candidate_signals: list[str] = []

    if watch_address in ACTIVE_LP_POOL_ADDRESSES:
        candidate_reason = "adjacent_watch_overlaps_active_lp_pool"
        candidate_confidence = 1.0
        candidate_signals = [
            "watch_address_in_active_lp_pool",
            f"touched_lp_pool_count={touched_lp_pool_count}",
            f"tx_pool_hit_count={tx_pool_hit_count}",
        ]
    elif strategy_role == "lp_pool":
        candidate_reason = "adjacent_watch_watch_meta_lp_pool"
        candidate_confidence = 0.995
        candidate_signals = [
            "watch_meta_strategy_role=lp_pool",
            f"touched_lp_pool_count={touched_lp_pool_count}",
            f"tx_pool_hit_count={tx_pool_hit_count}",
        ]
    elif listener_confidence >= 0.95 and listener_source_signals:
        candidate_reason = "adjacent_watch_listener_high_confidence_lp_noise"
        candidate_confidence = max(listener_confidence, 0.95)
        candidate_signals = list(listener_source_signals) + [
            f"listener_confidence={round(listener_confidence, 3)}",
        ]
    elif (
        watch_in_participants
        and len(touched_lp_pools) >= 2
        and touched_lp_pool_count >= 2
        and tx_pool_hit_count >= 2
        and max_pool_transfer_count >= 3
        and pool_candidate_weight >= 0.9
    ):
        candidate_reason = "adjacent_watch_multi_pool_inventory_loop"
        candidate_confidence = max(pool_candidate_weight, 0.92)
        candidate_signals = [
            "watch_address_in_participants",
            f"touched_lp_pools={len(touched_lp_pools)}",
            f"tx_pool_hit_count={tx_pool_hit_count}",
            f"max_pool_transfer_count={max_pool_transfer_count}",
            f"pool_candidate_weight={round(pool_candidate_weight, 3)}",
        ]
    elif (
        watch_in_participants
        and lp_only_counterparties
        and touched_lp_pool_count >= 1
        and tx_pool_hit_count >= touched_lp_pool_count
        and max_pool_transfer_count >= 2
        and pool_candidate_weight >= 0.82
    ):
        candidate_reason = "adjacent_watch_lp_only_counterparties_high_density"
        candidate_confidence = max(pool_candidate_weight, 0.93)
        candidate_signals = [
            "watch_address_in_participants",
            "lp_only_counterparties",
            f"touched_lp_pool_count={touched_lp_pool_count}",
            f"max_pool_transfer_count={max_pool_transfer_count}",
            f"pool_candidate_weight={round(pool_candidate_weight, 3)}",
        ]

    stage_threshold = 0.93 if stage == LP_ADJACENT_NOISE_STAGE_LISTENER else 0.92
    context_skip_reason = "adjacent_watch_runtime_downstream_context_present_skip_lp_noise"
    context_skip_signals = []
    if runtime_adjacent_watch:
        context_skip_signals.append("runtime_adjacent_watch")
    if runtime_state:
        context_skip_signals.append(f"runtime_state={runtime_state}")
    if anchor_watch_address:
        context_skip_signals.append("anchor_watch_address_present")
    if downstream_case_id:
        context_skip_signals.append("downstream_case_id_present")
    if strategy_hint:
        context_skip_signals.append(f"strategy_hint={strategy_hint}")

    strongest_reason = candidate_reason in {
        "adjacent_watch_overlaps_active_lp_pool",
        "adjacent_watch_watch_meta_lp_pool",
    }
    if candidate_reason and (runtime_context_present or downstream_context_present) and not strongest_reason:
        return _base_decision(
            stage=stage,
            context_used=context_used,
            runtime_context_present=runtime_context_present,
            downstream_context_present=downstream_context_present,
            reason=context_skip_reason,
            confidence=0.0,
            source_signals=context_skip_signals + [f"candidate_reason={candidate_reason}"],
            is_noise=False,
        )

    if candidate_reason and candidate_confidence >= stage_threshold:
        return _base_decision(
            stage=stage,
            context_used=context_used,
            runtime_context_present=runtime_context_present,
            downstream_context_present=downstream_context_present,
            reason=candidate_reason,
            confidence=candidate_confidence,
            source_signals=candidate_signals,
            is_noise=True,
        )

    if runtime_context_present or downstream_context_present:
        return _base_decision(
            stage=stage,
            context_used=context_used,
            runtime_context_present=runtime_context_present,
            downstream_context_present=downstream_context_present,
            reason=context_skip_reason,
            confidence=0.0,
            source_signals=context_skip_signals,
            is_noise=False,
        )

    return _base_decision(
        stage=stage,
        context_used=context_used,
        runtime_context_present=runtime_context_present,
        downstream_context_present=downstream_context_present,
    )
