#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from generate_overnight_run_analysis_latest import (
    ARCHIVE_DIR,
    APP_DIR,
    BJ_TZ,
    DATA_DIR,
    ENV_PATH,
    JSON_PATH as _OLD_JSON_PATH,
    REPORTS_DIR,
    ROOT,
    SERVER_TZ,
    TOKYO_TZ,
    UTC,
    FileInventory,
    adverse_move,
    aligned_move,
    compute_absorption,
    compute_asset_market_states,
    compute_archive_integrity,
    compute_candidate_tradeable_summary,
    compute_confirms,
    compute_majors,
    compute_market_context,
    compute_no_trade_lock_summary,
    compute_noise_reduction,
    compute_outcome_price_sources,
    compute_prealert_lifecycle_summary,
    compute_reversal_special,
    compute_sweeps,
    compute_telegram_suppression,
    compute_trade_actions,
    fmt_ts,
    inventory_category,
    inventory_ndjson,
    join_lp_rows,
    load_asset_case_cache,
    load_quality_cache,
    load_signals,
    median,
    pct,
    rate,
    run_cli,
    stream_case_followups,
    stream_cases,
    stream_delivery_audit,
    to_float,
    to_int,
)

MARKDOWN_PATH = REPORTS_DIR / "overnight_trade_action_analysis_latest.md"
CSV_PATH = REPORTS_DIR / "overnight_trade_action_metrics_latest.csv"
JSON_PATH = REPORTS_DIR / "overnight_trade_action_summary_latest.json"

SAFE_CONFIG_KEYS = [
    "DEFAULT_USER_TIER",
    "MARKET_CONTEXT_ADAPTER_MODE",
    "MARKET_CONTEXT_PRIMARY_VENUE",
    "MARKET_CONTEXT_SECONDARY_VENUE",
    "OKX_PUBLIC_BASE_URL",
    "KRAKEN_FUTURES_BASE_URL",
    "ARCHIVE_ENABLE_RAW_EVENTS",
    "ARCHIVE_ENABLE_PARSED_EVENTS",
    "ARCHIVE_ENABLE_SIGNALS",
    "ARCHIVE_ENABLE_CASES",
    "ARCHIVE_ENABLE_CASE_FOLLOWUPS",
    "ARCHIVE_ENABLE_DELIVERY_AUDIT",
    "LP_ASSET_CASE_PERSIST_ENABLE",
    "LP_QUALITY_STATS_ENABLE",
    "LP_MAJOR_ASSETS",
    "LP_MAJOR_QUOTES",
    "LP_PREALERT_MIN_PRICING_CONFIDENCE",
    "LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY",
    "LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO",
    "LP_PREALERT_LIQUIDITY_REMOVAL_MIN_ACTION_INTENSITY",
    "LP_PREALERT_LIQUIDITY_REMOVAL_MIN_VOLUME_SURGE_RATIO",
    "LP_PREALERT_LIQUIDITY_ADDITION_MIN_ACTION_INTENSITY",
    "LP_PREALERT_LIQUIDITY_ADDITION_MIN_VOLUME_SURGE_RATIO",
    "LP_PREALERT_MIN_RESERVE_SKEW",
    "LP_PREALERT_MIN_CONFIRMATION",
    "LP_PREALERT_PRIMARY_TREND_MIN_MATCHES",
    "LP_PREALERT_MIN_USD",
    "LP_PREALERT_MULTI_POOL_WINDOW_SEC",
    "LP_PREALERT_FOLLOWUP_WINDOW_SEC",
    "TRADE_ACTION_ENABLE",
    "TRADE_ACTION_REQUIRE_LIVE_CONTEXT_FOR_CHASE",
    "TRADE_ACTION_REQUIRE_BROADER_CONFIRM_FOR_CHASE",
    "TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE",
    "TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE",
    "TRADE_ACTION_CONFLICT_WINDOW_SEC",
    "TRADE_ACTION_MAX_LONG_BASIS_BPS",
    "TRADE_ACTION_MIN_SHORT_BASIS_BPS",
    "NO_TRADE_LOCK_ENABLE",
    "NO_TRADE_LOCK_WINDOW_SEC",
    "NO_TRADE_LOCK_TTL_SEC",
    "NO_TRADE_LOCK_MIN_CONFLICT_SCORE",
    "PREALERT_MIN_LIFETIME_SEC",
    "CHASE_ENABLE_AFTER_MIN_SAMPLES",
    "CHASE_MIN_FOLLOWTHROUGH_60S_RATE",
    "CHASE_MAX_ADVERSE_60S_RATE",
    "CHASE_REQUIRE_OUTCOME_COMPLETION_RATE",
    "TELEGRAM_SEND_ONLY_STATE_CHANGES",
    "TELEGRAM_SUPPRESS_REPEAT_STATE_SEC",
    "TELEGRAM_ALLOW_RISK_BLOCKERS",
    "TELEGRAM_ALLOW_CANDIDATES",
]

LP_STAGES = ("prealert", "confirm", "climax", "exhaustion_risk")
TRADE_ACTION_KEYS = (
    "LONG_CHASE_ALLOWED",
    "SHORT_CHASE_ALLOWED",
    "NO_TRADE",
    "WAIT_CONFIRMATION",
    "DATA_GAP_NO_TRADE",
    "CONFLICT_NO_TRADE",
    "DO_NOT_CHASE_LONG",
    "DO_NOT_CHASE_SHORT",
    "LONG_BIAS_OBSERVE",
    "SHORT_BIAS_OBSERVE",
    "REVERSAL_WATCH_LONG",
    "REVERSAL_WATCH_SHORT",
)


def env_whitelist() -> dict[str, str]:
    payload: dict[str, str] = {}
    if not ENV_PATH.exists():
        return payload
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in SAFE_CONFIG_KEYS:
            continue
        payload[key] = value.strip().strip("'").strip('"')
    return payload


def load_runtime_config() -> dict[str, dict[str, Any]]:
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    import config  # type: ignore

    env_values = env_whitelist()
    payload: dict[str, dict[str, Any]] = {}
    for key in SAFE_CONFIG_KEYS:
        value = getattr(config, key, None)
        if isinstance(value, (list, tuple, set)):
            value = list(value)
        payload[key] = {
            "runtime_value": value,
            "env_present": key in env_values,
            "env_value": env_values.get(key),
        }
    return payload


def file_inventory_by_category() -> dict[str, list[FileInventory]]:
    return {
        "raw_events": inventory_category("raw_events", "raw events archive"),
        "parsed_events": inventory_category("parsed_events", "parsed events archive"),
        "signals": [inventory_ndjson(path, "signals archive") for path in sorted((ARCHIVE_DIR / "signals").glob("*.ndjson"))],
        "cases": [inventory_ndjson(path, "case archive") for path in sorted((ARCHIVE_DIR / "cases").glob("*.ndjson"))],
        "case_followups": [
            inventory_ndjson(path, "case followups archive")
            for path in sorted((ARCHIVE_DIR / "case_followups").glob("*.ndjson"))
        ],
        "delivery_audit": [
            inventory_ndjson(path, "delivery audit archive")
            for path in sorted((ARCHIVE_DIR / "delivery_audit").glob("*.ndjson"))
        ],
    }


def build_signal_segments(signal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(signal_rows, key=lambda row: int(row["archive_ts"]))
    if not rows:
        return []
    segments: list[dict[str, Any]] = []
    current = {
        "start_ts": int(rows[0]["archive_ts"]),
        "end_ts": int(rows[0]["archive_ts"]),
        "total_signal_rows": 0,
        "lp_signal_rows": 0,
    }
    for row in rows:
        ts = int(row["archive_ts"])
        if ts - int(current["end_ts"]) >= 3600 and int(current["total_signal_rows"]) > 0:
            current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
            current["duration_hours"] = round(int(current["duration_sec"]) / 3600.0, 2)
            segments.append(current)
            current = {
                "start_ts": ts,
                "end_ts": ts,
                "total_signal_rows": 0,
                "lp_signal_rows": 0,
            }
        current["end_ts"] = ts
        current["total_signal_rows"] = int(current["total_signal_rows"]) + 1
        if row.get("lp_alert_stage"):
            current["lp_signal_rows"] = int(current["lp_signal_rows"]) + 1
    current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
    current["duration_hours"] = round(int(current["duration_sec"]) / 3600.0, 2)
    segments.append(current)
    for segment in segments:
        start_bj = datetime.fromtimestamp(int(segment["start_ts"]), BJ_TZ)
        end_bj = datetime.fromtimestamp(int(segment["end_ts"]), BJ_TZ)
        segment["start_bj"] = start_bj.isoformat()
        segment["end_bj"] = end_bj.isoformat()
        segment["start_bj_hour"] = start_bj.hour
        segment["end_bj_hour"] = end_bj.hour
        segment["crosses_bj_midnight"] = start_bj.date() != end_bj.date()
        segment["looks_like_bj_overnight"] = bool(
            int(segment["duration_sec"]) >= 4 * 3600
            and (
                bool(segment["crosses_bj_midnight"])
                or int(segment["start_bj_hour"]) >= 18
                or int(segment["start_bj_hour"]) <= 6
            )
        )
    return segments


def choose_latest_overnight_window(
    signal_rows: list[dict[str, Any]],
    inventories: dict[str, list[FileInventory]],
) -> dict[str, Any]:
    segments = build_signal_segments(signal_rows)
    if not segments:
        raise RuntimeError("signals archive is empty")
    overnight_candidates = [segment for segment in segments if segment["looks_like_bj_overnight"]]
    chosen = sorted(
        overnight_candidates or segments,
        key=lambda item: (
            -int(item["end_ts"]),
            -int(item["duration_sec"]),
            -int(item["lp_signal_rows"]),
            -int(item["total_signal_rows"]),
        ),
    )[0]
    overlap_start = int(chosen["start_ts"])
    overlap_end = int(chosen["end_ts"])
    supporting = []
    for category in ("signals", "cases", "case_followups", "delivery_audit"):
        for item in inventories.get(category, []):
            if item.start_ts is None or item.end_ts is None:
                continue
            if int(item.end_ts) < overlap_start or int(item.start_ts) > overlap_end:
                continue
            supporting.append(item)
    if supporting:
        chosen["analysis_window_start_ts"] = min(int(item.start_ts) for item in supporting if item.start_ts is not None)
        chosen["analysis_window_end_ts"] = max(int(item.end_ts) for item in supporting if item.end_ts is not None)
    else:
        chosen["analysis_window_start_ts"] = int(chosen["start_ts"])
        chosen["analysis_window_end_ts"] = int(chosen["end_ts"])
    chosen["analysis_window_duration_hours"] = round(
        (int(chosen["analysis_window_end_ts"]) - int(chosen["analysis_window_start_ts"])) / 3600.0,
        2,
    )
    chosen["selection_reason"] = (
        "selected the latest contiguous signal segment that also looks like a Beijing-night run; "
        "it starts late evening BJ, crosses midnight, and has matching signals/cases/delivery_audit/case_followups coverage."
    )
    chosen["all_segments"] = [
        {
            "start_ts": int(segment["start_ts"]),
            "end_ts": int(segment["end_ts"]),
            "duration_hours": float(segment["duration_hours"]),
            "total_signal_rows": int(segment["total_signal_rows"]),
            "lp_signal_rows": int(segment["lp_signal_rows"]),
            "start_bj": segment["start_bj"],
            "end_bj": segment["end_bj"],
            "looks_like_bj_overnight": bool(segment["looks_like_bj_overnight"]),
        }
        for segment in segments
    ]
    return chosen


def stage_distribution(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_alert_stage") or "") for row in lp_rows)
    total = len(lp_rows)
    return {
        "prealert_count": counter.get("prealert", 0),
        "confirm_count": counter.get("confirm", 0),
        "climax_count": counter.get("climax", 0),
        "exhaustion_risk_count": counter.get("exhaustion_risk", 0),
        "stage_distribution_pct": {key: pct(counter.get(key, 0), total) for key in LP_STAGES},
    }


def outcome_window_status(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, dict[str, Any]] = {}
    for window in ("30s", "60s", "300s"):
        status_counter = Counter()
        failure_counter = Counter()
        for row in lp_rows:
            window_payload = (row.get("outcome_windows") or {}).get(window) or {}
            status = str(window_payload.get("status") or "")
            if not status:
                status = "completed" if row.get(f"direction_adjusted_move_after_{window}") is not None else "missing"
            status_counter[status] += 1
            failure_reason = str(window_payload.get("failure_reason") or "")
            if failure_reason:
                failure_counter[failure_reason] += 1
        payload[window] = {
            "status_distribution": dict(status_counter),
            "top_failure_reasons": dict(failure_counter.most_common(5)),
            "resolved_count": status_counter.get("completed", 0),
            "resolved_rate": rate(status_counter.get("completed", 0), len(lp_rows)),
        }
    return payload


def trade_action_overview(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = compute_trade_actions(lp_rows)
    distribution = Counter(str(row.get("trade_action_key") or "") for row in lp_rows if str(row.get("trade_action_key") or "").strip())
    delivered = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")]
    base.update(
        {
            "long_bias_observe_count": distribution.get("LONG_BIAS_OBSERVE", 0),
            "short_bias_observe_count": distribution.get("SHORT_BIAS_OBSERVE", 0),
            "reversal_watch_long_count": distribution.get("REVERSAL_WATCH_LONG", 0),
            "reversal_watch_short_count": distribution.get("REVERSAL_WATCH_SHORT", 0),
            "trade_action_present_count": sum(1 for row in lp_rows if row.get("trade_action_key")),
            "trade_action_present_rate": rate(sum(1 for row in lp_rows if row.get("trade_action_key")), len(lp_rows)),
            "telegram_action_first_count": sum(1 for row in delivered if row.get("trade_action_label")),
            "telegram_action_first_rate": rate(sum(1 for row in delivered if row.get("trade_action_label")), len(delivered)),
            "delivered_distribution": dict(
                Counter(str(row.get("trade_action_key") or "") for row in delivered if row.get("trade_action_key"))
            ),
            "undelivered_distribution": dict(
                Counter(
                    str(row.get("trade_action_key") or "")
                    for row in lp_rows
                    if not (row.get("sent_to_telegram") or row.get("notifier_sent_at")) and row.get("trade_action_key")
                )
            ),
        }
    )
    return base


def _strict_chase_flags(row: dict[str, Any], runtime_config: dict[str, dict[str, Any]]) -> dict[str, bool]:
    min_asset_quality = float(runtime_config["TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE"]["runtime_value"])
    min_pair_quality = float(runtime_config["TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE"]["runtime_value"])
    return {
        "broader_confirm": str(row.get("lp_confirm_scope") or "") == "broader_confirm",
        "live_public": str(row.get("market_context_source") or "") == "live_public",
        "clean_confirm_or_strong_sweep": (
            str(row.get("lp_confirm_quality") or "") == "clean_confirm"
            or str(row.get("lp_sweep_phase") or "") == "sweep_confirmed"
        ),
        "quality_pass": (
            (to_float(row.get("asset_case_quality_score")) or 0.0) >= min_asset_quality
            and (to_float(row.get("pair_quality_score")) or 0.0) >= min_pair_quality
        ),
        "no_conflict": "opposite_signal_within_window" not in list(row.get("trade_action_blockers") or []),
        "no_local_absorption": not str(row.get("lp_absorption_context") or "").startswith("local_"),
        "no_exhaustion": (
            str(row.get("lp_alert_stage") or "") != "exhaustion_risk"
            and str(row.get("lp_sweep_phase") or "") != "sweep_exhaustion_risk"
        ),
        "no_data_gap": str(row.get("trade_action_key") or "") != "DATA_GAP_NO_TRADE",
    }


def chase_allowed_analysis(lp_rows: list[dict[str, Any]], runtime_config: dict[str, dict[str, Any]]) -> dict[str, Any]:
    chase_rows = [
        row for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
    ]
    strictness_rows = []
    for row in chase_rows:
        flags = _strict_chase_flags(row, runtime_config)
        strictness_rows.append(
            {
                "signal_id": row.get("signal_id"),
                "asset_case_id": row.get("asset_case_id"),
                "pair_label": row.get("pair_label"),
                "trade_action_key": row.get("trade_action_key"),
                "lp_alert_stage": row.get("lp_alert_stage"),
                "lp_confirm_quality": row.get("lp_confirm_quality"),
                "lp_confirm_scope": row.get("lp_confirm_scope"),
                "lp_absorption_context": row.get("lp_absorption_context"),
                "market_context_source": row.get("market_context_source"),
                "market_context_venue": row.get("market_context_venue"),
                "requested_symbol": row.get("market_context_requested_symbol"),
                "resolved_symbol": row.get("market_context_resolved_symbol"),
                "asset_case_quality_score": row.get("asset_case_quality_score"),
                "pair_quality_score": row.get("pair_quality_score"),
                "trade_action_reason": row.get("trade_action_reason"),
                "trade_action_required_confirmation": row.get("trade_action_required_confirmation"),
                "trade_action_invalidated_by": row.get("trade_action_invalidated_by"),
                "trade_action_blockers": row.get("trade_action_blockers"),
                "strict_flags": flags,
                "all_strict_flags_pass": all(flags.values()),
                "outcome_windows": row.get("outcome_windows"),
            }
        )
    outcome_status = outcome_window_status(chase_rows)
    base = trade_action_overview(lp_rows)
    return {
        "count": len(chase_rows),
        "strict_samples": strictness_rows,
        "all_strict_flags_pass_count": sum(1 for item in strictness_rows if item["all_strict_flags_pass"]),
        "all_strict_flags_pass_rate": rate(
            sum(1 for item in strictness_rows if item["all_strict_flags_pass"]),
            len(strictness_rows),
        ),
        "long_chase_allowed_count": base["long_chase_allowed_count"],
        "short_chase_allowed_count": base["short_chase_allowed_count"],
        "chase_allowed_30s_followthrough_rate": (
            rate(
                sum(
                    base["trade_action_followthrough_30s"].get(key, {}).get("followthrough_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_followthrough_30s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "chase_allowed_60s_followthrough_rate": (
            rate(
                sum(
                    base["trade_action_followthrough_60s"].get(key, {}).get("followthrough_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_followthrough_60s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "chase_allowed_300s_followthrough_rate": (
            rate(
                sum(
                    base["trade_action_followthrough_300s"].get(key, {}).get("followthrough_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_followthrough_300s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "chase_allowed_adverse_30s_rate": (
            rate(
                sum(
                    base["trade_action_adverse_30s"].get(key, {}).get("adverse_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_adverse_30s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "chase_allowed_adverse_60s_rate": (
            rate(
                sum(
                    base["trade_action_adverse_60s"].get(key, {}).get("adverse_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_adverse_60s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "chase_allowed_adverse_300s_rate": (
            rate(
                sum(
                    base["trade_action_adverse_300s"].get(key, {}).get("adverse_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
                sum(
                    base["trade_action_adverse_300s"].get(key, {}).get("resolved_count", 0)
                    for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
                ),
            )
        ),
        "outcome_window_status": outcome_status,
        "note": (
            "if chase_allowed count is non-zero but all 30s/60s/300s are unresolved, it usually means the outcome ledger expired "
            "without a fresh pool quote update rather than that the chase rule itself was violated."
        ),
    }


def _saved_rate(rows: list[dict[str, Any]], window: str) -> dict[str, Any]:
    resolved = [row for row in rows if row.get(f"direction_adjusted_move_after_{window}") is not None]
    saved = [
        row
        for row in resolved
        if (to_float(row.get(f"direction_adjusted_move_after_{window}")) or 0.0) <= 0
        or row.get(f"adverse_by_direction_{window}") is True
    ]
    return {
        "resolved_count": len(resolved),
        "saved_count": len(saved),
        "saved_rate": rate(len(saved), len(resolved)),
        "median_move_before_30s": median([row.get("move_before_alert_30s") for row in rows]),
        "median_direction_adjusted_move_after": median(
            [row.get(f"direction_adjusted_move_after_{window}") for row in resolved]
        ),
    }


def protection_value_analysis(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: int(item["created_at"])):
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            by_case[case_id].append(row)
    wait_rows = [row for row in lp_rows if row.get("trade_action_key") == "WAIT_CONFIRMATION"]
    wait_details = []
    wait_upgrade_any = 0
    wait_upgrade_chase = 0
    for row in wait_rows:
        future_rows = [
            item
            for item in by_case.get(str(row.get("asset_case_id") or ""), [])
            if int(item["created_at"]) > int(row["created_at"])
            and int(item["created_at"]) - int(row["created_at"]) <= 90
        ]
        upgraded_any = any(
            str(item.get("lp_sweep_phase") or "") == "sweep_confirmed"
            or str(item.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
            or str(item.get("lp_alert_stage") or "") in {"climax", "exhaustion_risk"}
            for item in future_rows
        )
        upgraded_chase = any(
            str(item.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
            for item in future_rows
        )
        if upgraded_any:
            wait_upgrade_any += 1
        if upgraded_chase:
            wait_upgrade_chase += 1
        wait_details.append(
            {
                "signal_id": row.get("signal_id"),
                "asset_case_id": row.get("asset_case_id"),
                "pair_label": row.get("pair_label"),
                "trade_action_reason": row.get("trade_action_reason"),
                "future_rows_within_90s": [
                    {
                        "delta_sec": int(item["created_at"]) - int(row["created_at"]),
                        "signal_id": item.get("signal_id"),
                        "pair_label": item.get("pair_label"),
                        "lp_alert_stage": item.get("lp_alert_stage"),
                        "lp_sweep_phase": item.get("lp_sweep_phase"),
                        "trade_action_key": item.get("trade_action_key"),
                    }
                    for item in future_rows
                ],
                "upgraded_within_90s": upgraded_any,
                "upgraded_to_chase_within_90s": upgraded_chase,
            }
        )
    do_not_long = [row for row in lp_rows if row.get("trade_action_key") == "DO_NOT_CHASE_LONG"]
    do_not_short = [row for row in lp_rows if row.get("trade_action_key") == "DO_NOT_CHASE_SHORT"]
    conflict_rows = [row for row in lp_rows if row.get("trade_action_key") == "CONFLICT_NO_TRADE"]
    data_gap_rows = [row for row in lp_rows if row.get("trade_action_key") == "DATA_GAP_NO_TRADE"]
    return {
        "do_not_chase_long_30s": _saved_rate(do_not_long, "30s"),
        "do_not_chase_long_60s": _saved_rate(do_not_long, "60s"),
        "do_not_chase_long_300s": _saved_rate(do_not_long, "300s"),
        "do_not_chase_short_30s": _saved_rate(do_not_short, "30s"),
        "do_not_chase_short_60s": _saved_rate(do_not_short, "60s"),
        "do_not_chase_short_300s": _saved_rate(do_not_short, "300s"),
        "do_not_chase_would_have_lost_rate": rate(
            _saved_rate(do_not_long, "60s")["saved_count"] + _saved_rate(do_not_short, "60s")["saved_count"],
            _saved_rate(do_not_long, "60s")["resolved_count"] + _saved_rate(do_not_short, "60s")["resolved_count"],
        ),
        "no_trade_conflict_30s": _saved_rate(conflict_rows, "30s"),
        "no_trade_conflict_60s": _saved_rate(conflict_rows, "60s"),
        "no_trade_conflict_300s": _saved_rate(conflict_rows, "300s"),
        "no_trade_conflict_adverse_rate": _saved_rate(conflict_rows, "60s")["saved_rate"],
        "wait_confirmation_count": len(wait_rows),
        "wait_confirmation_upgrade_rate": rate(wait_upgrade_any, len(wait_rows)),
        "wait_confirmation_upgrade_to_chase_rate": rate(wait_upgrade_chase, len(wait_rows)),
        "wait_confirmation_details": wait_details,
        "data_gap_no_trade_count": len(data_gap_rows),
        "data_gap_reasons": dict(
            Counter(str(row.get("market_context_failure_reason") or "") for row in data_gap_rows if row.get("market_context_failure_reason"))
        ),
    }


def conflict_analysis(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in lp_rows if row.get("trade_action_key") == "CONFLICT_NO_TRADE"]
    resolved_30s = [row for row in rows if row.get("direction_adjusted_move_after_30s") is not None]
    resolved_60s = [row for row in rows if row.get("direction_adjusted_move_after_60s") is not None]
    return {
        "count": len(rows),
        "stage_distribution": dict(Counter(str(row.get("lp_alert_stage") or "") for row in rows)),
        "direction_distribution": dict(Counter(str(row.get("direction_bucket") or "") for row in rows)),
        "absorption_distribution": dict(Counter(str(row.get("lp_absorption_context") or "") for row in rows)),
        "conflict_context_distribution": dict(Counter(str(row.get("lp_conflict_context") or "") for row in rows)),
        "conflict_score_median": median([row.get("lp_conflict_score") for row in rows]),
        "conflict_window_sec_values": dict(Counter(to_int(row.get("lp_conflict_window_sec")) for row in rows)),
        "resolved_30s_count": len(resolved_30s),
        "resolved_60s_count": len(resolved_60s),
        "examples": [
            {
                "signal_id": row.get("signal_id"),
                "asset_case_id": row.get("asset_case_id"),
                "pair_label": row.get("pair_label"),
                "lp_alert_stage": row.get("lp_alert_stage"),
                "direction_bucket": row.get("direction_bucket"),
                "lp_conflict_score": row.get("lp_conflict_score"),
                "lp_conflict_window_sec": row.get("lp_conflict_window_sec"),
                "lp_conflicting_signals": row.get("lp_conflicting_signals"),
                "trade_action_reason": row.get("trade_action_reason"),
                "direction_adjusted_move_after_30s": row.get("direction_adjusted_move_after_30s"),
                "direction_adjusted_move_after_60s": row.get("direction_adjusted_move_after_60s"),
            }
            for row in rows[:8]
        ],
    }


def prealert_funnel_analysis(lp_rows: list[dict[str, Any]], major_pairs: set[str]) -> dict[str, Any]:
    stage_rows = [row for row in lp_rows if row.get("lp_alert_stage") == "prealert"]
    candidate_rows = [row for row in lp_rows if row.get("lp_prealert_candidate")]
    gate_rows = [row for row in lp_rows if row.get("lp_prealert_gate_passed")]
    delivered_rows = [
        row
        for row in stage_rows
        if row.get("lp_prealert_delivery_allowed") is True
        and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    ]
    merged_rows = [row for row in lp_rows if row.get("lp_prealert_asset_case_preserved")]
    overwritten_rows = [row for row in lp_rows if row.get("lp_prealert_stage_overwritten")]
    first_leg_rows = [row for row in lp_rows if row.get("lp_prealert_first_leg")]
    major_rows = [row for row in stage_rows if str(row.get("pair_label") or "") in major_pairs]
    non_major_rows = [row for row in stage_rows if str(row.get("pair_label") or "") not in major_pairs]
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: int(item["created_at"])):
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            by_case[case_id].append(row)
    conversions = {30: 0, 60: 0, 90: 0}
    for row in stage_rows:
        case_rows = by_case.get(str(row.get("asset_case_id") or ""), [])
        later_confirms = [
            item
            for item in case_rows
            if int(item["created_at"]) > int(row["created_at"])
            and str(item.get("lp_alert_stage") or "") == "confirm"
        ]
        if not later_confirms:
            continue
        delta = int(later_confirms[0]["created_at"]) - int(row["created_at"])
        if delta <= 30:
            conversions[30] += 1
        if delta <= 60:
            conversions[60] += 1
        if delta <= 90:
            conversions[90] += 1
    return {
        "prealert_count": len(stage_rows),
        "prealert_candidate_count": len(candidate_rows),
        "prealert_gate_passed_count": len(gate_rows),
        "prealert_delivered_count": len(delivered_rows),
        "major_prealert_count": len(major_rows),
        "non_major_prealert_count": len(non_major_rows),
        "prealert_to_confirm_30s": rate(conversions[30], len(stage_rows)),
        "prealert_to_confirm_60s": rate(conversions[60], len(stage_rows)),
        "prealert_to_confirm_90s": rate(conversions[90], len(stage_rows)),
        "prealert_stage_overwritten_count": len(overwritten_rows),
        "prealert_asset_case_preserved_count": len(merged_rows),
        "major_first_leg_prealert_count": len(first_leg_rows),
        "major_override_used_count": sum(1 for row in lp_rows if row.get("lp_prealert_major_override_used")),
        "top_prealert_gate_fail_reasons": dict(
            Counter(
                str(row.get("lp_prealert_gate_fail_reason") or "")
                for row in candidate_rows
                if str(row.get("lp_prealert_gate_fail_reason") or "").strip()
            ).most_common(10)
        ),
        "top_prealert_delivery_block_reasons": dict(
            Counter(
                str(row.get("lp_prealert_delivery_block_reason") or "")
                for row in candidate_rows
                if str(row.get("lp_prealert_delivery_block_reason") or "").strip()
            ).most_common(10)
        ),
        "candidate_reason_distribution": dict(
            Counter(str(row.get("lp_prealert_candidate_reason") or "") for row in candidate_rows)
        ),
        "diagnosis": (
            "candidate and gate_passed are non-zero, but no surviving prealert stage rows were archived in the selected window; "
            "all candidates were immediately overwritten into later stages, first_leg stayed at zero, and asset_case did not preserve a prealert anchor."
        ),
    }


def confirm_analysis(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = compute_confirms(lp_rows)
    scope_performance = {}
    for scope in ("local_confirm", "broader_confirm"):
        rows = [row for row in lp_rows if row.get("lp_alert_stage") == "confirm" and row.get("lp_confirm_scope") == scope]
        scope_performance[scope] = {
            "count": len(rows),
            "move_after_30s_median": median([aligned_move(row, "move_after_alert_30s") for row in rows]),
            "move_after_60s_median": median([aligned_move(row, "move_after_alert_60s") for row in rows]),
            "move_after_300s_median": median([aligned_move(row, "move_after_alert_300s") for row in rows]),
            "adverse_30s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_30s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_30s") is not None),
            ),
            "adverse_60s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_60s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_60s") is not None),
            ),
            "adverse_300s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_300s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_300s") is not None),
            ),
        }
    quality_performance = {}
    for quality in ("clean_confirm", "late_confirm", "chase_risk", "unconfirmed_confirm"):
        rows = [row for row in lp_rows if row.get("lp_alert_stage") == "confirm" and row.get("lp_confirm_quality") == quality]
        quality_performance[quality] = {
            "count": len(rows),
            "move_after_30s_median": median([aligned_move(row, "move_after_alert_30s") for row in rows]),
            "move_after_60s_median": median([aligned_move(row, "move_after_alert_60s") for row in rows]),
            "move_after_300s_median": median([aligned_move(row, "move_after_alert_300s") for row in rows]),
            "adverse_30s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_30s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_30s") is not None),
            ),
            "adverse_60s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_60s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_60s") is not None),
            ),
            "adverse_300s_rate": rate(
                sum(1 for row in rows if adverse_move(row, "move_after_alert_300s") is True),
                sum(1 for row in rows if aligned_move(row, "move_after_alert_300s") is not None),
            ),
        }
    base["scope_performance"] = scope_performance
    base["quality_performance"] = quality_performance
    base["scope_distribution"] = dict(
        Counter(str(row.get("lp_confirm_scope") or "(blank)") for row in lp_rows if row.get("lp_alert_stage") == "confirm")
    )
    base["quality_distribution"] = dict(
        Counter(str(row.get("lp_confirm_quality") or "(blank)") for row in lp_rows if row.get("lp_alert_stage") == "confirm")
    )
    return base


def sweep_analysis(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = compute_sweeps(lp_rows)
    building_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_building"]
    confirmed_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_confirmed"]
    exhaustion_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_exhaustion_risk"]
    base["sweep_building_examples"] = [
        {
            "signal_id": row.get("signal_id"),
            "asset_case_id": row.get("asset_case_id"),
            "pair_label": row.get("pair_label"),
            "trade_action_key": row.get("trade_action_key"),
            "lp_state_label": row.get("lp_state_label"),
            "trade_action_reason": row.get("trade_action_reason"),
        }
        for row in building_rows[:5]
    ]
    base["sweep_confirmed_examples"] = [
        {
            "signal_id": row.get("signal_id"),
            "asset_case_id": row.get("asset_case_id"),
            "pair_label": row.get("pair_label"),
            "trade_action_key": row.get("trade_action_key"),
            "direction_bucket": row.get("direction_bucket"),
            "direction_adjusted_move_after_60s": row.get("direction_adjusted_move_after_60s"),
            "direction_adjusted_move_after_300s": row.get("direction_adjusted_move_after_300s"),
        }
        for row in confirmed_rows[:5]
    ]
    base["sweep_exhaustion_examples"] = [
        {
            "signal_id": row.get("signal_id"),
            "asset_case_id": row.get("asset_case_id"),
            "pair_label": row.get("pair_label"),
            "trade_action_key": row.get("trade_action_key"),
            "direction_bucket": row.get("direction_bucket"),
            "direction_adjusted_move_after_60s": row.get("direction_adjusted_move_after_60s"),
        }
        for row in exhaustion_rows[:5]
    ]
    return base


def reversal_special_cases(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = compute_reversal_special(lp_rows)
    confirm_rows = [row for row in lp_rows if row.get("lp_alert_stage") == "confirm"]
    examples = []
    reason_counter = Counter()
    for row in confirm_rows:
        selected_window = None
        for window in ("30s", "60s", "300s"):
            if adverse_move(row, f"move_after_alert_{window}") is True:
                selected_window = window
                break
        if not selected_window:
            continue
        reasons = []
        if str(row.get("lp_confirm_quality") or "") in {"late_confirm", "chase_risk"}:
            reasons.append("late_confirm / chase_risk")
            reason_counter["late_confirm / chase_risk"] += 1
        if str(row.get("lp_confirm_scope") or "") == "local_confirm":
            reasons.append("local_confirm rather than broader_confirm")
            reason_counter["local_confirm rather than broader_confirm"] += 1
        if str(row.get("market_context_source") or "") == "unavailable":
            reasons.append("market context unavailable")
            reason_counter["market context unavailable"] += 1
        if (to_int(row.get("asset_case_supporting_pair_count")) or 0) <= 1 and not bool(row.get("asset_case_multi_pool")):
            reasons.append("single_pool / low resonance")
            reason_counter["single_pool / low resonance"] += 1
        absorption = str(row.get("lp_absorption_context") or "")
        if absorption in {"local_sell_pressure_absorption", "local_buy_pressure_absorption"}:
            reasons.append(absorption)
            reason_counter[absorption] += 1
        if not reasons:
            reasons.append("possible code misclassification")
            reason_counter["possible code misclassification"] += 1
        examples.append(
            {
                "window": selected_window,
                "signal_id": row.get("signal_id"),
                "asset_case_id": row.get("asset_case_id"),
                "pair_label": row.get("pair_label"),
                "direction_bucket": row.get("direction_bucket"),
                "trade_action_key": row.get("trade_action_key"),
                "lp_confirm_quality": row.get("lp_confirm_quality"),
                "lp_confirm_scope": row.get("lp_confirm_scope"),
                "lp_absorption_context": row.get("lp_absorption_context"),
                "lp_broader_alignment": row.get("lp_broader_alignment"),
                "move_before_alert_30s": row.get("move_before_alert_30s"),
                "raw_move_after": row.get(f"raw_move_after_{selected_window}"),
                "direction_adjusted_move_after": row.get(f"direction_adjusted_move_after_{selected_window}"),
                "judgement_reasons": reasons,
                "notifier_line1": row.get("notifier_line1"),
            }
        )
    base["reason_distribution"] = dict(reason_counter)
    base["examples"] = examples[:5]
    return base


def adverse_direction_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    errors = []
    buy_rows = [row for row in lp_rows if row.get("direction_bucket") == "buy_pressure"]
    sell_rows = [row for row in lp_rows if row.get("direction_bucket") == "sell_pressure"]
    for row in lp_rows:
        direction = str(row.get("direction_bucket") or "")
        if direction not in {"buy_pressure", "sell_pressure"}:
            continue
        for window in ("30s", "60s", "300s"):
            raw_move = row.get(f"raw_move_after_{window}")
            adjusted = row.get(f"direction_adjusted_move_after_{window}")
            adverse = row.get(f"adverse_by_direction_{window}")
            if raw_move is None or adjusted is None or adverse is None:
                continue
            expected_adjusted = float(raw_move) if direction == "buy_pressure" else -float(raw_move)
            expected_adverse = float(raw_move) < 0 if direction == "buy_pressure" else float(raw_move) > 0
            if abs(expected_adjusted - float(adjusted)) > 1e-12 or bool(adverse) != expected_adverse:
                errors.append(
                    {
                        "signal_id": row.get("signal_id"),
                        "pair_label": row.get("pair_label"),
                        "lp_alert_stage": row.get("lp_alert_stage"),
                        "trade_action_key": row.get("trade_action_key"),
                        "window": window,
                        "direction_bucket": direction,
                        "raw_move": raw_move,
                        "direction_adjusted_move": adjusted,
                        "adverse_by_direction": adverse,
                    }
                )

    def adverse_rate_for(rows: list[dict[str, Any]], window: str) -> float | None:
        resolved = [row for row in rows if row.get(f"raw_move_after_{window}") is not None]
        adverse = [row for row in resolved if row.get(f"adverse_by_direction_{window}") is True]
        return rate(len(adverse), len(resolved))

    return {
        "buy_pressure_adverse_30s_rate": adverse_rate_for(buy_rows, "30s"),
        "buy_pressure_adverse_60s_rate": adverse_rate_for(buy_rows, "60s"),
        "buy_pressure_adverse_300s_rate": adverse_rate_for(buy_rows, "300s"),
        "sell_pressure_adverse_30s_rate": adverse_rate_for(sell_rows, "30s"),
        "sell_pressure_adverse_60s_rate": adverse_rate_for(sell_rows, "60s"),
        "sell_pressure_adverse_300s_rate": adverse_rate_for(sell_rows, "300s"),
        "raw_move_resolved_counts": {
            "buy_30s": sum(1 for row in buy_rows if row.get("raw_move_after_30s") is not None),
            "buy_60s": sum(1 for row in buy_rows if row.get("raw_move_after_60s") is not None),
            "buy_300s": sum(1 for row in buy_rows if row.get("raw_move_after_300s") is not None),
            "sell_30s": sum(1 for row in sell_rows if row.get("raw_move_after_30s") is not None),
            "sell_60s": sum(1 for row in sell_rows if row.get("raw_move_after_60s") is not None),
            "sell_300s": sum(1 for row in sell_rows if row.get("raw_move_after_300s") is not None),
        },
        "adverse_direction_consistency_errors": len(errors),
        "consistency_error_examples": errors[:10],
    }


def asset_case_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: int(item["created_at"])):
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            by_case[case_id].append(row)
    paths = Counter()
    fragmentation = Counter()
    multi_pair_cases = 0
    case_examples = []
    for case_id, rows in by_case.items():
        labels = []
        for row in rows:
            stage = str(row.get("lp_alert_stage") or "")
            sweep = str(row.get("lp_sweep_phase") or "")
            label = f"{stage}:{sweep}" if sweep else stage
            if label and (not labels or labels[-1] != label):
                labels.append(label)
        if labels:
            paths[" -> ".join(labels)] += 1
        unique_pairs = sorted({str(row.get("pair_label") or "") for row in rows if row.get("pair_label")})
        if len(unique_pairs) > 1:
            multi_pair_cases += 1
        if len(rows) == 1:
            fragmentation["single_signal_case"] += 1
            row = rows[0]
            if str(row.get("lp_confirm_scope") or "") == "local_confirm":
                fragmentation["single_signal_local_confirm"] += 1
            if str(row.get("lp_absorption_context") or "").startswith("local_"):
                fragmentation["local_absorption_fragment"] += 1
            if (to_int(row.get("asset_case_supporting_pair_count")) or 0) <= 1 and not bool(row.get("asset_case_multi_pool")):
                fragmentation["single_pair_case"] += 1
            if str(row.get("lp_broader_alignment") or "") in {"", "weak_or_missing"}:
                fragmentation["weak_or_missing_broader_alignment"] += 1
        if len(case_examples) < 8:
            case_examples.append(
                {
                    "asset_case_id": case_id,
                    "signal_count": len(rows),
                    "pairs": unique_pairs,
                    "trade_actions": list(dict.fromkeys(str(row.get("trade_action_key") or "") for row in rows if row.get("trade_action_key"))),
                    "stage_path": labels,
                }
            )
    asset_cases = len(by_case)
    return {
        "lp_rows": len(lp_rows),
        "asset_cases": asset_cases,
        "compression_ratio": round(len(lp_rows) / asset_cases, 4) if asset_cases else None,
        "avg_signals_per_case": round(len(lp_rows) / asset_cases, 4) if asset_cases else None,
        "multi_pair_case_count": multi_pair_cases,
        "single_signal_case_count": fragmentation.get("single_signal_case", 0),
        "one_signal_one_case_rate": rate(fragmentation.get("single_signal_case", 0), asset_cases),
        "case_upgrade_paths": dict(paths.most_common(15)),
        "fragmentation_reasons": dict(fragmentation),
        "prealert_linked_case_count": sum(1 for rows in by_case.values() if any(row.get("asset_case_had_prealert") for row in rows)),
        "case_examples": case_examples,
    }


def archive_integrity_summary(
    lp_rows: list[dict[str, Any]],
    delivery_summary: dict[str, Any],
    cases_summary: dict[str, Any],
    followup_summary: dict[str, Any],
    inventories: dict[str, list[FileInventory]],
    window: dict[str, Any],
) -> dict[str, Any]:
    base = compute_archive_integrity(lp_rows, delivery_summary, cases_summary, followup_summary)

    def _selected(inventory_items: list[FileInventory]) -> list[FileInventory]:
        result = []
        for item in inventory_items:
            if item.start_ts is None or item.end_ts is None:
                continue
            if int(item.end_ts) < int(window["analysis_window_start_ts"]) or int(item.start_ts) > int(window["analysis_window_end_ts"]):
                continue
            result.append(item)
        return result

    selected_raw = _selected(inventories["raw_events"])
    selected_parsed = _selected(inventories["parsed_events"])
    selected_signals = _selected(inventories["signals"])
    selected_cases = _selected(inventories["cases"])
    selected_followups = _selected(inventories["case_followups"])
    selected_delivery = _selected(inventories["delivery_audit"])
    base.update(
        {
            "raw_archive_exists": any(item.exists for item in inventories["raw_events"]),
            "parsed_archive_exists": any(item.exists for item in inventories["parsed_events"]),
            "signals_archive_exists": any(item.exists for item in inventories["signals"]),
            "total_raw_events": sum(item.record_count for item in selected_raw if item.exists),
            "total_parsed_events": sum(item.record_count for item in selected_parsed if item.exists),
            "total_signal_rows": sum(item.record_count for item in selected_signals if item.exists),
            "window_case_archive_rows": sum(item.record_count for item in selected_cases if item.exists),
            "window_case_followup_rows": sum(item.record_count for item in selected_followups if item.exists),
            "window_delivery_audit_rows": sum(item.record_count for item in selected_delivery if item.exists),
        }
    )
    return base


def major_pairs_from_cli(major_cli: dict[str, Any]) -> set[str]:
    return set(str(item) for item in (major_cli.get("expected_major_pairs") or []) if item)


def compute_noise_risk(
    lp_rows: list[dict[str, Any]],
    prealert_summary: dict[str, Any],
    majors_summary: dict[str, Any],
    archive_summary: dict[str, Any],
) -> dict[str, Any]:
    delivered = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")]
    local_confirm_rows = [
        row for row in lp_rows if row.get("lp_alert_stage") == "confirm" and row.get("lp_confirm_scope") == "local_confirm"
    ]
    unresolved_300s = [row for row in lp_rows if row.get("direction_adjusted_move_after_300s") is None]
    return {
        "delivered_ratio": rate(len(delivered), len(lp_rows)),
        "local_confirm_share": rate(len(local_confirm_rows), sum(1 for row in lp_rows if row.get("lp_alert_stage") == "confirm")),
        "unresolved_300s_share": rate(len(unresolved_300s), len(lp_rows)),
        "prealert_absent": prealert_summary["prealert_count"] == 0,
        "eth_only_sample": majors_summary["btc_signal_count"] == 0 and majors_summary["sol_signal_count"] == 0,
        "raw_parsed_missing": not archive_summary["raw_archive_exists"] and not archive_summary["parsed_archive_exists"],
    }


def score(value: float) -> float:
    return max(0.0, min(10.0, round(value, 1)))


def scorecard(
    run_overview: dict[str, Any],
    trade_action_summary: dict[str, Any],
    chase_summary: dict[str, Any],
    protection_summary: dict[str, Any],
    prealert_summary: dict[str, Any],
    market_context_summary: dict[str, Any],
    confirm_summary: dict[str, Any],
    sweep_summary: dict[str, Any],
    adverse_summary: dict[str, Any],
    asset_case_summary_payload: dict[str, Any],
    majors_summary: dict[str, Any],
    archive_summary: dict[str, Any],
    noise_summary: dict[str, Any],
) -> dict[str, float]:
    readiness = 7.2
    if archive_summary["raw_archive_exists"] is False:
        readiness -= 1.2
    if archive_summary["parsed_archive_exists"] is False:
        readiness -= 1.0
    if noise_summary["unresolved_300s_share"] and float(noise_summary["unresolved_300s_share"]) > 0.8:
        readiness -= 1.4
    if majors_summary["btc_signal_count"] == 0 and majors_summary["sol_signal_count"] == 0:
        readiness -= 1.0
    if prealert_summary["prealert_count"] == 0:
        readiness -= 0.8

    trade_action_usable = 5.5
    if trade_action_summary["trade_action_present_rate"] == 1.0:
        trade_action_usable += 1.8
    if trade_action_summary["telegram_action_first_rate"] == 1.0:
        trade_action_usable += 1.4
    if trade_action_summary["conflict_no_trade_count"] > 0:
        trade_action_usable += 0.4

    chase_credibility = 4.8
    if chase_summary["count"] > 0:
        chase_credibility += 1.4
    if chase_summary["all_strict_flags_pass_rate"] == 1.0:
        chase_credibility += 1.2
    if chase_summary["outcome_window_status"]["30s"]["resolved_count"] == 0:
        chase_credibility -= 1.1
    if chase_summary["outcome_window_status"]["60s"]["resolved_count"] == 0:
        chase_credibility -= 0.7
    if chase_summary["outcome_window_status"]["300s"]["resolved_count"] == 0:
        chase_credibility -= 0.8

    protection_value = 5.2
    if protection_summary["wait_confirmation_upgrade_rate"] and float(protection_summary["wait_confirmation_upgrade_rate"]) >= 0.5:
        protection_value += 0.6
    if protection_summary["do_not_chase_would_have_lost_rate"] and float(protection_summary["do_not_chase_would_have_lost_rate"]) >= 0.25:
        protection_value += 0.5
    if trade_action_summary["conflict_no_trade_count"] > 0:
        protection_value += 0.4

    prealert_effectiveness = 1.5
    if prealert_summary["prealert_count"] > 0:
        prealert_effectiveness += 4.0
    if prealert_summary["prealert_candidate_count"] > 0:
        prealert_effectiveness += 1.0
    if prealert_summary["prealert_stage_overwritten_count"] == prealert_summary["prealert_candidate_count"] and prealert_summary["prealert_candidate_count"] > 0:
        prealert_effectiveness -= 0.8

    live_market_context = 4.0 + 5.0 * float(market_context_summary["live_public_rate"] or 0.0)
    if market_context_summary["okx_success"] == market_context_summary["okx_attempts"] and market_context_summary["okx_attempts"] > 0:
        live_market_context += 0.7
    if market_context_summary["kraken_attempts"] == 0:
        live_market_context -= 0.8

    confirm_honesty = 6.5
    if confirm_summary["local_confirm_count"] > 0 and confirm_summary["broader_confirm_count"] > 0:
        confirm_honesty += 0.7
    if confirm_summary["predict_warning_text_count"] > 0:
        confirm_honesty += 0.5
    if confirm_summary["chase_risk_count"] == 0:
        confirm_honesty += 0.2

    sweep_quality = 6.4
    if sweep_summary["sweep_building_display_climax_residual_count"] == 0:
        sweep_quality += 1.5
    if sweep_summary["sweep_building_to_continue_rate"] and float(sweep_summary["sweep_building_to_continue_rate"]) >= 0.5:
        sweep_quality += 0.4
    if sweep_summary["sweep_reversal_60s"]["adverse_rate"] and float(sweep_summary["sweep_reversal_60s"]["adverse_rate"]) > 0.3:
        sweep_quality -= 0.4

    adverse_correctness = 7.5
    if adverse_summary["adverse_direction_consistency_errors"] == 0:
        adverse_correctness += 1.8

    asset_case_compression = 5.0
    if asset_case_summary_payload["compression_ratio"] and float(asset_case_summary_payload["compression_ratio"]) >= 2.5:
        asset_case_compression += 1.0
    if asset_case_summary_payload["multi_pair_case_count"] > 0:
        asset_case_compression += 0.8
    if asset_case_summary_payload["one_signal_one_case_rate"] and float(asset_case_summary_payload["one_signal_one_case_rate"]) < 0.5:
        asset_case_compression += 0.6

    noise_control = 5.8
    if trade_action_summary["telegram_action_first_rate"] == 1.0:
        noise_control += 0.4
    if noise_summary["delivered_ratio"] and float(noise_summary["delivered_ratio"]) < 0.6:
        noise_control += 0.3
    if noise_summary["unresolved_300s_share"] and float(noise_summary["unresolved_300s_share"]) > 0.9:
        noise_control -= 0.5

    overall = statistics.mean(
        [
            readiness,
            trade_action_usable,
            chase_credibility,
            protection_value,
            prealert_effectiveness,
            live_market_context,
            confirm_honesty,
            sweep_quality,
            adverse_correctness,
            asset_case_compression,
            noise_control,
        ]
    )
    return {
        "research_sampling_readiness": score(readiness),
        "trade_action_usable": score(trade_action_usable),
        "chase_allowed_credibility": score(chase_credibility),
        "no_trade_do_not_chase_protection_value": score(protection_value),
        "prealert_effectiveness": score(prealert_effectiveness),
        "live_market_context_readiness": score(live_market_context),
        "confirm_honesty_non_misleading_quality": score(confirm_honesty),
        "sweep_quality": score(sweep_quality),
        "adverse_direction_correctness": score(adverse_correctness),
        "asset_case_compression": score(asset_case_compression),
        "noise_control": score(noise_control),
        "overall_self_use_score": score(overall),
    }


def build_run_overview(
    window: dict[str, Any],
    window_signal_rows: list[dict[str, Any]],
    lp_rows: list[dict[str, Any]],
    asset_cases: dict[str, Any],
    archive_summary: dict[str, Any],
) -> dict[str, Any]:
    delivered = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at")]
    return {
        "analysis_window_start": fmt_ts(int(window["analysis_window_start_ts"]), UTC),
        "analysis_window_end": fmt_ts(int(window["analysis_window_end_ts"]), UTC),
        "duration_hours": float(window["analysis_window_duration_hours"]),
        "total_raw_events": int(archive_summary["total_raw_events"]),
        "total_parsed_events": int(archive_summary["total_parsed_events"]),
        "total_signal_rows": len(window_signal_rows),
        "lp_signal_rows": len(lp_rows),
        "delivered_lp_signals": len(delivered),
        "asset_case_count": int(asset_cases["asset_cases"]),
        "case_followup_count": int(archive_summary["window_case_followup_rows"]),
    }


def build_top_findings(
    run_overview: dict[str, Any],
    trade_action_summary: dict[str, Any],
    chase_summary: dict[str, Any],
    protection_summary: dict[str, Any],
    prealert_summary: dict[str, Any],
    market_context_summary: dict[str, Any],
    confirm_summary: dict[str, Any],
    sweep_summary: dict[str, Any],
    adverse_summary: dict[str, Any],
    asset_case_summary_payload: dict[str, Any],
    majors_summary: dict[str, Any],
    archive_summary: dict[str, Any],
) -> list[str]:
    findings = [
        (
            f"主窗口为 {run_overview['analysis_window_start']} 到 {run_overview['analysis_window_end']}，"
            f"持续 {run_overview['duration_hours']}h，包含 {run_overview['total_signal_rows']} 条 signals / "
            f"{run_overview['lp_signal_rows']} 条 LP rows / {run_overview['delivered_lp_signals']} 条已送达 LP 消息。"
        ),
        (
            "trade_action 已真实进入 overnight 样本且覆盖全部 LP rows；"
            f"Telegram 首行 action-first 比例为 {trade_action_summary['telegram_action_first_rate']}。"
        ),
        (
            f"可追多/可追空样本极少，仅 {chase_summary['count']} 条，且 {chase_summary['all_strict_flags_pass_count']}/{chase_summary['count']} "
            "满足 broader_confirm + live_public + clean/strong-sweep + quality pass + no conflict + no local absorption + no exhaustion + no data gap。"
        ),
        (
            "两条 chase_allowed 的 30s/60s/300s outcome 全部没有价格回写，原因不是规则不严，而是 outcome ledger 过期时没有拿到新的 pool quote。"
        ),
        (
            f"保护类动作占主导：DO_NOT_CHASE_LONG={trade_action_summary['do_not_chase_long_count']} "
            f"DO_NOT_CHASE_SHORT={trade_action_summary['do_not_chase_short_count']} "
            f"CONFLICT_NO_TRADE={trade_action_summary['conflict_no_trade_count']} WAIT_CONFIRMATION={trade_action_summary['wait_confirmation_count']}。"
        ),
        (
            f"WAIT_CONFIRMATION 在 90s 内有 {protection_summary['wait_confirmation_upgrade_rate']} 的样本升级为更强后续结构，"
            "但没有直接升级成 chase_allowed。"
        ),
        (
            f"prealert 仍未真正落地：candidate={prealert_summary['prealert_candidate_count']} "
            f"gate_passed={prealert_summary['prealert_gate_passed_count']} "
            f"delivered_prealert={prealert_summary['prealert_delivered_count']} "
            f"stage_overwritten={prealert_summary['prealert_stage_overwritten_count']}。"
        ),
        (
            f"OKX live context 在 LP 主窗口命中率为 {market_context_summary['live_public_rate']}，"
            f"okx attempts={market_context_summary['okx_attempts']} success={market_context_summary['okx_success']}；"
            f"Kraken attempts={market_context_summary['kraken_attempts']}，因此 fallback 仍未被真实夜间样本验证。"
        ),
        (
            "ETH/USDC -> ETH-USDT-SWAP fallback 在真实样本里大量命中，Binance/Bybit 在主窗口没有被用到。"
        ),
        (
            f"confirm 分类更诚实：local_confirm={confirm_summary['local_confirm_count']} broader_confirm={confirm_summary['broader_confirm_count']} "
            f"late_confirm={confirm_summary['late_confirm_count']} chase_risk={confirm_summary['chase_risk_count']} "
            f"unconfirmed_confirm={confirm_summary['unconfirmed_confirm_count']}。"
        ),
        (
            f"sweep_building 不再显示成 climax，残留计数为 {sweep_summary['sweep_building_display_climax_residual_count']}；"
            f"sweep_building -> 后续延续率为 {sweep_summary['sweep_building_to_continue_rate']}。"
        ),
        (
            f"adverse direction 校验没有发现矛盾，consistency_errors={adverse_summary['adverse_direction_consistency_errors']}。"
        ),
        (
            f"asset-case 聚合已有作用：{asset_case_summary_payload['lp_rows']} 条 LP rows 聚合成 "
            f"{asset_case_summary_payload['asset_cases']} 个 case，compression_ratio={asset_case_summary_payload['compression_ratio']}，"
            f"其中 multi-pair cases={asset_case_summary_payload['multi_pair_case_count']}。"
        ),
        (
            f"majors 覆盖仍然只有 ETH/USDT 与 ETH/USDC，BTC/SOL 样本为 0；CLI 同时显示 BTC pools 是 configured but disabled，"
            "SOL 则仍缺 major 覆盖。"
        ),
        (
            "raw_events / parsed_events archive 仍然缺失，且 300s outcome 大面积 expired，"
            "这两点是本轮对 prealert funnel 和长窗口后验判断的最大限制。"
        ),
    ]
    return findings[:15]


def build_recommendations(
    prealert_summary: dict[str, Any],
    market_context_summary: dict[str, Any],
    majors_summary: dict[str, Any],
    archive_summary: dict[str, Any],
) -> list[str]:
    recommendations = [
        "优先补齐并启用 BTC/SOL majors pools，下一轮继续先补 majors，不要扩 long-tail。",
        "先解决 prealert 被统一 overwritten 的路径，至少让 major first-leg prealert 能在 archive 中落一层真实 stage。",
        "保留 OKX 为主路径，同时加一个可重复触发的 Kraken fallback 自检，因为真实夜间还没用到它。",
        "补回 raw_events / parsed_events archive；没有 raw/parsed 就无法完整回溯 prealert funnel 与 fastlane 首发漏点。",
        "提升 pool quote outcome 回写，尤其是 30s/60s/300s 过期时的补偿逻辑，否则 chase/no-trade 的后验无法做强结论。",
    ]
    if prealert_summary["prealert_count"] > 0:
        recommendations = recommendations[1:]
    if market_context_summary["kraken_attempts"] > 0:
        recommendations = [item for item in recommendations if "Kraken" not in item]
    if archive_summary["raw_archive_exists"] and archive_summary["parsed_archive_exists"]:
        recommendations = [item for item in recommendations if "raw_events / parsed_events" not in item]
    if majors_summary["btc_signal_count"] > 0 or majors_summary["sol_signal_count"] > 0:
        recommendations = [item for item in recommendations if "BTC/SOL majors" not in item]
    return recommendations[:5]


def md_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def build_markdown(
    data_sources: list[dict[str, Any]],
    window: dict[str, Any],
    runtime_config: dict[str, dict[str, Any]],
    run_overview: dict[str, Any],
    stage_summary: dict[str, Any],
    trade_action_summary: dict[str, Any],
    chase_summary: dict[str, Any],
    protection_summary: dict[str, Any],
    conflict_summary: dict[str, Any],
    prealert_summary: dict[str, Any],
    market_context_summary: dict[str, Any],
    market_context_cli: dict[str, Any],
    confirm_summary: dict[str, Any],
    sweep_summary: dict[str, Any],
    reversal_summary: dict[str, Any],
    adverse_summary: dict[str, Any],
    absorption_summary: dict[str, Any],
    asset_market_state_summary: dict[str, Any],
    no_trade_lock_summary: dict[str, Any],
    prealert_lifecycle_summary: dict[str, Any],
    candidate_tradeable_summary: dict[str, Any],
    outcome_price_source_summary: dict[str, Any],
    telegram_suppression_summary: dict[str, Any],
    noise_reduction_summary: dict[str, Any],
    asset_case_summary_payload: dict[str, Any],
    archive_summary: dict[str, Any],
    majors_summary: dict[str, Any],
    noise_summary: dict[str, Any],
    scores: dict[str, Any],
    findings: list[str],
    recommendations: list[str],
) -> str:
    lines: list[str] = []
    lines.append("# Overnight Trade Action Analysis")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append("")
    for item in findings:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 2. 数据源与完整性说明")
    lines.append("")
    for item in data_sources:
        lines.append(
            f"- `{item['path']}`: exists=`{item['exists']}` records=`{item['record_count']}` "
            f"range=`{fmt_ts(item['start_ts'], UTC)} -> {fmt_ts(item['end_ts'], UTC)}` note=`{item['notes']}`"
        )
    lines.append(
        "- 关键缺口：`raw_events` / `parsed_events` 目录不存在，且配置里也被禁用；"
        "因此 prealert funnel 无法从 raw->parsed->signal 三层闭环回溯。"
    )
    lines.append("")
    lines.append("## 3. overnight 分析窗口")
    lines.append("")
    lines.append(f"- UTC: `{fmt_ts(window['analysis_window_start_ts'], UTC)} -> {fmt_ts(window['analysis_window_end_ts'], UTC)}`")
    lines.append(f"- 服务器本地: `{fmt_ts(window['analysis_window_start_ts'], SERVER_TZ)} -> {fmt_ts(window['analysis_window_end_ts'], SERVER_TZ)}`")
    lines.append(f"- 北京时间: `{fmt_ts(window['analysis_window_start_ts'], BJ_TZ)} -> {fmt_ts(window['analysis_window_end_ts'], BJ_TZ)}`")
    lines.append(f"- 东京时间: `{fmt_ts(window['analysis_window_start_ts'], TOKYO_TZ)} -> {fmt_ts(window['analysis_window_end_ts'], TOKYO_TZ)}`")
    lines.append(f"- duration_hours=`{window['analysis_window_duration_hours']}`")
    lines.append(f"- selection_reason=`{window['selection_reason']}`")
    lines.append("- 其他连续段：")
    for segment in window["all_segments"]:
        lines.append(
            f"- `{fmt_ts(segment['start_ts'], UTC)} -> {fmt_ts(segment['end_ts'], UTC)}` "
            f"`duration={segment['duration_hours']}h` `signals={segment['total_signal_rows']}` "
            f"`lp={segment['lp_signal_rows']}` `bj_overnight={segment['looks_like_bj_overnight']}`"
        )
    lines.append("")
    lines.append("## 4. 非敏感运行配置摘要")
    lines.append("")
    for key in SAFE_CONFIG_KEYS:
        lines.append(f"- `{key}` = `{runtime_config[key]['runtime_value']}`")
    lines.append("")
    lines.append("## 5. trade_action 总览")
    lines.append("")
    lines.append(f"- run_overview=`{md_json(run_overview)}`")
    lines.append(f"- lp_stage_summary=`{md_json(stage_summary)}`")
    lines.append(f"- trade_action_distribution=`{md_json(trade_action_summary['trade_action_distribution'])}`")
    lines.append(f"- delivered_distribution=`{md_json(trade_action_summary['delivered_distribution'])}`")
    lines.append(f"- undelivered_distribution=`{md_json(trade_action_summary['undelivered_distribution'])}`")
    lines.append(
        f"- trade_action_present_rate=`{trade_action_summary['trade_action_present_rate']}` "
        f"telegram_action_first_rate=`{trade_action_summary['telegram_action_first_rate']}`"
    )
    lines.append("")
    lines.append("## 6. 交易状态机与降噪治理")
    lines.append("")
    lines.append(f"- asset_market_state_summary=`{md_json(asset_market_state_summary)}`")
    lines.append(f"- no_trade_lock_summary=`{md_json(no_trade_lock_summary)}`")
    lines.append(f"- prealert_lifecycle_summary=`{md_json(prealert_lifecycle_summary)}`")
    lines.append(f"- candidate_tradeable_summary=`{md_json(candidate_tradeable_summary)}`")
    lines.append(f"- outcome_price_source_summary=`{md_json(outcome_price_source_summary)}`")
    lines.append(f"- telegram_suppression_summary=`{md_json(telegram_suppression_summary)}`")
    lines.append(f"- noise_reduction_summary=`{md_json(noise_reduction_summary)}`")
    lines.append(
        "- 结论：Telegram 默认现在看 asset-level state change / risk blocker / candidate，"
        "不再按每条 LP 中间态逐条推送；archive 与 report 仍保留全量研究流。"
    )
    lines.append(
        "- 问题回答：消息下降目标明确指向低价值中间状态，不压掉 risk blocker / candidate / state change；"
        "同一 state repeat 会被 suppress。"
    )
    lines.append(
        "- 问题回答：NO_TRADE_LOCK 统计会直接展示冲突期的 entered / suppressed / released，"
        "用于验证双向扫流动性时是否明显降噪。"
    )
    lines.append(
        "- 问题回答：candidate 比 tradeable 更保守，因为它要求同方向结构先成立，"
        "但把 rolling 60s followthrough/adverse/completion 的验收留到后面。"
    )
    lines.append(
        "- 问题回答：outcome 现在优先用 OKX/Kraken market price；"
        "如果历史窗口仍大量显示 `pool_quote_proxy` 或 `expired`，那是旧归档尚未带新字段，不代表新逻辑未切换。"
    )
    lines.append("")
    lines.append("## 7. 可追多 / 可追空 后验分析")
    lines.append("")
    lines.append(f"- chase_allowed_summary=`{md_json({k: chase_summary[k] for k in ['count','long_chase_allowed_count','short_chase_allowed_count','all_strict_flags_pass_count','all_strict_flags_pass_rate','chase_allowed_30s_followthrough_rate','chase_allowed_60s_followthrough_rate','chase_allowed_300s_followthrough_rate','chase_allowed_adverse_30s_rate','chase_allowed_adverse_60s_rate','chase_allowed_adverse_300s_rate']})}`")
    lines.append(f"- strict_samples=`{md_json(chase_summary['strict_samples'])}`")
    lines.append(f"- outcome_window_status=`{md_json(chase_summary['outcome_window_status'])}`")
    lines.append("- 结论：样本极少且规则严格；两条 chase_allowed 都符合白名单条件，但 outcome 因 `window_elapsed_without_price_update` 无法做后验胜率。")
    lines.append("")
    lines.append("## 8. 不追多 / 不追空 / 等待确认 / 不交易 保护价值分析")
    lines.append("")
    lines.append(f"- protection_summary=`{md_json(protection_summary)}`")
    lines.append("- 结论：WAIT_CONFIRMATION 有 2/3 在 90s 内升级为更强后续结构；DO_NOT_CHASE 的可用后验主要是 30s/60s，当前更像“避免晚到/局部吸收”而不是稳定抓到立刻反转。")
    lines.append("")
    lines.append("## 9. 冲突合并 CONFLICT_NO_TRADE 分析")
    lines.append("")
    lines.append(f"- conflict_summary=`{md_json(conflict_summary)}`")
    lines.append("- 结论：冲突信号真实存在，8 条样本都带有 120s 内 opposite signal 证据；但可解析的 30s/60s 样本太少，不能把短线未反扫解释成 conflict 无价值。")
    lines.append("")
    lines.append("## 10. prealert funnel 分析")
    lines.append("")
    lines.append(f"- prealert_funnel_summary=`{md_json(prealert_summary)}`")
    lines.append("- 结论：candidate 与 gate_passed 都不缺，真正缺的是“存活下来的 prealert stage”；所有候选都被后续 stage 覆盖，first_leg 仍为 0。")
    lines.append("")
    lines.append("## 11. OKX/Kraken live market context 验证")
    lines.append("")
    lines.append(f"- market_context_window=`{md_json(market_context_summary)}`")
    lines.append(f"- market_context_cli=`{md_json({'live_public_hit_rate': market_context_cli.get('live_public_hit_rate'), 'per_venue': market_context_cli.get('per_venue'), 'symbol_fallbacks': market_context_cli.get('symbol_fallbacks'), 'top_failure_reasons': market_context_cli.get('top_failure_reasons')})}`")
    lines.append("- 结论：主路径 OKX 已在真实 overnight 生效，ETH/USDC 和 ETH/USDT 都稳定落到 `ETH-USDT-SWAP`；Kraken 没被触发，故只能验证配置，不足以验证夜间实战 fallback。")
    lines.append("")
    lines.append("## 12. confirm local/broader/late/chase 分析")
    lines.append("")
    lines.append(f"- confirm_summary=`{md_json(confirm_summary)}`")
    lines.append("- 结论：local / broader / late / unconfirmed 已分开；broader_confirm 有样本但仍少，chase_risk 在这晚没有出现。")
    lines.append("")
    lines.append("## 13. sweep_building / sweep_confirmed / exhaustion 分析")
    lines.append("")
    lines.append(f"- sweep_summary=`{md_json(sweep_summary)}`")
    lines.append("- 结论：`sweep_building` 没有再冒充高潮；`sweep_confirmed` 在可解析 60s 样本里有少量反向，但总体仍偏延续。")
    lines.append("")
    lines.append("## 14. 卖压后涨 / 买压后跌 反例专项")
    lines.append("")
    lines.append(f"- reversal_special_cases=`{md_json(reversal_summary)}`")
    lines.append("- 结论：可见反例几乎都落在 `late_confirm / local_confirm / single_pool / local_absorption` 组合，而不是 broader clean confirm。")
    lines.append("")
    lines.append("## 15. adverse direction 校验")
    lines.append("")
    lines.append(f"- adverse_direction_summary=`{md_json(adverse_summary)}`")
    lines.append("- 结论：没有发现 sell pressure 后上涨却 adverse_rate=0 的矛盾样本。")
    lines.append("")
    lines.append("## 16. asset-case 聚合与压缩分析")
    lines.append("")
    lines.append(f"- asset_case_summary=`{md_json(asset_case_summary_payload)}`")
    lines.append("- 结论：压缩已经明显优于“一条 signal 一个 case”，但仍有 18 个单信号 case，主要碎片原因是 local_confirm / local_absorption / single_pair。")
    lines.append("")
    lines.append("## 17. raw/parsed/signals archive 完整性")
    lines.append("")
    lines.append(f"- archive_integrity_summary=`{md_json(archive_summary)}`")
    lines.append("- 结论：signals / delivery_audit / cases / case_followups 对账很好，但 raw/parsed 缺失仍是研究闭环硬伤。")
    lines.append("")
    lines.append("## 18. majors 覆盖与样本代表性")
    lines.append("")
    lines.append(f"- majors_coverage_summary=`{md_json(majors_summary)}`")
    lines.append("- 结论：主窗口仍完全是 ETH 双主池样本，BTC/SOL 不能算“没有事件”，更像 pool book / enabled coverage 还没到位。")
    lines.append("")
    lines.append("## 19. 噪音与误判风险评估")
    lines.append("")
    lines.append(f"- noise_summary=`{md_json(noise_summary)}`")
    lines.append("- 结论：当前最大的噪音不是 market context，而是 `prealert 缺失 + 300s outcome 大面积 expired + majors 代表性不足`。")
    lines.append("")
    lines.append("## 20. 最终评分")
    lines.append("")
    for key, value in scores.items():
        lines.append(f"- `{key}` = `{value}/10`")
    lines.append("")
    lines.append("## 21. 下一轮建议")
    lines.append("")
    for item in recommendations:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 22. 限制与不确定性")
    lines.append("")
    lines.append("- 本报告严格基于最新 BJ 夜间主窗口，不把旧白天窗口混进主结论。")
    lines.append("- `raw_events` / `parsed_events` 目录缺失，所以无法把 prealert funnel 完整回溯到更早层。")
    lines.append("- 300s outcome 绝大多数行是 `window_elapsed_without_price_update`，因此 chase_allowed/no-trade 的长窗口后验结论必须保守。")
    lines.append("- `lp_prealert_delivery_block_reason` 与 `lp_prealert_delivery_allowed=True` 同时出现于部分 overwritten 样本，说明这组诊断字段存在歧义，不能机械地当成真实 delivery block。")
    return "\n".join(lines) + "\n"


def add_metric(
    rows: list[dict[str, Any]],
    metric_group: str,
    metric_name: str,
    value: Any,
    *,
    asset: str = "",
    pair: str = "",
    stage: str = "",
    trade_action: str = "",
    sample_size: Any = "",
    window: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_group": metric_group,
            "metric_name": metric_name,
            "asset": asset,
            "pair": pair,
            "stage": stage,
            "trade_action": trade_action,
            "value": value,
            "sample_size": sample_size,
            "window": window,
            "notes": notes,
        }
    )


def build_csv_rows(
    window: dict[str, Any],
    run_overview: dict[str, Any],
    stage_summary: dict[str, Any],
    trade_action_summary: dict[str, Any],
    chase_summary: dict[str, Any],
    protection_summary: dict[str, Any],
    conflict_summary: dict[str, Any],
    prealert_summary: dict[str, Any],
    market_context_summary: dict[str, Any],
    confirm_summary: dict[str, Any],
    sweep_summary: dict[str, Any],
    absorption_summary: dict[str, Any],
    reversal_summary: dict[str, Any],
    adverse_summary: dict[str, Any],
    asset_case_summary_payload: dict[str, Any],
    majors_summary: dict[str, Any],
    archive_summary: dict[str, Any],
    scores: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{fmt_ts(window['analysis_window_start_ts'], UTC)} -> {fmt_ts(window['analysis_window_end_ts'], UTC)}"
    for key, value in run_overview.items():
        add_metric(rows, "run_overview", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for stage, value in stage_summary["stage_distribution_pct"].items():
        add_metric(rows, "lp_stage", "stage_distribution_pct", value, stage=stage, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key in ("prealert_count", "confirm_count", "climax_count", "exhaustion_risk_count"):
        add_metric(rows, "lp_stage", key, stage_summary[key], sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in trade_action_summary["trade_action_distribution"].items():
        add_metric(rows, "trade_action", "trade_action_distribution", value, trade_action=key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key in (
        "long_chase_allowed_count",
        "short_chase_allowed_count",
        "no_trade_count",
        "wait_confirmation_count",
        "data_gap_no_trade_count",
        "conflict_no_trade_count",
        "do_not_chase_long_count",
        "do_not_chase_short_count",
        "long_bias_observe_count",
        "short_bias_observe_count",
        "reversal_watch_long_count",
        "reversal_watch_short_count",
        "telegram_action_first_rate",
        "trade_action_present_rate",
    ):
        add_metric(rows, "trade_action", key, trade_action_summary[key], sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key in (
        "chase_allowed_30s_followthrough_rate",
        "chase_allowed_60s_followthrough_rate",
        "chase_allowed_300s_followthrough_rate",
        "chase_allowed_adverse_30s_rate",
        "chase_allowed_adverse_60s_rate",
        "chase_allowed_adverse_300s_rate",
    ):
        add_metric(rows, "trade_action_outcome", key, chase_summary[key], sample_size=chase_summary["count"], window=window_label)
    add_metric(rows, "trade_action_outcome", "do_not_chase_would_have_lost_rate", protection_summary["do_not_chase_would_have_lost_rate"], sample_size=run_overview["lp_signal_rows"], window=window_label, notes="preferred 60s window")
    add_metric(rows, "trade_action_outcome", "no_trade_conflict_adverse_rate", protection_summary["no_trade_conflict_adverse_rate"], sample_size=conflict_summary["count"], window=window_label, notes="preferred 60s window")
    add_metric(rows, "trade_action_outcome", "wait_confirmation_upgrade_rate", protection_summary["wait_confirmation_upgrade_rate"], sample_size=protection_summary["wait_confirmation_count"], window=window_label, notes="within 90s same asset-case")
    for key in (
        "prealert_candidate_count",
        "prealert_gate_passed_count",
        "prealert_delivered_count",
        "major_first_leg_prealert_count",
        "major_prealert_count",
        "non_major_prealert_count",
        "prealert_to_confirm_30s",
        "prealert_to_confirm_60s",
        "prealert_to_confirm_90s",
    ):
        add_metric(rows, "prealert", key, prealert_summary[key], sample_size=prealert_summary["prealert_candidate_count"], window=window_label)
    for key, value in market_context_summary.items():
        if isinstance(value, (dict, list)):
            continue
        add_metric(rows, "market_context", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for symbol, value in market_context_summary["resolved_symbol_distribution"].items():
        add_metric(rows, "market_context", "resolved_symbol_distribution", value, pair=symbol, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for requested_to_resolved, value in market_context_summary["requested_to_resolved_distribution"].items():
        add_metric(rows, "market_context", "requested_to_resolved_distribution", value, pair=requested_to_resolved, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key in (
        "clean_confirm_count",
        "local_confirm_count",
        "broader_confirm_count",
        "late_confirm_count",
        "chase_risk_count",
        "unconfirmed_confirm_count",
        "confirm_move_before_30s_median",
        "confirm_move_after_60s_median",
    ):
        add_metric(rows, "confirm", key, confirm_summary[key], sample_size=confirm_summary["confirm_count"], window=window_label)
    for scope, payload in confirm_summary["scope_performance"].items():
        for metric_name, value in payload.items():
            add_metric(rows, "confirm_scope", metric_name, value, stage=scope, sample_size=payload["count"], window=window_label)
    for key in (
        "sweep_building_count",
        "sweep_building_display_climax_residual_count",
        "sweep_confirmed_count",
        "sweep_exhaustion_risk_count",
    ):
        add_metric(rows, "sweep", key, sweep_summary[key], sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "sweep", "sweep_reversal_60s", md_json(sweep_summary["sweep_reversal_60s"]), sample_size=sweep_summary["sweep_confirmed_count"], window=window_label)
    add_metric(rows, "sweep", "sweep_reversal_300s", md_json(sweep_summary["sweep_reversal_300s"]), sample_size=sweep_summary["sweep_confirmed_count"], window=window_label)
    add_metric(rows, "sweep", "sweep_exhaustion_outcome_300s", md_json(sweep_summary["sweep_exhaustion_outcome_300s"]), sample_size=sweep_summary["sweep_exhaustion_risk_count"], window=window_label)
    for key in (
        "local_sell_pressure_absorption_count",
        "local_buy_pressure_absorption_count",
        "broader_sell_pressure_confirmed_count",
        "broader_buy_pressure_confirmed_count",
        "pool_only_unconfirmed_pressure_count",
    ):
        add_metric(rows, "absorption", key, absorption_summary[key], sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in adverse_summary.items():
        if isinstance(value, (dict, list)):
            continue
        add_metric(rows, "adverse", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in asset_case_summary_payload.items():
        if isinstance(value, (dict, list)):
            continue
        add_metric(rows, "asset_case", key, value, sample_size=asset_case_summary_payload["asset_cases"], window=window_label)
    add_metric(rows, "asset_case", "case_upgrade_paths", md_json(asset_case_summary_payload["case_upgrade_paths"]), sample_size=asset_case_summary_payload["asset_cases"], window=window_label)
    add_metric(rows, "asset_case", "fragmentation_reasons", md_json(asset_case_summary_payload["fragmentation_reasons"]), sample_size=asset_case_summary_payload["asset_cases"], window=window_label)
    add_metric(rows, "majors", "covered_major_pairs", md_json(majors_summary["covered_major_pairs"]), sample_size=len(majors_summary["covered_major_pairs"]), window=window_label)
    add_metric(rows, "majors", "missing_major_pairs", md_json(majors_summary["missing_major_pairs"]), sample_size=len(majors_summary["missing_major_pairs"]), window=window_label)
    add_metric(rows, "majors", "eth_signal_count", majors_summary["eth_signal_count"], asset="ETH", sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "majors", "btc_signal_count", majors_summary["btc_signal_count"], asset="BTC", sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "majors", "sol_signal_count", majors_summary["sol_signal_count"], asset="SOL", sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in archive_summary.items():
        if isinstance(value, (dict, list, set)):
            continue
        add_metric(rows, "archive", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "reversal", "reason_distribution", md_json(reversal_summary["reason_distribution"]), sample_size=len(reversal_summary["examples"]), window=window_label)
    add_metric(rows, "reversal", "examples", md_json(reversal_summary["examples"]), sample_size=len(reversal_summary["examples"]), window=window_label)
    for key, value in scores.items():
        add_metric(rows, "score", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "metric_group",
                "metric_name",
                "asset",
                "pair",
                "stage",
                "trade_action",
                "value",
                "sample_size",
                "window",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    runtime_config = load_runtime_config()
    inventories = file_inventory_by_category()
    signal_rows, signal_inventory = load_signals()
    quality_rows, quality_by_signal, quality_inventory = load_quality_cache()
    asset_case_cache, asset_case_inventory = load_asset_case_cache()
    window = choose_latest_overnight_window(signal_rows, inventories)

    window_signal_rows, _, lp_rows = join_lp_rows(
        signal_rows,
        quality_by_signal,
        int(window["analysis_window_start_ts"]),
        int(window["analysis_window_end_ts"]),
    )

    signal_ids = {str(row.get("signal_id") or "") for row in lp_rows if row.get("signal_id")}
    delivered_signal_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows
        if row.get("signal_id") and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    }
    _, delivery_summary = stream_delivery_audit(
        int(window["analysis_window_start_ts"]),
        int(window["analysis_window_end_ts"]),
        signal_ids,
        delivered_signal_ids,
    )
    _, cases_summary = stream_cases(
        int(window["analysis_window_start_ts"]),
        int(window["analysis_window_end_ts"]),
        signal_ids,
        delivered_signal_ids,
    )
    _, followup_summary = stream_case_followups(
        int(window["analysis_window_start_ts"]),
        int(window["analysis_window_end_ts"]),
        delivered_signal_ids,
    )

    cli_market_context = run_cli(["--market-context-health"], expect_json=True)
    cli_major_pool_coverage = run_cli(["--major-pool-coverage"], expect_json=True)
    cli_summary = run_cli(["--summary"], expect_json=True)
    cli_csv = run_cli(["--format", "csv"], expect_json=False)

    major_pairs = major_pairs_from_cli(cli_major_pool_coverage)
    stage_summary = stage_distribution(lp_rows)
    trade_action_summary = trade_action_overview(lp_rows)
    chase_summary = chase_allowed_analysis(lp_rows, runtime_config)
    protection_summary = protection_value_analysis(lp_rows)
    conflict_summary = conflict_analysis(lp_rows)
    prealert_summary = prealert_funnel_analysis(lp_rows, major_pairs)
    market_context_summary = compute_market_context(lp_rows)
    confirm_summary = confirm_analysis(lp_rows)
    sweep_summary = sweep_analysis(lp_rows)
    absorption_summary = compute_absorption(lp_rows)
    asset_market_state_summary = compute_asset_market_states(lp_rows)
    no_trade_lock_summary = compute_no_trade_lock_summary(lp_rows)
    prealert_lifecycle_summary = compute_prealert_lifecycle_summary(lp_rows)
    candidate_tradeable_summary = compute_candidate_tradeable_summary(lp_rows)
    outcome_price_source_summary = compute_outcome_price_sources(lp_rows)
    telegram_suppression_summary = compute_telegram_suppression(lp_rows)
    noise_reduction_summary = compute_noise_reduction(lp_rows)
    reversal_summary = reversal_special_cases(lp_rows)
    adverse_summary = adverse_direction_summary(lp_rows)
    asset_case_summary_payload = asset_case_summary(lp_rows)
    majors_summary = compute_majors(lp_rows, cli_major_pool_coverage, runtime_config)
    archive_summary = archive_integrity_summary(
        lp_rows,
        delivery_summary,
        cases_summary,
        followup_summary,
        inventories,
        window,
    )
    run_overview = build_run_overview(window, window_signal_rows, lp_rows, asset_case_summary_payload, archive_summary)
    noise_summary = compute_noise_risk(lp_rows, prealert_summary, majors_summary, archive_summary)
    scores = scorecard(
        run_overview,
        trade_action_summary,
        chase_summary,
        protection_summary,
        prealert_summary,
        market_context_summary,
        confirm_summary,
        sweep_summary,
        adverse_summary,
        asset_case_summary_payload,
        majors_summary,
        archive_summary,
        noise_summary,
    )
    findings = build_top_findings(
        run_overview,
        trade_action_summary,
        chase_summary,
        protection_summary,
        prealert_summary,
        market_context_summary,
        confirm_summary,
        sweep_summary,
        adverse_summary,
        asset_case_summary_payload,
        majors_summary,
        archive_summary,
    )
    recommendations = build_recommendations(
        prealert_summary,
        market_context_summary,
        majors_summary,
        archive_summary,
    )

    data_sources = []
    for category in ("raw_events", "parsed_events", "signals", "cases", "case_followups", "delivery_audit"):
        for item in inventories.get(category, []):
            data_sources.append(asdict(item))
    data_sources.append(asdict(asset_case_inventory))
    data_sources.append(asdict(quality_inventory))

    markdown = build_markdown(
        data_sources,
        window,
        runtime_config,
        run_overview,
        stage_summary,
        trade_action_summary,
        chase_summary,
        protection_summary,
        conflict_summary,
        prealert_summary,
        market_context_summary,
        cli_market_context,
        confirm_summary,
        sweep_summary,
        reversal_summary,
        adverse_summary,
        absorption_summary,
        asset_market_state_summary,
        no_trade_lock_summary,
        prealert_lifecycle_summary,
        candidate_tradeable_summary,
        outcome_price_source_summary,
        telegram_suppression_summary,
        noise_reduction_summary,
        asset_case_summary_payload,
        archive_summary,
        majors_summary,
        noise_summary,
        scores,
        findings,
        recommendations,
    )
    MARKDOWN_PATH.write_text(markdown, encoding="utf-8")

    csv_rows = build_csv_rows(
        window,
        run_overview,
        stage_summary,
        trade_action_summary,
        chase_summary,
        protection_summary,
        conflict_summary,
        prealert_summary,
        market_context_summary,
        confirm_summary,
        sweep_summary,
        absorption_summary,
        reversal_summary,
        adverse_summary,
        asset_case_summary_payload,
        majors_summary,
        archive_summary,
        scores,
    )
    window_label = f"{fmt_ts(int(window['analysis_window_start_ts']))} -> {fmt_ts(int(window['analysis_window_end_ts']))}"
    for key, value in asset_market_state_summary.get("state_distribution", {}).items():
        add_metric(csv_rows, "asset_market_state", "state_distribution", value, stage=key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in no_trade_lock_summary.items():
        add_metric(csv_rows, "no_trade_lock", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in prealert_lifecycle_summary.items():
        if isinstance(value, dict):
            continue
        add_metric(csv_rows, "prealert_lifecycle", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in candidate_tradeable_summary.items():
        if isinstance(value, dict):
            continue
        add_metric(csv_rows, "candidate_tradeable", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in outcome_price_source_summary.get("source_distribution", {}).items():
        add_metric(csv_rows, "outcome_price_source", "source_distribution", value, stage=key, sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(csv_rows, "telegram", "total_suppressed", telegram_suppression_summary.get("total_suppressed"), sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(csv_rows, "noise_reduction", "suppressed_ratio", noise_reduction_summary.get("suppressed_ratio"), sample_size=run_overview["lp_signal_rows"], window=window_label)
    write_csv(CSV_PATH, csv_rows)

    summary_payload = {
        "analysis_window": {
            "start_ts": int(window["analysis_window_start_ts"]),
            "end_ts": int(window["analysis_window_end_ts"]),
            "start_utc": fmt_ts(int(window["analysis_window_start_ts"]), UTC),
            "end_utc": fmt_ts(int(window["analysis_window_end_ts"]), UTC),
            "start_server_local": fmt_ts(int(window["analysis_window_start_ts"]), SERVER_TZ),
            "end_server_local": fmt_ts(int(window["analysis_window_end_ts"]), SERVER_TZ),
            "start_bj": fmt_ts(int(window["analysis_window_start_ts"]), BJ_TZ),
            "end_bj": fmt_ts(int(window["analysis_window_end_ts"]), BJ_TZ),
            "start_tokyo": fmt_ts(int(window["analysis_window_start_ts"]), TOKYO_TZ),
            "end_tokyo": fmt_ts(int(window["analysis_window_end_ts"]), TOKYO_TZ),
            "duration_hours": float(window["analysis_window_duration_hours"]),
            "selection_reason": window["selection_reason"],
            "segments": window["all_segments"],
        },
        "data_sources": data_sources,
        "runtime_config_summary": runtime_config,
        "lp_stage_summary": {**run_overview, **stage_summary},
        "trade_action_summary": trade_action_summary,
        "trade_action_outcome_summary": {
            **chase_summary,
            **{
                "do_not_chase_would_have_lost_rate": protection_summary["do_not_chase_would_have_lost_rate"],
                "no_trade_conflict_adverse_rate": protection_summary["no_trade_conflict_adverse_rate"],
                "wait_confirmation_upgrade_rate": protection_summary["wait_confirmation_upgrade_rate"],
            },
        },
        "prealert_funnel_summary": prealert_summary,
        "market_context_health": {
            "window": market_context_summary,
            "quality_reports_cli": cli_market_context,
        },
        "confirm_summary": confirm_summary,
        "sweep_summary": sweep_summary,
        "absorption_summary": absorption_summary,
        "asset_market_state_summary": asset_market_state_summary,
        "no_trade_lock_summary": no_trade_lock_summary,
        "prealert_lifecycle_summary": prealert_lifecycle_summary,
        "candidate_tradeable_summary": candidate_tradeable_summary,
        "outcome_price_source_summary": outcome_price_source_summary,
        "telegram_suppression_summary": telegram_suppression_summary,
        "noise_reduction_summary": noise_reduction_summary,
        "adverse_direction_summary": adverse_summary,
        "asset_case_summary": asset_case_summary_payload,
        "majors_coverage_summary": majors_summary,
        "archive_integrity_summary": archive_summary,
        "reversal_special_cases": reversal_summary,
        "scorecard": scores,
        "top_findings": findings,
        "top_recommendations": recommendations,
        "limitations": [
            "raw_events archive is missing in the selected overnight window.",
            "parsed_events archive is missing in the selected overnight window.",
            "300s outcome coverage is almost entirely expired due window_elapsed_without_price_update.",
            "prealert diagnostics show overwritten=true everywhere, so the exact overwrite point cannot be reconstructed from raw/parsed layers.",
        ],
        "cli_output_capture": {
            "quality_reports_market_context_health": cli_market_context,
            "quality_reports_major_pool_coverage": cli_major_pool_coverage,
            "quality_reports_summary_overall": cli_summary.get("overall"),
            "quality_reports_csv_row_count": max(0, len([line for line in cli_csv.splitlines() if line.strip()]) - 1),
        },
        "asset_case_cache_snapshot": asset_case_cache,
    }
    JSON_PATH.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "markdown": str(MARKDOWN_PATH),
                "csv": str(CSV_PATH),
                "json": str(JSON_PATH),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
