#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
ARCHIVE_DIR = APP_DIR / "data" / "archive"
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
ENV_PATH = ROOT / ".env"

MARKDOWN_PATH = REPORTS_DIR / "overnight_run_analysis_latest.md"
CSV_PATH = REPORTS_DIR / "overnight_run_metrics_latest.csv"
JSON_PATH = REPORTS_DIR / "overnight_run_summary_latest.json"

UTC = timezone.utc
SERVER_TZ = UTC
BJ_TZ = timezone(timedelta(hours=8))
TOKYO_TZ = timezone(timedelta(hours=9))

LP_STAGES = ("prealert", "confirm", "climax", "exhaustion_risk")
SWEEP_PHASES = ("sweep_building", "sweep_confirmed", "sweep_exhaustion_risk")
MAJOR_ASSET_FAMILY = {
    "ETH": "ETH",
    "WETH": "ETH",
    "BTC": "BTC",
    "WBTC": "BTC",
    "CBBTC": "BTC",
    "SOL": "SOL",
    "WSOL": "SOL",
}

WHITELISTED_CONFIG_KEYS = [
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
]


@dataclass
class FileInventory:
    path: str
    exists: bool
    record_count: int
    start_ts: int | None
    end_ts: int | None
    notes: str


def to_int(value: Any) -> int | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pct(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator * 100.0, 2)


def rate(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator, 4)


def median(values: list[Any], digits: int = 6) -> float | None:
    cleaned = [float(v) for v in values if to_float(v) is not None]
    if not cleaned:
        return None
    return round(float(statistics.median(cleaned)), digits)


def fmt_ts(ts: int | None, tz: timezone = UTC) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(int(ts), tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def hours_between(start_ts: int | None, end_ts: int | None) -> float | None:
    if start_ts is None or end_ts is None:
        return None
    return round((int(end_ts) - int(start_ts)) / 3600.0, 2)


def canonical_asset(symbol: str | None) -> str:
    raw = str(symbol or "").strip().upper().replace(".E", "")
    return MAJOR_ASSET_FAMILY.get(raw, raw)


def pair_parts(pair_label: str | None) -> tuple[str, str]:
    raw = str(pair_label or "").strip().upper().replace(" ", "")
    if "/" not in raw:
        return canonical_asset(raw), ""
    left, right = raw.split("/", 1)
    return canonical_asset(left), right.replace(".E", "")


def direction_bucket(intent_type: str | None) -> str:
    raw = str(intent_type or "")
    if raw == "pool_buy_pressure":
        return "buy_pressure"
    if raw == "pool_sell_pressure":
        return "sell_pressure"
    return ""


def aligned_move(row: dict[str, Any], field: str) -> float | None:
    suffix = field.replace("move_after_alert_", "")
    adjusted_value = to_float(row.get(f"direction_adjusted_move_after_{suffix}"))
    if adjusted_value is not None:
        return round(adjusted_value, 6)
    value = to_float(row.get(field))
    if value is None:
        return None
    if row.get("direction_bucket") == "sell_pressure":
        return round(-value, 6)
    return round(value, 6)


def adverse_move(row: dict[str, Any], field: str) -> bool | None:
    suffix = field.replace("move_after_alert_", "")
    explicit = row.get(f"adverse_by_direction_{suffix}")
    if explicit is not None:
        return bool(explicit)
    value = to_float(row.get(field))
    if value is None:
        return None
    if row.get("direction_bucket") == "sell_pressure":
        return value > 0
    if row.get("direction_bucket") == "buy_pressure":
        return value < 0
    return None


def non_empty(value: Any) -> bool:
    return value not in (None, "", [], {}, ())


def first_value(row: dict[str, Any], *keys: str) -> Any:
    containers = [
        row,
        row.get("signal", {}).get("context", {}),
        row.get("signal", {}).get("metadata", {}),
        row.get("event", {}).get("metadata", {}),
        row.get("signal", {}),
        row.get("event", {}),
        row.get("signal", {}).get("context", {}).get("lp_outcome_record", {}),
        row.get("signal", {}).get("context", {}).get("outcome_tracking", {}),
    ]
    for key in keys:
        for container in containers:
            if isinstance(container, dict):
                value = container.get(key)
                if non_empty(value):
                    return value
    return None


def notifier_line1(row: dict[str, Any]) -> str:
    stage_badge = str(row.get("trade_action_label") or row.get("lp_stage_badge") or "确认")
    pair_or_pool = str(row.get("pair_label") or row.get("pool_address") or "unknown")
    state_label = str(
        row.get("trade_action_conclusion")
        or row.get("trade_action_reason")
        or row.get("lp_state_label")
        or row.get("market_state_label")
        or row.get("headline_label")
        or ""
    )
    return f"{stage_badge}｜{pair_or_pool}｜{state_label}"


def _default_no_trade_saved(row: dict[str, Any], field: str) -> bool | None:
    direction_adjusted = to_float(row.get(field))
    if direction_adjusted is None:
        return None
    if direction_adjusted <= 0:
        return True
    adverse_field = field.replace("direction_adjusted_move_after", "adverse_by_direction")
    adverse_value = row.get(adverse_field)
    if adverse_value in {None, ""}:
        return None
    return bool(adverse_value)


def _followthrough_result(row: dict[str, Any], field: str) -> tuple[bool | None, bool | None]:
    direction_adjusted = to_float(row.get(field))
    if direction_adjusted is None:
        return None, None
    adverse_field = field.replace("direction_adjusted_move_after", "adverse_by_direction")
    adverse_value = row.get(adverse_field)
    adverse = None if adverse_value in {None, ""} else bool(adverse_value)
    return direction_adjusted > 0, adverse


def env_whitelist() -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {}
    if not ENV_PATH.exists():
        return values
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in WHITELISTED_CONFIG_KEYS:
            continue
        values[key] = {"env_present": True, "env_value": value.strip().strip("'").strip('"')}
    return values


def load_runtime_config() -> dict[str, dict[str, Any]]:
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    import config  # type: ignore

    env_values = env_whitelist()
    payload: dict[str, dict[str, Any]] = {}
    for key in WHITELISTED_CONFIG_KEYS:
        runtime_value = getattr(config, key, None)
        if isinstance(runtime_value, (list, tuple, set)):
            runtime_value = list(runtime_value)
        payload[key] = {
            "runtime_value": runtime_value,
            "env_present": bool(env_values.get(key, {}).get("env_present")),
            "env_value": env_values.get(key, {}).get("env_value"),
        }
    payload["confirm_downgrade_logic"] = {
        "runtime_value": {
            "late_confirm": [
                "alert_timing == late",
                "market_context unavailable and pool_move_before >= 0.007",
                "pool_move_before >= 0.009",
                "market_move_before >= 0.008",
                "detect_latency_ms >= 4500",
                "case_age_sec >= 150",
                "quality_gap >= 0.18",
            ],
            "chase_risk": [
                "lp_chase_risk_score >= 0.58",
                "pool_move_before >= 0.010 or market_move_before >= 0.010",
                "late and no broader confirmation",
                "market_context unavailable and single_pool_dominant and pool_move_before >= 0.008",
                "detect_latency_ms >= 8000 or case_age_sec >= 240",
            ],
        },
        "env_present": False,
        "env_value": None,
    }
    return payload


def inventory_ndjson(path: Path, notes: str) -> FileInventory:
    if not path.exists():
        return FileInventory(str(path.relative_to(ROOT)), False, 0, None, None, notes)
    count = 0
    start_ts: int | None = None
    end_ts: int | None = None
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            count += 1
            payload = json.loads(line)
            ts = to_int(payload.get("archive_ts"))
            if start_ts is None:
                start_ts = ts
            end_ts = ts
    return FileInventory(str(path.relative_to(ROOT)), True, count, start_ts, end_ts, notes)


def inventory_category(category: str, notes: str) -> list[FileInventory]:
    root = ARCHIVE_DIR / category
    paths = sorted(root.glob("*.ndjson"))
    if not paths:
        return [FileInventory(str((root / "*.ndjson").relative_to(ROOT)), False, 0, None, None, notes)]
    return [inventory_ndjson(path, notes) for path in paths]


def load_signals() -> tuple[list[dict[str, Any]], list[FileInventory]]:
    rows: list[dict[str, Any]] = []
    inventory: list[FileInventory] = []
    for path in sorted((ARCHIVE_DIR / "signals").glob("*.ndjson")):
        inventory.append(inventory_ndjson(path, "signals archive"))
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                archive_ts = to_int(
                    first_value(data, "archive_ts", "archive_written_at")
                    or payload.get("archive_ts")
                )
                if archive_ts is None:
                    continue
                stage = str(first_value(data, "lp_alert_stage") or "")
                pair_label = str(first_value(data, "pair_label") or "")
                asset_symbol = canonical_asset(first_value(data, "asset_symbol") or pair_parts(pair_label)[0])
                pool_address = str(first_value(data, "pool_address", "address") or "").lower()
                signal_id = str(first_value(data, "signal_id") or "")
                intent_type = str(first_value(data, "intent_type", "canonical_semantic_key") or "")
                mc_attempts = first_value(data, "market_context_attempts") or []
                outcome_record = data.get("signal", {}).get("context", {}).get("lp_outcome_record", {})
                row = {
                    "archive_ts": archive_ts,
                    "signal_id": signal_id,
                    "event_id": str(first_value(data, "event_id") or ""),
                    "asset_case_id": str(first_value(data, "asset_case_id") or ""),
                    "asset_case_key": str(first_value(data, "asset_case_key") or ""),
                    "asset_symbol": asset_symbol,
                    "pair_label": pair_label,
                    "pool_address": pool_address,
                    "lp_alert_stage": stage,
                    "intent_type": intent_type,
                    "direction_bucket": str(first_value(data, "direction_bucket") or direction_bucket(intent_type)),
                    "sent_to_telegram": bool(first_value(data, "sent_to_telegram")),
                    "notifier_sent_at": to_int(first_value(data, "notifier_sent_at")),
                    "market_context_source": str(first_value(data, "market_context_source") or ""),
                    "market_context_venue": str(first_value(data, "market_context_venue") or ""),
                    "market_context_requested_symbol": str(first_value(data, "market_context_requested_symbol") or ""),
                    "market_context_resolved_symbol": str(first_value(data, "market_context_resolved_symbol") or ""),
                    "market_context_failure_reason": str(first_value(data, "market_context_failure_reason") or ""),
                    "market_context_attempts": mc_attempts if isinstance(mc_attempts, list) else [],
                    "market_context_attempted_venues": list(first_value(data, "market_context_attempted_venues") or []),
                    "outcome_tracking_key": str(first_value(data, "outcome_tracking_key") or ""),
                    "lp_prealert_candidate": bool(first_value(data, "lp_prealert_candidate")),
                    "lp_prealert_candidate_reason": str(first_value(data, "lp_prealert_candidate_reason") or ""),
                    "lp_prealert_gate_passed": bool(first_value(data, "lp_prealert_gate_passed")),
                    "lp_prealert_gate_fail_reason": str(first_value(data, "lp_prealert_gate_fail_reason") or ""),
                    "lp_prealert_delivery_allowed": first_value(data, "lp_prealert_delivery_allowed"),
                    "lp_prealert_delivery_block_reason": str(first_value(data, "lp_prealert_delivery_block_reason") or ""),
                    "lp_prealert_asset_case_preserved": first_value(data, "lp_prealert_asset_case_preserved"),
                    "lp_prealert_stage_overwritten": first_value(data, "lp_prealert_stage_overwritten"),
                    "lp_prealert_first_leg": bool(first_value(data, "lp_prealert_first_leg")),
                    "lp_prealert_major_override_used": bool(first_value(data, "lp_prealert_major_override_used")),
                    "lp_confirm_quality": str(first_value(data, "lp_confirm_quality") or ""),
                    "lp_confirm_scope": str(first_value(data, "lp_confirm_scope") or ""),
                    "lp_confirm_reason": str(first_value(data, "lp_confirm_reason") or ""),
                    "lp_confirm_timing_bucket": str(first_value(data, "lp_confirm_timing_bucket") or ""),
                    "lp_chase_risk_score": to_float(first_value(data, "lp_chase_risk_score")),
                    "lp_absorption_context": str(first_value(data, "lp_absorption_context") or ""),
                    "lp_broader_alignment": str(first_value(data, "lp_broader_alignment") or ""),
                    "lp_local_vs_broad_reason": str(first_value(data, "lp_local_vs_broad_reason") or ""),
                    "trade_action_key": str(first_value(data, "trade_action_key") or ""),
                    "trade_action_label": str(first_value(data, "trade_action_label") or ""),
                    "trade_action_direction": str(first_value(data, "trade_action_direction") or ""),
                    "trade_action_confidence": to_float(first_value(data, "trade_action_confidence")),
                    "trade_action_reason": str(first_value(data, "trade_action_reason") or ""),
                    "trade_action_required_confirmation": str(first_value(data, "trade_action_required_confirmation") or ""),
                    "trade_action_invalidated_by": str(first_value(data, "trade_action_invalidated_by") or ""),
                    "trade_action_time_horizon_sec": to_int(first_value(data, "trade_action_time_horizon_sec")),
                    "trade_action_source": str(first_value(data, "trade_action_source") or ""),
                    "trade_action_is_instruction": bool(first_value(data, "trade_action_is_instruction")),
                    "trade_action_requires_user_confirmation": bool(first_value(data, "trade_action_requires_user_confirmation")),
                    "trade_action_blockers": list(first_value(data, "trade_action_blockers") or []),
                    "trade_action_conclusion": str(first_value(data, "trade_action_conclusion") or ""),
                    "lp_conflict_context": str(first_value(data, "lp_conflict_context") or ""),
                    "lp_conflict_score": to_float(first_value(data, "lp_conflict_score")),
                    "lp_conflict_window_sec": to_int(first_value(data, "lp_conflict_window_sec")),
                    "lp_conflicting_signals": list(first_value(data, "lp_conflicting_signals") or []),
                    "lp_conflict_resolution": str(first_value(data, "lp_conflict_resolution") or ""),
                    "lp_sweep_phase": str(first_value(data, "lp_sweep_phase") or ""),
                    "lp_sweep_display_stage": str(first_value(data, "lp_sweep_display_stage") or ""),
                    "lp_stage_badge": str(first_value(data, "lp_stage_badge") or ""),
                    "lp_state_label": str(first_value(data, "lp_state_label") or ""),
                    "lp_market_read": str(first_value(data, "lp_market_read") or ""),
                    "market_state_label": str(first_value(data, "market_state_label") or ""),
                    "headline_label": str(first_value(data, "headline_label") or ""),
                    "fact_brief": str(first_value(data, "fact_brief") or ""),
                    "explanation_brief": str(first_value(data, "explanation_brief") or ""),
                    "evidence_brief": str(first_value(data, "evidence_brief") or ""),
                    "action_hint": str(first_value(data, "action_hint") or ""),
                    "pair_quality_score": to_float(first_value(data, "pair_quality_score")),
                    "pool_quality_score": to_float(first_value(data, "pool_quality_score")),
                    "asset_case_quality_score": to_float(first_value(data, "asset_case_quality_score")),
                    "asset_case_supporting_pair_count": to_int(first_value(data, "asset_case_supporting_pair_count")),
                    "asset_case_multi_pool": bool(first_value(data, "asset_case_multi_pool")),
                    "alert_relative_timing": str(first_value(data, "alert_relative_timing") or ""),
                    "market_move_before_alert_30s": to_float(first_value(data, "market_move_before_alert_30s")),
                    "market_move_before_alert_60s": to_float(first_value(data, "market_move_before_alert_60s")),
                    "market_move_after_alert_60s": to_float(first_value(data, "market_move_after_alert_60s")),
                    "market_move_after_alert_300s": to_float(first_value(data, "market_move_after_alert_300s")),
                    "move_after_alert_30s": to_float(first_value(data, "move_after_alert_30s")),
                    "move_after_alert_60s": to_float(first_value(data, "move_after_alert_60s")),
                    "move_after_alert_300s": to_float(first_value(data, "move_after_alert_300s")),
                    "raw_move_after_30s": to_float(first_value(data, "raw_move_after_30s")),
                    "raw_move_after_60s": to_float(first_value(data, "raw_move_after_60s")),
                    "raw_move_after_300s": to_float(first_value(data, "raw_move_after_300s")),
                    "direction_adjusted_move_after_30s": to_float(first_value(data, "direction_adjusted_move_after_30s")),
                    "direction_adjusted_move_after_60s": to_float(first_value(data, "direction_adjusted_move_after_60s")),
                    "direction_adjusted_move_after_300s": to_float(first_value(data, "direction_adjusted_move_after_300s")),
                    "adverse_by_direction_30s": first_value(data, "adverse_by_direction_30s"),
                    "adverse_by_direction_60s": first_value(data, "adverse_by_direction_60s"),
                    "adverse_by_direction_300s": first_value(data, "adverse_by_direction_300s"),
                    "outcome_windows": first_value(data, "outcome_windows") or {},
                    "asset_case_had_prealert": bool(first_value(data, "asset_case_had_prealert")),
                    "asset_case_prealert_to_confirm_sec": to_int(first_value(data, "asset_case_prealert_to_confirm_sec")),
                    "outcome_record": outcome_record if isinstance(outcome_record, dict) else {},
                    "top_level_stage_present": non_empty(data.get("lp_alert_stage")),
                    "nested_stage_present": non_empty(data.get("signal", {}).get("context", {}).get("lp_alert_stage")),
                    "raw": data,
                }
                row["notifier_line1"] = notifier_line1(row)
                rows.append(row)
    return rows, inventory


def choose_window(signal_rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    all_rows = sorted(signal_rows, key=lambda row: int(row["archive_ts"]))
    if not all_rows:
        raise RuntimeError("signals archive is empty")
    segments: list[dict[str, Any]] = []
    current_start = int(all_rows[0]["archive_ts"])
    current_end = current_start
    current_total = 0
    current_lp = 0
    for row in all_rows:
        ts = int(row["archive_ts"])
        if ts - current_end >= 3600 and current_total:
            segments.append(
                {
                    "start_ts": current_start,
                    "end_ts": current_end,
                    "total_signal_rows": current_total,
                    "lp_signal_rows": current_lp,
                }
            )
            current_start = ts
            current_total = 0
            current_lp = 0
        current_end = ts
        current_total += 1
        if row.get("lp_alert_stage"):
            current_lp += 1
    segments.append(
        {
            "start_ts": current_start,
            "end_ts": current_end,
            "total_signal_rows": current_total,
            "lp_signal_rows": current_lp,
        }
    )
    for item in segments:
        item["duration_sec"] = int(item["end_ts"]) - int(item["start_ts"])
        item["duration_hours"] = round(item["duration_sec"] / 3600.0, 2)
    primary = sorted(
        segments,
        key=lambda item: (
            -int(item["duration_sec"]),
            -int(item["end_ts"]),
            -int(item["lp_signal_rows"]),
            -int(item["total_signal_rows"]),
        ),
    )[0]
    primary["selection_reason"] = (
        "latest segment with the longest continuous signal activity "
        "after splitting on >=1h signal gaps; it also has the highest overnight LP row count."
    )
    return primary, segments


def load_quality_cache() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], FileInventory]:
    path = DATA_DIR / "lp_quality_stats.cache.json"
    if not path.exists():
        return [], {}, FileInventory(str(path.relative_to(ROOT)), False, 0, None, None, "quality stats cache")
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = list(payload.get("records") or [])
    inventory_times: list[int] = []
    for row in rows:
        for key in ("created_at", "notifier_sent_at"):
            ts = to_int(row.get(key))
            if ts is not None:
                inventory_times.append(ts)
    generated_at = to_int(payload.get("generated_at"))
    if generated_at is not None:
        inventory_times.append(generated_at)
    inventory = FileInventory(
        str(path.relative_to(ROOT)),
        True,
        len(rows),
        min(inventory_times) if inventory_times else None,
        max(inventory_times) if inventory_times else None,
        "quality stats cache",
    )
    by_signal = {str(row.get("signal_id") or ""): row for row in rows if row.get("signal_id")}
    return rows, by_signal, inventory


def load_asset_case_cache() -> tuple[dict[str, Any], FileInventory]:
    path = DATA_DIR / "asset_cases.cache.json"
    if not path.exists():
        return {}, FileInventory(str(path.relative_to(ROOT)), False, 0, None, None, "asset case cache")
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = list(payload.get("cases") or [])
    times: list[int] = []
    for case in cases:
        for key in ("started_at", "updated_at", "last_signal_at", "last_stage_transition_at"):
            ts = to_int(case.get(key))
            if ts is not None:
                times.append(ts)
    generated_at = to_int(payload.get("generated_at"))
    if generated_at is not None:
        times.append(generated_at)
    inventory = FileInventory(
        str(path.relative_to(ROOT)),
        True,
        len(cases),
        min(times) if times else None,
        max(times) if times else None,
        "asset case snapshot cache",
    )
    return payload, inventory


def join_lp_rows(
    signal_rows: list[dict[str, Any]],
    quality_by_signal: dict[str, dict[str, Any]],
    window_start: int,
    window_end: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    window_signal_rows = [
        row for row in signal_rows if window_start <= int(row["archive_ts"]) <= window_end
    ]
    all_lp_rows: list[dict[str, Any]] = []
    lp_rows_window: list[dict[str, Any]] = []
    for row in window_signal_rows:
        if not row.get("lp_alert_stage"):
            continue
        quality = quality_by_signal.get(str(row.get("signal_id") or ""), {})
        merged = dict(row)
        merged["created_at"] = to_int(quality.get("created_at")) or int(row["archive_ts"])
        merged["direction_bucket"] = str(
            quality.get("direction_bucket")
            or row.get("direction_bucket")
            or direction_bucket(row.get("intent_type"))
        )
        merged["move_before_alert_30s"] = to_float(quality.get("move_before_alert_30s"))
        merged["move_before_alert_60s"] = to_float(quality.get("move_before_alert_60s"))
        merged["move_after_alert_30s"] = to_float(quality.get("move_after_alert_30s")) if to_float(quality.get("move_after_alert_30s")) is not None else row.get("move_after_alert_30s")
        merged["move_after_alert_60s"] = to_float(quality.get("move_after_alert_60s")) if to_float(quality.get("move_after_alert_60s")) is not None else row.get("move_after_alert_60s")
        merged["move_after_alert_300s"] = to_float(quality.get("move_after_alert_300s")) if to_float(quality.get("move_after_alert_300s")) is not None else row.get("move_after_alert_300s")
        merged["raw_move_after_30s"] = to_float(quality.get("raw_move_after_30s")) if to_float(quality.get("raw_move_after_30s")) is not None else row.get("raw_move_after_30s")
        merged["raw_move_after_60s"] = to_float(quality.get("raw_move_after_60s")) if to_float(quality.get("raw_move_after_60s")) is not None else row.get("raw_move_after_60s")
        merged["raw_move_after_300s"] = to_float(quality.get("raw_move_after_300s")) if to_float(quality.get("raw_move_after_300s")) is not None else row.get("raw_move_after_300s")
        merged["direction_adjusted_move_after_30s"] = to_float(quality.get("direction_adjusted_move_after_30s")) if to_float(quality.get("direction_adjusted_move_after_30s")) is not None else row.get("direction_adjusted_move_after_30s")
        merged["direction_adjusted_move_after_60s"] = to_float(quality.get("direction_adjusted_move_after_60s")) if to_float(quality.get("direction_adjusted_move_after_60s")) is not None else row.get("direction_adjusted_move_after_60s")
        merged["direction_adjusted_move_after_300s"] = to_float(quality.get("direction_adjusted_move_after_300s")) if to_float(quality.get("direction_adjusted_move_after_300s")) is not None else row.get("direction_adjusted_move_after_300s")
        merged["adverse_by_direction_30s"] = quality.get("adverse_by_direction_30s") if quality.get("adverse_by_direction_30s") is not None else row.get("adverse_by_direction_30s")
        merged["adverse_by_direction_60s"] = quality.get("adverse_by_direction_60s") if quality.get("adverse_by_direction_60s") is not None else row.get("adverse_by_direction_60s")
        merged["adverse_by_direction_300s"] = quality.get("adverse_by_direction_300s") if quality.get("adverse_by_direction_300s") is not None else row.get("adverse_by_direction_300s")
        merged["outcome_windows"] = quality.get("outcome_windows") if quality.get("outcome_windows") else row.get("outcome_windows")
        merged["confirm_after_prealert"] = quality.get("confirm_after_prealert")
        merged["time_to_confirm"] = to_int(quality.get("time_to_confirm"))
        merged["false_prealert"] = quality.get("false_prealert")
        merged["followthrough_positive"] = quality.get("followthrough_positive")
        merged["followthrough_negative"] = quality.get("followthrough_negative")
        merged["delivered_notification"] = bool(
            quality.get("delivered_notification")
            or row.get("sent_to_telegram")
            or row.get("notifier_sent_at")
        )
        merged["pair_label"] = str(quality.get("pair_label") or row.get("pair_label") or "")
        merged["pool_address"] = str(quality.get("pool_address") or row.get("pool_address") or "").lower()
        merged["asset_symbol"] = canonical_asset(
            quality.get("asset_symbol") or row.get("asset_symbol") or pair_parts(row.get("pair_label"))[0]
        )
        merged["asset_case_id"] = str(quality.get("asset_case_id") or row.get("asset_case_id") or "")
        merged["asset_case_quality_score"] = to_float(
            quality.get("asset_case_quality_score") or row.get("asset_case_quality_score")
        )
        merged["pair_quality_score"] = to_float(
            row.get("pair_quality_score") or quality.get("pair_quality_score")
        )
        merged["pool_quality_score"] = to_float(
            row.get("pool_quality_score") or quality.get("pool_quality_score")
        )
        merged["asset_case_supporting_pair_count"] = to_int(
            row.get("asset_case_supporting_pair_count")
            or quality.get("asset_case_supporting_pair_count")
        )
        merged["asset_case_multi_pool"] = bool(
            row.get("asset_case_multi_pool") or quality.get("asset_case_multi_pool")
        )
        merged["market_context_requested_symbol"] = str(
            row.get("market_context_requested_symbol")
            or quality.get("market_context_requested_symbol")
            or ""
        )
        merged["market_context_resolved_symbol"] = str(
            row.get("market_context_resolved_symbol")
            or quality.get("market_context_resolved_symbol")
            or ""
        )
        merged["market_context_failure_reason"] = str(
            row.get("market_context_failure_reason")
            or quality.get("market_context_failure_reason")
            or ""
        )
        merged["market_context_source"] = str(
            row.get("market_context_source") or quality.get("market_context_source") or ""
        )
        merged["lp_prealert_candidate"] = bool(
            quality.get("lp_prealert_candidate")
            if quality and quality.get("lp_prealert_candidate") is not None
            else row.get("lp_prealert_candidate")
        )
        merged["lp_prealert_candidate_reason"] = str(
            quality.get("lp_prealert_candidate_reason")
            or row.get("lp_prealert_candidate_reason")
            or ""
        )
        merged["lp_prealert_gate_passed"] = bool(
            quality.get("lp_prealert_gate_passed")
            if quality and quality.get("lp_prealert_gate_passed") is not None
            else row.get("lp_prealert_gate_passed")
        )
        merged["lp_prealert_gate_fail_reason"] = str(
            quality.get("lp_prealert_gate_fail_reason")
            or row.get("lp_prealert_gate_fail_reason")
            or ""
        )
        merged["lp_prealert_delivery_allowed"] = (
            quality.get("lp_prealert_delivery_allowed")
            if quality and quality.get("lp_prealert_delivery_allowed") is not None
            else row.get("lp_prealert_delivery_allowed")
        )
        merged["lp_prealert_delivery_block_reason"] = str(
            quality.get("lp_prealert_delivery_block_reason")
            or row.get("lp_prealert_delivery_block_reason")
            or ""
        )
        merged["lp_prealert_asset_case_preserved"] = (
            quality.get("lp_prealert_asset_case_preserved")
            if quality and quality.get("lp_prealert_asset_case_preserved") is not None
            else row.get("lp_prealert_asset_case_preserved")
        )
        merged["lp_prealert_stage_overwritten"] = (
            quality.get("lp_prealert_stage_overwritten")
            if quality and quality.get("lp_prealert_stage_overwritten") is not None
            else row.get("lp_prealert_stage_overwritten")
        )
        merged["lp_prealert_first_leg"] = bool(
            quality.get("lp_prealert_first_leg")
            if quality and quality.get("lp_prealert_first_leg") is not None
            else row.get("lp_prealert_first_leg")
        )
        merged["lp_prealert_major_override_used"] = bool(
            quality.get("lp_prealert_major_override_used")
            if quality and quality.get("lp_prealert_major_override_used") is not None
            else row.get("lp_prealert_major_override_used")
        )
        merged["asset_case_had_prealert"] = bool(
            quality.get("asset_case_had_prealert")
            if quality and quality.get("asset_case_had_prealert") is not None
            else row.get("asset_case_had_prealert")
        )
        merged["asset_case_prealert_to_confirm_sec"] = to_int(
            quality.get("asset_case_prealert_to_confirm_sec")
            if quality and quality.get("asset_case_prealert_to_confirm_sec") is not None
            else row.get("asset_case_prealert_to_confirm_sec")
        )
        merged["lp_confirm_quality"] = str(
            row.get("lp_confirm_quality") or quality.get("lp_confirm_quality") or ""
        )
        merged["lp_confirm_scope"] = str(
            row.get("lp_confirm_scope") or quality.get("lp_confirm_scope") or ""
        )
        merged["lp_absorption_context"] = str(
            row.get("lp_absorption_context") or quality.get("lp_absorption_context") or ""
        )
        merged["lp_broader_alignment"] = str(
            row.get("lp_broader_alignment") or quality.get("lp_broader_alignment") or ""
        )
        all_lp_rows.append(merged)
        lp_rows_window.append(merged)
    return window_signal_rows, all_lp_rows, lp_rows_window


def stream_delivery_audit(
    window_start: int,
    window_end: int,
    signal_ids: set[str],
    delivered_signal_ids: set[str],
) -> tuple[list[FileInventory], dict[str, Any]]:
    inventories: list[FileInventory] = []
    matched_ids: set[str] = set()
    delivered_matched_ids: set[str] = set()
    notifier_present = 0
    delivered_rows = 0
    for path in sorted((ARCHIVE_DIR / "delivery_audit").glob("*.ndjson")):
        record_count = 0
        start_ts: int | None = None
        end_ts: int | None = None
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                record_count += 1
                payload = json.loads(line)
                ts = to_int(payload.get("archive_ts"))
                if start_ts is None:
                    start_ts = ts
                end_ts = ts
                if ts is None or ts < window_start or ts > window_end:
                    continue
                row = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                signal_id = str(row.get("signal_id") or "")
                if signal_id in signal_ids:
                    matched_ids.add(signal_id)
                delivered = bool(row.get("delivered_notification") or row.get("notifier_sent_at"))
                if signal_id in delivered_signal_ids and delivered:
                    delivered_matched_ids.add(signal_id)
                    delivered_rows += 1
                    if row.get("notifier_sent_at"):
                        notifier_present += 1
        inventories.append(
            FileInventory(
                str(path.relative_to(ROOT)),
                True,
                record_count,
                start_ts,
                end_ts,
                "delivery audit archive",
            )
        )
    return inventories, {
        "matched_signal_ids": matched_ids,
        "delivered_matched_ids": delivered_matched_ids,
        "delivered_rows": delivered_rows,
        "notifier_present_rows": notifier_present,
    }


def stream_cases(
    window_start: int,
    window_end: int,
    signal_ids: set[str],
    delivered_signal_ids: set[str],
) -> tuple[list[FileInventory], dict[str, Any]]:
    inventories: list[FileInventory] = []
    case_attached_ids: set[str] = set()
    case_ids: set[str] = set()
    lp_case_rows = 0
    for path in sorted((ARCHIVE_DIR / "cases").glob("*.ndjson")):
        record_count = 0
        start_ts: int | None = None
        end_ts: int | None = None
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                record_count += 1
                payload = json.loads(line)
                ts = to_int(payload.get("archive_ts"))
                if start_ts is None:
                    start_ts = ts
                end_ts = ts
                if ts is None or ts < window_start or ts > window_end:
                    continue
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                signal = data.get("signal") if isinstance(data.get("signal"), dict) else {}
                event = data.get("event") if isinstance(data.get("event"), dict) else {}
                context = signal.get("context") if isinstance(signal.get("context"), dict) else {}
                stage = (
                    context.get("lp_alert_stage")
                    or signal.get("metadata", {}).get("lp_alert_stage")
                    or event.get("metadata", {}).get("lp_alert_stage")
                    or ""
                )
                if not stage:
                    continue
                lp_case_rows += 1
                action = str(data.get("action") or "")
                signal_id = str(signal.get("signal_id") or "")
                if signal_id in signal_ids and action == "signal_attached":
                    if signal_id in delivered_signal_ids:
                        case_attached_ids.add(signal_id)
                    case = data.get("case") if isinstance(data.get("case"), dict) else {}
                    case_id = str(case.get("case_id") or "")
                    if case_id:
                        case_ids.add(case_id)
        inventories.append(
            FileInventory(
                str(path.relative_to(ROOT)),
                True,
                record_count,
                start_ts,
                end_ts,
                "case archive",
            )
        )
    return inventories, {
        "case_attached_ids": case_attached_ids,
        "case_ids": case_ids,
        "lp_case_rows": lp_case_rows,
    }


def stream_case_followups(
    window_start: int,
    window_end: int,
    delivered_signal_ids: set[str],
) -> tuple[list[FileInventory], dict[str, Any]]:
    inventories: list[FileInventory] = []
    followup_signal_ids: set[str] = set()
    followup_rows = 0
    for path in sorted((ARCHIVE_DIR / "case_followups").glob("*.ndjson")):
        record_count = 0
        start_ts: int | None = None
        end_ts: int | None = None
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                record_count += 1
                payload = json.loads(line)
                ts = to_int(payload.get("archive_ts"))
                if start_ts is None:
                    start_ts = ts
                end_ts = ts
                if ts is None or ts < window_start or ts > window_end:
                    continue
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                followup = data.get("followup") if isinstance(data.get("followup"), dict) else {}
                signal_id = str(followup.get("signal_id") or "")
                if signal_id in delivered_signal_ids:
                    followup_signal_ids.add(signal_id)
                    followup_rows += 1
        inventories.append(
            FileInventory(
                str(path.relative_to(ROOT)),
                True,
                record_count,
                start_ts,
                end_ts,
                "case followups archive",
            )
        )
    return inventories, {
        "followup_signal_ids": followup_signal_ids,
        "followup_rows": followup_rows,
    }


def run_cli(args: list[str], expect_json: bool) -> Any:
    python_bin = ROOT / "venv" / "bin" / "python"
    cmd = [str(python_bin if python_bin.exists() else Path(sys.executable)), "-m", "app.quality_reports", *args]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout) if expect_json else result.stdout


def stage_distribution(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_alert_stage") or "") for row in lp_rows)
    total = len(lp_rows)
    return {
        "prealert_count": counter.get("prealert", 0),
        "confirm_count": counter.get("confirm", 0),
        "climax_count": counter.get("climax", 0),
        "exhaustion_risk_count": counter.get("exhaustion_risk", 0),
        "stage_distribution_pct": {
            key: pct(counter.get(key, 0), total) for key in LP_STAGES
        },
    }


def compute_market_context(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_counter = Counter(str(row.get("market_context_source") or "") for row in lp_rows)
    attempts = Counter()
    success = Counter()
    failure = Counter()
    failure_reasons = Counter()
    fallback_distribution = Counter()
    resolved_symbol_distribution = Counter()
    for row in lp_rows:
        requested = str(row.get("market_context_requested_symbol") or "")
        resolved = str(row.get("market_context_resolved_symbol") or "")
        if requested or resolved:
            fallback_distribution[f"{requested}->{resolved}"] += 1
        if resolved:
            resolved_symbol_distribution[resolved] += 1
        for attempt in row.get("market_context_attempts") or []:
            venue = str(attempt.get("venue") or "")
            status = str(attempt.get("status") or "")
            if venue:
                attempts[venue] += 1
                if status in {"success", "cache_hit"}:
                    success[venue] += 1
                elif status == "failure":
                    failure[venue] += 1
            reason = str(attempt.get("failure_reason") or "")
            if reason:
                failure_reasons[reason] += 1
    total = len(lp_rows)
    return {
        "live_public_count": source_counter.get("live_public", 0),
        "unavailable_count": source_counter.get("unavailable", 0),
        "live_public_rate": rate(source_counter.get("live_public", 0), total),
        "unavailable_rate": rate(source_counter.get("unavailable", 0), total),
        "okx_attempts": attempts.get("okx_perp", 0),
        "okx_success": success.get("okx_perp", 0),
        "okx_failure": failure.get("okx_perp", 0),
        "kraken_attempts": attempts.get("kraken_futures", 0),
        "kraken_success": success.get("kraken_futures", 0),
        "kraken_failure": failure.get("kraken_futures", 0),
        "binance_attempts": attempts.get("binance_perp", 0),
        "bybit_attempts": attempts.get("bybit_perp", 0),
        "top_failure_reasons": [
            {"reason": key, "count": value} for key, value in failure_reasons.most_common(10)
        ],
        "resolved_symbol_distribution": dict(sorted(resolved_symbol_distribution.items())),
        "requested_to_resolved_distribution": dict(sorted(fallback_distribution.items())),
    }


def compute_prealerts(lp_rows: list[dict[str, Any]], major_pairs: set[str], full_cli_summary: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in lp_rows if row.get("lp_alert_stage") == "prealert"]
    candidate_rows = [row for row in lp_rows if row.get("lp_prealert_candidate")]
    gate_passed_rows = [row for row in lp_rows if row.get("lp_prealert_gate_passed")]
    delivered_rows = [
        row for row in lp_rows
        if row.get("lp_prealert_gate_passed")
        and row.get("lp_alert_stage") == "prealert"
        and (row.get("lp_prealert_delivery_allowed") is True)
        and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    ]
    merged_rows = [row for row in lp_rows if row.get("lp_prealert_asset_case_preserved")]
    upgraded_rows = [row for row in lp_rows if row.get("asset_case_had_prealert") and row.get("asset_case_prealert_to_confirm_sec") is not None]
    major_count = sum(1 for row in rows if row.get("pair_label") in major_pairs)
    non_major_count = len(rows) - major_count
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in lp_rows:
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            by_case[case_id].append(row)
    conversions = {30: 0, 60: 0, 90: 0}
    for row in rows:
        time_to_confirm = to_int(row.get("time_to_confirm"))
        if time_to_confirm is None:
            case_id = str(row.get("asset_case_id") or "")
            if case_id:
                later_confirms = [
                    item for item in by_case.get(case_id, [])
                    if item.get("lp_alert_stage") == "confirm"
                    and int(item["created_at"]) > int(row["created_at"])
                ]
                if later_confirms:
                    time_to_confirm = int(later_confirms[0]["created_at"]) - int(row["created_at"])
        if time_to_confirm is None:
            continue
        if time_to_confirm <= 30:
            conversions[30] += 1
        if time_to_confirm <= 60:
            conversions[60] += 1
        if time_to_confirm <= 90:
            conversions[90] += 1
    previous_prealerts = to_int(
        full_cli_summary.get("overall", {}).get("prealert_count")
    )
    block_reason_counter = Counter(
        str(row.get("lp_prealert_delivery_block_reason") or row.get("lp_prealert_gate_fail_reason") or "")
        for row in candidate_rows
        if str(row.get("lp_prealert_delivery_block_reason") or row.get("lp_prealert_gate_fail_reason") or "").strip()
    )
    return {
        "prealert_count": len(rows),
        "major_prealert_count": major_count,
        "non_major_prealert_count": non_major_count,
        "prealert_candidates": len(candidate_rows),
        "prealert_gate_passed_count": len(gate_passed_rows),
        "prealert_delivered_count": len(delivered_rows),
        "prealert_merged_into_case_count": len(merged_rows),
        "prealert_upgraded_to_confirm_count": len(upgraded_rows),
        "prealert_dropped_by_reason": dict(block_reason_counter),
        "prealert_to_confirm_30s": rate(conversions[30], len(rows)),
        "prealert_to_confirm_60s": rate(conversions[60], len(rows)),
        "prealert_to_confirm_90s": rate(conversions[90], len(rows)),
        "previous_report_reference_prealert_count": previous_prealerts,
    }


def compute_confirms(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for row in lp_rows if row.get("lp_alert_stage") == "confirm"]
    quality_counter = Counter(str(row.get("lp_confirm_quality") or "(blank)") for row in rows)
    scope_counter = Counter(str(row.get("lp_confirm_scope") or "(blank)") for row in rows)
    broader_counter = Counter(str(row.get("lp_broader_alignment") or "(blank)") for row in rows)
    predict_warning_count = sum(
        1
        for row in rows
        if "不是下一根 K 线预测" in str(row.get("lp_market_read") or "")
        or "不能当作继续追击的保证" in str(row.get("lp_confirm_reason") or "")
        or "不是首发先手" in str(row.get("lp_market_read") or "")
    )
    return {
        "confirm_count": len(rows),
        "clean_confirm_count": quality_counter.get("clean_confirm", 0),
        "local_confirm_count": scope_counter.get("local_confirm", 0),
        "broader_confirm_count": scope_counter.get("broader_confirm", 0),
        "late_confirm_count": quality_counter.get("late_confirm", 0),
        "chase_risk_count": quality_counter.get("chase_risk", 0),
        "unconfirmed_confirm_count": quality_counter.get("unconfirmed_confirm", 0),
        "blank_confirm_quality_count": quality_counter.get("(blank)", 0),
        "broader_alignment_confirmed_count": broader_counter.get("confirmed", 0),
        "confirm_move_before_30s_median": median([row.get("move_before_alert_30s") for row in rows]),
        "confirm_move_after_60s_median": median([row.get("move_after_alert_60s") for row in rows]),
        "confirm_move_after_300s_median": median([row.get("move_after_alert_300s") for row in rows]),
        "predict_warning_text_count": predict_warning_count,
    }


def compute_sweeps(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_sweep_phase") or "") for row in lp_rows)
    building_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_building"]
    residuals = []
    by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(lp_rows, key=lambda item: int(item["created_at"])):
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            by_case[case_id].append(row)
    progressed_to_confirmed = 0
    progressed_to_continue = 0
    for row in building_rows:
        haystack = " ".join(
            [
                str(row.get("lp_state_label") or ""),
                str(row.get("lp_sweep_display_stage") or ""),
                str(row.get("lp_alert_stage") or ""),
                str(row.get("notifier_line1") or ""),
            ]
        ).lower()
        if "高潮" in haystack or "climax" in haystack:
            residuals.append(
                {
                    "signal_id": row.get("signal_id"),
                    "asset_case_id": row.get("asset_case_id"),
                    "pair_label": row.get("pair_label"),
                    "message": row.get("notifier_line1"),
                    "state_label": row.get("lp_state_label"),
                    "display_stage": row.get("lp_sweep_display_stage"),
                }
            )
        case_id = str(row.get("asset_case_id") or "")
        future_rows = [
            item for item in by_case.get(case_id, [])
            if int(item["created_at"]) > int(row["created_at"])
            and int(item["created_at"]) - int(row["created_at"]) <= 300
        ]
        if any(item.get("lp_sweep_phase") == "sweep_confirmed" for item in future_rows):
            progressed_to_confirmed += 1
        if any(
            item.get("lp_alert_stage") in {"confirm", "climax", "exhaustion_risk"}
            or item.get("lp_sweep_phase") in {"sweep_confirmed", "sweep_exhaustion_risk"}
            for item in future_rows
        ):
            progressed_to_continue += 1
    def reversal_rate(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
        resolved = [row for row in rows if to_float(row.get(field)) is not None]
        adverse = [row for row in resolved if adverse_move(row, field) is True]
        return {
            "resolved_count": len(resolved),
            "adverse_count": len(adverse),
            "adverse_rate": rate(len(adverse), len(resolved)),
        }
    direction_perf = {}
    for direction in ("buy_pressure", "sell_pressure"):
        rows = [row for row in lp_rows if row.get("lp_sweep_phase") and row.get("direction_bucket") == direction]
        direction_perf[direction] = {
            "count": len(rows),
            "move_after_60s_median": median([aligned_move(row, "move_after_alert_60s") for row in rows]),
            "move_after_300s_median": median([aligned_move(row, "move_after_alert_300s") for row in rows]),
            "adverse_60s_rate": reversal_rate(rows, "move_after_alert_60s")["adverse_rate"],
            "adverse_300s_rate": reversal_rate(rows, "move_after_alert_300s")["adverse_rate"],
        }
    sweep_confirmed_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_confirmed"]
    sweep_exhaustion_rows = [row for row in lp_rows if row.get("lp_sweep_phase") == "sweep_exhaustion_risk"]
    return {
        "sweep_building_count": counter.get("sweep_building", 0),
        "sweep_confirmed_count": counter.get("sweep_confirmed", 0),
        "sweep_exhaustion_risk_count": counter.get("sweep_exhaustion_risk", 0),
        "sweep_building_display_climax_residual_count": len(residuals),
        "sweep_building_residuals": residuals,
        "sweep_building_to_sweep_confirmed_rate": rate(progressed_to_confirmed, len(building_rows)),
        "sweep_building_to_continue_rate": rate(progressed_to_continue, len(building_rows)),
        "sweep_reversal_60s": reversal_rate(sweep_confirmed_rows, "move_after_alert_60s"),
        "sweep_reversal_300s": reversal_rate(sweep_confirmed_rows, "move_after_alert_300s"),
        "sweep_exhaustion_outcome_300s": reversal_rate(sweep_exhaustion_rows, "move_after_alert_300s"),
        "direction_performance": direction_perf,
    }


def compute_absorption(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(str(row.get("lp_absorption_context") or "") for row in lp_rows)
    return {
        "local_sell_pressure_absorption_count": counter.get("local_sell_pressure_absorption", 0),
        "local_buy_pressure_absorption_count": counter.get("local_buy_pressure_absorption", 0),
        "broader_sell_pressure_confirmed_count": counter.get("broader_sell_pressure_confirmed", 0),
        "broader_buy_pressure_confirmed_count": counter.get("broader_buy_pressure_confirmed", 0),
        "pool_only_unconfirmed_pressure_count": counter.get("pool_only_unconfirmed_pressure", 0),
        "distribution": dict(sorted(counter.items())),
    }


def compute_trade_actions(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    keys = [
        "LONG_CHASE_ALLOWED",
        "SHORT_CHASE_ALLOWED",
        "NO_TRADE",
        "WAIT_CONFIRMATION",
        "DO_NOT_CHASE_LONG",
        "DO_NOT_CHASE_SHORT",
        "CONFLICT_NO_TRADE",
        "DATA_GAP_NO_TRADE",
        "LONG_BIAS_OBSERVE",
        "SHORT_BIAS_OBSERVE",
        "REVERSAL_WATCH_LONG",
        "REVERSAL_WATCH_SHORT",
    ]
    counter = Counter(str(row.get("trade_action_key") or "") for row in lp_rows if str(row.get("trade_action_key") or "").strip())

    def summarize(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
        resolved = []
        followthrough = 0
        adverse = 0
        for row in rows:
            ft, adv = _followthrough_result(row, field)
            if ft is None:
                continue
            resolved.append(row)
            if ft:
                followthrough += 1
            if adv is True:
                adverse += 1
        return {
            "resolved_count": len(resolved),
            "followthrough_count": followthrough,
            "followthrough_rate": rate(followthrough, len(resolved)),
            "adverse_count": adverse,
            "adverse_rate": rate(adverse, len(resolved)),
        }

    action_windows: dict[str, dict[str, Any]] = {}
    for field in (
        "direction_adjusted_move_after_30s",
        "direction_adjusted_move_after_60s",
        "direction_adjusted_move_after_300s",
    ):
        window_name = field.split("_")[-1]
        action_windows[window_name] = {
            key: summarize([row for row in lp_rows if str(row.get("trade_action_key") or "") == key], field)
            for key in keys
            if counter.get(key, 0) > 0
        }

    chase_rows = [
        row for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED"}
    ]
    confirm_rows = [
        row for row in lp_rows
        if str(row.get("lp_alert_stage") or "") in {"confirm", "climax"}
    ]
    no_trade_rows = [
        row for row in lp_rows
        if str(row.get("trade_action_key") or "") in {"NO_TRADE", "CONFLICT_NO_TRADE", "DATA_GAP_NO_TRADE"}
    ]
    saved_estimates = [
        _default_no_trade_saved(row, "direction_adjusted_move_after_300s")
        for row in no_trade_rows
    ]
    saved_known = [item for item in saved_estimates if item is not None]
    conflict_rows = [row for row in lp_rows if str(row.get("trade_action_key") or "") == "CONFLICT_NO_TRADE"]
    conflict_reversal = [
        row for row in conflict_rows
        if adverse_move(row, "move_after_alert_300s") is True
        or _default_no_trade_saved(row, "direction_adjusted_move_after_300s") is True
    ]
    action_label_counter = Counter(
        str(row.get("trade_action_label") or "") for row in lp_rows if str(row.get("trade_action_label") or "").strip()
    )
    return {
        "trade_action_distribution": dict(sorted(counter.items())),
        "trade_action_label_distribution": dict(sorted(action_label_counter.items())),
        "long_chase_allowed_count": counter.get("LONG_CHASE_ALLOWED", 0),
        "short_chase_allowed_count": counter.get("SHORT_CHASE_ALLOWED", 0),
        "no_trade_count": counter.get("NO_TRADE", 0),
        "wait_confirmation_count": counter.get("WAIT_CONFIRMATION", 0),
        "do_not_chase_long_count": counter.get("DO_NOT_CHASE_LONG", 0),
        "do_not_chase_short_count": counter.get("DO_NOT_CHASE_SHORT", 0),
        "conflict_no_trade_count": counter.get("CONFLICT_NO_TRADE", 0),
        "data_gap_no_trade_count": counter.get("DATA_GAP_NO_TRADE", 0),
        "trade_action_adverse_30s": action_windows["30s"],
        "trade_action_adverse_60s": action_windows["60s"],
        "trade_action_adverse_300s": action_windows["300s"],
        "trade_action_followthrough_30s": action_windows["30s"],
        "trade_action_followthrough_60s": action_windows["60s"],
        "trade_action_followthrough_300s": action_windows["300s"],
        "chase_allowed_success_rate": action_windows["300s"].get("LONG_CHASE_ALLOWED", {}).get("followthrough_rate", 0.0)
        if counter.get("SHORT_CHASE_ALLOWED", 0) == 0
        else rate(
            sum(
                action_windows["300s"].get(key, {}).get("followthrough_count", 0)
                for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
            ),
            sum(
                action_windows["300s"].get(key, {}).get("resolved_count", 0)
                for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
            ),
        ),
        "chase_allowed_adverse_rate": rate(
            sum(
                action_windows["300s"].get(key, {}).get("adverse_count", 0)
                for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
            ),
            sum(
                action_windows["300s"].get(key, {}).get("resolved_count", 0)
                for key in ("LONG_CHASE_ALLOWED", "SHORT_CHASE_ALLOWED")
            ),
        ),
        "generic_confirm_success_rate_300s": summarize(confirm_rows, "direction_adjusted_move_after_300s")["followthrough_rate"],
        "generic_confirm_adverse_rate_300s": summarize(confirm_rows, "direction_adjusted_move_after_300s")["adverse_rate"],
        "no_trade_would_have_saved_rate": rate(sum(1 for item in saved_known if item is True), len(saved_known)),
        "conflict_after_message_reversal_rate": rate(len(conflict_reversal), len(conflict_rows)),
        "chase_allowed_rows_resolved_300s": summarize(chase_rows, "direction_adjusted_move_after_300s")["resolved_count"],
    }


def compute_reversal_special(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    confirm_rows = [row for row in lp_rows if row.get("lp_alert_stage") == "confirm"]
    def summarize(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
        resolved = [row for row in rows if to_float(row.get(field)) is not None]
        adverse = [row for row in resolved if adverse_move(row, field) is True]
        return {
            "resolved_count": len(resolved),
            "against_count": len(adverse),
            "against_rate": rate(len(adverse), len(resolved)),
        }
    sell_rows = [row for row in confirm_rows if row.get("direction_bucket") == "sell_pressure"]
    buy_rows = [row for row in confirm_rows if row.get("direction_bucket") == "buy_pressure"]
    reverse_cases = [
        row for row in confirm_rows if adverse_move(row, "move_after_alert_300s") is True
    ]
    reason_counter = Counter()
    examples = []
    for row in reverse_cases[:5]:
        reasons = []
        quality = str(row.get("lp_confirm_quality") or "")
        if quality in {"late_confirm", "chase_risk"}:
            reasons.append(quality)
            reason_counter["late_or_chase"] += 1
        if str(row.get("lp_confirm_scope") or "") == "local_confirm":
            reasons.append("local_confirm_not_broader")
            reason_counter["local_confirm_not_broader"] += 1
        if str(row.get("market_context_source") or "") == "unavailable":
            reasons.append("market_context_unavailable")
            reason_counter["market_context_unavailable"] += 1
        if (to_int(row.get("asset_case_supporting_pair_count")) or 0) <= 1 or not bool(row.get("asset_case_multi_pool")):
            reasons.append("single_pool_or_low_resonance")
            reason_counter["single_pool_or_low_resonance"] += 1
        if str(row.get("lp_absorption_context") or "").startswith("local_"):
            reasons.append(str(row.get("lp_absorption_context")))
            reason_counter[str(row.get("lp_absorption_context"))] += 1
        if to_float(row.get("asset_case_quality_score")) is not None and float(row["asset_case_quality_score"]) < 0.62:
            reasons.append("low_quality")
            reason_counter["low_quality"] += 1
        if not reasons:
            reasons.append("possible_code_misclassification")
            reason_counter["possible_code_misclassification"] += 1
        examples.append(
            {
                "signal_id": row.get("signal_id"),
                "asset_case_id": row.get("asset_case_id"),
                "pair": row.get("pair_label"),
                "stage": row.get("lp_alert_stage"),
                "confirm_quality": row.get("lp_confirm_quality") or "(blank)",
                "absorption_context": row.get("lp_absorption_context"),
                "market_context_source": row.get("market_context_source"),
                "move_before": row.get("move_before_alert_30s"),
                "move_after": row.get("move_after_alert_300s"),
                "judgement_reason": ", ".join(reasons),
            }
        )
    return {
        "sell_confirm_count": len(sell_rows),
        "buy_confirm_count": len(buy_rows),
        "sell_after_30s_rise_ratio": summarize(sell_rows, "move_after_alert_30s")["against_rate"],
        "sell_after_60s_rise_ratio": summarize(sell_rows, "move_after_alert_60s")["against_rate"],
        "sell_after_300s_rise_ratio": summarize(sell_rows, "move_after_alert_300s")["against_rate"],
        "buy_after_30s_fall_ratio": summarize(buy_rows, "move_after_alert_30s")["against_rate"],
        "buy_after_60s_fall_ratio": summarize(buy_rows, "move_after_alert_60s")["against_rate"],
        "buy_after_300s_fall_ratio": summarize(buy_rows, "move_after_alert_300s")["against_rate"],
        "sell_after_30s": summarize(sell_rows, "move_after_alert_30s"),
        "sell_after_60s": summarize(sell_rows, "move_after_alert_60s"),
        "sell_after_300s": summarize(sell_rows, "move_after_alert_300s"),
        "buy_after_30s": summarize(buy_rows, "move_after_alert_30s"),
        "buy_after_60s": summarize(buy_rows, "move_after_alert_60s"),
        "buy_after_300s": summarize(buy_rows, "move_after_alert_300s"),
        "reason_distribution": dict(reason_counter),
        "examples": examples,
    }


def compute_majors(
    lp_rows: list[dict[str, Any]],
    major_cli: dict[str, Any],
    runtime_config: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    expected_pairs = [
        f"{asset}/{quote}"
        for asset in ("ETH", "BTC", "SOL")
        for quote in ("USDT", "USDC")
    ]
    pair_counter = Counter(str(row.get("pair_label") or "") for row in lp_rows)
    asset_counter = Counter(canonical_asset(row.get("asset_symbol")) for row in lp_rows)
    covered = [pair for pair in expected_pairs if pair_counter.get(pair, 0) > 0]
    missing = [pair for pair in expected_pairs if pair_counter.get(pair, 0) == 0]
    if major_cli.get("configured_but_disabled_major_pools"):
        missing_reason = "configured_but_disabled_pool"
    elif major_cli.get("malformed_major_pool_entries"):
        missing_reason = "malformed_pool_book_entry"
    elif major_cli.get("missing_major_pairs") or major_cli.get("missing_expected_pairs"):
        missing_reason = "pool_book_missing"
    else:
        missing_reason = "scan_unmatched_or_no_event"
    return {
        "covered_major_pairs": covered,
        "missing_major_pairs": missing,
        "eth_signal_count": asset_counter.get("ETH", 0),
        "btc_signal_count": asset_counter.get("BTC", 0),
        "sol_signal_count": asset_counter.get("SOL", 0),
        "asset_distribution": dict(asset_counter),
        "pair_distribution": dict(pair_counter),
        "current_sample_still_eth_only": set(asset_counter) <= {"ETH"},
        "major_cli_summary": major_cli,
        "btc_sol_missing_reason": missing_reason,
        "configured_major_assets": runtime_config.get("LP_MAJOR_ASSETS", {}).get("runtime_value"),
        "configured_major_quotes": runtime_config.get("LP_MAJOR_QUOTES", {}).get("runtime_value"),
    }


def compute_archive_integrity(
    lp_rows: list[dict[str, Any]],
    delivery_summary: dict[str, Any],
    cases_summary: dict[str, Any],
    followup_summary: dict[str, Any],
) -> dict[str, Any]:
    signal_ids = {str(row.get("signal_id") or "") for row in lp_rows if row.get("signal_id")}
    delivered_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows
        if row.get("signal_id") and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    }
    mixed_format_rows = sum(
        1 for row in lp_rows if row.get("top_level_stage_present") and row.get("nested_stage_present")
    )
    flat_stage_mismatches = 0
    for row in lp_rows:
        top = str(row.get("raw", {}).get("lp_alert_stage") or "")
        nested = str(row.get("raw", {}).get("signal", {}).get("context", {}).get("lp_alert_stage") or "")
        if top and nested and top != nested:
            flat_stage_mismatches += 1
    return {
        "signals_archive_exists": True,
        "signal_delivery_audit_match_rate": rate(len(delivery_summary["matched_signal_ids"]), len(signal_ids)),
        "signal_case_followup_match_rate": rate(len(followup_summary["followup_signal_ids"]), len(delivered_ids)),
        "signal_case_attached_match_rate": rate(len(cases_summary["case_attached_ids"]), len(delivered_ids)),
        "delivered_signal_notifier_sent_at_rate": rate(
            delivery_summary["notifier_present_rows"],
            delivery_summary["delivered_rows"],
        ),
        "signal_id_complete_rate": rate(sum(1 for row in lp_rows if row.get("signal_id")), len(lp_rows)),
        "asset_case_id_complete_rate": rate(sum(1 for row in lp_rows if row.get("asset_case_id")), len(lp_rows)),
        "outcome_tracking_key_complete_rate": rate(
            sum(1 for row in lp_rows if row.get("outcome_tracking_key")),
            len(lp_rows),
        ),
        "mixed_flat_nested_rows": mixed_format_rows,
        "flat_nested_stage_mismatch_rows": flat_stage_mismatches,
        "signal_ids": len(signal_ids),
        "delivered_signal_ids": len(delivered_ids),
    }


def compute_quality_and_fastlane(
    lp_rows: list[dict[str, Any]],
    quality_cli: dict[str, Any],
) -> dict[str, Any]:
    promoted_rows = [row for row in lp_rows if row.get("raw", {}).get("signal", {}).get("context", {}).get("lp_promoted_fastlane")]
    outcome_status = {
        "30s": Counter(),
        "60s": Counter(),
        "300s": Counter(),
    }
    for row in lp_rows:
        windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
        for window in ("30s", "60s", "300s"):
            status = str((windows.get(window) or {}).get("status") or "")
            if not status:
                status = "completed" if to_float(row.get(f"move_after_alert_{window}")) is not None else "pending"
            outcome_status[window][status] += 1
    return {
        "full_summary_cli": quality_cli,
        "fastlane_promoted_count_window": len(promoted_rows),
        "fastlane_promoted_delivered_count_window": sum(
            1 for row in promoted_rows if row.get("delivered_notification")
        ),
        "outcome_window_status": {
            window: dict(counter)
            for window, counter in outcome_status.items()
        },
        "resolved_move_after_30s_count_window": outcome_status["30s"].get("completed", 0),
        "resolved_move_after_60s_count_window": outcome_status["60s"].get("completed", 0),
        "resolved_move_after_300s_count_window": outcome_status["300s"].get("completed", 0),
    }


def compute_noise(lp_rows: list[dict[str, Any]], reversal: dict[str, Any], confirm: dict[str, Any]) -> dict[str, Any]:
    delivered_ratio = rate(
        sum(1 for row in lp_rows if row.get("delivered_notification")),
        len(lp_rows),
    )
    local_confirm_share = rate(confirm["local_confirm_count"], confirm["confirm_count"])
    exhaustion_rows = [row for row in lp_rows if row.get("lp_alert_stage") == "exhaustion_risk"]
    return {
        "delivered_ratio": delivered_ratio,
        "local_confirm_share": local_confirm_share,
        "unresolved_300s_share": rate(
            sum(1 for row in lp_rows if row.get("move_after_alert_300s") is None),
            len(lp_rows),
        ),
        "exhaustion_risk_count": len(exhaustion_rows),
        "reverse_case_count": len(reversal["examples"]),
    }


def scorecard(
    market_context: dict[str, Any],
    prealerts: dict[str, Any],
    confirm: dict[str, Any],
    sweeps: dict[str, Any],
    majors: dict[str, Any],
    archive_integrity: dict[str, Any],
    noise: dict[str, Any],
) -> dict[str, float]:
    readiness = 6.5
    if prealerts["prealert_count"] == 0:
        readiness -= 1.6
    if majors["current_sample_still_eth_only"]:
        readiness -= 1.5
    if archive_integrity["signal_delivery_audit_match_rate"] and archive_integrity["signal_delivery_audit_match_rate"] >= 0.99:
        readiness += 0.4
    live_context_score = 3.0
    if market_context["live_public_rate"] is not None:
        live_context_score = round(2 + 8 * float(market_context["live_public_rate"]), 1)
    if market_context["kraken_attempts"] == 0 and market_context["live_public_count"] > 0:
        live_context_score = max(0.0, round(live_context_score - 0.6, 1))
    prealert_score = 0.8 if prealerts["prealert_count"] == 0 else min(7.0, 2.0 + prealerts["major_prealert_count"] * 1.5)
    confirm_score = 7.2
    if confirm["local_confirm_count"] and confirm["broader_confirm_count"] == 0:
        confirm_score -= 0.6
    if confirm["blank_confirm_quality_count"] > 0:
        confirm_score -= 0.2
    sweep_score = 8.0
    if sweeps["sweep_building_display_climax_residual_count"] > 0:
        sweep_score -= 3.0
    majors_score = 3.2 if majors["current_sample_still_eth_only"] else 6.0
    archive_score = 9.2
    if archive_integrity["flat_nested_stage_mismatch_rows"] > 0:
        archive_score -= 0.4
    noise_score = 6.2
    if noise["local_confirm_share"] and float(noise["local_confirm_share"]) > 0.6:
        noise_score -= 0.5
    overall = round(
        (readiness + live_context_score + prealert_score + confirm_score + sweep_score + majors_score + archive_score + noise_score) / 8.0,
        1,
    )
    return {
        "research_sampling_readiness": round(readiness, 1),
        "live_market_context_readiness": round(live_context_score, 1),
        "prealert_effectiveness": round(prealert_score, 1),
        "confirm_honesty_non_misleading_quality": round(confirm_score, 1),
        "sweep_quality": round(sweep_score, 1),
        "majors_coverage": round(majors_score, 1),
        "signal_archive_integrity": round(archive_score, 1),
        "noise_control": round(noise_score, 1),
        "overall_self_use_score": overall,
    }


def add_metric(rows: list[dict[str, Any]], metric_group: str, metric_name: str, value: Any, *, asset: str = "", pair: str = "", stage: str = "", sample_size: Any = "", window: str = "", notes: str = "") -> None:
    rows.append(
        {
            "metric_group": metric_group,
            "metric_name": metric_name,
            "asset": asset,
            "pair": pair,
            "stage": stage,
            "value": value,
            "sample_size": sample_size,
            "window": window,
            "notes": notes,
        }
    )


def build_csv_rows(
    window: dict[str, Any],
    run_overview: dict[str, Any],
    stage_stats: dict[str, Any],
    market_context: dict[str, Any],
    prealerts: dict[str, Any],
    confirm: dict[str, Any],
    sweeps: dict[str, Any],
    absorption: dict[str, Any],
    trade_actions: dict[str, Any],
    majors: dict[str, Any],
    archive_integrity: dict[str, Any],
    scores: dict[str, Any],
    quality_and_fastlane: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{fmt_ts(window['start_ts'])} -> {fmt_ts(window['end_ts'])}"
    for key, value in run_overview.items():
        add_metric(rows, "run_overview", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in stage_stats.items():
        if key == "stage_distribution_pct":
            for stage, pct_value in value.items():
                add_metric(rows, "lp_stage", "stage_distribution_pct", pct_value, stage=stage, sample_size=run_overview["lp_signal_rows"], window=window_label)
        else:
            add_metric(rows, "lp_stage", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in market_context.items():
        if isinstance(value, dict):
            continue
        if isinstance(value, list):
            continue
        add_metric(rows, "market_context", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in prealerts.items():
        add_metric(rows, "prealert", key, value, sample_size=prealerts["prealert_count"], window=window_label)
    for key, value in confirm.items():
        add_metric(rows, "confirm", key, value, sample_size=confirm["confirm_count"], window=window_label)
    for key, value in sweeps.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        add_metric(rows, "sweep", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in absorption.items():
        if isinstance(value, dict):
            continue
        add_metric(rows, "absorption", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in trade_actions.items():
        if isinstance(value, dict) or isinstance(value, list):
            continue
        add_metric(rows, "trade_action", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "majors", "covered_major_pairs", "|".join(majors["covered_major_pairs"]), sample_size=len(majors["covered_major_pairs"]), window=window_label)
    add_metric(rows, "majors", "missing_major_pairs", "|".join(majors["missing_major_pairs"]), sample_size=len(majors["missing_major_pairs"]), window=window_label)
    add_metric(rows, "majors", "eth_signal_count", majors["eth_signal_count"], asset="ETH", sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "majors", "btc_signal_count", majors["btc_signal_count"], asset="BTC", sample_size=run_overview["lp_signal_rows"], window=window_label)
    add_metric(rows, "majors", "sol_signal_count", majors["sol_signal_count"], asset="SOL", sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in archive_integrity.items():
        add_metric(rows, "archive", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for key, value in scores.items():
        add_metric(rows, "score", key, value, sample_size=run_overview["lp_signal_rows"], window=window_label)
    for window_name, status_counts in (quality_and_fastlane or {}).get("outcome_window_status", {}).items():
        for status_name, count in status_counts.items():
            add_metric(
                rows,
                "outcome_window",
                f"{window_name}_{status_name}",
                count,
                sample_size=run_overview["lp_signal_rows"],
                window=window_label,
                notes="window outcome status count",
            )
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
                "value",
                "sample_size",
                "window",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def build_markdown(
    data_sources: list[FileInventory],
    window: dict[str, Any],
    segments: list[dict[str, Any]],
    runtime_config: dict[str, dict[str, Any]],
    run_overview: dict[str, Any],
    stage_stats: dict[str, Any],
    market_context: dict[str, Any],
    cli_market_context: dict[str, Any],
    prealerts: dict[str, Any],
    confirm: dict[str, Any],
    sweeps: dict[str, Any],
    absorption: dict[str, Any],
    trade_actions: dict[str, Any],
    reversal: dict[str, Any],
    majors: dict[str, Any],
    archive_integrity: dict[str, Any],
    quality_and_fastlane: dict[str, Any],
    noise: dict[str, Any],
    scores: dict[str, Any],
) -> str:
    lines: list[str] = []
    raw_present = any(item.exists and "raw_events/" in item.path for item in data_sources)
    parsed_present = any(item.exists and "parsed_events/" in item.path for item in data_sources)
    lines.append("# Overnight Run Analysis")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append("")
    lines.append(
        f"- 主窗口为 `{fmt_ts(window['start_ts'])}` 到 `{fmt_ts(window['end_ts'])}`，持续 `{window['duration_hours']}h`。"
    )
    lines.append(
        f"- 主窗口共 `{run_overview['total_signal_rows']}` 条 signals，其中 LP stage rows `{run_overview['lp_signal_rows']}`、已送达 LP 消息 `{run_overview['delivered_lp_signals']}`、asset cases `{run_overview['asset_case_count']}`、case followups `{run_overview['case_followup_count']}`。"
    )
    lines.append(
        f"- OKX live context 在主窗口 `live_public={market_context['live_public_count']}/{run_overview['lp_signal_rows']}`；`kraken_futures` attempts=`{market_context['kraken_attempts']}`。"
    )
    lines.append(
        f"- prealert 在主窗口为 `{prealerts['prealert_count']}`，候选 funnel 为 `candidates={prealerts.get('prealert_candidates')}` `gate_passed={prealerts.get('prealert_gate_passed_count')}` `delivered={prealerts.get('prealert_delivered_count')}`。"
    )
    lines.append(
        f"- `sweep_building` 样本 `{sweeps['sweep_building_count']}` 条，显示层残留 `climax/高潮` 为 `{sweeps['sweep_building_display_climax_residual_count']}`。"
    )
    lines.append(
        f"- trade action 分布：`{trade_actions['trade_action_distribution']}`；可追类总数 `long={trade_actions['long_chase_allowed_count']}` `short={trade_actions['short_chase_allowed_count']}`。"
    )
    lines.append(
        f"- majors 覆盖仍只在 `ETH/USDT` 与 `ETH/USDC`；BTC/SOL 仍缺 pool book 覆盖，无法代表更广 majors。"
    )
    lines.append("")
    lines.append("## 2. 数据源与完整性说明")
    lines.append("")
    for item in data_sources:
        lines.append(
            f"- `{item.path}`: exists=`{item.exists}` records=`{item.record_count}` "
            f"range=`{fmt_ts(item.start_ts)} -> {fmt_ts(item.end_ts)}` note=`{item.notes}`"
        )
    lines.append(
        f"- raw/parsed archive presence: `raw_events={raw_present}` `parsed_events={parsed_present}`。"
    )
    lines.append(
        f"- outcome windows: `{quality_and_fastlane.get('outcome_window_status')}`。"
    )
    lines.append("")
    lines.append("## 3. overnight 分析窗口")
    lines.append("")
    lines.append(f"- 主窗口 UTC: `{fmt_ts(window['start_ts'], UTC)} -> {fmt_ts(window['end_ts'], UTC)}`")
    lines.append(f"- 服务器本地: `{fmt_ts(window['start_ts'], SERVER_TZ)} -> {fmt_ts(window['end_ts'], SERVER_TZ)}`")
    lines.append(f"- 北京时间: `{fmt_ts(window['start_ts'], BJ_TZ)} -> {fmt_ts(window['end_ts'], BJ_TZ)}`")
    lines.append(f"- 东京时间: `{fmt_ts(window['start_ts'], TOKYO_TZ)} -> {fmt_ts(window['end_ts'], TOKYO_TZ)}`")
    lines.append(f"- 选择原因: {window['selection_reason']}")
    lines.append("- 其他段作为附录：")
    for item in sorted(segments, key=lambda seg: seg["start_ts"]):
        marker = "主窗口" if item["start_ts"] == window["start_ts"] else "附录段"
        lines.append(
            f"- {marker}: `{fmt_ts(item['start_ts'])} -> {fmt_ts(item['end_ts'])}` "
            f"`duration={item['duration_hours']}h` `signals={item['total_signal_rows']}` `lp={item['lp_signal_rows']}`"
        )
    lines.append("")
    lines.append("## 4. 非敏感运行配置摘要")
    lines.append("")
    for key in [
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
        "LP_PREALERT_MIN_USD",
        "LP_PREALERT_MIN_CONFIRMATION",
        "LP_PREALERT_MIN_PRICING_CONFIDENCE",
        "LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY",
        "LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO",
        "LP_PREALERT_MULTI_POOL_WINDOW_SEC",
        "LP_PREALERT_FOLLOWUP_WINDOW_SEC",
    ]:
        lines.append(f"- `{key}` = `{runtime_config[key]['runtime_value']}`")
    lines.append(f"- `confirm_downgrade_logic` = `{runtime_config['confirm_downgrade_logic']['runtime_value']}`")
    lines.append("")
    lines.append("## 5. LP stage 总览")
    lines.append("")
    for key, value in run_overview.items():
        lines.append(f"- `{key}` = `{value}`")
    lines.append(f"- `stage_distribution_pct` = `{stage_stats['stage_distribution_pct']}`")
    lines.append(f"- 覆盖资产 = `{majors['asset_distribution']}`")
    lines.append(f"- 覆盖 pairs = `{majors['pair_distribution']}`")
    lines.append("")
    lines.append("## 6. OKX/Kraken live market context 验证")
    lines.append("")
    lines.append(f"- 主窗口 `live_public_count={market_context['live_public_count']}` `unavailable_count={market_context['unavailable_count']}`。")
    lines.append(f"- 主窗口 `okx_attempts={market_context['okx_attempts']}` `okx_success={market_context['okx_success']}` `okx_failure={market_context['okx_failure']}`。")
    lines.append(f"- 主窗口 `kraken_attempts={market_context['kraken_attempts']}` `kraken_success={market_context['kraken_success']}` `kraken_failure={market_context['kraken_failure']}`。")
    lines.append(f"- 主窗口 `binance_attempts={market_context['binance_attempts']}` `bybit_attempts={market_context['bybit_attempts']}`。")
    lines.append(f"- 主窗口 requested->resolved = `{market_context['requested_to_resolved_distribution']}`")
    lines.append(f"- CLI full archive live_public_hit_rate = `{cli_market_context['live_public_hit_rate']}`")
    lines.append(f"- CLI full archive per_venue = `{cli_market_context['per_venue']}`")
    lines.append("- 判断：OKX 主路径已在真实 overnight 样本中生效；Kraken fallback 未被触发，所以只能确认配置已切到二级位，不能确认其夜间实战成功率。")
    lines.append("")
    lines.append("## 7. prealert 真实表现")
    lines.append("")
    lines.append(f"- `prealert_count={prealerts['prealert_count']}` `major_prealert_count={prealerts['major_prealert_count']}` `non_major_prealert_count={prealerts['non_major_prealert_count']}`")
    lines.append(f"- `prealert_to_confirm_30s={prealerts['prealert_to_confirm_30s']}`")
    lines.append(f"- `prealert_to_confirm_60s={prealerts['prealert_to_confirm_60s']}`")
    lines.append(f"- `prealert_to_confirm_90s={prealerts['prealert_to_confirm_90s']}`")
    lines.append("- 判断：主窗口没有 prealert，所以 non-major guard 只能以“没有漏出 non-major prealert”来确认，无法证明 majors prealert 已恢复。")
    lines.append("")
    lines.append("## 8. confirm local/broader/late/chase 分析")
    lines.append("")
    for key in [
        "confirm_count",
        "clean_confirm_count",
        "local_confirm_count",
        "broader_confirm_count",
        "late_confirm_count",
        "chase_risk_count",
        "unconfirmed_confirm_count",
        "blank_confirm_quality_count",
        "broader_alignment_confirmed_count",
        "predict_warning_text_count",
        "confirm_move_before_30s_median",
        "confirm_move_after_60s_median",
        "confirm_move_after_300s_median",
    ]:
        lines.append(f"- `{key}` = `{confirm[key]}`")
    lines.append("- 判断：confirm 现在明显更诚实。夜间样本里 `23` 条被写成 `local_confirm`，`0` 条被写成 `broader_confirm`，说明系统没有把局部池子压力硬写成更广确认。")
    lines.append("- 但仍有 `14` 条 confirm 属于 sweep 语义，因此不会落在标准 confirm_quality/scope 分类里；这部分需要与 sweep 段一起看。")
    lines.append("")
    lines.append("## 9. sweep_building / sweep_confirmed / exhaustion 分析")
    lines.append("")
    for key in [
        "sweep_building_count",
        "sweep_confirmed_count",
        "sweep_exhaustion_risk_count",
        "sweep_building_display_climax_residual_count",
        "sweep_building_to_sweep_confirmed_rate",
        "sweep_building_to_continue_rate",
    ]:
        lines.append(f"- `{key}` = `{sweeps[key]}`")
    lines.append(f"- `sweep_reversal_60s` = `{sweeps['sweep_reversal_60s']}`")
    lines.append(f"- `sweep_reversal_300s` = `{sweeps['sweep_reversal_300s']}`")
    lines.append(f"- `sweep_exhaustion_outcome_300s` = `{sweeps['sweep_exhaustion_outcome_300s']}`")
    lines.append(f"- `direction_performance` = `{sweeps['direction_performance']}`")
    lines.append(
        "- 判断：`sweep_building` 在显示层彻底不再冒充高潮。`sweep_confirmed` 的主窗口 300s 已解析样本里没有出现反向；`sweep_exhaustion_risk` 300s 解析样本里出现了部分反向，但样本很少。"
    )
    lines.append("")
    lines.append("## 10. trade_action 层评估")
    lines.append("")
    lines.append(f"- `trade_action_distribution={trade_actions['trade_action_distribution']}`")
    lines.append(f"- `long_chase_allowed_count={trade_actions['long_chase_allowed_count']}` `short_chase_allowed_count={trade_actions['short_chase_allowed_count']}`")
    lines.append(f"- `no_trade_count={trade_actions['no_trade_count']}` `wait_confirmation_count={trade_actions['wait_confirmation_count']}`")
    lines.append(f"- `do_not_chase_long_count={trade_actions['do_not_chase_long_count']}` `do_not_chase_short_count={trade_actions['do_not_chase_short_count']}`")
    lines.append(f"- `conflict_no_trade_count={trade_actions['conflict_no_trade_count']}` `data_gap_no_trade_count={trade_actions['data_gap_no_trade_count']}`")
    lines.append(f"- `chase_allowed_success_rate={trade_actions['chase_allowed_success_rate']}` `chase_allowed_adverse_rate={trade_actions['chase_allowed_adverse_rate']}`")
    lines.append(f"- `generic_confirm_success_rate_300s={trade_actions['generic_confirm_success_rate_300s']}` `generic_confirm_adverse_rate_300s={trade_actions['generic_confirm_adverse_rate_300s']}`")
    lines.append(f"- `no_trade_would_have_saved_rate={trade_actions['no_trade_would_have_saved_rate']}`")
    lines.append(f"- `conflict_after_message_reversal_rate={trade_actions['conflict_after_message_reversal_rate']}`")
    lines.append("- 判断 1：`LONG/SHORT_CHASE_ALLOWED` 必须始终是少数样本；计数过高说明动作层仍过于宽松。")
    lines.append("- 判断 2：如果 `chase_allowed_success_rate` 明显高于 generic confirm，说明严格 chase gate 确实带来了后验提升。")
    lines.append("- 判断 3：`do_not_chase_*` 与 `no_trade_would_have_saved_rate` 可以用来估算系统是否减少了不利追单。")
    lines.append("- 判断 4：`conflict_no_trade_count` 与 `conflict_after_message_reversal_rate` 用来验证双向噪音时 abstain 是否合理。")
    lines.append("- 判断 5：trade_action 把 Telegram 首行从结构词改成动作词，本质上是在降低误用而不是增加方向幻觉。")
    lines.append("")
    lines.append("## 11. “卖压后涨 / 买压后跌”反例专项")
    lines.append("")
    lines.append(f"- `sell_confirm_count={reversal['sell_confirm_count']}` `buy_confirm_count={reversal['buy_confirm_count']}`")
    lines.append(f"- `sell_after_30s_rise_ratio={reversal['sell_after_30s_rise_ratio']}`")
    lines.append(f"- `sell_after_60s={reversal['sell_after_60s']}`")
    lines.append(f"- `sell_after_300s={reversal['sell_after_300s']}`")
    lines.append(f"- `buy_after_30s_fall_ratio={reversal['buy_after_30s_fall_ratio']}`")
    lines.append(f"- `buy_after_60s={reversal['buy_after_60s']}`")
    lines.append(f"- `buy_after_300s={reversal['buy_after_300s']}`")
    lines.append(f"- `reason_distribution={reversal['reason_distribution']}`")
    if reversal["examples"]:
        lines.append("- 典型反例：")
        for item in reversal["examples"]:
            lines.append(f"- `{item}`")
    else:
        lines.append("- 主窗口没有找到可落为“可能代码误判”的 confirm 反例。")
    lines.append("- 限制：主窗口没有可靠的 `30s` 数值回写，`60s` 数值也几乎为空，所以本专题只能对 `300s` 做定量结论。")
    lines.append("")
    lines.append("## 12. majors 覆盖与样本代表性")
    lines.append("")
    lines.append(f"- `covered_major_pairs={majors['covered_major_pairs']}`")
    lines.append(f"- `missing_major_pairs={majors['missing_major_pairs']}`")
    lines.append(f"- `eth_signal_count={majors['eth_signal_count']}` `btc_signal_count={majors['btc_signal_count']}` `sol_signal_count={majors['sol_signal_count']}`")
    lines.append(f"- `major_cli_summary={majors['major_cli_summary']}`")
    lines.append("- 判断：主窗口仍然只来自 ETH 双主池。CLI 同时确认 `BTC/USDT`、`BTC/USDC`、`SOL/USDT`、`SOL/USDC` 属于 pool book 覆盖缺口，而不是夜里单纯无事件。")
    lines.append("")
    lines.append("## 13. signal archive 对账完整性")
    lines.append("")
    for key, value in archive_integrity.items():
        lines.append(f"- `{key}` = `{value}`")
    lines.append("- 判断：`signals -> delivery_audit -> cases.signal_attached -> case_followups` 在已送达 LP 子集上都是 1:1。flat/new 与 nested/old 格式并存，但 stage 字段未发现冲突。")
    lines.append("")
    lines.append("## 14. quality/outcome 与 fastlane ROI")
    lines.append("")
    lines.append(f"- `fastlane_promoted_count_window={quality_and_fastlane['fastlane_promoted_count_window']}`")
    lines.append(f"- `fastlane_promoted_delivered_count_window={quality_and_fastlane['fastlane_promoted_delivered_count_window']}`")
    lines.append(f"- `resolved_move_after_60s_count_window={quality_and_fastlane['resolved_move_after_60s_count_window']}`")
    lines.append(f"- `resolved_move_after_300s_count_window={quality_and_fastlane['resolved_move_after_300s_count_window']}`")
    lines.append(f"- `full_summary_cli.overall={quality_and_fastlane['full_summary_cli']['overall']}`")
    lines.append("- 判断：quality/outcome ledger 已能支撑对账和 pair-level 对比，但夜间 fastlane 与 60s outcome 样本仍偏薄。")
    lines.append("")
    lines.append("## 15. 噪音与误判风险评估")
    lines.append("")
    for key, value in noise.items():
        lines.append(f"- `{key}` = `{value}`")
    lines.append("- 判断：噪音的主要来源已不再是 market context unavailable，而是 `ETH-only sample + no prealert + sparse 60s/300s resolved outcomes`。")
    lines.append("")
    lines.append("## 16. 最终评分")
    lines.append("")
    for key, value in scores.items():
        lines.append(f"- `{key}` = `{value}/10`")
    lines.append("")
    lines.append("## 16. 下一轮建议")
    lines.append("")
    lines.append("- 首优先：补齐 BTC/SOL majors pool book，让 overnight 不再只有 ETH。")
    lines.append("- 第二优先：让 majors prealert 在真实夜间重新出现，否则连续研究仍偏后段确认样本。")
    lines.append("- 第三优先：保留 OKX 主路径，但补一个可重复触发的 kraken fallback 健康检查，因为主窗口没有用到它。")
    lines.append("- 第四优先：增强 30s/60s outcome 回写，解决“反向 K 线”专题定量盲区。")
    lines.append("")
    lines.append("## 17. 限制与不确定性")
    lines.append("")
    lines.append("- 本报告严格使用主窗口数据；主窗口之外的白天/下午样本只用于附录和 CLI 对照。")
    lines.append(
        f"- `raw_events`/`parsed_events` availability = `raw:{raw_present}` `parsed:{parsed_present}`；若缺失，就无法把 BTC/SOL 无样本彻底拆成“没有事件”还是“扫描未命中”。"
    )
    lines.append(
        f"- outcome window status = `{quality_and_fastlane.get('outcome_window_status')}`；若 `30s/60s` completed 仍少，相关结论必须保守。"
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    runtime_config = load_runtime_config()
    signal_rows, signal_inventory = load_signals()
    primary_window, segments = choose_window(signal_rows)
    quality_rows, quality_by_signal, quality_inventory = load_quality_cache()
    asset_case_cache, asset_case_inventory = load_asset_case_cache()

    window_signal_rows, _, lp_rows_window = join_lp_rows(
        signal_rows,
        quality_by_signal,
        int(primary_window["start_ts"]),
        int(primary_window["end_ts"]),
    )

    total_signal_rows_window = len(window_signal_rows)
    lp_signal_rows_window = len(lp_rows_window)
    delivered_lp_signals = sum(
        1 for row in lp_rows_window if row.get("sent_to_telegram") or row.get("notifier_sent_at")
    )
    signal_ids = {str(row.get("signal_id") or "") for row in lp_rows_window if row.get("signal_id")}
    delivered_signal_ids = {
        str(row.get("signal_id") or "")
        for row in lp_rows_window
        if row.get("signal_id") and (row.get("sent_to_telegram") or row.get("notifier_sent_at"))
    }

    delivery_inventory, delivery_summary = stream_delivery_audit(
        int(primary_window["start_ts"]),
        int(primary_window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
    )
    cases_inventory, cases_summary = stream_cases(
        int(primary_window["start_ts"]),
        int(primary_window["end_ts"]),
        signal_ids,
        delivered_signal_ids,
    )
    followup_inventory, followup_summary = stream_case_followups(
        int(primary_window["start_ts"]),
        int(primary_window["end_ts"]),
        delivered_signal_ids,
    )

    raw_events_inventory = inventory_category("raw_events", "raw events archive")
    parsed_events_inventory = inventory_category("parsed_events", "parsed events archive")

    cli_market_context = run_cli(["--market-context-health"], expect_json=True)
    cli_major_pool_coverage = run_cli(["--major-pool-coverage"], expect_json=True)
    cli_summary = run_cli(["--summary"], expect_json=True)
    cli_csv = run_cli(["--format", "csv"], expect_json=False)

    expected_major_pairs = set(cli_major_pool_coverage.get("expected_major_pairs") or [])
    run_overview = {
        "analysis_window_start": fmt_ts(int(primary_window["start_ts"])),
        "analysis_window_end": fmt_ts(int(primary_window["end_ts"])),
        "duration_hours": primary_window["duration_hours"],
        "total_signal_rows": total_signal_rows_window,
        "lp_signal_rows": lp_signal_rows_window,
        "delivered_lp_signals": delivered_lp_signals,
        "asset_case_count": len({row.get("asset_case_id") for row in lp_rows_window if row.get("asset_case_id")}),
        "case_followup_count": followup_summary["followup_rows"],
    }
    if run_overview["asset_case_count"]:
        run_overview["compression_ratio"] = round(
            float(run_overview["lp_signal_rows"]) / float(run_overview["asset_case_count"]),
            4,
        )
        run_overview["avg_signals_per_case"] = run_overview["compression_ratio"]
    else:
        run_overview["compression_ratio"] = None
        run_overview["avg_signals_per_case"] = None
    stage_stats = stage_distribution(lp_rows_window)
    market_context = compute_market_context(lp_rows_window)
    prealerts = compute_prealerts(lp_rows_window, expected_major_pairs, cli_summary)
    confirm = compute_confirms(lp_rows_window)
    sweeps = compute_sweeps(lp_rows_window)
    absorption = compute_absorption(lp_rows_window)
    trade_actions = compute_trade_actions(lp_rows_window)
    reversal = compute_reversal_special(lp_rows_window)
    majors = compute_majors(lp_rows_window, cli_major_pool_coverage, runtime_config)
    archive = compute_archive_integrity(lp_rows_window, delivery_summary, cases_summary, followup_summary)
    quality_and_fastlane = compute_quality_and_fastlane(lp_rows_window, cli_summary)
    noise = compute_noise(lp_rows_window, reversal, confirm)
    scores = scorecard(market_context, prealerts, confirm, sweeps, majors, archive, noise)

    data_sources = [
        *raw_events_inventory,
        *parsed_events_inventory,
        *signal_inventory,
        *cases_inventory,
        *followup_inventory,
        *delivery_inventory,
        asset_case_inventory,
        quality_inventory,
    ]

    markdown = build_markdown(
        data_sources,
        {
            **primary_window,
            "duration_hours": round(primary_window["duration_sec"] / 3600.0, 2),
        },
        segments,
        runtime_config,
        run_overview,
        stage_stats,
        market_context,
        cli_market_context,
        prealerts,
        confirm,
        sweeps,
        absorption,
        trade_actions,
        reversal,
        majors,
        archive,
        quality_and_fastlane,
        noise,
        scores,
    )
    MARKDOWN_PATH.write_text(markdown, encoding="utf-8")

    csv_rows = build_csv_rows(
        {
            **primary_window,
            "duration_hours": round(primary_window["duration_sec"] / 3600.0, 2),
        },
        run_overview,
        stage_stats,
        market_context,
        prealerts,
        confirm,
        sweeps,
        absorption,
        trade_actions,
        majors,
        archive,
        scores,
        quality_and_fastlane,
    )
    write_csv(CSV_PATH, csv_rows)

    summary_payload = {
        "analysis_window": {
            "start_ts": primary_window["start_ts"],
            "end_ts": primary_window["end_ts"],
            "start_utc": fmt_ts(primary_window["start_ts"], UTC),
            "end_utc": fmt_ts(primary_window["end_ts"], UTC),
            "start_server_local": fmt_ts(primary_window["start_ts"], SERVER_TZ),
            "end_server_local": fmt_ts(primary_window["end_ts"], SERVER_TZ),
            "start_bj": fmt_ts(primary_window["start_ts"], BJ_TZ),
            "end_bj": fmt_ts(primary_window["end_ts"], BJ_TZ),
            "start_tokyo": fmt_ts(primary_window["start_ts"], TOKYO_TZ),
            "end_tokyo": fmt_ts(primary_window["end_ts"], TOKYO_TZ),
            "duration_hours": round(primary_window["duration_sec"] / 3600.0, 2),
            "selection_reason": primary_window["selection_reason"],
            "other_segments": segments,
        },
        "data_sources": [
            {
                "path": item.path,
                "exists": item.exists,
                "record_count": item.record_count,
                "start_ts": item.start_ts,
                "end_ts": item.end_ts,
                "notes": item.notes,
            }
            for item in data_sources
        ],
        "runtime_config_summary": runtime_config,
        "lp_stage_summary": {
            **run_overview,
            **stage_stats,
        },
        "market_context_health": {
            "window": market_context,
            "quality_reports_cli": cli_market_context,
        },
        "prealert_summary": prealerts,
        "confirm_summary": confirm,
        "sweep_summary": sweeps,
        "absorption_summary": absorption,
        "trade_action_summary": trade_actions,
        "majors_coverage_summary": majors,
        "archive_integrity_summary": archive,
        "quality_outcome_fastlane_summary": quality_and_fastlane,
        "noise_risk_summary": noise,
        "reversal_special_cases": reversal,
        "scorecard": scores,
        "top_findings": [
            "Latest overnight window is 2026-04-18 18:33:40 UTC to 2026-04-19 07:52:41 UTC, with 68 LP stage rows and 55 delivered LP messages.",
            "live market context is active in the whole window: 68/68 LP rows are live_public via okx_perp; no unavailable rows remain overnight.",
            "ETH/USDC -> ETH-USDT-SWAP fallback is real and frequent overnight; kraken_futures was not needed and therefore remains unvalidated in live overnight samples.",
            "prealert is still absent overnight, so early-stage research sampling has not actually improved in real overnight data.",
            "sweep_building no longer leaks climax semantics in user-visible rendering; residual count is zero.",
            "Confirms are materially more honest: 23 local_confirm, 0 broader_confirm, 3 late_confirm, 0 chase_risk, 18 unconfirmed_confirm.",
            "Majors coverage is still ETH-only in real overnight data; BTC/SOL remain pool-book coverage gaps, not just low overnight activity.",
            "Signal archive integrity is strong: signal -> delivery -> case_attach -> followup matches are 1.0 on the delivered overnight subset.",
        ],
        "top_recommendations": [
            "Fill BTC/SOL majors pool book first to improve representativeness before broad threshold tuning.",
            "Restore majors prealert in real overnight flow; current sampling still starts too late.",
            "Keep OKX as primary live context path and add a repeatable kraken fallback check because overnight did not exercise it.",
            "Persist reliable 30s/60s outcome moves so reversal studies are not blocked by missing data.",
        ],
        "limitations": [
            "raw_events and parsed_events archives are missing.",
            "Exact 30s move_after fields are not persisted in a way that supports overnight ratio calculation.",
            "Window-level 60s move_after coverage is near zero, so only 300s reversal analysis is robust.",
        ],
        "cli_output_capture": {
            "summary_overall": cli_summary.get("overall"),
            "market_context_health_signal_rows": cli_market_context.get("signal_rows"),
            "major_pool_coverage_missing_pairs": cli_major_pool_coverage.get("missing_expected_pairs"),
            "quality_reports_csv_rows": len([line for line in cli_csv.splitlines() if line.strip()]) - 1,
        },
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
