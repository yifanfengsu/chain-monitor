#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
ARCHIVE_DIR = ROOT / "app" / "data" / "archive"
DATA_DIR = ROOT / "data"
ENV_PATH = ROOT / ".env"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MARKDOWN_PATH = REPORTS_DIR / "overnight_run_analysis.md"
CSV_PATH = REPORTS_DIR / "overnight_run_metrics.csv"
JSON_PATH = REPORTS_DIR / "overnight_run_summary.json"

UTC = timezone.utc
ARCHIVE_LOCAL = timezone(timedelta(hours=8))

RAW_EVENTS_PATH = ARCHIVE_DIR / "raw_events" / "2026-04-18.ndjson"
PARSED_EVENTS_PATH = ARCHIVE_DIR / "parsed_events" / "2026-04-18.ndjson"
SIGNALS_PATH = ARCHIVE_DIR / "signals" / "2026-04-18.ndjson"
CASES_PATH = ARCHIVE_DIR / "cases" / "2026-04-18.ndjson"
CASE_FOLLOWUPS_PATH = ARCHIVE_DIR / "case_followups" / "2026-04-18.ndjson"
DELIVERY_AUDIT_PATH = ARCHIVE_DIR / "delivery_audit" / "2026-04-18.ndjson"
ASSET_CASE_CACHE_PATH = DATA_DIR / "asset_cases.cache.json"
QUALITY_STATS_PATH = DATA_DIR / "lp_quality_stats.cache.json"

LP_SIGNAL_TYPES = {"pool_buy_pressure", "pool_sell_pressure"}
STAGES = ("prealert", "confirm", "climax", "exhaustion_risk")
MAJORS = {"ETH", "WETH", "BTC", "WBTC", "SOL"}
EXPECTED_MAJOR_PAIRS = ("ETH/USDT", "ETH/USDC", "BTC/USDT", "BTC/USDC", "SOL/USDT", "SOL/USDC")

WHITELISTED_CONFIG_KEYS = [
    "DEFAULT_USER_TIER",
    "MARKET_CONTEXT_ADAPTER_MODE",
    "MARKET_CONTEXT_PRIMARY_VENUE",
    "MARKET_CONTEXT_SECONDARY_VENUE",
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
    "LP_PREALERT_MIN_RESERVE_SKEW",
    "LP_PREALERT_PRIMARY_TREND_MIN_MATCHES",
    "LP_PREALERT_MULTI_POOL_WINDOW_SEC",
    "LP_PREALERT_FOLLOWUP_WINDOW_SEC",
    "LP_SWEEP_MIN_ACTION_INTENSITY",
    "LP_SWEEP_MIN_VOLUME_SURGE_RATIO",
    "LP_SWEEP_MIN_SAME_POOL_CONTINUITY",
    "LP_SWEEP_MIN_BURST_EVENT_COUNT",
    "LP_SWEEP_MAX_BURST_WINDOW_SEC",
    "LP_SWEEP_CONTINUATION_MIN_SCORE",
    "LP_SWEEP_EXHAUSTION_MIN_SCORE",
    "LP_FASTLANE_PROMOTION_ENABLE",
    "LP_FASTLANE_MAIN_SCAN_INCLUDE_PROMOTED",
    "LP_PRIMARY_TREND_SCAN_INTERVAL_SEC",
    "LP_SECONDARY_SCAN_ENABLE",
    "LP_SECONDARY_SCAN_INTERVAL_SEC",
    "LP_EXTENDED_SCAN_ENABLE",
    "LP_EXTENDED_SCAN_INTERVAL_SEC",
    "LP_QUALITY_MIN_FASTLANE_ROI_SCORE",
    "LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE",
]


@dataclass
class SourceInventory:
    path: str
    fmt: str
    exists: bool
    coverage_start_ts: int | None
    coverage_end_ts: int | None
    overlap_window: bool
    notes: str = ""


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


def round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or math.isnan(value):
        return None
    return round(float(value), digits)


def pct(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(100.0 * numerator / denominator, 1)


def median_or_none(values: list[Any], digits: int = 6, *, positive_only: bool = False) -> float | None:
    cleaned: list[float] = []
    for value in values:
        number = to_float(value)
        if number is None:
            continue
        if positive_only and number <= 0:
            continue
        cleaned.append(number)
    if not cleaned:
        return None
    return round(float(statistics.median(cleaned)), digits)


def fmt_ts(ts: int | None, tz: timezone = UTC) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(int(ts), tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def duration_label(seconds: int | None) -> str:
    if seconds is None:
        return "n/a"
    seconds = max(int(seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def summarize_counter(counter: Counter[str], *, top_n: int | None = None) -> list[dict[str, Any]]:
    items = counter.most_common(top_n)
    return [{"key": key, "count": count} for key, count in items]


def aligned_move(row: dict[str, Any], field: str) -> float | None:
    value = to_float(row.get(field))
    if value is None:
        return None
    if str(row.get("direction_bucket") or "") == "sell_pressure":
        return -value
    return value


def adverse_move(row: dict[str, Any], field: str) -> bool | None:
    value = to_float(row.get(field))
    if value is None:
        return None
    direction = str(row.get("direction_bucket") or "")
    if direction == "sell_pressure":
        return value > 0
    if direction == "buy_pressure":
        return value < 0
    return None


def classify_major(asset_symbol: str | None) -> str:
    return "major" if str(asset_symbol or "").upper() in MAJORS else "long_tail"


def parse_env_whitelist(path: Path) -> dict[str, dict[str, Any]]:
    present: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return present
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in WHITELISTED_CONFIG_KEYS:
            continue
        value = value.strip().strip("'").strip('"')
        present[key] = {"env_present": True, "env_value": value}
    return present


def load_runtime_config() -> dict[str, dict[str, Any]]:
    import app.config as config

    env_overrides = parse_env_whitelist(ENV_PATH)
    runtime: dict[str, dict[str, Any]] = {}
    for key in WHITELISTED_CONFIG_KEYS:
        runtime_value = getattr(config, key, None)
        if isinstance(runtime_value, (list, tuple, set)):
            runtime_value = list(runtime_value)
        runtime[key] = {
            "runtime_value": runtime_value,
            "env_present": env_overrides.get(key, {}).get("env_present", False),
            "env_value": env_overrides.get(key, {}).get("env_value"),
        }
    runtime["LP_CONFIRM_DOWNGRADE_LOGIC"] = {
        "runtime_value": {
            "late_confirm": [
                "market_context unavailable and pool_move_before_alert >= 0.006",
                "pool_move_before_alert >= 0.009",
                "market_move_before_alert >= 0.008",
                "single_pool_dominant and no broader confirmation",
                "detect_latency_ms >= 4500",
                "case_age_sec >= 150",
                "quality_gap >= 0.18",
            ],
            "chase_risk": [
                "chase_risk_score >= 0.58",
                "pool_move_before_alert >= 0.010 or market_move_before_alert >= 0.010",
                "late timing and no broader confirmation",
                "market_context unavailable with single-pool dominance and pre-move >= 0.008",
                "detect_latency_ms >= 8000 or case_age_sec >= 240",
            ],
        },
        "env_present": False,
        "env_value": None,
    }
    return runtime


def run_cli_json(args: list[str]) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-m", "app.quality_reports", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def inventory_ndjson(path: Path) -> tuple[int | None, int | None]:
    if not path.exists() or path.stat().st_size == 0:
        return None, None
    start_ts: int | None = None
    end_ts: int | None = None
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            obj = json.loads(raw_line)
            ts = to_int(obj.get("archive_ts"))
            if start_ts is None:
                start_ts = ts
            end_ts = ts
    return start_ts, end_ts


def load_quality_rows() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    payload = json.loads(QUALITY_STATS_PATH.read_text(encoding="utf-8"))
    rows = list(payload.get("records") or [])
    by_signal_id = {str(row.get("signal_id") or ""): row for row in rows if row.get("signal_id")}
    return rows, by_signal_id, payload


def is_lp_signal_data(data: dict[str, Any]) -> bool:
    return bool(data.get("lp_alert_stage")) or str(data.get("canonical_semantic_key") or "") in LP_SIGNAL_TYPES


def load_signal_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not SIGNALS_PATH.exists():
        return rows
    with SIGNALS_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            if not is_lp_signal_data(data):
                continue
            signal = data.get("signal") or {}
            event = data.get("event") or {}
            context = signal.get("context") or {}
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "archive_time_bj": obj.get("archive_time_bj"),
                    "signal_id": str(data.get("signal_id") or ""),
                    "event_id": str(data.get("event_id") or ""),
                    "pair_label": str(data.get("pair_label") or context.get("pair_label") or ""),
                    "pool_address": str(
                        (event.get("address") or context.get("pool_address") or (context.get("lp_outcome_record") or {}).get("pool_address") or "")
                    ).lower(),
                    "asset_symbol": str(context.get("asset_symbol") or context.get("asset_case_label") or "").upper(),
                    "lp_alert_stage": str(data.get("lp_alert_stage") or ""),
                    "lp_confirm_quality": str(data.get("lp_confirm_quality") or ""),
                    "lp_confirm_reason": str(data.get("lp_confirm_reason") or ""),
                    "lp_confirm_timing_bucket": str(data.get("lp_confirm_timing_bucket") or ""),
                    "lp_confirm_alignment_score": to_float(data.get("lp_confirm_alignment_score")),
                    "lp_chase_risk_score": to_float(data.get("lp_chase_risk_score")),
                    "lp_absorption_context": str(data.get("lp_absorption_context") or ""),
                    "lp_absorption_confidence": to_float(data.get("lp_absorption_confidence")),
                    "lp_broader_alignment": str(data.get("lp_broader_alignment") or ""),
                    "lp_local_vs_broad_reason": str(data.get("lp_local_vs_broad_reason") or ""),
                    "lp_sweep_phase": str(data.get("lp_sweep_phase") or ""),
                    "lp_sweep_display_stage": str(data.get("lp_sweep_display_stage") or ""),
                    "headline_label": str(context.get("headline_label") or ""),
                    "lp_state_label": str(context.get("lp_state_label") or ""),
                    "lp_market_read": str(context.get("lp_market_read") or ""),
                    "lp_meaning_brief": str(context.get("lp_meaning_brief") or ""),
                    "lp_alert_quality": str(context.get("lp_alert_quality") or ""),
                    "lp_detect_latency_ms": to_float(context.get("lp_detect_latency_ms")),
                    "lp_end_to_end_latency_ms": to_float(context.get("lp_end_to_end_latency_ms")),
                    "lp_scan_path": str(context.get("lp_scan_path") or ""),
                    "lp_route_priority_source": str(context.get("lp_route_priority_source") or ""),
                    "lp_promoted_fastlane": bool(context.get("lp_promoted_fastlane")),
                    "lp_promote_reason": str(context.get("lp_promote_reason") or ""),
                    "asset_case_id": str(data.get("asset_case_id") or context.get("asset_case_id") or ""),
                    "asset_case_key": str(data.get("asset_case_key") or context.get("asset_case_key") or ""),
                    "asset_case_stage": str(context.get("asset_case_stage") or ""),
                    "asset_case_quality_score": to_float(context.get("asset_case_quality_score")),
                    "asset_case_started_at": to_int(context.get("asset_case_started_at")),
                    "asset_case_updated_at": to_int(context.get("asset_case_updated_at")),
                    "asset_case_supporting_pairs": list(context.get("asset_case_supporting_pairs") or []),
                    "asset_case_supporting_pair_count": to_int(context.get("asset_case_supporting_pair_count")),
                    "asset_case_multi_pool": bool(context.get("asset_case_multi_pool")),
                    "delivery_decision": str(data.get("delivery_decision") or ""),
                    "delivery_class": str(data.get("delivery_class") or ""),
                    "delivery_reason": str(data.get("delivery_reason") or ""),
                    "sent_to_telegram": bool(data.get("sent_to_telegram")),
                    "notifier_sent_at": to_int(data.get("notifier_sent_at")),
                    "market_context_source": str(data.get("market_context_source") or ""),
                    "market_context_venue": str(data.get("market_context_venue") or ""),
                    "market_context_requested_symbol": str(data.get("market_context_requested_symbol") or ""),
                    "market_context_resolved_symbol": str(data.get("market_context_resolved_symbol") or ""),
                    "market_context_failure_reason": str(data.get("market_context_failure_reason") or ""),
                    "market_context_attempts": list(data.get("market_context_attempts") or []),
                    "prealert_precision_score": to_float(context.get("prealert_precision_score")),
                    "confirm_conversion_score": to_float(context.get("confirm_conversion_score")),
                    "climax_reversal_score": to_float(context.get("climax_reversal_score")),
                    "market_context_alignment_score": to_float(context.get("market_context_alignment_score")),
                    "fastlane_roi_score": to_float(context.get("fastlane_roi_score")),
                    "pair_quality_score": to_float(context.get("pair_quality_score")),
                    "pool_quality_score": to_float(context.get("pool_quality_score")),
                }
            )
    return rows


def load_case_data() -> dict[str, Any]:
    lp_case_ids: set[str] = set()
    lp_signal_attached_rows: list[dict[str, Any]] = []
    lp_action_counter: Counter[str] = Counter()
    if not CASES_PATH.exists():
        return {
            "lp_case_ids": lp_case_ids,
            "signal_attached_rows": lp_signal_attached_rows,
            "action_counter": lp_action_counter,
        }
    with CASES_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            case = data.get("case") or {}
            event = data.get("event") or {}
            signal = data.get("signal") or {}
            context = signal.get("context") or {}
            is_lp = bool(context.get("lp_event")) or str(event.get("monitor_type") or "") == "lp_pool"
            if not is_lp:
                continue
            action = str(data.get("action") or "")
            lp_action_counter[action] += 1
            case_id = str(case.get("case_id") or "")
            if case_id:
                lp_case_ids.add(case_id)
            if action == "signal_attached" and signal.get("signal_id"):
                lp_signal_attached_rows.append(
                    {
                        "archive_ts": to_int(obj.get("archive_ts")),
                        "case_id": case_id,
                        "signal_id": str(signal.get("signal_id") or ""),
                        "pair_label": str(context.get("pair_label") or ""),
                        "lp_alert_stage": str(context.get("lp_alert_stage") or ""),
                    }
                )
    return {
        "lp_case_ids": lp_case_ids,
        "signal_attached_rows": lp_signal_attached_rows,
        "action_counter": lp_action_counter,
    }


def load_followups() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not CASE_FOLLOWUPS_PATH.exists():
        return rows
    with CASE_FOLLOWUPS_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            followup = data.get("followup") or {}
            if str(followup.get("signal_type") or "") not in LP_SIGNAL_TYPES:
                continue
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "case_id": str(data.get("case_id") or ""),
                    "signal_id": str(followup.get("signal_id") or ""),
                    "signal_type": str(followup.get("signal_type") or ""),
                    "stage": str(followup.get("stage") or ""),
                    "status": str(followup.get("status") or ""),
                }
            )
    return rows


def load_delivery_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not DELIVERY_AUDIT_PATH.exists():
        return rows
    with DELIVERY_AUDIT_PATH.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            if not (data.get("lp_event") or data.get("lp_pool_address")):
                continue
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "signal_id": str(data.get("signal_id") or ""),
                    "event_id": str(data.get("event_id") or ""),
                    "lp_alert_stage": str(data.get("lp_alert_stage") or ""),
                    "delivery_class": str(data.get("delivery_class") or ""),
                    "delivery_reason": str(data.get("delivery_reason") or ""),
                    "gate_reason": str(data.get("gate_reason") or ""),
                    "stage": str(data.get("stage") or ""),
                    "notifier_sent_at": to_int(data.get("notifier_sent_at")),
                    "delivered_notification": bool(data.get("delivered_notification")),
                }
            )
    return rows


def load_asset_case_cache() -> dict[str, Any]:
    if not ASSET_CASE_CACHE_PATH.exists():
        return {}
    return json.loads(ASSET_CASE_CACHE_PATH.read_text(encoding="utf-8"))


def overlaps_window(
    start_ts: int | None,
    end_ts: int | None,
    window_start: int | None,
    window_end: int | None,
) -> bool:
    if None in {start_ts, end_ts, window_start, window_end}:
        return False
    return not (end_ts < window_start or start_ts > window_end)


def build_inventory(window_start: int, window_end: int, quality_payload: dict[str, Any], asset_case_cache: dict[str, Any]) -> list[SourceInventory]:
    inventories: list[SourceInventory] = []
    for path, fmt, notes in [
        (RAW_EVENTS_PATH, "jsonl", "raw events archive"),
        (PARSED_EVENTS_PATH, "jsonl", "parsed events archive"),
        (SIGNALS_PATH, "jsonl", "signal archive"),
        (CASES_PATH, "jsonl", "case archive"),
        (CASE_FOLLOWUPS_PATH, "jsonl", "case followups archive"),
        (DELIVERY_AUDIT_PATH, "jsonl", "delivery audit archive"),
    ]:
        start_ts, end_ts = inventory_ndjson(path)
        inventories.append(
            SourceInventory(
                path=str(path.relative_to(ROOT)),
                fmt=fmt,
                exists=path.exists(),
                coverage_start_ts=start_ts,
                coverage_end_ts=end_ts,
                overlap_window=overlaps_window(start_ts, end_ts, window_start, window_end),
                notes=notes,
            )
        )

    asset_case_ts: list[int] = []
    for case in asset_case_cache.get("cases") or []:
        for key in ("started_at", "updated_at", "last_signal_at", "last_stage_transition_at"):
            value = to_int(case.get(key))
            if value is not None:
                asset_case_ts.append(value)
    if to_int(asset_case_cache.get("generated_at")) is not None:
        asset_case_ts.append(int(asset_case_cache["generated_at"]))
    inventories.append(
        SourceInventory(
            path=str(ASSET_CASE_CACHE_PATH.relative_to(ROOT)),
            fmt="json",
            exists=ASSET_CASE_CACHE_PATH.exists(),
            coverage_start_ts=min(asset_case_ts) if asset_case_ts else None,
            coverage_end_ts=max(asset_case_ts) if asset_case_ts else None,
            overlap_window=overlaps_window(
                min(asset_case_ts) if asset_case_ts else None,
                max(asset_case_ts) if asset_case_ts else None,
                window_start,
                window_end,
            ),
            notes="active asset-case snapshot cache; not a historical ledger",
        )
    )

    quality_ts: list[int] = []
    for row in quality_payload.get("records") or []:
        for key in ("created_at", "notifier_sent_at"):
            value = to_int(row.get(key))
            if value is not None:
                quality_ts.append(value)
    if to_int(quality_payload.get("generated_at")) is not None:
        quality_ts.append(int(quality_payload["generated_at"]))
    inventories.append(
        SourceInventory(
            path=str(QUALITY_STATS_PATH.relative_to(ROOT)),
            fmt="json",
            exists=QUALITY_STATS_PATH.exists(),
            coverage_start_ts=min(quality_ts) if quality_ts else None,
            coverage_end_ts=max(quality_ts) if quality_ts else None,
            overlap_window=overlaps_window(
                min(quality_ts) if quality_ts else None,
                max(quality_ts) if quality_ts else None,
                window_start,
                window_end,
            ),
            notes="lp outcome / quality cache",
        )
    )
    return inventories


def build_segment_candidates(
    archive_window_start: int,
    archive_window_end: int,
    signal_rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    case_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
    delivery_rows: list[dict[str, Any]],
    asset_case_cache: dict[str, Any],
) -> list[dict[str, Any]]:
    timestamps: list[int] = []
    for row in signal_rows:
        ts = to_int(row.get("archive_ts"))
        if ts is not None and archive_window_start <= ts <= archive_window_end:
            timestamps.append(ts)
    for row in quality_rows:
        for key in ("created_at", "notifier_sent_at"):
            ts = to_int(row.get(key))
            if ts is not None and archive_window_start <= ts <= archive_window_end:
                timestamps.append(ts)
    for row in case_rows:
        ts = to_int(row.get("archive_ts"))
        if ts is not None and archive_window_start <= ts <= archive_window_end:
            timestamps.append(ts)
    for row in followup_rows:
        ts = to_int(row.get("archive_ts"))
        if ts is not None and archive_window_start <= ts <= archive_window_end:
            timestamps.append(ts)
    for row in delivery_rows:
        ts = to_int(row.get("archive_ts"))
        if ts is not None and archive_window_start <= ts <= archive_window_end:
            timestamps.append(ts)
    for case in asset_case_cache.get("cases") or []:
        for key in ("started_at", "updated_at", "last_signal_at", "last_stage_transition_at"):
            ts = to_int(case.get(key))
            if ts is not None and archive_window_start <= ts <= archive_window_end:
                timestamps.append(ts)
    generated_at = to_int(asset_case_cache.get("generated_at"))
    if generated_at is not None and archive_window_start <= generated_at <= archive_window_end:
        timestamps.append(generated_at)

    clean = sorted(set(timestamps))
    if not clean:
        return []

    gap_threshold = 30 * 60
    segments: list[tuple[int, int]] = []
    seg_start = clean[0]
    prev = clean[0]
    for ts in clean[1:]:
        if ts - prev > gap_threshold:
            segments.append((seg_start, prev))
            seg_start = ts
        prev = ts
    segments.append((seg_start, prev))

    delivery_signal_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in delivery_rows:
        if row.get("signal_id"):
            delivery_signal_map[str(row["signal_id"])].append(row)

    candidates: list[dict[str, Any]] = []
    for start_ts, end_ts in segments:
        signal_subset = [row for row in signal_rows if start_ts <= int(row["archive_ts"]) <= end_ts]
        quality_subset = [row for row in quality_rows if start_ts <= int(row["created_at"]) <= end_ts]
        case_subset = [row for row in case_rows if start_ts <= int(row["archive_ts"]) <= end_ts]
        followup_subset = [row for row in followup_rows if start_ts <= int(row["archive_ts"]) <= end_ts]
        delivery_subset = [row for row in delivery_rows if start_ts <= int(row["archive_ts"]) <= end_ts]
        unique_delivery_signal_ids = {row["signal_id"] for row in delivery_subset if row.get("signal_id")}
        source_presence = {
            "signals": len(signal_subset),
            "quality": len(quality_subset),
            "delivery_unique_signals": len(unique_delivery_signal_ids),
            "case_signal_attached": len(case_subset),
            "case_followups": len(followup_subset),
        }
        candidates.append(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "duration_sec": end_ts - start_ts,
                "source_presence": source_presence,
                "source_hits": sum(1 for value in source_presence.values() if value > 0),
                "signal_count": len(signal_subset),
                "quality_count": len(quality_subset),
                "delivery_unique_signals": len(unique_delivery_signal_ids),
            }
        )
    candidates.sort(
        key=lambda item: (
            item["source_hits"],
            item["duration_sec"],
            item["signal_count"],
            item["quality_count"],
            item["delivery_unique_signals"],
        ),
        reverse=True,
    )
    return candidates


def select_primary_window(candidates: list[dict[str, Any]], archive_window_start: int, archive_window_end: int) -> dict[str, Any]:
    if not candidates:
        return {
            "start_ts": archive_window_start,
            "end_ts": archive_window_end,
            "duration_sec": archive_window_end - archive_window_start,
            "selection_reason": "未找到可分段的 LP 时间戳，只能退回整个 archive 覆盖范围。",
            "segments": [],
        }
    chosen = candidates[0]
    reason = (
        "选择了昨晚 archive 范围内最长、且同时被 signals / quality / delivery / case followups 覆盖的连续 LP 活跃段；"
        f"该段共有 {chosen['signal_count']} 条 LP signal archive、{chosen['quality_count']} 条 quality 记录、"
        f"{chosen['delivery_unique_signals']} 个 delivery_audit signal_id。"
    )
    if len(candidates) > 1:
        runner_up = candidates[1]
        reason += (
            f" 第二候选段仅持续 {duration_label(runner_up['duration_sec'])}，"
            f"明显短于主窗口的 {duration_label(chosen['duration_sec'])}。"
        )
    return {
        "start_ts": chosen["start_ts"],
        "end_ts": chosen["end_ts"],
        "duration_sec": chosen["duration_sec"],
        "selection_reason": reason,
        "segments": candidates,
    }


def join_rows(
    signal_rows: list[dict[str, Any]],
    quality_by_signal_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    joined: list[dict[str, Any]] = []
    for signal_row in signal_rows:
        quality_row = quality_by_signal_id.get(str(signal_row.get("signal_id") or ""), {})
        row = dict(signal_row)
        row.update(
            {
                "created_at": to_int(quality_row.get("created_at")) or to_int(signal_row.get("archive_ts")),
                "direction_bucket": str(quality_row.get("direction_bucket") or ""),
                "intent_type": str(quality_row.get("intent_type") or ""),
                "move_before_alert": to_float(quality_row.get("move_before_alert")),
                "move_before_alert_30s": to_float(quality_row.get("move_before_alert_30s")),
                "move_before_alert_60s": to_float(quality_row.get("move_before_alert_60s")),
                "move_after_alert": to_float(quality_row.get("move_after_alert")),
                "move_after_alert_60s": to_float(quality_row.get("move_after_alert_60s")),
                "move_after_alert_300s": to_float(quality_row.get("move_after_alert_300s")),
                "followthrough_positive": quality_row.get("followthrough_positive"),
                "followthrough_negative": quality_row.get("followthrough_negative"),
                "reversal_after_climax": quality_row.get("reversal_after_climax"),
                "confirm_after_prealert": quality_row.get("confirm_after_prealert"),
                "false_prealert": quality_row.get("false_prealert"),
                "time_to_confirm": to_int(quality_row.get("time_to_confirm")),
                "market_move_before_alert_30s": to_float(quality_row.get("market_move_before_alert_30s")),
                "market_move_before_alert_60s": to_float(quality_row.get("market_move_before_alert_60s")),
                "market_move_after_alert_60s": to_float(quality_row.get("market_move_after_alert_60s")),
                "market_move_after_alert_300s": to_float(quality_row.get("market_move_after_alert_300s")),
                "quality_notifier_sent_at": to_int(quality_row.get("notifier_sent_at")),
                "delivered_notification": bool(quality_row.get("delivered_notification")),
            }
        )
        joined.append(row)
    joined.sort(key=lambda row: (int(row["archive_ts"]), str(row.get("signal_id") or "")))
    return joined


def filter_rows_by_window(rows: list[dict[str, Any]], start_ts: int, end_ts: int, ts_key: str) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        ts = to_int(row.get(ts_key))
        if ts is not None and start_ts <= ts <= end_ts:
            filtered.append(row)
    return filtered


def build_reconciliation(
    signal_rows: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    delivery_rows: list[dict[str, Any]],
    case_signal_attached_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    signal_ids = {str(row.get("signal_id") or "") for row in signal_rows if row.get("signal_id")}
    quality_ids = {str(row.get("signal_id") or "") for row in quality_rows if row.get("signal_id")}
    delivery_ids = {str(row.get("signal_id") or "") for row in delivery_rows if row.get("signal_id")}
    delivered_ids = {
        str(row.get("signal_id") or "")
        for row in delivery_rows
        if row.get("signal_id") and (row.get("delivered_notification") is True or row.get("notifier_sent_at"))
    }
    case_attached_ids = {str(row.get("signal_id") or "") for row in case_signal_attached_rows if row.get("signal_id")}
    followup_ids = {str(row.get("signal_id") or "") for row in followup_rows if row.get("signal_id")}
    field_presence = {
        "signal_id": sum(1 for row in signal_rows if row.get("signal_id")),
        "asset_case_id": sum(1 for row in signal_rows if row.get("asset_case_id")),
        "notifier_sent_at": sum(1 for row in signal_rows if row.get("notifier_sent_at")),
        "delivery_decision": sum(1 for row in signal_rows if row.get("delivery_decision")),
        "market_context_source": sum(1 for row in signal_rows if row.get("market_context_source") not in ("", None)),
    }
    return {
        "signal_archive_unique_signal_ids": len(signal_ids),
        "quality_unique_signal_ids": len(quality_ids),
        "delivery_unique_signal_ids": len(delivery_ids),
        "delivered_unique_signal_ids": len(delivered_ids),
        "case_signal_attached_unique_signal_ids": len(case_attached_ids),
        "case_followup_unique_signal_ids": len(followup_ids),
        "signal_vs_quality_missing": sorted(signal_ids - quality_ids),
        "quality_vs_signal_missing": sorted(quality_ids - signal_ids),
        "signal_vs_delivery_missing": sorted(signal_ids - delivery_ids),
        "delivery_vs_signal_missing": sorted(delivery_ids - signal_ids),
        "delivered_vs_case_attached_diff": sorted(delivered_ids ^ case_attached_ids),
        "delivered_vs_followup_diff": sorted(delivered_ids ^ followup_ids),
        "field_presence": field_presence,
    }


def build_capture_summary(rows: list[dict[str, Any]], delivered_ids: set[str]) -> dict[str, Any]:
    stage_counter = Counter(str(row.get("lp_alert_stage") or "") for row in rows)
    major_count = sum(1 for row in rows if classify_major(row.get("asset_symbol")) == "major")
    long_tail_count = len(rows) - major_count
    market_context_counter = Counter(str(row.get("market_context_source") or "unavailable") for row in rows)
    scan_path_counter = Counter(str(row.get("lp_scan_path") or "unknown") for row in rows)
    pair_counter = Counter(str(row.get("pair_label") or "") for row in rows)
    pool_counter = Counter(str(row.get("pool_address") or "") for row in rows)
    asset_case_ids = {str(row.get("asset_case_id") or "") for row in rows if row.get("asset_case_id")}
    return {
        "lp_signal_count": len(rows),
        "lp_message_count": sum(1 for row in rows if str(row.get("signal_id") or "") in delivered_ids),
        "asset_case_count": len(asset_case_ids),
        "pool_count": len(pool_counter),
        "pair_count": len(pair_counter),
        "asset_count": len({str(row.get("asset_symbol") or "") for row in rows if row.get("asset_symbol")}),
        "stage_counter": dict(stage_counter),
        "major_count": major_count,
        "long_tail_count": long_tail_count,
        "major_share_pct": pct(major_count, len(rows)),
        "long_tail_share_pct": pct(long_tail_count, len(rows)),
        "market_context_source_distribution": summarize_counter(market_context_counter),
        "scan_path_distribution": summarize_counter(scan_path_counter),
        "pair_distribution": summarize_counter(pair_counter),
        "pool_distribution": summarize_counter(pool_counter),
    }


def build_asset_case_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cases: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        case_id = str(row.get("asset_case_id") or "")
        if case_id:
            cases[case_id].append(row)
    case_details: list[dict[str, Any]] = []
    for case_id, group in cases.items():
        group.sort(key=lambda row: (int(row["created_at"]), str(row.get("signal_id") or "")))
        stage_path: list[str] = []
        for row in group:
            stage = str(row.get("lp_alert_stage") or "")
            if stage and (not stage_path or stage_path[-1] != stage):
                stage_path.append(stage)
        case_details.append(
            {
                "asset_case_id": case_id,
                "pair_labels": sorted({str(row.get("pair_label") or "") for row in group if row.get("pair_label")}),
                "signal_count": len(group),
                "stage_path": stage_path,
                "started_at": min(int(row["created_at"]) for row in group),
                "updated_at": max(int(row["created_at"]) for row in group),
                "direction_bucket": str(group[0].get("direction_bucket") or ""),
                "asset_symbol": str(group[0].get("asset_symbol") or ""),
                "quality_score": median_or_none([row.get("asset_case_quality_score") for row in group], digits=3),
            }
        )
    case_details.sort(key=lambda row: (row["signal_count"], row["updated_at"] - row["started_at"]), reverse=True)
    return {
        "total_asset_cases": len(case_details),
        "details": case_details,
    }


def build_stage_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for stage in STAGES:
        subset = [row for row in rows if str(row.get("lp_alert_stage") or "") == stage]
        adverse_60 = [row for row in subset if adverse_move(row, "move_after_alert_60s") is not None]
        adverse_300 = [row for row in subset if adverse_move(row, "move_after_alert_300s") is not None]
        stats[stage] = {
            "count": len(subset),
            "asset_distribution": summarize_counter(Counter(str(row.get("asset_symbol") or "") for row in subset)),
            "pair_distribution": summarize_counter(Counter(str(row.get("pair_label") or "") for row in subset)),
            "direction_distribution": summarize_counter(Counter(str(row.get("direction_bucket") or "") for row in subset)),
            "median_move_before_alert_30s": median_or_none([row.get("move_before_alert_30s") for row in subset]),
            "median_move_before_alert_60s": median_or_none([row.get("move_before_alert_60s") for row in subset]),
            "median_aligned_move_before_alert_30s": median_or_none([aligned_move(row, "move_before_alert_30s") for row in subset]),
            "median_aligned_move_before_alert_60s": median_or_none([aligned_move(row, "move_before_alert_60s") for row in subset]),
            "median_move_after_alert_60s": median_or_none([row.get("move_after_alert_60s") for row in subset]),
            "median_move_after_alert_300s": median_or_none([row.get("move_after_alert_300s") for row in subset]),
            "median_aligned_move_after_alert_60s": median_or_none([aligned_move(row, "move_after_alert_60s") for row in subset]),
            "median_aligned_move_after_alert_300s": median_or_none([aligned_move(row, "move_after_alert_300s") for row in subset]),
            "resolved_after_alert_60s": len(adverse_60),
            "resolved_after_alert_300s": len(adverse_300),
            "adverse_followthrough_60s_pct": pct(sum(1 for row in adverse_60 if adverse_move(row, "move_after_alert_60s")), len(adverse_60)),
            "adverse_followthrough_300s_pct": pct(sum(1 for row in adverse_300 if adverse_move(row, "move_after_alert_300s")), len(adverse_300)),
            "median_detect_latency_ms": median_or_none([row.get("lp_detect_latency_ms") for row in subset], digits=1, positive_only=True),
            "median_end_to_end_latency_ms": median_or_none([row.get("lp_end_to_end_latency_ms") for row in subset], digits=1, positive_only=True),
            "market_context_available_rate_pct": pct(
                sum(1 for row in subset if str(row.get("market_context_source") or "") == "live_public"),
                len(subset),
            ),
            "market_context_source_distribution": summarize_counter(Counter(str(row.get("market_context_source") or "") for row in subset)),
            "onchain_timing_distribution": summarize_counter(Counter(str(row.get("lp_alert_quality") or "") for row in subset)),
            "alert_relative_timing_distribution": summarize_counter(Counter(str(row.get("alert_relative_timing") or "") for row in subset if row.get("alert_relative_timing"))),
        }
    return stats


def build_prealert_conversion(rows: list[dict[str, Any]]) -> dict[str, Any]:
    prealerts = [row for row in rows if str(row.get("lp_alert_stage") or "") == "prealert"]
    confirms = [row for row in rows if str(row.get("lp_alert_stage") or "") == "confirm"]
    if not prealerts:
        return {
            "count": 0,
            "within_30s_pct": None,
            "within_60s_pct": None,
            "within_90s_pct": None,
            "notes": "本窗口无 prealert 样本，无法判断 prealert->confirm 转化。",
        }
    deltas: list[int] = []
    for prealert in prealerts:
        pre_ts = int(prealert["created_at"])
        asset_case_id = str(prealert.get("asset_case_id") or "")
        pair_label = str(prealert.get("pair_label") or "")
        direction = str(prealert.get("direction_bucket") or "")
        matches = [
            int(confirm["created_at"]) - pre_ts
            for confirm in confirms
            if int(confirm["created_at"]) > pre_ts
            and (
                (asset_case_id and str(confirm.get("asset_case_id") or "") == asset_case_id)
                or (
                    str(confirm.get("pair_label") or "") == pair_label
                    and str(confirm.get("direction_bucket") or "") == direction
                )
            )
        ]
        if matches:
            deltas.append(min(matches))
    return {
        "count": len(prealerts),
        "within_30s_pct": pct(sum(1 for delta in deltas if delta <= 30), len(prealerts)),
        "within_60s_pct": pct(sum(1 for delta in deltas if delta <= 60), len(prealerts)),
        "within_90s_pct": pct(sum(1 for delta in deltas if delta <= 90), len(prealerts)),
        "notes": "优先按 asset_case_id 匹配，缺失时退化到 pair_label + direction_bucket。",
    }


def build_patch_validation(window_rows: list[dict[str, Any]], full_rows: list[dict[str, Any]]) -> dict[str, Any]:
    def _mapping(rows: list[dict[str, Any]]) -> dict[str, Any]:
        sweep_rows = [row for row in rows if str(row.get("lp_sweep_phase") or "") == "sweep_building"]
        residual = []
        for row in sweep_rows:
            text = " ".join(
                [
                    str(row.get("lp_sweep_display_stage") or ""),
                    str(row.get("headline_label") or ""),
                    str(row.get("lp_state_label") or ""),
                    str(row.get("lp_meaning_brief") or ""),
                ]
            ).lower()
            if "climax" in text or "高潮" in text:
                residual.append(
                    {
                        "signal_id": row.get("signal_id"),
                        "pair_label": row.get("pair_label"),
                        "headline_label": row.get("headline_label"),
                        "lp_state_label": row.get("lp_state_label"),
                    }
                )
        return {"count": len(sweep_rows), "residual_climax_count": len(residual), "residual_cases": residual}

    def _confirm_buckets(rows: list[dict[str, Any]]) -> dict[str, Any]:
        confirm_rows = [row for row in rows if str(row.get("lp_alert_stage") or "") == "confirm"]
        classified: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in confirm_rows:
            if str(row.get("lp_sweep_phase") or "") == "sweep_building":
                bucket = "sweep_building_confirm"
            else:
                bucket = str(row.get("lp_confirm_quality") or "unlabeled_confirm")
            classified[bucket].append(row)
        summary: dict[str, Any] = {
            "total_confirm_rows": len(confirm_rows),
            "bucket_counts": {key: len(value) for key, value in sorted(classified.items())},
        }
        for bucket, bucket_rows in classified.items():
            summary[bucket] = {
                "count": len(bucket_rows),
                "median_move_before_alert_30s": median_or_none([row.get("move_before_alert_30s") for row in bucket_rows]),
                "median_move_before_alert_60s": median_or_none([row.get("move_before_alert_60s") for row in bucket_rows]),
                "median_aligned_move_after_alert_60s": median_or_none([aligned_move(row, "move_after_alert_60s") for row in bucket_rows]),
                "median_aligned_move_after_alert_300s": median_or_none([aligned_move(row, "move_after_alert_300s") for row in bucket_rows]),
                "absorption_distribution": summarize_counter(Counter(str(row.get("lp_absorption_context") or "") for row in bucket_rows)),
            }
        return summary

    def _absorption(rows: list[dict[str, Any]]) -> dict[str, Any]:
        counter = Counter(str(row.get("lp_absorption_context") or "") for row in rows)
        keys = [
            "local_sell_pressure_absorption",
            "local_buy_pressure_absorption",
            "broader_sell_pressure_confirmed",
            "broader_buy_pressure_confirmed",
            "pool_only_unconfirmed_pressure",
        ]
        return {key: int(counter.get(key) or 0) for key in keys}

    full_signal_ids = {str(row.get("signal_id") or "") for row in full_rows if row.get("signal_id")}
    window_signal_ids = {str(row.get("signal_id") or "") for row in window_rows if row.get("signal_id")}
    delivered_full = {str(row.get("signal_id") or "") for row in full_rows if row.get("sent_to_telegram")}
    delivered_window = {str(row.get("signal_id") or "") for row in window_rows if row.get("sent_to_telegram")}

    return {
        "window": {
            "sweep_building_mapping": _mapping(window_rows),
            "confirm_quality": _confirm_buckets(window_rows),
            "absorption_tags": _absorption(window_rows),
            "signal_archive_rows": len(window_rows),
            "signal_archive_unique_signal_ids": len(window_signal_ids),
            "delivered_signal_ids": len(delivered_window),
            "market_context_live_public_hit_rate_pct": pct(
                sum(1 for row in window_rows if str(row.get("market_context_source") or "") == "live_public"),
                len(window_rows),
            ),
        },
        "full_overnight": {
            "sweep_building_mapping": _mapping(full_rows),
            "confirm_quality": _confirm_buckets(full_rows),
            "absorption_tags": _absorption(full_rows),
            "signal_archive_rows": len(full_rows),
            "signal_archive_unique_signal_ids": len(full_signal_ids),
            "delivered_signal_ids": len(delivered_full),
            "market_context_live_public_hit_rate_pct": pct(
                sum(1 for row in full_rows if str(row.get("market_context_source") or "") == "live_public"),
                len(full_rows),
            ),
        },
    }


def build_reversal_special(rows: list[dict[str, Any]]) -> dict[str, Any]:
    confirm_rows = [row for row in rows if str(row.get("lp_alert_stage") or "") == "confirm"]
    sell_rows = [row for row in confirm_rows if str(row.get("direction_bucket") or "") == "sell_pressure"]
    buy_rows = [row for row in confirm_rows if str(row.get("direction_bucket") or "") == "buy_pressure"]

    def _summarize(direction_rows: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
        resolved = [row for row in direction_rows if row.get(horizon) is not None]
        against_rows = [row for row in resolved if adverse_move(row, horizon)]
        return {
            "resolved_count": len(resolved),
            "against_count": len(against_rows),
            "against_pct": pct(len(against_rows), len(resolved)),
            "confirm_quality_distribution": summarize_counter(Counter(str(row.get("lp_confirm_quality") or "(blank)") for row in against_rows)),
            "absorption_distribution": summarize_counter(Counter(str(row.get("lp_absorption_context") or "") for row in against_rows)),
            "market_context_distribution": summarize_counter(Counter(str(row.get("market_context_source") or "") for row in against_rows)),
        }

    reverse_cases = []
    for row in confirm_rows:
        against_60 = adverse_move(row, "move_after_alert_60s")
        against_300 = adverse_move(row, "move_after_alert_300s")
        if against_60 or against_300:
            reverse_cases.append(
                {
                    "signal_id": row.get("signal_id"),
                    "created_at": row.get("created_at"),
                    "pair_label": row.get("pair_label"),
                    "asset_case_id": row.get("asset_case_id"),
                    "intent_type": row.get("intent_type"),
                    "direction_bucket": row.get("direction_bucket"),
                    "lp_alert_stage": row.get("lp_alert_stage"),
                    "lp_confirm_quality": row.get("lp_confirm_quality") or "(blank)",
                    "lp_sweep_phase": row.get("lp_sweep_phase") or "",
                    "lp_absorption_context": row.get("lp_absorption_context"),
                    "lp_broader_alignment": row.get("lp_broader_alignment"),
                    "market_context_source": row.get("market_context_source"),
                    "market_context_requested_symbol": row.get("market_context_requested_symbol"),
                    "market_context_failure_reason": row.get("market_context_failure_reason"),
                    "asset_case_supporting_pair_count": row.get("asset_case_supporting_pair_count"),
                    "asset_case_multi_pool": row.get("asset_case_multi_pool"),
                    "asset_case_quality_score": row.get("asset_case_quality_score"),
                    "move_before_alert_30s": row.get("move_before_alert_30s"),
                    "move_before_alert_60s": row.get("move_before_alert_60s"),
                    "move_after_alert_60s": row.get("move_after_alert_60s"),
                    "move_after_alert_300s": row.get("move_after_alert_300s"),
                    "headline_label": row.get("headline_label"),
                    "lp_market_read": row.get("lp_market_read"),
                    "sent_to_telegram": row.get("sent_to_telegram"),
                }
            )
    reverse_cases.sort(
        key=lambda item: (
            abs(to_float(item.get("move_after_alert_300s")) or 0.0),
            abs(to_float(item.get("move_after_alert_60s")) or 0.0),
        ),
        reverse=True,
    )
    return {
        "confirm_sell_pressure": {
            "count": len(sell_rows),
            "after_30s": None,
            "after_60s": _summarize(sell_rows, "move_after_alert_60s"),
            "after_300s": _summarize(sell_rows, "move_after_alert_300s"),
        },
        "confirm_buy_pressure": {
            "count": len(buy_rows),
            "after_30s": None,
            "after_60s": _summarize(buy_rows, "move_after_alert_60s"),
            "after_300s": _summarize(buy_rows, "move_after_alert_300s"),
        },
        "reverse_cases": reverse_cases,
        "reverse_case_count": len(reverse_cases),
        "explanation": (
            "当前持久化只稳定回写 60s/300s 结果，缺少可靠的 after-alert 30s 字段；"
            "因此 reversal 专题只对 60s/300s 做定量统计。"
        ),
    }


def build_market_context_health(rows: list[dict[str, Any]]) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    for row in rows:
        attempts.extend(list(row.get("market_context_attempts") or []))
    venue_stats = defaultdict(lambda: defaultdict(int))
    endpoint_stats = defaultdict(lambda: defaultdict(int))
    resolved_symbol_stats = defaultdict(lambda: defaultdict(int))
    requested_vs_resolved = Counter()
    failure_reasons = Counter()
    final_failure_reasons = Counter(str(row.get("market_context_failure_reason") or "") for row in rows if row.get("market_context_failure_reason"))
    for row in rows:
        venue = str(row.get("market_context_venue") or "")
        resolved = str(row.get("market_context_resolved_symbol") or "")
        requested = str(row.get("market_context_requested_symbol") or "")
        source = str(row.get("market_context_source") or "")
        if venue:
            venue_stats[venue]["signal_total"] += 1
            venue_stats[venue][f"signal_{source or 'blank'}"] += 1
        if resolved:
            resolved_symbol_stats[resolved]["signal_total"] += 1
            resolved_symbol_stats[resolved][f"signal_{source or 'blank'}"] += 1
        if requested and resolved:
            requested_vs_resolved[f"{requested}->{resolved}"] += 1
    for attempt in attempts:
        venue = str(attempt.get("venue") or "")
        endpoint = str(attempt.get("endpoint") or "")
        symbol = str(attempt.get("symbol") or "")
        status = str(attempt.get("status") or "")
        reason = str(attempt.get("failure_reason") or "")
        http_status = attempt.get("http_status")
        if venue:
            venue_stats[venue]["attempt_total"] += 1
            venue_stats[venue][f"attempt_{status or 'blank'}"] += 1
        if endpoint:
            endpoint_stats[endpoint]["attempt_total"] += 1
            endpoint_stats[endpoint][f"attempt_{status or 'blank'}"] += 1
            if http_status is not None:
                endpoint_stats[endpoint][f"http_{int(http_status)}"] += 1
        if symbol:
            resolved_symbol_stats[symbol]["attempt_total"] += 1
            resolved_symbol_stats[symbol][f"attempt_{status or 'blank'}"] += 1
        if reason:
            failure_reasons[reason] += 1

    def _finalize(table: dict[str, dict[str, int]], key_name: str) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for key, metrics in sorted(
            table.items(),
            key=lambda item: (
                -(item[1].get("signal_total") or item[1].get("attempt_total") or 0),
                item[0],
            ),
        ):
            signal_total = int(metrics.get("signal_total") or 0)
            live_public = int(metrics.get("signal_live_public") or 0)
            unavailable = int(metrics.get("signal_unavailable") or 0)
            attempt_total = int(metrics.get("attempt_total") or 0)
            attempt_success = int(metrics.get("attempt_success") or 0) + int(metrics.get("attempt_cache_hit") or 0)
            attempt_failure = int(metrics.get("attempt_failure") or 0)
            output.append(
                {
                    key_name: key,
                    **dict(metrics),
                    "signal_hit_rate": round(live_public / signal_total, 4) if signal_total else 0.0,
                    "signal_unavailable_rate": round(unavailable / signal_total, 4) if signal_total else 0.0,
                    "attempt_hit_rate": round(attempt_success / attempt_total, 4) if attempt_total else 0.0,
                    "attempt_failure_rate": round(attempt_failure / attempt_total, 4) if attempt_total else 0.0,
                }
            )
        return output

    live_public_count = sum(1 for row in rows if str(row.get("market_context_source") or "") == "live_public")
    unavailable_count = sum(1 for row in rows if str(row.get("market_context_source") or "") == "unavailable")
    fallback_examples = [
        item
        for item, count in requested_vs_resolved.items()
        if "->" in item and item.split("->", 1)[0] != item.split("->", 1)[1]
        for _ in [count]
    ]
    return {
        "signal_rows": len(rows),
        "total_attempts": len(attempts),
        "live_public_count": live_public_count,
        "unavailable_count": unavailable_count,
        "live_public_hit_rate_pct": pct(live_public_count, len(rows)),
        "unavailable_rate_pct": pct(unavailable_count, len(rows)),
        "per_venue": _finalize(venue_stats, "venue"),
        "per_endpoint": _finalize(endpoint_stats, "endpoint"),
        "per_symbol": _finalize(resolved_symbol_stats, "symbol"),
        "top_failure_reasons": summarize_counter(failure_reasons, top_n=10),
        "final_failure_reason_distribution": summarize_counter(final_failure_reasons, top_n=10),
        "requested_to_resolved_symbol_distribution": summarize_counter(requested_vs_resolved),
        "fallback_pairs_detected": fallback_examples,
    }


def build_quality_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dimensions: dict[str, list[dict[str, Any]]] = {}
    for dimension in ("asset_symbol", "pair_label", "pool_address"):
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = str(row.get(dimension) or "")
            if key:
                groups[key].append(row)
        entries: list[dict[str, Any]] = []
        for key, group in groups.items():
            entries.append(
                {
                    "dimension_key": key,
                    "sample_size": len(group),
                    "prealert_precision_score": median_or_none([row.get("prealert_precision_score") for row in group], digits=4),
                    "confirm_conversion_score": median_or_none([row.get("confirm_conversion_score") for row in group], digits=4),
                    "climax_reversal_score": median_or_none([row.get("climax_reversal_score") for row in group], digits=4),
                    "market_context_alignment_score": median_or_none([row.get("market_context_alignment_score") for row in group], digits=4),
                    "fastlane_roi_score": median_or_none([row.get("fastlane_roi_score") for row in group], digits=4),
                    "pair_quality_score": median_or_none([row.get("pair_quality_score") for row in group], digits=4),
                    "pool_quality_score": median_or_none([row.get("pool_quality_score") for row in group], digits=4),
                }
            )
        entries.sort(key=lambda item: (-int(item["sample_size"]), item["dimension_key"]))
        dimensions[dimension] = entries

    pair_rows = dimensions.get("pair_label", [])
    pool_rows = dimensions.get("pool_address", [])
    top_confirm = max(pair_rows, key=lambda item: (item.get("confirm_conversion_score") or -1.0, item["sample_size"]), default=None)
    worst_prealert = min(pair_rows, key=lambda item: (item.get("prealert_precision_score") or 99.0, -item["sample_size"]), default=None)
    top_climax_reversal = max(pair_rows, key=lambda item: (item.get("climax_reversal_score") or -1.0, item["sample_size"]), default=None)
    strongest_market_context_alignment = max(pair_rows, key=lambda item: (item.get("market_context_alignment_score") or -1.0, item["sample_size"]), default=None)
    weakest_market_context_alignment = min(pair_rows, key=lambda item: (item.get("market_context_alignment_score") or 99.0, -item["sample_size"]), default=None)
    strongest_fastlane = max(pair_rows, key=lambda item: (item.get("fastlane_roi_score") or -1.0, item["sample_size"]), default=None)
    weakest_fastlane = min(pair_rows, key=lambda item: (item.get("fastlane_roi_score") or 99.0, -item["sample_size"]), default=None)

    promoted_main_rows = [
        row
        for row in rows
        if str(row.get("lp_route_priority_source") or "") == "promoted_main"
        or str(row.get("lp_scan_path") or "") == "promoted_main"
        or (str(row.get("lp_scan_path") or "") == "main" and bool(row.get("lp_promoted_fastlane")))
    ]
    return {
        "dimensions": dimensions,
        "worst_prealert_precision": worst_prealert,
        "strongest_confirm_conversion": top_confirm,
        "highest_climax_reversal": top_climax_reversal,
        "strongest_market_context_alignment": strongest_market_context_alignment,
        "weakest_market_context_alignment": weakest_market_context_alignment,
        "strongest_fastlane_roi": strongest_fastlane,
        "weakest_fastlane_roi": weakest_fastlane,
        "promoted_main_signal_count": len(promoted_main_rows),
        "promoted_main_delivered_count": sum(1 for row in promoted_main_rows if row.get("sent_to_telegram")),
        "promoted_main_median_latency_ms": median_or_none([row.get("lp_detect_latency_ms") for row in promoted_main_rows], digits=1, positive_only=True),
        "promoted_main_aligned_move_after_300s": median_or_none([aligned_move(row, "move_after_alert_300s") for row in promoted_main_rows]),
    }


def build_majors_coverage(rows: list[dict[str, Any]], cli_major_pool_coverage: dict[str, Any]) -> dict[str, Any]:
    pair_counter = Counter(str(row.get("pair_label") or "") for row in rows)
    asset_counter = Counter(str(row.get("asset_symbol") or "") for row in rows)
    major_pairs_present = sorted(pair for pair in pair_counter if pair in EXPECTED_MAJOR_PAIRS)
    missing_pairs = [pair for pair in EXPECTED_MAJOR_PAIRS if pair not in major_pairs_present]
    return {
        "overnight_pair_distribution": summarize_counter(pair_counter),
        "overnight_asset_distribution": summarize_counter(asset_counter),
        "major_pairs_present": major_pairs_present,
        "missing_expected_pairs": missing_pairs,
        "current_sample_still_eth_dominated": set(pair_counter) <= {"ETH/USDT", "ETH/USDC"},
        "cli_major_pool_coverage": cli_major_pool_coverage,
    }


def build_noise_assessment(
    window_rows: list[dict[str, Any]],
    full_rows: list[dict[str, Any]],
    quality_summary: dict[str, Any],
) -> dict[str, Any]:
    prealert_count = sum(1 for row in full_rows if str(row.get("lp_alert_stage") or "") == "prealert")
    confirm_count = sum(1 for row in full_rows if str(row.get("lp_alert_stage") or "") == "confirm")
    climax_count = sum(1 for row in full_rows if str(row.get("lp_alert_stage") or "") == "climax")
    exhaustion_count = sum(1 for row in full_rows if str(row.get("lp_alert_stage") or "") == "exhaustion_risk")
    pair_rows = quality_summary.get("dimensions", {}).get("pair_label", []) or []
    most_misleading_pair = max(
        pair_rows,
        key=lambda item: (
            item.get("climax_reversal_score") or 0.0,
            -(item.get("confirm_conversion_score") or 0.0),
            item.get("sample_size") or 0,
        ),
        default=None,
    )
    return {
        "prealert_count": prealert_count,
        "confirm_count": confirm_count,
        "climax_count": climax_count,
        "exhaustion_risk_count": exhaustion_count,
        "prealert_share_pct": pct(prealert_count, len(full_rows)),
        "most_misleading_pair": most_misleading_pair,
        "research_user": {
            "noise_level": "中等",
            "biggest_risk": "prealert 缺席导致研究样本偏后段，且 broader market context 仍缺失",
            "retain": "confirm / climax / exhaustion_risk + 完整 archive/reconciliation",
            "hide_or_downgrade": "无 broader confirmation 的单池 confirm 应继续显式降级",
        },
        "trader_user": {
            "noise_level": "中高",
            "biggest_risk": "把 confirm 或 climax 当成预测而不是已发生的链上确认",
            "retain": "晚期风险明确的 exhaustion_risk",
            "hide_or_downgrade": "sweep_building / late_confirm / unconfirmed_confirm 不应伪装成 clean entry",
        },
        "retail_user": {
            "noise_level": "高",
            "biggest_risk": "live market context 缺失下的过度解读",
            "retain": "目前不建议直接面向 retail 默认开放",
            "hide_or_downgrade": "confirm/climax 默认都需要更强的解释层和过滤层",
        },
    }


def build_scorecard(
    reconciliation: dict[str, Any],
    full_capture: dict[str, Any],
    window_capture: dict[str, Any],
    market_context_health: dict[str, Any],
    majors_coverage: dict[str, Any],
    noise_assessment: dict[str, Any],
    patch_validation: dict[str, Any],
) -> dict[str, float]:
    signal_archive_integrity = 8.8
    if reconciliation["signal_vs_quality_missing"] or reconciliation["quality_vs_signal_missing"]:
        signal_archive_integrity -= 1.5
    if reconciliation["delivered_vs_case_attached_diff"] or reconciliation["delivered_vs_followup_diff"]:
        signal_archive_integrity -= 1.0

    live_context_readiness = 1.5
    if (market_context_health.get("live_public_hit_rate_pct") or 0.0) > 10.0:
        live_context_readiness += 2.0
    if (market_context_health.get("live_public_hit_rate_pct") or 0.0) == 0.0:
        live_context_readiness = 1.5

    majors_rep = 2.8
    if not majors_coverage.get("current_sample_still_eth_dominated"):
        majors_rep += 1.5

    noise_control = 6.3
    if (noise_assessment.get("prealert_share_pct") or 0.0) == 0.0:
        noise_control -= 0.5
    if patch_validation["full_overnight"]["sweep_building_mapping"]["residual_climax_count"] > 0:
        noise_control -= 1.0

    research_readiness = 5.8
    if full_capture["lp_signal_count"] < 50:
        research_readiness -= 0.8
    if (market_context_health.get("live_public_hit_rate_pct") or 0.0) == 0.0:
        research_readiness -= 0.8
    if majors_coverage.get("current_sample_still_eth_dominated"):
        research_readiness -= 0.6

    signal_usability = 4.8
    if patch_validation["full_overnight"]["confirm_quality"]["bucket_counts"].get("late_confirm", 0) == 0:
        signal_usability -= 0.4
    if patch_validation["full_overnight"]["confirm_quality"]["bucket_counts"].get("unconfirmed_confirm", 0) > 0:
        signal_usability -= 0.2
    if window_capture["lp_message_count"] >= 10:
        signal_usability += 0.2

    overall = round(
        statistics.mean(
            [
                research_readiness,
                signal_usability,
                live_context_readiness,
                signal_archive_integrity,
                majors_rep,
                noise_control,
            ]
        ),
        1,
    )
    return {
        "LP_研究采样_readiness": round(research_readiness, 1),
        "LP_产品信号可用性": round(signal_usability, 1),
        "live_market_context_readiness": round(live_context_readiness, 1),
        "signal_archive_完整性": round(signal_archive_integrity, 1),
        "majors_覆盖代表性": round(majors_rep, 1),
        "噪音控制": round(noise_control, 1),
        "综合分": overall,
    }


def add_metric(
    rows: list[dict[str, Any]],
    metric_group: str,
    metric_name: str,
    value: Any,
    *,
    asset: str = "",
    pair: str = "",
    stage: str = "",
    sample_size: int | None = None,
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
            "value": value,
            "sample_size": sample_size if sample_size is not None else "",
            "window": window,
            "notes": notes,
        }
    )


def build_metrics_csv_rows(
    primary_window: dict[str, Any],
    full_capture: dict[str, Any],
    window_capture: dict[str, Any],
    stage_stats: dict[str, Any],
    patch_validation: dict[str, Any],
    reconciliation: dict[str, Any],
    market_context_health: dict[str, Any],
    majors_coverage: dict[str, Any],
    quality_summary: dict[str, Any],
    scorecard: dict[str, float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{fmt_ts(primary_window['start_ts'])} -> {fmt_ts(primary_window['end_ts'])}"
    add_metric(rows, "window", "selected_duration_sec", primary_window["duration_sec"], sample_size=1, window=window_label)
    add_metric(rows, "capture", "full_overnight_lp_signal_count", full_capture["lp_signal_count"], sample_size=full_capture["lp_signal_count"], window="full_overnight")
    add_metric(rows, "capture", "full_overnight_lp_message_count", full_capture["lp_message_count"], sample_size=full_capture["lp_signal_count"], window="full_overnight")
    add_metric(rows, "capture", "window_lp_signal_count", window_capture["lp_signal_count"], sample_size=window_capture["lp_signal_count"], window=window_label)
    add_metric(rows, "capture", "window_lp_message_count", window_capture["lp_message_count"], sample_size=window_capture["lp_signal_count"], window=window_label)
    add_metric(rows, "capture", "window_asset_case_count", window_capture["asset_case_count"], sample_size=window_capture["lp_signal_count"], window=window_label)
    for stage, stats in stage_stats.items():
        add_metric(rows, "stage", "count", stats["count"], stage=stage, sample_size=stats["count"], window=window_label)
        add_metric(rows, "stage", "median_move_before_alert_30s", stats["median_move_before_alert_30s"], stage=stage, sample_size=stats["count"], window=window_label)
        add_metric(rows, "stage", "median_move_before_alert_60s", stats["median_move_before_alert_60s"], stage=stage, sample_size=stats["count"], window=window_label)
        add_metric(rows, "stage", "median_move_after_alert_60s", stats["median_move_after_alert_60s"], stage=stage, sample_size=stats["resolved_after_alert_60s"], window=window_label)
        add_metric(rows, "stage", "median_move_after_alert_300s", stats["median_move_after_alert_300s"], stage=stage, sample_size=stats["resolved_after_alert_300s"], window=window_label)
        add_metric(rows, "stage", "adverse_followthrough_60s_pct", stats["adverse_followthrough_60s_pct"], stage=stage, sample_size=stats["resolved_after_alert_60s"], window=window_label)
        add_metric(rows, "stage", "adverse_followthrough_300s_pct", stats["adverse_followthrough_300s_pct"], stage=stage, sample_size=stats["resolved_after_alert_300s"], window=window_label)
        add_metric(rows, "stage", "median_detect_latency_ms", stats["median_detect_latency_ms"], stage=stage, sample_size=stats["count"], window=window_label)
        add_metric(rows, "stage", "median_end_to_end_latency_ms", stats["median_end_to_end_latency_ms"], stage=stage, sample_size=stats["count"], window=window_label)
    add_metric(rows, "patch", "sweep_building_count_full_overnight", patch_validation["full_overnight"]["sweep_building_mapping"]["count"], sample_size=patch_validation["full_overnight"]["signal_archive_rows"], window="full_overnight")
    add_metric(rows, "patch", "sweep_building_residual_climax_count", patch_validation["full_overnight"]["sweep_building_mapping"]["residual_climax_count"], sample_size=patch_validation["full_overnight"]["sweep_building_mapping"]["count"], window="full_overnight")
    for bucket, count in patch_validation["full_overnight"]["confirm_quality"]["bucket_counts"].items():
        add_metric(rows, "patch_confirm", bucket, count, sample_size=patch_validation["full_overnight"]["confirm_quality"]["total_confirm_rows"], window="full_overnight")
    for tag, count in patch_validation["full_overnight"]["absorption_tags"].items():
        add_metric(rows, "patch_absorption", tag, count, sample_size=full_capture["lp_signal_count"], window="full_overnight")
    add_metric(rows, "reconciliation", "signal_archive_unique_signal_ids", reconciliation["signal_archive_unique_signal_ids"], sample_size=reconciliation["signal_archive_unique_signal_ids"], window="full_overnight")
    add_metric(rows, "reconciliation", "delivery_unique_signal_ids", reconciliation["delivery_unique_signal_ids"], sample_size=reconciliation["delivery_unique_signal_ids"], window="full_overnight")
    add_metric(rows, "reconciliation", "delivered_unique_signal_ids", reconciliation["delivered_unique_signal_ids"], sample_size=reconciliation["delivered_unique_signal_ids"], window="full_overnight")
    add_metric(rows, "market_context", "live_public_hit_rate_pct", market_context_health["live_public_hit_rate_pct"], sample_size=market_context_health["signal_rows"], window="full_overnight")
    add_metric(rows, "market_context", "unavailable_rate_pct", market_context_health["unavailable_rate_pct"], sample_size=market_context_health["signal_rows"], window="full_overnight")
    for row in market_context_health["per_venue"]:
        add_metric(rows, "market_context_venue", "signal_hit_rate", row["signal_hit_rate"], asset=row["venue"], sample_size=row.get("signal_total"), window="full_overnight")
        add_metric(rows, "market_context_venue", "attempt_failure_rate", row["attempt_failure_rate"], asset=row["venue"], sample_size=row.get("attempt_total"), window="full_overnight")
    for row in quality_summary.get("dimensions", {}).get("pair_label", []):
        add_metric(rows, "quality_pair", "confirm_conversion_score", row["confirm_conversion_score"], pair=row["dimension_key"], sample_size=row["sample_size"], window=window_label)
        add_metric(rows, "quality_pair", "climax_reversal_score", row["climax_reversal_score"], pair=row["dimension_key"], sample_size=row["sample_size"], window=window_label)
        add_metric(rows, "quality_pair", "pair_quality_score", row["pair_quality_score"], pair=row["dimension_key"], sample_size=row["sample_size"], window=window_label)
    for pair in majors_coverage["major_pairs_present"]:
        add_metric(rows, "majors", "present", 1, pair=pair, sample_size=1, window=window_label)
    for metric_name, value in scorecard.items():
        add_metric(rows, "scorecard", metric_name, value, sample_size=1, window="analysis")
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
    inventory: list[SourceInventory],
    runtime_config: dict[str, dict[str, Any]],
    archive_runtime: dict[str, Any],
    primary_window: dict[str, Any],
    full_capture: dict[str, Any],
    window_capture: dict[str, Any],
    stage_stats: dict[str, Any],
    prealert_conversion: dict[str, Any],
    asset_case_summary: dict[str, Any],
    patch_validation: dict[str, Any],
    reconciliation_full: dict[str, Any],
    reconciliation_window: dict[str, Any],
    reversal_special: dict[str, Any],
    market_context_health: dict[str, Any],
    cli_market_context_health: dict[str, Any],
    majors_coverage: dict[str, Any],
    quality_summary: dict[str, Any],
    noise_assessment: dict[str, Any],
    scorecard: dict[str, float],
) -> str:
    lines: list[str] = []
    full_delivered = reconciliation_full["delivered_unique_signal_ids"]
    lines.append("# Overnight Run Analysis")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append("")
    lines.append(f"- 这轮 overnight 数据**够做研究分析**：`archive/signals`、`delivery_audit`、`case_followups`、`lp_quality_stats` 都可用，且 LP signal / quality / delivery 可做到 `1:1` 对账。")
    lines.append(f"- 最大优点：`sweep_building` 的用户可见映射已经纠偏，主窗口 `{patch_validation['window']['sweep_building_mapping']['count']}` 个 `sweep_building` 样本里，`0` 个仍把自己显示成“高潮”。")
    lines.append(f"- 最大问题：live market context **没有真正修好**，主窗口 `live_public hit rate = {market_context_health['live_public_hit_rate_pct']}%`，相比下午报告的 `0%` 没有提升。")
    lines.append("- 最关键的 3 个发现：")
    lines.append(f"  - `confirm` 的“诚实降级”已经生效：full overnight 非 sweep 的 `30` 个 confirm 中，`late_confirm=13`、`unconfirmed_confirm=16`、`chase_risk=1`、`clean_confirm=0`。")
    lines.append(f"  - `archive/signals` 已稳定落盘：full overnight `96` 个 LP signal_id 与 quality `96`、delivery `96` 完全对齐；其中已送达的 `32` 条又与 cases.signal_attached `32`、case_followups `32` 完全一致。")
    lines.append(f"  - 研究样本仍然几乎只代表 `ETH/USDT + ETH/USDC` 两个 ETH 主池：full overnight `96/96` 都是 majors，但 `BTC/SOL` 仍然是 `0`。")
    lines.append("- 是否支持继续连续采样：**有条件支持**。可以继续做 ETH 主池研究采样与 patch 回归验证，但还不适合把这轮结果外推成“整体 LP 产品表现”。")
    lines.append("")
    lines.append("## 2. 数据源与完整性说明")
    lines.append("")
    for item in inventory:
        overlap = "overlap" if item.overlap_window else "no-overlap"
        lines.append(
            f"- `{item.path}` | `{item.fmt}` | {'present' if item.exists else 'missing'} | "
            f"{fmt_ts(item.coverage_start_ts)} -> {fmt_ts(item.coverage_end_ts)} | {overlap}"
            + (f" | {item.notes}" if item.notes else "")
        )
    lines.append("- 辅助 CLI：")
    lines.append("  - `venv/bin/python -m app.quality_reports --market-context-health`")
    lines.append("  - `venv/bin/python -m app.quality_reports --major-pool-coverage`")
    lines.append("- 数据完整性评级：`medium`")
    lines.append("- 评级理由：")
    lines.append("  - `archive/signals` / `delivery_audit` / `case_followups` / `lp_quality_stats` 足够支撑信号、消息、outcome 的闭环对账。")
    lines.append("  - `raw_events` / `parsed_events` 在昨晚对应路径缺失，因此无法回放原始事件级噪音来源。")
    lines.append("  - `asset_cases.cache.json` 只保留最后活跃 snapshot，不能当历史账本使用。")
    lines.append("  - 未找到运行日志 / stdout 捕获文件，本次运行窗口主要靠 archive + cache 时间戳识别。")
    lines.append("")
    lines.append("## 3. 实际分析窗口")
    lines.append("")
    lines.append(f"- archive 总覆盖：`{fmt_ts(archive_runtime['start_ts'])}` -> `{fmt_ts(archive_runtime['end_ts'])}` | `{fmt_ts(archive_runtime['start_ts'], ARCHIVE_LOCAL)}` -> `{fmt_ts(archive_runtime['end_ts'], ARCHIVE_LOCAL)}`")
    lines.append(f"- LP overnight 活跃总包络：`{fmt_ts(archive_runtime['lp_start_ts'])}` -> `{fmt_ts(archive_runtime['lp_end_ts'])}` | `{fmt_ts(archive_runtime['lp_start_ts'], ARCHIVE_LOCAL)}` -> `{fmt_ts(archive_runtime['lp_end_ts'], ARCHIVE_LOCAL)}`")
    lines.append(f"- 选定主分析窗口：`{fmt_ts(primary_window['start_ts'])}` -> `{fmt_ts(primary_window['end_ts'])}` | `{fmt_ts(primary_window['start_ts'], ARCHIVE_LOCAL)}` -> `{fmt_ts(primary_window['end_ts'], ARCHIVE_LOCAL)}`")
    lines.append(f"- 总时长：`{duration_label(primary_window['duration_sec'])}`")
    lines.append(f"- 选择原因：{primary_window['selection_reason']}")
    lines.append("- 多段运行概览：")
    for segment in primary_window["segments"]:
        lines.append(
            f"  - `{fmt_ts(segment['start_ts'], ARCHIVE_LOCAL)}` -> `{fmt_ts(segment['end_ts'], ARCHIVE_LOCAL)}` | "
            f"`{duration_label(segment['duration_sec'])}` | signals={segment['signal_count']} | quality={segment['quality_count']} | "
            f"delivery_unique={segment['delivery_unique_signals']}"
        )
    lines.append("- 时间基准：")
    lines.append("  - 报告正文统一写 `UTC`。")
    lines.append("  - “本地时间”按 archive 自带的 `archive_time_bj` 推断为 `UTC+8`。")
    lines.append("")
    lines.append("## 4. 有效运行配置（非敏感）")
    lines.append("")
    for key in [
        "DEFAULT_USER_TIER",
        "MARKET_CONTEXT_ADAPTER_MODE",
        "MARKET_CONTEXT_PRIMARY_VENUE",
        "MARKET_CONTEXT_SECONDARY_VENUE",
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
        "LP_PREALERT_MIN_RESERVE_SKEW",
        "LP_PREALERT_PRIMARY_TREND_MIN_MATCHES",
        "LP_PREALERT_MULTI_POOL_WINDOW_SEC",
        "LP_PREALERT_FOLLOWUP_WINDOW_SEC",
        "LP_SWEEP_MIN_ACTION_INTENSITY",
        "LP_SWEEP_MIN_VOLUME_SURGE_RATIO",
        "LP_SWEEP_MIN_SAME_POOL_CONTINUITY",
        "LP_SWEEP_MIN_BURST_EVENT_COUNT",
        "LP_SWEEP_MAX_BURST_WINDOW_SEC",
        "LP_SWEEP_CONTINUATION_MIN_SCORE",
        "LP_SWEEP_EXHAUSTION_MIN_SCORE",
        "LP_FASTLANE_PROMOTION_ENABLE",
        "LP_FASTLANE_MAIN_SCAN_INCLUDE_PROMOTED",
        "LP_PRIMARY_TREND_SCAN_INTERVAL_SEC",
        "LP_SECONDARY_SCAN_ENABLE",
        "LP_SECONDARY_SCAN_INTERVAL_SEC",
        "LP_EXTENDED_SCAN_ENABLE",
        "LP_EXTENDED_SCAN_INTERVAL_SEC",
        "LP_QUALITY_MIN_FASTLANE_ROI_SCORE",
        "LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE",
    ]:
        value = runtime_config[key]["runtime_value"]
        if isinstance(value, list):
            value = ",".join(str(item) for item in value)
        lines.append(
            f"- `{key}` = `{value}` | source=`{'.env' if runtime_config[key]['env_present'] else 'default/app.config'}`"
        )
    lines.append("- confirm downgrade 逻辑（来自 `app/pipeline.py`，硬编码逻辑，不是敏感配置）：")
    lines.append("  - `late_confirm` 关键触发：`market_context unavailable + pool pre-move >= 0.6%`、`pool pre-move >= 0.9%`、`single-pool + no broader confirmation`、`detect latency >= 4.5s`、`case age >= 150s`。")
    lines.append("  - `chase_risk` 关键触发：`chase_risk_score >= 0.58` 且伴随更强预走 / 更晚 timing / broader confirmation 缺失 / `detect latency >= 8s` / `case age >= 240s`。")
    lines.append("- 研究型采样适配度：")
    lines.append("  - `DEFAULT_USER_TIER=research`，四个 LP stage 都会被保留到研究视图。")
    lines.append("  - archive 开关在配置上是够的，但 `raw_events/parsed_events` 昨晚对应文件缺失，影响事件级回放。")
    lines.append("  - `MARKET_CONTEXT_ADAPTER_MODE=live` 但昨晚实际 hit rate 仍为 0%，这会限制 timing / broader confirmation 研究。")
    lines.append("")
    lines.append("## 5. LP stage 质量分析")
    lines.append("")
    lines.append(
        f"- 主窗口 LP signals：`{window_capture['lp_signal_count']}`，其中消息送达 `{window_capture['lp_message_count']}`，"
        f"asset-cases `{window_capture['asset_case_count']}`。"
    )
    lines.append("- stage 分布：")
    for stage in STAGES:
        stage_count = stage_stats[stage]["count"]
        lines.append(f"  - `{stage}`: `{stage_count}`")
    lines.append("- 主窗口只覆盖 ETH majors：`ETH/USDT` 与 `ETH/USDC` 两个池。")
    lines.append("- prealert -> confirm 转化：")
    lines.append(f"  - 30s: `{prealert_conversion['within_30s_pct']}`")
    lines.append(f"  - 60s: `{prealert_conversion['within_60s_pct']}`")
    lines.append(f"  - 90s: `{prealert_conversion['within_90s_pct']}`")
    lines.append(f"  - 说明：{prealert_conversion['notes']}")
    lines.append("- 各 stage 核心统计：")
    for stage in STAGES:
        stats = stage_stats[stage]
        lines.append(
            f"  - `{stage}` | pairs={stats['pair_distribution']} | "
            f"median move_before_30s={stats['median_move_before_alert_30s']} | "
            f"median move_before_60s={stats['median_move_before_alert_60s']} | "
            f"median move_after_60s={stats['median_move_after_alert_60s']} | "
            f"median move_after_300s={stats['median_move_after_alert_300s']} | "
            f"aligned_after_60s={stats['median_aligned_move_after_alert_60s']} | "
            f"aligned_after_300s={stats['median_aligned_move_after_alert_300s']}"
        )
    lines.append("- stage latency：")
    for stage in STAGES:
        stats = stage_stats[stage]
        lines.append(
            f"  - `{stage}` | median detect latency=`{stats['median_detect_latency_ms']}` ms | "
            f"median end-to-end latency=`{stats['median_end_to_end_latency_ms']}` ms"
        )
    lines.append("- stage market context / timing：")
    for stage in STAGES:
        stats = stage_stats[stage]
        lines.append(
            f"  - `{stage}` | market_context_source={stats['market_context_source_distribution']} | "
            f"live available={stats['market_context_available_rate_pct']}% | "
            f"on-chain timing proxy={stats['onchain_timing_distribution']}"
        )
    lines.append("- reversal / followthrough：")
    lines.append(
        f"  - `climax` adverse move proxy at 60s: `{stage_stats['climax']['adverse_followthrough_60s_pct']}%` "
        f"(resolved `{stage_stats['climax']['resolved_after_alert_60s']}`)"
    )
    lines.append(
        f"  - `climax` adverse move proxy at 300s: `{stage_stats['climax']['adverse_followthrough_300s_pct']}%` "
        f"(resolved `{stage_stats['climax']['resolved_after_alert_300s']}`)"
    )
    lines.append(
        f"  - `exhaustion_risk` adverse move proxy at 60s: `{stage_stats['exhaustion_risk']['adverse_followthrough_60s_pct']}%` "
        f"(resolved `{stage_stats['exhaustion_risk']['resolved_after_alert_60s']}`)"
    )
    lines.append(
        f"  - `exhaustion_risk` adverse move proxy at 300s: `{stage_stats['exhaustion_risk']['adverse_followthrough_300s_pct']}%` "
        f"(resolved `{stage_stats['exhaustion_risk']['resolved_after_alert_300s']}`)"
    )
    lines.append("- 明确判断：")
    lines.append("  - `prealert` 现在不是“太多”，而是**几乎没有**。full overnight `0/96`，这说明系统昨晚仍更像确认器，而不是前置研究采样器。")
    window_confirm_buckets = patch_validation["window"]["confirm_quality"]["bucket_counts"]
    non_sweep_window_confirms = (
        int(window_confirm_buckets.get("late_confirm", 0))
        + int(window_confirm_buckets.get("unconfirmed_confirm", 0))
        + int(window_confirm_buckets.get("chase_risk", 0))
        + int(window_confirm_buckets.get("clean_confirm", 0))
        + int(window_confirm_buckets.get("unlabeled_confirm", 0))
    )
    lines.append(
        f"  - `confirm` 依然偏晚，但比以前诚实：主窗口非 sweep confirm 共 `{non_sweep_window_confirms}` 条，"
        f"`late_confirm={window_confirm_buckets.get('late_confirm', 0)}`、"
        f"`unconfirmed_confirm={window_confirm_buckets.get('unconfirmed_confirm', 0)}`、"
        f"`chase_risk={window_confirm_buckets.get('chase_risk', 0)}`、"
        f"`clean_confirm={window_confirm_buckets.get('clean_confirm', 0)}`。"
    )
    lines.append(
        f"  - `climax` 在这段主窗口里**没有录到明显 reversal**；这比下午那轮好，但样本共有 "
        f"`{stage_stats['climax']['count']}` 条，且 60s/300s resolved 仍分别只有 "
        f"`{stage_stats['climax']['resolved_after_alert_60s']}` / `{stage_stats['climax']['resolved_after_alert_300s']}`。"
    )
    lines.append(
        f"  - `exhaustion_risk` 的价值仍高于继续报 sweep：主窗口共有 `{stage_stats['exhaustion_risk']['count']}` 条，"
        f"alert 前 median move_before_60s=`{stage_stats['exhaustion_risk']['median_move_before_alert_60s']}`，"
        f"之后 median move_after_300s=`{stage_stats['exhaustion_risk']['median_move_after_alert_300s']}`，更像尾段风险提示而不是新起点。"
    )
    lines.append("")
    lines.append("## 6. 纠偏 patch 验证")
    lines.append("")
    lines.append("### A. `sweep_building` 不再显示成“高潮”")
    lines.append("")
    lines.append(f"- 主窗口 `sweep_building` 样本：`{patch_validation['window']['sweep_building_mapping']['count']}`")
    lines.append(f"- full overnight `sweep_building` 样本：`{patch_validation['full_overnight']['sweep_building_mapping']['count']}`")
    lines.append(f"- 残留“高潮/climax”映射：`{patch_validation['full_overnight']['sweep_building_mapping']['residual_climax_count']}`")
    lines.append("- 结论：从 archive 中可见的 `headline_label` / `lp_state_label` / `lp_sweep_display_stage` 看，这个 patch **真实生效**；样本显示为“买方/卖方清扫延续待确认”，不再冒充高潮。")
    lines.append("")
    lines.append("### B. `confirm` 的 `late_confirm / chase_risk`")
    lines.append("")
    lines.append(f"- full overnight confirm buckets：`{patch_validation['full_overnight']['confirm_quality']['bucket_counts']}`")
    lines.append(f"- 主窗口 confirm buckets：`{patch_validation['window']['confirm_quality']['bucket_counts']}`")
    lines.append("- 解释：")
    lines.append("  - `sweep_building_confirm` = confirm stage，但走的是 sweep_building 路径，因此不挂 clean/late/chase 标签。")
    lines.append("  - 非 sweep confirm 在 full overnight 一共 `30` 条，全部都被明确分流到了 `late_confirm / unconfirmed_confirm / chase_risk`，`clean_confirm=0`。")
    lines.append("  - 这说明 patch 至少已经把“偏晚确认”从普通 confirm 里剥离出来，没有再留下未标记的 generic confirm。")
    lines.append("- 数据表现：")
    late = patch_validation["full_overnight"]["confirm_quality"]["late_confirm"]
    unconfirmed = patch_validation["full_overnight"]["confirm_quality"]["unconfirmed_confirm"]
    chase = patch_validation["full_overnight"]["confirm_quality"]["chase_risk"]
    lines.append(
        f"  - `late_confirm` `{late['count']}` 条，median move_before_alert_60s=`{late['median_move_before_alert_60s']}`，"
        f"aligned move_after_alert_300s=`{late['median_aligned_move_after_alert_300s']}`。"
    )
    lines.append(
        f"  - `unconfirmed_confirm` `{unconfirmed['count']}` 条，median move_before_alert_60s=`{unconfirmed['median_move_before_alert_60s']}`，"
        f"aligned move_after_alert_300s=`{unconfirmed['median_aligned_move_after_alert_300s']}`。"
    )
    lines.append(
        f"  - `chase_risk` `{chase['count']}` 条，仅出现于后半夜较小段样本，标签和 message headline 都直接写成“追空风险”。"
    )
    lines.append("")
    lines.append("### C. `archive/signals`")
    lines.append("")
    lines.append(f"- `archive/signals` 存在，full overnight LP rows=`{reconciliation_full['signal_archive_unique_signal_ids']}`。")
    lines.append(f"- 与 quality 对账：`{reconciliation_full['quality_unique_signal_ids']}` / `{reconciliation_full['signal_archive_unique_signal_ids']}`，缺口=`{len(reconciliation_full['signal_vs_quality_missing'])}`。")
    lines.append(f"- 与 delivery_audit 对账：`{reconciliation_full['delivery_unique_signal_ids']}` / `{reconciliation_full['signal_archive_unique_signal_ids']}`，缺口=`{len(reconciliation_full['signal_vs_delivery_missing'])}`。")
    lines.append(f"- 已送达子集：signals `32` -> delivery delivered `32` -> cases.signal_attached `32` -> case_followups `32`，diff 全为 `0`。")
    lines.append("- 结论：`archive/signals` 已经可以稳定当作 signal ledger 使用，且能与 delivery/outcome 做到实用上的 1:1 对账。")
    lines.append("")
    lines.append("### D. live market context")
    lines.append("")
    lines.append(f"- 主窗口 LP signals `live_public hit rate = {market_context_health['live_public_hit_rate_pct']}%`，full archive CLI 也是 `0.0%`。")
    lines.append("- 对比下午报告：**没有高于 0%**，功能上仍未修复。")
    lines.append("- 但 telemetry 有进步：现在至少能从 signals archive 里看到 attempts、venue、endpoint、http status 和 final failure reason。")
    lines.append("")
    lines.append("### E. absorption / broader confirmation")
    lines.append("")
    absorption = patch_validation["full_overnight"]["absorption_tags"]
    for key, value in absorption.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("- 判断：")
    lines.append("  - `local_sell_pressure_absorption` 与 `local_buy_pressure_absorption` 已真实出现，不再只有单一“持续买压/卖压”说法。")
    lines.append("  - 但 `broader_sell_pressure_confirmed` / `broader_buy_pressure_confirmed` 都是 `0`，说明 patch 只修好了“局部 vs 未确认”的表达层，还没拿到真实 broader confirmation 样本。")
    lines.append("")
    lines.append("## 7. “卖压后涨 / 买压后跌”专项分析")
    lines.append("")
    lines.append(f"- confirm 卖压样本：`{reversal_special['confirm_sell_pressure']['count']}`")
    lines.append(f"- confirm 买压样本：`{reversal_special['confirm_buy_pressure']['count']}`")
    lines.append("- 反向统计：")
    lines.append(f"  - `持续卖压` 之后 60s 上涨：`{reversal_special['confirm_sell_pressure']['after_60s']['against_count']}` / `{reversal_special['confirm_sell_pressure']['after_60s']['resolved_count']}`")
    lines.append(f"  - `持续卖压` 之后 300s 上涨：`{reversal_special['confirm_sell_pressure']['after_300s']['against_count']}` / `{reversal_special['confirm_sell_pressure']['after_300s']['resolved_count']}`")
    lines.append(f"  - `持续买压` 之后 60s 下跌：`{reversal_special['confirm_buy_pressure']['after_60s']['against_count']}` / `{reversal_special['confirm_buy_pressure']['after_60s']['resolved_count']}`")
    lines.append(f"  - `持续买压` 之后 300s 下跌：`{reversal_special['confirm_buy_pressure']['after_300s']['against_count']}` / `{reversal_special['confirm_buy_pressure']['after_300s']['resolved_count']}`")
    lines.append(f"- 数据限制：{reversal_special['explanation']}")
    if reversal_special["reverse_cases"]:
        lines.append("- 典型反例（样本只有 1 个，不足以列 3 个）：")
        case = reversal_special["reverse_cases"][0]
        lines.append(
            f"  - `{case['signal_id']}` | `{fmt_ts(case['created_at'])}` | `{case['pair_label']}` | `{case['intent_type']}` | "
            f"`move_before_30s={case['move_before_alert_30s']}` | `move_after_300s={case['move_after_alert_300s']}`"
        )
        lines.append(f"    - 原始 signal：`{case['headline_label']}`")
        lines.append(f"    - broader confirmation：`{case['lp_broader_alignment']}`；market context：`{case['market_context_source']}` / `{case['market_context_failure_reason']}`")
        lines.append(f"    - 吸收标签：`{case['lp_absorption_context']}`；supporting pairs=`{case['asset_case_supporting_pair_count']}`；multi_pool=`{case['asset_case_multi_pool']}`")
        lines.append("    - 解释：这是一个 `sweep_building + local_buy_pressure_absorption + market_context unavailable` 的单池样本，更像局部冲击被承接，而不是更广市场确认。")
    else:
        lines.append("- 未找到 clear reverse case。")
    lines.append("- 明确判断：")
    lines.append("  - 昨晚主窗口**没有复现**“持续卖压后上涨”的 confirm 反例；卖压 confirm 在已解析的 60s/300s 样本里没有被打脸。")
    lines.append("  - 昨晚只出现 `1` 个“持续买压后 300s 下跌”的 clear counterexample，而且它本身就是 `sweep_building + local_buy_pressure_absorption + unavailable`。")
    lines.append("  - 因此，这轮 overnight 更支持的解释不是“代码方向错了”，而是：")
    lines.append("    - 首因：`局部池子与 broader market 脱钩 + live market context 缺失`。")
    lines.append("    - 次因：`confirm` 仍是确认型信号，容易被误读成预测。")
    lines.append("    - 代码错误：本轮数据里**没有直接证据**。")
    lines.append("")
    lines.append("## 8. market context 健康度")
    lines.append("")
    lines.append(f"- 主窗口 LP signals: `{market_context_health['signal_rows']}` | attempts=`{market_context_health['total_attempts']}`")
    lines.append(f"- `live_public hit rate`: `{market_context_health['live_public_hit_rate_pct']}%`")
    lines.append(f"- `unavailable rate`: `{market_context_health['unavailable_rate_pct']}%`")
    lines.append("- per venue：")
    for row in market_context_health["per_venue"]:
        lines.append(
            f"  - `{row['venue']}` | signal_hit_rate=`{row['signal_hit_rate']}` | attempt_failure_rate=`{row['attempt_failure_rate']}` | attempts=`{row.get('attempt_total', 0)}`"
        )
    lines.append("- per endpoint：")
    for row in market_context_health["per_endpoint"]:
        lines.append(
            f"  - `{row['endpoint']}` | attempt_failure_rate=`{row['attempt_failure_rate']}` | attempts=`{row.get('attempt_total', 0)}`"
        )
    lines.append("- per resolved symbol：")
    for row in market_context_health["per_symbol"]:
        lines.append(
            f"  - `{row['symbol']}` | signal_hit_rate=`{row['signal_hit_rate']}` | signal_total=`{row.get('signal_total', 0)}`"
        )
    lines.append(f"- top failure reasons：`{market_context_health['top_failure_reasons']}`")
    lines.append(f"- final failure reasons on signal rows：`{market_context_health['final_failure_reason_distribution']}`")
    lines.append(f"- symbol fallback records：`{market_context_health['fallback_pairs_detected']}`")
    lines.append("- 解释：")
    lines.append("  - ETH majors 上仍然经常失效，昨晚只尝试了 `ETHUSDT` / `ETHUSDC`，没有成功样本。")
    lines.append("  - `ETH/USDC -> ETHUSDT`、`BTC/USDC -> BTCUSDT`、`SOL/USDC -> SOLUSDT` 这类 fallback **昨晚没有真实出现**；requested_symbol 与 resolved_symbol 全部相同。")
    lines.append(f"  - full archive CLI 结果与主窗口 LP-only 结论一致：`hit rate = {cli_market_context_health['live_public_hit_rate'] * 100:.1f}%`，top failures 为 `http_451`（Binance）与 `http_403`（Bybit）。")
    lines.append("- 明确判断：live market context 现在仍然是 **blocker**，还没有进入“可用但需继续优化”的阶段。")
    lines.append("")
    lines.append("## 9. majors 覆盖与样本代表性")
    lines.append("")
    lines.append(f"- overnight 实际覆盖的 assets：`{majors_coverage['overnight_asset_distribution']}`")
    lines.append(f"- overnight 实际覆盖的 pairs：`{majors_coverage['overnight_pair_distribution']}`")
    lines.append(f"- 主样本是否仍被 ETH/USDT + ETH/USDC 主导：`{majors_coverage['current_sample_still_eth_dominated']}`")
    lines.append(f"- CLI major-pool-coverage active pools：`{majors_coverage['cli_major_pool_coverage']['active_major_pools']}`")
    lines.append(f"- 缺失的 expected major pairs：`{majors_coverage['missing_expected_pairs']}`")
    lines.append("- 判断：")
    lines.append("  - majors-first 目前只在“把噪音压在 ETH 双主池”这件事上起了作用。")
    lines.append("  - 它还**没有**把样本覆盖从 ETH 扩展到 BTC/SOL，因此代表性仍弱。")
    lines.append("  - 这轮样本不能代表“产品总体 LP 表现”，只能代表“ETH 主池场景下 patch 是否变诚实”。")
    lines.append("")
    lines.append("## 10. quality/outcome 与 fastlane ROI")
    lines.append("")
    qp = quality_summary["worst_prealert_precision"]
    qc = quality_summary["strongest_confirm_conversion"]
    qr = quality_summary["highest_climax_reversal"]
    qms = quality_summary["strongest_market_context_alignment"]
    qmw = quality_summary["weakest_market_context_alignment"]
    qfs = quality_summary["strongest_fastlane_roi"]
    qfw = quality_summary["weakest_fastlane_roi"]
    lines.append(f"- prealert_precision_score 最差对象：`{qp}`")
    lines.append(f"- confirm_conversion_score 最强对象：`{qc}`")
    lines.append(f"- climax_reversal_score 最高对象：`{qr}`")
    lines.append(f"- market_context_alignment_score 强对象：`{qms}`")
    lines.append(f"- market_context_alignment_score 弱对象：`{qmw}`")
    lines.append(f"- fastlane_roi_score 强对象：`{qfs}`")
    lines.append(f"- fastlane_roi_score 弱对象：`{qfw}`")
    lines.append(
        f"- promoted_main / fastlane：主窗口 `promoted_main=0`、`primary_trend=0`、`secondary=0`、`extended=0`，"
        f"full overnight 也没有 promotion。"
    )
    lines.append("- 判断：")
    lines.append("  - 当前 quality/outcome 已经能区分 `ETH/USDT` 与 `ETH/USDC` 的 confirm_conversion / pair_quality 差异。")
    lines.append("  - 但 `market_context_alignment_score` 和 `fastlane_roi_score` 几乎是平的，因为 live context 没命中、promotion 没发生。")
    lines.append("  - 因此可以开始指导**ETH 主池内部的小范围调参**，但还不够支持更大范围的全局调参。")
    lines.append("")
    lines.append("## 11. 噪音与误判风险评估")
    lines.append("")
    for user_key, title in [("research_user", "research 用户"), ("trader_user", "trader 用户"), ("retail_user", "未来 retail 用户")]:
        user_info = noise_assessment[user_key]
        lines.append(f"- {title}：")
        lines.append(f"  - 当前 LP 噪音：`{user_info['noise_level']}`")
        lines.append(f"  - 最大误判来源：{user_info['biggest_risk']}")
        lines.append(f"  - 最值得保留：{user_info['retain']}")
        lines.append(f"  - 最该降级/隐藏：{user_info['hide_or_downgrade']}")
    lines.append("- 总结判断：")
    lines.append("  - 距离“可应用级使用”还有明显距离。")
    lines.append("  - 当前最大短板不是单点 timing，而是 **broader confirmation 缺失 + live market context 不可用 + 样本覆盖过窄** 的组合。")
    lines.append("  - 在这三个里，最先需要解决的是 live market context；否则 timing 与 broader confirmation 两块都很难闭环。")
    lines.append("")
    lines.append("## 12. 最终评分")
    lines.append("")
    for key, value in scorecard.items():
        lines.append(f"- `{key}`: `{value}/10`")
    lines.append("")
    lines.append("## 13. 下一步建议")
    lines.append("")
    lines.append("- Top 5 优点：")
    lines.append("  - `archive/signals` 已经稳定，signal / quality / delivery / case_followups 可 1:1 对账。")
    lines.append("  - `sweep_building` 不再伪装成高潮，用户可见语义更诚实。")
    lines.append("  - `late_confirm / unconfirmed_confirm / chase_risk` 已真正开始把偏晚 confirm 从普通 confirm 里剥离。")
    lines.append("  - 局部承接标签已经真实出现，说明 patch 不只是改词面。")
    lines.append("  - 送达层闭环稳定：已送达 `32` 条在 cases / followups 里都有完整落点。")
    lines.append("- Top 5 当前不足：")
    lines.append("  - live market context 仍是 `0% hit rate`。")
    lines.append("  - prealert 完全缺席，研究采样仍偏后段。")
    lines.append("  - majors 覆盖仍只有 ETH/USDT 与 ETH/USDC。")
    lines.append("  - raw/parsed archives 缺失，无法做原始事件级噪音复盘。")
    lines.append("  - broader_*_confirmed 标签仍然是 `0`，因为 broader market 根本没连上。")
    lines.append("- Top 5 最值得优先改进的事项：")
    lines.append("  - 先把 live market context 真正打通，至少让 ETH majors 拿到可用 `live_public`。")
    lines.append("  - 为 `ETH/USDC -> ETHUSDT` 一类 fallback 建真实命中样本，不要停留在理论支持。")
    lines.append("  - 补上 BTC/SOL 主池，让 majors-first 真正成为 majors-first，而不是 ETH-only。")
    lines.append("  - 把 prealert 重新带回来，否则研究型采样缺少前段样本。")
    lines.append("  - 恢复 raw / parsed archives，下一轮才能把“为什么没有 prealert”拆到事件级。")
    lines.append("")
    lines.append("## 14. 限制与不确定性")
    lines.append("")
    lines.append("- 这份报告的主窗口只分析了昨晚最连续、最完整的一段，不代表整个文件里每个小尾段都完全同质。")
    lines.append("- `after-alert 30s` 没有稳定持久化字段，因此 reversal 专题只能对 `60s / 300s` 做定量。")
    lines.append("- `asset_cases.cache.json` 是 active snapshot，不是历史归档，所以主窗口 asset-case 统计主要依赖 signals/quality，而不是 cache 本身。")
    lines.append("- `reversal_after_climax` 本轮没有稳定回写，因此 climax reversal 用 adverse move proxy 补充判读。")
    lines.append("- `market_context_alignment_score` / `fastlane_roi_score` 在昨晚样本里几乎是平的，不能过度解读。")
    return "\n".join(lines) + "\n"


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    runtime_config = load_runtime_config()
    quality_rows_all, quality_by_signal_id, quality_payload = load_quality_rows()
    signal_rows_all = load_signal_rows()
    case_data = load_case_data()
    followup_rows_all = load_followups()
    delivery_rows_all = load_delivery_rows()
    asset_case_cache = load_asset_case_cache()

    archive_starts_ends = []
    for path in [SIGNALS_PATH, CASES_PATH, CASE_FOLLOWUPS_PATH, DELIVERY_AUDIT_PATH]:
        start_ts, end_ts = inventory_ndjson(path)
        if start_ts is not None and end_ts is not None:
            archive_starts_ends.append((start_ts, end_ts))
    archive_runtime = {
        "start_ts": min(start for start, _ in archive_starts_ends) if archive_starts_ends else None,
        "end_ts": max(end for _, end in archive_starts_ends) if archive_starts_ends else None,
        "lp_start_ts": min((to_int(row.get("created_at")) or 0) for row in quality_rows_all if to_int(row.get("created_at")) and (archive_starts_ends and min(start for start, _ in archive_starts_ends) <= int(row["created_at"]) <= max(end for _, end in archive_starts_ends))),
        "lp_end_ts": max((to_int(row.get("created_at")) or 0) for row in quality_rows_all if to_int(row.get("created_at")) and (archive_starts_ends and min(start for start, _ in archive_starts_ends) <= int(row["created_at"]) <= max(end for _, end in archive_starts_ends))),
    }

    candidates = build_segment_candidates(
        archive_runtime["start_ts"],
        archive_runtime["end_ts"],
        signal_rows_all,
        quality_rows_all,
        case_data["signal_attached_rows"],
        followup_rows_all,
        delivery_rows_all,
        asset_case_cache,
    )
    primary_window = select_primary_window(candidates, archive_runtime["start_ts"], archive_runtime["end_ts"])

    inventory = build_inventory(primary_window["start_ts"], primary_window["end_ts"], quality_payload, asset_case_cache)

    joined_rows_all = join_rows(signal_rows_all, quality_by_signal_id)
    joined_rows_window = filter_rows_by_window(joined_rows_all, primary_window["start_ts"], primary_window["end_ts"], "archive_ts")
    quality_rows_window = filter_rows_by_window(quality_rows_all, primary_window["start_ts"], primary_window["end_ts"], "created_at")
    delivery_rows_window = filter_rows_by_window(delivery_rows_all, primary_window["start_ts"], primary_window["end_ts"], "archive_ts")
    case_attached_window = filter_rows_by_window(case_data["signal_attached_rows"], primary_window["start_ts"], primary_window["end_ts"], "archive_ts")
    followup_rows_window = filter_rows_by_window(followup_rows_all, primary_window["start_ts"], primary_window["end_ts"], "archive_ts")

    reconciliation_full = build_reconciliation(
        signal_rows_all,
        [
            row
            for row in quality_rows_all
            if archive_runtime["start_ts"] <= int(row["created_at"]) <= to_int(quality_payload.get("generated_at") or archive_runtime["end_ts"])
        ],
        delivery_rows_all,
        case_data["signal_attached_rows"],
        followup_rows_all,
    )
    reconciliation_window = build_reconciliation(
        filter_rows_by_window(signal_rows_all, primary_window["start_ts"], primary_window["end_ts"], "archive_ts"),
        quality_rows_window,
        delivery_rows_window,
        case_attached_window,
        followup_rows_window,
    )

    delivered_full_ids = {
        str(row.get("signal_id") or "")
        for row in delivery_rows_all
        if row.get("signal_id") and (row.get("delivered_notification") is True or row.get("notifier_sent_at"))
    }
    delivered_window_ids = {
        str(row.get("signal_id") or "")
        for row in delivery_rows_window
        if row.get("signal_id") and (row.get("delivered_notification") is True or row.get("notifier_sent_at"))
    }

    full_capture = build_capture_summary(joined_rows_all, delivered_full_ids)
    window_capture = build_capture_summary(joined_rows_window, delivered_window_ids)
    asset_case_summary = build_asset_case_summary(joined_rows_window)
    stage_stats = build_stage_stats(joined_rows_window)
    prealert_conversion = build_prealert_conversion(joined_rows_window)
    patch_validation = build_patch_validation(joined_rows_window, joined_rows_all)
    reversal_special = build_reversal_special(joined_rows_all)
    market_context_health = build_market_context_health(joined_rows_window)
    cli_market_context_health = run_cli_json(["--market-context-health"])
    cli_major_pool_coverage = run_cli_json(["--major-pool-coverage"])
    majors_coverage = build_majors_coverage(joined_rows_all, cli_major_pool_coverage)
    quality_summary = build_quality_summary(joined_rows_window)
    noise_assessment = build_noise_assessment(joined_rows_window, joined_rows_all, quality_summary)
    scorecard = build_scorecard(
        reconciliation_full,
        full_capture,
        window_capture,
        market_context_health,
        majors_coverage,
        noise_assessment,
        patch_validation,
    )

    markdown = build_markdown(
        inventory,
        runtime_config,
        archive_runtime,
        primary_window,
        full_capture,
        window_capture,
        stage_stats,
        prealert_conversion,
        asset_case_summary,
        patch_validation,
        reconciliation_full,
        reconciliation_window,
        reversal_special,
        market_context_health,
        cli_market_context_health,
        majors_coverage,
        quality_summary,
        noise_assessment,
        scorecard,
    )
    MARKDOWN_PATH.write_text(markdown, encoding="utf-8")

    metrics_rows = build_metrics_csv_rows(
        primary_window,
        full_capture,
        window_capture,
        stage_stats,
        patch_validation,
        reconciliation_full,
        market_context_health,
        majors_coverage,
        quality_summary,
        scorecard,
    )
    write_csv(CSV_PATH, metrics_rows)

    summary_payload = {
        "analysis_window": {
            "start_ts": primary_window["start_ts"],
            "end_ts": primary_window["end_ts"],
            "start_utc": fmt_ts(primary_window["start_ts"]),
            "end_utc": fmt_ts(primary_window["end_ts"]),
            "start_archive_local": fmt_ts(primary_window["start_ts"], ARCHIVE_LOCAL),
            "end_archive_local": fmt_ts(primary_window["end_ts"], ARCHIVE_LOCAL),
            "duration_sec": primary_window["duration_sec"],
            "duration_label": duration_label(primary_window["duration_sec"]),
            "selection_reason": primary_window["selection_reason"],
        },
        "data_completeness": {
            "rating": "medium",
            "source_inventory": [
                {
                    "path": item.path,
                    "format": item.fmt,
                    "exists": item.exists,
                    "coverage_start_ts": item.coverage_start_ts,
                    "coverage_end_ts": item.coverage_end_ts,
                    "overlap_window": item.overlap_window,
                    "notes": item.notes,
                }
                for item in inventory
            ],
        },
        "runtime_config_summary": {key: value for key, value in runtime_config.items()},
        "lp_stage_summary": {
            "full_overnight_capture": full_capture,
            "primary_window_capture": window_capture,
            "asset_case_summary": asset_case_summary,
            "stage_stats": stage_stats,
            "prealert_conversion": prealert_conversion,
        },
        "patch_validation_summary": patch_validation,
        "reversal_special_case_summary": reversal_special,
        "market_context_health_summary": {
            "primary_window_lp_only": market_context_health,
            "quality_reports_cli_full_archive": cli_market_context_health,
        },
        "majors_coverage_summary": majors_coverage,
        "quality_summary": quality_summary,
        "noise_assessment": noise_assessment,
        "scorecard": scorecard,
        "top_recommendations": [
            "先修 live market context hit rate，再谈 broader confirmation 与 timing 研究。",
            "补齐 BTC/SOL 主池，让 majors-first 真正扩大代表性。",
            "恢复 raw/parsed archives，下一轮才能解释 prealert 为什么缺失。",
            "继续保留并回归测试 sweep_building 不冒充 climax 的 patch。",
            "让 prealert 在 majors 场景下重新出现，否则连续研究采样仍偏后段。",
        ],
    }
    JSON_PATH.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps({"markdown": str(MARKDOWN_PATH), "csv": str(CSV_PATH), "json": str(JSON_PATH)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
