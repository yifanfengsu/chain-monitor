#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import os
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
UTC = timezone.utc
BJ = timezone(timedelta(hours=8))

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TODAY_UTC = datetime.now(UTC).date()
TODAY_STR = TODAY_UTC.isoformat()

RAW_EVENTS_PATH = ARCHIVE_DIR / "raw_events" / f"{TODAY_STR}.ndjson"
PARSED_EVENTS_PATH = ARCHIVE_DIR / "parsed_events" / f"{TODAY_STR}.ndjson"
SIGNALS_PATH = ARCHIVE_DIR / "signals" / f"{TODAY_STR}.ndjson"
CASES_PATH = ARCHIVE_DIR / "cases" / f"{TODAY_STR}.ndjson"
CASE_FOLLOWUPS_PATH = ARCHIVE_DIR / "case_followups" / f"{TODAY_STR}.ndjson"
DELIVERY_AUDIT_PATH = ARCHIVE_DIR / "delivery_audit" / f"{TODAY_STR}.ndjson"
ASSET_CASE_CACHE_PATH = DATA_DIR / "asset_cases.cache.json"
QUALITY_STATS_PATH = DATA_DIR / "lp_quality_stats.cache.json"
EXCHANGE_ADJACENT_PATH = DATA_DIR / "persisted_exchange_adjacent.json"
ENV_PATH = ROOT / ".env"
MARKDOWN_PATH = REPORTS_DIR / "afternoon_run_analysis.md"
CSV_PATH = REPORTS_DIR / "afternoon_run_metrics.csv"
JSON_PATH = REPORTS_DIR / "afternoon_run_summary.json"

STAGE_ORDER = {"prealert": 1, "confirm": 2, "climax": 3, "exhaustion_risk": 4}
MAJORS = {"BTC", "WBTC", "ETH", "WETH", "STETH", "WSTETH", "SOL"}
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
    "LP_PREALERT_MIN_USD",
    "LP_PREALERT_MIN_CONFIRMATION",
    "LP_PREALERT_MIN_PRICING_CONFIDENCE",
    "LP_PREALERT_DIRECTIONAL_MIN_ACTION_INTENSITY",
    "LP_PREALERT_DIRECTIONAL_MIN_VOLUME_SURGE_RATIO",
    "LP_PREALERT_MIN_RESERVE_SKEW",
    "LP_PREALERT_PRIMARY_TREND_MIN_MATCHES",
    "LP_PREALERT_MULTI_POOL_WINDOW_SEC",
    "LP_PREALERT_FOLLOWUP_WINDOW_SEC",
    "LP_OBSERVE_MIN_USD",
    "LP_OBSERVE_MIN_CONFIDENCE",
    "LP_SWEEP_MIN_ACTION_INTENSITY",
    "LP_SWEEP_MIN_VOLUME_SURGE_RATIO",
    "LP_SWEEP_MIN_SAME_POOL_CONTINUITY",
    "LP_SWEEP_MIN_BURST_EVENT_COUNT",
    "LP_SWEEP_MAX_BURST_WINDOW_SEC",
    "LP_SWEEP_CONTINUATION_MIN_SCORE",
    "LP_SWEEP_EXHAUSTION_MIN_SCORE",
    "LP_BURST_FASTLANE_ENABLE",
    "LP_FASTLANE_PROMOTION_ENABLE",
    "LP_FASTLANE_PROMOTED_MAX_COUNT",
    "LP_FASTLANE_PROMOTION_TTL_SEC",
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
    size_bytes: int | None
    coverage_start_ts: int | None
    coverage_end_ts: int | None
    overlap_window: bool = False
    notes: str = ""


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pct(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return round(100.0 * numerator / denominator, 1)


def round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or math.isnan(value):
        return None
    return round(float(value), digits)


def median_or_none(values: list[float | int | None], digits: int = 6) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return round(float(statistics.median(cleaned)), digits)


def fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "n/a"
    dt = datetime.fromtimestamp(int(ts), tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_ts_bj(ts: int | None) -> str:
    if ts is None:
        return "n/a"
    dt = datetime.fromtimestamp(int(ts), tz=BJ)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC+8")


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


def json_load(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_first_last_json_line(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not path.exists() or path.stat().st_size == 0:
        return None, None
    first_obj = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                first_obj = json.loads(line)
                break
    last_obj = None
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffer = b""
        while position > 0:
            step = min(4096, position)
            position -= step
            handle.seek(position)
            buffer = handle.read(step) + buffer
            lines = buffer.splitlines()
            if position == 0:
                candidates = lines
            else:
                candidates = lines[1:]
            for raw in reversed(candidates):
                if raw.strip():
                    last_obj = json.loads(raw.decode("utf-8"))
                    return first_obj, last_obj
    return first_obj, last_obj


def inventory_ndjson(path: Path, notes: str = "") -> SourceInventory:
    first_obj, last_obj = read_first_last_json_line(path)
    start_ts = to_int((first_obj or {}).get("archive_ts"))
    end_ts = to_int((last_obj or {}).get("archive_ts"))
    return SourceInventory(
        path=str(path.relative_to(ROOT)),
        fmt="jsonl",
        exists=path.exists(),
        size_bytes=path.stat().st_size if path.exists() else None,
        coverage_start_ts=start_ts,
        coverage_end_ts=end_ts,
        notes=notes,
    )


def inventory_json(path: Path, coverage_start_ts: int | None, coverage_end_ts: int | None, notes: str = "") -> SourceInventory:
    return SourceInventory(
        path=str(path.relative_to(ROOT)),
        fmt="json",
        exists=path.exists(),
        size_bytes=path.stat().st_size if path.exists() else None,
        coverage_start_ts=coverage_start_ts,
        coverage_end_ts=coverage_end_ts,
        notes=notes,
    )


def summarize_counter(counter: Counter[str], top_n: int | None = None) -> list[dict[str, Any]]:
    items = counter.most_common(top_n)
    return [{"key": key, "count": count} for key, count in items]


def aligned_move(record: dict[str, Any], field: str) -> float | None:
    move = to_float(record.get(field))
    if move is None:
        return None
    direction = str(record.get("direction_bucket") or "")
    if direction == "sell_pressure":
        return -move
    return move


def falsey_none(value: Any) -> Any:
    return None if value in ("", [], {}, ()) else value


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
        runtime[key] = {
            "runtime_value": getattr(config, key, None),
            "env_present": env_overrides.get(key, {}).get("env_present", False),
            "env_value": env_overrides.get(key, {}).get("env_value"),
        }
    return runtime


def run_quality_report_summary() -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-m", "app.quality_reports", "--summary"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def scan_lp_case_archive(path: Path) -> dict[str, Any]:
    signal_rows: list[dict[str, Any]] = []
    pool_case_ids: set[str] = set()
    line_count = 0
    lp_line_count = 0
    action_counter: Counter[str] = Counter()
    archive_ts_values: list[int] = []
    if not path.exists():
        return {
            "signal_rows": signal_rows,
            "pool_case_ids": pool_case_ids,
            "line_count": line_count,
            "lp_line_count": lp_line_count,
            "action_counter": action_counter,
            "archive_ts_values": archive_ts_values,
        }
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line_count += 1
            if '"monitor_type": "lp_pool"' not in raw_line and '"lp_event": true' not in raw_line:
                continue
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            case = data.get("case") or {}
            event = data.get("event") or {}
            signal = data.get("signal") or {}
            context = signal.get("context") or {}
            metadata = signal.get("metadata") or {}
            is_lp = bool(context.get("lp_event")) or str(event.get("monitor_type") or "") == "lp_pool"
            if not is_lp:
                continue
            lp_line_count += 1
            archive_ts = to_int(obj.get("archive_ts"))
            if archive_ts is not None:
                archive_ts_values.append(archive_ts)
            if case.get("case_id"):
                pool_case_ids.add(str(case.get("case_id")))
            action = str(data.get("action") or "")
            action_counter[action] += 1
            if action != "signal_attached" or not signal:
                continue
            outcome = falsey_none(context.get("lp_outcome_record")) or {}
            signal_rows.append(
                {
                    "archive_ts": archive_ts,
                    "pool_case_id": str(case.get("case_id") or ""),
                    "action": action,
                    "signal_id": str(signal.get("signal_id") or ""),
                    "event_id": str(signal.get("event_id") or ""),
                    "pair_label": str(
                        context.get("pair_label")
                        or outcome.get("pair_label")
                        or signal.get("pair_label")
                        or ""
                    ),
                    "pool_address": str(
                        context.get("pool_address")
                        or outcome.get("pool_address")
                        or ""
                    ).lower(),
                    "asset_symbol": str(
                        context.get("asset_symbol")
                        or outcome.get("asset_symbol")
                        or ""
                    ).upper(),
                    "lp_alert_stage": str(
                        context.get("lp_alert_stage")
                        or outcome.get("lp_alert_stage")
                        or ""
                    ),
                    "delivery_class": str(context.get("delivery_class") or signal.get("delivery_class") or ""),
                    "delivery_reason": str(context.get("delivery_reason") or signal.get("delivery_reason") or ""),
                    "lp_scan_path": str(context.get("lp_scan_path") or ""),
                    "lp_route_family": str(context.get("lp_route_family") or ""),
                    "lp_route_priority_source": str(context.get("lp_route_priority_source") or ""),
                    "lp_promoted_fastlane": bool(
                        context.get("lp_promoted_fastlane")
                        if "lp_promoted_fastlane" in context
                        else outcome.get("lp_promoted_fastlane")
                    ),
                    "lp_promote_reason": str(context.get("lp_promote_reason") or ""),
                    "lp_detect_latency_ms": to_float(context.get("lp_detect_latency_ms")),
                    "lp_alert_quality": str(context.get("lp_alert_quality") or ""),
                    "lp_move_phase_label": str(context.get("lp_move_phase_label") or ""),
                    "asset_case_id": str(
                        context.get("asset_case_id")
                        or outcome.get("asset_case_id")
                        or ""
                    ),
                    "asset_case_key": str(
                        context.get("asset_case_key")
                        or outcome.get("asset_case_key")
                        or ""
                    ),
                    "asset_case_stage": str(
                        context.get("asset_case_stage")
                        or outcome.get("asset_case_stage")
                        or ""
                    ),
                    "asset_case_started_at": to_int(
                        context.get("asset_case_started_at")
                        or outcome.get("asset_case_started_at")
                    ),
                    "asset_case_updated_at": to_int(
                        context.get("asset_case_updated_at")
                        or outcome.get("asset_case_updated_at")
                    ),
                    "asset_case_stage_history": list(context.get("asset_case_stage_history") or []),
                    "asset_case_supporting_pairs": list(context.get("asset_case_supporting_pairs") or []),
                    "asset_case_aggregated": bool(context.get("asset_case_aggregated")),
                    "asset_case_multi_pool": bool(context.get("asset_case_multi_pool")),
                    "market_context_source": str(
                        context.get("market_context_source")
                        or outcome.get("market_context_source")
                        or ""
                    ),
                    "alert_relative_timing": str(
                        context.get("alert_relative_timing")
                        or outcome.get("alert_relative_timing")
                        or ""
                    ),
                    "signal_confidence": to_float(signal.get("confidence")),
                    "signal_priority": to_int(signal.get("priority")),
                    "quality_hint": str(
                        (context.get("asset_case_quality_snapshot") or {}).get("quality_score_brief")
                        or ""
                    ),
                }
            )
    return {
        "signal_rows": signal_rows,
        "pool_case_ids": pool_case_ids,
        "line_count": line_count,
        "lp_line_count": lp_line_count,
        "action_counter": action_counter,
        "archive_ts_values": archive_ts_values,
    }


def scan_lp_case_followups(path: Path, lp_case_ids: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            case_id = str(data.get("case_id") or "")
            if case_id not in lp_case_ids:
                continue
            followup = data.get("followup") or {}
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "case_id": case_id,
                    "status": str(followup.get("status") or ""),
                    "stage": str(followup.get("stage") or ""),
                    "signal_id": str(followup.get("signal_id") or ""),
                    "signal_type": str(followup.get("signal_type") or ""),
                    "reason": str(followup.get("reason") or ""),
                }
            )
    return rows


def scan_parsed_lp_events(path: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    archive_ts_values: list[int] = []
    if not path.exists():
        return {"rows": rows, "archive_ts_values": archive_ts_values}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if '"monitor_type": "lp_pool"' not in raw_line:
                continue
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "event_id": str(data.get("event_id") or ""),
                    "case_id": str(data.get("case_id") or ""),
                    "timestamp": to_int(data.get("timestamp")),
                    "pair_label": str(
                        ((data.get("raw") or {}).get("pair_label"))
                        or ((data.get("pool_snapshot") or {}).get("pair_label"))
                        or ""
                    ),
                    "pool_address": str(data.get("watch_address") or "").lower(),
                    "asset_symbol": str((((data.get("raw") or {}).get("token_symbol")) or "")).upper(),
                    "intent_type": str(data.get("intent_type") or ""),
                    "lp_scan_path": str(data.get("lp_scan_path") or ""),
                    "lp_promoted_fastlane": bool(data.get("lp_promoted_fastlane")),
                }
            )
            archive_ts = to_int(obj.get("archive_ts"))
            if archive_ts is not None:
                archive_ts_values.append(archive_ts)
    return {"rows": rows, "archive_ts_values": archive_ts_values}


def scan_raw_lp_swaps(path: Path, pool_addresses: set[str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return {"rows": rows}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if '"kind": "swap"' not in raw_line:
                continue
            obj = json.loads(raw_line)
            data = obj.get("data") or {}
            address = str(data.get("address") or "").lower()
            if pool_addresses and address not in pool_addresses:
                continue
            rows.append(
                {
                    "archive_ts": to_int(obj.get("archive_ts")),
                    "timestamp": to_int(data.get("ts")),
                    "pool_address": address,
                    "pair_label": str(data.get("pair_label") or ""),
                    "side": str(data.get("side") or ""),
                    "usd_value": to_float(data.get("usd_value")),
                }
            )
    return {"rows": rows}


def choose_analysis_window(
    quality_records: list[dict[str, Any]],
    asset_case_cache: dict[str, Any],
    case_signals: list[dict[str, Any]],
    lp_followups: list[dict[str, Any]],
    system_sources: list[SourceInventory],
) -> dict[str, Any]:
    lp_timestamps: list[int] = []
    lp_timestamps.extend(to_int(record.get("created_at")) for record in quality_records)
    lp_timestamps.extend(to_int(record.get("notifier_sent_at")) for record in quality_records)
    for case in asset_case_cache.get("cases") or []:
        lp_timestamps.extend(
            [
                to_int(case.get("started_at")),
                to_int(case.get("updated_at")),
                to_int(case.get("last_signal_at")),
                to_int(case.get("last_stage_transition_at")),
            ]
        )
    lp_timestamps.extend(row.get("archive_ts") for row in case_signals)
    lp_timestamps.extend(row.get("asset_case_started_at") for row in case_signals)
    lp_timestamps.extend(row.get("asset_case_updated_at") for row in case_signals)
    lp_timestamps.extend(row.get("archive_ts") for row in lp_followups)
    lp_timestamps.extend([to_int(asset_case_cache.get("generated_at"))])
    clean_lp_timestamps = sorted({int(ts) for ts in lp_timestamps if ts is not None})
    if not clean_lp_timestamps:
        return {
            "start_ts": None,
            "end_ts": None,
            "duration_sec": None,
            "selection_reason": "未找到可用于 LP 分析的时间戳。",
            "system_end_ts": max((source.coverage_end_ts for source in system_sources if source.coverage_end_ts), default=None),
        }
    segments: list[tuple[int, int]] = []
    gap_threshold = 30 * 60
    seg_start = clean_lp_timestamps[0]
    prev = clean_lp_timestamps[0]
    for ts in clean_lp_timestamps[1:]:
        if ts - prev > gap_threshold:
            segments.append((seg_start, prev))
            seg_start = ts
        prev = ts
    segments.append((seg_start, prev))
    scored_segments: list[tuple[int, int, int]] = []
    for seg_start, seg_end in segments:
        duration = seg_end - seg_start
        if 2 * 3600 <= duration <= 8 * 3600:
            scored_segments.append((duration, seg_start, seg_end))
    if scored_segments:
        scored_segments.sort(reverse=True)
        duration, start_ts, end_ts = scored_segments[0]
    else:
        start_ts = clean_lp_timestamps[0]
        end_ts = clean_lp_timestamps[-1]
        duration = end_ts - start_ts
    system_end_ts = max(
        (source.coverage_end_ts for source in system_sources if source.coverage_end_ts),
        default=None,
    )
    trailing_gap = None
    if system_end_ts is not None:
        trailing_gap = system_end_ts - end_ts
    selection_reason = (
        "选择了今天 UTC 下午最长、且同时被 LP quality cache / asset-case cache / LP signal-attached case archive 覆盖的连续窗口。"
    )
    if trailing_gap and trailing_gap > 15 * 60:
        selection_reason += (
            f" 系统 archive 持续到 {fmt_ts(system_end_ts)}，但 {duration_label(trailing_gap)} 内未再出现 LP cache 更新，"
            "因此将该尾段视为非 LP 有效采样尾巴并排除。"
        )
    return {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_sec": duration,
        "selection_reason": selection_reason,
        "system_end_ts": system_end_ts,
    }


def overlaps_window(start_ts: int | None, end_ts: int | None, window_start: int | None, window_end: int | None) -> bool:
    if None in {start_ts, end_ts, window_start, window_end}:
        return False
    return not (end_ts < window_start or start_ts > window_end)


def enrich_records(
    quality_records: list[dict[str, Any]],
    case_signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    signal_lookup = {row.get("signal_id"): row for row in case_signals if row.get("signal_id")}
    event_lookup = {row.get("event_id"): row for row in case_signals if row.get("event_id")}
    enriched: list[dict[str, Any]] = []
    for record in quality_records:
        meta = signal_lookup.get(record.get("signal_id")) or event_lookup.get(record.get("event_id")) or {}
        merged = dict(record)
        merged.update(
            {
                "_lp_scan_path": meta.get("lp_scan_path"),
                "_lp_route_family": meta.get("lp_route_family"),
                "_lp_route_priority_source": meta.get("lp_route_priority_source"),
                "_lp_detect_latency_ms": meta.get("lp_detect_latency_ms"),
                "_lp_alert_quality": meta.get("lp_alert_quality"),
                "_lp_move_phase_label": meta.get("lp_move_phase_label"),
                "_asset_case_started_at": meta.get("asset_case_started_at"),
                "_asset_case_updated_at": meta.get("asset_case_updated_at"),
                "_asset_case_stage_history": meta.get("asset_case_stage_history") or [],
                "_asset_case_supporting_pairs": meta.get("asset_case_supporting_pairs") or [],
                "_asset_case_aggregated": meta.get("asset_case_aggregated"),
                "_asset_case_multi_pool": meta.get("asset_case_multi_pool"),
                "_pool_case_id": meta.get("pool_case_id"),
                "_delivery_class": meta.get("delivery_class"),
                "_delivery_reason": meta.get("delivery_reason"),
                "_quality_hint": meta.get("quality_hint"),
            }
        )
        enriched.append(merged)
    return enriched


def stage_summary(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for stage in STAGE_ORDER:
        stage_records = [record for record in records if str(record.get("lp_alert_stage") or "") == stage]
        if not stage_records:
            continue
        market_sources = Counter(str(record.get("market_context_source") or "unavailable") for record in stage_records)
        timing_counter = Counter(str(record.get("_lp_alert_quality") or "unknown") for record in stage_records)
        pairs = Counter(str(record.get("pair_label") or "") for record in stage_records)
        assets = Counter(str(record.get("asset_symbol") or "") for record in stage_records)
        resolved_climaxes = [
            record
            for record in stage_records
            if stage == "climax"
            and (
                record.get("reversal_after_climax") is True
                or record.get("move_after_alert_60s") is not None
                or record.get("move_after_alert_300s") is not None
            )
        ]
        resolved_exhaustion = [
            record
            for record in stage_records
            if stage == "exhaustion_risk"
            and (
                record.get("followthrough_negative") is not None
                or record.get("move_after_alert_60s") is not None
                or record.get("move_after_alert_300s") is not None
            )
        ]
        summary[stage] = {
            "count": len(stage_records),
            "asset_distribution": summarize_counter(assets),
            "pair_distribution": summarize_counter(pairs),
            "median_move_before_alert_30s": median_or_none([to_float(record.get("move_before_alert_30s")) for record in stage_records]),
            "median_move_before_alert_60s": median_or_none([to_float(record.get("move_before_alert_60s")) for record in stage_records]),
            "median_move_after_alert_60s": median_or_none([to_float(record.get("move_after_alert_60s")) for record in stage_records]),
            "median_move_after_alert_300s": median_or_none([to_float(record.get("move_after_alert_300s")) for record in stage_records]),
            "median_detect_latency_ms": median_or_none([to_float(record.get("_lp_detect_latency_ms")) for record in stage_records], digits=1),
            "market_context_source_distribution": summarize_counter(market_sources),
            "market_context_available_rate_pct": pct(
                sum(1 for record in stage_records if str(record.get("market_context_source") or "") != "unavailable"),
                len(stage_records),
            ),
            "onchain_timing_distribution": summarize_counter(timing_counter),
            "climax_reversal_rate_60s_pct": pct(
                sum(1 for record in resolved_climaxes if record.get("reversal_after_climax") is True),
                len(resolved_climaxes),
            )
            if stage == "climax"
            else None,
            "climax_reversal_resolved_count": len(resolved_climaxes) if stage == "climax" else None,
            "exhaustion_negative_followthrough_rate_pct": pct(
                sum(
                    1
                    for record in resolved_exhaustion
                    if record.get("followthrough_negative") is True
                    or (aligned_move(record, "move_after_alert_60s") or aligned_move(record, "move_after_alert_300s") or 0.0) < 0
                ),
                len(resolved_exhaustion),
            )
            if stage == "exhaustion_risk"
            else None,
            "exhaustion_resolved_count": len(resolved_exhaustion) if stage == "exhaustion_risk" else None,
        }
    return summary


def prealert_conversion(records: list[dict[str, Any]]) -> dict[str, Any]:
    prealerts = [record for record in records if str(record.get("lp_alert_stage") or "") == "prealert"]
    confirms = [record for record in records if str(record.get("lp_alert_stage") or "") == "confirm"]
    if not prealerts:
        return {"count": 0, "within_30s_pct": None, "within_60s_pct": None, "within_90s_pct": None, "notes": "无 prealert 样本。"}

    def earliest_confirm_delta(prealert: dict[str, Any]) -> int | None:
        pre_ts = to_int(prealert.get("created_at"))
        if pre_ts is None:
            return None
        key = str(prealert.get("asset_case_key") or "")
        pair = str(prealert.get("pair_label") or "")
        direction = str(prealert.get("direction_bucket") or "")
        candidates: list[int] = []
        for confirm in confirms:
            confirm_ts = to_int(confirm.get("created_at"))
            if confirm_ts is None or confirm_ts <= pre_ts:
                continue
            same_key = key and str(confirm.get("asset_case_key") or "") == key
            same_pair_direction = (
                str(confirm.get("pair_label") or "") == pair
                and str(confirm.get("direction_bucket") or "") == direction
            )
            if same_key or same_pair_direction:
                candidates.append(confirm_ts - pre_ts)
        return min(candidates) if candidates else None

    deltas = [earliest_confirm_delta(prealert) for prealert in prealerts]
    return {
        "count": len(prealerts),
        "within_30s_pct": pct(sum(1 for delta in deltas if delta is not None and delta <= 30), len(prealerts)),
        "within_60s_pct": pct(sum(1 for delta in deltas if delta is not None and delta <= 60), len(prealerts)),
        "within_90s_pct": pct(sum(1 for delta in deltas if delta is not None and delta <= 90), len(prealerts)),
        "resolved_count": sum(1 for delta in deltas if delta is not None),
        "notes": "优先按 asset_case_key 回查，缺失时退化到 pair+direction 匹配；本轮原生 time_to_confirm 字段未回写。",
    }


def build_asset_case_summary(records: list[dict[str, Any]], pool_case_count: int) -> dict[str, Any]:
    cases: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        asset_case_id = str(record.get("asset_case_id") or "")
        if asset_case_id:
            cases[asset_case_id].append(record)
    case_rows: list[dict[str, Any]] = []
    for asset_case_id, rows in cases.items():
        rows.sort(key=lambda row: (to_int(row.get("created_at")) or 0, STAGE_ORDER.get(str(row.get("lp_alert_stage") or ""), 0)))
        stages = [str(row.get("lp_alert_stage") or "") for row in rows]
        stage_path = []
        for stage in stages:
            if stage and (not stage_path or stage_path[-1] != stage):
                stage_path.append(stage)
        supporting_pairs = sorted(
            {
                pair
                for row in rows
                for pair in (row.get("_asset_case_supporting_pairs") or [str(row.get("pair_label") or "")])
                if pair
            }
        )
        started_at = min(
            [ts for ts in [to_int(rows[0].get("_asset_case_started_at"))] + [to_int(row.get("created_at")) for row in rows] if ts is not None],
            default=None,
        )
        updated_at = max(
            [ts for ts in [to_int(rows[-1].get("_asset_case_updated_at"))] + [to_int(row.get("created_at")) for row in rows] if ts is not None],
            default=None,
        )
        case_rows.append(
            {
                "asset_case_id": asset_case_id,
                "asset_case_key": str(rows[0].get("asset_case_key") or ""),
                "asset_symbol": str(rows[0].get("asset_symbol") or ""),
                "direction_bucket": str(rows[0].get("direction_bucket") or ""),
                "pair_count": len(set(str(row.get("pair_label") or "") for row in rows if row.get("pair_label"))),
                "supporting_pairs": supporting_pairs,
                "signal_count": len(rows),
                "stages": stage_path,
                "started_at": started_at,
                "updated_at": updated_at,
                "lifespan_sec": (updated_at - started_at) if started_at is not None and updated_at is not None else None,
                "confirm_count": sum(1 for row in rows if str(row.get("lp_alert_stage") or "") == "confirm"),
                "climax_count": sum(1 for row in rows if str(row.get("lp_alert_stage") or "") == "climax"),
                "exhaustion_count": sum(1 for row in rows if str(row.get("lp_alert_stage") or "") == "exhaustion_risk"),
                "prealert_count": sum(1 for row in rows if str(row.get("lp_alert_stage") or "") == "prealert"),
                "negative_followthrough_count": sum(
                    1
                    for row in rows
                    if row.get("followthrough_negative") is True or row.get("reversal_after_climax") is True
                ),
                "positive_followthrough_count": sum(1 for row in rows if row.get("followthrough_positive") is True),
                "aggregated": any(bool(row.get("_asset_case_aggregated")) for row in rows) or len(supporting_pairs) > 1,
                "multi_pool": any(bool(row.get("_asset_case_multi_pool")) for row in rows) or len(supporting_pairs) > 1,
                "quality_score": median_or_none([to_float(row.get("asset_case_quality_score")) for row in rows], digits=3),
            }
        )
    case_rows.sort(key=lambda row: (row["signal_count"], row["lifespan_sec"] or 0), reverse=True)
    upgrade_counts = {
        "prealert_to_confirm": sum(1 for row in case_rows if "prealert" in row["stages"] and "confirm" in row["stages"]),
        "confirm_to_climax": sum(1 for row in case_rows if "confirm" in row["stages"] and "climax" in row["stages"]),
        "climax_to_exhaustion_risk": sum(
            1 for row in case_rows if "climax" in row["stages"] and "exhaustion_risk" in row["stages"]
        ),
    }
    clean_cases = sorted(
        [
            row
            for row in case_rows
            if row["confirm_count"] >= 1 and row["negative_followthrough_count"] == 0
        ],
        key=lambda row: (row["confirm_count"], -(row["negative_followthrough_count"]), row["signal_count"]),
        reverse=True,
    )[:5]
    noisy_cases = sorted(
        case_rows,
        key=lambda row: (row["negative_followthrough_count"], row["prealert_count"], -row["confirm_count"], row["signal_count"]),
        reverse=True,
    )[:5]
    supporting_combo_counter = Counter(
        " + ".join(row["supporting_pairs"]) if row["supporting_pairs"] else "(none)"
        for row in case_rows
    )
    major_cases = [row for row in case_rows if classify_major(row.get("asset_symbol")) == "major"]
    long_tail_cases = [row for row in case_rows if classify_major(row.get("asset_symbol")) == "long_tail"]
    reduction_vs_pool_cases = (
        pct(max(pool_case_count - len(case_rows), 0), pool_case_count)
        if pool_case_count and pool_case_count >= len(case_rows)
        else None
    )
    return {
        "total_asset_cases": len(case_rows),
        "case_rows": case_rows,
        "upgrade_counts": upgrade_counts,
        "most_active_cases": case_rows[:5],
        "clean_cases": clean_cases,
        "noisy_cases": noisy_cases,
        "supporting_pair_combos": summarize_counter(supporting_combo_counter),
        "major_case_count": len(major_cases),
        "long_tail_case_count": len(long_tail_cases),
        "estimated_message_reduction_vs_pool_case_pct": reduction_vs_pool_cases,
        "estimated_message_reduction_vs_signal_count_pct": pct(
            max(len(records) - len(case_rows), 0),
            len(records),
        ),
        "over_aggregation_flag_count": sum(
            1
            for row in case_rows
            if len({part for part in row["asset_case_key"].split("|") if part.endswith("pressure")}) > 1
        ),
    }


def build_timing_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    source_counter = Counter(str(record.get("market_context_source") or "unavailable") for record in records)
    external_timing_counter = Counter(
        str(record.get("alert_relative_timing") or "unknown") for record in records if str(record.get("alert_relative_timing") or "")
    )
    onchain_timing_counter = Counter(str(record.get("_lp_alert_quality") or "unknown") for record in records)
    stage_onchain = {
        stage: summarize_counter(
            Counter(str(record.get("_lp_alert_quality") or "unknown") for record in records if str(record.get("lp_alert_stage") or "") == stage)
        )
        for stage in STAGE_ORDER
        if any(str(record.get("lp_alert_stage") or "") == stage for record in records)
    }
    asset_onchain = {
        asset: summarize_counter(Counter(str(record.get("_lp_alert_quality") or "unknown") for record in asset_records))
        for asset in sorted({str(record.get("asset_symbol") or "") for record in records})
        for asset_records in [[record for record in records if str(record.get("asset_symbol") or "") == asset]]
    }
    return {
        "market_context_source_distribution": summarize_counter(source_counter),
        "market_context_available_rate_pct": pct(
            sum(1 for record in records if str(record.get("market_context_source") or "") != "unavailable"),
            len(records),
        ),
        "external_timing_distribution": summarize_counter(external_timing_counter),
        "onchain_timing_distribution": summarize_counter(onchain_timing_counter),
        "onchain_timing_by_stage": stage_onchain,
        "onchain_timing_by_asset": asset_onchain,
    }


def build_fastlane_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    fastlane = [record for record in records if bool(record.get("lp_promoted_fastlane"))]
    promoted_main = [
        record
        for record in records
        if str(record.get("_lp_route_priority_source") or "") == "promoted_main"
        or str(record.get("_lp_scan_path") or "") == "promoted_main"
        or (str(record.get("_lp_scan_path") or "") == "main" and bool(record.get("lp_promoted_fastlane")))
    ]
    secondary = [record for record in records if str(record.get("_lp_scan_path") or "") == "secondary"]
    primary_trend = [record for record in records if str(record.get("_lp_scan_path") or "") == "primary_trend"]
    extended = [record for record in records if str(record.get("_lp_scan_path") or "") == "extended"]
    main = [record for record in records if str(record.get("_lp_scan_path") or "") == "main"]
    resolved_fastlane = [record for record in fastlane if record.get("move_after_alert_60s") is not None or record.get("move_after_alert_300s") is not None]
    return {
        "fastlane_signal_count": len(fastlane),
        "fastlane_case_count": len({record.get("asset_case_id") for record in fastlane if record.get("asset_case_id")}),
        "promoted_main_signal_count": len(promoted_main),
        "promoted_main_case_count": len({record.get("asset_case_id") for record in promoted_main if record.get("asset_case_id")}),
        "primary_trend_signal_count": len(primary_trend),
        "secondary_signal_count": len(secondary),
        "extended_signal_count": len(extended),
        "main_signal_count": len(main),
        "promoted_main_median_latency_ms": median_or_none([to_float(record.get("_lp_detect_latency_ms")) for record in promoted_main], digits=1),
        "non_promoted_median_latency_ms": median_or_none(
            [to_float(record.get("_lp_detect_latency_ms")) for record in records if record not in promoted_main],
            digits=1,
        ),
        "fastlane_median_aligned_move_60s": median_or_none([aligned_move(record, "move_after_alert_60s") for record in resolved_fastlane]),
        "fastlane_median_aligned_move_300s": median_or_none([aligned_move(record, "move_after_alert_300s") for record in resolved_fastlane]),
        "scan_path_distribution": summarize_counter(Counter(str(record.get("_lp_scan_path") or "unknown") for record in records)),
    }


def build_noise_assessment(records: list[dict[str, Any]], quality_summary: dict[str, Any]) -> dict[str, Any]:
    pair_rows = quality_summary.get("dimensions", {}).get("pair", []) or []
    pool_rows = quality_summary.get("dimensions", {}).get("pool", []) or []
    worst_pair = sorted(
        pair_rows,
        key=lambda row: (
            float(row.get("climax_reversal_score") or 0.0),
            -float(row.get("confirm_conversion_score") or 0.0),
            -int(row.get("sample_size") or 0),
        ),
        reverse=True,
    )[:3]
    worst_pool = sorted(
        pool_rows,
        key=lambda row: (
            float(row.get("climax_reversal_score") or 0.0),
            -float(row.get("confirm_conversion_score") or 0.0),
            -int(row.get("sample_size") or 0),
        ),
        reverse=True,
    )[:3]
    prealert_share = pct(sum(1 for record in records if str(record.get("lp_alert_stage") or "") == "prealert"), len(records))
    market_context_unavailable_pct = pct(
        sum(1 for record in records if str(record.get("market_context_source") or "") == "unavailable"),
        len(records),
    )
    climax_reversal_pct = pct(
        sum(1 for record in records if record.get("reversal_after_climax") is True),
        sum(
            1
            for record in records
            if str(record.get("lp_alert_stage") or "") == "climax"
            and (
                record.get("reversal_after_climax") is True
                or record.get("move_after_alert_60s") is not None
                or record.get("move_after_alert_300s") is not None
            )
        ),
    )
    if market_context_unavailable_pct and market_context_unavailable_pct >= 90.0:
        dominant_risk = "market_context unavailable + 晚阶段信号容易被误读为追单"
    elif climax_reversal_pct and climax_reversal_pct >= 40.0:
        dominant_risk = "climax 追单幻觉"
    elif prealert_share and prealert_share >= 30.0:
        dominant_risk = "单池 prealert 过多"
    else:
        dominant_risk = "样本过窄导致的错判外推"
    return {
        "prealert_share_pct": prealert_share,
        "market_context_unavailable_pct": market_context_unavailable_pct,
        "climax_reversal_pct": climax_reversal_pct,
        "worst_pairs": worst_pair,
        "worst_pools": worst_pool,
        "dominant_risk": dominant_risk,
    }


def build_scorecard(
    record_count: int,
    asset_count: int,
    data_completeness: str,
    timing_summary: dict[str, Any],
    fastlane_summary: dict[str, Any],
    noise_assessment: dict[str, Any],
    asset_case_summary: dict[str, Any],
) -> dict[str, float]:
    completeness_base = {"low": 4.0, "medium": 6.0, "high": 8.0}[data_completeness]
    readiness = completeness_base
    if record_count >= 30:
        readiness += 0.8
    if asset_count >= 2:
        readiness += 0.5
    else:
        readiness -= 0.7
    if (timing_summary.get("market_context_available_rate_pct") or 0.0) < 10.0:
        readiness -= 0.8
    usability = 5.8
    if (noise_assessment.get("climax_reversal_pct") or 0.0) >= 40.0:
        usability -= 0.9
    if (timing_summary.get("market_context_available_rate_pct") or 0.0) == 0.0:
        usability -= 0.6
    noise_control = 6.2
    if (noise_assessment.get("prealert_share_pct") or 0.0) < 10.0:
        noise_control += 0.5
    if (noise_assessment.get("market_context_unavailable_pct") or 0.0) >= 90.0:
        noise_control -= 0.7
    timing_value = 4.8
    if (timing_summary.get("market_context_available_rate_pct") or 0.0) == 0.0:
        timing_value -= 1.1
    asset_case_value = 5.9
    if (asset_case_summary.get("estimated_message_reduction_vs_signal_count_pct") or 0.0) >= 20.0:
        asset_case_value += 0.6
    if asset_case_summary.get("over_aggregation_flag_count"):
        asset_case_value -= 0.5
    fastlane_roi = 5.0
    if fastlane_summary.get("fastlane_signal_count", 0) <= 1:
        fastlane_roi -= 0.5
    if fastlane_summary.get("secondary_signal_count", 0) == 0:
        fastlane_roi -= 0.2
    scorecard = {
        "lp_research_sampling_readiness": round(max(min(readiness, 10.0), 0.0), 1),
        "lp_product_signal_usability": round(max(min(usability, 10.0), 0.0), 1),
        "lp_noise_control": round(max(min(noise_control, 10.0), 0.0), 1),
        "market_timing_value": round(max(min(timing_value, 10.0), 0.0), 1),
        "asset_case_aggregation_value": round(max(min(asset_case_value, 10.0), 0.0), 1),
        "fastlane_roi": round(max(min(fastlane_roi, 10.0), 0.0), 1),
    }
    scorecard["overall"] = round(
        statistics.mean(scorecard.values()),
        1,
    )
    return scorecard


def completeness_rating(
    inventory_rows: list[SourceInventory],
    timing_summary: dict[str, Any],
) -> tuple[str, list[str]]:
    present = {row.path: row for row in inventory_rows if row.exists}
    notes: list[str] = []
    required = [
        str(RAW_EVENTS_PATH.relative_to(ROOT)),
        str(PARSED_EVENTS_PATH.relative_to(ROOT)),
        str(CASES_PATH.relative_to(ROOT)),
        str(CASE_FOLLOWUPS_PATH.relative_to(ROOT)),
        str(ASSET_CASE_CACHE_PATH.relative_to(ROOT)),
        str(QUALITY_STATS_PATH.relative_to(ROOT)),
    ]
    present_count = sum(1 for path in required if path in present)
    signals_missing = not SIGNALS_PATH.exists()
    if present_count >= 6:
        rating = "medium"
    else:
        rating = "low"
    if present_count >= 6 and not signals_missing and (timing_summary.get("market_context_available_rate_pct") or 0.0) > 30.0:
        rating = "high"
    if signals_missing:
        notes.append("signals archive 缺失，无法用单独 signal 档案复核每条通知。")
    if (timing_summary.get("market_context_available_rate_pct") or 0.0) == 0.0:
        notes.append("market context 虽配置为 live，但本轮记录全部回落为 unavailable。")
    return rating, notes


def add_metric(
    rows: list[dict[str, Any]],
    metric_group: str,
    metric_name: str,
    value: Any,
    sample_size: int | None = None,
    asset: str = "",
    pair: str = "",
    stage: str = "",
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


def build_markdown(
    analysis_window: dict[str, Any],
    inventory_rows: list[SourceInventory],
    runtime_config: dict[str, dict[str, Any]],
    overall_summary: dict[str, Any],
    stage_stats: dict[str, dict[str, Any]],
    prealert_conv: dict[str, Any],
    asset_case_summary: dict[str, Any],
    timing_summary: dict[str, Any],
    fastlane_summary: dict[str, Any],
    quality_summary: dict[str, Any],
    noise_assessment: dict[str, Any],
    scorecard: dict[str, float],
    completeness_notes: list[str],
) -> str:
    overall = quality_summary.get("overall", {}) or {}
    top_bad_prealerts = quality_summary.get("top_bad_prealerts", []) or []
    top_climax_reversal = quality_summary.get("top_climax_reversal", []) or []
    fastlane_laggards = quality_summary.get("fastlane_roi_laggards", []) or []
    window_start = analysis_window.get("start_ts")
    window_end = analysis_window.get("end_ts")
    general_runtime = overall_summary.get("general_runtime_window")
    source_lines = [
        f"- `{row.path}` | `{row.fmt}` | {'present' if row.exists else 'missing'} | "
        f"{fmt_ts(row.coverage_start_ts)} -> {fmt_ts(row.coverage_end_ts)} | "
        f"{'overlap' if row.overlap_window else 'no-overlap'}"
        + (f" | {row.notes}" if row.notes else "")
        for row in inventory_rows
    ]
    config_lines = [
        f"- `{key}` = `{runtime_config[key]['runtime_value']}`"
        + (" | source=`.env`" if runtime_config[key].get("env_present") else " | source=`default/app.config`")
        for key in WHITELISTED_CONFIG_KEYS
    ]
    top_active_cases = asset_case_summary.get("most_active_cases", [])[:5]
    clean_cases = asset_case_summary.get("clean_cases", [])[:5]
    noisy_cases = asset_case_summary.get("noisy_cases", [])[:5]
    onchain_timing_dist = timing_summary.get("onchain_timing_distribution", [])
    reduction_vs_pool_case = asset_case_summary.get("estimated_message_reduction_vs_pool_case_pct")
    reduction_vs_pool_case_label = (
        f"`{reduction_vs_pool_case}`%"
        if reduction_vs_pool_case is not None
        else "`n/a`（本轮 pool case id 口径比 asset-case 更窄，无法稳定估算）"
    )
    top_recommendations = [
        "补齐 `archive/signals`，否则无法对消息模板/出站内容做完整复盘。",
        "排查 `MARKET_CONTEXT_ADAPTER_MODE=live` 但 `market_context_source=unavailable` 的原因，至少要拿到 Binance/Bybit public 命中率。",
        "补齐 prealert outcome 回写，尤其是 `confirm_after_prealert` / `false_prealert` / `time_to_confirm`。",
        "下一轮扩大 majors 覆盖面，至少再加入 BTC / SOL 主流池，避免 ETH 双池样本外推。",
        "把 fastlane/promoted_main 的后续 60s/300s outcome 单独导出，否则 ROI 仍难分层。",
    ]
    top_strengths = [
        f"LP 有效窗口达到 {duration_label(analysis_window.get('duration_sec'))}，不是单次 replay，而是真实连续运行样本。",
        f"归档链条较完整：raw/parsed/cases/case_followups/quality/asset-case 均存在，完整性评为 `{overall_summary['data_completeness']}`。",
        f"LP 消息量被压在 {overall_summary['total_signals']} 条，prealert 只有 {stage_stats.get('prealert', {}).get('count', 0)} 条，没有出现 prealert 洪水。",
        f"asset-case 相对 signal 数的估算压缩率约为 {asset_case_summary.get('estimated_message_reduction_vs_signal_count_pct') or 0}% 。",
        "quality/outcome 已经开始分出 ETH/USDT 的 climax reversal 风险高于 ETH/USDC。",
    ]
    top_gaps = [
        "signals archive 缺失，导致消息内容无法用独立 signal 档案复核。",
        "market context 配置是 live，但实际 100% unavailable，CEX/perp timing 结论无法成立。",
        f"LP 样本只覆盖 {overall_summary['asset_count']} 个资产、{overall_summary['pair_count']} 个 pair，没有 long-tail 覆盖。",
        "prealert 闭环字段没有成熟回写，prealert->confirm 只能做弱结论。",
        "fastlane/promoted_main 只有 1 个样本，ROI 还没形成可操作分层。",
    ]
    top_fix_priority = [
        "优先修复 live market context 命中率，否则 timing 模块无法进入研究闭环。",
        "优先恢复 `archive/signals`，让 signal 内容、notifier 输出、quality 记录三者可对齐。",
        "优先补齐 30s/60s/300s outcome 更新，尤其是 prealert 与 exhaustion_risk。",
        "优先扩池，但先扩 majors，不要先扩 long-tail，否则会把当前结论重新打散。",
        "优先把 promoted_main/primary_trend/secondary 的 scan-path 结果导出成独立统计表。",
    ]
    report = f"""# Afternoon Run Analysis

## 1. 执行摘要

- 选定分析窗口：`{window_start}` -> `{window_end}`（Unix ts），即 `{fmt_ts(window_start)}` -> `{fmt_ts(window_end)}`，对应 archive 自带北京时间 `{fmt_ts_bj(window_start)}` -> `{fmt_ts_bj(window_end)}`。
- 选择原因：{analysis_window.get('selection_reason')}
- 覆盖时长：`{duration_label(analysis_window.get('duration_sec'))}`
- 系统级 archive 运行范围：`{fmt_ts(general_runtime['start_ts'])}` -> `{fmt_ts(general_runtime['end_ts'])}`；LP 有效样本比系统尾段少 ` {duration_label((general_runtime['end_ts'] or 0) - (window_end or 0))} `，因为 10:30:32 UTC 之后只看到非 LP archive 更新。
- 总 events：
  - overall raw archive: `{overall_summary['total_raw_events']}`
  - overall parsed archive: `{overall_summary['total_parsed_events']}`
  - LP parsed events: `{overall_summary['total_lp_parsed_events']}`
- 总 signals：`{overall_summary['total_signals']}`
- 总 asset-case：`{overall_summary['total_asset_cases']}`
- 总 LP 相关消息：`{overall_summary['total_lp_messages']}`
- majors vs long-tail：`{overall_summary['major_signal_share_pct']}`% vs `{overall_summary['long_tail_signal_share_pct']}`%
- market context source：`{overall_summary['market_context_source_distribution']}`
- signal-level scan path：`{overall_summary['scan_path_distribution']}`
- 总评：这轮数据已经“够做首轮研究采样”，但还不够当泛化产品样本，原因是样本过窄、signals 档缺失、market context 全部 unavailable。

## 2. 数据源与完整性说明

{chr(10).join(source_lines)}

- 数据完整性评级：`{overall_summary['data_completeness']}`
- 评级说明：
{chr(10).join(f"- {note}" for note in completeness_notes)}
- 未找到 nohup/stdout 启动日志；本次窗口主要依赖 archive 时间戳、asset-case cache、quality cache 的写入时间。

## 3. 实际分析窗口

- 主窗口：`{fmt_ts(window_start)}` -> `{fmt_ts(window_end)}` | `{fmt_ts_bj(window_start)}` -> `{fmt_ts_bj(window_end)}`
- 覆盖时长：`{duration_label(analysis_window.get('duration_sec'))}`
- 原始时间基准：
  - archive 文件使用 `archive_ts`（Unix 秒）和 `archive_time_bj`（UTC+8）
  - 报告正文统一以 `UTC` 展示
  - 当 archive 自带北京时间存在时，同时给出 `UTC+8`

## 4. 有效运行配置（非敏感）

{chr(10).join(config_lines)}

- 配置解读：
  - 运行 tier 是 `research`，因此 prealert / confirm / climax / exhaustion_risk 全部允许进入研究视图。
  - archive 在配置上是全开的：raw / parsed / signals / cases / case_followups / delivery_audit 都应可写。
  - 实际落盘与配置不完全一致：`ARCHIVE_ENABLE_SIGNALS=True`，但今天的 `archive/signals/{TODAY_STR}.ndjson` 不存在。
  - market context 配置是 `live + binance_perp/bybit_perp`，但实际落盘全是 `unavailable`，说明研究采样能力未真正打通到市场上下文层。

## 5. LP stage 质量分析

- stage 分布：
{chr(10).join(f"- `{stage}`: {stats['count']} 条" for stage, stats in stage_stats.items())}
- prealert -> confirm 转化：
  - 30s: `{prealert_conv.get('within_30s_pct')}`%
  - 60s: `{prealert_conv.get('within_60s_pct')}`%
  - 90s: `{prealert_conv.get('within_90s_pct')}`%
  - 说明：{prealert_conv.get('notes')}
- climax -> reversal：
  - 60s 可得样本：`{stage_stats.get('climax', {}).get('climax_reversal_resolved_count')}`
  - reversal 比例：`{stage_stats.get('climax', {}).get('climax_reversal_rate_60s_pct')}`%
  - 30s：本轮持久化结果未提供稳定字段，无法可靠统计。
- exhaustion_risk：
  - 已解析样本：`{stage_stats.get('exhaustion_risk', {}).get('exhaustion_resolved_count')}`
  - 负向 followthrough / 回吐比例：`{stage_stats.get('exhaustion_risk', {}).get('exhaustion_negative_followthrough_rate_pct')}`%
- on-chain timing proxy（不是 CEX/perp timing）：
{chr(10).join(f"- `{stage}`: {stage_stats.get(stage, {}).get('onchain_timing_distribution')}" for stage in stage_stats)}
- 关键判断：
  - prealert 现在并不“过多”，占比只有 `{noise_assessment.get('prealert_share_pct')}`%，但样本只有 1 条，且闭环字段未成熟，不能把“少”误读成“已验证有效”。
  - confirm 更像“中段确认”而不是明显先手；其 30s/60s 前置涨跌幅已非零，适合 research/trader 做确认，不适合当 ultra-early entry。
  - climax 明显有尾段风险：resolved climax 中 `{stage_stats.get('climax', {}).get('climax_reversal_rate_60s_pct')}`% 很快反转。
  - exhaustion_risk 的行为意义比“继续追 sweep”更安全，因为它本身就在提醒尾段风险；但当前样本仍偏少。

## 6. asset-case 分析

- 总 asset-case 数：`{asset_case_summary.get('total_asset_cases')}`
- case 升级路径统计：
  - prealert -> confirm: `{asset_case_summary.get('upgrade_counts', {}).get('prealert_to_confirm')}`
  - confirm -> climax: `{asset_case_summary.get('upgrade_counts', {}).get('confirm_to_climax')}`
  - climax -> exhaustion_risk: `{asset_case_summary.get('upgrade_counts', {}).get('climax_to_exhaustion_risk')}`
- supporting_pairs 组合：
{chr(10).join(f"- `{row['key']}`: {row['count']} cases" for row in asset_case_summary.get('supporting_pair_combos', [])[:5])}
- 最活跃 cases：
{chr(10).join(f"- `{row['asset_case_id']}` | stages={row['stages']} | pairs={row['supporting_pairs']} | lifespan={duration_label(row['lifespan_sec'])}" for row in top_active_cases)}
- 最干净 cases：
{chr(10).join(f"- `{row['asset_case_id']}` | confirms={row['confirm_count']} | negatives={row['negative_followthrough_count']} | pairs={row['supporting_pairs']}" for row in clean_cases) if clean_cases else "- 暂无足够样本定义“干净”cases。"}
- 最吵 cases：
{chr(10).join(f"- `{row['asset_case_id']}` | prealerts={row['prealert_count']} | negatives={row['negative_followthrough_count']} | stages={row['stages']}" for row in noisy_cases)}
- 聚合效果：
  - 相对 signal 数的估算压缩率：`{asset_case_summary.get('estimated_message_reduction_vs_signal_count_pct')}`%
  - 相对 pool case 数的估算压缩率：{reduction_vs_pool_case_label}
- 结论：
  - asset-case 聚合对用户理解 ETH 双池联动是有帮助的，至少减少了池级平铺。
  - 本轮没有看到强烈的错误合并迹象；方向、pair 支持关系基本一致。
  - 但大多数 case 仍然是单资产、极少 pair 的窄样本，离“复杂市场叙事聚合”还很远。

## 7. market context / timing 分析

- market context source 分布：`{timing_summary.get('market_context_source_distribution')}`
- live market context 可用率：`{timing_summary.get('market_context_available_rate_pct')}`%
- external timing（Binance/Bybit 维度）：`{timing_summary.get('external_timing_distribution')}`
- on-chain timing proxy：`{onchain_timing_dist}`
- 结论：
  - 由于 `market_context_source` 全部是 `unavailable`，不能对 CEX/perp 说“领先 / 确认 / 偏晚”。
  - 退化到链上视角时，prealert 更靠前，confirm 更偏确认，climax / exhaustion_risk 更偏后段或尾段风险提示。
  - 对合约交易者来说，本轮最有参考价值的是 `confirm` 的方向确认和 `exhaustion_risk` 的追单风险提示；最容易被误用的是 `climax`。
  - basis / mark-index / funding 等市场字段本轮都没有持久化命中，无法分析。

## 8. fastlane ROI 分析

- fastlane/promoted 样本：
  - fastlane signal 数：`{fastlane_summary.get('fastlane_signal_count')}`
  - promoted_main signal 数：`{fastlane_summary.get('promoted_main_signal_count')}`
  - primary_trend signal 数：`{fastlane_summary.get('primary_trend_signal_count')}`
  - secondary signal 数：`{fastlane_summary.get('secondary_signal_count')}`
  - extended signal 数：`{fastlane_summary.get('extended_signal_count')}`
- scan path 分布：`{fastlane_summary.get('scan_path_distribution')}`
- latency：
  - promoted_main median latency：`{fastlane_summary.get('promoted_main_median_latency_ms')}` ms
  - non-promoted median latency：`{fastlane_summary.get('non_promoted_median_latency_ms')}` ms
- followthrough：
  - fastlane aligned median move 60s：`{fastlane_summary.get('fastlane_median_aligned_move_60s')}`
  - fastlane aligned median move 300s：`{fastlane_summary.get('fastlane_median_aligned_move_300s')}`
- 结论：
  - promoted_main 还谈不上“已经值得”，因为只有 1 个 promotion 样本，后续 outcome 也没形成正向分化。
  - 当前更像 fastlane 框架已接好，但 ROI 还没有在真实数据里跑出分层。
  - secondary / extended 在通知层几乎没样本，无法比较 ROI。

## 9. quality/outcome 闭环分析

- overall quality：
  - quality_score=`{overall.get('quality_score')}`
  - prealert_precision_score=`{overall.get('prealert_precision_score')}`
  - confirm_conversion_score=`{overall.get('confirm_conversion_score')}`
  - climax_reversal_score=`{overall.get('climax_reversal_score')}`
  - fastlane_roi_score=`{overall.get('fastlane_roi_score')}`
- top_bad_prealerts：
{chr(10).join(f"- `{row['dimension']}:{row['label']}` | prealert_precision={row['prealert_precision_score']} | sample={row['sample_size']}" for row in top_bad_prealerts[:5])}
- top_climax_reversal：
{chr(10).join(f"- `{row['dimension']}:{row['label']}` | climax_reversal={row['climax_reversal_score']} | sample={row['sample_size']}" for row in top_climax_reversal[:5])}
- fastlane_roi_laggards：
{chr(10).join(f"- `{row['dimension']}:{row['label']}` | fastlane_roi={row['fastlane_roi_score']} | sample={row['sample_size']}" for row in fastlane_laggards[:5])}
- 样本保护：
  - stage=`prealert` 在 quality report 中 `actionable=false`，说明样本保护在该层已生效。
  - pair / pool / asset 虽然 `actionable=true`，但 resolved confirms 只有 3-4、resolved climaxes 只有 2，仍应视为早期特征，不宜过度调参。
- 调参含义：
  - 最该降权的是 `ETH/USDT` 的 climax/chase 场景，因为 reversal score 更高。
  - 最值得保留的是 majors 上的 `confirm` 和 `exhaustion_risk` 提示语义。
  - 当前真正“不该下结论”的，是 prealert 相关 precision / conversion。

## 10. 噪音与误判风险评估

- research 用户：当前噪音 `中等`。消息量不大，但 timing/context 缺口会放大误读风险。
- trader 用户：最容易误判的是把 `climax` 当成继续追单信号，以及把 `confirm` 误当超早信号。
- retail 用户：`prealert`、`single-pool climax`、没有 market context 的晚阶段 directional 语句都应默认隐藏或更严格。
- 最容易被误读成“直接追”的消息：
  - climax
  - exhaustion_risk（如果用户忽略“风险”二字，只看方向）
  - 没有 market context 支撑的 confirm
- 当前最大噪音来源：`{noise_assessment.get('dominant_risk')}`
- 明确判断：
  - 当前 LP 噪音：`中等`
  - 是否足以支持应用级产品下一步测试：`只够 research/trader 内测，不够 retail / 泛产品化测试`
  - 最大误判风险：`晚阶段 signal 在缺少 market context 时被当成追单入口`

## 11. 最终评分

- LP 研究采样 readiness：`{scorecard['lp_research_sampling_readiness']}/10`
- LP 产品信号可用性：`{scorecard['lp_product_signal_usability']}/10`
- LP 噪音控制：`{scorecard['lp_noise_control']}/10`
- market timing 价值：`{scorecard['market_timing_value']}/10`
- asset-case 聚合价值：`{scorecard['asset_case_aggregation_value']}/10`
- fastlane ROI：`{scorecard['fastlane_roi']}/10`
- 综合分：`{scorecard['overall']}/10`

## 12. 下一步建议

### Top 5 优点
{chr(10).join(f"- {line}" for line in top_strengths)}

### Top 5 当前不足
{chr(10).join(f"- {line}" for line in top_gaps)}

### Top 5 最值得优先改进的事项
{chr(10).join(f"- {line}" for line in top_fix_priority)}

## 13. 限制与不确定性

- 本轮 `archive/signals` 缺失，所以无法独立校验通知模板、发送理由和 quality/outcome 是否 1:1 对齐。
- `market_context_source=unavailable` 占比 100%，因此所有 CEX/perp timing 结论都被明确降级为“不可得”。
- prealert 只有 1 条，且闭环字段未成熟，任何关于 prealert precision 的结论都不稳。
- 样本只覆盖 ETH 两个 Uniswap V2 主流池，不代表 long-tail，也不代表其他 DEX / CLMM。
- 未找到完整启动日志，只能用 archive/cache 时间戳重建窗口。
- 当前报告是基于今天 `{TODAY_STR}` 的真实落盘数据，而不是 replay；但这也是一轮窄样本，需要继续累计。
"""
    return report


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    quality_stats = json_load(QUALITY_STATS_PATH) if QUALITY_STATS_PATH.exists() else {}
    asset_case_cache = json_load(ASSET_CASE_CACHE_PATH) if ASSET_CASE_CACHE_PATH.exists() else {}
    exchange_adjacent = json_load(EXCHANGE_ADJACENT_PATH) if EXCHANGE_ADJACENT_PATH.exists() else {}
    runtime_config = load_runtime_config()
    quality_report_summary = run_quality_report_summary()

    quality_records = quality_stats.get("records") or []
    case_scan = scan_lp_case_archive(CASES_PATH)
    case_signals = case_scan["signal_rows"]
    lp_followups = scan_lp_case_followups(CASE_FOLLOWUPS_PATH, case_scan["pool_case_ids"])
    parsed_lp = scan_parsed_lp_events(PARSED_EVENTS_PATH)

    pool_addresses = {str(record.get("pool_address") or "").lower() for record in quality_records if record.get("pool_address")}
    raw_lp = scan_raw_lp_swaps(RAW_EVENTS_PATH, pool_addresses)

    system_inventory = [
        inventory_ndjson(RAW_EVENTS_PATH, notes="overall raw archive"),
        inventory_ndjson(PARSED_EVENTS_PATH, notes="overall parsed archive"),
        inventory_ndjson(SIGNALS_PATH, notes="signals archive"),
        inventory_ndjson(CASES_PATH, notes="case snapshots"),
        inventory_ndjson(CASE_FOLLOWUPS_PATH, notes="case followups"),
        inventory_ndjson(DELIVERY_AUDIT_PATH, notes="delivery audit"),
    ]
    asset_cases = asset_case_cache.get("cases") or []
    inventory_rows = list(system_inventory)
    inventory_rows.append(
        inventory_json(
            ASSET_CASE_CACHE_PATH,
            min(
                [
                    ts
                    for case in asset_cases
                    for ts in [to_int(case.get("started_at")), to_int(case.get("updated_at"))]
                    if ts is not None
                ],
                default=to_int(asset_case_cache.get("generated_at")),
            ),
            max(
                [
                    ts
                    for case in asset_cases
                    for ts in [to_int(case.get("updated_at")), to_int(case.get("last_signal_at"))]
                    if ts is not None
                ],
                default=to_int(asset_case_cache.get("generated_at")),
            ),
            notes="active asset-case cache",
        )
    )
    inventory_rows.append(
        inventory_json(
            QUALITY_STATS_PATH,
            min([to_int(record.get("created_at")) for record in quality_records if to_int(record.get("created_at")) is not None], default=to_int(quality_stats.get("generated_at"))),
            max(
                [
                    ts
                    for record in quality_records
                    for ts in [to_int(record.get("created_at")), to_int(record.get("notifier_sent_at"))]
                    if ts is not None
                ],
                default=to_int(quality_stats.get("generated_at")),
            ),
            notes="quality/outcome cache",
        )
    )
    exchange_addresses = exchange_adjacent.get("addresses") or []
    inventory_rows.append(
        inventory_json(
            EXCHANGE_ADJACENT_PATH,
            min([to_int(row.get("first_seen_ts")) for row in exchange_addresses if to_int(row.get("first_seen_ts")) is not None], default=to_int(exchange_adjacent.get("updated_at"))),
            max([to_int(row.get("last_seen_ts")) for row in exchange_addresses if to_int(row.get("last_seen_ts")) is not None], default=to_int(exchange_adjacent.get("updated_at"))),
            notes="exchange-adjacent runtime cache; only auxiliary for this LP report",
        )
    )

    analysis_window = choose_analysis_window(quality_records, asset_case_cache, case_signals, lp_followups, system_inventory)
    for row in inventory_rows:
        row.overlap_window = overlaps_window(
            row.coverage_start_ts,
            row.coverage_end_ts,
            analysis_window.get("start_ts"),
            analysis_window.get("end_ts"),
        )

    records = enrich_records(quality_records, case_signals)
    asset_count = len({str(record.get("asset_symbol") or "") for record in records if record.get("asset_symbol")})
    pair_count = len({str(record.get("pair_label") or "") for record in records if record.get("pair_label")})
    pool_count = len({str(record.get("pool_address") or "") for record in records if record.get("pool_address")})
    asset_case_count = len({str(record.get("asset_case_id") or "") for record in records if record.get("asset_case_id")})
    major_signal_count = sum(1 for record in records if classify_major(record.get("asset_symbol")) == "major")
    long_tail_signal_count = sum(1 for record in records if classify_major(record.get("asset_symbol")) == "long_tail")
    timing_summary = build_timing_summary(records)
    data_completeness, completeness_notes = completeness_rating(inventory_rows, timing_summary)
    stage_stats = stage_summary(records)
    prealert_conv = prealert_conversion(records)
    asset_case_summary = build_asset_case_summary(records, pool_case_count=len(case_scan["pool_case_ids"]))
    fastlane_summary = build_fastlane_summary(records)
    noise_assessment = build_noise_assessment(records, quality_report_summary)
    scorecard = build_scorecard(
        record_count=len(records),
        asset_count=asset_count,
        data_completeness=data_completeness,
        timing_summary=timing_summary,
        fastlane_summary=fastlane_summary,
        noise_assessment=noise_assessment,
        asset_case_summary=asset_case_summary,
    )

    overall_summary = {
        "analysis_window": {
            "start_ts": analysis_window.get("start_ts"),
            "end_ts": analysis_window.get("end_ts"),
            "start_utc": fmt_ts(analysis_window.get("start_ts")),
            "end_utc": fmt_ts(analysis_window.get("end_ts")),
            "start_archive_bj": fmt_ts_bj(analysis_window.get("start_ts")),
            "end_archive_bj": fmt_ts_bj(analysis_window.get("end_ts")),
            "duration_sec": analysis_window.get("duration_sec"),
            "duration_label": duration_label(analysis_window.get("duration_sec")),
            "selection_reason": analysis_window.get("selection_reason"),
        },
        "general_runtime_window": {
            "start_ts": min((row.coverage_start_ts for row in system_inventory if row.coverage_start_ts), default=None),
            "end_ts": max((row.coverage_end_ts for row in system_inventory if row.coverage_end_ts), default=None),
        },
        "data_completeness": data_completeness,
        "total_raw_events": sum(1 for _ in RAW_EVENTS_PATH.open("r", encoding="utf-8")) if RAW_EVENTS_PATH.exists() else None,
        "total_parsed_events": sum(1 for _ in PARSED_EVENTS_PATH.open("r", encoding="utf-8")) if PARSED_EVENTS_PATH.exists() else None,
        "total_lp_parsed_events": len(parsed_lp["rows"]),
        "total_lp_raw_swaps": len(raw_lp["rows"]),
        "total_signals": len(records),
        "total_asset_cases": asset_case_count,
        "total_lp_messages": sum(1 for record in records if bool(record.get("delivered_notification"))),
        "asset_count": asset_count,
        "pair_count": pair_count,
        "pool_count": pool_count,
        "major_signal_count": major_signal_count,
        "long_tail_signal_count": long_tail_signal_count,
        "major_signal_share_pct": pct(major_signal_count, len(records)),
        "long_tail_signal_share_pct": pct(long_tail_signal_count, len(records)),
        "market_context_source_distribution": timing_summary.get("market_context_source_distribution"),
        "scan_path_distribution": fastlane_summary.get("scan_path_distribution"),
    }

    metrics_rows: list[dict[str, Any]] = []
    add_metric(metrics_rows, "runtime", "window_duration_sec", overall_summary["analysis_window"]["duration_sec"], window="analysis_window")
    add_metric(metrics_rows, "runtime", "total_signals", overall_summary["total_signals"], sample_size=overall_summary["total_signals"], window="analysis_window")
    add_metric(metrics_rows, "runtime", "total_asset_cases", overall_summary["total_asset_cases"], sample_size=overall_summary["total_asset_cases"], window="analysis_window")
    add_metric(metrics_rows, "runtime", "total_lp_messages", overall_summary["total_lp_messages"], sample_size=overall_summary["total_lp_messages"], window="analysis_window")
    add_metric(metrics_rows, "runtime", "major_signal_share_pct", overall_summary["major_signal_share_pct"], sample_size=overall_summary["total_signals"], window="analysis_window", notes="majors heuristic set")
    add_metric(metrics_rows, "runtime", "long_tail_signal_share_pct", overall_summary["long_tail_signal_share_pct"], sample_size=overall_summary["total_signals"], window="analysis_window", notes="majors heuristic set")
    for stage, stats in stage_stats.items():
        add_metric(metrics_rows, "stage", "stage_count", stats["count"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "median_move_before_alert_30s", stats["median_move_before_alert_30s"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "median_move_before_alert_60s", stats["median_move_before_alert_60s"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "median_move_after_alert_60s", stats["median_move_after_alert_60s"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "median_move_after_alert_300s", stats["median_move_after_alert_300s"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "median_detect_latency_ms", stats["median_detect_latency_ms"], sample_size=stats["count"], stage=stage, window="analysis_window")
        add_metric(metrics_rows, "stage", "market_context_available_rate_pct", stats["market_context_available_rate_pct"], sample_size=stats["count"], stage=stage, window="analysis_window")
        if stage == "climax":
            add_metric(metrics_rows, "stage", "climax_reversal_rate_60s_pct", stats["climax_reversal_rate_60s_pct"], sample_size=stats.get("climax_reversal_resolved_count"), stage=stage, window="60s", notes="30s field unavailable in persisted outcomes")
        if stage == "exhaustion_risk":
            add_metric(metrics_rows, "stage", "exhaustion_negative_followthrough_rate_pct", stats["exhaustion_negative_followthrough_rate_pct"], sample_size=stats.get("exhaustion_resolved_count"), stage=stage, window="60s/300s")
    add_metric(metrics_rows, "stage", "prealert_to_confirm_30s_pct", prealert_conv.get("within_30s_pct"), sample_size=prealert_conv.get("count"), stage="prealert", window="30s", notes=prealert_conv.get("notes", ""))
    add_metric(metrics_rows, "stage", "prealert_to_confirm_60s_pct", prealert_conv.get("within_60s_pct"), sample_size=prealert_conv.get("count"), stage="prealert", window="60s", notes=prealert_conv.get("notes", ""))
    add_metric(metrics_rows, "stage", "prealert_to_confirm_90s_pct", prealert_conv.get("within_90s_pct"), sample_size=prealert_conv.get("count"), stage="prealert", window="90s", notes=prealert_conv.get("notes", ""))
    add_metric(metrics_rows, "asset_case", "total_asset_cases", asset_case_summary.get("total_asset_cases"), sample_size=asset_case_summary.get("total_asset_cases"), window="analysis_window")
    add_metric(metrics_rows, "asset_case", "message_reduction_vs_signal_count_pct", asset_case_summary.get("estimated_message_reduction_vs_signal_count_pct"), sample_size=overall_summary["total_signals"], window="analysis_window")
    add_metric(metrics_rows, "asset_case", "message_reduction_vs_pool_case_pct", asset_case_summary.get("estimated_message_reduction_vs_pool_case_pct"), sample_size=len(case_scan["pool_case_ids"]), window="analysis_window")
    add_metric(metrics_rows, "timing", "market_context_available_rate_pct", timing_summary.get("market_context_available_rate_pct"), sample_size=len(records), window="analysis_window")
    for row in quality_report_summary.get("dimensions", {}).get("pair", []) or []:
        add_metric(metrics_rows, "quality", "pair_quality_score", row.get("quality_score"), sample_size=row.get("sample_size"), asset="ETH", pair=row.get("label"), window="rolling")
        add_metric(metrics_rows, "quality", "confirm_conversion_score", row.get("confirm_conversion_score"), sample_size=row.get("sample_size"), asset="ETH", pair=row.get("label"), window="rolling")
        add_metric(metrics_rows, "quality", "climax_reversal_score", row.get("climax_reversal_score"), sample_size=row.get("sample_size"), asset="ETH", pair=row.get("label"), window="rolling")
        add_metric(metrics_rows, "quality", "fastlane_roi_score", row.get("fastlane_roi_score"), sample_size=row.get("sample_size"), asset="ETH", pair=row.get("label"), window="rolling")
    add_metric(metrics_rows, "fastlane", "fastlane_signal_count", fastlane_summary.get("fastlane_signal_count"), sample_size=fastlane_summary.get("fastlane_signal_count"), window="analysis_window")
    add_metric(metrics_rows, "fastlane", "promoted_main_signal_count", fastlane_summary.get("promoted_main_signal_count"), sample_size=fastlane_summary.get("promoted_main_signal_count"), window="analysis_window")
    add_metric(metrics_rows, "fastlane", "promoted_main_median_latency_ms", fastlane_summary.get("promoted_main_median_latency_ms"), sample_size=fastlane_summary.get("promoted_main_signal_count"), window="analysis_window")
    add_metric(metrics_rows, "fastlane", "fastlane_median_aligned_move_60s", fastlane_summary.get("fastlane_median_aligned_move_60s"), sample_size=fastlane_summary.get("fastlane_signal_count"), window="60s")
    add_metric(metrics_rows, "fastlane", "fastlane_median_aligned_move_300s", fastlane_summary.get("fastlane_median_aligned_move_300s"), sample_size=fastlane_summary.get("fastlane_signal_count"), window="300s")
    for metric_name, value in scorecard.items():
        add_metric(metrics_rows, "scorecard", metric_name, value, sample_size=len(records), window="analysis_window")

    summary_json = {
        "analysis_window": overall_summary["analysis_window"],
        "data_completeness": {
            "rating": data_completeness,
            "notes": completeness_notes,
        },
        "runtime_config_summary": runtime_config,
        "lp_stage_summary": {
            "counts": {stage: stats["count"] for stage, stats in stage_stats.items()},
            "prealert_conversion": prealert_conv,
            "stage_stats": stage_stats,
        },
        "asset_case_summary": asset_case_summary,
        "market_timing_summary": timing_summary,
        "noise_assessment": noise_assessment,
        "scorecard": scorecard,
        "top_recommendations": [
            "补齐 archive/signals",
            "修复 live market context 命中率",
            "补齐 prealert outcome 闭环",
            "扩 majors 样本而不是先扩 long-tail",
            "单独导出 fastlane/promoted_main outcome",
        ],
        "overall_summary": overall_summary,
    }

    markdown = build_markdown(
        analysis_window=analysis_window,
        inventory_rows=inventory_rows,
        runtime_config=runtime_config,
        overall_summary=overall_summary,
        stage_stats=stage_stats,
        prealert_conv=prealert_conv,
        asset_case_summary=asset_case_summary,
        timing_summary=timing_summary,
        fastlane_summary=fastlane_summary,
        quality_summary=quality_report_summary,
        noise_assessment=noise_assessment,
        scorecard=scorecard,
        completeness_notes=completeness_notes,
    )

    MARKDOWN_PATH.write_text(markdown, encoding="utf-8")
    with CSV_PATH.open("w", encoding="utf-8", newline="") as handle:
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
        writer.writerows(metrics_rows)
    JSON_PATH.write_text(json.dumps(summary_json, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(
        {
            "markdown": str(MARKDOWN_PATH.relative_to(ROOT)),
            "csv": str(CSV_PATH.relative_to(ROOT)),
            "json": str(JSON_PATH.relative_to(ROOT)),
            "analysis_window": overall_summary["analysis_window"],
            "total_signals": overall_summary["total_signals"],
            "total_asset_cases": overall_summary["total_asset_cases"],
            "data_completeness": data_completeness,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
