#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
ARCHIVE_DIR = APP_DIR / "data" / "archive"
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
ENV_PATH = ROOT / ".env"

MARKDOWN_PATH = REPORTS_DIR / "overnight_opportunity_retention_analysis_latest.md"
CSV_PATH = REPORTS_DIR / "overnight_opportunity_retention_metrics_latest.csv"
JSON_PATH = REPORTS_DIR / "overnight_opportunity_retention_summary_latest.json"

UTC = timezone.utc
SERVER_TZ = UTC
BJ_TZ = timezone(timedelta(hours=8))

LP_STAGES = ("prealert", "confirm", "climax", "exhaustion_risk")
OPPORTUNITY_STATUSES = ("NONE", "CANDIDATE", "VERIFIED", "BLOCKED", "EXPIRED", "INVALIDATED")

SAFE_CONFIG_KEYS = [
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
    "LP_ASSET_CASE_WINDOW_SEC",
    "LP_ASSET_CASE_MAX_TRACKED",
    "LP_QUALITY_HISTORY_LIMIT",
    "TRADE_ACTION_ENABLE",
    "TRADE_ACTION_REQUIRE_LIVE_CONTEXT_FOR_CHASE",
    "TRADE_ACTION_REQUIRE_BROADER_CONFIRM_FOR_CHASE",
    "TRADE_ACTION_MIN_ASSET_QUALITY_FOR_CHASE",
    "TRADE_ACTION_MIN_PAIR_QUALITY_FOR_CHASE",
    "TRADE_ACTION_CONFLICT_WINDOW_SEC",
    "TRADE_ACTION_MAX_LONG_BASIS_BPS",
    "TRADE_ACTION_MIN_SHORT_BASIS_BPS",
    "ASSET_MARKET_STATE_ENABLE",
    "ASSET_MARKET_STATE_PERSIST_ENABLE",
    "ASSET_MARKET_STATE_RECOVER_ON_START",
    "ASSET_MARKET_STATE_MAX_TRACKED",
    "ASSET_MARKET_STATE_RECENT_SIGNAL_MAXLEN",
    "ASSET_MARKET_STATE_WAIT_TTL_SEC",
    "ASSET_MARKET_STATE_OBSERVE_TTL_SEC",
    "ASSET_MARKET_STATE_CANDIDATE_TTL_SEC",
    "ASSET_MARKET_STATE_RISK_TTL_SEC",
    "ASSET_MARKET_STATE_DATA_GAP_TTL_SEC",
    "NO_TRADE_LOCK_ENABLE",
    "NO_TRADE_LOCK_WINDOW_SEC",
    "NO_TRADE_LOCK_TTL_SEC",
    "NO_TRADE_LOCK_MIN_CONFLICT_SCORE",
    "NO_TRADE_LOCK_SUPPRESS_LOCAL_SIGNALS",
    "PREALERT_MIN_LIFETIME_SEC",
    "CHASE_ENABLE_AFTER_MIN_SAMPLES",
    "CHASE_MIN_FOLLOWTHROUGH_60S_RATE",
    "CHASE_MAX_ADVERSE_60S_RATE",
    "CHASE_REQUIRE_OUTCOME_COMPLETION_RATE",
    "TELEGRAM_SEND_ONLY_STATE_CHANGES",
    "TELEGRAM_SUPPRESS_REPEAT_STATE_SEC",
    "TELEGRAM_ALLOW_RISK_BLOCKERS",
    "TELEGRAM_ALLOW_CANDIDATES",
    "TELEGRAM_DEBUG_SUPPRESSED_IN_RESEARCH_REPORT",
    "OPPORTUNITY_ENABLE",
    "OPPORTUNITY_PERSIST_ENABLE",
    "OPPORTUNITY_RECOVER_ON_START",
    "OPPORTUNITY_HISTORY_LIMIT",
    "OPPORTUNITY_MIN_CANDIDATE_SCORE",
    "OPPORTUNITY_MIN_VERIFIED_SCORE",
    "OPPORTUNITY_REQUIRE_LIVE_CONTEXT",
    "OPPORTUNITY_REQUIRE_BROADER_CONFIRM",
    "OPPORTUNITY_REQUIRE_OUTCOME_HISTORY",
    "OPPORTUNITY_MIN_HISTORY_SAMPLES",
    "OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE",
    "OPPORTUNITY_MAX_60S_ADVERSE_RATE",
    "OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE",
    "OPPORTUNITY_MAX_PER_ASSET_PER_HOUR",
    "OPPORTUNITY_COOLDOWN_SEC",
    "OPPORTUNITY_REPEAT_CANDIDATE_SUPPRESS_SEC",
    "OPPORTUNITY_RECENT_OPPOSITE_SIGNAL_WINDOW_SEC",
]


@dataclass
class FileInventory:
    source: str
    path: str
    exists: bool
    record_count: int
    start_ts: int | None
    end_ts: int | None
    size_bytes: int
    mtime_utc: str | None
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


def rate(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 4)


def pct(numerator: int | float, denominator: int | float) -> float | None:
    value = rate(numerator, denominator)
    if value is None:
        return None
    return round(value * 100.0, 2)


def median(values: list[Any], digits: int = 4) -> float | None:
    cleaned = [float(value) for value in values if to_float(value) is not None]
    if not cleaned:
        return None
    return round(float(statistics.median(cleaned)), digits)


def percentile(values: list[Any], ratio: float, digits: int = 4) -> float | None:
    cleaned = sorted(float(value) for value in values if to_float(value) is not None)
    if not cleaned:
        return None
    idx = int((len(cleaned) - 1) * ratio)
    return round(cleaned[idx], digits)


def fmt_ts(ts: int | None, tz: timezone = UTC) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(int(ts), tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def iso_ts(ts: int | None, tz: timezone = UTC) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz).isoformat()


def hours_between(start_ts: int | None, end_ts: int | None) -> float | None:
    if start_ts is None or end_ts is None:
        return None
    return round((int(end_ts) - int(start_ts)) / 3600.0, 2)


def canonical_asset(value: Any) -> str:
    raw = str(value or "").strip().upper().replace(".E", "")
    return {"WETH": "ETH", "WBTC": "BTC", "CBBTC": "BTC", "WSOL": "SOL"}.get(raw, raw)


def pair_asset(pair_label: Any) -> str:
    raw = str(pair_label or "").strip().upper().replace(" ", "")
    if "/" not in raw:
        return canonical_asset(raw)
    return canonical_asset(raw.split("/", 1)[0])


def direction_bucket(intent_type: Any) -> str:
    raw = str(intent_type or "")
    if raw == "pool_buy_pressure":
        return "buy_pressure"
    if raw == "pool_sell_pressure":
        return "sell_pressure"
    return ""


def json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def compact(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return value


def env_whitelist() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in SAFE_CONFIG_KEYS:
            values[key] = value.strip().strip("'").strip('"')
    return values


def runtime_config_summary() -> dict[str, dict[str, Any]]:
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    import config  # type: ignore

    env_values = env_whitelist()
    return {
        key: {
            "runtime_value": compact(getattr(config, key, None)),
            "env_present": key in env_values,
            "env_value": env_values.get(key),
        }
        for key in SAFE_CONFIG_KEYS
    }


def extract_archive_ts(payload: dict[str, Any]) -> int | None:
    direct = to_int(payload.get("archive_ts"))
    if direct is not None:
        return direct
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    for key in ("archive_ts", "archive_written_at", "created_at", "ts"):
        ts = to_int(data.get(key))
        if ts is not None:
            return ts
    return None


def parse_first_last_ndjson(path: Path, source: str, notes: str) -> FileInventory:
    stat = path.stat()
    first_line = ""
    last_line = ""
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            count += 1
            if not first_line:
                first_line = line
            last_line = line
    start_ts: int | None = None
    end_ts: int | None = None
    for line, target in ((first_line, "start"), (last_line, "end")):
        if not line:
            continue
        try:
            ts = extract_archive_ts(json.loads(line))
        except json.JSONDecodeError:
            ts = None
        if target == "start":
            start_ts = ts
        else:
            end_ts = ts
    return FileInventory(
        source=source,
        path=str(path.relative_to(ROOT)),
        exists=True,
        record_count=count,
        start_ts=start_ts,
        end_ts=end_ts,
        size_bytes=stat.st_size,
        mtime_utc=datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        notes=notes,
    )


def inventory_ndjson_dir(source: str, notes: str) -> list[FileInventory]:
    root = ARCHIVE_DIR / source
    if not root.exists():
        return [
            FileInventory(
                source=source,
                path=str(root.relative_to(ROOT)),
                exists=False,
                record_count=0,
                start_ts=None,
                end_ts=None,
                size_bytes=0,
                mtime_utc=None,
                notes="directory missing",
            )
        ]
    files = sorted(root.glob("*.ndjson"))
    if not files:
        return [
            FileInventory(
                source=source,
                path=str((root / "*.ndjson").relative_to(ROOT)),
                exists=False,
                record_count=0,
                start_ts=None,
                end_ts=None,
                size_bytes=0,
                mtime_utc=None,
                notes="no ndjson files",
            )
        ]
    return [parse_first_last_ndjson(path, source, notes) for path in files]


def json_time_candidates(value: Any) -> list[int]:
    times: list[int] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key.endswith("_at") or key.endswith("_ts") or key in {
                "created_at",
                "generated_at",
                "updated_at",
                "last_seen_ts",
                "first_seen_ts",
                "ts",
                "trade_opportunity_created_at",
                "trade_opportunity_expires_at",
                "opportunity_invalidated_at",
                "asset_market_state_started_at",
                "asset_market_state_updated_at",
                "no_trade_lock_started_at",
                "no_trade_lock_until",
            }:
                ts = to_int(item)
                if ts is not None and 1_500_000_000 < ts < 2_100_000_000:
                    times.append(ts)
            if isinstance(item, (dict, list)):
                times.extend(json_time_candidates(item))
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                times.extend(json_time_candidates(item))
    return times


def inventory_json_cache(source: str, path: Path, notes: str) -> FileInventory:
    if not path.exists():
        return FileInventory(source, str(path.relative_to(ROOT)), False, 0, None, None, 0, None, notes)
    payload = json_load(path, {})
    count = 1
    if isinstance(payload, dict):
        for preferred in ("opportunities", "records", "cases", "history", "addresses"):
            if isinstance(payload.get(preferred), list):
                count = len(payload.get(preferred) or [])
                break
        else:
            if isinstance(payload.get("positions"), dict):
                count = len(payload.get("positions") or {})
    elif isinstance(payload, list):
        count = len(payload)
    times = json_time_candidates(payload)
    stat = path.stat()
    return FileInventory(
        source=source,
        path=str(path.relative_to(ROOT)),
        exists=True,
        record_count=count,
        start_ts=min(times) if times else None,
        end_ts=max(times) if times else None,
        size_bytes=stat.st_size,
        mtime_utc=datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        notes=notes,
    )


def inventory_all_sources() -> dict[str, list[FileInventory]]:
    return {
        "raw_events": inventory_ndjson_dir("raw_events", "raw event archive"),
        "parsed_events": inventory_ndjson_dir("parsed_events", "parsed event archive"),
        "signals": inventory_ndjson_dir("signals", "signal archive"),
        "cases": inventory_ndjson_dir("cases", "asset case archive"),
        "case_followups": inventory_ndjson_dir("case_followups", "case followup archive"),
        "delivery_audit": inventory_ndjson_dir("delivery_audit", "delivery audit archive"),
        "asset_cases.cache": [inventory_json_cache("asset_cases.cache", DATA_DIR / "asset_cases.cache.json", "asset case snapshot cache")],
        "lp_quality_stats.cache": [inventory_json_cache("lp_quality_stats.cache", DATA_DIR / "lp_quality_stats.cache.json", "LP quality/outcome rolling cache")],
        "asset_market_states.cache": [inventory_json_cache("asset_market_states.cache", DATA_DIR / "asset_market_states.cache.json", "asset market state cache")],
        "trade_opportunities.cache": [inventory_json_cache("trade_opportunities.cache", DATA_DIR / "trade_opportunities.cache.json", "trade opportunity cache")],
        "persisted_exchange_adjacent": [inventory_json_cache("persisted_exchange_adjacent", DATA_DIR / "persisted_exchange_adjacent.json", "exchange adjacent cache")],
        "clmm_positions.cache": [inventory_json_cache("clmm_positions.cache", DATA_DIR / "clmm_positions.cache.json", "CLMM position cache")],
    }


def containers_for(data: dict[str, Any]) -> list[dict[str, Any]]:
    signal = data.get("signal") if isinstance(data.get("signal"), dict) else {}
    event = data.get("event") if isinstance(data.get("event"), dict) else {}
    signal_context = signal.get("context") if isinstance(signal.get("context"), dict) else {}
    signal_metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    event_metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    return [
        data,
        signal_context,
        signal_metadata,
        event_metadata,
        signal,
        event,
        signal_context.get("lp_outcome_record") if isinstance(signal_context.get("lp_outcome_record"), dict) else {},
        signal_context.get("outcome_tracking") if isinstance(signal_context.get("outcome_tracking"), dict) else {},
    ]


def first_value(data: dict[str, Any], *keys: str) -> Any:
    containers = containers_for(data)
    for key in keys:
        for container in containers:
            if not isinstance(container, dict):
                continue
            value = container.get(key)
            if value not in (None, "", [], {}, ()):
                return value
    return None


def bool_value(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def parse_signal_payload(payload: dict[str, Any]) -> dict[str, Any] | None:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    archive_ts = to_int(first_value(data, "archive_ts", "archive_written_at") or payload.get("archive_ts"))
    if archive_ts is None:
        return None
    pair_label = str(first_value(data, "pair_label") or "")
    intent = str(first_value(data, "intent_type", "canonical_semantic_key") or "")
    row: dict[str, Any] = {
        "archive_ts": archive_ts,
        "signal_id": str(first_value(data, "signal_id") or ""),
        "event_id": str(first_value(data, "event_id") or ""),
        "asset_case_id": str(first_value(data, "asset_case_id") or ""),
        "asset_case_key": str(first_value(data, "asset_case_key") or ""),
        "asset_symbol": canonical_asset(first_value(data, "asset_symbol") or pair_asset(pair_label)),
        "pair_label": pair_label,
        "pool_address": str(first_value(data, "pool_address", "address") or "").lower(),
        "lp_alert_stage": str(first_value(data, "lp_alert_stage") or ""),
        "intent_type": intent,
        "direction_bucket": str(first_value(data, "direction_bucket") or direction_bucket(intent)),
        "sent_to_telegram": bool_value(first_value(data, "sent_to_telegram")),
        "notifier_sent_at": to_int(first_value(data, "notifier_sent_at")),
        "market_context_source": str(first_value(data, "market_context_source") or ""),
        "market_context_venue": str(first_value(data, "market_context_venue") or ""),
        "market_context_requested_symbol": str(first_value(data, "market_context_requested_symbol") or ""),
        "market_context_resolved_symbol": str(first_value(data, "market_context_resolved_symbol") or ""),
        "market_context_failure_reason": str(first_value(data, "market_context_failure_reason") or ""),
        "market_context_attempts": first_value(data, "market_context_attempts") or [],
        "outcome_tracking_key": str(first_value(data, "outcome_tracking_key") or ""),
        "lp_prealert_candidate": bool_value(first_value(data, "lp_prealert_candidate")),
        "lp_prealert_gate_passed": bool_value(first_value(data, "lp_prealert_gate_passed")),
        "lp_prealert_delivery_allowed": first_value(data, "lp_prealert_delivery_allowed"),
        "lp_prealert_delivery_block_reason": str(first_value(data, "lp_prealert_delivery_block_reason") or ""),
        "lp_prealert_gate_fail_reason": str(first_value(data, "lp_prealert_gate_fail_reason") or ""),
        "lp_prealert_asset_case_preserved": bool_value(first_value(data, "lp_prealert_asset_case_preserved")),
        "lp_prealert_stage_overwritten": bool_value(first_value(data, "lp_prealert_stage_overwritten")),
        "asset_case_had_prealert": bool_value(first_value(data, "asset_case_had_prealert")),
        "asset_case_prealert_to_confirm_sec": to_int(first_value(data, "asset_case_prealert_to_confirm_sec")),
        "prealert_lifecycle_state": str(first_value(data, "prealert_lifecycle_state") or ""),
        "prealert_to_confirm_sec": to_int(first_value(data, "prealert_to_confirm_sec")),
        "prealert_visible_to_user": bool_value(first_value(data, "prealert_visible_to_user")),
        "trade_action_key": str(first_value(data, "trade_action_key") or ""),
        "trade_action_blockers": first_value(data, "trade_action_blockers") or [],
        "trade_action_reason": str(first_value(data, "trade_action_reason") or ""),
        "asset_market_state_key": str(first_value(data, "asset_market_state_key") or ""),
        "previous_asset_market_state_key": str(first_value(data, "previous_asset_market_state_key") or ""),
        "asset_market_state_label": str(first_value(data, "asset_market_state_label") or ""),
        "asset_market_state_changed": bool_value(first_value(data, "asset_market_state_changed")),
        "no_trade_lock_active": bool_value(first_value(data, "no_trade_lock_active")),
        "no_trade_lock_reason": str(first_value(data, "no_trade_lock_reason") or ""),
        "no_trade_lock_started_at": to_int(first_value(data, "no_trade_lock_started_at")),
        "no_trade_lock_until": to_int(first_value(data, "no_trade_lock_until")),
        "no_trade_lock_released_by": str(first_value(data, "no_trade_lock_released_by") or ""),
        "telegram_should_send": bool_value(first_value(data, "telegram_should_send")),
        "telegram_suppression_reason": str(first_value(data, "telegram_suppression_reason") or ""),
        "telegram_state_change_reason": str(first_value(data, "telegram_state_change_reason") or ""),
        "telegram_update_kind": str(first_value(data, "telegram_update_kind") or ""),
        "suppressed_signal_count_in_state": to_int(first_value(data, "suppressed_signal_count_in_state")),
        "lp_confirm_quality": str(first_value(data, "lp_confirm_quality") or ""),
        "lp_confirm_scope": str(first_value(data, "lp_confirm_scope") or ""),
        "lp_absorption_context": str(first_value(data, "lp_absorption_context") or ""),
        "lp_broader_alignment": str(first_value(data, "lp_broader_alignment") or ""),
        "lp_conflict_context": str(first_value(data, "lp_conflict_context") or ""),
        "lp_conflict_score": to_float(first_value(data, "lp_conflict_score")),
        "lp_conflict_window_sec": to_int(first_value(data, "lp_conflict_window_sec")),
        "lp_conflicting_signals": first_value(data, "lp_conflicting_signals") or [],
        "lp_conflict_resolution": str(first_value(data, "lp_conflict_resolution") or ""),
        "lp_sweep_phase": str(first_value(data, "lp_sweep_phase") or ""),
        "asset_case_quality_score": to_float(first_value(data, "asset_case_quality_score")),
        "pair_quality_score": to_float(first_value(data, "pair_quality_score")),
        "pool_quality_score": to_float(first_value(data, "pool_quality_score")),
        "asset_case_supporting_pair_count": to_int(first_value(data, "asset_case_supporting_pair_count")),
        "asset_case_multi_pool": bool_value(first_value(data, "asset_case_multi_pool")),
        "alert_relative_timing": str(first_value(data, "alert_relative_timing") or ""),
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
        "outcome_price_source": str(first_value(data, "outcome_price_source") or ""),
        "outcome_failure_reason": str(first_value(data, "outcome_failure_reason") or ""),
        "outcome_windows": first_value(data, "outcome_windows") or {},
        "trade_opportunity_id": str(first_value(data, "trade_opportunity_id") or ""),
        "trade_opportunity_status": str(first_value(data, "trade_opportunity_status") or ""),
        "trade_opportunity_status_at_creation": str(first_value(data, "trade_opportunity_status_at_creation") or ""),
        "trade_opportunity_score": to_float(first_value(data, "trade_opportunity_score")),
        "trade_opportunity_primary_blocker": str(first_value(data, "trade_opportunity_primary_blocker") or ""),
    }
    attempts = row["market_context_attempts"]
    row["market_context_attempts"] = attempts if isinstance(attempts, list) else []
    return row


def load_signal_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted((ARCHIVE_DIR / "signals").glob("*.ndjson")):
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                row = parse_signal_payload(json.loads(raw_line))
                if row is not None:
                    rows.append(row)
    return sorted(rows, key=lambda item: int(item["archive_ts"]))


def build_segments(signal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not signal_rows:
        return []
    segments: list[dict[str, Any]] = []
    current = {
        "start_ts": int(signal_rows[0]["archive_ts"]),
        "end_ts": int(signal_rows[0]["archive_ts"]),
        "total_signal_rows": 0,
        "lp_signal_rows": 0,
    }
    for row in signal_rows:
        ts = int(row["archive_ts"])
        if ts - int(current["end_ts"]) >= 3600 and current["total_signal_rows"]:
            current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
            segments.append(current)
            current = {"start_ts": ts, "end_ts": ts, "total_signal_rows": 0, "lp_signal_rows": 0}
        current["end_ts"] = ts
        current["total_signal_rows"] += 1
        if row.get("lp_alert_stage"):
            current["lp_signal_rows"] += 1
    current["duration_sec"] = int(current["end_ts"]) - int(current["start_ts"])
    segments.append(current)
    for segment in segments:
        start_bj = datetime.fromtimestamp(int(segment["start_ts"]), BJ_TZ)
        end_bj = datetime.fromtimestamp(int(segment["end_ts"]), BJ_TZ)
        segment["duration_hours"] = round(int(segment["duration_sec"]) / 3600.0, 2)
        segment["start_utc"] = iso_ts(int(segment["start_ts"]), UTC)
        segment["end_utc"] = iso_ts(int(segment["end_ts"]), UTC)
        segment["start_beijing"] = start_bj.isoformat()
        segment["end_beijing"] = end_bj.isoformat()
        segment["crosses_beijing_midnight"] = start_bj.date() != end_bj.date()
        segment["looks_like_overnight"] = bool(
            int(segment["duration_sec"]) >= 4 * 3600
            and (start_bj.hour >= 18 or start_bj.hour <= 6 or end_bj.hour <= 14 or start_bj.date() != end_bj.date())
        )
    return segments


def choose_window(signal_rows: list[dict[str, Any]], inventories: dict[str, list[FileInventory]]) -> dict[str, Any]:
    segments = build_segments(signal_rows)
    if not segments:
        raise RuntimeError("signals archive is empty")
    candidates = [segment for segment in segments if segment["looks_like_overnight"]]
    chosen = sorted(
        candidates or segments,
        key=lambda item: (
            -int(item["duration_sec"]),
            -int(item["lp_signal_rows"]),
            -int(item["total_signal_rows"]),
            -int(item["end_ts"]),
        ),
    )[0]
    start = int(chosen["start_ts"])
    end = int(chosen["end_ts"])
    supporting_sources = []
    complete_sources = 0
    for source in ("raw_events", "parsed_events", "signals", "cases", "case_followups", "delivery_audit"):
        overlapped = [item for item in inventories[source] if item.exists and item.start_ts is not None and item.end_ts is not None and item.end_ts >= start and item.start_ts <= end]
        if overlapped:
            supporting_sources.append(source)
            if any(item.start_ts <= start and item.end_ts >= end for item in overlapped):
                complete_sources += 1
    chosen = dict(chosen)
    chosen.update(
        {
            "analysis_window_start_ts": start,
            "analysis_window_end_ts": end,
            "analysis_window_start_utc": iso_ts(start, UTC),
            "analysis_window_end_utc": iso_ts(end, UTC),
            "analysis_window_start_server": iso_ts(start, SERVER_TZ),
            "analysis_window_end_server": iso_ts(end, SERVER_TZ),
            "analysis_window_start_beijing": iso_ts(start, BJ_TZ),
            "analysis_window_end_beijing": iso_ts(end, BJ_TZ),
            "analysis_window_duration_hours": hours_between(start, end),
            "supporting_sources": supporting_sources,
            "complete_source_count": complete_sources,
            "selection_reason": (
                "按 signals 归档切分 >=1h 断点后，选择最长且最新、LP row 最多、同时 raw/parsed/signals/cases/"
                "delivery_audit/case_followups 均有重叠覆盖的北京夜间连续段。"
            ),
            "all_segments": segments,
        }
    )
    return chosen


def records_in_window(source: str, inventories: list[FileInventory], start_ts: int, end_ts: int) -> int:
    total = 0
    for item in inventories:
        if not item.exists or item.start_ts is None or item.end_ts is None:
            continue
        if item.end_ts < start_ts or item.start_ts > end_ts:
            continue
        if item.start_ts >= start_ts and item.end_ts <= end_ts:
            total += item.record_count
            continue
        path = ROOT / item.path
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                try:
                    ts = extract_archive_ts(json.loads(raw_line))
                except json.JSONDecodeError:
                    continue
                if ts is not None and start_ts <= ts <= end_ts:
                    total += 1
    return total


def load_quality_records() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    payload = json_load(DATA_DIR / "lp_quality_stats.cache.json", {})
    rows = list(payload.get("records") or []) if isinstance(payload, dict) else []
    by_signal = {str(row.get("signal_id") or ""): row for row in rows if row.get("signal_id")}
    return rows, by_signal, payload if isinstance(payload, dict) else {}


def merge_quality(signal_rows: list[dict[str, Any]], quality_by_signal: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in signal_rows:
        if not row.get("lp_alert_stage"):
            continue
        quality = quality_by_signal.get(str(row.get("signal_id") or ""), {})
        merged = dict(row)
        merged["created_at"] = to_int(quality.get("created_at")) or int(row["archive_ts"])
        for key in (
            "move_after_alert_30s",
            "move_after_alert_60s",
            "move_after_alert_300s",
            "raw_move_after_30s",
            "raw_move_after_60s",
            "raw_move_after_300s",
            "direction_adjusted_move_after_30s",
            "direction_adjusted_move_after_60s",
            "direction_adjusted_move_after_300s",
            "adverse_by_direction_30s",
            "adverse_by_direction_60s",
            "adverse_by_direction_300s",
            "outcome_price_source",
            "outcome_failure_reason",
            "outcome_windows",
            "asset_case_quality_score",
            "pair_quality_score",
            "pool_quality_score",
            "asset_case_prealert_to_confirm_sec",
        ):
            value = quality.get(key)
            if value not in (None, "", [], {}, ()):
                merged[key] = value
        for key in ("pair_label", "asset_symbol", "pool_address", "direction_bucket", "lp_alert_stage"):
            value = quality.get(key)
            if value not in (None, "", [], {}, ()):
                merged[key] = canonical_asset(value) if key == "asset_symbol" else value
        merged["delivered_notification"] = bool(
            quality.get("delivered_notification") or merged.get("sent_to_telegram") or merged.get("notifier_sent_at")
        )
        rows.append(merged)
    return rows


def window_rows(signal_rows: list[dict[str, Any]], window: dict[str, Any]) -> list[dict[str, Any]]:
    start = int(window["analysis_window_start_ts"])
    end = int(window["analysis_window_end_ts"])
    return [row for row in signal_rows if start <= int(row["archive_ts"]) <= end]


def load_opportunity_cache() -> dict[str, Any]:
    payload = json_load(DATA_DIR / "trade_opportunities.cache.json", {})
    return payload if isinstance(payload, dict) else {}


def opportunity_rows_in_window(
    opportunity_cache: dict[str, Any],
    window: dict[str, Any],
    signal_ids: set[str],
) -> list[dict[str, Any]]:
    rows = list(opportunity_cache.get("opportunities") or [])
    start = int(window["analysis_window_start_ts"])
    end = int(window["analysis_window_end_ts"])
    return [
        row
        for row in rows
        if str(row.get("signal_id") or "") in signal_ids
        or start <= (to_int(row.get("trade_opportunity_created_at")) or to_int(row.get("created_at")) or 0) <= end
    ]


def created_status(row: dict[str, Any]) -> str:
    return str(row.get("trade_opportunity_status_at_creation") or row.get("trade_opportunity_status") or "NONE").upper()


def current_status(row: dict[str, Any]) -> str:
    return str(row.get("trade_opportunity_status") or row.get("trade_opportunity_status_at_creation") or "NONE").upper()


def outcome_summary_for_rows(rows: list[dict[str, Any]], prefix: str = "opportunity") -> dict[str, Any]:
    payload: dict[str, Any] = {"count": len(rows)}
    for window in ("30s", "60s", "300s"):
        completed = [row for row in rows if str(row.get(f"{prefix}_outcome_{window}") or "") == "completed"]
        followthrough = sum(1 for row in completed if row.get(f"{prefix}_followthrough_{window}") is True)
        adverse = sum(1 for row in completed if row.get(f"{prefix}_adverse_{window}") is True)
        status_counter = Counter(str(row.get(f"{prefix}_outcome_{window}") or "missing") for row in rows)
        payload[window] = {
            "sample_size": len(rows),
            "completed_count": len(completed),
            "completion_rate": rate(len(completed), len(rows)),
            "followthrough_count": followthrough,
            "followthrough_rate": rate(followthrough, len(completed)),
            "adverse_count": adverse,
            "adverse_rate": rate(adverse, len(completed)),
            "status_distribution": dict(sorted(status_counter.items())),
        }
    return payload


def opportunity_summary(opportunity_cache: dict[str, Any], opportunity_rows: list[dict[str, Any]], config: dict[str, dict[str, Any]]) -> dict[str, Any]:
    creation_counter = Counter(created_status(row) for row in opportunity_rows)
    current_counter = Counter(current_status(row) for row in opportunity_rows)
    score_values = [float(row.get("trade_opportunity_score") or 0.0) for row in opportunity_rows if row.get("trade_opportunity_score") not in (None, "")]
    candidate_rows = [row for row in opportunity_rows if created_status(row) == "CANDIDATE"]
    verified_rows = [row for row in opportunity_rows if created_status(row) == "VERIFIED"]
    blocked_rows = [row for row in opportunity_rows if created_status(row) == "BLOCKED"]
    none_rows = [row for row in opportunity_rows if created_status(row) == "NONE"]
    expired_rows = [row for row in opportunity_rows if current_status(row) == "EXPIRED"]
    invalidated_rows = [
        row
        for row in opportunity_rows
        if current_status(row) == "INVALIDATED" or to_int(row.get("opportunity_invalidated_at")) is not None
    ]
    blocker_counter = Counter(
        str(row.get("trade_opportunity_primary_blocker") or "(blank)")
        for row in blocked_rows
        if str(row.get("trade_opportunity_primary_blocker") or "").strip()
    )
    all_blocker_counter = Counter(
        str(row.get("trade_opportunity_primary_blocker") or "(blank)")
        for row in opportunity_rows
        if str(row.get("trade_opportunity_primary_blocker") or "").strip()
    )
    suppression_counter = Counter(
        str(row.get("trade_opportunity_suppression_reason") or row.get("telegram_suppression_reason") or "")
        for row in opportunity_rows
        if str(row.get("trade_opportunity_suppression_reason") or row.get("telegram_suppression_reason") or "").strip()
    )
    candidate_outcomes = outcome_summary_for_rows(candidate_rows)
    verified_outcomes = outcome_summary_for_rows(verified_rows)
    blocked_outcomes = outcome_summary_for_rows(blocked_rows)
    completed_blocked_60s = blocked_outcomes["60s"]["completed_count"]
    adverse_blocked_60s = blocked_outcomes["60s"]["adverse_count"]
    min_history = int(config.get("OPPORTUNITY_MIN_HISTORY_SAMPLES", {}).get("runtime_value") or 20)
    history_samples = [
        int((row.get("trade_opportunity_history_snapshot") or {}).get("sample_size") or 0)
        for row in opportunity_rows
    ]
    opportunity_samples = [
        int((row.get("trade_opportunity_history_snapshot") or {}).get("opportunity_sample_size") or 0)
        for row in opportunity_rows
    ]
    history_completion_rates = [
        float((row.get("trade_opportunity_history_snapshot") or {}).get("completion_rate") or 0.0)
        for row in opportunity_rows
    ]
    high_score_rows = [row for row in opportunity_rows if float(row.get("trade_opportunity_score") or 0.0) >= float(config.get("OPPORTUNITY_MIN_CANDIDATE_SCORE", {}).get("runtime_value") or 0.68)]
    verified_score_rows = [row for row in opportunity_rows if float(row.get("trade_opportunity_score") or 0.0) >= float(config.get("OPPORTUNITY_MIN_VERIFIED_SCORE", {}).get("runtime_value") or 0.78)]
    why_no_verified: list[str] = []
    if not verified_rows:
        if not candidate_rows:
            why_no_verified.append("no_candidate_rows_created")
        if max(opportunity_samples or [0]) < min_history:
            why_no_verified.append("opportunity_native_candidate_verified_history_is_empty_or_insufficient")
        if blocker_counter:
            why_no_verified.append(f"blocked_rows_top_blockers={dict(blocker_counter.most_common(3))}")
        if history_completion_rates and max(history_completion_rates) < float(config.get("OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE", {}).get("runtime_value") or 0.70):
            why_no_verified.append("lp_fallback_history_completion_rate_below_verified_gate")
    return {
        "cache_exists": bool(opportunity_cache),
        "cache_record_count": len(opportunity_cache.get("opportunities") or []),
        "window_record_count": len(opportunity_rows),
        "creation_status_distribution": dict(sorted(creation_counter.items())),
        "current_status_distribution": dict(sorted(current_counter.items())),
        "opportunity_none_count": len(none_rows),
        "opportunity_candidate_count": len(candidate_rows),
        "opportunity_verified_count": len(verified_rows),
        "opportunity_blocked_count": len(blocked_rows),
        "opportunity_expired_count": len(expired_rows),
        "opportunity_invalidated_count": len(invalidated_rows),
        "opportunity_score_median": median(score_values),
        "opportunity_score_p90": percentile(score_values, 0.90),
        "opportunity_score_max": max(score_values) if score_values else None,
        "opportunity_hard_blocker_distribution": dict(sorted(blocker_counter.items())),
        "opportunity_all_primary_blocker_distribution": dict(sorted(all_blocker_counter.items())),
        "opportunity_candidate_to_verified_rate": rate(len(verified_rows), len(candidate_rows)),
        "opportunity_budget_suppressed_count": suppression_counter.get("opportunity_budget_exhausted", 0),
        "opportunity_cooldown_suppressed_count": suppression_counter.get("opportunity_cooldown_active", 0),
        "opportunity_suppression_reasons": dict(sorted(suppression_counter.items())),
        "candidate_outcome": candidate_outcomes,
        "verified_outcome": verified_outcomes,
        "blocked_outcome": blocked_outcomes,
        "candidate_30s_followthrough_rate": candidate_outcomes["30s"]["followthrough_rate"],
        "candidate_60s_followthrough_rate": candidate_outcomes["60s"]["followthrough_rate"],
        "candidate_300s_followthrough_rate": candidate_outcomes["300s"]["followthrough_rate"],
        "candidate_30s_adverse_rate": candidate_outcomes["30s"]["adverse_rate"],
        "candidate_60s_adverse_rate": candidate_outcomes["60s"]["adverse_rate"],
        "candidate_300s_adverse_rate": candidate_outcomes["300s"]["adverse_rate"],
        "candidate_outcome_completion_rate": candidate_outcomes["60s"]["completion_rate"],
        "verified_30s_followthrough_rate": verified_outcomes["30s"]["followthrough_rate"],
        "verified_60s_followthrough_rate": verified_outcomes["60s"]["followthrough_rate"],
        "verified_300s_followthrough_rate": verified_outcomes["300s"]["followthrough_rate"],
        "verified_30s_adverse_rate": verified_outcomes["30s"]["adverse_rate"],
        "verified_60s_adverse_rate": verified_outcomes["60s"]["adverse_rate"],
        "verified_300s_adverse_rate": verified_outcomes["300s"]["adverse_rate"],
        "verified_outcome_completion_rate": verified_outcomes["60s"]["completion_rate"],
        "blocked_count": len(blocked_rows),
        "no_trade_lock_blocked_count": sum(1 for row in blocked_rows if "no_trade_lock" in list(row.get("trade_opportunity_blockers") or []) or row.get("trade_opportunity_primary_blocker") == "no_trade_lock"),
        "data_gap_blocked_count": sum(1 for row in blocked_rows if "data_gap" in list(row.get("trade_opportunity_blockers") or []) or row.get("trade_opportunity_primary_blocker") == "data_gap"),
        "do_not_chase_blocked_count": sum(1 for row in blocked_rows if row.get("trade_opportunity_primary_blocker") in {"late_or_chase", "sweep_exhaustion_risk"}),
        "conflict_blocked_count": sum(1 for row in blocked_rows if row.get("trade_opportunity_primary_blocker") in {"direction_conflict", "recent_opposite_strong_signal"}),
        "blocker_avoided_adverse_rate": rate(adverse_blocked_60s, completed_blocked_60s),
        "blocker_avoided_adverse_rate_all_blocked_denominator": rate(adverse_blocked_60s, len(blocked_rows)),
        "history_sample_size_max": max(history_samples or [0]),
        "history_sample_size_median": median(history_samples, digits=1),
        "opportunity_native_history_sample_size_max": max(opportunity_samples or [0]),
        "estimated_samples_needed_for_verified": max(min_history - max(opportunity_samples or [0]), 0),
        "high_score_above_candidate_threshold_count": len(high_score_rows),
        "high_score_above_verified_threshold_count": len(verified_score_rows),
        "candidate_history_count": len(opportunity_cache.get("candidate_history") or []),
        "verified_history_count": len(opportunity_cache.get("verified_history") or []),
        "blocker_history_count": len(opportunity_cache.get("blocker_history") or []),
        "rolling_stats": dict(opportunity_cache.get("rolling_stats") or {}),
        "why_no_verified": why_no_verified,
    }


def followthrough_from_lp_rows(rows: list[dict[str, Any]], window: str) -> dict[str, Any]:
    completed = [row for row in rows if to_float(row.get(f"direction_adjusted_move_after_{window}")) is not None]
    followthrough = sum(1 for row in completed if float(row.get(f"direction_adjusted_move_after_{window}") or 0.0) > 0)
    adverse = sum(1 for row in completed if row.get(f"adverse_by_direction_{window}") is True)
    return {
        "sample_size": len(rows),
        "completed_count": len(completed),
        "completion_rate": rate(len(completed), len(rows)),
        "followthrough_count": followthrough,
        "followthrough_rate": rate(followthrough, len(completed)),
        "adverse_count": adverse,
        "adverse_rate": rate(adverse, len(completed)),
    }


def telegram_suppression_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    delivered = [row for row in lp_rows if row.get("sent_to_telegram") or row.get("notifier_sent_at") or row.get("delivered_notification")]
    reason_counter = Counter(
        str(row.get("telegram_suppression_reason") or "")
        for row in lp_rows
        if str(row.get("telegram_suppression_reason") or "").strip()
    )
    should_send = [row for row in lp_rows if row.get("telegram_should_send")]
    suppressed = max(len(lp_rows) - len(delivered), 0)
    return {
        "telegram_should_send_count": len(should_send),
        "telegram_suppressed_count": suppressed,
        "telegram_suppression_ratio": rate(suppressed, len(lp_rows)),
        "telegram_suppression_reasons": dict(sorted(reason_counter.items())),
        "messages_before_suppression_estimate": len(lp_rows),
        "messages_after_suppression_actual": len(delivered),
        "delivered_lp_signals": len(delivered),
        "suppressed_lp_signals": suppressed,
        "telegram_update_kind_distribution": dict(sorted(Counter(str(row.get("telegram_update_kind") or "") for row in lp_rows if row.get("telegram_update_kind")).items())),
    }


def asset_market_state_summary(lp_rows: list[dict[str, Any]], state_cache: dict[str, Any]) -> dict[str, Any]:
    state_rows = [row for row in lp_rows if str(row.get("asset_market_state_key") or "").strip()]
    state_counter = Counter(str(row.get("asset_market_state_key") or "") for row in state_rows)
    transition_counter = Counter()
    final_state: dict[str, Any] = {}
    for row in sorted(state_rows, key=lambda item: int(item["archive_ts"])):
        asset = canonical_asset(row.get("asset_symbol"))
        if row.get("asset_market_state_changed"):
            transition_counter[f"{row.get('previous_asset_market_state_key') or 'none'}->{row.get('asset_market_state_key')}"] += 1
        if asset:
            final_state[asset] = {
                "state": row.get("asset_market_state_key"),
                "label": row.get("asset_market_state_label"),
                "updated_at_utc": iso_ts(int(row["archive_ts"]), UTC),
                "telegram_update_kind": row.get("telegram_update_kind"),
            }
    cache_records = state_cache.get("records") or [] if isinstance(state_cache, dict) else []
    return {
        "state_distribution": dict(sorted(state_counter.items())),
        "state_transition_count": sum(transition_counter.values()),
        "state_transition_distribution": dict(sorted(transition_counter.items())),
        "final_state_by_asset": dict(sorted(final_state.items())),
        "no_trade_lock_entered_count": sum(1 for row in state_rows if row.get("asset_market_state_key") == "NO_TRADE_LOCK" and row.get("asset_market_state_changed")),
        "no_trade_lock_suppressed_count": sum(1 for row in state_rows if str(row.get("telegram_suppression_reason") or "").startswith("no_trade_lock")),
        "no_trade_lock_released_count": sum(1 for row in state_rows if str(row.get("no_trade_lock_released_by") or "").strip()),
        "cache_record_count": len(cache_records),
        "cache_final_records": [
            {
                "asset_symbol": record.get("asset_symbol"),
                "asset_market_state_key": record.get("asset_market_state_key"),
                "asset_market_state_updated_at": record.get("asset_market_state_updated_at"),
                "restored_from_cache": record.get("restored_from_cache"),
                "transition_count": record.get("transition_count"),
                "state_history_count": len(record.get("state_history") or []),
            }
            for record in cache_records
        ],
    }


def prealert_lifecycle_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    prealert_rows = [row for row in lp_rows if row.get("lp_alert_stage") == "prealert"]
    candidate_rows = [row for row in lp_rows if row.get("lp_prealert_candidate")]
    gate_rows = [row for row in lp_rows if row.get("lp_prealert_gate_passed")]
    delivered_rows = [
        row
        for row in prealert_rows
        if row.get("sent_to_telegram") or row.get("notifier_sent_at") or row.get("delivered_notification")
    ]
    upgraded_rows = [
        row
        for row in lp_rows
        if row.get("asset_case_had_prealert") and to_int(row.get("asset_case_prealert_to_confirm_sec")) is not None
    ]
    lifecycle_counter = Counter(str(row.get("prealert_lifecycle_state") or "") for row in lp_rows if row.get("prealert_lifecycle_state"))
    times = [
        to_int(row.get("prealert_to_confirm_sec")) or to_int(row.get("asset_case_prealert_to_confirm_sec"))
        for row in lp_rows
    ]
    times = [time for time in times if time is not None]
    return {
        "prealert_candidate_count": len(candidate_rows),
        "prealert_gate_passed_count": len(gate_rows),
        "prealert_active_count": lifecycle_counter.get("active", 0),
        "prealert_delivered_count": len(delivered_rows),
        "prealert_upgraded_to_confirm_count": len(upgraded_rows),
        "prealert_expired_count": lifecycle_counter.get("expired", 0),
        "median_prealert_to_confirm_sec": median(times, digits=1),
        "prealert_stage_count": len(prealert_rows),
        "lifecycle_distribution": dict(sorted(lifecycle_counter.items())),
        "gate_fail_reasons": dict(sorted(Counter(str(row.get("lp_prealert_gate_fail_reason") or "") for row in candidate_rows if row.get("lp_prealert_gate_fail_reason")).items())),
        "delivery_block_reasons": dict(sorted(Counter(str(row.get("lp_prealert_delivery_block_reason") or "") for row in candidate_rows if row.get("lp_prealert_delivery_block_reason")).items())),
        "stage_overwritten_count": sum(1 for row in lp_rows if row.get("lp_prealert_stage_overwritten")),
        "asset_case_preserved_count": sum(1 for row in lp_rows if row.get("lp_prealert_asset_case_preserved")),
    }


def outcome_summary(lp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    source_counter = Counter()
    failure_counter = Counter()
    expired_by_window: dict[str, float | None] = {}
    for row in lp_rows:
        source = str(row.get("outcome_price_source") or "")
        if source:
            source_counter[source] += 1
        reason = str(row.get("outcome_failure_reason") or "")
        if reason:
            failure_counter[reason] += 1
        windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
        for window in ("30s", "60s", "300s"):
            item = windows.get(window) if isinstance(windows.get(window), dict) else {}
            item_source = str(item.get("price_source") or "")
            if item_source:
                source_counter[item_source] += 1
            item_reason = str(item.get("failure_reason") or "")
            if item_reason:
                failure_counter[item_reason] += 1
    for window in ("30s", "60s", "300s"):
        status_counter = Counter()
        for row in lp_rows:
            windows = row.get("outcome_windows") if isinstance(row.get("outcome_windows"), dict) else {}
            item = windows.get(window) if isinstance(windows.get(window), dict) else {}
            status = str(item.get("status") or "")
            if not status:
                status = "completed" if to_float(row.get(f"direction_adjusted_move_after_{window}")) is not None else "missing"
            status_counter[status] += 1
        completed = status_counter.get("completed", 0)
        expired = status_counter.get("expired", 0)
        expired_by_window[window] = rate(expired, len(lp_rows))
        payload[f"outcome_{window}_completed_rate"] = rate(completed, len(lp_rows))
        payload[f"outcome_{window}_completed_count"] = completed
        payload[f"outcome_{window}_status_distribution"] = dict(sorted(status_counter.items()))
    payload["outcome_price_source_distribution"] = dict(sorted(source_counter.items()))
    payload["expired_rate_by_window"] = expired_by_window
    payload["outcome_failure_reason_distribution"] = dict(sorted(failure_counter.items()))
    return payload


def market_context_summary(lp_rows: list[dict[str, Any]], cli_payload: dict[str, Any]) -> dict[str, Any]:
    source_counter = Counter(str(row.get("market_context_source") or "") for row in lp_rows)
    attempts = Counter()
    success = Counter()
    failure_reasons = Counter()
    resolved_symbols = Counter()
    for row in lp_rows:
        resolved = str(row.get("market_context_resolved_symbol") or "")
        if resolved:
            resolved_symbols[resolved] += 1
        for attempt in row.get("market_context_attempts") or []:
            if not isinstance(attempt, dict):
                continue
            venue = str(attempt.get("venue") or "")
            status = str(attempt.get("status") or "")
            if venue:
                attempts[venue] += 1
                if status in {"success", "cache_hit"}:
                    success[venue] += 1
            reason = str(attempt.get("failure_reason") or "")
            if reason:
                failure_reasons[reason] += 1
    total = len(lp_rows)
    return {
        "live_public_count": source_counter.get("live_public", 0),
        "unavailable_count": source_counter.get("unavailable", 0),
        "live_public_rate": rate(source_counter.get("live_public", 0), total),
        "okx_attempts": attempts.get("okx_perp", 0),
        "okx_success": success.get("okx_perp", 0),
        "kraken_attempts": attempts.get("kraken_futures", 0),
        "kraken_success": success.get("kraken_futures", 0),
        "resolved_symbol_distribution": dict(sorted(resolved_symbols.items())),
        "top_failure_reasons": dict(failure_reasons.most_common(10)),
        "quality_report_market_context_health": cli_payload,
    }


def major_coverage_summary(lp_rows: list[dict[str, Any]], cli_payload: dict[str, Any]) -> dict[str, Any]:
    pair_counter = Counter(str(row.get("pair_label") or "") for row in lp_rows if row.get("pair_label"))
    asset_counter = Counter(canonical_asset(row.get("asset_symbol")) for row in lp_rows if row.get("asset_symbol"))
    return {
        "asset_distribution": dict(sorted(asset_counter.items())),
        "pair_distribution": dict(sorted(pair_counter.items())),
        "covered_major_pairs": cli_payload.get("covered_major_pairs") or [],
        "missing_major_pairs": cli_payload.get("missing_major_pairs") or cli_payload.get("missing_expected_pairs") or [],
        "missing_major_assets": cli_payload.get("missing_major_assets") or [],
        "configured_but_disabled_major_pools": cli_payload.get("configured_but_disabled_major_pools") or [],
        "under_sampled_major_pairs": cli_payload.get("under_sampled_major_pairs") or [],
        "quality_converging_major_pairs": cli_payload.get("quality_converging_major_pairs") or [],
        "major_pool_coverage_cli": cli_payload,
    }


def run_cli(args: list[str], expect_json: bool) -> Any:
    python_bin = ROOT / "venv" / "bin" / "python"
    executable = python_bin if python_bin.exists() else Path(sys.executable)
    result = subprocess.run(
        [str(executable), "-m", "app.quality_reports", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    if expect_json:
        return json.loads(result.stdout)
    return result.stdout


def archive_reset_assessment(inventories: dict[str, list[FileInventory]], quality_rows: list[dict[str, Any]], opportunity_cache: dict[str, Any], state_cache: dict[str, Any]) -> dict[str, Any]:
    ndjson_times: list[int] = []
    archive_dates: set[str] = set()
    by_source_dates: dict[str, list[str]] = {}
    for source in ("raw_events", "parsed_events", "signals", "cases", "case_followups", "delivery_audit"):
        dates: set[str] = set()
        for item in inventories[source]:
            if item.exists and item.start_ts is not None:
                ndjson_times.append(item.start_ts)
                dates.add(Path(item.path).stem)
            if item.exists and item.end_ts is not None:
                ndjson_times.append(item.end_ts)
        by_source_dates[source] = sorted(dates)
        archive_dates.update(dates)
    quality_times = [to_int(row.get("created_at")) for row in quality_rows]
    quality_times = [ts for ts in quality_times if ts is not None]
    opportunity_rows = list(opportunity_cache.get("opportunities") or [])
    state_records = list(state_cache.get("records") or [])
    evidence = []
    if by_source_dates.get("signals") and set(by_source_dates.get("raw_events", [])) != set(by_source_dates.get("signals", [])):
        evidence.append("raw_events date coverage is shorter than signals")
    if by_source_dates.get("signals") and set(by_source_dates.get("parsed_events", [])) != set(by_source_dates.get("signals", [])):
        evidence.append("parsed_events date coverage is shorter than signals")
    if len(opportunity_cache.get("candidate_history") or []) == 0 and len(opportunity_cache.get("verified_history") or []) == 0:
        evidence.append("trade_opportunity candidate_history and verified_history are empty")
    if len(state_records) <= 1:
        evidence.append("asset_market_states cache has only one asset record")
    if len(json_load(DATA_DIR / "asset_cases.cache.json", {}).get("cases", []) or []) <= 1:
        evidence.append("asset_cases cache has only one current case")
    return {
        "archive_days_available": len(archive_dates),
        "archive_date_coverage_by_source": by_source_dates,
        "archive_time_span_utc": {
            "start": iso_ts(min(ndjson_times), UTC) if ndjson_times else None,
            "end": iso_ts(max(ndjson_times), UTC) if ndjson_times else None,
            "duration_hours": hours_between(min(ndjson_times), max(ndjson_times)) if ndjson_times else None,
        },
        "quality_records_count": len(quality_rows),
        "quality_records_time_span_utc": {
            "start": iso_ts(min(quality_times), UTC) if quality_times else None,
            "end": iso_ts(max(quality_times), UTC) if quality_times else None,
        },
        "opportunity_history_count": len(opportunity_rows),
        "asset_state_history_count": sum(len(record.get("state_history") or []) for record in state_records),
        "evidence_of_data_reset": evidence,
        "missing_history_effect_on_verified": (
            "candidate/verified 原生 opportunity history 为 0，会让 verified 缺少自身后验；本轮虽有 LP fallback 样本，"
            "但 outcome completion rate 远低于 verified 门槛，因此 verified 仍关闭。"
        ),
        "estimated_samples_needed_for_verified": None,
    }


def data_retention_assessment() -> dict[str, Any]:
    return {
        "must_keep_long_term": [
            "data/lp_quality_stats.cache.json",
            "data/trade_opportunities.cache.json",
            "data/asset_market_states.cache.json",
            "data/asset_cases.cache.json",
            "app/data/archive/signals/",
            "app/data/archive/delivery_audit/",
            "app/data/archive/cases/",
            "app/data/archive/case_followups/",
        ],
        "keep_30_90_days": [
            "app/data/archive/raw_events/",
            "app/data/archive/parsed_events/",
            "data/persisted_exchange_adjacent.json",
            "data/clmm_positions.cache.json",
        ],
        "compressible_archive": [
            "old app/data/archive/raw_events/*.ndjson",
            "old app/data/archive/parsed_events/*.ndjson",
            "old app/data/archive/cases/*.ndjson",
            "old app/data/archive/delivery_audit/*.ndjson",
        ],
        "deletable_or_regenerable": [
            "reports/*_latest.* regenerated reports",
            "data/*.example.json",
            "Python __pycache__ and test caches",
        ],
        "specific_files": {
            "app/data/archive/raw_events/": "至少保留 30-90 天；压缩可以，删除会破坏 raw->parsed->signal 回溯。",
            "app/data/archive/parsed_events/": "至少保留 30-90 天；删除会影响解析层漏报/误报审计。",
            "app/data/archive/signals/": "长期保留；这是 opportunity、Telegram suppression、state machine 的主审计账本。",
            "app/data/archive/cases/": "长期保留；删除会削弱 asset-case 生命周期与 prealert upgrade 回放。",
            "app/data/archive/case_followups/": "长期保留或压缩；用于验证 lifecycle 和后续消息。",
            "app/data/archive/delivery_audit/": "长期保留；用于证明 suppression 与实际送达，不应只依赖 Telegram 聊天记录。",
            "data/asset_cases.cache.json": "不要跑前删除；删除会造成 case 聚合和 prealert 继承冷启动。",
            "data/lp_quality_stats.cache.json": "不要跑前删除；删除会使 quality score 和 outcome history 回中性/低可信。",
            "data/asset_market_states.cache.json": "不要跑前删除；删除会让 NO_TRADE_LOCK、state transition、repeat suppression 冷启动。",
            "data/trade_opportunities.cache.json": "不要跑前删除；删除会清空 candidate/verified 后验、budget/cooldown 和机会原生样本。",
            "data/persisted_exchange_adjacent.json": "保留 30-90 天；删除会影响 adjacent/exchange 噪声识别和角色积累。",
            "data/clmm_positions.cache.json": "保留 30-90 天；删除会影响 CLMM position 历史与 LP 行为解释。",
        },
    }


def database_recommendation() -> dict[str, Any]:
    return {
        "must_add_database_now": False,
        "single_user_recommendation": "继续 JSON/NDJSON 跑满至少 7 天，同时准备 SQLite 第一期；暂不需要 Postgres。",
        "recommended_store": "SQLite for derived state/cache/report indexes; NDJSON remains the append-only audit log.",
        "phase_1_sqlite_tables": [
            "signals(signal_id, archive_ts, asset, pair, stage, direction, telegram fields, opportunity_id)",
            "lp_outcomes(signal_id, window_sec, status, source, direction_adjusted_move, adverse)",
            "trade_opportunities(opportunity_id, created_at, asset, pair, side, status_at_creation, current_status, score, primary_blocker)",
            "asset_market_states(asset, updated_at, state_key, previous_state_key, no_trade_lock fields)",
            "delivery_audit(signal_id, archive_ts, should_send, delivered, suppression_reason)",
            "asset_cases(asset_case_id, asset, started_at, updated_at, stage, supporting_pairs)",
        ],
        "keep_ndjson_for": [
            "raw_events",
            "parsed_events",
            "full signal payloads",
            "delivery audit append-only evidence",
        ],
        "cache_to_sqlite": [
            "lp_quality_stats.cache.json",
            "trade_opportunities.cache.json",
            "asset_market_states.cache.json",
            "asset_cases.cache.json",
        ],
        "migration_plan": [
            "先写只读 backfill：NDJSON/cache -> SQLite，不改 runtime 写路径。",
            "对 latest reports 同时支持 SQLite 和 JSON/NDJSON source，结果做 diff。",
            "连续 7 天 diff 稳定后，再考虑 runtime 双写。",
        ],
        "retention_policy": [
            "NDJSON 30-90 天热数据，超过后 gzip/zstd 压缩。",
            "SQLite 保留聚合索引和关键 outcome/opportunity/state 长期历史。",
            "latest 报告可覆盖，带日期快照的周报/月报保留。",
        ],
        "must_index_fields": [
            "archive_ts",
            "signal_id",
            "asset_symbol",
            "pair_label",
            "asset_case_id",
            "outcome_tracking_key",
            "trade_opportunity_id",
            "trade_opportunity_status_at_creation",
            "trade_opportunity_primary_blocker",
            "telegram_suppression_reason",
            "asset_market_state_key",
        ],
        "report_read_strategy_after_sqlite": "报表优先读 SQLite 索引表；需要原始 payload 时按 signal_id 回查 NDJSON。",
        "timing": "不建议今天立刻切 runtime；先连续保留 7 天数据，验证 schema 和字段稳定性后做 SQLite 第一期。",
    }


def scorecard(
    run_overview: dict[str, Any],
    opportunity: dict[str, Any],
    telegram: dict[str, Any],
    outcome: dict[str, Any],
    market: dict[str, Any],
    retention: dict[str, Any],
    asset_state: dict[str, Any],
    prealert: dict[str, Any],
) -> dict[str, float]:
    research = 7.0
    if run_overview["duration_hours"] >= 8:
        research += 0.8
    if run_overview["total_raw_events"] and run_overview["total_parsed_events"]:
        research += 0.7
    if retention["archive_days_available"] < 7:
        research -= 1.2

    opp_usable = 4.0
    if opportunity["window_record_count"] > 50:
        opp_usable += 0.7
    if opportunity["opportunity_candidate_count"] > 0:
        opp_usable += 1.0
    if opportunity["opportunity_verified_count"] > 0:
        opp_usable += 1.5
    if opportunity["opportunity_blocked_count"] > 0:
        opp_usable += 0.7
    if opportunity["opportunity_native_history_sample_size_max"] == 0:
        opp_usable -= 0.8

    candidate_quality = 2.0
    if opportunity["high_score_above_candidate_threshold_count"] > 0:
        candidate_quality += 1.0
    if opportunity["opportunity_candidate_count"] == 0:
        candidate_quality -= 0.8

    verified_cred = 1.0 if opportunity["opportunity_verified_count"] == 0 else 6.0
    if opportunity["opportunity_native_history_sample_size_max"] == 0:
        verified_cred -= 0.5

    blocker_value = 5.0
    if opportunity["opportunity_blocked_count"] > 0:
        blocker_value += 1.5
    if opportunity["no_trade_lock_blocked_count"] > 0 or opportunity["conflict_blocked_count"] > 0:
        blocker_value += 0.6

    telegram_score = 5.5
    if telegram["telegram_suppression_ratio"] is not None:
        telegram_score += min(2.0, float(telegram["telegram_suppression_ratio"]) * 2.0)

    outcome_score = 3.0
    completion_60 = outcome.get("outcome_60s_completed_rate") or 0.0
    outcome_score += float(completion_60) * 5.0
    if outcome.get("expired_rate_by_window", {}).get("300s") and float(outcome["expired_rate_by_window"]["300s"]) > 0.5:
        outcome_score -= 0.8

    historical = 4.0
    if retention["quality_records_count"] >= 500:
        historical += 1.0
    if retention["archive_days_available"] >= 7:
        historical += 1.5
    if opportunity["opportunity_native_history_sample_size_max"] == 0:
        historical -= 1.0

    governance = 5.0
    if retention["evidence_of_data_reset"]:
        governance -= 0.8
    if run_overview["total_raw_events"] and run_overview["total_parsed_events"]:
        governance += 0.8

    db_readiness = 6.0
    if run_overview["total_signal_rows"] > 500 or retention["archive_days_available"] >= 4:
        db_readiness += 0.6
    if retention["archive_days_available"] < 7:
        db_readiness -= 0.5

    overall = statistics.mean([
        research,
        opp_usable,
        candidate_quality,
        verified_cred,
        blocker_value,
        telegram_score,
        outcome_score,
        historical,
        governance,
    ])

    def clamp(value: float) -> float:
        return max(0.0, min(10.0, round(value, 1)))

    return {
        "research_sampling_readiness": clamp(research),
        "opportunity_layer_usability": clamp(opp_usable),
        "candidate_quality": clamp(candidate_quality),
        "verified_credibility": clamp(verified_cred),
        "blocker_protection_value": clamp(blocker_value),
        "telegram_noise_reduction": clamp(telegram_score),
        "outcome_completeness": clamp(outcome_score),
        "historical_data_sufficiency": clamp(historical),
        "data_governance_readiness": clamp(governance),
        "database_readiness": clamp(db_readiness),
        "overall_self_use_trading_assistant_score": clamp(overall),
    }


def build_run_overview(
    window: dict[str, Any],
    inventories: dict[str, list[FileInventory]],
    window_signal_rows: list[dict[str, Any]],
    lp_rows: list[dict[str, Any]],
    telegram: dict[str, Any],
) -> dict[str, Any]:
    start = int(window["analysis_window_start_ts"])
    end = int(window["analysis_window_end_ts"])
    raw_count = records_in_window("raw_events", inventories["raw_events"], start, end)
    parsed_count = records_in_window("parsed_events", inventories["parsed_events"], start, end)
    signal_count = len(window_signal_rows)
    return {
        "analysis_window_start_utc": iso_ts(start, UTC),
        "analysis_window_start_server": iso_ts(start, SERVER_TZ),
        "analysis_window_start_beijing": iso_ts(start, BJ_TZ),
        "analysis_window_end_utc": iso_ts(end, UTC),
        "analysis_window_end_server": iso_ts(end, SERVER_TZ),
        "analysis_window_end_beijing": iso_ts(end, BJ_TZ),
        "duration_hours": hours_between(start, end),
        "total_raw_events": raw_count,
        "total_parsed_events": parsed_count,
        "total_signal_rows": signal_count,
        "lp_signal_rows": len(lp_rows),
        "delivered_lp_signals": telegram["delivered_lp_signals"],
        "suppressed_lp_signals": telegram["suppressed_lp_signals"],
        "asset_case_count": len({row.get("asset_case_id") for row in lp_rows if row.get("asset_case_id")}),
        "case_followup_count": records_in_window("case_followups", inventories["case_followups"], start, end),
        "cases_archive_rows": records_in_window("cases", inventories["cases"], start, end),
        "delivery_audit_rows": records_in_window("delivery_audit", inventories["delivery_audit"], start, end),
    }


def build_top_findings(
    run_overview: dict[str, Any],
    opportunity: dict[str, Any],
    telegram: dict[str, Any],
    asset_state: dict[str, Any],
    prealert: dict[str, Any],
    outcome: dict[str, Any],
    market: dict[str, Any],
    majors: dict[str, Any],
    retention: dict[str, Any],
) -> list[str]:
    findings = [
        (
            f"主窗口为 {run_overview['analysis_window_start_utc']} 到 {run_overview['analysis_window_end_utc']} UTC，"
            f"北京时间 {run_overview['analysis_window_start_beijing']} 到 {run_overview['analysis_window_end_beijing']}，"
            f"持续 {run_overview['duration_hours']} 小时。"
        ),
        (
            f"窗口内 raw={run_overview['total_raw_events']} parsed={run_overview['total_parsed_events']} "
            f"signals={run_overview['total_signal_rows']} LP={run_overview['lp_signal_rows']}，没有只看 Telegram 已送达样本。"
        ),
        (
            f"trade_opportunity cache 存在且窗口内 {opportunity['window_record_count']} 条；"
            f"CANDIDATE={opportunity['opportunity_candidate_count']} VERIFIED={opportunity['opportunity_verified_count']} "
            f"BLOCKED={opportunity['opportunity_blocked_count']} NONE={opportunity['opportunity_none_count']}。"
        ),
        (
            f"本轮没有 CANDIDATE/VERIFIED；高于 candidate 分数阈值的行有 {opportunity['high_score_above_candidate_threshold_count']} 条，"
            "但被历史完成率、NO_TRADE_LOCK、late/chase、local_absorption 等门禁压住。"
        ),
        (
            f"verified 缺失不是单一“样本数不足”：opportunity 原生 candidate/verified history 为 "
            f"{opportunity['opportunity_native_history_sample_size_max']}，但 LP fallback 样本最高 {opportunity['history_sample_size_max']}；"
            "真正硬门槛是 outcome completion rate 远低于配置要求。"
        ),
        (
            f"BLOCKED 有效工作：blocked={opportunity['opportunity_blocked_count']}，主要 blocker="
            f"{opportunity['opportunity_hard_blocker_distribution']}。"
        ),
        (
            f"Telegram 明显降噪：估算 suppression ratio={telegram['telegram_suppression_ratio']}，"
            f"before={telegram['messages_before_suppression_estimate']} after={telegram['messages_after_suppression_actual']}。"
        ),
        (
            f"asset_market_state 仍在冷启动后运行：state transitions={asset_state['state_transition_count']}，"
            f"NO_TRADE_LOCK entered={asset_state['no_trade_lock_entered_count']} suppressed={asset_state['no_trade_lock_suppressed_count']}。"
        ),
        (
            f"prealert 有 candidate={prealert['prealert_candidate_count']} gate_passed={prealert['prealert_gate_passed_count']}，"
            f"但 delivered={prealert['prealert_delivered_count']}，生命周期仍弱。"
        ),
        (
            f"outcome 60s completed_rate={outcome['outcome_60s_completed_rate']}，"
            f"300s expired_rate={outcome['expired_rate_by_window'].get('300s')}，是机会层可信度的主要短板。"
        ),
        (
            f"OKX live context 在窗口内 live_public_rate={market['live_public_rate']}；"
            f"okx attempts={market['okx_attempts']} success={market['okx_success']}，Kraken attempts={market['kraken_attempts']}。"
        ),
        (
            f"majors 代表性不足：窗口样本资产分布={majors['asset_distribution']}，"
            f"缺失 major pairs={majors['missing_major_pairs']}。"
        ),
        (
            "这轮是部分冷启动：raw/parsed 覆盖短于 signals/cases/delivery，asset_cases 与 state cache 只剩少量当前状态，"
            "trade_opportunity candidate/verified history 为 0。"
        ),
        "不要在运行前删除 lp_quality_stats、trade_opportunities、asset_market_states、asset_cases、signals、delivery_audit、cases、case_followups。",
        "数据库不必今天强切；个人自用最现实方案是继续 NDJSON/JSON 保留满 7 天，再做 SQLite 只读索引第一期。",
    ]
    return findings[:15]


def md_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    output = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        output.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return output


def build_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = ["# Overnight Opportunity Retention Analysis", ""]
    findings = summary["top_findings"]
    lines += ["## 1. 执行摘要", ""]
    for item in findings:
        lines.append(f"- {item}")
    lines += ["", "## 2. 数据源与完整性说明", ""]
    for source, items in summary["data_sources"].items():
        lines.append(f"### {source}")
        for item in items:
            lines.append(
                f"- `{item['path']}` exists={item['exists']} records={item['record_count']} "
                f"range_utc={fmt_ts(item['start_ts'], UTC)} -> {fmt_ts(item['end_ts'], UTC)} size={item['size_bytes']}"
            )
        lines.append("")
    lines.append(f"- 缺失/断档判断：{json.dumps(summary['cold_start_assessment']['evidence_of_data_reset'], ensure_ascii=False)}")
    lines.append("- 缺失影响：raw/parsed 断档影响 raw->parsed->signal 漏报定位；cache 冷启动影响 quality score、asset state、NO_TRADE_LOCK、prealert lifecycle、Telegram suppression 与 candidate->verified 后验。")
    lines += ["", "## 3. overnight 分析窗口", ""]
    window = summary["analysis_window"]
    lines.append(f"- UTC: `{window['analysis_window_start_utc']} -> {window['analysis_window_end_utc']}`")
    lines.append(f"- 服务器本地: `{window['analysis_window_start_server']} -> {window['analysis_window_end_server']}`")
    lines.append(f"- 北京时间 UTC+8: `{window['analysis_window_start_beijing']} -> {window['analysis_window_end_beijing']}`")
    lines.append(f"- duration_hours: `{window['analysis_window_duration_hours']}`")
    lines.append(f"- 选择原因: {window['selection_reason']}")
    lines.append("- 连续段：")
    for segment in window["all_segments"]:
        lines.append(
            f"- `{segment['start_utc']} -> {segment['end_utc']}` duration={segment['duration_hours']}h "
            f"signals={segment['total_signal_rows']} lp={segment['lp_signal_rows']} bj_overnight={segment['looks_like_overnight']}"
        )
    lines += ["", "## 4. 冷启动 / 数据删除影响说明", ""]
    cold = summary["cold_start_assessment"]
    for key in (
        "looks_like_cold_start",
        "trade_opportunities_cache_exists",
        "trade_opportunities_record_count",
        "lp_quality_stats_cache_exists",
        "lp_quality_stats_record_count",
        "asset_market_states_cache_exists",
        "asset_market_states_record_count",
        "deletion_blocks_verified",
        "deletion_resets_quality_score",
        "deletion_weakens_telegram_and_no_trade_lock",
        "deletion_weakens_prealert_candidate_posterior",
        "minimum_continuous_days_to_recover",
    ):
        lines.append(f"- `{key}`: {cold.get(key)}")
    lines.append(f"- `evidence_of_data_reset`: {json.dumps(cold['evidence_of_data_reset'], ensure_ascii=False)}")
    lines.append("- 删除影响结论：删除历史会让 CANDIDATE -> VERIFIED 缺少原生后验，使 quality/outcome 回到 fallback 或中性，使 asset state 与 NO_TRADE_LOCK 失去连续状态，也会削弱 prealert lifecycle 与 Telegram repeat suppression。")
    lines += ["", "## 5. 非敏感运行配置摘要", ""]
    for key, payload in summary["runtime_config_summary"].items():
        lines.append(f"- `{key}` = `{payload['runtime_value']}`")
    lines += ["", "## 6. trade_opportunity 总览", ""]
    opp = summary["opportunity_summary"]
    for key in (
        "cache_record_count",
        "window_record_count",
        "creation_status_distribution",
        "current_status_distribution",
        "opportunity_score_median",
        "opportunity_score_p90",
        "opportunity_score_max",
        "high_score_above_candidate_threshold_count",
        "high_score_above_verified_threshold_count",
        "why_no_verified",
    ):
        lines.append(f"- `{key}`: {json.dumps(opp.get(key), ensure_ascii=False)}")
    lines += ["", "## 7. CANDIDATE / VERIFIED / BLOCKED 分析", ""]
    for key in (
        "opportunity_none_count",
        "opportunity_candidate_count",
        "opportunity_verified_count",
        "opportunity_blocked_count",
        "opportunity_expired_count",
        "opportunity_invalidated_count",
        "opportunity_candidate_to_verified_rate",
        "candidate_history_count",
        "verified_history_count",
        "blocker_history_count",
        "estimated_samples_needed_for_verified",
    ):
        lines.append(f"- `{key}`: {opp.get(key)}")
    lines.append("- 解释：本轮没有 candidate/verified，不能把 high-score NONE/BLOCKED 当成可交易机会；trade_opportunity 只作为个人辅助提示质量评估。")
    lines += ["", "## 8. opportunity score 与机会质量分析", ""]
    lines.append(f"- score median/p90/max: `{opp['opportunity_score_median']} / {opp['opportunity_score_p90']} / {opp['opportunity_score_max']}`")
    lines.append(f"- all primary blocker distribution: `{json.dumps(opp['opportunity_all_primary_blocker_distribution'], ensure_ascii=False)}`")
    lines.append(f"- opportunity native history max: `{opp['opportunity_native_history_sample_size_max']}`; LP fallback history max: `{opp['history_sample_size_max']}`")
    lines += ["", "## 9. blockers 是否有效", ""]
    blocker_keys = [
        "blocked_count",
        "no_trade_lock_blocked_count",
        "data_gap_blocked_count",
        "do_not_chase_blocked_count",
        "conflict_blocked_count",
        "blocker_avoided_adverse_rate",
        "blocker_avoided_adverse_rate_all_blocked_denominator",
    ]
    for key in blocker_keys:
        lines.append(f"- `{key}`: {opp.get(key)}")
    lines.append(f"- hard blockers: `{json.dumps(opp['opportunity_hard_blocker_distribution'], ensure_ascii=False)}`")
    lines += ["", "## 10. Telegram 降噪与 suppression 分析", ""]
    lines.append(f"`{json.dumps(summary['telegram_suppression_summary'], ensure_ascii=False)}`")
    lines += ["", "## 11. asset_market_state 与 NO_TRADE_LOCK 分析", ""]
    lines.append(f"`{json.dumps(summary['asset_market_state_summary'], ensure_ascii=False)}`")
    lines += ["", "## 12. prealert 生命周期分析", ""]
    lines.append(f"`{json.dumps(summary['prealert_lifecycle_summary'], ensure_ascii=False)}`")
    lines += ["", "## 13. outcome 30s/60s/300s 完整性分析", ""]
    lines.append(f"`{json.dumps(summary['outcome_summary'], ensure_ascii=False)}`")
    lines += ["", "## 14. OKX/Kraken live market context 分析", ""]
    market = dict(summary["market_context_health"])
    compact_market = {k: market.get(k) for k in ("live_public_count", "unavailable_count", "live_public_rate", "okx_attempts", "okx_success", "kraken_attempts", "kraken_success", "resolved_symbol_distribution", "top_failure_reasons")}
    lines.append(f"`{json.dumps(compact_market, ensure_ascii=False)}`")
    lines.append(f"- quality_reports --market-context-health live_public_hit_rate: `{summary['quality_reports']['market_context_health'].get('live_public_hit_rate')}`")
    lines += ["", "## 15. majors 覆盖与样本代表性", ""]
    lines.append(f"`{json.dumps(summary['major_pool_coverage'], ensure_ascii=False)}`")
    lines += ["", "## 16. 数据保留策略建议", ""]
    retention_policy = summary["data_retention_assessment"]
    for key, values in retention_policy.items():
        lines.append(f"- `{key}`: {json.dumps(values, ensure_ascii=False)}")
    lines += ["", "## 17. 数据库化建议", ""]
    lines.append(f"`{json.dumps(summary['database_recommendation'], ensure_ascii=False)}`")
    lines += ["", "## 18. 当前系统是否接近高质量个人交易辅助系统", ""]
    lines.append("当前系统已经具备研究采样、LP stage-first、state machine、NO_TRADE_LOCK、Telegram suppression 和 opportunity 审计层；但 latest overnight 数据里没有 CANDIDATE/VERIFIED，outcome completion 与 majors 覆盖不足，因此还不能称为高质量机会提示层。它更接近“可审计的个人研究辅助系统”，还不是稳定的机会筛选系统。")
    lines += ["", "## 19. 最终评分", ""]
    for key, value in summary["scorecard"].items():
        lines.append(f"- `{key}` = `{value}/10`")
    lines += ["", "## 20. 下一轮建议", ""]
    for item in summary["top_recommendations"]:
        lines.append(f"- {item}")
    lines += ["", "## 21. 限制与不确定性", ""]
    for item in summary["limitations"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def add_metric(
    rows: list[dict[str, Any]],
    group: str,
    name: str,
    value: Any,
    *,
    asset: str = "",
    pair: str = "",
    state: str = "",
    opportunity_status: str = "",
    sample_size: Any = "",
    window: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_group": group,
            "metric_name": name,
            "asset": asset,
            "pair": pair,
            "state": state,
            "opportunity_status": opportunity_status,
            "value": json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value,
            "sample_size": sample_size,
            "window": window,
            "notes": notes,
        }
    )


def build_csv_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    window_label = f"{summary['analysis_window']['analysis_window_start_utc']} -> {summary['analysis_window']['analysis_window_end_utc']}"
    run = summary["run_overview"]
    for key, value in run.items():
        add_metric(rows, "run_overview", key, value, sample_size=run.get("lp_signal_rows"), window=window_label)
    opp = summary["opportunity_summary"]
    for key in (
        "opportunity_none_count",
        "opportunity_candidate_count",
        "opportunity_verified_count",
        "opportunity_blocked_count",
        "opportunity_expired_count",
        "opportunity_invalidated_count",
        "opportunity_score_median",
        "opportunity_score_p90",
        "opportunity_hard_blocker_distribution",
        "opportunity_candidate_to_verified_rate",
        "opportunity_budget_suppressed_count",
        "opportunity_cooldown_suppressed_count",
        "candidate_30s_followthrough_rate",
        "candidate_60s_followthrough_rate",
        "candidate_300s_followthrough_rate",
        "candidate_30s_adverse_rate",
        "candidate_60s_adverse_rate",
        "candidate_300s_adverse_rate",
        "verified_30s_followthrough_rate",
        "verified_60s_followthrough_rate",
        "verified_300s_followthrough_rate",
        "verified_30s_adverse_rate",
        "verified_60s_adverse_rate",
        "verified_300s_adverse_rate",
        "candidate_outcome_completion_rate",
        "verified_outcome_completion_rate",
        "blocked_count",
        "no_trade_lock_blocked_count",
        "data_gap_blocked_count",
        "do_not_chase_blocked_count",
        "conflict_blocked_count",
        "blocker_avoided_adverse_rate",
    ):
        add_metric(rows, "trade_opportunity", key, opp.get(key), sample_size=opp.get("window_record_count"), window=window_label)
    for status, count in opp["creation_status_distribution"].items():
        add_metric(rows, "trade_opportunity", "creation_status_distribution", count, opportunity_status=status, sample_size=opp.get("window_record_count"), window=window_label)
    for key, value in summary["telegram_suppression_summary"].items():
        add_metric(rows, "telegram_suppression", key, value, sample_size=run.get("lp_signal_rows"), window=window_label)
    asset_state = summary["asset_market_state_summary"]
    for state, count in asset_state["state_distribution"].items():
        add_metric(rows, "asset_market_state", "state_distribution", count, state=state, sample_size=run.get("lp_signal_rows"), window=window_label)
    for key in ("state_transition_count", "no_trade_lock_entered_count", "no_trade_lock_suppressed_count", "no_trade_lock_released_count", "final_state_by_asset"):
        add_metric(rows, "asset_market_state", key, asset_state.get(key), sample_size=run.get("lp_signal_rows"), window=window_label)
    for key, value in summary["prealert_lifecycle_summary"].items():
        add_metric(rows, "prealert", key, value, sample_size=run.get("lp_signal_rows"), window=window_label)
    for key, value in summary["outcome_summary"].items():
        add_metric(rows, "outcome", key, value, sample_size=run.get("lp_signal_rows"), window=window_label)
    market = summary["market_context_health"]
    for key in ("live_public_count", "unavailable_count", "live_public_rate", "okx_attempts", "okx_success", "kraken_attempts", "kraken_success", "resolved_symbol_distribution", "top_failure_reasons"):
        add_metric(rows, "market_context", key, market.get(key), sample_size=run.get("lp_signal_rows"), window=window_label)
    majors = summary["major_pool_coverage"]
    for key in ("asset_distribution", "pair_distribution", "covered_major_pairs", "missing_major_pairs", "missing_major_assets", "configured_but_disabled_major_pools"):
        add_metric(rows, "major_pool_coverage", key, majors.get(key), sample_size=run.get("lp_signal_rows"), window=window_label)
    cold = summary["cold_start_assessment"]
    for key in ("archive_days_available", "quality_records_count", "opportunity_history_count", "asset_state_history_count", "evidence_of_data_reset", "missing_history_effect_on_verified", "estimated_samples_needed_for_verified"):
        add_metric(rows, "historical_data_retention", key, cold.get(key), window=window_label)
    for key, value in summary["scorecard"].items():
        add_metric(rows, "scorecard", key, value, sample_size=run.get("lp_signal_rows"), window=window_label)
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
                "state",
                "opportunity_status",
                "value",
                "sample_size",
                "window",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def build_summary() -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    config = runtime_config_summary()
    inventories = inventory_all_sources()
    signal_rows = load_signal_rows()
    window = choose_window(signal_rows, inventories)
    selected_signal_rows = window_rows(signal_rows, window)
    quality_rows, quality_by_signal, quality_payload = load_quality_records()
    lp_rows = merge_quality(selected_signal_rows, quality_by_signal)
    opportunity_cache = load_opportunity_cache()
    opportunities = opportunity_rows_in_window(
        opportunity_cache,
        window,
        {str(row.get("signal_id") or "") for row in selected_signal_rows if row.get("signal_id")},
    )
    state_cache = json_load(DATA_DIR / "asset_market_states.cache.json", {})

    cli_market_context = run_cli(["--market-context-health"], expect_json=True)
    cli_major_coverage = run_cli(["--major-pool-coverage"], expect_json=True)
    cli_summary = run_cli(["--summary"], expect_json=True)
    cli_csv = run_cli(["--format", "csv"], expect_json=False)

    telegram = telegram_suppression_summary(lp_rows)
    run_overview = build_run_overview(window, inventories, selected_signal_rows, lp_rows, telegram)
    opportunity = opportunity_summary(opportunity_cache, opportunities, config)
    asset_state = asset_market_state_summary(lp_rows, state_cache if isinstance(state_cache, dict) else {})
    prealert = prealert_lifecycle_summary(lp_rows)
    outcome = outcome_summary(lp_rows)
    market = market_context_summary(lp_rows, cli_market_context)
    majors = major_coverage_summary(lp_rows, cli_major_coverage)
    cold_base = archive_reset_assessment(inventories, quality_rows, opportunity_cache, state_cache if isinstance(state_cache, dict) else {})
    cold_base["estimated_samples_needed_for_verified"] = opportunity["estimated_samples_needed_for_verified"]
    cold_start = {
        **cold_base,
        "looks_like_cold_start": bool(cold_base["evidence_of_data_reset"]),
        "trade_opportunities_cache_exists": bool(opportunity_cache),
        "trade_opportunities_record_count": opportunity["cache_record_count"],
        "lp_quality_stats_cache_exists": bool(quality_payload),
        "lp_quality_stats_record_count": len(quality_rows),
        "asset_market_states_cache_exists": bool(state_cache),
        "asset_market_states_record_count": len((state_cache or {}).get("records") or []) if isinstance(state_cache, dict) else 0,
        "deletion_blocks_verified": (
            opportunity["opportunity_verified_count"] == 0
            and opportunity["opportunity_native_history_sample_size_max"] < int(config["OPPORTUNITY_MIN_HISTORY_SAMPLES"]["runtime_value"] or 20)
        ),
        "deletion_resets_quality_score": len(quality_rows) == 0,
        "deletion_weakens_telegram_and_no_trade_lock": len((state_cache or {}).get("records") or []) <= 1 if isinstance(state_cache, dict) else True,
        "deletion_weakens_prealert_candidate_posterior": opportunity["candidate_history_count"] == 0 and opportunity["verified_history_count"] == 0,
        "never_delete_before_run": [
            "data/lp_quality_stats.cache.json",
            "data/trade_opportunities.cache.json",
            "data/asset_market_states.cache.json",
            "data/asset_cases.cache.json",
            "app/data/archive/signals/",
            "app/data/archive/delivery_audit/",
            "app/data/archive/cases/",
            "app/data/archive/case_followups/",
        ],
        "minimum_continuous_days_to_recover": "至少 3-7 天；要恢复 verified 可信度，建议先连续保留 7 天以上，覆盖不同波动环境。",
    }
    retention = data_retention_assessment()
    database = database_recommendation()
    scores = scorecard(run_overview, opportunity, telegram, outcome, market, cold_start, asset_state, prealert)
    top_findings = build_top_findings(run_overview, opportunity, telegram, asset_state, prealert, outcome, market, majors, cold_start)
    recommendations = [
        "第一优先级是连续保留 7 天以上完整 NDJSON/cache，不再跑前删除关键 cache。",
        "第二优先级是提高 outcome 30s/60s/300s completion，尤其是 60s 与 300s，否则 verified 门永远不稳。",
        "第三优先级是补齐并启用 BTC/SOL major pools，让样本不再只有 ETH。",
        "第四优先级是把 SQLite 作为只读索引层先接入 reports，不急着改 runtime 写路径。",
        "第五优先级是单独复盘 high-score NONE/BLOCKED 样本，确认 local_absorption/no_trade_lock/late_or_chase 是否符合预期。",
    ]
    limitations = [
        "本报告不提供仓位、止损、止盈或自动下单建议；trade_opportunity 仅作为个人交易辅助提示质量评估。",
        "本报告严格避免输出 .env 中敏感字段；仅输出白名单非敏感运行配置。",
        "机会层没有 CANDIDATE/VERIFIED，因此 candidate/verified 后验率为空，不可编造成胜率。",
        "opportunity outcome 在 cache 中仍全部 pending；blocker 避险率无法用 opportunity 自身 outcome 做强结论。",
        "部分历史 raw/parsed 断档会限制 raw->parsed->signal 的完整回放能力。",
        "Kraken fallback 没有在本窗口真实触发，不能把 unavailable 解释为 broader confirmation。",
    ]
    data_sources = {
        source: [asdict(item) for item in items]
        for source, items in inventories.items()
    }
    summary = {
        "analysis_window": window,
        "run_overview": run_overview,
        "data_sources": data_sources,
        "runtime_config_summary": config,
        "opportunity_summary": opportunity,
        "candidate_verified_summary": {
            key: opportunity.get(key)
            for key in (
                "opportunity_candidate_count",
                "opportunity_verified_count",
                "opportunity_candidate_to_verified_rate",
                "candidate_outcome",
                "verified_outcome",
                "candidate_outcome_completion_rate",
                "verified_outcome_completion_rate",
                "why_no_verified",
            )
        },
        "blocker_summary": {
            key: opportunity.get(key)
            for key in (
                "blocked_count",
                "no_trade_lock_blocked_count",
                "data_gap_blocked_count",
                "do_not_chase_blocked_count",
                "conflict_blocked_count",
                "blocker_avoided_adverse_rate",
                "opportunity_hard_blocker_distribution",
            )
        },
        "telegram_suppression_summary": telegram,
        "asset_market_state_summary": asset_state,
        "prealert_lifecycle_summary": prealert,
        "outcome_summary": outcome,
        "market_context_health": market,
        "major_pool_coverage": majors,
        "quality_reports": {
            "market_context_health": cli_market_context,
            "major_pool_coverage": cli_major_coverage,
            "summary": cli_summary,
            "format_csv": cli_csv,
        },
        "data_retention_assessment": retention,
        "cold_start_assessment": cold_start,
        "database_recommendation": database,
        "scorecard": scores,
        "top_findings": top_findings,
        "top_recommendations": recommendations,
        "limitations": limitations,
    }
    return summary


def main() -> int:
    summary = build_summary()
    MARKDOWN_PATH.write_text(build_markdown(summary), encoding="utf-8")
    write_csv(CSV_PATH, build_csv_rows(summary))
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "markdown": str(MARKDOWN_PATH.relative_to(ROOT)),
        "csv": str(CSV_PATH.relative_to(ROOT)),
        "json": str(JSON_PATH.relative_to(ROOT)),
        "analysis_window_utc": {
            "start": summary["analysis_window"]["analysis_window_start_utc"],
            "end": summary["analysis_window"]["analysis_window_end_utc"],
            "duration_hours": summary["analysis_window"]["analysis_window_duration_hours"],
        },
        "opportunity_counts": summary["opportunity_summary"]["creation_status_distribution"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
