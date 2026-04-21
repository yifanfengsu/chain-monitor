from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any

from config import (
    ARCHIVE_BASE_DIR,
    MARKET_CONTEXT_PRIMARY_VENUE,
    MARKET_CONTEXT_SECONDARY_VENUE,
    PROJECT_ROOT,
    SQLITE_ARCHIVE_MIRROR_ENABLE,
    SQLITE_BUSY_TIMEOUT_MS,
    SQLITE_DB_PATH,
    SQLITE_ENABLE,
    SQLITE_PRAGMA_SYNCHRONOUS,
    SQLITE_RETENTION_OPPORTUNITY_DAYS,
    SQLITE_RETENTION_OUTCOME_DAYS,
    SQLITE_RETENTION_PARSED_DAYS,
    SQLITE_RETENTION_RAW_DAYS,
    SQLITE_RETENTION_SIGNAL_DAYS,
    SQLITE_SCHEMA_VERSION,
    SQLITE_WAL_MODE,
    SQLITE_WRITE_ASSET_CASES,
    SQLITE_WRITE_ASSET_MARKET_STATES,
    SQLITE_WRITE_CASE_FOLLOWUPS,
    SQLITE_WRITE_DELIVERY_AUDIT,
    SQLITE_WRITE_OUTCOMES,
    SQLITE_WRITE_PARSED_EVENTS,
    SQLITE_WRITE_QUALITY_STATS,
    SQLITE_WRITE_RAW_EVENTS,
    SQLITE_WRITE_SIGNALS,
    SQLITE_WRITE_TRADE_OPPORTUNITIES,
)


_CONNECTION: sqlite3.Connection | None = None
_DB_PATH: Path | None = None
_INIT_FAILED_REASON = ""
_WRITE_ERROR_COUNT = 0
_WARNINGS: list[str] = []
_BATCH_MODE = False

REQUIRED_TABLES = (
    "schema_meta",
    "runs",
    "raw_events",
    "parsed_events",
    "signals",
    "signal_features",
    "market_context_snapshots",
    "market_context_attempts",
    "outcomes",
    "asset_cases",
    "asset_market_states",
    "no_trade_locks",
    "trade_opportunities",
    "opportunity_outcomes",
    "quality_stats",
    "telegram_deliveries",
    "prealert_lifecycle",
    "delivery_audit",
    "case_followups",
)

ARCHIVE_CATEGORY_TABLES = {
    "raw_events": "raw_events",
    "parsed_events": "parsed_events",
    "signals": "signals",
    "delivery_audit": "delivery_audit",
    "case_followups": "case_followups",
}


def _now() -> float:
    return float(time.time())


def _warn(message: str) -> None:
    global _WRITE_ERROR_COUNT
    _WRITE_ERROR_COUNT += 1
    text = f"sqlite_store warning: {message}"
    _WARNINGS.append(text)
    del _WARNINGS[:-25]
    print(text)


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    path = Path(db_path or SQLITE_DB_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _json(value: Any) -> str:
    try:
        return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False)


def _from_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _hash(value: Any, prefix: str = "") -> str:
    digest = hashlib.sha1(_json(value).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}{digest}" if prefix else digest


def _unwrap(record: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(record, dict):
        return {}, {}
    if isinstance(record.get("data"), dict):
        data = dict(record.get("data") or {})
        envelope = dict(record)
        return data, envelope
    return dict(record), {}


def _first(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    containers = [
        data,
        data.get("signal") if isinstance(data.get("signal"), dict) else {},
        (data.get("signal") or {}).get("context") if isinstance(data.get("signal"), dict) else {},
        (data.get("signal") or {}).get("metadata") if isinstance(data.get("signal"), dict) else {},
        data.get("event") if isinstance(data.get("event"), dict) else {},
        (data.get("event") or {}).get("metadata") if isinstance(data.get("event"), dict) else {},
        data.get("followup") if isinstance(data.get("followup"), dict) else {},
        data.get("case") if isinstance(data.get("case"), dict) else {},
    ]
    for key in keys:
        for container in containers:
            if not isinstance(container, dict):
                continue
            value = container.get(key)
            if value not in (None, "", [], {}, ()):
                return value
    return default


def _text(value: Any) -> str | None:
    if value in (None, "", [], {}, ()):
        return None
    return str(value)


def _real(value: Any) -> float | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _bool_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if value in ("", [], {}, ()):
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return 1
    if normalized in {"0", "false", "no", "off"}:
        return 0
    return 1 if bool(value) else 0


def _archive_ts(data: dict[str, Any], envelope: dict[str, Any]) -> float:
    return float(
        _real(data.get("archive_written_at"))
        or _real(data.get("archive_ts"))
        or _real(envelope.get("archive_ts"))
        or _real(data.get("timestamp"))
        or _real(data.get("ts"))
        or _now()
    )


def _signal_id(data: dict[str, Any]) -> str:
    return str(_first(data, "signal_id") or _first(data, "signal_archive_key") or "").strip()


def _direction_from_record(data: dict[str, Any]) -> str | None:
    raw = str(_first(data, "direction", "trade_action_direction", "direction_bucket") or "").strip().lower()
    if raw in {"buy_pressure", "buy", "long", "upward"}:
        return "long"
    if raw in {"sell_pressure", "sell", "short", "downward"}:
        return "short"
    return raw or None


def _execute(sql: str, params: tuple[Any, ...] = ()) -> bool:
    conn = get_connection()
    if conn is None:
        return False
    try:
        conn.execute(sql, params)
        if not _BATCH_MODE:
            conn.commit()
        return True
    except Exception as exc:
        _warn(str(exc))
        return False


def init_sqlite_store(db_path: str | Path | None = None) -> sqlite3.Connection | None:
    global _CONNECTION, _DB_PATH, _INIT_FAILED_REASON
    if not bool(SQLITE_ENABLE):
        _INIT_FAILED_REASON = "disabled"
        return None
    resolved = _resolve_db_path(db_path)
    if _CONNECTION is not None:
        if db_path is None or _DB_PATH == resolved:
            return _CONNECTION
        close()
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(resolved),
            timeout=max(float(SQLITE_BUSY_TIMEOUT_MS or 0) / 1000.0, 0.1),
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_MS or 0)}")
        if bool(SQLITE_WAL_MODE):
            conn.execute("PRAGMA journal_mode=WAL")
        synchronous = str(SQLITE_PRAGMA_SYNCHRONOUS or "NORMAL").upper()
        if synchronous not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            synchronous = "NORMAL"
        conn.execute(f"PRAGMA synchronous={synchronous}")
        _CONNECTION = conn
        _DB_PATH = resolved
        _INIT_FAILED_REASON = ""
        migrate_schema()
        return conn
    except Exception as exc:
        _CONNECTION = None
        _DB_PATH = resolved
        _INIT_FAILED_REASON = str(exc)
        _warn(f"init failed: {exc}")
        return None


def get_connection() -> sqlite3.Connection | None:
    if _CONNECTION is not None:
        return _CONNECTION
    return init_sqlite_store()


def close() -> None:
    global _CONNECTION
    if _CONNECTION is None:
        return
    try:
        _CONNECTION.close()
    finally:
        _CONNECTION = None


def migrate_schema() -> bool:
    conn = get_connection()
    if conn is None:
        return False
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at REAL
            );

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                git_commit TEXT,
                started_at REAL,
                ended_at REAL,
                default_user_tier TEXT,
                market_context_primary_venue TEXT,
                market_context_secondary_venue TEXT,
                config_hash TEXT,
                config_summary_json TEXT,
                created_at REAL,
                updated_at REAL
            );

            CREATE TABLE IF NOT EXISTS raw_events (
                event_id TEXT PRIMARY KEY,
                tx_hash TEXT,
                block_number INTEGER,
                chain TEXT,
                address TEXT,
                pool_address TEXT,
                raw_kind TEXT,
                listener_scan_path TEXT,
                captured_at REAL,
                raw_json TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_raw_events_tx_hash ON raw_events(tx_hash);
            CREATE INDEX IF NOT EXISTS idx_raw_events_block_number ON raw_events(block_number);
            CREATE INDEX IF NOT EXISTS idx_raw_events_captured_at ON raw_events(captured_at);
            CREATE INDEX IF NOT EXISTS idx_raw_events_pool_address ON raw_events(pool_address);

            CREATE TABLE IF NOT EXISTS parsed_events (
                event_id TEXT PRIMARY KEY,
                tx_hash TEXT,
                chain TEXT,
                parsed_kind TEXT,
                role_group TEXT,
                parse_status TEXT,
                lp_alert_stage_candidate TEXT,
                asset TEXT,
                pair TEXT,
                pool_address TEXT,
                parsed_at REAL,
                parsed_json TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_parsed_events_tx_hash ON parsed_events(tx_hash);
            CREATE INDEX IF NOT EXISTS idx_parsed_events_parsed_at ON parsed_events(parsed_at);
            CREATE INDEX IF NOT EXISTS idx_parsed_events_asset ON parsed_events(asset);
            CREATE INDEX IF NOT EXISTS idx_parsed_events_pair ON parsed_events(pair);
            CREATE INDEX IF NOT EXISTS idx_parsed_events_pool_address ON parsed_events(pool_address);

            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                signal_archive_key TEXT UNIQUE,
                event_id TEXT,
                tx_hash TEXT,
                asset_case_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                pair TEXT,
                pool_address TEXT,
                chain TEXT,
                timestamp REAL,
                lp_alert_stage TEXT,
                canonical_semantic_key TEXT,
                trade_action_key TEXT,
                asset_market_state_key TEXT,
                trade_opportunity_status TEXT,
                direction TEXT,
                notional_usd REAL,
                price_impact_ratio REAL,
                volume_surge_ratio REAL,
                multi_pool_resonance REAL,
                same_pool_continuity REAL,
                scan_path TEXT,
                latency_ms REAL,
                market_context_source TEXT,
                alert_relative_timing TEXT,
                notifier_variant TEXT,
                notifier_template TEXT,
                delivery_decision TEXT,
                sent_to_telegram INTEGER,
                notifier_sent_at REAL,
                outcome_tracking_key TEXT,
                signal_json TEXT,
                archive_written_at REAL,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_signals_event_id ON signals(event_id);
            CREATE INDEX IF NOT EXISTS idx_signals_asset_case_id ON signals(asset_case_id);
            CREATE INDEX IF NOT EXISTS idx_signals_trade_opportunity_id ON signals(trade_opportunity_id);
            CREATE INDEX IF NOT EXISTS idx_signals_asset ON signals(asset);
            CREATE INDEX IF NOT EXISTS idx_signals_pair ON signals(pair);
            CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
            CREATE INDEX IF NOT EXISTS idx_signals_lp_alert_stage ON signals(lp_alert_stage);
            CREATE INDEX IF NOT EXISTS idx_signals_trade_action_key ON signals(trade_action_key);
            CREATE INDEX IF NOT EXISTS idx_signals_asset_market_state_key ON signals(asset_market_state_key);
            CREATE INDEX IF NOT EXISTS idx_signals_trade_opportunity_status ON signals(trade_opportunity_status);

            CREATE TABLE IF NOT EXISTS signal_features (
                signal_id TEXT,
                feature_name TEXT,
                feature_value_text TEXT,
                feature_value_real REAL,
                feature_type TEXT,
                created_at REAL,
                PRIMARY KEY(signal_id, feature_name)
            );

            CREATE TABLE IF NOT EXISTS market_context_snapshots (
                context_id TEXT PRIMARY KEY,
                signal_id TEXT,
                asset TEXT,
                pair TEXT,
                venue TEXT,
                resolved_symbol TEXT,
                source TEXT,
                perp_last_price REAL,
                perp_mark_price REAL,
                perp_index_price REAL,
                spot_reference_price REAL,
                basis_bps REAL,
                mark_index_spread_bps REAL,
                funding_rate REAL,
                alert_relative_timing TEXT,
                market_move_before_30s REAL,
                market_move_before_60s REAL,
                market_move_after_60s REAL,
                market_move_after_300s REAL,
                failure_reason TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_signal_id ON market_context_snapshots(signal_id);
            CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_asset ON market_context_snapshots(asset);
            CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_venue ON market_context_snapshots(venue);
            CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_resolved_symbol ON market_context_snapshots(resolved_symbol);
            CREATE INDEX IF NOT EXISTS idx_market_context_snapshots_created_at ON market_context_snapshots(created_at);

            CREATE TABLE IF NOT EXISTS market_context_attempts (
                attempt_id TEXT PRIMARY KEY,
                signal_id TEXT,
                venue TEXT,
                endpoint TEXT,
                requested_symbol TEXT,
                resolved_symbol TEXT,
                http_status TEXT,
                success INTEGER,
                failure_reason TEXT,
                latency_ms REAL,
                created_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_market_context_attempts_signal_id ON market_context_attempts(signal_id);
            CREATE INDEX IF NOT EXISTS idx_market_context_attempts_venue ON market_context_attempts(venue);
            CREATE INDEX IF NOT EXISTS idx_market_context_attempts_endpoint ON market_context_attempts(endpoint);
            CREATE INDEX IF NOT EXISTS idx_market_context_attempts_resolved_symbol ON market_context_attempts(resolved_symbol);
            CREATE INDEX IF NOT EXISTS idx_market_context_attempts_created_at ON market_context_attempts(created_at);

            CREATE TABLE IF NOT EXISTS outcomes (
                outcome_id TEXT PRIMARY KEY,
                signal_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                pair TEXT,
                direction TEXT,
                window_sec INTEGER,
                price_source TEXT,
                start_price REAL,
                end_price REAL,
                raw_move REAL,
                direction_adjusted_move REAL,
                followthrough INTEGER,
                adverse INTEGER,
                reversal INTEGER,
                status TEXT,
                failure_reason TEXT,
                completed_at REAL,
                created_at REAL,
                updated_at REAL,
                UNIQUE(signal_id, window_sec)
            );
            CREATE INDEX IF NOT EXISTS idx_outcomes_signal_id ON outcomes(signal_id);
            CREATE INDEX IF NOT EXISTS idx_outcomes_trade_opportunity_id ON outcomes(trade_opportunity_id);
            CREATE INDEX IF NOT EXISTS idx_outcomes_asset ON outcomes(asset);
            CREATE INDEX IF NOT EXISTS idx_outcomes_pair ON outcomes(pair);
            CREATE INDEX IF NOT EXISTS idx_outcomes_window_sec ON outcomes(window_sec);
            CREATE INDEX IF NOT EXISTS idx_outcomes_status ON outcomes(status);
            CREATE INDEX IF NOT EXISTS idx_outcomes_created_at ON outcomes(created_at);

            CREATE TABLE IF NOT EXISTS asset_cases (
                asset_case_id TEXT PRIMARY KEY,
                asset_case_key TEXT,
                chain TEXT,
                asset TEXT,
                direction TEXT,
                stage TEXT,
                confidence REAL,
                primary_pair TEXT,
                supporting_pairs_json TEXT,
                evidence_summary TEXT,
                started_at REAL,
                updated_at REAL,
                closed_at REAL,
                last_signal_at REAL,
                last_stage_transition_at REAL,
                first_seen_stage TEXT,
                first_seen_at REAL,
                prealert_to_confirm_sec REAL,
                aggregate_metrics_json TEXT,
                quality_snapshot_json TEXT,
                market_context_snapshot_json TEXT,
                case_json TEXT,
                created_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_asset_cases_asset_case_key ON asset_cases(asset_case_key);
            CREATE INDEX IF NOT EXISTS idx_asset_cases_asset ON asset_cases(asset);
            CREATE INDEX IF NOT EXISTS idx_asset_cases_direction ON asset_cases(direction);
            CREATE INDEX IF NOT EXISTS idx_asset_cases_stage ON asset_cases(stage);
            CREATE INDEX IF NOT EXISTS idx_asset_cases_updated_at ON asset_cases(updated_at);

            CREATE TABLE IF NOT EXISTS asset_market_states (
                state_id TEXT PRIMARY KEY,
                asset TEXT,
                previous_state TEXT,
                current_state TEXT,
                state_reason TEXT,
                confidence REAL,
                started_at REAL,
                updated_at REAL,
                ended_at REAL,
                state_changed INTEGER,
                no_trade_lock_active INTEGER,
                evidence_json TEXT,
                required_confirmation TEXT,
                invalidated_by TEXT,
                state_json TEXT,
                created_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_asset_market_states_asset ON asset_market_states(asset);
            CREATE INDEX IF NOT EXISTS idx_asset_market_states_current_state ON asset_market_states(current_state);
            CREATE INDEX IF NOT EXISTS idx_asset_market_states_updated_at ON asset_market_states(updated_at);

            CREATE TABLE IF NOT EXISTS no_trade_locks (
                lock_id TEXT PRIMARY KEY,
                asset TEXT,
                started_at REAL,
                ended_at REAL,
                ttl_sec REAL,
                reason TEXT,
                conflict_score REAL,
                conflicting_signals_json TEXT,
                suppressed_count INTEGER,
                released_by TEXT,
                release_reason TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_no_trade_locks_asset ON no_trade_locks(asset);
            CREATE INDEX IF NOT EXISTS idx_no_trade_locks_started_at ON no_trade_locks(started_at);
            CREATE INDEX IF NOT EXISTS idx_no_trade_locks_ended_at ON no_trade_locks(ended_at);

            CREATE TABLE IF NOT EXISTS trade_opportunities (
                trade_opportunity_id TEXT PRIMARY KEY,
                signal_id TEXT,
                asset TEXT,
                pair TEXT,
                side TEXT,
                status TEXT,
                score REAL,
                confidence TEXT,
                label TEXT,
                reason TEXT,
                primary_blocker TEXT,
                blockers_json TEXT,
                evidence_json TEXT,
                score_components_json TEXT,
                quality_snapshot_json TEXT,
                history_snapshot_json TEXT,
                required_confirmation TEXT,
                invalidated_by TEXT,
                created_at REAL,
                expires_at REAL,
                telegram_sent INTEGER,
                opportunity_json TEXT,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_signal_id ON trade_opportunities(signal_id);
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_asset ON trade_opportunities(asset);
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_side ON trade_opportunities(side);
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_status ON trade_opportunities(status);
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_score ON trade_opportunities(score);
            CREATE INDEX IF NOT EXISTS idx_trade_opportunities_created_at ON trade_opportunities(created_at);

            CREATE TABLE IF NOT EXISTS opportunity_outcomes (
                trade_opportunity_id TEXT,
                window_sec INTEGER,
                raw_move REAL,
                direction_adjusted_move REAL,
                followthrough INTEGER,
                adverse INTEGER,
                result_label TEXT,
                price_source TEXT,
                status TEXT,
                failure_reason TEXT,
                completed_at REAL,
                created_at REAL,
                PRIMARY KEY(trade_opportunity_id, window_sec)
            );

            CREATE TABLE IF NOT EXISTS quality_stats (
                scope_type TEXT,
                scope_key TEXT,
                asset TEXT,
                pair TEXT,
                pool_address TEXT,
                stage TEXT,
                sample_count INTEGER,
                prealert_precision_score REAL,
                confirm_conversion_score REAL,
                climax_reversal_score REAL,
                candidate_followthrough_rate REAL,
                candidate_adverse_rate REAL,
                verified_followthrough_rate REAL,
                verified_adverse_rate REAL,
                outcome_completion_rate REAL,
                fastlane_roi_score REAL,
                stats_json TEXT,
                updated_at REAL,
                PRIMARY KEY(scope_type, scope_key, stage)
            );

            CREATE TABLE IF NOT EXISTS telegram_deliveries (
                telegram_delivery_id TEXT PRIMARY KEY,
                signal_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                template TEXT,
                headline TEXT,
                sent INTEGER,
                sent_at REAL,
                suppressed INTEGER,
                suppression_reason TEXT,
                telegram_update_kind TEXT,
                chat_id_hash TEXT,
                message_json TEXT,
                created_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_signal_id ON telegram_deliveries(signal_id);
            CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_trade_opportunity_id ON telegram_deliveries(trade_opportunity_id);
            CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_asset ON telegram_deliveries(asset);
            CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_sent_at ON telegram_deliveries(sent_at);
            CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_suppressed ON telegram_deliveries(suppressed);

            CREATE TABLE IF NOT EXISTS prealert_lifecycle (
                prealert_id TEXT PRIMARY KEY,
                signal_id TEXT,
                asset_case_id TEXT,
                asset TEXT,
                pair TEXT,
                candidate INTEGER,
                gate_passed INTEGER,
                active INTEGER,
                delivered INTEGER,
                merged INTEGER,
                upgraded_to_confirm INTEGER,
                expired INTEGER,
                suppressed_by_lock INTEGER,
                first_seen_at REAL,
                prealert_to_confirm_sec REAL,
                fail_reason TEXT,
                lifecycle_json TEXT,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_prealert_lifecycle_signal_id ON prealert_lifecycle(signal_id);
            CREATE INDEX IF NOT EXISTS idx_prealert_lifecycle_asset_case_id ON prealert_lifecycle(asset_case_id);
            CREATE INDEX IF NOT EXISTS idx_prealert_lifecycle_asset ON prealert_lifecycle(asset);
            CREATE INDEX IF NOT EXISTS idx_prealert_lifecycle_first_seen_at ON prealert_lifecycle(first_seen_at);

            CREATE TABLE IF NOT EXISTS delivery_audit (
                audit_id TEXT PRIMARY KEY,
                signal_id TEXT,
                event_id TEXT,
                trade_opportunity_id TEXT,
                asset TEXT,
                stage TEXT,
                gate_reason TEXT,
                delivered INTEGER,
                notifier_sent_at REAL,
                telegram_update_kind TEXT,
                suppression_reason TEXT,
                audit_json TEXT,
                archive_written_at REAL,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_signal_id ON delivery_audit(signal_id);
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_event_id ON delivery_audit(event_id);
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_stage ON delivery_audit(stage);
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_archive_written_at ON delivery_audit(archive_written_at);

            CREATE TABLE IF NOT EXISTS case_followups (
                followup_id TEXT PRIMARY KEY,
                case_id TEXT,
                signal_id TEXT,
                asset TEXT,
                stage TEXT,
                status TEXT,
                should_notify INTEGER,
                reason TEXT,
                followup_json TEXT,
                archive_written_at REAL,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_case_followups_case_id ON case_followups(case_id);
            CREATE INDEX IF NOT EXISTS idx_case_followups_signal_id ON case_followups(signal_id);
            CREATE INDEX IF NOT EXISTS idx_case_followups_archive_written_at ON case_followups(archive_written_at);
            """
        )
        conn.execute(
            """
            INSERT INTO schema_meta(key, value, updated_at)
            VALUES('schema_version', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (SQLITE_SCHEMA_VERSION, _now()),
        )
        conn.commit()
        return True
    except Exception as exc:
        _warn(f"migrate failed: {exc}")
        return False


def _upsert(table: str, rows: dict[str, Any], key_columns: tuple[str, ...]) -> bool:
    if not rows:
        return False
    columns = list(rows.keys())
    placeholders = ", ".join("?" for _ in columns)
    update_columns = [column for column in columns if column not in key_columns]
    assignments = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
    conflict = ", ".join(key_columns)
    sql = (
        f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders}) "
        f"ON CONFLICT({conflict}) DO UPDATE SET {assignments}"
        if assignments
        else f"INSERT OR IGNORE INTO {table}({', '.join(columns)}) VALUES({placeholders})"
    )
    return _execute(sql, tuple(rows[column] for column in columns))


def write_raw_event(record: Any) -> bool:
    if not bool(SQLITE_WRITE_RAW_EVENTS):
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    captured_at = _real(_first(data, "captured_at", "ingest_ts", "timestamp", "ts")) or _archive_ts(data, envelope)
    event_id = str(_first(data, "event_id") or "").strip()
    if not event_id:
        event_id = "raw_" + _hash(
            {
                "tx_hash": _first(data, "tx_hash"),
                "block_number": _first(data, "block_number", "block"),
                "address": _first(data, "address", "watch_address"),
                "captured_at": captured_at,
            }
        )[:16]
    return _upsert(
        "raw_events",
        {
            "event_id": event_id,
            "tx_hash": _text(_first(data, "tx_hash")),
            "block_number": _int(_first(data, "block_number", "block")),
            "chain": _text(_first(data, "chain")) or "ethereum",
            "address": _text(_first(data, "address", "watch_address")),
            "pool_address": _text(_first(data, "pool_address", "touched_lp_pool")),
            "raw_kind": _text(_first(data, "raw_kind", "source_kind", "monitor_type")),
            "listener_scan_path": _text(_first(data, "listener_scan_path", "lp_scan_path")),
            "captured_at": captured_at,
            "raw_json": _json(data),
            "created_at": now,
            "updated_at": now,
        },
        ("event_id",),
    )


def write_parsed_event(record: Any) -> bool:
    if not bool(SQLITE_WRITE_PARSED_EVENTS):
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    parsed_at = _real(_first(data, "parsed_at", "timestamp", "ts")) or _archive_ts(data, envelope)
    event_id = str(_first(data, "event_id") or "").strip()
    if not event_id:
        event_id = "parsed_" + _hash(
            {
                "tx_hash": _first(data, "tx_hash"),
                "address": _first(data, "address", "watch_address"),
                "parsed_at": parsed_at,
            }
        )[:16]
    return _upsert(
        "parsed_events",
        {
            "event_id": event_id,
            "tx_hash": _text(_first(data, "tx_hash")),
            "chain": _text(_first(data, "chain")) or "ethereum",
            "parsed_kind": _text(_first(data, "parsed_kind", "intent_type", "source_kind")),
            "role_group": _text(_first(data, "role_group", "strategy_role")),
            "parse_status": _text(_first(data, "parse_status", "lp_parse_status")) or "parsed",
            "lp_alert_stage_candidate": _text(_first(data, "lp_alert_stage_candidate", "lp_alert_stage")),
            "asset": _text(_first(data, "asset_symbol", "token_symbol", "asset_case_label")),
            "pair": _text(_first(data, "pair_label")),
            "pool_address": _text(_first(data, "pool_address", "address", "watch_address")),
            "parsed_at": parsed_at,
            "parsed_json": _json(data),
            "created_at": now,
            "updated_at": now,
        },
        ("event_id",),
    )


def write_signal(record: Any) -> bool:
    if not bool(SQLITE_WRITE_SIGNALS):
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    signal_archive_key = str(_first(data, "signal_archive_key") or "").strip()
    signal_id = _signal_id(data)
    if not signal_id:
        signal_id = "sig_" + _hash({"archive_key": signal_archive_key, "payload": data})[:16]
    if not signal_archive_key:
        signal_archive_key = signal_id
    archive_written_at = _archive_ts(data, envelope)
    ok = _upsert(
        "signals",
        {
            "signal_id": signal_id,
            "signal_archive_key": signal_archive_key,
            "event_id": _text(_first(data, "event_id")),
            "tx_hash": _text(_first(data, "tx_hash")),
            "asset_case_id": _text(_first(data, "asset_case_id")),
            "trade_opportunity_id": _text(_first(data, "trade_opportunity_id")),
            "asset": _text(_first(data, "asset_symbol", "asset_case_label", "asset")),
            "pair": _text(_first(data, "pair_label", "pair")),
            "pool_address": _text(_first(data, "pool_address", "address")),
            "chain": _text(_first(data, "chain")) or "ethereum",
            "timestamp": _real(_first(data, "timestamp", "ts", "created_at")) or archive_written_at,
            "lp_alert_stage": _text(_first(data, "lp_alert_stage", "stage")),
            "canonical_semantic_key": _text(_first(data, "canonical_semantic_key", "intent_type")),
            "trade_action_key": _text(_first(data, "trade_action_key")),
            "asset_market_state_key": _text(_first(data, "asset_market_state_key")),
            "trade_opportunity_status": _text(_first(data, "trade_opportunity_status")),
            "direction": _direction_from_record(data),
            "notional_usd": _real(_first(data, "notional_usd", "usd_value", "amount_usd")),
            "price_impact_ratio": _real(_first(data, "price_impact_ratio", "lp_price_impact_ratio")),
            "volume_surge_ratio": _real(_first(data, "volume_surge_ratio", "lp_pool_volume_surge_ratio")),
            "multi_pool_resonance": _real(_first(data, "lp_multi_pool_resonance", "multi_pool_resonance")),
            "same_pool_continuity": _real(_first(data, "lp_same_pool_continuity", "same_pool_continuity")),
            "scan_path": _text(_first(data, "scan_path", "lp_scan_path")),
            "latency_ms": _real(_first(data, "latency_ms", "lp_detect_latency_ms", "lp_end_to_end_latency_ms")),
            "market_context_source": _text(_first(data, "market_context_source")),
            "alert_relative_timing": _text(_first(data, "alert_relative_timing")),
            "notifier_variant": _text(_first(data, "notifier_variant", "message_variant")),
            "notifier_template": _text(_first(data, "notifier_template", "message_template")),
            "delivery_decision": _text(_first(data, "delivery_decision", "delivery_reason", "delivery_class")),
            "sent_to_telegram": _bool_int(_first(data, "sent_to_telegram")),
            "notifier_sent_at": _real(_first(data, "notifier_sent_at")),
            "outcome_tracking_key": _text(_first(data, "outcome_tracking_key", "record_id")),
            "signal_json": _json(data),
            "archive_written_at": archive_written_at,
            "created_at": now,
            "updated_at": now,
        },
        ("signal_id",),
    )
    if ok:
        _write_signal_features(signal_id, data)
        write_market_context_snapshot(data)
        for attempt in list(_first(data, "market_context_attempts") or []):
            if isinstance(attempt, dict):
                attempt_payload = dict(attempt)
                attempt_payload.setdefault("signal_id", signal_id)
                attempt_payload.setdefault("created_at", archive_written_at)
                write_market_context_attempt(attempt_payload)
        write_outcome(_first(data, "lp_outcome_record", "outcome_tracking") or data)
        upsert_trade_opportunity(data)
        upsert_asset_market_state(data)
        write_telegram_delivery(data)
        write_prealert_lifecycle(data)
    return ok


def _write_signal_features(signal_id: str, data: dict[str, Any]) -> None:
    features = {
        "notional_usd": _first(data, "notional_usd", "usd_value"),
        "price_impact_ratio": _first(data, "price_impact_ratio", "lp_price_impact_ratio"),
        "volume_surge_ratio": _first(data, "volume_surge_ratio", "lp_pool_volume_surge_ratio"),
        "multi_pool_resonance": _first(data, "lp_multi_pool_resonance"),
        "same_pool_continuity": _first(data, "lp_same_pool_continuity"),
        "pool_quality_score": _first(data, "pool_quality_score"),
        "pair_quality_score": _first(data, "pair_quality_score"),
        "asset_case_quality_score": _first(data, "asset_case_quality_score"),
        "trade_action_confidence": _first(data, "trade_action_confidence"),
        "asset_market_state_confidence": _first(data, "asset_market_state_confidence"),
        "trade_opportunity_score": _first(data, "trade_opportunity_score"),
    }
    created_at = _now()
    for name, value in features.items():
        if value in (None, "", [], {}, ()):
            continue
        numeric = _real(value)
        _execute(
            """
            INSERT INTO signal_features(signal_id, feature_name, feature_value_text, feature_value_real, feature_type, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_id, feature_name) DO UPDATE SET
                feature_value_text=excluded.feature_value_text,
                feature_value_real=excluded.feature_value_real,
                feature_type=excluded.feature_type
            """,
            (
                signal_id,
                name,
                None if numeric is not None else str(value),
                numeric,
                "real" if numeric is not None else "text",
                created_at,
            ),
        )


def write_delivery_audit(record: Any) -> bool:
    if not bool(SQLITE_WRITE_DELIVERY_AUDIT):
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    archive_written_at = _archive_ts(data, envelope)
    signal_id = str(_first(data, "signal_id") or "").strip()
    audit_id = str(_first(data, "audit_id", "delivery_audit_id") or "").strip()
    if not audit_id:
        audit_id = "audit_" + _hash({"signal_id": signal_id, "stage": _first(data, "stage"), "ts": archive_written_at, "data": data})[:16]
    ok = _upsert(
        "delivery_audit",
        {
            "audit_id": audit_id,
            "signal_id": signal_id or None,
            "event_id": _text(_first(data, "event_id")),
            "trade_opportunity_id": _text(_first(data, "trade_opportunity_id")),
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "stage": _text(_first(data, "stage")),
            "gate_reason": _text(_first(data, "gate_reason", "delivery_reason")),
            "delivered": _bool_int(_first(data, "delivered_notification", "delivered", "sent_to_telegram")),
            "notifier_sent_at": _real(_first(data, "notifier_sent_at")),
            "telegram_update_kind": _text(_first(data, "telegram_update_kind")),
            "suppression_reason": _text(_first(data, "telegram_suppression_reason", "suppression_reason")),
            "audit_json": _json(data),
            "archive_written_at": archive_written_at,
            "created_at": now,
            "updated_at": now,
        },
        ("audit_id",),
    )
    if ok:
        write_telegram_delivery(data)
    return ok


def write_case_followup(record: Any) -> bool:
    if not bool(SQLITE_WRITE_CASE_FOLLOWUPS):
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    followup = data.get("followup") if isinstance(data.get("followup"), dict) else data
    now = _now()
    archive_written_at = _archive_ts(data, envelope)
    case_id = str(data.get("case_id") or _first(data, "case_id") or "").strip()
    signal_id = str(_first(followup if isinstance(followup, dict) else data, "signal_id") or "").strip()
    followup_id = str(_first(data, "followup_id") or "").strip()
    if not followup_id:
        followup_id = "followup_" + _hash({"case_id": case_id, "signal_id": signal_id, "ts": archive_written_at, "data": followup})[:16]
    return _upsert(
        "case_followups",
        {
            "followup_id": followup_id,
            "case_id": case_id or None,
            "signal_id": signal_id or None,
            "asset": _text(_first(followup if isinstance(followup, dict) else data, "asset_symbol", "asset")),
            "stage": _text(_first(followup if isinstance(followup, dict) else data, "stage")),
            "status": _text(_first(followup if isinstance(followup, dict) else data, "status")),
            "should_notify": _bool_int(_first(followup if isinstance(followup, dict) else data, "should_notify")),
            "reason": _text(_first(followup if isinstance(followup, dict) else data, "reason")),
            "followup_json": _json(data),
            "archive_written_at": archive_written_at,
            "created_at": now,
            "updated_at": now,
        },
        ("followup_id",),
    )


def upsert_asset_case(record: Any) -> bool:
    if not bool(SQLITE_WRITE_ASSET_CASES):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    case = data.get("case") if isinstance(data.get("case"), dict) else data
    if not isinstance(case, dict):
        return False
    asset_case_id = str(case.get("asset_case_id") or case.get("case_id") or _first(case, "asset_case_id") or "").strip()
    if not asset_case_id:
        return False
    created_at = _real(case.get("asset_case_started_at") or case.get("started_at")) or _now()
    return _upsert(
        "asset_cases",
        {
            "asset_case_id": asset_case_id,
            "asset_case_key": _text(case.get("asset_case_key")),
            "chain": _text(case.get("chain")) or "ethereum",
            "asset": _text(case.get("asset_symbol") or case.get("asset")),
            "direction": _text(case.get("asset_case_direction") or case.get("direction")),
            "stage": _text(case.get("asset_case_stage") or case.get("stage")),
            "confidence": _real(case.get("asset_case_confidence") or case.get("confidence")),
            "primary_pair": _text(case.get("asset_case_primary_pair") or case.get("primary_pair")),
            "supporting_pairs_json": _json(case.get("asset_case_supporting_pairs") or case.get("supporting_pairs") or []),
            "evidence_summary": _text(case.get("asset_case_evidence_summary") or case.get("evidence_summary")),
            "started_at": _real(case.get("asset_case_started_at") or case.get("started_at")),
            "updated_at": _real(case.get("asset_case_updated_at") or case.get("updated_at")),
            "closed_at": _real(case.get("closed_at")),
            "last_signal_at": _real(case.get("last_signal_at") or case.get("asset_case_last_signal_at")),
            "last_stage_transition_at": _real(case.get("last_stage_transition_at") or case.get("asset_case_last_stage_transition_at")),
            "first_seen_stage": _text(case.get("first_seen_stage") or ("prealert" if (case.get("had_prealert") or case.get("asset_case_had_prealert")) else "")),
            "first_seen_at": _real(case.get("first_seen_at") or case.get("first_prealert_at") or case.get("asset_case_first_prealert_at")),
            "prealert_to_confirm_sec": _real(case.get("prealert_to_confirm_sec") or case.get("asset_case_prealert_to_confirm_sec")),
            "aggregate_metrics_json": _json(case.get("aggregate_metrics") or case.get("asset_case_aggregate_metrics") or {}),
            "quality_snapshot_json": _json(case.get("quality_snapshot") or case.get("asset_case_quality_snapshot") or {}),
            "market_context_snapshot_json": _json(case.get("market_context_snapshot") or case.get("asset_case_market_context_snapshot") or {}),
            "case_json": _json(case),
            "created_at": created_at,
        },
        ("asset_case_id",),
    )


def upsert_asset_market_state(record: Any) -> bool:
    if not bool(SQLITE_WRITE_ASSET_MARKET_STATES):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    current_state = str(_first(data, "asset_market_state_key", "current_state") or "").strip()
    asset = str(_first(data, "asset_symbol", "asset_case_label", "asset") or "").strip().upper()
    if not current_state and not asset:
        return False
    updated_at = _real(_first(data, "asset_market_state_updated_at", "updated_at", "timestamp", "ts")) or _now()
    signal_id = str(_first(data, "signal_id") or "").strip()
    state_id = str(_first(data, "state_id") or "").strip()
    if not state_id:
        state_id = "state_" + _hash({"asset": asset, "state": current_state, "signal_id": signal_id, "updated_at": updated_at})[:16]
    ok = _upsert(
        "asset_market_states",
        {
            "state_id": state_id,
            "asset": asset or None,
            "previous_state": _text(_first(data, "previous_asset_market_state_key", "previous_state")),
            "current_state": current_state or None,
            "state_reason": _text(_first(data, "asset_market_state_reason", "state_reason")),
            "confidence": _real(_first(data, "asset_market_state_confidence", "confidence")),
            "started_at": _real(_first(data, "asset_market_state_started_at", "started_at")),
            "updated_at": updated_at,
            "ended_at": _real(_first(data, "ended_at")),
            "state_changed": _bool_int(_first(data, "asset_market_state_changed", "state_changed")),
            "no_trade_lock_active": _bool_int(_first(data, "no_trade_lock_active")),
            "evidence_json": _json(_first(data, "asset_market_state_evidence_pack", "evidence", default={})),
            "required_confirmation": _text(_first(data, "asset_market_state_required_confirmation", "required_confirmation")),
            "invalidated_by": _text(_first(data, "asset_market_state_invalidated_by", "invalidated_by")),
            "state_json": _json(data),
            "created_at": _now(),
        },
        ("state_id",),
    )
    if ok and (_first(data, "no_trade_lock_active") is not None or current_state == "NO_TRADE_LOCK"):
        write_no_trade_lock(data)
    return ok


def write_no_trade_lock(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    asset = str(_first(data, "asset_symbol", "asset_case_label", "asset") or "").strip().upper()
    started_at = _real(_first(data, "no_trade_lock_started_at", "started_at"))
    ended_at = _real(_first(data, "no_trade_lock_until", "ended_at"))
    signal_id = str(_first(data, "signal_id") or "").strip()
    if not asset and not started_at:
        return False
    lock_id = str(_first(data, "lock_id") or "").strip()
    if not lock_id:
        lock_id = "lock_" + _hash({"asset": asset, "started_at": started_at, "signal_id": signal_id})[:16]
    now = _now()
    return _upsert(
        "no_trade_locks",
        {
            "lock_id": lock_id,
            "asset": asset or None,
            "started_at": started_at,
            "ended_at": ended_at,
            "ttl_sec": _real(_first(data, "no_trade_lock_ttl_sec", "asset_market_state_ttl_sec")),
            "reason": _text(_first(data, "no_trade_lock_reason")),
            "conflict_score": _real(_first(data, "no_trade_lock_conflict_score")),
            "conflicting_signals_json": _json(_first(data, "no_trade_lock_conflicting_signals", default=[])),
            "suppressed_count": _int(_first(data, "suppressed_signal_count_in_state")),
            "released_by": _text(_first(data, "no_trade_lock_released_by")),
            "release_reason": _text(_first(data, "no_trade_lock_release_condition")),
            "created_at": now,
            "updated_at": now,
        },
        ("lock_id",),
    )


def upsert_trade_opportunity(record: Any) -> bool:
    if not bool(SQLITE_WRITE_TRADE_OPPORTUNITIES):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    opportunity_id = str(_first(data, "trade_opportunity_id") or "").strip()
    status = str(_first(data, "trade_opportunity_status") or "").strip()
    if not opportunity_id or not status:
        return False
    now = _now()
    created_at = _real(_first(data, "trade_opportunity_created_at", "created_at", "timestamp", "ts")) or now
    ok = _upsert(
        "trade_opportunities",
        {
            "trade_opportunity_id": opportunity_id,
            "signal_id": _text(_first(data, "signal_id")),
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "pair": _text(_first(data, "pair_label", "pair")),
            "side": _text(_first(data, "trade_opportunity_side", "side")),
            "status": status,
            "score": _real(_first(data, "trade_opportunity_score", "score")),
            "confidence": _text(_first(data, "trade_opportunity_confidence", "confidence")),
            "label": _text(_first(data, "trade_opportunity_label", "label")),
            "reason": _text(_first(data, "trade_opportunity_reason", "reason")),
            "primary_blocker": _text(_first(data, "trade_opportunity_primary_blocker", "primary_blocker")),
            "blockers_json": _json(_first(data, "trade_opportunity_blockers", "blockers", default=[])),
            "evidence_json": _json(_first(data, "trade_opportunity_evidence", "evidence", default=[])),
            "score_components_json": _json(_first(data, "trade_opportunity_score_components", "score_components", default={})),
            "quality_snapshot_json": _json(_first(data, "trade_opportunity_quality_snapshot", "quality_snapshot", default={})),
            "history_snapshot_json": _json(_first(data, "trade_opportunity_history_snapshot", "history_snapshot", default={})),
            "required_confirmation": _text(_first(data, "trade_opportunity_required_confirmation", "required_confirmation")),
            "invalidated_by": _text(_first(data, "trade_opportunity_invalidated_by", "invalidated_by")),
            "created_at": created_at,
            "expires_at": _real(_first(data, "trade_opportunity_expires_at", "expires_at")),
            "telegram_sent": _bool_int(_first(data, "trade_opportunity_delivered_notification", "telegram_should_send", "sent_to_telegram")),
            "opportunity_json": _json(data),
            "updated_at": now,
        },
        ("trade_opportunity_id",),
    )
    if ok:
        for window_sec in (30, 60, 300):
            _write_opportunity_outcome_from_record(data, opportunity_id, window_sec, created_at)
    return ok


def _window_item(record: dict[str, Any], window_sec: int) -> dict[str, Any]:
    windows = record.get("outcome_windows") if isinstance(record.get("outcome_windows"), dict) else {}
    item = windows.get(f"{int(window_sec)}s") if isinstance(windows, dict) else {}
    return dict(item or {}) if isinstance(item, dict) else {}


def write_outcome(record: Any) -> bool:
    if not bool(SQLITE_WRITE_OUTCOMES):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    if isinstance(data.get("lp_outcome_record"), dict):
        data = dict(data.get("lp_outcome_record") or {})
    signal_id = str(_first(data, "signal_id", "record_id", "outcome_tracking_key") or "").strip()
    if not signal_id:
        return False
    wrote = False
    for window_sec in (30, 60, 300):
        window = _window_item(data, window_sec)
        if not window:
            continue
        wrote = _write_outcome_window(data, window, window_sec, signal_id=signal_id) or wrote
    return wrote


def _write_outcome_window(data: dict[str, Any], window: dict[str, Any], window_sec: int, *, signal_id: str) -> bool:
    now = _now()
    created_at = _real(_first(data, "created_at", "timestamp", "ts")) or now
    trade_opportunity_id = _text(_first(data, "trade_opportunity_id"))
    outcome_id = str(_first(window, "outcome_id") or "").strip()
    if not outcome_id:
        outcome_id = f"outcome_{signal_id}_{int(window_sec)}"
    direction_adjusted_move = _real(
        window.get("direction_adjusted_move_after")
        if window.get("direction_adjusted_move_after") is not None
        else data.get(f"direction_adjusted_move_after_{int(window_sec)}s")
    )
    raw_move = _real(
        window.get("raw_move_after")
        if window.get("raw_move_after") is not None
        else data.get(f"raw_move_after_{int(window_sec)}s")
    )
    return _upsert(
        "outcomes",
        {
            "outcome_id": outcome_id,
            "signal_id": signal_id,
            "trade_opportunity_id": trade_opportunity_id,
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "pair": _text(_first(data, "pair_label", "pair")),
            "direction": _direction_from_record(data),
            "window_sec": int(window_sec),
            "price_source": _text(window.get("price_source") or data.get("outcome_price_source")),
            "start_price": _real(window.get("price_start") if window.get("price_start") is not None else data.get("outcome_price_start")),
            "end_price": _real(window.get("price_end") if window.get("price_end") is not None else data.get("outcome_price_end")),
            "raw_move": raw_move,
            "direction_adjusted_move": direction_adjusted_move,
            "followthrough": _bool_int(window.get("followthrough_positive")),
            "adverse": _bool_int(window.get("adverse_by_direction") if window.get("adverse_by_direction") is not None else data.get(f"adverse_by_direction_{int(window_sec)}s")),
            "reversal": _bool_int(window.get("reversal") or data.get("reversal_after_climax")),
            "status": _text(window.get("status") or data.get("outcome_window_status")),
            "failure_reason": _text(window.get("failure_reason") or data.get("outcome_failure_reason")),
            "completed_at": _real(window.get("completed_at")),
            "created_at": created_at,
            "updated_at": now,
        },
        ("signal_id", "window_sec"),
    )


def _write_opportunity_outcome_from_record(record: dict[str, Any], opportunity_id: str, window_sec: int, created_at: float) -> bool:
    key = f"{int(window_sec)}s"
    status = record.get(f"opportunity_outcome_{key}") or None
    if not status:
        return False
    followthrough = record.get(f"opportunity_followthrough_{key}")
    adverse = record.get(f"opportunity_adverse_{key}")
    result_label = str(record.get("opportunity_result_label") or "")
    now = _now()
    return _upsert(
        "opportunity_outcomes",
        {
            "trade_opportunity_id": opportunity_id,
            "window_sec": int(window_sec),
            "raw_move": _real(record.get(f"raw_move_after_{key}")),
            "direction_adjusted_move": _real(record.get(f"direction_adjusted_move_after_{key}")),
            "followthrough": _bool_int(followthrough),
            "adverse": _bool_int(adverse),
            "result_label": result_label,
            "price_source": _text(record.get("opportunity_outcome_source") or record.get("outcome_price_source")),
            "status": _text(status),
            "failure_reason": _text(record.get("opportunity_invalidated_reason") or record.get("outcome_failure_reason")),
            "completed_at": _real(record.get("opportunity_invalidated_at")),
            "created_at": created_at or now,
        },
        ("trade_opportunity_id", "window_sec"),
    )


def upsert_quality_stat(record: Any) -> bool:
    if not bool(SQLITE_WRITE_QUALITY_STATS):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    scope_type = str(data.get("scope_type") or data.get("dimension") or "signal").strip()
    scope_key = str(data.get("scope_key") or data.get("key") or data.get("signal_id") or data.get("record_id") or "").strip()
    stage = str(data.get("stage") or data.get("lp_alert_stage") or "").strip()
    if not scope_key:
        scope_key = _hash(data, "quality_")
    return _upsert(
        "quality_stats",
        {
            "scope_type": scope_type,
            "scope_key": scope_key,
            "asset": _text(data.get("asset") or data.get("asset_symbol")),
            "pair": _text(data.get("pair") or data.get("pair_label")),
            "pool_address": _text(data.get("pool_address")),
            "stage": stage,
            "sample_count": _int(data.get("sample_count") or data.get("sample_size")),
            "prealert_precision_score": _real(data.get("prealert_precision_score")),
            "confirm_conversion_score": _real(data.get("confirm_conversion_score")),
            "climax_reversal_score": _real(data.get("climax_reversal_score")),
            "candidate_followthrough_rate": _real(data.get("candidate_followthrough_rate") or data.get("opportunity_candidate_followthrough_60s_rate")),
            "candidate_adverse_rate": _real(data.get("candidate_adverse_rate") or data.get("opportunity_candidate_adverse_60s_rate")),
            "verified_followthrough_rate": _real(data.get("verified_followthrough_rate") or data.get("opportunity_verified_followthrough_60s_rate")),
            "verified_adverse_rate": _real(data.get("verified_adverse_rate") or data.get("opportunity_verified_adverse_60s_rate")),
            "outcome_completion_rate": _real(data.get("outcome_completion_rate") or data.get("completion_rate")),
            "fastlane_roi_score": _real(data.get("fastlane_roi_score")),
            "stats_json": _json(data),
            "updated_at": _real(data.get("updated_at") or data.get("generated_at")) or _now(),
        },
        ("scope_type", "scope_key", "stage"),
    )


def write_market_context_snapshot(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    signal_id = str(_first(data, "signal_id") or "").strip()
    source = str(_first(data, "market_context_source") or "").strip()
    if not signal_id and not source:
        return False
    created_at = _real(_first(data, "market_context_sampled_at", "timestamp", "ts", "archive_written_at")) or _now()
    context_id = str(_first(data, "context_id") or "").strip()
    if not context_id:
        context_id = "ctx_" + _hash({"signal_id": signal_id, "created_at": created_at, "source": source})[:16]
    return _upsert(
        "market_context_snapshots",
        {
            "context_id": context_id,
            "signal_id": signal_id or None,
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "pair": _text(_first(data, "pair_label", "pair")),
            "venue": _text(_first(data, "market_context_venue", "venue")),
            "resolved_symbol": _text(_first(data, "market_context_resolved_symbol", "resolved_symbol")),
            "source": source or None,
            "perp_last_price": _real(_first(data, "perp_last_price")),
            "perp_mark_price": _real(_first(data, "perp_mark_price")),
            "perp_index_price": _real(_first(data, "perp_index_price")),
            "spot_reference_price": _real(_first(data, "spot_reference_price")),
            "basis_bps": _real(_first(data, "basis_bps")),
            "mark_index_spread_bps": _real(_first(data, "mark_index_spread_bps")),
            "funding_rate": _real(_first(data, "funding_rate", "funding_estimate")),
            "alert_relative_timing": _text(_first(data, "alert_relative_timing")),
            "market_move_before_30s": _real(_first(data, "market_move_before_alert_30s")),
            "market_move_before_60s": _real(_first(data, "market_move_before_alert_60s")),
            "market_move_after_60s": _real(_first(data, "market_move_after_alert_60s")),
            "market_move_after_300s": _real(_first(data, "market_move_after_alert_300s")),
            "failure_reason": _text(_first(data, "market_context_failure_reason", "failure_reason")),
            "created_at": created_at,
            "updated_at": _now(),
        },
        ("context_id",),
    )


def write_market_context_attempt(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    created_at = _real(data.get("created_at") or data.get("ts")) or _now()
    signal_id = str(data.get("signal_id") or "").strip()
    venue = str(data.get("venue") or "").strip()
    endpoint = str(data.get("endpoint") or "").strip()
    requested_symbol = str(data.get("requested_symbol") or data.get("symbol") or "").strip()
    resolved_symbol = str(data.get("resolved_symbol") or data.get("symbol") or "").strip()
    attempt_id = str(data.get("attempt_id") or "").strip()
    if not attempt_id:
        attempt_id = "mca_" + _hash({"signal_id": signal_id, "venue": venue, "endpoint": endpoint, "symbol": requested_symbol, "created_at": created_at})[:16]
    status = str(data.get("status") or "").strip().lower()
    return _upsert(
        "market_context_attempts",
        {
            "attempt_id": attempt_id,
            "signal_id": signal_id or None,
            "venue": venue or None,
            "endpoint": endpoint or None,
            "requested_symbol": requested_symbol or None,
            "resolved_symbol": resolved_symbol or None,
            "http_status": _text(data.get("http_status")),
            "success": 1 if status == "success" or bool(data.get("success")) else 0,
            "failure_reason": _text(data.get("failure_reason")),
            "latency_ms": _real(data.get("latency_ms")),
            "created_at": created_at,
        },
        ("attempt_id",),
    )


def write_telegram_delivery(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    signal_id = str(_first(data, "signal_id") or "").strip()
    sent = _bool_int(_first(data, "sent_to_telegram", "delivered_notification", "telegram_should_send"))
    suppressed_reason = str(_first(data, "telegram_suppression_reason", "suppression_reason") or "").strip()
    if not signal_id and sent is None and not suppressed_reason:
        return False
    sent_at = _real(_first(data, "notifier_sent_at", "sent_at"))
    created_at = _real(_first(data, "archive_written_at", "timestamp", "ts")) or _now()
    delivery_id = str(_first(data, "telegram_delivery_id") or "").strip()
    if not delivery_id:
        delivery_id = "tg_" + _hash({"signal_id": signal_id, "sent_at": sent_at, "suppression": suppressed_reason, "created_at": created_at})[:16]
    return _upsert(
        "telegram_deliveries",
        {
            "telegram_delivery_id": delivery_id,
            "signal_id": signal_id or None,
            "trade_opportunity_id": _text(_first(data, "trade_opportunity_id")),
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "template": _text(_first(data, "notifier_template", "message_template", "template")),
            "headline": _text(_first(data, "headline", "headline_label", "market_state_label")),
            "sent": sent if sent is not None else 0,
            "sent_at": sent_at,
            "suppressed": 1 if suppressed_reason else 0 if sent else None,
            "suppression_reason": suppressed_reason or None,
            "telegram_update_kind": _text(_first(data, "telegram_update_kind")),
            "chat_id_hash": _text(_first(data, "chat_id_hash")),
            "message_json": _json(data),
            "created_at": created_at,
        },
        ("telegram_delivery_id",),
    )


def write_prealert_lifecycle(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    state = str(_first(data, "prealert_lifecycle_state") or "").strip()
    candidate = _bool_int(_first(data, "lp_prealert_candidate"))
    gate_passed = _bool_int(_first(data, "lp_prealert_gate_passed"))
    if not state and candidate is None and gate_passed is None:
        return False
    signal_id = str(_first(data, "signal_id") or "").strip()
    asset_case_id = str(_first(data, "asset_case_id") or "").strip()
    created_at = _real(_first(data, "first_seen_at", "timestamp", "ts")) or _now()
    prealert_id = str(_first(data, "prealert_id") or "").strip()
    if not prealert_id:
        prealert_id = "prealert_" + _hash({"signal_id": signal_id, "asset_case_id": asset_case_id, "created_at": created_at})[:16]
    return _upsert(
        "prealert_lifecycle",
        {
            "prealert_id": prealert_id,
            "signal_id": signal_id or None,
            "asset_case_id": asset_case_id or None,
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "pair": _text(_first(data, "pair_label", "pair")),
            "candidate": candidate,
            "gate_passed": gate_passed,
            "active": 1 if state == "active" else 0,
            "delivered": 1 if state == "delivered" or bool(_first(data, "sent_to_telegram")) else 0,
            "merged": 1 if state == "merged" else 0,
            "upgraded_to_confirm": 1 if state == "upgraded_to_confirm" else 0,
            "expired": 1 if state == "expired" else 0,
            "suppressed_by_lock": 1 if state == "suppressed_by_lock" else 0,
            "first_seen_at": _real(_first(data, "first_seen_at")),
            "prealert_to_confirm_sec": _real(_first(data, "prealert_to_confirm_sec")),
            "fail_reason": _text(_first(data, "lp_prealert_gate_fail_reason", "lp_prealert_delivery_block_reason")),
            "lifecycle_json": _json(data),
            "created_at": created_at,
            "updated_at": _now(),
        },
        ("prealert_id",),
    )


def table_row_counts(tables: tuple[str, ...] = REQUIRED_TABLES) -> dict[str, int]:
    conn = get_connection()
    counts: dict[str, int] = {}
    if conn is None:
        return counts
    for table in tables:
        try:
            counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        except sqlite3.Error:
            counts[table] = -1
    return counts


def archive_row_counts(base_dir: str | Path | None = None) -> dict[str, int]:
    root = Path(base_dir or ARCHIVE_BASE_DIR)
    counts: dict[str, int] = {}
    for category in ARCHIVE_CATEGORY_TABLES:
        total = 0
        for path in sorted((root / category).glob("*.ndjson")):
            try:
                with path.open(encoding="utf-8") as handle:
                    total += sum(1 for line in handle if line.strip())
            except OSError:
                continue
        counts[category] = total
    return counts


def mirror_match_rate() -> dict[str, Any]:
    db_counts = table_row_counts(tuple(ARCHIVE_CATEGORY_TABLES.values()))
    archive_counts = archive_row_counts()
    per_category: dict[str, Any] = {}
    ratios = []
    for category, table in ARCHIVE_CATEGORY_TABLES.items():
        archive_count = int(archive_counts.get(category) or 0)
        db_count = max(int(db_counts.get(table) or 0), 0)
        match = 1.0 if archive_count == 0 and db_count == 0 else min(db_count, archive_count) / max(db_count, archive_count, 1)
        ratios.append(match)
        per_category[category] = {
            "archive_rows": archive_count,
            "sqlite_rows": db_count,
            "match_rate": round(match, 4),
            "mismatch": archive_count != db_count,
        }
    return {
        "db_archive_mirror_match_rate": round(sum(ratios) / len(ratios), 4) if ratios else 1.0,
        "per_category": per_category,
    }


def health_summary() -> dict[str, Any]:
    conn = get_connection()
    missing_tables: list[str] = []
    existing: set[str] = set()
    if conn is not None:
        try:
            existing = {
                str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        except sqlite3.Error:
            existing = set()
    for table in REQUIRED_TABLES:
        if table not in existing:
            missing_tables.append(table)
    counts = table_row_counts()
    last_updated: dict[str, float | None] = {}
    for table in REQUIRED_TABLES:
        if counts.get(table, -1) < 0:
            last_updated[table] = None
            continue
        column = "updated_at"
        if table in {"market_context_attempts", "telegram_deliveries", "opportunity_outcomes"}:
            column = "created_at"
        try:
            row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone() if conn is not None else None
            last_updated[table] = _real(row[0]) if row else None
        except sqlite3.Error:
            last_updated[table] = None
    return {
        "enabled": bool(SQLITE_ENABLE),
        "db_path": str(_DB_PATH or _resolve_db_path()),
        "initialized": conn is not None and not missing_tables,
        "init_failed_reason": _INIT_FAILED_REASON,
        "schema_version": SQLITE_SCHEMA_VERSION,
        "table_row_counts": counts,
        "last_updated": last_updated,
        "missing_tables": missing_tables,
        "write_error_count": int(_WRITE_ERROR_COUNT),
        "warnings": list(_WARNINGS[-10:]),
        "archive_mirror": mirror_match_rate(),
    }


def opportunity_db_summary() -> dict[str, Any]:
    conn = get_connection()
    if conn is None:
        return {"available": False, "reason": _INIT_FAILED_REASON or "not_initialized"}
    status_counts = {
        str(row["status"] or "NONE"): int(row["count"])
        for row in conn.execute(
            "SELECT status, COUNT(*) AS count FROM trade_opportunities GROUP BY status"
        ).fetchall()
    }
    blocker_counts = {
        str(row["primary_blocker"] or ""): int(row["count"])
        for row in conn.execute(
            "SELECT primary_blocker, COUNT(*) AS count FROM trade_opportunities WHERE primary_blocker IS NOT NULL AND primary_blocker != '' GROUP BY primary_blocker ORDER BY count DESC"
        ).fetchall()
    }
    total_outcomes = int(conn.execute("SELECT COUNT(*) FROM opportunity_outcomes").fetchone()[0])
    completed_outcomes = int(conn.execute("SELECT COUNT(*) FROM opportunity_outcomes WHERE status='completed'").fetchone()[0])
    attempts_total = int(conn.execute("SELECT COUNT(*) FROM market_context_attempts").fetchone()[0])
    attempts_success = int(conn.execute("SELECT COUNT(*) FROM market_context_attempts WHERE success=1").fetchone()[0])
    return {
        "available": True,
        "status_counts": status_counts,
        "candidate_count": int(status_counts.get("CANDIDATE", 0)),
        "verified_count": int(status_counts.get("VERIFIED", 0)),
        "blocked_count": int(status_counts.get("BLOCKED", 0)),
        "none_count": int(status_counts.get("NONE", 0)),
        "blocker_counts": blocker_counts,
        "outcome_completion_rate": round(completed_outcomes / total_outcomes, 4) if total_outcomes else 0.0,
        "market_context_attempt_success_rate": round(attempts_success / attempts_total, 4) if attempts_total else 0.0,
    }


def integrity_check() -> dict[str, Any]:
    conn = get_connection()
    payload = health_summary()
    if conn is None:
        payload["ok"] = False
        return payload
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    except sqlite3.Error as exc:
        result = str(exc)
    payload["pragma_integrity_check"] = result
    payload["ok"] = result == "ok" and not payload.get("missing_tables")
    payload["db_archive_mismatch"] = [
        category
        for category, item in (payload.get("archive_mirror", {}).get("per_category") or {}).items()
        if item.get("mismatch")
    ]
    payload["opportunity_summary"] = opportunity_db_summary()
    return payload


def _iter_archive_payloads(category: str, *, date: str | None = None, all_dates: bool = False) -> tuple[int, int]:
    root = Path(ARCHIVE_BASE_DIR) / category
    if all_dates:
        paths = sorted(root.glob("*.ndjson"))
    elif date:
        paths = [root / f"{date}.ndjson"]
    else:
        paths = []
    imported = 0
    bad_rows = 0
    for path in paths:
        if not path.exists():
            continue
        try:
            with path.open(encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        bad_rows += 1
                        continue
                    if mirror_archive_record(category, payload):
                        imported += 1
        except OSError:
            bad_rows += 1
            continue
    return imported, bad_rows


def mirror_archive_record(category: str, payload: dict[str, Any]) -> bool:
    if not bool(SQLITE_ARCHIVE_MIRROR_ENABLE):
        return False
    if category == "raw_events":
        return write_raw_event(payload)
    if category == "parsed_events":
        return write_parsed_event(payload)
    if category == "signals":
        return write_signal(payload)
    if category == "delivery_audit":
        return write_delivery_audit(payload)
    if category == "case_followups":
        return write_case_followup(payload)
    return False


def migrate_archive(*, date: str | None = None, all_dates: bool = False) -> dict[str, Any]:
    global _BATCH_MODE
    migrate_schema()
    result: dict[str, Any] = {
        "date": date,
        "all": bool(all_dates),
        "categories": {},
        "imported_rows": 0,
        "bad_row_count": 0,
    }
    conn = get_connection()
    previous_batch_mode = _BATCH_MODE
    try:
        _BATCH_MODE = True
        if conn is not None:
            conn.execute("BEGIN")
        for category in ARCHIVE_CATEGORY_TABLES:
            imported, bad_rows = _iter_archive_payloads(category, date=date, all_dates=all_dates)
            result["categories"][category] = {
                "imported_rows": imported,
                "bad_row_count": bad_rows,
            }
            result["imported_rows"] += imported
            result["bad_row_count"] += bad_rows
        cache_payload = migrate_cache_snapshots()
        result["categories"]["cache_snapshots"] = cache_payload
        result["imported_rows"] += int(cache_payload.get("imported_rows") or 0)
        result["bad_row_count"] += int(cache_payload.get("bad_row_count") or 0)
        if conn is not None:
            conn.commit()
            if bool(SQLITE_WAL_MODE):
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        _BATCH_MODE = previous_batch_mode
    return result


def migrate_cache_snapshots() -> dict[str, Any]:
    result = {
        "imported_rows": 0,
        "bad_row_count": 0,
        "files": {},
    }

    def _load(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            result["files"][path.name] = {"exists": False, "imported_rows": 0, "bad_row_count": 0}
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result["files"][path.name] = {"exists": True, "imported_rows": 0, "bad_row_count": 1}
            result["bad_row_count"] += 1
            return None
        return payload if isinstance(payload, dict) else None

    asset_cases_path = PROJECT_ROOT / "data" / "asset_cases.cache.json"
    payload = _load(asset_cases_path)
    imported = 0
    if payload is not None:
        for row in payload.get("cases") or []:
            if isinstance(row, dict) and upsert_asset_case(row):
                imported += 1
            elif not isinstance(row, dict):
                result["bad_row_count"] += 1
        result["files"][asset_cases_path.name] = {"exists": True, "imported_rows": imported, "bad_row_count": 0}
        result["imported_rows"] += imported

    asset_states_path = PROJECT_ROOT / "data" / "asset_market_states.cache.json"
    payload = _load(asset_states_path)
    imported = 0
    if payload is not None:
        for row in payload.get("records") or []:
            if isinstance(row, dict) and upsert_asset_market_state(row):
                imported += 1
            elif not isinstance(row, dict):
                result["bad_row_count"] += 1
        result["files"][asset_states_path.name] = {"exists": True, "imported_rows": imported, "bad_row_count": 0}
        result["imported_rows"] += imported

    opportunities_path = PROJECT_ROOT / "data" / "trade_opportunities.cache.json"
    payload = _load(opportunities_path)
    imported = 0
    if payload is not None:
        for row in payload.get("opportunities") or []:
            if isinstance(row, dict) and upsert_trade_opportunity(row):
                imported += 1
            elif not isinstance(row, dict):
                result["bad_row_count"] += 1
        rolling = payload.get("rolling_stats")
        if isinstance(rolling, dict):
            stat = dict(rolling)
            stat.update({"scope_type": "opportunity", "scope_key": "rolling", "stage": "all", "updated_at": payload.get("generated_at")})
            if upsert_quality_stat(stat):
                imported += 1
        result["files"][opportunities_path.name] = {"exists": True, "imported_rows": imported, "bad_row_count": 0}
        result["imported_rows"] += imported

    quality_path = PROJECT_ROOT / "data" / "lp_quality_stats.cache.json"
    payload = _load(quality_path)
    imported = 0
    if payload is not None:
        generated_at = payload.get("generated_at")
        for row in payload.get("records") or []:
            if isinstance(row, dict):
                wrote = bool(write_outcome(row))
                stat = dict(row)
                stat.update(
                    {
                        "scope_type": "signal",
                        "scope_key": str(row.get("signal_id") or row.get("record_id") or ""),
                        "stage": str(row.get("lp_alert_stage") or ""),
                        "updated_at": generated_at,
                    }
                )
                wrote = bool(upsert_quality_stat(stat)) or wrote
                imported += 1 if wrote else 0
            else:
                result["bad_row_count"] += 1
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        for dimension, rows in (summary.get("dimensions") or {}).items():
            for row in rows or []:
                if isinstance(row, dict):
                    stat = dict(row)
                    stat.update(
                        {
                            "scope_type": str(dimension),
                            "scope_key": str(row.get("key") or ""),
                            "stage": str(row.get("stage") or "all"),
                            "updated_at": generated_at,
                        }
                    )
                    if upsert_quality_stat(stat):
                        imported += 1
                else:
                    result["bad_row_count"] += 1
        result["files"][quality_path.name] = {"exists": True, "imported_rows": imported, "bad_row_count": 0}
        result["imported_rows"] += imported

    return result


def prune(*, dry_run: bool = True) -> dict[str, Any]:
    conn = get_connection()
    if conn is None:
        return {"ok": False, "reason": _INIT_FAILED_REASON or "not_initialized"}
    now_ts = _now()
    specs = [
        ("raw_events", "captured_at", SQLITE_RETENTION_RAW_DAYS),
        ("parsed_events", "parsed_at", SQLITE_RETENTION_PARSED_DAYS),
        ("signals", "archive_written_at", SQLITE_RETENTION_SIGNAL_DAYS),
        ("outcomes", "created_at", SQLITE_RETENTION_OUTCOME_DAYS),
        ("trade_opportunities", "created_at", SQLITE_RETENTION_OPPORTUNITY_DAYS),
    ]
    result: dict[str, Any] = {"dry_run": bool(dry_run), "tables": {}}
    for table, column, days in specs:
        cutoff = now_ts - max(int(days), 1) * 86400
        count = int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} < ?", (cutoff,)).fetchone()[0])
        result["tables"][table] = {"cutoff": cutoff, "candidate_rows": count}
        if not dry_run and count:
            conn.execute(f"DELETE FROM {table} WHERE {column} < ?", (cutoff,))
    if not dry_run:
        conn.commit()
    return result


def rows_by_table() -> dict[str, int]:
    return table_row_counts()


def load_signal_rows_from_db(start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    if conn is None:
        return []
    clauses = []
    params: list[Any] = []
    if start_ts is not None:
        clauses.append("archive_written_at >= ?")
        params.append(int(start_ts))
    if end_ts is not None:
        clauses.append("archive_written_at <= ?")
        params.append(int(end_ts))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    try:
        rows = conn.execute(
            f"SELECT signal_json, archive_written_at FROM signals {where} ORDER BY archive_written_at",
            tuple(params),
        ).fetchall()
    except sqlite3.Error:
        return []
    result = []
    for row in rows:
        data = _from_json(row["signal_json"], {})
        if isinstance(data, dict):
            data.setdefault("archive_written_at", row["archive_written_at"])
            data.setdefault("archive_ts", row["archive_written_at"])
            result.append(data)
    return result


def report_source_summary() -> dict[str, Any]:
    payload = health_summary()
    sqlite_rows = payload.get("table_row_counts") or {}
    archive_rows = archive_row_counts()
    has_sqlite_rows = any(int(value or 0) > 0 for value in sqlite_rows.values() if isinstance(value, int))
    has_archive_rows = any(int(value or 0) > 0 for value in archive_rows.values())
    data_source = "mixed" if has_sqlite_rows and has_archive_rows else "sqlite" if has_sqlite_rows else "archive"
    return {
        "data_source": data_source,
        "db_path": payload.get("db_path"),
        "sqlite_rows_by_table": sqlite_rows,
        "archive_rows_by_category": archive_rows,
        "db_archive_mirror_match_rate": (payload.get("archive_mirror") or {}).get("db_archive_mirror_match_rate"),
        "db_archive_mirror_detail": (payload.get("archive_mirror") or {}).get("per_category") or {},
        "warnings": payload.get("warnings") or [],
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="chain-monitor SQLite mirror store")
    parser.add_argument("--init", action="store_true", help="initialize SQLite schema")
    parser.add_argument("--summary", action="store_true", help="print table row counts and health")
    parser.add_argument("--migrate-archive", action="store_true", help="mirror archive NDJSON into SQLite")
    parser.add_argument("--date", help="archive date YYYY-MM-DD for --migrate-archive")
    parser.add_argument("--all", action="store_true", help="migrate all archive dates")
    parser.add_argument("--integrity-check", action="store_true", help="run schema/count/integrity checks")
    parser.add_argument("--prune", action="store_true", help="show or execute retention prune")
    parser.add_argument("--dry-run", action="store_true", help="dry-run retention prune")
    parser.add_argument("--execute", action="store_true", help="execute retention prune")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.init:
        conn = init_sqlite_store()
        _print_json({"ok": conn is not None, "summary": health_summary()})
        return 0 if conn is not None else 1
    if args.summary:
        init_sqlite_store()
        _print_json(health_summary())
        return 0
    if args.integrity_check:
        init_sqlite_store()
        payload = integrity_check()
        _print_json(payload)
        return 0 if payload.get("ok") else 1
    if args.migrate_archive:
        if not args.all and not args.date:
            parser.error("--migrate-archive requires --date YYYY-MM-DD or --all")
        init_sqlite_store()
        _print_json(migrate_archive(date=args.date, all_dates=bool(args.all)))
        return 0
    if args.prune:
        init_sqlite_store()
        dry_run = True
        if args.execute:
            dry_run = False
        elif args.dry_run:
            dry_run = True
        _print_json(prune(dry_run=dry_run))
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
