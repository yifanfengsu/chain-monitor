from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from typing import Any

from config import (
    ARCHIVE_BASE_DIR,
    MARKET_CONTEXT_PRIMARY_VENUE,
    MARKET_CONTEXT_SECONDARY_VENUE,
    PROJECT_ROOT,
    REPORT_ARCHIVE_READ_GZIP,
    OPPORTUNITY_MAX_60S_ADVERSE_RATE,
    OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE,
    OPPORTUNITY_MIN_HISTORY_SAMPLES,
    OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE,
    SQLITE_ARCHIVE_PATH_REFERENCE_ENABLE,
    SQLITE_ARCHIVE_PAYLOAD_HASH_ENABLE,
    SQLITE_ARCHIVE_MIRROR_ENABLE,
    SQLITE_BUSY_TIMEOUT_MS,
    SQLITE_CASE_FOLLOWUP_MODE,
    SQLITE_COMPACT_DRY_RUN_DEFAULT,
    SQLITE_COMPACT_ENABLE,
    SQLITE_DB_PATH,
    SQLITE_DELIVERY_AUDIT_MODE,
    SQLITE_ENABLE,
    SQLITE_FULL_JSON_RETENTION_DAYS,
    SQLITE_MARKET_CONTEXT_ATTEMPT_MODE,
    SQLITE_OPPORTUNITY_MODE,
    SQLITE_OUTCOME_MODE,
    SQLITE_PARSED_EVENTS_MODE,
    SQLITE_PRAGMA_SYNCHRONOUS,
    SQLITE_QUALITY_MODE,
    SQLITE_RAW_EVENTS_MODE,
    SQLITE_RETENTION_OPPORTUNITY_DAYS,
    SQLITE_RETENTION_OUTCOME_DAYS,
    SQLITE_RETENTION_PARSED_DAYS,
    SQLITE_RETENTION_RAW_DAYS,
    SQLITE_RETENTION_SIGNAL_DAYS,
    SQLITE_SCHEMA_VERSION,
    SQLITE_SIGNAL_MODE,
    SQLITE_STATE_MODE,
    SQLITE_TELEGRAM_DELIVERY_MODE,
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

TABLE_JSON_COLUMNS = {
    "raw_events": ("raw_json",),
    "parsed_events": ("parsed_json",),
    "signals": ("signal_json",),
    "asset_cases": ("case_json",),
    "asset_market_states": ("state_json", "evidence_json"),
    "trade_opportunities": (
        "opportunity_json",
        "evidence_json",
        "score_components_json",
        "quality_snapshot_json",
        "history_snapshot_json",
        "profile_features_json",
        "blockers_json",
        "hard_blockers_json",
        "verification_blockers_json",
    ),
    "quality_stats": ("stats_json",),
    "telegram_deliveries": ("message_json",),
    "delivery_audit": ("audit_json",),
    "case_followups": ("followup_json",),
    "prealert_lifecycle": ("lifecycle_json",),
    "market_context_snapshots": (),
    "market_context_attempts": (),
    "outcomes": (),
    "opportunity_outcomes": (),
    "signal_features": (),
    "no_trade_locks": ("conflicting_signals_json",),
}

TABLE_VALUE_CLASSES = {
    "signals": "core_learning",
    "signal_features": "core_learning",
    "outcomes": "core_outcome",
    "trade_opportunities": "core_learning",
    "opportunity_outcomes": "core_outcome",
    "quality_stats": "core_learning",
    "asset_market_states": "core_state",
    "no_trade_locks": "core_state",
    "asset_cases": "core_state",
    "prealert_lifecycle": "core_state",
    "market_context_snapshots": "core_learning",
    "delivery_audit": "operational_diagnostics",
    "telegram_deliveries": "operational_diagnostics",
    "market_context_attempts": "operational_diagnostics",
    "case_followups": "operational_diagnostics",
    "raw_events": "archive_debug",
    "parsed_events": "archive_debug",
    "runs": "disposable_or_rebuildable",
    "schema_meta": "disposable_or_rebuildable",
}

TABLE_RECOMMENDED_MODES = {
    "raw_events": "index_only",
    "parsed_events": "index_only",
    "delivery_audit": "slim",
    "telegram_deliveries": "slim",
    "market_context_attempts": "slim",
    "case_followups": "slim",
    "signals": "full",
    "signal_features": "full",
    "outcomes": "full",
    "trade_opportunities": "full",
    "opportunity_outcomes": "full",
    "quality_stats": "full",
    "asset_market_states": "full",
    "no_trade_locks": "full",
    "asset_cases": "full",
    "prealert_lifecycle": "full",
    "market_context_snapshots": "full",
}

PAYLOAD_METADATA_SPECS: dict[str, dict[str, Any]] = {
    "raw_events": {
        "json_column": "raw_json",
        "time_column": "captured_at",
        "archive_categories": ("raw_events",),
        "identity_columns": ("event_id", "tx_hash", "block_number", "captured_at"),
        "time_keys": ("captured_at", "ingest_ts", "timestamp", "ts", "archive_ts"),
        "bucket_seconds": 60,
    },
    "parsed_events": {
        "json_column": "parsed_json",
        "time_column": "parsed_at",
        "archive_categories": ("parsed_events",),
        "identity_columns": ("event_id", "tx_hash", "parsed_kind", "parsed_at"),
        "time_keys": ("parsed_at", "timestamp", "ts", "archive_ts"),
        "bucket_seconds": 60,
    },
    "delivery_audit": {
        "json_column": "audit_json",
        "time_column": "archive_written_at",
        "archive_categories": ("delivery_audit",),
        "identity_columns": (
            "audit_id",
            "signal_id",
            "event_id",
            "delivery_decision",
            "sent_to_telegram",
            "timestamp",
            "archive_written_at",
        ),
        "time_keys": ("timestamp", "archive_written_at", "notifier_sent_at", "ts", "created_at", "archive_ts"),
        "bucket_seconds": 60,
    },
    "telegram_deliveries": {
        "json_column": "message_json",
        "time_column": "created_at",
        "archive_categories": ("delivery_audit", "signals"),
        "identity_columns": ("telegram_delivery_id", "signal_id", "headline", "sent_at", "created_at"),
        "time_keys": ("sent_at", "created_at", "archive_written_at", "notifier_sent_at", "timestamp", "archive_ts"),
        "bucket_seconds": 60,
    },
    "case_followups": {
        "json_column": "followup_json",
        "time_column": "archive_written_at",
        "archive_categories": ("case_followups",),
        "identity_columns": ("followup_id", "case_id", "signal_id", "asset", "stage", "archive_written_at", "created_at"),
        "time_keys": ("archive_written_at", "created_at", "timestamp", "ts", "archive_ts"),
        "bucket_seconds": 300,
    },
}

TABLE_VALUE_FLAGS = {
    "signals": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": True,
        "telegram_suppression": True,
        "no_trade_lock": True,
        "prealert_lifecycle": True,
        "quality_outcome": True,
        "market_context_health": True,
        "archive_debug_only": False,
    },
    "signal_features": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "outcomes": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": True,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "trade_opportunities": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": True,
        "telegram_suppression": True,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "opportunity_outcomes": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": True,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "quality_stats": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": True,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": True,
        "market_context_health": True,
        "archive_debug_only": False,
    },
    "asset_market_states": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": True,
        "no_trade_lock": True,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "no_trade_locks": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": True,
        "telegram_suppression": True,
        "no_trade_lock": True,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "asset_cases": {
        "candidate_to_verified": False,
        "opportunity_score": True,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": True,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "prealert_lifecycle": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": True,
        "no_trade_lock": False,
        "prealert_lifecycle": True,
        "quality_outcome": True,
        "market_context_health": False,
        "archive_debug_only": False,
    },
    "market_context_snapshots": {
        "candidate_to_verified": True,
        "opportunity_score": True,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": True,
        "archive_debug_only": False,
    },
    "delivery_audit": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": True,
        "no_trade_lock": False,
        "prealert_lifecycle": True,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": True,
    },
    "telegram_deliveries": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": True,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": True,
    },
    "market_context_attempts": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": True,
        "archive_debug_only": True,
    },
    "case_followups": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": True,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": True,
    },
    "raw_events": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": True,
    },
    "parsed_events": {
        "candidate_to_verified": False,
        "opportunity_score": False,
        "blocker_effectiveness": False,
        "telegram_suppression": False,
        "no_trade_lock": False,
        "prealert_lifecycle": False,
        "quality_outcome": False,
        "market_context_health": False,
        "archive_debug_only": True,
    },
}


def _archive_file_key(path: Path) -> str:
    name = path.name
    if name.endswith(".ndjson.gz"):
        return name[: -len(".ndjson.gz")]
    if name.endswith(".ndjson"):
        return name[: -len(".ndjson")]
    return path.stem


def _archive_payload_paths(
    category: str,
    *,
    base_dir: str | Path | None = None,
    date: str | None = None,
    all_dates: bool = False,
) -> list[Path]:
    root = Path(base_dir or ARCHIVE_BASE_DIR) / category
    if all_dates:
        plain = {_archive_file_key(path): path for path in sorted(root.glob("*.ndjson"))}
        paths = dict(plain)
        if bool(REPORT_ARCHIVE_READ_GZIP):
            for path in sorted(root.glob("*.ndjson.gz")):
                paths.setdefault(_archive_file_key(path), path)
        return [paths[key] for key in sorted(paths)]
    if date:
        plain = root / f"{date}.ndjson"
        if plain.exists():
            return [plain]
        gz = root / f"{date}.ndjson.gz"
        if bool(REPORT_ARCHIVE_READ_GZIP) and gz.exists():
            return [gz]
    return []


def _open_archive_payload_path(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def _count_archive_payload_lines(path: Path) -> int:
    if path.name.endswith(".gz"):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                return sum(1 for line in handle if line.strip())
        except OSError:
            return 0
    try:
        result = subprocess.run(
            ["wc", "-l", str(path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return int(str(result.stdout).strip().split()[0])
    except (OSError, ValueError, IndexError):
        pass
    try:
        with path.open(encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
    except OSError:
        return 0


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


def stable_payload_hash(payload: Any) -> str | None:
    try:
        if isinstance(payload, str):
            try:
                normalized = json.dumps(
                    json.loads(payload),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                normalized = payload
        else:
            normalized = json.dumps(
                payload if payload is not None else {},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    except (TypeError, ValueError):
        try:
            return hashlib.sha256(str(payload).encode("utf-8")).hexdigest()
        except Exception:
            return None


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


def _payload_hash(value: Any) -> str | None:
    if not bool(SQLITE_ARCHIVE_PAYLOAD_HASH_ENABLE):
        return None
    return stable_payload_hash(value)


def _archive_date_from_ts(ts_value: Any) -> str | None:
    ts = _real(ts_value)
    if ts is None:
        return None
    bj = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(bj).strftime("%Y-%m-%d")


def _archive_reference_path(category: str, ts_value: Any) -> str | None:
    if not bool(SQLITE_ARCHIVE_PATH_REFERENCE_ENABLE):
        return None
    archive_date = _archive_date_from_ts(ts_value)
    if not archive_date:
        return None
    root = Path(ARCHIVE_BASE_DIR)
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    return str((root / category / f"{archive_date}.ndjson").resolve())


def _payload_meta(category: str, data: dict[str, Any], envelope: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    archive_ts = _archive_ts(data, envelope)
    archive_path = _archive_reference_path(category, archive_ts)
    archive_date = _archive_date_from_ts(archive_ts)
    payload_hash = _payload_hash(data)
    return archive_path, archive_date, payload_hash


def _suppressed_flag(sent: int | None, reason: str | None) -> int | None:
    if str(reason or "").strip():
        return 1
    if sent is None:
        return None
    return 0


def _telegram_archive_category(data: dict[str, Any]) -> str:
    explicit = str(_first(data, "archive_category") or "").strip()
    if explicit in ARCHIVE_CATEGORY_TABLES:
        return explicit
    if str(_first(data, "delivery_audit_id", "audit_id") or "").strip():
        return "delivery_audit"
    return "signals"


def _row_mode(mode: str, allowed: tuple[str, ...], default: str) -> str:
    normalized = str(mode or "").strip().lower()
    return normalized if normalized in allowed else default


def _mode_for_table(table: str) -> str:
    mapping = {
        "raw_events": _row_mode(SQLITE_RAW_EVENTS_MODE, ("index_only", "full", "off"), "index_only"),
        "parsed_events": _row_mode(SQLITE_PARSED_EVENTS_MODE, ("index_only", "full", "off"), "index_only"),
        "delivery_audit": _row_mode(SQLITE_DELIVERY_AUDIT_MODE, ("slim", "full", "off"), "slim"),
        "telegram_deliveries": _row_mode(SQLITE_TELEGRAM_DELIVERY_MODE, ("slim", "full", "off"), "slim"),
        "market_context_attempts": _row_mode(SQLITE_MARKET_CONTEXT_ATTEMPT_MODE, ("slim", "full", "aggregate", "off"), "slim"),
        "case_followups": _row_mode(SQLITE_CASE_FOLLOWUP_MODE, ("slim", "full", "off"), "slim"),
        "signals": _row_mode(SQLITE_SIGNAL_MODE, ("full",), "full"),
        "trade_opportunities": _row_mode(SQLITE_OPPORTUNITY_MODE, ("full",), "full"),
        "outcomes": _row_mode(SQLITE_OUTCOME_MODE, ("full",), "full"),
        "asset_market_states": _row_mode(SQLITE_STATE_MODE, ("full",), "full"),
        "quality_stats": _row_mode(SQLITE_QUALITY_MODE, ("full",), "full"),
    }
    return mapping.get(table, TABLE_RECOMMENDED_MODES.get(table, "full"))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


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
                payload_mode TEXT,
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
                payload_mode TEXT,
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
                payload_hash TEXT,
                payload_mode TEXT,
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
                due_at REAL,
                price_source TEXT,
                start_price_source TEXT,
                end_price_source TEXT,
                outcome_price_source TEXT,
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
                settled_by TEXT,
                catchup INTEGER,
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
                opportunity_profile_key TEXT,
                opportunity_profile_version TEXT,
                opportunity_profile_side TEXT,
                opportunity_profile_asset TEXT,
                opportunity_profile_pair_family TEXT,
                opportunity_profile_strategy TEXT,
                side TEXT,
                status TEXT,
                raw_score REAL,
                score REAL,
                calibrated_score REAL,
                calibration_adjustment REAL,
                calibration_reason TEXT,
                calibration_sample_count INTEGER,
                calibration_confidence REAL,
                calibration_source TEXT,
                confidence TEXT,
                label TEXT,
                reason TEXT,
                primary_blocker TEXT,
                primary_hard_blocker TEXT,
                primary_verification_blocker TEXT,
                blockers_json TEXT,
                hard_blockers_json TEXT,
                verification_blockers_json TEXT,
                evidence_json TEXT,
                score_components_json TEXT,
                profile_features_json TEXT,
                quality_snapshot_json TEXT,
                history_snapshot_json TEXT,
                blocker_type TEXT,
                would_have_been_direction TEXT,
                adverse_after_block INTEGER,
                blocker_saved_trade INTEGER,
                blocker_false_block_possible INTEGER,
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
                opportunity_profile_key TEXT,
                opportunity_profile_version TEXT,
                opportunity_profile_side TEXT,
                opportunity_profile_asset TEXT,
                opportunity_profile_pair_family TEXT,
                opportunity_profile_strategy TEXT,
                due_at REAL,
                start_price REAL,
                end_price REAL,
                start_price_source TEXT,
                end_price_source TEXT,
                outcome_price_source TEXT,
                raw_move REAL,
                direction_adjusted_move REAL,
                followthrough INTEGER,
                adverse INTEGER,
                reversal INTEGER,
                result_label TEXT,
                price_source TEXT,
                status TEXT,
                failure_reason TEXT,
                blocker_type TEXT,
                would_have_been_direction TEXT,
                adverse_after_block INTEGER,
                blocker_saved_trade INTEGER,
                blocker_false_block_possible INTEGER,
                completed_at REAL,
                created_at REAL,
                updated_at REAL,
                settled_by TEXT,
                catchup INTEGER,
                PRIMARY KEY(trade_opportunity_id, window_sec)
            );
            CREATE INDEX IF NOT EXISTS idx_opportunity_outcomes_status ON opportunity_outcomes(status);

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
                payload_mode TEXT,
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
                delivery_decision TEXT,
                reason TEXT,
                sent_to_telegram INTEGER,
                suppressed INTEGER,
                notifier_variant TEXT,
                notifier_template TEXT,
                timestamp REAL,
                stage TEXT,
                gate_reason TEXT,
                delivered INTEGER,
                notifier_sent_at REAL,
                telegram_update_kind TEXT,
                suppression_reason TEXT,
                audit_json TEXT,
                payload_mode TEXT,
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
                payload_mode TEXT,
                archive_written_at REAL,
                created_at REAL,
                updated_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_case_followups_case_id ON case_followups(case_id);
            CREATE INDEX IF NOT EXISTS idx_case_followups_signal_id ON case_followups(signal_id);
            CREATE INDEX IF NOT EXISTS idx_case_followups_archive_written_at ON case_followups(archive_written_at);
            """
        )
        _ensure_columns(
            conn,
            "raw_events",
            {
                "archive_path": "TEXT",
                "archive_date": "TEXT",
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "parsed_events",
            {
                "archive_path": "TEXT",
                "archive_date": "TEXT",
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "market_context_attempts",
            {
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "delivery_audit",
            {
                "delivery_decision": "TEXT",
                "reason": "TEXT",
                "sent_to_telegram": "INTEGER",
                "suppressed": "INTEGER",
                "notifier_variant": "TEXT",
                "notifier_template": "TEXT",
                "timestamp": "REAL",
                "archive_path": "TEXT",
                "archive_date": "TEXT",
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "telegram_deliveries",
            {
                "archive_path": "TEXT",
                "archive_date": "TEXT",
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "case_followups",
            {
                "archive_path": "TEXT",
                "archive_date": "TEXT",
                "payload_hash": "TEXT",
                "payload_mode": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "outcomes",
            {
                "due_at": "REAL",
                "start_price_source": "TEXT",
                "end_price_source": "TEXT",
                "outcome_price_source": "TEXT",
                "settled_by": "TEXT",
                "catchup": "INTEGER",
            },
        )
        _ensure_columns(
            conn,
            "opportunity_outcomes",
            {
                "opportunity_profile_key": "TEXT",
                "opportunity_profile_version": "TEXT",
                "opportunity_profile_side": "TEXT",
                "opportunity_profile_asset": "TEXT",
                "opportunity_profile_pair_family": "TEXT",
                "opportunity_profile_strategy": "TEXT",
                "due_at": "REAL",
                "start_price": "REAL",
                "end_price": "REAL",
                "start_price_source": "TEXT",
                "end_price_source": "TEXT",
                "outcome_price_source": "TEXT",
                "reversal": "INTEGER",
                "updated_at": "REAL",
                "settled_by": "TEXT",
                "catchup": "INTEGER",
                "blocker_type": "TEXT",
                "would_have_been_direction": "TEXT",
                "adverse_after_block": "INTEGER",
                "blocker_saved_trade": "INTEGER",
                "blocker_false_block_possible": "INTEGER",
            },
        )
        _ensure_columns(
            conn,
            "signals",
            {
                "final_trading_output_source": "TEXT",
                "final_trading_output_label": "TEXT",
                "final_trading_output_allowed": "INTEGER",
                "legacy_chase_downgraded": "INTEGER",
                "legacy_chase_downgrade_reason": "TEXT",
                "opportunity_gate_required": "INTEGER",
                "opportunity_gate_passed": "INTEGER",
                "opportunity_gate_failure_reason": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "trade_opportunities",
            {
                "opportunity_profile_key": "TEXT",
                "opportunity_profile_version": "TEXT",
                "opportunity_profile_side": "TEXT",
                "opportunity_profile_asset": "TEXT",
                "opportunity_profile_pair_family": "TEXT",
                "opportunity_profile_strategy": "TEXT",
                "raw_score": "REAL",
                "calibrated_score": "REAL",
                "calibration_adjustment": "REAL",
                "calibration_reason": "TEXT",
                "calibration_sample_count": "INTEGER",
                "calibration_confidence": "REAL",
                "calibration_source": "TEXT",
                "primary_hard_blocker": "TEXT",
                "primary_verification_blocker": "TEXT",
                "hard_blockers_json": "TEXT",
                "verification_blockers_json": "TEXT",
                "profile_features_json": "TEXT",
                "blocker_type": "TEXT",
                "would_have_been_direction": "TEXT",
                "adverse_after_block": "INTEGER",
                "blocker_saved_trade": "INTEGER",
                "blocker_false_block_possible": "INTEGER",
                "final_trading_output_source": "TEXT",
                "final_trading_output_label": "TEXT",
                "final_trading_output_allowed": "INTEGER",
                "legacy_chase_downgraded": "INTEGER",
                "legacy_chase_downgrade_reason": "TEXT",
                "opportunity_gate_required": "INTEGER",
                "opportunity_gate_passed": "INTEGER",
                "opportunity_gate_failure_reason": "TEXT",
            },
        )
        _ensure_columns(
            conn,
            "delivery_audit",
            {
                "final_trading_output_source": "TEXT",
                "final_trading_output_label": "TEXT",
                "final_trading_output_allowed": "INTEGER",
                "legacy_chase_downgraded": "INTEGER",
                "legacy_chase_downgrade_reason": "TEXT",
                "opportunity_gate_required": "INTEGER",
                "opportunity_gate_passed": "INTEGER",
                "opportunity_gate_failure_reason": "TEXT",
            },
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_due_at ON outcomes(due_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_opportunity_outcomes_due_at ON opportunity_outcomes(due_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_archive_date ON raw_events(archive_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_parsed_events_archive_date ON parsed_events(archive_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delivery_audit_archive_date ON delivery_audit(archive_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_telegram_deliveries_archive_date ON telegram_deliveries(archive_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_case_followups_archive_date ON case_followups(archive_date)")
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


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for column, column_type in columns.items():
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


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
    mode = _mode_for_table("raw_events")
    if not bool(SQLITE_WRITE_RAW_EVENTS) or mode == "off":
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    captured_at = _real(_first(data, "captured_at", "ingest_ts", "timestamp", "ts")) or _archive_ts(data, envelope)
    archive_path, archive_date, payload_hash = _payload_meta("raw_events", data, envelope)
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
            "raw_json": _json(data) if mode == "full" else None,
            "archive_path": archive_path,
            "archive_date": archive_date,
            "payload_hash": payload_hash,
            "payload_mode": mode,
            "created_at": now,
            "updated_at": now,
        },
        ("event_id",),
    )


def write_parsed_event(record: Any) -> bool:
    mode = _mode_for_table("parsed_events")
    if not bool(SQLITE_WRITE_PARSED_EVENTS) or mode == "off":
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    parsed_at = _real(_first(data, "parsed_at", "timestamp", "ts")) or _archive_ts(data, envelope)
    archive_path, archive_date, payload_hash = _payload_meta("parsed_events", data, envelope)
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
            "parsed_json": _json(data) if mode == "full" else None,
            "archive_path": archive_path,
            "archive_date": archive_date,
            "payload_hash": payload_hash,
            "payload_mode": mode,
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
            "final_trading_output_source": _text(_first(data, "final_trading_output_source")),
            "final_trading_output_label": _text(_first(data, "final_trading_output_label")),
            "final_trading_output_allowed": _bool_int(_first(data, "final_trading_output_allowed")),
            "legacy_chase_downgraded": _bool_int(_first(data, "legacy_chase_downgraded")),
            "legacy_chase_downgrade_reason": _text(_first(data, "legacy_chase_downgrade_reason")),
            "opportunity_gate_required": _bool_int(_first(data, "opportunity_gate_required")),
            "opportunity_gate_passed": _bool_int(_first(data, "opportunity_gate_passed")),
            "opportunity_gate_failure_reason": _text(_first(data, "opportunity_gate_failure_reason")),
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
        "trade_opportunity_raw_score": _first(data, "trade_opportunity_raw_score", "opportunity_raw_score"),
        "trade_opportunity_calibrated_score": _first(data, "trade_opportunity_calibrated_score", "opportunity_calibrated_score"),
        "trade_opportunity_calibration_adjustment": _first(data, "opportunity_calibration_adjustment"),
        "trade_opportunity_calibration_confidence": _first(data, "opportunity_calibration_confidence"),
        "non_lp_support_score": _first(data, "non_lp_support_score"),
        "non_lp_risk_score": _first(data, "non_lp_risk_score"),
        "non_lp_component_score": _first(data, "trade_opportunity_non_lp_component_score"),
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
    mode = _mode_for_table("delivery_audit")
    if not bool(SQLITE_WRITE_DELIVERY_AUDIT) or mode == "off":
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    now = _now()
    archive_written_at = _archive_ts(data, envelope)
    archive_path, archive_date, payload_hash = _payload_meta("delivery_audit", data, envelope)
    signal_id = str(_first(data, "signal_id") or "").strip()
    audit_id = str(_first(data, "audit_id", "delivery_audit_id") or "").strip()
    if not audit_id:
        audit_id = "audit_" + _hash({"signal_id": signal_id, "stage": _first(data, "stage"), "ts": archive_written_at, "data": data})[:16]
    sent_to_telegram = _bool_int(_first(data, "sent_to_telegram", "delivered_notification", "delivered"))
    suppression_reason = _text(_first(data, "telegram_suppression_reason", "suppression_reason"))
    timestamp_value = _real(_first(data, "timestamp", "archive_written_at", "ts", "created_at")) or archive_written_at
    ok = _upsert(
        "delivery_audit",
        {
            "audit_id": audit_id,
            "signal_id": signal_id or None,
            "event_id": _text(_first(data, "event_id")),
            "trade_opportunity_id": _text(_first(data, "trade_opportunity_id")),
            "asset": _text(_first(data, "asset_symbol", "asset")),
            "delivery_decision": _text(_first(data, "delivery_decision", "delivery_reason", "delivery_class")),
            "reason": _text(_first(data, "reason", "gate_reason", "delivery_reason", "suppression_reason")),
            "sent_to_telegram": sent_to_telegram,
            "suppressed": _suppressed_flag(sent_to_telegram, suppression_reason),
            "notifier_variant": _text(_first(data, "notifier_variant", "message_variant")),
            "notifier_template": _text(_first(data, "notifier_template", "message_template", "template")),
            "timestamp": timestamp_value,
            "stage": _text(_first(data, "stage")),
            "gate_reason": _text(_first(data, "gate_reason", "delivery_reason")),
            "delivered": sent_to_telegram,
            "notifier_sent_at": _real(_first(data, "notifier_sent_at")),
            "telegram_update_kind": _text(_first(data, "telegram_update_kind")),
            "suppression_reason": suppression_reason,
            "final_trading_output_source": _text(_first(data, "final_trading_output_source")),
            "final_trading_output_label": _text(_first(data, "final_trading_output_label")),
            "final_trading_output_allowed": _bool_int(_first(data, "final_trading_output_allowed")),
            "legacy_chase_downgraded": _bool_int(_first(data, "legacy_chase_downgraded")),
            "legacy_chase_downgrade_reason": _text(_first(data, "legacy_chase_downgrade_reason")),
            "opportunity_gate_required": _bool_int(_first(data, "opportunity_gate_required")),
            "opportunity_gate_passed": _bool_int(_first(data, "opportunity_gate_passed")),
            "opportunity_gate_failure_reason": _text(_first(data, "opportunity_gate_failure_reason")),
            "audit_json": _json(data) if mode == "full" else None,
            "archive_written_at": archive_written_at,
            "archive_path": archive_path,
            "archive_date": archive_date,
            "payload_hash": payload_hash,
            "payload_mode": mode,
            "created_at": now,
            "updated_at": now,
        },
        ("audit_id",),
    )
    if ok:
        telegram_payload = dict(data)
        telegram_payload.setdefault("delivery_audit_id", audit_id)
        telegram_payload.setdefault("archive_category", "delivery_audit")
        write_telegram_delivery(telegram_payload)
    return ok


def write_case_followup(record: Any) -> bool:
    mode = _mode_for_table("case_followups")
    if not bool(SQLITE_WRITE_CASE_FOLLOWUPS) or mode == "off":
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    followup = data.get("followup") if isinstance(data.get("followup"), dict) else data
    now = _now()
    archive_written_at = _archive_ts(data, envelope)
    archive_path, archive_date, payload_hash = _payload_meta("case_followups", data, envelope)
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
            "followup_json": _json(data) if mode == "full" else None,
            "archive_written_at": archive_written_at,
            "archive_path": archive_path,
            "archive_date": archive_date,
            "payload_hash": payload_hash,
            "payload_mode": mode,
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
            "opportunity_profile_key": _text(_first(data, "opportunity_profile_key")),
            "opportunity_profile_version": _text(_first(data, "opportunity_profile_version")),
            "opportunity_profile_side": _text(_first(data, "opportunity_profile_side")),
            "opportunity_profile_asset": _text(_first(data, "opportunity_profile_asset")),
            "opportunity_profile_pair_family": _text(_first(data, "opportunity_profile_pair_family")),
            "opportunity_profile_strategy": _text(_first(data, "opportunity_profile_strategy")),
            "side": _text(_first(data, "trade_opportunity_side", "side")),
            "status": status,
            "raw_score": _real(_first(data, "opportunity_raw_score", "trade_opportunity_raw_score")),
            "score": _real(_first(data, "trade_opportunity_score", "score")),
            "calibrated_score": _real(_first(data, "opportunity_calibrated_score", "trade_opportunity_calibrated_score", "trade_opportunity_score")),
            "calibration_adjustment": _real(_first(data, "opportunity_calibration_adjustment")),
            "calibration_reason": _text(_first(data, "opportunity_calibration_reason")),
            "calibration_sample_count": _int(_first(data, "opportunity_calibration_sample_count")),
            "calibration_confidence": _real(_first(data, "opportunity_calibration_confidence")),
            "calibration_source": _text(_first(data, "opportunity_calibration_source")),
            "confidence": _text(_first(data, "trade_opportunity_confidence", "confidence")),
            "label": _text(_first(data, "trade_opportunity_label", "label")),
            "reason": _text(_first(data, "trade_opportunity_reason", "reason")),
            "primary_blocker": _text(_first(data, "trade_opportunity_primary_blocker", "primary_blocker")),
            "primary_hard_blocker": _text(_first(data, "trade_opportunity_primary_hard_blocker")),
            "primary_verification_blocker": _text(_first(data, "trade_opportunity_primary_verification_blocker")),
            "blockers_json": _json(_first(data, "trade_opportunity_blockers", "blockers", default=[])),
            "hard_blockers_json": _json(_first(data, "trade_opportunity_hard_blockers", default=[])),
            "verification_blockers_json": _json(_first(data, "trade_opportunity_verification_blockers", default=[])),
            "evidence_json": _json(_first(data, "trade_opportunity_evidence", "evidence", default=[])),
            "score_components_json": _json(_first(data, "trade_opportunity_score_components", "score_components", default={})),
            "profile_features_json": _json(_first(data, "opportunity_profile_features_json", default={})),
            "quality_snapshot_json": _json(_first(data, "trade_opportunity_quality_snapshot", "quality_snapshot", default={})),
            "history_snapshot_json": _json(_first(data, "trade_opportunity_history_snapshot", "history_snapshot", default={})),
            "blocker_type": _text(_first(data, "blocker_type", "trade_opportunity_primary_hard_blocker", "trade_opportunity_primary_blocker")),
            "would_have_been_direction": _text(_first(data, "would_have_been_direction", "trade_opportunity_side")),
            "adverse_after_block": _bool_int(_first(data, "adverse_after_block")),
            "blocker_saved_trade": _bool_int(_first(data, "blocker_saved_trade")),
            "blocker_false_block_possible": _bool_int(_first(data, "blocker_false_block_possible")),
            "required_confirmation": _text(_first(data, "trade_opportunity_required_confirmation", "required_confirmation")),
            "invalidated_by": _text(_first(data, "trade_opportunity_invalidated_by", "invalidated_by")),
            "created_at": created_at,
            "expires_at": _real(_first(data, "trade_opportunity_expires_at", "expires_at")),
            "telegram_sent": _bool_int(_first(data, "trade_opportunity_delivered_notification", "telegram_should_send", "sent_to_telegram")),
            "final_trading_output_source": _text(_first(data, "final_trading_output_source")),
            "final_trading_output_label": _text(_first(data, "final_trading_output_label")),
            "final_trading_output_allowed": _bool_int(_first(data, "final_trading_output_allowed")),
            "legacy_chase_downgraded": _bool_int(_first(data, "legacy_chase_downgraded")),
            "legacy_chase_downgrade_reason": _text(_first(data, "legacy_chase_downgrade_reason")),
            "opportunity_gate_required": _bool_int(_first(data, "opportunity_gate_required")),
            "opportunity_gate_passed": _bool_int(_first(data, "opportunity_gate_passed")),
            "opportunity_gate_failure_reason": _text(_first(data, "opportunity_gate_failure_reason")),
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
            "due_at": _real(window.get("due_at")) or (created_at + int(window_sec)),
            "price_source": _text(window.get("price_source") or data.get("outcome_price_source")),
            "start_price_source": _text(window.get("start_price_source") or data.get("start_price_source") or data.get("outcome_price_source")),
            "end_price_source": _text(window.get("end_price_source") or window.get("price_source")),
            "outcome_price_source": _text(window.get("outcome_price_source") or window.get("end_price_source") or window.get("price_source") or data.get("outcome_price_source")),
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
            "settled_by": _text(window.get("settled_by") or data.get("settled_by")),
            "catchup": _bool_int(window.get("catchup") if window.get("catchup") is not None else data.get("catchup")),
        },
        ("signal_id", "window_sec"),
    )


def upsert_outcome_window(record: Any) -> bool:
    if not bool(SQLITE_WRITE_OUTCOMES):
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    signal_id = str(_first(data, "signal_id", "record_id", "outcome_tracking_key") or "").strip()
    window_sec = _int(data.get("window_sec"))
    if not signal_id or window_sec is None:
        return False
    window = {
        "outcome_id": data.get("outcome_id"),
        "due_at": data.get("due_at"),
        "status": data.get("status"),
        "price_source": data.get("price_source") or data.get("outcome_price_source"),
        "start_price_source": data.get("start_price_source"),
        "end_price_source": data.get("end_price_source"),
        "outcome_price_source": data.get("outcome_price_source"),
        "price_start": data.get("start_price") if data.get("start_price") is not None else data.get("price_start"),
        "price_end": data.get("end_price") if data.get("end_price") is not None else data.get("price_end"),
        "raw_move_after": data.get("raw_move"),
        "direction_adjusted_move_after": data.get("direction_adjusted_move"),
        "followthrough_positive": data.get("followthrough"),
        "adverse_by_direction": data.get("adverse"),
        "reversal": data.get("reversal"),
        "failure_reason": data.get("failure_reason") or data.get("outcome_failure_reason"),
        "completed_at": data.get("completed_at"),
        "settled_by": data.get("settled_by"),
        "catchup": data.get("catchup"),
    }
    payload = {
        "signal_id": signal_id,
        "record_id": str(data.get("record_id") or signal_id),
        "trade_opportunity_id": data.get("trade_opportunity_id"),
        "asset_symbol": data.get("asset") or data.get("asset_symbol"),
        "pair_label": data.get("pair") or data.get("pair_label"),
        "direction": data.get("direction"),
        "direction_bucket": data.get("direction_bucket"),
        "created_at": data.get("created_at"),
        "outcome_price_source": data.get("outcome_price_source") or data.get("price_source"),
        "outcome_price_start": data.get("start_price") if data.get("start_price") is not None else data.get("price_start"),
        "outcome_price_end": data.get("end_price") if data.get("end_price") is not None else data.get("price_end"),
        "outcome_window_status": data.get("status"),
        "outcome_failure_reason": data.get("failure_reason") or data.get("outcome_failure_reason"),
        "settled_by": data.get("settled_by"),
        "catchup": data.get("catchup"),
        "outcome_windows": {f"{int(window_sec)}s": window},
    }
    return _write_outcome_window(payload, window, int(window_sec), signal_id=signal_id)


def _trade_opportunity_profile_lookup(opportunity_id: str) -> dict[str, Any]:
    conn = get_connection()
    if conn is None or not opportunity_id:
        return {}
    row = conn.execute(
        """
        SELECT opportunity_profile_key,
               opportunity_profile_version,
               opportunity_profile_side,
               opportunity_profile_asset,
               opportunity_profile_pair_family,
               opportunity_profile_strategy,
               primary_blocker,
               primary_hard_blocker
        FROM trade_opportunities
        WHERE trade_opportunity_id=?
        """,
        (str(opportunity_id),),
    ).fetchone()
    return dict(row) if row else {}


def upsert_opportunity_outcome(record: Any) -> bool:
    data, _envelope = _unwrap(record)
    if not data:
        return False
    opportunity_id = str(data.get("trade_opportunity_id") or "").strip()
    window_sec = _int(data.get("window_sec"))
    if not opportunity_id or window_sec is None:
        return False
    lookup = _trade_opportunity_profile_lookup(opportunity_id)
    now = _now()
    return _upsert(
        "opportunity_outcomes",
        {
            "trade_opportunity_id": opportunity_id,
            "window_sec": int(window_sec),
            "opportunity_profile_key": _text(data.get("opportunity_profile_key") or lookup.get("opportunity_profile_key")),
            "opportunity_profile_version": _text(data.get("opportunity_profile_version") or lookup.get("opportunity_profile_version")),
            "opportunity_profile_side": _text(data.get("opportunity_profile_side") or lookup.get("opportunity_profile_side")),
            "opportunity_profile_asset": _text(data.get("opportunity_profile_asset") or lookup.get("opportunity_profile_asset")),
            "opportunity_profile_pair_family": _text(data.get("opportunity_profile_pair_family") or lookup.get("opportunity_profile_pair_family")),
            "opportunity_profile_strategy": _text(data.get("opportunity_profile_strategy") or lookup.get("opportunity_profile_strategy")),
            "due_at": _real(data.get("due_at")),
            "start_price": _real(data.get("start_price") or data.get("price_start")),
            "end_price": _real(data.get("end_price") or data.get("price_end")),
            "start_price_source": _text(data.get("start_price_source")),
            "end_price_source": _text(data.get("end_price_source")),
            "outcome_price_source": _text(data.get("outcome_price_source") or data.get("price_source")),
            "raw_move": _real(data.get("raw_move")),
            "direction_adjusted_move": _real(data.get("direction_adjusted_move")),
            "followthrough": _bool_int(data.get("followthrough")),
            "adverse": _bool_int(data.get("adverse")),
            "reversal": _bool_int(data.get("reversal")),
            "result_label": _text(data.get("result_label")),
            "price_source": _text(data.get("price_source") or data.get("outcome_price_source")),
            "status": _text(data.get("status")),
            "failure_reason": _text(data.get("failure_reason")),
            "blocker_type": _text(data.get("blocker_type") or lookup.get("primary_hard_blocker") or lookup.get("primary_blocker")),
            "would_have_been_direction": _text(data.get("would_have_been_direction") or lookup.get("opportunity_profile_side")),
            "adverse_after_block": _bool_int(data.get("adverse_after_block")),
            "blocker_saved_trade": _bool_int(data.get("blocker_saved_trade")),
            "blocker_false_block_possible": _bool_int(data.get("blocker_false_block_possible")),
            "completed_at": _real(data.get("completed_at")),
            "created_at": _real(data.get("created_at")) or now,
            "updated_at": now,
            "settled_by": _text(data.get("settled_by")),
            "catchup": _bool_int(data.get("catchup")),
        },
        ("trade_opportunity_id", "window_sec"),
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
            "opportunity_profile_key": _text(record.get("opportunity_profile_key")),
            "opportunity_profile_version": _text(record.get("opportunity_profile_version")),
            "opportunity_profile_side": _text(record.get("opportunity_profile_side") or record.get("trade_opportunity_side")),
            "opportunity_profile_asset": _text(record.get("opportunity_profile_asset") or record.get("asset_symbol")),
            "opportunity_profile_pair_family": _text(record.get("opportunity_profile_pair_family")),
            "opportunity_profile_strategy": _text(record.get("opportunity_profile_strategy")),
            "due_at": _real(record.get(f"due_at_{key}") or record.get("due_at")) or (created_at + int(window_sec)),
            "start_price": _real(record.get(f"outcome_price_start_{key}") or record.get("outcome_price_start")),
            "end_price": _real(record.get(f"outcome_price_end_{key}") or record.get("outcome_price_end")),
            "start_price_source": _text(record.get(f"start_price_source_{key}") or record.get("start_price_source") or record.get("opportunity_outcome_source") or record.get("outcome_price_source")),
            "end_price_source": _text(record.get(f"end_price_source_{key}") or record.get("end_price_source") or record.get("opportunity_outcome_source") or record.get("outcome_price_source")),
            "outcome_price_source": _text(record.get(f"outcome_price_source_{key}") or record.get("opportunity_outcome_source") or record.get("outcome_price_source")),
            "raw_move": _real(record.get(f"raw_move_after_{key}")),
            "direction_adjusted_move": _real(record.get(f"direction_adjusted_move_after_{key}")),
            "followthrough": _bool_int(followthrough),
            "adverse": _bool_int(adverse),
            "reversal": _bool_int(record.get(f"opportunity_reversal_{key}") or record.get("reversal_after_climax")),
            "result_label": result_label,
            "price_source": _text(record.get(f"outcome_price_source_{key}") or record.get("opportunity_outcome_source") or record.get("outcome_price_source")),
            "status": _text(status),
            "failure_reason": _text(record.get(f"outcome_failure_reason_{key}") or record.get("opportunity_invalidated_reason") or record.get("outcome_failure_reason")),
            "blocker_type": _text(record.get("blocker_type") or record.get("trade_opportunity_primary_hard_blocker") or record.get("trade_opportunity_primary_blocker")),
            "would_have_been_direction": _text(record.get("would_have_been_direction") or record.get("trade_opportunity_side")),
            "adverse_after_block": _bool_int(record.get(f"adverse_after_block_{key}") if record.get(f"adverse_after_block_{key}") is not None else record.get("adverse_after_block")),
            "blocker_saved_trade": _bool_int(record.get(f"blocker_saved_trade_{key}") if record.get(f"blocker_saved_trade_{key}") is not None else record.get("blocker_saved_trade")),
            "blocker_false_block_possible": _bool_int(record.get(f"blocker_false_block_possible_{key}") if record.get(f"blocker_false_block_possible_{key}") is not None else record.get("blocker_false_block_possible")),
            "completed_at": _real(record.get(f"outcome_completed_at_{key}") or record.get("opportunity_invalidated_at")),
            "created_at": created_at or now,
            "updated_at": now,
            "settled_by": _text(record.get("settled_by")),
            "catchup": _bool_int(record.get("catchup")),
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
    mode = _mode_for_table("market_context_attempts")
    if mode == "off":
        return False
    data, _envelope = _unwrap(record)
    if not data:
        return False
    if mode == "aggregate" and not str(data.get("attempt_id") or "").strip():
        return False
    created_at = _real(data.get("created_at") or data.get("ts")) or _now()
    payload_hash = _payload_hash(data)
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
            "payload_hash": payload_hash,
            "payload_mode": mode,
            "created_at": created_at,
        },
        ("attempt_id",),
    )


def write_telegram_delivery(record: Any) -> bool:
    mode = _mode_for_table("telegram_deliveries")
    if mode == "off":
        return False
    data, envelope = _unwrap(record)
    if not data:
        return False
    signal_id = str(_first(data, "signal_id") or "").strip()
    sent = _bool_int(_first(data, "sent_to_telegram", "delivered_notification", "telegram_should_send"))
    suppressed_reason = str(_first(data, "telegram_suppression_reason", "suppression_reason") or "").strip()
    if not signal_id and sent is None and not suppressed_reason:
        return False
    sent_at = _real(_first(data, "notifier_sent_at", "sent_at"))
    created_at = _real(_first(data, "archive_written_at", "timestamp", "ts")) or _now()
    archive_path, archive_date, payload_hash = _payload_meta(_telegram_archive_category(data), data, envelope)
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
            "headline": _text(_first(data, "headline", "final_trading_output_label", "headline_label", "market_state_label")),
            "sent": sent if sent is not None else 0,
            "sent_at": sent_at,
            "suppressed": _suppressed_flag(sent, suppressed_reason),
            "suppression_reason": suppressed_reason or None,
            "telegram_update_kind": _text(_first(data, "telegram_update_kind")),
            "chat_id_hash": _text(_first(data, "chat_id_hash")),
            "message_json": _json(data) if mode == "full" else None,
            "archive_path": archive_path,
            "archive_date": archive_date,
            "payload_hash": payload_hash,
            "payload_mode": mode,
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
    counts: dict[str, int] = {}
    for category in ARCHIVE_CATEGORY_TABLES:
        total = 0
        for path in _archive_payload_paths(category, base_dir=base_dir, all_dates=True):
            total += _count_archive_payload_lines(path)
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
    size_info = db_file_sizes()
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
        **size_info,
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
    profile_rows = conn.execute(
        "SELECT scope_key, asset, pair, stats_json FROM quality_stats WHERE scope_type='opportunity_profile' AND stage='all'"
    ).fetchall()
    profiles: list[dict[str, Any]] = []
    for row in profile_rows:
        stats = _from_json(row["stats_json"], default={}) if row["stats_json"] else {}
        if not isinstance(stats, dict):
            stats = {}
        profile = dict(stats)
        profile.setdefault("profile_key", str(row["scope_key"] or ""))
        profile.setdefault("asset", str(row["asset"] or ""))
        profile.setdefault("pair_family", str(row["pair"] or ""))
        profiles.append(profile)
    ready_profiles = [
        row
        for row in profiles
        if int(row.get("sample_count") or 0) >= int(OPPORTUNITY_MIN_HISTORY_SAMPLES)
        and float(row.get("completion_60s_rate") or 0.0) >= float(OPPORTUNITY_MIN_OUTCOME_COMPLETION_RATE)
        and float(row.get("followthrough_60s_rate") or 0.0) >= float(OPPORTUNITY_MIN_60S_FOLLOWTHROUGH_RATE)
        and float(row.get("adverse_60s_rate") if row.get("adverse_60s_rate") is not None else 1.0) <= float(OPPORTUNITY_MAX_60S_ADVERSE_RATE)
    ]
    calibration_summary = opportunity_calibration_summary()
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
        "opportunity_profile_count": len(profiles),
        "profiles_ready_for_verified": [str(row.get("profile_key") or "") for row in sorted(ready_profiles, key=lambda item: (-int(item.get("sample_count") or 0), str(item.get("profile_key") or "")))[:10]],
        "top_profiles_by_sample": [
            {
                "profile_key": str(row.get("profile_key") or ""),
                "sample_count": int(row.get("sample_count") or 0),
                "followthrough_60s_rate": float(row.get("followthrough_60s_rate") or 0.0),
                "adverse_60s_rate": float(row.get("adverse_60s_rate") if row.get("adverse_60s_rate") is not None else 1.0),
            }
            for row in sorted(profiles, key=lambda item: (-int(item.get("sample_count") or 0), str(item.get("profile_key") or "")))[:10]
        ],
        "top_profiles_by_followthrough": [
            {
                "profile_key": str(row.get("profile_key") or ""),
                "sample_count": int(row.get("sample_count") or 0),
                "followthrough_60s_rate": float(row.get("followthrough_60s_rate") or 0.0),
            }
            for row in sorted(
                [item for item in profiles if int(item.get("completed_60s") or 0) > 0],
                key=lambda item: (-float(item.get("followthrough_60s_rate") or 0.0), -int(item.get("sample_count") or 0), str(item.get("profile_key") or "")),
            )[:10]
        ],
        "top_profiles_by_adverse": [
            {
                "profile_key": str(row.get("profile_key") or ""),
                "sample_count": int(row.get("sample_count") or 0),
                "adverse_60s_rate": float(row.get("adverse_60s_rate") if row.get("adverse_60s_rate") is not None else 1.0),
            }
            for row in sorted(
                [item for item in profiles if int(item.get("completed_60s") or 0) > 0],
                key=lambda item: (-float(item.get("adverse_60s_rate") if item.get("adverse_60s_rate") is not None else 1.0), -int(item.get("sample_count") or 0), str(item.get("profile_key") or "")),
            )[:10]
        ],
        "calibration_summary": calibration_summary,
    }


def opportunity_calibration_summary() -> dict[str, Any]:
    conn = get_connection()
    if conn is None:
        return {"available": False, "reason": _INIT_FAILED_REASON or "not_initialized"}
    try:
        from opportunity_calibration import calibration_health_summary

        calibration_rows = [
            {
                "scope_key": row["scope_key"],
                "asset": row["asset"],
                "pair": row["pair"],
                "stats_json": _from_json(row["stats_json"], default={}) if row["stats_json"] else {},
            }
            for row in conn.execute(
                "SELECT scope_key, asset, pair, stats_json FROM quality_stats WHERE scope_type='opportunity_calibration' AND stage='60s'"
            ).fetchall()
        ]
        profile_rows = [
            {
                "scope_key": row["scope_key"],
                "asset": row["asset"],
                "pair": row["pair"],
                "stats_json": _from_json(row["stats_json"], default={}) if row["stats_json"] else {},
            }
            for row in conn.execute(
                "SELECT scope_key, asset, pair, stats_json FROM quality_stats WHERE scope_type='opportunity_profile' AND stage='all'"
            ).fetchall()
        ]
        payload = calibration_health_summary(calibration_rows, profile_rows=profile_rows)
        payload["available"] = True
        return payload
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


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


def _iter_archive_payloads(category: str, *, date: str | None = None, all_dates: bool = False) -> tuple[int, int, int]:
    imported = 0
    bad_rows = 0
    skipped_payload_bytes = 0
    for path in _archive_payload_paths(category, date=date, all_dates=all_dates):
        if not path.exists():
            continue
        try:
            with _open_archive_payload_path(path) as handle:
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
                        mode = _mode_for_table(ARCHIVE_CATEGORY_TABLES.get(category, category))
                        if mode in {"index_only", "slim"}:
                            data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                            skipped_payload_bytes += len(_json(data).encode("utf-8"))
        except OSError:
            bad_rows += 1
            continue
    return imported, bad_rows, skipped_payload_bytes


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
        "imported_rows_by_category": {},
        "mode_by_category": {},
        "full_payload_rows": 0,
        "slim_rows": 0,
        "index_only_rows": 0,
        "estimated_payload_mb_skipped": 0.0,
    }
    conn = get_connection()
    previous_batch_mode = _BATCH_MODE
    try:
        _BATCH_MODE = True
        if conn is not None:
            conn.execute("BEGIN")
        for category in ARCHIVE_CATEGORY_TABLES:
            imported, bad_rows, skipped_payload_bytes = _iter_archive_payloads(category, date=date, all_dates=all_dates)
            mode = _mode_for_table(ARCHIVE_CATEGORY_TABLES.get(category, category))
            result["categories"][category] = {
                "imported_rows": imported,
                "bad_row_count": bad_rows,
                "mode": mode,
            }
            result["imported_rows_by_category"][category] = imported
            result["mode_by_category"][category] = mode
            result["imported_rows"] += imported
            result["bad_row_count"] += bad_rows
            result["estimated_payload_mb_skipped"] += round(skipped_payload_bytes / (1024 * 1024), 6)
            if mode == "full":
                result["full_payload_rows"] += imported
            elif mode == "slim":
                result["slim_rows"] += imported
            elif mode == "index_only":
                result["index_only_rows"] += imported
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


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def load_pending_outcome_rows(limit: int | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    if conn is None:
        return []
    sql = (
        "SELECT * FROM outcomes "
        "WHERE COALESCE(status, '') = 'pending' "
        "ORDER BY COALESCE(due_at, created_at + window_sec), created_at, signal_id, window_sec"
    )
    params: tuple[Any, ...] = ()
    if limit is not None and int(limit) > 0:
        sql = f"{sql} LIMIT ?"
        params = (int(limit),)
    try:
        return _rows_to_dicts(conn.execute(sql, params).fetchall())
    except sqlite3.Error as exc:
        _warn(f"load pending outcomes failed: {exc}")
        return []


def load_outcome_rows_from_db(
    *,
    signal_ids: list[str] | set[str] | tuple[str, ...] | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> list[dict[str, Any]]:
    conn = get_connection()
    if conn is None:
        return []
    clauses = []
    params: list[Any] = []
    if signal_ids:
        ids = [str(item) for item in signal_ids if str(item or "").strip()]
        if ids:
            placeholders = ", ".join("?" for _ in ids)
            clauses.append(f"signal_id IN ({placeholders})")
            params.extend(ids)
    if start_ts is not None:
        clauses.append("created_at >= ?")
        params.append(int(start_ts))
    if end_ts is not None:
        clauses.append("created_at <= ?")
        params.append(int(end_ts))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    try:
        rows = conn.execute(
            f"SELECT * FROM outcomes {where} ORDER BY created_at, signal_id, window_sec",
            tuple(params),
        ).fetchall()
        return _rows_to_dicts(rows)
    except sqlite3.Error as exc:
        _warn(f"load outcomes failed: {exc}")
        return []


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


def load_recent_non_lp_signal_rows(
    *,
    asset_symbol: str,
    start_ts: int,
    end_ts: int,
    limit: int = 120,
) -> list[dict[str, Any]]:
    conn = get_connection()
    if conn is None:
        return []
    asset = str(asset_symbol or "").strip().upper()
    if not asset:
        return []
    try:
        rows = conn.execute(
            """
            SELECT signal_id,
                   asset,
                   pair,
                   timestamp,
                   canonical_semantic_key,
                   lp_alert_stage,
                   trade_action_key,
                   asset_market_state_key,
                   direction,
                   signal_json
            FROM signals
            WHERE asset = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (asset, int(start_ts), int(end_ts), max(int(limit or 0), 1)),
        ).fetchall()
    except sqlite3.Error as exc:
        _warn(f"load recent non-lp signals failed: {exc}")
        return []
    payloads: list[dict[str, Any]] = []
    for row in rows:
        data = _from_json(row["signal_json"], {})
        if not isinstance(data, dict):
            data = {}
        merged = dict(data)
        merged.setdefault("signal_id", row["signal_id"])
        merged.setdefault("asset_symbol", row["asset"])
        merged.setdefault("asset", row["asset"])
        merged.setdefault("pair_label", row["pair"])
        merged.setdefault("timestamp", row["timestamp"])
        merged.setdefault("archive_ts", row["timestamp"])
        merged.setdefault("intent_type", row["canonical_semantic_key"])
        merged.setdefault("lp_alert_stage", row["lp_alert_stage"])
        merged.setdefault("trade_action_key", row["trade_action_key"])
        merged.setdefault("asset_market_state_key", row["asset_market_state_key"])
        merged.setdefault("direction", row["direction"])
        payloads.append(merged)
    return payloads


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


def checkpoint(db_path: str | Path | None = None) -> dict[str, Any]:
    resolved_db_path = _resolve_db_path(db_path) if db_path is not None else _DB_PATH
    db_path = resolved_db_path if resolved_db_path is not None else _resolve_db_path()
    wal_path = Path(f"{db_path}-wal")
    wal_exists_before = wal_path.exists()
    wal_size_before = wal_path.stat().st_size if wal_exists_before else 0
    payload: dict[str, Any] = {
        "db_path": str(db_path),
        "enabled": bool(SQLITE_ENABLE),
        "db_exists": db_path.exists(),
        "wal_exists_before": wal_exists_before,
        "wal_size_before": wal_size_before,
        "checkpoint_result": None,
        "wal_size_after": 0,
        "success": False,
        "message": "",
    }
    if not bool(SQLITE_ENABLE):
        payload["message"] = "sqlite_disabled"
        payload["success"] = True
        return payload
    if not db_path.exists():
        payload["message"] = "db_not_found"
        payload["success"] = True
        return payload
    conn = None
    try:
        conn = sqlite3.connect(
            str(db_path),
            timeout=max(float(SQLITE_BUSY_TIMEOUT_MS or 0) / 1000.0, 0.1),
        )
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        payload["checkpoint_result"] = list(row) if row is not None else []
        payload["wal_size_after"] = wal_path.stat().st_size if wal_path.exists() else 0
        payload["success"] = True
        payload["message"] = "ok"
    except sqlite3.Error as exc:
        payload["message"] = f"checkpoint_failed:{exc}"
    finally:
        if conn is not None:
            conn.close()
    return payload


def db_file_sizes(db_path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_db_path(db_path)
    wal_path = Path(f"{resolved}-wal")
    shm_path = Path(f"{resolved}-shm")
    db_exists = resolved.exists()
    db_size = int(resolved.stat().st_size) if db_exists else 0
    wal_size = int(wal_path.stat().st_size) if wal_path.exists() else 0
    shm_size = int(shm_path.stat().st_size) if shm_path.exists() else 0
    return {
        "db_path": str(resolved),
        "db_exists": db_exists,
        "db_size_bytes": db_size,
        "db_size_mb": round(db_size / (1024 * 1024), 4) if db_size else 0.0,
        "wal_path": str(wal_path),
        "wal_exists": wal_path.exists(),
        "wal_size_bytes": wal_size,
        "wal_size_mb": round(wal_size / (1024 * 1024), 4) if wal_size else 0.0,
        "shm_path": str(shm_path),
        "shm_exists": shm_path.exists(),
        "shm_size_bytes": shm_size,
        "shm_size_mb": round(shm_size / (1024 * 1024), 4) if shm_size else 0.0,
        "total_size_bytes": int(db_size + wal_size + shm_size),
        "total_size_mb": round((db_size + wal_size + shm_size) / (1024 * 1024), 4) if (db_size + wal_size + shm_size) else 0.0,
    }


def _table_last_updated(conn: sqlite3.Connection, table: str) -> float | None:
    columns = _table_columns(conn, table)
    for column in ("updated_at", "archive_written_at", "created_at", "captured_at", "parsed_at", "sent_at", "started_at"):
        if column not in columns:
            continue
        try:
            row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
        except sqlite3.Error:
            return None
        return _real(row[0]) if row else None
    return None


def _dbstat_table_sizes(conn: sqlite3.Connection) -> tuple[dict[str, int], bool]:
    try:
        rows = conn.execute(
            "SELECT name, SUM(pgsize) AS size_bytes FROM dbstat GROUP BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}, False
    sizes: dict[str, int] = {}
    for row in rows:
        name = str(row["name"] or "")
        if not name:
            continue
        sizes[name] = int(row["size_bytes"] or 0)
    return sizes, True


def _json_payload_stats_for_table(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    columns = _table_columns(conn, table)
    payload_sizes: dict[str, int] = {}
    total = 0
    for column in TABLE_JSON_COLUMNS.get(table, ()):
        if column not in columns:
            continue
        try:
            row = conn.execute(
                f"SELECT COALESCE(SUM(LENGTH({column})), 0) AS payload_size FROM {table}"
            ).fetchone()
        except sqlite3.Error:
            continue
        size_bytes = int(row["payload_size"] or 0)
        payload_sizes[column] = size_bytes
        total += size_bytes
    return {
        "json_payload_columns": payload_sizes,
        "json_payload_size_bytes": total,
        "json_payload_size_mb": round(total / (1024 * 1024), 4) if total else 0.0,
    }


def table_size_summary() -> dict[str, Any]:
    conn = get_connection()
    size_info = db_file_sizes()
    if conn is None:
        return {
            **size_info,
            "available": False,
            "reason": _INIT_FAILED_REASON or "not_initialized",
            "table_row_counts": {},
            "tables": [],
        }
    dbstat_sizes, dbstat_available = _dbstat_table_sizes(conn)
    tables: list[dict[str, Any]] = []
    counts = table_row_counts(tuple(table for table in REQUIRED_TABLES if _table_exists(conn, table)))
    payload_hash_coverage_by_table: dict[str, Any] = {}
    archive_path_coverage_by_table: dict[str, Any] = {}
    compactable_rows_by_table: dict[str, int] = {}
    backfill_needed_rows_by_table: dict[str, int] = {}
    full_payload_rows_by_table: dict[str, int] = {}
    legacy_full_payload_rows_by_table: dict[str, int] = {}
    for table in REQUIRED_TABLES:
        if not _table_exists(conn, table):
            continue
        row_count = max(int(counts.get(table) or 0), 0)
        payload_stats = _json_payload_stats_for_table(conn, table)
        metadata_stats = _payload_metadata_stats_for_table(conn, table)
        size_bytes = int(dbstat_sizes.get(table) or 0)
        current_mode = _mode_for_table(table)
        recommended_mode = TABLE_RECOMMENDED_MODES.get(table, current_mode)
        value_flags = TABLE_VALUE_FLAGS.get(table, {})
        estimated_savings = int(metadata_stats.get("compactable_payload_bytes") or 0) if recommended_mode in {"index_only", "slim"} else 0
        can_compact = bool(metadata_stats.get("compactable_rows"))
        payload_hash_coverage_by_table[table] = {
            "covered_rows": int(metadata_stats.get("payload_hash_covered_rows") or 0),
            "full_payload_rows": int(metadata_stats.get("full_payload_rows") or 0),
            "coverage": float(metadata_stats.get("payload_hash_coverage") or 0.0),
        }
        archive_path_coverage_by_table[table] = {
            "covered_rows": int(metadata_stats.get("archive_path_covered_rows") or 0),
            "full_payload_rows": int(metadata_stats.get("full_payload_rows") or 0),
            "coverage": float(metadata_stats.get("archive_path_coverage") or 0.0),
        }
        compactable_rows_by_table[table] = int(metadata_stats.get("compactable_rows") or 0)
        backfill_needed_rows_by_table[table] = int(metadata_stats.get("backfill_needed_rows") or 0)
        full_payload_rows_by_table[table] = int(metadata_stats.get("full_payload_rows") or 0)
        legacy_full_payload_rows_by_table[table] = int(metadata_stats.get("legacy_full_payload_rows") or 0)
        tables.append(
            {
                "table": table,
                "row_count": row_count,
                "table_size_bytes": size_bytes,
                "table_size_mb": round(size_bytes / (1024 * 1024), 4) if size_bytes else 0.0,
                "last_updated": _table_last_updated(conn, table),
                "data_value_class": TABLE_VALUE_CLASSES.get(table, "operational_diagnostics"),
                "current_mode": current_mode,
                "recommended_mode": recommended_mode,
                "can_compact": can_compact,
                "estimated_savings_bytes": estimated_savings,
                "estimated_savings_mb": round(estimated_savings / (1024 * 1024), 4) if estimated_savings else 0.0,
                "critical_for_verified": bool(value_flags.get("candidate_to_verified") or value_flags.get("quality_outcome")),
                "critical_for_opportunity_score": bool(value_flags.get("opportunity_score")),
                "archive_only_ok": table in {"raw_events", "parsed_events"},
                **payload_stats,
                **metadata_stats,
                **value_flags,
            }
        )
    biggest_tables_by_rows = [
        {"table": item["table"], "row_count": item["row_count"]}
        for item in sorted(tables, key=lambda row: (-int(row["row_count"]), row["table"]))[:10]
    ]
    biggest_tables_by_size = [
        {
            "table": item["table"],
            "table_size_bytes": item["table_size_bytes"],
            "json_payload_size_bytes": item["json_payload_size_bytes"],
        }
        for item in sorted(tables, key=lambda row: (-int(row["table_size_bytes"]), row["table"]))[:10]
    ]
    return {
        **size_info,
        "available": True,
        "dbstat_available": dbstat_available,
        "table_row_counts": {item["table"]: item["row_count"] for item in tables},
        "payload_hash_coverage_by_table": payload_hash_coverage_by_table,
        "archive_path_coverage_by_table": archive_path_coverage_by_table,
        "compactable_rows_by_table": compactable_rows_by_table,
        "backfill_needed_rows_by_table": backfill_needed_rows_by_table,
        "full_payload_rows_by_table": full_payload_rows_by_table,
        "legacy_full_payload_rows_by_table": legacy_full_payload_rows_by_table,
        "estimated_savings_after_backfill_bytes": sum(
            int(item.get("estimated_savings_after_backfill_bytes") or 0) for item in tables
        ),
        "estimated_savings_after_backfill_mb": round(
            sum(int(item.get("estimated_savings_after_backfill_bytes") or 0) for item in tables) / (1024 * 1024),
            4,
        ),
        "tables": tables,
        "biggest_tables_by_rows": biggest_tables_by_rows,
        "biggest_tables_by_size": biggest_tables_by_size,
    }


def sqlite_data_value_audit(*, write_reports: bool = False) -> dict[str, Any]:
    summary = table_size_summary()
    tables = list(summary.get("tables") or [])
    must_keep_full = [
        item["table"]
        for item in tables
        if item["table"] in {
            "signals",
            "signal_features",
            "outcomes",
            "trade_opportunities",
            "opportunity_outcomes",
            "quality_stats",
            "asset_market_states",
            "no_trade_locks",
            "asset_cases",
            "prealert_lifecycle",
            "market_context_snapshots",
        }
    ]
    long_term_slim = [
        item["table"]
        for item in tables
        if item["table"] in {"delivery_audit", "telegram_deliveries", "market_context_attempts", "case_followups"}
    ]
    index_only = [
        item["table"]
        for item in tables
        if item["table"] in {"raw_events", "parsed_events"}
    ]
    archive_only_payload_fields = [
        "raw_events.raw_json",
        "parsed_events.parsed_json",
        "delivery_audit.audit_json",
        "telegram_deliveries.message_json",
        "case_followups.followup_json",
    ]
    fields_not_recommended_for_db = [
        "raw_events.raw_json",
        "parsed_events.parsed_json",
        "delivery_audit.audit_json",
        "telegram_deliveries.message_json",
    ]
    fields_required_for_verified = [
        "signals.signal_id",
        "signals.asset",
        "signals.pair",
        "signals.trade_opportunity_status",
        "signal_features.trade_opportunity_score",
        "outcomes.direction_adjusted_move",
        "opportunity_outcomes.followthrough",
        "opportunity_outcomes.adverse",
        "quality_stats.sample_count",
        "quality_stats.stats_json",
        "asset_market_states.current_state",
        "no_trade_locks.reason",
        "trade_opportunities.status",
        "trade_opportunities.score_components_json",
        "trade_opportunities.history_snapshot_json",
        "trade_opportunities.evidence_json",
    ]
    compact_candidates = [
        item["table"]
        for item in tables
        if bool(item.get("compactable_rows"))
    ]
    total_json_payload = sum(int(item.get("json_payload_size_bytes") or 0) for item in tables)
    estimated_compact_savings = sum(int(item.get("estimated_savings_bytes") or 0) for item in tables)
    estimated_savings_after_backfill = sum(int(item.get("estimated_savings_after_backfill_bytes") or 0) for item in tables)
    payload = {
        **summary,
        "must_keep_full_tables": must_keep_full,
        "long_term_slim_tables": long_term_slim,
        "index_only_tables": index_only,
        "archive_only_full_payload_fields": archive_only_payload_fields,
        "compact_candidate_tables": compact_candidates,
        "safe_prune_or_compact_tables": compact_candidates + ["runs", "schema_meta"],
        "fields_not_recommended_for_db": fields_not_recommended_for_db,
        "fields_required_for_verified_and_score": fields_required_for_verified,
        "total_json_payload_size_bytes": total_json_payload,
        "total_json_payload_size_mb": round(total_json_payload / (1024 * 1024), 4) if total_json_payload else 0.0,
        "estimated_compact_savings_bytes": estimated_compact_savings,
        "estimated_compact_savings_mb": round(estimated_compact_savings / (1024 * 1024), 4) if estimated_compact_savings else 0.0,
        "estimated_savings_after_backfill_bytes": estimated_savings_after_backfill,
        "estimated_savings_after_backfill_mb": round(estimated_savings_after_backfill / (1024 * 1024), 4)
        if estimated_savings_after_backfill
        else 0.0,
        "answers": {
            "must_long_term_retain": must_keep_full,
            "should_keep_slim_or_index_only": long_term_slim + index_only,
            "full_json_archive_only": archive_only_payload_fields,
            "safe_to_prune_or_compact": compact_candidates,
            "fields_not_for_future_import": fields_not_recommended_for_db,
            "fields_required_for_verified": fields_required_for_verified,
        },
    }
    if write_reports:
        payload["report_files"] = write_sqlite_data_value_audit_reports(payload)
    return payload


def db_retention_recommendation() -> dict[str, Any]:
    audit = sqlite_data_value_audit(write_reports=False)
    actions = [
        "signals/outcomes/opportunities/states/quality 保持 full，作为长期分析主数据",
        "raw_events / parsed_events 默认切到 index_only，full payload 交给 .ndjson/.ndjson.gz archive",
        "delivery_audit / telegram_deliveries / market_context_attempts / case_followups 默认 slim，保留路由决策与关键诊断索引，不保留长期 full payload",
        "如果 skipped_no_payload_hash / skipped_no_archive_path 很高，先跑 --backfill-payload-metadata 再 compact",
        "compact 前先确认 archive_path + payload_hash + archive 文件存在；没有备份不清 payload",
        "compact 后如需真正回收磁盘，再单独执行 vacuum",
    ]
    if audit.get("estimated_compact_savings_bytes"):
        actions.append(
            f"当前按推荐模式预计可释放约 {round(float(audit['estimated_compact_savings_bytes']) / (1024 * 1024), 2)} MB 的 JSON payload 空间"
        )
    if audit.get("estimated_savings_after_backfill_bytes"):
        actions.append(
            f"完成 legacy payload metadata backfill 后，预计可进一步释放约 {round(float(audit['estimated_savings_after_backfill_bytes']) / (1024 * 1024), 2)} MB"
        )
    return {
        "db_path": audit.get("db_path"),
        "db_size_bytes": audit.get("db_size_bytes"),
        "wal_size_bytes": audit.get("wal_size_bytes"),
        "current_modes": {
            "raw_events": _mode_for_table("raw_events"),
            "parsed_events": _mode_for_table("parsed_events"),
            "delivery_audit": _mode_for_table("delivery_audit"),
            "telegram_deliveries": _mode_for_table("telegram_deliveries"),
            "market_context_attempts": _mode_for_table("market_context_attempts"),
            "case_followups": _mode_for_table("case_followups"),
            "signals": _mode_for_table("signals"),
            "trade_opportunities": _mode_for_table("trade_opportunities"),
            "outcomes": _mode_for_table("outcomes"),
            "asset_market_states": _mode_for_table("asset_market_states"),
            "quality_stats": _mode_for_table("quality_stats"),
        },
        "recommended_modes": {
            key: value
            for key, value in TABLE_RECOMMENDED_MODES.items()
            if key in {
                "raw_events",
                "parsed_events",
                "delivery_audit",
                "telegram_deliveries",
                "market_context_attempts",
                "case_followups",
                "signals",
                "trade_opportunities",
                "outcomes",
                "asset_market_states",
                "quality_stats",
            }
        },
        "retention_policy": {
            "long_term_full": audit.get("must_keep_full_tables") or [],
            "long_term_slim": audit.get("long_term_slim_tables") or [],
            "index_only": audit.get("index_only_tables") or [],
            "archive_only_full_payload_fields": audit.get("archive_only_full_payload_fields") or [],
        },
        "can_compact": audit.get("compact_candidate_tables") or [],
        "estimated_compact_savings_bytes": audit.get("estimated_compact_savings_bytes"),
        "estimated_compact_savings_mb": audit.get("estimated_compact_savings_mb"),
        "payload_hash_coverage_by_table": audit.get("payload_hash_coverage_by_table") or {},
        "archive_path_coverage_by_table": audit.get("archive_path_coverage_by_table") or {},
        "compactable_rows_by_table": audit.get("compactable_rows_by_table") or {},
        "backfill_needed_rows_by_table": audit.get("backfill_needed_rows_by_table") or {},
        "full_payload_rows_by_table": audit.get("full_payload_rows_by_table") or {},
        "legacy_full_payload_rows_by_table": audit.get("legacy_full_payload_rows_by_table") or {},
        "estimated_savings_after_backfill_bytes": audit.get("estimated_savings_after_backfill_bytes"),
        "estimated_savings_after_backfill_mb": audit.get("estimated_savings_after_backfill_mb"),
        "recommended_actions": actions,
    }


def _write_audit_csv(path: Path, tables: list[dict[str, Any]]) -> None:
    fieldnames = [
        "table",
        "row_count",
        "table_size_bytes",
        "json_payload_size_bytes",
        "full_payload_rows",
        "payload_hash_coverage",
        "archive_path_coverage",
        "compactable_rows",
        "backfill_needed_rows",
        "data_value_class",
        "current_mode",
        "recommended_mode",
        "can_compact",
        "estimated_savings_bytes",
        "critical_for_verified",
        "critical_for_opportunity_score",
        "archive_only_ok",
        "last_updated",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in tables:
            writer.writerow({key: item.get(key) for key in fieldnames})


def _render_audit_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# SQLite Data Value Audit",
        "",
        f"- DB path: `{payload.get('db_path')}`",
        f"- DB size: `{payload.get('db_size_mb')}` MB",
        f"- WAL size: `{payload.get('wal_size_mb')}` MB",
        f"- Total JSON payload: `{payload.get('total_json_payload_size_mb')}` MB",
        f"- Estimated compact savings: `{payload.get('estimated_compact_savings_mb')}` MB",
        f"- Estimated savings after backfill: `{payload.get('estimated_savings_after_backfill_mb')}` MB",
        "",
        "## Long-term Full",
        "",
        ", ".join(payload.get("must_keep_full_tables") or []) or "(none)",
        "",
        "## Long-term Slim",
        "",
        ", ".join(payload.get("long_term_slim_tables") or []) or "(none)",
        "",
        "## Index-only",
        "",
        ", ".join(payload.get("index_only_tables") or []) or "(none)",
        "",
        "## Archive-only Full Payload",
        "",
        ", ".join(payload.get("archive_only_full_payload_fields") or []) or "(none)",
        "",
        "## Table Audit",
        "",
        "| table | rows | full_payload_rows | payload_hash_coverage | archive_path_coverage | compactable_rows | backfill_needed_rows | value_class | current_mode | recommended_mode | json_payload_mb | table_size_mb | can_compact |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |",
    ]
    for item in payload.get("tables") or []:
        lines.append(
            "| {table} | {row_count} | {full_payload_rows} | {payload_hash_coverage:.4f} | {archive_path_coverage:.4f} | {compactable_rows} | {backfill_needed_rows} | {data_value_class} | {current_mode} | {recommended_mode} | {json_payload_size_mb:.4f} | {table_size_mb:.4f} | {can_compact} |".format(
                table=item.get("table"),
                row_count=int(item.get("row_count") or 0),
                full_payload_rows=int(item.get("full_payload_rows") or 0),
                payload_hash_coverage=float(item.get("payload_hash_coverage") or 0.0),
                archive_path_coverage=float(item.get("archive_path_coverage") or 0.0),
                compactable_rows=int(item.get("compactable_rows") or 0),
                backfill_needed_rows=int(item.get("backfill_needed_rows") or 0),
                data_value_class=item.get("data_value_class"),
                current_mode=item.get("current_mode"),
                recommended_mode=item.get("recommended_mode"),
                json_payload_size_mb=float(item.get("json_payload_size_mb") or 0.0),
                table_size_mb=float(item.get("table_size_mb") or 0.0),
                can_compact="yes" if item.get("can_compact") else "no",
            )
        )
    lines.extend(
        [
            "",
            "## Audit Answers",
            "",
            f"- 必须长期保留：{', '.join(payload.get('answers', {}).get('must_long_term_retain') or []) or '(none)'}",
            f"- 应只保留 slim/index：{', '.join(payload.get('answers', {}).get('should_keep_slim_or_index_only') or []) or '(none)'}",
            f"- full JSON 只留 archive：{', '.join(payload.get('answers', {}).get('full_json_archive_only') or []) or '(none)'}",
            f"- 可安全 compact/prune：{', '.join(payload.get('answers', {}).get('safe_to_prune_or_compact') or []) or '(none)'}",
            f"- 不应再导入数据库字段：{', '.join(payload.get('answers', {}).get('fields_not_for_future_import') or []) or '(none)'}",
            f"- 支持 verified/opportunity_score 必留字段：{', '.join(payload.get('answers', {}).get('fields_required_for_verified') or []) or '(none)'}",
            f"- payload_hash 覆盖：{json.dumps(payload.get('payload_hash_coverage_by_table') or {}, ensure_ascii=False, sort_keys=True)}",
            f"- archive_path 覆盖：{json.dumps(payload.get('archive_path_coverage_by_table') or {}, ensure_ascii=False, sort_keys=True)}",
            f"- backfill 需求：{json.dumps(payload.get('backfill_needed_rows_by_table') or {}, ensure_ascii=False, sort_keys=True)}",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sqlite_data_value_audit_reports(payload: dict[str, Any], *, report_dir: str | Path | None = None) -> dict[str, str]:
    root = Path(report_dir or (PROJECT_ROOT / "reports"))
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "sqlite_data_value_audit_latest.json"
    csv_path = root / "sqlite_data_value_audit_latest.csv"
    md_path = root / "sqlite_data_value_audit_latest.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    _write_audit_csv(csv_path, list(payload.get("tables") or []))
    md_path.write_text(_render_audit_markdown(payload), encoding="utf-8")
    return {
        "json": str(json_path),
        "csv": str(csv_path),
        "md": str(md_path),
    }


def _resolve_archive_reference(path_value: str | None) -> Path | None:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _archive_reference_exists(path_value: str | None) -> bool:
    path = _resolve_archive_reference(path_value)
    if path is None:
        return False
    if path.exists():
        return True
    if str(path).endswith(".ndjson"):
        gz = Path(f"{path}.gz")
        return gz.exists()
    if str(path).endswith(".ndjson.gz"):
        plain = Path(str(path)[: -len(".gz")])
        return plain.exists()
    return False


def _resolve_archive_reference_existing_path(path_value: str | None) -> Path | None:
    path = _resolve_archive_reference(path_value)
    if path is None:
        return None
    if path.exists():
        if str(path).endswith(".ndjson.gz"):
            plain = Path(str(path)[: -len(".gz")])
            if plain.exists():
                return plain
        return path
    if str(path).endswith(".ndjson"):
        gz = Path(f"{path}.gz")
        if gz.exists():
            return gz
    if str(path).endswith(".ndjson.gz"):
        plain = Path(str(path)[: -len(".gz")])
        if plain.exists():
            return plain
    return None


def _archive_category_from_path(path: Path | None) -> str:
    if path is None:
        return ""
    return str(path.parent.name or "").strip()


def _archive_date_from_path(path: Path | None) -> str:
    if path is None:
        return ""
    return str(_archive_file_key(path) or "").strip()


def _archive_date_bounds(date_value: str) -> tuple[float, float]:
    bj = timezone(timedelta(hours=8))
    start = datetime.strptime(date_value, "%Y-%m-%d").replace(tzinfo=bj)
    end = start + timedelta(days=1)
    return start.astimezone(timezone.utc).timestamp(), end.astimezone(timezone.utc).timestamp()


def _row_or_payload_value(
    row: dict[str, Any],
    payload_data: dict[str, Any] | None,
    *keys: str,
    default: Any = None,
) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, "", [], {}, ()):
            return value
    if isinstance(payload_data, dict):
        return _first(payload_data, *keys, default=default)
    return default


def _row_payload_data(row: dict[str, Any], json_column: str) -> dict[str, Any] | None:
    value = row.get(json_column)
    parsed = _from_json(value, default=None)
    return parsed if isinstance(parsed, dict) else None


def _row_archive_ts(table: str, row: dict[str, Any], payload_data: dict[str, Any] | None) -> float | None:
    spec = PAYLOAD_METADATA_SPECS.get(table) or {}
    time_column = str(spec.get("time_column") or "")
    time_keys = tuple(spec.get("time_keys") or ())
    return _real(row.get(time_column)) or _real(_row_or_payload_value(row, payload_data, *time_keys))


def _row_archive_date(table: str, row: dict[str, Any], payload_data: dict[str, Any] | None) -> str:
    explicit = str(row.get("archive_date") or "").strip()
    if explicit:
        return explicit
    ts_value = _row_archive_ts(table, row, payload_data)
    return str(_archive_date_from_ts(ts_value) or "").strip()


def _ts_bucket(value: Any, bucket_seconds: int) -> int | None:
    ts_value = _real(value)
    if ts_value is None:
        return None
    seconds = max(int(bucket_seconds or 0), 1)
    return int(ts_value) // seconds


def _archive_entry_payload(data: Any) -> dict[str, Any]:
    return dict(data) if isinstance(data, dict) else {}


def _build_archive_index_entry(
    *,
    category: str,
    archive_date: str,
    archive_path: Path,
    archive_payload: dict[str, Any],
) -> dict[str, Any]:
    data = _archive_entry_payload(archive_payload.get("data") if isinstance(archive_payload, dict) else archive_payload)
    archive_ts = _real(archive_payload.get("archive_ts") if isinstance(archive_payload, dict) else None)
    timestamp_value = (
        _real(_first(data, "timestamp", "archive_written_at", "notifier_sent_at", "sent_at", "ts", "created_at"))
        or archive_ts
    )
    event_id = str(_first(data, "event_id") or "").strip()
    tx_hash = str(_first(data, "tx_hash") or "").strip()
    block_number = _int(_first(data, "block_number", "block"))
    parsed_kind = str(_first(data, "parsed_kind", "intent_type", "source_kind") or "").strip()
    signal_id = str(_first(data, "signal_id", "signal_archive_key") or "").strip()
    delivery_decision = str(_first(data, "delivery_decision", "delivery_reason", "delivery_class") or "").strip()
    sent_to_telegram = _bool_int(_first(data, "sent_to_telegram", "delivered_notification", "delivered"))
    telegram_delivery_id = str(_first(data, "telegram_delivery_id") or "").strip()
    headline = str(_first(data, "headline", "final_trading_output_label", "headline_label", "market_state_label") or "").strip()
    audit_id = str(_first(data, "audit_id", "delivery_audit_id") or "").strip()
    case_id = str(_first(data, "case_id", "asset_case_id") or "").strip()
    followup_id = str(_first(data, "followup_id") or "").strip()
    bucket_60 = _ts_bucket(timestamp_value, 60)
    bucket_300 = _ts_bucket(timestamp_value, 300)
    return {
        "archive_category": category,
        "archive_date": archive_date,
        "archive_path": str(archive_path.resolve()),
        "archive_ts": archive_ts,
        "payload_hash": stable_payload_hash(data),
        "event_id": event_id,
        "tx_hash": tx_hash,
        "block_number": block_number,
        "parsed_kind": parsed_kind,
        "signal_id": signal_id,
        "delivery_decision": delivery_decision,
        "sent_to_telegram": sent_to_telegram,
        "timestamp": timestamp_value,
        "telegram_delivery_id": telegram_delivery_id,
        "headline": headline,
        "audit_id": audit_id,
        "case_id": case_id,
        "followup_id": followup_id,
        "bucket_60": bucket_60,
        "bucket_300": bucket_300,
    }


def _load_archive_payload_index(
    category: str,
    archive_date: str,
    cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    key = (str(category), str(archive_date))
    cached = cache.get(key)
    if cached is not None:
        return cached
    paths = _archive_payload_paths(category, date=archive_date)
    if not paths:
        payload = {
            "exists": False,
            "archive_category": str(category),
            "archive_date": str(archive_date),
            "archive_path": "",
            "entries": [],
            "by_event_id": {},
            "by_tx_block": {},
            "by_tx_kind": {},
            "by_signal_delivery_bucket": {},
            "by_signal_bucket": {},
            "by_signal_headline_bucket": {},
            "by_signal_id": {},
            "by_audit_id": {},
            "by_telegram_delivery_id": {},
            "by_followup_id": {},
            "by_signal_case_bucket": {},
            "by_case_bucket": {},
        }
        cache[key] = payload
        return payload
    path = paths[0]
    payload = {
        "exists": True,
        "archive_category": str(category),
        "archive_date": str(archive_date),
        "archive_path": str(path.resolve()),
        "entries": [],
        "by_event_id": {},
        "by_tx_block": {},
        "by_tx_kind": {},
        "by_signal_delivery_bucket": {},
        "by_signal_bucket": {},
        "by_signal_headline_bucket": {},
        "by_signal_id": {},
        "by_audit_id": {},
        "by_telegram_delivery_id": {},
        "by_followup_id": {},
        "by_signal_case_bucket": {},
        "by_case_bucket": {},
    }

    def _add(index_name: str, index_key: Any, entry_id: int) -> None:
        if index_key in (None, "", [], {}, ()):
            return
        bucket = payload[index_name].setdefault(index_key, [])
        bucket.append(entry_id)

    try:
        with _open_archive_payload_path(path) as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    archive_payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                entry = _build_archive_index_entry(
                    category=str(category),
                    archive_date=str(archive_date),
                    archive_path=path,
                    archive_payload=archive_payload if isinstance(archive_payload, dict) else {},
                )
                entry_id = len(payload["entries"])
                payload["entries"].append(entry)
                _add("by_event_id", entry["event_id"], entry_id)
                if entry["tx_hash"] and entry["block_number"] is not None:
                    _add("by_tx_block", (entry["tx_hash"], int(entry["block_number"])), entry_id)
                if entry["tx_hash"] and entry["parsed_kind"]:
                    _add("by_tx_kind", (entry["tx_hash"], entry["parsed_kind"]), entry_id)
                if entry["signal_id"] and entry["delivery_decision"] and entry["bucket_60"] is not None:
                    _add(
                        "by_signal_delivery_bucket",
                        (entry["signal_id"], entry["delivery_decision"], int(entry["bucket_60"])),
                        entry_id,
                    )
                if entry["signal_id"] and entry["bucket_60"] is not None:
                    _add("by_signal_bucket", (entry["signal_id"], int(entry["bucket_60"])), entry_id)
                if entry["signal_id"]:
                    _add("by_signal_id", entry["signal_id"], entry_id)
                if entry["signal_id"] and entry["headline"] and entry["bucket_60"] is not None:
                    _add(
                        "by_signal_headline_bucket",
                        (entry["signal_id"], entry["headline"], int(entry["bucket_60"])),
                        entry_id,
                    )
                _add("by_audit_id", entry["audit_id"], entry_id)
                _add("by_telegram_delivery_id", entry["telegram_delivery_id"], entry_id)
                _add("by_followup_id", entry["followup_id"], entry_id)
                if entry["signal_id"] and entry["case_id"] and entry["bucket_300"] is not None:
                    _add(
                        "by_signal_case_bucket",
                        (entry["signal_id"], entry["case_id"], int(entry["bucket_300"])),
                        entry_id,
                    )
                if entry["case_id"] and entry["bucket_300"] is not None:
                    _add("by_case_bucket", (entry["case_id"], int(entry["bucket_300"])), entry_id)
    except OSError:
        payload["exists"] = False
        payload["archive_path"] = ""
        payload["entries"] = []
    cache[key] = payload
    return payload


def _dedupe_int_list(values: list[int]) -> list[int]:
    seen: set[int] = set()
    ordered: list[int] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _candidate_entry_ids_for_table(
    table: str,
    row: dict[str, Any],
    payload_data: dict[str, Any] | None,
    archive_index: dict[str, Any],
) -> list[int]:
    candidates: list[int] = []
    bucket_seconds = int((PAYLOAD_METADATA_SPECS.get(table) or {}).get("bucket_seconds") or 60)
    match_bucket = _ts_bucket(_row_archive_ts(table, row, payload_data), bucket_seconds)
    event_id = str(_row_or_payload_value(row, payload_data, "event_id") or "").strip()
    tx_hash = str(_row_or_payload_value(row, payload_data, "tx_hash") or "").strip()
    block_number = _int(_row_or_payload_value(row, payload_data, "block_number", "block"))
    parsed_kind = str(_row_or_payload_value(row, payload_data, "parsed_kind", "intent_type", "source_kind") or "").strip()
    signal_id = str(_row_or_payload_value(row, payload_data, "signal_id", "signal_archive_key") or "").strip()
    delivery_decision = str(_row_or_payload_value(row, payload_data, "delivery_decision", "delivery_reason", "delivery_class") or "").strip()
    headline = str(_row_or_payload_value(row, payload_data, "headline", "final_trading_output_label", "headline_label", "market_state_label") or "").strip()
    audit_id = str(_row_or_payload_value(row, payload_data, "audit_id", "delivery_audit_id") or "").strip()
    telegram_delivery_id = str(_row_or_payload_value(row, payload_data, "telegram_delivery_id") or "").strip()
    case_id = str(_row_or_payload_value(row, payload_data, "case_id", "asset_case_id") or "").strip()
    followup_id = str(_row_or_payload_value(row, payload_data, "followup_id") or "").strip()

    if table == "raw_events":
        if event_id:
            candidates.extend(archive_index["by_event_id"].get(event_id, []))
        if not candidates and tx_hash and block_number is not None:
            candidates.extend(archive_index["by_tx_block"].get((tx_hash, int(block_number)), []))
    elif table == "parsed_events":
        if event_id:
            candidates.extend(archive_index["by_event_id"].get(event_id, []))
        if not candidates and tx_hash and parsed_kind:
            candidates.extend(archive_index["by_tx_kind"].get((tx_hash, parsed_kind), []))
    elif table == "delivery_audit":
        if audit_id:
            candidates.extend(archive_index["by_audit_id"].get(audit_id, []))
        if not candidates and signal_id and delivery_decision and match_bucket is not None:
            candidates.extend(
                archive_index["by_signal_delivery_bucket"].get((signal_id, delivery_decision, int(match_bucket)), [])
            )
        if not candidates and signal_id and match_bucket is not None:
            candidates.extend(archive_index["by_signal_bucket"].get((signal_id, int(match_bucket)), []))
    elif table == "telegram_deliveries":
        if telegram_delivery_id:
            candidates.extend(archive_index["by_telegram_delivery_id"].get(telegram_delivery_id, []))
        if not candidates and signal_id and headline and match_bucket is not None:
            candidates.extend(
                archive_index["by_signal_headline_bucket"].get((signal_id, headline, int(match_bucket)), [])
            )
        if not candidates and signal_id and match_bucket is not None:
            candidates.extend(archive_index["by_signal_bucket"].get((signal_id, int(match_bucket)), []))
    elif table == "case_followups":
        if followup_id:
            candidates.extend(archive_index["by_followup_id"].get(followup_id, []))
        if not candidates and signal_id and case_id and match_bucket is not None:
            candidates.extend(
                archive_index["by_signal_case_bucket"].get((signal_id, case_id, int(match_bucket)), [])
            )
        if not candidates and case_id and match_bucket is not None:
            candidates.extend(archive_index["by_case_bucket"].get((case_id, int(match_bucket)), []))
    return _dedupe_int_list(candidates)


def _candidate_archive_categories_for_row(
    table: str,
    payload_data: dict[str, Any] | None,
) -> tuple[str, ...]:
    spec = PAYLOAD_METADATA_SPECS.get(table) or {}
    categories = [str(item) for item in tuple(spec.get("archive_categories") or ())]
    if table != "telegram_deliveries" or not isinstance(payload_data, dict):
        return tuple(categories)
    preferred = _telegram_archive_category(payload_data)
    ordered: list[str] = []
    if preferred in categories:
        ordered.append(preferred)
    ordered.extend(item for item in categories if item not in ordered)
    return tuple(ordered)


def _resolve_payload_archive_match(
    table: str,
    row: dict[str, Any],
    *,
    payload_data: dict[str, Any] | None,
    archive_cache: dict[tuple[str, str], dict[str, Any]],
    exact_archive_path: str | None = None,
) -> dict[str, Any]:
    spec = PAYLOAD_METADATA_SPECS.get(table) or {}
    json_column = str(spec.get("json_column") or "")
    payload_value = row.get(json_column)
    payload_hash = stable_payload_hash(payload_data if payload_data is not None else payload_value)
    stored_hash = str(row.get("payload_hash") or "").strip()
    stored_archive_path = str(row.get("archive_path") or "").strip()
    stored_archive_date = str(row.get("archive_date") or "").strip()
    desired_mode = _mode_for_table(table)
    result = {
        "status": "unresolved",
        "reason": "",
        "payload_hash": payload_hash,
        "archive_path": "",
        "archive_date": "",
        "archive_matched": False,
    }
    if payload_hash is None:
        result["reason"] = "payload_hash_compute_failed"
        return result

    categories: tuple[str, ...]
    archive_date = stored_archive_date or _row_archive_date(table, row, payload_data)
    if exact_archive_path:
        resolved_path = _resolve_archive_reference_existing_path(exact_archive_path)
        if resolved_path is None:
            result["status"] = "archive_missing"
            result["reason"] = "archive_file_missing"
            return result
        category = _archive_category_from_path(resolved_path)
        archive_date = _archive_date_from_path(resolved_path) or archive_date
        if not category or not archive_date:
            result["reason"] = "invalid_archive_reference"
            return result
        categories = (category,)
    else:
        categories = _candidate_archive_categories_for_row(table, payload_data)

    if not archive_date:
        result["reason"] = "missing_archive_date"
        return result

    matching_entries: list[dict[str, Any]] = []
    candidate_entries: list[dict[str, Any]] = []
    archive_exists = False
    for category in categories:
        archive_index = _load_archive_payload_index(category, archive_date, archive_cache)
        if not archive_index.get("exists"):
            continue
        archive_exists = True
        entry_ids = _candidate_entry_ids_for_table(table, row, payload_data, archive_index)
        for entry_id in entry_ids:
            entry = archive_index["entries"][entry_id]
            candidate_entries.append(entry)
            if entry.get("payload_hash") == payload_hash:
                matching_entries.append(entry)
    if not archive_exists:
        result["status"] = "archive_missing"
        result["reason"] = "archive_file_missing"
        return result
    if not candidate_entries:
        result["reason"] = "no_identity_match"
        return result

    result["archive_matched"] = True
    if stored_hash and stored_hash != payload_hash:
        result["status"] = "hash_mismatch"
        result["reason"] = "stored_payload_hash_mismatch"
        return result
    if not matching_entries:
        result["status"] = "hash_mismatch"
        result["reason"] = "archive_payload_hash_mismatch"
        return result

    distinct_matches: list[dict[str, Any]] = []
    seen_matches: set[tuple[str, str, str]] = set()
    for entry in matching_entries:
        key = (
            str(entry.get("archive_path") or ""),
            str(entry.get("archive_date") or ""),
            str(entry.get("payload_hash") or ""),
        )
        if key in seen_matches:
            continue
        seen_matches.add(key)
        distinct_matches.append(entry)
    if len(distinct_matches) != 1:
        result["reason"] = "ambiguous_archive_match"
        return result

    match = distinct_matches[0]
    resolved_archive_path = str(match.get("archive_path") or "")
    resolved_archive_date = str(match.get("archive_date") or archive_date)
    normalized_stored_archive_path = ""
    if stored_archive_path:
        normalized_stored_archive_path = str(
            (
                _resolve_archive_reference_existing_path(stored_archive_path)
                or _resolve_archive_reference(stored_archive_path)
                or Path(stored_archive_path)
            ).resolve()
        )
    if stored_archive_path and normalized_stored_archive_path != resolved_archive_path:
        result["reason"] = "conflicting_archive_path"
        return result
    if stored_archive_date and stored_archive_date != resolved_archive_date:
        result["reason"] = "conflicting_archive_date"
        return result

    result.update(
        {
            "status": "matched",
            "reason": "matched",
            "archive_path": resolved_archive_path,
            "archive_date": resolved_archive_date,
            "payload_mode": desired_mode,
        }
    )
    return result


def _payload_row_query(table: str, *, date: str | None = None) -> tuple[str, tuple[Any, ...]]:
    spec = PAYLOAD_METADATA_SPECS.get(table) or {}
    json_column = str(spec.get("json_column") or "")
    time_column = str(spec.get("time_column") or "")
    selected_columns = [
        "rowid",
        json_column,
        f"LENGTH({json_column}) AS payload_bytes",
        "archive_path",
        "archive_date",
        "payload_hash",
        "payload_mode",
    ]
    for column in tuple(spec.get("identity_columns") or ()):
        if column not in selected_columns:
            selected_columns.append(column)
    where = [f"{json_column} IS NOT NULL", f"{json_column} != ''"]
    params: list[Any] = []
    if date and time_column:
        start_ts, end_ts = _archive_date_bounds(date)
        where.append(f"{time_column} >= ?")
        where.append(f"{time_column} < ?")
        params.extend([start_ts, end_ts])
    sql = f"SELECT {', '.join(selected_columns)} FROM {table} WHERE {' AND '.join(where)}"
    return sql, tuple(params)


def _compactable_tables() -> tuple[str, ...]:
    return tuple(PAYLOAD_METADATA_SPECS.keys())


def _backfill_table(table: str, *, dry_run: bool = True, date: str | None = None) -> dict[str, Any]:
    conn = get_connection()
    if conn is None:
        return {"ok": False, "reason": _INIT_FAILED_REASON or "not_initialized", "table": table}
    if table not in PAYLOAD_METADATA_SPECS:
        return {"ok": False, "reason": "table_not_supported", "table": table}
    spec = PAYLOAD_METADATA_SPECS[table]
    json_column = str(spec["json_column"])
    total_rows = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    sql, params = _payload_row_query(table, date=date)
    cursor = conn.execute(sql, params)
    archive_cache: dict[tuple[str, str], dict[str, Any]] = {}
    updates: list[tuple[str, str, str, str, int]] = []
    unresolved_reasons: Counter[str] = Counter()
    result = {
        "ok": True,
        "table": table,
        "dry_run": bool(dry_run),
        "date": date,
        "rows_total": total_rows,
        "rows_examined": 0,
        "rows_with_full_payload": 0,
        "rows_missing_payload_hash": 0,
        "rows_backfillable": 0,
        "rows_backfilled": 0,
        "rows_archive_matched": 0,
        "rows_archive_missing": 0,
        "rows_hash_mismatch": 0,
        "rows_unresolved": 0,
        "rows_already_compactable": 0,
        "estimated_compactable_after_backfill": 0,
        "estimated_bytes_freed_after_compact": 0,
        "estimated_mb_freed_after_compact": 0.0,
        "unresolved_reasons": {},
    }
    for raw_row in cursor:
        row = dict(raw_row)
        payload_value = row.get(json_column)
        if payload_value in (None, ""):
            continue
        result["rows_examined"] += 1
        result["rows_with_full_payload"] += 1
        payload_bytes = int(row.get("payload_bytes") or 0)
        payload_data = _row_payload_data(row, json_column)
        stored_hash = str(row.get("payload_hash") or "").strip()
        stored_archive_path = str(row.get("archive_path") or "").strip()
        stored_archive_date = str(row.get("archive_date") or "").strip()
        stored_payload_mode = str(row.get("payload_mode") or "").strip()
        needs_backfill = not (stored_hash and stored_archive_path and stored_archive_date and stored_payload_mode)
        if not stored_hash:
            result["rows_missing_payload_hash"] += 1

        match = _resolve_payload_archive_match(
            table,
            row,
            payload_data=payload_data,
            archive_cache=archive_cache,
        )
        if match.get("archive_matched"):
            result["rows_archive_matched"] += 1
        status = str(match.get("status") or "")
        if status == "archive_missing":
            result["rows_archive_missing"] += 1
            unresolved_reasons[str(match.get("reason") or "archive_file_missing")] += 1
            continue
        if status == "hash_mismatch":
            result["rows_hash_mismatch"] += 1
            unresolved_reasons[str(match.get("reason") or "hash_mismatch")] += 1
            continue
        if status != "matched":
            result["rows_unresolved"] += 1
            unresolved_reasons[str(match.get("reason") or "unresolved")] += 1
            continue

        if not needs_backfill:
            result["rows_already_compactable"] += 1
            result["estimated_bytes_freed_after_compact"] += payload_bytes
            continue

        result["rows_backfillable"] += 1
        result["estimated_bytes_freed_after_compact"] += payload_bytes
        updates.append(
            (
                str(match.get("archive_path") or ""),
                str(match.get("archive_date") or ""),
                str(match.get("payload_hash") or ""),
                str(match.get("payload_mode") or _mode_for_table(table)),
                int(row["rowid"]),
            )
        )
    if not dry_run and updates:
        conn.executemany(
            f"UPDATE {table} SET archive_path=?, archive_date=?, payload_hash=?, payload_mode=? WHERE rowid=?",
            tuple(updates),
        )
        conn.commit()
        result["rows_backfilled"] = len(updates)
    result["estimated_compactable_after_backfill"] = result["rows_already_compactable"] + result["rows_backfillable"]
    result["estimated_mb_freed_after_compact"] = round(
        int(result["estimated_bytes_freed_after_compact"] or 0) / (1024 * 1024),
        4,
    )
    result["unresolved_reasons"] = dict(sorted(unresolved_reasons.items()))
    return result


def backfill_payload_metadata(
    *,
    dry_run: bool = True,
    table: str | None = None,
    date: str | None = None,
) -> dict[str, Any]:
    target_tables = [table] if table else list(_compactable_tables())
    summaries = [_backfill_table(name, dry_run=dry_run, date=date) for name in target_tables]
    return {
        "ok": all(bool(item.get("ok")) for item in summaries),
        "dry_run": bool(dry_run),
        "date": date,
        "tables": {str(item.get("table")): item for item in summaries},
        "rows_examined": sum(int(item.get("rows_examined") or 0) for item in summaries),
        "rows_with_full_payload": sum(int(item.get("rows_with_full_payload") or 0) for item in summaries),
        "rows_missing_payload_hash": sum(int(item.get("rows_missing_payload_hash") or 0) for item in summaries),
        "rows_backfillable": sum(int(item.get("rows_backfillable") or 0) for item in summaries),
        "rows_backfilled": sum(int(item.get("rows_backfilled") or 0) for item in summaries),
        "rows_archive_matched": sum(int(item.get("rows_archive_matched") or 0) for item in summaries),
        "rows_archive_missing": sum(int(item.get("rows_archive_missing") or 0) for item in summaries),
        "rows_hash_mismatch": sum(int(item.get("rows_hash_mismatch") or 0) for item in summaries),
        "rows_unresolved": sum(int(item.get("rows_unresolved") or 0) for item in summaries),
        "estimated_compactable_after_backfill": sum(
            int(item.get("estimated_compactable_after_backfill") or 0) for item in summaries
        ),
        "estimated_bytes_freed_after_compact": sum(
            int(item.get("estimated_bytes_freed_after_compact") or 0) for item in summaries
        ),
        "estimated_mb_freed_after_compact": round(
            sum(int(item.get("estimated_bytes_freed_after_compact") or 0) for item in summaries) / (1024 * 1024),
            4,
        ),
    }


def _payload_metadata_stats_for_table(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    spec = PAYLOAD_METADATA_SPECS.get(table)
    if spec is None:
        return {
            "full_payload_rows": 0,
            "payload_hash_covered_rows": 0,
            "archive_path_covered_rows": 0,
            "archive_date_covered_rows": 0,
            "payload_mode_covered_rows": 0,
            "backfill_needed_rows": 0,
            "legacy_full_payload_rows": 0,
            "compactable_rows": 0,
            "estimated_savings_after_backfill_bytes": 0,
            "estimated_savings_after_backfill_mb": 0.0,
        }
    json_column = str(spec["json_column"])
    try:
        row = conn.execute(
            f"""
            SELECT
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' THEN 1 ELSE 0 END) AS full_payload_rows,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND payload_hash IS NOT NULL AND TRIM(payload_hash) != '' THEN 1 ELSE 0 END) AS payload_hash_covered_rows,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND archive_path IS NOT NULL AND TRIM(archive_path) != '' THEN 1 ELSE 0 END) AS archive_path_covered_rows,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND archive_date IS NOT NULL AND TRIM(archive_date) != '' THEN 1 ELSE 0 END) AS archive_date_covered_rows,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND payload_mode IS NOT NULL AND TRIM(payload_mode) != '' THEN 1 ELSE 0 END) AS payload_mode_covered_rows,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND (
                    payload_hash IS NULL OR TRIM(payload_hash) = '' OR
                    archive_path IS NULL OR TRIM(archive_path) = '' OR
                    archive_date IS NULL OR TRIM(archive_date) = '' OR
                    payload_mode IS NULL OR TRIM(payload_mode) = ''
                ) THEN 1 ELSE 0 END) AS backfill_needed_rows,
                COALESCE(SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND (
                    payload_hash IS NULL OR TRIM(payload_hash) = '' OR
                    archive_path IS NULL OR TRIM(archive_path) = ''
                ) THEN LENGTH({json_column}) ELSE 0 END), 0) AS backfill_needed_payload_bytes,
                COALESCE(SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND
                    payload_hash IS NOT NULL AND TRIM(payload_hash) != '' AND
                    archive_path IS NOT NULL AND TRIM(archive_path) != '' AND
                    archive_date IS NOT NULL AND TRIM(archive_date) != ''
                THEN LENGTH({json_column}) ELSE 0 END), 0) AS compactable_payload_bytes,
                SUM(CASE WHEN {json_column} IS NOT NULL AND {json_column} != '' AND
                    payload_hash IS NOT NULL AND TRIM(payload_hash) != '' AND
                    archive_path IS NOT NULL AND TRIM(archive_path) != '' AND
                    archive_date IS NOT NULL AND TRIM(archive_date) != ''
                THEN 1 ELSE 0 END) AS compactable_rows
            FROM {table}
            """
        ).fetchone()
    except sqlite3.Error:
        row = None
    full_payload_rows = int(row["full_payload_rows"] or 0) if row else 0
    payload_hash_covered_rows = int(row["payload_hash_covered_rows"] or 0) if row else 0
    archive_path_covered_rows = int(row["archive_path_covered_rows"] or 0) if row else 0
    archive_date_covered_rows = int(row["archive_date_covered_rows"] or 0) if row else 0
    payload_mode_covered_rows = int(row["payload_mode_covered_rows"] or 0) if row else 0
    backfill_needed_rows = int(row["backfill_needed_rows"] or 0) if row else 0
    compactable_rows = int(row["compactable_rows"] or 0) if row else 0
    backfill_needed_bytes = int(row["backfill_needed_payload_bytes"] or 0) if row else 0
    compactable_bytes = int(row["compactable_payload_bytes"] or 0) if row else 0
    estimated_bytes = int(backfill_needed_bytes + compactable_bytes)
    return {
        "full_payload_rows": full_payload_rows,
        "payload_hash_covered_rows": payload_hash_covered_rows,
        "archive_path_covered_rows": archive_path_covered_rows,
        "archive_date_covered_rows": archive_date_covered_rows,
        "payload_mode_covered_rows": payload_mode_covered_rows,
        "backfill_needed_rows": backfill_needed_rows,
        "legacy_full_payload_rows": backfill_needed_rows,
        "compactable_rows": compactable_rows,
        "payload_hash_coverage": round(payload_hash_covered_rows / full_payload_rows, 4) if full_payload_rows else 1.0,
        "archive_path_coverage": round(archive_path_covered_rows / full_payload_rows, 4) if full_payload_rows else 1.0,
        "compactable_payload_bytes": compactable_bytes,
        "backfill_needed_payload_bytes": backfill_needed_bytes,
        "estimated_savings_after_backfill_bytes": estimated_bytes,
        "estimated_savings_after_backfill_mb": round(estimated_bytes / (1024 * 1024), 4) if estimated_bytes else 0.0,
    }


def compact_table(table: str, *, dry_run: bool = True) -> dict[str, Any]:
    conn = get_connection()
    if conn is None:
        return {"ok": False, "reason": _INIT_FAILED_REASON or "not_initialized", "table": table}
    specs = PAYLOAD_METADATA_SPECS
    if table not in specs:
        return {"ok": False, "reason": "table_not_compactable", "table": table}
    spec = specs[table]
    columns = _table_columns(conn, table)
    json_column = str(spec["json_column"])
    if json_column not in columns:
        return {"ok": False, "reason": "json_column_missing", "table": table}
    total_rows = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    selected_columns = [
        "rowid",
        f"LENGTH({json_column}) AS payload_bytes",
        "archive_path",
        "archive_date",
        "payload_hash",
        "payload_mode",
    ]
    for column in tuple(spec.get("identity_columns") or ()):
        if column not in selected_columns:
            selected_columns.append(column)
    sql = f"SELECT {', '.join(selected_columns)} FROM {table} WHERE {json_column} IS NOT NULL AND {json_column} != ''"
    params: tuple[Any, ...] = ()
    cursor = conn.execute(sql, params)
    result = {
        "ok": True,
        "table": table,
        "dry_run": bool(dry_run),
        "rows_total": total_rows,
        "rows_examined": 0,
        "rows_compacted": 0,
        "skipped_no_archive_path": 0,
        "skipped_archive_missing": 0,
        "skipped_missing_archive": 0,
        "skipped_no_payload_hash": 0,
        "skipped_hash_mismatch": 0,
        "skipped_unresolved": 0,
        "skipped_payload_already_empty": 0,
        "estimated_bytes_freed": 0,
        "actual_bytes_freed_after_vacuum": None,
        "backfillable_hint": "",
        "estimated_after_backfill_compactable_rows": 0,
    }
    updates: list[int] = []
    archive_cache: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_row in cursor:
        row = dict(raw_row)
        result["rows_examined"] += 1
        payload_hash = str(row.get("payload_hash") or "").strip()
        archive_path = str(row.get("archive_path") or "").strip()
        payload_bytes = int(row.get("payload_bytes") or 0)
        if not payload_hash:
            result["skipped_no_payload_hash"] += 1
            continue
        if not archive_path:
            result["skipped_no_archive_path"] += 1
            continue
        payload_row = conn.execute(
            f"SELECT {json_column} FROM {table} WHERE rowid=?",
            (int(row["rowid"]),),
        ).fetchone()
        payload = payload_row[json_column] if payload_row is not None else None
        if payload in (None, ""):
            result["skipped_unresolved"] += 1
            continue
        row[json_column] = payload
        payload_data = _row_payload_data(row, json_column)
        match = _resolve_payload_archive_match(
            table,
            row,
            payload_data=payload_data,
            archive_cache=archive_cache,
            exact_archive_path=archive_path,
        )
        status = str(match.get("status") or "")
        if status == "archive_missing":
            result["skipped_archive_missing"] += 1
            continue
        if status == "hash_mismatch":
            result["skipped_hash_mismatch"] += 1
            continue
        if status != "matched":
            result["skipped_unresolved"] += 1
            continue
        result["estimated_bytes_freed"] += payload_bytes
        updates.append(int(row["rowid"]))
    if not dry_run and updates:
        placeholders = ", ".join("?" for _ in updates)
        conn.execute(
            f"UPDATE {table} SET {json_column}=NULL WHERE rowid IN ({placeholders})",
            tuple(updates),
        )
        conn.commit()
    result["skipped_payload_already_empty"] = max(total_rows - int(result["rows_examined"] or 0), 0)
    result["rows_compacted"] = len(updates)
    result["skipped_missing_archive"] = result["skipped_archive_missing"]
    result["estimated_after_backfill_compactable_rows"] = int(result["rows_compacted"] or 0)
    if dry_run and (result["skipped_no_payload_hash"] or result["skipped_no_archive_path"]):
        metadata_stats = _payload_metadata_stats_for_table(conn, table)
        result["estimated_after_backfill_compactable_rows"] = int(result["rows_compacted"] or 0) + int(
            metadata_stats.get("backfill_needed_rows") or 0
        )
        if int(metadata_stats.get("backfill_needed_rows") or 0) > 0:
            result["backfillable_hint"] = "Run --backfill-payload-metadata before compact."
    return result


def compact(*, dry_run: bool = True, table: str | None = None) -> dict[str, Any]:
    if not bool(SQLITE_COMPACT_ENABLE):
        return {"ok": False, "reason": "sqlite_compact_disabled"}
    target_tables = [table] if table else list(_compactable_tables())
    summaries = [compact_table(name, dry_run=dry_run) for name in target_tables]
    tables_compacted = [str(item.get("table")) for item in summaries if int(item.get("rows_compacted") or 0) > 0]
    backfill_recommended = any(str(item.get("backfillable_hint") or "").strip() for item in summaries)
    return {
        "ok": all(bool(item.get("ok")) for item in summaries),
        "dry_run": bool(dry_run),
        "tables": {str(item.get("table")): item for item in summaries},
        "tables_compacted": tables_compacted,
        "rows_examined": sum(int(item.get("rows_examined") or 0) for item in summaries),
        "rows_compacted": sum(int(item.get("rows_compacted") or 0) for item in summaries),
        "skipped_no_archive_path": sum(int(item.get("skipped_no_archive_path") or 0) for item in summaries),
        "skipped_archive_missing": sum(int(item.get("skipped_archive_missing") or 0) for item in summaries),
        "skipped_missing_archive": sum(int(item.get("skipped_archive_missing") or 0) for item in summaries),
        "skipped_no_payload_hash": sum(int(item.get("skipped_no_payload_hash") or 0) for item in summaries),
        "skipped_hash_mismatch": sum(int(item.get("skipped_hash_mismatch") or 0) for item in summaries),
        "skipped_unresolved": sum(int(item.get("skipped_unresolved") or 0) for item in summaries),
        "skipped_payload_already_empty": sum(int(item.get("skipped_payload_already_empty") or 0) for item in summaries),
        "estimated_bytes_freed": sum(int(item.get("estimated_bytes_freed") or 0) for item in summaries),
        "estimated_mb_freed": round(sum(int(item.get("estimated_bytes_freed") or 0) for item in summaries) / (1024 * 1024), 4),
        "backfillable_hint": "Run --backfill-payload-metadata before compact." if backfill_recommended else "",
        "estimated_after_backfill_compactable_rows": sum(
            int(item.get("estimated_after_backfill_compactable_rows") or 0) for item in summaries
        ),
        "vacuum_recommended": True,
    }


def vacuum_database(*, confirm: bool = False, db_path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_db_path(db_path)
    if not confirm:
        return {
            "ok": False,
            "reason": "confirm_required",
            "db_path": str(resolved),
        }
    before = db_file_sizes(resolved)
    close()
    conn = sqlite3.connect(str(resolved))
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()
    after = db_file_sizes(resolved)
    return {
        "ok": True,
        "db_path": str(resolved),
        "before": before,
        "after": after,
        "actual_bytes_freed": int(before.get("db_size_bytes") or 0) - int(after.get("db_size_bytes") or 0),
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="chain-monitor SQLite mirror store")
    parser.add_argument("--init", action="store_true", help="initialize SQLite schema")
    parser.add_argument("--summary", action="store_true", help="print table row counts and health")
    parser.add_argument("--table-size-summary", action="store_true", help="print SQLite size / table size summary")
    parser.add_argument("--data-value-audit", action="store_true", help="print SQLite data value audit and write reports")
    parser.add_argument("--migrate-archive", action="store_true", help="mirror archive NDJSON into SQLite")
    parser.add_argument("--date", help="archive date YYYY-MM-DD for --migrate-archive")
    parser.add_argument("--all", action="store_true", help="migrate all archive dates")
    parser.add_argument("--table", help="table filter for payload metadata backfill")
    parser.add_argument("--integrity-check", action="store_true", help="run schema/count/integrity checks")
    parser.add_argument("--checkpoint", action="store_true", help="run SQLite WAL checkpoint truncate")
    parser.add_argument("--prune", action="store_true", help="show or execute retention prune")
    parser.add_argument("--compact", action="store_true", help="dry-run or execute compact on compactable tables")
    parser.add_argument("--compact-table", help="dry-run or execute compact on one table")
    parser.add_argument("--backfill-payload-metadata", action="store_true", help="backfill legacy archive_path/archive_date/payload_hash/payload_mode")
    parser.add_argument("--vacuum", action="store_true", help="run VACUUM only with CONFIRM=YES")
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
    if args.table_size_summary:
        init_sqlite_store()
        _print_json(table_size_summary())
        return 0
    if args.data_value_audit:
        init_sqlite_store()
        _print_json(sqlite_data_value_audit(write_reports=True))
        return 0
    if args.integrity_check:
        init_sqlite_store()
        payload = integrity_check()
        _print_json(payload)
        return 0 if payload.get("ok") else 1
    if args.checkpoint:
        payload = checkpoint()
        _print_json(payload)
        return 0 if payload.get("success") else 1
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
    if args.backfill_payload_metadata:
        init_sqlite_store()
        dry_run = not bool(args.execute)
        if args.dry_run:
            dry_run = True
        payload = backfill_payload_metadata(dry_run=dry_run, table=args.table, date=args.date)
        _print_json(payload)
        return 0 if payload.get("ok") else 1
    if args.compact or args.compact_table:
        init_sqlite_store()
        dry_run = bool(SQLITE_COMPACT_DRY_RUN_DEFAULT)
        if args.execute:
            dry_run = False
        elif args.dry_run:
            dry_run = True
        payload = compact(dry_run=dry_run, table=args.compact_table if args.compact_table else None)
        _print_json(payload)
        return 0 if payload.get("ok") else 1
    if args.vacuum:
        confirm = str(os.environ.get("CONFIRM") or "").strip().upper() == "YES"
        payload = vacuum_database(confirm=confirm)
        _print_json(payload)
        return 0 if payload.get("ok") else 1
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
