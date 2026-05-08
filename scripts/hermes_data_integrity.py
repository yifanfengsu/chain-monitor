#!/usr/bin/env python3
"""Read-only data integrity check for one Beijing logical day."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import gzip
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any


BJ_TZ = dt.timezone(dt.timedelta(hours=8))
UTC = dt.timezone.utc

CORE_TABLES = (
    "raw_events",
    "parsed_events",
    "signals",
    "delivery_audit",
    "telegram_deliveries",
    "trade_opportunities",
    "outcomes",
    "opportunity_outcomes",
    "trade_replay_examples",
    "trade_replay_profile_daily_stats",
)

DATE_FIELDS = ("logical_date", "archive_date", "date")
TIME_FIELDS = (
    "created_at",
    "updated_at",
    "timestamp",
    "sent_at",
    "archive_written_at",
    "archive_ts",
    "event_ts",
    "outcome_ts",
    "completed_at",
    "evaluated_at",
    "captured_at",
    "parsed_at",
    "notifier_sent_at",
    "signal_ts",
)
TIMESTAMP_CANDIDATES = DATE_FIELDS + TIME_FIELDS

REPORT_FIELDS = (
    "data_quality_summary",
    "trade_replay_summary",
    "outcome_diagnosis_summary",
    "candidate_coverage_summary",
    "candidate_frontier_summary",
    "lp_signal_summary",
    "lp_suppression_sample_replay_summary",
    "major_coverage_summary",
)

CORE_COLLECTION_TABLES = (
    "raw_events",
    "parsed_events",
    "signals",
    "delivery_audit",
    "trade_opportunities",
)

CORE_MIRROR_TABLES = (
    "raw_events",
    "parsed_events",
    "signals",
    "delivery_audit",
    "trade_opportunities",
    "trade_replay_examples",
)

LOCK_WARNING_TERMS = (
    "database is locked",
    "sqlite_store warning",
    "sqlite_busy",
    "operationalerror",
)
FINAL_FAILURE_TERMS = (
    "sqlite_write_failed",
    "write_error",
    "locked_failures",
    "final write failure",
)


def validate_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value or ""):
        raise ValueError("date must be YYYY-MM-DD")
    parsed = dt.date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("date must be YYYY-MM-DD")
    return value


def resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else Path.cwd() / path


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return int(value)
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def boolish_true(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def pct(completed: int | str, total: int | str) -> str:
    if isinstance(completed, str) or isinstance(total, str) or total <= 0:
        return "missing"
    return f"{completed / total:.4f}".rstrip("0").rstrip(".")


def logical_window(logical_date: str) -> tuple[int, int]:
    parsed = dt.date.fromisoformat(logical_date)
    start_bj = dt.datetime(parsed.year, parsed.month, parsed.day, tzinfo=BJ_TZ)
    start_ts = int(start_bj.timestamp())
    return start_ts, start_ts + 24 * 3600


def fmt_bj_time(ts: Any) -> str:
    parsed = safe_float(ts)
    if parsed is None:
        return "missing"
    if parsed > 100000000000:
        parsed /= 1000.0
    return dt.datetime.fromtimestamp(parsed, UTC).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S+08:00")


def fmt_ts(ts: Any) -> str:
    parsed = safe_float(ts)
    if parsed is None:
        return "missing"
    if parsed > 100000000000:
        parsed /= 1000.0
    return str(int(parsed))


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_ts_expr(column: str) -> str:
    ref = quote_ident(column)
    text = f"TRIM(CAST({ref} AS TEXT))"
    numeric_text = f"({text} GLOB '*[0-9]*' AND {text} NOT GLOB '*[^0-9.]*')"
    numeric_value = f"CAST({ref} AS REAL)"
    text_numeric_value = f"CAST({text} AS REAL)"
    return (
        "CASE "
        f"WHEN {ref} IS NULL OR {text} = '' THEN NULL "
        f"WHEN typeof({ref}) IN ('integer','real') THEN "
        f"CASE WHEN {numeric_value} > 100000000000 THEN {numeric_value} / 1000.0 "
        f"WHEN {numeric_value} > 1000000000 THEN {numeric_value} ELSE NULL END "
        f"WHEN {numeric_text} THEN "
        f"CASE WHEN {text_numeric_value} > 100000000000 THEN {text_numeric_value} / 1000.0 "
        f"WHEN {text_numeric_value} > 1000000000 THEN {text_numeric_value} ELSE NULL END "
        f"ELSE CAST(strftime('%s', {text}) AS REAL) "
        "END"
    )


def open_ro_sqlite(db_path: Path) -> sqlite3.Connection:
    # sqlite_store opens read-only connections with URI ?mode=ro and shared PRAGMAs.
    app_dir = Path(__file__).resolve().parents[1] / "app"
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    try:
        import sqlite_store  # type: ignore
    except Exception:  # noqa: BLE001
        uri = db_path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    return sqlite_store.open_sqlite_connection(db_path, readonly=True, row_factory=True)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return bool(row)


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()}


def date_filter(cols: set[str], logical_date: str, start_ts: int, end_ts: int) -> tuple[str, list[Any], str]:
    for field in DATE_FIELDS:
        if field in cols:
            return f"CAST({quote_ident(field)} AS TEXT) = ?", [logical_date], field
    for field in TIME_FIELDS:
        if field in cols:
            expr = sql_ts_expr(field)
            return f"({expr}) >= ? AND ({expr}) < ?", [start_ts, end_ts], field
    return "", [], "field_unavailable"


def max_ts_expr(cols: set[str]) -> tuple[str, str] | tuple[None, str]:
    present = [field for field in TIME_FIELDS if field in cols]
    if not present:
        return None, "field_unavailable"
    if len(present) == 1:
        return sql_ts_expr(present[0]), present[0]
    return "MAX(" + ", ".join(sql_ts_expr(field) for field in present) + ")", ",".join(present[:4])


def table_summary(
    conn: sqlite3.Connection,
    table: str,
    logical_date: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    if not table_exists(conn, table):
        return {
            "table_name": table,
            "exists": False,
            "rows_for_date": 0,
            "latest_ts": None,
            "latest_time_bj": "missing",
            "date_filter_source": "missing_table",
            "status": "missing_table",
        }
    cols = columns(conn, table)
    where_sql, params, source = date_filter(cols, logical_date, start_ts, end_ts)
    if not where_sql:
        return {
            "table_name": table,
            "exists": True,
            "rows_for_date": 0,
            "latest_ts": None,
            "latest_time_bj": "missing",
            "date_filter_source": source,
            "status": "field_unavailable",
        }
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE {where_sql}", params).fetchone()
        rows_for_date = safe_int(row[0] if row else 0)
    except sqlite3.Error as exc:
        return {
            "table_name": table,
            "exists": True,
            "rows_for_date": 0,
            "latest_ts": None,
            "latest_time_bj": "missing",
            "date_filter_source": source,
            "status": f"sqlite_error:{exc.__class__.__name__}",
        }

    expr, ts_source = max_ts_expr(cols)
    latest_ts = None
    status = "ok" if rows_for_date > 0 else "no_rows"
    if expr is None:
        status = "no_timestamp" if rows_for_date > 0 else status
    else:
        try:
            row = conn.execute(f"SELECT MAX({expr}) FROM {quote_ident(table)} WHERE {where_sql}", params).fetchone()
            latest_ts = row[0] if row else None
        except sqlite3.Error:
            latest_ts = None
            status = "no_timestamp" if rows_for_date > 0 else status
    return {
        "table_name": table,
        "exists": True,
        "rows_for_date": rows_for_date,
        "latest_ts": latest_ts,
        "latest_time_bj": fmt_bj_time(latest_ts),
        "date_filter_source": source,
        "timestamp_source": ts_source,
        "status": status,
    }


def count_with_condition(
    conn: sqlite3.Connection,
    table: str,
    base_where: str,
    base_params: list[Any],
    condition: str,
) -> int | str:
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE ({base_where}) AND ({condition})",
            base_params,
        ).fetchone()
    except sqlite3.Error as exc:
        return f"sqlite_error:{exc.__class__.__name__}"
    return safe_int(row[0] if row else 0)


def completed_condition(cols: set[str]) -> str:
    parts: list[str] = []
    if "completed" in cols:
        parts.append("CAST(COALESCE(\"completed\", 0) AS INTEGER) = 1")
    if "is_completed" in cols:
        parts.append("CAST(COALESCE(\"is_completed\", 0) AS INTEGER) = 1")
    for field in ("status", "outcome_status", "state"):
        if field in cols:
            parts.append(
                f"LOWER(COALESCE(CAST({quote_ident(field)} AS TEXT), '')) "
                "IN ('completed','settled','done','success','resolved')"
            )
    if "completed_at" in cols:
        parts.append("TRIM(COALESCE(CAST(\"completed_at\" AS TEXT), '')) != ''")
    return " OR ".join(parts)


def pending_condition(cols: set[str]) -> str:
    parts: list[str] = []
    for field in ("status", "outcome_status", "state"):
        if field in cols:
            parts.append(
                f"LOWER(COALESCE(CAST({quote_ident(field)} AS TEXT), '')) "
                "IN ('pending','open','queued','unresolved')"
            )
    if "completed" in cols:
        parts.append("CAST(COALESCE(\"completed\", 0) AS INTEGER) = 0")
    if "is_completed" in cols:
        parts.append("CAST(COALESCE(\"is_completed\", 0) AS INTEGER) = 0")
    if "completed_at" in cols:
        parts.append("TRIM(COALESCE(CAST(\"completed_at\" AS TEXT), '')) = ''")
    return " OR ".join(parts)


def outcome_stats(
    conn: sqlite3.Connection,
    table: str,
    logical_date: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    summary = table_summary(conn, table, logical_date, start_ts, end_ts)
    result: dict[str, Any] = {
        "total": safe_int(summary.get("rows_for_date")),
        "completed": "field_unavailable",
        "completed_rate": "missing",
        "pending": "field_unavailable",
        "past_due_pending": "field_unavailable",
    }
    if not summary.get("exists"):
        return result
    cols = columns(conn, table)
    where_sql, params, _source = date_filter(cols, logical_date, start_ts, end_ts)
    if not where_sql:
        return result
    complete_sql = completed_condition(cols)
    if complete_sql:
        completed = count_with_condition(conn, table, where_sql, params, complete_sql)
        result["completed"] = completed
        result["completed_rate"] = pct(completed, result["total"])
    pending_sql = pending_condition(cols)
    if pending_sql:
        pending = count_with_condition(conn, table, where_sql, params, pending_sql)
        result["pending"] = pending
        due_fields = (
            "due_ts",
            "due_at",
            "deadline_ts",
            "deadline_at",
            "evaluation_due_ts",
            "horizon_due_ts",
            "target_ts",
        )
        due_field = next((field for field in due_fields if field in cols), "")
        if due_field:
            due_expr = sql_ts_expr(due_field)
            result["past_due_pending"] = count_with_condition(
                conn,
                table,
                where_sql,
                params,
                f"({pending_sql}) AND ({due_expr}) IS NOT NULL AND ({due_expr}) < {int(dt.datetime.now(UTC).timestamp())}",
            )
    return result


def replay_stats(
    conn: sqlite3.Connection,
    logical_date: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    table = "trade_replay_examples"
    result: dict[str, Any] = {
        "scope_counts": {},
        "full_count": 0,
        "valid_count": "field_unavailable",
    }
    if not table_exists(conn, table):
        return result
    cols = columns(conn, table)
    where_sql, params, _source = date_filter(cols, logical_date, start_ts, end_ts)
    if not where_sql:
        return result
    if "replay_scope" in cols:
        try:
            rows = conn.execute(
                f"SELECT COALESCE(CAST(\"replay_scope\" AS TEXT), '') AS scope, COUNT(*) "
                f"FROM {quote_ident(table)} WHERE {where_sql} GROUP BY COALESCE(CAST(\"replay_scope\" AS TEXT), '')",
                params,
            ).fetchall()
            result["scope_counts"] = {str(row[0] or "missing"): safe_int(row[1]) for row in rows}
            result["full_count"] = safe_int(result["scope_counts"].get("full"))
        except sqlite3.Error:
            result["scope_counts"] = {}
    valid_condition = ""
    for field in ("valid", "is_valid"):
        if field in cols:
            valid_condition = f"CAST(COALESCE({quote_ident(field)}, 0) AS INTEGER) = 1"
            break
    if not valid_condition and "net_pnl_bps" in cols:
        valid_condition = "net_pnl_bps IS NOT NULL"
    if valid_condition:
        result["valid_count"] = count_with_condition(conn, table, where_sql, params, valid_condition)
    return result


def scan_archive(logical_date: str, archive_root: Path) -> dict[str, Any]:
    files: list[Path] = []
    if archive_root.exists():
        for path in archive_root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name
            if logical_date not in name:
                continue
            if name.endswith(".ndjson") or name.endswith(".ndjson.gz"):
                files.append(path)
    unique = sorted(set(files), key=lambda item: item.as_posix())
    ndjson = [path for path in unique if path.name.endswith(".ndjson")]
    gz = [path for path in unique if path.name.endswith(".ndjson.gz")]
    sizes = []
    mtimes = []
    empty = 0
    for path in unique:
        try:
            stat = path.stat()
        except OSError:
            continue
        sizes.append(stat.st_size)
        mtimes.append(stat.st_mtime)
        if stat.st_size <= 0:
            empty += 1
    total_size = sum(sizes)
    if not unique:
        status = "missing"
    elif total_size <= 0 or empty == len(unique):
        status = "empty_or_invalid"
    else:
        status = "present"
    latest = max(mtimes) if mtimes else None
    return {
        "archive_status": status,
        "archive_files_count": len(unique),
        "archive_ndjson_count": len(ndjson),
        "archive_gz_count": len(gz),
        "latest_mtime": latest,
        "latest_archive_time": fmt_bj_time(latest),
        "total_size_mb": round(total_size / (1024 * 1024), 4),
        "empty_file_count": empty,
        "archive_write_continuity": "present_nonempty" if status == "present" else status,
    }


def read_daily_report(logical_date: str, daily_dir: Path) -> dict[str, Any]:
    json_path = daily_dir / f"daily_report_{logical_date}.json"
    md_path = daily_dir / f"daily_report_{logical_date}.md"
    csv_path = daily_dir / f"daily_report_{logical_date}.csv"
    payload: dict[str, Any] = {}
    json_status = "missing"
    warnings: list[str] = []
    if json_path.exists():
        try:
            loaded = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
                json_status = "present"
            else:
                json_status = "unreadable"
                warnings.append("daily_report_schema_non_dict")
        except Exception as exc:  # noqa: BLE001
            json_status = "unreadable"
            warnings.append(f"daily_report_unreadable:{exc.__class__.__name__}")
    return {
        "payload": payload,
        "json_status": json_status,
        "md_status": "present" if md_path.exists() else "missing",
        "csv_status": "present" if csv_path.exists() else "missing",
        "daily_report_status": "present" if json_status == "present" else "missing",
        "warnings": warnings,
    }


def report_field(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    return "present" if isinstance(value, dict) and value else "missing"


def report_metrics(report: dict[str, Any]) -> dict[str, Any]:
    payload = as_dict(report.get("payload"))
    quality = as_dict(payload.get("data_quality_summary"))
    replay = as_dict(payload.get("trade_replay_summary"))
    outcome = as_dict(payload.get("outcome_diagnosis_summary"))
    candidate = as_dict(payload.get("candidate_coverage_summary"))
    lp_signal = as_dict(payload.get("lp_signal_summary"))
    lp_sample = as_dict(payload.get("lp_suppression_sample_replay_summary"))
    run_overview = as_dict(payload.get("run_overview"))

    lp_rows = lp_signal.get("lp_signal_rows")
    if lp_rows is None:
        lp_rows = run_overview.get("lp_signal_rows", payload.get("lp_signal_rows", "missing"))

    return {
        "data_quality_status": quality.get("data_quality_status", "missing"),
        "zero_activity_day": quality.get("zero_activity_day", "missing"),
        "replay_source": replay.get("replay_source", "missing"),
        "replay_scope": replay.get("replay_scope", "missing"),
        "replay_count": replay.get("replay_count", replay.get("persisted_rows_found", "missing")),
        "valid_replay_count": replay.get("valid_replay_count", "missing"),
        "outcomes_completed_rate": outcome.get("outcomes_completed_rate", outcome.get("outcome_completed_rate", "missing")),
        "opportunity_outcomes_completed_rate": outcome.get(
            "opportunity_outcomes_completed_rate",
            "missing",
        ),
        "opportunity_outcomes_past_due_pending": outcome.get(
            "opportunity_outcomes_past_due_pending",
            outcome.get("past_due_pending_count", "missing"),
        ),
        "candidate_coverage_available": candidate.get("available", bool(candidate)) if candidate else "missing",
        "candidate_frontier_available": report_field(payload, "candidate_frontier_summary"),
        "lp_signal_summary_available": lp_signal.get("available", bool(lp_signal)) if lp_signal else "missing",
        "lp_signal_rows": lp_rows,
        "lp_suppression_sample_replay_available": lp_sample.get("available", bool(lp_sample)) if lp_sample else "missing",
        "outcome_diagnosis_available": bool(outcome),
        "major_coverage_available": report_field(payload, "major_coverage_summary"),
        "field_status": {field: report_field(payload, field) for field in REPORT_FIELDS},
    }


def iter_log_files(log_roots: list[Path]) -> list[Path]:
    result: list[Path] = []
    for root in log_roots:
        if root.is_file():
            result.append(root)
            continue
        if not root.exists() or not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name.lower()
            if name.endswith((".log", ".out", ".err", ".txt", ".md", ".ndjson")):
                result.append(path)
            if len(result) >= 250:
                return result
    return result


def parse_line_time(line: str) -> float | None:
    match = re.search(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\+00:00|Z)?", line)
    if not match:
        return None
    value = match.group(0).replace(" ", "T").replace("Z", "+00:00")
    if value.endswith("T"):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def locked_warning_scan(logical_date: str, log_roots: list[Path]) -> dict[str, Any]:
    files = iter_log_files(log_roots)
    if not files:
        return {
            "sqlite_locked_warning_count": 0,
            "sqlite_final_write_failure_count": 0,
            "latest_locked_warning_time": "missing",
            "locked_status": "unknown",
            "log_files_scanned": 0,
        }
    warning_count = 0
    final_count = 0
    latest_ts: float | None = None
    for path in files[:250]:
        try:
            stat = path.stat()
        except OSError:
            continue
        try:
            opener = gzip.open if path.name.endswith(".gz") else open
            with opener(path, "rt", encoding="utf-8", errors="ignore") as handle:  # type: ignore[arg-type]
                for index, line in enumerate(handle):
                    if index >= 4000:
                        break
                    lowered = line.lower()
                    if logical_date not in line and dt.datetime.fromtimestamp(stat.st_mtime, UTC).astimezone(BJ_TZ).strftime("%Y-%m-%d") != logical_date:
                        continue
                    is_warning = any(term in lowered for term in LOCK_WARNING_TERMS)
                    is_final = any(term in lowered for term in FINAL_FAILURE_TERMS)
                    if not is_warning and not is_final:
                        continue
                    warning_count += int(is_warning)
                    final_count += int(is_final)
                    latest_ts = max(latest_ts or 0, parse_line_time(line) or stat.st_mtime)
        except (OSError, UnicodeError):
            continue
    if final_count > 0:
        status = "final_failures_detected"
    elif warning_count > 0:
        status = "warnings_only"
    else:
        status = "none"
    return {
        "sqlite_locked_warning_count": warning_count,
        "sqlite_final_write_failure_count": final_count,
        "latest_locked_warning_time": fmt_bj_time(latest_ts),
        "locked_status": status,
        "log_files_scanned": len(files[:250]),
    }


def sqlite_layer(logical_date: str, db_path: Path, start_ts: int, end_ts: int) -> dict[str, Any]:
    if not db_path.exists():
        return {
            "db_status": "missing",
            "tables": {
                table: {
                    "table_name": table,
                    "exists": False,
                    "rows_for_date": 0,
                    "latest_ts": None,
                    "latest_time_bj": "missing",
                    "date_filter_source": "missing_db",
                    "status": "missing_db",
                }
                for table in CORE_TABLES
            },
            "replay": {"scope_counts": {}, "full_count": 0, "valid_count": "field_unavailable"},
            "outcomes": {"total": 0, "completed": "field_unavailable", "completed_rate": "missing"},
            "opportunity_outcomes": {
                "total": 0,
                "completed": "field_unavailable",
                "completed_rate": "missing",
                "pending": "field_unavailable",
                "past_due_pending": "field_unavailable",
            },
            "warnings": ["sqlite_missing:data/chain_monitor.sqlite"],
        }
    try:
        conn = open_ro_sqlite(db_path)
    except sqlite3.Error as exc:
        return {
            "db_status": f"open_failed:{exc.__class__.__name__}",
            "tables": {},
            "replay": {"scope_counts": {}, "full_count": 0, "valid_count": "field_unavailable"},
            "outcomes": {"total": 0, "completed": "field_unavailable", "completed_rate": "missing"},
            "opportunity_outcomes": {
                "total": 0,
                "completed": "field_unavailable",
                "completed_rate": "missing",
                "pending": "field_unavailable",
                "past_due_pending": "field_unavailable",
            },
            "warnings": [f"sqlite_open_failed:{exc.__class__.__name__}"],
        }
    try:
        tables = {table: table_summary(conn, table, logical_date, start_ts, end_ts) for table in CORE_TABLES}
        return {
            "db_status": "present",
            "tables": tables,
            "replay": replay_stats(conn, logical_date, start_ts, end_ts),
            "outcomes": outcome_stats(conn, "outcomes", logical_date, start_ts, end_ts),
            "opportunity_outcomes": outcome_stats(conn, "opportunity_outcomes", logical_date, start_ts, end_ts),
            "warnings": [],
        }
    finally:
        conn.close()


def archive_sqlite_status(archive: dict[str, Any], tables: dict[str, dict[str, Any]]) -> tuple[str, str]:
    core_rows = [safe_int(tables.get(table, {}).get("rows_for_date")) for table in ("raw_events", "parsed_events", "signals")]
    archive_status = str(archive.get("archive_status"))
    if archive_status in {"missing", "empty_or_invalid"} and all(count == 0 for count in core_rows):
        return "invalid", "archive 与 SQLite raw/parsed/signals 都没有有效数据"
    if archive_status == "present" and any(count == 0 for count in core_rows):
        return "recoverable", "archive 有数据但 SQLite core table 存在 0 行，可能可通过 archive migrate 补齐"
    if archive_status in {"missing", "empty_or_invalid"} and any(count > 0 for count in core_rows):
        return "degraded", "SQLite 有数据但 archive 不可用于补齐"
    if archive_status == "present" and all(count > 0 for count in core_rows):
        return "complete", "archive 与 SQLite core tables 均有当日数据"
    return "unchecked", "字段不足，无法判断 archive 与 SQLite 是否匹配"


def list_text(values: list[str]) -> str:
    return ",".join(values) if values else "none"


def mirror_detail_summary(report: dict[str, Any]) -> dict[str, Any]:
    payload = as_dict(report.get("payload"))
    data_source = as_dict(payload.get("data_source_summary"))
    quality = as_dict(payload.get("data_quality_summary"))
    detail = as_dict(data_source.get("db_archive_mirror_detail"))
    checked: list[str] = []
    matched: list[str] = []
    mismatched: list[str] = []
    unchecked: list[str] = []
    mismatch_by_table: dict[str, Any] = {}

    for table, raw_item in sorted(detail.items()):
        item = as_dict(raw_item)
        match_rate = item.get("match_rate")
        mismatch = item.get("mismatch")
        if match_rate is None and mismatch is None:
            unchecked.append(str(table))
            continue
        checked.append(str(table))
        if boolish_true(mismatch):
            mismatched.append(str(table))
            mismatch_by_table[str(table)] = {
                key: item.get(key)
                for key in ("match_rate", "sqlite_rows", "archive_rows", "cache_rows", "compressed_archive_rows", "unavailable_rows")
                if key in item
            }
        else:
            matched.append(str(table))

    sqlite_health = as_dict(data_source.get("sqlite_health"))
    archive_mirror = as_dict(sqlite_health.get("archive_mirror"))
    skipped = str(archive_mirror.get("skipped") or "").strip()
    skipped_by_fast_mode = unchecked if skipped == "fast_mode" else []
    match_rate = data_source.get("db_archive_mirror_match_rate", quality.get("db_archive_mirror_match_rate"))

    return {
        "db_archive_mirror_match_rate": match_rate if match_rate is not None else "missing",
        "checked_tables": checked,
        "matched_tables": matched,
        "mismatched_tables": mismatched,
        "unchecked_tables": unchecked,
        "skipped_by_fast_mode": skipped_by_fast_mode,
        "mismatch_by_table": mismatch_by_table,
        "low_rate_without_mismatch": bool(checked or unchecked) and not mismatched and safe_float(match_rate) is not None and float(match_rate) < 0.95,
        "core_mismatched_tables": [table for table in mismatched if table in CORE_MIRROR_TABLES],
    }


def core_collection_status(
    archive: dict[str, Any],
    sqlite_data: dict[str, Any],
    report_stats: dict[str, Any],
) -> tuple[str, list[str]]:
    tables = as_dict(sqlite_data.get("tables"))
    missing: list[str] = []
    for table in CORE_COLLECTION_TABLES:
        if safe_int(as_dict(tables.get(table)).get("rows_for_date")) <= 0:
            missing.append(table)
    replay = as_dict(sqlite_data.get("replay"))
    replay_full = safe_int(replay.get("full_count"), 0)
    report_replay_count = safe_int(report_stats.get("replay_count"), 0)
    replay_source = str(report_stats.get("replay_source") or "missing")
    replay_scope = str(report_stats.get("replay_scope") or "missing")
    if replay_full <= 0 and not (replay_source == "persisted" and replay_scope == "full" and report_replay_count > 0):
        missing.append("trade_replay_examples")
    if str(archive.get("archive_status")) != "present":
        missing.append("archive")
    if not missing:
        return "complete", []
    return "incomplete", missing


def final_status(
    archive: dict[str, Any],
    sqlite_data: dict[str, Any],
    report: dict[str, Any],
    report_stats: dict[str, Any],
    locked: dict[str, Any],
    mirror_status: str,
    mirror_detail: dict[str, Any],
) -> tuple[str, str, list[str], dict[str, str]]:
    reasons: list[str] = []
    categories = {
        "collection_degraded": "false",
        "mirror_degraded": "false",
        "learning_loop_degraded": "false",
        "report_schema_degraded": "false",
    }
    tables = as_dict(sqlite_data.get("tables"))
    core_rows = [safe_int(as_dict(tables.get(table)).get("rows_for_date")) for table in ("raw_events", "parsed_events", "signals")]
    archive_status = str(archive.get("archive_status"))
    daily_json_present = report.get("json_status") == "present"
    replay_count = safe_int(report_stats.get("replay_count"), 0)
    sqlite_replay_rows = safe_int(as_dict(tables.get("trade_replay_examples")).get("rows_for_date"), 0)
    replay_source = str(report_stats.get("replay_source") or "missing")
    replay_scope = str(report_stats.get("replay_scope") or "missing")
    data_quality_status = str(report_stats.get("data_quality_status") or "missing")
    past_due = report_stats.get("opportunity_outcomes_past_due_pending")
    if past_due == "missing":
        past_due = as_dict(sqlite_data.get("opportunity_outcomes")).get("past_due_pending")
    past_due_count = safe_int(past_due, 0) if not isinstance(past_due, str) or str(past_due).isdigit() else 0
    core_status, core_missing = core_collection_status(archive, sqlite_data, report_stats)
    field_status = as_dict(report_stats.get("field_status"))
    missing_report_fields = [field for field in REPORT_FIELDS if field_status.get(field) == "missing"]

    if boolish_true(report_stats.get("zero_activity_day")):
        categories["collection_degraded"] = "true"
        return "invalid", "collection_degraded", ["data_quality zero_activity_day=true"], categories
    if archive_status in {"missing", "empty_or_invalid"} and all(count == 0 for count in core_rows):
        categories["collection_degraded"] = "true"
        return "invalid", "collection_degraded", ["archive 缺失或为空，且 SQLite raw/parsed/signals 均为 0"], categories

    if archive_status == "present" and any(count == 0 for count in core_rows):
        categories["collection_degraded"] = "true"
        reasons.append("archive 有数据但 SQLite core table 不完整")
        return "recoverable", "collection_degraded", reasons, categories

    if core_status != "complete":
        categories["collection_degraded"] = "true"
        reasons.append("core collection incomplete:" + ",".join(core_missing))
    if data_quality_status in {"invalid", "invalid_or_no_activity"}:
        categories["collection_degraded"] = "true"
        reasons.append(f"daily_report data_quality_status={data_quality_status}")
    if daily_json_present and (replay_source != "persisted" or replay_scope != "full" or replay_count <= 0):
        categories["learning_loop_degraded"] = "true"
        reasons.append("replay 不是 persisted/full 或 replay_count=0")
    if not daily_json_present and any(count > 0 for count in core_rows):
        categories["report_schema_degraded"] = "true"
        reasons.append("SQLite 有数据但 daily_report 缺失")
    if sqlite_replay_rows == 0 and any(count > 0 for count in core_rows):
        categories["learning_loop_degraded"] = "true"
        reasons.append("SQLite 有 core 数据但 trade_replay_examples 为 0")
    if past_due_count > 0:
        categories["learning_loop_degraded"] = "true"
        reasons.append(f"opportunity_outcomes past_due_pending={past_due_count}")
    if missing_report_fields and daily_json_present:
        categories["report_schema_degraded"] = "true"
        reasons.append("daily_report 字段缺失:" + ",".join(missing_report_fields))
    if mirror_detail.get("core_mismatched_tables"):
        categories["mirror_degraded"] = "true"
        reasons.append("core mirror mismatch:" + ",".join(mirror_detail.get("core_mismatched_tables") or []))

    if reasons:
        for key in ("collection_degraded", "learning_loop_degraded", "mirror_degraded", "report_schema_degraded"):
            if categories[key] == "true":
                return "degraded", key, reasons, categories
        return "degraded", "degraded", reasons, categories

    if (
        core_status == "complete"
        and daily_json_present
        and replay_source == "persisted"
        and replay_scope == "full"
        and replay_count > 0
        and past_due_count == 0
    ):
        return "complete", "complete", ["archive、SQLite、persisted/full replay 和 outcome 闭环未见明显缺口"], categories

    return "unchecked", "unchecked", ["缺少足够字段，未发现明确异常但不能判定 complete"], categories


def print_table(summary: dict[str, Any]) -> None:
    print(
        f"{summary.get('table_name')}: "
        f"exists={str(summary.get('exists')).lower()} "
        f"rows={summary.get('rows_for_date')} "
        f"latest={summary.get('latest_time_bj')} "
        f"date_filter_source={summary.get('date_filter_source')} "
        f"status={summary.get('status')}"
    )


def run(
    logical_date: str,
    db_path: Path,
    archive_root: Path,
    daily_dir: Path,
    log_roots: list[Path],
) -> int:
    start_ts, end_ts = logical_window(logical_date)
    archive = scan_archive(logical_date, archive_root)
    sqlite_data = sqlite_layer(logical_date, db_path, start_ts, end_ts)
    report = read_daily_report(logical_date, daily_dir)
    report_stats = report_metrics(report)
    locked = locked_warning_scan(logical_date, log_roots)
    mirror_status, mirror_reason = archive_sqlite_status(archive, as_dict(sqlite_data.get("tables")))
    mirror_detail = mirror_detail_summary(report)
    status, reason, reasons, categories = final_status(archive, sqlite_data, report, report_stats, locked, mirror_status, mirror_detail)
    core_status, core_missing = core_collection_status(archive, sqlite_data, report_stats)
    sqlite_write_warning = (
        "final_write_failure_detected"
        if safe_int(locked.get("sqlite_final_write_failure_count")) > 0
        else "none"
    )

    print(f"Chain Monitor 数据完整性检查｜{logical_date}")
    print()
    print("policy=只读检查；未修改 DB；未执行 make；未执行 migrate。")
    print()
    print("archive 层：")
    print(f"- archive_status={archive['archive_status']}")
    print(f"- archive_files={archive['archive_files_count']}")
    print(f"- archive_ndjson_count={archive['archive_ndjson_count']}")
    print(f"- archive_gz_count={archive['archive_gz_count']}")
    print(f"- latest_archive_time={archive['latest_archive_time']}")
    print(f"- archive_compressed={archive['archive_gz_count'] > 0}")
    print(f"- total_size_mb={archive['total_size_mb']}")
    print(f"- empty_files={archive['empty_file_count']}")
    print(f"- archive_write_continuity={archive['archive_write_continuity']}")
    print()
    print("SQLite mirror 层：")
    print(f"sqlite_db_status={sqlite_data.get('db_status')}")
    for table in CORE_TABLES:
        print_table(as_dict(as_dict(sqlite_data.get("tables")).get(table)))
    print()
    replay = as_dict(sqlite_data.get("replay"))
    outcomes = as_dict(sqlite_data.get("outcomes"))
    opp_outcomes = as_dict(sqlite_data.get("opportunity_outcomes"))
    print(
        "trade_replay_examples: "
        f"scope_distribution={json.dumps(replay.get('scope_counts', {}), ensure_ascii=False, sort_keys=True)} "
        f"full={replay.get('full_count')} valid={replay.get('valid_count')}"
    )
    print(
        "outcomes: "
        f"rows={outcomes.get('total')} completed={outcomes.get('completed')} "
        f"completed_rate={outcomes.get('completed_rate')}"
    )
    print(
        "opportunity_outcomes: "
        f"rows={opp_outcomes.get('total')} completed={opp_outcomes.get('completed')} "
        f"pending={opp_outcomes.get('pending')} past_due_pending={opp_outcomes.get('past_due_pending')} "
        f"completed_rate={opp_outcomes.get('completed_rate')}"
    )
    print()
    print("daily_report 文件：")
    print(f"- daily_report_json={report['json_status']}")
    print(f"- daily_report_md={report['md_status']}")
    print(f"- daily_report_csv={report['csv_status']}")
    field_status = as_dict(report_stats.get("field_status"))
    print("- daily_report_fields=" + "；".join(f"{field}={field_status.get(field, 'missing')}" for field in REPORT_FIELDS))
    print()
    print("学习闭环：")
    print(f"- data_quality_status={report_stats.get('data_quality_status')}")
    print(f"- zero_activity_day={report_stats.get('zero_activity_day')}")
    print(f"- replay_source={report_stats.get('replay_source')}")
    print(f"- replay_scope={report_stats.get('replay_scope')}")
    print(f"- replay_count={report_stats.get('replay_count')}")
    print(f"- valid_replay_count={report_stats.get('valid_replay_count')}")
    print(f"- outcomes_completed_rate={report_stats.get('outcomes_completed_rate')}")
    print(f"- opportunity_outcomes_completed_rate={report_stats.get('opportunity_outcomes_completed_rate')}")
    print(f"- past_due_pending={report_stats.get('opportunity_outcomes_past_due_pending')}")
    print(f"- candidate_coverage_available={report_stats.get('candidate_coverage_available')}")
    print(f"- candidate_frontier_available={report_stats.get('candidate_frontier_available')}")
    print(f"- lp_signal_summary_available={report_stats.get('lp_signal_summary_available')}")
    print(f"- lp_signal_rows={report_stats.get('lp_signal_rows')}")
    print(f"- lp_suppression_sample_replay_summary={report_stats.get('lp_suppression_sample_replay_available')}")
    print(f"- outcome_diagnosis_available={report_stats.get('outcome_diagnosis_available')}")
    print()
    print("SQLite locked warning：")
    print(f"- sqlite_locked_warning_count={locked['sqlite_locked_warning_count']}")
    print(f"- sqlite_final_write_failure_count={locked['sqlite_final_write_failure_count']}")
    print(f"- sqlite_write_warning={sqlite_write_warning}")
    print(f"- latest_locked_warning_time={locked['latest_locked_warning_time']}")
    print(f"- locked_status={locked['locked_status']}")
    print()
    print("mirror 判断：")
    print(f"- archive_sqlite_status={mirror_status}")
    print(f"- reason={mirror_reason}")
    print(f"- db_archive_mirror_match_rate={mirror_detail.get('db_archive_mirror_match_rate')}")
    print(f"- checked_tables={list_text(list(mirror_detail.get('checked_tables') or []))}")
    print(f"- matched_tables={list_text(list(mirror_detail.get('matched_tables') or []))}")
    print(f"- mismatched_tables={list_text(list(mirror_detail.get('mismatched_tables') or []))}")
    print(f"- unchecked_tables={list_text(list(mirror_detail.get('unchecked_tables') or []))}")
    print(f"- skipped_by_fast_mode={list_text(list(mirror_detail.get('skipped_by_fast_mode') or []))}")
    print("- mismatch_by_table=" + json.dumps(mirror_detail.get("mismatch_by_table") or {}, ensure_ascii=False, sort_keys=True))
    if mirror_detail.get("low_rate_without_mismatch"):
        print("- mirror_rate_warning=low_match_rate_nonblocking_no_mismatch")
    print()
    print("分层状态：")
    print(f"- core_collection_status={core_status}")
    print(f"- core_collection_missing={list_text(core_missing)}")
    print(f"- collection_degraded={categories.get('collection_degraded')}")
    print(f"- mirror_degraded={categories.get('mirror_degraded')}")
    print(f"- learning_loop_degraded={categories.get('learning_loop_degraded')}")
    print(f"- report_schema_degraded={categories.get('report_schema_degraded')}")
    print()
    print("最终结论：")
    print(f"- final_status={status}")
    print(f"- reason={reason}")
    print("- reasons=" + "；".join(reasons))
    print()
    print("下一步建议：")
    if status == "complete":
        print(f"- 可以运行 学习复盘{logical_date}。")
    elif status == "recoverable":
        print(f"- 建议 SSH 手动执行 make db-migrate-date DATE={logical_date}，然后重新 report/replay。")
    elif status == "degraded":
        if reason == "learning_loop_degraded" and core_status == "complete":
            print("- 核心采集完整；先修 opportunity_outcomes / replay 学习闭环，再用于策略质量评估。")
        elif reason == "mirror_degraded":
            print("- 核心采集未必缺失；先查看 mismatch_by_table，再决定是否补 mirror。")
        elif reason == "report_schema_degraded":
            print("- 先补齐 canonical daily report schema，再复查。")
        else:
            print("- 先排查 listener / archive / SQLite core table 缺口，不建议用于策略评估。")
    elif status == "invalid":
        print("- 该日仅用于运维排障。")
    else:
        print("- 字段不足，先补齐 canonical daily report 或 SQLite schema 后复查。")
    print()
    print("注意：仅做数据完整性诊断。")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Chain Monitor data integrity check.")
    parser.add_argument("--date", required=True, help="Beijing logical date YYYY-MM-DD")
    parser.add_argument(
        "--db-path",
        default=os.environ.get("HERMES_DATA_INTEGRITY_DB_PATH", os.environ.get("SQLITE_DB_PATH", "data/chain_monitor.sqlite")),
    )
    parser.add_argument(
        "--archive-root",
        default=os.environ.get("HERMES_DATA_INTEGRITY_ARCHIVE_ROOT", "app/data/archive"),
    )
    parser.add_argument(
        "--daily-dir",
        default=os.environ.get("HERMES_DATA_INTEGRITY_DAILY_DIR", "reports/daily"),
    )
    parser.add_argument(
        "--log-roots",
        default=os.environ.get("HERMES_DATA_INTEGRITY_LOG_ROOTS", "reports/hermes/ops_audit.ndjson:logs:reports"),
        help="Colon-separated read-only log roots.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        logical_date = validate_date(args.date)
    except ValueError as exc:
        print(f"error: invalid_date:{exc}", file=sys.stderr)
        return 2
    log_roots = [resolve_repo_path(Path(item)) for item in str(args.log_roots).split(":") if item]
    return run(
        logical_date,
        resolve_repo_path(Path(args.db_path)),
        resolve_repo_path(Path(args.archive_root)),
        resolve_repo_path(Path(args.daily_dir)),
        log_roots,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
