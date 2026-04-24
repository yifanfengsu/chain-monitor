from __future__ import annotations

from dataclasses import asdict, dataclass
import gzip
import json
from pathlib import Path
import sqlite3
import subprocess
from typing import Any, Callable

from config import (
    ARCHIVE_BASE_DIR,
    PROJECT_ROOT,
    REPORT_ARCHIVE_READ_GZIP,
    REPORT_DB_ARCHIVE_COMPARE,
    REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH,
    SQLITE_DB_PATH,
    SQLITE_ENABLE,
    SQLITE_REPORT_FALLBACK_TO_ARCHIVE,
    SQLITE_REPORT_READ_PREFER_DB,
)


@dataclass
class LoadResult:
    rows: list[dict[str, Any]]
    source: str
    row_count: int
    warnings: list[str]
    fallback_used: bool
    mismatch_info: dict[str, Any]
    compressed_archive_rows: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


DB_TABLES = {
    "raw_events": {"table": "raw_events", "time_column": "captured_at", "json_column": "raw_json", "archive": "raw_events"},
    "parsed_events": {"table": "parsed_events", "time_column": "parsed_at", "json_column": "parsed_json", "archive": "parsed_events"},
    "signals": {"table": "signals", "time_column": "archive_written_at", "json_column": "signal_json", "archive": "signals"},
    "delivery_audit": {"table": "delivery_audit", "time_column": "archive_written_at", "json_column": "audit_json", "archive": "delivery_audit"},
    "case_followups": {"table": "case_followups", "time_column": "archive_written_at", "json_column": "followup_json", "archive": "case_followups"},
    "asset_cases": {"table": "asset_cases", "time_column": "updated_at", "json_column": "case_json", "cache": "asset_cases.cache.json"},
    "asset_market_states": {"table": "asset_market_states", "time_column": "updated_at", "json_column": "state_json", "cache": "asset_market_states.cache.json"},
    "trade_opportunities": {"table": "trade_opportunities", "time_column": "created_at", "json_column": "opportunity_json", "cache": "trade_opportunities.cache.json"},
    "outcomes": {"table": "outcomes", "time_column": "created_at", "json_column": "", "archive": ""},
    "quality_stats": {"table": "quality_stats", "time_column": "updated_at", "json_column": "stats_json", "cache": "lp_quality_stats.cache.json"},
    "telegram_deliveries": {"table": "telegram_deliveries", "time_column": "created_at", "json_column": "message_json", "archive": ""},
    "market_context_attempts": {"table": "market_context_attempts", "time_column": "created_at", "json_column": "", "archive": ""},
}

REPORT_SOURCE_ARCHIVE_CATEGORIES = ("raw_events", "parsed_events", "signals", "delivery_audit", "cases", "case_followups")
REPORT_SOURCE_COMPONENTS = ("signals", "delivery_audit", "case_followups")


def _db_path(db_path: str | Path | None = None) -> Path:
    path = Path(db_path or SQLITE_DB_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _window_bounds(window: Any = None) -> tuple[int | None, int | None]:
    if window is None:
        return None, None
    if isinstance(window, dict):
        start = (
            window.get("start_ts")
            or window.get("analysis_window_start_ts")
            or window.get("window_start_ts")
        )
        end = (
            window.get("end_ts")
            or window.get("analysis_window_end_ts")
            or window.get("window_end_ts")
        )
        return _to_int(start), _to_int(end)
    if isinstance(window, (list, tuple)) and len(window) >= 2:
        return _to_int(window[0]), _to_int(window[1])
    return None, None


def _to_int(value: Any) -> int | None:
    if value in (None, "", [], {}, ()):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _from_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _archive_key(path: Path) -> str:
    name = path.name
    if name.endswith(".ndjson.gz"):
        return name[: -len(".ndjson.gz")]
    if name.endswith(".ndjson"):
        return name[: -len(".ndjson")]
    return path.stem


def archive_date_key(path: Path) -> str:
    return _archive_key(path)


def archive_paths(category: str, *, base_dir: str | Path | None = None, read_gzip: bool | None = None) -> list[Path]:
    root = Path(base_dir or ARCHIVE_BASE_DIR) / category
    plain_paths = { _archive_key(path): path for path in sorted(root.glob("*.ndjson")) }
    paths = dict(plain_paths)
    use_gzip = REPORT_ARCHIVE_READ_GZIP if read_gzip is None else bool(read_gzip)
    if use_gzip:
        for path in sorted(root.glob("*.ndjson.gz")):
            paths.setdefault(_archive_key(path), path)
    return [paths[key] for key in sorted(paths)]


def _open_archive_text(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def open_archive_text(path: Path):
    return _open_archive_text(path)


def _count_archive_lines(path: Path) -> int:
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
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
    except OSError:
        return 0


def iter_archive_payloads(
    category: str,
    *,
    window: Any = None,
    base_dir: str | Path | None = None,
    read_gzip: bool | None = None,
) -> list[dict[str, Any]]:
    start_ts, end_ts = _window_bounds(window)
    rows: list[dict[str, Any]] = []
    for path in archive_paths(category, base_dir=base_dir, read_gzip=read_gzip):
        try:
            with _open_archive_text(path) as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = _to_int(payload.get("archive_ts"))
                    if start_ts is not None and ts is not None and ts < start_ts:
                        continue
                    if end_ts is not None and ts is not None and ts > end_ts:
                        continue
                    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                    if isinstance(data, dict):
                        data = dict(data)
                        data.setdefault("archive_ts", ts)
                        data.setdefault("archive_written_at", ts)
                        rows.append(data)
        except OSError:
            continue
    return rows


def archive_row_counts(base_dir: str | Path | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for category in ("raw_events", "parsed_events", "signals", "delivery_audit", "cases", "case_followups"):
        total = 0
        for path in archive_paths(category, base_dir=base_dir):
            total += _count_archive_lines(path)
        counts[category] = total
    return counts


def compressed_archive_row_counts(base_dir: str | Path | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    root = Path(base_dir or ARCHIVE_BASE_DIR)
    for category in ("raw_events", "parsed_events", "signals", "delivery_audit", "cases", "case_followups"):
        total = 0
        for path in sorted((root / category).glob("*.ndjson.gz")):
            total += _count_archive_lines(path)
        counts[category] = total
    return counts


def _cache_rows(cache_name: str, *, cache_base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    root = Path(cache_base_dir or (PROJECT_ROOT / "data"))
    path = root / cache_name
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("records", "cases", "states", "opportunities", "rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
    return [dict(payload)]


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection | None:
    if not bool(SQLITE_ENABLE):
        return None
    path = _db_path(db_path)
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _db_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _fast_table_row_counts(db_path: str | Path | None = None) -> dict[str, int]:
    conn = _connect(db_path)
    if conn is None:
        return {}
    counts: dict[str, int] = {}
    table_names = sorted({str(config.get("table") or "") for config in DB_TABLES.values() if config.get("table")})
    try:
        for table in table_names:
            try:
                if not _db_table_exists(conn, table):
                    counts[table] = -1
                    continue
                counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except sqlite3.Error:
                counts[table] = -1
        return counts
    finally:
        conn.close()


def _archive_directory_status(archive_base_dir: str | Path | None = None) -> dict[str, Any]:
    root = Path(archive_base_dir or ARCHIVE_BASE_DIR)
    return {
        "archive_base_dir": str(root),
        "archive_base_dir_exists": root.exists(),
        "archive_dirs_by_category": {
            category: (root / category).exists()
            for category in REPORT_SOURCE_ARCHIVE_CATEGORIES
        },
    }


def _load_db_rows(loader_key: str, *, window: Any = None, db_path: str | Path | None = None) -> tuple[list[dict[str, Any]], str]:
    config = DB_TABLES[loader_key]
    table = str(config["table"])
    json_column = str(config.get("json_column") or "")
    time_column = str(config.get("time_column") or "")
    conn = _connect(db_path)
    if conn is None:
        return [], "db_unavailable"
    try:
        if not _db_table_exists(conn, table):
            return [], "db_table_missing"
        start_ts, end_ts = _window_bounds(window)
        clauses = []
        params: list[Any] = []
        if time_column and start_ts is not None:
            clauses.append(f"{time_column} >= ?")
            params.append(start_ts)
        if time_column and end_ts is not None:
            clauses.append(f"{time_column} <= ?")
            params.append(end_ts)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(f"SELECT * FROM {table} {where}", tuple(params)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            if json_column and json_column in item:
                decoded = _from_json(item.get(json_column), {})
                if isinstance(decoded, dict) and decoded:
                    decoded.update({key: value for key, value in item.items() if key not in decoded})
                    item = decoded
            if time_column and time_column in item and item.get("archive_written_at") in (None, ""):
                item.setdefault("archive_written_at", item.get(time_column))
            if time_column and time_column in item and item.get("archive_ts") in (None, ""):
                item.setdefault("archive_ts", item.get(time_column))
            item.setdefault("data_source", "sqlite")
            result.append(item)
        return result, ""
    except sqlite3.Error as exc:
        return [], f"db_read_failed:{exc}"
    finally:
        conn.close()


def _archive_or_cache_rows(loader_key: str, *, window: Any = None, archive_base_dir: str | Path | None = None) -> tuple[list[dict[str, Any]], str, int]:
    config = DB_TABLES[loader_key]
    archive_category = str(config.get("archive") or "")
    cache_name = str(config.get("cache") or "")
    if archive_category:
        rows = iter_archive_payloads(archive_category, window=window, base_dir=archive_base_dir)
        compressed_count = compressed_archive_row_counts(archive_base_dir).get(archive_category, 0)
        return rows, "archive", int(compressed_count or 0)
    if cache_name:
        rows = _cache_rows(cache_name)
        return rows, "cache", 0
    return [], "unavailable", 0


def _mismatch_info(loader_key: str, db_count: int, fallback_count: int, fallback_source: str, compressed_rows: int) -> dict[str, Any]:
    match_rate = 1.0 if db_count == 0 and fallback_count == 0 else min(db_count, fallback_count) / max(db_count, fallback_count, 1)
    mismatch = db_count != fallback_count and fallback_source in {"archive", "cache"}
    return {
        "loader": loader_key,
        "sqlite_rows": int(db_count),
        f"{fallback_source}_rows": int(fallback_count),
        "match_rate": round(match_rate, 4),
        "mismatch": bool(mismatch),
        "compressed_archive_rows": int(compressed_rows),
    }


def load_dataset(
    loader_key: str,
    *,
    window: Any = None,
    prefer_db: bool = True,
    db_path: str | Path | None = None,
    archive_base_dir: str | Path | None = None,
) -> LoadResult:
    warnings: list[str] = []
    effective_prefer_db = bool(prefer_db) and bool(SQLITE_REPORT_READ_PREFER_DB)
    db_rows: list[dict[str, Any]] = []
    db_reason = ""
    if effective_prefer_db:
        db_rows, db_reason = _load_db_rows(loader_key, window=window, db_path=db_path)
        if db_reason:
            warnings.append(db_reason)

    fallback_rows: list[dict[str, Any]] = []
    fallback_source = "unavailable"
    compressed_rows = 0
    need_fallback = not db_rows
    compare_enabled = bool(REPORT_DB_ARCHIVE_COMPARE)
    if bool(SQLITE_REPORT_FALLBACK_TO_ARCHIVE) and (need_fallback or compare_enabled):
        fallback_rows, fallback_source, compressed_rows = _archive_or_cache_rows(
            loader_key,
            window=window,
            archive_base_dir=archive_base_dir,
        )

    mismatch_info = _mismatch_info(loader_key, len(db_rows), len(fallback_rows), fallback_source, compressed_rows)
    if mismatch_info.get("mismatch"):
        message = f"db_archive_mismatch:{loader_key}:sqlite={len(db_rows)}:{fallback_source}={len(fallback_rows)}"
        warnings.append(message)
        if bool(REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH):
            raise RuntimeError(message)

    if db_rows:
        return LoadResult(db_rows, "sqlite", len(db_rows), warnings, False, mismatch_info, compressed_rows)
    if fallback_rows:
        return LoadResult(fallback_rows, fallback_source, len(fallback_rows), warnings, effective_prefer_db, mismatch_info, compressed_rows)
    if not bool(SQLITE_REPORT_FALLBACK_TO_ARCHIVE) and not db_rows:
        warnings.append("fallback_disabled")
    return LoadResult([], "unavailable", 0, warnings, effective_prefer_db, mismatch_info, compressed_rows)


def load_signals(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("signals", window=window, prefer_db=prefer_db, **kwargs)


def load_raw_events(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("raw_events", window=window, prefer_db=prefer_db, **kwargs)


def load_parsed_events(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("parsed_events", window=window, prefer_db=prefer_db, **kwargs)


def load_delivery_audit(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("delivery_audit", window=window, prefer_db=prefer_db, **kwargs)


def load_case_followups(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("case_followups", window=window, prefer_db=prefer_db, **kwargs)


def load_asset_cases(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("asset_cases", window=window, prefer_db=prefer_db, **kwargs)


def load_asset_market_states(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("asset_market_states", window=window, prefer_db=prefer_db, **kwargs)


def load_trade_opportunities(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("trade_opportunities", window=window, prefer_db=prefer_db, **kwargs)


def load_outcomes(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("outcomes", window=window, prefer_db=prefer_db, **kwargs)


def load_quality_stats(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("quality_stats", window=window, prefer_db=prefer_db, **kwargs)


def load_telegram_deliveries(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("telegram_deliveries", window=window, prefer_db=prefer_db, **kwargs)


def load_market_context_attempts(window: Any = None, prefer_db: bool = True, **kwargs) -> LoadResult:
    return load_dataset("market_context_attempts", window=window, prefer_db=prefer_db, **kwargs)


def sqlite_health(db_path: str | Path | None = None, *, fast: bool = False) -> dict[str, Any]:
    if fast:
        path = _db_path(db_path)
        table_counts = _fast_table_row_counts(db_path)
        return {
            "enabled": bool(SQLITE_ENABLE),
            "db_path": str(path),
            "initialized": path.exists() and any(value >= 0 for value in table_counts.values()),
            "warnings": [],
            "table_row_counts": table_counts,
            "archive_mirror": {
                "db_archive_mirror_match_rate": None,
                "per_category": {},
                "skipped": "fast_mode",
            },
        }
    try:
        import sqlite_store

        sqlite_store.init_sqlite_store(db_path)
        return sqlite_store.health_summary()
    except Exception as exc:
        return {
            "enabled": bool(SQLITE_ENABLE),
            "db_path": str(_db_path(db_path)),
            "initialized": False,
            "warnings": [f"sqlite_health_unavailable:{exc}"],
            "table_row_counts": {},
        }


def report_source_summary(
    *,
    window: Any = None,
    db_path: str | Path | None = None,
    archive_base_dir: str | Path | None = None,
    fast: bool = False,
) -> dict[str, Any]:
    health = sqlite_health(db_path, fast=fast)
    sqlite_rows = dict(health.get("table_row_counts") or {})
    archive_status = _archive_directory_status(archive_base_dir)
    if fast:
        sources: dict[str, str] = {}
        loader_results: dict[str, dict[str, Any]] = {}
        for key in REPORT_SOURCE_COMPONENTS:
            config = DB_TABLES[key]
            table = str(config.get("table") or key)
            archive_category = str(config.get("archive") or key)
            db_count = max(int(sqlite_rows.get(table) or 0), 0)
            archive_dir_exists = bool((archive_status.get("archive_dirs_by_category") or {}).get(archive_category))
            source = "sqlite" if bool(SQLITE_REPORT_READ_PREFER_DB) and db_count > 0 else "archive_unscanned" if archive_dir_exists else "unavailable"
            sources[key] = source
            loader_results[key] = {
                "rows": [],
                "source": source,
                "row_count": db_count if source == "sqlite" else None,
                "warnings": [],
                "fallback_used": False,
                "mismatch_info": {
                    "loader": key,
                    "sqlite_rows": db_count,
                    "archive_rows": None,
                    "match_rate": None,
                    "mismatch": None,
                    "compressed_archive_rows": None,
                    "skipped": "fast_mode",
                },
                "compressed_archive_rows": None,
            }
        unique_sources = {source for source in sources.values() if source and source != "unavailable"}
        data_source = "mixed" if len(unique_sources) > 1 else next(iter(unique_sources), "unavailable")
        return {
            "report_data_source": data_source,
            "data_source": data_source,
            "fast_mode": True,
            "archive_row_counts_scanned": False,
            "compressed_archive_row_counts_scanned": False,
            "source_components": sources,
            "current_report_read_preference": {
                "SQLITE_REPORT_READ_PREFER_DB": bool(SQLITE_REPORT_READ_PREFER_DB),
                "SQLITE_REPORT_FALLBACK_TO_ARCHIVE": bool(SQLITE_REPORT_FALLBACK_TO_ARCHIVE),
                "REPORT_ARCHIVE_READ_GZIP": bool(REPORT_ARCHIVE_READ_GZIP),
                "REPORT_DB_ARCHIVE_COMPARE": bool(REPORT_DB_ARCHIVE_COMPARE),
                "REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH": bool(REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH),
            },
            "db_path": str(_db_path(db_path)),
            "db_exists": _db_path(db_path).exists(),
            "sqlite_health": health,
            "sqlite_rows_by_table": sqlite_rows,
            "archive_rows_by_category": {},
            "compressed_archive_rows": {},
            "compressed_archive_detected": None,
            "archive_fallback_used": False,
            "fallback_mode": "archive_cache_enabled" if SQLITE_REPORT_FALLBACK_TO_ARCHIVE else "disabled",
            "db_archive_mirror_match_rate": None,
            "db_archive_mirror_detail": {},
            "mismatch_warnings": [],
            "loader_results": loader_results,
            "warnings": list(health.get("warnings") or []),
            **archive_status,
        }
    archive_rows = archive_row_counts(archive_base_dir)
    compressed_rows = compressed_archive_row_counts(archive_base_dir)
    sources: dict[str, str] = {}
    mismatch_warnings: list[str] = []
    loader_results: dict[str, dict[str, Any]] = {}
    for key in ("signals", "delivery_audit", "case_followups"):
        config = DB_TABLES[key]
        table = str(config.get("table") or key)
        archive_category = str(config.get("archive") or key)
        db_count = max(int(sqlite_rows.get(table) or 0), 0)
        archive_count = int(archive_rows.get(archive_category) or 0)
        fallback_source = "archive" if archive_count > 0 else "unavailable"
        source = "sqlite" if bool(SQLITE_REPORT_READ_PREFER_DB) and db_count > 0 else fallback_source
        sources[key] = source
        mismatch = db_count != archive_count and (db_count > 0 or archive_count > 0)
        if mismatch:
            mismatch_warnings.append(
                f"db_archive_mismatch:{key}:sqlite={db_count}:archive={archive_count}"
            )
        loader_results[key] = {
            "rows": [],
            "source": source,
            "row_count": db_count if source == "sqlite" else archive_count,
            "warnings": [mismatch_warnings[-1]] if mismatch else [],
            "fallback_used": source == "archive" and bool(SQLITE_REPORT_READ_PREFER_DB),
            "mismatch_info": _mismatch_info(
                key,
                db_count,
                archive_count,
                "archive",
                int(compressed_rows.get(archive_category) or 0),
            ),
            "compressed_archive_rows": int(compressed_rows.get(archive_category) or 0),
        }
    mirror_detail = (health.get("archive_mirror") or {}).get("per_category") or {}
    for category, item in mirror_detail.items():
        if not item.get("mismatch"):
            continue
        warning = (
            f"db_archive_mirror_mismatch:{category}:"
            f"sqlite={int(item.get('sqlite_rows') or 0)}:"
            f"archive={int(item.get('archive_rows') or 0)}"
        )
        if warning not in mismatch_warnings:
            mismatch_warnings.append(warning)
    unique_sources = {source for source in sources.values() if source and source != "unavailable"}
    data_source = "mixed" if len(unique_sources) > 1 else next(iter(unique_sources), "unavailable")
    fallback_used = any(bool(payload.get("fallback_used")) for payload in loader_results.values())
    return {
        "report_data_source": data_source,
        "data_source": data_source,
        "fast_mode": False,
        "archive_row_counts_scanned": True,
        "compressed_archive_row_counts_scanned": True,
        "source_components": sources,
        "current_report_read_preference": {
            "SQLITE_REPORT_READ_PREFER_DB": bool(SQLITE_REPORT_READ_PREFER_DB),
            "SQLITE_REPORT_FALLBACK_TO_ARCHIVE": bool(SQLITE_REPORT_FALLBACK_TO_ARCHIVE),
            "REPORT_ARCHIVE_READ_GZIP": bool(REPORT_ARCHIVE_READ_GZIP),
            "REPORT_DB_ARCHIVE_COMPARE": bool(REPORT_DB_ARCHIVE_COMPARE),
            "REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH": bool(REPORT_FAIL_ON_DB_ARCHIVE_MISMATCH),
        },
        "db_path": str(_db_path(db_path)),
        "db_exists": _db_path(db_path).exists(),
        "sqlite_health": health,
        "sqlite_rows_by_table": sqlite_rows,
        "archive_rows_by_category": archive_rows,
        "compressed_archive_rows": compressed_rows,
        "compressed_archive_detected": any(int(value or 0) > 0 for value in compressed_rows.values()),
        "archive_fallback_used": bool(fallback_used),
        "fallback_mode": "archive_cache_enabled" if SQLITE_REPORT_FALLBACK_TO_ARCHIVE else "disabled",
        "db_archive_mirror_match_rate": (health.get("archive_mirror") or {}).get("db_archive_mirror_match_rate"),
        "db_archive_mirror_detail": (health.get("archive_mirror") or {}).get("per_category") or {},
        "mismatch_warnings": mismatch_warnings,
        "loader_results": loader_results,
        "warnings": list(health.get("warnings") or []) + mismatch_warnings,
        **archive_status,
    }
