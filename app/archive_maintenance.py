from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

from config import ARCHIVE_BASE_DIR, PROJECT_ROOT


ARCHIVE_CATEGORIES = (
    "raw_events",
    "parsed_events",
    "signals",
    "delivery_audit",
    "cases",
    "case_followups",
)


def _archive_base(base_dir: str | Path | None = None) -> Path:
    path = Path(base_dir or ARCHIVE_BASE_DIR)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _validate_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("DATE must be YYYY-MM-DD") from exc
    return value


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _active_archive_dates() -> set[str]:
    now = datetime.now(timezone.utc)
    bj_tz = timezone(timedelta(hours=8))
    return {
        now.strftime("%Y-%m-%d"),
        now.astimezone(bj_tz).strftime("%Y-%m-%d"),
    }


def _allow_today(allow_today: bool | None = None) -> bool:
    if allow_today is not None:
        return bool(allow_today)
    return str(os.environ.get("ALLOW_TODAY") or "").strip().upper() == "YES"


def _file_info(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "size": 0, "mtime": None}
    stat = path.stat()
    return {
        "exists": True,
        "size": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def archive_status(date: str, *, base_dir: str | Path | None = None) -> dict[str, Any]:
    root = _archive_base(base_dir)
    categories: dict[str, Any] = {}
    for category in ARCHIVE_CATEGORIES:
        plain = root / category / f"{date}.ndjson"
        gz = root / category / f"{date}.ndjson.gz"
        categories[category] = {
            "ndjson": str(plain),
            "ndjson_exists": plain.exists(),
            "ndjson_size": _file_info(plain)["size"],
            "ndjson_mtime": _file_info(plain)["mtime"],
            "gzip": str(gz),
            "gzip_exists": gz.exists(),
            "gzip_size": _file_info(gz)["size"],
            "gzip_mtime": _file_info(gz)["mtime"],
        }
    return {
        "date": date,
        "archive_base_dir": str(root),
        "categories": categories,
    }


def _gzip_deterministic(source: Path, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{source.name}.", suffix=".gz.tmp", dir=str(target.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with source.open("rb") as src, tmp_path.open("wb") as raw_out:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_out, mtime=0) as gz_out:
                shutil.copyfileobj(src, gz_out)
        tmp_path.replace(target)
        return int(target.stat().st_size)
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def compress_date(
    date: str,
    *,
    execute: bool = False,
    base_dir: str | Path | None = None,
    allow_today: bool | None = None,
) -> dict[str, Any]:
    date = _validate_date(date)
    root = _archive_base(base_dir)
    today = _today_utc()
    active_dates = sorted(_active_archive_dates())
    allowed_today = _allow_today(allow_today)
    payload: dict[str, Any] = {
        "date": date,
        "archive_base_dir": str(root),
        "mode": "execute" if execute else "dry-run",
        "today_utc": today,
        "today_archive_dates": active_dates,
        "allow_today": allowed_today,
        "refused": False,
        "reason": "",
        "compressed": [],
        "skipped": [],
        "missing": [],
        "already_compressed": [],
        "categories": {},
    }
    if date in active_dates and not allowed_today:
        payload["refused"] = True
        payload["reason"] = "refusing_to_compress_active_archive_date"
        for category in ARCHIVE_CATEGORIES:
            payload["skipped"].append(category)
            payload["categories"][category] = {"status": "refused_today"}
        return payload

    for category in ARCHIVE_CATEGORIES:
        plain = root / category / f"{date}.ndjson"
        gz = root / category / f"{date}.ndjson.gz"
        category_payload = {
            "ndjson": str(plain),
            "gzip": str(gz),
            "ndjson_exists": plain.exists(),
            "gzip_exists": gz.exists(),
            "status": "",
            "source_size": plain.stat().st_size if plain.exists() else 0,
            "gzip_size": gz.stat().st_size if gz.exists() else 0,
        }
        if gz.exists():
            category_payload["status"] = "already_compressed"
            payload["already_compressed"].append(category)
        elif not plain.exists():
            category_payload["status"] = "missing"
            payload["missing"].append(category)
        elif not execute:
            category_payload["status"] = "dry_run_would_compress"
            payload["skipped"].append(category)
        else:
            size = _gzip_deterministic(plain, gz)
            plain.unlink()
            category_payload["status"] = "compressed"
            category_payload["gzip_exists"] = True
            category_payload["gzip_size"] = size
            category_payload["ndjson_exists"] = False
            payload["compressed"].append(category)
        payload["categories"][category] = category_payload
    return payload


def mirror_check(date: str | None = None) -> dict[str, Any]:
    import sqlite_store

    sqlite_store.init_sqlite_store()
    payload = sqlite_store.integrity_check()
    mismatches = list(payload.get("db_archive_mismatch") or [])
    return {
        "date": date,
        "db_archive_mirror_match_rate": (payload.get("archive_mirror") or {}).get("db_archive_mirror_match_rate"),
        "db_archive_mismatch": mismatches,
        "strict_ok": not mismatches,
        "sqlite_integrity_ok": bool(payload.get("ok")),
        "archive_mirror": payload.get("archive_mirror") or {},
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="chain-monitor archive maintenance")
    parser.add_argument("--compress-date", type=_validate_date, help="archive date YYYY-MM-DD to gzip")
    parser.add_argument("--status-date", type=_validate_date, help="archive date YYYY-MM-DD to inspect")
    parser.add_argument("--mirror-check-date", type=_validate_date, help="archive date YYYY-MM-DD for mirror-check label")
    parser.add_argument("--archive-base-dir", help="override archive base dir")
    parser.add_argument("--dry-run", action="store_true", help="preview compression without changing files")
    parser.add_argument("--execute", action="store_true", help="execute compression")
    parser.add_argument("--allow-today", action="store_true", help="allow compressing today's UTC date")
    parser.add_argument("--strict", action="store_true", help="return non-zero when mirror-check sees mismatch")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.status_date:
        _print_json(archive_status(args.status_date, base_dir=args.archive_base_dir))
        return 0
    if args.mirror_check_date:
        payload = mirror_check(args.mirror_check_date)
        _print_json(payload)
        return 0 if (not args.strict or payload.get("strict_ok")) else 1
    if args.compress_date:
        execute = bool(args.execute) and not bool(args.dry_run)
        payload = compress_date(
            args.compress_date,
            execute=execute,
            base_dir=args.archive_base_dir,
            allow_today=bool(args.allow_today) or None,
        )
        _print_json(payload)
        return 1 if payload.get("refused") and execute else 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
