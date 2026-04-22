from __future__ import annotations

import shutil
from datetime import datetime, tzinfo
from pathlib import Path


def report_snapshot_date(*, tz: tzinfo, now: datetime | None = None) -> str:
    current = now.astimezone(tz) if now is not None and now.tzinfo is not None else now
    if current is None:
        current = datetime.now(tz)
    elif current.tzinfo is None:
        current = current.replace(tzinfo=tz)
    return current.strftime("%Y-%m-%d")


def dated_report_path(latest_path: Path, report_date: str) -> Path:
    return latest_path.with_name(f"{latest_path.stem}_{report_date}{latest_path.suffix}")


def write_dated_report_copies(
    outputs: dict[str, Path],
    *,
    tz: tzinfo,
    report_date: str | None = None,
) -> dict[str, Path]:
    snapshot_date = report_date or report_snapshot_date(tz=tz)
    snapshots: dict[str, Path] = {}
    for name, path in outputs.items():
        snapshot_path = dated_report_path(path, snapshot_date)
        shutil.copyfile(path, snapshot_path)
        snapshots[name] = snapshot_path
    return snapshots
