from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent


def is_test_runtime() -> bool:
    module_names = set(sys.modules)
    if any(name.startswith("pytest") for name in module_names):
        return True
    if any(name.startswith("unittest") for name in module_names):
        return True
    return any(name.startswith("app.test_") or name.startswith("test_") for name in module_names)


def resolve_persistence_path(path_value: str | None, *, namespace: str) -> Path | None:
    if not path_value:
        return None
    raw_path = Path(str(path_value))
    if raw_path.is_absolute():
        return raw_path
    if is_test_runtime():
        base = Path(tempfile.gettempdir()) / f"chain-monitor-tests-{os.getpid()}"
        base.mkdir(parents=True, exist_ok=True)
        suffix = uuid.uuid4().hex[:10]
        return base / f"{namespace}-{suffix}-{raw_path.name}"
    return (REPO_ROOT / raw_path).resolve()


def read_json_file(path: Path | None) -> Any:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_json_file(path: Path | None, payload: Any) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    temp_path.write_text(text, encoding="utf-8")
    temp_path.replace(path)
