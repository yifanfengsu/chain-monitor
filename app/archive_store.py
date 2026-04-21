from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
from typing import Any
from zoneinfo import ZoneInfo

from config import (
    ARCHIVE_BASE_DIR,
    ARCHIVE_ENABLE_CASE_FOLLOWUPS,
    ARCHIVE_ENABLE_CASES,
    ARCHIVE_ENABLE_DELIVERY_AUDIT,
    ARCHIVE_ENABLE_PARSED_EVENTS,
    ARCHIVE_ENABLE_RAW_EVENTS,
    ARCHIVE_ENABLE_SIGNALS,
    SQLITE_ARCHIVE_MIRROR_ENABLE,
)


class ArchiveStore:
    """
    本地 append-only NDJSON 归档层。
    写入失败只打印错误，不阻塞主流程。
    """

    _TIME_FIELD_MAP = {
        "archive_ts": "archive_time_bj",
        "ts": "ts_bj",
        "ingest_ts": "ingest_time_bj",
    }
    _BJ_TZ = ZoneInfo("Asia/Shanghai")

    def __init__(
        self,
        base_dir: str | Path = ARCHIVE_BASE_DIR,
        *,
        category_enabled: dict[str, bool] | None = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._global_lock = threading.Lock()
        self._locks_by_path: dict[str, threading.Lock] = {}
        self._seen_keys_by_path: dict[str, set[str]] = {}
        self._category_enabled = {
            "raw_events": bool(ARCHIVE_ENABLE_RAW_EVENTS),
            "parsed_events": bool(ARCHIVE_ENABLE_PARSED_EVENTS),
            "signals": bool(ARCHIVE_ENABLE_SIGNALS),
            "delivery_audit": bool(ARCHIVE_ENABLE_DELIVERY_AUDIT),
            "cases": bool(ARCHIVE_ENABLE_CASES),
            "case_followups": bool(ARCHIVE_ENABLE_CASE_FOLLOWUPS),
        }
        for category, enabled in dict(category_enabled or {}).items():
            self._category_enabled[str(category)] = bool(enabled)

    def write_raw_event(self, raw_item: dict, archive_ts: int | None = None) -> bool:
        return self._write("raw_events", self._payload(raw_item, archive_ts=archive_ts))

    def write_parsed_event(self, parsed_event: dict, archive_ts: int | None = None) -> bool:
        return self._write("parsed_events", self._payload(parsed_event, archive_ts=archive_ts))

    def write_signal(
        self,
        signal: Any,
        event: Any | None = None,
        archive_ts: int | None = None,
        dedupe_key: str | None = None,
    ) -> bool:
        serialized_signal = self._serialize(signal)
        if isinstance(serialized_signal, dict) and (
            serialized_signal.get("signal_id")
            or serialized_signal.get("signal_archive_key")
        ):
            payload = dict(serialized_signal)
        else:
            payload = {
                "signal": serialized_signal,
                "event": self._serialize(event) if event is not None else None,
            }
        dedupe_key = str(
            dedupe_key
            or payload.get("signal_archive_key")
            or payload.get("signal_id")
            or ""
        ).strip()
        if "archive_written_at" not in payload:
            payload["archive_written_at"] = int(archive_ts or self._now_ts())
        return self._write("signals", self._payload(payload, archive_ts=archive_ts), dedupe_key=dedupe_key)

    def write_delivery_audit(self, audit_record: dict, archive_ts: int | None = None) -> bool:
        return self._write("delivery_audit", self._payload(audit_record, archive_ts=archive_ts))

    def write_case_update(
        self,
        behavior_case: Any,
        event: Any | None = None,
        signal: Any | None = None,
        action: str = "updated",
        archive_ts: int | None = None,
    ) -> bool:
        payload = {
            "action": action,
            "case": self._serialize(behavior_case),
            "event": self._serialize(event) if event is not None else None,
            "signal": self._serialize(signal) if signal is not None else None,
        }
        return self._write("cases", self._payload(payload, archive_ts=archive_ts))

    def write_case_followup(self, case_id: str, followup: dict, archive_ts: int | None = None) -> bool:
        payload = {
            "case_id": case_id,
            "followup": self._serialize(followup),
        }
        return self._write("case_followups", self._payload(payload, archive_ts=archive_ts))

    def _payload(self, data: Any, archive_ts: int | None = None) -> dict:
        ts = int(archive_ts or self._now_ts())
        serialized = self._serialize(data)
        canonical_archive_time_bj = self._format_bj_time(ts)
        if isinstance(serialized, dict):
            inner_archive_ts = serialized.get("archive_ts")
            inner_archive_time_bj = serialized.get("archive_time_bj")
            same_archive_ts = False
            try:
                same_archive_ts = inner_archive_ts is not None and int(inner_archive_ts) == ts
            except (TypeError, ValueError):
                same_archive_ts = False
            same_archive_time_bj = (
                canonical_archive_time_bj is not None
                and str(inner_archive_time_bj or "") == canonical_archive_time_bj
            )
            if same_archive_ts or same_archive_time_bj:
                serialized.pop("archive_ts", None)
                serialized.pop("archive_time_bj", None)
        payload = {
            "archive_ts": ts,
            "data": serialized,
        }
        return self._enrich_time_fields(payload)

    def _write(self, category: str, payload: dict, *, dedupe_key: str = "") -> bool:
        sqlite_wrote = self._mirror_to_sqlite(category, payload)
        if not self._is_enabled(category):
            return bool(sqlite_wrote)
        try:
            path = self._path_for(category, int(payload.get("archive_ts") or self._now_ts()))
            path.parent.mkdir(parents=True, exist_ok=True)
            lock = self._lock_for_path(path)
            with lock:
                if dedupe_key:
                    seen = self._seen_keys_for_path(path)
                    if dedupe_key in seen:
                        return bool(sqlite_wrote)
                with path.open("a", encoding="utf-8") as fp:
                    fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
                if dedupe_key:
                    seen.add(dedupe_key)
            return True
        except Exception as e:
            print(f"归档失败[{category}]: {e}")
            return bool(sqlite_wrote)

    def _mirror_to_sqlite(self, category: str, payload: dict) -> bool:
        if not bool(SQLITE_ARCHIVE_MIRROR_ENABLE):
            return False
        try:
            import sqlite_store

            if category == "raw_events":
                return bool(sqlite_store.write_raw_event(payload))
            if category == "parsed_events":
                return bool(sqlite_store.write_parsed_event(payload))
            if category == "signals":
                return bool(sqlite_store.write_signal(payload))
            if category == "delivery_audit":
                return bool(sqlite_store.write_delivery_audit(payload))
            if category == "case_followups":
                return bool(sqlite_store.write_case_followup(payload))
            if category == "cases":
                data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                return bool(sqlite_store.upsert_asset_case(data))
            return False
        except Exception as e:
            print(f"sqlite mirror failed[{category}]: {e}")
            return False

    def _is_enabled(self, category: str) -> bool:
        return bool(self._category_enabled.get(category, True))

    def _path_for(self, category: str, archive_ts: int) -> Path:
        day = (
            datetime.fromtimestamp(int(archive_ts), tz=timezone.utc)
            .astimezone(self._BJ_TZ)
            .strftime("%Y-%m-%d")
        )
        return self.base_dir / category / f"{day}.ndjson"

    def _lock_for_path(self, path: Path) -> threading.Lock:
        key = str(path.resolve())
        with self._global_lock:
            lock = self._locks_by_path.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks_by_path[key] = lock
            return lock

    def _seen_keys_for_path(self, path: Path) -> set[str]:
        key = str(path.resolve())
        seen = self._seen_keys_by_path.get(key)
        if seen is not None:
            return seen
        seen = set()
        if path.exists():
            try:
                with path.open(encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        data = payload.get("data") if isinstance(payload, dict) else {}
                        if not isinstance(data, dict):
                            continue
                        dedupe_key = str(
                            data.get("signal_archive_key")
                            or data.get("signal_id")
                            or ""
                        ).strip()
                        if dedupe_key:
                            seen.add(dedupe_key)
            except OSError:
                pass
        self._seen_keys_by_path[key] = seen
        return seen

    def _serialize(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {
                str(key): self._serialize(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self._serialize(item) for item in value]
        if isinstance(value, set):
            return sorted(self._serialize(item) for item in value)
        if hasattr(value, "items") and not isinstance(value, dict):
            return {
                str(key): self._serialize(item)
                for key, item in value.items()
            }
        if is_dataclass(value):
            return self._serialize(asdict(value))
        if hasattr(value, "__dict__"):
            return self._serialize(vars(value))
        return str(value)

    def _format_bj_time(self, ts: Any) -> str | None:
        if ts is None or isinstance(ts, bool):
            return None
        try:
            if isinstance(ts, str):
                ts = ts.strip()
                if not ts:
                    return None
                raw_ts = int(float(ts))
            else:
                raw_ts = int(ts)
            return (
                datetime.fromtimestamp(raw_ts, tz=timezone.utc)
                .astimezone(self._BJ_TZ)
                .strftime("%Y-%m-%d %H:%M:%S")
            )
        except (TypeError, ValueError, OverflowError, OSError):
            return None

    def _enrich_time_fields(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            enriched: dict[str, Any] = {}
            for key, value in payload.items():
                normalized_key = str(key)
                enriched_value = self._enrich_time_fields(value)
                enriched[normalized_key] = enriched_value

                bj_key = self._TIME_FIELD_MAP.get(normalized_key)
                if bj_key:
                    formatted = self._format_bj_time(value)
                    if formatted is not None:
                        enriched[bj_key] = formatted
            return enriched
        if isinstance(payload, list):
            return [self._enrich_time_fields(item) for item in payload]
        if isinstance(payload, tuple):
            return [self._enrich_time_fields(item) for item in payload]
        return payload

    def _now_ts(self) -> int:
        return int(datetime.now(tz=timezone.utc).timestamp())
