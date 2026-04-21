from __future__ import annotations

import atexit
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import hashlib
import time

from config import (
    LP_ASSET_CASE_CACHE_PATH,
    LP_ASSET_CASE_FLUSH_INTERVAL_SEC,
    LP_ASSET_CASE_MAX_SUPPORTING_PAIRS,
    LP_ASSET_CASE_MAX_TRACKED,
    LP_ASSET_CASE_PERSIST_ENABLE,
    LP_ASSET_CASE_RECOVER_ON_START,
    LP_ASSET_CASE_SCHEMA_VERSION,
    LP_ASSET_CASE_WINDOW_SEC,
)
from lp_product_helpers import (
    asset_symbol_from_event,
    lp_direction_bucket_for_event,
    lp_stage_rank,
    pair_label_from_event,
    venue_family_from_event,
)
from persistence_utils import read_json_file, resolve_persistence_path, write_json_file


@dataclass
class AssetCase:
    asset_case_id: str
    asset_case_key: str
    asset_case_stage: str
    asset_case_direction: str
    asset_case_confidence: float
    asset_case_primary_pair: str
    asset_case_supporting_pairs: list[str] = field(default_factory=list)
    asset_case_evidence_summary: str = ""
    asset_case_started_at: int = 0
    asset_case_updated_at: int = 0
    asset_symbol: str = ""
    chain: str = "ethereum"
    venue_family: str = "onchain_lp"
    stage_history: list[str] = field(default_factory=list)
    pool_addresses: list[str] = field(default_factory=list)
    event_ids: list[str] = field(default_factory=list)
    last_signal_at: int = 0
    last_stage_transition_at: int = 0
    aggregate_metrics: dict = field(default_factory=dict)
    market_context_snapshot: dict = field(default_factory=dict)
    quality_snapshot: dict = field(default_factory=dict)
    had_prealert: bool = False
    first_prealert_at: int = 0
    last_prealert_signal_id: str = ""
    last_prealert_delivery_allowed: bool | None = None
    last_prealert_delivery_block_reason: str = ""
    prealert_upgraded_to_confirm_at: int = 0
    prealert_to_confirm_sec: int | None = None
    restored_from_cache: bool = False


class AssetCaseManager:
    def __init__(
        self,
        *,
        window_sec: int = LP_ASSET_CASE_WINDOW_SEC,
        max_supporting_pairs: int = LP_ASSET_CASE_MAX_SUPPORTING_PAIRS,
        max_tracked: int = LP_ASSET_CASE_MAX_TRACKED,
        persistence_enabled: bool = LP_ASSET_CASE_PERSIST_ENABLE,
        cache_path: str | None = LP_ASSET_CASE_CACHE_PATH,
        flush_interval_sec: float = LP_ASSET_CASE_FLUSH_INTERVAL_SEC,
        recover_on_start: bool = LP_ASSET_CASE_RECOVER_ON_START,
        schema_version: str = LP_ASSET_CASE_SCHEMA_VERSION,
    ) -> None:
        self.window_sec = max(int(window_sec), 30)
        self.max_supporting_pairs = max(int(max_supporting_pairs), 2)
        self.max_tracked = max(int(max_tracked), 32)
        self.persistence_enabled = bool(persistence_enabled)
        self.flush_interval_sec = max(float(flush_interval_sec or 0.0), 0.2)
        self.schema_version = str(schema_version or LP_ASSET_CASE_SCHEMA_VERSION)
        self.cache_path = resolve_persistence_path(cache_path, namespace="asset-cases") if self.persistence_enabled else None
        self._cases: dict[str, AssetCase] = {}
        self._index: dict[tuple[str, str, str, str], deque[str]] = defaultdict(lambda: deque(maxlen=self.max_tracked))
        self._dirty = False
        self._last_flush_monotonic = 0.0
        self._last_load_status = "not_loaded"
        if self.persistence_enabled and bool(recover_on_start):
            self.load_from_disk()
        if self.persistence_enabled:
            atexit.register(self.flush, True)

    def merge_lp_signal(self, event, signal, gate_metrics: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if str(getattr(event, "strategy_role", "") or "") != "lp_pool":
            return {}
        if str(getattr(signal, "context", {}).get("lp_alert_stage") or getattr(event, "metadata", {}).get("lp_alert_stage") or "") not in {
            "prealert",
            "confirm",
            "climax",
            "exhaustion_risk",
        }:
            return {}
        if str(getattr(event, "metadata", {}).get("liquidation_stage") or "none") in {"risk", "execution"}:
            return {}

        asset_symbol = asset_symbol_from_event(event)
        direction = lp_direction_bucket_for_event(event)
        pair_label = pair_label_from_event(event)
        venue_family = venue_family_from_event(event)
        chain = str(getattr(event, "chain", "ethereum") or "ethereum").strip().lower()
        if not asset_symbol or not direction:
            return {}

        now_ts = int(getattr(event, "ts", 0) or 0)
        self._prune(now_ts=now_ts)
        case = self._match_case(
            chain=chain,
            asset_symbol=asset_symbol,
            direction=direction,
            venue_family=venue_family,
            ts=now_ts,
        )
        if case is None:
            case = self._create_case(
                chain=chain,
                asset_symbol=asset_symbol,
                direction=direction,
                venue_family=venue_family,
                ts=now_ts,
                pair_label=pair_label,
            )

        self._update_case(case=case, event=event, signal=signal, gate_metrics=gate_metrics, pair_label=pair_label)
        payload = self._payload(case, event=event, signal=signal)
        getattr(event, "metadata", {}).update(payload)
        getattr(signal, "metadata", {}).update(payload)
        getattr(signal, "context", {}).update(payload)
        self._mirror_sqlite_case(payload)
        self._mark_dirty()
        self.flush()
        return payload

    def attach_runtime_context(self, event, signal, *, gate_metrics: dict | None = None) -> dict:
        case_id = str(
            getattr(signal, "context", {}).get("asset_case_id")
            or getattr(signal, "metadata", {}).get("asset_case_id")
            or getattr(event, "metadata", {}).get("asset_case_id")
            or ""
        ).strip()
        if not case_id or case_id not in self._cases:
            return {}
        case = self._cases[case_id]
        gate_metrics = gate_metrics or {}
        case.aggregate_metrics = {
            "lp_multi_pool_resonance": int(gate_metrics.get("lp_multi_pool_resonance") or case.aggregate_metrics.get("lp_multi_pool_resonance") or 0),
            "lp_same_pool_continuity": int(gate_metrics.get("lp_same_pool_continuity") or case.aggregate_metrics.get("lp_same_pool_continuity") or 0),
            "lp_pool_volume_surge_ratio": round(float(gate_metrics.get("lp_pool_volume_surge_ratio") or case.aggregate_metrics.get("lp_pool_volume_surge_ratio") or 0.0), 4),
        }
        case.market_context_snapshot = self._market_context_snapshot(event, signal)
        case.quality_snapshot = self._quality_snapshot(event, signal)
        self._mark_dirty()
        self.flush()
        payload = self._payload(case, event=event, signal=signal)
        getattr(event, "metadata", {}).update(payload)
        getattr(signal, "metadata", {}).update(payload)
        getattr(signal, "context", {}).update(payload)
        self._mirror_sqlite_case(payload)
        return payload

    def flush(self, force: bool = False) -> None:
        if not self.persistence_enabled or self.cache_path is None or not self._dirty:
            return
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_flush_monotonic) < self.flush_interval_sec:
            return
        payload = {
            "schema_version": self.schema_version,
            "generated_at": int(time.time()),
            "window_sec": int(self.window_sec),
            "cases": [self._serialize_case(case) for case in self.snapshot_cases()],
        }
        write_json_file(self.cache_path, payload)
        self._dirty = False
        self._last_flush_monotonic = now_monotonic

    def load_from_disk(self) -> int:
        payload = read_json_file(self.cache_path)
        if not payload:
            self._last_load_status = "empty_or_missing"
            return 0
        if not isinstance(payload, dict):
            self._last_load_status = "malformed_payload"
            return 0
        schema_version = str(payload.get("schema_version") or "")
        migrated_cases = self._migrate_cases(payload, schema_version=schema_version)
        if migrated_cases is None:
            self._last_load_status = f"schema_mismatch:{schema_version or 'unknown'}"
            return 0

        self._cases.clear()
        self._index.clear()
        loaded = 0
        for item in migrated_cases:
            case = self._deserialize_case(item)
            if case is None:
                continue
            self._cases[case.asset_case_id] = case
            self._index[(case.chain, case.asset_symbol, case.asset_case_direction, case.venue_family)].append(case.asset_case_id)
            loaded += 1
        self._trim_cases(mark_dirty=False)
        self._last_load_status = "loaded"
        return loaded

    def prune_expired(self, *, now_ts: int | None = None) -> list[str]:
        reference_ts = int(now_ts or time.time())
        removed = self._prune(now_ts=reference_ts)
        self.flush()
        return removed

    def _mirror_sqlite_case(self, payload: dict) -> None:
        if not payload:
            return
        try:
            import sqlite_store

            sqlite_store.upsert_asset_case(payload)
        except Exception as exc:
            print(f"sqlite asset case mirror failed: {exc}")

    def snapshot(self) -> list[dict]:
        return [asdict(item) for item in self.snapshot_cases()]

    def snapshot_cases(self) -> list[AssetCase]:
        return sorted(
            self._cases.values(),
            key=lambda item: (-int(item.asset_case_updated_at or 0), -lp_stage_rank(item.asset_case_stage)),
        )

    def _create_case(
        self,
        *,
        chain: str,
        asset_symbol: str,
        direction: str,
        venue_family: str,
        ts: int,
        pair_label: str,
    ) -> AssetCase:
        case_key = "|".join([chain, asset_symbol, direction, venue_family])
        case_hash = hashlib.sha1(f"{case_key}:{ts}".encode("utf-8")).hexdigest()[:10]
        case = AssetCase(
            asset_case_id=f"asset_case:{asset_symbol}:{case_hash}",
            asset_case_key=case_key,
            asset_case_stage="prealert",
            asset_case_direction=direction,
            asset_case_confidence=0.0,
            asset_case_primary_pair=pair_label,
            asset_case_supporting_pairs=[pair_label] if pair_label else [],
            asset_case_started_at=ts,
            asset_case_updated_at=ts,
            asset_symbol=asset_symbol,
            chain=chain,
            venue_family=venue_family,
            last_signal_at=ts,
            last_stage_transition_at=ts,
        )
        self._cases[case.asset_case_id] = case
        self._index[(chain, asset_symbol, direction, venue_family)].append(case.asset_case_id)
        self._trim_cases()
        return case

    def _match_case(
        self,
        *,
        chain: str,
        asset_symbol: str,
        direction: str,
        venue_family: str,
        ts: int,
    ) -> AssetCase | None:
        candidate_ids = list(self._index.get((chain, asset_symbol, direction, venue_family)) or [])
        candidates = []
        for candidate_id in candidate_ids:
            if candidate_id not in self._cases:
                continue
            case = self._cases[candidate_id]
            if ts - int(case.asset_case_updated_at or 0) <= self._effective_case_window(case):
                candidates.append(case)
        if not candidates:
            return None
        return sorted(
            candidates,
            key=lambda item: (
                -lp_stage_rank(item.asset_case_stage),
                -int(item.asset_case_updated_at or 0),
                -len(item.asset_case_supporting_pairs),
            ),
        )[0]

    def _update_case(self, *, case: AssetCase, event, signal, gate_metrics: dict, pair_label: str) -> None:
        stage = str(getattr(signal, "context", {}).get("lp_alert_stage") or getattr(event, "metadata", {}).get("lp_alert_stage") or "")
        confidence = float(
            getattr(signal, "context", {}).get("lp_follow_confidence")
            or getattr(signal, "context", {}).get("lp_lead_confidence")
            or getattr(signal, "context", {}).get("lp_exhaustion_confidence")
            or 0.0
        )
        if stage and lp_stage_rank(stage) >= lp_stage_rank(case.asset_case_stage):
            if stage != case.asset_case_stage:
                case.last_stage_transition_at = int(getattr(event, "ts", 0) or case.asset_case_updated_at)
            case.asset_case_stage = stage or case.asset_case_stage
        signal_id = str(getattr(signal, "signal_id", "") or "")
        prealert_delivery_allowed = getattr(signal, "context", {}).get("lp_prealert_delivery_allowed")
        prealert_delivery_block_reason = str(
            getattr(signal, "context", {}).get("lp_prealert_delivery_block_reason")
            or getattr(signal, "metadata", {}).get("lp_prealert_delivery_block_reason")
            or getattr(event, "metadata", {}).get("lp_prealert_delivery_block_reason")
            or ""
        )
        event_ts = int(getattr(event, "ts", 0) or case.asset_case_updated_at)
        if stage == "prealert":
            case.had_prealert = True
            if not int(case.first_prealert_at or 0):
                case.first_prealert_at = event_ts
            case.last_prealert_signal_id = signal_id or case.last_prealert_signal_id
            if prealert_delivery_allowed is not None:
                case.last_prealert_delivery_allowed = bool(prealert_delivery_allowed)
            case.last_prealert_delivery_block_reason = prealert_delivery_block_reason
        elif stage in {"confirm", "climax", "exhaustion_risk"} and case.had_prealert and int(case.first_prealert_at or 0) > 0:
            if not int(case.prealert_upgraded_to_confirm_at or 0):
                case.prealert_upgraded_to_confirm_at = event_ts
                case.prealert_to_confirm_sec = max(event_ts - int(case.first_prealert_at or 0), 0)
        case.asset_case_confidence = round(max(float(case.asset_case_confidence or 0.0), confidence), 3)
        case.asset_case_updated_at = int(getattr(event, "ts", 0) or case.asset_case_updated_at)
        case.last_signal_at = int(getattr(event, "ts", 0) or case.last_signal_at or case.asset_case_updated_at)
        if pair_label and pair_label not in case.asset_case_supporting_pairs:
            case.asset_case_supporting_pairs.append(pair_label)
        if not case.asset_case_primary_pair:
            case.asset_case_primary_pair = pair_label
        pool_address = str(getattr(event, "address", "") or "").lower()
        if pool_address and pool_address not in case.pool_addresses:
            case.pool_addresses.append(pool_address)
        event_id = str(getattr(event, "event_id", "") or getattr(event, "tx_hash", "") or "")
        if event_id and event_id not in case.event_ids:
            case.event_ids.append(event_id)
        if stage and (not case.stage_history or case.stage_history[-1] != stage):
            case.stage_history.append(stage)
        case.asset_case_supporting_pairs = case.asset_case_supporting_pairs[: self.max_supporting_pairs]
        case.pool_addresses = case.pool_addresses[: self.max_supporting_pairs]
        case.event_ids = case.event_ids[-8:]
        case.aggregate_metrics = {
            "lp_multi_pool_resonance": int(gate_metrics.get("lp_multi_pool_resonance") or case.aggregate_metrics.get("lp_multi_pool_resonance") or 0),
            "lp_same_pool_continuity": int(gate_metrics.get("lp_same_pool_continuity") or case.aggregate_metrics.get("lp_same_pool_continuity") or 0),
            "lp_pool_volume_surge_ratio": round(float(gate_metrics.get("lp_pool_volume_surge_ratio") or case.aggregate_metrics.get("lp_pool_volume_surge_ratio") or 0.0), 4),
        }
        case.asset_case_evidence_summary = self._evidence_summary(case=case, gate_metrics=gate_metrics)

    def _evidence_summary(self, *, case: AssetCase, gate_metrics: dict) -> str:
        pair_count = len(case.asset_case_supporting_pairs)
        if pair_count >= 2:
            prefix = f"首发{pair_count}池"
        elif case.asset_case_stage == "prealert":
            prefix = "单池首发"
        else:
            prefix = "单池主导"
        pair_brief = ", ".join(case.asset_case_supporting_pairs[: self.max_supporting_pairs])
        volume_surge_ratio = float(gate_metrics.get("lp_pool_volume_surge_ratio") or case.aggregate_metrics.get("lp_pool_volume_surge_ratio") or 0.0)
        same_pool_continuity = int(gate_metrics.get("lp_same_pool_continuity") or case.aggregate_metrics.get("lp_same_pool_continuity") or 0)
        multi_pool_resonance = int(gate_metrics.get("lp_multi_pool_resonance") or case.aggregate_metrics.get("lp_multi_pool_resonance") or 0)
        parts = [prefix]
        if pair_brief:
            parts.append(pair_brief)
        if multi_pool_resonance >= 2:
            parts.append(f"跨池{multi_pool_resonance}")
        elif same_pool_continuity >= 2:
            parts.append(f"同池连续{same_pool_continuity}")
        if volume_surge_ratio > 0:
            parts.append(f"放量{volume_surge_ratio:.1f}x")
        return "｜".join(parts[:4])

    def _payload(self, case: AssetCase, *, event, signal) -> dict:
        pair_count = len(case.asset_case_supporting_pairs)
        pool_count = len(case.pool_addresses)
        evidence_pack = case.asset_case_evidence_summary
        continuity_hint = "续案" if bool(case.restored_from_cache) else ""
        if continuity_hint and evidence_pack and not evidence_pack.startswith(f"{continuity_hint}｜"):
            evidence_pack = f"{continuity_hint}｜{evidence_pack}"
        return {
            "asset_case_id": case.asset_case_id,
            "asset_case_key": case.asset_case_key,
            "asset_case_stage": case.asset_case_stage,
            "asset_case_direction": case.asset_case_direction,
            "asset_case_confidence": float(case.asset_case_confidence or 0.0),
            "asset_case_primary_pair": case.asset_case_primary_pair,
            "asset_case_supporting_pairs": list(case.asset_case_supporting_pairs),
            "asset_case_supporting_pair_count": pair_count,
            "asset_case_supporting_pool_count": pool_count,
            "asset_case_evidence_summary": case.asset_case_evidence_summary,
            "asset_case_started_at": int(case.asset_case_started_at or 0),
            "asset_case_updated_at": int(case.asset_case_updated_at or 0),
            "asset_case_last_signal_at": int(case.last_signal_at or 0),
            "asset_case_last_stage_transition_at": int(case.last_stage_transition_at or 0),
            "asset_case_label": case.asset_symbol,
            "asset_symbol": case.asset_symbol,
            "asset_case_aggregated": pair_count >= 2 or pool_count >= 2,
            "asset_case_multi_pool": pair_count >= 2 or pool_count >= 2,
            "asset_case_stage_rank": lp_stage_rank(case.asset_case_stage),
            "asset_case_pool_fallback_only": pair_count <= 1 and case.asset_case_stage == "prealert",
            "asset_case_evidence_pack": evidence_pack,
            "asset_case_stage_history": list(case.stage_history),
            "asset_case_last_pair": pair_label_from_event(event),
            "asset_case_last_signal_id": str(getattr(signal, "signal_id", "") or ""),
            "asset_case_window_sec": int(self.window_sec),
            "asset_case_venue_family": case.venue_family,
            "asset_case_continuity_hint": continuity_hint,
            "asset_case_recovered": bool(case.restored_from_cache),
            "asset_case_market_context_snapshot": dict(case.market_context_snapshot),
            "asset_case_quality_snapshot": dict(case.quality_snapshot),
            "asset_case_had_prealert": bool(case.had_prealert),
            "asset_case_first_prealert_at": int(case.first_prealert_at or 0),
            "asset_case_last_prealert_signal_id": case.last_prealert_signal_id,
            "asset_case_last_prealert_delivery_allowed": case.last_prealert_delivery_allowed,
            "asset_case_last_prealert_delivery_block_reason": case.last_prealert_delivery_block_reason,
            "asset_case_prealert_upgraded_to_confirm_at": int(case.prealert_upgraded_to_confirm_at or 0),
            "asset_case_prealert_to_confirm_sec": case.prealert_to_confirm_sec,
        }

    def _prune(self, *, now_ts: int) -> list[str]:
        stale_case_ids = [
            case_id
            for case_id, case in self._cases.items()
            if now_ts - int(case.asset_case_updated_at or 0) > self._effective_case_window(case)
        ]
        for case_id in stale_case_ids:
            case = self._cases.pop(case_id, None)
            if case is None:
                continue
            index_key = (case.chain, case.asset_symbol, case.asset_case_direction, case.venue_family)
            queue = self._index.get(index_key)
            if queue is None:
                continue
            self._index[index_key] = deque([item for item in queue if item != case_id], maxlen=self.max_tracked)
            if not self._index[index_key]:
                self._index.pop(index_key, None)
        if stale_case_ids:
            self._mark_dirty()
        return stale_case_ids

    def _trim_cases(self, *, mark_dirty: bool = True) -> None:
        if len(self._cases) <= self.max_tracked:
            return
        ordered_ids = sorted(
            self._cases,
            key=lambda case_id: int(self._cases[case_id].asset_case_updated_at or 0),
        )
        removed = False
        for case_id in ordered_ids[: max(len(self._cases) - self.max_tracked, 0)]:
            case = self._cases.pop(case_id, None)
            if case is None:
                continue
            index_key = (case.chain, case.asset_symbol, case.asset_case_direction, case.venue_family)
            queue = self._index.get(index_key)
            if queue is None:
                continue
            self._index[index_key] = deque([item for item in queue if item != case_id], maxlen=self.max_tracked)
            if not self._index[index_key]:
                self._index.pop(index_key, None)
            removed = True
        if removed and mark_dirty:
            self._mark_dirty()

    def _mark_dirty(self) -> None:
        self._dirty = True

    def _effective_case_window(self, case: AssetCase) -> int:
        if lp_stage_rank(case.asset_case_stage) >= lp_stage_rank("confirm") or bool(case.had_prealert):
            return max(int(self.window_sec), 300)
        return int(self.window_sec)

    def _serialize_case(self, case: AssetCase) -> dict:
        return {
            "asset_case_id": case.asset_case_id,
            "asset_case_key": case.asset_case_key,
            "chain": case.chain,
            "asset_symbol": case.asset_symbol,
            "direction_bucket": case.asset_case_direction,
            "stage": case.asset_case_stage,
            "confidence": round(float(case.asset_case_confidence or 0.0), 4),
            "primary_pair": case.asset_case_primary_pair,
            "supporting_pairs": list(case.asset_case_supporting_pairs),
            "evidence_summary": case.asset_case_evidence_summary,
            "started_at": int(case.asset_case_started_at or 0),
            "updated_at": int(case.asset_case_updated_at or 0),
            "last_signal_at": int(case.last_signal_at or 0),
            "last_stage_transition_at": int(case.last_stage_transition_at or 0),
            "aggregate_metrics": dict(case.aggregate_metrics),
            "market_context_snapshot": dict(case.market_context_snapshot),
            "quality_snapshot": dict(case.quality_snapshot),
            "stage_history": list(case.stage_history),
            "pool_addresses": list(case.pool_addresses),
            "event_ids": list(case.event_ids),
            "venue_family": case.venue_family,
            "had_prealert": bool(case.had_prealert),
            "first_prealert_at": int(case.first_prealert_at or 0),
            "last_prealert_signal_id": case.last_prealert_signal_id,
            "last_prealert_delivery_allowed": case.last_prealert_delivery_allowed,
            "last_prealert_delivery_block_reason": case.last_prealert_delivery_block_reason,
            "prealert_upgraded_to_confirm_at": int(case.prealert_upgraded_to_confirm_at or 0),
            "prealert_to_confirm_sec": case.prealert_to_confirm_sec,
        }

    def _deserialize_case(self, payload: dict) -> AssetCase | None:
        if not isinstance(payload, dict):
            return None
        asset_case_id = str(payload.get("asset_case_id") or "").strip()
        asset_case_key = str(payload.get("asset_case_key") or "").strip()
        if not asset_case_id or not asset_case_key:
            return None
        return AssetCase(
            asset_case_id=asset_case_id,
            asset_case_key=asset_case_key,
            asset_case_stage=str(payload.get("stage") or "prealert"),
            asset_case_direction=str(payload.get("direction_bucket") or ""),
            asset_case_confidence=float(payload.get("confidence") or 0.0),
            asset_case_primary_pair=str(payload.get("primary_pair") or ""),
            asset_case_supporting_pairs=list(payload.get("supporting_pairs") or []),
            asset_case_evidence_summary=str(payload.get("evidence_summary") or ""),
            asset_case_started_at=int(payload.get("started_at") or 0),
            asset_case_updated_at=int(payload.get("updated_at") or 0),
            asset_symbol=str(payload.get("asset_symbol") or ""),
            chain=str(payload.get("chain") or "ethereum"),
            venue_family=str(payload.get("venue_family") or "onchain_lp"),
            stage_history=list(payload.get("stage_history") or []),
            pool_addresses=list(payload.get("pool_addresses") or []),
            event_ids=list(payload.get("event_ids") or []),
            last_signal_at=int(payload.get("last_signal_at") or payload.get("updated_at") or 0),
            last_stage_transition_at=int(payload.get("last_stage_transition_at") or payload.get("updated_at") or 0),
            aggregate_metrics=dict(payload.get("aggregate_metrics") or {}),
            market_context_snapshot=dict(payload.get("market_context_snapshot") or {}),
            quality_snapshot=dict(payload.get("quality_snapshot") or {}),
            had_prealert=bool(payload.get("had_prealert")),
            first_prealert_at=int(payload.get("first_prealert_at") or 0),
            last_prealert_signal_id=str(payload.get("last_prealert_signal_id") or ""),
            last_prealert_delivery_allowed=payload.get("last_prealert_delivery_allowed"),
            last_prealert_delivery_block_reason=str(payload.get("last_prealert_delivery_block_reason") or ""),
            prealert_upgraded_to_confirm_at=int(payload.get("prealert_upgraded_to_confirm_at") or 0),
            prealert_to_confirm_sec=payload.get("prealert_to_confirm_sec"),
            restored_from_cache=True,
        )

    def _migrate_cases(self, payload: dict, *, schema_version: str) -> list[dict] | None:
        if schema_version == self.schema_version:
            cases = payload.get("cases") or []
            return cases if isinstance(cases, list) else []
        return None

    def _market_context_snapshot(self, event, signal) -> dict:
        source_payload = getattr(signal, "context", {}) or getattr(event, "metadata", {}) or {}
        return {
            "market_context_source": str(source_payload.get("market_context_source") or ""),
            "market_context_venue": str(source_payload.get("market_context_venue") or ""),
            "alert_relative_timing": str(source_payload.get("alert_relative_timing") or ""),
            "market_context_attempted": bool(source_payload.get("market_context_attempted")),
            "market_context_requested_symbol": str(source_payload.get("market_context_requested_symbol") or ""),
            "market_context_resolved_symbol": str(source_payload.get("market_context_resolved_symbol") or ""),
            "market_context_failure_reason": str(source_payload.get("market_context_failure_reason") or ""),
            "market_context_failure_stage": str(source_payload.get("market_context_failure_stage") or ""),
            "basis_bps": source_payload.get("basis_bps"),
            "perp_mark_price": source_payload.get("perp_mark_price"),
            "perp_index_price": source_payload.get("perp_index_price"),
            "spot_reference_price": source_payload.get("spot_reference_price"),
        }

    def _quality_snapshot(self, event, signal) -> dict:
        source_payload = getattr(signal, "context", {}) or getattr(event, "metadata", {}) or {}
        return {
            "pool_quality_score": source_payload.get("pool_quality_score"),
            "pair_quality_score": source_payload.get("pair_quality_score"),
            "asset_case_quality_score": source_payload.get("asset_case_quality_score"),
            "prealert_precision_score": source_payload.get("prealert_precision_score"),
            "climax_reversal_score": source_payload.get("climax_reversal_score"),
            "fastlane_roi_score": source_payload.get("fastlane_roi_score"),
            "lp_confirm_quality": str(source_payload.get("lp_confirm_quality") or ""),
            "lp_absorption_context": str(source_payload.get("lp_absorption_context") or ""),
            "trade_action_key": str(source_payload.get("trade_action_key") or ""),
            "trade_action_label": str(source_payload.get("trade_action_label") or ""),
            "quality_score_brief": str(source_payload.get("quality_score_brief") or ""),
        }
