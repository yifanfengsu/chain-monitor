from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import hashlib

from config import LP_ASSET_CASE_MAX_SUPPORTING_PAIRS, LP_ASSET_CASE_MAX_TRACKED, LP_ASSET_CASE_WINDOW_SEC
from lp_product_helpers import (
    asset_symbol_from_event,
    lp_direction_bucket_for_event,
    lp_stage_rank,
    pair_label_from_event,
    venue_family_from_event,
)


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


class AssetCaseManager:
    def __init__(
        self,
        *,
        window_sec: int = LP_ASSET_CASE_WINDOW_SEC,
        max_supporting_pairs: int = LP_ASSET_CASE_MAX_SUPPORTING_PAIRS,
        max_tracked: int = LP_ASSET_CASE_MAX_TRACKED,
    ) -> None:
        self.window_sec = max(int(window_sec), 30)
        self.max_supporting_pairs = max(int(max_supporting_pairs), 2)
        self.max_tracked = max(int(max_tracked), 32)
        self._cases: dict[str, AssetCase] = {}
        self._index: dict[tuple[str, str, str, str], deque[str]] = defaultdict(lambda: deque(maxlen=self.max_tracked))

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
        payload = self._payload(case, event=event, signal=signal, gate_metrics=gate_metrics)
        getattr(event, "metadata", {}).update(payload)
        getattr(signal, "metadata", {}).update(payload)
        getattr(signal, "context", {}).update(payload)
        return payload

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
        candidates = [
            self._cases[candidate_id]
            for candidate_id in candidate_ids
            if candidate_id in self._cases
            and ts - int(self._cases[candidate_id].asset_case_updated_at or 0) <= self.window_sec
        ]
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
        if lp_stage_rank(stage) >= lp_stage_rank(case.asset_case_stage):
            case.asset_case_stage = stage or case.asset_case_stage
        case.asset_case_confidence = round(max(float(case.asset_case_confidence or 0.0), confidence), 3)
        case.asset_case_updated_at = int(getattr(event, "ts", 0) or case.asset_case_updated_at)
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
        volume_surge_ratio = float(gate_metrics.get("lp_pool_volume_surge_ratio") or 0.0)
        same_pool_continuity = int(gate_metrics.get("lp_same_pool_continuity") or 0)
        multi_pool_resonance = int(gate_metrics.get("lp_multi_pool_resonance") or 0)
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

    def _payload(self, case: AssetCase, *, event, signal, gate_metrics: dict) -> dict:
        pair_count = len(case.asset_case_supporting_pairs)
        pool_count = len(case.pool_addresses)
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
            "asset_case_label": case.asset_symbol,
            "asset_symbol": case.asset_symbol,
            "asset_case_aggregated": pair_count >= 2 or pool_count >= 2,
            "asset_case_multi_pool": pair_count >= 2 or pool_count >= 2,
            "asset_case_stage_rank": lp_stage_rank(case.asset_case_stage),
            "asset_case_pool_fallback_only": pair_count <= 1 and case.asset_case_stage == "prealert",
            "asset_case_evidence_pack": case.asset_case_evidence_summary,
            "asset_case_stage_history": list(case.stage_history),
            "asset_case_last_pair": pair_label_from_event(event),
            "asset_case_last_signal_id": str(getattr(signal, "signal_id", "") or ""),
            "asset_case_window_sec": int(self.window_sec),
            "asset_case_venue_family": case.venue_family,
        }

    def _prune(self, *, now_ts: int) -> None:
        stale_case_ids = [
            case_id
            for case_id, case in self._cases.items()
            if now_ts - int(case.asset_case_updated_at or 0) > self.window_sec
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

    def _trim_cases(self) -> None:
        if len(self._cases) <= self.max_tracked:
            return
        ordered_ids = sorted(
            self._cases,
            key=lambda case_id: int(self._cases[case_id].asset_case_updated_at or 0),
        )
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

    def snapshot(self) -> list[dict]:
        ordered = sorted(
            self._cases.values(),
            key=lambda item: (-int(item.asset_case_updated_at or 0), -lp_stage_rank(item.asset_case_stage)),
        )
        return [asdict(item) for item in ordered]
