from collections import defaultdict
from dataclasses import asdict
import time

from models import AddressIntel, Event


class AddressIntelligenceManager:
    """
    运行中动态采集地址画像：
    - 高频对手方
    - 下一跳地址
    - 同 token 短时共振地址
    - 与交易所 / 路由 / 协议反复交互的地址

    这里只维护内存态候选池，不自动提升为 WATCH 地址。
    """

    def __init__(
        self,
        watch_addresses: set[str] | None = None,
        max_counterparties: int = 12,
        candidate_threshold: float = 28.0,
    ) -> None:
        self.watch_addresses = {str(addr).lower() for addr in (watch_addresses or set()) if addr}
        self.max_counterparties = int(max_counterparties)
        self.candidate_threshold = float(candidate_threshold)
        self._intel_by_address: dict[str, AddressIntel] = {}

    def observe_event(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict | None,
        raw_item: dict | None = None,
        token_snapshot: dict | None = None,
    ) -> dict | None:
        """采集本次事件中的候选地址画像更新。"""
        ts = int(event.ts or time.time())
        usd_value = float(event.usd_value or 0.0)
        watch_address = str(event.address or "").lower()
        raw_item = raw_item or {}
        token_snapshot = token_snapshot or {}

        candidate_addresses = self._candidate_addresses(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            raw_item=raw_item,
            token_snapshot=token_snapshot,
        )
        if not candidate_addresses:
            return None

        updates = []
        exchange_related = bool(parsed.get("is_exchange_related"))
        router_related = bool(parsed.get("is_router_related"))
        protocol_related = bool(parsed.get("is_protocol_related"))
        resonance_addresses = self._resonance_addresses(token_snapshot, event)
        touched_watch_count = len(raw_item.get("touched_watch_addresses") or [])

        for candidate_address, reason in candidate_addresses:
            if not candidate_address or candidate_address == watch_address:
                continue
            if candidate_address in self.watch_addresses:
                continue

            intel = self._ensure_intel(candidate_address, ts)
            intel.last_seen_ts = ts
            intel.seen_count += 1
            if event.token:
                intel.seen_tokens.add(str(event.token).lower())

            if watch_address:
                intel.top_counterparties[watch_address] += 1

            if usd_value > 0:
                total_usd = intel.avg_usd * max(intel.seen_count - 1, 0)
                intel.avg_usd = (total_usd + usd_value) / max(intel.seen_count, 1)
                intel.max_usd = max(float(intel.max_usd or 0.0), usd_value)

            if reason in {"counterparty", "next_hop", "participant"}:
                intel.same_block_with_watch_count += max(touched_watch_count, 1)

            if candidate_address in resonance_addresses:
                intel.same_token_resonance_count += 1

            if exchange_related:
                intel.exchange_interactions += 1
            if router_related and reason in {"next_hop", "participant"}:
                intel.router_interactions += 1
            if protocol_related and reason in {"next_hop", "participant"}:
                intel.protocol_interactions += 1

            suspected_role, role_confidence = self._infer_role(intel)
            intel.suspected_role = suspected_role
            intel.role_confidence = role_confidence
            intel.candidate_score = self._candidate_score(intel)
            intel.candidate_status = self._candidate_status(intel)
            self._trim_counterparties(intel)
            updates.append(candidate_address)

        if not updates:
            return None

        return {
            "updated_addresses": updates,
            "primary_candidates": [
                self.get_intelligence(address)
                for address in updates[:3]
            ],
            "count": len(updates),
        }

    def get_intelligence(self, address: str) -> dict | None:
        intel = self._intel_by_address.get(str(address or "").lower())
        if not intel:
            return None

        payload = asdict(intel)
        payload["seen_tokens"] = sorted(intel.seen_tokens)
        payload["top_counterparties"] = self._sorted_counterparties(intel)
        return payload

    def get_meta_patch(self, address: str) -> dict:
        intel = self._intel_by_address.get(str(address or "").lower())
        if not intel:
            return {}

        return {
            "intelligence_status": intel.candidate_status,
            "suspected_role": intel.suspected_role,
            "role_confidence": round(float(intel.role_confidence or 0.0), 3),
            "candidate_score": round(float(intel.candidate_score or 0.0), 2),
            "first_seen_ts": int(intel.first_seen_ts or 0),
            "last_seen_ts": int(intel.last_seen_ts or 0),
        }

    def get_candidate_pool(self, min_score: float | None = None) -> list[dict]:
        threshold = float(min_score if min_score is not None else self.candidate_threshold)
        items = []
        for address, intel in self._intel_by_address.items():
            if float(intel.candidate_score or 0.0) < threshold:
                continue
            items.append(self.get_intelligence(address))
        items.sort(key=lambda item: float(item.get("candidate_score") or 0.0), reverse=True)
        return items

    def snapshot(self) -> dict:
        return {
            "size": len(self._intel_by_address),
            "candidates": self.get_candidate_pool(),
        }

    def _candidate_addresses(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict | None,
        raw_item: dict,
        token_snapshot: dict,
    ) -> list[tuple[str, str]]:
        addresses = []
        seen = set()

        def add(address: str | None, reason: str) -> None:
            addr = str(address or "").lower()
            if not addr or addr in seen:
                return
            seen.add(addr)
            addresses.append((addr, reason))

        if watch_context:
            add(watch_context.get("counterparty"), "counterparty")

        raw = parsed.get("raw") or {}
        add(raw.get("counterparty"), "counterparty")
        add(raw.get("from"), "participant")
        add(raw.get("to"), "participant")
        add(raw.get("quote_from"), "next_hop")
        add(raw.get("quote_to"), "next_hop")
        add(raw.get("token_from"), "next_hop")
        add(raw.get("token_to"), "next_hop")

        for address in raw_item.get("participant_addresses") or []:
            add(address, "participant")
        for address in raw_item.get("next_hop_addresses") or []:
            add(address, "next_hop")

        for address in self._resonance_addresses(token_snapshot, event):
            add(address, "resonance")

        return addresses

    def _resonance_addresses(self, token_snapshot: dict, event: Event) -> set[str]:
        windows = token_snapshot.get("windows") or {}
        recent_events = windows.get("15m", {}).get("recent") or []
        direction_bucket = self._direction_bucket(event.side)
        addresses = set()
        for item in recent_events:
            if not isinstance(item, Event):
                continue
            if item.address == event.address:
                continue
            if self._direction_bucket(item.side) != direction_bucket:
                continue
            if (item.token or "").lower() != (event.token or "").lower():
                continue
            addresses.add(str(item.address or "").lower())
        return addresses

    def _ensure_intel(self, address: str, ts: int) -> AddressIntel:
        intel = self._intel_by_address.get(address)
        if intel is not None:
            return intel

        intel = AddressIntel(
            address=address,
            first_seen_ts=ts,
            last_seen_ts=ts,
            candidate_status="observed",
        )
        self._intel_by_address[address] = intel
        return intel

    def _infer_role(self, intel: AddressIntel) -> tuple[str, float]:
        if intel.router_interactions >= max(intel.exchange_interactions, intel.protocol_interactions, 3):
            confidence = min(0.92, 0.45 + intel.router_interactions * 0.08)
            return "router_adjacent", round(confidence, 3)

        if intel.protocol_interactions >= max(intel.exchange_interactions, intel.router_interactions, 3):
            confidence = min(0.9, 0.42 + intel.protocol_interactions * 0.08)
            return "protocol_adjacent", round(confidence, 3)

        if intel.exchange_interactions >= 3:
            confidence = min(0.9, 0.4 + intel.exchange_interactions * 0.07)
            return "exchange_adjacent", round(confidence, 3)

        if intel.same_token_resonance_count >= 3:
            confidence = min(0.85, 0.38 + intel.same_token_resonance_count * 0.08)
            return "resonant_counterparty", round(confidence, 3)

        if intel.seen_count >= 3:
            confidence = min(0.8, 0.3 + intel.seen_count * 0.06)
            return "recurring_counterparty", round(confidence, 3)

        return "unknown", 0.2

    def _candidate_score(self, intel: AddressIntel) -> float:
        score = 0.0
        score += min(24.0, intel.seen_count * 3.5)
        score += min(18.0, intel.same_block_with_watch_count * 2.5)
        score += min(18.0, intel.same_token_resonance_count * 4.0)
        score += min(12.0, intel.exchange_interactions * 2.0)
        score += min(10.0, intel.router_interactions * 2.5)
        score += min(10.0, intel.protocol_interactions * 2.5)
        score += min(12.0, float(intel.avg_usd or 0.0) / 5000.0)
        score += min(14.0, float(intel.max_usd or 0.0) / 10000.0)
        score += float(intel.role_confidence or 0.0) * 10.0
        return round(min(100.0, score), 2)

    def _candidate_status(self, intel: AddressIntel) -> str:
        score = float(intel.candidate_score or 0.0)
        if score >= 60:
            return "watch_adjacent"
        if score >= self.candidate_threshold:
            return "candidate"
        return "observed"

    def _sorted_counterparties(self, intel: AddressIntel) -> list[dict]:
        ordered = sorted(
            intel.top_counterparties.items(),
            key=lambda item: (-int(item[1]), item[0]),
        )
        return [
            {"address": address, "count": int(count)}
            for address, count in ordered[: self.max_counterparties]
        ]

    def _trim_counterparties(self, intel: AddressIntel) -> None:
        ordered = sorted(
            intel.top_counterparties.items(),
            key=lambda item: (-int(item[1]), item[0]),
        )
        if len(ordered) <= self.max_counterparties:
            return
        intel.top_counterparties = defaultdict(int, ordered[: self.max_counterparties])

    def _direction_bucket(self, side: str | None) -> str:
        normalized = str(side or "")
        if normalized in {"买入", "流入"}:
            return "buy"
        if normalized in {"卖出", "流出"}:
            return "sell"
        return "other"
