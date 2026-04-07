from collections import defaultdict
from dataclasses import asdict
import json
from pathlib import Path
import time

from models import AddressIntel, Event


PERSISTED_EXCHANGE_ADJACENT_VERSION = 1


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
        persist_interval_sec: int = 20,
    ) -> None:
        self.watch_addresses = {str(addr).lower() for addr in (watch_addresses or set()) if addr}
        self.max_counterparties = int(max_counterparties)
        self.candidate_threshold = float(candidate_threshold)
        self._intel_by_address: dict[str, AddressIntel] = {}
        self._display_hints_by_address: dict[str, dict] = {}
        self._persist_interval_sec = max(int(persist_interval_sec or 20), 1)
        self._persisted_exchange_adjacent_path: Path | None = None
        self._last_persisted_exchange_adjacent_at = 0
        self._last_persisted_exchange_adjacent_signature = ""

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

        self.maybe_persist_exchange_adjacent()

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
        self._expire_display_hints()
        intel = self._intel_by_address.get(str(address or "").lower())
        hint = self._display_hints_by_address.get(str(address or "").lower())
        if not intel and not hint:
            return {}

        patch = {}
        if intel:
            patch.update({
                "intelligence_status": intel.candidate_status,
                "suspected_role": intel.suspected_role,
                "role_confidence": round(float(intel.role_confidence or 0.0), 3),
                "candidate_score": round(float(intel.candidate_score or 0.0), 2),
                "first_seen_ts": int(intel.first_seen_ts or 0),
                "last_seen_ts": int(intel.last_seen_ts or 0),
            })
        if hint:
            patch.update({
                "display_hint_label": str(hint.get("display_hint_label") or ""),
                "display_hint_reason": str(hint.get("display_hint_reason") or ""),
                "display_hint_anchor_label": str(hint.get("display_hint_anchor_label") or ""),
                "display_hint_anchor_address": str(hint.get("display_hint_anchor_address") or ""),
                "display_hint_usd_value": round(float(hint.get("display_hint_usd_value") or 0.0), 2),
                "display_hint_token_symbol": str(hint.get("display_hint_token_symbol") or ""),
                "display_hint_expire_at": int(hint.get("display_hint_expire_at") or 0),
                "display_hint_source": str(hint.get("display_hint_source") or "adjacent_watch"),
            })
        return patch

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

    def mark_display_hint(
        self,
        address: str,
        display_hint_label: str,
        expire_at: int,
        display_hint_reason: str = "",
        display_hint_anchor_label: str = "",
        display_hint_anchor_address: str = "",
        display_hint_usd_value: float = 0.0,
        display_hint_token_symbol: str = "",
        display_hint_source: str = "adjacent_watch",
    ) -> dict | None:
        addr = str(address or "").lower()
        if not addr:
            return None
        payload = {
            "display_hint_label": str(display_hint_label or ""),
            "display_hint_reason": str(display_hint_reason or ""),
            "display_hint_anchor_label": str(display_hint_anchor_label or ""),
            "display_hint_anchor_address": str(display_hint_anchor_address or "").lower(),
            "display_hint_usd_value": float(display_hint_usd_value or 0.0),
            "display_hint_token_symbol": str(display_hint_token_symbol or ""),
            "display_hint_expire_at": int(expire_at or 0),
            "display_hint_source": str(display_hint_source or "adjacent_watch"),
        }
        self._display_hints_by_address[addr] = payload
        return dict(payload)

    def clear_display_hint(self, address: str) -> None:
        addr = str(address or "").lower()
        if not addr:
            return
        self._display_hints_by_address.pop(addr, None)

    def is_persistable_exchange_adjacent(self, intel_or_address) -> bool:
        if isinstance(intel_or_address, AddressIntel):
            intel = intel_or_address
        else:
            intel = self._intel_by_address.get(str(intel_or_address or "").lower())
        if intel is None:
            return False
        return (
            str(intel.suspected_role or "") == "exchange_adjacent"
            and int(intel.exchange_interactions or 0) >= 3
            and str(intel.candidate_status or "") == "watch_adjacent"
        )

    def export_persistable_exchange_adjacent(self) -> list[dict]:
        items = []
        for address, intel in self._intel_by_address.items():
            if not self.is_persistable_exchange_adjacent(intel):
                continue
            items.append({
                "address": address,
                "suspected_role": str(intel.suspected_role or ""),
                "role_confidence": round(float(intel.role_confidence or 0.0), 3),
                "candidate_score": round(float(intel.candidate_score or 0.0), 2),
                "candidate_status": str(intel.candidate_status or ""),
                "exchange_interactions": int(intel.exchange_interactions or 0),
                "router_interactions": int(intel.router_interactions or 0),
                "protocol_interactions": int(intel.protocol_interactions or 0),
                "seen_count": int(intel.seen_count or 0),
                "avg_usd": round(float(intel.avg_usd or 0.0), 4),
                "max_usd": round(float(intel.max_usd or 0.0), 4),
                "same_block_with_watch_count": int(intel.same_block_with_watch_count or 0),
                "same_token_resonance_count": int(intel.same_token_resonance_count or 0),
                "first_seen_ts": int(intel.first_seen_ts or 0),
                "last_seen_ts": int(intel.last_seen_ts or 0),
                "seen_tokens": sorted(
                    str(token).lower()
                    for token in (intel.seen_tokens or set())
                    if token
                ),
                "top_counterparties": [
                    [str(item["address"] or "").lower(), int(item["count"] or 0)]
                    for item in self._sorted_counterparties(intel)
                    if item.get("address")
                ],
            })
        items.sort(
            key=lambda item: (
                -float(item.get("candidate_score") or 0.0),
                -int(item.get("exchange_interactions") or 0),
                item.get("address", ""),
            )
        )
        return items

    def export_persisted_exchange_adjacent_payload(self) -> dict:
        return {
            "version": PERSISTED_EXCHANGE_ADJACENT_VERSION,
            "updated_at": int(time.time()),
            "addresses": self.export_persistable_exchange_adjacent(),
        }

    def load_persisted_exchange_adjacent(self, path) -> list[dict]:
        resolved_path = self._resolve_persist_path(path)
        self._persisted_exchange_adjacent_path = resolved_path
        if not resolved_path.exists():
            self._last_persisted_exchange_adjacent_signature = self._persistable_exchange_adjacent_signature()
            return []

        try:
            with resolved_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            print(f"persisted exchange_adjacent 加载失败: {e}")
            return []

        if not isinstance(payload, dict):
            print("persisted exchange_adjacent 加载失败: payload 不是对象")
            return []

        loaded_addresses = []
        for item in payload.get("addresses") or []:
            try:
                intel = self._intel_from_persisted_item(item)
            except Exception as e:
                print(f"persisted exchange_adjacent 条目恢复失败: {e}")
                continue
            if intel is None:
                continue
            self._intel_by_address[intel.address] = intel
            loaded_addresses.append(self.get_intelligence(intel.address) or {"address": intel.address})

        self._last_persisted_exchange_adjacent_signature = self._persistable_exchange_adjacent_signature()
        self._last_persisted_exchange_adjacent_at = int(time.time())
        return loaded_addresses

    def save_persisted_exchange_adjacent(self, path=None) -> bool:
        resolved_path = self._resolve_persist_path(path)
        payload = self.export_persisted_exchange_adjacent_payload()
        try:
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            with resolved_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
        except Exception as e:
            print(f"persisted exchange_adjacent 保存失败: {e}")
            return False

        self._persisted_exchange_adjacent_path = resolved_path
        self._last_persisted_exchange_adjacent_at = int(time.time())
        self._last_persisted_exchange_adjacent_signature = self._persistable_exchange_adjacent_signature(
            payload.get("addresses") or []
        )
        return True

    def maybe_persist_exchange_adjacent(self, path=None, force: bool = False) -> bool:
        resolved_path = self._resolve_persist_path(path, allow_empty=True)
        if resolved_path is None:
            return False

        current_signature = self._persistable_exchange_adjacent_signature()
        now_ts = int(time.time())
        if not force:
            if current_signature == self._last_persisted_exchange_adjacent_signature:
                return False
            if now_ts - int(self._last_persisted_exchange_adjacent_at or 0) < self._persist_interval_sec:
                return False
        return self.save_persisted_exchange_adjacent(resolved_path)

    def _expire_display_hints(self) -> None:
        now_ts = int(time.time())
        for address, hint in list(self._display_hints_by_address.items()):
            if int(hint.get("display_hint_expire_at") or 0) > now_ts:
                continue
            self._display_hints_by_address.pop(address, None)

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

    def _resolve_persist_path(self, path=None, allow_empty: bool = False) -> Path | None:
        candidate = path or self._persisted_exchange_adjacent_path
        if not candidate:
            return None if allow_empty else Path("persisted_exchange_adjacent.json")
        return Path(candidate)

    def _persistable_exchange_adjacent_signature(self, items: list[dict] | None = None) -> str:
        payload = items if items is not None else self.export_persistable_exchange_adjacent()
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def _intel_from_persisted_item(self, item: dict) -> AddressIntel | None:
        if not isinstance(item, dict):
            return None

        address = str(item.get("address") or "").lower()
        if not address:
            return None

        counterparties = defaultdict(int)
        for entry in item.get("top_counterparties") or []:
            if isinstance(entry, dict):
                cp_address = str(entry.get("address") or "").lower()
                count = int(entry.get("count") or 0)
            elif isinstance(entry, (list, tuple)) and entry:
                cp_address = str(entry[0] or "").lower()
                count = int(entry[1] or 0) if len(entry) > 1 else 0
            else:
                continue
            if cp_address:
                counterparties[cp_address] = max(int(counterparties.get(cp_address) or 0), count)

        intel = AddressIntel(
            address=address,
            first_seen_ts=int(item.get("first_seen_ts") or 0),
            last_seen_ts=int(item.get("last_seen_ts") or 0),
            seen_count=int(item.get("seen_count") or 0),
            seen_tokens={
                str(token).lower()
                for token in (item.get("seen_tokens") or [])
                if token
            },
            top_counterparties=counterparties,
            suspected_role=str(item.get("suspected_role") or "unknown"),
            role_confidence=float(item.get("role_confidence") or 0.0),
            exchange_interactions=int(item.get("exchange_interactions") or 0),
            router_interactions=int(item.get("router_interactions") or 0),
            protocol_interactions=int(item.get("protocol_interactions") or 0),
            avg_usd=float(item.get("avg_usd") or 0.0),
            max_usd=float(item.get("max_usd") or 0.0),
            same_block_with_watch_count=int(item.get("same_block_with_watch_count") or 0),
            same_token_resonance_count=int(item.get("same_token_resonance_count") or 0),
            candidate_score=float(item.get("candidate_score") or 0.0),
            candidate_status=str(item.get("candidate_status") or "observed"),
        )
        self._trim_counterparties(intel)
        return intel

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
