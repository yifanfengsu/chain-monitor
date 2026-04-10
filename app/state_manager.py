from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import math
import time

from config import (
    ADJACENT_WATCH_RUNTIME_PRIORITY,
    ADJACENT_WATCH_RUNTIME_STRATEGY_ROLE,
    LP_BURST_WINDOW_SEC,
    LP_TREND_CONTINUATION_MIN_SCORE,
    LP_TREND_REVERSAL_MIN_SCORE,
    LP_TREND_STATE_WINDOW_SEC,
)
from models import AdjacentWatchState, Event


TIME_WINDOWS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "24h": 86400,
}

HIGH_QUALITY_STRATEGY_ROLES = {
    "smart_money_wallet",
    "alpha_wallet",
    "market_maker_wallet",
    "celebrity_wallet",
}
SMART_MONEY_STRATEGY_ROLES = {
    "smart_money_wallet",
    "alpha_wallet",
    "celebrity_wallet",
}
_PRIMARY_STATE_MANAGER = None


@dataclass
class AddressState:
    recent_events: deque = field(default_factory=lambda: deque(maxlen=300))
    # 保留历史命名兼容：holding_delta 代表 token 持仓数量变化。
    holding_delta: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # 近似成本（USD），用于简单 PnL 与持仓 USD 化。
    holding_cost_usd: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    realized_pnl_usd: float = 0.0
    first_seen_ts: int | None = None
    last_seen_ts: int | None = None


@dataclass
class TokenState:
    recent_events: deque = field(default_factory=lambda: deque(maxlen=500))
    net_flow_usd: float = 0.0
    first_seen_ts: int | None = None
    last_seen_ts: int | None = None
    # 保存 token 参与者短时行为，用于轻量共振判断。
    participant_events: deque = field(default_factory=lambda: deque(maxlen=4000))


@dataclass
class PoolState:
    recent_events: deque = field(default_factory=lambda: deque(maxlen=500))
    first_seen_ts: int | None = None
    last_seen_ts: int | None = None


class StateManager:
    """
    全局状态层：
    - 统一维护地址状态与 token 状态
    - 支持多时间窗口快照（1m/5m/15m/1h/24h）
    - 提供简单仓位成本与 realized PnL
    - 对 token proxy 指标保留新旧命名兼容
    - 为上层提供轻量序列确认与 token 共振统计基础
    """

    def __init__(self, max_events_per_address: int = 300, max_events_per_token: int = 500) -> None:
        global _PRIMARY_STATE_MANAGER
        self.max_events_per_address = max_events_per_address
        self.max_events_per_token = max_events_per_token
        self.lp_trend_state_window_sec = int(LP_TREND_STATE_WINDOW_SEC)
        self.lp_trend_continuation_min_score = float(LP_TREND_CONTINUATION_MIN_SCORE)
        self.lp_trend_reversal_min_score = float(LP_TREND_REVERSAL_MIN_SCORE)
        self._address_states: dict[str, AddressState] = {}
        self._token_states: dict[str, TokenState] = {}
        self._pool_states: dict[str, PoolState] = {}
        self._last_signal_ts_by_address: dict[str, int] = {}
        self._last_signal_ts_by_key: dict[str, int] = {}
        self._recent_counterparties_by_address: dict[str, deque] = {}
        self._address_interaction_stats: dict[str, dict[str, dict]] = {}
        self._open_cases_by_address: dict[str, set[str]] = defaultdict(set)
        self._open_cases_by_token: dict[str, set[str]] = defaultdict(set)
        self._adjacent_watch_states: dict[str, AdjacentWatchState] = {}
        _PRIMARY_STATE_MANAGER = self

    def apply_event(self, event: Event) -> None:
        self._apply_address_event(event)
        self._apply_token_event(event)
        self._apply_pool_event(event)

    def get_address_snapshot(self, address: str, window_sec: int = 3600, now_ts: int | None = None) -> dict:
        addr = (address or "").lower()
        state = self._address_states.get(addr)
        if state is None:
            return self._empty_address_snapshot(addr, window_sec)

        reference_now = int(now_ts or state.last_seen_ts or 0)
        recent_all = list(state.recent_events)
        recent = self._filter_events_by_window(recent_all, reference_now, window_sec)
        base = self._address_summary(recent, window_sec)

        windows = {}
        for window_name, sec in TIME_WINDOWS.items():
            window_events = self._filter_events_by_window(recent_all, reference_now, sec)
            windows[window_name] = self._address_summary(window_events, sec)

        holding_delta = dict(state.holding_delta)
        holding_cost_usd = dict(state.holding_cost_usd)
        cost_basis = {}
        holding_delta_usd = {}
        for token, qty in holding_delta.items():
            qty = float(qty or 0.0)
            cost = float(holding_cost_usd.get(token, 0.0))
            avg_cost = (cost / qty) if qty > 0 else 0.0
            cost_basis[token] = avg_cost
            holding_delta_usd[token] = qty * avg_cost

        return {
            "address": addr,
            **base,
            "window_sec": window_sec,
            "windows": windows,
            "holding_delta": holding_delta,
            "holding_cost_usd": holding_cost_usd,
            "holding_delta_usd": holding_delta_usd,
            "cost_basis": cost_basis,
            "realized_pnl_usd": float(state.realized_pnl_usd),
            "recent_counterparties": self.get_recent_counterparties(addr, limit=10),
            "interaction_stats": self._address_interaction_stats.get(addr, {}),
            "open_case_ids": sorted(self._open_cases_by_address.get(addr, set())),
        }

    def get_token_snapshot(self, token: str | None, window_sec: int = 3600, now_ts: int | None = None) -> dict:
        tok = (token or "").lower()
        if not tok:
            return self._empty_token_snapshot(tok)

        state = self._token_states.get(tok)
        if state is None:
            return self._empty_token_snapshot(tok)

        reference_now = int(now_ts or state.last_seen_ts or 0)
        recent_all = list(state.recent_events)
        participant_all = list(state.participant_events)

        recent = self._filter_events_by_window(recent_all, reference_now, window_sec)
        participant_recent = self._filter_participants_by_window(participant_all, reference_now, window_sec)
        base = self._token_summary(recent, participant_recent, window_sec)

        windows = {}
        participant_windows = {}
        for window_name, sec in TIME_WINDOWS.items():
            window_events = self._filter_events_by_window(recent_all, reference_now, sec)
            window_participants = self._filter_participants_by_window(participant_all, reference_now, sec)
            windows[window_name] = self._token_summary(window_events, window_participants, sec)
            participant_windows[window_name] = window_participants

        window_24h = windows["24h"]
        window_1h = windows["1h"]
        window_5m = windows["5m"]
        volume_24h_proxy_usd = float(window_24h["volume_usd"])
        liquidity_proxy_usd = self._estimate_liquidity_proxy(windows)
        holder_distribution_score = self._estimate_holder_distribution_score(windows)

        contract_age_days = 180
        first_seen_ts = int(state.first_seen_ts or reference_now or 0)
        if first_seen_ts and reference_now:
            observed_days = max(0, int((reference_now - first_seen_ts) / 86400))
            contract_age_days = max(180, observed_days)

        resonance_5m = self._resonance_summary(participant_windows.get("5m", []))
        resonance_15m = self._resonance_summary(participant_windows.get("15m", []))
        resonance_1h = self._resonance_summary(participant_windows.get("1h", []))

        return {
            "token": tok,
            **base,
            "windows": windows,
            # 统一后的 proxy 命名。
            "volume_24h_proxy_usd": volume_24h_proxy_usd,
            "liquidity_proxy_usd": liquidity_proxy_usd,
            # 兼容旧字段名，避免前两阶段调用直接失效。
            "volume_24h_usd": volume_24h_proxy_usd,
            "liquidity_usd": liquidity_proxy_usd,
            "holder_distribution_score": holder_distribution_score,
            "contract_age_days": contract_age_days,
            "swap_frequency": float(window_1h["swap_count"]),
            "unique_participants": int(window_24h["unique_participants"]),
            "buy_cluster_5m": int(window_5m["buy_count"]),
            "sell_cluster_5m": int(window_5m["sell_count"]),
            "token_net_flow_usd_5m": float(window_5m["net_flow_usd"]),
            "token_net_flow_usd_1h": float(window_1h["net_flow_usd"]),
            "token_net_flow_usd_24h": float(window_24h["net_flow_usd"]),
            "resonance_5m": resonance_5m,
            "resonance_15m": resonance_15m,
            "resonance_1h": resonance_1h,
            "multi_address_resonance_5m": bool(resonance_5m["multi_address_resonance"]),
            "smart_money_resonance_5m": bool(resonance_5m["smart_money_resonance"]),
            "high_quality_resonance_5m": bool(resonance_5m["high_quality_resonance"]),
            "leader_follow_resonance_5m": bool(resonance_5m["leader_follow_resonance"]),
            "resonance_score_5m": float(resonance_5m["resonance_score"]),
            "resonance_score_15m": float(resonance_15m["resonance_score"]),
            "open_case_ids": sorted(self._open_cases_by_token.get(tok, set())),
        }

    def get_pool_snapshot(self, pool_address: str, window_sec: int = 900, now_ts: int | None = None) -> dict:
        pool_key = str(pool_address or "").lower()
        state = self._pool_states.get(pool_key)
        if state is None:
            return self._empty_pool_snapshot(pool_key, window_sec)

        reference_now = int(now_ts or state.last_seen_ts or 0)
        recent_all = list(state.recent_events)
        recent = self._filter_events_by_window(recent_all, reference_now, window_sec)
        return self._pool_summary(pool_key, recent, window_sec)

    def get_lp_burst_snapshot(
        self,
        pool_address: str,
        direction: str,
        window_sec: int = LP_BURST_WINDOW_SEC,
        now_ts: int | None = None,
    ) -> dict:
        pool_key = str(pool_address or "").lower()
        direction_key = str(direction or "").strip().lower()
        state = self._pool_states.get(pool_key)
        if state is None:
            return self._empty_lp_burst_snapshot(pool_key, direction_key, window_sec)

        reference_now = int(now_ts or state.last_seen_ts or 0)
        recent_all = list(state.recent_events)
        recent = self._filter_events_by_window(recent_all, reference_now, window_sec)
        directional_events = [
            evt for evt in recent
            if self._lp_directional_bucket(evt) == direction_key
        ]
        return self._lp_burst_summary(pool_key, direction_key, directional_events, window_sec)

    def get_lp_trend_snapshot(
        self,
        pool_address: str,
        window_sec: int | None = None,
        now_ts: int | None = None,
        trend_pool_context: dict | None = None,
    ) -> dict:
        pool_key = str(pool_address or "").lower()
        effective_window_sec = int(window_sec or self.lp_trend_state_window_sec)
        state = self._pool_states.get(pool_key)
        if state is None:
            return self._empty_lp_trend_snapshot(pool_key, effective_window_sec, trend_pool_context)

        reference_now = int(now_ts or state.last_seen_ts or 0)
        recent_all = list(state.recent_events)
        recent = self._filter_events_by_window(recent_all, reference_now, effective_window_sec)
        directional_events = [
            evt for evt in recent
            if self._lp_directional_bucket(evt) in {"buy_pressure", "sell_pressure"}
        ]
        return self._lp_trend_summary(
            pool_address=pool_key,
            events=directional_events,
            window_sec=effective_window_sec,
            trend_pool_context=trend_pool_context,
        )

    def can_emit_signal(self, address: str, now_ts: int, cooldown_sec: int) -> bool:
        addr = (address or "").lower()
        if not addr:
            return False

        key = self._address_cooldown_key(addr)
        return self.can_emit_signal_by_key(key, now_ts, cooldown_sec)

    def mark_signal_emitted(self, address: str, ts: int) -> None:
        addr = (address or "").lower()
        if not addr:
            return
        key = self._address_cooldown_key(addr)
        self.mark_signal_emitted_by_key(key, ts)
        self._last_signal_ts_by_address[addr] = int(ts)

    def get_last_signal_ts(self, address: str) -> int | None:
        addr = (address or "").lower()
        return self._last_signal_ts_by_address.get(addr) or self._last_signal_ts_by_key.get(self._address_cooldown_key(addr))

    def get_cooldown_key(self, event: Event, intent_type: str | None = None) -> str:
        """默认 cooldown key：address + intent + token + side。"""
        case_family = str((event.metadata or {}).get("case_family") or "")
        if case_family == "downstream_counterparty_followup" and event.case_id:
            stage = str(
                (event.metadata or {}).get("downstream_followup_stage")
                or event.followup_stage
                or "followup_opened"
            )
            chain = str(event.chain or "ethereum").lower()
            return "|".join([chain, str(event.case_id), case_family, stage])
        lp_direction = self._lp_directional_bucket(event, intent_type=intent_type)
        if str(event.strategy_role or "") == "lp_pool" and lp_direction:
            pool_address = str(event.address or "").lower()
            if pool_address:
                return f"lp:{pool_address}:{lp_direction}"
        address = (event.address or "").lower()
        intent = str(intent_type or event.intent_type or "unknown_intent").lower()
        token = (event.token or "native").lower()
        side = str(event.side or "unknown")
        chain = str(event.chain or "ethereum").lower()
        return "|".join([chain, address, intent, token, side])

    def can_emit_signal_by_key(self, key: str, now_ts: int, cooldown_sec: int) -> bool:
        if not key:
            return False

        last_ts = self._last_signal_ts_by_key.get(key)
        if last_ts is None:
            return True

        return now_ts - last_ts >= max(int(cooldown_sec), 0)

    def mark_signal_emitted_by_key(self, key: str, ts: int) -> None:
        if not key:
            return
        self._last_signal_ts_by_key[key] = int(ts)

    def get_last_signal_ts_by_key(self, key: str) -> int | None:
        if not key:
            return None
        return self._last_signal_ts_by_key.get(key)

    def register_adjacent_watch(
        self,
        address: str,
        anchor_watch_address: str,
        anchor_label: str,
        root_tx_hash: str,
        token: str | None,
        anchor_usd_value: float,
        opened_at: int,
        active_until: int,
        cooling_until: int,
        closing_until: int,
        hop: int = 1,
        reason: str = "",
        strategy_hint: str = "",
        runtime_label_hint: str = "",
        metadata: dict | None = None,
    ) -> dict | None:
        addr = str(address or "").lower()
        anchor_address = str(anchor_watch_address or "").lower()
        if not addr or not anchor_address:
            return None

        now_ts = int(opened_at or time.time())
        self.expire_adjacent_watch(None, now_ts=now_ts)
        payload = dict(metadata or {})
        existing = self._adjacent_watch_states.get(addr)
        if existing is not None and int(existing.closing_until or 0) > now_ts:
            existing.anchor_watch_address = existing.anchor_watch_address or anchor_address
            existing.anchor_label = anchor_label or existing.anchor_label
            existing.root_tx_hash = root_tx_hash or existing.root_tx_hash
            existing.token = token or existing.token
            existing.anchor_usd_value = max(float(existing.anchor_usd_value or 0.0), float(anchor_usd_value or 0.0))
            existing.opened_at = min(int(existing.opened_at or now_ts), now_ts)
            existing.active_until = max(int(existing.active_until or now_ts), int(active_until or now_ts))
            existing.cooling_until = max(int(existing.cooling_until or now_ts), int(cooling_until or now_ts))
            existing.closing_until = max(int(existing.closing_until or now_ts), int(closing_until or now_ts))
            # hop 表示相对 anchor 的传播深度。未来扩展到 2-hop/多-hop 时，
            # 运行态需要保留已知最深层级，不能在重复注册时被较浅 hop 覆盖回去。
            existing.hop = max(max(int(existing.hop or 1), 1), max(int(hop or 1), 1))
            existing.reason = reason or existing.reason
            existing.strategy_hint = strategy_hint or existing.strategy_hint
            existing.runtime_label_hint = runtime_label_hint or existing.runtime_label_hint
            existing.last_seen_ts = max(int(existing.last_seen_ts or 0), now_ts)
            if payload:
                existing.metadata.update(payload)
            return self._adjacent_watch_payload(existing)

        state = AdjacentWatchState(
            address=addr,
            anchor_watch_address=anchor_address,
            anchor_label=str(anchor_label or ""),
            root_tx_hash=str(root_tx_hash or ""),
            token=str(token).lower() if token else None,
            anchor_usd_value=float(anchor_usd_value or 0.0),
            opened_at=now_ts,
            active_until=int(active_until or now_ts),
            cooling_until=int(cooling_until or now_ts),
            closing_until=int(closing_until or now_ts),
            hop=max(int(hop or 1), 1),
            reason=str(reason or ""),
            strategy_hint=str(strategy_hint or ""),
            emitted_notification_count=0,
            observed_count=0,
            last_seen_ts=now_ts,
            last_event_type="opened",
            runtime_label_hint=str(runtime_label_hint or ""),
            metadata=payload,
        )
        self._adjacent_watch_states[addr] = state
        return self._adjacent_watch_payload(state)

    def touch_adjacent_watch(
        self,
        address: str,
        ts: int | None = None,
        event_type: str | None = None,
    ) -> dict | None:
        addr = str(address or "").lower()
        if not addr:
            return None
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return None
        now_ts = int(ts or time.time())
        if int(state.closing_until or 0) <= now_ts:
            return None
        state.observed_count = int(state.observed_count or 0) + 1
        state.last_seen_ts = now_ts
        if event_type:
            state.last_event_type = str(event_type or state.last_event_type)
        return self._adjacent_watch_payload(state)

    def is_adjacent_watch_active(self, address: str, now_ts: int | None = None) -> bool:
        addr = str(address or "").lower()
        if not addr:
            return False
        self.expire_adjacent_watch(None, now_ts=now_ts)
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return False
        reference_ts = int(now_ts or time.time())
        return int(state.active_until or 0) > reference_ts

    def get_adjacent_watch_context(self, address: str, now_ts: int | None = None) -> dict | None:
        addr = str(address or "").lower()
        self.expire_adjacent_watch(None, now_ts=now_ts)
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return None
        return self._adjacent_watch_payload(state)

    def maybe_get_adjacent_watch_meta(self, address: str, now_ts: int | None = None) -> dict:
        state = self.get_adjacent_watch_context(address, now_ts=now_ts)
        if not state:
            return {}
        metadata = dict(state.get("metadata") or {})
        display_hint_label = str(metadata.get("display_hint_label") or state.get("reason") or "new_large_counterparty")
        return {
            "runtime_adjacent_watch": True,
            "watch_meta_source": "runtime_adjacent_watch",
            "role": str(metadata.get("role") or "user_watch"),
            "strategy_role": str(metadata.get("strategy_role") or ADJACENT_WATCH_RUNTIME_STRATEGY_ROLE or "adjacent_watch"),
            "semantic_role": str(metadata.get("semantic_role") or "watched_wallet"),
            "anchor_watch_address": state.get("anchor_watch_address", ""),
            "anchor_label": state.get("anchor_label", ""),
            "root_tx_hash": state.get("root_tx_hash", ""),
            "opened_at": int(state.get("opened_at") or 0),
            "active_until": int(state.get("active_until") or 0),
            "cooling_until": int(state.get("cooling_until") or 0),
            "closing_until": int(state.get("closing_until") or 0),
            "expire_at": int(state.get("active_until") or 0),
            "hop": int(state.get("hop") or 1),
            "strategy_hint": state.get("strategy_hint", ""),
            "observed_count": int(state.get("observed_count") or 0),
            "emitted_notification_count": int(state.get("emitted_notification_count") or 0),
            "last_seen_ts": int(state.get("last_seen_ts") or 0),
            "last_event_type": str(state.get("last_event_type") or ""),
            "anchor_usd_value": float(state.get("anchor_usd_value") or 0.0),
            "priority": int(metadata.get("priority") or ADJACENT_WATCH_RUNTIME_PRIORITY or 3),
            "display_hint_label": display_hint_label,
            "display_hint_reason": str(metadata.get("display_hint_reason") or state.get("reason") or ""),
            "display_hint_anchor_label": state.get("anchor_label", ""),
            "display_hint_anchor_address": state.get("anchor_watch_address", ""),
            "display_hint_usd_value": float(state.get("anchor_usd_value") or 0.0),
            "display_hint_token_symbol": str(metadata.get("token_symbol") or ""),
            "anchor_strategy_role": str(metadata.get("anchor_strategy_role") or ""),
            "restore_anchor_mode": str(metadata.get("restore_anchor_mode") or ""),
            "restored_top_counterparties": list(metadata.get("restored_top_counterparties") or []),
            "restore_source": str(metadata.get("restore_source") or ""),
            "restore_strength": str(metadata.get("restore_strength") or ""),
            "downstream_case_id": str(metadata.get("case_id") or ""),
            "runtime_label_hint": str(state.get("runtime_label_hint") or display_hint_label),
            "runtime_state": self._adjacent_watch_phase(state, reference_ts=int(now_ts or time.time())),
        }

    def get_active_adjacent_watch_addresses(self, now_ts: int | None = None) -> set[str]:
        self.expire_adjacent_watch(None, now_ts=now_ts)
        reference_ts = int(now_ts or time.time())
        return {
            address
            for address, state in self._adjacent_watch_states.items()
            if int(state.active_until or 0) > reference_ts
        }

    def get_all_adjacent_watch_addresses(self, now_ts: int | None = None) -> set[str]:
        self.expire_adjacent_watch(None, now_ts=now_ts)
        return set(self._adjacent_watch_states.keys())

    def get_adjacent_watch_phase(self, address: str, now_ts: int | None = None) -> str:
        addr = str(address or "").lower()
        self.expire_adjacent_watch(None, now_ts=now_ts)
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return "closed"
        return self._adjacent_watch_phase(state, reference_ts=int(now_ts or time.time()))

    def mark_adjacent_watch_notification(
        self,
        address: str,
        ts: int | None = None,
        stage: str | None = None,
    ) -> dict | None:
        addr = str(address or "").lower()
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return None
        state.emitted_notification_count = int(state.emitted_notification_count or 0) + 1
        state.last_seen_ts = int(ts or time.time())
        if stage:
            state.last_event_type = str(stage)
        return self._adjacent_watch_payload(state)

    def close_adjacent_watch(self, address: str, ts: int | None = None, reason: str = "closed") -> dict | None:
        addr = str(address or "").lower()
        state = self._adjacent_watch_states.get(addr)
        if state is None:
            return None
        closed_ts = int(ts or time.time())
        state.active_until = min(int(state.active_until or closed_ts), closed_ts)
        state.cooling_until = min(int(state.cooling_until or closed_ts), closed_ts)
        state.closing_until = min(int(state.closing_until or closed_ts), closed_ts)
        state.last_seen_ts = closed_ts
        state.last_event_type = str(reason or "closed")
        payload = self._adjacent_watch_payload(state)
        self._adjacent_watch_states.pop(addr, None)
        return payload

    def expire_adjacent_watch(self, address: str | None = None, now_ts: int | None = None) -> list[str]:
        reference_ts = int(now_ts or time.time())
        expired = []
        if address:
            addr = str(address or "").lower()
            state = self._adjacent_watch_states.get(addr)
            if state is not None and int(state.closing_until or 0) <= reference_ts:
                self._adjacent_watch_states.pop(addr, None)
                expired.append(addr)
            return expired

        for addr, state in list(self._adjacent_watch_states.items()):
            if int(state.closing_until or 0) > reference_ts:
                continue
            self._adjacent_watch_states.pop(addr, None)
            expired.append(addr)
        return expired

    def _adjacent_watch_payload(self, state: AdjacentWatchState) -> dict:
        payload = asdict(state)
        payload["token"] = str(payload.get("token") or "").lower() or None
        payload["expire_at"] = int(payload.get("active_until") or 0)
        return payload

    def _adjacent_watch_phase(self, state: AdjacentWatchState, reference_ts: int) -> str:
        if int(state.active_until or 0) > reference_ts:
            return "active"
        if int(state.cooling_until or 0) > reference_ts:
            return "cooling"
        if int(state.closing_until or 0) > reference_ts:
            return "closing"
        return "closed"

    def record_counterparty(
        self,
        address: str,
        counterparty: str,
        usd_value: float,
        ts: int,
        metadata: dict | None = None,
    ) -> None:
        addr = str(address or "").lower()
        other = str(counterparty or "").lower()
        if not addr or not other or addr == other:
            return

        recent = self._recent_counterparties_by_address.get(addr)
        if recent is None:
            recent = deque(maxlen=120)
            self._recent_counterparties_by_address[addr] = recent

        record = {
            "counterparty": other,
            "usd_value": float(usd_value or 0.0),
            "ts": int(ts or 0),
            "metadata": dict(metadata or {}),
        }
        recent.append(record)

        stats_by_counterparty = self._address_interaction_stats.get(addr)
        if stats_by_counterparty is None:
            stats_by_counterparty = {}
            self._address_interaction_stats[addr] = stats_by_counterparty

        stats = stats_by_counterparty.get(other)
        if stats is None:
            stats = {
                "count": 0,
                "total_usd": 0.0,
                "avg_usd": 0.0,
                "max_usd": 0.0,
                "last_ts": 0,
                "last_metadata": {},
            }
            stats_by_counterparty[other] = stats

        stats["count"] += 1
        stats["total_usd"] += float(usd_value or 0.0)
        stats["avg_usd"] = stats["total_usd"] / max(stats["count"], 1)
        stats["max_usd"] = max(float(stats["max_usd"] or 0.0), float(usd_value or 0.0))
        stats["last_ts"] = int(ts or 0)
        stats["last_metadata"] = dict(metadata or {})

    def get_recent_counterparties(self, address: str, limit: int = 10) -> list[dict]:
        addr = str(address or "").lower()
        records = list(self._recent_counterparties_by_address.get(addr, []))
        if not records:
            return []

        dedup = {}
        for item in reversed(records):
            counterparty = str(item.get("counterparty") or "")
            if counterparty and counterparty not in dedup:
                dedup[counterparty] = item

        recent = list(dedup.values())
        recent.sort(key=lambda item: int(item.get("ts") or 0), reverse=True)
        return recent[: max(int(limit), 1)]

    def register_case(self, case_id: str, watch_address: str, token: str | None = None) -> None:
        if not case_id or not watch_address:
            return
        self._open_cases_by_address[str(watch_address).lower()].add(str(case_id))
        if token:
            self._open_cases_by_token[str(token).lower()].add(str(case_id))

    def update_case_pointer(
        self,
        case_id: str,
        watch_address: str | None = None,
        token: str | None = None,
        is_open: bool = True,
    ) -> None:
        if not case_id:
            return

        if watch_address:
            address_key = str(watch_address).lower()
            if is_open:
                self._open_cases_by_address[address_key].add(str(case_id))
            else:
                self._open_cases_by_address.get(address_key, set()).discard(str(case_id))

        if token:
            token_key = str(token).lower()
            if is_open:
                self._open_cases_by_token[token_key].add(str(case_id))
            else:
                self._open_cases_by_token.get(token_key, set()).discard(str(case_id))

    def _address_cooldown_key(self, address: str) -> str:
        return f"ethereum|{address}|address_wide"

    def _apply_address_event(self, event: Event) -> None:
        address = event.address.lower()
        state = self._address_states.get(address)
        if state is None:
            state = AddressState(recent_events=deque(maxlen=self.max_events_per_address))
            self._address_states[address] = state

        if state.first_seen_ts is None:
            state.first_seen_ts = int(event.ts)
        state.last_seen_ts = int(event.ts)
        state.recent_events.append(event)

        if not event.token:
            return

        token = event.token.lower()
        qty = float(event.amount or 0.0)
        usd = float(event.usd_value or 0.0)
        if qty <= 0:
            return

        if event.kind == "swap":
            if event.side == "买入":
                state.holding_delta[token] += qty
                state.holding_cost_usd[token] += max(usd, 0.0)
                return

            if event.side == "卖出":
                current_qty = float(state.holding_delta.get(token, 0.0))
                current_cost = float(state.holding_cost_usd.get(token, 0.0))
                if current_qty <= 0:
                    # 没有可靠持仓成本时仅减少数量，不计算 PnL。
                    state.holding_delta[token] -= qty
                    return

                sold_qty = min(current_qty, qty)
                avg_cost = (current_cost / current_qty) if current_qty > 0 else 0.0
                realized_cost = avg_cost * sold_qty
                realized_proceeds = usd * (sold_qty / qty) if qty > 0 else 0.0
                state.realized_pnl_usd += realized_proceeds - realized_cost

                state.holding_delta[token] = max(0.0, current_qty - sold_qty)
                state.holding_cost_usd[token] = max(0.0, current_cost - realized_cost)
                return

        if event.kind == "token_transfer":
            if event.side == "流入":
                state.holding_delta[token] += qty
            elif event.side == "流出":
                state.holding_delta[token] -= qty

    def _apply_token_event(self, event: Event) -> None:
        if not event.token:
            return

        token = event.token.lower()
        state = self._token_states.get(token)
        if state is None:
            state = TokenState(recent_events=deque(maxlen=self.max_events_per_token))
            self._token_states[token] = state

        if state.first_seen_ts is None:
            state.first_seen_ts = int(event.ts)
        state.last_seen_ts = int(event.ts)
        state.recent_events.append(event)
        state.participant_events.append(self._participant_record(event))

        usd = float(event.usd_value or 0.0)
        if event.side == "买入":
            state.net_flow_usd += usd
        elif event.side == "卖出":
            state.net_flow_usd -= usd

    def _apply_pool_event(self, event: Event) -> None:
        if str(event.strategy_role or "") != "lp_pool":
            return

        pool_address = str(event.address or "").lower()
        if not pool_address:
            return

        state = self._pool_states.get(pool_address)
        if state is None:
            state = PoolState()
            state.recent_events = deque(maxlen=max(self.max_events_per_token, 500))
            self._pool_states[pool_address] = state

        if state.first_seen_ts is None:
            state.first_seen_ts = int(event.ts)
        state.last_seen_ts = int(event.ts)
        state.recent_events.append(event)

    def _participant_record(self, event: Event) -> dict:
        watch_meta = event.metadata.get("watch_meta") or {}
        priority_raw = watch_meta.get("priority", 3)
        try:
            priority = int(priority_raw or 3)
        except (TypeError, ValueError):
            priority = 3

        strategy_role = str(event.strategy_role or watch_meta.get("strategy_role") or "unknown")
        semantic_role = str(event.semantic_role or watch_meta.get("semantic_role") or "unknown")
        role = str(watch_meta.get("role") or "unknown")

        return {
            "ts": int(event.ts),
            "address": event.address.lower(),
            "side": event.side or "",
            "kind": event.kind,
            "intent_type": event.intent_type,
            "usd_value": float(event.usd_value or 0.0),
            "strategy_role": strategy_role,
            "semantic_role": semantic_role,
            "role": role,
            "priority": priority,
            "quality_class": self._address_quality_class(strategy_role, semantic_role, role, priority),
            "smart_money": self._is_smart_money_role(strategy_role, role),
            "exchange_related": strategy_role.startswith("exchange_"),
        }

    def _address_quality_class(self, strategy_role: str, semantic_role: str, role: str, priority: int) -> str:
        if strategy_role == "lp_pool":
            return "high"
        if strategy_role.startswith("exchange_") or strategy_role in {"aggregator_router", "protocol_treasury"}:
            return "normal"
        if priority == 1:
            return "high"
        if strategy_role in HIGH_QUALITY_STRATEGY_ROLES:
            return "high"
        if semantic_role in {"trader_wallet", "investment_wallet", "market_maker_wallet"}:
            return "high"
        if role in {"smart_money", "celebrity"}:
            return "high"
        return "normal"

    def _is_smart_money_role(self, strategy_role: str, role: str) -> bool:
        if strategy_role in SMART_MONEY_STRATEGY_ROLES:
            return True
        if role in {"smart_money", "celebrity"} and not strategy_role.startswith("exchange_"):
            return True
        return False

    def _empty_address_snapshot(self, address: str, window_sec: int) -> dict:
        base = self._address_summary([], window_sec)
        windows = {name: self._address_summary([], sec) for name, sec in TIME_WINDOWS.items()}
        return {
            "address": address,
            **base,
            "window_sec": window_sec,
            "windows": windows,
            "holding_delta": {},
            "holding_cost_usd": {},
            "holding_delta_usd": {},
            "cost_basis": {},
            "realized_pnl_usd": 0.0,
            "recent_counterparties": [],
            "interaction_stats": {},
            "open_case_ids": [],
        }

    def _empty_token_snapshot(self, token: str) -> dict:
        base = self._token_summary([], [], 3600)
        windows = {name: self._token_summary([], [], sec) for name, sec in TIME_WINDOWS.items()}
        empty_resonance = self._resonance_summary([])
        return {
            "token": token,
            **base,
            "windows": windows,
            "volume_24h_proxy_usd": 0.0,
            "liquidity_proxy_usd": 0.0,
            "volume_24h_usd": 0.0,
            "liquidity_usd": 0.0,
            "holder_distribution_score": 0.0,
            "contract_age_days": 0,
            "swap_frequency": 0.0,
            "unique_participants": 0,
            "buy_cluster_5m": 0,
            "sell_cluster_5m": 0,
            "token_net_flow_usd_5m": 0.0,
            "token_net_flow_usd_1h": 0.0,
            "token_net_flow_usd_24h": 0.0,
            "resonance_5m": empty_resonance,
            "resonance_15m": empty_resonance,
            "resonance_1h": empty_resonance,
            "multi_address_resonance_5m": False,
            "smart_money_resonance_5m": False,
            "high_quality_resonance_5m": False,
            "leader_follow_resonance_5m": False,
            "resonance_score_5m": 0.0,
            "resonance_score_15m": 0.0,
            "open_case_ids": [],
        }

    def _empty_pool_snapshot(self, pool_address: str, window_sec: int) -> dict:
        summary = self._pool_summary(pool_address, [], window_sec)
        summary["recent"] = []
        return summary

    def _empty_lp_burst_snapshot(self, pool_address: str, direction: str, window_sec: int) -> dict:
        return {
            "lp_burst_window_sec": int(window_sec),
            "lp_burst_pool_address": str(pool_address or "").lower(),
            "lp_burst_direction": str(direction or ""),
            "lp_burst_event_count": 0,
            "lp_burst_total_usd": 0.0,
            "lp_burst_max_single_usd": 0.0,
            "lp_burst_same_pool_continuity": 0,
            "lp_burst_volume_surge_ratio": 0.0,
            "lp_burst_action_intensity": 0.0,
            "lp_burst_reserve_skew": 0.0,
            "lp_burst_first_ts": 0,
            "lp_burst_last_ts": 0,
        }

    def _empty_lp_trend_snapshot(self, pool_address: str, window_sec: int, trend_pool_context: dict | None = None) -> dict:
        trend_pool_context = trend_pool_context or {}
        return {
            "lp_trend_pool_address": str(pool_address or "").lower(),
            "lp_trend_state_window_sec": int(window_sec),
            "lp_trend_state": "trend_neutral",
            "lp_trend_side_bias": "neutral",
            "lp_trend_continuation_score": 0.0,
            "lp_trend_reversal_score": 0.0,
            "lp_trend_buy_pressure_count": 0,
            "lp_trend_sell_pressure_count": 0,
            "lp_trend_window_total_usd": 0.0,
            "lp_trend_last_shift_ts": 0,
            "lp_trend_state_source": "state_manager_window",
            "lp_trend_primary_pool": bool(trend_pool_context.get("is_primary_trend_pool")),
        }

    def _address_summary(self, events: list[Event], window_sec: int) -> dict:
        buy_events = [evt for evt in events if evt.kind == "swap" and evt.side == "买入"]
        sell_events = [evt for evt in events if evt.kind == "swap" and evt.side == "卖出"]
        buy_usd = sum(float(evt.usd_value or 0.0) for evt in buy_events)
        sell_usd = sum(float(evt.usd_value or 0.0) for evt in sell_events)

        trade_values = [abs(float(evt.usd_value or 0.0)) for evt in events if float(evt.usd_value or 0.0) > 0]
        volume_usd = sum(trade_values)
        avg_usd = volume_usd / len(trade_values) if trade_values else 0.0
        usd_std = self._std(trade_values, avg_usd) if trade_values else 0.0
        max_usd = max(trade_values) if trade_values else 0.0
        window_hours = max(window_sec / 3600.0, 1.0 / 60.0)
        trade_frequency_1h = len(events) / window_hours
        behavior_consistency = self._side_consistency(events)
        consecutive_buy_count = self._count_consecutive_swaps(events, "买入")
        consecutive_sell_count = self._count_consecutive_swaps(events, "卖出")

        return {
            "recent": events,
            "recent_count": len(events),
            "buy_count": len(buy_events),
            "sell_count": len(sell_events),
            "buy_usd": buy_usd,
            "sell_usd": sell_usd,
            "volume_usd": volume_usd,
            "avg_usd": avg_usd,
            "usd_std": usd_std,
            "max_usd": max_usd,
            "trade_frequency_1h": trade_frequency_1h,
            "behavior_consistency": behavior_consistency,
            "consecutive_buy_count": consecutive_buy_count,
            "consecutive_sell_count": consecutive_sell_count,
        }

    def _token_summary(self, events: list[Event], participant_events: list[dict], window_sec: int) -> dict:
        buy_count = sum(1 for evt in events if evt.kind == "swap" and evt.side == "买入")
        sell_count = sum(1 for evt in events if evt.kind == "swap" and evt.side == "卖出")
        swap_count = sum(1 for evt in events if evt.kind == "swap")

        trade_values = [abs(float(evt.usd_value or 0.0)) for evt in events if float(evt.usd_value or 0.0) > 0]
        volume_usd = sum(trade_values)
        avg_usd = volume_usd / len(trade_values) if trade_values else 0.0
        median_usd = self._median(trade_values) if trade_values else 0.0
        net_flow_usd = sum(
            float(evt.usd_value or 0.0) * (1 if evt.side == "买入" else -1 if evt.side == "卖出" else 0)
            for evt in events
        )

        unique_participants = len({str(item.get("address") or "") for item in participant_events if item.get("address")})
        window_hours = max(window_sec / 3600.0, 1.0 / 60.0)
        swap_frequency = swap_count / window_hours

        return {
            "recent": events,
            "activity": len(events),
            "buy_count": buy_count,
            "sell_count": sell_count,
            "swap_count": swap_count,
            "net_flow_usd": net_flow_usd,
            "volume_usd": volume_usd,
            "avg_usd": avg_usd,
            "median_usd": median_usd,
            "swap_frequency": swap_frequency,
            "unique_participants": unique_participants,
        }

    def _pool_summary(self, pool_address: str, events: list[Event], window_sec: int) -> dict:
        trade_values = [abs(float(evt.usd_value or 0.0)) for evt in events if float(evt.usd_value or 0.0) > 0]
        volume_usd = sum(trade_values)
        avg_usd = volume_usd / len(trade_values) if trade_values else 0.0
        max_usd = max(trade_values) if trade_values else 0.0
        buy_count = sum(1 for evt in events if str(evt.intent_type or "") == "pool_buy_pressure" or str(evt.side or "") == "买入")
        sell_count = sum(1 for evt in events if str(evt.intent_type or "") == "pool_sell_pressure" or str(evt.side or "") == "卖出")
        liquidity_add_count = sum(1 for evt in events if str(evt.intent_type or "") == "liquidity_addition")
        liquidity_remove_count = sum(1 for evt in events if str(evt.intent_type or "") == "liquidity_removal")

        counterparties = set()
        for evt in events:
            raw = evt.metadata.get("raw") or {}
            counterparty = str(raw.get("counterparty") or evt.metadata.get("counterparty_label") or "").lower()
            if counterparty:
                counterparties.add(counterparty)

        return {
            "pool_address": pool_address,
            "window_sec": window_sec,
            "recent": events,
            "recent_count": len(events),
            "volume_usd": float(volume_usd),
            "avg_usd": float(avg_usd),
            "max_usd": float(max_usd),
            "buy_count": int(buy_count),
            "sell_count": int(sell_count),
            "liquidity_add_count": int(liquidity_add_count),
            "liquidity_remove_count": int(liquidity_remove_count),
            "unique_counterparties": len(counterparties),
            "same_direction_streak": self._pool_same_direction_streak(events),
        }

    def _lp_burst_summary(
        self,
        pool_address: str,
        direction: str,
        events: list[Event],
        window_sec: int,
    ) -> dict:
        if not events:
            return self._empty_lp_burst_snapshot(pool_address, direction, window_sec)

        total_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in events)
        max_single_usd = max(abs(float(evt.usd_value or 0.0)) for evt in events)
        continuity_values = []
        volume_surge_values = []
        action_intensity_values = []
        reserve_skew_values = []
        first_ts = min(int(evt.ts or 0) for evt in events)
        last_ts = max(int(evt.ts or 0) for evt in events)

        for evt in events:
            lp_analysis = getattr(evt, "metadata", {}).get("lp_analysis") or {}
            continuity_values.append(int(lp_analysis.get("same_pool_continuity") or 0) + 1)
            volume_surge_values.append(float(lp_analysis.get("pool_volume_surge_ratio") or 0.0))
            action_intensity_values.append(float(lp_analysis.get("action_intensity") or 0.0))
            reserve_skew_values.append(float(lp_analysis.get("reserve_skew") or 0.0))

        return {
            "lp_burst_window_sec": int(window_sec),
            "lp_burst_pool_address": str(pool_address or "").lower(),
            "lp_burst_direction": str(direction or ""),
            "lp_burst_event_count": len(events),
            "lp_burst_total_usd": round(float(total_usd), 2),
            "lp_burst_max_single_usd": round(float(max_single_usd), 2),
            "lp_burst_same_pool_continuity": max(max(continuity_values or [0]), len(events)),
            "lp_burst_volume_surge_ratio": round(float(max(volume_surge_values or [0.0])), 3),
            "lp_burst_action_intensity": round(float(max(action_intensity_values or [0.0])), 3),
            "lp_burst_reserve_skew": round(float(max(reserve_skew_values or [0.0])), 3),
            "lp_burst_first_ts": int(first_ts),
            "lp_burst_last_ts": int(last_ts),
        }

    def _lp_trend_summary(
        self,
        *,
        pool_address: str,
        events: list[Event],
        window_sec: int,
        trend_pool_context: dict | None = None,
    ) -> dict:
        trend_pool_context = trend_pool_context or {}
        if not events:
            return self._empty_lp_trend_snapshot(pool_address, window_sec, trend_pool_context)

        directional_events = [
            evt for evt in events
            if self._lp_directional_bucket(evt) in {"buy_pressure", "sell_pressure"}
        ]
        if not directional_events:
            return self._empty_lp_trend_snapshot(pool_address, window_sec, trend_pool_context)

        buy_events = [evt for evt in directional_events if self._lp_directional_bucket(evt) == "buy_pressure"]
        sell_events = [evt for evt in directional_events if self._lp_directional_bucket(evt) == "sell_pressure"]
        buy_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in buy_events)
        sell_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in sell_events)
        window_total_usd = buy_usd + sell_usd
        is_primary = bool(trend_pool_context.get("is_primary_trend_pool"))

        buy_side_score = self._lp_side_state_score(
            events=directional_events,
            side="buy_pressure",
            total_usd=window_total_usd,
            is_primary=is_primary,
        )
        sell_side_score = self._lp_side_state_score(
            events=directional_events,
            side="sell_pressure",
            total_usd=window_total_usd,
            is_primary=is_primary,
        )
        side_bias = self._lp_state_bias(
            buy_score=buy_side_score,
            sell_score=sell_side_score,
            buy_usd=buy_usd,
            sell_usd=sell_usd,
            buy_count=len(buy_events),
            sell_count=len(sell_events),
        )

        last_side = self._lp_directional_bucket(directional_events[-1])
        current_streak = self._lp_current_streak(direction_events=directional_events)
        last_shift_ts = int(directional_events[-current_streak].ts or 0) if current_streak > 0 else 0
        continuation_score = buy_side_score if side_bias == "buy_pressure" else sell_side_score if side_bias == "sell_pressure" else max(buy_side_score, sell_side_score)

        history_before_streak = directional_events[:-current_streak] if current_streak > 0 else directional_events
        previous_bias = self._lp_previous_side_bias(history_before_streak)
        reversal_score = self._lp_reversal_score(
            events=directional_events,
            current_streak=current_streak,
            total_usd=window_total_usd,
            is_primary=is_primary,
        )

        trend_state = "trend_neutral"
        if last_side == "buy_pressure":
            if previous_bias == "sell_pressure" and reversal_score >= self.lp_trend_reversal_min_score:
                trend_state = "trend_reversal_to_buy"
            elif side_bias == "buy_pressure" and continuation_score >= self.lp_trend_continuation_min_score:
                trend_state = "trend_continuation_buy"
        elif last_side == "sell_pressure":
            if previous_bias == "buy_pressure" and reversal_score >= self.lp_trend_reversal_min_score:
                trend_state = "trend_reversal_to_sell"
            elif side_bias == "sell_pressure" and continuation_score >= self.lp_trend_continuation_min_score:
                trend_state = "trend_continuation_sell"

        return {
            "lp_trend_pool_address": str(pool_address or "").lower(),
            "lp_trend_state_window_sec": int(window_sec),
            "lp_trend_state": trend_state,
            "lp_trend_side_bias": side_bias,
            "lp_trend_continuation_score": round(float(continuation_score), 3),
            "lp_trend_reversal_score": round(float(reversal_score), 3),
            "lp_trend_buy_pressure_count": len(buy_events),
            "lp_trend_sell_pressure_count": len(sell_events),
            "lp_trend_window_total_usd": round(float(window_total_usd), 2),
            "lp_trend_last_shift_ts": int(last_shift_ts),
            "lp_trend_state_source": "state_manager_window",
            "lp_trend_primary_pool": is_primary,
        }

    def _lp_side_state_score(
        self,
        *,
        events: list[Event],
        side: str,
        total_usd: float,
        is_primary: bool,
    ) -> float:
        side_events = [evt for evt in events if self._lp_directional_bucket(evt) == side]
        if not side_events:
            return 0.0
        usd_value = sum(abs(float(evt.usd_value or 0.0)) for evt in side_events)
        trailing_streak = self._lp_trailing_streak(events, side)
        continuity_max = 0
        surge_max = 0.0
        skew_max = 0.0
        intensity_max = 0.0
        for evt in side_events:
            lp_analysis = evt.metadata.get("lp_analysis") or {}
            continuity_max = max(continuity_max, int(lp_analysis.get("same_pool_continuity") or 0) + 1)
            surge_max = max(surge_max, float(lp_analysis.get("pool_volume_surge_ratio") or 0.0))
            skew_max = max(skew_max, float(lp_analysis.get("reserve_skew") or 0.0))
            intensity_max = max(intensity_max, float(lp_analysis.get("action_intensity") or 0.0))
        score = 0.0
        score += min(len(side_events) / 5.0, 1.0) * 0.22
        score += min(usd_value / max(total_usd, 1.0), 1.0) * 0.24
        score += min(trailing_streak / 4.0, 1.0) * 0.18
        score += min(continuity_max / 4.0, 1.0) * 0.12
        score += min(max(surge_max - 1.0, 0.0) / 4.0, 1.0) * 0.10
        score += min(skew_max / 1.2, 1.0) * 0.08
        score += min(intensity_max / 0.75, 1.0) * 0.06
        if is_primary:
            score += 0.04
        return self._clamp(score, 0.0, 1.0)

    def _lp_state_bias(
        self,
        *,
        buy_score: float,
        sell_score: float,
        buy_usd: float,
        sell_usd: float,
        buy_count: int,
        sell_count: int,
    ) -> str:
        total_usd = buy_usd + sell_usd
        usd_gap_ratio = abs(buy_usd - sell_usd) / max(total_usd, 1.0)
        score_gap = abs(buy_score - sell_score)
        if score_gap < 0.08 and usd_gap_ratio < 0.12 and abs(buy_count - sell_count) <= 1:
            return "neutral"
        if buy_score > sell_score:
            return "buy_pressure"
        if sell_score > buy_score:
            return "sell_pressure"
        if buy_usd > sell_usd:
            return "buy_pressure"
        if sell_usd > buy_usd:
            return "sell_pressure"
        return "neutral"

    def _lp_current_streak(self, direction_events: list[Event]) -> int:
        if not direction_events:
            return 0
        target = self._lp_directional_bucket(direction_events[-1])
        streak = 0
        for evt in reversed(direction_events):
            if self._lp_directional_bucket(evt) != target:
                break
            streak += 1
        return streak

    def _lp_trailing_streak(self, events: list[Event], side: str) -> int:
        streak = 0
        for evt in reversed(events):
            if self._lp_directional_bucket(evt) != side:
                break
            streak += 1
        return streak

    def _lp_previous_side_bias(self, events: list[Event]) -> str:
        if not events:
            return "neutral"
        buy_events = [evt for evt in events if self._lp_directional_bucket(evt) == "buy_pressure"]
        sell_events = [evt for evt in events if self._lp_directional_bucket(evt) == "sell_pressure"]
        buy_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in buy_events)
        sell_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in sell_events)
        if len(buy_events) >= len(sell_events) + 1 or buy_usd > sell_usd * 1.10:
            return "buy_pressure"
        if len(sell_events) >= len(buy_events) + 1 or sell_usd > buy_usd * 1.10:
            return "sell_pressure"
        return "neutral"

    def _lp_reversal_score(
        self,
        *,
        events: list[Event],
        current_streak: int,
        total_usd: float,
        is_primary: bool,
    ) -> float:
        if not events or current_streak <= 0:
            return 0.0
        tail_events = events[-current_streak:]
        if not tail_events:
            return 0.0
        tail_total_usd = sum(abs(float(evt.usd_value or 0.0)) for evt in tail_events)
        surge_max = 0.0
        skew_max = 0.0
        intensity_max = 0.0
        for evt in tail_events:
            lp_analysis = evt.metadata.get("lp_analysis") or {}
            surge_max = max(surge_max, float(lp_analysis.get("pool_volume_surge_ratio") or 0.0))
            skew_max = max(skew_max, float(lp_analysis.get("reserve_skew") or 0.0))
            intensity_max = max(intensity_max, float(lp_analysis.get("action_intensity") or 0.0))
        score = 0.0
        score += min(current_streak / 3.0, 1.0) * 0.30
        score += min(tail_total_usd / max(total_usd, 1.0), 1.0) * 0.26
        score += min(max(surge_max - 1.0, 0.0) / 4.0, 1.0) * 0.16
        score += min(skew_max / 1.2, 1.0) * 0.14
        score += min(intensity_max / 0.75, 1.0) * 0.10
        if is_primary:
            score += 0.04
        return self._clamp(score, 0.0, 1.0)

    def _resonance_summary(self, participant_events: list[dict]) -> dict:
        buckets = {
            "buy": {
                "addresses": set(),
                "high_quality": set(),
                "smart_money": set(),
                "low_quality": set(),
                "volume": 0.0,
            },
            "sell": {
                "addresses": set(),
                "high_quality": set(),
                "smart_money": set(),
                "low_quality": set(),
                "volume": 0.0,
            },
        }

        for item in participant_events:
            bucket_name = self._direction_bucket(item.get("side"))
            if bucket_name not in buckets:
                continue

            address = str(item.get("address") or "")
            if not address:
                continue

            bucket = buckets[bucket_name]
            bucket["addresses"].add(address)
            bucket["volume"] += float(item.get("usd_value") or 0.0)
            if item.get("quality_class") == "high":
                bucket["high_quality"].add(address)
            else:
                bucket["low_quality"].add(address)
            if bool(item.get("smart_money")):
                bucket["smart_money"].add(address)

        def pack(name: str) -> dict:
            bucket = buckets[name]
            unique_addresses = len(bucket["addresses"])
            high_quality_addresses = len(bucket["high_quality"])
            smart_money_addresses = len(bucket["smart_money"])
            leader_follow = high_quality_addresses >= 1 and unique_addresses > high_quality_addresses
            return {
                "unique_addresses": unique_addresses,
                "high_quality_addresses": high_quality_addresses,
                "smart_money_addresses": smart_money_addresses,
                "volume_usd": round(float(bucket["volume"]), 2),
                "leader_follow": leader_follow,
            }

        buy = pack("buy")
        sell = pack("sell")
        if buy["unique_addresses"] >= sell["unique_addresses"]:
            dominant_side = "buy"
            dominant = buy
        else:
            dominant_side = "sell"
            dominant = sell

        resonance_score = 0.0
        if dominant["unique_addresses"] >= 2:
            resonance_score += 0.22
        resonance_score += min(0.24, dominant["unique_addresses"] * 0.12)
        resonance_score += min(0.26, dominant["high_quality_addresses"] * 0.14)
        resonance_score += min(0.20, dominant["smart_money_addresses"] * 0.12)
        if dominant["leader_follow"]:
            resonance_score += 0.08

        return {
            "dominant_side": dominant_side,
            "buy_unique_addresses": buy["unique_addresses"],
            "sell_unique_addresses": sell["unique_addresses"],
            "buy_high_quality_addresses": buy["high_quality_addresses"],
            "sell_high_quality_addresses": sell["high_quality_addresses"],
            "buy_smart_money_addresses": buy["smart_money_addresses"],
            "sell_smart_money_addresses": sell["smart_money_addresses"],
            "buy_volume_usd": buy["volume_usd"],
            "sell_volume_usd": sell["volume_usd"],
            "buy_leader_follow": buy["leader_follow"],
            "sell_leader_follow": sell["leader_follow"],
            "multi_address_resonance": max(buy["unique_addresses"], sell["unique_addresses"]) >= 2,
            "high_quality_resonance": max(buy["high_quality_addresses"], sell["high_quality_addresses"]) >= 2,
            "smart_money_resonance": max(buy["smart_money_addresses"], sell["smart_money_addresses"]) >= 2,
            "leader_follow_resonance": buy["leader_follow"] or sell["leader_follow"],
            "resonance_score": self._clamp(resonance_score, 0.0, 1.0),
        }

    def _estimate_liquidity_proxy(self, windows: dict[str, dict]) -> float:
        w5 = windows["5m"]
        w1h = windows["1h"]
        w24 = windows["24h"]

        vol_5m = float(w5["volume_usd"])
        vol_1h = float(w1h["volume_usd"])
        vol_24h = float(w24["volume_usd"])
        median_1h = float(w1h["median_usd"])
        participants_1h = int(w1h["unique_participants"])

        proxy = max(
            150_000.0,
            vol_5m * 18.0,
            vol_1h * 5.0,
            vol_24h * 0.35,
            median_1h * max(participants_1h, 1) * 8.0,
        )
        return float(proxy)

    def _estimate_holder_distribution_score(self, windows: dict[str, dict]) -> float:
        w24 = windows["24h"]
        unique_participants = int(w24["unique_participants"])
        swap_count = int(w24["swap_count"])
        if swap_count <= 0:
            return 0.5

        diversity_ratio = unique_participants / max(swap_count, 1)
        score = 0.35 + diversity_ratio * 0.8
        return max(0.0, min(1.0, score))

    def _filter_events_by_window(self, events: list[Event], now_ts: int, window_sec: int) -> list[Event]:
        if not events:
            return []
        if now_ts <= 0:
            return list(events)
        return [evt for evt in events if now_ts - int(evt.ts) <= window_sec]

    def _filter_participants_by_window(self, participants: list[dict], now_ts: int, window_sec: int) -> list[dict]:
        if not participants:
            return []
        if now_ts <= 0:
            return list(participants)
        filtered = []
        for item in participants:
            item_ts = int(item.get("ts") or 0)
            if now_ts - item_ts <= window_sec:
                filtered.append(item)
        return filtered

    def _count_consecutive_swaps(self, events: list[Event], side: str) -> int:
        count = 0
        for event in reversed(events):
            if event.kind != "swap":
                continue
            if event.side == side:
                count += 1
                continue
            break
        return count

    def _side_consistency(self, events: list[Event]) -> float:
        swap_sides = [evt.side for evt in events if evt.kind == "swap" and evt.side in {"买入", "卖出"}]
        if not swap_sides:
            return 0.5
        buy_count = sum(1 for side in swap_sides if side == "买入")
        sell_count = sum(1 for side in swap_sides if side == "卖出")
        return max(buy_count, sell_count) / max(len(swap_sides), 1)

    def _direction_bucket(self, side: str | None) -> str:
        normalized = str(side or "")
        if normalized in {"买入", "流入"}:
            return "buy"
        if normalized in {"卖出", "流出"}:
            return "sell"
        return "other"

    def _lp_directional_bucket(self, event: Event, intent_type: str | None = None) -> str:
        normalized_intent = str(intent_type or event.intent_type or "").lower()
        if normalized_intent == "pool_buy_pressure":
            return "buy_pressure"
        if normalized_intent == "pool_sell_pressure":
            return "sell_pressure"
        normalized_side = str(event.side or "")
        if normalized_side == "买入":
            return "buy_pressure"
        if normalized_side == "卖出":
            return "sell_pressure"
        return ""

    def _pool_same_direction_streak(self, events: list[Event]) -> int:
        streak = 0
        target_bucket = None
        for event in reversed(events):
            intent_type = str(event.intent_type or "")
            if intent_type not in {"pool_buy_pressure", "pool_sell_pressure", "liquidity_addition", "liquidity_removal"}:
                continue
            bucket = self._direction_bucket(event.side)
            if intent_type == "liquidity_addition":
                bucket = "liquidity_add"
            elif intent_type == "liquidity_removal":
                bucket = "liquidity_remove"
            if target_bucket is None:
                target_bucket = bucket
            if bucket != target_bucket:
                break
            streak += 1
        return streak

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))

    def _std(self, values: list[float], mean_value: float) -> float:
        if not values:
            return 0.0
        variance = sum((value - mean_value) ** 2 for value in values) / len(values)
        return math.sqrt(max(variance, 0.0))

    def _median(self, values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        n = len(ordered)
        mid = n // 2
        if n % 2 == 1:
            return float(ordered[mid])
        return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def get_primary_state_manager():
    return _PRIMARY_STATE_MANAGER


def get_runtime_adjacent_watch_addresses(now_ts: int | None = None) -> set[str]:
    manager = get_primary_state_manager()
    if manager is None:
        return set()
    return manager.get_active_adjacent_watch_addresses(now_ts=now_ts)


def is_runtime_adjacent_watch_active(address: str, now_ts: int | None = None) -> bool:
    manager = get_primary_state_manager()
    if manager is None:
        return False
    return manager.is_adjacent_watch_active(address, now_ts=now_ts)


def maybe_get_runtime_adjacent_watch_meta(address: str, now_ts: int | None = None) -> dict:
    manager = get_primary_state_manager()
    if manager is None:
        return {}
    return manager.maybe_get_adjacent_watch_meta(address, now_ts=now_ts)


def get_runtime_adjacent_watch_phase(address: str, now_ts: int | None = None) -> str:
    manager = get_primary_state_manager()
    if manager is None:
        return "closed"
    return manager.get_adjacent_watch_phase(address, now_ts=now_ts)


def get_all_runtime_adjacent_watch_addresses(now_ts: int | None = None) -> set[str]:
    manager = get_primary_state_manager()
    if manager is None:
        return set()
    return manager.get_all_adjacent_watch_addresses(now_ts=now_ts)
