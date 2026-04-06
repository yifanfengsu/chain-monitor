from collections import defaultdict
import hashlib
import time

from config import (
    ADJACENT_WATCH_ALLOWED_ROLE_GROUPS,
    ADJACENT_WATCH_CLOSING_SEC,
    ADJACENT_WATCH_COOLING_SEC,
    ADJACENT_WATCH_ENABLE,
    ADJACENT_WATCH_EXCLUDE_EXCHANGE,
    ADJACENT_WATCH_EXCLUDE_LP_POOL,
    ADJACENT_WATCH_EXCLUDE_PROTOCOL,
    ADJACENT_WATCH_EXCLUDE_ROUTER,
    ADJACENT_WATCH_FOLLOWUP_MIN_RATIO,
    ADJACENT_WATCH_FOLLOWUP_MIN_USD,
    ADJACENT_WATCH_MAJOR_ASSET_SUPER_LARGE_USD,
    ADJACENT_WATCH_MAX_HOPS,
    ADJACENT_WATCH_MAX_NOTIFICATIONS,
    ADJACENT_WATCH_OTHER_ASSET_SUPER_LARGE_USD,
    ADJACENT_WATCH_STABLE_SUPER_LARGE_USD,
    ADJACENT_WATCH_SUPER_LARGE_USD,
    ADJACENT_WATCH_WINDOW_SEC,
    LIQUIDATION_EXECUTION_MIN_SCORE,
    LIQUIDATION_RISK_MIN_SCORE,
    SMART_MONEY_CASE_WINDOW_SEC,
)
from constants import (
    MAJOR_FOLLOWUP_ASSET_CONTRACTS,
    MAJOR_FOLLOWUP_ASSET_SYMBOLS,
    STABLE_TOKEN_METADATA,
    STABLE_TOKEN_CONTRACTS,
    STABLE_TOKEN_SYMBOLS,
    WBTC_TOKEN_CONTRACT,
    WETH_TOKEN_CONTRACT,
)
from filter import (
    get_address_meta,
    get_threshold,
    is_exchange_strategy_role,
    is_market_maker_strategy_role,
    is_priority_smart_money_strategy_role,
    is_smart_money_strategy_role,
    shorten_address,
)
from models import BehaviorCase, Event, Signal


EXCHANGE_FOLLOWUP_CASE_FAMILY = "exchange_cross_token_followup"
SMART_MONEY_EXECUTION_CASE_FAMILY = "smart_money_execution_case"
LIQUIDATION_CASE_FAMILY = "liquidation_case_family"
DOWNSTREAM_FOLLOWUP_CASE_FAMILY = "downstream_counterparty_followup"
EXCHANGE_FOLLOWUP_INTENTS = {
    "exchange_deposit_candidate",
    "possible_sell_preparation",
}
DOWNSTREAM_STABLE_SYMBOLS = set(STABLE_TOKEN_SYMBOLS) | {"USD1"}
DOWNSTREAM_MAJOR_SYMBOLS = set(MAJOR_FOLLOWUP_ASSET_SYMBOLS) | {"BTC", "ETH"}


class FollowupTracker:
    """
    轻量行为案例跟踪器：
    - 归并相近事件为同一 case
    - 维护 case 生命周期
    - 在原有交易所 followup 之上，补一条轻量 smart money 执行连续性 case
    """

    def __init__(
        self,
        merge_window_sec: int = 1800,
        cooling_sec: int = 3600,
        closing_sec: int = 14_400,
        exchange_followup_window_sec: int = 900,
        smart_money_case_window_sec: int = SMART_MONEY_CASE_WINDOW_SEC,
        liquidation_case_window_sec: int = 900,
        downstream_followup_window_sec: int = ADJACENT_WATCH_WINDOW_SEC,
    ) -> None:
        self.merge_window_sec = int(merge_window_sec)
        self.cooling_sec = int(cooling_sec)
        self.closing_sec = int(closing_sec)
        self.exchange_followup_window_sec = int(exchange_followup_window_sec)
        self.smart_money_case_window_sec = int(smart_money_case_window_sec)
        self.liquidation_case_window_sec = int(liquidation_case_window_sec)
        self.downstream_followup_window_sec = int(downstream_followup_window_sec)
        self.downstream_followup_cooling_sec = int(ADJACENT_WATCH_COOLING_SEC)
        self.downstream_followup_closing_sec = int(ADJACENT_WATCH_CLOSING_SEC)
        self.downstream_followup_enabled = bool(ADJACENT_WATCH_ENABLE)
        self.downstream_followup_allowed_roles = {str(item or "").strip() for item in ADJACENT_WATCH_ALLOWED_ROLE_GROUPS if item}
        self.downstream_followup_max_hops = max(int(ADJACENT_WATCH_MAX_HOPS or 1), 1)
        self.downstream_followup_min_usd = float(ADJACENT_WATCH_FOLLOWUP_MIN_USD)
        self.downstream_followup_min_ratio = float(ADJACENT_WATCH_FOLLOWUP_MIN_RATIO)
        self.downstream_followup_max_notifications = max(int(ADJACENT_WATCH_MAX_NOTIFICATIONS or 1), 1)
        self._cases: dict[str, BehaviorCase] = {}
        self._open_case_ids_by_watch: dict[str, set[str]] = defaultdict(set)
        self._open_case_ids_by_token: dict[str, set[str]] = defaultdict(set)

    def match_or_open_case(
        self,
        event: Event,
        watch_meta: dict | None = None,
        behavior: dict | None = None,
    ) -> dict:
        now_ts = int(event.ts or time.time())
        stale_updates = self.close_stale_cases(now_ts)
        invalidated_cases = self._invalidate_conflicting_cases(event, now_ts)

        watch_meta = watch_meta or {}
        behavior = behavior or {}

        downstream_result = {
            "case": None,
            "created": False,
            "downstream_followup_case": False,
            "downstream_followup_anchor": False,
            "downstream_followup_type": "",
            "downstream_followup_stage": "",
        }
        if self.downstream_followup_enabled:
            downstream_result = self._match_downstream_followup_case(
                event=event,
                watch_meta=watch_meta,
                now_ts=now_ts,
            )
        matched = downstream_result.get("case")
        created = bool(downstream_result.get("created"))
        used_downstream_case = bool(downstream_result.get("downstream_followup_case"))

        exchange_result = {
            "case": None,
            "created": False,
            "exchange_followup_anchor": False,
            "exchange_followup_confirmed": False,
        }
        if matched is None:
            exchange_result = self._match_exchange_followup_case(
                event=event,
                watch_meta=watch_meta,
                now_ts=now_ts,
            )
            matched = exchange_result.get("case")
            created = bool(exchange_result.get("created"))
        used_exchange_case = bool(exchange_result.get("case") is not None and matched is exchange_result.get("case"))

        smart_money_result = {
            "case": None,
            "created": False,
            "smart_money_case": False,
        }
        if matched is None:
            smart_money_result = self._match_smart_money_execution_case(
                event=event,
                watch_meta=watch_meta,
                now_ts=now_ts,
            )
            matched = smart_money_result.get("case")
            created = bool(smart_money_result.get("created"))

        used_smart_money_case = bool(smart_money_result.get("smart_money_case"))
        liquidation_result = {
            "case": None,
            "created": False,
            "liquidation_case": False,
        }
        if matched is None:
            liquidation_result = self._match_liquidation_case(
                event=event,
                watch_meta=watch_meta,
                now_ts=now_ts,
            )
            matched = liquidation_result.get("case")
            created = bool(liquidation_result.get("created"))

        used_liquidation_case = bool(liquidation_result.get("liquidation_case"))

        if matched is None:
            matched = self._find_matching_case(event, now_ts)
            created = matched is None
            if matched is None:
                matched = self._open_case(event, watch_meta=watch_meta)

        self.update_case_with_event(matched, event, watch_meta=watch_meta, behavior=behavior)
        return {
            "case": matched,
            "created": created,
            "invalidated_cases": invalidated_cases,
            "stale_updates": stale_updates,
            "used_downstream_case": used_downstream_case,
            "downstream_followup_anchor": bool(downstream_result.get("downstream_followup_anchor")),
            "downstream_followup_type": str(downstream_result.get("downstream_followup_type") or ""),
            "downstream_followup_stage": str(downstream_result.get("downstream_followup_stage") or ""),
            "used_exchange_case": used_exchange_case,
            "exchange_followup_anchor": bool(exchange_result.get("exchange_followup_anchor")),
            "exchange_followup_confirmed": bool(exchange_result.get("exchange_followup_confirmed")),
            "used_smart_money_case": used_smart_money_case,
            "used_liquidation_case": used_liquidation_case,
            "smart_money_case_confirmed": bool(
                (matched.metadata or {}).get("continuation_confirmed")
            ) if matched is not None else False,
            "liquidation_case_confirmed": bool(
                (matched.metadata or {}).get("execution_confirmed")
            ) if matched is not None else False,
        }

    def update_case_with_event(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict | None = None,
        behavior: dict | None = None,
    ) -> BehaviorCase:
        behavior = behavior or {}
        if event.event_id and event.event_id not in behavior_case.event_ids:
            behavior_case.event_ids.append(event.event_id)
        behavior_case.last_event_ts = int(event.ts or behavior_case.last_event_ts or 0)
        if event.tx_hash and not behavior_case.root_tx_hash:
            behavior_case.root_tx_hash = event.tx_hash

        for evidence in list(event.intent_evidence or []):
            if evidence and evidence not in behavior_case.evidence:
                behavior_case.evidence.append(evidence)
        if behavior.get("reason"):
            reason = str(behavior.get("reason") or "")
            if reason and reason not in behavior_case.evidence:
                behavior_case.evidence.append(reason)

        if self._is_smart_money_case(behavior_case):
            self._update_smart_money_case_metadata(behavior_case, event, watch_meta or {})
        if self._is_liquidation_case(behavior_case):
            self._update_liquidation_case_metadata(behavior_case, event, watch_meta or {})
        if self._is_downstream_followup_case(behavior_case):
            self._update_downstream_case_metadata(behavior_case, event, watch_meta or {})

        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta or {})
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        status, stage = self._status_and_stage(behavior_case, event)
        behavior_case.status = status
        behavior_case.stage = stage
        return behavior_case

    def attach_signal(self, case_ref: str | BehaviorCase, signal: Signal) -> BehaviorCase | None:
        behavior_case = self.get_case(case_ref) if isinstance(case_ref, str) else case_ref
        if behavior_case is None:
            return None

        signal_id = str(signal.signal_id or signal.event_id or signal.tx_hash or "")
        if signal_id and signal_id not in behavior_case.signal_ids:
            behavior_case.signal_ids.append(signal_id)

        if self._is_exchange_followup_case(behavior_case):
            return behavior_case
        if self._is_downstream_followup_case(behavior_case):
            status, stage = self._downstream_status_and_stage(behavior_case)
            behavior_case.status = status
            behavior_case.stage = stage
            return behavior_case

        if self._is_smart_money_case(behavior_case):
            status, stage = self._smart_money_status_and_stage(behavior_case)
            behavior_case.status = status
            behavior_case.stage = stage
            return behavior_case
        if self._is_liquidation_case(behavior_case):
            status, stage = self._liquidation_status_and_stage(behavior_case)
            behavior_case.status = status
            behavior_case.stage = stage
            return behavior_case

        if behavior_case.status in {"open", "developing"}:
            behavior_case.status = "confirmed"
            behavior_case.stage = "confirmed"
        return behavior_case

    def close_stale_cases(self, now_ts: int | None = None) -> list[BehaviorCase]:
        reference_now = int(now_ts or time.time())
        updates = []
        for _, behavior_case in list(self._cases.items()):
            if behavior_case.status in {"closed", "invalidated"}:
                continue
            if self._is_downstream_followup_case(behavior_case):
                metadata = self._ensure_downstream_case_metadata(behavior_case)
                active_until = int(metadata.get("active_until") or metadata.get("expire_at") or 0)
                cooling_until = int(metadata.get("cooling_until") or 0)
                closing_until = int(metadata.get("closing_until") or 0)
                previous_status = str(behavior_case.status or "")
                previous_stage = str(behavior_case.stage or "")
                if closing_until and reference_now >= closing_until:
                    metadata["last_lifecycle_stage"] = previous_stage
                    metadata["lifecycle_reason"] = "downstream_window_expired"
                    behavior_case.status = "closed"
                    behavior_case.stage = "expired"
                    self._detach_open_case(behavior_case)
                    if previous_status != behavior_case.status or previous_stage != behavior_case.stage:
                        updates.append(behavior_case)
                    continue
                if cooling_until and reference_now >= cooling_until:
                    metadata["last_lifecycle_stage"] = previous_stage
                    metadata["lifecycle_reason"] = "downstream_window_closing"
                    behavior_case.status = "cooled"
                    behavior_case.stage = "closing"
                    self._detach_open_case(behavior_case)
                    if previous_status != behavior_case.status or previous_stage != behavior_case.stage:
                        updates.append(behavior_case)
                    continue
                if active_until and reference_now >= active_until:
                    metadata["last_lifecycle_stage"] = previous_stage
                    metadata["lifecycle_reason"] = "downstream_window_cooling"
                    behavior_case.status = "cooled"
                    behavior_case.stage = "cooling"
                    self._detach_open_case(behavior_case)
                    if previous_status != behavior_case.status or previous_stage != behavior_case.stage:
                        updates.append(behavior_case)
                    continue
            idle_sec = reference_now - int(behavior_case.last_event_ts or reference_now)
            if idle_sec >= self.closing_sec:
                behavior_case.status = "closed"
                behavior_case.stage = "closed"
                self._detach_open_case(behavior_case)
                updates.append(behavior_case)
                continue
            if idle_sec >= self.cooling_sec and behavior_case.status in {"open", "developing", "confirmed"}:
                behavior_case.status = "cooled"
                behavior_case.stage = "cooldown"
                self._detach_open_case(behavior_case)
                updates.append(behavior_case)
        return updates

    def get_case(self, case_id: str | None) -> BehaviorCase | None:
        if not case_id:
            return None
        return self._cases.get(str(case_id))

    def is_exchange_followup_anchor(self, event: Event, watch_meta: dict | None = None) -> bool:
        watch_meta = watch_meta or {}
        if not is_exchange_strategy_role(watch_meta.get("strategy_role") or event.strategy_role):
            return False
        if str(event.side or "") != "流入":
            return False
        if str(event.intent_type or "") not in EXCHANGE_FOLLOWUP_INTENTS:
            return False
        if not self._is_stable_asset(event.token, event.metadata.get("token_symbol")):
            return False
        return float(event.usd_value or 0.0) >= self._exchange_followup_min_usd(watch_meta)

    def is_exchange_followup_event(self, event: Event, watch_meta: dict | None = None) -> bool:
        watch_meta = watch_meta or {}
        if not is_exchange_strategy_role(watch_meta.get("strategy_role") or event.strategy_role):
            return False
        if str(event.side or "") != "流入":
            return False
        if str(event.intent_type or "") not in EXCHANGE_FOLLOWUP_INTENTS:
            return False
        if not self._is_major_followup_asset(event.token, event.metadata.get("token_symbol")):
            return False
        return float(event.usd_value or 0.0) >= self._exchange_followup_min_usd(watch_meta)

    def is_smart_money_execution_event(self, event: Event, watch_meta: dict | None = None) -> bool:
        watch_meta = watch_meta or {}
        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        if not is_smart_money_strategy_role(strategy_role):
            return False
        if event.kind != "swap" or str(event.intent_type or "") != "swap_execution":
            return False
        if self._direction_bucket(event.side) not in {"buy", "sell"}:
            return False
        base_threshold = float(get_threshold(watch_meta or {"strategy_role": event.strategy_role, "priority": 3}))
        if is_market_maker_strategy_role(strategy_role):
            min_usd = max(1500.0, base_threshold * 0.95)
        elif is_priority_smart_money_strategy_role(strategy_role):
            min_usd = max(500.0, base_threshold * 0.40)
        else:
            min_usd = max(900.0, base_threshold * 0.55)
        return float(event.usd_value or 0.0) >= min_usd

    def is_liquidation_case_event(self, event: Event, watch_meta: dict | None = None) -> bool:
        watch_meta = watch_meta or {}
        if str(watch_meta.get("strategy_role") or event.strategy_role or "unknown") != "lp_pool":
            return False
        if str(event.intent_type or "") not in {"pool_buy_pressure", "pool_sell_pressure"}:
            return False
        liquidation_stage = str(event.metadata.get("liquidation_stage") or "none")
        liquidation_score = float(event.metadata.get("liquidation_score") or 0.0)
        if liquidation_stage not in {"risk", "execution"}:
            return False
        if liquidation_stage == "execution":
            return liquidation_score >= max(LIQUIDATION_EXECUTION_MIN_SCORE - 0.03, 0.80)
        return liquidation_score >= max(LIQUIDATION_RISK_MIN_SCORE - 0.02, 0.60)

    def can_emit_case_notification(self, behavior_case: BehaviorCase, stage: str | None) -> bool:
        allowed, _ = self.case_notification_decision(behavior_case, stage)
        return allowed

    def case_notification_decision(self, behavior_case: BehaviorCase, stage: str | None) -> tuple[bool, str]:
        if self._is_downstream_followup_case(behavior_case):
            return self._downstream_case_notification_decision(behavior_case, stage)
        if self._is_smart_money_case(behavior_case):
            return self._smart_money_case_notification_decision(behavior_case, stage)
        if self._is_liquidation_case(behavior_case):
            return self._liquidation_case_notification_decision(behavior_case, stage)
        if not self._is_exchange_followup_case(behavior_case):
            return True, "not_exchange_followup_case"
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return False, "case_inactive"

        metadata = behavior_case.metadata
        emitted = set(metadata.get("emitted_notification_stages") or [])
        if stage is None:
            return False, "case_stage_not_emittable"
        if stage in emitted:
            return False, "case_stage_already_emitted"
        if len(emitted) >= 2:
            return False, "case_notification_cap_reached"
        if stage == "followup_confirmed" and not bool(metadata.get("followup_confirmed")):
            return False, "followup_not_confirmed"
        return True, "allowed"

    def mark_case_notification_emitted(
        self,
        behavior_case: BehaviorCase,
        stage: str,
        signal: Signal | None = None,
    ) -> BehaviorCase:
        if self._is_downstream_followup_case(behavior_case):
            metadata = self._ensure_downstream_case_metadata(behavior_case)
            emitted = list(metadata.get("emitted_notification_stages") or [])
            if stage not in emitted:
                emitted.append(stage)
            metadata["emitted_notification_stages"] = emitted[-self.downstream_followup_max_notifications:]
            metadata["emitted_notification_count"] = len(metadata["emitted_notification_stages"])
            signal_id = ""
            if signal is not None:
                signal_id = str(signal.signal_id or signal.event_id or signal.tx_hash or "")
            metadata["last_notification_signal_id"] = signal_id
            metadata["last_notification_stage"] = stage
            return behavior_case

        if self._is_smart_money_case(behavior_case):
            metadata = self._ensure_smart_money_case_metadata(behavior_case)
            emitted = list(metadata.get("emitted_notification_stages") or [])
            if stage not in emitted:
                emitted.append(stage)
            metadata["emitted_notification_stages"] = emitted[-2:]

            signal_id = ""
            if signal is not None:
                signal_id = str(signal.signal_id or signal.event_id or signal.tx_hash or "")

            if stage == "execution_opened":
                metadata["execution_opened_notified"] = True
                metadata["execution_opened_signal_id"] = signal_id
            elif stage == "continuation_confirmed":
                metadata["continuation_confirmed_notified"] = True
                metadata["continuation_confirmed_signal_id"] = signal_id
            return behavior_case

        if self._is_liquidation_case(behavior_case):
            metadata = self._ensure_liquidation_case_metadata(behavior_case)
            emitted = list(metadata.get("emitted_notification_stages") or [])
            if stage not in emitted:
                emitted.append(stage)
            metadata["emitted_notification_stages"] = emitted[-3:]

            signal_id = ""
            if signal is not None:
                signal_id = str(signal.signal_id or signal.event_id or signal.tx_hash or "")

            if stage == "risk_opened":
                metadata["risk_opened_notified"] = True
                metadata["risk_opened_signal_id"] = signal_id
            elif stage == "risk_escalating":
                metadata["risk_escalating_notified"] = True
                metadata["risk_escalating_signal_id"] = signal_id
            elif stage == "execution_confirmed":
                metadata["execution_confirmed_notified"] = True
                metadata["execution_confirmed_signal_id"] = signal_id
            return behavior_case

        metadata = self._ensure_exchange_followup_metadata(behavior_case)
        emitted = list(metadata.get("emitted_notification_stages") or [])
        if stage not in emitted:
            emitted.append(stage)
        metadata["emitted_notification_stages"] = emitted[-2:]

        signal_id = ""
        if signal is not None:
            signal_id = str(signal.signal_id or signal.event_id or signal.tx_hash or "")

        if stage == "anchor_opened":
            metadata["anchor_notified"] = True
            metadata["anchor_signal_id"] = signal_id
        elif stage == "followup_confirmed":
            metadata["followup_notified"] = True
            metadata["followup_signal_id"] = signal_id
        return behavior_case

    def _smart_money_case_notification_decision(
        self,
        behavior_case: BehaviorCase,
        stage: str | None,
    ) -> tuple[bool, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return False, "smart_money_case_inactive"

        metadata = self._ensure_smart_money_case_metadata(behavior_case)
        emitted = set(metadata.get("emitted_notification_stages") or [])
        if stage is None:
            return False, "smart_money_case_stage_not_emittable"
        if stage not in {"execution_opened", "continuation_confirmed"}:
            return False, "smart_money_case_stage_not_emittable"
        if stage in emitted:
            return False, "smart_money_case_stage_already_emitted"
        if len(emitted) >= 2:
            return False, "smart_money_case_notification_cap_reached"
        if stage == "continuation_confirmed" and not bool(metadata.get("continuation_confirmed")):
            return False, "smart_money_case_not_confirmed"
        return True, "allowed"

    def _liquidation_case_notification_decision(
        self,
        behavior_case: BehaviorCase,
        stage: str | None,
    ) -> tuple[bool, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return False, "liquidation_case_inactive"

        metadata = self._ensure_liquidation_case_metadata(behavior_case)
        emitted = set(metadata.get("emitted_notification_stages") or [])
        if stage is None:
            return False, "liquidation_case_stage_not_emittable"
        if stage not in {"risk_opened", "risk_escalating", "execution_confirmed"}:
            return False, "liquidation_case_stage_not_emittable"
        if stage in emitted:
            return False, "liquidation_case_stage_already_emitted"
        if len(emitted) >= 3:
            return False, "liquidation_case_notification_cap_reached"
        if stage == "risk_escalating" and not (
            int(metadata.get("risk_hits") or 0) >= 2
            or len(list(metadata.get("pools_seen") or [])) >= 2
            or float(metadata.get("max_liquidation_score") or 0.0) >= max(LIQUIDATION_RISK_MIN_SCORE + 0.08, 0.74)
        ):
            return False, "liquidation_case_not_escalated"
        if stage == "execution_confirmed" and not bool(metadata.get("execution_confirmed")):
            return False, "liquidation_case_execution_not_confirmed"
        return True, "allowed"

    def _downstream_case_notification_decision(
        self,
        behavior_case: BehaviorCase,
        stage: str | None,
    ) -> tuple[bool, str]:
        metadata = self._ensure_downstream_case_metadata(behavior_case)
        if behavior_case.status in {"closed", "invalidated"}:
            return False, "downstream_window_expired"
        if behavior_case.status == "cooled":
            return False, "downstream_case_inactive"
        emitted = set(metadata.get("emitted_notification_stages") or [])
        if stage is None:
            return False, "downstream_case_stage_not_emittable"
        if stage not in {
            "followup_opened",
            "downstream_seen",
            "exchange_arrival_confirmed",
            "swap_execution_confirmed",
            "distribution_confirmed",
        }:
            return False, "downstream_case_stage_not_emittable"
        if stage in emitted:
            return False, "downstream_case_already_emitted"
        if len(emitted) >= self.downstream_followup_max_notifications:
            return False, "downstream_case_notification_cap_reached"
        if stage != "followup_opened" and not bool(metadata.get("current_event_is_followup")):
            return False, str(metadata.get("current_followup_reason") or "downstream_followup_not_confirmed")
        return True, "allowed"

    def _find_matching_case(self, event: Event, now_ts: int) -> BehaviorCase | None:
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        intent_family = self._intent_family(event.intent_type, direction_bucket)

        candidates = []
        for case_id in list(self._open_case_ids_by_watch.get(watch_address, set())):
            behavior_case = self._cases.get(case_id)
            if behavior_case is None:
                continue
            if (
                self._is_exchange_followup_case(behavior_case)
                or self._is_smart_money_case(behavior_case)
                or self._is_liquidation_case(behavior_case)
            ):
                continue
            if (behavior_case.token or "native").lower() != token:
                continue
            if behavior_case.direction_bucket != direction_bucket:
                continue
            if not self._intent_compatible(behavior_case.intent_type, intent_family, event.intent_type, direction_bucket):
                continue
            if now_ts - int(behavior_case.last_event_ts or now_ts) > self.merge_window_sec:
                continue
            candidates.append(behavior_case)

        if not candidates:
            return None

        candidates.sort(key=lambda item: int(item.last_event_ts or 0), reverse=True)
        return candidates[0]

    def _open_case(self, event: Event, watch_meta: dict | None = None) -> BehaviorCase:
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        intent_family = self._intent_family(event.intent_type, direction_bucket)
        case_id = self._build_case_id(
            watch_address=watch_address,
            token=token,
            direction_bucket=direction_bucket,
            intent_family=intent_family,
            ts=int(event.ts or time.time()),
        )
        behavior_case = BehaviorCase(
            case_id=case_id,
            watch_address=watch_address,
            token=token,
            direction_bucket=direction_bucket,
            intent_type=intent_family,
            opened_at=int(event.ts or time.time()),
            last_event_ts=int(event.ts or time.time()),
            status="open",
            stage="initial",
            root_tx_hash=str(event.tx_hash or ""),
        )
        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta or {})
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        self._cases[case_id] = behavior_case
        self._attach_open_case(behavior_case)
        return behavior_case

    def _match_exchange_followup_case(
        self,
        event: Event,
        watch_meta: dict,
        now_ts: int,
    ) -> dict:
        existing = self.match_exchange_followup_case(event, watch_meta=watch_meta, now_ts=now_ts)
        is_anchor = self.is_exchange_followup_anchor(event, watch_meta=watch_meta)
        is_followup = self.is_exchange_followup_event(event, watch_meta=watch_meta)
        opened = False
        confirmed = False

        if existing is None and not is_anchor:
            return {
                "case": None,
                "created": False,
                "exchange_followup_anchor": False,
                "exchange_followup_confirmed": False,
            }
        if existing is not None and not (is_anchor or is_followup):
            return {
                "case": None,
                "created": False,
                "exchange_followup_anchor": False,
                "exchange_followup_confirmed": False,
            }

        if existing is None and is_anchor:
            existing = self._open_exchange_followup_case(event, watch_meta=watch_meta)
            opened = True
        elif existing is not None and is_anchor:
            metadata = self._ensure_exchange_followup_metadata(existing)
            metadata["deadline_ts"] = max(
                int(metadata.get("deadline_ts") or 0),
                int(event.ts or now_ts) + self.exchange_followup_window_sec,
            )

        if existing is not None and is_followup:
            confirmed = self.attach_exchange_followup_event(existing, event, watch_meta=watch_meta)

        if existing is not None:
            metadata = self._ensure_exchange_followup_metadata(existing)
            metadata["current_event_is_anchor"] = bool(
                is_anchor and not confirmed and not bool(metadata.get("followup_confirmed"))
            )
            metadata["current_event_is_followup_confirmation"] = bool(confirmed)

        return {
            "case": existing,
            "created": opened,
            "exchange_followup_anchor": bool(is_anchor and not confirmed),
            "exchange_followup_confirmed": bool(confirmed),
        }

    def _match_smart_money_execution_case(
        self,
        event: Event,
        watch_meta: dict,
        now_ts: int,
    ) -> dict:
        if not self.is_smart_money_execution_event(event, watch_meta=watch_meta):
            return {
                "case": None,
                "created": False,
                "smart_money_case": False,
            }

        existing = self.match_smart_money_execution_case(event, now_ts=now_ts)
        opened = False
        if existing is None:
            existing = self._open_smart_money_execution_case(event, watch_meta=watch_meta)
            opened = True

        metadata = self._ensure_smart_money_case_metadata(existing)
        metadata["current_event_is_execution"] = True
        return {
            "case": existing,
            "created": opened,
            "smart_money_case": True,
        }

    def _match_liquidation_case(
        self,
        event: Event,
        watch_meta: dict,
        now_ts: int,
    ) -> dict:
        if not self.is_liquidation_case_event(event, watch_meta=watch_meta):
            return {
                "case": None,
                "created": False,
                "liquidation_case": False,
            }

        existing = self.match_liquidation_case(event, now_ts=now_ts)
        opened = False
        if existing is None:
            existing = self._open_liquidation_case(event, watch_meta=watch_meta)
            opened = True

        metadata = self._ensure_liquidation_case_metadata(existing)
        metadata["current_event_stage"] = str(event.metadata.get("liquidation_stage") or "none")
        metadata["execution_confirmed"] = bool(
            metadata.get("execution_confirmed")
            or str(event.metadata.get("liquidation_stage") or "") == "execution"
        )
        return {
            "case": existing,
            "created": opened,
            "liquidation_case": True,
        }

    def _match_downstream_followup_case(
        self,
        event: Event,
        watch_meta: dict,
        now_ts: int,
    ) -> dict:
        existing = self.match_downstream_followup_case(event, now_ts=now_ts)
        is_anchor = self.is_downstream_followup_anchor(event, watch_meta=watch_meta)
        followup = self._classify_downstream_followup_event(event, existing)
        opened = False

        if existing is None and not is_anchor:
            return {
                "case": None,
                "created": False,
                "downstream_followup_case": False,
                "downstream_followup_anchor": False,
                "downstream_followup_type": "",
                "downstream_followup_stage": "",
            }
        if existing is not None and not (is_anchor or followup.get("matched")):
            metadata = self._ensure_downstream_case_metadata(existing)
            if str(event.address or "").lower() == str(metadata.get("downstream_address") or existing.watch_address or "").lower():
                metadata["current_event_is_anchor"] = False
                metadata["current_event_is_followup"] = False
                metadata["current_followup_type"] = str(followup.get("followup_type") or "")
                metadata["current_followup_reason"] = str(followup.get("reason") or "downstream_followup_not_confirmed")
                metadata["current_stage"] = str(existing.stage or "followup_opened")
                metadata["last_observed_usd"] = round(float(event.usd_value or 0.0), 2)
                metadata["last_observed_tx_hash"] = str(event.tx_hash or "")
                return {
                    "case": existing,
                    "created": False,
                    "downstream_followup_case": True,
                    "downstream_followup_anchor": False,
                    "downstream_followup_type": str(followup.get("followup_type") or ""),
                    "downstream_followup_stage": str(existing.stage or "followup_opened"),
                }
            return {
                "case": None,
                "created": False,
                "downstream_followup_case": False,
                "downstream_followup_anchor": False,
                "downstream_followup_type": "",
                "downstream_followup_stage": "",
            }

        if existing is None and is_anchor:
            existing = self._open_downstream_followup_case(event, watch_meta=watch_meta)
            opened = True
        elif existing is not None and is_anchor:
            self._refresh_downstream_anchor(existing, event, watch_meta=watch_meta, now_ts=now_ts)

        if existing is not None and followup.get("matched"):
            self.attach_downstream_followup_event(existing, event, followup)

        if existing is not None:
            metadata = self._ensure_downstream_case_metadata(existing)
            metadata["current_event_is_anchor"] = bool(is_anchor)
            metadata["current_event_is_followup"] = bool(followup.get("matched"))
            metadata["current_followup_type"] = str(followup.get("followup_type") or metadata.get("current_followup_type") or "")
            metadata["current_followup_reason"] = str(followup.get("reason") or metadata.get("current_followup_reason") or "")
            metadata["current_stage"] = str(followup.get("stage") or existing.stage or "")

        return {
            "case": existing,
            "created": opened,
            "downstream_followup_case": bool(existing is not None),
            "downstream_followup_anchor": bool(is_anchor),
            "downstream_followup_type": str(followup.get("followup_type") or ""),
            "downstream_followup_stage": str(followup.get("stage") or (existing.stage if existing is not None else "")),
        }

    def match_downstream_followup_case(
        self,
        event: Event,
        now_ts: int | None = None,
    ) -> BehaviorCase | None:
        reference_ts = int(now_ts or event.ts or time.time())
        candidates = []
        case_address = self._downstream_case_address(event)
        if not case_address:
            return None

        for behavior_case in list(self._cases.values()):
            if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
                continue
            if behavior_case.status in {"closed", "invalidated"}:
                continue
            metadata = self._ensure_downstream_case_metadata(behavior_case)
            if str(metadata.get("downstream_address") or behavior_case.watch_address or "").lower() != case_address:
                continue
            closing_until = int(metadata.get("closing_until") or metadata.get("expire_at") or 0)
            if closing_until and reference_ts > closing_until:
                continue
            candidates.append(behavior_case)

        if not candidates:
            return None
        candidates.sort(key=lambda item: int(item.last_event_ts or 0), reverse=True)
        return candidates[0]

    def match_exchange_followup_case(
        self,
        event: Event,
        watch_meta: dict | None = None,
        now_ts: int | None = None,
    ) -> BehaviorCase | None:
        watch_address = str(event.address or "").lower()
        reference_ts = int(now_ts or event.ts or time.time())

        candidates = []
        for case_id in list(self._open_case_ids_by_watch.get(watch_address, set())):
            behavior_case = self._cases.get(case_id)
            if behavior_case is None or not self._is_exchange_followup_case(behavior_case):
                continue
            metadata = self._ensure_exchange_followup_metadata(behavior_case)
            deadline_ts = int(metadata.get("deadline_ts") or 0)
            if deadline_ts and reference_ts > deadline_ts and not bool(metadata.get("followup_confirmed")):
                continue
            if reference_ts - int(behavior_case.last_event_ts or reference_ts) > self.merge_window_sec:
                continue
            candidates.append(behavior_case)

        if not candidates:
            return None
        candidates.sort(key=lambda item: int(item.last_event_ts or 0), reverse=True)
        return candidates[0]

    def match_smart_money_execution_case(
        self,
        event: Event,
        now_ts: int | None = None,
    ) -> BehaviorCase | None:
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        reference_ts = int(now_ts or event.ts or time.time())
        candidates = []

        for case_id in list(self._open_case_ids_by_token.get(token, set())):
            behavior_case = self._cases.get(case_id)
            if behavior_case is None or not self._is_smart_money_case(behavior_case):
                continue
            if behavior_case.direction_bucket != direction_bucket:
                continue
            if behavior_case.status in {"cooled", "closed", "invalidated"}:
                continue
            if reference_ts - int(behavior_case.last_event_ts or reference_ts) > self.smart_money_case_window_sec:
                continue
            candidates.append(behavior_case)

        if not candidates:
            return None
        candidates.sort(key=lambda item: int(item.last_event_ts or 0), reverse=True)
        return candidates[0]

    def match_liquidation_case(
        self,
        event: Event,
        now_ts: int | None = None,
    ) -> BehaviorCase | None:
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        reference_ts = int(now_ts or event.ts or time.time())
        current_protocols = set(event.metadata.get("liquidation_protocols") or [])
        current_pool = str(event.address or "").lower()
        candidates = []

        for case_id in list(self._open_case_ids_by_token.get(token, set())):
            behavior_case = self._cases.get(case_id)
            if behavior_case is None or not self._is_liquidation_case(behavior_case):
                continue
            if behavior_case.direction_bucket != direction_bucket:
                continue
            if behavior_case.status in {"cooled", "closed", "invalidated"}:
                continue
            if reference_ts - int(behavior_case.last_event_ts or reference_ts) > self.liquidation_case_window_sec:
                continue
            metadata = self._ensure_liquidation_case_metadata(behavior_case)
            existing_protocols = set(metadata.get("protocols_seen") or [])
            existing_pools = set(metadata.get("pools_seen") or [])
            if current_protocols and existing_protocols and not (current_protocols & existing_protocols):
                continue
            if current_pool and existing_pools and current_pool not in existing_pools and not current_protocols:
                continue
            candidates.append(behavior_case)

        if not candidates:
            return None
        candidates.sort(key=lambda item: int(item.last_event_ts or 0), reverse=True)
        return candidates[0]

    def attach_exchange_followup_event(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict | None = None,
    ) -> bool:
        metadata = self._ensure_exchange_followup_metadata(behavior_case)
        symbol = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        token_ref = (event.token or symbol or "native").lower()
        tokens_seen = list(metadata.get("followup_tokens_seen") or [])
        if symbol and symbol not in tokens_seen:
            tokens_seen.append(symbol)
        metadata["followup_tokens_seen"] = tokens_seen

        confirmations = list(metadata.get("followup_confirmations") or [])
        confirmation = {
            "event_id": str(event.event_id or ""),
            "tx_hash": str(event.tx_hash or ""),
            "ts": int(event.ts or time.time()),
            "token": token_ref,
            "symbol": symbol,
            "usd_value": round(float(event.usd_value or 0.0), 2),
        }
        duplicate = any(
            item.get("event_id") == confirmation["event_id"] or (
                item.get("tx_hash") == confirmation["tx_hash"] and item.get("token") == confirmation["token"]
            )
            for item in confirmations
        )
        if not duplicate:
            confirmations.append(confirmation)
        metadata["followup_confirmations"] = confirmations
        metadata["followup_confirmed"] = True
        behavior_case.status = "confirmed"
        behavior_case.stage = "followup_confirmed"
        return True

    def attach_downstream_followup_event(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        followup: dict,
    ) -> bool:
        metadata = self._ensure_downstream_case_metadata(behavior_case)
        tx_hash = str(event.tx_hash or "")
        followup_events = list(metadata.get("followup_events") or [])
        followup_type = str(followup.get("followup_type") or "downstream_seen")
        stage = str(followup.get("stage") or "downstream_seen")
        record = {
            "event_id": str(event.event_id or ""),
            "tx_hash": tx_hash,
            "ts": int(event.ts or time.time()),
            "followup_type": followup_type,
            "stage": stage,
            "usd_value": round(float(event.usd_value or 0.0), 2),
            "counterparty": str(followup.get("counterparty") or ""),
            "counterparty_label": str(followup.get("counterparty_label") or ""),
            "token_symbol": self._display_symbol(event.token, event.metadata.get("token_symbol")),
        }
        duplicate = any(
            item.get("event_id") == record["event_id"] or (
                item.get("tx_hash") == record["tx_hash"] and item.get("followup_type") == record["followup_type"]
            )
            for item in followup_events
        )
        if not duplicate:
            followup_events.append(record)
        metadata["followup_events"] = followup_events[-12:]
        metadata["followup_confirmed"] = True
        metadata["current_event_is_followup"] = True
        metadata["current_followup_type"] = followup_type
        metadata["current_followup_reason"] = str(followup.get("reason") or "downstream_followup_confirmed")
        metadata["last_followup_type"] = followup_type
        metadata["last_followup_stage"] = stage
        metadata["last_followup_tx_hash"] = tx_hash
        metadata["last_followup_usd"] = round(float(event.usd_value or 0.0), 2)
        metadata["last_counterparty"] = str(followup.get("counterparty") or "")
        metadata["last_counterparty_label"] = str(followup.get("counterparty_label") or "")
        if followup_type == "exchange_arrival":
            metadata["downstream_exchange_arrival"] = True
        if followup_type in {"swap_execution", "protocol_path"}:
            metadata["downstream_execution_confirmed"] = True
        if followup_type == "distribution":
            metadata["downstream_distribution_detected"] = True
        if followup_type == "return_flow":
            metadata["downstream_return_confirmed"] = True
        if followup.get("counterparty"):
            destinations = list(metadata.get("destinations_seen") or [])
            counterparty = str(followup.get("counterparty") or "")
            if counterparty not in destinations:
                destinations.append(counterparty)
            metadata["destinations_seen"] = destinations[-8:]
        behavior_case.status = "developing"
        behavior_case.stage = stage
        return True

    def _open_exchange_followup_case(self, event: Event, watch_meta: dict | None = None) -> BehaviorCase:
        watch_meta = watch_meta or {}
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        case_id = self._build_exchange_followup_case_id(
            watch_address=watch_address,
            ts=int(event.ts or time.time()),
        )
        behavior_case = BehaviorCase(
            case_id=case_id,
            watch_address=watch_address,
            token=token,
            direction_bucket="buy",
            intent_type=EXCHANGE_FOLLOWUP_CASE_FAMILY,
            opened_at=int(event.ts or time.time()),
            last_event_ts=int(event.ts or time.time()),
            status="open",
            stage="anchor_tracking",
            root_tx_hash=str(event.tx_hash or ""),
        )
        behavior_case.metadata = {
            "case_family": EXCHANGE_FOLLOWUP_CASE_FAMILY,
            "anchor_token": token,
            "anchor_symbol": self._display_symbol(event.token, event.metadata.get("token_symbol")),
            "anchor_usd": round(float(event.usd_value or 0.0), 2),
            "expected_assets": sorted(MAJOR_FOLLOWUP_ASSET_SYMBOLS),
            "followup_tokens_seen": [],
            "followup_confirmations": [],
            "deadline_ts": int(event.ts or time.time()) + self.exchange_followup_window_sec,
            "watch_strategy_role": str(watch_meta.get("strategy_role") or event.strategy_role or "unknown"),
            "anchor_notified": False,
            "followup_notified": False,
            "anchor_signal_id": "",
            "followup_signal_id": "",
            "emitted_notification_stages": [],
            "followup_confirmed": False,
            "current_event_is_anchor": True,
            "current_event_is_followup_confirmation": False,
        }
        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta)
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        self._cases[case_id] = behavior_case
        self._attach_open_case(behavior_case)
        return behavior_case

    def _open_downstream_followup_case(self, event: Event, watch_meta: dict | None = None) -> BehaviorCase:
        watch_meta = watch_meta or {}
        downstream_address = self._downstream_counterparty_address(event)
        downstream_meta = get_address_meta(downstream_address)
        anchor_label = str(watch_meta.get("label") or event.address or "重点地址")
        anchor_symbol = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        case_id = self._build_downstream_case_id(
            downstream_address=downstream_address,
            ts=int(event.ts or time.time()),
        )
        behavior_case = BehaviorCase(
            case_id=case_id,
            watch_address=downstream_address,
            token=(event.token or "native").lower(),
            direction_bucket="sell",
            intent_type=DOWNSTREAM_FOLLOWUP_CASE_FAMILY,
            opened_at=int(event.ts or time.time()),
            last_event_ts=int(event.ts or time.time()),
            status="open",
            stage="followup_opened",
            root_tx_hash=str(event.tx_hash or ""),
        )
        behavior_case.metadata = {
            "case_family": DOWNSTREAM_FOLLOWUP_CASE_FAMILY,
            "anchor_watch_address": str(event.address or "").lower(),
            "anchor_label": anchor_label,
            "anchor_strategy_role": str(watch_meta.get("strategy_role") or event.strategy_role or "unknown"),
            "anchor_priority": int(watch_meta.get("priority", 1) or 1),
            "downstream_address": downstream_address,
            "downstream_label": self._downstream_label(downstream_address, downstream_meta),
            "root_tx_hash": str(event.tx_hash or ""),
            "anchor_token": (event.token or "native").lower(),
            "anchor_symbol": anchor_symbol,
            "anchor_usd_value": round(float(event.usd_value or 0.0), 2),
            "anchor_intent": str(event.intent_type or "pure_transfer"),
            "opened_at": int(event.ts or time.time()),
            "active_until": int(event.ts or time.time()) + self.downstream_followup_window_sec,
            "cooling_until": int(event.ts or time.time()) + self.downstream_followup_cooling_sec,
            "closing_until": int(event.ts or time.time()) + self.downstream_followup_closing_sec,
            "expire_at": int(event.ts or time.time()) + self.downstream_followup_window_sec,
            "deadline_ts": int(event.ts or time.time()) + self.downstream_followup_window_sec,
            "window_sec": int(self.downstream_followup_window_sec),
            "max_notifications": int(self.downstream_followup_max_notifications),
            "hop": 1,
            "reason": "anchor_large_transfer",
            "strategy_hint": "runtime_adjacent_watch",
            "followup_confirmed": False,
            "downstream_execution_confirmed": False,
            "downstream_exchange_arrival": False,
            "downstream_distribution_detected": False,
            "downstream_return_confirmed": False,
            "emitted_notification_stages": [],
            "followup_events": [],
            "destinations_seen": [],
            "current_event_is_anchor": True,
            "current_event_is_followup": False,
            "current_followup_type": "",
            "current_followup_reason": "downstream_anchor_opened",
            "current_stage": "followup_opened",
        }
        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta)
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        self._cases[case_id] = behavior_case
        self._attach_open_case(behavior_case)
        return behavior_case

    def _refresh_downstream_anchor(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict | None = None,
        now_ts: int | None = None,
    ) -> None:
        watch_meta = watch_meta or {}
        metadata = self._ensure_downstream_case_metadata(behavior_case)
        reference_ts = int(now_ts or event.ts or time.time())
        metadata["anchor_watch_address"] = str(event.address or metadata.get("anchor_watch_address") or "").lower()
        metadata["anchor_label"] = str(watch_meta.get("label") or metadata.get("anchor_label") or "")
        metadata["anchor_strategy_role"] = str(watch_meta.get("strategy_role") or metadata.get("anchor_strategy_role") or event.strategy_role or "unknown")
        metadata["anchor_priority"] = int(watch_meta.get("priority", metadata.get("anchor_priority", 1)) or 1)
        metadata["anchor_usd_value"] = round(max(float(metadata.get("anchor_usd_value") or 0.0), float(event.usd_value or 0.0)), 2)
        metadata["anchor_symbol"] = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        metadata["active_until"] = max(int(metadata.get("active_until") or 0), reference_ts + self.downstream_followup_window_sec)
        metadata["cooling_until"] = max(int(metadata.get("cooling_until") or 0), reference_ts + self.downstream_followup_cooling_sec)
        metadata["closing_until"] = max(int(metadata.get("closing_until") or 0), reference_ts + self.downstream_followup_closing_sec)
        metadata["expire_at"] = int(metadata.get("active_until") or reference_ts + self.downstream_followup_window_sec)
        metadata["deadline_ts"] = int(metadata.get("active_until") or reference_ts + self.downstream_followup_window_sec)
        metadata["current_event_is_anchor"] = True
        metadata["current_followup_reason"] = "downstream_anchor_refreshed"

    def _open_smart_money_execution_case(self, event: Event, watch_meta: dict | None = None) -> BehaviorCase:
        watch_meta = watch_meta or {}
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        case_id = self._build_smart_money_case_id(
            token=token,
            direction_bucket=direction_bucket,
            ts=int(event.ts or time.time()),
        )
        behavior_case = BehaviorCase(
            case_id=case_id,
            watch_address=watch_address,
            token=token,
            direction_bucket=direction_bucket,
            intent_type=SMART_MONEY_EXECUTION_CASE_FAMILY,
            opened_at=int(event.ts or time.time()),
            last_event_ts=int(event.ts or time.time()),
            status="open",
            stage="execution_opened",
            root_tx_hash=str(event.tx_hash or ""),
        )
        symbol = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        behavior_case.metadata = {
            "case_family": SMART_MONEY_EXECUTION_CASE_FAMILY,
            "execution_count": 0,
            "cohort_addresses": [watch_address] if watch_address else [],
            "actor_execution_counts": {watch_address: 0} if watch_address else {},
            "first_execution_usd": round(float(event.usd_value or 0.0), 2),
            "latest_execution_usd": round(float(event.usd_value or 0.0), 2),
            "max_execution_usd": round(float(event.usd_value or 0.0), 2),
            "largest_execution_tx": str(event.tx_hash or ""),
            "latest_symbol": symbol,
            "watch_strategy_role": str(watch_meta.get("strategy_role") or event.strategy_role or "unknown"),
            "continuation_confirmed": False,
            "current_event_is_execution": True,
            "last_actor": watch_address,
            "same_actor_continuation": False,
            "size_expansion_ratio": 1.0,
        }
        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta)
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        self._cases[case_id] = behavior_case
        self._attach_open_case(behavior_case)
        return behavior_case

    def _open_liquidation_case(self, event: Event, watch_meta: dict | None = None) -> BehaviorCase:
        watch_meta = watch_meta or {}
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        case_id = self._build_liquidation_case_id(
            token=token,
            direction_bucket=direction_bucket,
            ts=int(event.ts or time.time()),
        )
        behavior_case = BehaviorCase(
            case_id=case_id,
            watch_address=watch_address,
            token=token,
            direction_bucket=direction_bucket,
            intent_type=LIQUIDATION_CASE_FAMILY,
            opened_at=int(event.ts or time.time()),
            last_event_ts=int(event.ts or time.time()),
            status="open",
            stage="risk_opened",
            root_tx_hash=str(event.tx_hash or ""),
        )
        symbol = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        protocols = list(event.metadata.get("liquidation_protocols") or [])
        behavior_case.metadata = {
            "case_family": LIQUIDATION_CASE_FAMILY,
            "focus_symbol": symbol,
            "watch_strategy_role": str(watch_meta.get("strategy_role") or event.strategy_role or "unknown"),
            "protocols_seen": protocols,
            "protocol_roles_seen": list(event.metadata.get("liquidation_roles") or []),
            "pools_seen": [watch_address] if watch_address else [],
            "risk_hits": 0,
            "execution_hits": 0,
            "max_liquidation_score": round(float(event.metadata.get("liquidation_score") or 0.0), 3),
            "latest_liquidation_score": round(float(event.metadata.get("liquidation_score") or 0.0), 3),
            "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
            "execution_confirmed": str(event.metadata.get("liquidation_stage") or "") == "execution",
            "current_event_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "emitted_notification_stages": [],
            "risk_opened_notified": False,
            "risk_opened_signal_id": "",
            "risk_escalating_notified": False,
            "risk_escalating_signal_id": "",
            "execution_confirmed_notified": False,
            "execution_confirmed_signal_id": "",
        }
        behavior_case.summary = self._case_summary(behavior_case, event, watch_meta)
        behavior_case.followup_steps = self._followup_steps(behavior_case, event)
        self._cases[case_id] = behavior_case
        self._attach_open_case(behavior_case)
        return behavior_case

    def _invalidate_conflicting_cases(self, event: Event, now_ts: int) -> list[BehaviorCase]:
        watch_address = str(event.address or "").lower()
        token = (event.token or "native").lower()
        direction_bucket = self._direction_bucket(event.side)
        updates = []
        seen_case_ids = set()

        for case_id in list(self._open_case_ids_by_watch.get(watch_address, set())):
            behavior_case = self._cases.get(case_id)
            if behavior_case is None:
                continue
            if self._is_exchange_followup_case(behavior_case):
                metadata = self._ensure_exchange_followup_metadata(behavior_case)
                if now_ts > int(metadata.get("deadline_ts") or 0) and not bool(metadata.get("followup_confirmed")):
                    behavior_case.status = "cooled"
                    behavior_case.stage = "cooldown"
                    self._detach_open_case(behavior_case)
                    updates.append(behavior_case)
                    seen_case_ids.add(behavior_case.case_id)
                    continue
                same_family_asset = (
                    str(event.intent_type or "") in EXCHANGE_FOLLOWUP_INTENTS
                    or self._is_stable_asset(event.token, event.metadata.get("token_symbol"))
                    or self._is_major_followup_asset(event.token, event.metadata.get("token_symbol"))
                )
                if (
                    same_family_asset
                    and behavior_case.direction_bucket != direction_bucket
                    and now_ts - int(behavior_case.last_event_ts or now_ts) <= self.exchange_followup_window_sec
                ):
                    behavior_case.status = "invalidated"
                    behavior_case.stage = "invalidated"
                    self._detach_open_case(behavior_case)
                    updates.append(behavior_case)
                    seen_case_ids.add(behavior_case.case_id)
                continue
            if self._is_downstream_followup_case(behavior_case):
                metadata = self._ensure_downstream_case_metadata(behavior_case)
                active_until = int(metadata.get("active_until") or metadata.get("expire_at") or 0)
                closing_until = int(metadata.get("closing_until") or 0)
                if closing_until and now_ts > closing_until:
                    behavior_case.status = "closed"
                    behavior_case.stage = "expired"
                    metadata["lifecycle_reason"] = "downstream_window_expired"
                    self._detach_open_case(behavior_case)
                    updates.append(behavior_case)
                    seen_case_ids.add(behavior_case.case_id)
                    continue
                if active_until and now_ts > active_until:
                    behavior_case.status = "cooled"
                    behavior_case.stage = "cooling"
                    metadata["lifecycle_reason"] = "downstream_window_cooling"
                    self._detach_open_case(behavior_case)
                    updates.append(behavior_case)
                    seen_case_ids.add(behavior_case.case_id)
                continue
            if self._is_smart_money_case(behavior_case):
                continue
            if self._is_liquidation_case(behavior_case):
                if behavior_case.direction_bucket == direction_bucket:
                    continue
                if now_ts - int(behavior_case.last_event_ts or now_ts) > self.liquidation_case_window_sec:
                    continue
                behavior_case.status = "invalidated"
                behavior_case.stage = "invalidated"
                self._detach_open_case(behavior_case)
                updates.append(behavior_case)
                seen_case_ids.add(behavior_case.case_id)
                continue
            if (behavior_case.token or "native").lower() != token:
                continue
            if behavior_case.direction_bucket == direction_bucket:
                continue
            if now_ts - int(behavior_case.last_event_ts or now_ts) > self.merge_window_sec:
                continue
            behavior_case.status = "invalidated"
            behavior_case.stage = "invalidated"
            self._detach_open_case(behavior_case)
            updates.append(behavior_case)
            seen_case_ids.add(behavior_case.case_id)

        if self.is_smart_money_execution_event(event):
            for case_id in list(self._open_case_ids_by_token.get(token, set())):
                behavior_case = self._cases.get(case_id)
                if behavior_case is None or not self._is_smart_money_case(behavior_case):
                    continue
                if behavior_case.case_id in seen_case_ids:
                    continue
                if behavior_case.direction_bucket == direction_bucket:
                    continue
                if now_ts - int(behavior_case.last_event_ts or now_ts) > self.smart_money_case_window_sec:
                    continue
                behavior_case.status = "invalidated"
                behavior_case.stage = "invalidated"
                self._detach_open_case(behavior_case)
                updates.append(behavior_case)
                seen_case_ids.add(behavior_case.case_id)

        return updates

    def _attach_open_case(self, behavior_case: BehaviorCase) -> None:
        self._open_case_ids_by_watch[behavior_case.watch_address].add(behavior_case.case_id)
        self._open_case_ids_by_token[(behavior_case.token or "native").lower()].add(behavior_case.case_id)

    def _detach_open_case(self, behavior_case: BehaviorCase) -> None:
        self._open_case_ids_by_watch.get(behavior_case.watch_address, set()).discard(behavior_case.case_id)
        self._open_case_ids_by_token.get((behavior_case.token or "native").lower(), set()).discard(behavior_case.case_id)

    def _status_and_stage(self, behavior_case: BehaviorCase, event: Event) -> tuple[str, str]:
        if self._is_downstream_followup_case(behavior_case):
            return self._downstream_status_and_stage(behavior_case)
        if self._is_exchange_followup_case(behavior_case):
            return self._exchange_status_and_stage(behavior_case)
        if self._is_smart_money_case(behavior_case):
            return self._smart_money_status_and_stage(behavior_case)
        if self._is_liquidation_case(behavior_case):
            return self._liquidation_status_and_stage(behavior_case)

        event_count = len(behavior_case.event_ids)
        confirmation_score = float(event.confirmation_score or 0.0)

        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return behavior_case.status, behavior_case.stage

        if confirmation_score >= 0.78 or event.intent_stage == "confirmed" or len(behavior_case.signal_ids) >= 1:
            return "confirmed", "confirmed"
        if event_count >= 2 or confirmation_score >= 0.52:
            return "developing", "tracking"
        return "open", "initial"

    def _exchange_status_and_stage(self, behavior_case: BehaviorCase) -> tuple[str, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return behavior_case.status, behavior_case.stage

        metadata = self._ensure_exchange_followup_metadata(behavior_case)
        if bool(metadata.get("followup_confirmed")) or list(metadata.get("followup_confirmations") or []):
            return "confirmed", "followup_confirmed"
        if len(behavior_case.event_ids) >= 2:
            return "developing", "anchor_tracking"
        return "open", "anchor_tracking"

    def _downstream_status_and_stage(self, behavior_case: BehaviorCase) -> tuple[str, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return behavior_case.status, behavior_case.stage

        metadata = self._ensure_downstream_case_metadata(behavior_case)
        if bool(metadata.get("downstream_execution_confirmed")):
            return "confirmed", "swap_execution_confirmed"
        if bool(metadata.get("downstream_exchange_arrival")):
            return "confirmed", "exchange_arrival_confirmed"
        if bool(metadata.get("downstream_distribution_detected")):
            return "developing", "distribution_confirmed"
        if list(metadata.get("followup_events") or []):
            return "developing", str(metadata.get("last_followup_stage") or "downstream_seen")
        return "open", "followup_opened"

    def _smart_money_status_and_stage(self, behavior_case: BehaviorCase) -> tuple[str, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return behavior_case.status, behavior_case.stage

        metadata = self._ensure_smart_money_case_metadata(behavior_case)
        execution_count = int(metadata.get("execution_count") or 0)
        unique_addresses = len(list(metadata.get("cohort_addresses") or []))
        first_execution_usd = float(metadata.get("first_execution_usd") or 0.0)
        max_execution_usd = float(metadata.get("max_execution_usd") or 0.0)
        watch_strategy_role = str(metadata.get("watch_strategy_role") or "unknown")
        same_actor_continuation = bool(metadata.get("same_actor_continuation"))
        size_expansion_ratio = float(metadata.get("size_expansion_ratio") or 1.0)
        priority_smart_money = is_priority_smart_money_strategy_role(watch_strategy_role)
        market_maker = is_market_maker_strategy_role(watch_strategy_role)

        if market_maker:
            confirmed = (
                execution_count >= 3
                or (execution_count >= 2 and unique_addresses >= 2)
                or (execution_count >= 2 and size_expansion_ratio >= 1.80)
            )
        elif priority_smart_money:
            confirmed = (
                execution_count >= 2 and same_actor_continuation
            ) or (
                execution_count >= 2 and size_expansion_ratio >= 1.22
            ) or (
                execution_count >= 2 and unique_addresses >= 2
            )
        else:
            confirmed = (
                execution_count >= 3
                or (execution_count >= 2 and unique_addresses >= 2)
                or (
                    execution_count >= 2
                    and first_execution_usd > 0
                    and max_execution_usd >= first_execution_usd * 1.35
                )
            )
        if confirmed:
            metadata["continuation_confirmed"] = True
            return "confirmed", "execution_followup_confirmed"
        if execution_count >= 2:
            return "developing", "execution_tracking"
        return "open", "execution_opened"

    def _liquidation_status_and_stage(self, behavior_case: BehaviorCase) -> tuple[str, str]:
        if behavior_case.status in {"cooled", "closed", "invalidated"}:
            return behavior_case.status, behavior_case.stage

        metadata = self._ensure_liquidation_case_metadata(behavior_case)
        execution_hits = int(metadata.get("execution_hits") or 0)
        risk_hits = int(metadata.get("risk_hits") or 0)
        max_score = float(metadata.get("max_liquidation_score") or 0.0)
        pools_seen = list(metadata.get("pools_seen") or [])

        if execution_hits >= 1 or bool(metadata.get("execution_confirmed")):
            metadata["execution_confirmed"] = True
            return "confirmed", "execution_confirmed"
        if risk_hits >= 2 or len(pools_seen) >= 2 or max_score >= max(LIQUIDATION_RISK_MIN_SCORE + 0.08, 0.74):
            return "developing", "risk_escalating"
        return "open", "risk_opened"

    def _update_smart_money_case_metadata(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict,
    ) -> None:
        metadata = self._ensure_smart_money_case_metadata(behavior_case)
        actor = str(event.address or "").lower()
        cohort_addresses = list(metadata.get("cohort_addresses") or [])
        if actor and actor not in cohort_addresses:
            cohort_addresses.append(actor)
        metadata["cohort_addresses"] = cohort_addresses[-12:]
        actor_counts = dict(metadata.get("actor_execution_counts") or {})

        if event.kind == "swap" or str(event.intent_type or "") == "swap_execution":
            metadata["execution_count"] = int(metadata.get("execution_count") or 0) + 1
            if actor:
                actor_counts[actor] = int(actor_counts.get(actor) or 0) + 1
        metadata["actor_execution_counts"] = actor_counts

        usd_value = round(float(event.usd_value or 0.0), 2)
        metadata["latest_execution_usd"] = usd_value
        if usd_value >= float(metadata.get("max_execution_usd") or 0.0):
            metadata["max_execution_usd"] = usd_value
            metadata["largest_execution_tx"] = str(event.tx_hash or "")
        metadata.setdefault("first_execution_usd", usd_value)
        metadata["latest_symbol"] = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        metadata["watch_strategy_role"] = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        metadata["last_actor"] = actor
        max_actor_count = max((int(count or 0) for count in actor_counts.values()), default=0)
        metadata["same_actor_continuation"] = max_actor_count >= 2
        first_execution_usd = float(metadata.get("first_execution_usd") or usd_value or 0.0)
        if first_execution_usd > 0:
            metadata["size_expansion_ratio"] = round(max(float(metadata.get("max_execution_usd") or 0.0), usd_value) / first_execution_usd, 3)
        else:
            metadata["size_expansion_ratio"] = 1.0

    def _update_liquidation_case_metadata(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict,
    ) -> None:
        metadata = self._ensure_liquidation_case_metadata(behavior_case)
        stage = str(event.metadata.get("liquidation_stage") or "none")
        score = round(float(event.metadata.get("liquidation_score") or 0.0), 3)
        pool = str(event.address or "").lower()
        protocols = list(event.metadata.get("liquidation_protocols") or [])
        roles = list(event.metadata.get("liquidation_roles") or [])

        if stage == "risk":
            metadata["risk_hits"] = int(metadata.get("risk_hits") or 0) + 1
        elif stage == "execution":
            metadata["execution_hits"] = int(metadata.get("execution_hits") or 0) + 1
            metadata["execution_confirmed"] = True

        if pool:
            pools_seen = list(metadata.get("pools_seen") or [])
            if pool not in pools_seen:
                pools_seen.append(pool)
            metadata["pools_seen"] = pools_seen[-8:]

        protocols_seen = list(metadata.get("protocols_seen") or [])
        for protocol in protocols:
            if protocol not in protocols_seen:
                protocols_seen.append(protocol)
        metadata["protocols_seen"] = protocols_seen

        role_seen = list(metadata.get("protocol_roles_seen") or [])
        for role in roles:
            if role not in role_seen:
                role_seen.append(role)
        metadata["protocol_roles_seen"] = role_seen

        metadata["watch_strategy_role"] = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        metadata["latest_liquidation_score"] = score
        metadata["max_liquidation_score"] = round(max(float(metadata.get("max_liquidation_score") or 0.0), score), 3)
        metadata["liquidation_side"] = str(event.metadata.get("liquidation_side") or metadata.get("liquidation_side") or "unknown")
        metadata["current_event_stage"] = stage

    def _case_summary(self, behavior_case: BehaviorCase, event: Event, watch_meta: dict) -> str:
        if self._is_downstream_followup_case(behavior_case):
            metadata = self._ensure_downstream_case_metadata(behavior_case)
            anchor_label = str(metadata.get("anchor_label") or watch_meta.get("label") or "重点地址")
            downstream_label = str(metadata.get("downstream_label") or self._downstream_label(metadata.get("downstream_address"), None))
            anchor_symbol = str(metadata.get("anchor_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
            if bool(metadata.get("downstream_execution_confirmed")):
                return f"{anchor_label} 下游大额转移后已升级为执行观察（{downstream_label}）"
            if bool(metadata.get("downstream_exchange_arrival")):
                return f"{anchor_label} 下游大额转移后已进入交易场景（{downstream_label}）"
            if bool(metadata.get("downstream_distribution_detected")):
                return f"{anchor_label} 下游大额转移后出现大额分发（{downstream_label}）"
            return f"{anchor_label} -> {downstream_label} 的 {anchor_symbol} 下游观察窗口"
        if self._is_exchange_followup_case(behavior_case):
            label = str(watch_meta.get("label") or event.address or "重点地址")
            metadata = self._ensure_exchange_followup_metadata(behavior_case)
            anchor_symbol = str(metadata.get("anchor_symbol") or "稳定币")
            followup_assets = list(metadata.get("followup_tokens_seen") or [])
            if followup_assets:
                assets_text = "/".join(followup_assets[:3])
                return f"{label} 交易场景结构延续观察案例（{anchor_symbol} -> {assets_text}）"
            return f"{label} 交易场景观察锚点案例（等待同地址后续资产流）"

        if self._is_smart_money_case(behavior_case):
            metadata = self._ensure_smart_money_case_metadata(behavior_case)
            token = str(metadata.get("latest_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
            execution_count = int(metadata.get("execution_count") or 0)
            unique_addresses = len(list(metadata.get("cohort_addresses") or []))
            direction_text = "买入" if behavior_case.direction_bucket == "buy" else "卖出"
            if bool(metadata.get("continuation_confirmed")):
                return f"{token} 聪明钱连续同向执行确认案例（{direction_text}，{execution_count} 次，{unique_addresses} 地址）"
            if execution_count >= 2:
                return f"{token} 聪明钱连续执行跟踪案例（{direction_text}，{execution_count} 次）"
            return f"{token} 聪明钱首次真实执行案例（{direction_text}）"

        if self._is_liquidation_case(behavior_case):
            metadata = self._ensure_liquidation_case_metadata(behavior_case)
            token = str(metadata.get("focus_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
            direction_text = "long flush" if metadata.get("liquidation_side") == "long_flush" else "short squeeze"
            protocols = list(metadata.get("protocols_seen") or [])
            protocol_text = "/".join(protocols[:2]) if protocols else "protocol"
            if bool(metadata.get("execution_confirmed")):
                return f"{token} 疑似清算执行案例（{direction_text}，{protocol_text}）"
            if int(metadata.get("risk_hits") or 0) >= 2 or len(list(metadata.get("pools_seen") or [])) >= 2:
                return f"{token} liquidation 风险升级案例（{direction_text}，{protocol_text}）"
            return f"{token} liquidation 风险开启案例（{direction_text}）"

        label = str(watch_meta.get("label") or event.address or "重点地址")
        token = str(event.metadata.get("token_symbol") or event.token or "资产")
        intent = str(event.intent_type or "unknown_intent")
        return f"{label} 的 {token} {intent} 跟踪案例"

    def _followup_steps(self, behavior_case: BehaviorCase, event: Event) -> list[str]:
        if self._is_downstream_followup_case(behavior_case):
            metadata = self._ensure_downstream_case_metadata(behavior_case)
            if bool(metadata.get("downstream_execution_confirmed")):
                return [
                    "观察下游地址是否继续沿协议路径完成第二次大额执行",
                    "观察执行后是否快速进入交易所或回流锚点地址",
                    "观察窗口结束前是否出现更多同方向高价值动作",
                ]
            if bool(metadata.get("downstream_exchange_arrival")):
                return [
                    "观察交易所到达后是否继续出现跨 token 执行或回流",
                    "观察是否很快出现反向大额流出，削弱交易场景解释",
                    "观察窗口内是否再有第二次高价值动作",
                ]
            if bool(metadata.get("downstream_distribution_detected")):
                return [
                    "观察是否继续向更多地址拆分，形成明确分发结构",
                    "观察是否有一部分资金回流锚点或同类地址",
                    "观察是否最终转入交易所或协议执行路径",
                ]
            return [
                "观察 30 分钟窗口内是否再出现高价值转出",
                "观察是否进入交易所、协议路由或真实 swap 执行",
                "观察是否只剩零散小额测试转账，若是则自然过期",
            ]
        if self._is_exchange_followup_case(behavior_case):
            metadata = self._ensure_exchange_followup_metadata(behavior_case)
            if bool(metadata.get("followup_confirmed")):
                return [
                    "观察同一交易所地址是否继续出现更多主流资产流入",
                    "观察是否很快出现反向流出，削弱连续确认",
                    "观察该连续流是否最终转成更明确交易执行",
                ]
            return [
                "观察 5-15 分钟内同一交易所地址是否再出现 WETH/WBTC/ETH-equivalent 流入",
                "观察是否形成稳定币先行、主流资产跟随的连续路径",
                "观察是否出现反向流出或内部划转，削弱该锚点解释",
            ]

        if self._is_smart_money_case(behavior_case):
            metadata = self._ensure_smart_money_case_metadata(behavior_case)
            if bool(metadata.get("continuation_confirmed")):
                return [
                    "观察 15-30 分钟内是否继续出现更多同向真实执行",
                    "观察是否有新的聪明钱地址加入同向执行",
                    "观察该执行是否继续与 token 资金流方向共振",
                ]
            if int(metadata.get("execution_count") or 0) >= 2:
                return [
                    "观察 15-30 分钟内是否出现第三次同向真实执行",
                    "观察是否从单地址连续执行扩展为多地址同向",
                    "观察单笔执行金额是否继续放大",
                ]
            return [
                "观察 15-30 分钟内是否再次出现同向真实执行",
                "观察是否出现第二个聪明钱地址同向跟进",
                "观察该执行是否继续带来短时共振",
            ]

        if self._is_liquidation_case(behavior_case):
            metadata = self._ensure_liquidation_case_metadata(behavior_case)
            if bool(metadata.get("execution_confirmed")):
                return [
                    "观察同协议地址是否继续处置更多仓位",
                    "观察同方向压力是否扩展到更多 ETH 主流池",
                    "观察是否出现快速反向吸收，削弱执行延续性",
                ]
            if int(metadata.get("risk_hits") or 0) >= 2 or len(list(metadata.get("pools_seen") or [])) >= 2:
                return [
                    "观察风险簇是否从多池扩展到执行确认",
                    "观察 protocol/vault/auction 证据是否继续增强",
                    "观察连续卖压/买压是否继续加速",
                ]
            return [
                "观察 5-15 分钟内是否继续出现同方向 liquidation 风险",
                "观察是否新增协议地址或 keeper/vault 证据",
                "观察是否从单池风险升级到跨池 cluster",
            ]

        if behavior_case.status == "confirmed":
            return [
                "观察 30-60 分钟内是否继续出现同 token 延续动作",
                "观察是否出现第二个高质量地址跟进",
                "观察后续价格与成交量是否继续共振",
            ]
        if behavior_case.status == "developing":
            return [
                "观察 15-30 分钟内是否继续出现同方向事件",
                "观察是否从行为线索升级成真实交易执行",
                "观察是否出现新的下一跳地址",
            ]
        return [
            "观察 5-15 分钟内是否继续出现同地址动作",
            "观察是否形成连续路径或第二跳",
            "观察是否有其他地址产生共振",
        ]

    def _build_case_id(
        self,
        watch_address: str,
        token: str,
        direction_bucket: str,
        intent_family: str,
        ts: int,
    ) -> str:
        bucket = int(ts / max(self.merge_window_sec, 1))
        raw = "|".join([watch_address, token, direction_bucket, intent_family, str(bucket)])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"case_{digest}"

    def _build_exchange_followup_case_id(self, watch_address: str, ts: int) -> str:
        bucket = int(ts / max(self.exchange_followup_window_sec, 1))
        raw = "|".join([watch_address, EXCHANGE_FOLLOWUP_CASE_FAMILY, str(bucket)])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"case_{digest}"

    def _build_downstream_case_id(self, downstream_address: str, ts: int) -> str:
        bucket = int(ts / max(self.downstream_followup_window_sec, 1))
        raw = "|".join([downstream_address, DOWNSTREAM_FOLLOWUP_CASE_FAMILY, str(bucket)])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"case_{digest}"

    def _build_smart_money_case_id(self, token: str, direction_bucket: str, ts: int) -> str:
        bucket = int(ts / max(self.smart_money_case_window_sec, 1))
        raw = "|".join([token, direction_bucket, SMART_MONEY_EXECUTION_CASE_FAMILY, str(bucket)])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"case_{digest}"

    def _build_liquidation_case_id(self, token: str, direction_bucket: str, ts: int) -> str:
        bucket = int(ts / max(self.liquidation_case_window_sec, 1))
        raw = "|".join([token, direction_bucket, LIQUIDATION_CASE_FAMILY, str(bucket)])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"case_{digest}"

    def _intent_family(self, intent_type: str | None, direction_bucket: str) -> str:
        intent = str(intent_type or "unknown_intent")
        if intent == "swap_execution":
            if direction_bucket == "buy":
                return "buy_case"
            if direction_bucket == "sell":
                return "sell_case"
            return "trade_case"
        if intent in {"exchange_withdraw_candidate", "possible_buy_preparation"}:
            return "buy_case"
        if intent in {"exchange_deposit_candidate", "possible_sell_preparation"}:
            return "sell_case"
        if intent == "pool_buy_pressure":
            return "buy_case"
        if intent == "pool_sell_pressure":
            return "sell_case"
        if intent in {"liquidity_addition", "liquidity_removal", "pool_rebalance"}:
            return "liquidity_case"
        if intent == "pool_noise":
            return "pool_noise"
        if intent in {"internal_rebalance", "market_making_inventory_move"}:
            return "inventory_case"
        if intent in {"pure_transfer", "unknown_intent"}:
            return f"{direction_bucket}_transfer" if direction_bucket in {"buy", "sell"} else "transfer_case"
        return intent

    def _intent_compatible(
        self,
        current_intent_family: str,
        target_intent_family: str,
        event_intent_type: str,
        direction_bucket: str,
    ) -> bool:
        if current_intent_family == target_intent_family:
            return True
        upgrade_pairs = {
            ("buy_case", "buy_transfer"),
            ("buy_transfer", "buy_case"),
            ("sell_case", "sell_transfer"),
            ("sell_transfer", "sell_case"),
        }
        if (current_intent_family, target_intent_family) in upgrade_pairs:
            return True
        if event_intent_type == "swap_execution" and direction_bucket == "buy" and current_intent_family == "buy_case":
            return True
        if event_intent_type == "swap_execution" and direction_bucket == "sell" and current_intent_family == "sell_case":
            return True
        return False

    def _direction_bucket(self, side: str | None) -> str:
        normalized = str(side or "")
        if normalized in {"买入", "流入"}:
            return "buy"
        if normalized in {"卖出", "流出"}:
            return "sell"
        return "other"

    def _ensure_exchange_followup_metadata(self, behavior_case: BehaviorCase) -> dict:
        metadata = behavior_case.metadata
        metadata.setdefault("case_family", EXCHANGE_FOLLOWUP_CASE_FAMILY)
        metadata.setdefault("anchor_token", behavior_case.token)
        metadata.setdefault("anchor_symbol", "")
        metadata.setdefault("anchor_usd", 0.0)
        metadata.setdefault("expected_assets", sorted(MAJOR_FOLLOWUP_ASSET_SYMBOLS))
        metadata.setdefault("followup_tokens_seen", [])
        metadata.setdefault("followup_confirmations", [])
        metadata.setdefault("deadline_ts", int(behavior_case.opened_at or 0) + self.exchange_followup_window_sec)
        metadata.setdefault("watch_strategy_role", "")
        metadata.setdefault("anchor_notified", False)
        metadata.setdefault("followup_notified", False)
        metadata.setdefault("anchor_signal_id", "")
        metadata.setdefault("followup_signal_id", "")
        metadata.setdefault("emitted_notification_stages", [])
        metadata.setdefault("followup_confirmed", False)
        metadata.setdefault("current_event_is_anchor", False)
        metadata.setdefault("current_event_is_followup_confirmation", False)
        return metadata

    def _ensure_downstream_case_metadata(self, behavior_case: BehaviorCase) -> dict:
        metadata = behavior_case.metadata
        metadata.setdefault("case_family", DOWNSTREAM_FOLLOWUP_CASE_FAMILY)
        metadata.setdefault("anchor_watch_address", "")
        metadata.setdefault("anchor_label", "")
        metadata.setdefault("anchor_strategy_role", "unknown")
        metadata.setdefault("anchor_priority", 1)
        metadata.setdefault("downstream_address", behavior_case.watch_address)
        metadata.setdefault("downstream_label", self._downstream_label(behavior_case.watch_address, None))
        metadata.setdefault("root_tx_hash", behavior_case.root_tx_hash)
        metadata.setdefault("anchor_token", behavior_case.token)
        metadata.setdefault("anchor_symbol", "")
        metadata.setdefault("anchor_usd_value", 0.0)
        metadata.setdefault("anchor_intent", "")
        metadata.setdefault("opened_at", int(behavior_case.opened_at or 0))
        metadata.setdefault("active_until", int(behavior_case.opened_at or 0) + self.downstream_followup_window_sec)
        metadata.setdefault("cooling_until", int(behavior_case.opened_at or 0) + self.downstream_followup_cooling_sec)
        metadata.setdefault("closing_until", int(behavior_case.opened_at or 0) + self.downstream_followup_closing_sec)
        metadata.setdefault("expire_at", metadata.get("active_until"))
        metadata.setdefault("deadline_ts", metadata.get("active_until"))
        metadata.setdefault("window_sec", int(self.downstream_followup_window_sec))
        metadata.setdefault("max_notifications", int(self.downstream_followup_max_notifications))
        metadata.setdefault("hop", 1)
        metadata.setdefault("reason", "anchor_large_transfer")
        metadata.setdefault("strategy_hint", "runtime_adjacent_watch")
        metadata.setdefault("followup_confirmed", False)
        metadata.setdefault("downstream_execution_confirmed", False)
        metadata.setdefault("downstream_exchange_arrival", False)
        metadata.setdefault("downstream_distribution_detected", False)
        metadata.setdefault("downstream_return_confirmed", False)
        metadata.setdefault("emitted_notification_stages", [])
        metadata.setdefault("followup_events", [])
        metadata.setdefault("destinations_seen", [])
        metadata.setdefault("current_event_is_anchor", False)
        metadata.setdefault("current_event_is_followup", False)
        metadata.setdefault("current_followup_type", "")
        metadata.setdefault("current_followup_reason", "")
        metadata.setdefault("current_stage", behavior_case.stage or "")
        metadata.setdefault("last_followup_type", "")
        metadata.setdefault("last_followup_stage", "")
        metadata.setdefault("last_followup_tx_hash", "")
        metadata.setdefault("last_followup_usd", 0.0)
        metadata.setdefault("last_counterparty", "")
        metadata.setdefault("last_counterparty_label", "")
        metadata.setdefault("last_notification_signal_id", "")
        metadata.setdefault("last_notification_stage", "")
        metadata.setdefault("emitted_notification_count", len(list(metadata.get("emitted_notification_stages") or [])))
        metadata.setdefault("lifecycle_reason", "")
        return metadata

    def _ensure_smart_money_case_metadata(self, behavior_case: BehaviorCase) -> dict:
        metadata = behavior_case.metadata
        metadata.setdefault("case_family", SMART_MONEY_EXECUTION_CASE_FAMILY)
        metadata.setdefault("execution_count", 0)
        metadata.setdefault("cohort_addresses", [])
        metadata.setdefault("actor_execution_counts", {})
        metadata.setdefault("first_execution_usd", 0.0)
        metadata.setdefault("latest_execution_usd", 0.0)
        metadata.setdefault("max_execution_usd", 0.0)
        metadata.setdefault("largest_execution_tx", "")
        metadata.setdefault("latest_symbol", "")
        metadata.setdefault("watch_strategy_role", "")
        metadata.setdefault("continuation_confirmed", False)
        metadata.setdefault("emitted_notification_stages", [])
        metadata.setdefault("execution_opened_notified", False)
        metadata.setdefault("execution_opened_signal_id", "")
        metadata.setdefault("continuation_confirmed_notified", False)
        metadata.setdefault("continuation_confirmed_signal_id", "")
        metadata.setdefault("current_event_is_execution", False)
        metadata.setdefault("last_actor", "")
        metadata.setdefault("same_actor_continuation", False)
        metadata.setdefault("size_expansion_ratio", 1.0)
        return metadata

    def _ensure_liquidation_case_metadata(self, behavior_case: BehaviorCase) -> dict:
        metadata = behavior_case.metadata
        metadata.setdefault("case_family", LIQUIDATION_CASE_FAMILY)
        metadata.setdefault("focus_symbol", "")
        metadata.setdefault("watch_strategy_role", "")
        metadata.setdefault("protocols_seen", [])
        metadata.setdefault("protocol_roles_seen", [])
        metadata.setdefault("pools_seen", [])
        metadata.setdefault("risk_hits", 0)
        metadata.setdefault("execution_hits", 0)
        metadata.setdefault("max_liquidation_score", 0.0)
        metadata.setdefault("latest_liquidation_score", 0.0)
        metadata.setdefault("liquidation_side", "unknown")
        metadata.setdefault("execution_confirmed", False)
        metadata.setdefault("current_event_stage", "none")
        metadata.setdefault("emitted_notification_stages", [])
        metadata.setdefault("risk_opened_notified", False)
        metadata.setdefault("risk_opened_signal_id", "")
        metadata.setdefault("risk_escalating_notified", False)
        metadata.setdefault("risk_escalating_signal_id", "")
        metadata.setdefault("execution_confirmed_notified", False)
        metadata.setdefault("execution_confirmed_signal_id", "")
        return metadata

    def _is_exchange_followup_case(self, behavior_case: BehaviorCase | None) -> bool:
        if behavior_case is None:
            return False
        case_family = str((behavior_case.metadata or {}).get("case_family") or "")
        return case_family == EXCHANGE_FOLLOWUP_CASE_FAMILY or behavior_case.intent_type == EXCHANGE_FOLLOWUP_CASE_FAMILY

    def _is_downstream_followup_case(self, behavior_case: BehaviorCase | None) -> bool:
        if behavior_case is None:
            return False
        case_family = str((behavior_case.metadata or {}).get("case_family") or "")
        return case_family == DOWNSTREAM_FOLLOWUP_CASE_FAMILY or behavior_case.intent_type == DOWNSTREAM_FOLLOWUP_CASE_FAMILY

    def _is_smart_money_case(self, behavior_case: BehaviorCase | None) -> bool:
        if behavior_case is None:
            return False
        case_family = str((behavior_case.metadata or {}).get("case_family") or "")
        return case_family == SMART_MONEY_EXECUTION_CASE_FAMILY or behavior_case.intent_type == SMART_MONEY_EXECUTION_CASE_FAMILY

    def _is_liquidation_case(self, behavior_case: BehaviorCase | None) -> bool:
        if behavior_case is None:
            return False
        case_family = str((behavior_case.metadata or {}).get("case_family") or "")
        return case_family == LIQUIDATION_CASE_FAMILY or behavior_case.intent_type == LIQUIDATION_CASE_FAMILY

    def _exchange_followup_min_usd(self, watch_meta: dict | None) -> float:
        try:
            return float(get_threshold(watch_meta or {}))
        except Exception:
            return 1000.0

    def is_downstream_followup_anchor(self, event: Event, watch_meta: dict | None = None) -> bool:
        watch_meta = watch_meta or {}
        if not self.downstream_followup_enabled:
            return False
        if event.kind != "token_transfer" or str(event.side or "") != "流出":
            return False
        if self.downstream_followup_max_hops < 1:
            return False

        strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        if self.downstream_followup_allowed_roles and strategy_role not in self.downstream_followup_allowed_roles:
            return False

        raw = event.metadata.get("raw") or {}
        downstream_address = self._downstream_counterparty_address(event)
        if not downstream_address:
            return False
        counterparty_meta = get_address_meta(downstream_address)
        has_explicit_label = str(counterparty_meta.get("label") or "").strip().lower() not in {
            "",
            str(counterparty_meta.get("address") or "").strip().lower(),
        }
        if has_explicit_label:
            return False
        if str(counterparty_meta.get("role") or "unknown") not in {"", "unknown"}:
            return False
        if bool(raw.get("possible_internal_transfer")):
            return False
        if self._excluded_downstream_counterparty(counterparty_meta):
            return False

        usd_value = float(event.usd_value or 0.0)
        return usd_value >= self._downstream_anchor_min_usd(event)

    def _classify_downstream_followup_event(
        self,
        event: Event,
        behavior_case: BehaviorCase | None,
    ) -> dict:
        if behavior_case is None or not self._is_downstream_followup_case(behavior_case):
            return {"matched": False}
        metadata = self._ensure_downstream_case_metadata(behavior_case)
        reference_ts = int(event.ts or time.time())
        if int(metadata.get("closing_until") or metadata.get("expire_at") or 0) and reference_ts > int(metadata.get("closing_until") or metadata.get("expire_at") or 0):
            return {"matched": False, "reason": "downstream_window_expired"}
        if str(event.address or "").lower() != str(metadata.get("downstream_address") or "").lower():
            return {"matched": False, "reason": "downstream_followup_not_confirmed"}
        if event.kind not in {"token_transfer", "swap"}:
            return {"matched": False, "reason": "downstream_followup_not_confirmed"}

        usd_value = float(event.usd_value or 0.0)
        min_followup_usd = max(
            self.downstream_followup_min_usd,
            float(metadata.get("anchor_usd_value") or 0.0) * max(self.downstream_followup_min_ratio, 0.0),
        )
        if usd_value < min_followup_usd:
            followup_type = "observed_small_noise" if usd_value > 0 else ""
            return {
                "matched": False,
                "reason": "amount_below_downstream_followup_min",
                "followup_type": followup_type,
                "min_followup_usd": round(min_followup_usd, 2),
            }

        raw = event.metadata.get("raw") or {}
        counterparty = str(raw.get("counterparty") or raw.get("to") or "").lower()
        counterparty_meta = get_address_meta(counterparty)
        counterparty_label = self._downstream_label(counterparty, counterparty_meta)

        if event.kind == "swap" or str(event.intent_type or "") == "swap_execution":
            return {
                "matched": True,
                "followup_type": "swap_execution",
                "stage": "swap_execution_confirmed",
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "reason": "downstream_target_is_protocol",
            }
        if self._is_exchange_meta(counterparty_meta):
            return {
                "matched": True,
                "followup_type": "exchange_arrival",
                "stage": "exchange_arrival_confirmed",
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "reason": "downstream_target_is_exchange",
            }
        if self._is_protocol_or_router(counterparty_meta):
            protocol_reason = "downstream_target_is_router" if str(counterparty_meta.get("semantic_role") or "") == "router_contract" or str(counterparty_meta.get("suspected_role") or "") == "router_adjacent" or str(counterparty_meta.get("strategy_role") or "") == "aggregator_router" else "downstream_target_is_protocol"
            return {
                "matched": True,
                "followup_type": "protocol_path",
                "stage": "swap_execution_confirmed",
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "reason": protocol_reason,
            }
        if counterparty and counterparty == str(metadata.get("anchor_watch_address") or "").lower():
            return {
                "matched": True,
                "followup_type": "return_flow",
                "stage": "downstream_seen",
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "reason": "downstream_return_flow",
            }

        existing_destinations = set(metadata.get("destinations_seen") or [])
        if counterparty and counterparty not in existing_destinations and len(existing_destinations) >= 1:
            return {
                "matched": True,
                "followup_type": "distribution",
                "stage": "distribution_confirmed",
                "counterparty": counterparty,
                "counterparty_label": counterparty_label,
                "reason": "downstream_distribution_confirmed",
            }

        return {
            "matched": True,
            "followup_type": "downstream_seen",
            "stage": "downstream_seen",
            "counterparty": counterparty,
            "counterparty_label": counterparty_label,
            "reason": "downstream_followup_confirmed",
        }

    def _update_downstream_case_metadata(
        self,
        behavior_case: BehaviorCase,
        event: Event,
        watch_meta: dict,
    ) -> None:
        metadata = self._ensure_downstream_case_metadata(behavior_case)
        metadata["current_event_is_anchor"] = False
        metadata["current_event_is_followup"] = False
        metadata["current_stage"] = behavior_case.stage
        metadata["expire_at"] = int(metadata.get("active_until") or metadata.get("expire_at") or 0)
        metadata["deadline_ts"] = int(metadata.get("active_until") or metadata.get("deadline_ts") or 0)
        metadata["anchor_label"] = str(metadata.get("anchor_label") or watch_meta.get("anchor_label") or metadata.get("anchor_label") or "")
        metadata["downstream_label"] = str(metadata.get("downstream_label") or self._downstream_label(metadata.get("downstream_address"), None))
        metadata["last_seen_ts"] = int(event.ts or time.time())

    def _downstream_case_address(self, event: Event) -> str:
        event_address = str(event.address or "").lower()
        if event_address and not self.is_downstream_followup_anchor(event, watch_meta={"strategy_role": event.strategy_role}):
            return event_address
        return self._downstream_counterparty_address(event)

    def _downstream_counterparty_address(self, event: Event) -> str:
        raw = event.metadata.get("raw") or {}
        counterparty = str(raw.get("counterparty") or "").lower()
        if counterparty:
            return counterparty
        if str(event.side or "") == "流出":
            return str(raw.get("to") or "").lower()
        if str(event.side or "") == "流入":
            return str(raw.get("from") or "").lower()
        return ""

    def _downstream_anchor_min_usd(self, event: Event) -> float:
        symbol = self._display_symbol(event.token, event.metadata.get("token_symbol"))
        if self._is_stable_asset(event.token, symbol) or symbol in DOWNSTREAM_STABLE_SYMBOLS:
            return float(ADJACENT_WATCH_STABLE_SUPER_LARGE_USD)
        if self._is_major_followup_asset(event.token, symbol) or symbol in DOWNSTREAM_MAJOR_SYMBOLS:
            return float(ADJACENT_WATCH_MAJOR_ASSET_SUPER_LARGE_USD)
        return float(ADJACENT_WATCH_OTHER_ASSET_SUPER_LARGE_USD or ADJACENT_WATCH_SUPER_LARGE_USD)

    def _excluded_downstream_counterparty(self, counterparty_meta: dict) -> bool:
        strategy_role = str(counterparty_meta.get("strategy_role") or "unknown")
        semantic_role = str(counterparty_meta.get("semantic_role") or "unknown")
        if ADJACENT_WATCH_EXCLUDE_ROUTER and (
            strategy_role == "aggregator_router" or semantic_role == "router_contract" or str(counterparty_meta.get("suspected_role") or "") == "router_adjacent"
        ):
            return True
        if ADJACENT_WATCH_EXCLUDE_PROTOCOL and (
            strategy_role == "protocol_treasury" or semantic_role == "protocol_wallet" or str(counterparty_meta.get("suspected_role") or "") == "protocol_adjacent"
        ):
            return True
        if ADJACENT_WATCH_EXCLUDE_EXCHANGE and self._is_exchange_meta(counterparty_meta):
            return True
        if ADJACENT_WATCH_EXCLUDE_LP_POOL and strategy_role == "lp_pool":
            return True
        return False

    def _is_exchange_meta(self, meta: dict) -> bool:
        return str(meta.get("strategy_role") or "").startswith("exchange_") or str(meta.get("role") or "") == "exchange"

    def _is_protocol_or_router(self, meta: dict) -> bool:
        strategy_role = str(meta.get("strategy_role") or "")
        semantic_role = str(meta.get("semantic_role") or "")
        suspected_role = str(meta.get("suspected_role") or "")
        return strategy_role in {"aggregator_router", "protocol_treasury"} or semantic_role in {"router_contract", "protocol_wallet"} or suspected_role in {"router_adjacent", "protocol_adjacent"}

    def _downstream_label(self, address: str | None, meta: dict | None) -> str:
        addr = str(address or "").lower()
        meta = meta or get_address_meta(addr)
        hint = str(meta.get("display_hint_label") or "").strip()
        if hint:
            return f"{hint} ({shorten_address(addr)})"
        return meta.get("display") or shorten_address(addr)

    def _is_stable_asset(self, token_contract: str | None, token_symbol: str | None) -> bool:
        contract = str(token_contract or "").lower()
        symbol = str(token_symbol or "").upper()
        return contract in STABLE_TOKEN_CONTRACTS or symbol in STABLE_TOKEN_SYMBOLS

    def _is_major_followup_asset(self, token_contract: str | None, token_symbol: str | None) -> bool:
        contract = str(token_contract or "").lower()
        symbol = str(token_symbol or "").upper()
        return contract in MAJOR_FOLLOWUP_ASSET_CONTRACTS or symbol in MAJOR_FOLLOWUP_ASSET_SYMBOLS

    def _display_symbol(self, token_contract: str | None, token_symbol: str | None) -> str:
        symbol = str(token_symbol or "").upper().strip()
        if symbol:
            return symbol
        contract = str(token_contract or "").lower()
        stable_meta = STABLE_TOKEN_METADATA.get(contract)
        if stable_meta:
            return str(stable_meta.get("symbol") or contract).upper()
        if contract == WETH_TOKEN_CONTRACT:
            return "WETH"
        if contract == WBTC_TOKEN_CONTRACT:
            return "WBTC"
        if contract in MAJOR_FOLLOWUP_ASSET_CONTRACTS:
            return "ETH"
        return contract or "UNKNOWN"
