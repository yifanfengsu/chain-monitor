import time
import hashlib

from analyzer import BehaviorAnalyzer
from config import (
    DELIVERY_ALLOW_EXCHANGE_OBSERVE,
    DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE,
    DELIVERY_ALLOW_LP_OBSERVE,
    DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE,
)
from constants import STABLE_TOKEN_CONTRACTS
from filter import (
    get_address_meta,
    get_flow_endpoints,
    get_primary_watch_meta,
    get_threshold,
    get_watch_context,
    is_smart_money_strategy_role,
    strategy_role_group,
)
from lp_analyzer import LP_ALL_INTENTS, LPAnalyzer
from liquidation_detector import LiquidationDetector
from models import Event
from price_service import PriceService
from processor import parse_tx
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from signal_interpreter import SignalInterpreter
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class SignalPipeline:
    """
    主处理管道：
    Data -> Parsing -> Pricing -> Intent -> State -> Intelligence -> Strategy -> Output

    本阶段增强：
    - 先给出初步意图
    - 再基于短时序列与 token 共振做确认/降级
    - 把确认结构贯穿到 gate / strategy / interpreter
    """

    def __init__(
        self,
        price_service: PriceService,
        state_manager: StateManager,
        behavior_analyzer: BehaviorAnalyzer,
        address_scorer: AddressScorer,
        token_scorer: TokenScorer,
        strategy_engine: StrategyEngine,
        quality_gate: SignalQualityGate | None = None,
        signal_interpreter: SignalInterpreter | None = None,
        lp_analyzer: LPAnalyzer | None = None,
        liquidation_detector: LiquidationDetector | None = None,
        address_intelligence=None,
        archive_store=None,
        followup_tracker=None,
    ) -> None:
        self.price_service = price_service
        self.state_manager = state_manager
        self.behavior_analyzer = behavior_analyzer
        self.address_scorer = address_scorer
        self.token_scorer = token_scorer
        self.quality_gate = quality_gate or SignalQualityGate(state_manager=state_manager)
        self.strategy_engine = strategy_engine
        self.signal_interpreter = signal_interpreter or SignalInterpreter()
        self.lp_analyzer = lp_analyzer or LPAnalyzer()
        self.liquidation_detector = liquidation_detector or LiquidationDetector()
        self.address_intelligence = address_intelligence
        self.archive_store = archive_store
        self.followup_tracker = followup_tracker

    async def process(self, raw_item: dict):
        archive_status = {
            "raw_event": False,
            "parsed_event": False,
            "signal": False,
            "delivery_audit": False,
            "case_update": False,
            "case_followup": False,
        }
        archive_ts = int(time.time())
        self._archive_raw_event(raw_item, archive_status, archive_ts)

        parsed = parse_tx(raw_item)
        if not parsed:
            self._archive_parse_failure(raw_item, archive_status, archive_ts)
            return None

        watch_context = get_watch_context(parsed)
        if not watch_context:
            return None

        parsed = self._normalize_parsed(parsed, watch_context, raw_item=raw_item)
        watch_meta = (
            get_primary_watch_meta(parsed, watch_context=watch_context)
            or watch_context.get("watch_meta")
            or get_address_meta(parsed.get("watch_address"))
        )

        pricing = await self._evaluate_pricing(parsed)
        event = self._to_event(parsed, watch_context=watch_context, watch_meta=watch_meta, pricing=pricing)
        event.archive_ts = archive_ts
        event.event_id = self._build_event_id(event)
        parsed["event_id"] = event.event_id
        parsed["archive_ts"] = archive_ts

        preliminary_intent = self._classify_intent(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
        )
        event.intent_type = preliminary_intent["intent_type"]
        event.intent_confidence = float(preliminary_intent["intent_confidence"])
        event.intent_stage = "preliminary"
        event.confirmation_score = float(preliminary_intent.get("confirmation_score") or 0.0)
        event.intent_evidence = list(preliminary_intent.get("intent_evidence") or [])
        event.metadata["watch_meta"] = {
            "role": watch_meta.get("role", "unknown"),
            "strategy_role": watch_meta.get("strategy_role", "unknown"),
            "semantic_role": watch_meta.get("semantic_role", "unknown"),
            "priority": int(watch_meta.get("priority", 3) or 3),
            "label": watch_meta.get("label", ""),
            "category": watch_meta.get("category", "unknown"),
            "intelligence_status": watch_meta.get("intelligence_status", ""),
            "suspected_role": watch_meta.get("suspected_role", ""),
            "candidate_score": float(watch_meta.get("candidate_score") or 0.0),
        }
        event.metadata["intent"] = preliminary_intent

        self.state_manager.apply_event(event)
        self._record_counterparty(event, watch_context, parsed)

        address_snapshot = self.state_manager.get_address_snapshot(
            event.address,
            window_sec=self.behavior_analyzer.window_sec,
            now_ts=event.ts,
        )
        token_snapshot = self.state_manager.get_token_snapshot(
            event.token,
            window_sec=max(self.behavior_analyzer.window_sec, 86400),
            now_ts=event.ts,
        )
        pool_snapshot = self.state_manager.get_pool_snapshot(
            event.address,
            window_sec=900,
            now_ts=event.ts,
        ) if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed) else {}

        behavior = self.behavior_analyzer.detect(event, address_snapshot)
        confirmed_intent = self._confirm_intent(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
            behavior=behavior,
            preliminary_intent=preliminary_intent,
        )
        self._apply_confirmed_intent(event, confirmed_intent)
        self._apply_liquidation_overlay(
            event=event,
            parsed=parsed,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
        )

        address_intel_update = self._observe_address_intelligence(
            event=event,
            parsed=parsed,
            watch_context=watch_context,
            raw_item=raw_item,
            token_snapshot=token_snapshot,
        )

        case_result = self._match_or_open_case(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
        )
        behavior_case = case_result.get("case") if case_result else None
        for stale_case in (case_result or {}).get("stale_updates") or []:
            self.state_manager.update_case_pointer(
                case_id=stale_case.case_id,
                watch_address=stale_case.watch_address,
                token=stale_case.token,
                is_open=False,
            )
        for invalid_case in (case_result or {}).get("invalidated_cases") or []:
            self.state_manager.update_case_pointer(
                case_id=invalid_case.case_id,
                watch_address=invalid_case.watch_address,
                token=invalid_case.token,
                is_open=False,
            )
        if behavior_case is not None:
            self._bind_case_to_event(event, behavior_case, case_result)
            self.state_manager.register_case(
                case_id=behavior_case.case_id,
                watch_address=behavior_case.watch_address,
                token=behavior_case.token,
            )
            if behavior_case.status in {"cooled", "closed", "invalidated"}:
                self.state_manager.update_case_pointer(
                    case_id=behavior_case.case_id,
                    watch_address=behavior_case.watch_address,
                    token=behavior_case.token,
                    is_open=False,
                )
            event.metadata["case_summary"] = behavior_case.summary
            event.metadata["case_followup_steps"] = list(behavior_case.followup_steps or [])
            event.metadata["case"] = {
                "case_id": behavior_case.case_id,
                "status": behavior_case.status,
                "stage": behavior_case.stage,
                "summary": behavior_case.summary,
            }
            self._apply_exchange_followup_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )
            self._apply_smart_money_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )
            self._apply_liquidation_case_context(
                event=event,
                behavior_case=behavior_case,
                case_result=case_result,
                watch_meta=watch_meta,
            )

        self._archive_parsed_event(
            parsed=self._parsed_archive_payload(
                parsed=parsed,
                event=event,
                watch_meta=watch_meta,
                behavior=behavior,
                address_snapshot=address_snapshot,
                pool_snapshot=pool_snapshot,
                token_snapshot=token_snapshot,
            ),
            archive_status=archive_status,
            archive_ts=archive_ts,
        )
        self._archive_case_update(case_result, event=event, archive_status=archive_status, archive_ts=archive_ts)

        address_score = self.address_scorer.score(watch_meta, address_snapshot, behavior)
        token_score = self.token_scorer.score(event, token_snapshot)

        gate_decision = self.quality_gate.evaluate(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
            address_score=address_score,
            token_score=token_score,
            address_snapshot=address_snapshot,
            token_snapshot=token_snapshot,
        )
        if not gate_decision.passed:
            event.delivery_class = "drop"
            event.delivery_reason = gate_decision.reason
            self._archive_delivery_audit(
                event=event,
                signal=None,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="gate",
                gate_reason=gate_decision.reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        gate_decision.metrics["token_context_score"] = self._token_context_score(
            base_token_score=float(token_score.get("score") or 0.0),
            gate_metrics=gate_decision.metrics,
        )

        signal = self.strategy_engine.decide(
            event=event,
            watch_meta=watch_meta,
            behavior=behavior,
            address_score=address_score,
            token_score=token_score,
            gate_metrics=gate_decision.metrics,
        )
        if not signal:
            event.delivery_class = "drop"
            event.delivery_reason = "strategy_rejected"
            self._archive_delivery_audit(
                event=event,
                signal=None,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="strategy",
                gate_reason="strategy_rejected",
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        cooldown_key = str(
            gate_decision.metrics.get("cooldown_key")
            or self.state_manager.get_cooldown_key(event, intent_type=event.intent_type)
        )
        now_ts = int(event.ts)
        if not self.state_manager.can_emit_signal_by_key(cooldown_key, now_ts, self.quality_gate.cooldown_sec):
            last_signal_ts = self.state_manager.get_last_signal_ts_by_key(cooldown_key)
            event.delivery_class = "drop"
            event.delivery_reason = "cooldown_suppressed"
            signal.delivery_class = "drop"
            signal.delivery_reason = "cooldown_suppressed"
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="cooldown",
                gate_reason="cooldown_suppressed",
                archive_status=archive_status,
                archive_ts=archive_ts,
                audit_extras={
                    "cooldown_key": cooldown_key,
                    "now_ts": now_ts,
                    "last_signal_ts": int(last_signal_ts) if last_signal_ts is not None else None,
                    "cooldown_sec": int(self.quality_gate.cooldown_sec),
                },
            )
            return None

        signal.archive_ts = archive_ts
        signal.signal_id = self._build_signal_id(signal, event)
        signal.event_id = event.event_id
        signal.case_id = event.case_id
        signal.parent_case_id = event.parent_case_id
        signal.followup_stage = event.followup_stage
        signal.followup_status = event.followup_status
        signal.intent_type = event.intent_type
        signal.intent_stage = event.intent_stage
        signal.confirmation_score = float(event.confirmation_score or 0.0)
        signal.information_level = confirmed_intent["information_level"]
        signal.abnormal_ratio = self._abnormal_ratio(address_snapshot, event)
        signal.pricing_confidence = float(event.pricing_confidence or 0.0)
        signal.cooldown_key = cooldown_key
        signal.base_token_score = float(token_score.get("score") or 0.0)
        signal.token_context_score = float(gate_decision.metrics.get("token_context_score") or signal.base_token_score)
        signal.metadata.setdefault("pricing", pricing)
        signal.metadata["intent"] = confirmed_intent
        signal.metadata["watch_meta"] = event.metadata.get("watch_meta", {})
        signal.metadata["confirmation"] = {
            "intent_stage": event.intent_stage,
            "confirmation_score": float(event.confirmation_score or 0.0),
            "intent_evidence": list(event.intent_evidence or []),
        }
        if behavior_case is not None:
            updated_case = self._attach_signal_to_case(behavior_case, signal)
            if updated_case is not None:
                behavior_case = updated_case
                event.followup_stage = behavior_case.stage
                event.followup_status = behavior_case.status
                event.metadata["case_summary"] = behavior_case.summary
                event.metadata["case"] = {
                    "case_id": behavior_case.case_id,
                    "status": behavior_case.status,
                    "stage": behavior_case.stage,
                    "summary": behavior_case.summary,
                }
                signal.followup_stage = behavior_case.stage
                signal.followup_status = behavior_case.status
                signal.metadata["case"] = {
                    "case_id": behavior_case.case_id,
                    "status": behavior_case.status,
                    "stage": behavior_case.stage,
                    "summary": behavior_case.summary,
                }
                self._apply_exchange_followup_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )
                self._apply_smart_money_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )
                self._apply_liquidation_case_context(
                    event=event,
                    behavior_case=behavior_case,
                    case_result=case_result,
                    watch_meta=watch_meta,
                )

        self._sync_exchange_followup_to_signal(event, signal, behavior_case)
        self._sync_smart_money_case_to_signal(event, signal, behavior_case)
        self._sync_liquidation_to_signal(event, signal, behavior_case)

        interpretation = self.signal_interpreter.interpret(
            event=event,
            signal=signal,
            behavior=behavior,
            watch_meta=watch_meta,
            watch_context=watch_context,
            address_snapshot=address_snapshot,
            token_snapshot=token_snapshot,
            gate_metrics=gate_decision.metrics,
        )
        if not interpretation.should_notify:
            event.delivery_class = "drop"
            event.delivery_reason = interpretation.reason
            signal.delivery_class = "drop"
            signal.delivery_reason = interpretation.reason
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="strategy",
                gate_reason=interpretation.reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        delivery_class, delivery_reason = self.strategy_engine.classify_delivery(
            event=event,
            signal=signal,
            watch_meta=watch_meta,
            gate_metrics=gate_decision.metrics,
            behavior_case=behavior_case,
        )
        should_send = self._should_emit_delivery_notification(event, signal)
        if delivery_class == "drop" or not should_send:
            self._archive_non_primary_signal(
                event=event,
                signal=signal,
                reason=delivery_reason,
                gate_metrics=gate_decision.metrics,
                behavior=behavior,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        if not self._apply_case_notification_control(event, signal, behavior_case):
            event.delivery_class = "drop"
            event.delivery_reason = str(event.metadata.get("case_notification_reason") or "case_notification_suppressed")
            signal.delivery_class = "drop"
            signal.delivery_reason = event.delivery_reason
            self._archive_delivery_audit(
                event=event,
                signal=signal,
                behavior=behavior,
                gate_metrics=gate_decision.metrics,
                stage="notifier",
                gate_reason=event.delivery_reason,
                archive_status=archive_status,
                archive_ts=archive_ts,
            )
            return None

        if delivery_class == "primary":
            self._archive_signal(signal, event, archive_status, archive_ts)
        self._archive_delivery_audit(
            event=event,
            signal=signal,
            behavior=behavior,
            gate_metrics=gate_decision.metrics,
            stage="notifier",
            gate_reason="passed",
            archive_status=archive_status,
            archive_ts=archive_ts,
        )
        if behavior_case is not None:
            self._archive_case_followup(behavior_case, signal, archive_status, archive_ts)

        self.quality_gate.mark_emitted(event)
        return {
            "event": event,
            "signal": signal,
            "behavior": behavior,
            "intent": confirmed_intent,
            "pricing": pricing,
            "address_score": address_score,
            "token_score": token_score,
            "gate": gate_decision,
            "watch_meta": watch_meta,
            "watch_context": watch_context,
            "address_snapshot": address_snapshot,
            "token_snapshot": token_snapshot,
            "interpretation": interpretation,
            "case": behavior_case,
            "address_intel_update": address_intel_update,
            "archive_status": archive_status,
        }

    def _normalize_parsed(self, parsed: dict, watch_context: dict, raw_item: dict | None = None) -> dict:
        raw_item = raw_item or {}
        if parsed.get("watch_address"):
            parsed["watch_address"] = str(parsed["watch_address"]).lower()
        elif watch_context.get("watch_address"):
            parsed["watch_address"] = str(watch_context["watch_address"]).lower()
        elif watch_context.get("watch_from"):
            parsed["watch_address"] = str(watch_context["watch_from"]).lower()
        else:
            parsed["watch_address"] = ""

        if not parsed.get("direction") and watch_context.get("direction"):
            parsed["direction"] = watch_context["direction"]

        parsed["timestamp"] = int(raw_item.get("ingest_ts") or time.time())
        parsed["ingest_ts"] = int(raw_item.get("ingest_ts") or parsed["timestamp"])
        parsed["source_kind"] = str(raw_item.get("source_kind") or "")
        parsed["touched_watch_addresses"] = list(raw_item.get("touched_watch_addresses") or [])
        parsed["touched_lp_pools"] = list(raw_item.get("touched_lp_pools") or [])
        parsed["raw_log_count"] = int(raw_item.get("raw_log_count") or 0)
        parsed["participant_addresses"] = list(raw_item.get("participant_addresses") or [])
        parsed["next_hop_addresses"] = list(raw_item.get("next_hop_addresses") or [])
        parsed["token_addresses"] = list(raw_item.get("token_addresses") or [])
        parsed["monitor_type"] = str(raw_item.get("monitor_type") or parsed.get("monitor_type") or "watch_address")
        parsed["tx_pool_hit_count"] = int(raw_item.get("tx_pool_hit_count") or 0)
        parsed["touched_lp_pool_count"] = int(raw_item.get("touched_lp_pool_count") or 0)
        parsed["pool_transfer_count_by_pool"] = dict(raw_item.get("pool_transfer_count_by_pool") or {})
        parsed["pool_candidate_weight"] = float(raw_item.get("pool_candidate_weight") or 0.0)
        parsed["replay_source"] = str(raw_item.get("replay_source") or "")
        return parsed

    def _archive_raw_event(self, raw_item: dict, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["raw_event"] = bool(self.archive_store.write_raw_event(raw_item, archive_ts=archive_ts))
        except Exception as e:
            print(f"raw event 归档失败: {e}")

    def _archive_parse_failure(self, raw_item: dict, archive_status: dict, archive_ts: int) -> None:
        lp_debug = dict(raw_item.get("lp_parse_debug") or {})
        touched_lp_pools = list(raw_item.get("touched_lp_pools") or [])
        if not lp_debug and not touched_lp_pools:
            return

        watch_address = str(lp_debug.get("watch_address") or (touched_lp_pools[0] if touched_lp_pools else "")).lower()
        tx_hash = str(raw_item.get("tx_hash") or "")
        reason = str(lp_debug.get("reason") or "lp_parse_failed")
        event_id_seed = "|".join([tx_hash, watch_address or "lp_pool", reason])
        event_id = f"evt_{hashlib.sha1(event_id_seed.encode('utf-8')).hexdigest()[:16]}"
        record = {
            "event_id": event_id,
            "tx_hash": tx_hash,
            "watch_address": watch_address,
            "strategy_role": "lp_pool" if watch_address else "unknown",
            "role_group": "lp_pool" if watch_address else "other",
            "intent_type": "lp_parse_failed",
            "behavior_type": "unknown",
            "gate_reason": reason,
            "delivery_class": "drop",
            "stage": "strategy",
            "archive_ts": int(archive_ts),
            "lp_parse_status": str(lp_debug.get("status") or "failed"),
            "lp_parse_missing_legs": list(lp_debug.get("missing_legs") or []),
            "replay_source": str(raw_item.get("replay_source") or ""),
        }
        try:
            archive_status["delivery_audit"] = bool(
                self.archive_store.write_delivery_audit(record, archive_ts=archive_ts)
            ) or bool(archive_status.get("delivery_audit"))
        except Exception as e:
            print(f"lp parse failure audit 归档失败: {e}")

    def _archive_parsed_event(self, parsed: dict, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["parsed_event"] = bool(self.archive_store.write_parsed_event(parsed, archive_ts=archive_ts))
        except Exception as e:
            print(f"parsed event 归档失败: {e}")

    def _archive_signal(self, signal, event: Event, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["signal"] = bool(self.archive_store.write_signal(signal, event=event, archive_ts=archive_ts))
        except Exception as e:
            print(f"signal 归档失败: {e}")

    def _archive_delivery_audit(
        self,
        event: Event,
        signal,
        behavior: dict | None,
        stage: str,
        gate_reason: str,
        archive_status: dict,
        archive_ts: int,
        gate_metrics: dict | None = None,
        audit_extras: dict | None = None,
    ) -> None:
        if self.archive_store is None:
            return
        try:
            archive_status["delivery_audit"] = bool(
                self.archive_store.write_delivery_audit(
                    self._delivery_audit_record(
                        event=event,
                        signal=signal,
                        behavior=behavior,
                        gate_metrics=gate_metrics,
                        stage=stage,
                        gate_reason=gate_reason,
                        archive_ts=archive_ts,
                        audit_extras=audit_extras,
                    ),
                    archive_ts=archive_ts,
                )
            )
        except Exception as e:
            print(f"delivery audit 归档失败: {e}")

    def _archive_case_update(self, case_result: dict | None, event: Event, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None or not case_result:
            return

        wrote = False
        try:
            for stale_case in case_result.get("stale_updates") or []:
                wrote = bool(
                    self.archive_store.write_case_update(
                        stale_case,
                        event=event,
                        action="stale",
                        archive_ts=archive_ts,
                    )
                ) or wrote

            for invalid_case in case_result.get("invalidated_cases") or []:
                wrote = bool(
                    self.archive_store.write_case_update(
                        invalid_case,
                        event=event,
                        action="invalidated",
                        archive_ts=archive_ts,
                    )
                ) or wrote

            behavior_case = case_result.get("case")
            if behavior_case is not None:
                action = "opened" if bool(case_result.get("created")) else "updated"
                wrote = bool(
                    self.archive_store.write_case_update(
                        behavior_case,
                        event=event,
                        action=action,
                        archive_ts=archive_ts,
                    )
                ) or wrote
        except Exception as e:
            print(f"case update 归档失败: {e}")

        archive_status["case_update"] = bool(archive_status.get("case_update") or wrote)

    def _archive_case_followup(self, behavior_case, signal, archive_status: dict, archive_ts: int) -> None:
        if self.archive_store is None or behavior_case is None:
            return

        wrote_followup = False
        try:
            self.archive_store.write_case_update(
                behavior_case,
                signal=signal,
                action="signal_attached",
                archive_ts=archive_ts,
            )
            wrote_followup = bool(
                self.archive_store.write_case_followup(
                    behavior_case.case_id,
                    {
                        "signal_id": getattr(signal, "signal_id", ""),
                        "signal_type": getattr(signal, "type", ""),
                        "confidence": float(getattr(signal, "confidence", 0.0) or 0.0),
                        "status": behavior_case.status,
                        "stage": behavior_case.stage,
                    },
                    archive_ts=archive_ts,
                )
            )
        except Exception as e:
            print(f"case followup 归档失败: {e}")

        archive_status["case_followup"] = bool(archive_status.get("case_followup") or wrote_followup)

    def _archive_non_primary_signal(
        self,
        event: Event,
        signal,
        reason: str,
        gate_metrics: dict,
        behavior: dict | None,
        archive_status: dict,
        archive_ts: int,
    ) -> None:
        self._archive_delivery_audit(
            event=event,
            signal=signal,
            behavior=behavior,
            gate_metrics=gate_metrics,
            stage="strategy",
            gate_reason=reason,
            archive_status=archive_status,
            archive_ts=archive_ts,
        )

    def _delivery_audit_record(
        self,
        event: Event,
        signal=None,
        behavior: dict | None = None,
        gate_metrics: dict | None = None,
        stage: str = "strategy",
        gate_reason: str = "",
        archive_ts: int | None = None,
        audit_extras: dict | None = None,
    ) -> dict:
        behavior = behavior or {}
        gate_metrics = gate_metrics or dict(getattr(signal, "metadata", {}).get("gate") or {})
        signal_metadata = getattr(signal, "metadata", {}) or {}
        signal_context = getattr(signal, "context", {}) or {}
        event_metadata = event.metadata or {}
        raw = event_metadata.get("raw") or {}
        lp_context = raw.get("lp_context") or {}
        lp_analysis = event_metadata.get("lp_analysis") or {}
        watch_meta = event_metadata.get("watch_meta") or {}
        pricing = event_metadata.get("pricing") or {}
        liquidation = event_metadata.get("liquidation") or {}

        def _first_value(*values):
            for value in values:
                if value is not None:
                    return value
            return None

        def _num(*values, digits: int = 3):
            value = _first_value(*values)
            if value is None or value == "":
                return None
            try:
                return round(float(value), digits)
            except (TypeError, ValueError):
                return None

        def _int_value(*values):
            value = _first_value(*values)
            if value is None or value == "":
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return None

        def _text(*values):
            value = _first_value(*values)
            if value is None:
                return ""
            return str(value)

        def _bool_value(*values):
            value = _first_value(*values)
            if value is None:
                return False
            return bool(value)

        usd_value = _num(
            getattr(signal, "usd_value", None),
            gate_metrics.get("usd_value"),
            event.usd_value,
            digits=2,
        )
        dynamic_min_usd = _num(
            gate_metrics.get("dynamic_min_usd"),
            signal_metadata.get("dynamic_min_usd"),
            digits=2,
        )
        base_threshold_usd = _num(
            signal_metadata.get("base_threshold_usd"),
            gate_metrics.get("base_threshold_usd"),
            digits=2,
        )
        effective_threshold_usd = _num(
            getattr(signal, "effective_threshold_usd", None),
            signal_metadata.get("effective_threshold_usd"),
            gate_metrics.get("effective_threshold_usd"),
            dynamic_min_usd,
            digits=2,
        )
        below_min_usd_gap = None
        threshold_ratio = None
        if usd_value is not None and dynamic_min_usd is not None:
            below_min_usd_gap = round(max(dynamic_min_usd - usd_value, 0.0), 2)
            threshold_ratio = round((usd_value / dynamic_min_usd) if dynamic_min_usd > 0 else 0.0, 3)

        lp_event = bool(_bool_value(
            gate_metrics.get("lp_event"),
            signal_context.get("lp_event"),
            self._is_lp_event(event=event),
        ))
        counterparty = _text(
            raw.get("counterparty"),
            event_metadata.get("counterparty"),
        )
        payload = {
            "event_id": _text(event.event_id),
            "tx_hash": _text(event.tx_hash),
            "watch_address": _text(event.address),
            "strategy_role": _text(
                signal_metadata.get("strategy_role"),
                gate_metrics.get("strategy_role"),
                event.strategy_role,
            ),
            "role_group": _text(
                signal_metadata.get("role_group"),
                gate_metrics.get("role_group"),
                strategy_role_group(event.strategy_role),
            ),
            "intent_type": _text(getattr(signal, "intent_type", None), gate_metrics.get("intent_type"), event.intent_type),
            "behavior_type": _text(
                getattr(signal, "behavior_type", None),
                gate_metrics.get("behavior_type"),
                behavior.get("behavior_type"),
                "unknown",
            ),
            "gate_reason": _text(gate_reason, getattr(signal, "delivery_reason", None), event.delivery_reason),
            "delivery_class": _text(getattr(signal, "delivery_class", None), event.delivery_class, "drop"),
            "delivery_reason": _text(getattr(signal, "delivery_reason", None), event.delivery_reason),
            "stage": _text(stage or "strategy"),
            "archive_ts": _int_value(archive_ts, event.archive_ts, time.time()),
            "chain": _text(event.chain, "ethereum"),
            "event_kind": _text(event.kind),
            "side": _text(event.side),
            "token": _text(event.token),
            "counterparty": counterparty,
            "counterparty_label": _text(event_metadata.get("counterparty_label"), raw.get("counterparty_label")),
            "monitor_type": _text(raw.get("monitor_type"), event_metadata.get("monitor_type")),
            "source_kind": _text(raw.get("source_kind"), event_metadata.get("source_kind")),
            "raw_log_count": _int_value(raw.get("raw_log_count"), event_metadata.get("raw_log_count"), 0),
            "touched_watch_addresses_count": _int_value(len(raw.get("touched_watch_addresses") or event_metadata.get("touched_watch_addresses") or []), 0),
            "touched_lp_pools_count": _int_value(len(raw.get("touched_lp_pools") or event_metadata.get("touched_lp_pools") or []), 0),
            "usd_value": usd_value,
            "usd_value_available": _bool_value(event.usd_value_available, pricing.get("usd_value_available")),
            "usd_value_estimated": _bool_value(event.usd_value_estimated, pricing.get("usd_value_estimated")),
            "pricing_status": _text(getattr(signal, "metadata", {}).get("pricing_status"), gate_metrics.get("pricing_status"), event.pricing_status),
            "pricing_source": _text(gate_metrics.get("pricing_source"), event.pricing_source, pricing.get("pricing_source")),
            "pricing_confidence": _num(getattr(signal, "pricing_confidence", None), gate_metrics.get("pricing_confidence"), event.pricing_confidence),
            "dynamic_min_usd": dynamic_min_usd,
            "base_threshold_usd": base_threshold_usd,
            "effective_threshold_usd": effective_threshold_usd,
            "quality_threshold": _num(gate_metrics.get("quality_threshold")),
            "below_min_usd_gap": below_min_usd_gap,
            "threshold_ratio": threshold_ratio,
            "quality_score": _num(getattr(signal, "quality_score", None), gate_metrics.get("quality_score")),
            "adjusted_quality_score": _num(gate_metrics.get("adjusted_quality_score"), getattr(signal, "quality_score", None)),
            "quality_tier": _text(gate_metrics.get("quality_tier"), signal_metadata.get("quality_tier")),
            "address_score": _num(getattr(signal, "address_score", None), gate_metrics.get("address_score")),
            "token_score": _num(getattr(signal, "token_score", None), gate_metrics.get("token_score")),
            "token_context_score": _num(getattr(signal, "token_context_score", None), gate_metrics.get("token_context_score")),
            "behavior_confidence": _num(gate_metrics.get("behavior_confidence"), behavior.get("confidence")),
            "intent_confidence": _num(signal_metadata.get("intent_confidence"), gate_metrics.get("intent_confidence"), event.intent_confidence),
            "confirmation_score": _num(getattr(signal, "confirmation_score", None), gate_metrics.get("confirmation_score"), event.confirmation_score),
            "confirmation_evidence_count": _int_value(gate_metrics.get("confirmation_evidence_count"), len(event.intent_evidence or []), 0),
            "resonance_score": _num(gate_metrics.get("resonance_score"), signal_metadata.get("resonance_score")),
            "intent_stage": _text(getattr(signal, "intent_stage", None), gate_metrics.get("intent_stage"), event.intent_stage),
            "information_level": _text(getattr(signal, "information_level", None), signal_context.get("information_label"), event_metadata.get("intent", {}).get("information_level")),
            "semantic_role": _text(gate_metrics.get("semantic_role"), event.semantic_role, watch_meta.get("semantic_role")),
            "cooldown_key": _text(getattr(signal, "cooldown_key", None), gate_metrics.get("cooldown_key")),
            "last_signal_ts": _int_value(gate_metrics.get("last_signal_ts")),
            "cooldown_sec": _int_value(gate_metrics.get("cooldown_sec")),
            "lp_event": lp_event,
            "lp_pool_address": _text(event.address if lp_event else "", raw.get("watch_address")),
            "lp_pair_label": _text(gate_metrics.get("lp_pair_label"), lp_context.get("pair_label")),
            "lp_dex": _text(gate_metrics.get("lp_dex"), lp_context.get("dex")),
            "lp_protocol": _text(gate_metrics.get("lp_protocol"), lp_context.get("protocol")),
            "lp_action": _text(gate_metrics.get("lp_action"), lp_context.get("action")),
            "lp_direction": _text(gate_metrics.get("lp_direction"), lp_context.get("direction")),
            "lp_same_pool_continuity": _int_value(gate_metrics.get("lp_same_pool_continuity"), lp_analysis.get("same_pool_continuity")),
            "lp_multi_pool_resonance": _int_value(gate_metrics.get("lp_multi_pool_resonance"), lp_analysis.get("multi_pool_resonance")),
            "lp_action_intensity": _num(gate_metrics.get("lp_action_intensity"), lp_analysis.get("action_intensity")),
            "lp_reserve_skew": _num(gate_metrics.get("lp_reserve_skew"), lp_analysis.get("reserve_skew")),
            "lp_pool_volume_surge_ratio": _num(gate_metrics.get("lp_pool_volume_surge_ratio"), lp_analysis.get("pool_volume_surge_ratio")),
            "lp_pool_window_trade_count": _int_value(gate_metrics.get("lp_pool_window_trade_count"), lp_analysis.get("pool_window_trade_count")),
            "lp_pool_window_usd_total": _num(gate_metrics.get("lp_pool_window_usd_total"), lp_analysis.get("pool_window_usd_total"), digits=2),
            "lp_observe_exception_applied": _bool_value(gate_metrics.get("lp_observe_exception_applied")),
            "lp_observe_exception_reason": _text(gate_metrics.get("lp_observe_exception_reason")),
            "lp_observe_threshold_ratio": _num(gate_metrics.get("lp_observe_threshold_ratio")),
            "lp_observe_below_min_gap": _num(gate_metrics.get("lp_observe_below_min_gap"), digits=2),
            "value_weight_multiplier": _num(gate_metrics.get("value_weight_multiplier")),
            "role_group_value_bonus": _num(gate_metrics.get("role_group_value_bonus")),
            "smart_money_value_bonus": _num(gate_metrics.get("smart_money_value_bonus")),
            "smart_money_non_exec_exception_applied": _bool_value(gate_metrics.get("smart_money_non_exec_exception_applied")),
            "smart_money_non_exec_exception_reason": _text(gate_metrics.get("smart_money_non_exec_exception_reason")),
            "smart_money_non_exec_threshold_ratio": _num(gate_metrics.get("smart_money_non_exec_threshold_ratio")),
            "smart_money_non_exec_quality_gap": _num(gate_metrics.get("smart_money_non_exec_quality_gap")),
            "smart_money_non_exec_value_bonus": _num(gate_metrics.get("smart_money_non_exec_value_bonus")),
            "market_maker_observe_exception_applied": _bool_value(
                gate_metrics.get("market_maker_observe_exception_applied"),
                signal_metadata.get("market_maker_observe_exception_applied"),
            ),
            "market_maker_observe_exception_reason": _text(
                gate_metrics.get("market_maker_observe_exception_reason"),
                signal_metadata.get("market_maker_observe_exception_reason"),
            ),
            "market_maker_threshold_ratio": _num(
                gate_metrics.get("market_maker_threshold_ratio"),
                signal_metadata.get("market_maker_threshold_ratio"),
            ),
            "market_maker_quality_gap": _num(
                gate_metrics.get("market_maker_quality_gap"),
                signal_metadata.get("market_maker_quality_gap"),
            ),
            "gate_exception_passed": _bool_value(gate_metrics.get("gate_exception_passed")),
            "smart_money_style_variant": _text(
                signal_context.get("smart_money_style_variant"),
                signal_metadata.get("smart_money_style_variant"),
            ),
            "lp_buy_cluster_5m": _int_value(gate_metrics.get("buy_cluster_5m")),
            "lp_sell_cluster_5m": _int_value(gate_metrics.get("sell_cluster_5m")),
            "lp_liquidity_proxy_usd": _num(gate_metrics.get("liquidity_proxy_usd"), digits=2),
            "lp_volume_24h_proxy_usd": _num(gate_metrics.get("volume_24h_proxy_usd"), digits=2),
            "lp_price_impact_ratio": _num(gate_metrics.get("price_impact_ratio")),
            "lp_token_volume_ratio": _num(gate_metrics.get("token_volume_ratio")),
            "is_exchange_related": _bool_value(gate_metrics.get("is_exchange_related"), raw.get("is_exchange_related")),
            "is_stablecoin_flow": _bool_value(gate_metrics.get("is_stablecoin_flow"), raw.get("is_stablecoin_flow")),
            "stablecoin_dominant": _bool_value(gate_metrics.get("stablecoin_dominant"), raw.get("is_stablecoin_flow")),
            "possible_internal_transfer": _bool_value(gate_metrics.get("possible_internal_transfer"), raw.get("possible_internal_transfer")),
            "exchange_noise_sensitive": _bool_value(gate_metrics.get("exchange_noise_sensitive")),
            "followup_strength": _text(gate_metrics.get("followup_strength"), event_metadata.get("followup_strength")),
            "followup_semantic": _text(gate_metrics.get("followup_semantic"), event_metadata.get("followup_semantic")),
            "followup_confirmed": _bool_value(gate_metrics.get("followup_confirmed"), event_metadata.get("followup_confirmed")),
            "execution_required_but_missing": _bool_value(gate_metrics.get("execution_required_but_missing"), event_metadata.get("execution_required_but_missing")),
            "stable_non_swap_filtered_flag": _bool_value(gate_metrics.get("stable_non_swap_filtered_flag"), gate_reason == "stable_non_swap_filtered"),
            "stable_non_swap_hard_filter_usd": _num(gate_metrics.get("stable_non_swap_hard_filter_usd"), digits=2),
            "stable_transfer_min_usd": _num(gate_metrics.get("stable_transfer_min_usd"), digits=2),
            "liquidation_stage": _text(event_metadata.get("liquidation_stage"), liquidation.get("liquidation_stage")),
            "liquidation_score": _num(event_metadata.get("liquidation_score"), liquidation.get("liquidation_score")),
            "liquidation_side": _text(event_metadata.get("liquidation_side"), liquidation.get("liquidation_side")),
            "liquidation_reason": _text(event_metadata.get("liquidation_reason"), liquidation.get("liquidation_reason")),
            "liquidation_protocols": list(event_metadata.get("liquidation_protocols") or liquidation.get("liquidation_protocols") or []),
            "liquidation_evidence_count": _int_value(
                len(event_metadata.get("liquidation_evidence") or liquidation.get("liquidation_evidence") or []),
                liquidation.get("liquidation_evidence_count"),
            ),
            "is_liquidation_protocol_related": _bool_value(
                raw.get("is_liquidation_protocol_related"),
                (raw.get("inferred_context") or {}).get("is_liquidation_protocol_related"),
            ),
            "possible_keeper_executor": _bool_value(
                raw.get("possible_keeper_executor"),
                (raw.get("inferred_context") or {}).get("possible_keeper_executor"),
            ),
            "possible_vault_or_auction": _bool_value(
                raw.get("possible_vault_or_auction"),
                (raw.get("inferred_context") or {}).get("possible_vault_or_auction"),
            ),
            "replay_source": _text(event_metadata.get("replay_source"), raw.get("replay_source")),
            "metadata_summary": _text(
                signal_context.get("headline_label"),
                signal_context.get("fact_brief"),
                event_metadata.get("followup_label"),
            ),
        }
        if audit_extras:
            payload.update(audit_extras)
        return payload

    def _observe_address_intelligence(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict | None,
        raw_item: dict,
        token_snapshot: dict,
    ) -> dict | None:
        if self.address_intelligence is None:
            return None
        if self._is_lp_event(event=event):
            return None
        try:
            return self.address_intelligence.observe_event(
                event=event,
                parsed=parsed,
                watch_context=watch_context,
                raw_item=raw_item,
                token_snapshot=token_snapshot,
            )
        except Exception as e:
            print(f"地址情报采集失败: {e}")
            return None

    def _match_or_open_case(self, event: Event, watch_meta: dict, behavior: dict) -> dict | None:
        if self.followup_tracker is None:
            return None
        try:
            return self.followup_tracker.match_or_open_case(
                event=event,
                watch_meta=watch_meta,
                behavior=behavior,
            )
        except Exception as e:
            print(f"followup case 处理失败: {e}")
            return None

    def _attach_signal_to_case(self, behavior_case, signal):
        if self.followup_tracker is None or behavior_case is None:
            return behavior_case
        try:
            return self.followup_tracker.attach_signal(behavior_case, signal)
        except Exception as e:
            print(f"case 绑定 signal 失败: {e}")
            return behavior_case

    def _bind_case_to_event(self, event: Event, behavior_case, case_result: dict | None) -> None:
        if behavior_case is None:
            return
        event.case_id = behavior_case.case_id
        event.followup_stage = behavior_case.stage
        event.followup_status = behavior_case.status
        invalidated_cases = (case_result or {}).get("invalidated_cases") or []
        if invalidated_cases and not event.parent_case_id:
            event.parent_case_id = invalidated_cases[0].case_id

    def _apply_exchange_followup_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        if not self._is_exchange_followup_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        followup_assets = list(metadata.get("followup_tokens_seen") or [])
        followup_confirmed = bool(metadata.get("followup_confirmed"))
        anchor_symbol = str(metadata.get("anchor_symbol") or event.metadata.get("token_symbol") or "稳定币")
        has_execution_evidence = bool(event.kind == "swap" or str(event.intent_type or "") == "swap_execution")
        followup_strength = "weak"
        followup_semantic = "observation_upgrade"
        execution_required_but_missing = False
        label = "结构延续观察升级" if followup_confirmed else "交易场景观察建立"
        if followup_confirmed and followup_assets:
            assets_text = "/".join(followup_assets[:3])
            detail = f"前序{anchor_symbol}流入后，{assets_text} 又流入同一交易所地址，当前更像结构延续观察"
        else:
            detail = f"{anchor_symbol} 已流入该交易所地址，先建立观察锚点，继续看是否出现真实执行或更强共振"

        if followup_confirmed:
            if has_execution_evidence or float(event.confirmation_score or 0.0) >= 0.82:
                followup_strength = "strong"
                followup_semantic = "execution_confirmed" if has_execution_evidence else "structure_continuation"
            else:
                followup_strength = "medium"
                followup_semantic = "structure_continuation"
                execution_required_but_missing = not has_execution_evidence

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "followup_confirmed": followup_confirmed,
            "followup_assets": followup_assets,
        }
        event.metadata["case_summary"] = behavior_case.summary
        event.metadata["case_followup_steps"] = list(behavior_case.followup_steps or [])
        event.metadata["followup_confirmed"] = followup_confirmed
        event.metadata["followup_assets"] = followup_assets
        event.metadata["followup_label"] = label
        event.metadata["followup_detail"] = detail
        event.metadata["followup_strength"] = followup_strength
        event.metadata["followup_semantic"] = followup_semantic
        event.metadata["execution_required_but_missing"] = execution_required_but_missing
        event.metadata["exchange_followup_case"] = True
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["exchange_followup_anchor_event"] = bool((case_result or {}).get("exchange_followup_anchor"))
        event.metadata["exchange_followup_confirmed_event"] = bool((case_result or {}).get("exchange_followup_confirmed"))

        if followup_confirmed and not bool(event.metadata.get("exchange_followup_confirmation_applied")):
            boost = 0.14 if has_execution_evidence else 0.08
            event.confirmation_score = self._clamp(max(float(event.confirmation_score or 0.0), 0.60) + boost, 0.0, 0.90)
            for evidence in [
                "前序稳定币已进入交易场景",
                "同一交易所地址后续出现主流资产继续流入",
                "形成跨 token 结构延续观察",
            ]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if has_execution_evidence and event.intent_stage != "confirmed":
                event.intent_stage = "confirmed"
            elif event.intent_stage == "weak":
                event.intent_stage = "preliminary"
            intent_meta = dict(event.metadata.get("intent") or {})
            intent_meta["intent_stage"] = event.intent_stage
            intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
            intent_meta["intent_evidence"] = list(event.intent_evidence or [])
            intent_meta["exchange_cross_token_followup"] = True
            intent_meta["followup_strength"] = followup_strength
            intent_meta["followup_semantic"] = followup_semantic
            intent_meta["execution_required_but_missing"] = execution_required_but_missing
            event.metadata["intent"] = intent_meta
            event.metadata["exchange_followup_confirmation_applied"] = True

    def _apply_smart_money_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        if not self._is_smart_money_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        execution_count = int(metadata.get("execution_count") or 0)
        cohort_addresses = list(metadata.get("cohort_addresses") or [])
        continuation_confirmed = bool(metadata.get("continuation_confirmed"))
        latest_symbol = str(metadata.get("latest_symbol") or event.metadata.get("token_symbol") or event.token or "")
        same_actor_continuation = bool(metadata.get("same_actor_continuation"))
        size_expansion_ratio = float(metadata.get("size_expansion_ratio") or 1.0)
        first_execution_usd = float(metadata.get("first_execution_usd") or 0.0)
        max_execution_usd = float(metadata.get("max_execution_usd") or 0.0)
        watch_strategy_role = str(metadata.get("watch_strategy_role") or event.strategy_role or "unknown")

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "execution_count": execution_count,
            "cohort_addresses": cohort_addresses,
            "continuation_confirmed": continuation_confirmed,
            "same_actor_continuation": same_actor_continuation,
            "size_expansion_ratio": size_expansion_ratio,
        }
        event.metadata["smart_money_case"] = True
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["smart_money_case_confirmed"] = continuation_confirmed
        event.metadata["smart_money_case_execution_count"] = execution_count
        event.metadata["smart_money_case_addresses"] = cohort_addresses
        event.metadata["smart_money_same_actor_continuation"] = same_actor_continuation
        event.metadata["smart_money_size_expansion_ratio"] = size_expansion_ratio
        event.metadata["smart_money_case_first_execution_usd"] = first_execution_usd
        event.metadata["smart_money_case_max_execution_usd"] = max_execution_usd
        event.metadata["smart_money_case_role"] = watch_strategy_role
        event.metadata["smart_money_case_label"] = f"{latest_symbol} 聪明钱连续执行"
        event.metadata["smart_money_case_detail"] = (
            f"{execution_count} 次同向真实执行，{len(cohort_addresses)} 个地址参与，放大量 {size_expansion_ratio:.2f}x"
            if execution_count > 1 else f"{latest_symbol} 首次真实执行，等待后续连续确认"
        )

        if continuation_confirmed and not bool(event.metadata.get("smart_money_case_confirmation_applied")):
            event.confirmation_score = self._clamp(max(float(event.confirmation_score or 0.0), 0.72) + 0.18, 0.0, 0.98)
            event.intent_stage = "confirmed"
            event.intent_confidence = self._clamp(max(float(event.intent_confidence or 0.0), 0.74) + 0.10, 0.0, 0.98)
            for evidence in [
                "短窗口内再次出现同向真实执行",
                "聪明钱执行连续性得到确认",
            ]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if same_actor_continuation:
                evidence = "同地址连续两次同向真实执行"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if size_expansion_ratio >= 1.22:
                evidence = f"执行金额相对首次放大 {size_expansion_ratio:.2f}x"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if len(cohort_addresses) >= 2:
                evidence = "多聪明钱地址出现同向真实执行"
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            intent_meta = dict(event.metadata.get("intent") or {})
            intent_meta["intent_stage"] = event.intent_stage
            intent_meta["intent_confidence"] = round(float(event.intent_confidence or 0.0), 3)
            intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
            intent_meta["intent_evidence"] = list(event.intent_evidence or [])
            intent_meta["smart_money_execution_case"] = True
            event.metadata["intent"] = intent_meta
            event.metadata["smart_money_case_confirmation_applied"] = True

    def _apply_liquidation_case_context(
        self,
        event: Event,
        behavior_case,
        case_result: dict | None,
        watch_meta: dict | None = None,
    ) -> None:
        del watch_meta, case_result
        if not self._is_liquidation_case(behavior_case):
            return

        metadata = behavior_case.metadata or {}
        protocols = list(metadata.get("protocols_seen") or event.metadata.get("liquidation_protocols") or [])
        pools = list(metadata.get("pools_seen") or [event.address])
        stage = str(metadata.get("case_stage") or behavior_case.stage or "")
        risk_hits = int(metadata.get("risk_hits") or 0)
        execution_hits = int(metadata.get("execution_hits") or 0)
        max_score = float(metadata.get("max_liquidation_score") or event.metadata.get("liquidation_score") or 0.0)

        event.metadata["case"] = {
            **dict(event.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": metadata.get("case_family", ""),
            "protocols_seen": protocols,
            "pools_seen": pools,
            "risk_hits": risk_hits,
            "execution_hits": execution_hits,
            "max_liquidation_score": max_score,
        }
        event.metadata["case_family"] = metadata.get("case_family", "")
        event.metadata["liquidation_case"] = True
        event.metadata["liquidation_case_protocols"] = protocols
        event.metadata["liquidation_case_pools"] = pools
        event.metadata["liquidation_case_risk_hits"] = risk_hits
        event.metadata["liquidation_case_execution_hits"] = execution_hits
        event.metadata["liquidation_case_stage"] = stage
        event.metadata["liquidation_case_confirmed"] = execution_hits >= 1
        event.metadata["liquidation_case_label"] = "Liquidation 风险簇" if stage != "execution_confirmed" else "Liquidation 执行簇"
        event.metadata["liquidation_case_detail"] = (
            f"{len(pools)} 个池子，{risk_hits} 次风险，{execution_hits} 次执行，协议 {('/'.join(protocols[:3]) or 'unknown')}"
        )

    def _sync_exchange_followup_to_signal(self, event: Event, signal, behavior_case) -> None:
        if behavior_case is None or not self._is_exchange_followup_case(behavior_case):
            return

        followup_fields = {
            "case_family": event.metadata.get("case_family", ""),
            "case_notification_stage": event.metadata.get("case_notification_stage"),
            "case_notification_allowed": event.metadata.get("case_notification_allowed"),
            "case_notification_suppressed": event.metadata.get("case_notification_suppressed"),
            "case_notification_reason": event.metadata.get("case_notification_reason"),
            "followup_confirmed": bool(event.metadata.get("followup_confirmed")),
            "followup_assets": list(event.metadata.get("followup_assets") or []),
            "followup_label": event.metadata.get("followup_label", ""),
            "followup_detail": event.metadata.get("followup_detail", ""),
            "followup_strength": event.metadata.get("followup_strength", ""),
            "followup_semantic": event.metadata.get("followup_semantic", ""),
            "execution_required_but_missing": bool(event.metadata.get("execution_required_but_missing")),
            "notification_stage_label": event.metadata.get("notification_stage_label", ""),
        }
        signal.metadata.update(followup_fields)
        signal.metadata["case"] = {
            **dict(signal.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "case_family": followup_fields["case_family"],
            "followup_confirmed": followup_fields["followup_confirmed"],
            "followup_assets": followup_fields["followup_assets"],
        }

    def _sync_smart_money_case_to_signal(self, event: Event, signal, behavior_case) -> None:
        if behavior_case is None or not self._is_smart_money_case(behavior_case):
            return

        payload = {
            "smart_money_case": True,
            "smart_money_case_confirmed": bool(event.metadata.get("smart_money_case_confirmed")),
            "smart_money_case_execution_count": int(event.metadata.get("smart_money_case_execution_count") or 0),
            "smart_money_case_addresses": list(event.metadata.get("smart_money_case_addresses") or []),
            "smart_money_same_actor_continuation": bool(event.metadata.get("smart_money_same_actor_continuation")),
            "smart_money_size_expansion_ratio": float(event.metadata.get("smart_money_size_expansion_ratio") or 1.0),
            "smart_money_case_label": event.metadata.get("smart_money_case_label", ""),
            "smart_money_case_detail": event.metadata.get("smart_money_case_detail", ""),
            "case_family": event.metadata.get("case_family", "") or getattr(behavior_case, "metadata", {}).get("case_family", ""),
        }
        signal.metadata.update(payload)
        signal.context.update(payload)
        signal.metadata["case"] = {
            **dict(signal.metadata.get("case") or {}),
            "case_id": behavior_case.case_id,
            "status": behavior_case.status,
            "stage": behavior_case.stage,
            "summary": behavior_case.summary,
            "execution_count": payload["smart_money_case_execution_count"],
            "cohort_addresses": payload["smart_money_case_addresses"],
            "continuation_confirmed": payload["smart_money_case_confirmed"],
        }

    def _sync_liquidation_to_signal(self, event: Event, signal, behavior_case) -> None:
        payload = {
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "liquidation_score": float(event.metadata.get("liquidation_score") or 0.0),
            "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
            "liquidation_reason": str(event.metadata.get("liquidation_reason") or ""),
            "liquidation_evidence": list(event.metadata.get("liquidation_evidence") or []),
            "liquidation_protocols": list(event.metadata.get("liquidation_protocols") or []),
            "liquidation_roles": list(event.metadata.get("liquidation_roles") or []),
            "semantic_overlay": str(event.metadata.get("semantic_overlay") or ""),
            "liquidation_primary_candidate": bool(event.metadata.get("liquidation_primary_candidate")),
            "liquidation_case": bool(event.metadata.get("liquidation_case")),
            "liquidation_case_confirmed": bool(event.metadata.get("liquidation_case_confirmed")),
            "liquidation_case_label": str(event.metadata.get("liquidation_case_label") or ""),
            "liquidation_case_detail": str(event.metadata.get("liquidation_case_detail") or ""),
            "case_family": str(event.metadata.get("case_family") or ""),
        }
        signal.metadata.update(payload)
        signal.context.update(payload)
        if behavior_case is not None and self._is_liquidation_case(behavior_case):
            signal.metadata["case"] = {
                **dict(signal.metadata.get("case") or {}),
                "case_id": behavior_case.case_id,
                "status": behavior_case.status,
                "stage": behavior_case.stage,
                "summary": behavior_case.summary,
                "protocols_seen": list((behavior_case.metadata or {}).get("protocols_seen") or []),
                "pools_seen": list((behavior_case.metadata or {}).get("pools_seen") or []),
            }

    def _apply_case_notification_control(self, event: Event, signal, behavior_case) -> bool:
        if behavior_case is None or not (
            self._is_exchange_followup_case(behavior_case)
            or self._is_smart_money_case(behavior_case)
            or self._is_liquidation_case(behavior_case)
        ):
            return True

        stage = self._resolve_case_notification_stage(event, signal, behavior_case)
        allowed, reason = self._should_emit_case_signal(behavior_case, stage)
        self._apply_case_notification_metadata(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
            stage=stage,
            allowed=allowed,
            reason=reason,
        )
        if not allowed:
            return False

        self._mark_case_signal_emitted(behavior_case, stage, signal)
        self._apply_case_notification_metadata(
            event=event,
            signal=signal,
            behavior_case=behavior_case,
            stage=stage,
            allowed=True,
            reason="allowed",
        )
        return True

    def _resolve_case_notification_stage(self, event: Event, signal, behavior_case) -> str | None:
        if self._is_exchange_followup_case(behavior_case):
            if bool(event.metadata.get("exchange_followup_confirmed_event")) and bool(event.metadata.get("followup_confirmed")):
                return "followup_confirmed"
            if bool(event.metadata.get("exchange_followup_anchor_event")) and not bool(event.metadata.get("followup_confirmed")):
                return "anchor_opened"
            return None

        if self._is_smart_money_case(behavior_case):
            if bool(event.metadata.get("smart_money_case_confirmed")) or str(getattr(behavior_case, "stage", "") or "") == "execution_followup_confirmed":
                return "continuation_confirmed"
            if str(getattr(behavior_case, "stage", "") or "") in {"execution_opened", "execution_tracking"}:
                return "execution_opened"
        if self._is_liquidation_case(behavior_case):
            stage = str(getattr(behavior_case, "stage", "") or event.metadata.get("liquidation_case_stage") or "")
            if stage == "execution_confirmed":
                return "execution_confirmed"
            if stage == "risk_escalating":
                return "risk_escalating"
            if stage == "risk_opened":
                return "risk_opened"
        return None

    def _should_emit_case_signal(self, behavior_case, stage: str | None) -> tuple[bool, str]:
        if self.followup_tracker is None:
            return True, "no_followup_tracker"
        if not (
            self._is_exchange_followup_case(behavior_case)
            or self._is_smart_money_case(behavior_case)
            or self._is_liquidation_case(behavior_case)
        ):
            return True, "not_exchange_followup_case"
        try:
            return self.followup_tracker.case_notification_decision(behavior_case, stage)
        except Exception as e:
            print(f"case 通知判断失败: {e}")
            return False, "case_notification_decision_error"

    def _mark_case_signal_emitted(self, behavior_case, stage: str | None, signal) -> None:
        if self.followup_tracker is None or behavior_case is None or not stage:
            return
        try:
            self.followup_tracker.mark_case_notification_emitted(behavior_case, stage, signal)
        except Exception as e:
            print(f"case 通知状态标记失败: {e}")

    def _apply_case_notification_metadata(
        self,
        event: Event,
        signal,
        behavior_case,
        stage: str | None,
        allowed: bool,
        reason: str,
    ) -> None:
        metadata = behavior_case.metadata if behavior_case is not None else {}
        payload = {
            "case_notification_stage": stage,
            "case_notification_allowed": bool(allowed),
            "case_notification_suppressed": not bool(allowed),
            "case_notification_reason": reason,
            "followup_confirmed": bool(metadata.get("followup_confirmed")),
            "followup_assets": list(metadata.get("followup_tokens_seen") or []),
            "followup_label": event.metadata.get("followup_label", ""),
            "followup_detail": event.metadata.get("followup_detail", ""),
            "liquidation_case_label": event.metadata.get("liquidation_case_label", ""),
            "liquidation_case_detail": event.metadata.get("liquidation_case_detail", ""),
            "notification_stage_label": self._notification_stage_label(stage),
        }
        event.metadata.update(payload)
        signal.context.update(payload)
        signal.metadata.update(payload)
        decision = {
            "stage": stage,
            "allowed": bool(allowed),
            "reason": reason,
            "signal_id": str(getattr(signal, "signal_id", "") or getattr(signal, "event_id", "") or ""),
            "emitted_stages": list(metadata.get("emitted_notification_stages") or []),
        }
        if behavior_case is not None:
            behavior_case.metadata["last_notification_decision"] = decision

    def _notification_stage_label(self, stage: str | None) -> str:
        if stage == "anchor_opened":
            return "观察建立"
        if stage == "followup_confirmed":
            return "观察升级"
        if stage == "execution_opened":
            return "首次执行"
        if stage == "continuation_confirmed":
            return "连续确认"
        if stage == "risk_opened":
            return "风险开启"
        if stage == "risk_escalating":
            return "风险升级"
        if stage == "execution_confirmed":
            return "执行确认"
        return ""

    def _should_emit_delivery_notification(self, event: Event, signal) -> bool:
        delivery_class = str(signal.delivery_class or event.delivery_class or "drop")
        if delivery_class == "primary":
            return True
        if delivery_class != "observe":
            return False

        role_group = strategy_role_group(event.strategy_role)
        if role_group == "lp_pool":
            if str(event.metadata.get("liquidation_stage") or "none") in {"risk", "execution"}:
                return bool(DELIVERY_ALLOW_LIQUIDATION_RISK_OBSERVE)
            return bool(DELIVERY_ALLOW_LP_OBSERVE)
        if role_group == "exchange":
            return bool(DELIVERY_ALLOW_EXCHANGE_OBSERVE)
        if role_group == "smart_money":
            if not DELIVERY_ALLOW_SMART_MONEY_TRANSFER_OBSERVE:
                return False
            allowed_reasons = {
                "smart_money_transfer_observe",
                "market_maker_execution_observe",
                "market_maker_inventory_observe",
                "market_maker_inventory_shift_observe",
                "market_maker_non_execution_observe",
                "smart_money_non_execution_observe",
                "smart_money_execution_observe",
            }
            return str(signal.delivery_reason or event.delivery_reason or "") in allowed_reasons
        return False

    def _is_exchange_followup_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "exchange_cross_token_followup"

    def _is_smart_money_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "smart_money_execution_case"

    def _is_liquidation_case(self, behavior_case) -> bool:
        metadata = getattr(behavior_case, "metadata", {}) or {}
        return str(metadata.get("case_family") or "") == "liquidation_case_family"

    def _record_counterparty(self, event: Event, watch_context: dict | None, parsed: dict) -> None:
        counterparty = ""
        if watch_context:
            counterparty = str(watch_context.get("counterparty") or "").lower()
        if not counterparty:
            counterparty = str(parsed.get("counterparty") or "").lower()
        if not counterparty:
            return

        counterparty_meta = (watch_context or {}).get("counterparty_meta") or get_address_meta(counterparty)
        self.state_manager.record_counterparty(
            address=event.address,
            counterparty=counterparty,
            usd_value=float(event.usd_value or 0.0),
            ts=int(event.ts or 0),
            metadata={
                "role": counterparty_meta.get("role", "unknown"),
                "strategy_role": counterparty_meta.get("strategy_role", "unknown"),
                "semantic_role": counterparty_meta.get("semantic_role", "unknown"),
                "intent_type": event.intent_type,
            },
        )

    def _parsed_archive_payload(
        self,
        parsed: dict,
        event: Event,
        watch_meta: dict,
        behavior: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
    ) -> dict:
        return {
            **parsed,
            "event_id": event.event_id,
            "case_id": event.case_id,
            "followup_stage": event.followup_stage,
            "followup_status": event.followup_status,
            "intent_type": event.intent_type,
            "intent_confidence": float(event.intent_confidence or 0.0),
            "intent_stage": event.intent_stage,
            "confirmation_score": float(event.confirmation_score or 0.0),
            "watch_meta": event.metadata.get("watch_meta", {}),
            "behavior": behavior,
            "recent_counterparties": address_snapshot.get("recent_counterparties", []),
            "token_open_case_ids": token_snapshot.get("open_case_ids", []),
            "watch_label": watch_meta.get("label", ""),
            "pool_snapshot": pool_snapshot,
            "lp_analysis": dict(event.metadata.get("lp_analysis") or {}),
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "liquidation_score": float(event.metadata.get("liquidation_score") or 0.0),
            "liquidation_side": str(event.metadata.get("liquidation_side") or "unknown"),
            "liquidation_protocols": list(event.metadata.get("liquidation_protocols") or []),
            "liquidation_evidence_count": len(event.metadata.get("liquidation_evidence") or []),
            "liquidation_reason": str(event.metadata.get("liquidation_reason") or ""),
            "is_liquidation_protocol_related": bool(parsed.get("is_liquidation_protocol_related")),
            "possible_keeper_executor": bool(parsed.get("possible_keeper_executor")),
            "possible_vault_or_auction": bool(parsed.get("possible_vault_or_auction")),
            "replay_source": str(event.metadata.get("replay_source") or ""),
        }

    def _build_event_id(self, event: Event) -> str:
        raw = "|".join([
            str(event.tx_hash or ""),
            str(event.address or ""),
            str(event.token or "native"),
            str(event.kind or ""),
            str(event.side or ""),
            str(int(event.ts or 0)),
        ])
        return "evt_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def _build_signal_id(self, signal, event: Event) -> str:
        raw = "|".join([
            str(event.event_id or ""),
            str(getattr(signal, "type", "")),
            str(getattr(signal, "tier", "")),
            str(round(float(getattr(signal, "confidence", 0.0) or 0.0), 4)),
        ])
        return "sig_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    async def _evaluate_pricing(self, parsed: dict) -> dict:
        return await self.price_service.evaluate_event_pricing(parsed)

    def _classify_intent(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict,
        watch_meta: dict,
    ) -> dict:
        if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed):
            return self.lp_analyzer.preliminary_intent(event, parsed)

        inferred = parsed.get("inferred_context") or {}
        counterparty = str(watch_context.get("counterparty") or parsed.get("counterparty") or "").lower()
        counterparty_meta = watch_context.get("counterparty_meta") or get_address_meta(counterparty)

        watch_strategy_role = str(watch_meta.get("strategy_role") or "unknown")
        watch_semantic_role = str(watch_meta.get("semantic_role") or "unknown")
        counterparty_strategy_role = str(counterparty_meta.get("strategy_role") or "unknown")
        counterparty_role = str(counterparty_meta.get("role") or "unknown")

        is_stable = bool(parsed.get("is_stablecoin_flow") or (event.token or "").lower() in STABLE_TOKEN_CONTRACTS)
        is_exchange_related = bool(parsed.get("is_exchange_related"))
        is_router_related = bool(parsed.get("is_router_related"))
        is_protocol_related = bool(parsed.get("is_protocol_related"))
        possible_internal_transfer = bool(
            parsed.get("possible_internal_transfer")
            or watch_context.get("direction") == "内部划转"
        )
        direction = str(event.side or watch_context.get("direction") or "")
        watch_is_exchange = watch_strategy_role.startswith("exchange_")
        counterparty_is_exchange = counterparty_strategy_role.startswith("exchange_")
        both_exchange = watch_is_exchange and counterparty_is_exchange

        usd_value = float(event.usd_value or 0.0)
        participant_count = len(parsed.get("participant_addresses") or [])
        next_hop_count = len(parsed.get("next_hop_addresses") or [])
        single_hop = participant_count <= 2 and next_hop_count <= 1
        counterparty_unknown = (
            counterparty_role == "unknown"
            and counterparty_strategy_role == "unknown"
            and not counterparty_is_exchange
        )
        route_like = is_router_related or is_protocol_related
        external_non_exchange = not counterparty_is_exchange and not possible_internal_transfer

        if usd_value < 1500:
            size_bucket = "small"
        elif usd_value < 10000:
            size_bucket = "medium"
        else:
            size_bucket = "large"

        if event.kind == "swap":
            return {
                "intent_type": "swap_execution",
                "intent_confidence": 0.98,
                "information_level": "high",
                "confirmation_score": 0.58,
                "intent_evidence": ["当前事件本身是链上实际 swap"],
                "basis": {
                    "kind": event.kind,
                    "side": direction,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "is_router_related": is_router_related,
                },
            }

        if possible_internal_transfer or both_exchange:
            if watch_strategy_role == "market_maker_wallet" or counterparty_strategy_role == "market_maker_wallet":
                return {
                    "intent_type": "market_making_inventory_move",
                    "intent_confidence": 0.68,
                    "information_level": "medium",
                    "confirmation_score": 0.36,
                    "intent_evidence": ["路径存在同体系地址/同策略角色转移特征，更像库存迁移"],
                    "basis": {
                        "possible_internal_transfer": True,
                        "watch_strategy_role": watch_strategy_role,
                        "counterparty_strategy_role": counterparty_strategy_role,
                        "both_exchange": both_exchange,
                    },
                }

            return {
                "intent_type": "internal_rebalance",
                "intent_confidence": 0.64,
                "information_level": "low",
                "confirmation_score": 0.34,
                "intent_evidence": ["路径更像同体系地址调拨或归集"],
                "basis": {
                    "possible_internal_transfer": True,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "both_exchange": both_exchange,
                },
            }

        if event.kind != "swap" and watch_strategy_role == "market_maker_wallet" and route_like:
            return {
                "intent_type": "market_making_inventory_move",
                "intent_confidence": 0.64,
                "information_level": "medium",
                "confirmation_score": 0.32,
                "intent_evidence": ["做市地址与路由/协议发生非 swap 资金变动，更像库存调节"],
                "basis": {
                    "watch_strategy_role": watch_strategy_role,
                    "is_router_related": is_router_related,
                    "is_protocol_related": is_protocol_related,
                    "size_bucket": size_bucket,
                },
            }

        # 重点修正：交易所热钱包非 swap 事件不再一律 pure_transfer
        if watch_is_exchange and event.kind != "swap":
            if direction == "流入":
                if size_bucket == "large" and external_non_exchange:
                    return {
                        "intent_type": "exchange_deposit_candidate",
                        "intent_confidence": 0.56,
                        "information_level": "medium",
                        "confirmation_score": 0.32,
                        "intent_evidence": ["大额资产正在流入交易所热钱包，具备进入交易场景特征"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_strategy_role": counterparty_strategy_role,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

                if size_bucket == "medium" and external_non_exchange and (counterparty_unknown or is_stable):
                    return {
                        "intent_type": "exchange_deposit_candidate",
                        "intent_confidence": 0.51,
                        "information_level": "low",
                        "confirmation_score": 0.26,
                        "intent_evidence": ["中等规模资金正流入交易所热钱包，先保留为交易场景线索"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

            if direction == "流出":
                if is_stable and external_non_exchange and counterparty_unknown and size_bucket == "large":
                    return {
                        "intent_type": "possible_buy_preparation",
                        "intent_confidence": 0.58,
                        "information_level": "medium",
                        "confirmation_score": 0.34,
                        "intent_evidence": ["大额稳定币正从交易所热钱包流向未知外部地址，更像入场前资金准备"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                            "counterparty_strategy_role": counterparty_strategy_role,
                        },
                    }

                if external_non_exchange and size_bucket == "large":
                    return {
                        "intent_type": "exchange_withdraw_candidate",
                        "intent_confidence": 0.56,
                        "information_level": "medium",
                        "confirmation_score": 0.30,
                        "intent_evidence": ["大额资产正在离开交易所热钱包，当前先按外流方向线索处理"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                            "counterparty_strategy_role": counterparty_strategy_role,
                        },
                    }

                if is_stable and external_non_exchange and size_bucket == "medium":
                    return {
                        "intent_type": "exchange_withdraw_candidate",
                        "intent_confidence": 0.52,
                        "information_level": "low",
                        "confirmation_score": 0.28,
                        "intent_evidence": ["中等规模稳定币正离开交易所热钱包，先保留为外流观察线索"],
                        "basis": {
                            "direction": direction,
                            "size_bucket": size_bucket,
                            "is_stablecoin_flow": is_stable,
                            "counterparty_unknown": counterparty_unknown,
                        },
                    }

            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.40 if size_bucket == "small" and single_hop else 0.44,
                "information_level": "low",
                "confirmation_score": 0.20 if size_bucket == "small" else 0.24,
                "intent_evidence": ["交易所热钱包发生非 swap 资金流，当前先按行为通知保守处理"],
                "basis": {
                    "watch_role": watch_strategy_role,
                    "counterparty_role": counterparty_strategy_role,
                    "direction": direction,
                    "size_bucket": size_bucket,
                    "single_hop": single_hop,
                },
            }

        if is_exchange_related and (counterparty_is_exchange or watch_is_exchange):
            if not watch_is_exchange and counterparty_is_exchange and direction == "流出":
                if is_stable:
                    return {
                        "intent_type": "possible_buy_preparation",
                        "intent_confidence": 0.56,
                        "information_level": "low",
                        "confirmation_score": 0.26,
                        "intent_evidence": ["稳定资产正在流向交易场景，先记为行为线索"],
                        "basis": {"exchange_leg": "outbound_stable", "counterparty": counterparty_strategy_role},
                    }
                return {
                    "intent_type": "exchange_deposit_candidate",
                    "intent_confidence": 0.60,
                    "information_level": "low",
                    "confirmation_score": 0.28,
                    "intent_evidence": ["token 正在流向交易所相关地址，当前仅保留观察性判断"],
                    "basis": {"exchange_leg": "outbound_token", "counterparty": counterparty_strategy_role},
                }

            if not watch_is_exchange and counterparty_is_exchange and direction == "流入":
                return {
                    "intent_type": "exchange_withdraw_candidate",
                    "intent_confidence": 0.58,
                    "information_level": "low",
                    "confirmation_score": 0.26,
                    "intent_evidence": ["资金正在离开交易场景，仍需后续动作确认"],
                    "basis": {"exchange_leg": "inbound_from_exchange", "counterparty": counterparty_strategy_role},
                }

        if event.kind != "swap" and direction in {"流入", "流出"}:
            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.44,
                "information_level": "low",
                "confirmation_score": 0.20,
                "intent_evidence": ["当前仅观察到单笔资金动作，先按重点地址行为通知处理"],
                "basis": {
                    "direction": direction,
                    "watch_semantic_role": watch_semantic_role,
                    "is_stablecoin_flow": is_stable,
                    "size_bucket": size_bucket,
                },
            }

        if event.kind in {"token_transfer", "eth_transfer"}:
            return {
                "intent_type": "pure_transfer",
                "intent_confidence": 0.46,
                "information_level": "low",
                "confirmation_score": 0.20,
                "intent_evidence": ["当前仅观察到单笔转移，未看到交易执行"],
                "basis": {
                    "kind": event.kind,
                    "direction": direction,
                    "watch_strategy_role": watch_strategy_role,
                    "counterparty_strategy_role": counterparty_strategy_role,
                    "inferred": inferred,
                    "size_bucket": size_bucket,
                },
            }

        return {
            "intent_type": "unknown_intent",
            "intent_confidence": 0.2,
            "information_level": "low",
            "confirmation_score": 0.1,
            "intent_evidence": ["缺少足够路径信息"],
            "basis": {"kind": event.kind, "direction": direction},
        }
    def _confirm_intent(
        self,
        event: Event,
        parsed: dict,
        watch_context: dict,
        watch_meta: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
        behavior: dict,
        preliminary_intent: dict,
    ) -> dict:
        if self._is_lp_event(event=event, watch_meta=watch_meta, parsed=parsed):
            return self.lp_analyzer.confirm_intent(
                event=event,
                parsed=parsed,
                address_snapshot=address_snapshot,
                pool_snapshot=pool_snapshot,
                token_snapshot=token_snapshot,
                behavior=behavior,
                preliminary_intent=preliminary_intent,
            )

        counterparty_meta = watch_context.get("counterparty_meta") or get_address_meta(
            str(watch_context.get("counterparty") or parsed.get("counterparty") or "").lower()
        )
        watch_strategy_role = str(watch_meta.get("strategy_role") or event.strategy_role or "unknown")
        counterparty_strategy_role = str(counterparty_meta.get("strategy_role") or parsed.get("counterparty_strategy_role") or "unknown")
        exchange_sensitive = self._is_exchange_sensitive_role(watch_strategy_role) or self._is_exchange_sensitive_role(counterparty_strategy_role)
        watch_is_exchange = self._is_exchange_sensitive_role(watch_strategy_role)
        both_exchange = watch_strategy_role.startswith("exchange_") and counterparty_strategy_role.startswith("exchange_")

        recent_window = address_snapshot.get("windows", {}).get("15m", {}).get("recent") or address_snapshot.get("recent") or []
        prior_events = [item for item in recent_window if item.tx_hash != event.tx_hash]
        direction_bucket = self._direction_bucket(event.side)
        same_direction_prior = [item for item in prior_events if self._direction_bucket(item.side) == direction_bucket]
        same_token_prior = [
            item for item in same_direction_prior
            if (item.token or "").lower() == (event.token or "").lower()
        ]
        same_intent_prior = [item for item in same_direction_prior if item.intent_type == event.intent_type]
        prior_prep_count = self._prior_preparation_count(event, prior_events)
        recent_internal_count = sum(
            1 for item in prior_events if item.intent_type in {"internal_rebalance", "market_making_inventory_move"}
        )

        abnormal_ratio = self._abnormal_ratio(address_snapshot, event)
        relative_address_size = self._relative_address_size(address_snapshot, event)
        resonance = self._side_resonance(token_snapshot, event.side)
        resonance_score = float(resonance.get("resonance_score") or 0.0)
        behavior_type = str(behavior.get("behavior_type") or "normal")

        score = self._clamp(0.16 + 0.45 * float(preliminary_intent.get("intent_confidence") or 0.0), 0.0, 1.0)
        evidence = []

        if event.kind == "swap":
            score += 0.12
            evidence.append("当前事件本身是链上真实 swap")

        if len(same_token_prior) >= 1:
            score += 0.12
            evidence.append("同地址 15 分钟内重复出现同 token 同向路径")
        elif len(same_intent_prior) >= 1:
            score += 0.08
            evidence.append("同地址 15 分钟内重复出现同类资金路径")
        elif len(same_direction_prior) >= 2:
            score += 0.06
            evidence.append("同地址 15 分钟内出现连续同向资金流")

        if event.intent_type == "swap_execution" and prior_prep_count >= 1:
            score += 0.16
            evidence.append("前序准备动作已在短时间内转为实际交易执行")
        elif event.intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        } and len(same_direction_prior) >= 1:
            score += 0.10
            evidence.append("交易场景相关路径出现短时重复")

        if int(resonance.get("same_side_high_quality_addresses") or 0) >= 2:
            score += 0.16
            evidence.append("短窗口内多个高质量地址同向触发")
        elif int(resonance.get("same_side_unique_addresses") or 0) >= 3:
            score += 0.10
            evidence.append("短窗口内多地址同向共振")

        if int(resonance.get("same_side_smart_money_addresses") or 0) >= 2:
            score += 0.10
            evidence.append("短窗口内多个聪明钱地址同向")

        if bool(resonance.get("leader_follow")):
            score += 0.06
            evidence.append("存在高质量地址带动其他地址跟随")

        if event.intent_type in {"internal_rebalance", "market_making_inventory_move"} and (
            parsed.get("possible_internal_transfer") or both_exchange
        ):
            score += 0.18
            evidence.append("路径更像同体系地址调拨或库存迁移")

        if event.intent_type == "market_making_inventory_move" and behavior_type == "inventory_management":
            score += 0.12
            evidence.append("做市地址短时出现双向换手，更像库存管理")

        if exchange_sensitive and event.kind != "swap":
            if parsed.get("possible_internal_transfer") or both_exchange:
                if event.intent_type not in {"internal_rebalance", "market_making_inventory_move"}:
                    score -= 0.16
                    evidence.append("存在交易所内部划转特征，削弱外部交易推断")

            if (
                len(same_direction_prior) == 0
                and abnormal_ratio < 1.6
                and relative_address_size < 1.4
                and resonance_score < 0.45
            ):
                score -= 0.18
                evidence.append("单笔交易所路径，缺少连续性和共振确认")

            if recent_internal_count >= 1 and event.intent_type in {
                "exchange_deposit_candidate",
                "exchange_withdraw_candidate",
                "possible_buy_preparation",
                "possible_sell_preparation",
            }:
                score -= 0.08
                evidence.append("同地址近期已出现内部调拨样式")

        if event.pricing_status in {"unknown", "unavailable"}:
            score -= 0.08
            evidence.append("定价可信度较弱")
        elif event.usd_value_estimated:
            score -= 0.03

        if event.intent_type in {"pure_transfer", "unknown_intent"} and resonance_score < 0.4 and not same_intent_prior:
            score -= 0.08

        score = self._clamp(score, 0.0, 1.0)
        if score >= 0.78:
            stage = "confirmed"
        elif score >= 0.52:
            stage = "preliminary"
        else:
            stage = "weak"

        intent_confidence = self._clamp(
            0.55 * float(preliminary_intent.get("intent_confidence") or 0.0) + 0.45 * score,
            0.0,
            1.0,
        )
        if exchange_sensitive and event.kind != "swap" and stage != "confirmed":
            intent_confidence = min(intent_confidence, 0.74)
        if stage == "weak":
            intent_confidence = min(intent_confidence, 0.62)

        information_level = str(preliminary_intent.get("information_level") or "low")
        if stage == "confirmed":
            if event.kind == "swap" or resonance_score >= 0.6 or prior_prep_count >= 1:
                information_level = "high"
            else:
                information_level = "medium"
        elif stage == "weak":
            information_level = "low"
        elif exchange_sensitive and event.kind != "swap":
            information_level = "low"

        if not evidence:
            evidence.append("当前主要来自单跳路径规则，尚缺连续确认")

        sequence_confirmed = (
            len(same_token_prior) >= 2
            or prior_prep_count >= 1
            or resonance_score >= 0.7
        )
        exchange_behavior_supported = (
            sequence_confirmed
            or len(same_direction_prior) >= 1
            or abnormal_ratio >= 1.9
            or relative_address_size >= 1.75
            or resonance_score >= 0.55
        )

        # 交易所相关非 swap 事件默认更保守：
        # 更偏“重点地址行为通知”，只有出现明显短时重复路径才允许提升。
        if watch_is_exchange and event.kind != "swap":
            if event.intent_type not in {"internal_rebalance", "market_making_inventory_move"} and not exchange_behavior_supported:
                stage = "weak"
                score = min(score, 0.48)
                intent_confidence = min(intent_confidence, 0.52)
                information_level = "low"
                evidence.append("监控对象为交易所侧重点地址，当前优先按行为通知保守处理")
            elif stage == "confirmed" and not sequence_confirmed:
                stage = "preliminary"
                intent_confidence = min(intent_confidence, 0.68)
                information_level = "medium" if abnormal_ratio >= 2.2 or resonance_score >= 0.6 else "low"
                evidence.append("交易所侧事件当前具备规模或异常特征，但仍等待连续路径确认")

        if event.intent_type in {
            "exchange_deposit_candidate",
            "exchange_withdraw_candidate",
            "possible_buy_preparation",
            "possible_sell_preparation",
        } and event.kind != "swap":
            if not exchange_behavior_supported:
                if watch_is_exchange:
                    stage = "weak"
                    intent_confidence = min(intent_confidence, 0.52)
                else:
                    if stage == "confirmed":
                        stage = "preliminary"
                    intent_confidence = min(intent_confidence, 0.58)
                information_level = "low"
                evidence.append("尚未看到足够的连续路径确认，当前仅保留为观察性推断")
            elif stage == "confirmed" and not sequence_confirmed:
                stage = "preliminary"
                intent_confidence = min(intent_confidence, 0.66 if watch_is_exchange else 0.62)
                information_level = "medium" if abnormal_ratio >= 2.0 or resonance_score >= 0.55 else "low"
                evidence.append("当前已具备一定规模或异常特征，但仍优先等待连续路径确认")

        unique_evidence = []
        for item in evidence:
            if item not in unique_evidence:
                unique_evidence.append(item)

        return {
            **preliminary_intent,
            "intent_type": event.intent_type,
            "intent_confidence": round(intent_confidence, 3),
            "information_level": information_level,
            "intent_stage": stage,
            "confirmation_score": round(score, 3),
            "intent_evidence": unique_evidence[:4],
            "sequence": {
                "same_direction_prior_count": len(same_direction_prior),
                "same_token_prior_count": len(same_token_prior),
                "same_intent_prior_count": len(same_intent_prior),
                "prior_preparation_count": prior_prep_count,
            },
            "resonance": resonance,
            "exchange_noise_sensitive": exchange_sensitive,
            "abnormal_ratio": round(abnormal_ratio, 3),
            "relative_address_size": round(relative_address_size, 3),
        }

    def _apply_confirmed_intent(self, event: Event, confirmed_intent: dict) -> None:
        event.intent_type = str(confirmed_intent.get("intent_type") or event.intent_type)
        event.intent_confidence = float(confirmed_intent.get("intent_confidence") or event.intent_confidence)
        event.intent_stage = str(confirmed_intent.get("intent_stage") or "preliminary")
        event.confirmation_score = float(confirmed_intent.get("confirmation_score") or 0.0)
        event.intent_evidence = list(confirmed_intent.get("intent_evidence") or [])
        event.metadata["intent"] = confirmed_intent
        if self._is_lp_event(event=event):
            event.metadata["lp_analysis"] = {
                "same_pool_continuity": int(confirmed_intent.get("same_pool_continuity") or 0),
                "multi_pool_resonance": int(confirmed_intent.get("multi_pool_resonance") or 0),
                "reserve_skew": float(confirmed_intent.get("reserve_skew") or 0.0),
                "action_intensity": float(confirmed_intent.get("action_intensity") or 0.0),
                "pool_volume_surge_ratio": float(confirmed_intent.get("pool_volume_surge_ratio") or 0.0),
                "pool_window_trade_count": int(confirmed_intent.get("pool_window_trade_count") or 0),
                "pool_window_usd_total": float(confirmed_intent.get("pool_window_usd_total") or 0.0),
                "abnormal_ratio": float(confirmed_intent.get("abnormal_ratio") or 0.0),
                "market_impact_hint": str(confirmed_intent.get("market_impact_hint") or ""),
            }

    def _apply_liquidation_overlay(
        self,
        event: Event,
        parsed: dict,
        watch_meta: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
    ) -> None:
        detection = self.liquidation_detector.detect(
            event=event,
            parsed=parsed,
            watch_meta=watch_meta,
            address_snapshot=address_snapshot,
            pool_snapshot=pool_snapshot,
            token_snapshot=token_snapshot,
        )
        event.metadata["liquidation"] = detection
        event.metadata["liquidation_score"] = float(detection.get("liquidation_score") or 0.0)
        event.metadata["liquidation_stage"] = str(detection.get("liquidation_stage") or "none")
        event.metadata["liquidation_side"] = str(detection.get("liquidation_side") or "unknown")
        event.metadata["liquidation_reason"] = str(detection.get("liquidation_reason") or "")
        event.metadata["liquidation_evidence"] = list(detection.get("liquidation_evidence") or [])
        event.metadata["liquidation_protocols"] = list(detection.get("liquidation_protocols") or [])
        event.metadata["liquidation_roles"] = list(detection.get("liquidation_roles") or [])
        event.metadata["semantic_overlay"] = str(detection.get("semantic_overlay") or "")
        event.metadata["liquidation_confidence"] = float(detection.get("liquidation_confidence") or 0.0)
        event.metadata["liquidation_primary_candidate"] = bool(detection.get("primary_candidate"))

        if event.metadata["liquidation_stage"] in {"risk", "execution"}:
            for evidence in list(event.metadata.get("liquidation_evidence") or [])[:3]:
                if evidence not in event.intent_evidence:
                    event.intent_evidence.append(evidence)
            if event.metadata["liquidation_stage"] == "risk":
                event.confirmation_score = self._clamp(float(event.confirmation_score or 0.0) + 0.04, 0.0, 0.96)
            elif event.metadata["liquidation_stage"] == "execution":
                event.confirmation_score = self._clamp(float(event.confirmation_score or 0.0) + 0.10, 0.0, 0.98)
                if event.intent_stage == "weak":
                    event.intent_stage = "preliminary"

        intent_meta = dict(event.metadata.get("intent") or {})
        intent_meta["confirmation_score"] = round(float(event.confirmation_score or 0.0), 3)
        intent_meta["intent_evidence"] = list(event.intent_evidence or [])
        intent_meta["liquidation_stage"] = event.metadata["liquidation_stage"]
        intent_meta["liquidation_score"] = event.metadata["liquidation_score"]
        intent_meta["liquidation_side"] = event.metadata["liquidation_side"]
        intent_meta["liquidation_protocols"] = list(event.metadata.get("liquidation_protocols") or [])
        intent_meta["semantic_overlay"] = str(event.metadata.get("semantic_overlay") or "")
        event.metadata["intent"] = intent_meta

    def _prior_preparation_count(self, event: Event, prior_events: list[Event]) -> int:
        if event.side == "买入":
            expected = {"possible_buy_preparation", "exchange_withdraw_candidate"}
        elif event.side == "卖出":
            expected = {"possible_sell_preparation", "exchange_deposit_candidate"}
        elif event.side == "流入":
            expected = {"exchange_withdraw_candidate"}
        elif event.side == "流出":
            expected = {"exchange_deposit_candidate", "possible_buy_preparation", "possible_sell_preparation"}
        else:
            expected = {
                "possible_buy_preparation",
                "possible_sell_preparation",
                "exchange_withdraw_candidate",
                "exchange_deposit_candidate",
            }
        return sum(1 for item in prior_events if item.intent_type in expected)

    def _side_resonance(self, token_snapshot: dict, side: str | None) -> dict:
        bucket = self._direction_bucket(side)
        resonance_5m = token_snapshot.get("resonance_5m") or {}
        resonance_15m = token_snapshot.get("resonance_15m") or {}

        if bucket == "buy":
            same_side_unique_5m = int(resonance_5m.get("buy_unique_addresses") or 0)
            same_side_high_quality_5m = int(resonance_5m.get("buy_high_quality_addresses") or 0)
            same_side_smart_money_5m = int(resonance_5m.get("buy_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("buy_leader_follow"))
        elif bucket == "sell":
            same_side_unique_5m = int(resonance_5m.get("sell_unique_addresses") or 0)
            same_side_high_quality_5m = int(resonance_5m.get("sell_high_quality_addresses") or 0)
            same_side_smart_money_5m = int(resonance_5m.get("sell_smart_money_addresses") or 0)
            leader_follow = bool(resonance_5m.get("sell_leader_follow"))
        else:
            same_side_unique_5m = max(int(resonance_5m.get("buy_unique_addresses") or 0), int(resonance_5m.get("sell_unique_addresses") or 0))
            same_side_high_quality_5m = max(int(resonance_5m.get("buy_high_quality_addresses") or 0), int(resonance_5m.get("sell_high_quality_addresses") or 0))
            same_side_smart_money_5m = max(int(resonance_5m.get("buy_smart_money_addresses") or 0), int(resonance_5m.get("sell_smart_money_addresses") or 0))
            leader_follow = bool(resonance_5m.get("leader_follow_resonance"))

        if bucket == "buy":
            same_side_unique_15m = int(resonance_15m.get("buy_unique_addresses") or 0)
        elif bucket == "sell":
            same_side_unique_15m = int(resonance_15m.get("sell_unique_addresses") or 0)
        else:
            same_side_unique_15m = max(int(resonance_15m.get("buy_unique_addresses") or 0), int(resonance_15m.get("sell_unique_addresses") or 0))

        resonance_score = max(
            float(resonance_5m.get("resonance_score") or 0.0),
            min(1.0, float(resonance_15m.get("resonance_score") or 0.0) * 0.9),
        )
        return {
            "same_side_unique_addresses": same_side_unique_5m,
            "same_side_high_quality_addresses": same_side_high_quality_5m,
            "same_side_smart_money_addresses": same_side_smart_money_5m,
            "same_side_unique_addresses_15m": same_side_unique_15m,
            "leader_follow": leader_follow,
            "multi_address_resonance": same_side_unique_5m >= 2,
            "resonance_score": round(resonance_score, 3),
        }

    def _is_exchange_sensitive_role(self, strategy_role: str) -> bool:
        return str(strategy_role or "unknown").startswith("exchange_")

    def _direction_bucket(self, side: str | None) -> str:
        normalized = str(side or "")
        if normalized in {"买入", "流入"}:
            return "buy"
        if normalized in {"卖出", "流出"}:
            return "sell"
        return "other"

    def _abnormal_ratio(self, address_snapshot: dict, event: Event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 0.0
        return float(event.usd_value or 0.0) / avg_usd

    def _relative_address_size(self, address_snapshot: dict, event: Event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 2.0
        return float(event.usd_value or 0.0) / avg_usd

    def _token_context_score(self, base_token_score: float, gate_metrics: dict) -> float:
        volume_ratio = float(gate_metrics.get("token_volume_ratio") or 0.0)
        price_impact_ratio = float(gate_metrics.get("price_impact_ratio") or 0.0)
        cluster_boost = float(gate_metrics.get("cluster_boost") or 0.0)
        resonance_score = float(gate_metrics.get("resonance_score") or 0.0)

        boost = (
            min(12.0, volume_ratio * 1000.0)
            + min(8.0, price_impact_ratio * 1000.0)
            + (cluster_boost * 20.0)
            + (resonance_score * 10.0)
        )
        score = base_token_score + boost
        return max(0.0, min(100.0, score))

    def _to_event(self, parsed: dict, watch_context: dict | None, watch_meta: dict, pricing: dict) -> Event:
        kind = parsed.get("kind", "unknown")
        side = parsed.get("side") or parsed.get("direction")
        if kind == "swap":
            amount = float(parsed.get("token_amount") or 0.0)
        else:
            amount = float(parsed.get("value") or 0.0)

        token_contract = parsed.get("token_contract")
        token = str(token_contract).lower() if token_contract else None
        watch_address = str(parsed.get("watch_address") or "").lower()
        flow_source_label, flow_target_label = get_flow_endpoints(watch_context) if watch_context else ("", "")

        return Event(
            tx_hash=str(parsed.get("tx_hash") or ""),
            address=watch_address,
            token=token,
            amount=amount,
            side=side,
            usd_value=float(pricing["usd_value"] or 0.0),
            kind=kind,
            ts=int(parsed.get("timestamp") or int(time.time())),
            pricing_status=str(pricing.get("pricing_status") or "unknown"),
            pricing_source=str(pricing.get("pricing_source") or "none"),
            pricing_confidence=float(pricing.get("pricing_confidence") or 0.0),
            usd_value_available=bool(pricing.get("usd_value_available", False)),
            usd_value_estimated=bool(pricing.get("usd_value_estimated", False)),
            semantic_role=str(watch_meta.get("semantic_role") or "unknown"),
            strategy_role=str(watch_meta.get("strategy_role") or "unknown"),
            metadata={
                "token_symbol": parsed.get("token_symbol"),
                "quote_symbol": parsed.get("quote_symbol"),
                "quote_amount": parsed.get("quote_amount"),
                "quote_token_contract": parsed.get("quote_token_contract"),
                "flow_direction": watch_context.get("direction") if watch_context else "",
                "flow_label": watch_context.get("flow_label") if watch_context else "",
                "flow_source_label": flow_source_label,
                "flow_target_label": flow_target_label,
                "watch_address_label": watch_context.get("watch_address_label") if watch_context else "",
                "counterparty_label": watch_context.get("counterparty_label") if watch_context else "",
                "counterparty_role": parsed.get("counterparty_role", "unknown"),
                "counterparty_strategy_role": parsed.get("counterparty_strategy_role", "unknown"),
                "counterparty_semantic_role": parsed.get("counterparty_semantic_role", "unknown"),
                "inferred_context": parsed.get("inferred_context", {}),
                "is_liquidation_protocol_related": bool(parsed.get("is_liquidation_protocol_related")),
                "liquidation_protocols_touched": list(parsed.get("liquidation_protocols_touched") or []),
                "liquidation_roles_touched": list(parsed.get("liquidation_roles_touched") or []),
                "liquidation_related_addresses": list(parsed.get("liquidation_related_addresses") or []),
                "possible_keeper_executor": bool(parsed.get("possible_keeper_executor")),
                "possible_vault_or_auction": bool(parsed.get("possible_vault_or_auction")),
                "possible_lending_protocol": bool(parsed.get("possible_lending_protocol")),
                "ingest_ts": int(parsed.get("ingest_ts") or 0),
                "source_kind": parsed.get("source_kind", ""),
                "touched_watch_addresses": list(parsed.get("touched_watch_addresses") or []),
                "touched_lp_pools": list(parsed.get("touched_lp_pools") or []),
                "participant_addresses": list(parsed.get("participant_addresses") or []),
                "next_hop_addresses": list(parsed.get("next_hop_addresses") or []),
                "raw_log_count": int(parsed.get("raw_log_count") or 0),
                "monitor_type": parsed.get("monitor_type", "watch_address"),
                "tx_pool_hit_count": int(parsed.get("tx_pool_hit_count") or 0),
                "touched_lp_pool_count": int(parsed.get("touched_lp_pool_count") or 0),
                "pool_transfer_count_by_pool": dict(parsed.get("pool_transfer_count_by_pool") or {}),
                "pool_candidate_weight": float(parsed.get("pool_candidate_weight") or 0.0),
                "replay_source": str(parsed.get("replay_source") or ""),
                "pricing": pricing,
                "raw": parsed,
            },
        )

    def _is_lp_event(
        self,
        event: Event | None = None,
        watch_meta: dict | None = None,
        parsed: dict | None = None,
    ) -> bool:
        if watch_meta and str(watch_meta.get("strategy_role") or "") == "lp_pool":
            return True
        if event and str(event.strategy_role or "") == "lp_pool":
            return True
        if parsed and str(parsed.get("monitor_type") or "") == "lp_pool":
            return True
        if event and str(event.intent_type or "") in LP_ALL_INTENTS:
            return True
        return False

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))
