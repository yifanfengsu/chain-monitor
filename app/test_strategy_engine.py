import sys
import unittest

from filter import strategy_role_group
from models import Event, Signal
from processor import _is_inventory_style_internal
from signal_interpreter import SignalInterpreter
from strategy_engine import StrategyEngine


class StrategyEngineClassifyDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = StrategyEngine()

    def _event(
        self,
        *,
        strategy_role: str,
        intent_type: str,
        kind: str = "transfer",
        usd_value: float = 150_000.0,
        confirmation_score: float = 0.2,
        pricing_confidence: float = 0.9,
        metadata: dict | None = None,
    ) -> Event:
        return Event(
            tx_hash="0xtx",
            address="0xabc",
            token="PEPE",
            amount=1.0,
            side="买入",
            usd_value=usd_value,
            kind=kind,
            ts=1,
            intent_type=intent_type,
            confirmation_score=confirmation_score,
            pricing_confidence=pricing_confidence,
            strategy_role=strategy_role,
            metadata=metadata or {},
        )

    def _signal(
        self,
        *,
        intent_type: str,
        quality_score: float,
        abnormal_ratio: float = 0.0,
        metadata: dict | None = None,
    ) -> Signal:
        return Signal(
            type="watch",
            confidence=0.8,
            priority=1,
            tier="high",
            address="0xabc",
            token="PEPE",
            tx_hash="0xtx",
            usd_value=150_000.0,
            reason="unit_test",
            quality_score=quality_score,
            intent_type=intent_type,
            abnormal_ratio=abnormal_ratio,
            effective_threshold_usd=100_000.0,
            metadata=metadata or {},
        )

    def _decide_lp(
        self,
        gate_metrics: dict | None = None,
        *,
        event_metadata: dict | None = None,
    ) -> tuple[Event, Signal]:
        engine = StrategyEngine(
            min_confidence=0.0,
            min_address_score=0.0,
            min_token_score=0.0,
            min_behavior_confidence=0.0,
            require_non_normal_behavior=False,
        )
        event = Event(
            tx_hash="0xdecide",
            address="0xlp",
            token="PEPE",
            amount=1.0,
            side="买入",
            usd_value=150_000.0,
            kind="swap",
            ts=1,
            intent_type="pool_buy_pressure",
            intent_stage="confirmed",
            confirmation_score=0.9,
            pricing_status="priced",
            pricing_confidence=0.9,
            strategy_role="lp_pool",
            metadata=event_metadata or {},
        )
        signal = engine.decide(
            event,
            {"strategy_role": "lp_pool"},
            {"behavior_type": "aggressive", "confidence": 1.0, "reason": "unit_test"},
            {"score": 1.0, "grade": "A"},
            {"score": 1.0, "grade": "A"},
            gate_metrics={
                "cooldown_key": "lp:test",
                "quality_score": 0.9,
                "adjusted_quality_score": 0.9,
                "resonance_score": 0.9,
                "dynamic_min_usd": 0.0,
                **(gate_metrics or {}),
            },
        )
        self.assertIsNotNone(signal)
        return event, signal

    def _capture_classify_locals(
        self,
        engine: StrategyEngine,
        event: Event,
        signal: Signal,
        watch_meta: dict,
        gate_metrics: dict | None = None,
        behavior_case=None,
    ) -> dict:
        captured: dict = {}

        def tracer(frame, event_name, arg):
            if (
                event_name == "return"
                and frame.f_code.co_name == "classify_delivery"
                and frame.f_globals.get("__name__") == "strategy_engine"
            ):
                captured.update(frame.f_locals)
            return tracer

        previous = sys.gettrace()
        sys.settrace(tracer)
        try:
            engine.classify_delivery(
                event,
                signal,
                watch_meta,
                gate_metrics=gate_metrics,
                behavior_case=behavior_case,
            )
        finally:
            sys.settrace(previous)
        return captured

    def test_classify_delivery_missing_observe_exception_keys_does_not_raise(self) -> None:
        event = self._event(
            strategy_role="smart_money_wallet",
            intent_type="pure_transfer",
        )
        signal = self._signal(
            intent_type="pure_transfer",
            quality_score=0.1,
        )

        delivery_class, delivery_reason = self.engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "smart_money_wallet"},
            gate_metrics={},
        )

        self.assertEqual(("drop", "smart_money_non_execution_archived"), (delivery_class, delivery_reason))

    def test_strategy_role_group_market_maker_is_independent(self) -> None:
        self.assertEqual("market_maker", strategy_role_group("market_maker_wallet"))

    def test_classify_delivery_smart_money_non_exec_exception_continues_to_observe(self) -> None:
        event = self._event(
            strategy_role="smart_money_wallet",
            intent_type="pure_transfer",
            confirmation_score=0.1,
        )
        signal = self._signal(
            intent_type="pure_transfer",
            quality_score=max(self.engine.smart_money_high_value_observe_min_quality, 0.7),
        )

        delivery_class, delivery_reason = self.engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "smart_money_wallet"},
            gate_metrics={
                "smart_money_non_exec_exception_applied": True,
                "smart_money_non_exec_exception_reason": "unit_test_exception",
            },
        )

        self.assertEqual(("observe", "smart_money_non_execution_observe"), (delivery_class, delivery_reason))
        self.assertTrue(signal.metadata.get("smart_money_legacy_non_exec_branch_disabled"))
        self.assertFalse(signal.metadata.get("market_maker_legacy_inventory_branch_disabled"))

    def test_classify_delivery_market_maker_observe_exception_continues_to_observe(self) -> None:
        event = self._event(
            strategy_role="market_maker_wallet",
            intent_type="market_making_inventory_move",
            confirmation_score=0.1,
        )
        signal = self._signal(
            intent_type="market_making_inventory_move",
            quality_score=max(self.engine.smart_money_high_value_observe_min_quality, 0.7),
        )

        delivery_class, delivery_reason = self.engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "market_maker_wallet"},
            gate_metrics={
                "market_maker_observe_exception_applied": True,
                "market_maker_observe_exception_reason": "unit_test_mm_exception",
                "market_maker_context_confirmed": True,
            },
        )

        self.assertEqual(("observe", "market_maker_non_execution_observe"), (delivery_class, delivery_reason))
        self.assertEqual("market_maker", signal.metadata.get("role_group"))
        self.assertTrue(signal.metadata.get("market_maker_legacy_inventory_branch_disabled"))
        self.assertFalse(signal.metadata.get("smart_money_legacy_non_exec_branch_disabled"))

    def test_legacy_lp_directional_keys_are_canonicalized_to_pool_semantics(self) -> None:
        interpreter = SignalInterpreter()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="LP_Buy_Pressure",
            kind="swap",
            confirmation_score=0.8,
            metadata={"raw": {}},
        )
        signal = self._signal(
            intent_type="LP_Buy_Pressure",
            quality_score=0.8,
        )
        signal.type = "LP_Buy_Pressure"
        signal.intent_type = "LP_Buy_Pressure"

        interpreter.interpret(
            event,
            signal,
            {"behavior_type": "pool_buy_pressure", "confidence": 0.8, "reason": "legacy_lp_key"},
            {"strategy_role": "lp_pool"},
            {},
            {"recent": []},
            {},
            gate_metrics={},
        )

        self.assertEqual("pool_buy_pressure", event.intent_type)
        self.assertEqual("pool_buy_pressure", signal.type)
        self.assertEqual("pool_buy_pressure", signal.intent_type)
        self.assertEqual("Pool_Buy_Pressure", signal.context.get("signal_type_label"))

    def test_inventory_style_internal_does_not_use_same_market_maker_role_alone(self) -> None:
        self.assertFalse(
            _is_inventory_style_internal(
                {
                    "address": "0xaaa",
                    "is_watch_address": False,
                    "strategy_role": "market_maker_wallet",
                },
                {
                    "address": "0xbbb",
                    "is_watch_address": False,
                    "strategy_role": "market_maker_wallet",
                },
            )
        )

    def test_inventory_style_internal_still_accepts_both_watch_addresses(self) -> None:
        self.assertTrue(
            _is_inventory_style_internal(
                {
                    "address": "0xaaa",
                    "is_watch_address": True,
                    "strategy_role": "market_maker_wallet",
                },
                {
                    "address": "0xbbb",
                    "is_watch_address": True,
                    "strategy_role": "market_maker_wallet",
                },
            )
        )

    def test_unrelated_exchange_branch_behavior_is_unchanged(self) -> None:
        event = self._event(
            strategy_role="exchange_hot_wallet",
            intent_type="pure_transfer",
            usd_value=50_000.0,
        )
        signal = self._signal(
            intent_type="pure_transfer",
            quality_score=0.2,
        )

        delivery_class, delivery_reason = self.engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "exchange_hot_wallet"},
            gate_metrics={},
        )

        self.assertEqual(("drop", "exchange_transfer_drop"), (delivery_class, delivery_reason))

    def test_abnormal_ratio_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
            abnormal_ratio=9.0,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"abnormal_ratio": 0.0},
        )

        self.assertEqual([0.0, 0.0], engine.captured_abnormal_ratios)

    def test_abnormal_ratio_falls_back_to_signal_only_when_gate_metrics_missing(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
            abnormal_ratio=7.5,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={},
        )

        self.assertEqual([7.5, 7.5], engine.captured_abnormal_ratios)

    def test_lp_action_intensity_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={"lp_analysis": {"action_intensity": 8.0}},
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_action_intensity": 0.0},
        )

        self.assertEqual([0.0, 0.0], engine.captured_lp_action_intensity)

    def test_lp_pool_volume_surge_ratio_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={"lp_analysis": {"pool_volume_surge_ratio": 8.0}},
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_pool_volume_surge_ratio": 0.0},
        )

        self.assertEqual([0.0, 0.0], engine.captured_lp_volume_surge_ratio)

    def test_lp_same_pool_continuity_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={"lp_analysis": {"same_pool_continuity": 6}},
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_same_pool_continuity": 0},
        )

        self.assertEqual([0, 0], engine.captured_lp_same_pool_continuity)

    def test_lp_multi_pool_resonance_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={"lp_analysis": {"multi_pool_resonance": 7}},
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_multi_pool_resonance": 0},
        )

        self.assertEqual([0.0, 0.0], engine.captured_route_continuation_scores[-2:])

    def test_lp_reserve_skew_keeps_gate_metrics_zero_without_falling_back(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={"lp_analysis": {"reserve_skew": 6.0}},
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_reserve_skew": 0.0},
        )

        self.assertEqual([0.0, 0.0], engine.captured_lp_reserve_skew)

    def test_lp_observe_exception_applied_parses_false_strings(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_observe_exception_applied": "False"},
        )
        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_observe_exception_applied": "0"},
        )

        self.assertEqual([False, False], engine.captured_lp_observe_exception_applied[-2:])

    def test_lp_observe_exception_applied_parses_true_strings(self) -> None:
        engine = RecordingLpStrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
        )

        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_observe_exception_applied": "True"},
        )
        engine.classify_delivery(
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={"lp_observe_exception_applied": "1"},
        )

        self.assertEqual([True, True], engine.captured_lp_observe_exception_applied[-2:])

    def test_lp_prealert_applied_parses_false_strings(self) -> None:
        engine = RecordingLpStrategyEngine()
        for raw_value in ("False", "0"):
            event = self._event(
                strategy_role="lp_pool",
                intent_type="pool_buy_pressure",
                usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
                pricing_confidence=0.9,
            )
            signal = self._signal(
                intent_type="pool_buy_pressure",
                quality_score=0.1,
            )

            engine.classify_delivery(
                event,
                signal,
                {"strategy_role": "lp_pool"},
                gate_metrics={"lp_prealert_applied": raw_value},
            )

        self.assertEqual([False, False], engine.captured_lp_prealert_applied[-2:])

    def test_lp_prealert_applied_parses_true_strings(self) -> None:
        engine = RecordingLpStrategyEngine()
        for raw_value in ("True", "1"):
            event = self._event(
                strategy_role="lp_pool",
                intent_type="pool_buy_pressure",
                usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
                pricing_confidence=0.9,
            )
            signal = self._signal(
                intent_type="pool_buy_pressure",
                quality_score=0.1,
            )

            engine.classify_delivery(
                event,
                signal,
                {"strategy_role": "lp_pool"},
                gate_metrics={"lp_prealert_applied": raw_value},
            )

        self.assertEqual([True, True], engine.captured_lp_prealert_applied[-2:])

    def test_observe_exception_flags_preserve_false_values(self) -> None:
        parsed = self.engine._observe_exception_flags(
            {
                "smart_money_non_exec_exception_applied": False,
                "market_maker_observe_exception_applied": False,
            }
        )

        self.assertEqual((False, "", False, ""), parsed)

    def test_observe_exception_flags_parse_false_strings(self) -> None:
        parsed_false = self.engine._observe_exception_flags(
            {
                "smart_money_non_exec_exception_applied": "False",
                "market_maker_observe_exception_applied": "0",
            }
        )

        self.assertEqual((False, "", False, ""), parsed_false)

    def test_observe_exception_flags_parse_true_strings(self) -> None:
        parsed_true = self.engine._observe_exception_flags(
            {
                "smart_money_non_exec_exception_applied": "True",
                "market_maker_observe_exception_applied": "1",
            }
        )

        self.assertEqual((True, "", True, ""), parsed_true)

    def test_decide_lp_observe_exception_applied_parses_false_strings(self) -> None:
        for raw_value in ("False", "0"):
            _, signal = self._decide_lp(
                {
                    "lp_observe_exception_applied": raw_value,
                }
            )
            self.assertFalse(signal.metadata["lp_observe_exception_applied"])

    def test_decide_lp_observe_exception_applied_parses_true_strings(self) -> None:
        for raw_value in ("True", "1"):
            _, signal = self._decide_lp(
                {
                    "lp_observe_exception_applied": raw_value,
                }
            )
            self.assertTrue(signal.metadata["lp_observe_exception_applied"])

    def test_decide_lp_prealert_candidate_parses_false_strings(self) -> None:
        for raw_value in ("False", "0"):
            _, signal = self._decide_lp(
                {
                    "lp_prealert_candidate": raw_value,
                }
            )
            self.assertFalse(signal.metadata["lp_prealert_candidate"])

    def test_decide_lp_prealert_applied_parses_false_strings_and_metadata_writeback(self) -> None:
        for raw_value in ("False", "0"):
            event, signal = self._decide_lp(
                {
                    "lp_prealert_applied": raw_value,
                }
            )
            self.assertFalse(signal.metadata["lp_prealert_applied"])
            self.assertFalse(event.metadata["lp_prealert_applied"])

    def test_decide_lp_burst_zero_values_are_preserved(self) -> None:
        _, signal = self._decide_lp(
            {
                "lp_burst_event_count": 0,
                "lp_burst_total_usd": 0.0,
                "lp_burst_max_single_usd": 0.0,
                "lp_burst_same_pool_continuity": 0,
                "lp_burst_volume_surge_ratio": 0.0,
                "lp_burst_action_intensity": 0.0,
                "lp_burst_reserve_skew": 0.0,
            }
        )

        self.assertEqual(0, signal.metadata["lp_burst_event_count"])
        self.assertEqual(0.0, signal.metadata["lp_burst_total_usd"])
        self.assertEqual(0.0, signal.metadata["lp_burst_max_single_usd"])
        self.assertEqual(0, signal.metadata["lp_burst_same_pool_continuity"])
        self.assertEqual(0.0, signal.metadata["lp_burst_volume_surge_ratio"])
        self.assertEqual(0.0, signal.metadata["lp_burst_action_intensity"])
        self.assertEqual(0.0, signal.metadata["lp_burst_reserve_skew"])

    def test_decide_lp_remaining_boolean_fields_parse_false_strings(self) -> None:
        for raw_value in ("False", "0"):
            _, signal = self._decide_lp(
                {
                    "lp_fast_exception_applied": raw_value,
                    "lp_burst_fastlane_ready": raw_value,
                    "lp_trend_sensitivity_mode": raw_value,
                    "lp_trend_primary_pool": raw_value,
                    "lp_fast_exception_structure_passed": raw_value,
                    "lp_burst_trend_mode": raw_value,
                }
            )
            self.assertFalse(signal.metadata["lp_fast_exception_applied"])
            self.assertFalse(signal.metadata["lp_burst_fastlane_ready"])
            self.assertFalse(signal.metadata["lp_trend_sensitivity_mode"])
            self.assertFalse(signal.metadata["lp_trend_primary_pool"])
            self.assertFalse(signal.metadata["lp_fast_exception_structure_passed"])
            self.assertFalse(signal.metadata["lp_burst_trend_mode"])

    def test_decide_lp_remaining_boolean_fields_parse_true_strings(self) -> None:
        for raw_value in ("True", "1"):
            _, signal = self._decide_lp(
                {
                    "lp_fast_exception_applied": raw_value,
                    "lp_burst_fastlane_ready": raw_value,
                    "lp_trend_sensitivity_mode": raw_value,
                    "lp_trend_primary_pool": raw_value,
                    "lp_fast_exception_structure_passed": raw_value,
                    "lp_burst_trend_mode": raw_value,
                }
            )
            self.assertTrue(signal.metadata["lp_fast_exception_applied"])
            self.assertTrue(signal.metadata["lp_burst_fastlane_ready"])
            self.assertTrue(signal.metadata["lp_trend_sensitivity_mode"])
            self.assertTrue(signal.metadata["lp_trend_primary_pool"])
            self.assertTrue(signal.metadata["lp_fast_exception_structure_passed"])
            self.assertTrue(signal.metadata["lp_burst_trend_mode"])

    def test_classify_delivery_lp_burst_zero_values_are_preserved(self) -> None:
        engine = StrategyEngine()
        event = self._event(
            strategy_role="lp_pool",
            intent_type="pool_buy_pressure",
            usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
            pricing_confidence=0.9,
            metadata={
                "lp_burst_fastlane_ready": True,
                "lp_burst_event_count": 8,
                "lp_burst_total_usd": 9000.0,
                "lp_burst_max_single_usd": 7000.0,
                "lp_burst_same_pool_continuity": 5,
                "lp_burst_volume_surge_ratio": 3.2,
                "lp_burst_action_intensity": 2.4,
                "lp_burst_reserve_skew": 1.8,
            },
        )
        signal = self._signal(
            intent_type="pool_buy_pressure",
            quality_score=0.1,
            metadata={
                "lp_burst_fastlane_ready": True,
                "lp_burst_event_count": 7,
                "lp_burst_total_usd": 8000.0,
                "lp_burst_max_single_usd": 6000.0,
                "lp_burst_same_pool_continuity": 4,
                "lp_burst_volume_surge_ratio": 2.2,
                "lp_burst_action_intensity": 1.4,
                "lp_burst_reserve_skew": 0.8,
            },
        )

        captured = self._capture_classify_locals(
            engine,
            event,
            signal,
            {"strategy_role": "lp_pool"},
            gate_metrics={
                "lp_burst_fastlane_ready": True,
                "lp_burst_event_count": 0,
                "lp_burst_total_usd": 0.0,
                "lp_burst_max_single_usd": 0.0,
                "lp_burst_same_pool_continuity": 0,
                "lp_burst_volume_surge_ratio": 0.0,
                "lp_burst_action_intensity": 0.0,
                "lp_burst_reserve_skew": 0.0,
            },
        )

        self.assertEqual(0, captured["lp_burst_event_count"])
        self.assertEqual(0.0, captured["lp_burst_total_usd"])
        self.assertEqual(0.0, captured["lp_burst_max_single_usd"])
        self.assertEqual(0, captured["lp_burst_same_pool_continuity"])
        self.assertEqual(0.0, captured["lp_burst_volume_surge_ratio"])
        self.assertEqual(0.0, captured["lp_burst_action_intensity"])
        self.assertEqual(0.0, captured["lp_burst_reserve_skew"])

    def test_classify_delivery_lp_burst_fastlane_ready_false_strings_do_not_override_gate_priority(self) -> None:
        engine = StrategyEngine()
        for raw_value in ("False", "0"):
            event = self._event(
                strategy_role="lp_pool",
                intent_type="pool_buy_pressure",
                usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
                pricing_confidence=0.9,
                metadata={"lp_burst_fastlane_ready": True},
            )
            signal = self._signal(
                intent_type="pool_buy_pressure",
                quality_score=0.1,
                metadata={"lp_burst_fastlane_ready": True},
            )

            captured = self._capture_classify_locals(
                engine,
                event,
                signal,
                {"strategy_role": "lp_pool"},
                gate_metrics={"lp_burst_fastlane_ready": raw_value},
            )
            self.assertFalse(captured["lp_burst_fastlane_ready"])

    def test_classify_delivery_lp_trend_primary_pool_false_strings_keep_gate_priority(self) -> None:
        engine = StrategyEngine()
        false_cases = [
            ("False", True, True),
            ("0", False, False),
        ]
        for raw_value, signal_value, event_value in false_cases:
            event = self._event(
                strategy_role="lp_pool",
                intent_type="pool_buy_pressure",
                usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
                pricing_confidence=0.9,
                metadata={"lp_trend_primary_pool": event_value},
            )
            signal = self._signal(
                intent_type="pool_buy_pressure",
                quality_score=0.1,
                metadata={"lp_trend_primary_pool": signal_value},
            )

            captured = self._capture_classify_locals(
                engine,
                event,
                signal,
                {"strategy_role": "lp_pool"},
                gate_metrics={"lp_trend_primary_pool": raw_value},
            )
            self.assertFalse(captured["lp_trend_primary_pool"])

    def test_classify_delivery_lp_trend_primary_pool_true_strings_parse_true(self) -> None:
        engine = StrategyEngine()
        for raw_value in ("True", "1"):
            event = self._event(
                strategy_role="lp_pool",
                intent_type="pool_buy_pressure",
                usd_value=max(engine.lp_notify_hard_min_usd + 1.0, 100_000.0),
                pricing_confidence=0.9,
            )
            signal = self._signal(
                intent_type="pool_buy_pressure",
                quality_score=0.1,
            )

            captured = self._capture_classify_locals(
                engine,
                event,
                signal,
                {"strategy_role": "lp_pool"},
                gate_metrics={"lp_trend_primary_pool": raw_value},
            )
            self.assertTrue(captured["lp_trend_primary_pool"])


class RecordingLpStrategyEngine(StrategyEngine):
    def __init__(self) -> None:
        super().__init__()
        self.captured_abnormal_ratios: list[float] = []
        self.captured_lp_volume_surge_ratio: list[float] = []
        self.captured_lp_same_pool_continuity: list[int] = []
        self.captured_lp_action_intensity: list[float] = []
        self.captured_lp_reserve_skew: list[float] = []
        self.captured_lp_observe_exception_applied: list[bool] = []
        self.captured_lp_prealert_applied: list[bool] = []
        self.captured_route_continuation_scores: list[float] = []

    def _allow_lp_prealert_observe(self, *args, **kwargs) -> bool:
        self.captured_lp_prealert_applied.append(bool(kwargs["lp_prealert_applied"]))
        return False

    def _allow_lp_burst_directional_primary(self, *args, **kwargs) -> bool:
        return False

    def _allow_lp_first_hit_directional_primary_direct(self, *args, **kwargs) -> bool:
        self.captured_abnormal_ratios.append(float(kwargs["abnormal_ratio"]))
        self.captured_lp_volume_surge_ratio.append(float(kwargs["lp_volume_surge_ratio"]))
        self.captured_lp_same_pool_continuity.append(int(kwargs["lp_same_pool_continuity"]))
        self.captured_lp_action_intensity.append(float(kwargs["lp_action_intensity"]))
        self.captured_lp_reserve_skew.append(float(kwargs["lp_reserve_skew"]))
        return False

    def _allow_lp_first_hit_directional_primary(self, *args, **kwargs) -> bool:
        self.captured_abnormal_ratios.append(float(kwargs["abnormal_ratio"]))
        self.captured_lp_volume_surge_ratio.append(float(kwargs["lp_volume_surge_ratio"]))
        self.captured_lp_same_pool_continuity.append(int(kwargs["lp_same_pool_continuity"]))
        self.captured_lp_action_intensity.append(float(kwargs["lp_action_intensity"]))
        self.captured_lp_reserve_skew.append(float(kwargs["lp_reserve_skew"]))
        self.captured_lp_observe_exception_applied.append(bool(kwargs["lp_observe_exception_applied"]))
        return False

    def _allow_lp_directional_early_observe(self, *args, **kwargs) -> bool:
        return False

    def _role_stage_route(self, **kwargs):
        self.captured_route_continuation_scores.append(float(kwargs["continuation_score"]))
        return super()._role_stage_route(**kwargs)


if __name__ == "__main__":
    unittest.main()
