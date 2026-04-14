import unittest

from models import Event, Signal
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
            },
        )

        self.assertEqual(("observe", "market_maker_non_execution_observe"), (delivery_class, delivery_reason))
        # `market_maker_wallet` 归在 smart_money role group，下游消费的 legacy 关停标记是共享 key。
        self.assertTrue(signal.metadata.get("smart_money_legacy_non_exec_branch_disabled"))

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


class RecordingLpStrategyEngine(StrategyEngine):
    def __init__(self) -> None:
        super().__init__()
        self.captured_abnormal_ratios: list[float] = []

    def _allow_lp_prealert_observe(self, *args, **kwargs) -> bool:
        return False

    def _allow_lp_burst_directional_primary(self, *args, **kwargs) -> bool:
        return False

    def _allow_lp_first_hit_directional_primary_direct(self, *args, **kwargs) -> bool:
        self.captured_abnormal_ratios.append(float(kwargs["abnormal_ratio"]))
        return False

    def _allow_lp_first_hit_directional_primary(self, *args, **kwargs) -> bool:
        self.captured_abnormal_ratios.append(float(kwargs["abnormal_ratio"]))
        return False

    def _allow_lp_directional_early_observe(self, *args, **kwargs) -> bool:
        return False


if __name__ == "__main__":
    unittest.main()
