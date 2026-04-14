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


if __name__ == "__main__":
    unittest.main()
