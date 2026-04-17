import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from signal_interpreter import SignalInterpreter
from signal_quality_gate import detect_lp_liquidity_sweep


class LpSweepPhaseMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.interpreter = SignalInterpreter()

    def _event(
        self,
        *,
        intent_type: str = "pool_buy_pressure",
        usd_value: float = 95_000.0,
        same_pool_continuity: int = 2,
        multi_pool_resonance: int = 0,
        pool_volume_surge_ratio: float = 2.0,
        reserve_skew: float = 0.24,
        action_intensity: float = 0.50,
        lp_burst_event_count: int = 4,
        lp_burst_window_sec: int = 15,
        price_impact_ratio: float = 0.006,
        metadata: dict | None = None,
    ) -> Event:
        meta = {
            "token_symbol": "ETH",
            "price_impact_ratio": price_impact_ratio,
            "raw": {
                "lp_context": {
                    "pair_label": "ETH/USDC",
                    "pool_label": "ETH/USDC",
                    "base_token_symbol": "ETH",
                    "quote_token_symbol": "USDC",
                }
            },
            "lp_analysis": {
                "reserve_skew": reserve_skew,
                "action_intensity": action_intensity,
                "same_pool_continuity": same_pool_continuity,
                "multi_pool_resonance": multi_pool_resonance,
                "pool_volume_surge_ratio": pool_volume_surge_ratio,
            },
            "lp_burst": {
                "lp_burst_event_count": lp_burst_event_count,
                "lp_burst_window_sec": lp_burst_window_sec,
            },
            "pool_price_move_before_alert_30s": 0.002,
            "pool_price_move_before_alert_60s": 0.003,
        }
        if metadata:
            meta.update(metadata)
        return Event(
            tx_hash=f"0xsweepmap{intent_type}{same_pool_continuity}{multi_pool_resonance}",
            address="0xsweeppool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type == "pool_buy_pressure" else "卖出",
            usd_value=usd_value,
            kind="swap",
            ts=1_710_000_000,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.90,
            confirmation_score=0.84,
            pricing_status="exact",
            pricing_confidence=0.96,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata=meta,
        )

    def _signal(self, event: Event, *, delivery_class: str = "observe") -> Signal:
        return Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class=delivery_class,
            delivery_reason="unit_test",
        )

    def _gate_metrics(self, event: Event, *, price_impact_ratio: float = 0.006) -> dict:
        lp_analysis = event.metadata.get("lp_analysis") or {}
        lp_burst = event.metadata.get("lp_burst") or {}
        sweep = detect_lp_liquidity_sweep(
            event=event,
            reserve_skew=float(lp_analysis.get("reserve_skew") or 0.0),
            action_intensity=float(lp_analysis.get("action_intensity") or 0.0),
            same_pool_continuity=int(lp_analysis.get("same_pool_continuity") or 0),
            multi_pool_resonance=int(lp_analysis.get("multi_pool_resonance") or 0),
            pool_volume_surge_ratio=float(lp_analysis.get("pool_volume_surge_ratio") or 0.0),
            lp_burst_event_count=int(lp_burst.get("lp_burst_event_count") or 0),
            lp_burst_window_sec=int(lp_burst.get("lp_burst_window_sec") or 0),
            price_impact_ratio=price_impact_ratio,
            min_price_impact_ratio=0.003,
            liquidation_stage=str(event.metadata.get("liquidation_stage") or "none"),
        )
        return {
            "resonance_score": 0.0,
            "price_impact_ratio": price_impact_ratio,
            "lp_reserve_skew": float(lp_analysis.get("reserve_skew") or 0.0),
            "lp_action_intensity": float(lp_analysis.get("action_intensity") or 0.0),
            "lp_same_pool_continuity": int(lp_analysis.get("same_pool_continuity") or 0),
            "lp_multi_pool_resonance": int(lp_analysis.get("multi_pool_resonance") or 0),
            "lp_pool_volume_surge_ratio": float(lp_analysis.get("pool_volume_surge_ratio") or 0.0),
            "lp_burst_event_count": int(lp_burst.get("lp_burst_event_count") or 0),
            "lp_burst_window_sec": int(lp_burst.get("lp_burst_window_sec") or 0),
            "lp_semantic_subtype": str(sweep.get("semantic_subtype") or ""),
            "semantic_subtype": str(sweep.get("semantic_subtype") or ""),
            "lp_sweep_detected": bool(sweep.get("detected")),
            "lp_sweep_confidence": str(sweep.get("sweep_confidence") or ""),
            "lp_sweep_phase": str(sweep.get("lp_sweep_phase") or ""),
            "lp_sweep_followthrough_score": float(sweep.get("lp_sweep_followthrough_score") or 0.0),
            "lp_sweep_exhaustion_score": float(sweep.get("lp_sweep_exhaustion_score") or 0.0),
            "lp_sweep_continuation_score": float(sweep.get("lp_sweep_continuation_score") or 0.0),
            "lp_impact_to_size_ratio": float(sweep.get("lp_impact_to_size_ratio") or 0.0),
            "lp_sweep_min_price_impact_ratio": 0.003,
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
        }

    def _interpret(self, event: Event, signal: Signal, gate_metrics: dict) -> dict:
        self.interpreter.interpret(
            event,
            signal,
            {"behavior_type": event.intent_type, "confidence": 0.9, "reason": "unit_test"},
            {"strategy_role": "lp_pool", "pair_label": "ETH/USDC"},
            {},
            {"recent": []},
            {},
            gate_metrics=gate_metrics,
        )
        signal.context["message_template"] = "brief"
        signal.metadata["message_template"] = "brief"
        return signal.context

    def test_sweep_building_brief_line_never_uses_climax(self) -> None:
        event = self._event(same_pool_continuity=2, multi_pool_resonance=0, pool_volume_surge_ratio=2.0, action_intensity=0.50)
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event))

        message = format_signal_message(signal, event)
        first_line = message.strip().splitlines()[0]

        self.assertEqual("sweep_building", context.get("lp_sweep_phase"))
        self.assertIn(context.get("lp_alert_stage"), {"prealert", "confirm"})
        self.assertNotIn("高潮", first_line)
        self.assertTrue(any(keyword in first_line for keyword in ("建立中", "待确认", "冲击建立中")))

    def test_stronger_sweep_building_can_use_confirm_without_climax(self) -> None:
        event = self._event(
            same_pool_continuity=3,
            multi_pool_resonance=1,
            pool_volume_surge_ratio=2.0,
            action_intensity=0.50,
        )
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event))

        message = format_signal_message(signal, event)
        first_line = message.strip().splitlines()[0]

        self.assertEqual("sweep_building", context.get("lp_sweep_phase"))
        self.assertEqual("confirm", context.get("lp_alert_stage"))
        self.assertTrue(first_line.startswith("确认｜"))
        self.assertIn("待确认", first_line)
        self.assertNotIn("高潮", first_line)

    def test_sweep_confirmed_uses_confirmation_semantics(self) -> None:
        event = self._event(same_pool_continuity=2, multi_pool_resonance=2, pool_volume_surge_ratio=2.3, action_intensity=0.55)
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event))

        message = format_signal_message(signal, event)
        first_line = message.strip().splitlines()[0]

        self.assertEqual("sweep_confirmed", context.get("lp_sweep_phase"))
        self.assertIn(context.get("lp_alert_stage"), {"confirm", "climax"})
        self.assertIn("清扫确认", first_line)
        self.assertNotIn("建立中", first_line)

    def test_sweep_exhaustion_risk_uses_risk_semantics(self) -> None:
        event = self._event(
            usd_value=12_000.0,
            same_pool_continuity=1,
            multi_pool_resonance=0,
            pool_volume_surge_ratio=3.8,
            action_intensity=0.50,
            price_impact_ratio=0.028,
        )
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.028))

        message = format_signal_message(signal, event)
        first_line = message.strip().splitlines()[0]

        self.assertEqual("sweep_exhaustion_risk", context.get("lp_sweep_phase"))
        self.assertEqual("exhaustion_risk", context.get("lp_alert_stage"))
        self.assertTrue(first_line.startswith("风险｜"))
        self.assertIn("风险高", first_line)

    def test_liquidation_execution_still_overrides_sweep(self) -> None:
        event = self._event(
            intent_type="pool_sell_pressure",
            metadata={
                "liquidation_stage": "execution",
                "liquidation_score": 0.91,
                "liquidation_side": "long_flush",
                "liquidation_protocols": ["Aave"],
            },
        )
        signal = self._signal(event, delivery_class="primary")
        context = self._interpret(event, signal, self._gate_metrics(event))

        message = format_signal_message(signal, event)
        first_line = message.strip().splitlines()[0]

        self.assertEqual("", context.get("semantic_subtype"))
        self.assertTrue(first_line.startswith("风险｜") or first_line.startswith("确认｜") or "疑似清算执行" in first_line)
        self.assertEqual("liquidation_execution", context.get("message_variant"))


if __name__ == "__main__":
    unittest.main()
