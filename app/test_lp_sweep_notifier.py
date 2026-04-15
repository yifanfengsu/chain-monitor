import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from signal_interpreter import SignalInterpreter
from signal_quality_gate import detect_lp_liquidity_sweep


class LpSweepNotifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.interpreter = SignalInterpreter()

    def _event(
        self,
        *,
        intent_type: str = "pool_buy_pressure",
        kind: str = "swap",
        usd_value: float = 120_000.0,
        same_pool_continuity: int = 2,
        multi_pool_resonance: int = 2,
        pool_volume_surge_ratio: float = 2.3,
        reserve_skew: float = 0.24,
        action_intensity: float = 0.55,
        lp_burst_event_count: int = 4,
        lp_burst_window_sec: int = 15,
        metadata: dict | None = None,
    ) -> Event:
        meta = {
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
        }
        if metadata:
            meta.update(metadata)
        return Event(
            tx_hash="0xtesthash1234567890",
            address="0xpool",
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=usd_value,
            kind=kind,
            ts=1_710_000_000,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.92,
            confirmation_score=0.88,
            pricing_status="exact",
            pricing_confidence=0.96,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata=meta,
        )

    def _signal(self, event: Event, *, delivery_class: str = "observe") -> Signal:
        return Signal(
            type=event.intent_type,
            confidence=0.9,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.9,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            information_level="high",
            abnormal_ratio=2.2,
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
            "price_impact_ratio": price_impact_ratio,
            "lp_reserve_skew": float(lp_analysis.get("reserve_skew") or 0.0),
            "lp_action_intensity": float(lp_analysis.get("action_intensity") or 0.0),
            "lp_same_pool_continuity": int(lp_analysis.get("same_pool_continuity") or 0),
            "lp_multi_pool_resonance": int(lp_analysis.get("multi_pool_resonance") or 0),
            "lp_pool_volume_surge_ratio": float(lp_analysis.get("pool_volume_surge_ratio") or 0.0),
            "lp_burst_event_count": int(lp_burst.get("lp_burst_event_count") or 0),
            "lp_burst_window_sec": int(lp_burst.get("lp_burst_window_sec") or 0),
            "lp_trend_primary_pool": bool(event.metadata.get("lp_trend_primary_pool")),
            "liquidation_stage": str(event.metadata.get("liquidation_stage") or "none"),
            "liquidation_score": float(event.metadata.get("liquidation_score") or 0.0),
            "resonance_score": 0.0,
            "lp_semantic_subtype": str(sweep.get("semantic_subtype") or ""),
            "semantic_subtype": str(sweep.get("semantic_subtype") or ""),
            "lp_sweep_detected": bool(sweep.get("detected")),
            "lp_sweep_confidence": str(sweep.get("sweep_confidence") or ""),
            "lp_sweep_display_label": str(sweep.get("display_label") or ""),
            "lp_sweep_min_price_impact_ratio": 0.003,
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
        return signal.context

    def test_buy_side_sweep(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.006))

        self.assertEqual("buy_side_liquidity_sweep", context.get("semantic_subtype"))
        self.assertEqual("likely", context.get("sweep_confidence"))
        self.assertEqual("买方清扫", context.get("market_state_label"))
        self.assertEqual("买方清扫", context.get("headline_label"))
        self.assertIn("推断型", context.get("lp_meaning_brief", ""))
        self.assertEqual("pool_buy_pressure", event.intent_type)

    def test_sell_side_sweep(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.007))

        self.assertEqual("sell_side_liquidity_sweep", context.get("semantic_subtype"))
        self.assertEqual("卖方清扫", context.get("market_state_label"))
        self.assertIn("主动卖盘", context.get("lp_meaning_brief", ""))

    def test_regular_buy_pressure_is_not_sweep(self) -> None:
        event = self._event(
            intent_type="pool_buy_pressure",
            reserve_skew=0.10,
            action_intensity=0.30,
            same_pool_continuity=0,
            multi_pool_resonance=0,
            pool_volume_surge_ratio=1.2,
            lp_burst_event_count=1,
            lp_burst_window_sec=90,
        )
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.001))

        self.assertEqual("", context.get("semantic_subtype"))
        self.assertEqual("持续买压", context.get("market_state_label"))
        self.assertFalse(context.get("lp_sweep_detected"))

    def test_liquidity_removal_stays_distinct_from_sweep(self) -> None:
        event = self._event(
            intent_type="liquidity_removal",
            kind="token_transfer",
            same_pool_continuity=1,
            multi_pool_resonance=0,
            pool_volume_surge_ratio=1.4,
            metadata={"lp_trend_primary_pool": True},
        )
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.0))

        self.assertEqual("", context.get("semantic_subtype"))
        self.assertEqual("流动性抽离", context.get("market_state_label"))
        self.assertIn("深度下降", context.get("evidence_brief", ""))

    def test_liquidation_execution_overrides_sweep(self) -> None:
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
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.008))

        self.assertEqual("", context.get("semantic_subtype"))
        self.assertEqual("疑似清算执行", context.get("market_state_label"))
        self.assertEqual("liquidation_execution", context.get("message_variant"))
        self.assertIn("Aave 命中", context.get("evidence_brief", ""))

    def test_brief_template_outputs_two_main_lines(self) -> None:
        event = self._event(intent_type="pool_buy_pressure")
        signal = self._signal(event, delivery_class="observe")
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.006))
        signal.context["message_template"] = "brief"

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertEqual(2, len(lines))
        self.assertEqual("ETH/USDC｜买方清扫｜$120,000", lines[0])
        self.assertEqual("15s 4笔｜同池连续3｜跨池2｜放量2.3x", lines[1])
        self.assertEqual("买方清扫", context.get("headline_label"))


if __name__ == "__main__":
    unittest.main()
