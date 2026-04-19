import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from signal_interpreter import SignalInterpreter
from signal_quality_gate import detect_lp_liquidity_sweep


class LpStageInterpreterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.interpreter = SignalInterpreter()

    def _event(
        self,
        *,
        intent_type: str = "pool_buy_pressure",
        usd_value: float = 2_000.0,
        same_pool_continuity: int = 1,
        multi_pool_resonance: int = 0,
        pool_volume_surge_ratio: float = 1.45,
        reserve_skew: float = 0.15,
        action_intensity: float = 0.34,
        confirmation_score: float = 0.34,
        price_impact_ratio: float = 0.0,
        lp_scan_path: str = "secondary",
        first_chain_seen_at: int = 1_710_000_000,
        parsed_at: int = 1_710_000_002,
        lp_detect_latency_ms: int = 2_000,
        lp_end_to_end_latency_ms: int | None = None,
    ) -> Event:
        metadata = {
            "token_symbol": "ETH",
            "first_chain_seen_at": first_chain_seen_at,
            "parsed_at": parsed_at,
            "lp_scan_path": lp_scan_path,
            "lp_detect_latency_ms": lp_detect_latency_ms,
            "price_impact_ratio": price_impact_ratio,
            "pool_price_move_before_alert_30s": 0.001,
            "pool_price_move_before_alert_60s": 0.0015,
            "raw": {
                "lp_context": {
                    "pair_label": "ETH/USDC",
                    "pool_label": "ETH/USDC",
                    "base_amount": 1.0,
                    "quote_amount": usd_value,
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
                "lp_burst_event_count": 4,
                "lp_burst_window_sec": 15,
            },
        }
        if lp_end_to_end_latency_ms is not None:
            metadata["lp_end_to_end_latency_ms"] = lp_end_to_end_latency_ms
        return Event(
            tx_hash=f"0xstage{intent_type}{usd_value}",
            address="0xstagepool",
            token="ETH",
            amount=1.0,
            side="买入" if intent_type != "pool_sell_pressure" else "卖出",
            usd_value=usd_value,
            kind="swap",
            ts=1_710_000_100,
            intent_type=intent_type,
            intent_confidence=0.88,
            intent_stage="confirmed" if confirmation_score >= 0.72 else "preliminary",
            confirmation_score=confirmation_score,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata=metadata,
        )

    def _signal(self, event: Event, *, delivery_class: str = "observe") -> Signal:
        return Signal(
            type=event.intent_type,
            confidence=0.88,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.86,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class=delivery_class,
            delivery_reason="unit_test",
            metadata={},
        )

    def _gate_metrics(
        self,
        event: Event,
        *,
        lp_prealert_applied: bool = False,
        lp_prealert_reason: str = "",
        price_impact_ratio: float = 0.0,
    ) -> dict:
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
            "lp_prealert_applied": lp_prealert_applied,
            "lp_prealert_reason": lp_prealert_reason,
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
            {"behavior_type": event.intent_type, "confidence": 0.88, "reason": "unit_test"},
            {"strategy_role": "lp_pool", "pair_label": "ETH/USDC"},
            {},
            {"recent": []},
            {},
            gate_metrics=gate_metrics,
        )
        signal.context["message_template"] = "brief"
        return signal.context

    def test_prealert_message_uses_stage_first_building_label(self) -> None:
        event = self._event()
        signal = self._signal(event)
        context = self._interpret(
            event,
            signal,
            self._gate_metrics(
                event,
                lp_prealert_applied=True,
                lp_prealert_reason="lp_prealert_direction_building",
            ),
        )

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertEqual("prealert", context.get("lp_alert_stage"))
        self.assertTrue(lines[0].startswith("数据缺口，不交易｜ETH/USDC｜"))
        self.assertTrue(lines[2].startswith("为什么："))
        self.assertTrue(lines[3].startswith("触发："))

    def test_confirm_message_marks_trend_confirmation_and_shows_scan_path(self) -> None:
        event = self._event(
            usd_value=120_000.0,
            same_pool_continuity=2,
            multi_pool_resonance=2,
            pool_volume_surge_ratio=2.4,
            reserve_skew=0.24,
            action_intensity=0.56,
            confirmation_score=0.88,
            lp_scan_path="promoted_main",
            lp_end_to_end_latency_ms=4_200,
        )
        signal = self._signal(event, delivery_class="primary")
        context = self._interpret(event, signal, self._gate_metrics(event))

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertEqual("confirm", context.get("lp_alert_stage"))
        self.assertEqual("数据缺口，不交易｜ETH/USDC｜缺 broader live context", lines[0])
        self.assertTrue(lines[2].startswith("为什么："))
        self.assertIn("调试：stage=confirm｜scope=-｜context=unavailable｜quality=-｜latency=4200ms", lines[-1])

    def test_exhaustion_message_marks_risk_instead_of_continuation(self) -> None:
        event = self._event(
            usd_value=12_000.0,
            same_pool_continuity=1,
            multi_pool_resonance=0,
            pool_volume_surge_ratio=3.8,
            reserve_skew=0.22,
            action_intensity=0.50,
            confirmation_score=0.68,
            price_impact_ratio=0.028,
        )
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.028))

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertEqual("exhaustion_risk", context.get("lp_alert_stage"))
        self.assertEqual("sweep_exhaustion_risk", context.get("lp_sweep_phase"))
        self.assertEqual("不追多｜ETH/USDC｜买方清扫后回吐风险", lines[0])
        self.assertTrue(lines[2].startswith("为什么："))

    def test_medium_confidence_prealert_keeps_tentative_prefix(self) -> None:
        event = self._event()
        signal = self._signal(event)
        context = self._interpret(
            event,
            signal,
            self._gate_metrics(
                event,
                lp_prealert_applied=True,
                lp_prealert_reason="lp_prealert_direction_building",
            ),
        )

        self.assertGreaterEqual(float(context.get("lp_lead_confidence") or 0.0), 0.56)
        self.assertIn("可能买盘建立中", str(context.get("lp_state_label") or ""))

    def test_multi_pool_resonance_changes_sweep_continuation_profile(self) -> None:
        event = self._event(
            usd_value=120_000.0,
            same_pool_continuity=2,
            multi_pool_resonance=2,
            pool_volume_surge_ratio=2.3,
            reserve_skew=0.24,
            action_intensity=0.55,
            confirmation_score=0.82,
        )
        without_resonance = detect_lp_liquidity_sweep(
            event=event,
            reserve_skew=0.24,
            action_intensity=0.55,
            same_pool_continuity=2,
            multi_pool_resonance=0,
            pool_volume_surge_ratio=2.3,
            lp_burst_event_count=4,
            lp_burst_window_sec=15,
            price_impact_ratio=0.006,
            min_price_impact_ratio=0.003,
            liquidation_stage="none",
        )
        with_resonance = detect_lp_liquidity_sweep(
            event=event,
            reserve_skew=0.24,
            action_intensity=0.55,
            same_pool_continuity=2,
            multi_pool_resonance=2,
            pool_volume_surge_ratio=2.3,
            lp_burst_event_count=4,
            lp_burst_window_sec=15,
            price_impact_ratio=0.006,
            min_price_impact_ratio=0.003,
            liquidation_stage="none",
        )

        self.assertLess(float(without_resonance.get("lp_sweep_continuation_score") or 0.0), float(with_resonance.get("lp_sweep_continuation_score") or 0.0))
        self.assertEqual("sweep_building", without_resonance.get("lp_sweep_phase"))
        self.assertEqual("sweep_confirmed", with_resonance.get("lp_sweep_phase"))


if __name__ == "__main__":
    unittest.main()
