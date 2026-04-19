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
            "lp_sweep_phase": str(sweep.get("lp_sweep_phase") or ""),
            "lp_sweep_followthrough_score": float(sweep.get("lp_sweep_followthrough_score") or 0.0),
            "lp_sweep_exhaustion_score": float(sweep.get("lp_sweep_exhaustion_score") or 0.0),
            "lp_sweep_continuation_score": float(sweep.get("lp_sweep_continuation_score") or 0.0),
            "lp_impact_to_size_ratio": float(sweep.get("lp_impact_to_size_ratio") or 0.0),
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
        self.assertEqual("sweep_confirmed", context.get("lp_sweep_phase"))
        self.assertEqual("climax", context.get("lp_alert_stage"))
        self.assertEqual("买方清扫确认", context.get("market_state_label"))
        self.assertEqual("买方清扫确认", context.get("headline_label"))
        self.assertIn("冲击延续", context.get("lp_meaning_brief", ""))
        self.assertEqual("pool_buy_pressure", event.intent_type)

    def test_sell_side_sweep(self) -> None:
        event = self._event(intent_type="pool_sell_pressure")
        signal = self._signal(event)
        context = self._interpret(event, signal, self._gate_metrics(event, price_impact_ratio=0.007))

        self.assertEqual("sell_side_liquidity_sweep", context.get("semantic_subtype"))
        self.assertEqual("卖方清扫确认", context.get("market_state_label"))
        self.assertEqual("sweep_confirmed", context.get("lp_sweep_phase"))
        self.assertEqual("climax", context.get("lp_alert_stage"))
        self.assertIn("冲击延续", context.get("lp_meaning_brief", ""))

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

        self.assertEqual(6, len(lines))
        self.assertEqual("数据缺口，不交易｜ETH/USDC｜缺 broader live context", lines[0])
        self.assertTrue(lines[1].startswith("证据："))
        self.assertTrue(lines[2].startswith("为什么："))
        self.assertTrue(lines[3].startswith("触发："))
        self.assertTrue(lines[4].startswith("失效："))
        self.assertIn("调试：stage=climax｜scope=-｜context=unavailable｜quality=-｜latency=0ms", lines[5])
        self.assertEqual("买方清扫确认", context.get("headline_label"))


class MarketMakerNotifierFormattingTests(unittest.TestCase):
    def _event(self, *, delivery_reason: str) -> Event:
        return Event(
            tx_hash="0xmaker1234567890",
            address="0xmaker",
            token="PEPE",
            amount=1.0,
            side="买入",
            usd_value=150_000.0,
            kind="swap",
            ts=1_710_000_000,
            intent_type="swap_execution",
            intent_stage="confirmed",
            intent_confidence=0.95,
            confirmation_score=0.86,
            pricing_status="exact",
            pricing_confidence=0.96,
            usd_value_available=True,
            strategy_role="market_maker_wallet",
            delivery_reason=delivery_reason,
            metadata={"raw": {"from": "0xmaker", "to": "0xpool"}},
        )

    def _signal(self, *, delivery_class: str, delivery_reason: str) -> Signal:
        return Signal(
            type="swap_execution",
            confidence=0.92,
            priority=1,
            tier="Tier 1",
            address="0xmaker",
            token="PEPE",
            tx_hash="0xmaker1234567890",
            usd_value=150_000.0,
            reason="unit_test",
            quality_score=0.9,
            semantic="execution",
            intent_type="swap_execution",
            intent_stage="confirmed",
            confirmation_score=0.86,
            information_level="high",
            abnormal_ratio=2.0,
            pricing_confidence=0.96,
            delivery_class=delivery_class,
            delivery_reason=delivery_reason,
            metadata={
                "is_real_execution": True,
                "strategy_role": "market_maker_wallet",
                "role_priority_tier": "tier1",
            },
            context={
                "message_variant": "smart_money_primary" if delivery_class == "primary" else "smart_money_observe",
                "smart_money_style_variant": "market_maker",
                "object_label": "Maker Desk",
                "path_label": "Maker Desk -> PEPE/USDC Pool",
                "resonance_label": "库存回补共振",
                "confirmation_label": "库存确认",
                "market_maker_fact_brief": "库存对冲｜$150,000",
                "market_maker_explanation_brief": "更像库存管理 / 流动性供给，不按主观建仓解读。",
                "market_maker_evidence_brief": "双边补货｜库存语境",
                "market_maker_action_hint": "继续看库存是否回补完成。",
                "market_maker_observe_label": "库存管理 / 流动性供给观察路径",
                "smart_money_fact_brief": "SHOULD_NOT_USE_SM_FACT",
                "smart_money_explanation_brief": "SHOULD_NOT_USE_SM_EXPLANATION",
                "smart_money_evidence_brief": "SHOULD_NOT_USE_SM_EVIDENCE",
                "smart_money_action_hint": "SHOULD_NOT_USE_SM_ACTION",
            },
        )

    def test_market_maker_primary_message_uses_market_maker_fields(self) -> None:
        event = self._event(delivery_reason="market_maker_execution_primary")
        signal = self._signal(delivery_class="primary", delivery_reason="market_maker_execution_primary")

        message = format_signal_message(signal, event)

        self.assertIn("🏦 Market Maker 库存执行", message)
        self.assertIn("阶段：Primary｜T1 Market Maker｜P3", message)
        self.assertIn("库存动作：库存对冲｜$150,000", message)
        self.assertIn("库存语境：更像库存管理 / 流动性供给，不按主观建仓解读。", message)
        self.assertIn("证据：双边补货｜库存语境", message)
        self.assertIn("继续看：继续看库存是否回补完成。", message)
        self.assertNotIn("Smart Money", message)
        self.assertNotIn("SHOULD_NOT_USE_SM_", message)

    def test_market_maker_observe_message_uses_market_maker_fields(self) -> None:
        event = self._event(delivery_reason="market_maker_execution_observe")
        signal = self._signal(delivery_class="observe", delivery_reason="market_maker_execution_observe")

        message = format_signal_message(signal, event)

        self.assertIn("🏦 Market Maker 库存观察", message)
        self.assertIn("阶段：Observe｜T1 Market Maker｜P3", message)
        self.assertIn("库存动作：库存对冲｜$150,000", message)
        self.assertIn("当前更像：更像库存管理 / 流动性供给，不按主观建仓解读。", message)
        self.assertIn("证据：双边补货｜库存语境", message)
        self.assertIn("为什么值得观察：库存管理 / 流动性供给观察路径", message)
        self.assertIn("继续看：继续看库存是否回补完成。", message)
        self.assertNotIn("Smart Money", message)
        self.assertNotIn("SHOULD_NOT_USE_SM_", message)


if __name__ == "__main__":
    unittest.main()
