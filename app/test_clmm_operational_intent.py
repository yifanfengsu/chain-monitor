import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from signal_interpreter import SignalInterpreter


OWNER = "0x1111111111111111111111111111111111111111"
POOL = "0x3333333333333333333333333333333333333333"


class ClmmOperationalIntentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.interpreter = SignalInterpreter()

    def _event(
        self,
        *,
        intent_type: str = "clmm_range_shift",
        kind: str = "lp_add_liquidity",
        usd_value: float = 180_000.0,
        intent_confidence: float = 0.86,
        confirmation_score: float = 0.82,
        token_id: str = "12345",
        tick_lower: int = -300,
        tick_upper: int = 300,
    ) -> Event:
        clmm_context = {
            "protocol": "uniswap_v3",
            "chain": "ethereum",
            "position_manager": "0x2222222222222222222222222222222222222222",
            "token_id": token_id,
            "position_key": f"uniswap_v3:ethereum:0x2222222222222222222222222222222222222222:{token_id}",
            "pool": POOL,
            "pair_label": "ETH/USDC",
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "position_actor_label": f"Uniswap v3 Position #{token_id}",
        }
        return Event(
            tx_hash=f"0xclmmintent{token_id}",
            address=OWNER,
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            amount=1.0,
            side="增加流动性",
            usd_value=usd_value,
            kind=kind,
            ts=1_710_000_000,
            intent_type=intent_type,
            intent_confidence=intent_confidence,
            intent_stage="confirmed",
            confirmation_score=confirmation_score,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="liquidity_provider",
            metadata={
                "token_symbol": "ETH",
                "quote_symbol": "USDC",
                "pair_label": "ETH/USDC",
                "clmm_position_event": True,
                "clmm_context": clmm_context,
                "raw": {
                    "clmm_position_event": True,
                    "clmm_context": clmm_context,
                    "from": OWNER,
                    "to": POOL,
                    "counterparty": POOL,
                },
            },
        )

    def _signal(self, event: Event, *, delivery_class: str = "primary") -> Signal:
        return Signal(
            type="lp_position_intent",
            confidence=0.84,
            priority=1,
            tier="Tier 1",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.88,
            semantic="lp_position",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            information_level="high",
            abnormal_ratio=1.6,
            pricing_confidence=event.pricing_confidence,
            delivery_class=delivery_class,
            delivery_reason="unit_test",
        )

    def _interpret(self, event: Event, signal: Signal, *, watch_meta: dict | None = None) -> dict:
        watch_meta = watch_meta or {
            "address": OWNER,
            "label": "LP Alpha",
            "strategy_role": "liquidity_provider",
            "strategy_role_label": "LP 地址",
            "semantic_role": "lp_wallet",
            "semantic_role_label": "LP 地址",
            "role_label": "流动性提供者",
            "wallet_function": "unknown",
        }
        watch_context = {
            "watch_address": OWNER,
            "watch_address_label": "LP Alpha",
            "watch_meta": watch_meta,
            "counterparty": POOL,
            "counterparty_label": "ETH/USDC Pool",
            "counterparty_meta": {
                "address": POOL,
                "label": "ETH/USDC Pool",
                "strategy_role": "lp_pool",
                "semantic_role": "liquidity_pool",
                "wallet_function": "lp_pool",
            },
        }
        self.interpreter.interpret(
            event=event,
            signal=signal,
            behavior={"behavior_type": event.intent_type, "confidence": 0.84, "reason": "unit_test_behavior"},
            watch_meta=watch_meta,
            watch_context=watch_context,
            address_snapshot={"recent": []},
            token_snapshot={},
            gate_metrics={"resonance_score": 0.36},
        )
        return signal.context

    def test_clmm_range_shift_maps_to_operational_range_recenter(self) -> None:
        event = self._event(intent_type="clmm_range_shift")
        signal = self._signal(event)
        context = self._interpret(event, signal)

        self.assertEqual("clmm_range_recenter", context.get("operational_intent_key"))
        self.assertEqual("区间重设", context.get("operational_intent_label"))

    def test_collect_maps_to_passive_fee_harvest_operational_intent(self) -> None:
        event = self._event(
            intent_type="clmm_passive_fee_harvest",
            kind="clmm_collect_fees",
            usd_value=24_000.0,
            intent_confidence=0.72,
            confirmation_score=0.58,
        )
        signal = self._signal(event, delivery_class="observe")
        context = self._interpret(event, signal)

        self.assertEqual("clmm_passive_fee_harvest", context.get("operational_intent_key"))
        self.assertEqual("被动收手续费", context.get("operational_intent_label"))

    def test_jit_likely_maps_to_jit_fee_extraction_operational_intent(self) -> None:
        event = self._event(
            intent_type="clmm_jit_liquidity_likely",
            kind="lp_remove_liquidity",
            usd_value=95_000.0,
            intent_confidence=0.62,
            confirmation_score=0.34,
        )
        signal = self._signal(event, delivery_class="observe")
        context = self._interpret(event, signal)

        self.assertEqual("clmm_jit_fee_extraction_likely", context.get("operational_intent_key"))
        self.assertEqual("疑似 JIT 抽费", context.get("operational_intent_label"))

    def test_clmm_intent_template_line1_is_actor_intent_confidence(self) -> None:
        event = self._event(intent_type="clmm_range_shift")
        signal = self._signal(event)
        self._interpret(event, signal)

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertEqual("Uniswap v3 Position #12345｜区间重设｜高", lines[0])

    def test_clmm_message_includes_position_context(self) -> None:
        event = self._event(intent_type="clmm_position_remove_range_liquidity", kind="lp_remove_liquidity")
        signal = self._signal(event)
        self._interpret(event, signal)

        message = format_signal_message(signal, event)
        self.assertIn("Uniswap v3 · ETH/USDC · tick [-300,300]", message)

    def test_clmm_primary_message_includes_invalidation_and_next_check(self) -> None:
        event = self._event(intent_type="clmm_position_close", kind="lp_remove_liquidity")
        signal = self._signal(event, delivery_class="primary")
        self._interpret(event, signal)

        message = format_signal_message(signal, event)
        lines = message.strip().splitlines()

        self.assertGreaterEqual(len(lines), 5)
        self.assertTrue(lines[4].startswith("失效："))

    def test_pool_layer_and_position_layer_messages_remain_distinct(self) -> None:
        clmm_event = self._event(intent_type="clmm_range_shift")
        clmm_signal = self._signal(clmm_event)
        self._interpret(clmm_event, clmm_signal)
        clmm_message = format_signal_message(clmm_signal, clmm_event)

        pool_event = Event(
            tx_hash="0xpoolmsg",
            address=POOL,
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=120_000.0,
            kind="swap",
            ts=1_710_000_100,
            intent_type="pool_buy_pressure",
            intent_confidence=0.90,
            intent_stage="confirmed",
            confirmation_score=0.88,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "raw": {"lp_context": {"pair_label": "ETH/USDC", "pool_label": "ETH/USDC Pool"}},
            },
        )
        pool_signal = Signal(
            type="pool_buy_pressure",
            confidence=0.9,
            priority=1,
            tier="Tier 2",
            address=pool_event.address,
            token=pool_event.token,
            tx_hash=pool_event.tx_hash,
            usd_value=float(pool_event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.9,
            semantic="pool_trade_pressure",
            intent_type=pool_event.intent_type,
            intent_stage=pool_event.intent_stage,
            confirmation_score=pool_event.confirmation_score,
            information_level="high",
            abnormal_ratio=2.0,
            pricing_confidence=pool_event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        self.interpreter.interpret(
            pool_event,
            pool_signal,
            {"behavior_type": "pool_buy_pressure", "confidence": 0.9, "reason": "unit_test"},
            {
                "address": POOL,
                "label": "ETH/USDC Pool",
                "strategy_role": "lp_pool",
                "semantic_role": "liquidity_pool",
                "wallet_function": "lp_pool",
                "pair_label": "ETH/USDC",
            },
            {"watch_address": POOL, "watch_meta": {"strategy_role": "lp_pool"}},
            {"recent": []},
            {},
            gate_metrics={"resonance_score": 0.3},
        )
        pool_signal.context["message_template"] = "brief"
        pool_message = format_signal_message(pool_signal, pool_event)

        self.assertIn("Uniswap v3 Position #12345", clmm_message)
        self.assertTrue(pool_message.splitlines()[0].startswith("ETH/USDC｜"))
        self.assertNotEqual(clmm_message.splitlines()[0], pool_message.splitlines()[0])

    def test_outcome_tracking_seam_has_required_windows(self) -> None:
        event = self._event(intent_type="clmm_position_remove_range_liquidity", kind="lp_remove_liquidity")
        signal = self._signal(event)
        context = self._interpret(event, signal)

        tracking = context.get("outcome_tracking") or {}
        windows = tracking.get("windows") or {}

        self.assertTrue(context.get("outcome_tracking_enabled"))
        self.assertEqual({"5m", "15m", "1h"}, set(windows.keys()))
        self.assertIn("price_follow_through", windows["5m"])
        self.assertIn("position_continuation", windows["15m"])
        self.assertIn("invalidation", windows["1h"])


if __name__ == "__main__":
    unittest.main()
