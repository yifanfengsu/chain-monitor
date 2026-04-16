import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from models import Event, Signal
from notifier import format_signal_message
from signal_interpreter import SignalInterpreter


class OperationalIntentNotifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.interpreter = SignalInterpreter()

    def _signal(
        self,
        event: Event,
        *,
        signal_type: str,
        behavior_type: str = "normal",
        delivery_class: str = "observe",
        quality_score: float = 0.84,
    ) -> Signal:
        return Signal(
            type=signal_type,
            confidence=0.82,
            priority=1,
            tier="Tier 1",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            behavior_type=behavior_type,
            quality_score=quality_score,
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            information_level="medium",
            abnormal_ratio=1.8,
            pricing_confidence=event.pricing_confidence,
            delivery_class=delivery_class,
            delivery_reason="unit_test",
            metadata={},
        )

    def _interpret(
        self,
        event: Event,
        signal: Signal,
        *,
        watch_meta: dict,
        counterparty_meta: dict | None = None,
        gate_metrics: dict | None = None,
    ) -> dict:
        watch_context = {
            "watch_address": event.address,
            "watch_address_label": watch_meta.get("label", ""),
            "watch_meta": watch_meta,
            "counterparty": str((counterparty_meta or {}).get("address") or "").lower(),
            "counterparty_label": str((counterparty_meta or {}).get("label") or ""),
            "counterparty_meta": counterparty_meta or {},
        }
        self.interpreter.interpret(
            event=event,
            signal=signal,
            behavior={
                "behavior_type": signal.behavior_type,
                "confidence": 0.82,
                "reason": "unit_test_behavior",
            },
            watch_meta=watch_meta,
            watch_context=watch_context,
            address_snapshot={"recent": []},
            token_snapshot={},
            gate_metrics=gate_metrics or {"resonance_score": 0.36},
        )
        return signal.context

    def test_smart_money_router_path_without_execution_is_preparation_only(self) -> None:
        actor = "0x2000000000000000000000000000000000000001"
        router = "0x2000000000000000000000000000000000000002"
        event = Event(
            tx_hash="0xsmprep",
            address=actor,
            token="USDT",
            amount=1.0,
            side="流出",
            usd_value=90_000.0,
            kind="token_transfer",
            ts=1_710_000_000,
            intent_type="possible_buy_preparation",
            intent_confidence=0.62,
            intent_stage="preliminary",
            confirmation_score=0.48,
            pricing_status="exact",
            pricing_confidence=0.94,
            strategy_role="smart_money_wallet",
            metadata={
                "token_symbol": "USDT",
                "raw": {
                    "from": actor,
                    "to": router,
                },
            },
        )
        signal = self._signal(event, signal_type="buy_preparation")
        context = self._interpret(
            event,
            signal,
            watch_meta={
                "address": actor,
                "label": "ETH Smart Trader Whale",
                "strategy_role": "smart_money_wallet",
                "semantic_role": "trader_wallet",
            },
            counterparty_meta={
                "address": router,
                "label": "1inch Router",
                "strategy_role": "aggregator_router",
                "semantic_role": "router_contract",
                "wallet_function": "router",
            },
        )

        self.assertEqual("smart_money_preparation_only", context.get("operational_intent_key"))
        self.assertEqual("仅交易准备", context.get("operational_intent_label"))

    def test_smart_money_actual_swap_maps_to_entry_or_exit_execution(self) -> None:
        for side, expected in [("买入", "smart_money_entry_execution"), ("卖出", "smart_money_exit_execution")]:
            with self.subTest(side=side):
                actor = "0x2000000000000000000000000000000000000011"
                pool = "0x2000000000000000000000000000000000000012"
                event = Event(
                    tx_hash=f"0xsmexec{side}",
                    address=actor,
                    token="PEPE",
                    amount=1.0,
                    side=side,
                    usd_value=140_000.0,
                    kind="swap",
                    ts=1_710_000_100,
                    intent_type="swap_execution",
                    intent_confidence=0.95,
                    intent_stage="confirmed",
                    confirmation_score=0.86,
                    pricing_status="exact",
                    pricing_confidence=0.96,
                    strategy_role="smart_money_wallet",
                    metadata={
                        "token_symbol": "PEPE",
                        "raw": {"from": actor, "to": pool},
                    },
                )
                signal = self._signal(event, signal_type="active_trade", delivery_class="primary")
                context = self._interpret(
                    event,
                    signal,
                    watch_meta={
                        "address": actor,
                        "label": "ETH Smart Trader Whale",
                        "strategy_role": "smart_money_wallet",
                        "semantic_role": "trader_wallet",
                    },
                )
                self.assertEqual(expected, context.get("operational_intent_key"))

    def test_market_maker_settlement_like_move_uses_settlement_intent(self) -> None:
        maker = "0x2000000000000000000000000000000000000021"
        venue = "0x2000000000000000000000000000000000000022"
        event = Event(
            tx_hash="0xmakersettle",
            address=maker,
            token="ETH",
            amount=1.0,
            side="流出",
            usd_value=180_000.0,
            kind="token_transfer",
            ts=1_710_000_200,
            intent_type="market_making_inventory_move",
            intent_confidence=0.68,
            intent_stage="preliminary",
            confirmation_score=0.54,
            pricing_status="exact",
            pricing_confidence=0.95,
            strategy_role="market_maker_wallet",
            metadata={
                "token_symbol": "ETH",
                "counterparty_wallet_function": "exchange_trading",
                "raw": {"from": maker, "to": venue},
            },
        )
        signal = self._signal(
            event,
            signal_type="inventory_rebalance",
            behavior_type="inventory_management",
        )
        context = self._interpret(
            event,
            signal,
            watch_meta={
                "address": maker,
                "label": "Wintermute",
                "entity_label": "Wintermute",
                "entity_type": "market_maker_firm",
                "wallet_function": "mm_inventory",
                "strategy_role": "market_maker_wallet",
                "semantic_role": "market_maker_wallet",
            },
            counterparty_meta={
                "address": venue,
                "label": "Binance Trading",
                "entity_label": "Binance",
                "entity_type": "exchange",
                "wallet_function": "exchange_trading",
                "strategy_role": "exchange_trading_wallet",
                "semantic_role": "exchange_wallet",
            },
            gate_metrics={"resonance_score": 0.34},
        )

        self.assertEqual("market_maker_settlement_move", context.get("operational_intent_key"))
        self.assertEqual("结算/轮转", context.get("operational_intent_label"))

    def test_pool_only_event_does_not_claim_lp_subject_intent(self) -> None:
        pool = "0x2000000000000000000000000000000000000031"
        event = Event(
            tx_hash="0xlppool",
            address=pool,
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=125_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type="pool_buy_pressure",
            intent_confidence=0.90,
            intent_stage="confirmed",
            confirmation_score=0.88,
            pricing_status="exact",
            pricing_confidence=0.95,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC Pool",
                    }
                },
            },
        )
        signal = self._signal(event, signal_type="pool_buy_pressure")
        context = self._interpret(
            event,
            signal,
            watch_meta={
                "address": pool,
                "label": "ETH/USDC Pool",
                "strategy_role": "lp_pool",
                "semantic_role": "liquidity_pool",
                "wallet_function": "lp_pool",
                "pair_label": "ETH/USDC",
            },
        )

        self.assertEqual("lp_pool_buy_pressure", context.get("operational_intent_key"))
        self.assertIn("不替 LP 主体发言", context.get("operational_intent_market_implication", ""))

    def test_liquidation_risk_maps_to_risk_building_intent(self) -> None:
        pool = "0x2000000000000000000000000000000000000041"
        event = Event(
            tx_hash="0xliq",
            address=pool,
            token="ETH",
            amount=1.0,
            side="卖出",
            usd_value=220_000.0,
            kind="swap",
            ts=1_710_000_400,
            intent_type="pool_sell_pressure",
            intent_confidence=0.78,
            intent_stage="confirmed",
            confirmation_score=0.74,
            pricing_status="exact",
            pricing_confidence=0.96,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "liquidation_stage": "risk",
                "liquidation_score": 0.83,
                "liquidation_side": "long_flush",
                "liquidation_protocols": ["Aave"],
                "liquidation_reason": "keeper_and_vault_hits",
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "pool_label": "ETH/USDC Pool",
                    }
                },
            },
        )
        signal = self._signal(event, signal_type="pool_sell_pressure")
        context = self._interpret(
            event,
            signal,
            watch_meta={
                "address": pool,
                "label": "ETH/USDC Pool",
                "strategy_role": "lp_pool",
                "semantic_role": "liquidity_pool",
                "wallet_function": "lp_pool",
                "pair_label": "ETH/USDC",
            },
        )

        self.assertEqual("liquidation_risk_building", context.get("operational_intent_key"))

    def test_intent_template_line1_actor_intent_confidence(self) -> None:
        deposit = "0x2000000000000000000000000000000000000051"
        hot = "0x2000000000000000000000000000000000000052"
        event = Event(
            tx_hash="0xexchangeintent",
            address=hot,
            token="USDT",
            amount=1.0,
            side="流入",
            usd_value=120_000.0,
            kind="token_transfer",
            ts=1_710_000_500,
            intent_type="internal_rebalance",
            intent_confidence=0.64,
            intent_stage="confirmed",
            confirmation_score=0.72,
            pricing_status="exact",
            pricing_confidence=0.96,
            strategy_role="exchange_hot_wallet",
            metadata={
                "token_symbol": "USDT",
                "watch_wallet_function": "exchange_hot",
                "counterparty_wallet_function": "exchange_deposit",
                "exchange_internality": "confirmed",
                "exchange_transfer_purpose": "exchange_user_deposit_consolidation",
                "exchange_transfer_confidence": 0.88,
                "exchange_entity_label": "Binance",
                "exchange_transfer_why": ["deposit -> same_entity hot"],
                "raw": {
                    "from": deposit,
                    "to": hot,
                    "exchange_internality": "confirmed",
                    "exchange_transfer_purpose": "exchange_user_deposit_consolidation",
                },
            },
        )
        signal = self._signal(event, signal_type="internal_rebalance", delivery_class="primary")
        context = self._interpret(
            event,
            signal,
            watch_meta={
                "address": hot,
                "label": "Binance 14",
                "entity_label": "Binance",
                "entity_type": "exchange",
                "wallet_function": "exchange_hot",
                "strategy_role": "exchange_hot_wallet",
                "semantic_role": "exchange_hot_wallet",
            },
            counterparty_meta={
                "address": deposit,
                "label": "Binance Deposit",
                "entity_label": "Binance",
                "entity_type": "exchange",
                "wallet_function": "exchange_deposit",
                "strategy_role": "exchange_deposit_wallet",
                "semantic_role": "exchange_deposit_address",
            },
        )

        message = format_signal_message(signal, event)
        line1 = message.strip().splitlines()[0]

        self.assertEqual("Binance Hot Wallet｜内部归集｜高", line1)
        self.assertEqual("intent", context.get("message_template"))

    def test_exchange_intent_message_contains_entity_context_and_transfer_purpose(self) -> None:
        deposit = "0x2000000000000000000000000000000000000061"
        hot = "0x2000000000000000000000000000000000000062"
        event = Event(
            tx_hash="0xexchangecontext",
            address=hot,
            token="USDT",
            amount=1.0,
            side="流入",
            usd_value=120_000.0,
            kind="token_transfer",
            ts=1_710_000_600,
            intent_type="internal_rebalance",
            intent_confidence=0.64,
            intent_stage="confirmed",
            confirmation_score=0.72,
            pricing_status="exact",
            pricing_confidence=0.96,
            strategy_role="exchange_hot_wallet",
            metadata={
                "token_symbol": "USDT",
                "watch_wallet_function": "exchange_hot",
                "counterparty_wallet_function": "exchange_deposit",
                "exchange_internality": "confirmed",
                "exchange_transfer_purpose": "exchange_user_deposit_consolidation",
                "exchange_transfer_confidence": 0.88,
                "exchange_entity_label": "Binance",
                "exchange_transfer_why": ["deposit -> same_entity hot"],
                "raw": {
                    "from": deposit,
                    "to": hot,
                    "exchange_internality": "confirmed",
                    "exchange_transfer_purpose": "exchange_user_deposit_consolidation",
                },
            },
        )
        signal = self._signal(event, signal_type="internal_rebalance", delivery_class="primary")
        self._interpret(
            event,
            signal,
            watch_meta={
                "address": hot,
                "label": "Binance 14",
                "entity_label": "Binance",
                "entity_type": "exchange",
                "wallet_function": "exchange_hot",
                "strategy_role": "exchange_hot_wallet",
                "semantic_role": "exchange_hot_wallet",
            },
            counterparty_meta={
                "address": deposit,
                "label": "Binance Deposit",
                "entity_label": "Binance",
                "entity_type": "exchange",
                "wallet_function": "exchange_deposit",
                "strategy_role": "exchange_deposit_wallet",
                "semantic_role": "exchange_deposit_address",
            },
        )

        message = format_signal_message(signal, event)
        self.assertIn("Binance Hot Wallet · 用户入金归集 · 同实体内部", message)
        self.assertIn("purpose=exchange_user_deposit_consolidation", message)


if __name__ == "__main__":
    unittest.main()
