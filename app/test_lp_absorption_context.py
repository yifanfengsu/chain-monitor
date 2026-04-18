import os
import unittest

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from analyzer import BehaviorAnalyzer
from market_context_adapter import LiveMarketContextAdapter, UnavailableMarketContextAdapter
from models import Event, Signal
from notifier import format_signal_message
from pipeline import SignalPipeline
from price_service import PriceService
from quality_manager import QualityManager
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


class _StubClient:
    def __init__(self, payload: dict) -> None:
        self.payload = dict(payload)

    def fetch_market_context(self, token_or_pair: str | None, alert_ts: int | None = None) -> dict:
        del token_or_pair, alert_ts
        return dict(self.payload)


class LpAbsorptionContextTests(unittest.TestCase):
    def _pipeline(self, adapter) -> SignalPipeline:
        state_manager = StateManager()
        return SignalPipeline(
            price_service=PriceService(),
            state_manager=state_manager,
            behavior_analyzer=BehaviorAnalyzer(),
            address_scorer=AddressScorer(),
            token_scorer=TokenScorer(),
            strategy_engine=StrategyEngine(),
            quality_gate=SignalQualityGate(state_manager=state_manager),
            market_context_adapter=adapter,
            quality_manager=QualityManager(state_manager=state_manager),
        )

    def _event(self, *, intent_type: str, stage: str, same_pool: int, multi_pool: int) -> Event:
        return Event(
            tx_hash=f"0xabsorb{intent_type}{stage}",
            address="0xabsorbpool",
            token="ETH",
            amount=1.0,
            side="卖出" if intent_type == "pool_sell_pressure" else "买入",
            usd_value=26_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.84,
            confirmation_score=0.80,
            pricing_status="exact",
            pricing_confidence=0.94,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "pool_price_move_before_alert_30s": 0.004,
                "pool_price_move_before_alert_60s": 0.006,
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "base_token_symbol": "ETH",
                        "quote_token_symbol": "USDC",
                    }
                },
                "lp_analysis": {
                    "same_pool_continuity": same_pool,
                    "multi_pool_resonance": multi_pool,
                    "pool_volume_surge_ratio": 2.0,
                },
                "lp_alert_stage": stage,
            },
        )

    def _signal(self, event: Event, *, stage: str) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.86,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="absorption_test",
            quality_score=0.82,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        base_label = "持续卖压" if event.intent_type == "pool_sell_pressure" else "持续买压"
        signal.context.update(
            {
                "user_tier": "research",
                "message_template": "brief",
                "pair_label": "ETH/USDC",
                "asset_case_label": "ETH",
                "asset_case_id": "asset_case:absorb",
                "asset_case_supporting_pair_count": 1,
                "message_variant": "lp_directional",
                "lp_alert_stage": stage,
                "lp_stage_badge": "确认" if stage == "confirm" else "预警",
                "lp_state_label": base_label if stage == "confirm" else f"{base_label}建立中",
                "lp_market_read": "更像趋势确认，不是首发先手" if stage == "confirm" else "先手观察｜需看后续 30-90s 是否续单 / 跨池共振",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "结构回落",
                "lp_followup_required": True,
                "lp_same_pool_continuity": int((event.metadata.get("lp_analysis") or {}).get("same_pool_continuity") or 0),
                "lp_multi_pool_resonance": int((event.metadata.get("lp_analysis") or {}).get("multi_pool_resonance") or 0),
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_single_pool_sell_without_broader_confirmation_marks_local_absorption(self) -> None:
        pipeline = self._pipeline(UnavailableMarketContextAdapter())
        event = self._event(intent_type="pool_sell_pressure", stage="confirm", same_pool=1, multi_pool=0)
        signal = self._signal(event, stage="confirm")

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertEqual("local_sell_pressure_absorption", signal.context["lp_absorption_context"])
        self.assertIn("局部卖压，可能被承接", message.splitlines()[0])
        self.assertNotIn("LP 主体", message)

    def test_multi_pool_sell_with_live_alignment_marks_broader_confirmation(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(
                    {
                        "market_context_source": "live_public",
                        "market_context_venue": "binance_perp",
                        "market_context_requested_symbol": "ETHUSDC",
                        "market_context_resolved_symbol": "ETHUSDT",
                        "market_move_before_alert_30s": -0.002,
                        "market_move_before_alert_60s": -0.003,
                        "market_move_after_alert_60s": -0.004,
                        "perp_last_price": 3512.0,
                        "perp_mark_price": 3511.5,
                        "perp_index_price": 3510.9,
                        "spot_reference_price": 3511.2,
                    }
                )
            }
        )
        pipeline = self._pipeline(adapter)
        event = self._event(intent_type="pool_sell_pressure", stage="confirm", same_pool=2, multi_pool=2)
        signal = self._signal(event, stage="confirm")
        signal.context["asset_case_supporting_pair_count"] = 2
        signal.metadata["asset_case_supporting_pair_count"] = 2

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertEqual("broader_sell_pressure_confirmed", signal.context["lp_absorption_context"])
        self.assertIn("更广泛卖压确认", message.splitlines()[0])

    def test_single_pool_buy_without_broader_confirmation_stays_local_and_unconfirmed(self) -> None:
        pipeline = self._pipeline(UnavailableMarketContextAdapter())
        event = self._event(intent_type="pool_buy_pressure", stage="prealert", same_pool=1, multi_pool=0)
        signal = self._signal(event, stage="prealert")

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertIn(signal.context["lp_absorption_context"], {"local_buy_pressure_absorption", "pool_only_unconfirmed_pressure"})
        self.assertIn("局部买压", message.splitlines()[0])
        self.assertIn("待确认", message.splitlines()[0])
        self.assertNotIn("LP 主体", message)


if __name__ == "__main__":
    unittest.main()
