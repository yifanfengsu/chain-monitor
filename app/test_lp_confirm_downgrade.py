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


class LpConfirmDowngradeTests(unittest.TestCase):
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

    def _event(
        self,
        *,
        intent_type: str,
        pool_move_before_30s: float,
        pool_move_before_60s: float,
        same_pool_continuity: int,
        multi_pool_resonance: int,
    ) -> Event:
        return Event(
            tx_hash=f"0xconfirm{intent_type}",
            address="0xconfirmpool",
            token="ETH",
            amount=1.0,
            side="卖出" if intent_type == "pool_sell_pressure" else "买入",
            usd_value=38_000.0,
            kind="swap",
            ts=1_710_000_300,
            intent_type=intent_type,
            intent_stage="confirmed",
            intent_confidence=0.86,
            confirmation_score=0.82,
            pricing_status="exact",
            pricing_confidence=0.94,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": "ETH",
                "climax_reversal_score": 0.68,
                "pool_price_move_before_alert_30s": pool_move_before_30s,
                "pool_price_move_before_alert_60s": pool_move_before_60s,
                "raw": {
                    "lp_context": {
                        "pair_label": "ETH/USDC",
                        "base_token_symbol": "ETH",
                        "quote_token_symbol": "USDC",
                    }
                },
                "lp_analysis": {
                    "same_pool_continuity": same_pool_continuity,
                    "multi_pool_resonance": multi_pool_resonance,
                    "pool_volume_surge_ratio": 2.2,
                },
            },
        )

    def _signal(self, event: Event) -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.88,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="confirm_test",
            quality_score=0.84,
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
                "asset_case_id": "asset_case:confirm",
                "asset_case_supporting_pair_count": 2,
                "message_variant": "lp_directional",
                "lp_alert_stage": "confirm",
                "lp_stage_badge": "确认",
                "lp_state_label": base_label,
                "lp_market_read": "更像趋势确认，不是首发先手",
                "lp_followup_check": "90s：是否继续跨池放大",
                "lp_invalidation": "连续性中断 / 共振消失",
                "lp_followup_required": True,
                "lp_same_pool_continuity": int((event.metadata.get("lp_analysis") or {}).get("same_pool_continuity") or 0),
                "lp_multi_pool_resonance": int((event.metadata.get("lp_analysis") or {}).get("multi_pool_resonance") or 0),
                "pool_quality_score": 0.72,
                "pair_quality_score": 0.74,
                "asset_case_quality_score": 0.73,
            }
        )
        signal.metadata.update(signal.context)
        return signal

    def test_unavailable_and_large_move_before_alert_downgrades_confirm(self) -> None:
        pipeline = self._pipeline(UnavailableMarketContextAdapter())
        event = self._event(
            intent_type="pool_sell_pressure",
            pool_move_before_30s=0.015,
            pool_move_before_60s=0.022,
            same_pool_continuity=1,
            multi_pool_resonance=0,
        )
        signal = self._signal(event)

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertIn(signal.context["lp_confirm_quality"], {"late_confirm", "chase_risk"})
        self.assertTrue(
            message.splitlines()[0].startswith("数据缺口，不交易｜")
            or message.splitlines()[0].startswith("不追空｜")
        )

    def test_live_alignment_with_small_pre_move_stays_clean_confirm(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(
                    {
                        "market_context_source": "live_public",
                        "market_context_venue": "binance_perp",
                        "market_context_requested_symbol": "ETHUSDC",
                        "market_context_resolved_symbol": "ETHUSDT",
                        "market_move_before_alert_30s": 0.0010,
                        "market_move_before_alert_60s": 0.0018,
                        "market_move_after_alert_60s": 0.0042,
                        "perp_last_price": 3512.0,
                        "perp_mark_price": 3511.5,
                        "perp_index_price": 3510.9,
                        "spot_reference_price": 3511.2,
                    }
                )
            }
        )
        pipeline = self._pipeline(adapter)
        event = self._event(
            intent_type="pool_buy_pressure",
            pool_move_before_30s=0.002,
            pool_move_before_60s=0.004,
            same_pool_continuity=2,
            multi_pool_resonance=2,
        )
        signal = self._signal(event)

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertEqual("clean_confirm", signal.context["lp_confirm_quality"])
        self.assertTrue(
            message.splitlines()[0].startswith("可顺势追多｜")
            or message.splitlines()[0].startswith("偏多观察｜")
        )

    def test_single_pool_without_broader_confirmation_skews_late_or_chase(self) -> None:
        pipeline = self._pipeline(UnavailableMarketContextAdapter())
        event = self._event(
            intent_type="pool_sell_pressure",
            pool_move_before_30s=0.007,
            pool_move_before_60s=0.009,
            same_pool_continuity=1,
            multi_pool_resonance=0,
        )
        signal = self._signal(event)
        signal.context["asset_case_supporting_pair_count"] = 1
        signal.metadata["asset_case_supporting_pair_count"] = 1

        pipeline._annotate_market_context(event, signal)
        pipeline._apply_lp_signal_corrections(event, signal, gate_metrics=event.metadata.get("lp_analysis") or {})
        message = format_signal_message(signal, event)

        self.assertIn(signal.context["lp_confirm_quality"], {"late_confirm", "chase_risk"})
        self.assertTrue(
            message.splitlines()[0].startswith("数据缺口，不交易｜")
            or any(keyword in message.splitlines()[0] for keyword in ("不追空", "偏空观察"))
        )


if __name__ == "__main__":
    unittest.main()
