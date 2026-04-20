import unittest

from models import Event, Signal
from state_manager import StateManager


class OutcomeMarketPriceSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state_manager = StateManager()

    def _event(
        self,
        *,
        ts: int,
        quote_amount: float,
        market_context: dict | None = None,
    ) -> Event:
        metadata = {
            "token_symbol": "ETH",
            "raw": {
                "lp_context": {
                    "pair_label": "ETH/USDC",
                    "pool_label": "ETH/USDC",
                    "base_amount": 1.0,
                    "quote_amount": quote_amount,
                }
            },
        }
        metadata.update(market_context or {})
        return Event(
            tx_hash=f"0xoutcomesource{ts}{quote_amount}",
            address="0xoutcomesourcepool",
            token="ETH",
            amount=1.0,
            side="买入",
            usd_value=65_000.0,
            kind="swap",
            ts=ts,
            intent_type="pool_buy_pressure",
            intent_stage="confirmed",
            intent_confidence=0.88,
            confirmation_score=0.84,
            pricing_status="exact",
            pricing_confidence=0.95,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata=metadata,
        )

    def _signal(self, event: Event, *, signal_id: str, stage: str = "confirm") -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.90,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="outcome_market_source_test",
            quality_score=0.88,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
            signal_id=signal_id,
        )
        context = {
            "pair_label": "ETH/USDC",
            "asset_symbol": "ETH",
            "asset_case_id": "asset_case:ETH:outcome_source",
            "asset_case_stage": stage,
            "lp_alert_stage": stage,
            "market_context_source": str(event.metadata.get("market_context_source") or "unavailable"),
            "market_context_venue": str(event.metadata.get("market_context_venue") or ""),
            "perp_mark_price": event.metadata.get("perp_mark_price"),
            "perp_index_price": event.metadata.get("perp_index_price"),
            "perp_last_price": event.metadata.get("perp_last_price"),
            "spot_reference_price": event.metadata.get("spot_reference_price"),
        }
        signal.context.update(context)
        signal.metadata.update(context)
        event.metadata.update(context)
        return signal

    def test_okx_mark_price_completes_30s_window(self) -> None:
        anchor_event = self._event(
            ts=1_710_000_000,
            quote_amount=1_000.0,
            market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp", "perp_mark_price": 3_000.0},
        )
        anchor_signal = self._signal(anchor_event, signal_id="sig:anchor")
        anchor_record = self.state_manager.record_lp_outcome_alert(anchor_event, anchor_signal)

        follow_event = self._event(
            ts=1_710_000_040,
            quote_amount=1_010.0,
            market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp", "perp_mark_price": 3_030.0},
        )
        follow_signal = self._signal(follow_event, signal_id="sig:follow")
        self.state_manager.record_lp_outcome_alert(follow_event, follow_signal)

        updated = self.state_manager.get_lp_outcome_record(str(anchor_record["record_id"]))
        self.assertEqual("okx_mark", updated["outcome_price_source"])
        self.assertEqual("completed", ((updated["outcome_windows"] or {}).get("30s") or {}).get("status"))
        self.assertEqual("okx_mark", ((updated["outcome_windows"] or {}).get("30s") or {}).get("price_source"))

    def test_okx_index_fallback_is_used_when_mark_missing(self) -> None:
        anchor_event = self._event(
            ts=1_710_000_100,
            quote_amount=1_000.0,
            market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp", "perp_index_price": 3_000.0},
        )
        anchor_signal = self._signal(anchor_event, signal_id="sig:index_anchor")
        anchor_record = self.state_manager.record_lp_outcome_alert(anchor_event, anchor_signal)

        follow_event = self._event(
            ts=1_710_000_140,
            quote_amount=1_010.0,
            market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp", "perp_index_price": 3_018.0},
        )
        follow_signal = self._signal(follow_event, signal_id="sig:index_follow")
        self.state_manager.record_lp_outcome_alert(follow_event, follow_signal)

        updated = self.state_manager.get_lp_outcome_record(str(anchor_record["record_id"]))
        self.assertEqual("okx_index", updated["outcome_price_source"])
        self.assertEqual("okx_index", ((updated["outcome_windows"] or {}).get("30s") or {}).get("price_source"))

    def test_pool_quote_proxy_fallback_still_works_without_market_context(self) -> None:
        anchor_event = self._event(ts=1_710_000_200, quote_amount=1_000.0, market_context={"market_context_source": "unavailable"})
        anchor_signal = self._signal(anchor_event, signal_id="sig:pool_anchor")
        anchor_record = self.state_manager.record_lp_outcome_alert(anchor_event, anchor_signal)

        follow_event = self._event(ts=1_710_000_240, quote_amount=1_025.0, market_context={"market_context_source": "unavailable"})
        follow_signal = self._signal(follow_event, signal_id="sig:pool_follow")
        self.state_manager.record_lp_outcome_alert(follow_event, follow_signal)

        updated = self.state_manager.get_lp_outcome_record(str(anchor_record["record_id"]))
        self.assertEqual("pool_quote_proxy", updated["outcome_price_source"])
        self.assertEqual("pool_quote_proxy", ((updated["outcome_windows"] or {}).get("30s") or {}).get("price_source"))

    def test_unavailable_price_writes_failure_reason(self) -> None:
        anchor_event = self._event(ts=1_710_000_300, quote_amount=0.0, market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp"})
        anchor_signal = self._signal(anchor_event, signal_id="sig:none_anchor")
        anchor_record = self.state_manager.record_lp_outcome_alert(anchor_event, anchor_signal)

        follow_event = self._event(ts=1_710_000_340, quote_amount=0.0, market_context={"market_context_source": "live_public", "market_context_venue": "okx_perp"})
        follow_signal = self._signal(follow_event, signal_id="sig:none_follow")
        self.state_manager.record_lp_outcome_alert(follow_event, follow_signal)

        updated = self.state_manager.get_lp_outcome_record(str(anchor_record["record_id"]))
        self.assertEqual("unavailable", updated["outcome_price_source"])
        self.assertIn(updated["outcome_window_status"], {"unavailable", "expired"})
        self.assertTrue(updated["outcome_failure_reason"])


if __name__ == "__main__":
    unittest.main()
