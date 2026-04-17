import os
import unittest
from urllib.error import HTTPError

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from analyzer import BehaviorAnalyzer
from market_context_adapter import DEFAULT_MARKET_CONTEXT, FixtureMarketContextAdapter, LiveMarketContextAdapter, UnavailableMarketContextAdapter
from market_data_clients import BinancePublicMarketClient, BybitPublicMarketClient
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
from user_tiers import apply_user_tier_context


class _RecordingFetcher:
    def __init__(self, mapping: dict[str, object]) -> None:
        self.mapping = dict(mapping)
        self.calls: list[str] = []

    def __call__(self, url: str, timeout_sec: float) -> object:
        self.calls.append(url)
        response = self.mapping.get(url)
        if isinstance(response, Exception):
            raise response
        if response is None:
            raise AssertionError(f"unexpected url: {url}")
        return response


class _StubClient:
    def __init__(self, payload: dict | Exception) -> None:
        self.payload = payload

    def fetch_market_context(self, token_or_pair: str | None, alert_ts: int | None = None) -> dict:
        del token_or_pair, alert_ts
        if isinstance(self.payload, Exception):
            raise self.payload
        return dict(self.payload)


class LiveMarketContextAdapterTests(unittest.TestCase):
    def _event(self, *, token: str = "ETH", ts: int = 1_710_000_300) -> Event:
        return Event(
            tx_hash=f"0xlive{token}",
            address="0x1111111111111111111111111111111111111111",
            token=token,
            amount=1.0,
            side="买入",
            usd_value=25_000.0,
            kind="swap",
            ts=ts,
            intent_type="pool_buy_pressure",
            intent_stage="preliminary",
            intent_confidence=0.70,
            confirmation_score=0.38,
            pricing_status="exact",
            pricing_confidence=0.92,
            usd_value_available=True,
            strategy_role="lp_pool",
            metadata={
                "token_symbol": token,
                "raw": {
                    "lp_context": {
                        "pair_label": f"{token}/USDC",
                        "pool_label": f"{token}/USDC",
                        "base_token_symbol": token,
                        "quote_token_symbol": "USDC",
                    }
                },
            },
        )

    def _signal(self, event: Event, *, stage: str = "prealert") -> Signal:
        signal = Signal(
            type=event.intent_type,
            confidence=0.82,
            priority=1,
            tier="Tier 2",
            address=event.address,
            token=event.token,
            tx_hash=event.tx_hash,
            usd_value=float(event.usd_value or 0.0),
            reason="unit_test",
            quality_score=0.80,
            semantic="pool_trade_pressure",
            intent_type=event.intent_type,
            intent_stage=event.intent_stage,
            confirmation_score=event.confirmation_score,
            pricing_confidence=event.pricing_confidence,
            delivery_class="observe",
            delivery_reason="unit_test",
        )
        signal.context.update(
            {
                "user_tier": "research",
                "pair_label": f"{event.token}/USDC",
                "lp_event": True,
                "asset_case_label": event.token,
                "asset_case_id": "asset_case:test",
                "asset_case_supporting_pair_count": 2,
                "lp_alert_stage": stage,
                "lp_stage_badge": "预警",
                "lp_state_label": "买盘建立中",
                "lp_market_read": "先手观察｜需看 60s 是否续单",
                "lp_followup_check": "60s：是否跨池共振 / 是否续单",
                "lp_invalidation": "续单消失 / 快速反向池流",
                "lp_followup_required": True,
                "lp_scan_path": "secondary",
                "lp_detect_latency_ms": 800,
            }
        )
        signal.metadata.update(signal.context)
        return signal

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

    def test_binance_public_client_builds_requests_and_parses_context(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT": {
                    "markPrice": "3511.5",
                    "indexPrice": "3510.9",
                    "lastFundingRate": "0.00012",
                    "time": 1_710_000_360_000,
                },
                "https://fapi.binance.com/fapi/v1/ticker/price?symbol=ETHUSDT": {"price": "3512.0"},
                "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT": {"price": "3511.2"},
                "https://fapi.binance.com/fapi/v1/klines?symbol=ETHUSDT&interval=1m&limit=8": [
                    [1_710_000_180_000, "3490", "3500", "3488", "3495", "0", 1_710_000_239_999],
                    [1_710_000_240_000, "3495", "3502", "3494", "3500", "0", 1_710_000_299_999],
                    [1_710_000_300_000, "3500", "3512", "3499", "3510", "0", 1_710_000_359_999],
                    [1_710_000_360_000, "3510", "3514", "3508", "3512", "0", 1_710_000_419_999],
                ],
            }
        )
        client = BinancePublicMarketClient(
            base_url="https://fapi.binance.com",
            spot_base_url="https://api.binance.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )

        payload = client.fetch_market_context("ETH", alert_ts=1_710_000_300)

        self.assertEqual("live_public", payload["market_context_source"])
        self.assertEqual("binance_perp", payload["market_context_venue"])
        self.assertAlmostEqual(3512.0, payload["perp_last_price"])
        self.assertAlmostEqual(3511.2, payload["spot_reference_price"])
        self.assertEqual("positive", payload["funding_direction"])
        self.assertGreater(payload["market_move_after_alert_60s"], 0.002)
        self.assertIn("/fapi/v1/premiumIndex?symbol=ETHUSDT", fetcher.calls[0])
        self.assertEqual(4, len(fetcher.calls))

    def test_bybit_public_client_parses_linear_payload(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDC": OSError("missing usdc perp"),
                "https://api.bybit.com/v5/market/tickers?category=spot&symbol=ETHUSDC": OSError("missing usdc spot"),
                "https://api.bybit.com/v5/market/kline?category=linear&symbol=ETHUSDC&interval=1&limit=8": OSError("missing usdc kline"),
                "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDT": {
                    "time": "1710000360000",
                    "result": {
                        "list": [
                            {
                                "lastPrice": "3512.1",
                                "markPrice": "3511.4",
                                "indexPrice": "3510.8",
                                "fundingRate": "-0.00010",
                            }
                        ]
                    },
                },
                "https://api.bybit.com/v5/market/tickers?category=spot&symbol=ETHUSDT": {
                    "time": "1710000360000",
                    "result": {"list": [{"lastPrice": "3511.0"}]},
                },
                "https://api.bybit.com/v5/market/kline?category=linear&symbol=ETHUSDT&interval=1&limit=8": {
                    "result": {
                        "list": [
                            ["1710000360000", "3510", "3514", "3508", "3512", "0", "0"],
                            ["1710000300000", "3500", "3512", "3499", "3510", "0", "0"],
                            ["1710000240000", "3495", "3502", "3494", "3500", "0", "0"],
                        ]
                    }
                },
            }
        )
        client = BybitPublicMarketClient(
            base_url="https://api.bybit.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )

        payload = client.fetch_market_context("ETH/USDC", alert_ts=1_710_000_300)

        self.assertEqual("negative", payload["funding_direction"])
        self.assertAlmostEqual(3511.0, payload["spot_reference_price"])
        self.assertIsNotNone(payload["mark_index_spread_bps"])

    def test_live_adapter_timeout_and_malformed_payload_fall_back_to_unavailable(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(TimeoutError("timeout")),
                "bybit_perp": _StubClient(ValueError("bad")),
            },
            primary_venue="binance_perp",
            secondary_venue="bybit_perp",
        )

        payload = adapter.get_market_context("ETH", 1_710_000_300, venue="binance_perp")

        self.assertEqual("unavailable", payload["market_context_source"])
        self.assertEqual("", payload["alert_relative_timing"])

    def test_live_adapter_http_error_falls_back_to_unavailable(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT": HTTPError(
                    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
                    503,
                    "service unavailable",
                    hdrs=None,
                    fp=None,
                ),
                "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDC": HTTPError(
                    "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDC",
                    503,
                    "service unavailable",
                    hdrs=None,
                    fp=None,
                ),
            }
        )
        client = BinancePublicMarketClient(
            base_url="https://fapi.binance.com",
            spot_base_url="https://api.binance.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )
        adapter = LiveMarketContextAdapter(clients={"binance_perp": client}, primary_venue="binance_perp", secondary_venue="")

        payload = adapter.get_market_context("ETH", 1_710_000_300, venue="binance_perp")

        self.assertEqual("unavailable", payload["market_context_source"])

    def test_fixture_live_and_unavailable_modes_share_same_interface_keys(self) -> None:
        fixture = FixtureMarketContextAdapter(
            {"binance_perp:ETH": {"perp_last_price": 3512.0, "market_move_before_alert_60s": 0.001}}
        )
        live = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(
                    {
                        "market_context_source": "live_public",
                        "market_context_venue": "binance_perp",
                        "perp_last_price": 3512.0,
                        "perp_mark_price": 3511.5,
                        "perp_index_price": 3510.9,
                        "spot_reference_price": 3511.2,
                        "funding_estimate": 0.00012,
                        "market_move_before_alert_30s": 0.0010,
                        "market_move_before_alert_60s": 0.0015,
                        "market_move_after_alert_60s": 0.0060,
                    }
                )
            }
        )
        unavailable = UnavailableMarketContextAdapter()

        fixture_payload = fixture.get_market_context("ETH", 1_710_000_300)
        live_payload = live.get_market_context("ETH", 1_710_000_300)
        unavailable_payload = unavailable.get_market_context("ETH", 1_710_000_300)

        self.assertEqual(set(DEFAULT_MARKET_CONTEXT), set(fixture_payload))
        self.assertEqual(set(DEFAULT_MARKET_CONTEXT), set(live_payload))
        self.assertEqual(set(DEFAULT_MARKET_CONTEXT), set(unavailable_payload))

    def test_message_renders_live_public_contract_view_for_research(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(
                    {
                        "market_context_source": "live_public",
                        "market_context_venue": "binance_perp",
                        "perp_last_price": 3512.0,
                        "perp_mark_price": 3511.5,
                        "perp_index_price": 3510.9,
                        "spot_reference_price": 3511.2,
                        "basis_bps": 8.4,
                        "market_move_before_alert_30s": 0.0010,
                        "market_move_before_alert_60s": 0.0018,
                        "market_move_after_alert_60s": 0.0062,
                    }
                )
            }
        )
        pipeline = self._pipeline(adapter)
        event = self._event()
        signal = self._signal(event)

        pipeline._annotate_market_context(event, signal)
        apply_user_tier_context(event, signal, {"user_tier": "research"})
        message = format_signal_message(signal, event)

        self.assertIn("合约视角：领先", message)
        self.assertIn("基差", message)

    def test_unavailable_context_does_not_invent_contract_story(self) -> None:
        pipeline = self._pipeline(UnavailableMarketContextAdapter())
        event = self._event()
        signal = self._signal(event)

        pipeline._annotate_market_context(event, signal)
        apply_user_tier_context(event, signal, {"user_tier": "research"})
        message = format_signal_message(signal, event)

        self.assertNotIn("合约视角：", message)


if __name__ == "__main__":
    unittest.main()
