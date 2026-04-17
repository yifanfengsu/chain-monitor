import os
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:unit-test-token")
os.environ.setdefault("CHAT_ID", "0")

from archive_store import ArchiveStore
from market_context_adapter import LiveMarketContextAdapter
from market_data_clients import BinancePublicMarketClient, MarketDataClientError, candidate_symbols
from quality_reports import build_market_context_health_report


class _RecordingFetcher:
    def __init__(self, mapping: dict[str, object]) -> None:
        self.mapping = dict(mapping)
        self.calls: list[str] = []

    def __call__(self, url: str, timeout_sec: float) -> object:
        del timeout_sec
        self.calls.append(url)
        response = self.mapping.get(url)
        if isinstance(response, Exception):
            raise response
        if response is None:
            raise AssertionError(f"unexpected url: {url}")
        return response


class _StubClient:
    def __init__(self, payload: dict | None = None, error: Exception | None = None) -> None:
        self.payload = payload or {}
        self.error = error

    def fetch_market_context(self, token_or_pair: str | None, alert_ts: int | None = None) -> dict:
        del token_or_pair, alert_ts
        if self.error is not None:
            raise self.error
        return dict(self.payload)


class MarketContextDiagnosticsTests(unittest.TestCase):
    def test_binance_public_success_records_requested_and_resolved_symbol(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT": {
                    "symbol": "ETHUSDT",
                    "markPrice": "3511.5",
                    "indexPrice": "3510.9",
                    "lastFundingRate": "0.00012",
                    "time": 1_710_000_360_000,
                },
                "https://fapi.binance.com/fapi/v1/ticker/price?symbol=ETHUSDT": {
                    "symbol": "ETHUSDT",
                    "price": "3512.0",
                },
                "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT": {
                    "symbol": "ETHUSDT",
                    "price": "3511.2",
                },
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

        payload = client.fetch_market_context("ETH/USDC", alert_ts=1_710_000_300)

        self.assertEqual("ETHUSDC", payload["market_context_requested_symbol"])
        self.assertEqual("ETHUSDT", payload["market_context_resolved_symbol"])
        self.assertEqual("binance_perp", payload["market_context_venue"])
        self.assertTrue(payload["market_context_attempts"])
        self.assertTrue(payload["market_context_endpoint"].endswith("limit=8"))

    def test_live_adapter_bybit_secondary_fallback_keeps_attempt_log(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(
                    error=MarketDataClientError(
                        "timeout",
                        stage="premium_index",
                        venue="binance_perp",
                        symbol="ETHUSDT",
                        endpoint="https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
                        latency_ms=250,
                        attempts=[
                            {
                                "venue": "binance_perp",
                                "symbol": "ETHUSDT",
                                "requested_symbol": "ETHUSDC",
                                "stage": "premium_index",
                                "endpoint": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
                                "status": "failure",
                                "failure_reason": "timeout",
                                "failure_stage": "premium_index",
                                "http_status": None,
                                "latency_ms": 250,
                            }
                        ],
                    )
                ),
                "bybit_perp": _StubClient(
                    payload={
                        "market_context_source": "live_public",
                        "market_context_venue": "bybit_perp",
                        "market_context_requested_symbol": "ETHUSDC",
                        "market_context_resolved_symbol": "ETHUSDT",
                        "market_context_endpoint": "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDT",
                        "market_context_latency_ms": 120,
                        "market_context_attempts": [
                            {
                                "venue": "bybit_perp",
                                "symbol": "ETHUSDT",
                                "requested_symbol": "ETHUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDT",
                                "status": "success",
                                "failure_reason": "",
                                "failure_stage": "",
                                "http_status": None,
                                "latency_ms": 120,
                            }
                        ],
                        "perp_last_price": 3512.1,
                        "perp_mark_price": 3511.4,
                        "perp_index_price": 3510.8,
                        "spot_reference_price": 3511.0,
                        "market_move_before_alert_30s": 0.0010,
                        "market_move_before_alert_60s": 0.0015,
                        "market_move_after_alert_60s": 0.0042,
                    }
                ),
            },
            primary_venue="binance_perp",
            secondary_venue="bybit_perp",
        )

        payload = adapter.get_market_context("ETH/USDC", 1_710_000_300)

        self.assertEqual("live_public", payload["market_context_source"])
        self.assertEqual("bybit_perp", payload["market_context_venue"])
        self.assertEqual("binance_perp", payload["market_context_primary_venue"])
        self.assertEqual("bybit_perp", payload["market_context_secondary_venue"])
        self.assertEqual("ETHUSDT", payload["market_context_resolved_symbol"])
        self.assertEqual(2, len(payload["market_context_attempts"]))
        self.assertEqual("timeout", payload["market_context_attempts"][0]["failure_reason"])

    def test_timeout_http_and_malformed_failures_return_unavailable_with_reason(self) -> None:
        http_error = MarketDataClientError(
            "http_503",
            stage="premium_index",
            venue="binance_perp",
            symbol="BTCUSDT",
            http_status=503,
            endpoint="https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
            latency_ms=180,
            attempts=[
                {
                    "venue": "binance_perp",
                    "symbol": "BTCUSDT",
                    "requested_symbol": "BTCUSDC",
                    "stage": "premium_index",
                    "endpoint": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT",
                    "status": "failure",
                    "failure_reason": "http_503",
                    "failure_stage": "premium_index",
                    "http_status": 503,
                    "latency_ms": 180,
                }
            ],
        )
        malformed = MarketDataClientError(
            "malformed_payload",
            stage="perp_ticker",
            venue="bybit_perp",
            symbol="BTCUSDT",
            endpoint="https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT",
            latency_ms=90,
            attempts=[
                {
                    "venue": "bybit_perp",
                    "symbol": "BTCUSDT",
                    "requested_symbol": "BTCUSDC",
                    "stage": "perp_ticker",
                    "endpoint": "https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT",
                    "status": "failure",
                    "failure_reason": "malformed_payload",
                    "failure_stage": "perp_ticker",
                    "http_status": None,
                    "latency_ms": 90,
                }
            ],
        )
        adapter = LiveMarketContextAdapter(
            clients={
                "binance_perp": _StubClient(error=http_error),
                "bybit_perp": _StubClient(error=malformed),
            },
            primary_venue="binance_perp",
            secondary_venue="bybit_perp",
        )

        payload = adapter.get_market_context("BTC/USDC", 1_710_000_300)

        self.assertEqual("unavailable", payload["market_context_source"])
        self.assertEqual("malformed_payload", payload["market_context_failure_reason"])
        self.assertEqual("perp_ticker", payload["market_context_failure_stage"])
        self.assertEqual("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT", payload["market_context_endpoint"])

    def test_symbol_fallback_prefers_usdt_for_eth_btc_and_sol(self) -> None:
        self.assertEqual(["ETHUSDT", "ETHUSDC"], candidate_symbols("ETH/USDC"))
        self.assertEqual(["BTCUSDT", "BTCUSDC"], candidate_symbols("BTC/USDC"))
        self.assertEqual(["SOLUSDT", "SOLUSDC"], candidate_symbols("SOL/USDC"))

    def test_market_context_health_report_reads_signal_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            archive = ArchiveStore(base_dir=temp_dir)
            archive.write_signal(
                {
                    "signal_archive_key": "sig-1",
                    "signal_id": "sig-1",
                    "market_context_source": "live_public",
                    "market_context_venue": "binance_perp",
                    "market_context_requested_symbol": "ETHUSDC",
                    "market_context_resolved_symbol": "ETHUSDT",
                    "market_context_attempts": [
                        {
                            "venue": "binance_perp",
                            "symbol": "ETHUSDT",
                            "requested_symbol": "ETHUSDC",
                            "stage": "premium_index",
                            "endpoint": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT",
                            "status": "success",
                            "failure_reason": "",
                            "failure_stage": "",
                            "http_status": None,
                            "latency_ms": 80,
                        }
                    ],
                },
                archive_ts=1_710_000_000,
            )
            archive.write_signal(
                {
                    "signal_archive_key": "sig-2",
                    "signal_id": "sig-2",
                    "market_context_source": "unavailable",
                    "market_context_venue": "bybit_perp",
                    "market_context_requested_symbol": "ETHUSDC",
                    "market_context_resolved_symbol": "",
                    "market_context_attempts": [
                        {
                            "venue": "bybit_perp",
                            "symbol": "ETHUSDT",
                            "requested_symbol": "ETHUSDC",
                            "stage": "perp_ticker",
                            "endpoint": "https://api.bybit.com/v5/market/tickers?category=linear&symbol=ETHUSDT",
                            "status": "failure",
                            "failure_reason": "timeout",
                            "failure_stage": "perp_ticker",
                            "http_status": None,
                            "latency_ms": 200,
                        }
                    ],
                },
                archive_ts=1_710_000_001,
            )

            report = build_market_context_health_report(base_dir=Path(temp_dir))

        self.assertEqual(2, report["signal_rows"])
        self.assertEqual(0.5, report["live_public_hit_rate"])
        self.assertEqual(0.5, report["unavailable_rate"])
        self.assertEqual(1, report["failure_reason_counts"]["timeout"])
        self.assertEqual("binance_perp", report["per_venue"][0]["venue"])


if __name__ == "__main__":
    unittest.main()
