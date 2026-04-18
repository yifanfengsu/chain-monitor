import json
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError

from market_context_adapter import LiveMarketContextAdapter
from market_data_clients import (
    KrakenFuturesPublicMarketClient,
    MarketDataClientError,
    OKXPublicMarketClient,
    candidate_symbols_for_venue,
)
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


class MarketContextOkxKrakenTests(unittest.TestCase):
    def test_okx_public_success_uses_major_symbol_fallback(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP": {
                    "data": [{"instId": "ETH-USDT-SWAP", "last": "3512.4", "ts": "1710000360000"}]
                },
                "https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=ETH-USDT-SWAP": {
                    "data": [{"instId": "ETH-USDT-SWAP", "markPx": "3511.8", "ts": "1710000360000"}]
                },
                "https://www.okx.com/api/v5/public/funding-rate?instId=ETH-USDT-SWAP": {
                    "data": [{"instId": "ETH-USDT-SWAP", "fundingRate": "0.00010", "nextFundingRate": "0.00012"}]
                },
                "https://www.okx.com/api/v5/market/index-tickers?instId=ETH-USDT": {
                    "data": [{"instId": "ETH-USDT", "idxPx": "3510.9", "ts": "1710000360000"}]
                },
                "https://www.okx.com/api/v5/market/history-candles?instId=ETH-USDT-SWAP&bar=1m&limit=8": {
                    "data": [
                        ["1710000360000", "3510", "3514", "3508", "3512", "0", "0", "0", "1"],
                        ["1710000300000", "3500", "3512", "3499", "3510", "0", "0", "0", "1"],
                        ["1710000240000", "3495", "3502", "3494", "3500", "0", "0", "0", "1"],
                    ]
                },
                "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT": {
                    "data": [{"instId": "ETH-USDT", "last": "3511.1", "ts": "1710000360000"}]
                },
            }
        )
        client = OKXPublicMarketClient(
            base_url="https://www.okx.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )

        payload = client.fetch_market_context("ETH/USDC", alert_ts=1_710_000_300)

        self.assertEqual("live_public", payload["market_context_source"])
        self.assertEqual("okx_perp", payload["market_context_venue"])
        self.assertEqual("ETHUSDC", payload["market_context_requested_symbol"])
        self.assertEqual("ETH-USDT-SWAP", payload["market_context_resolved_symbol"])
        self.assertAlmostEqual(3512.4, payload["perp_last_price"])
        self.assertAlmostEqual(3511.8, payload["perp_mark_price"])
        self.assertAlmostEqual(3510.9, payload["perp_index_price"])
        self.assertAlmostEqual(3511.1, payload["spot_reference_price"])
        self.assertEqual("positive", payload["funding_direction"])
        self.assertTrue(any("ETH-USDT-SWAP" in url for url in fetcher.calls))

    def test_kraken_futures_public_success_uses_pf_symbol(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_ETHUSD": {
                    "tickers": [
                        {
                            "symbol": "PF_ETHUSD",
                            "last": "3512.0",
                            "markPrice": "3511.5",
                            "indexPrice": "3511.0",
                            "fundingRatePrediction": "-0.00020",
                            "time": "1710000360000",
                        }
                    ]
                },
                "https://futures.kraken.com/api/history/v2/historical-funding-rates?symbol=PF_ETHUSD": {
                    "rates": [{"fundingRate": "-0.00021"}]
                },
                "https://futures.kraken.com/api/charts/v1/trade?symbol=PF_ETHUSD&resolution=1m": {
                    "candles": [
                        {"time": 1710000240000, "open": "3495", "close": "3500"},
                        {"time": 1710000300000, "open": "3500", "close": "3510"},
                        {"time": 1710000360000, "open": "3510", "close": "3512"},
                    ]
                },
            }
        )
        client = KrakenFuturesPublicMarketClient(
            base_url="https://futures.kraken.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )

        payload = client.fetch_market_context("ETH/USDC", alert_ts=1_710_000_300)

        self.assertEqual("kraken_futures", payload["market_context_venue"])
        self.assertEqual("PF_ETHUSD", payload["market_context_resolved_symbol"])
        self.assertEqual("negative", payload["funding_direction"])
        self.assertAlmostEqual(3511.0, payload["perp_index_price"])
        self.assertIsNone(payload["spot_reference_price"])

    def test_okx_fail_then_kraken_success_keeps_fallback_chain(self) -> None:
        adapter = LiveMarketContextAdapter(
            clients={
                "okx_perp": _StubClient(
                    error=MarketDataClientError(
                        "timeout",
                        stage="perp_ticker",
                        venue="okx_perp",
                        symbol="BTC-USDT-SWAP",
                        endpoint="https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP",
                        latency_ms=180,
                        attempts=[
                            {
                                "venue": "okx_perp",
                                "symbol": "BTC-USDT-SWAP",
                                "requested_symbol": "BTCUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP",
                                "status": "failure",
                                "failure_reason": "timeout",
                                "failure_stage": "perp_ticker",
                                "http_status": None,
                                "latency_ms": 180,
                            }
                        ],
                    )
                ),
                "kraken_futures": _StubClient(
                    payload={
                        "market_context_source": "live_public",
                        "market_context_venue": "kraken_futures",
                        "market_context_requested_symbol": "BTCUSDC",
                        "market_context_resolved_symbol": "PF_XBTUSD",
                        "market_context_endpoint": "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_XBTUSD",
                        "market_context_latency_ms": 95,
                        "market_context_attempts": [
                            {
                                "venue": "kraken_futures",
                                "symbol": "PF_XBTUSD",
                                "requested_symbol": "BTCUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_XBTUSD",
                                "status": "success",
                                "failure_reason": "",
                                "failure_stage": "",
                                "http_status": None,
                                "latency_ms": 95,
                            }
                        ],
                        "perp_last_price": 64000.0,
                        "perp_mark_price": 63990.0,
                        "perp_index_price": 63970.0,
                        "spot_reference_price": None,
                        "market_move_before_alert_30s": 0.0008,
                        "market_move_before_alert_60s": 0.0012,
                        "market_move_after_alert_60s": 0.0035,
                    }
                ),
            },
            primary_venue="okx_perp",
            secondary_venue="kraken_futures",
        )

        payload = adapter.get_market_context("BTC/USDC", 1_710_000_300, venue="okx_perp")

        self.assertEqual("live_public", payload["market_context_source"])
        self.assertEqual("kraken_futures", payload["market_context_venue"])
        self.assertEqual(["okx_perp", "kraken_futures"], payload["market_context_attempted_venues"])
        self.assertEqual(2, len(payload["market_context_attempts"]))
        self.assertTrue(payload["market_context_fallback_chain"])

    def test_timeout_5xx_and_malformed_payload_end_unavailable_with_diagnostics(self) -> None:
        okx_error = MarketDataClientError(
            "http_503",
            stage="perp_ticker",
            venue="okx_perp",
            symbol="SOL-USDT-SWAP",
            endpoint="https://www.okx.com/api/v5/market/ticker?instId=SOL-USDT-SWAP",
            http_status=503,
            latency_ms=210,
            attempts=[
                {
                    "venue": "okx_perp",
                    "symbol": "SOL-USDT-SWAP",
                    "requested_symbol": "SOLUSDC",
                    "stage": "perp_ticker",
                    "endpoint": "https://www.okx.com/api/v5/market/ticker?instId=SOL-USDT-SWAP",
                    "status": "failure",
                    "failure_reason": "http_503",
                    "failure_stage": "perp_ticker",
                    "http_status": 503,
                    "latency_ms": 210,
                }
            ],
        )
        kraken_error = MarketDataClientError(
            "malformed_payload",
            stage="perp_ticker",
            venue="kraken_futures",
            symbol="PF_SOLUSD",
            endpoint="https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_SOLUSD",
            latency_ms=90,
            attempts=[
                {
                    "venue": "kraken_futures",
                    "symbol": "PF_SOLUSD",
                    "requested_symbol": "SOLUSDC",
                    "stage": "perp_ticker",
                    "endpoint": "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_SOLUSD",
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
                "okx_perp": _StubClient(error=okx_error),
                "kraken_futures": _StubClient(error=kraken_error),
            },
            primary_venue="okx_perp",
            secondary_venue="kraken_futures",
        )

        payload = adapter.get_market_context("SOL/USDC", 1_710_000_300, venue="okx_perp")

        self.assertEqual("unavailable", payload["market_context_source"])
        self.assertEqual("malformed_payload", payload["market_context_failure_reason"])
        self.assertEqual("https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_SOLUSD", payload["market_context_endpoint"])
        self.assertEqual(["okx_perp", "kraken_futures"], payload["market_context_attempted_venues"])

    def test_major_symbol_fallbacks_cover_eth_btc_and_sol(self) -> None:
        self.assertEqual(
            ["ETH-USDT-SWAP", "ETH-USDC-SWAP", "ETH-USD-SWAP"],
            candidate_symbols_for_venue("ETH/USDC", "okx_perp"),
        )
        self.assertEqual(
            ["BTC-USDT-SWAP", "BTC-USDC-SWAP", "BTC-USD-SWAP"],
            candidate_symbols_for_venue("BTC/USDC", "okx_perp"),
        )
        self.assertEqual(
            ["SOL-USDT-SWAP", "SOL-USDC-SWAP", "SOL-USD-SWAP"],
            candidate_symbols_for_venue("SOL/USDC", "okx_perp"),
        )
        self.assertEqual(
            ["PF_ETHUSD", "PI_ETHUSD"],
            candidate_symbols_for_venue("ETH/USDC", "kraken_futures"),
        )
        self.assertEqual(
            ["PF_XBTUSD", "PI_XBTUSD"],
            candidate_symbols_for_venue("BTC/USDC", "kraken_futures"),
        )
        self.assertEqual(
            ["PF_SOLUSD", "PI_SOLUSD"],
            candidate_symbols_for_venue("SOL/USDC", "kraken_futures"),
        )

    def test_market_context_health_reports_okx_and_kraken_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            signals_dir = Path(temp_dir) / "signals"
            signals_dir.mkdir(parents=True, exist_ok=True)
            path = signals_dir / "2026-04-18.ndjson"
            rows = [
                {
                    "data": {
                        "market_context_source": "live_public",
                        "market_context_venue": "okx_perp",
                        "market_context_requested_symbol": "ETHUSDC",
                        "market_context_resolved_symbol": "ETH-USDT-SWAP",
                        "market_context_attempts": [
                            {
                                "venue": "okx_perp",
                                "symbol": "ETH-USDT-SWAP",
                                "requested_symbol": "ETHUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP",
                                "status": "success",
                                "failure_reason": "",
                                "failure_stage": "",
                                "http_status": None,
                                "latency_ms": 45,
                            }
                        ],
                    }
                },
                {
                    "data": {
                        "market_context_source": "unavailable",
                        "market_context_venue": "",
                        "market_context_requested_symbol": "BTCUSDC",
                        "market_context_resolved_symbol": "",
                        "market_context_failure_reason": "timeout",
                        "market_context_attempts": [
                            {
                                "venue": "okx_perp",
                                "symbol": "BTC-USDT-SWAP",
                                "requested_symbol": "BTCUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP",
                                "status": "failure",
                                "failure_reason": "timeout",
                                "failure_stage": "perp_ticker",
                                "http_status": None,
                                "latency_ms": 160,
                            },
                            {
                                "venue": "kraken_futures",
                                "symbol": "PF_XBTUSD",
                                "requested_symbol": "BTCUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_XBTUSD",
                                "status": "failure",
                                "failure_reason": "http_503",
                                "failure_stage": "perp_ticker",
                                "http_status": 503,
                                "latency_ms": 120,
                            },
                        ],
                    }
                },
            ]
            with path.open("w", encoding="utf-8") as fp:
                for row in rows:
                    fp.write(json.dumps(row, ensure_ascii=False) + "\n")

            report = build_market_context_health_report(base_dir=temp_dir)

        self.assertEqual(2, report["signal_rows"])
        self.assertEqual(3, report["total_attempts"])
        self.assertAlmostEqual(0.5, report["live_public_hit_rate"])
        self.assertAlmostEqual(0.5, report["unavailable_rate"])
        self.assertTrue(any(row["venue"] == "okx_perp" for row in report["per_venue"]))
        self.assertTrue(any(row["venue"] == "kraken_futures" for row in report["per_venue"]))
        self.assertTrue(any(row["endpoint"].startswith("https://www.okx.com") for row in report["per_endpoint"]))
        self.assertTrue(any(row["endpoint"].startswith("https://futures.kraken.com") for row in report["per_endpoint"]))
        self.assertTrue(any(row["symbol"] == "ETHUSDC" for row in report["per_requested_symbol"]))
        self.assertTrue(any(row["symbol"] == "ETH-USDT-SWAP" for row in report["per_resolved_symbol"]))
        self.assertTrue(report["top_failure_reasons"])

    def test_okx_http_error_and_kraken_timeout_gracefully_fall_back(self) -> None:
        fetcher = _RecordingFetcher(
            {
                "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP": HTTPError(
                    "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT-SWAP",
                    503,
                    "service unavailable",
                    hdrs=None,
                    fp=None,
                ),
                "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDC-SWAP": HTTPError(
                    "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDC-SWAP",
                    503,
                    "service unavailable",
                    hdrs=None,
                    fp=None,
                ),
                "https://www.okx.com/api/v5/market/ticker?instId=ETH-USD-SWAP": HTTPError(
                    "https://www.okx.com/api/v5/market/ticker?instId=ETH-USD-SWAP",
                    503,
                    "service unavailable",
                    hdrs=None,
                    fp=None,
                ),
            }
        )
        okx_client = OKXPublicMarketClient(
            base_url="https://www.okx.com",
            fetcher=fetcher,
            clock=lambda: 1_710_000_360,
        )
        adapter = LiveMarketContextAdapter(
            clients={
                "okx_perp": okx_client,
                "kraken_futures": _StubClient(
                    error=MarketDataClientError(
                        "timeout",
                        stage="perp_ticker",
                        venue="kraken_futures",
                        symbol="PF_ETHUSD",
                        endpoint="https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_ETHUSD",
                        latency_ms=140,
                        attempts=[
                            {
                                "venue": "kraken_futures",
                                "symbol": "PF_ETHUSD",
                                "requested_symbol": "ETHUSDC",
                                "stage": "perp_ticker",
                                "endpoint": "https://futures.kraken.com/derivatives/api/v3/tickers?symbol=PF_ETHUSD",
                                "status": "failure",
                                "failure_reason": "timeout",
                                "failure_stage": "perp_ticker",
                                "http_status": None,
                                "latency_ms": 140,
                            }
                        ],
                    )
                ),
            },
            primary_venue="okx_perp",
            secondary_venue="kraken_futures",
        )

        payload = adapter.get_market_context("ETH/USDC", 1_710_000_300, venue="okx_perp")

        self.assertEqual("unavailable", payload["market_context_source"])
        self.assertEqual("timeout", payload["market_context_failure_reason"])
        self.assertTrue(payload["market_context_attempts"])


if __name__ == "__main__":
    unittest.main()
