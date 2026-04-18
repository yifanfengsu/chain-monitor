from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import json
import time
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from lp_product_helpers import canonical_asset_symbol, normalize_symbol


JsonFetcher = Callable[[str, float], object]
MAJOR_PERP_QUOTES = {
    "ETH": ["USDT", "USDC"],
    "BTC": ["USDT", "USDC"],
    "SOL": ["USDT", "USDC"],
}
OKX_PERP_SYMBOLS = {
    "ETH": ["ETH-USDT-SWAP", "ETH-USDC-SWAP", "ETH-USD-SWAP"],
    "BTC": ["BTC-USDT-SWAP", "BTC-USDC-SWAP", "BTC-USD-SWAP"],
    "SOL": ["SOL-USDT-SWAP", "SOL-USDC-SWAP", "SOL-USD-SWAP"],
}
KRAKEN_FUTURES_SYMBOLS = {
    "ETH": ["PF_ETHUSD", "PI_ETHUSD"],
    "BTC": ["PF_XBTUSD", "PI_XBTUSD"],
    "SOL": ["PF_SOLUSD", "PI_SOLUSD"],
}
KRAKEN_BASE_SYMBOLS = {
    "BTC": "XBT",
    "ETH": "ETH",
    "SOL": "SOL",
}


@dataclass
class MarketDataClientError(RuntimeError):
    reason: str
    stage: str = ""
    venue: str = ""
    symbol: str = ""
    requested_symbol: str = ""
    http_status: int | None = None
    endpoint: str = ""
    latency_ms: int | None = None
    attempts: list[dict] | None = None

    def __str__(self) -> str:
        return self.reason

    def to_diagnostic(self) -> dict:
        return {
            "failure_reason": str(self.reason or ""),
            "failure_stage": str(self.stage or ""),
            "http_status": self.http_status,
            "endpoint": str(self.endpoint or ""),
            "latency_ms": self.latency_ms,
            "venue": str(self.venue or ""),
            "symbol": str(self.symbol or ""),
            "requested_symbol": str(self.requested_symbol or ""),
        }


def _as_float(value) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percent_move(target_price: float | None, anchor_price: float | None) -> float | None:
    if target_price is None or anchor_price is None or float(anchor_price) <= 0:
        return None
    return round((float(target_price) / float(anchor_price)) - 1.0, 6)


def _spread_bps(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or float(right) == 0.0:
        return None
    return round(((float(left) / float(right)) - 1.0) * 10_000.0, 3)


def _funding_direction(funding_rate: float | None) -> str | None:
    if funding_rate is None:
        return None
    if funding_rate > 0:
        return "positive"
    if funding_rate < 0:
        return "negative"
    return "flat"


def _normalize_quote_symbol(value: str | None) -> str:
    normalized = normalize_symbol(value)
    if normalized in {"USDC.E", "USDCE"}:
        return "USDC"
    return normalized


def requested_symbol(token_or_pair: str | None) -> str:
    raw = str(token_or_pair or "").strip().upper()
    if not raw:
        return ""
    if "/" in raw:
        left, right = [normalize_symbol(part) for part in raw.split("/", 1)]
        return f"{canonical_asset_symbol(left)}{_normalize_quote_symbol(right)}"
    base_asset = canonical_asset_symbol(raw)
    quotes = MAJOR_PERP_QUOTES.get(base_asset) or ["USDT"]
    return f"{base_asset}{quotes[0]}"


def _split_requested_asset_quote(token_or_pair: str | None) -> tuple[str, str]:
    raw = str(token_or_pair or "").strip().upper()
    if not raw:
        return "", ""
    if "/" in raw:
        left, right = [normalize_symbol(part) for part in raw.split("/", 1)]
        return canonical_asset_symbol(left), _normalize_quote_symbol(right)
    return canonical_asset_symbol(raw), ""


def candidate_symbols(token_or_pair: str | None) -> list[str]:
    base_asset, pair_quote = _split_requested_asset_quote(token_or_pair)
    if not base_asset:
        return []
    ordered_quotes: list[str] = []
    for quote in (MAJOR_PERP_QUOTES.get(base_asset) or []):
        normalized = _normalize_quote_symbol(quote)
        if normalized and normalized not in ordered_quotes:
            ordered_quotes.append(normalized)
    for quote in [pair_quote, "USDT", "USDC"]:
        normalized = _normalize_quote_symbol(quote)
        if normalized and normalized not in ordered_quotes:
            ordered_quotes.append(normalized)
    return [f"{base_asset}{quote}" for quote in ordered_quotes if base_asset and quote]


def okx_candidate_symbols(token_or_pair: str | None) -> list[str]:
    base_asset, _ = _split_requested_asset_quote(token_or_pair)
    if not base_asset:
        return []
    configured = list(OKX_PERP_SYMBOLS.get(base_asset) or [])
    if configured:
        return configured
    return [f"{base_asset}-USDT-SWAP", f"{base_asset}-USD-SWAP"]


def kraken_futures_candidate_symbols(token_or_pair: str | None) -> list[str]:
    base_asset, _ = _split_requested_asset_quote(token_or_pair)
    if not base_asset:
        return []
    configured = list(KRAKEN_FUTURES_SYMBOLS.get(base_asset) or [])
    if configured:
        return configured
    kraken_base = KRAKEN_BASE_SYMBOLS.get(base_asset, base_asset)
    return [f"PF_{kraken_base}USD", f"PI_{kraken_base}USD"]


def candidate_symbols_for_venue(token_or_pair: str | None, venue: str | None) -> list[str]:
    normalized_venue = str(venue or "").strip().lower()
    if normalized_venue == "okx_perp":
        return okx_candidate_symbols(token_or_pair)
    if normalized_venue == "kraken_futures":
        return kraken_futures_candidate_symbols(token_or_pair)
    return candidate_symbols(token_or_pair)


class PublicMarketDataClient:
    venue = ""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_sec: float = 2.5,
        cache_ttl_sec: float = 2.0,
        retry_count: int = 1,
        fetcher: JsonFetcher | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_sec = max(float(timeout_sec or 0.0), 0.2)
        self.cache_ttl_sec = max(float(cache_ttl_sec or 0.0), 0.2)
        self.retry_count = max(int(retry_count or 0), 0)
        self._fetcher = fetcher or self._default_fetcher
        self._clock = clock or time.time
        self._cache: dict[str, tuple[float, dict]] = {}
        self._price_history: dict[str, deque[tuple[int, float]]] = defaultdict(lambda: deque(maxlen=256))

    def fetch_market_context(self, token_or_pair: str | None, alert_ts: int | None = None) -> dict:
        requested = requested_symbol(token_or_pair)
        attempts: list[dict] = []
        last_error: MarketDataClientError | None = None

        for symbol in self.candidate_symbols(token_or_pair):
            try:
                raw_payload = self._get_cached_raw_payload(symbol, attempts=attempts, requested_symbol=requested)
                return self._build_market_context(
                    symbol,
                    raw_payload,
                    int(alert_ts or 0),
                    requested_symbol=requested,
                    attempts=attempts,
                )
            except MarketDataClientError as exc:
                last_error = exc

        if last_error is not None:
            raise MarketDataClientError(
                str(last_error.reason or "market_symbol_unavailable"),
                stage=str(last_error.stage or "symbol_resolution"),
                venue=str(last_error.venue or self.venue),
                symbol=str(last_error.symbol or ""),
                requested_symbol=str(last_error.requested_symbol or requested),
                http_status=last_error.http_status,
                endpoint=str(last_error.endpoint or ""),
                latency_ms=last_error.latency_ms,
                attempts=[dict(item) for item in attempts],
            )
        raise MarketDataClientError(
            "market_symbol_unavailable",
            stage="symbol_resolution",
            venue=self.venue,
            requested_symbol=requested,
            attempts=[dict(item) for item in attempts],
        )

    def candidate_symbols(self, token_or_pair: str | None) -> list[str]:
        return candidate_symbols_for_venue(token_or_pair, self.venue)

    def _build_market_context(
        self,
        symbol: str,
        raw_payload: dict,
        alert_ts: int,
        *,
        requested_symbol: str,
        attempts: list[dict],
    ) -> dict:
        sampled_at = int(raw_payload.get("sampled_at") or self._clock())
        perp_last = _as_float(raw_payload.get("perp_last_price"))
        perp_mark = _as_float(raw_payload.get("perp_mark_price"))
        perp_index = _as_float(raw_payload.get("perp_index_price"))
        spot_reference = _as_float(raw_payload.get("spot_reference_price"))
        funding_estimate = _as_float(raw_payload.get("funding_estimate"))
        reference_price = perp_mark or perp_last or perp_index
        price_at_alert = self._historical_price(symbol, raw_payload, alert_ts, fallback_to_current=True) if alert_ts > 0 else reference_price
        move_before_30s = None
        move_before_60s = None
        move_after_60s = None
        move_after_300s = None
        if alert_ts > 0 and price_at_alert is not None:
            before_30 = self._historical_price(symbol, raw_payload, max(alert_ts - 30, 0))
            before_60 = self._historical_price(symbol, raw_payload, max(alert_ts - 60, 0))
            move_before_30s = _percent_move(price_at_alert, before_30)
            move_before_60s = _percent_move(price_at_alert, before_60)
            if sampled_at - alert_ts >= 60:
                move_after_60s = _percent_move(
                    self._historical_price(symbol, raw_payload, alert_ts + 60, fallback_to_current=True),
                    price_at_alert,
                )
            if sampled_at - alert_ts >= 300:
                move_after_300s = _percent_move(
                    self._historical_price(symbol, raw_payload, alert_ts + 300, fallback_to_current=True),
                    price_at_alert,
                )

        success_attempt = self._last_attempt(attempts, symbol=symbol, status="success")
        latency_ms = sum(
            int(item.get("latency_ms") or 0)
            for item in attempts
            if str(item.get("symbol") or "") == symbol
        )
        return {
            "market_context_source": "live_public",
            "market_context_venue": self.venue,
            "market_context_symbol": symbol,
            "market_context_requested_symbol": requested_symbol,
            "market_context_resolved_symbol": symbol,
            "market_context_endpoint": str(success_attempt.get("endpoint") or ""),
            "market_context_latency_ms": int(latency_ms),
            "market_context_attempts": [dict(item) for item in attempts],
            "perp_last_price": perp_last,
            "perp_mark_price": perp_mark,
            "perp_index_price": perp_index,
            "spot_reference_price": spot_reference,
            "funding_direction": _funding_direction(funding_estimate),
            "funding_estimate": funding_estimate,
            "basis_bps": _spread_bps(perp_mark or perp_last, spot_reference or perp_index),
            "last_mark_spread_bps": _spread_bps(perp_last, perp_mark),
            "mark_index_spread_bps": _spread_bps(perp_mark, perp_index),
            "market_move_before_alert_30s": move_before_30s,
            "market_move_before_alert_60s": move_before_60s,
            "market_move_after_alert_60s": move_after_60s,
            "market_move_after_alert_300s": move_after_300s,
            "market_context_sampled_at": sampled_at,
        }

    def _historical_price(
        self,
        symbol: str,
        raw_payload: dict,
        target_ts: int,
        *,
        fallback_to_current: bool = False,
    ) -> float | None:
        if target_ts <= 0:
            return None
        history = list(self._price_history.get(symbol) or [])
        prior = None
        for ts, price in history:
            if ts <= target_ts:
                prior = price
            elif ts > target_ts:
                break
        if prior is not None:
            return float(prior)
        klines = list(raw_payload.get("klines") or [])
        interpolated = self._interpolate_kline_price(klines, target_ts)
        if interpolated is not None:
            return interpolated
        if fallback_to_current:
            return _as_float(raw_payload.get("perp_mark_price")) or _as_float(raw_payload.get("perp_last_price"))
        return None

    def _interpolate_kline_price(self, klines: list[dict], target_ts: int) -> float | None:
        latest_close = None
        for candle in sorted(klines, key=lambda item: int(item.get("open_time") or 0)):
            open_ts = int(candle.get("open_time") or 0)
            close_ts = int(candle.get("close_time") or (open_ts + 60))
            open_price = _as_float(candle.get("open_price"))
            close_price = _as_float(candle.get("close_price"))
            if close_price is not None and close_ts <= target_ts:
                latest_close = close_price
            if not (open_ts <= target_ts <= close_ts):
                continue
            if open_price is None or close_price is None:
                return latest_close
            span = max(close_ts - open_ts, 1)
            progress = min(max((target_ts - open_ts) / span, 0.0), 1.0)
            return round(open_price + (close_price - open_price) * progress, 8)
        return latest_close

    def _get_cached_raw_payload(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        now = float(self._clock())
        cached = self._cache.get(symbol)
        if cached and (now - cached[0]) <= self.cache_ttl_sec:
            attempts.append(
                {
                    "venue": self.venue,
                    "symbol": symbol,
                    "requested_symbol": requested_symbol,
                    "stage": "cache",
                    "endpoint": "cache",
                    "status": "cache_hit",
                    "failure_reason": "",
                    "failure_stage": "",
                    "http_status": None,
                    "latency_ms": 0,
                }
            )
            return dict(cached[1])
        raw_payload = self._fetch_raw_payload(symbol, attempts=attempts, requested_symbol=requested_symbol)
        self._cache[symbol] = (now, dict(raw_payload))
        sampled_at = int(raw_payload.get("sampled_at") or now)
        mark_or_last = _as_float(raw_payload.get("perp_mark_price")) or _as_float(raw_payload.get("perp_last_price"))
        if mark_or_last is not None:
            history = self._price_history[symbol]
            if not history or history[-1][0] != sampled_at:
                history.append((sampled_at, float(mark_or_last)))
        return dict(raw_payload)

    def _get_optional_json(
        self,
        path: str,
        params: dict | None = None,
        *,
        stage: str,
        symbol: str,
        requested_symbol: str,
        attempts: list[dict],
        base_url: str | None = None,
    ) -> object | None:
        try:
            return self._get_json(
                path,
                params,
                stage=stage,
                symbol=symbol,
                requested_symbol=requested_symbol,
                attempts=attempts,
                base_url=base_url,
            )
        except MarketDataClientError:
            return None

    def _fetch_raw_payload(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        last_error = None
        for _ in range(self.retry_count + 1):
            try:
                return dict(
                    self._fetch_raw_payload_once(
                        symbol,
                        attempts=attempts,
                        requested_symbol=requested_symbol,
                    )
                )
            except MarketDataClientError as exc:
                last_error = exc
                if not exc.endpoint:
                    previous_attempt = self._last_attempt(attempts, symbol=symbol)
                    attempts.append(
                        self._build_attempt(
                            symbol=symbol,
                            requested_symbol=requested_symbol,
                            stage=str(exc.stage or "payload_validation"),
                            endpoint=str(previous_attempt.get("endpoint") or ""),
                            status="failure",
                            failure_reason=str(exc.reason or ""),
                            http_status=exc.http_status,
                            latency_ms=exc.latency_ms if exc.latency_ms is not None else previous_attempt.get("latency_ms"),
                        )
                    )
        raise last_error or MarketDataClientError(
            f"{self.venue}_payload_fetch_failed",
            stage="fetch",
            venue=self.venue,
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )

    def _fetch_raw_payload_once(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        raise NotImplementedError

    def _build_attempt(
        self,
        *,
        symbol: str,
        requested_symbol: str,
        stage: str,
        endpoint: str,
        status: str,
        failure_reason: str = "",
        http_status: int | None = None,
        latency_ms: int | None = None,
    ) -> dict:
        return {
            "venue": self.venue,
            "symbol": str(symbol or ""),
            "requested_symbol": str(requested_symbol or ""),
            "stage": str(stage or ""),
            "endpoint": str(endpoint or ""),
            "status": str(status or ""),
            "failure_reason": str(failure_reason or ""),
            "failure_stage": str(stage or ""),
            "http_status": http_status,
            "latency_ms": 0 if latency_ms is None else int(latency_ms),
        }

    def _get_json(
        self,
        path: str,
        params: dict | None = None,
        *,
        stage: str,
        symbol: str,
        requested_symbol: str,
        attempts: list[dict],
        base_url: str | None = None,
    ) -> object:
        url = self._build_url(path, params or {}, base_url=base_url)
        started = time.monotonic()
        try:
            payload = self._fetcher(url, self.timeout_sec)
        except HTTPError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            attempts.append(
                self._build_attempt(
                    symbol=symbol,
                    requested_symbol=requested_symbol,
                    stage=stage,
                    endpoint=url,
                    status="failure",
                    failure_reason=f"http_{exc.code}",
                    http_status=int(exc.code),
                    latency_ms=latency_ms,
                )
            )
            raise MarketDataClientError(
                f"http_{exc.code}",
                stage=stage,
                venue=self.venue,
                symbol=symbol,
                requested_symbol=requested_symbol,
                http_status=int(exc.code),
                endpoint=url,
                latency_ms=latency_ms,
            ) from exc
        except URLError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            attempts.append(
                self._build_attempt(
                    symbol=symbol,
                    requested_symbol=requested_symbol,
                    stage=stage,
                    endpoint=url,
                    status="failure",
                    failure_reason="network_unavailable",
                    latency_ms=latency_ms,
                )
            )
            raise MarketDataClientError(
                "network_unavailable",
                stage=stage,
                venue=self.venue,
                symbol=symbol,
                requested_symbol=requested_symbol,
                endpoint=url,
                latency_ms=latency_ms,
            ) from exc
        except TimeoutError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            attempts.append(
                self._build_attempt(
                    symbol=symbol,
                    requested_symbol=requested_symbol,
                    stage=stage,
                    endpoint=url,
                    status="failure",
                    failure_reason="timeout",
                    latency_ms=latency_ms,
                )
            )
            raise MarketDataClientError(
                "timeout",
                stage=stage,
                venue=self.venue,
                symbol=symbol,
                requested_symbol=requested_symbol,
                endpoint=url,
                latency_ms=latency_ms,
            ) from exc
        except json.JSONDecodeError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            attempts.append(
                self._build_attempt(
                    symbol=symbol,
                    requested_symbol=requested_symbol,
                    stage=stage,
                    endpoint=url,
                    status="failure",
                    failure_reason="malformed_json",
                    latency_ms=latency_ms,
                )
            )
            raise MarketDataClientError(
                "malformed_json",
                stage=stage,
                venue=self.venue,
                symbol=symbol,
                requested_symbol=requested_symbol,
                endpoint=url,
                latency_ms=latency_ms,
            ) from exc
        except OSError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            attempts.append(
                self._build_attempt(
                    symbol=symbol,
                    requested_symbol=requested_symbol,
                    stage=stage,
                    endpoint=url,
                    status="failure",
                    failure_reason="network_os_error",
                    latency_ms=latency_ms,
                )
            )
            raise MarketDataClientError(
                "network_os_error",
                stage=stage,
                venue=self.venue,
                symbol=symbol,
                requested_symbol=requested_symbol,
                endpoint=url,
                latency_ms=latency_ms,
            ) from exc

        latency_ms = int((time.monotonic() - started) * 1000)
        attempts.append(
            self._build_attempt(
                symbol=symbol,
                requested_symbol=requested_symbol,
                stage=stage,
                endpoint=url,
                status="success",
                latency_ms=latency_ms,
            )
        )
        return payload

    def _validate_payload_symbol(
        self,
        actual_symbol: str | None,
        expected_symbol: str,
        *,
        stage: str,
    ) -> None:
        normalized_actual = normalize_symbol(actual_symbol)
        normalized_expected = normalize_symbol(expected_symbol)
        if not normalized_actual:
            return
        if normalized_actual != normalized_expected:
            raise MarketDataClientError(
                "symbol_mismatch",
                stage=stage,
                venue=self.venue,
                symbol=expected_symbol,
            )

    def _last_attempt(self, attempts: list[dict], *, symbol: str, status: str | None = None) -> dict:
        for item in reversed(attempts):
            if str(item.get("symbol") or "") != str(symbol or ""):
                continue
            if status and str(item.get("status") or "") != status:
                continue
            return dict(item)
        return {}

    def _build_url(self, path: str, params: dict, *, base_url: str | None = None) -> str:
        query = urlencode({key: value for key, value in params.items() if value is not None})
        suffix = f"?{query}" if query else ""
        prefix = str(base_url or self.base_url).rstrip("/")
        return f"{prefix}/{path.lstrip('/')}{suffix}"

    def _default_fetcher(self, url: str, timeout_sec: float) -> object:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "chain-monitor/1.0"})
        with urlopen(request, timeout=timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))


class BinancePublicMarketClient(PublicMarketDataClient):
    venue = "binance_perp"

    def __init__(self, *, spot_base_url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.spot_base_url = str(spot_base_url or "").rstrip("/")

    def _fetch_raw_payload_once(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        premium = self._get_json(
            "/fapi/v1/premiumIndex",
            {"symbol": symbol},
            stage="premium_index",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        perp_last = self._get_json(
            "/fapi/v1/ticker/price",
            {"symbol": symbol},
            stage="perp_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        spot_last = self._get_json(
            "/api/v3/ticker/price",
            {"symbol": symbol},
            stage="spot_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
            base_url=self.spot_base_url,
        )
        klines = self._get_json(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "1m", "limit": 8},
            stage="klines",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        premium_payload = self._parse_premium_index_payload(premium, symbol=symbol)
        perp_last_price = self._parse_ticker_price_payload(perp_last, symbol=symbol, stage="perp_ticker")
        spot_reference_price = self._parse_ticker_price_payload(spot_last, symbol=symbol, stage="spot_ticker")
        kline_payload = self._parse_binance_klines_payload(klines)
        if premium_payload["perp_mark_price"] is None and perp_last_price is None:
            raise MarketDataClientError("no_symbol", stage="pricing", venue=self.venue, symbol=symbol)
        return {
            **premium_payload,
            "perp_last_price": perp_last_price,
            "spot_reference_price": spot_reference_price,
            "klines": kline_payload,
            "sampled_at": int(premium_payload.get("sampled_at") or self._clock()),
        }

    def _parse_premium_index_payload(self, payload: object, *, symbol: str) -> dict:
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage="premium_index", venue=self.venue, symbol=symbol)
        self._validate_payload_symbol(payload.get("symbol"), symbol, stage="premium_index")
        return {
            "perp_mark_price": _as_float(payload.get("markPrice")),
            "perp_index_price": _as_float(payload.get("indexPrice")),
            "funding_estimate": _as_float(payload.get("lastFundingRate")),
            "sampled_at": int((_as_float(payload.get("time")) or time.time() * 1000) / 1000),
        }

    def _parse_ticker_price_payload(self, payload: object, *, symbol: str, stage: str) -> float | None:
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        self._validate_payload_symbol(payload.get("symbol"), symbol, stage=stage)
        return _as_float(payload.get("price"))

    @staticmethod
    def _parse_binance_klines_payload(payload: object) -> list[dict]:
        if not isinstance(payload, list):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="binance_perp")
        klines = []
        for item in payload:
            if not isinstance(item, list) or len(item) < 5:
                raise MarketDataClientError("malformed_payload", stage="klines", venue="binance_perp")
            klines.append(
                {
                    "open_time": int(int(item[0]) / 1000),
                    "close_time": int(int(item[6]) / 1000) if len(item) > 6 else int(int(item[0]) / 1000) + 60,
                    "open_price": _as_float(item[1]),
                    "close_price": _as_float(item[4]),
                }
            )
        return klines


class BybitPublicMarketClient(PublicMarketDataClient):
    venue = "bybit_perp"

    def _fetch_raw_payload_once(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        perp_ticker = self._get_json(
            "/v5/market/tickers",
            {"category": "linear", "symbol": symbol},
            stage="perp_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        spot_ticker = self._get_json(
            "/v5/market/tickers",
            {"category": "spot", "symbol": symbol},
            stage="spot_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        klines = self._get_json(
            "/v5/market/kline",
            {"category": "linear", "symbol": symbol, "interval": 1, "limit": 8},
            stage="klines",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        perp_payload = self._parse_bybit_ticker_payload(perp_ticker, symbol=symbol, stage="perp_ticker")
        spot_payload = self._parse_bybit_ticker_payload(spot_ticker, symbol=symbol, stage="spot_ticker", allow_missing_symbol=True)
        kline_payload = self._parse_bybit_klines_payload(klines)
        if perp_payload["perp_mark_price"] is None and perp_payload["perp_last_price"] is None:
            raise MarketDataClientError("no_symbol", stage="pricing", venue=self.venue, symbol=symbol)
        return {
            **perp_payload,
            "spot_reference_price": spot_payload.get("perp_last_price"),
            "klines": kline_payload,
            "sampled_at": int(perp_payload.get("sampled_at") or self._clock()),
        }

    def _parse_bybit_ticker_payload(
        self,
        payload: object,
        *,
        symbol: str,
        stage: str,
        allow_missing_symbol: bool = False,
    ) -> dict:
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        result = payload.get("result") or {}
        if not isinstance(result, dict):
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        rows = result.get("list") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        if not rows:
            raise MarketDataClientError("no_symbol", stage=stage, venue=self.venue, symbol=symbol)
        row = rows[0]
        if not isinstance(row, dict):
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        actual_symbol = row.get("symbol")
        if actual_symbol:
            self._validate_payload_symbol(actual_symbol, symbol, stage=stage)
        elif not allow_missing_symbol and len(rows) > 1:
            raise MarketDataClientError("no_symbol", stage=stage, venue=self.venue, symbol=symbol)
        return {
            "perp_last_price": _as_float(row.get("lastPrice")),
            "perp_mark_price": _as_float(row.get("markPrice")),
            "perp_index_price": _as_float(row.get("indexPrice")),
            "funding_estimate": _as_float(row.get("fundingRate")),
            "sampled_at": int((_as_float(payload.get("time")) or time.time() * 1000) / 1000),
        }

    @staticmethod
    def _parse_bybit_klines_payload(payload: object) -> list[dict]:
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="bybit_perp")
        result = payload.get("result") or {}
        rows = result.get("list") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="bybit_perp")
        klines = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 5:
                raise MarketDataClientError("malformed_payload", stage="klines", venue="bybit_perp")
            open_time = int(int(item[0]) / 1000)
            klines.append(
                {
                    "open_time": open_time,
                    "close_time": open_time + 60,
                    "open_price": _as_float(item[1]),
                    "close_price": _as_float(item[4]),
                }
            )
        return sorted(klines, key=lambda item: int(item.get("open_time") or 0))


class OKXPublicMarketClient(PublicMarketDataClient):
    venue = "okx_perp"

    def _fetch_raw_payload_once(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        perp_ticker = self._get_json(
            "/api/v5/market/ticker",
            {"instId": symbol},
            stage="perp_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        mark_price = self._get_optional_json(
            "/api/v5/public/mark-price",
            {"instType": "SWAP", "instId": symbol},
            stage="mark_price",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        funding_rate = self._get_optional_json(
            "/api/v5/public/funding-rate",
            {"instId": symbol},
            stage="funding_rate",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        index_symbol = self._okx_index_symbol(symbol)
        index_ticker = self._get_optional_json(
            "/api/v5/market/index-tickers",
            {"instId": index_symbol},
            stage="index_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        klines = self._get_optional_json(
            "/api/v5/market/history-candles",
            {"instId": symbol, "bar": "1m", "limit": 8},
            stage="klines",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )

        perp_row = self._parse_okx_single_row(
            perp_ticker,
            symbol=symbol,
            stage="perp_ticker",
        )
        mark_row = self._parse_okx_single_row(
            mark_price,
            symbol=symbol,
            stage="mark_price",
            allow_missing=True,
        )
        funding_row = self._parse_okx_single_row(
            funding_rate,
            symbol=symbol,
            stage="funding_rate",
            allow_missing=True,
        )
        index_row = self._parse_okx_single_row(
            index_ticker,
            symbol=index_symbol,
            stage="index_ticker",
            allow_missing=True,
        )
        spot_reference_price = self._okx_spot_reference_price(
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        kline_payload = self._parse_okx_klines_payload(klines)
        perp_last_price = _as_float(perp_row.get("last"))
        perp_mark_price = _as_float(mark_row.get("markPx")) or _as_float(perp_row.get("markPx"))
        perp_index_price = _as_float(index_row.get("idxPx")) or _as_float(perp_row.get("idxPx"))
        funding_estimate = (
            _as_float(funding_row.get("nextFundingRate"))
            or _as_float(funding_row.get("fundingRate"))
            or _as_float(perp_row.get("fundingRate"))
        )
        sampled_at = int(
            (
                _as_float(perp_row.get("ts"))
                or _as_float(mark_row.get("ts"))
                or _as_float(index_row.get("ts"))
                or time.time() * 1000
            )
            / 1000
        )
        if perp_last_price is None and perp_mark_price is None and perp_index_price is None:
            raise MarketDataClientError("no_symbol", stage="pricing", venue=self.venue, symbol=symbol)
        return {
            "perp_last_price": perp_last_price,
            "perp_mark_price": perp_mark_price,
            "perp_index_price": perp_index_price,
            "spot_reference_price": spot_reference_price,
            "funding_estimate": funding_estimate,
            "klines": kline_payload,
            "sampled_at": sampled_at,
        }

    def _okx_index_symbol(self, symbol: str) -> str:
        parts = str(symbol or "").split("-")
        if len(parts) >= 2:
            return f"{parts[0]}-{parts[1]}"
        return str(symbol or "")

    def _okx_spot_symbol_candidates(self, symbol: str) -> list[str]:
        parts = str(symbol or "").split("-")
        if len(parts) >= 2:
            base = parts[0]
            quote = parts[1]
        else:
            base, quote = "BTC", "USDT"
        ordered = []
        for candidate_quote in [quote, "USDT", "USDC"]:
            normalized = _normalize_quote_symbol(candidate_quote)
            candidate = f"{base}-{normalized}"
            if candidate not in ordered:
                ordered.append(candidate)
        return ordered

    def _okx_spot_reference_price(
        self,
        *,
        symbol: str,
        requested_symbol: str,
        attempts: list[dict],
    ) -> float | None:
        for spot_symbol in self._okx_spot_symbol_candidates(symbol):
            payload = self._get_optional_json(
                "/api/v5/market/ticker",
                {"instId": spot_symbol},
                stage="spot_ticker",
                symbol=symbol,
                requested_symbol=requested_symbol,
                attempts=attempts,
            )
            row = self._parse_okx_single_row(
                payload,
                symbol=spot_symbol,
                stage="spot_ticker",
                allow_missing=True,
            )
            spot_last = _as_float(row.get("last"))
            if spot_last is not None:
                return spot_last
        return None

    def _parse_okx_single_row(
        self,
        payload: object | None,
        *,
        symbol: str,
        stage: str,
        allow_missing: bool = False,
    ) -> dict:
        if payload is None:
            return {}
        if not isinstance(payload, dict):
            if allow_missing:
                return {}
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        rows = payload.get("data")
        if not isinstance(rows, list):
            if allow_missing:
                return {}
            raise MarketDataClientError("malformed_payload", stage=stage, venue=self.venue, symbol=symbol)
        if not rows:
            if allow_missing:
                return {}
            raise MarketDataClientError("no_symbol", stage=stage, venue=self.venue, symbol=symbol)
        for row in rows:
            if not isinstance(row, dict):
                continue
            actual_symbol = str(row.get("instId") or row.get("uly") or "")
            if actual_symbol:
                if stage == "index_ticker":
                    if normalize_symbol(actual_symbol.replace("-", "")) != normalize_symbol(symbol.replace("-", "")):
                        continue
                else:
                    self._validate_payload_symbol(actual_symbol, symbol, stage=stage)
            return dict(row)
        if allow_missing:
            return {}
        raise MarketDataClientError("no_symbol", stage=stage, venue=self.venue, symbol=symbol)

    @staticmethod
    def _parse_okx_klines_payload(payload: object | None) -> list[dict]:
        if payload is None:
            return []
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="okx_perp")
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="okx_perp")
        klines = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 5:
                raise MarketDataClientError("malformed_payload", stage="klines", venue="okx_perp")
            open_time = int(int(item[0]) / 1000)
            klines.append(
                {
                    "open_time": open_time,
                    "close_time": open_time + 60,
                    "open_price": _as_float(item[1]),
                    "close_price": _as_float(item[4]),
                }
            )
        return sorted(klines, key=lambda item: int(item.get("open_time") or 0))


class KrakenFuturesPublicMarketClient(PublicMarketDataClient):
    venue = "kraken_futures"

    def _fetch_raw_payload_once(self, symbol: str, *, attempts: list[dict], requested_symbol: str) -> dict:
        ticker = self._get_json(
            "/derivatives/api/v3/tickers",
            {"symbol": symbol},
            stage="perp_ticker",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
        )
        funding = self._get_optional_json(
            "/api/history/v2/historical-funding-rates",
            {"symbol": symbol},
            stage="funding_history",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
            base_url=self.base_url,
        )
        candles = self._get_optional_json(
            "/api/charts/v1/trade",
            {"symbol": symbol, "resolution": "1m"},
            stage="klines",
            symbol=symbol,
            requested_symbol=requested_symbol,
            attempts=attempts,
            base_url=self.base_url,
        )
        ticker_row = self._parse_kraken_ticker_payload(ticker, symbol=symbol)
        funding_estimate = (
            _as_float(ticker_row.get("fundingRatePrediction"))
            or _as_float(ticker_row.get("funding_rate_prediction"))
            or _as_float(ticker_row.get("fundingRate"))
            or _as_float(ticker_row.get("funding_rate"))
        )
        if funding_estimate is None:
            funding_estimate = self._parse_kraken_funding_history(funding)
        payload = {
            "perp_last_price": _as_float(ticker_row.get("last")) or _as_float(ticker_row.get("lastPrice")),
            "perp_mark_price": _as_float(ticker_row.get("markPrice")) or _as_float(ticker_row.get("mark_price")),
            "perp_index_price": _as_float(ticker_row.get("indexPrice")) or _as_float(ticker_row.get("index_price")),
            "spot_reference_price": None,
            "funding_estimate": funding_estimate,
            "klines": self._parse_kraken_candles_payload(candles),
            "sampled_at": int((_as_float(ticker_row.get("time")) or time.time() * 1000) / 1000),
        }
        if payload["perp_last_price"] is None and payload["perp_mark_price"] is None and payload["perp_index_price"] is None:
            raise MarketDataClientError("no_symbol", stage="pricing", venue=self.venue, symbol=symbol)
        return payload

    def _parse_kraken_ticker_payload(self, payload: object, *, symbol: str) -> dict:
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage="perp_ticker", venue=self.venue, symbol=symbol)
        rows = payload.get("tickers") or payload.get("contracts") or payload.get("data") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("malformed_payload", stage="perp_ticker", venue=self.venue, symbol=symbol)
        if not rows:
            raise MarketDataClientError("no_symbol", stage="perp_ticker", venue=self.venue, symbol=symbol)
        for row in rows:
            if not isinstance(row, dict):
                continue
            actual_symbol = str(
                row.get("symbol")
                or row.get("product_id")
                or row.get("productId")
                or ""
            )
            if actual_symbol:
                self._validate_payload_symbol(actual_symbol, symbol, stage="perp_ticker")
            return dict(row)
        raise MarketDataClientError("no_symbol", stage="perp_ticker", venue=self.venue, symbol=symbol)

    @staticmethod
    def _parse_kraken_funding_history(payload: object | None) -> float | None:
        if not isinstance(payload, dict):
            return None
        rows = (
            payload.get("rates")
            or payload.get("historicalFundingRates")
            or payload.get("data")
            or []
        )
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0]
        if not isinstance(row, dict):
            return None
        return (
            _as_float(row.get("fundingRate"))
            or _as_float(row.get("funding_rate"))
            or _as_float(row.get("rate"))
        )

    @staticmethod
    def _parse_kraken_candles_payload(payload: object | None) -> list[dict]:
        if payload is None:
            return []
        if not isinstance(payload, dict):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="kraken_futures")
        rows = payload.get("candles") or payload.get("result") or payload.get("data") or []
        if isinstance(rows, dict):
            rows = rows.get("candles") or rows.get("data") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("malformed_payload", stage="klines", venue="kraken_futures")
        klines = []
        for item in rows:
            if isinstance(item, dict):
                open_time = int((_as_float(item.get("time")) or _as_float(item.get("openTime")) or 0) / 1000)
                if open_time <= 0:
                    continue
                klines.append(
                    {
                        "open_time": open_time,
                        "close_time": open_time + 60,
                        "open_price": _as_float(item.get("open")),
                        "close_price": _as_float(item.get("close")),
                    }
                )
                continue
            if isinstance(item, list) and len(item) >= 5:
                open_time = int((_as_float(item[0]) or 0) / 1000)
                if open_time <= 0:
                    continue
                klines.append(
                    {
                        "open_time": open_time,
                        "close_time": open_time + 60,
                        "open_price": _as_float(item[1]),
                        "close_price": _as_float(item[4]),
                    }
                )
                continue
            raise MarketDataClientError("malformed_payload", stage="klines", venue="kraken_futures")
        return sorted(klines, key=lambda item: int(item.get("open_time") or 0))
