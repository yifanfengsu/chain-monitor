from __future__ import annotations

from collections import defaultdict, deque
import time
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

from lp_product_helpers import canonical_asset_symbol, normalize_symbol


JsonFetcher = Callable[[str, float], object]


class MarketDataClientError(RuntimeError):
    pass


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


def _candidate_symbols(token_or_pair: str | None) -> list[str]:
    raw = str(token_or_pair or "").strip().upper()
    if not raw:
        return []
    if "/" in raw:
        left, right = [normalize_symbol(part) for part in raw.split("/", 1)]
        base_asset = canonical_asset_symbol(left)
        quote_candidates = [right] if right else []
    else:
        base_asset = canonical_asset_symbol(raw)
        quote_candidates = []
    ordered_quotes = []
    for quote in ["USDT"] + quote_candidates + ["USDC"]:
        normalized = normalize_symbol(quote)
        if normalized and normalized not in ordered_quotes:
            ordered_quotes.append(normalized)
    return [f"{base_asset}{quote}" for quote in ordered_quotes if base_asset]


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
        last_error = None
        for symbol in _candidate_symbols(token_or_pair):
            try:
                raw_payload = self._get_cached_raw_payload(symbol)
                return self._build_market_context(symbol, raw_payload, int(alert_ts or 0))
            except MarketDataClientError as exc:
                last_error = exc
        raise last_error or MarketDataClientError("market_symbol_unavailable")

    def _build_market_context(self, symbol: str, raw_payload: dict, alert_ts: int) -> dict:
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
        return {
            "market_context_source": "live_public",
            "market_context_venue": self.venue,
            "market_context_symbol": symbol,
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

    def _get_cached_raw_payload(self, symbol: str) -> dict:
        now = float(self._clock())
        cached = self._cache.get(symbol)
        if cached and (now - cached[0]) <= self.cache_ttl_sec:
            return dict(cached[1])
        raw_payload = self._fetch_raw_payload(symbol)
        self._cache[symbol] = (now, dict(raw_payload))
        sampled_at = int(raw_payload.get("sampled_at") or now)
        mark_or_last = _as_float(raw_payload.get("perp_mark_price")) or _as_float(raw_payload.get("perp_last_price"))
        if mark_or_last is not None:
            history = self._price_history[symbol]
            if not history or history[-1][0] != sampled_at:
                history.append((sampled_at, float(mark_or_last)))
        return dict(raw_payload)

    def _fetch_raw_payload(self, symbol: str) -> dict:
        last_error = None
        for _ in range(self.retry_count + 1):
            try:
                return dict(self._fetch_raw_payload_once(symbol))
            except MarketDataClientError as exc:
                last_error = exc
        raise last_error or MarketDataClientError(f"{self.venue}_payload_fetch_failed")

    def _fetch_raw_payload_once(self, symbol: str) -> dict:
        raise NotImplementedError

    def _get_json(self, path: str, params: dict | None = None) -> object:
        url = self._build_url(path, params or {})
        try:
            return self._fetcher(url, self.timeout_sec)
        except HTTPError as exc:
            raise MarketDataClientError(f"http_{exc.code}") from exc
        except URLError as exc:
            raise MarketDataClientError("network_unavailable") from exc
        except TimeoutError as exc:
            raise MarketDataClientError("timeout") from exc
        except json.JSONDecodeError as exc:
            raise MarketDataClientError("malformed_json") from exc
        except OSError as exc:
            raise MarketDataClientError("network_os_error") from exc

    def _build_url(self, path: str, params: dict) -> str:
        query = urlencode({key: value for key, value in params.items() if value is not None})
        suffix = f"?{query}" if query else ""
        return f"{self.base_url}/{path.lstrip('/')}{suffix}"

    def _default_fetcher(self, url: str, timeout_sec: float) -> object:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "chain-monitor/1.0"})
        with urlopen(request, timeout=timeout_sec) as response:
            return json.loads(response.read().decode("utf-8"))


class BinancePublicMarketClient(PublicMarketDataClient):
    venue = "binance_perp"

    def __init__(self, *, spot_base_url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.spot_base_url = str(spot_base_url or "").rstrip("/")

    def _fetch_raw_payload_once(self, symbol: str) -> dict:
        premium = self._get_json("/fapi/v1/premiumIndex", {"symbol": symbol})
        perp_last = self._get_json("/fapi/v1/ticker/price", {"symbol": symbol})
        spot_last = self._get_spot_json("/api/v3/ticker/price", {"symbol": symbol})
        klines = self._get_json("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": 8})
        premium_payload = self._parse_premium_index_payload(premium)
        perp_last_price = self._parse_ticker_price_payload(perp_last)
        spot_reference_price = self._parse_ticker_price_payload(spot_last)
        kline_payload = self._parse_binance_klines_payload(klines)
        if premium_payload["perp_mark_price"] is None and perp_last_price is None:
            raise MarketDataClientError("binance_missing_prices")
        return {
            **premium_payload,
            "perp_last_price": perp_last_price,
            "spot_reference_price": spot_reference_price,
            "klines": kline_payload,
            "sampled_at": int(premium_payload.get("sampled_at") or self._clock()),
        }

    def _get_spot_json(self, path: str, params: dict | None = None) -> object:
        original_base = self.base_url
        self.base_url = self.spot_base_url
        try:
            return self._get_json(path, params)
        finally:
            self.base_url = original_base

    @staticmethod
    def _parse_premium_index_payload(payload: object) -> dict:
        if not isinstance(payload, dict):
            raise MarketDataClientError("binance_premium_payload_invalid")
        return {
            "perp_mark_price": _as_float(payload.get("markPrice")),
            "perp_index_price": _as_float(payload.get("indexPrice")),
            "funding_estimate": _as_float(payload.get("lastFundingRate")),
            "sampled_at": int((_as_float(payload.get("time")) or time.time() * 1000) / 1000),
        }

    @staticmethod
    def _parse_ticker_price_payload(payload: object) -> float | None:
        if not isinstance(payload, dict):
            raise MarketDataClientError("binance_ticker_payload_invalid")
        return _as_float(payload.get("price"))

    @staticmethod
    def _parse_binance_klines_payload(payload: object) -> list[dict]:
        if not isinstance(payload, list):
            raise MarketDataClientError("binance_klines_payload_invalid")
        klines = []
        for item in payload:
            if not isinstance(item, list) or len(item) < 5:
                raise MarketDataClientError("binance_kline_entry_invalid")
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

    def _fetch_raw_payload_once(self, symbol: str) -> dict:
        perp_ticker = self._get_json("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        spot_ticker = self._get_json("/v5/market/tickers", {"category": "spot", "symbol": symbol})
        klines = self._get_json("/v5/market/kline", {"category": "linear", "symbol": symbol, "interval": 1, "limit": 8})
        perp_payload = self._parse_bybit_ticker_payload(perp_ticker)
        spot_payload = self._parse_bybit_ticker_payload(spot_ticker)
        kline_payload = self._parse_bybit_klines_payload(klines)
        if perp_payload["perp_mark_price"] is None and perp_payload["perp_last_price"] is None:
            raise MarketDataClientError("bybit_missing_prices")
        return {
            **perp_payload,
            "spot_reference_price": spot_payload.get("perp_last_price"),
            "klines": kline_payload,
            "sampled_at": int(perp_payload.get("sampled_at") or self._clock()),
        }

    @staticmethod
    def _parse_bybit_ticker_payload(payload: object) -> dict:
        if not isinstance(payload, dict):
            raise MarketDataClientError("bybit_ticker_payload_invalid")
        result = payload.get("result") or {}
        if not isinstance(result, dict):
            raise MarketDataClientError("bybit_ticker_result_invalid")
        rows = result.get("list") or []
        if not isinstance(rows, list) or not rows:
            raise MarketDataClientError("bybit_ticker_rows_missing")
        row = rows[0]
        if not isinstance(row, dict):
            raise MarketDataClientError("bybit_ticker_row_invalid")
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
            raise MarketDataClientError("bybit_klines_payload_invalid")
        result = payload.get("result") or {}
        rows = result.get("list") or []
        if not isinstance(rows, list):
            raise MarketDataClientError("bybit_klines_rows_invalid")
        klines = []
        for item in rows:
            if not isinstance(item, list) or len(item) < 5:
                raise MarketDataClientError("bybit_kline_entry_invalid")
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
