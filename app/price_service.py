import asyncio
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from constants import (
    ETH_EQUIVALENT_CONTRACTS,
    ETH_EQUIVALENT_SYMBOLS,
    STABLE_TOKEN_CONTRACTS,
    STABLE_TOKEN_SYMBOLS,
)


@dataclass
class _CacheEntry:
    value: float | None
    expire_at: float


class PriceService:
    """
    异步价格服务（USD）：
    1) 先读缓存
    2) 失败命中失败缓存则快速返回
    3) 远程拉取并写回缓存
    4) ETH/WETH 共享缓存与兜底价格
    """

    ETH_CACHE_KEY = "asset:eth"

    def __init__(
        self,
        ttl_sec: int = 30,
        fail_ttl_sec: int = 120,
        request_timeout_sec: int = 6,
        max_concurrency: int = 8,
        stale_ok_sec: int = 1800,
    ) -> None:
        self.ttl_sec = ttl_sec
        self.fail_ttl_sec = fail_ttl_sec
        self.request_timeout_sec = request_timeout_sec
        self.stale_ok_sec = stale_ok_sec
        self._cache: dict[str, _CacheEntry] = {}
        self._fail_cache: dict[str, float] = {}
        self._last_success: dict[str, tuple[float, float]] = {}
        self._sem = asyncio.Semaphore(max_concurrency)

    async def get_price_usd(self, token_contract: str | None = None, symbol: str | None = None) -> float | None:
        """获取单个 token 的 USD 价格。"""
        contract = (token_contract or "").lower()
        symbol_upper = (symbol or "").upper()

        if self._is_stable(contract, symbol_upper):
            return 1.0

        cache_key = self._cache_key(contract, symbol_upper)
        now = time.time()

        hit = self._cache.get(cache_key)
        if hit and hit.expire_at > now:
            return hit.value

        fail_expire = self._fail_cache.get(cache_key)
        if fail_expire and fail_expire > now:
            return self._stale_fallback(cache_key, now)

        async with self._sem:
            now = time.time()
            hit = self._cache.get(cache_key)
            if hit and hit.expire_at > now:
                return hit.value

            price = await self._fetch_price_usd(contract, symbol_upper)
            if price is None:
                self._fail_cache[cache_key] = now + self.fail_ttl_sec
                return self._stale_fallback(cache_key, now)

            self._cache[cache_key] = _CacheEntry(value=price, expire_at=now + self.ttl_sec)
            self._last_success[cache_key] = (price, now)
            self._fail_cache.pop(cache_key, None)
            return price

    async def evaluate_event_pricing(self, event: dict) -> dict:
        """根据解析后的事件返回完整定价结果，而不是只返回 USD 数值。"""
        kind = str(event.get("kind") or "")
        token_contract = (event.get("token_contract") or "").lower()
        token_symbol = str(event.get("token_symbol") or "").upper()
        quote_contract = (event.get("quote_token_contract") or "").lower()
        quote_symbol = str(event.get("quote_symbol") or "").upper()

        if kind == "swap":
            quote_amount = float(event.get("quote_amount") or 0.0)
            token_amount = float(event.get("token_amount") or 0.0)

            if quote_amount > 0:
                quote_price = await self.get_price_usd(token_contract=quote_contract, symbol=quote_symbol)
                if quote_price is not None:
                    return {
                        "usd_value": quote_amount * quote_price,
                        "pricing_status": "exact" if self._quote_leg_high_confidence(quote_contract, quote_symbol) else "estimated",
                        "pricing_source": self._quote_pricing_source(quote_contract, quote_symbol),
                        "pricing_confidence": self._quote_pricing_confidence(quote_contract, quote_symbol),
                        "usd_value_available": True,
                        "usd_value_estimated": not self._quote_leg_high_confidence(quote_contract, quote_symbol),
                    }

            # quote 价格失败时，回退到 token 腿市场价，属于估算值。
            if token_amount > 0:
                token_price = await self.get_price_usd(token_contract=token_contract, symbol=token_symbol)
                if token_price is not None:
                    return {
                        "usd_value": token_amount * token_price,
                        "pricing_status": "estimated",
                        "pricing_source": "swap_token_leg_market",
                        "pricing_confidence": 0.68 if not self._is_eth_like(token_contract, token_symbol) else 0.8,
                        "usd_value_available": True,
                        "usd_value_estimated": True,
                    }

            return self._unavailable_result("swap_price_missing")

        if kind in {"lp_add_liquidity", "lp_remove_liquidity", "lp_rebalance", "clmm_collect_fees"}:
            legs = list(event.get("lp_legs") or [])
            if not legs:
                return self._unavailable_result("lp_leg_missing")

            total_usd = 0.0
            priced_legs = 0
            best_confidence = 0.0
            exact = True
            for leg in legs:
                amount = abs(float(leg.get("amount") or 0.0))
                if amount <= 0:
                    continue

                leg_contract = str(leg.get("token_contract") or "").lower()
                leg_symbol = str(leg.get("token_symbol") or "").upper()
                leg_price = await self.get_price_usd(token_contract=leg_contract, symbol=leg_symbol)
                if leg_price is None:
                    exact = False
                    continue

                priced_legs += 1
                total_usd += amount * leg_price
                best_confidence = max(best_confidence, self._quote_pricing_confidence(leg_contract, leg_symbol))
                if not self._quote_leg_high_confidence(leg_contract, leg_symbol):
                    exact = False

            if priced_legs == 0 or total_usd <= 0:
                return self._unavailable_result("lp_leg_price_missing")

            return {
                "usd_value": total_usd,
                "pricing_status": "exact" if exact else "estimated",
                "pricing_source": "lp_leg_composite",
                "pricing_confidence": 0.88 if exact else max(0.65, best_confidence * 0.82),
                "usd_value_available": True,
                "usd_value_estimated": not exact,
            }

        if kind == "eth_transfer":
            value = float(event.get("value") or 0.0)
            if value <= 0:
                return self._unavailable_result("eth_value_missing")

            eth_price = await self.get_price_usd(symbol="ETH")
            if eth_price is None:
                return self._unavailable_result("eth_price_missing")

            return {
                "usd_value": value * eth_price,
                "pricing_status": "exact",
                "pricing_source": "native_eth_market",
                "pricing_confidence": 0.94,
                "usd_value_available": True,
                "usd_value_estimated": False,
            }

        value = float(event.get("value") or 0.0)
        if value <= 0:
            return self._unavailable_result("token_value_missing")

        if self._is_stable(token_contract, token_symbol):
            return {
                "usd_value": value,
                "pricing_status": "exact",
                "pricing_source": "stablecoin_parity",
                "pricing_confidence": 0.99,
                "usd_value_available": True,
                "usd_value_estimated": False,
            }

        token_price = await self.get_price_usd(token_contract=token_contract, symbol=token_symbol)
        if token_price is not None:
            if self._is_eth_like(token_contract, token_symbol):
                return {
                    "usd_value": value * token_price,
                    "pricing_status": "exact",
                    "pricing_source": "eth_equivalent_market",
                    "pricing_confidence": 0.94,
                    "usd_value_available": True,
                    "usd_value_estimated": False,
                }

            return {
                "usd_value": value * token_price,
                "pricing_status": "estimated",
                "pricing_source": "token_market",
                "pricing_confidence": 0.78,
                "usd_value_available": True,
                "usd_value_estimated": True,
            }

        return self._unavailable_result("token_price_missing")

    async def event_usd_value(self, event: dict) -> float | None:
        """兼容旧接口：只返回可用的 USD 数值。"""
        pricing = await self.evaluate_event_pricing(event)
        if pricing.get("usd_value_available"):
            return float(pricing.get("usd_value") or 0.0)
        return None

    def _cache_key(self, token_contract: str, symbol_upper: str) -> str:
        if self._is_eth_like(token_contract, symbol_upper):
            return self.ETH_CACHE_KEY
        if token_contract:
            return token_contract
        if symbol_upper:
            return f"symbol:{symbol_upper}"
        return "unknown"

    def _is_stable(self, token_contract: str, symbol_upper: str) -> bool:
        return token_contract in STABLE_TOKEN_CONTRACTS or symbol_upper in STABLE_TOKEN_SYMBOLS or symbol_upper == "STABLE"

    def _is_eth_like(self, token_contract: str, symbol_upper: str) -> bool:
        return token_contract in ETH_EQUIVALENT_CONTRACTS or symbol_upper in ETH_EQUIVALENT_SYMBOLS

    def _stale_fallback(self, cache_key: str, now: float) -> float | None:
        last_success = self._last_success.get(cache_key)
        if not last_success:
            return None

        price, ts = last_success
        if now - ts <= self.stale_ok_sec:
            return price
        return None

    def _quote_leg_high_confidence(self, token_contract: str, symbol_upper: str) -> bool:
        return self._is_stable(token_contract, symbol_upper) or self._is_eth_like(token_contract, symbol_upper)

    def _quote_pricing_source(self, token_contract: str, symbol_upper: str) -> str:
        if self._is_stable(token_contract, symbol_upper):
            return "swap_quote_stable"
        if self._is_eth_like(token_contract, symbol_upper):
            return "swap_quote_eth"
        return "swap_quote_market"

    def _quote_pricing_confidence(self, token_contract: str, symbol_upper: str) -> float:
        if self._is_stable(token_contract, symbol_upper):
            return 0.99
        if self._is_eth_like(token_contract, symbol_upper):
            return 0.93
        return 0.82

    def _unavailable_result(self, source: str) -> dict:
        return {
            "usd_value": 0.0,
            "pricing_status": "unavailable",
            "pricing_source": source,
            "pricing_confidence": 0.0,
            "usd_value_available": False,
            "usd_value_estimated": False,
        }

    async def _fetch_price_usd(self, token_contract: str, symbol_upper: str) -> float | None:
        if self._is_eth_like(token_contract, symbol_upper):
            return await self._fetch_eth_price_usd()

        # 优先按合约地址从 Coingecko 查价格。
        if token_contract:
            price = await self._fetch_from_coingecko_contract(token_contract)
            if price is not None:
                return price

        # 回退：按 symbol 走 Binance ticker（symbolUSDT）。
        if symbol_upper:
            price = await self._fetch_from_binance_symbol(symbol_upper)
            if price is not None:
                return price

        return None

    async def _fetch_eth_price_usd(self) -> float | None:
        price = await self._fetch_from_binance_symbol("ETH")
        if price is not None:
            return price
        return await self._fetch_from_coingecko_asset("ethereum")

    async def _fetch_from_coingecko_asset(self, asset_id: str) -> float | None:
        query = urllib.parse.urlencode({"ids": asset_id, "vs_currencies": "usd"})
        url = f"https://api.coingecko.com/api/v3/simple/price?{query}"
        payload = await asyncio.to_thread(self._safe_get_json, url)
        if not isinstance(payload, dict):
            return None

        node = payload.get(asset_id)
        if not isinstance(node, dict):
            return None

        usd = node.get("usd")
        if usd is None:
            return None

        try:
            return float(usd)
        except (TypeError, ValueError):
            return None

    async def _fetch_from_coingecko_contract(self, token_contract: str) -> float | None:
        query = urllib.parse.urlencode({
            "contract_addresses": token_contract,
            "vs_currencies": "usd",
        })
        url = f"https://api.coingecko.com/api/v3/simple/token_price/ethereum?{query}"
        payload = await asyncio.to_thread(self._safe_get_json, url)
        if not payload:
            return None

        node = payload.get(token_contract.lower())
        if not isinstance(node, dict):
            return None

        usd = node.get("usd")
        if usd is None:
            return None
        try:
            return float(usd)
        except (TypeError, ValueError):
            return None

    async def _fetch_from_binance_symbol(self, symbol_upper: str) -> float | None:
        # Binance 以 USDT 计价，近似等于 USD。
        pair = f"{symbol_upper}USDT"
        query = urllib.parse.urlencode({"symbol": pair})
        url = f"https://api.binance.com/api/v3/ticker/price?{query}"
        payload = await asyncio.to_thread(self._safe_get_json, url)
        if not isinstance(payload, dict):
            return None

        price = payload.get("price")
        if price is None:
            return None
        try:
            return float(price)
        except (TypeError, ValueError):
            return None

    def _safe_get_json(self, url: str) -> dict | list | None:
        try:
            with urllib.request.urlopen(url, timeout=self.request_timeout_sec) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
            return None
