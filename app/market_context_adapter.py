from __future__ import annotations

import json
from pathlib import Path

from config import (
    BINANCE_FAPI_BASE_URL,
    BINANCE_SPOT_BASE_URL,
    BYBIT_V5_BASE_URL,
    MARKET_CONTEXT_ADAPTER_MODE,
    MARKET_CONTEXT_CACHE_TTL_SEC,
    MARKET_CONTEXT_FIXTURE_PATH,
    MARKET_CONTEXT_PRIMARY_VENUE,
    MARKET_CONTEXT_RETRY_COUNT,
    MARKET_CONTEXT_SECONDARY_VENUE,
    MARKET_CONTEXT_TIMEOUT_SEC,
)
from lp_product_helpers import canonical_asset_symbol, normalize_symbol
from market_data_clients import (
    BinancePublicMarketClient,
    BybitPublicMarketClient,
    MarketDataClientError,
)


DEFAULT_MARKET_CONTEXT = {
    "perp_last_price": None,
    "perp_mark_price": None,
    "perp_index_price": None,
    "spot_reference_price": None,
    "funding_direction": None,
    "funding_estimate": None,
    "basis_bps": None,
    "last_mark_spread_bps": None,
    "mark_index_spread_bps": None,
    "market_move_before_alert_30s": None,
    "market_move_before_alert_60s": None,
    "market_move_after_alert_60s": None,
    "market_move_after_alert_300s": None,
    "alert_relative_timing": "",
    "market_context_source": "unavailable",
    "market_context_venue": "",
    "market_context_symbol": "",
    "market_context_sampled_at": None,
}


def _normalize_lookup_key(token_or_pair: str | None, venue: str) -> str:
    raw = str(token_or_pair or "").strip().upper()
    if not raw:
        return f"{venue}:"
    if "/" in raw:
        left, right = [normalize_symbol(part) for part in raw.split("/", 1)]
        return f"{venue}:{canonical_asset_symbol(left)}/{normalize_symbol(right)}"
    return f"{venue}:{canonical_asset_symbol(raw)}"


def _spread_bps(left, right) -> float | None:
    if left in {None, ""} or right in {None, ""}:
        return None
    try:
        right_value = float(right)
        if right_value == 0.0:
            return None
        return round(((float(left) / right_value) - 1.0) * 10_000.0, 3)
    except (TypeError, ValueError):
        return None


class MarketContextAdapter:
    def get_market_context(self, token_or_pair, ts, venue: str = "binance_perp") -> dict:
        del token_or_pair, ts, venue
        return dict(DEFAULT_MARKET_CONTEXT)

    def get_recent_market_move(
        self,
        token_or_pair,
        before_sec: int,
        after_sec: int,
        venue: str = "binance_perp",
    ) -> dict:
        context = self.get_market_context(token_or_pair, None, venue=venue)
        return {
            "before_sec": int(before_sec),
            "after_sec": int(after_sec),
            "market_move_before_alert": context.get(f"market_move_before_alert_{int(before_sec)}s"),
            "market_move_after_alert": context.get(f"market_move_after_alert_{int(after_sec)}s"),
            "market_context_source": context.get("market_context_source") or "unavailable",
        }

    def finalize_context(self, payload: dict | None, *, source: str | None = None, venue: str | None = None, stage: str | None = None) -> dict:
        context = dict(DEFAULT_MARKET_CONTEXT)
        context.update(dict(payload or {}))
        if context.get("basis_bps") is None:
            context["basis_bps"] = _spread_bps(
                context.get("perp_mark_price") or context.get("perp_last_price"),
                context.get("spot_reference_price") or context.get("perp_index_price"),
            )
        if context.get("last_mark_spread_bps") is None:
            context["last_mark_spread_bps"] = _spread_bps(context.get("perp_last_price"), context.get("perp_mark_price"))
        if context.get("mark_index_spread_bps") is None:
            context["mark_index_spread_bps"] = _spread_bps(context.get("perp_mark_price"), context.get("perp_index_price"))
        if source:
            context["market_context_source"] = source
        if venue:
            context["market_context_venue"] = venue
        if context.get("market_context_source") != "unavailable" and not context.get("alert_relative_timing"):
            context["alert_relative_timing"] = self.classify_alert_relative_timing(context, stage=stage)
        return context

    def classify_alert_relative_timing(self, context: dict, *, stage: str | None = None) -> str:
        normalized_stage = str(stage or context.get("lp_alert_stage") or "").strip().lower()
        move_before_30s = abs(float(context.get("market_move_before_alert_30s") or 0.0))
        move_before_60s = abs(float(context.get("market_move_before_alert_60s") or 0.0))
        move_after_60s = abs(float(context.get("market_move_after_alert_60s") or 0.0))
        basis_bps = abs(float(context.get("basis_bps") or 0.0))
        mark_index_spread_bps = abs(float(context.get("mark_index_spread_bps") or 0.0))
        last_mark_spread_bps = abs(float(context.get("last_mark_spread_bps") or 0.0))

        if max(move_before_30s, move_before_60s) <= 0.003:
            if move_after_60s >= 0.002 or normalized_stage == "prealert":
                return "leading"
            if basis_bps <= 15.0 and mark_index_spread_bps <= 8.0:
                return "leading"
        if max(move_before_30s, move_before_60s) <= 0.015:
            if normalized_stage in {"confirm", "prealert"}:
                return "confirming"
            if basis_bps <= 35.0 and max(last_mark_spread_bps, mark_index_spread_bps) <= 18.0:
                return "confirming"
        if normalized_stage in {"climax", "exhaustion_risk"} and max(move_before_30s, move_before_60s) >= 0.010:
            return "late"
        return "late"


class UnavailableMarketContextAdapter(MarketContextAdapter):
    pass


class FixtureMarketContextAdapter(MarketContextAdapter):
    def __init__(self, fixtures: dict | None = None, *, fixture_path: str | None = None) -> None:
        self._fixtures = dict(fixtures or {})
        if fixture_path:
            self._fixtures.update(self._load_fixture_path(fixture_path))

    def _load_fixture_path(self, fixture_path: str) -> dict:
        path = Path(fixture_path)
        if not path.exists():
            return {}
        try:
            return dict(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            return {}

    def get_market_context(self, token_or_pair, ts, venue: str = "binance_perp") -> dict:
        del ts
        lookup_keys = []
        raw = str(token_or_pair or "").strip()
        normalized_key = _normalize_lookup_key(raw, venue)
        lookup_keys.append(normalized_key)
        if raw and "/" in raw:
            lookup_keys.append(f"{venue}:{raw.strip().upper()}")
        else:
            lookup_keys.append(f"{venue}:{normalize_symbol(raw)}")

        payload = {}
        for key in lookup_keys:
            if key in self._fixtures:
                payload = dict(self._fixtures.get(key) or {})
                break
        if not payload:
            return self.finalize_context({}, source="unavailable")
        return self.finalize_context(payload, source="fixture", venue=venue)


class LiveMarketContextAdapter(MarketContextAdapter):
    def __init__(
        self,
        *,
        clients: dict | None = None,
        primary_venue: str = MARKET_CONTEXT_PRIMARY_VENUE,
        secondary_venue: str = MARKET_CONTEXT_SECONDARY_VENUE,
    ) -> None:
        self.primary_venue = str(primary_venue or MARKET_CONTEXT_PRIMARY_VENUE or "binance_perp").strip().lower()
        self.secondary_venue = str(secondary_venue or MARKET_CONTEXT_SECONDARY_VENUE or "").strip().lower()
        self.clients = dict(clients or self._default_clients())

    def _default_clients(self) -> dict[str, object]:
        return {
            "binance_perp": BinancePublicMarketClient(
                base_url=BINANCE_FAPI_BASE_URL,
                spot_base_url=BINANCE_SPOT_BASE_URL,
                timeout_sec=MARKET_CONTEXT_TIMEOUT_SEC,
                cache_ttl_sec=MARKET_CONTEXT_CACHE_TTL_SEC,
                retry_count=MARKET_CONTEXT_RETRY_COUNT,
            ),
            "bybit_perp": BybitPublicMarketClient(
                base_url=BYBIT_V5_BASE_URL,
                timeout_sec=MARKET_CONTEXT_TIMEOUT_SEC,
                cache_ttl_sec=MARKET_CONTEXT_CACHE_TTL_SEC,
                retry_count=MARKET_CONTEXT_RETRY_COUNT,
            ),
        }

    def _ordered_venues(self, requested_venue: str | None) -> list[str]:
        ordered = []
        for venue in [
            str(requested_venue or "").strip().lower(),
            self.primary_venue,
            self.secondary_venue,
        ]:
            if venue and venue not in ordered:
                ordered.append(venue)
        return ordered

    def get_market_context(self, token_or_pair, ts, venue: str = "binance_perp") -> dict:
        for candidate_venue in self._ordered_venues(venue):
            client = self.clients.get(candidate_venue)
            if client is None:
                continue
            try:
                payload = client.fetch_market_context(token_or_pair, alert_ts=int(ts or 0))
                return self.finalize_context(payload, source="live_public", venue=candidate_venue)
            except (MarketDataClientError, ValueError, TypeError, TimeoutError, OSError):
                continue
        return self.finalize_context({}, source="unavailable")


def build_market_context_adapter() -> MarketContextAdapter:
    mode = str(MARKET_CONTEXT_ADAPTER_MODE or "unavailable").strip().lower()
    if mode == "fixture":
        return FixtureMarketContextAdapter(fixture_path=MARKET_CONTEXT_FIXTURE_PATH)
    if mode == "live":
        return LiveMarketContextAdapter()
    return UnavailableMarketContextAdapter()
