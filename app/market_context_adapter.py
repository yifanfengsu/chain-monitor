from __future__ import annotations

import json
from pathlib import Path

from config import MARKET_CONTEXT_ADAPTER_MODE, MARKET_CONTEXT_FIXTURE_PATH
from lp_product_helpers import canonical_asset_symbol, normalize_symbol


DEFAULT_MARKET_CONTEXT = {
    "perp_last_price": None,
    "perp_mark_price": None,
    "perp_index_price": None,
    "spot_reference_price": None,
    "funding_direction": None,
    "funding_estimate": None,
    "basis_bps": None,
    "market_move_before_alert_30s": None,
    "market_move_before_alert_60s": None,
    "market_move_after_alert_60s": None,
    "market_move_after_alert_300s": None,
    "alert_relative_timing": "",
    "market_context_source": "unavailable",
}


def _normalize_lookup_key(token_or_pair: str | None, venue: str) -> str:
    raw = str(token_or_pair or "").strip().upper()
    if not raw:
        return f"{venue}:"
    if "/" in raw:
        left, right = [normalize_symbol(part) for part in raw.split("/", 1)]
        return f"{venue}:{canonical_asset_symbol(left)}/{normalize_symbol(right)}"
    return f"{venue}:{canonical_asset_symbol(raw)}"


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

    def classify_alert_relative_timing(self, context: dict) -> str:
        move_before_30s = abs(float(context.get("market_move_before_alert_30s") or 0.0))
        move_before_60s = abs(float(context.get("market_move_before_alert_60s") or 0.0))
        move_after_60s = abs(float(context.get("market_move_after_alert_60s") or 0.0))
        if max(move_before_30s, move_before_60s) <= 0.003 and move_after_60s >= 0.002:
            return "leading"
        if max(move_before_30s, move_before_60s) <= 0.015:
            return "confirming"
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
            fallback = dict(DEFAULT_MARKET_CONTEXT)
            fallback["market_context_source"] = "unavailable"
            return fallback

        context = dict(DEFAULT_MARKET_CONTEXT)
        context.update(payload)
        if not context.get("alert_relative_timing"):
            context["alert_relative_timing"] = self.classify_alert_relative_timing(context)
        context["market_context_source"] = "fixture"
        return context


class LiveMarketContextAdapter(MarketContextAdapter):
    """Optional live adapter seam. This patch keeps it offline-safe on purpose."""

    def get_market_context(self, token_or_pair, ts, venue: str = "binance_perp") -> dict:
        del token_or_pair, ts, venue
        return dict(DEFAULT_MARKET_CONTEXT)


def build_market_context_adapter() -> MarketContextAdapter:
    mode = str(MARKET_CONTEXT_ADAPTER_MODE or "unavailable").strip().lower()
    if mode == "fixture":
        return FixtureMarketContextAdapter(fixture_path=MARKET_CONTEXT_FIXTURE_PATH)
    if mode == "live":
        return LiveMarketContextAdapter()
    return UnavailableMarketContextAdapter()
