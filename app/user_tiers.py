from __future__ import annotations

from dataclasses import asdict, dataclass

from config import (
    DEFAULT_USER_TIER,
    LP_QUALITY_MIN_PREALERT_PRECISION_FOR_RETAIL,
    LP_QUALITY_MIN_PREALERT_PRECISION_FOR_TRADER,
)
from lp_product_helpers import is_major_asset_symbol


LP_STAGES = {"prealert", "confirm", "climax", "exhaustion_risk"}


@dataclass(frozen=True)
class UserTierProfile:
    name: str
    allowed_lp_stages: tuple[str, ...]
    allow_prealert: bool
    allow_single_pool_prealert: bool
    allow_long_tail_pool: bool
    show_debug_latency: bool
    show_exhaustion_risk: bool
    show_clmm_pool_only_details: bool
    show_followup_line: bool
    show_quality_hint: bool
    show_market_context_line: bool
    prefer_asset_case_primary: bool
    prefer_asset_case_single_pool: bool
    allow_partial_support_debug: bool
    allow_fastlane_results: bool
    default_message_template: str
    min_asset_case_quality: float
    min_prealert_quality: float
    min_confirm_quality: float


USER_TIER_PROFILES = {
    "retail": UserTierProfile(
        name="retail",
        allowed_lp_stages=("confirm", "exhaustion_risk"),
        allow_prealert=False,
        allow_single_pool_prealert=False,
        allow_long_tail_pool=False,
        show_debug_latency=False,
        show_exhaustion_risk=True,
        show_clmm_pool_only_details=False,
        show_followup_line=False,
        show_quality_hint=True,
        show_market_context_line=True,
        prefer_asset_case_primary=True,
        prefer_asset_case_single_pool=True,
        allow_partial_support_debug=False,
        allow_fastlane_results=False,
        default_message_template="brief",
        min_asset_case_quality=0.62,
        min_prealert_quality=float(LP_QUALITY_MIN_PREALERT_PRECISION_FOR_RETAIL),
        min_confirm_quality=0.56,
    ),
    "trader": UserTierProfile(
        name="trader",
        allowed_lp_stages=("prealert", "confirm", "climax", "exhaustion_risk"),
        allow_prealert=True,
        allow_single_pool_prealert=False,
        allow_long_tail_pool=False,
        show_debug_latency=False,
        show_exhaustion_risk=True,
        show_clmm_pool_only_details=False,
        show_followup_line=True,
        show_quality_hint=True,
        show_market_context_line=True,
        prefer_asset_case_primary=True,
        prefer_asset_case_single_pool=True,
        allow_partial_support_debug=False,
        allow_fastlane_results=True,
        default_message_template="brief",
        min_asset_case_quality=0.54,
        min_prealert_quality=float(LP_QUALITY_MIN_PREALERT_PRECISION_FOR_TRADER),
        min_confirm_quality=0.48,
    ),
    "research": UserTierProfile(
        name="research",
        allowed_lp_stages=("prealert", "confirm", "climax", "exhaustion_risk"),
        allow_prealert=True,
        allow_single_pool_prealert=True,
        allow_long_tail_pool=True,
        show_debug_latency=True,
        show_exhaustion_risk=True,
        show_clmm_pool_only_details=True,
        show_followup_line=True,
        show_quality_hint=True,
        show_market_context_line=True,
        prefer_asset_case_primary=False,
        prefer_asset_case_single_pool=False,
        allow_partial_support_debug=True,
        allow_fastlane_results=True,
        default_message_template="",
        min_asset_case_quality=0.0,
        min_prealert_quality=0.0,
        min_confirm_quality=0.0,
    ),
}


def resolve_user_tier(event=None, signal=None, watch_meta: dict | None = None) -> str:
    for candidate in (
        (watch_meta or {}).get("user_tier"),
        getattr(signal, "context", {}).get("user_tier") if signal is not None else None,
        getattr(signal, "metadata", {}).get("user_tier") if signal is not None else None,
        getattr(event, "metadata", {}).get("user_tier") if event is not None else None,
        DEFAULT_USER_TIER,
    ):
        normalized = str(candidate or "").strip().lower()
        if normalized in USER_TIER_PROFILES:
            return normalized
    return "research"


def get_user_tier_profile(tier: str | None) -> UserTierProfile:
    normalized = str(tier or "").strip().lower()
    return USER_TIER_PROFILES.get(normalized, USER_TIER_PROFILES["research"])


def apply_user_tier_context(event=None, signal=None, watch_meta: dict | None = None) -> dict:
    tier = resolve_user_tier(event=event, signal=signal, watch_meta=watch_meta)
    profile = get_user_tier_profile(tier)
    payload = {
        "user_tier": tier,
        "user_tier_profile": asdict(profile),
        "show_debug_latency": bool(profile.show_debug_latency),
        "show_followup_line": bool(profile.show_followup_line),
        "show_quality_hint": bool(profile.show_quality_hint),
        "show_market_context_line": bool(profile.show_market_context_line),
        "prefer_asset_case_primary": bool(profile.prefer_asset_case_primary),
        "prefer_asset_case_single_pool": bool(profile.prefer_asset_case_single_pool),
    }
    if event is not None:
        getattr(event, "metadata", {}).update(payload)
    if signal is not None:
        getattr(signal, "metadata", {}).update(payload)
        context = getattr(signal, "context", {})
        context.update(payload)
        if not context.get("message_template") and profile.default_message_template:
            context["message_template"] = profile.default_message_template
            getattr(signal, "metadata", {})["message_template"] = profile.default_message_template
    return payload


def evaluate_user_tier_lp_delivery(event, signal, delivery_class: str) -> tuple[bool, str]:
    tier = resolve_user_tier(event=event, signal=signal)
    profile = get_user_tier_profile(tier)
    context = getattr(signal, "context", {}) or {}
    event_metadata = getattr(event, "metadata", {}) or {}

    if bool(context.get("clmm_partial_support") or event_metadata.get("clmm_partial_support")) and not profile.allow_partial_support_debug:
        return False, f"user_tier_{tier}_partial_support_filtered"

    lp_stage = str(context.get("lp_alert_stage") or event_metadata.get("lp_alert_stage") or "").strip()
    if not lp_stage:
        return True, f"user_tier_{tier}_not_lp_stage_constrained"
    if lp_stage not in LP_STAGES:
        return True, f"user_tier_{tier}_unknown_stage_passthrough"
    if lp_stage not in profile.allowed_lp_stages:
        return False, f"user_tier_{tier}_stage_filtered_{lp_stage}"

    asset_symbol = str(
        context.get("asset_case_label")
        or context.get("asset_symbol")
        or event_metadata.get("asset_case_label")
        or event_metadata.get("token_symbol")
        or getattr(event, "token", "")
        or ""
    )
    supporting_pair_count = int(
        context.get("asset_case_supporting_pair_count")
        or event_metadata.get("asset_case_supporting_pair_count")
        or 0
    )
    pool_quality_score = float(
        context.get("pool_quality_score")
        or event_metadata.get("pool_quality_score")
        or 0.58
    )
    pair_quality_score = float(
        context.get("pair_quality_score")
        or event_metadata.get("pair_quality_score")
        or 0.58
    )
    asset_case_quality_score = float(
        context.get("asset_case_quality_score")
        or event_metadata.get("asset_case_quality_score")
        or max(pool_quality_score, pair_quality_score, 0.58)
    )
    prealert_precision_score = float(
        context.get("prealert_precision_score")
        or event_metadata.get("prealert_precision_score")
        or 0.58
    )
    fastlane_roi_score = float(
        context.get("fastlane_roi_score")
        or event_metadata.get("fastlane_roi_score")
        or 0.55
    )
    long_tail = (
        not is_major_asset_symbol(asset_symbol)
        and max(asset_case_quality_score, pair_quality_score, pool_quality_score) < 0.64
    )

    if lp_stage == "prealert":
        if not profile.allow_prealert:
            return False, f"user_tier_{tier}_prealert_disabled"
        if supporting_pair_count <= 1 and not profile.allow_single_pool_prealert:
            return False, f"user_tier_{tier}_single_pool_prealert_filtered"
        if prealert_precision_score < profile.min_prealert_quality:
            return False, f"user_tier_{tier}_prealert_precision_filtered"
        if asset_case_quality_score < profile.min_asset_case_quality:
            return False, f"user_tier_{tier}_prealert_quality_filtered"
    elif lp_stage == "confirm" and asset_case_quality_score < profile.min_confirm_quality:
        return False, f"user_tier_{tier}_confirm_quality_filtered"
    elif lp_stage == "climax" and asset_case_quality_score < max(profile.min_confirm_quality, 0.50):
        return False, f"user_tier_{tier}_climax_quality_filtered"

    if long_tail and not profile.allow_long_tail_pool:
        return False, f"user_tier_{tier}_long_tail_filtered"
    if delivery_class == "primary" and not profile.allow_fastlane_results and fastlane_roi_score < 0.50:
        return False, f"user_tier_{tier}_fastlane_filtered"
    return True, f"user_tier_{tier}_allowed"


def should_use_asset_case_primary(context: dict, tier: str | None = None) -> bool:
    profile = get_user_tier_profile(tier or context.get("user_tier"))
    supporting_pair_count = int(context.get("asset_case_supporting_pair_count") or 0)
    if supporting_pair_count >= 2:
        return True
    if bool(profile.prefer_asset_case_single_pool) and str(context.get("lp_alert_stage") or "") != "prealert":
        return bool(context.get("asset_case_id"))
    return bool(profile.prefer_asset_case_primary and supporting_pair_count >= 2)
