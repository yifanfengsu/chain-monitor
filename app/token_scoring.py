from constants import STABLE_TOKEN_CONTRACTS, TOKEN_QUALITY_HINTS
from models import Event


class TokenScorer:
    """Token 市场质量评分，输出 score(0~100) 与等级。"""

    def score(self, event: Event, snapshot: dict) -> dict:
        token = (event.token or "").lower()
        if not token:
            return {
                "score": 35.0,
                "token_quality_score": 35.0,
                "grade": "C",
                "factors": {"reason": "no_token"},
            }

        hint = TOKEN_QUALITY_HINTS.get(token, {})
        activity = int(snapshot.get("activity", 0))
        net_flow_usd = float(snapshot.get("net_flow_usd", 0.0))
        buy_count = int(snapshot.get("buy_count", 0))
        sell_count = int(snapshot.get("sell_count", 0))
        liquidity_proxy_usd = float(
            snapshot.get("liquidity_proxy_usd", 0.0)
            or snapshot.get("liquidity_usd", 0.0)
            or hint.get("liquidity_proxy_usd", 0.0)
            or hint.get("liquidity_usd", 0.0)
        )
        volume_24h_proxy_usd = float(
            snapshot.get("volume_24h_proxy_usd", 0.0)
            or snapshot.get("volume_24h_usd", 0.0)
            or hint.get("volume_24h_proxy_usd", 0.0)
            or hint.get("volume_24h_usd", 0.0)
        )
        holder_distribution = float(
            snapshot.get("holder_distribution_score", 0.0) or hint.get("holder_distribution", 0.0)
        )
        contract_age_days = int(snapshot.get("contract_age_days", 0) or hint.get("contract_age_days", 0))
        swap_frequency = float(snapshot.get("swap_frequency", 0.0))
        pricing_confidence = float(event.pricing_confidence or 0.0)
        pricing_status = str(event.pricing_status or "unknown")

        score = 38.0

        if liquidity_proxy_usd >= 50_000_000:
            score += 14.0
        elif liquidity_proxy_usd >= 10_000_000:
            score += 10.0
        elif liquidity_proxy_usd >= 2_000_000:
            score += 6.0
        elif liquidity_proxy_usd >= 500_000:
            score += 3.0
        else:
            score -= 9.0

        if volume_24h_proxy_usd >= 200_000_000:
            score += 14.0
        elif volume_24h_proxy_usd >= 30_000_000:
            score += 10.0
        elif volume_24h_proxy_usd >= 5_000_000:
            score += 5.0
        elif volume_24h_proxy_usd < 1_000_000:
            score -= 7.0

        score += max(-6.0, min(6.0, (holder_distribution - 0.5) * 16.0))

        if contract_age_days >= 365:
            score += 5.0
        elif contract_age_days >= 30:
            score += 2.0
        else:
            score -= 5.0

        if 1.0 <= swap_frequency <= 60.0:
            score += 6.0
        elif swap_frequency < 0.1:
            score -= 4.0
        elif swap_frequency > 150.0:
            score -= 4.0

        imbalance = abs(buy_count - sell_count)
        if buy_count + sell_count > 0:
            score -= min(8.0, imbalance)

        if abs(net_flow_usd) > 100_000:
            score += 4.0
        elif abs(net_flow_usd) < 1_000:
            score -= 2.0

        if activity > 50:
            score += 3.0
        elif activity < 3:
            score -= 3.0

        # 稳定币属于高质量资产，但不再自动把事件质量托到很高。
        if token in STABLE_TOKEN_CONTRACTS:
            score += 2.0
            score = max(score, 54.0)

        reliability_multiplier = 1.0
        if pricing_status == "estimated":
            reliability_multiplier = 0.92
        elif pricing_status in {"unknown", "unavailable"}:
            reliability_multiplier = 0.78
        reliability_multiplier *= max(0.7, 0.72 + 0.28 * pricing_confidence)

        score = 52.0 + (score - 52.0) * reliability_multiplier
        if pricing_status in {"unknown", "unavailable"}:
            score -= 4.0

        score = max(0.0, min(100.0, score))
        grade = "A" if score >= 82 else "B" if score >= 65 else "C"
        return {
            "score": round(score, 2),
            "token_quality_score": round(score, 2),
            "grade": grade,
            "factors": {
                "activity": activity,
                "net_flow_usd": round(net_flow_usd, 2),
                "buy_count": buy_count,
                "sell_count": sell_count,
                "liquidity_proxy_usd": round(liquidity_proxy_usd, 2),
                "volume_24h_proxy_usd": round(volume_24h_proxy_usd, 2),
                "liquidity_usd": round(liquidity_proxy_usd, 2),
                "volume_24h_usd": round(volume_24h_proxy_usd, 2),
                "holder_distribution_score": round(holder_distribution, 3),
                "contract_age_days": contract_age_days,
                "swap_frequency": round(swap_frequency, 3),
                "pricing_status": pricing_status,
                "pricing_confidence": round(pricing_confidence, 3),
                "reliability_multiplier": round(reliability_multiplier, 3),
            },
        }
