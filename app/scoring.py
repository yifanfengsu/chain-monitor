class AddressScorer:
    """地址长期质量评分：输出 score(0~100) 与等级 A/B/C。"""

    CATEGORY_BASE_SCORE = {
        "smart_money": 72,
        "exchange_whale": 54,
        "issuer_whale": 58,
        "celebrity_whale": 60,
        "user_added": 44,
        "lp_pool": 58,
        "unknown": 42,
    }

    PRIORITY_BONUS = {
        1: 15,
        2: 8,
        3: 0,
    }

    BEHAVIOR_BONUS = {
        "accumulation": 10,
        "distribution": 4,
        "whale_action": 6,
        "inventory_management": -2,
        "scalping": -10,
        "normal": 0,
    }

    STRATEGY_ROLE_ADJUSTMENT = {
        "smart_money_wallet": 6,
        "alpha_wallet": 8,
        "market_maker_wallet": 5,
        "celebrity_wallet": 2,
        "treasury_issuer": 2,
        "exchange_hot_wallet": -14,
        "exchange_deposit_wallet": -16,
        "exchange_trading_wallet": -11,
        "aggregator_router": -12,
        "protocol_treasury": -7,
        "liquidity_provider": -5,
        "lp_pool": 6,
        "arbitrage_bot": -12,
        "user_watch": 0,
        "unknown": 0,
    }

    def score(self, meta: dict, snapshot: dict, behavior: dict) -> dict:
        category = meta.get("category", "unknown")
        strategy_role = str(meta.get("strategy_role") or "unknown")
        try:
            priority = int(meta.get("priority", 3) or 3)
        except (TypeError, ValueError):
            priority = 3
        behavior_type = behavior.get("behavior_type", "normal")

        base = self.CATEGORY_BASE_SCORE.get(category, self.CATEGORY_BASE_SCORE["unknown"])
        score = float(base)
        score += float(self.PRIORITY_BONUS.get(priority, 0))
        score += float(self.BEHAVIOR_BONUS.get(behavior_type, 0))
        score += float(self.STRATEGY_ROLE_ADJUSTMENT.get(strategy_role, 0))

        recent_count = int(snapshot.get("recent_count", len(snapshot.get("recent", []))))
        trade_frequency_1h = float(snapshot.get("trade_frequency_1h", 0.0))
        avg_usd = float(snapshot.get("avg_usd", 0.0))
        usd_std = float(snapshot.get("usd_std", 0.0))
        behavior_consistency = float(snapshot.get("behavior_consistency", 0.5))

        # 活跃度只给温和加分，避免高频执行地址仅因“活跃”获得过高分。
        if 0.8 <= trade_frequency_1h <= 10.0:
            score += 5.0
        elif trade_frequency_1h > 25.0:
            score -= 7.0
        elif trade_frequency_1h < 0.15:
            score -= 4.0

        # 资金规模大但稳定，才更接近“值得长期关注的情报源”。
        if avg_usd >= 15_000:
            score += 8.0
        elif avg_usd >= 5_000:
            score += 5.0
        elif avg_usd >= 2_000:
            score += 2.0
        elif avg_usd < 500:
            score -= 7.0

        if avg_usd > 0:
            cv = usd_std / avg_usd
            if cv < 0.85:
                score += 4.0
            elif cv > 2.2:
                score -= 6.0

        buy_count = int(snapshot.get("buy_count", 0))
        sell_count = int(snapshot.get("sell_count", 0))
        balance = abs(buy_count - sell_count)
        if buy_count + sell_count > 0:
            stability_bonus = max(0.0, 4.5 - 0.5 * balance)
            score += stability_bonus

        # 一致性是长期质量辅助因子，不再放大得过强。
        score += (behavior_consistency - 0.5) * 8.0

        label = str(meta.get("label", "")).lower()
        note = str(meta.get("note", "")).lower()
        if "contract" in label or "contract" in note:
            score -= 6.0

        if strategy_role.startswith("exchange_") and trade_frequency_1h > 20.0:
            score -= 4.0

        score = max(0.0, min(100.0, score))
        grade = self._grade(score)

        return {
            "score": round(score, 2),
            "grade": grade,
            "factors": {
                "category": category,
                "priority": priority,
                "strategy_role": strategy_role,
                "behavior_type": behavior_type,
                "recent_count": recent_count,
                "trade_frequency_1h": round(trade_frequency_1h, 3),
                "avg_usd": round(avg_usd, 2),
                "usd_std": round(usd_std, 2),
                "behavior_consistency": round(behavior_consistency, 3),
                "buy_count": buy_count,
                "sell_count": sell_count,
            },
        }

    def _grade(self, score: float) -> str:
        if score >= 84:
            return "A"
        if score >= 66:
            return "B"
        return "C"
