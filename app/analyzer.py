from models import Event


class BehaviorAnalyzer:
    """基于地址最近交易序列识别行为模式，并按角色体系抑制执行器/协议噪声。"""

    def __init__(
        self,
        window_sec: int = 3600,
        min_consecutive_count: int = 3,
        whale_single_trade_usd: float = 20000.0,
    ) -> None:
        self.window_sec = window_sec
        self.min_consecutive_count = min_consecutive_count
        self.whale_single_trade_usd = whale_single_trade_usd

    def detect(self, event: Event, snapshot: dict) -> dict:
        """输入当前事件和地址快照，输出行为标签。"""
        recent = snapshot.get("recent", [])
        if not recent:
            return {
                "behavior_type": "normal",
                "confidence": 0.5,
                "window_sec": self.window_sec,
                "reason": "无足够历史数据",
            }

        role_decision = self._role_guard(event, recent)
        if role_decision:
            return role_decision

        latest = recent[-1]
        latest_usd = float(latest.usd_value or 0.0)
        if latest_usd >= self.whale_single_trade_usd:
            return {
                "behavior_type": "whale_action",
                "confidence": 0.88,
                "window_sec": self.window_sec,
                "reason": f"单笔金额 {latest_usd:.2f} USD",
            }

        consecutive_buy = self._count_consecutive_swaps(recent, side="买入")
        if consecutive_buy >= self.min_consecutive_count and float(snapshot.get("buy_usd") or 0.0) > 1000:
            confidence = min(0.95, 0.65 + 0.07 * consecutive_buy)
            return {
                "behavior_type": "accumulation",
                "confidence": confidence,
                "window_sec": self.window_sec,
                "reason": f"连续买入 {consecutive_buy} 笔",
            }

        consecutive_sell = self._count_consecutive_swaps(recent, side="卖出")
        if consecutive_sell >= self.min_consecutive_count and float(snapshot.get("sell_usd") or 0.0) > 1000:
            confidence = min(0.95, 0.65 + 0.07 * consecutive_sell)
            return {
                "behavior_type": "distribution",
                "confidence": confidence,
                "window_sec": self.window_sec,
                "reason": f"连续卖出 {consecutive_sell} 笔",
            }

        if self._looks_like_scalping(recent):
            return {
                "behavior_type": "scalping",
                "confidence": 0.72,
                "window_sec": self.window_sec,
                "reason": "短周期内频繁双向交易",
            }

        return {
            "behavior_type": "normal",
            "confidence": 0.55,
            "window_sec": self.window_sec,
            "reason": "未命中特殊行为模式",
        }

    def _role_guard(self, event: Event, recent: list[Event]) -> dict | None:
        strategy_role = str(event.strategy_role or "unknown")
        semantic_role = str(event.semantic_role or "unknown")

        if strategy_role == "aggregator_router" or semantic_role == "router_contract":
            return {
                "behavior_type": "normal",
                "confidence": 0.3,
                "window_sec": self.window_sec,
                "reason": "路由/聚合执行地址，不做主观交易行为推断",
            }

        if strategy_role == "protocol_treasury" or semantic_role == "protocol_wallet":
            return {
                "behavior_type": "normal",
                "confidence": 0.35,
                "window_sec": self.window_sec,
                "reason": "协议资金地址，优先按运营/调拨理解",
            }

        if strategy_role == "market_maker_wallet":
            inventory_signal = self._market_maker_inventory_signal(event, recent)
            if inventory_signal:
                return inventory_signal
            if float(event.usd_value or 0.0) >= self.whale_single_trade_usd * 1.5:
                return {
                    "behavior_type": "whale_action",
                    "confidence": 0.82,
                    "window_sec": self.window_sec,
                    "reason": "做市地址的大额库存变动",
                }
            if self._has_fast_bilateral_swaps(recent):
                return {
                    "behavior_type": "inventory_management",
                    "confidence": 0.68,
                    "window_sec": self.window_sec,
                    "reason": "做市地址短周期双向换手，偏库存管理",
                }
            return {
                "behavior_type": "normal",
                "confidence": 0.45,
                "window_sec": self.window_sec,
                "reason": "做市地址，避免误判为主观建仓/出货",
            }

        if strategy_role == "lp_pool":
            if event.kind == "swap":
                consecutive_buy = self._count_consecutive_swaps(recent, side="买入")
                consecutive_sell = self._count_consecutive_swaps(recent, side="卖出")
                if event.side == "买入" and consecutive_buy >= max(self.min_consecutive_count - 1, 2):
                    return {
                        "behavior_type": "pool_buy_pressure",
                        "confidence": min(0.92, 0.68 + 0.06 * consecutive_buy),
                        "window_sec": self.window_sec,
                        "reason": f"同池连续买压 {consecutive_buy} 笔",
                    }
                if event.side == "卖出" and consecutive_sell >= max(self.min_consecutive_count - 1, 2):
                    return {
                        "behavior_type": "pool_sell_pressure",
                        "confidence": min(0.92, 0.68 + 0.06 * consecutive_sell),
                        "window_sec": self.window_sec,
                        "reason": f"同池连续卖压 {consecutive_sell} 笔",
                    }
                return {
                    "behavior_type": "normal",
                    "confidence": 0.62,
                    "window_sec": self.window_sec,
                    "reason": "流动性池事件，先按市场结构节点处理",
                }

            if float(event.usd_value or 0.0) >= self.whale_single_trade_usd * 2.5:
                return {
                    "behavior_type": "whale_action",
                    "confidence": 0.80,
                    "window_sec": self.window_sec,
                    "reason": "池子流动性结构发生大额变化",
                }
            return {
                "behavior_type": "normal",
                "confidence": 0.60,
                "window_sec": self.window_sec,
                "reason": "池子流动性动作，优先按结构事件理解",
            }

        return None

    def _count_consecutive_swaps(self, recent: list[Event], side: str) -> int:
        count = 0
        for event in reversed(recent):
            if event.kind != "swap":
                continue
            if event.side == side:
                count += 1
                continue
            break
        return count

    def _looks_like_scalping(self, recent: list[Event]) -> bool:
        swaps = [item for item in recent if item.kind == "swap"]
        if len(swaps) < 6:
            return False

        latest_ts = int(swaps[-1].ts or 0)
        near = [item for item in swaps if latest_ts - int(item.ts or latest_ts) <= 600]
        if len(near) < 6:
            return False

        sides = {item.side for item in near}
        return "买入" in sides and "卖出" in sides

    def _has_fast_bilateral_swaps(self, recent: list[Event]) -> bool:
        swaps = [item for item in recent if item.kind == "swap"]
        if len(swaps) < 4:
            return False

        latest_ts = int(swaps[-1].ts or 0)
        near = [item for item in swaps if latest_ts - int(item.ts or latest_ts) <= 900]
        if len(near) < 4:
            return False

        sides = {item.side for item in near}
        return "买入" in sides and "卖出" in sides

    def _market_maker_inventory_signal(self, event: Event, recent: list[Event]) -> dict | None:
        current_usd = float(event.usd_value or 0.0)
        latest_ts = int(getattr(recent[-1], "ts", 0) or event.ts or 0)
        near = [item for item in recent if latest_ts - int(getattr(item, "ts", latest_ts) or latest_ts) <= 900]
        same_token = [
            item
            for item in near
            if str(getattr(item, "token", "") or "").lower() == str(event.token or "").lower()
        ]
        intent_type = str(event.intent_type or "")
        behavior_floor = 0.56
        inbound_count = 0
        outbound_count = 0
        bilateral_count = 0
        for item in same_token:
            side = str(getattr(item, "side", "") or "")
            if side in {"流入", "买入"}:
                inbound_count += 1
            elif side in {"流出", "卖出"}:
                outbound_count += 1
            if side in {"流入", "流出", "买入", "卖出"}:
                bilateral_count += 1

        strong_value = current_usd >= self.whale_single_trade_usd * 1.35
        very_large_value = current_usd >= self.whale_single_trade_usd * 2.3
        fast_bilateral = self._has_fast_bilateral_swaps(recent) or (inbound_count >= 2 and outbound_count >= 2)
        same_token_active = len(same_token) >= 2
        raw_meta = event.metadata.get("raw") or {}
        possible_internal = bool(
            raw_meta.get("possible_internal_transfer")
            or str(raw_meta.get("exchange_internality") or "") == "confirmed"
        )

        if very_large_value and same_token_active and outbound_count >= inbound_count + 1:
            return {
                "behavior_type": "inventory_distribution",
                "confidence": 0.74,
                "window_sec": self.window_sec,
                "reason": "做市地址出现大额同 token 外流，偏库存分配/派发",
            }
        if very_large_value and same_token_active and inbound_count >= outbound_count + 1:
            return {
                "behavior_type": "inventory_expansion",
                "confidence": 0.73,
                "window_sec": self.window_sec,
                "reason": "做市地址出现大额同 token 回补，偏库存扩张",
            }
        if fast_bilateral and same_token_active and bilateral_count >= 3:
            return {
                "behavior_type": "inventory_shift",
                "confidence": 0.66,
                "window_sec": self.window_sec,
                "reason": "短周期双向换手明显，偏库存切换/方向调整",
            }
        if intent_type == "market_making_inventory_move" and (strong_value or same_token_active or possible_internal):
            return {
                "behavior_type": "inventory_management",
                "confidence": 0.64 if strong_value or same_token_active else behavior_floor,
                "window_sec": self.window_sec,
                "reason": "路径与金额更像做市库存管理，而非普通转移",
            }
        if possible_internal and strong_value:
            return {
                "behavior_type": "inventory_management",
                "confidence": 0.60,
                "window_sec": self.window_sec,
                "reason": "做市地址内部调拨金额较大，偏库存管理",
            }
        return None
