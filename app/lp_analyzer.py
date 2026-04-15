from constants import ETH_EQUIVALENT_CONTRACTS, ETH_EQUIVALENT_SYMBOLS, STABLE_TOKEN_CONTRACTS, STABLE_TOKEN_SYMBOLS
from config import LP_STRUCTURE_MIN_USD_PER_EVENT
from models import Event


POOL_CANONICAL_SEMANTIC_KEYS = {
    "pool_buy_pressure",
    "pool_sell_pressure",
    "liquidity_addition",
    "liquidity_removal",
    "pool_rebalance",
    "pool_noise",
}
POOL_SEMANTIC_KEY_ALIASES = {
    "lp_buy_pressure": "pool_buy_pressure",
    "LP_Buy_Pressure": "pool_buy_pressure",
    "lp_sell_pressure": "pool_sell_pressure",
    "LP_Sell_Pressure": "pool_sell_pressure",
    "lp_liquidity_add": "liquidity_addition",
    "LP_Liquidity_Add": "liquidity_addition",
    "lp_liquidity_remove": "liquidity_removal",
    "LP_Liquidity_Remove": "liquidity_removal",
    "lp_rebalance": "pool_rebalance",
    "LP_Rebalance": "pool_rebalance",
}
LP_BUY_INTENTS = {"pool_buy_pressure"}
LP_SELL_INTENTS = {"pool_sell_pressure"}
LP_LIQUIDITY_INTENTS = {"liquidity_addition", "liquidity_removal", "pool_rebalance", "pool_noise"}
LP_ALL_INTENTS = LP_BUY_INTENTS | LP_SELL_INTENTS | LP_LIQUIDITY_INTENTS


def canonicalize_pool_semantic_key(value: str | None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    return POOL_SEMANTIC_KEY_ALIASES.get(normalized, normalized)


def _is_stable(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in STABLE_TOKEN_CONTRACTS or symbol in STABLE_TOKEN_SYMBOLS


def _is_eth_like(token_contract: str | None, token_symbol: str | None) -> bool:
    contract = str(token_contract or "").lower()
    symbol = str(token_symbol or "").upper()
    return contract in ETH_EQUIVALENT_CONTRACTS or symbol in ETH_EQUIVALENT_SYMBOLS


def _display_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").upper()
    return "ETH" if normalized == "WETH" else normalized


class LPAnalyzer:
    """LP 池子专用解析与意图确认逻辑，描述池子 order-flow / 流动性结构。"""

    def __init__(self) -> None:
        self.structure_min_usd_per_event = float(LP_STRUCTURE_MIN_USD_PER_EVENT)

    def parse_pool_candidate(
        self,
        item: dict,
        pool_meta: dict,
        transfers: list[dict],
        flows: dict,
        tx_hash: str,
    ) -> dict | None:
        self._set_parse_debug(item, pool_meta, status="pending", reason="lp_parse_started")
        pool_address = str(pool_meta.get("pool_address") or "").lower()
        base_token = str(pool_meta.get("base_token_contract") or "").lower()
        quote_token = str(pool_meta.get("quote_token_contract") or "").lower()
        base_symbol = _display_symbol(pool_meta.get("base_token_symbol"))
        quote_symbol = _display_symbol(pool_meta.get("quote_token_symbol"))

        base_flow = flows.get(base_token)
        quote_flow = flows.get(quote_token)
        if base_flow is None or quote_flow is None:
            # TODO: CLMM position manager 路径需要独立 parser；当前只覆盖双腿 transfer 可见的池子结构流。
            missing_legs = []
            if base_flow is None:
                missing_legs.append("base_leg")
            if quote_flow is None:
                missing_legs.append("quote_leg")
            self._set_parse_debug(
                item,
                pool_meta,
                status="failed",
                reason="lp_parse_missing_required_leg",
                missing_legs=missing_legs,
            )
            return None

        base_net = float(base_flow.get("net") or 0.0)
        quote_net = float(quote_flow.get("net") or 0.0)
        base_amount = abs(base_net) if abs(base_net) > 0 else max(float(base_flow.get("in") or 0.0), float(base_flow.get("out") or 0.0))
        quote_amount = abs(quote_net) if abs(quote_net) > 0 else max(float(quote_flow.get("in") or 0.0), float(quote_flow.get("out") or 0.0))
        if base_amount <= 0 and quote_amount <= 0:
            self._set_parse_debug(
                item,
                pool_meta,
                status="failed",
                reason="lp_parse_zero_dual_leg_amount",
                missing_legs=[],
            )
            return None

        action = "pool_noise"
        kind = "lp_rebalance"
        side = "再平衡"
        direction = "rebalance"

        if base_net < 0 and quote_net > 0:
            action = "swap"
            kind = "swap"
            side = "买入"
            direction = "buy"
        elif base_net > 0 and quote_net < 0:
            action = "swap"
            kind = "swap"
            side = "卖出"
            direction = "sell"
        elif base_net > 0 and quote_net > 0:
            action = "liquidity_addition"
            kind = "lp_add_liquidity"
            side = "增加流动性"
            direction = "liquidity_add"
        elif base_net < 0 and quote_net < 0:
            action = "liquidity_removal"
            kind = "lp_remove_liquidity"
            side = "减少流动性"
            direction = "liquidity_remove"
        elif abs(base_net) > 0 or abs(quote_net) > 0:
            action = "pool_rebalance"
            kind = "lp_rebalance"
            side = "再平衡"
            direction = "rebalance"

        counterparty = self._resolve_counterparty(
            transfers=transfers,
            pool_address=pool_address,
            base_token=base_token,
            quote_token=quote_token,
            direction=direction,
        )
        flow_from, flow_to = self._primary_path(pool_address, counterparty, direction)
        quote_from, quote_to, token_from, token_to = self._leg_paths(pool_address, counterparty, direction)
        self._set_parse_debug(
            item,
            pool_meta,
            status="parsed",
            reason="lp_parse_success",
            missing_legs=[],
        )

        return {
            "kind": kind,
            "side": side,
            "direction": side,
            "monitor_type": "lp_pool",
            "watch_address": pool_address,
            "from": flow_from,
            "to": flow_to,
            "counterparty": counterparty,
            "value": base_amount,
            "token_contract": base_token,
            "token_symbol": base_symbol,
            "quote_token_contract": quote_token,
            "quote_symbol": quote_symbol,
            "token_amount": base_amount,
            "quote_amount": quote_amount,
            "tx_hash": tx_hash,
            "pair_label": pool_meta.get("pair_label", ""),
            "dex": pool_meta.get("dex", ""),
            "protocol": pool_meta.get("protocol", ""),
            "quote_from": quote_from,
            "quote_to": quote_to,
            "token_from": token_from,
            "token_to": token_to,
            "flow_source_label": flow_from,
            "flow_target_label": flow_to,
            "lp_legs": [
                {
                    "token_contract": base_token,
                    "token_symbol": base_symbol,
                    "amount": base_amount,
                    "net": base_net,
                    "is_base": True,
                    "is_quote": False,
                },
                {
                    "token_contract": quote_token,
                    "token_symbol": quote_symbol,
                    "amount": quote_amount,
                    "net": quote_net,
                    "is_base": False,
                    "is_quote": True,
                },
            ],
            "lp_context": {
                "pool_address": pool_address,
                "pool_label": pool_meta.get("label", ""),
                "pair_label": pool_meta.get("pair_label", ""),
                "dex": pool_meta.get("dex", ""),
                "protocol": pool_meta.get("protocol", ""),
                "action": action,
                "direction": direction,
                "base_token_contract": base_token,
                "base_token_symbol": base_symbol,
                "quote_token_contract": quote_token,
                "quote_token_symbol": quote_symbol,
                "base_amount": round(base_amount, 8),
                "quote_amount": round(quote_amount, 8),
                "base_net": round(base_net, 8),
                "quote_net": round(quote_net, 8),
            },
        }

    def preliminary_intent(self, event: Event, parsed: dict) -> dict:
        lp_context = parsed.get("lp_context") or {}
        action = str(lp_context.get("action") or "pool_noise")
        base_symbol = str(lp_context.get("base_token_symbol") or event.metadata.get("token_symbol") or event.token or "资产")
        quote_symbol = str(lp_context.get("quote_token_symbol") or parsed.get("quote_symbol") or "稳定币")

        if action == "swap" and event.side == "买入":
            return {
                "intent_type": "pool_buy_pressure",
                "intent_confidence": 0.78,
                "information_level": "high",
                "confirmation_score": 0.42,
                "intent_evidence": [f"池子内 {quote_symbol} 净流入，{base_symbol} 净流出"],
            }
        if action == "swap" and event.side == "卖出":
            return {
                "intent_type": "pool_sell_pressure",
                "intent_confidence": 0.78,
                "information_level": "high",
                "confirmation_score": 0.42,
                "intent_evidence": [f"池子内 {base_symbol} 净流入，{quote_symbol} 净流出"],
            }
        if action == "liquidity_addition":
            return {
                "intent_type": "liquidity_addition",
                "intent_confidence": 0.72,
                "information_level": "medium",
                "confirmation_score": 0.36,
                "intent_evidence": [f"池子两侧资产同步流入，流动性增加 ({base_symbol}/{quote_symbol})"],
            }
        if action == "liquidity_removal":
            return {
                "intent_type": "liquidity_removal",
                "intent_confidence": 0.76,
                "information_level": "medium",
                "confirmation_score": 0.40,
                "intent_evidence": [f"池子两侧资产同步流出，流动性下降 ({base_symbol}/{quote_symbol})"],
            }
        if action == "pool_rebalance":
            return {
                "intent_type": "pool_rebalance",
                "intent_confidence": 0.56,
                "information_level": "low",
                "confirmation_score": 0.24,
                "intent_evidence": [f"池子发生混合型库存调整，先按再平衡观察 ({base_symbol}/{quote_symbol})"],
            }
        return {
            "intent_type": "pool_noise",
            "intent_confidence": 0.28,
            "information_level": "low",
            "confirmation_score": 0.12,
            "intent_evidence": ["池子动作较弱，当前更接近噪声流"],
        }

    def confirm_intent(
        self,
        event: Event,
        parsed: dict,
        address_snapshot: dict,
        pool_snapshot: dict,
        token_snapshot: dict,
        behavior: dict,
        preliminary_intent: dict,
    ) -> dict:
        lp_context = parsed.get("lp_context") or {}
        current_intent = str(preliminary_intent.get("intent_type") or "pool_noise")
        prior_events = self._prior_events(address_snapshot, event)
        structure_eligible = self._is_structure_eligible_event(event)
        eligible_prior = [item for item in prior_events if self._is_structure_eligible_event(item)]
        same_intent_prior = [item for item in eligible_prior if item.intent_type == current_intent]
        same_side_prior = [item for item in eligible_prior if str(item.side or "") == str(event.side or "")]
        same_action_prior = [
            item for item in eligible_prior
            if (item.metadata.get("raw") or {}).get("lp_context", {}).get("action") == lp_context.get("action")
        ]
        pool_recent = list(pool_snapshot.get("recent") or [])
        eligible_pool_recent = [item for item in pool_recent if self._is_structure_eligible_event(item)]

        pool_same_direction_streak = self._eligible_pool_same_direction_streak(eligible_pool_recent)
        same_pool_continuity = 0
        if structure_eligible:
            same_pool_continuity = max(
                max(pool_same_direction_streak - 1, 0),
                len(same_action_prior),
                len(same_intent_prior),
                len(same_side_prior),
            )
        multi_pool_resonance = self._multi_pool_resonance(token_snapshot, event.side, event)
        resonance_score = self._pool_resonance_score(token_snapshot, event.side)
        abnormal_ratio = self._abnormal_ratio(address_snapshot, event)
        pool_window_trade_count = int(pool_snapshot.get("recent_count") or 0)
        pool_window_usd_total = float(pool_snapshot.get("volume_usd") or 0.0)
        pool_volume_surge_ratio = self._pool_volume_surge_ratio(pool_snapshot, event)
        action_intensity = self._action_intensity(
            event,
            abnormal_ratio,
            same_pool_continuity,
            multi_pool_resonance,
            pool_volume_surge_ratio,
        )
        reserve_skew = self._reserve_skew(lp_context, event, abnormal_ratio)
        behavior_conf = float(behavior.get("confidence") or 0.0)

        score = 0.18 + 0.52 * float(preliminary_intent.get("intent_confidence") or 0.0)
        evidence = list(preliminary_intent.get("intent_evidence") or [])

        if same_pool_continuity >= 1:
            score += 0.12
            evidence.append("同池 15 分钟内出现重复同向动作")
        if same_pool_continuity >= 2:
            score += 0.08
            evidence.append("同池短时连续性增强")
        if multi_pool_resonance >= 2:
            score += 0.16
            evidence.append(f"{multi_pool_resonance} 个主流池短时同向")
        if pool_volume_surge_ratio >= 1.5:
            score += 0.10
            evidence.append(f"相对该池短窗均值放大 {pool_volume_surge_ratio:.2f}x")
        if pool_volume_surge_ratio >= 2.2:
            score += 0.06
        if pool_window_trade_count >= 3:
            score += 0.04
        if pool_window_usd_total >= max(float(event.usd_value or 0.0) * 2.4, 120_000.0):
            score += 0.04
        if resonance_score >= 0.55:
            score += 0.08
        if abnormal_ratio >= 1.5:
            score += 0.10
            evidence.append(f"相对池子近 24h 基线放大 {abnormal_ratio:.2f}x")
        if action_intensity >= 0.65:
            score += 0.08
        if reserve_skew >= 0.20:
            score += 0.06
            evidence.append(f"池子偏斜度 {reserve_skew:.2f}")
        if behavior_conf >= 0.7:
            score += 0.04

        if current_intent == "pool_rebalance":
            score -= 0.05
        if current_intent == "pool_noise":
            score -= 0.12
        if (
            current_intent in {"pool_buy_pressure", "pool_sell_pressure"}
            and same_pool_continuity == 0
            and multi_pool_resonance <= 1
            and pool_volume_surge_ratio < 1.25
        ):
            score -= 0.03
            if (
                action_intensity < 0.60
                and reserve_skew < 0.18
                and abnormal_ratio < 1.6
            ):
                score -= 0.03
        if event.pricing_status in {"unknown", "unavailable"}:
            score -= 0.08

        score = self._clamp(score, 0.0, 1.0)
        if current_intent == "pool_noise":
            stage = "weak"
        elif score >= 0.76:
            stage = "confirmed"
        elif score >= 0.50:
            stage = "preliminary"
        else:
            stage = "weak"

        intent_confidence = self._clamp(
            0.52 * float(preliminary_intent.get("intent_confidence") or 0.0) + 0.48 * score,
            0.0,
            1.0,
        )
        if stage == "weak":
            intent_confidence = min(intent_confidence, 0.62 if current_intent != "pool_noise" else 0.42)

        information_level = str(preliminary_intent.get("information_level") or "low")
        if current_intent in {"pool_buy_pressure", "pool_sell_pressure"} and stage == "confirmed":
            information_level = "high"
        elif current_intent in {"liquidity_addition", "liquidity_removal"} and stage != "weak":
            information_level = "medium"
        elif stage == "weak":
            information_level = "low"

        unique_evidence = []
        for item in evidence:
            if item and item not in unique_evidence:
                unique_evidence.append(item)

        raw_same_intent_prior = [item for item in prior_events if item.intent_type == current_intent]
        raw_same_side_prior = [item for item in prior_events if str(item.side or "") == str(event.side or "")]
        raw_same_action_prior = [
            item for item in prior_events
            if (item.metadata.get("raw") or {}).get("lp_context", {}).get("action") == lp_context.get("action")
        ]
        continuity_filtered_by_min_usd = max(
            len(raw_same_action_prior) - len(same_action_prior),
            len(raw_same_intent_prior) - len(same_intent_prior),
            len(raw_same_side_prior) - len(same_side_prior),
            max(int(pool_snapshot.get("same_direction_streak") or 0) - pool_same_direction_streak, 0),
        )
        resonance_filtered_by_min_usd = max(
            self._raw_multi_pool_resonance(token_snapshot, event.side, event) - multi_pool_resonance,
            0,
        )

        return {
            **preliminary_intent,
            "intent_type": current_intent,
            "intent_confidence": round(intent_confidence, 3),
            "information_level": information_level,
            "intent_stage": stage,
            "confirmation_score": round(score, 3),
            "intent_evidence": unique_evidence[:4],
            "same_pool_continuity": same_pool_continuity,
            "multi_pool_resonance": multi_pool_resonance,
            "resonance_score": round(resonance_score, 3),
            "abnormal_ratio": round(abnormal_ratio, 3),
            "reserve_skew": round(reserve_skew, 3),
            "action_intensity": round(action_intensity, 3),
            "pool_volume_surge_ratio": round(pool_volume_surge_ratio, 3),
            "pool_window_trade_count": pool_window_trade_count,
            "pool_window_usd_total": round(pool_window_usd_total, 2),
            "market_impact_hint": self._market_impact_hint(action_intensity, reserve_skew, multi_pool_resonance),
            "lp_structure_min_usd_per_event": round(float(self.structure_min_usd_per_event), 2),
            "lp_continuity_eligible": bool(structure_eligible),
            "lp_resonance_eligible": bool(structure_eligible),
            "lp_continuity_filtered_by_min_usd": int(continuity_filtered_by_min_usd),
            "lp_resonance_filtered_by_min_usd": int(resonance_filtered_by_min_usd),
        }

    def _resolve_counterparty(
        self,
        transfers: list[dict],
        pool_address: str,
        base_token: str,
        quote_token: str,
        direction: str,
    ) -> str:
        if direction == "buy":
            quote_candidates = [
                item for item in transfers
                if item["token_contract"] == quote_token and item["to"] == pool_address
            ]
            token_candidates = [
                item for item in transfers
                if item["token_contract"] == base_token and item["from"] == pool_address
            ]
            if quote_candidates:
                return max(quote_candidates, key=lambda item: float(item["value"] or 0.0))["from"]
            if token_candidates:
                return max(token_candidates, key=lambda item: float(item["value"] or 0.0))["to"]
        elif direction == "sell":
            token_candidates = [
                item for item in transfers
                if item["token_contract"] == base_token and item["to"] == pool_address
            ]
            quote_candidates = [
                item for item in transfers
                if item["token_contract"] == quote_token and item["from"] == pool_address
            ]
            if token_candidates:
                return max(token_candidates, key=lambda item: float(item["value"] or 0.0))["from"]
            if quote_candidates:
                return max(quote_candidates, key=lambda item: float(item["value"] or 0.0))["to"]
        elif direction == "liquidity_add":
            candidates = [
                item for item in transfers
                if item["to"] == pool_address and item["token_contract"] in {base_token, quote_token}
            ]
            if candidates:
                return max(candidates, key=lambda item: float(item["value"] or 0.0))["from"]
        elif direction == "liquidity_remove":
            candidates = [
                item for item in transfers
                if item["from"] == pool_address and item["token_contract"] in {base_token, quote_token}
            ]
            if candidates:
                return max(candidates, key=lambda item: float(item["value"] or 0.0))["to"]
        return ""

    def _primary_path(self, pool_address: str, counterparty: str, direction: str) -> tuple[str, str]:
        if direction in {"buy", "sell", "liquidity_add"}:
            return counterparty, pool_address
        if direction == "liquidity_remove":
            return pool_address, counterparty
        return counterparty or pool_address, pool_address if counterparty else pool_address

    def _leg_paths(self, pool_address: str, counterparty: str, direction: str) -> tuple[str, str, str, str]:
        if direction == "buy":
            return counterparty, pool_address, pool_address, counterparty
        if direction == "sell":
            return pool_address, counterparty, counterparty, pool_address
        if direction == "liquidity_add":
            return counterparty, pool_address, counterparty, pool_address
        if direction == "liquidity_remove":
            return pool_address, counterparty, pool_address, counterparty
        return counterparty, pool_address, counterparty, pool_address

    def _prior_events(self, address_snapshot: dict, event: Event) -> list[Event]:
        recent = address_snapshot.get("windows", {}).get("15m", {}).get("recent") or address_snapshot.get("recent") or []
        return [item for item in recent if item.tx_hash != event.tx_hash]

    def _multi_pool_resonance(self, token_snapshot: dict, side: str | None, event: Event) -> int:
        return self._raw_multi_pool_resonance(token_snapshot, side, event, eligible_only=True)

    def _raw_multi_pool_resonance(
        self,
        token_snapshot: dict,
        side: str | None,
        event: Event,
        *,
        eligible_only: bool = False,
    ) -> int:
        recent = token_snapshot.get("windows", {}).get("5m", {}).get("recent") or token_snapshot.get("recent") or []
        pool_addresses = set()
        current_pool = str(event.address or "").lower()
        direction = str(side or "")
        for item in recent:
            if str(getattr(item, "strategy_role", "") or "") != "lp_pool":
                continue
            if str(getattr(item, "side", "") or "") != direction:
                continue
            if str(getattr(item, "intent_type", "") or "") not in {"pool_buy_pressure", "pool_sell_pressure"}:
                continue
            if eligible_only and not self._is_structure_eligible_event(item):
                continue
            pool_address = str(getattr(item, "address", "") or "").lower()
            if pool_address:
                pool_addresses.add(pool_address)
        if current_pool and (not eligible_only or self._is_structure_eligible_event(event)):
            pool_addresses.add(current_pool)
        return len(pool_addresses)

    def _eligible_pool_same_direction_streak(self, events: list[Event]) -> int:
        streak = 0
        target_bucket = None
        for item in reversed(events):
            intent_type = str(item.intent_type or "")
            if intent_type not in {"pool_buy_pressure", "pool_sell_pressure", "liquidity_addition", "liquidity_removal"}:
                continue
            bucket = str(item.side or "")
            if intent_type == "liquidity_addition":
                bucket = "liquidity_add"
            elif intent_type == "liquidity_removal":
                bucket = "liquidity_remove"
            if target_bucket is None:
                target_bucket = bucket
            if bucket != target_bucket:
                break
            streak += 1
        return streak

    def _is_structure_eligible_event(self, event: Event) -> bool:
        return abs(float(getattr(event, "usd_value", 0.0) or 0.0)) >= float(self.structure_min_usd_per_event)

    def _pool_resonance_score(self, token_snapshot: dict, side: str | None) -> float:
        resonance = token_snapshot.get("resonance_5m") or {}
        if side == "买入" and int(resonance.get("buy_unique_addresses") or 0) >= 2:
            return float(token_snapshot.get("resonance_score_5m") or 0.0)
        if side == "卖出" and int(resonance.get("sell_unique_addresses") or 0) >= 2:
            return float(token_snapshot.get("resonance_score_5m") or 0.0)
        return 0.0

    def _abnormal_ratio(self, address_snapshot: dict, event: Event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 0.0
        return float(event.usd_value or 0.0) / avg_usd

    def _action_intensity(
        self,
        event: Event,
        abnormal_ratio: float,
        same_pool_continuity: int,
        multi_pool_resonance: int,
        pool_volume_surge_ratio: float,
    ) -> float:
        usd_value = float(event.usd_value or 0.0)
        base = min(0.55, usd_value / 300_000.0)
        base += min(0.20, max(abnormal_ratio - 1.0, 0.0) * 0.10)
        base += min(0.15, same_pool_continuity * 0.06)
        base += min(0.15, max(multi_pool_resonance - 1, 0) * 0.05)
        base += min(0.18, max(pool_volume_surge_ratio - 1.0, 0.0) * 0.10)
        return self._clamp(base, 0.0, 1.0)

    def _pool_volume_surge_ratio(self, pool_snapshot: dict, event: Event) -> float:
        recent_count = int(pool_snapshot.get("recent_count") or 0)
        volume_usd = float(pool_snapshot.get("volume_usd") or 0.0)
        current_usd = float(event.usd_value or 0.0)
        prior_count = max(recent_count - 1, 0)
        prior_volume_usd = max(volume_usd - current_usd, 0.0)
        if prior_count <= 0 or prior_volume_usd <= 0:
            return 1.0
        baseline_avg = prior_volume_usd / prior_count
        if baseline_avg <= 0:
            return 1.0
        return self._clamp(current_usd / baseline_avg, 0.0, 12.0)

    def _reserve_skew(self, lp_context: dict, event: Event, abnormal_ratio: float) -> float:
        base_amount = abs(float(lp_context.get("base_amount") or 0.0))
        quote_amount = abs(float(lp_context.get("quote_amount") or 0.0))
        total_amount = base_amount + quote_amount
        if total_amount <= 0:
            return 0.0

        if _is_stable(lp_context.get("quote_token_contract"), lp_context.get("quote_token_symbol")):
            size_proxy = min(1.0, float(event.usd_value or 0.0) / max(100_000.0, float(event.usd_value or 0.0)))
            quantity_gap = abs(base_amount - quote_amount) / total_amount
            return self._clamp(max(quantity_gap, size_proxy * 0.35, min(abnormal_ratio / 4.0, 0.35)), 0.0, 1.0)

        if _is_eth_like(lp_context.get("base_token_contract"), lp_context.get("base_token_symbol")):
            return self._clamp(min(abnormal_ratio / 3.0, 0.4), 0.0, 1.0)

        return self._clamp(abs(base_amount - quote_amount) / total_amount, 0.0, 1.0)

    def _market_impact_hint(self, action_intensity: float, reserve_skew: float, multi_pool_resonance: int) -> str:
        if action_intensity >= 0.72 or reserve_skew >= 0.28 or multi_pool_resonance >= 3:
            return "高冲击"
        if action_intensity >= 0.48 or reserve_skew >= 0.14 or multi_pool_resonance >= 2:
            return "中冲击"
        return "低冲击"

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))

    def _set_parse_debug(
        self,
        item: dict,
        pool_meta: dict,
        status: str,
        reason: str,
        missing_legs: list[str] | None = None,
    ) -> None:
        item["lp_parse_debug"] = {
            "status": status,
            "reason": reason,
            "watch_address": str(pool_meta.get("pool_address") or "").lower(),
            "pair_label": str(pool_meta.get("pair_label") or ""),
            "missing_legs": list(missing_legs or []),
        }
