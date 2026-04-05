from __future__ import annotations

from config import (
    LIQUIDATION_CONTINUITY_BONUS,
    LIQUIDATION_EXECUTION_MIN_SCORE,
    LIQUIDATION_MIN_USD,
    LIQUIDATION_PRIMARY_MIN_USD,
    LIQUIDATION_PROTOCOL_HIT_BONUS,
    LIQUIDATION_RESONANCE_BONUS,
    LIQUIDATION_RISK_MIN_SCORE,
    LIQUIDATION_SURGE_BONUS,
)
from constants import (
    ETH_EQUIVALENT_CONTRACTS,
    ETH_EQUIVALENT_SYMBOLS,
    LIQUIDATION_FOCUS_BASE_CONTRACTS,
    LIQUIDATION_FOCUS_QUOTE_CONTRACTS,
    LIQUIDATION_FOCUS_QUOTE_SYMBOLS,
)


class LiquidationDetector:
    """LP 主流池 liquidation overlay 检测。"""

    DIRECTIONAL_LP_INTENTS = {"pool_buy_pressure", "pool_sell_pressure"}

    def __init__(self) -> None:
        self.risk_min_score = float(LIQUIDATION_RISK_MIN_SCORE)
        self.execution_min_score = float(LIQUIDATION_EXECUTION_MIN_SCORE)
        self.protocol_hit_bonus = float(LIQUIDATION_PROTOCOL_HIT_BONUS)
        self.continuity_bonus = float(LIQUIDATION_CONTINUITY_BONUS)
        self.resonance_bonus = float(LIQUIDATION_RESONANCE_BONUS)
        self.surge_bonus = float(LIQUIDATION_SURGE_BONUS)
        self.min_usd = float(LIQUIDATION_MIN_USD)
        self.primary_min_usd = float(LIQUIDATION_PRIMARY_MIN_USD)

    def detect(
        self,
        event,
        parsed: dict,
        watch_meta: dict | None = None,
        address_snapshot: dict | None = None,
        pool_snapshot: dict | None = None,
        token_snapshot: dict | None = None,
        gate_metrics: dict | None = None,
    ) -> dict:
        watch_meta = watch_meta or {}
        address_snapshot = address_snapshot or {}
        pool_snapshot = pool_snapshot or {}
        token_snapshot = token_snapshot or {}
        gate_metrics = gate_metrics or {}
        raw = event.metadata.get("raw") or parsed or {}
        inferred = raw.get("inferred_context") or {}
        lp_context = raw.get("lp_context") or {}
        lp_analysis = event.metadata.get("lp_analysis") or {}

        result = self._base_result()
        result["is_focus_pool"] = self._is_focus_eth_pool(lp_context, raw)
        result["is_directional_lp"] = str(event.intent_type or "") in self.DIRECTIONAL_LP_INTENTS
        result["liquidation_side"] = self._liquidation_side(event.intent_type)
        result["liquidation_protocols"] = list(inferred.get("liquidation_protocols_touched") or [])
        result["liquidation_roles"] = list(inferred.get("liquidation_roles_touched") or [])
        result["liquidation_related_addresses"] = list(inferred.get("liquidation_related_addresses") or [])

        if not result["is_focus_pool"] or not result["is_directional_lp"]:
            result["liquidation_reason"] = "非主流 ETH 稳定币方向池，保持普通池子压力解释"
            return result

        usd_value = float(event.usd_value or 0.0)
        same_pool_continuity = int(lp_analysis.get("same_pool_continuity") or 0)
        multi_pool_resonance = int(lp_analysis.get("multi_pool_resonance") or 0)
        pool_volume_surge_ratio = float(lp_analysis.get("pool_volume_surge_ratio") or 0.0)
        abnormal_ratio = float(lp_analysis.get("abnormal_ratio") or self._abnormal_ratio(address_snapshot, event) or 0.0)
        action_intensity = float(lp_analysis.get("action_intensity") or 0.0)
        reserve_skew = float(lp_analysis.get("reserve_skew") or 0.0)
        pool_window_trade_count = int(lp_analysis.get("pool_window_trade_count") or pool_snapshot.get("recent_count") or 0)
        pool_window_usd_total = float(lp_analysis.get("pool_window_usd_total") or pool_snapshot.get("volume_usd") or 0.0)
        protocol_hit = bool(inferred.get("is_liquidation_protocol_related"))
        possible_keeper_executor = bool(inferred.get("possible_keeper_executor"))
        possible_vault_or_auction = bool(inferred.get("possible_vault_or_auction"))
        possible_lending_protocol = bool(inferred.get("possible_lending_protocol"))
        max_relevance = float(inferred.get("liquidation_relevance_score_max") or 0.0)
        is_router_related = bool(raw.get("is_router_related"))
        stable_tokens_touched = list(inferred.get("stable_tokens_touched") or [])
        volatile_tokens_touched = list(inferred.get("volatile_tokens_touched") or [])
        same_tx_collateral_and_stable = bool(stable_tokens_touched) and self._has_eth_like(volatile_tokens_touched, lp_context, raw)
        protocol_router_pool_path = protocol_hit and is_router_related and same_tx_collateral_and_stable
        repeated_protocol_disposals = self._repeated_protocol_disposals(
            event=event,
            pool_snapshot=pool_snapshot,
            protocols=result["liquidation_protocols"],
        )

        score = 0.12
        evidence = []

        if same_pool_continuity >= 2:
            score += self.continuity_bonus
            evidence.append(f"同池连续 {same_pool_continuity + 1} 笔")
        if same_pool_continuity >= 4:
            score += 0.04
        if multi_pool_resonance >= 2:
            score += self.resonance_bonus
            evidence.append(f"跨池同向 {multi_pool_resonance} 池")
        if pool_volume_surge_ratio >= 1.6:
            score += min(self.surge_bonus + max(pool_volume_surge_ratio - 1.6, 0.0) * 0.035, 0.16)
            evidence.append(f"池子放量 {pool_volume_surge_ratio:.2f}x")
        if abnormal_ratio >= 1.8:
            score += 0.07
            evidence.append(f"相对基线放大 {abnormal_ratio:.2f}x")
        if pool_window_trade_count >= 4:
            score += 0.04
        if pool_window_usd_total >= max(usd_value * 2.8, 180_000.0):
            score += 0.04
        if reserve_skew >= 0.22:
            score += 0.05
        if action_intensity >= 0.60:
            score += 0.05
        if is_router_related:
            score += 0.03
            evidence.append("路径含 router / aggregator")

        if protocol_hit:
            score += self.protocol_hit_bonus + min(max_relevance * 0.18, 0.16)
            evidence.append(f"命中协议地址：{'/'.join(result['liquidation_protocols'][:3])}")
        if possible_lending_protocol:
            score += 0.07
        if possible_keeper_executor:
            score += 0.08
            evidence.append("出现 keeper / executor 角色")
        if possible_vault_or_auction:
            score += 0.12
            evidence.append("出现 vault / auction 角色")
        if same_tx_collateral_and_stable and protocol_hit:
            score += 0.10
            evidence.append("同 tx 同时出现抵押资产与稳定币腿")
        if protocol_router_pool_path:
            score += 0.12
            evidence.append("路径更像 protocol -> executor/router -> pool")
        if repeated_protocol_disposals >= 2:
            score += 0.12
            evidence.append(f"同协议短窗连续处置 {repeated_protocol_disposals + 1} 次")

        score = self._clamp(score, 0.0, 0.99)
        strong_execution_evidence = protocol_hit and (
            possible_vault_or_auction
            or (possible_keeper_executor and protocol_router_pool_path)
            or repeated_protocol_disposals >= 2
        )

        if strong_execution_evidence and usd_value >= self.min_usd and score >= self.execution_min_score:
            stage = "execution"
        elif usd_value >= self.min_usd and score >= self.risk_min_score:
            stage = "risk"
        else:
            stage = "none"

        if stage == "none":
            result["liquidation_reason"] = "当前更像普通池子压力，协议/连续性证据仍不足"
        elif stage == "risk":
            result["liquidation_reason"] = self._risk_reason(
                event.intent_type,
                protocols=result["liquidation_protocols"],
                possible_vault_or_auction=possible_vault_or_auction,
                protocol_hit=protocol_hit,
            )
        else:
            result["liquidation_reason"] = self._execution_reason(
                event.intent_type,
                protocols=result["liquidation_protocols"],
                possible_vault_or_auction=possible_vault_or_auction,
                repeated_protocol_disposals=repeated_protocol_disposals,
            )

        result.update({
            "liquidation_score": round(score, 3),
            "liquidation_stage": stage,
            "liquidation_confidence": round(self._confidence(score, stage), 3),
            "liquidation_evidence": self._unique(evidence)[:6],
            "liquidation_evidence_count": len(self._unique(evidence)),
            "possible_keeper_executor": possible_keeper_executor,
            "possible_vault_or_auction": possible_vault_or_auction,
            "possible_lending_protocol": possible_lending_protocol,
            "protocol_router_pool_path": protocol_router_pool_path,
            "same_tx_collateral_and_stable": same_tx_collateral_and_stable,
            "repeated_protocol_disposals": repeated_protocol_disposals,
            "strong_execution_evidence": strong_execution_evidence,
            "semantic_overlay": self._semantic_overlay(stage, event.intent_type),
            "primary_candidate": bool(
                stage == "execution"
                and usd_value >= self.primary_min_usd
                and strong_execution_evidence
            ),
        })
        return result

    def _base_result(self) -> dict:
        return {
            "liquidation_score": 0.0,
            "liquidation_stage": "none",
            "liquidation_side": "unknown",
            "liquidation_reason": "",
            "liquidation_evidence": [],
            "liquidation_evidence_count": 0,
            "liquidation_protocols": [],
            "liquidation_roles": [],
            "liquidation_related_addresses": [],
            "liquidation_confidence": 0.0,
            "semantic_overlay": "",
            "is_focus_pool": False,
            "is_directional_lp": False,
            "possible_keeper_executor": False,
            "possible_vault_or_auction": False,
            "possible_lending_protocol": False,
            "protocol_router_pool_path": False,
            "same_tx_collateral_and_stable": False,
            "repeated_protocol_disposals": 0,
            "strong_execution_evidence": False,
            "primary_candidate": False,
        }

    def _is_focus_eth_pool(self, lp_context: dict, raw: dict) -> bool:
        base_contract = str(lp_context.get("base_token_contract") or raw.get("token_contract") or "").lower()
        quote_contract = str(lp_context.get("quote_token_contract") or raw.get("quote_token_contract") or "").lower()
        base_symbol = str(lp_context.get("base_token_symbol") or raw.get("token_symbol") or "").upper()
        quote_symbol = str(lp_context.get("quote_token_symbol") or raw.get("quote_symbol") or "").upper()
        return (
            (base_contract in LIQUIDATION_FOCUS_BASE_CONTRACTS or base_contract in ETH_EQUIVALENT_CONTRACTS or base_symbol in ETH_EQUIVALENT_SYMBOLS)
            and (quote_contract in LIQUIDATION_FOCUS_QUOTE_CONTRACTS or quote_symbol in LIQUIDATION_FOCUS_QUOTE_SYMBOLS)
        )

    def _has_eth_like(self, volatile_tokens_touched: list[str], lp_context: dict, raw: dict) -> bool:
        candidates = {
            str(token).lower()
            for token in volatile_tokens_touched
            if token
        }
        for token in [
            lp_context.get("base_token_contract"),
            raw.get("token_contract"),
        ]:
            normalized = str(token or "").lower()
            if normalized:
                candidates.add(normalized)
        return any(token in ETH_EQUIVALENT_CONTRACTS or token in LIQUIDATION_FOCUS_BASE_CONTRACTS for token in candidates)

    def _liquidation_side(self, intent_type: str | None) -> str:
        if str(intent_type or "") == "pool_sell_pressure":
            return "long_flush"
        if str(intent_type or "") == "pool_buy_pressure":
            return "short_squeeze"
        return "unknown"

    def _repeated_protocol_disposals(self, event, pool_snapshot: dict, protocols: list[str]) -> int:
        if not protocols:
            return 0
        target = set(protocols)
        count = 0
        for prior_event in reversed(list(pool_snapshot.get("recent") or [])):
            if getattr(prior_event, "tx_hash", "") == event.tx_hash:
                continue
            prior_protocols = set(prior_event.metadata.get("liquidation_protocols") or [])
            if not prior_protocols:
                continue
            if prior_event.intent_type != event.intent_type:
                continue
            if target & prior_protocols:
                count += 1
        return count

    def _risk_reason(
        self,
        intent_type: str,
        protocols: list[str],
        possible_vault_or_auction: bool,
        protocol_hit: bool,
    ) -> str:
        if str(intent_type or "") == "pool_sell_pressure":
            base = "疑似多头被动出清风险卖压"
        else:
            base = "疑似空头回补/清算型买压"
        if possible_vault_or_auction and protocols:
            return f"{base}，且命中 {'/'.join(protocols[:2])} auction/vault 相关地址"
        if protocol_hit and protocols:
            return f"{base}，路径已触及 {'/'.join(protocols[:2])} 协议地址"
        return f"{base}，但当前仍停留在清算环境风险层面"

    def _execution_reason(
        self,
        intent_type: str,
        protocols: list[str],
        possible_vault_or_auction: bool,
        repeated_protocol_disposals: int,
    ) -> str:
        if str(intent_type or "") == "pool_sell_pressure":
            base = "疑似协议清算执行卖出"
        else:
            base = "疑似清算反向回补执行买入"
        if possible_vault_or_auction and protocols:
            return f"{base}，已看到 {'/'.join(protocols[:2])} auction/vault 处置证据"
        if repeated_protocol_disposals >= 2 and protocols:
            return f"{base}，同协议短窗连续处置结构已较清晰"
        if protocols:
            return f"{base}，协议地址与池子方向执行已形成更强组合证据"
        return f"{base}，但仍建议按疑似执行处理"

    def _semantic_overlay(self, stage: str, intent_type: str | None) -> str:
        if stage == "none":
            return ""
        suffix = "sell" if str(intent_type or "") == "pool_sell_pressure" else "buy"
        return f"liquidation_{stage}_{suffix}"

    def _confidence(self, score: float, stage: str) -> float:
        if stage == "execution":
            return self._clamp(score + 0.06, 0.0, 0.99)
        if stage == "risk":
            return self._clamp(score - 0.02, 0.0, 0.95)
        return self._clamp(score * 0.7, 0.0, 0.8)

    def _abnormal_ratio(self, address_snapshot: dict, event) -> float:
        avg_usd = float(
            address_snapshot.get("windows", {}).get("24h", {}).get("avg_usd")
            or address_snapshot.get("avg_usd")
            or 0.0
        )
        if avg_usd <= 0:
            return 0.0
        return float(event.usd_value or 0.0) / avg_usd

    def _unique(self, values: list[str]) -> list[str]:
        unique = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in unique:
                unique.append(text)
        return unique

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))
