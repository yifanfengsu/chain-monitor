from __future__ import annotations

from config import (
    LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE,
    LP_QUALITY_HISTORY_LIMIT,
    LP_QUALITY_MIN_FASTLANE_ROI_SCORE,
)
from lp_product_helpers import aligned_move, asset_symbol_from_event, lp_stage_rank, pair_label_from_event


class QualityManager:
    def __init__(
        self,
        state_manager,
        *,
        history_limit: int = LP_QUALITY_HISTORY_LIMIT,
        exhaustion_bias_reversal_score: float = LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE,
        min_fastlane_roi_score: float = LP_QUALITY_MIN_FASTLANE_ROI_SCORE,
    ) -> None:
        self.state_manager = state_manager
        self.history_limit = max(int(history_limit), 50)
        self.exhaustion_bias_reversal_score = float(exhaustion_bias_reversal_score)
        self.min_fastlane_roi_score = float(min_fastlane_roi_score)

    def annotate_lp_signal(self, event, signal, gate_metrics: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if str(getattr(event, "strategy_role", "") or "") != "lp_pool":
            return {}

        records = list(self.state_manager.get_recent_lp_outcome_records(limit=self.history_limit))
        pool_address = str(getattr(event, "address", "") or "").lower()
        pair_label = pair_label_from_event(event)
        asset_symbol = str(
            getattr(signal, "context", {}).get("asset_case_label")
            or getattr(event, "metadata", {}).get("asset_case_label")
            or asset_symbol_from_event(event)
            or ""
        )

        pool_scores = self._scope_scores(records, lambda record: str(record.get("pool_address") or "").lower() == pool_address)
        pair_scores = self._scope_scores(records, lambda record: str(record.get("pair_label") or "") == pair_label)
        asset_scores = self._scope_scores(records, lambda record: self._record_asset_symbol(record) == asset_symbol)

        payload = {
            "pool_quality_score": round(float(pool_scores["quality_score"]), 3),
            "pair_quality_score": round(float(pair_scores["quality_score"]), 3),
            "asset_case_quality_score": round(float(asset_scores["quality_score"]), 3),
            "prealert_precision_score": round(float(max(asset_scores["prealert_precision_score"], pair_scores["prealert_precision_score"], pool_scores["prealert_precision_score"])), 3),
            "confirm_conversion_score": round(float(max(asset_scores["confirm_conversion_score"], pair_scores["confirm_conversion_score"], pool_scores["confirm_conversion_score"])), 3),
            "climax_reversal_score": round(float(max(asset_scores["climax_reversal_score"], pair_scores["climax_reversal_score"], pool_scores["climax_reversal_score"])), 3),
            "market_context_alignment_score": round(float(max(asset_scores["market_context_alignment_score"], pair_scores["market_context_alignment_score"], pool_scores["market_context_alignment_score"])), 3),
            "fastlane_roi_score": round(float(max(asset_scores["fastlane_roi_score"], pair_scores["fastlane_roi_score"], pool_scores["fastlane_roi_score"])), 3),
            "quality_sample_size": int(asset_scores["sample_size"]),
            "quality_scope_pool_size": int(pool_scores["sample_size"]),
            "quality_scope_pair_size": int(pair_scores["sample_size"]),
            "quality_scope_asset_size": int(asset_scores["sample_size"]),
            "quality_score_brief": self._quality_brief(asset_scores, pair_scores, pool_scores),
            "prealert_delivery_penalized": bool(
                max(asset_scores["prealert_precision_score"], pair_scores["prealert_precision_score"], pool_scores["prealert_precision_score"]) < 0.50
            ),
            "fastlane_delivery_penalized": bool(
                max(asset_scores["fastlane_roi_score"], pair_scores["fastlane_roi_score"], pool_scores["fastlane_roi_score"]) < self.min_fastlane_roi_score
            ),
            "exhaustion_bias_preferred": bool(
                max(asset_scores["climax_reversal_score"], pair_scores["climax_reversal_score"], pool_scores["climax_reversal_score"])
                >= self.exhaustion_bias_reversal_score
            ),
            "quality_rationale": {
                "pool": pool_scores,
                "pair": pair_scores,
                "asset": asset_scores,
            },
        }
        self._apply_exhaustion_bias(event=event, signal=signal, payload=payload)
        getattr(event, "metadata", {}).update(payload)
        getattr(signal, "metadata", {}).update(payload)
        getattr(signal, "context", {}).update(payload)
        return payload

    def _apply_exhaustion_bias(self, *, event, signal, payload: dict) -> None:
        context = getattr(signal, "context", {})
        event_metadata = getattr(event, "metadata", {})
        stage = str(context.get("lp_alert_stage") or event_metadata.get("lp_alert_stage") or "")
        if stage != "climax":
            return
        if not bool(payload.get("exhaustion_bias_preferred")):
            return
        exhaustion_confidence = float(context.get("lp_exhaustion_confidence") or event_metadata.get("lp_exhaustion_confidence") or 0.0)
        if exhaustion_confidence < 0.42:
            return
        update_payload = {
            "lp_alert_stage": "exhaustion_risk",
            "lp_stage_badge": "风险",
            "lp_state_label": str(context.get("lp_state_label") or "冲击后回吐风险高"),
            "lp_market_read": "回吐风险抬升｜不宜直接追击",
            "lp_alert_stage_reason": "quality_reversal_bias",
            "quality_stage_override": "quality_reversal_bias",
        }
        event_metadata.update(update_payload)
        getattr(signal, "metadata", {}).update(update_payload)
        context.update(update_payload)
        asset_case_stage_rank = lp_stage_rank(context.get("asset_case_stage") or event_metadata.get("asset_case_stage") or "")
        if asset_case_stage_rank and asset_case_stage_rank < lp_stage_rank("exhaustion_risk"):
            context["asset_case_stage"] = "exhaustion_risk"
            getattr(signal, "metadata", {})["asset_case_stage"] = "exhaustion_risk"
            event_metadata["asset_case_stage"] = "exhaustion_risk"

    def _scope_scores(self, records: list[dict], predicate) -> dict:
        scoped = [record for record in records if predicate(record)]
        sample_size = len(scoped)
        prealert_records = [record for record in scoped if str(record.get("lp_alert_stage") or "") == "prealert"]
        confirm_records = [record for record in scoped if str(record.get("lp_alert_stage") or "") == "confirm"]
        climax_records = [record for record in scoped if str(record.get("lp_alert_stage") or "") == "climax"]
        market_records = [record for record in scoped if str(record.get("market_context_source") or "") != "unavailable"]
        fastlane_records = [record for record in scoped if bool(record.get("lp_promoted_fastlane"))]

        prealert_resolved = [record for record in prealert_records if self._prealert_resolved(record)]
        confirm_resolved = [record for record in confirm_records if self._confirm_resolved(record)]
        climax_resolved = [record for record in climax_records if self._climax_resolved(record)]
        fastlane_resolved = [record for record in fastlane_records if record.get("move_after_alert_60s") is not None]

        prealert_precision_score = self._bayesian_rate(
            successes=sum(1 for record in prealert_resolved if self._prealert_success(record)),
            total=len(prealert_resolved),
            prior=0.58,
        )
        confirm_conversion_score = self._bayesian_rate(
            successes=sum(1 for record in confirm_resolved if self._confirm_success(record)),
            total=len(confirm_resolved),
            prior=0.56,
        )
        climax_reversal_score = self._bayesian_rate(
            successes=sum(1 for record in climax_resolved if self._climax_reversal(record)),
            total=len(climax_resolved),
            prior=0.45,
        )
        market_context_alignment_score = self._market_alignment_score(market_records)
        fastlane_roi_score = self._bayesian_rate(
            successes=sum(1 for record in fastlane_resolved if self._fastlane_success(record)),
            total=len(fastlane_resolved),
            prior=0.55,
        )
        quality_score = self._clamp(
            prealert_precision_score * 0.28
            + confirm_conversion_score * 0.26
            + (1.0 - climax_reversal_score * 0.60) * 0.18
            + market_context_alignment_score * 0.14
            + fastlane_roi_score * 0.14
        )
        return {
            "sample_size": sample_size,
            "prealert_precision_score": round(prealert_precision_score, 4),
            "confirm_conversion_score": round(confirm_conversion_score, 4),
            "climax_reversal_score": round(climax_reversal_score, 4),
            "market_context_alignment_score": round(market_context_alignment_score, 4),
            "fastlane_roi_score": round(fastlane_roi_score, 4),
            "quality_score": round(quality_score, 4),
            "resolved_prealerts": len(prealert_resolved),
            "resolved_confirms": len(confirm_resolved),
            "resolved_climaxes": len(climax_resolved),
            "resolved_fastlanes": len(fastlane_resolved),
        }

    def _prealert_resolved(self, record: dict) -> bool:
        return (
            record.get("confirm_after_prealert") is True
            or record.get("false_prealert") is True
            or record.get("move_after_alert_300s") is not None
        )

    def _prealert_success(self, record: dict) -> bool:
        if record.get("confirm_after_prealert") is True:
            return True
        if record.get("false_prealert") is True:
            return False
        return aligned_move(record.get("move_after_alert_300s"), record.get("direction_bucket")) > 0.002

    def _confirm_resolved(self, record: dict) -> bool:
        return (
            record.get("followthrough_positive") is True
            or record.get("followthrough_negative") is True
            or record.get("move_after_alert_60s") is not None
        )

    def _confirm_success(self, record: dict) -> bool:
        if record.get("followthrough_positive") is True:
            return True
        if record.get("followthrough_negative") is True:
            return False
        return aligned_move(record.get("move_after_alert_60s"), record.get("direction_bucket")) > 0.002

    def _climax_resolved(self, record: dict) -> bool:
        return (
            record.get("reversal_after_climax") is True
            or record.get("move_after_alert_60s") is not None
            or record.get("move_after_alert_300s") is not None
        )

    def _climax_reversal(self, record: dict) -> bool:
        if record.get("reversal_after_climax") is True:
            return True
        move = record.get("move_after_alert_60s")
        if move is None:
            move = record.get("move_after_alert_300s")
        return aligned_move(move, record.get("direction_bucket")) < -0.002

    def _market_alignment_score(self, records: list[dict]) -> float:
        if not records:
            return 0.50
        weights = {"leading": 1.0, "confirming": 0.72, "late": 0.28}
        total = 0.0
        counted = 0
        for record in records:
            timing = str(record.get("alert_relative_timing") or "").strip()
            if timing not in weights:
                continue
            total += weights[timing]
            counted += 1
        if counted <= 0:
            return 0.50
        return self._bayesian_rate(successes=total, total=counted, prior=0.50)

    def _fastlane_success(self, record: dict) -> bool:
        return aligned_move(record.get("move_after_alert_60s"), record.get("direction_bucket")) > 0.003

    def _record_asset_symbol(self, record: dict) -> str:
        return str(record.get("asset_symbol") or record.get("asset_case_label") or "")

    def _quality_brief(self, asset_scores: dict, pair_scores: dict, pool_scores: dict) -> str:
        asset_quality = float(asset_scores.get("quality_score") or 0.0)
        prealert_precision = float(asset_scores.get("prealert_precision_score") or 0.0)
        market_alignment = float(asset_scores.get("market_context_alignment_score") or 0.0)
        if int(asset_scores.get("sample_size") or 0) < 3:
            return "历史样本有限"
        if prealert_precision < 0.48 or asset_quality < 0.52:
            return "历史命中一般"
        if market_alignment >= 0.62 and asset_quality >= 0.70:
            return "历史传导较强"
        if float(pair_scores.get("quality_score") or 0.0) >= 0.72 or float(pool_scores.get("quality_score") or 0.0) >= 0.72:
            return "历史结构较稳"
        return "历史一般"

    def _bayesian_rate(self, *, successes: float, total: int, prior: float, weight: float = 3.0) -> float:
        if total <= 0:
            return float(prior)
        return self._clamp((float(successes) + prior * weight) / (float(total) + weight))

    def _clamp(self, value: float) -> float:
        return max(0.0, min(1.0, float(value)))
