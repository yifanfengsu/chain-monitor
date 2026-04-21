from __future__ import annotations

import atexit
from collections import defaultdict
import time

from config import (
    LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE,
    LP_QUALITY_HISTORY_LIMIT,
    LP_QUALITY_MIN_FASTLANE_ROI_SCORE,
    LP_QUALITY_MIN_PREALERT_PRECISION_FOR_RETAIL,
    LP_QUALITY_MIN_PREALERT_PRECISION_FOR_TRADER,
    LP_QUALITY_REPORT_MIN_ACTIONABLE_SAMPLES,
    LP_QUALITY_REPORT_TOP_N,
    LP_QUALITY_STATS_ENABLE,
    LP_QUALITY_STATS_FLUSH_INTERVAL_SEC,
    LP_QUALITY_STATS_PATH,
    LP_QUALITY_STATS_SCHEMA_VERSION,
)
from lp_product_helpers import aligned_move, asset_symbol_from_event, lp_stage_rank, pair_label_from_event
from persistence_utils import read_json_file, resolve_persistence_path, write_json_file


class QualityManager:
    def __init__(
        self,
        state_manager,
        *,
        history_limit: int = LP_QUALITY_HISTORY_LIMIT,
        exhaustion_bias_reversal_score: float = LP_QUALITY_EXHAUSTION_BIAS_REVERSAL_SCORE,
        min_fastlane_roi_score: float = LP_QUALITY_MIN_FASTLANE_ROI_SCORE,
        persistence_enabled: bool = LP_QUALITY_STATS_ENABLE,
        stats_path: str | None = LP_QUALITY_STATS_PATH,
        stats_schema_version: str = LP_QUALITY_STATS_SCHEMA_VERSION,
        stats_flush_interval_sec: float = LP_QUALITY_STATS_FLUSH_INTERVAL_SEC,
        actionable_min_samples: int = LP_QUALITY_REPORT_MIN_ACTIONABLE_SAMPLES,
    ) -> None:
        self.state_manager = state_manager
        self.history_limit = max(int(history_limit), 50)
        self.exhaustion_bias_reversal_score = float(exhaustion_bias_reversal_score)
        self.min_fastlane_roi_score = float(min_fastlane_roi_score)
        self.persistence_enabled = bool(persistence_enabled)
        self.stats_schema_version = str(stats_schema_version or LP_QUALITY_STATS_SCHEMA_VERSION)
        self.stats_flush_interval_sec = max(float(stats_flush_interval_sec or 0.0), 0.2)
        self.actionable_min_samples = max(int(actionable_min_samples or 0), 1)
        self.stats_path = resolve_persistence_path(stats_path, namespace="quality-stats") if self.persistence_enabled else None
        self._persisted_records: dict[str, dict] = {}
        self._last_flush_monotonic = 0.0
        self._dirty = False
        self._last_load_status = "not_loaded"
        if self.persistence_enabled:
            self.load_from_disk()
            self._rehydrate_state_manager()
            atexit.register(self.flush, True)

    def annotate_lp_signal(self, event, signal, gate_metrics: dict | None = None) -> dict:
        gate_metrics = gate_metrics or {}
        if str(getattr(event, "strategy_role", "") or "") != "lp_pool":
            return {}

        self.sync_from_state_manager()
        records = self._combined_records(limit=self.history_limit)
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
            "quality_actionable_sample_size": int(self.actionable_min_samples),
        }
        self._apply_exhaustion_bias(event=event, signal=signal, payload=payload)
        getattr(event, "metadata", {}).update(payload)
        getattr(signal, "metadata", {}).update(payload)
        getattr(signal, "context", {}).update(payload)
        return payload

    def sync_from_state_manager(self, *, force: bool = False) -> int:
        if self.state_manager is None:
            return 0
        records = list(self.state_manager.get_recent_lp_outcome_records(limit=self.history_limit))
        updated = 0
        for record in records:
            record_id = self._record_id(record)
            if not record_id:
                continue
            current = self._persisted_records.get(record_id)
            normalized = dict(record)
            if current != normalized:
                self._persisted_records[record_id] = normalized
                updated += 1
        self._trim_persisted_records()
        if updated:
            self._dirty = True
            self.flush(force=force)
        return updated

    def flush(self, force: bool = False) -> None:
        if not self.persistence_enabled or self.stats_path is None or not self._dirty:
            return
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_flush_monotonic) < self.stats_flush_interval_sec:
            return
        records = self._combined_records(limit=self.history_limit)
        payload = {
            "schema_version": self.stats_schema_version,
            "generated_at": int(time.time()),
            "history_limit": int(self.history_limit),
            "records": records,
            "summary": self.build_report(records=records, top_n=min(LP_QUALITY_REPORT_TOP_N, 10)),
        }
        write_json_file(self.stats_path, payload)
        self._mirror_sqlite_quality(payload)
        self._dirty = False
        self._last_flush_monotonic = now_monotonic

    def load_from_disk(self) -> int:
        payload = read_json_file(self.stats_path)
        if not payload:
            self._last_load_status = "empty_or_missing"
            return 0
        if not isinstance(payload, dict):
            self._last_load_status = "malformed_payload"
            return 0
        if str(payload.get("schema_version") or "") != self.stats_schema_version:
            self._last_load_status = f"schema_mismatch:{payload.get('schema_version') or 'unknown'}"
            return 0
        records = payload.get("records") or []
        if not isinstance(records, list):
            self._last_load_status = "records_invalid"
            return 0
        self._persisted_records = {}
        for record in records:
            record_id = self._record_id(record)
            if not record_id:
                continue
            self._persisted_records[record_id] = dict(record)
        self._trim_persisted_records()
        self._last_load_status = "loaded"
        return len(self._persisted_records)

    def _rehydrate_state_manager(self) -> int:
        if self.state_manager is None or not hasattr(self.state_manager, "restore_lp_outcome_records"):
            return 0
        return int(self.state_manager.restore_lp_outcome_records(list(self._persisted_records.values())) or 0)

    def _mirror_sqlite_quality(self, payload: dict) -> None:
        try:
            import sqlite_store

            generated_at = int(payload.get("generated_at") or time.time())
            for record in payload.get("records") or []:
                if isinstance(record, dict):
                    sqlite_store.write_outcome(record)
                    sqlite_store.upsert_quality_stat(
                        {
                            **record,
                            "scope_type": "signal",
                            "scope_key": str(record.get("signal_id") or record.get("record_id") or ""),
                            "updated_at": generated_at,
                        }
                    )
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            for dimension, rows in (summary.get("dimensions") or {}).items():
                for row in rows or []:
                    if isinstance(row, dict):
                        sqlite_store.upsert_quality_stat(
                            {
                                **row,
                                "scope_type": str(dimension),
                                "scope_key": str(row.get("key") or ""),
                                "updated_at": generated_at,
                            }
                        )
        except Exception as exc:
            print(f"sqlite quality mirror failed: {exc}")

    def build_report(
        self,
        *,
        records: list[dict] | None = None,
        days: int | None = None,
        limit: int | None = None,
        top_n: int = LP_QUALITY_REPORT_TOP_N,
    ) -> dict:
        scoped_records = list(records or self._combined_records(limit=limit or self.history_limit))
        if days is not None and days > 0:
            cutoff = int(time.time()) - int(days) * 86400
            scoped_records = [record for record in scoped_records if int(record.get("created_at") or 0) >= cutoff]
        if limit is not None and limit > 0:
            scoped_records = scoped_records[-int(limit):]

        dimensions = {}
        for dimension, resolver in {
            "pool": lambda record: str(record.get("pool_address") or "").lower(),
            "pair": lambda record: str(record.get("pair_label") or ""),
            "asset": self._record_asset_symbol,
            "stage": lambda record: str(record.get("lp_alert_stage") or ""),
        }.items():
            grouped = defaultdict(list)
            for record in scoped_records:
                key = resolver(record)
                if key:
                    grouped[key].append(record)
            dimensions[dimension] = [
                self._dimension_report_row(dimension, key, group_records)
                for key, group_records in grouped.items()
            ]
            dimensions[dimension].sort(
                key=lambda item: (
                    bool(item.get("actionable")),
                    float(item.get("quality_score") or 0.0),
                    -int(item.get("sample_size") or 0),
                    str(item.get("key") or ""),
                ),
                reverse=True,
            )

        overall = self._dimension_report_row("overall", "all", scoped_records)
        actionable_assets = [item for item in dimensions.get("asset", []) if bool(item.get("actionable"))]
        actionable_pairs = [item for item in dimensions.get("pair", []) if bool(item.get("actionable"))]
        actionable_pools = [item for item in dimensions.get("pool", []) if bool(item.get("actionable"))]
        prealert_laggards = sorted(
            actionable_assets + actionable_pairs + actionable_pools,
            key=lambda item: (float(item.get("prealert_precision_score") or 0.0), float(item.get("quality_score") or 0.0), str(item.get("label") or "")),
        )[: max(int(top_n or 0), 0)]
        climax_reversal = sorted(
            actionable_assets + actionable_pairs + actionable_pools,
            key=lambda item: (-float(item.get("climax_reversal_score") or 0.0), -int(item.get("sample_size") or 0), str(item.get("label") or "")),
        )[: max(int(top_n or 0), 0)]
        fastlane_roi = sorted(
            actionable_assets + actionable_pairs + actionable_pools,
            key=lambda item: (float(item.get("fastlane_roi_score") or 0.0), float(item.get("quality_score") or 0.0), str(item.get("label") or "")),
        )[: max(int(top_n or 0), 0)]

        return {
            "schema_version": self.stats_schema_version,
            "generated_at": int(time.time()),
            "filters": {
                "days": int(days) if days is not None else None,
                "limit": int(limit) if limit is not None else None,
                "top_n": int(top_n or 0),
            },
            "overall": overall,
            "dimensions": dimensions,
            "top_bad_prealerts": prealert_laggards,
            "top_climax_reversal": climax_reversal,
            "fastlane_roi_laggards": fastlane_roi,
        }

    def export_rows(self, *, days: int | None = None, limit: int | None = None) -> list[dict]:
        report = self.build_report(days=days, limit=limit, top_n=LP_QUALITY_REPORT_TOP_N)
        rows = []
        for dimension, items in (report.get("dimensions") or {}).items():
            for item in items:
                rows.append(
                    {
                        "dimension": dimension,
                        "key": item.get("key"),
                        "label": item.get("label"),
                        "sample_size": item.get("sample_size"),
                        "quality_score": item.get("quality_score"),
                        "prealert_precision_score": item.get("prealert_precision_score"),
                        "confirm_conversion_score": item.get("confirm_conversion_score"),
                        "climax_reversal_score": item.get("climax_reversal_score"),
                        "market_context_alignment_score": item.get("market_context_alignment_score"),
                        "fastlane_roi_score": item.get("fastlane_roi_score"),
                        "quality_hint": item.get("quality_hint"),
                        "tuning_hint": item.get("tuning_hint"),
                        "actionable": item.get("actionable"),
                    }
                )
        return rows

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

    def _dimension_report_row(self, dimension: str, key: str, records: list[dict]) -> dict:
        scores = self._scope_scores(records, lambda _: True)
        prealert_count = sum(1 for record in records if str(record.get("lp_alert_stage") or "") == "prealert")
        confirm_count = sum(1 for record in records if str(record.get("lp_alert_stage") or "") == "confirm")
        climax_count = sum(1 for record in records if str(record.get("lp_alert_stage") or "") == "climax")
        counts = {
            "sample_size": int(scores["sample_size"]),
            "prealert_count": prealert_count,
            "prealert_confirmed_count": sum(1 for record in records if record.get("confirm_after_prealert") is True),
            "prealert_false_count": sum(1 for record in records if record.get("false_prealert") is True),
            "confirm_count": confirm_count,
            "climax_count": climax_count,
            "climax_reversal_count": sum(1 for record in records if record.get("reversal_after_climax") is True),
            "fastlane_promotions": sum(1 for record in records if bool(record.get("lp_promoted_fastlane"))),
            "fastlane_positive_followthrough": sum(1 for record in records if bool(record.get("lp_promoted_fastlane")) and self._fastlane_success(record)),
            "market_context_available_count": sum(1 for record in records if str(record.get("market_context_source") or "") != "unavailable"),
            "market_context_alignment_positive_count": sum(1 for record in records if str(record.get("alert_relative_timing") or "") in {"leading", "confirming"}),
            "last_updated_at": max([int(record.get("created_at") or 0) for record in records], default=0),
        }
        actionable = counts["sample_size"] >= self.actionable_min_samples
        hint = self._quality_brief(scores, scores, scores)
        return {
            "dimension": dimension,
            "key": key,
            "label": key,
            **counts,
            **scores,
            "quality_hint": hint,
            "actionable": actionable,
            "tuning_hint": self._tuning_hint(dimension, key, counts, scores, actionable=actionable),
        }

    def _tuning_hint(self, dimension: str, key: str, counts: dict, scores: dict, *, actionable: bool) -> str:
        del dimension, key
        if not actionable:
            return "样本不足，仅观察"
        prealert_precision = float(scores.get("prealert_precision_score") or 0.0)
        climax_reversal = float(scores.get("climax_reversal_score") or 0.0)
        fastlane_roi = float(scores.get("fastlane_roi_score") or 0.0)
        if prealert_precision < float(LP_QUALITY_MIN_PREALERT_PRECISION_FOR_RETAIL):
            if prealert_precision < float(LP_QUALITY_MIN_PREALERT_PRECISION_FOR_TRADER):
                return "retail/trader 均应下调 prealert 权重"
            return "retail 建议关闭该维度 prealert"
        if climax_reversal >= self.exhaustion_bias_reversal_score:
            return "高潮后回吐率高，建议提高 exhaustion 倾向"
        if counts.get("fastlane_promotions") and fastlane_roi < self.min_fastlane_roi_score:
            return "fastlane ROI 偏弱，建议降低 promotion 优先级"
        if float(scores.get("market_context_alignment_score") or 0.0) < 0.45:
            return "合约上下文对齐偏弱，retail 建议降权"
        return "保持当前阈值"

    def _combined_records(self, *, limit: int | None = None) -> list[dict]:
        merged = dict(self._persisted_records)
        if self.state_manager is not None:
            for record in self.state_manager.get_recent_lp_outcome_records(limit=self.history_limit):
                record_id = self._record_id(record)
                if record_id:
                    merged[record_id] = dict(record)
        ordered = sorted(
            merged.values(),
            key=lambda record: (
                int(record.get("created_at") or 0),
                str(record.get("record_id") or ""),
            ),
        )
        if limit is not None and limit > 0:
            return [dict(item) for item in ordered[-int(limit):]]
        return [dict(item) for item in ordered]

    def _trim_persisted_records(self) -> None:
        ordered = sorted(
            self._persisted_records.values(),
            key=lambda record: (
                int(record.get("created_at") or 0),
                str(record.get("record_id") or ""),
            ),
        )
        overflow = max(len(ordered) - self.history_limit, 0)
        for record in ordered[:overflow]:
            self._persisted_records.pop(self._record_id(record), None)

    def _record_id(self, record: dict) -> str:
        return str(record.get("record_id") or record.get("signal_id") or record.get("event_id") or "").strip()

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
        return self._direction_adjusted_move(record, 300) > 0.002

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
        return self._direction_adjusted_move(record, 60) > 0.002

    def _climax_resolved(self, record: dict) -> bool:
        return (
            record.get("reversal_after_climax") is True
            or record.get("move_after_alert_60s") is not None
            or record.get("move_after_alert_300s") is not None
        )

    def _climax_reversal(self, record: dict) -> bool:
        if record.get("reversal_after_climax") is True:
            return True
        move = self._direction_adjusted_move(record, 60, allow_none=True)
        if move is None:
            move = self._direction_adjusted_move(record, 300, allow_none=True)
        return bool(move is not None and move < -0.002)

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
        return self._direction_adjusted_move(record, 60) > 0.003

    def _direction_adjusted_move(self, record: dict, window_sec: int, *, allow_none: bool = False) -> float | None:
        key = f"direction_adjusted_move_after_{int(window_sec)}s"
        value = record.get(key)
        if value is None:
            value = aligned_move(record.get(f"move_after_alert_{int(window_sec)}s"), record.get("direction_bucket"))
        if allow_none and value in (None, ""):
            return None
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return None if allow_none else 0.0

    def _record_asset_symbol(self, record: dict) -> str:
        return str(record.get("asset_symbol") or record.get("asset_case_label") or "")

    def _quality_brief(self, asset_scores: dict, pair_scores: dict, pool_scores: dict) -> str:
        asset_quality = float(asset_scores.get("quality_score") or 0.0)
        prealert_precision = float(asset_scores.get("prealert_precision_score") or 0.0)
        market_alignment = float(asset_scores.get("market_context_alignment_score") or 0.0)
        if int(asset_scores.get("sample_size") or 0) < self.actionable_min_samples:
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
