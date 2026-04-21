import asyncio
from collections import defaultdict
from pathlib import Path
import time
import traceback

from address_intelligence import AddressIntelligenceManager
from analyzer import BehaviorAnalyzer
from archive_store import ArchiveStore
from config import (
    OUTCOME_SCHEDULER_ENABLE,
    OUTCOME_SETTLE_BATCH_SIZE,
    OUTCOME_TICK_INTERVAL_SEC,
    PRICE_FAIL_TTL_SEC,
    PRICE_MAX_CONCURRENCY,
    PRICE_REQUEST_TIMEOUT_SEC,
    PRICE_TTL_SEC,
)
from filter import WATCH_ADDRESSES, get_address_meta, set_address_intelligence_manager
from followup_tracker import FollowupTracker
from listener import producer, replay_spill_worker, set_listener_archive_store, worker
from market_context_adapter import (
    MarketContextConfigError,
    build_market_context_runtime_self_check,
    format_market_context_runtime_self_check,
    validate_market_context_runtime_self_check,
)
from notifier import send_signal
from pipeline import SignalPipeline
from price_service import PriceService
from scoring import AddressScorer
from signal_quality_gate import SignalQualityGate
from signal_interpreter import SignalInterpreter
from state_manager import StateManager
from strategy_engine import StrategyEngine
from token_scoring import TokenScorer


PERSISTED_EXCHANGE_ADJACENT_PATH = Path(__file__).resolve().parent.parent / "data" / "persisted_exchange_adjacent.json"
PERSISTED_EXCHANGE_ACTIVE_WATCH_ACTIVE_SEC = 12 * 3600
PERSISTED_EXCHANGE_ACTIVE_WATCH_COOLING_SEC = 24 * 3600
PERSISTED_EXCHANGE_ACTIVE_WATCH_CLOSING_SEC = 72 * 3600
PERSISTED_EXCHANGE_HISTORICAL_ACTIVE_SEC = 4 * 3600
PERSISTED_EXCHANGE_HISTORICAL_COOLING_SEC = 12 * 3600
PERSISTED_EXCHANGE_HISTORICAL_CLOSING_SEC = 24 * 3600
PERSISTED_EXCHANGE_HISTORICAL_MIN_CANDIDATE_SCORE = 72.0
PERSISTED_EXCHANGE_HISTORICAL_MIN_EXCHANGE_INTERACTIONS = 5
PERSISTED_EXCHANGE_HISTORICAL_MIN_SEEN_COUNT = 6
PERSISTED_EXCHANGE_HISTORICAL_MIN_MAX_USD = 250000.0
PERSISTED_EXCHANGE_HISTORICAL_MIN_AVG_USD = 50000.0
PERSISTED_EXCHANGE_HISTORICAL_MIN_COUNTERPARTY_COUNT = 4


price_service = PriceService(
    ttl_sec=PRICE_TTL_SEC,
    fail_ttl_sec=PRICE_FAIL_TTL_SEC,
    request_timeout_sec=PRICE_REQUEST_TIMEOUT_SEC,
    max_concurrency=PRICE_MAX_CONCURRENCY,
)
state_manager = StateManager(max_events_per_address=300, max_events_per_token=500)
behavior_analyzer = BehaviorAnalyzer()
address_scorer = AddressScorer()
token_scorer = TokenScorer()
quality_gate = SignalQualityGate(state_manager=state_manager)
strategy_engine = StrategyEngine()
signal_interpreter = SignalInterpreter()
address_intelligence = AddressIntelligenceManager(watch_addresses=WATCH_ADDRESSES)
archive_store = ArchiveStore()
followup_tracker = FollowupTracker()
set_listener_archive_store(archive_store)
set_address_intelligence_manager(address_intelligence)

pipeline: SignalPipeline | None = None


def _build_pipeline() -> SignalPipeline:
    return SignalPipeline(
        price_service=price_service,
        state_manager=state_manager,
        behavior_analyzer=behavior_analyzer,
        address_scorer=address_scorer,
        token_scorer=token_scorer,
        quality_gate=quality_gate,
        strategy_engine=strategy_engine,
        signal_interpreter=signal_interpreter,
        address_intelligence=address_intelligence,
        archive_store=archive_store,
        followup_tracker=followup_tracker,
    )


def _run_market_context_startup_check() -> dict:
    report = build_market_context_runtime_self_check()
    print(format_market_context_runtime_self_check(report))
    return validate_market_context_runtime_self_check(report)


def _extract_counterparty_address(counterparty_entry) -> str:
    if isinstance(counterparty_entry, dict):
        return str(counterparty_entry.get("address") or "").lower()
    if isinstance(counterparty_entry, (list, tuple)) and counterparty_entry:
        return str(counterparty_entry[0] or "").lower()
    return ""


def _extract_counterparty_count(counterparty_entry) -> int:
    if isinstance(counterparty_entry, dict):
        return int(counterparty_entry.get("count") or 0)
    if isinstance(counterparty_entry, (list, tuple)) and len(counterparty_entry) > 1:
        return int(counterparty_entry[1] or 0)
    return 0


def _select_restore_anchor_watch_address(counterparties: list[dict] | list[list] | None) -> str:
    for entry in counterparties or []:
        address = _extract_counterparty_address(entry)
        if address and address in WATCH_ADDRESSES:
            return address
    return ""


def _select_restore_anchor(counterparties: list[dict] | list[list] | None) -> tuple[str, str]:
    active_watch_anchor = _select_restore_anchor_watch_address(counterparties)
    if active_watch_anchor:
        return active_watch_anchor, "active_watch"

    ranked = sorted(
        list(counterparties or []),
        key=lambda entry: (-_extract_counterparty_count(entry), _extract_counterparty_address(entry)),
    )
    for entry in ranked:
        address = _extract_counterparty_address(entry)
        if address:
            return address, "historical_non_watch"
    return "", ""


def _restore_window_by_mode(now_ts: int, restore_anchor_mode: str) -> tuple[int, int, int]:
    if restore_anchor_mode == "historical_non_watch":
        return (
            now_ts + PERSISTED_EXCHANGE_HISTORICAL_ACTIVE_SEC,
            now_ts + PERSISTED_EXCHANGE_HISTORICAL_COOLING_SEC,
            now_ts + PERSISTED_EXCHANGE_HISTORICAL_CLOSING_SEC,
        )
    return (
        now_ts + PERSISTED_EXCHANGE_ACTIVE_WATCH_ACTIVE_SEC,
        now_ts + PERSISTED_EXCHANGE_ACTIVE_WATCH_COOLING_SEC,
        now_ts + PERSISTED_EXCHANGE_ACTIVE_WATCH_CLOSING_SEC,
    )


def _evaluate_restore_candidate(
    item: dict,
    *,
    anchor_watch_address: str,
    restore_anchor_mode: str,
    full_top_counterparties: list[dict] | list[list],
) -> tuple[bool, str]:
    if not anchor_watch_address:
        return False, "missing_restore_anchor"

    if restore_anchor_mode != "historical_non_watch":
        return True, ""

    candidate_score = float(item.get("candidate_score") or 0.0)
    exchange_interactions = int(item.get("exchange_interactions") or 0)
    seen_count = int(item.get("seen_count") or 0)
    max_usd = float(item.get("max_usd") or 0.0)
    avg_usd = float(item.get("avg_usd") or 0.0)
    top_counterparty_count = 0
    if full_top_counterparties:
        top_counterparty_count = max(
            _extract_counterparty_count(entry)
            for entry in full_top_counterparties
        )

    if candidate_score < PERSISTED_EXCHANGE_HISTORICAL_MIN_CANDIDATE_SCORE:
        return False, "historical_candidate_score_too_low"
    if exchange_interactions < PERSISTED_EXCHANGE_HISTORICAL_MIN_EXCHANGE_INTERACTIONS:
        return False, "historical_exchange_interactions_too_low"
    if seen_count < PERSISTED_EXCHANGE_HISTORICAL_MIN_SEEN_COUNT:
        return False, "historical_seen_count_too_low"
    if (
        max_usd < PERSISTED_EXCHANGE_HISTORICAL_MIN_MAX_USD
        and avg_usd < PERSISTED_EXCHANGE_HISTORICAL_MIN_AVG_USD
    ):
        return False, "historical_value_evidence_too_low"
    if top_counterparty_count < PERSISTED_EXCHANGE_HISTORICAL_MIN_COUNTERPARTY_COUNT:
        return False, "historical_counterparty_support_too_low"
    return True, ""


def restore_persisted_exchange_adjacent() -> dict:
    loaded = address_intelligence.load_persisted_exchange_adjacent(PERSISTED_EXCHANGE_ADJACENT_PATH)
    now_ts = int(time.time())
    stats = {
        "restored_attempted": 0,
        "restored_runtime_count": 0,
        "restored_fallback_count": 0,
        "restored_skipped_count": 0,
        "restored_skipped_by_reason": defaultdict(int),
        "restored_by_anchor": defaultdict(int),
        "restored_by_mode": defaultdict(int),
    }

    for item in address_intelligence.export_persistable_exchange_adjacent():
        address = str(item.get("address") or "").lower()
        if not address:
            stats["restored_skipped_count"] += 1
            stats["restored_skipped_by_reason"]["missing_address"] += 1
            continue
        stats["restored_attempted"] += 1

        full_top_counterparties = list(item.get("top_counterparties") or [])
        restored_top_counterparties = full_top_counterparties[:5]
        anchor_watch_address, restore_anchor_mode = _select_restore_anchor(full_top_counterparties)
        restore_allowed, skip_reason = _evaluate_restore_candidate(
            item,
            anchor_watch_address=anchor_watch_address,
            restore_anchor_mode=restore_anchor_mode,
            full_top_counterparties=full_top_counterparties,
        )
        if not restore_allowed:
            stats["restored_skipped_count"] += 1
            stats["restored_skipped_by_reason"][skip_reason or "restore_candidate_rejected"] += 1
            continue

        anchor_meta = get_address_meta(anchor_watch_address)
        active_until, cooling_until, closing_until = _restore_window_by_mode(
            now_ts,
            restore_anchor_mode,
        )
        anchor_usd_value = max(
            float(item.get("max_usd") or 0.0),
            float(item.get("avg_usd") or 0.0),
        )
        anchor_label = str(
            anchor_meta.get("label")
            or anchor_meta.get("display")
            or anchor_watch_address
        )
        restore_priority = 2 if restore_anchor_mode == "historical_non_watch" else int(anchor_meta.get("priority") or 1)
        restored_priority_mode = (
            "historical_non_watch_default"
            if restore_anchor_mode == "historical_non_watch"
            else "active_watch_inherit"
        )
        registered = state_manager.register_adjacent_watch(
            address=address,
            anchor_watch_address=anchor_watch_address,
            anchor_label=anchor_label,
            root_tx_hash="",
            token=None,
            anchor_usd_value=anchor_usd_value,
            opened_at=now_ts,
            active_until=active_until,
            cooling_until=cooling_until,
            closing_until=closing_until,
            hop=1,
            reason="persisted_exchange_adjacent",
            strategy_hint="persisted_exchange_adjacent",
            runtime_label_hint="exchange_adjacent",
            metadata={
                "priority": restore_priority,
                "anchor_strategy_role": str(anchor_meta.get("strategy_role") or ""),
                "display_hint_label": "exchange_adjacent",
                "display_hint_reason": "persisted_exchange_adjacent_restored",
                "restore_anchor_mode": restore_anchor_mode,
                "restored_priority_mode": restored_priority_mode,
                "restored_top_counterparties": restored_top_counterparties,
                "restore_source": "persisted_exchange_adjacent",
                "restore_strength": "aggressive_restore",
            },
        )
        if not registered:
            stats["restored_skipped_count"] += 1
            stats["restored_skipped_by_reason"]["register_adjacent_watch_failed"] += 1
            continue

        stats["restored_runtime_count"] += 1
        stats["restored_by_anchor"][anchor_watch_address] += 1
        stats["restored_by_mode"][restore_anchor_mode] += 1
        if restore_anchor_mode == "historical_non_watch":
            stats["restored_fallback_count"] += 1
        address_intelligence.mark_display_hint(
            address=address,
            display_hint_label="exchange_adjacent",
            expire_at=active_until,
            display_hint_reason="persisted_exchange_adjacent_restored",
            display_hint_anchor_label=anchor_label,
            display_hint_anchor_address=anchor_watch_address,
            display_hint_usd_value=anchor_usd_value,
            display_hint_source="persisted_exchange_adjacent",
        )

    return {
        "loaded_intel_count": len(loaded),
        "restored_attempted": int(stats["restored_attempted"]),
        "restored_runtime_count": int(stats["restored_runtime_count"]),
        "restored_fallback_count": int(stats["restored_fallback_count"]),
        "restored_skipped_count": int(stats["restored_skipped_count"]),
        "restored_skipped_by_reason": dict(sorted(stats["restored_skipped_by_reason"].items())),
        "restored_by_anchor": dict(
            sorted(
                stats["restored_by_anchor"].items(),
                key=lambda item: (-int(item[1]), item[0]),
            )[:5]
        ),
        "restored_by_mode": dict(sorted(stats["restored_by_mode"].items())),
        "path": str(PERSISTED_EXCHANGE_ADJACENT_PATH),
    }


async def handle_tx(raw_item):
    """worker 入口：执行完整管道并输出信号。"""
    try:
        if pipeline is None:
            raise RuntimeError("signal pipeline not initialized")
        result = await pipeline.process(raw_item)
        if not result:
            return

        signal = result["signal"]
        event = result["event"]
        delivered = await send_signal(signal, event)
        pipeline.finalize_notification_delivery(
            event=event,
            signal=signal,
            delivered=delivered,
            behavior_case=result.get("case"),
            behavior=result.get("behavior"),
            gate_metrics=(result.get("gate").metrics if result.get("gate") is not None else None),
            archive_status=result.get("archive_status"),
        )

    except Exception as e:
        print("处理交易出错:", e)
        traceback.print_exc()


async def outcome_scheduler_worker():
    while True:
        await asyncio.sleep(max(int(OUTCOME_TICK_INTERVAL_SEC or 1), 1))
        try:
            if pipeline is None or not bool(OUTCOME_SCHEDULER_ENABLE):
                continue
            pipeline.outcome_scheduler.settle_due_outcomes(
                now=int(time.time()),
                limit=int(OUTCOME_SETTLE_BATCH_SIZE or 200),
            )
        except Exception as exc:
            print(f"outcome scheduler tick failed: {exc}")


async def main():
    global pipeline
    print("🚀 系统启动...")
    _run_market_context_startup_check()
    if pipeline is None:
        pipeline = _build_pipeline()
    if bool(OUTCOME_SCHEDULER_ENABLE):
        restore_result = pipeline.outcome_scheduler.restore_pending_outcomes_from_sqlite(now=int(time.time()), catchup=True)
        if restore_result.get("restored_pending_count") or (restore_result.get("catchup") or {}).get("processed_count"):
            print(
                "⏱️ outcome scheduler restored:",
                f"pending={restore_result.get('restored_pending_count')},",
                f"state_records={restore_result.get('restored_state_record_count')},",
                f"catchup={restore_result.get('catchup')}",
            )
    restore_stats = restore_persisted_exchange_adjacent()
    if restore_stats["loaded_intel_count"] or restore_stats["restored_runtime_count"]:
        print(
            "♻️ 已恢复 persisted exchange_adjacent:",
            f"intel={restore_stats['loaded_intel_count']},",
            f"attempted={restore_stats['restored_attempted']},",
            f"runtime_watch={restore_stats['restored_runtime_count']},",
            f"fallback_anchor={restore_stats['restored_fallback_count']},",
            f"skipped={restore_stats['restored_skipped_count']},",
            f"path={restore_stats['path']}",
        )
        print(
            "♻️ persisted exchange_adjacent 恢复摘要:",
            f"by_mode={restore_stats['restored_by_mode']},",
            f"top_anchor={restore_stats['restored_by_anchor']},",
            f"skipped_by_reason={restore_stats['restored_skipped_by_reason']}",
        )
    asyncio.create_task(producer())
    asyncio.create_task(replay_spill_worker())
    if bool(OUTCOME_SCHEDULER_ENABLE):
        asyncio.create_task(outcome_scheduler_worker())
    workers = [
        asyncio.create_task(worker(handle_tx))
        for _ in range(5)
    ]
    await asyncio.gather(*workers)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except MarketContextConfigError as exc:
        print(f"❌ market context 启动校验失败: {exc}")
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("🛑 程序已手动停止")
