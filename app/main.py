import asyncio
from pathlib import Path
import time

from address_intelligence import AddressIntelligenceManager
from analyzer import BehaviorAnalyzer
from archive_store import ArchiveStore
from config import (
    PRICE_FAIL_TTL_SEC,
    PRICE_MAX_CONCURRENCY,
    PRICE_REQUEST_TIMEOUT_SEC,
    PRICE_TTL_SEC,
)
from filter import WATCH_ADDRESSES, get_address_meta, set_address_intelligence_manager
from followup_tracker import FollowupTracker
from listener import producer, replay_spill_worker, worker
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
set_address_intelligence_manager(address_intelligence)

pipeline = SignalPipeline(
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


def restore_persisted_exchange_adjacent() -> dict:
    loaded = address_intelligence.load_persisted_exchange_adjacent(PERSISTED_EXCHANGE_ADJACENT_PATH)
    now_ts = int(time.time())
    restored_runtime_count = 0
    restored_fallback_count = 0

    for item in address_intelligence.export_persistable_exchange_adjacent():
        address = str(item.get("address") or "").lower()
        if not address:
            continue

        full_top_counterparties = list(item.get("top_counterparties") or [])
        restored_top_counterparties = full_top_counterparties[:5]
        anchor_watch_address, restore_anchor_mode = _select_restore_anchor(full_top_counterparties)
        if not anchor_watch_address:
            continue

        anchor_meta = get_address_meta(anchor_watch_address)
        active_until = now_ts + 86400
        cooling_until = now_ts + 2 * 86400
        closing_until = now_ts + 7 * 86400
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
            continue

        restored_runtime_count += 1
        if restore_anchor_mode == "historical_non_watch":
            restored_fallback_count += 1
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
        "restored_runtime_count": restored_runtime_count,
        "restored_fallback_count": restored_fallback_count,
        "path": str(PERSISTED_EXCHANGE_ADJACENT_PATH),
    }


async def handle_tx(raw_item):
    """worker 入口：执行完整管道并输出信号。"""
    try:
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


async def main():
    print("🚀 系统启动...")
    restore_stats = restore_persisted_exchange_adjacent()
    if restore_stats["loaded_intel_count"] or restore_stats["restored_runtime_count"]:
        print(
            "♻️ 已恢复 persisted exchange_adjacent:",
            f"intel={restore_stats['loaded_intel_count']},",
            f"runtime_watch={restore_stats['restored_runtime_count']},",
            f"fallback_anchor={restore_stats['restored_fallback_count']},",
            f"path={restore_stats['path']}",
        )
    asyncio.create_task(producer())
    asyncio.create_task(replay_spill_worker())
    workers = [
        asyncio.create_task(worker(handle_tx))
        for _ in range(5)
    ]
    await asyncio.gather(*workers)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 程序已手动停止")
