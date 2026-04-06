import asyncio

from address_intelligence import AddressIntelligenceManager
from analyzer import BehaviorAnalyzer
from archive_store import ArchiveStore
from config import (
    PRICE_FAIL_TTL_SEC,
    PRICE_MAX_CONCURRENCY,
    PRICE_REQUEST_TIMEOUT_SEC,
    PRICE_TTL_SEC,
)
from filter import WATCH_ADDRESSES, set_address_intelligence_manager
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
