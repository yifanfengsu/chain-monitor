import asyncio
from collections import defaultdict
import hashlib
from itertools import islice
import json
from pathlib import Path
import time

from web3 import Web3

from config import (
    RPC_URL,
    SPILL_REPLAY_INTERVAL_SEC,
    SPILL_REPLAY_MAX_BATCH,
    SPILL_REPLAY_QUEUE_WATERMARK,
)
from constants import ERC20_TRANSFER_EVENT_SIG
from filter import WATCH_ADDRESSES, get_address_meta, strategy_role_group
from lp_noise_rules import (
    LP_ADJACENT_NOISE_RULE_VERSION,
    LP_ADJACENT_NOISE_STAGE_LISTENER,
    lp_adjacent_noise_confidence_bucket,
    lp_adjacent_noise_core_decision,
)
from lp_registry import ACTIVE_LP_POOL_ADDRESSES
from state_manager import get_runtime_adjacent_watch_addresses

# 使用 HTTPProvider 做新区块轮询。
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# 生产者/消费者缓冲队列。
tx_queue = asyncio.Queue(maxsize=5000)
SPILL_FILE = Path("/tmp/whale_tx_queue_spill.ndjson")
_listener_archive_store = None
QUEUE_STATS = {
    "spill_count": 0,
    "replay_count": 0,
    "replay_skipped_count": 0,
    "replay_error_count": 0,
    "lp_adjacent_noise_skipped_in_listener": 0,
    "lp_adjacent_noise_listener_reason": "",
    "lp_adjacent_noise_listener_confidence": 0.0,
    "lp_adjacent_noise_listener_source_signals": [],
    "lp_adjacent_noise_rule_version": LP_ADJACENT_NOISE_RULE_VERSION,
    "lp_adjacent_noise_listener_reason_counts": {},
    "lp_adjacent_noise_listener_confidence_buckets": {},
    "lp_adjacent_noise_listener_source_signal_counts": {},
}

# get_logs 的 topic OR 数量不宜过大，按批次分片查询。
TOPIC_CHUNK_SIZE = 25


def get_monitored_watch_addresses() -> set[str]:
    return WATCH_ADDRESSES | get_runtime_adjacent_watch_addresses()


def get_monitored_transfer_addresses() -> set[str]:
    return get_monitored_watch_addresses() | ACTIVE_LP_POOL_ADDRESSES


def set_listener_archive_store(archive_store) -> None:
    global _listener_archive_store
    _listener_archive_store = archive_store


def _to_hex(value):
    """把 HexBytes/bytes/str 统一成 0x 前缀字符串。"""
    if value is None:
        return "0x"
    if isinstance(value, str):
        return value if value.startswith("0x") else f"0x{value}"
    if hasattr(value, "hex"):
        raw = value.hex()
        return raw if raw.startswith("0x") else f"0x{raw}"
    return str(value)


def _topic_for_address(address: str) -> str:
    """把地址编码成 ERC20 Transfer topic 格式。方便后续过滤"""
    return f"0x{'0' * 24}{address.lower()[2:]}"


def _address_from_topic(topic) -> str:
    return "0x" + _to_hex(topic)[-40:].lower()


def _compact_log(log):
    """仅保留后续 processor 需要的日志字段。"""
    return {
        "address": str(log.get("address", "")).lower(),
        "topics": list(log.get("topics", [])),
        "data": log.get("data"),
        "logIndex": int(log.get("logIndex", 0)) if log.get("logIndex") is not None else 0,
        "transactionHash": log.get("transactionHash"),
        "blockNumber": int(log.get("blockNumber", 0)) if log.get("blockNumber") is not None else 0,
    }


def _chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _extract_touched_watch_addresses(logs, monitored_watch_addresses: set[str] | None = None):
    """从 Transfer 日志中找出与监控地址相关的地址集合。"""
    monitored_watch_addresses = monitored_watch_addresses or get_monitored_watch_addresses()
    touched = set()
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue

        from_addr = _address_from_topic(topics[1])
        to_addr = _address_from_topic(topics[2])
        if from_addr in monitored_watch_addresses:
            touched.add(from_addr)
        if to_addr in monitored_watch_addresses:
            touched.add(to_addr)
    return touched


def _extract_touched_lp_pools(logs):
    """从 Transfer 日志中找出与监控 LP 池相关的地址集合。"""
    touched = set()
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue

        from_addr = _address_from_topic(topics[1])
        to_addr = _address_from_topic(topics[2])
        if from_addr in ACTIVE_LP_POOL_ADDRESSES:
            touched.add(from_addr)
        if to_addr in ACTIVE_LP_POOL_ADDRESSES:
            touched.add(to_addr)
    return touched


def _extract_participant_addresses(logs):
    """提取同一笔 token flow 中出现的参与地址。"""
    participants = set()
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        participants.add(_address_from_topic(topics[1]))
        participants.add(_address_from_topic(topics[2]))
    return participants


def _extract_token_addresses(logs):
    """提取日志涉及的 token 合约地址。"""
    tokens = []
    seen = set()
    for log in logs:
        token = str(log.get("address") or "").lower()
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _pool_transfer_stats(logs: list[dict], touched_lp_pools: list[str]) -> dict[str, int]:
    stats = {}
    pools = {str(address or "").lower() for address in touched_lp_pools if address}
    for pool_address in pools:
        transfer_count = 0
        for log in logs:
            topics = log.get("topics") or []
            if len(topics) < 3:
                continue
            from_addr = _address_from_topic(topics[1])
            to_addr = _address_from_topic(topics[2])
            if from_addr == pool_address or to_addr == pool_address:
                transfer_count += 1
        stats[pool_address] = transfer_count
    return stats


def _pool_candidate_weight(raw_log_count: int, touched_lp_pools: list[str], pool_transfer_counts: dict[str, int]) -> float:
    if not touched_lp_pools:
        return 0.0
    base = min(1.0, float(raw_log_count or 0) / 8.0)
    pool_hits = min(0.6, len(touched_lp_pools) * 0.18)
    transfer_density = min(0.6, sum(int(value or 0) for value in pool_transfer_counts.values()) / 10.0)
    return round(min(1.0, 0.2 + base * 0.35 + pool_hits + transfer_density * 0.25), 3)


def _build_eth_candidate(tx, block_num: int, touched_watch_addresses: list[str]) -> dict:
    return {
        "hash": tx.get("hash"),
        "from": tx["from"].lower(),
        "to": tx["to"].lower() if tx.get("to") else None,
        "value": tx.get("value", 0),
        "input": tx.get("input"),
        "blockNumber": int(block_num),
        "ingest_ts": int(time.time()),
        "source_kind": "eth_transfer_candidate",
        "touched_watch_addresses": list(touched_watch_addresses),
        "raw_log_count": 0,
        "participant_addresses": [
            addr for addr in [tx["from"].lower(), tx["to"].lower() if tx.get("to") else None]
            if addr
        ],
        "next_hop_addresses": [tx["to"].lower()] if tx.get("to") else [],
        "lp_adjacent_noise_skipped_in_listener": False,
        "lp_adjacent_noise_listener_reason": "",
        "lp_adjacent_noise_listener_confidence": 0.0,
        "lp_adjacent_noise_listener_source_signals": [],
        "lp_adjacent_noise_rule_version": "",
        "lp_adjacent_noise_decision_stage": "",
        "lp_adjacent_noise_filtered": False,
        "lp_adjacent_noise_reason": "",
        "lp_adjacent_noise_confidence": 0.0,
        "lp_adjacent_noise_source_signals": [],
        "lp_adjacent_noise_context_used": [],
        "lp_adjacent_noise_runtime_context_present": False,
        "lp_adjacent_noise_downstream_context_present": False,
        "replay_source": "",
    }


def _build_token_flow_candidate(
    tx_hash: str,
    logs: list[dict],
    block_num: int,
    touched_watch_addresses: list[str],
    touched_lp_pools: list[str],
) -> dict:
    participant_addresses = sorted(_extract_participant_addresses(logs))
    touched_targets = set(touched_watch_addresses) | set(touched_lp_pools)
    pool_transfer_counts = _pool_transfer_stats(logs, touched_lp_pools)
    pool_candidate_weight = _pool_candidate_weight(len(logs), touched_lp_pools, pool_transfer_counts)
    next_hop_addresses = [
        address for address in participant_addresses
        if address not in touched_targets
    ]
    return {
        "kind": "token_flow_candidate",
        "tx_hash": tx_hash,
        "logs": logs,
        "block_number": block_num,
        "ingest_ts": int(time.time()),
        "source_kind": "token_flow_candidate",
        "touched_watch_addresses": list(touched_watch_addresses),
        "touched_lp_pools": list(touched_lp_pools),
        "touched_lp_pool_count": len(touched_lp_pools),
        "tx_pool_hit_count": len(touched_lp_pools),
        "raw_log_count": len(logs),
        "participant_addresses": participant_addresses,
        "next_hop_addresses": next_hop_addresses,
        "token_addresses": _extract_token_addresses(logs),
        "pool_transfer_count_by_pool": pool_transfer_counts,
        "pool_candidate_weight": pool_candidate_weight,
        "lp_adjacent_noise_skipped_in_listener": False,
        "lp_adjacent_noise_listener_reason": "",
        "lp_adjacent_noise_listener_confidence": 0.0,
        "lp_adjacent_noise_listener_source_signals": [],
        "lp_adjacent_noise_rule_version": "",
        "lp_adjacent_noise_decision_stage": "",
        "lp_adjacent_noise_filtered": False,
        "lp_adjacent_noise_reason": "",
        "lp_adjacent_noise_confidence": 0.0,
        "lp_adjacent_noise_source_signals": [],
        "lp_adjacent_noise_context_used": [],
        "lp_adjacent_noise_runtime_context_present": False,
        "lp_adjacent_noise_downstream_context_present": False,
        "replay_source": "",
    }


def _listener_lp_adjacent_skip_audit_record(
    *,
    candidate: dict,
    watch_address: str,
    decision: dict,
) -> dict:
    watch_address = str(watch_address or "").lower()
    watch_meta = get_address_meta(watch_address)
    strategy_role = str(
        watch_meta.get("strategy_role")
        or candidate.get("strategy_role")
        or "unknown"
    )
    tx_hash = str(candidate.get("tx_hash") or candidate.get("hash") or "").lower()
    counterparty_candidates = [
        str(address or "").lower()
        for address in (candidate.get("participant_addresses") or [])
        if address and str(address or "").lower() != watch_address
    ]
    counterparty = counterparty_candidates[0] if counterparty_candidates else ""
    event_id_seed = "|".join(
        [
            tx_hash,
            watch_address or "adjacent_watch",
            "lp_adjacent_noise_skipped_in_listener",
        ]
    )
    silent_reason = {
        "stage": "listener_prefilter",
        "reason_code": "lp_adjacent_noise_skipped_in_listener",
        "reason_detail": str(decision.get("reason") or "lp_adjacent_noise_skipped_in_listener"),
        "reason_bucket": "prefilter_blocked",
    }
    return {
        "event_id": f"evt_{hashlib.sha1(event_id_seed.encode('utf-8')).hexdigest()[:16]}",
        "tx_hash": tx_hash,
        "watch_address": watch_address,
        "monitor_type": "adjacent_watch",
        "strategy_role": strategy_role,
        "role_group": str(strategy_role_group(strategy_role)),
        "intent_type": str(candidate.get("intent_type") or ""),
        "behavior_type": "unknown",
        "gate_reason": "lp_adjacent_noise_skipped_in_listener",
        "delivery_class": "drop",
        "stage": "listener_prefilter",
        "counterparty": counterparty,
        "watch_meta_source": str(watch_meta.get("watch_meta_source") or ""),
        "strategy_hint": str(watch_meta.get("strategy_hint") or ""),
        "runtime_adjacent_watch": bool(watch_meta.get("runtime_adjacent_watch")),
        "runtime_state": str(watch_meta.get("runtime_state") or ""),
        "anchor_watch_address": str(watch_meta.get("anchor_watch_address") or ""),
        "downstream_case_id": str(watch_meta.get("downstream_case_id") or ""),
        "touched_watch_addresses": list(candidate.get("touched_watch_addresses") or []),
        "touched_lp_pools": list(candidate.get("touched_lp_pools") or []),
        "touched_lp_pool_count": int(candidate.get("touched_lp_pool_count") or 0),
        "tx_pool_hit_count": int(candidate.get("tx_pool_hit_count") or 0),
        "pool_transfer_count_by_pool": dict(candidate.get("pool_transfer_count_by_pool") or {}),
        "pool_candidate_weight": round(float(candidate.get("pool_candidate_weight") or 0.0), 3),
        "participant_addresses": list(candidate.get("participant_addresses") or []),
        "lp_adjacent_noise_skipped_in_listener": True,
        "lp_adjacent_noise_listener_reason": str(decision.get("reason") or ""),
        "lp_adjacent_noise_listener_confidence": round(float(decision.get("confidence") or 0.0), 3),
        "lp_adjacent_noise_listener_source_signals": list(decision.get("source_signals") or []),
        "lp_adjacent_noise_rule_version": str(
            decision.get("rule_version") or LP_ADJACENT_NOISE_RULE_VERSION
        ),
        "lp_adjacent_noise_decision_stage": str(
            decision.get("decision_stage") or LP_ADJACENT_NOISE_STAGE_LISTENER
        ),
        "lp_adjacent_noise_filtered": True,
        "lp_adjacent_noise_reason": str(
            decision.get("reason") or "lp_adjacent_noise_skipped_in_listener"
        ),
        "lp_adjacent_noise_confidence": round(float(decision.get("confidence") or 0.0), 3),
        "lp_adjacent_noise_source_signals": list(decision.get("source_signals") or []),
        "lp_adjacent_noise_context_used": list(decision.get("context_used") or []),
        "lp_adjacent_noise_runtime_context_present": bool(
            decision.get("runtime_context_present")
        ),
        "lp_adjacent_noise_downstream_context_present": bool(
            decision.get("downstream_context_present")
        ),
        "silent_reason": silent_reason,
        "silent_reason_bucket": "prefilter_blocked",
        "usd_value": round(float(candidate.get("usd_value") or candidate.get("value") or 0.0), 2),
        "pricing_confidence": round(float(candidate.get("pricing_confidence") or 0.0), 3),
        "ingest_ts": int(candidate.get("ingest_ts") or time.time()),
    }


def _archive_listener_lp_adjacent_skip(
    *,
    candidate: dict,
    watch_address: str,
    decision: dict,
) -> None:
    if _listener_archive_store is None:
        return
    try:
        record = _listener_lp_adjacent_skip_audit_record(
            candidate=candidate,
            watch_address=watch_address,
            decision=dict(decision or {}),
        )
        _listener_archive_store.write_delivery_audit(
            record,
            archive_ts=int(candidate.get("ingest_ts") or time.time()),
        )
    except Exception as e:
        print(f"listener lp_adjacent_noise audit 归档失败: {e}")


def _schedule_listener_lp_adjacent_skip_audit(
    *,
    candidate: dict,
    watch_address: str,
    decision: dict,
) -> None:
    if _listener_archive_store is None:
        return
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(
            asyncio.to_thread(
                _archive_listener_lp_adjacent_skip,
                candidate=dict(candidate),
                watch_address=str(watch_address or "").lower(),
                decision=dict(decision or {}),
            )
        )
    except RuntimeError:
        _archive_listener_lp_adjacent_skip(
            candidate=dict(candidate),
            watch_address=str(watch_address or "").lower(),
            decision=dict(decision or {}),
        )


def _update_lp_adjacent_noise_listener_stats(decision: dict) -> None:
    reason = str(decision.get("reason") or "")
    confidence = float(decision.get("confidence") or 0.0)
    source_signals = list(decision.get("source_signals") or [])
    reason_counts = dict(QUEUE_STATS.get("lp_adjacent_noise_listener_reason_counts") or {})
    confidence_buckets = dict(QUEUE_STATS.get("lp_adjacent_noise_listener_confidence_buckets") or {})
    source_signal_counts = dict(QUEUE_STATS.get("lp_adjacent_noise_listener_source_signal_counts") or {})
    bucket = lp_adjacent_noise_confidence_bucket(confidence)

    QUEUE_STATS["lp_adjacent_noise_skipped_in_listener"] = int(
        QUEUE_STATS.get("lp_adjacent_noise_skipped_in_listener") or 0
    ) + 1
    QUEUE_STATS["lp_adjacent_noise_listener_reason"] = reason
    QUEUE_STATS["lp_adjacent_noise_listener_confidence"] = round(confidence, 3)
    QUEUE_STATS["lp_adjacent_noise_listener_source_signals"] = list(source_signals)
    QUEUE_STATS["lp_adjacent_noise_rule_version"] = str(
        decision.get("rule_version") or LP_ADJACENT_NOISE_RULE_VERSION
    )

    if reason:
        reason_counts[reason] = int(reason_counts.get(reason) or 0) + 1
    confidence_buckets[bucket] = int(confidence_buckets.get(bucket) or 0) + 1
    for signal in source_signals:
        text = str(signal or "").strip()
        if not text:
            continue
        signal_key = text.split("=", 1)[0]
        source_signal_counts[signal_key] = int(source_signal_counts.get(signal_key) or 0) + 1

    QUEUE_STATS["lp_adjacent_noise_listener_reason_counts"] = reason_counts
    QUEUE_STATS["lp_adjacent_noise_listener_confidence_buckets"] = confidence_buckets
    QUEUE_STATS["lp_adjacent_noise_listener_source_signal_counts"] = source_signal_counts


def _lp_adjacent_noise_listener_decision(
    *,
    watch_address: str,
    candidate: dict | None = None,
) -> dict:
    candidate = dict(candidate or {})
    candidate["watch_address"] = str(watch_address or "").lower()
    candidate["monitor_type"] = str(candidate.get("monitor_type") or "adjacent_watch")
    return lp_adjacent_noise_core_decision(
        candidate,
        stage=LP_ADJACENT_NOISE_STAGE_LISTENER,
        watch_addresses=WATCH_ADDRESSES,
    )


async def _fetch_transfer_logs_for_block(block_num: int):
    """
    在单区块内抓取与监控地址相关的 ERC20 Transfer 日志。
    规则：
    - from 在监控地址内
    - 或 to 在监控地址内
    """
    monitored_addresses = get_monitored_transfer_addresses()
    if not monitored_addresses:
        return {}

    watch_topics = [_topic_for_address(addr) for addr in monitored_addresses]
    grouped = defaultdict(list)
    seen = set()

    for topic_chunk in _chunked(watch_topics, TOPIC_CHUNK_SIZE):
        filters = [
            {
                "fromBlock": block_num,
                "toBlock": block_num,
                "topics": [ERC20_TRANSFER_EVENT_SIG, topic_chunk, None],
            },
            {
                "fromBlock": block_num,
                "toBlock": block_num,
                "topics": [ERC20_TRANSFER_EVENT_SIG, None, topic_chunk],
            },
        ]

        for query_filter in filters:
            try:
                logs = w3.eth.get_logs(query_filter)
            except Exception as e:
                print(f"❌ get_logs 失败: block={block_num}, error={e}")
                continue

            for log in logs:
                tx_hash = _to_hex(log.get("transactionHash")).lower()
                log_index = int(log.get("logIndex", 0)) if log.get("logIndex") is not None else 0
                dedup_key = (tx_hash, log_index)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                grouped[tx_hash].append(_compact_log(log))

    for tx_hash in grouped:
        grouped[tx_hash].sort(key=lambda item: item["logIndex"])

    return grouped


def _spill_to_disk(item):
    """队列高压时把候选写入本地 ndjson，后续可做重放。"""
    try:
        SPILL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with SPILL_FILE.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
        QUEUE_STATS["spill_count"] += 1
        print(
            f"⚠️ spill 落盘: spill_count={QUEUE_STATS['spill_count']} "
            f"queue_size={tx_queue.qsize()} tx={item.get('tx_hash') or item.get('hash') or ''}"
        )
    except Exception as e:
        print(f"❌ spill 失败: {e}")


async def _enqueue(item, allow_spill: bool = True):
    """
    队列写入策略：
    1) 尝试短时间入队
    2) 超时则落盘，避免阻塞主监听循环
    """
    try:
        await asyncio.wait_for(tx_queue.put(item), timeout=0.05)
        return True
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        print(f"❌ 入队异常: {e}")

    if not allow_spill:
        return False

    await asyncio.to_thread(_spill_to_disk, item)
    return False


def _load_spill_batch(max_batch: int) -> tuple[list[dict], list[str], int]:
    if not SPILL_FILE.exists():
        return [], [], 0

    with SPILL_FILE.open("r", encoding="utf-8") as fp:
        raw_lines = fp.readlines()

    selected = []
    remaining_lines = []
    error_count = 0
    for line in raw_lines:
        if len(selected) >= max_batch:
            remaining_lines.append(line)
            continue
        text = str(line or "").strip()
        if not text:
            continue
        try:
            selected.append(json.loads(text))
        except Exception:
            error_count += 1
    return selected, remaining_lines, error_count


def _rewrite_spill_file(remaining_entries: list[dict], remaining_lines: list[str]) -> None:
    if not remaining_entries and not remaining_lines:
        if SPILL_FILE.exists():
            SPILL_FILE.unlink(missing_ok=True)
        return

    SPILL_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SPILL_FILE.open("w", encoding="utf-8") as fp:
        for item in remaining_entries:
            fp.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
        for line in remaining_lines:
            fp.write(line if line.endswith("\n") else f"{line}\n")


async def replay_spill_worker(
    interval_sec: int = SPILL_REPLAY_INTERVAL_SEC,
    max_batch: int = SPILL_REPLAY_MAX_BATCH,
    queue_watermark: int = SPILL_REPLAY_QUEUE_WATERMARK,
):
    while True:
        try:
            if tx_queue.qsize() >= max(int(queue_watermark), 1):
                QUEUE_STATS["replay_skipped_count"] += 1
                await asyncio.sleep(max(int(interval_sec), 1))
                continue

            entries, remaining_lines, load_errors = await asyncio.to_thread(
                _load_spill_batch,
                max(int(max_batch), 1),
            )
            if load_errors:
                QUEUE_STATS["replay_error_count"] += int(load_errors)
                print(
                    f"⚠️ replay 解析异常: replay_error_count={QUEUE_STATS['replay_error_count']}"
                )
            if not entries:
                await asyncio.sleep(max(int(interval_sec), 1))
                continue

            deferred_entries = []
            replayed = 0
            skipped = 0
            for item in entries:
                item["replay_source"] = str(item.get("replay_source") or "spill_replay")
                ok = await _enqueue(item, allow_spill=False)
                if ok:
                    replayed += 1
                    continue
                skipped += 1
                deferred_entries.append(item)

            await asyncio.to_thread(_rewrite_spill_file, deferred_entries, remaining_lines)
            QUEUE_STATS["replay_count"] += replayed
            QUEUE_STATS["replay_skipped_count"] += skipped
            print(
                "♻️ spill replay: "
                f"replay_count={QUEUE_STATS['replay_count']} "
                f"replay_skipped_count={QUEUE_STATS['replay_skipped_count']} "
                f"replay_error_count={QUEUE_STATS['replay_error_count']} "
                f"queue_size={tx_queue.qsize()}"
            )
        except Exception as e:
            QUEUE_STATS["replay_error_count"] += 1
            print(f"❌ spill replay 失败: {e}")

        await asyncio.sleep(max(int(interval_sec), 1))


async def producer():
    """生产者：拉取新区块并把候选交易放入队列。"""
    print("🚀 启动 Listener...")
    last_block = w3.eth.block_number

    while True:
        try:
            latest_block = w3.eth.block_number

            for block_num in range(last_block + 1, latest_block + 1):
                block = w3.eth.get_block(block_num, full_transactions=True)
                print(f"新区块: {block_num}, 交易数: {len(block.transactions)}")
                monitored_watch_addresses = get_monitored_watch_addresses()

                # 先抓 token flow 候选（不依赖 Router，可覆盖聚合器/代理/多跳）。
                tx_transfer_logs = await _fetch_transfer_logs_for_block(block_num)
                token_flow_tx_hashes = set(tx_transfer_logs.keys())

                # ETH 原生转账候选。
                for tx in block.transactions:
                    from_addr = tx["from"].lower()
                    to_addr = tx["to"].lower() if tx["to"] else None
                    tx_hash_hex = _to_hex(tx.get("hash")).lower()

                    # 同一 tx 如果已命中 token flow 候选，后续主要走 token 资金流解析。
                    if tx_hash_hex in token_flow_tx_hashes:
                        continue

                    if tx["value"] > 0 and (
                        from_addr in monitored_watch_addresses
                        or (to_addr and to_addr in monitored_watch_addresses)
                    ):
                        touched_watch_addresses = [
                            addr for addr in [from_addr, to_addr]
                            if addr and addr in monitored_watch_addresses
                        ]
                        await _enqueue(_build_eth_candidate(tx, block_num, touched_watch_addresses))

                # 为每个触达地址单独生成候选，方便后续行为建模按地址归因。
                for tx_hash, logs in tx_transfer_logs.items():
                    touched_watch_addresses = _extract_touched_watch_addresses(logs, monitored_watch_addresses)
                    touched_lp_pools = _extract_touched_lp_pools(logs)
                    if not touched_watch_addresses and not touched_lp_pools:
                        continue

                    candidate = _build_token_flow_candidate(
                        tx_hash=tx_hash,
                        logs=logs,
                        block_num=block_num,
                        touched_watch_addresses=sorted(touched_watch_addresses),
                        touched_lp_pools=sorted(touched_lp_pools),
                    )
                    for watch_address in touched_watch_addresses:
                        listener_skip_decision = _lp_adjacent_noise_listener_decision(
                            watch_address=watch_address,
                            candidate=candidate,
                        )
                        if listener_skip_decision.get("is_noise"):
                            _update_lp_adjacent_noise_listener_stats(listener_skip_decision)
                            _schedule_listener_lp_adjacent_skip_audit(
                                candidate=candidate,
                                watch_address=watch_address,
                                decision=listener_skip_decision,
                            )
                            continue
                        enriched = dict(candidate)
                        enriched["watch_address"] = watch_address
                        enriched["monitor_type"] = "watch_address" if watch_address in WATCH_ADDRESSES else "adjacent_watch"
                        enriched["lp_adjacent_noise_skipped_in_listener"] = False
                        enriched["lp_adjacent_noise_listener_reason"] = ""
                        enriched["lp_adjacent_noise_listener_confidence"] = 0.0
                        enriched["lp_adjacent_noise_listener_source_signals"] = []
                        enriched["lp_adjacent_noise_rule_version"] = ""
                        enriched["lp_adjacent_noise_decision_stage"] = ""
                        enriched["lp_adjacent_noise_filtered"] = False
                        enriched["lp_adjacent_noise_reason"] = ""
                        enriched["lp_adjacent_noise_confidence"] = 0.0
                        enriched["lp_adjacent_noise_source_signals"] = []
                        enriched["lp_adjacent_noise_context_used"] = []
                        enriched["lp_adjacent_noise_runtime_context_present"] = False
                        enriched["lp_adjacent_noise_downstream_context_present"] = False
                        await _enqueue(enriched)
                    for pool_address in touched_lp_pools:
                        enriched = dict(candidate)
                        enriched["watch_address"] = pool_address
                        enriched["monitor_type"] = "lp_pool"
                        enriched["lp_adjacent_noise_skipped_in_listener"] = False
                        enriched["lp_adjacent_noise_listener_reason"] = ""
                        enriched["lp_adjacent_noise_listener_confidence"] = 0.0
                        enriched["lp_adjacent_noise_listener_source_signals"] = []
                        enriched["lp_adjacent_noise_rule_version"] = ""
                        enriched["lp_adjacent_noise_decision_stage"] = ""
                        enriched["lp_adjacent_noise_filtered"] = False
                        enriched["lp_adjacent_noise_reason"] = ""
                        enriched["lp_adjacent_noise_confidence"] = 0.0
                        enriched["lp_adjacent_noise_source_signals"] = []
                        enriched["lp_adjacent_noise_context_used"] = []
                        enriched["lp_adjacent_noise_runtime_context_present"] = False
                        enriched["lp_adjacent_noise_downstream_context_present"] = False
                        await _enqueue(enriched)

            last_block = latest_block

        except Exception as e:
            print("❌ 获取区块失败:", e)
            await asyncio.sleep(2)

        # 轮询间隔，避免对 RPC 过载。
        await asyncio.sleep(0.2)


async def worker(handle_tx):
    """消费者：从队列中取数据并调用处理函数。"""
    while True:
        tx = await tx_queue.get()
        try:
            await handle_tx(tx)
        except Exception as e:
            print("❌ 处理交易失败:", e)
        tx_queue.task_done()
