import asyncio
from collections import defaultdict
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
from filter import WATCH_ADDRESSES
from lp_registry import ACTIVE_LP_POOL_ADDRESSES

# 使用 HTTPProvider 做新区块轮询。
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# 生产者/消费者缓冲队列。
tx_queue = asyncio.Queue(maxsize=5000)
SPILL_FILE = Path("/tmp/whale_tx_queue_spill.ndjson")
QUEUE_STATS = {
    "spill_count": 0,
    "replay_count": 0,
    "replay_skipped_count": 0,
    "replay_error_count": 0,
}

# get_logs 的 topic OR 数量不宜过大，按批次分片查询。
TOPIC_CHUNK_SIZE = 25
MONITORED_TRANSFER_ADDRESSES = WATCH_ADDRESSES | ACTIVE_LP_POOL_ADDRESSES


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


def _extract_touched_watch_addresses(logs):
    """从 Transfer 日志中找出与监控地址相关的地址集合。"""
    touched = set()
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue

        from_addr = _address_from_topic(topics[1])
        to_addr = _address_from_topic(topics[2])
        if from_addr in WATCH_ADDRESSES:
            touched.add(from_addr)
        if to_addr in WATCH_ADDRESSES:
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
        "replay_source": "",
    }


async def _fetch_transfer_logs_for_block(block_num: int):
    """
    在单区块内抓取与监控地址相关的 ERC20 Transfer 日志。
    规则：
    - from 在监控地址内
    - 或 to 在监控地址内
    """
    if not MONITORED_TRANSFER_ADDRESSES:
        return {}

    watch_topics = [_topic_for_address(addr) for addr in MONITORED_TRANSFER_ADDRESSES]
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
                        from_addr in WATCH_ADDRESSES
                        or (to_addr and to_addr in WATCH_ADDRESSES)
                    ):
                        touched_watch_addresses = [
                            addr for addr in [from_addr, to_addr]
                            if addr and addr in WATCH_ADDRESSES
                        ]
                        await _enqueue(_build_eth_candidate(tx, block_num, touched_watch_addresses))

                # 为每个触达地址单独生成候选，方便后续行为建模按地址归因。
                for tx_hash, logs in tx_transfer_logs.items():
                    touched_watch_addresses = _extract_touched_watch_addresses(logs)
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
                        enriched = dict(candidate)
                        enriched["watch_address"] = watch_address
                        enriched["monitor_type"] = "watch_address"
                        await _enqueue(enriched)
                    for pool_address in touched_lp_pools:
                        enriched = dict(candidate)
                        enriched["watch_address"] = pool_address
                        enriched["monitor_type"] = "lp_pool"
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
