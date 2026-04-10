import asyncio
from collections import defaultdict
import hashlib
from itertools import islice
import json
from pathlib import Path
import time

from web3 import Web3

from config import (
    LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE,
    LISTENER_GET_LOGS_DIRECTIONAL_SCAN_ENABLE,
    LISTENER_GET_LOGS_SECONDARY_SIDE_FALLBACK,
    LISTENER_GET_LOGS_SECONDARY_SIDE_REQUIRED_GROUPS,
    LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN,
    LISTENER_SELECTIVE_TX_FETCH_MAX_COUNT,
    LISTENER_TOPIC_CHUNK_SIZE,
    LOW_CU_MODE,
    LP_EXTENDED_SCAN_ENABLE,
    LP_EXTENDED_SCAN_INTERVAL_SEC,
    LP_PRIMARY_TREND_SCAN_INTERVAL_SEC,
    LP_SECONDARY_SCAN_ENABLE,
    LP_SECONDARY_SCAN_INTERVAL_SEC,
    LP_SECONDARY_SCAN_PRIMARY_TREND_ONLY,
    PRODUCER_POLL_INTERVAL_SEC,
    RPC_URL,
    RUNTIME_ADJACENT_CORE_MAX_COUNT,
    RUNTIME_ADJACENT_CORE_SCAN_ENABLE,
    RUNTIME_ADJACENT_SECONDARY_MAX_COUNT,
    RUNTIME_ADJACENT_SECONDARY_SCAN_ENABLE,
    RUNTIME_ADJACENT_SECONDARY_SCAN_INTERVAL_SEC,
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
from lp_registry import (
    ACTIVE_EXTENDED_LP_POOL_ADDRESSES,
    ACTIVE_LP_POOL_ADDRESSES,
    ACTIVE_PRIMARY_TREND_SCAN_LP_POOL_ADDRESSES,
)
from processor import get_token_metadata_stats_snapshot
from rpc_resilience import get_rpc_stats_snapshot, rpc_call_with_backoff
from state_manager import get_runtime_adjacent_watch_addresses, maybe_get_runtime_adjacent_watch_meta

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
    "listener_rpc_mode": "low_cu" if LOW_CU_MODE else "standard",
    "listener_block_fetch_mode": "",
    "listener_block_fetch_reason": "",
    "listener_block_get_logs_request_count": 0,
    "listener_block_topic_chunk_count": 0,
    "listener_block_monitored_address_count": 0,
    "listener_block_lp_secondary_scan_used": False,
    "listener_block_bloom_prefilter_used": False,
    "listener_block_bloom_skipped_get_logs_count": 0,
    "listener_block_bloom_transfer_possible": True,
    "listener_block_bloom_address_possible_count": 0,
    "listener_runtime_adjacent_core_count": 0,
    "listener_runtime_adjacent_secondary_count": 0,
    "listener_runtime_adjacent_secondary_scan_used": False,
    "listener_runtime_adjacent_secondary_skipped_count": 0,
    "listener_block_lp_primary_trend_scan_used": False,
    "listener_block_lp_extended_scan_used": False,
    "listener_block_lp_primary_trend_pool_count": 0,
    "listener_block_lp_extended_pool_count": 0,
    "listener_block_get_logs_primary_side_count": 0,
    "listener_block_get_logs_secondary_side_count": 0,
    "listener_block_get_logs_secondary_side_skipped_count": 0,
    "listener_block_get_logs_empty_response_count": 0,
    "low_cu_mode_enabled": bool(LOW_CU_MODE),
    "low_cu_mode_lp_secondary_only": bool(
        LOW_CU_MODE
        and not LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN
        and LP_SECONDARY_SCAN_ENABLE
    ),
    "low_cu_mode_poll_interval_sec": float(
        max(float(PRODUCER_POLL_INTERVAL_SEC or 1.0), 2.0) if LOW_CU_MODE else float(PRODUCER_POLL_INTERVAL_SEC or 1.0)
    ),
    **get_rpc_stats_snapshot(),
    **get_token_metadata_stats_snapshot(),
}

# get_logs 的 topic OR 数量不宜过大，按批次分片查询。
TOPIC_CHUNK_SIZE = max(int(LISTENER_TOPIC_CHUNK_SIZE or 25), 1)
SECONDARY_SIDE_REQUIRED_SCAN_GROUPS = {
    str(item).strip().lower()
    for item in LISTENER_GET_LOGS_SECONDARY_SIDE_REQUIRED_GROUPS
    if str(item).strip()
}
SCAN_PRIMARY_SIDE_BY_GROUP = {
    "core_watch": "to",
    "runtime_adjacent_core": "from",
    "runtime_adjacent_secondary": "from",
    "lp_primary_trend": "to",
    "lp_extended": "to",
}
HIGH_VALUE_ADJACENT_ANCHOR_ROLE_GROUPS = {"smart_money", "exchange"}


def get_monitored_watch_addresses() -> set[str]:
    return WATCH_ADDRESSES | get_runtime_adjacent_watch_addresses()


def get_core_monitored_addresses() -> set[str]:
    return set(get_monitored_watch_addresses())


def get_lp_monitored_addresses() -> set[str]:
    return set(ACTIVE_LP_POOL_ADDRESSES)


def get_monitored_transfer_addresses(
    *,
    include_active_lp_pools: bool | None = None,
) -> set[str]:
    monitored = set(get_core_monitored_addresses())
    include_lp = (
        bool(LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN)
        if include_active_lp_pools is None
        else bool(include_active_lp_pools)
    )
    if include_lp:
        monitored |= get_lp_monitored_addresses()
    return monitored


def _effective_poll_interval_sec() -> float:
    base_interval = max(float(PRODUCER_POLL_INTERVAL_SEC or 1.0), 0.2)
    if LOW_CU_MODE:
        return max(base_interval, 2.0)
    return base_interval


def _effective_lp_secondary_interval_sec() -> float:
    base_interval = max(float(LP_PRIMARY_TREND_SCAN_INTERVAL_SEC or LP_SECONDARY_SCAN_INTERVAL_SEC or 2.0), 0.5)
    if LOW_CU_MODE:
        return max(base_interval, 12.0)
    return base_interval


def _effective_lp_extended_interval_sec() -> float:
    base_interval = max(float(LP_EXTENDED_SCAN_INTERVAL_SEC or 60.0), 1.0)
    if LOW_CU_MODE:
        return max(base_interval, 60.0)
    return base_interval


def _effective_runtime_adjacent_secondary_interval_sec() -> float:
    base_interval = max(float(RUNTIME_ADJACENT_SECONDARY_SCAN_INTERVAL_SEC or 15.0), 1.0)
    if LOW_CU_MODE:
        return max(base_interval, 15.0)
    return base_interval


def _listener_runtime_defaults() -> dict:
    return {
        "listener_rpc_mode": "low_cu" if LOW_CU_MODE else "standard",
        "listener_block_fetch_mode": "",
        "listener_block_fetch_reason": "",
        "listener_block_get_logs_request_count": 0,
        "listener_block_topic_chunk_count": 0,
        "listener_block_monitored_address_count": 0,
        "listener_block_lp_secondary_scan_used": False,
        "listener_block_bloom_prefilter_used": False,
        "listener_block_bloom_skipped_get_logs_count": 0,
        "listener_block_bloom_transfer_possible": True,
        "listener_block_bloom_address_possible_count": 0,
        "listener_runtime_adjacent_core_count": 0,
        "listener_runtime_adjacent_secondary_count": 0,
        "listener_runtime_adjacent_secondary_scan_used": False,
        "listener_runtime_adjacent_secondary_skipped_count": 0,
        "listener_block_lp_primary_trend_scan_used": False,
        "listener_block_lp_extended_scan_used": False,
        "listener_block_lp_primary_trend_pool_count": 0,
        "listener_block_lp_extended_pool_count": 0,
        "listener_block_get_logs_primary_side_count": 0,
        "listener_block_get_logs_secondary_side_count": 0,
        "listener_block_get_logs_secondary_side_skipped_count": 0,
        "listener_block_get_logs_empty_response_count": 0,
        "low_cu_mode_enabled": bool(LOW_CU_MODE),
        "low_cu_mode_lp_secondary_only": bool(
            LOW_CU_MODE
            and not LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN
            and LP_SECONDARY_SCAN_ENABLE
        ),
        "low_cu_mode_poll_interval_sec": _effective_poll_interval_sec(),
    }


def _listener_runtime_metadata(block_context: dict | None = None) -> dict:
    payload = _listener_runtime_defaults()
    if block_context:
        payload.update(
            {
                "listener_rpc_mode": str(
                    block_context.get("listener_rpc_mode") or payload["listener_rpc_mode"]
                ),
                "listener_block_fetch_mode": str(
                    block_context.get("listener_block_fetch_mode") or ""
                ),
                "listener_block_fetch_reason": str(
                    block_context.get("listener_block_fetch_reason") or ""
                ),
                "listener_block_get_logs_request_count": int(
                    block_context.get("listener_block_get_logs_request_count") or 0
                ),
                "listener_block_topic_chunk_count": int(
                    block_context.get("listener_block_topic_chunk_count") or 0
                ),
                "listener_block_monitored_address_count": int(
                    block_context.get("listener_block_monitored_address_count") or 0
                ),
                "listener_block_lp_secondary_scan_used": bool(
                    block_context.get("listener_block_lp_secondary_scan_used")
                ),
                "listener_block_bloom_prefilter_used": bool(
                    block_context.get("listener_block_bloom_prefilter_used")
                ),
                "listener_block_bloom_skipped_get_logs_count": int(
                    block_context.get("listener_block_bloom_skipped_get_logs_count") or 0
                ),
                "listener_block_bloom_transfer_possible": bool(
                    block_context.get("listener_block_bloom_transfer_possible")
                    if "listener_block_bloom_transfer_possible" in block_context
                    else payload["listener_block_bloom_transfer_possible"]
                ),
                "listener_block_bloom_address_possible_count": int(
                    block_context.get("listener_block_bloom_address_possible_count") or 0
                ),
                "listener_runtime_adjacent_core_count": int(
                    block_context.get("listener_runtime_adjacent_core_count") or 0
                ),
                "listener_runtime_adjacent_secondary_count": int(
                    block_context.get("listener_runtime_adjacent_secondary_count") or 0
                ),
                "listener_runtime_adjacent_secondary_scan_used": bool(
                    block_context.get("listener_runtime_adjacent_secondary_scan_used")
                ),
                "listener_runtime_adjacent_secondary_skipped_count": int(
                    block_context.get("listener_runtime_adjacent_secondary_skipped_count") or 0
                ),
                "listener_block_lp_primary_trend_scan_used": bool(
                    block_context.get("listener_block_lp_primary_trend_scan_used")
                ),
                "listener_block_lp_extended_scan_used": bool(
                    block_context.get("listener_block_lp_extended_scan_used")
                ),
                "listener_block_lp_primary_trend_pool_count": int(
                    block_context.get("listener_block_lp_primary_trend_pool_count") or 0
                ),
                "listener_block_lp_extended_pool_count": int(
                    block_context.get("listener_block_lp_extended_pool_count") or 0
                ),
                "listener_block_get_logs_primary_side_count": int(
                    block_context.get("listener_block_get_logs_primary_side_count") or 0
                ),
                "listener_block_get_logs_secondary_side_count": int(
                    block_context.get("listener_block_get_logs_secondary_side_count") or 0
                ),
                "listener_block_get_logs_secondary_side_skipped_count": int(
                    block_context.get("listener_block_get_logs_secondary_side_skipped_count") or 0
                ),
                "listener_block_get_logs_empty_response_count": int(
                    block_context.get("listener_block_get_logs_empty_response_count") or 0
                ),
                "low_cu_mode_enabled": bool(
                    block_context.get("low_cu_mode_enabled")
                    if "low_cu_mode_enabled" in block_context
                    else payload["low_cu_mode_enabled"]
                ),
                "low_cu_mode_lp_secondary_only": bool(
                    block_context.get("low_cu_mode_lp_secondary_only")
                    if "low_cu_mode_lp_secondary_only" in block_context
                    else payload["low_cu_mode_lp_secondary_only"]
                ),
                "low_cu_mode_poll_interval_sec": float(
                    block_context.get("low_cu_mode_poll_interval_sec")
                    or payload["low_cu_mode_poll_interval_sec"]
                ),
            }
        )
    return payload


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


def _normalize_scan_group_name(value: str | None) -> str:
    return str(value or "").strip().lower()


def _scan_group_primary_side(scan_group: str | None) -> str:
    side = SCAN_PRIMARY_SIDE_BY_GROUP.get(_normalize_scan_group_name(scan_group), "to")
    return side if side in {"from", "to"} else "to"


def _scan_group_secondary_side(scan_group: str | None) -> str:
    return "from" if _scan_group_primary_side(scan_group) == "to" else "to"


def _scan_group_requires_secondary_side(scan_group: str | None) -> bool:
    if not bool(LISTENER_GET_LOGS_DIRECTIONAL_SCAN_ENABLE):
        return True
    if not bool(LISTENER_GET_LOGS_SECONDARY_SIDE_FALLBACK):
        return False
    return _normalize_scan_group_name(scan_group) in SECONDARY_SIDE_REQUIRED_SCAN_GROUPS


def _hex_bytes(value) -> bytes:
    text = _to_hex(value)
    if text.startswith("0x"):
        text = text[2:]
    if not text:
        return b""
    if len(text) % 2 == 1:
        text = f"0{text}"
    try:
        return bytes.fromhex(text)
    except ValueError:
        return b""


def _extract_block_logs_bloom_bytes(block) -> bytes | None:
    if not bool(LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE) or block is None:
        return None
    bloom = None
    try:
        bloom = block.get("logsBloom")
    except Exception:
        bloom = getattr(block, "logsBloom", None)
    if bloom is None:
        bloom = getattr(block, "logsBloom", None)
    data = _hex_bytes(bloom)
    return data if len(data) == 256 else None


def _bloom_maybe_contains_value(block_bloom: bytes | None, value) -> bool:
    if not bool(LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE) or not block_bloom:
        return True
    data = _hex_bytes(value)
    if not data:
        return True
    digest = Web3.keccak(data)
    for offset in (0, 2, 4):
        bit_index = ((digest[offset] << 8) | digest[offset + 1]) & 2047
        byte_index = 255 - (bit_index >> 3)
        mask = 1 << (bit_index & 0x07)
        if block_bloom[byte_index] & mask != mask:
            return False
    return True


def _bloom_maybe_contains_topic(block_bloom: bytes | None, topic: str | None) -> bool:
    return _bloom_maybe_contains_value(block_bloom, topic)


def _bloom_maybe_contains_any_topic(block_bloom: bytes | None, topics: list[str]) -> tuple[bool, int]:
    candidates = [topic for topic in topics if topic]
    if not bool(LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE) or not block_bloom:
        return bool(candidates), len(candidates)
    possible_count = sum(1 for topic in candidates if _bloom_maybe_contains_topic(block_bloom, topic))
    return possible_count > 0, int(possible_count)


def _block_transfer_bloom_possible(block_bloom: bytes | None) -> bool:
    return _bloom_maybe_contains_topic(block_bloom, ERC20_TRANSFER_EVENT_SIG)


def _should_issue_get_logs_for_chunk(
    *,
    block_bloom: bytes | None,
    topic_chunk: list[str],
    block_transfer_possible: bool | None = None,
) -> dict:
    if not bool(LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE) or not block_bloom:
        return {
            "prefilter_used": False,
            "should_issue": True,
            "transfer_possible": True if block_transfer_possible is None else bool(block_transfer_possible),
            "address_possible_count": len([topic for topic in topic_chunk if topic]),
        }

    transfer_possible = (
        bool(block_transfer_possible)
        if block_transfer_possible is not None
        else _block_transfer_bloom_possible(block_bloom)
    )
    if not transfer_possible:
        return {
            "prefilter_used": True,
            "should_issue": False,
            "transfer_possible": False,
            "address_possible_count": 0,
        }

    address_possible, address_possible_count = _bloom_maybe_contains_any_topic(block_bloom, topic_chunk)
    return {
        "prefilter_used": True,
        "should_issue": bool(address_possible),
        "transfer_possible": True,
        "address_possible_count": int(address_possible_count),
    }


def _query_filter_for_side(block_num: int, topic_chunk: list[str], side: str) -> dict:
    if side == "from":
        topics = [ERC20_TRANSFER_EVENT_SIG, topic_chunk, None]
    else:
        topics = [ERC20_TRANSFER_EVENT_SIG, None, topic_chunk]
    return {
        "fromBlock": block_num,
        "toBlock": block_num,
        "topics": topics,
    }


def _append_grouped_logs(grouped: dict[str, list[dict]], seen: set[tuple[str, int]], logs) -> None:
    for log in logs or []:
        tx_hash = _to_hex(log.get("transactionHash")).lower()
        log_index = int(log.get("logIndex", 0)) if log.get("logIndex") is not None else 0
        dedup_key = (tx_hash, log_index)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        grouped[tx_hash].append(_compact_log(log))


def _empty_transfer_scan_stats(scan_label: str) -> dict:
    return {
        "request_count": 0,
        "topic_chunk_count": 0,
        "monitored_address_count": 0,
        "failure_count": 0,
        "scan_label": str(scan_label or ""),
        "listener_block_bloom_prefilter_used": False,
        "listener_block_bloom_skipped_get_logs_count": 0,
        "listener_block_bloom_transfer_possible": True,
        "listener_block_bloom_address_possible_count": 0,
        "listener_block_get_logs_primary_side_count": 0,
        "listener_block_get_logs_secondary_side_count": 0,
        "listener_block_get_logs_secondary_side_skipped_count": 0,
        "listener_block_get_logs_empty_response_count": 0,
    }


def _merge_transfer_scan_stats(base_stats: dict, extra_stats: dict) -> dict:
    base = dict(base_stats or {})
    extra = dict(extra_stats or {})
    base["request_count"] = int(base.get("request_count") or 0) + int(extra.get("request_count") or 0)
    base["topic_chunk_count"] = int(base.get("topic_chunk_count") or 0) + int(extra.get("topic_chunk_count") or 0)
    base["monitored_address_count"] = int(base.get("monitored_address_count") or 0) + int(extra.get("monitored_address_count") or 0)
    base["failure_count"] = int(base.get("failure_count") or 0) + int(extra.get("failure_count") or 0)
    base["listener_block_bloom_prefilter_used"] = bool(
        base.get("listener_block_bloom_prefilter_used")
        or extra.get("listener_block_bloom_prefilter_used")
    )
    base["listener_block_bloom_skipped_get_logs_count"] = int(
        base.get("listener_block_bloom_skipped_get_logs_count") or 0
    ) + int(extra.get("listener_block_bloom_skipped_get_logs_count") or 0)
    base["listener_block_bloom_transfer_possible"] = bool(
        base.get("listener_block_bloom_transfer_possible", True)
        and extra.get("listener_block_bloom_transfer_possible", True)
    )
    base["listener_block_bloom_address_possible_count"] = int(
        base.get("listener_block_bloom_address_possible_count") or 0
    ) + int(extra.get("listener_block_bloom_address_possible_count") or 0)
    base["listener_block_get_logs_primary_side_count"] = int(
        base.get("listener_block_get_logs_primary_side_count") or 0
    ) + int(extra.get("listener_block_get_logs_primary_side_count") or 0)
    base["listener_block_get_logs_secondary_side_count"] = int(
        base.get("listener_block_get_logs_secondary_side_count") or 0
    ) + int(extra.get("listener_block_get_logs_secondary_side_count") or 0)
    base["listener_block_get_logs_secondary_side_skipped_count"] = int(
        base.get("listener_block_get_logs_secondary_side_skipped_count") or 0
    ) + int(extra.get("listener_block_get_logs_secondary_side_skipped_count") or 0)
    base["listener_block_get_logs_empty_response_count"] = int(
        base.get("listener_block_get_logs_empty_response_count") or 0
    ) + int(extra.get("listener_block_get_logs_empty_response_count") or 0)
    return base


def _runtime_adjacent_priority_score(meta: dict | None) -> float:
    meta = dict(meta or {})
    score = 0.0
    runtime_state = str(meta.get("runtime_state") or "")
    priority = int(meta.get("priority") or 3)
    anchor_usd_value = float(meta.get("anchor_usd_value") or 0.0)
    observed_count = int(meta.get("observed_count") or 0)
    emitted_count = int(meta.get("emitted_notification_count") or 0)
    strategy_hint = str(meta.get("strategy_hint") or "")
    anchor_role_group = str(strategy_role_group(meta.get("anchor_strategy_role") or ""))

    if runtime_state == "active":
        score += 2.0
    if priority <= 2:
        score += 1.4
    if strategy_hint == "persisted_exchange_adjacent":
        score += 2.0
    if anchor_role_group in HIGH_VALUE_ADJACENT_ANCHOR_ROLE_GROUPS:
        score += 1.4
    if str(meta.get("downstream_case_id") or ""):
        score += 1.3
    if bool(meta.get("runtime_adjacent_anchor_flag")):
        score += 1.0
    if anchor_usd_value >= 500_000.0:
        score += 2.0
    elif anchor_usd_value >= 150_000.0:
        score += 1.0
    if observed_count > 0:
        score += min(2.0, observed_count * 0.4)
    if emitted_count > 0:
        score += 1.2
    return round(score, 3)


def _is_runtime_adjacent_core(meta: dict | None) -> bool:
    meta = dict(meta or {})
    if not bool(RUNTIME_ADJACENT_CORE_SCAN_ENABLE):
        return False
    if str(meta.get("runtime_state") or "") != "active":
        return False

    priority = int(meta.get("priority") or 3)
    anchor_usd_value = float(meta.get("anchor_usd_value") or 0.0)
    observed_count = int(meta.get("observed_count") or 0)
    emitted_count = int(meta.get("emitted_notification_count") or 0)
    strategy_hint = str(meta.get("strategy_hint") or "")
    anchor_role_group = str(strategy_role_group(meta.get("anchor_strategy_role") or ""))

    return bool(
        strategy_hint == "persisted_exchange_adjacent"
        or str(meta.get("downstream_case_id") or "")
        or emitted_count > 0
        or observed_count > 0
        or anchor_usd_value >= 150_000.0
        or priority <= 2
        or anchor_role_group in HIGH_VALUE_ADJACENT_ANCHOR_ROLE_GROUPS
        or _runtime_adjacent_priority_score(meta) >= 3.5
    )


def _runtime_adjacent_sort_key(item: dict) -> tuple:
    return (
        -float(item.get("scan_priority_score") or 0.0),
        int(item.get("priority") or 3),
        -float(item.get("anchor_usd_value") or 0.0),
        -int(item.get("observed_count") or 0),
        -int(item.get("emitted_notification_count") or 0),
        -int(item.get("last_seen_ts") or 0),
        str(item.get("address") or ""),
    )


def _should_run_runtime_adjacent_secondary_scan(last_scan_ts: float, *, secondary_available: bool) -> tuple[bool, str]:
    if not secondary_available:
        return False, "runtime_adjacent_secondary_empty"
    if not bool(RUNTIME_ADJACENT_SECONDARY_SCAN_ENABLE):
        return False, "runtime_adjacent_secondary_disabled"
    if LOW_CU_MODE and tx_queue.qsize() >= int(SPILL_REPLAY_QUEUE_WATERMARK or 0):
        return False, "runtime_adjacent_secondary_queue_pressure"
    now_ts = time.time()
    if now_ts - float(last_scan_ts or 0.0) < _effective_runtime_adjacent_secondary_interval_sec():
        return False, "runtime_adjacent_secondary_interval_throttled"
    return True, "runtime_adjacent_secondary_scheduled"


def _build_runtime_adjacent_scan_plan(
    *,
    now_ts: int | None = None,
    last_secondary_scan_ts: float = 0.0,
) -> dict:
    reference_ts = int(now_ts or time.time())
    active_addresses = sorted(get_runtime_adjacent_watch_addresses(now_ts=reference_ts))
    core_entries = []
    secondary_entries = []

    for address in active_addresses:
        if address in WATCH_ADDRESSES:
            continue
        try:
            meta = dict(maybe_get_runtime_adjacent_watch_meta(address, now_ts=reference_ts) or {})
        except Exception as e:
            print(f"[listener] runtime adjacent scan skipped address={str(address or '').lower()} detail={e}")
            continue
        if not meta:
            continue
        meta["address"] = str(address or "").lower()
        meta["scan_priority_score"] = _runtime_adjacent_priority_score(meta)
        if _is_runtime_adjacent_core(meta):
            core_entries.append(meta)
        else:
            secondary_entries.append(meta)

    core_entries.sort(key=_runtime_adjacent_sort_key)
    secondary_entries.sort(key=_runtime_adjacent_sort_key)

    if not bool(RUNTIME_ADJACENT_CORE_SCAN_ENABLE):
        secondary_entries = core_entries + secondary_entries
        core_entries = []

    core_limit = max(int(RUNTIME_ADJACENT_CORE_MAX_COUNT or 0), 0)
    secondary_limit = max(int(RUNTIME_ADJACENT_SECONDARY_MAX_COUNT or 0), 0)
    selected_core_entries = core_entries[:core_limit] if core_limit else []
    selected_secondary_entries = secondary_entries[:secondary_limit] if secondary_limit else []

    secondary_scan_used, secondary_reason = _should_run_runtime_adjacent_secondary_scan(
        last_secondary_scan_ts,
        secondary_available=bool(selected_secondary_entries),
    )
    scanned_secondary_entries = selected_secondary_entries if secondary_scan_used else []

    return {
        "core_addresses": [item["address"] for item in selected_core_entries],
        "secondary_addresses": [item["address"] for item in scanned_secondary_entries],
        "selected_secondary_addresses": [item["address"] for item in selected_secondary_entries],
        "core_count": len(selected_core_entries),
        "secondary_count": len(selected_secondary_entries),
        "secondary_scan_used": bool(secondary_scan_used and scanned_secondary_entries),
        "secondary_reason": secondary_reason,
        "secondary_skipped_count": max(
            len(secondary_entries) - (len(scanned_secondary_entries) if secondary_scan_used else 0),
            0,
        ),
    }


def _lp_primary_trend_scan_addresses() -> list[str]:
    if not bool(LP_SECONDARY_SCAN_PRIMARY_TREND_ONLY):
        return sorted(ACTIVE_LP_POOL_ADDRESSES)
    return sorted(ACTIVE_PRIMARY_TREND_SCAN_LP_POOL_ADDRESSES)


def _lp_extended_scan_addresses() -> list[str]:
    if not bool(LP_SECONDARY_SCAN_PRIMARY_TREND_ONLY):
        return []
    return sorted(ACTIVE_EXTENDED_LP_POOL_ADDRESSES)


def _should_run_lp_primary_trend_scan(last_scan_ts: float) -> tuple[bool, str]:
    if not bool(LP_SECONDARY_SCAN_ENABLE):
        return False, "lp_secondary_disabled"
    if bool(LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN):
        return False, "lp_included_in_main_scan"
    if LOW_CU_MODE and tx_queue.qsize() >= int(SPILL_REPLAY_QUEUE_WATERMARK or 0):
        return False, "lp_primary_trend_queue_pressure"
    if not _lp_primary_trend_scan_addresses():
        return False, "lp_primary_trend_empty"
    now_ts = time.time()
    if now_ts - float(last_scan_ts or 0.0) < _effective_lp_secondary_interval_sec():
        return False, "lp_primary_trend_interval_throttled"
    return True, "lp_primary_trend_scheduled"


def _should_run_lp_extended_scan(last_scan_ts: float) -> tuple[bool, str]:
    if not bool(LP_SECONDARY_SCAN_ENABLE):
        return False, "lp_secondary_disabled"
    if bool(LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN):
        return False, "lp_included_in_main_scan"
    if not bool(LP_EXTENDED_SCAN_ENABLE):
        return False, "lp_extended_disabled"
    if LOW_CU_MODE and tx_queue.qsize() >= int(SPILL_REPLAY_QUEUE_WATERMARK or 0):
        return False, "lp_extended_queue_pressure"
    if not _lp_extended_scan_addresses():
        return False, "lp_extended_empty"
    now_ts = time.time()
    if now_ts - float(last_scan_ts or 0.0) < _effective_lp_extended_interval_sec():
        return False, "lp_extended_interval_throttled"
    return True, "lp_extended_scheduled"


def _build_lp_scan_plan(
    *,
    last_primary_scan_ts: float = 0.0,
    last_extended_scan_ts: float = 0.0,
    include_active_lp_pools: bool = False,
) -> dict:
    primary_addresses = _lp_primary_trend_scan_addresses()
    extended_addresses = _lp_extended_scan_addresses()

    if include_active_lp_pools:
        return {
            "primary_addresses": primary_addresses,
            "extended_addresses": extended_addresses,
            "primary_scan_used": bool(primary_addresses),
            "extended_scan_used": bool(extended_addresses),
            "secondary_scan_used": False,
            "primary_reason": "lp_included_in_main_scan",
            "extended_reason": "lp_included_in_main_scan",
        }

    primary_scan_used, primary_reason = _should_run_lp_primary_trend_scan(last_primary_scan_ts)
    extended_scan_used, extended_reason = _should_run_lp_extended_scan(last_extended_scan_ts)
    primary_scan_used = bool(primary_scan_used and primary_addresses)
    extended_scan_used = bool(extended_scan_used and extended_addresses)

    return {
        "primary_addresses": primary_addresses,
        "extended_addresses": extended_addresses,
        "primary_scan_used": primary_scan_used,
        "extended_scan_used": extended_scan_used,
        "secondary_scan_used": bool(primary_scan_used or extended_scan_used),
        "primary_reason": primary_reason,
        "extended_reason": extended_reason,
    }


def _build_transfer_scan_groups(
    *,
    runtime_adjacent_plan: dict,
    lp_scan_plan: dict,
    include_active_lp_pools: bool = False,
) -> list[dict]:
    groups = []
    if WATCH_ADDRESSES:
        groups.append({
            "group_name": "core_watch",
            "scan_label": "core_watch",
            "addresses": sorted(WATCH_ADDRESSES),
        })
    if runtime_adjacent_plan.get("core_addresses"):
        groups.append({
            "group_name": "runtime_adjacent_core",
            "scan_label": "runtime_adjacent_core",
            "addresses": list(runtime_adjacent_plan.get("core_addresses") or []),
        })
    if runtime_adjacent_plan.get("secondary_scan_used") and runtime_adjacent_plan.get("secondary_addresses"):
        groups.append({
            "group_name": "runtime_adjacent_secondary",
            "scan_label": "runtime_adjacent_secondary",
            "addresses": list(runtime_adjacent_plan.get("secondary_addresses") or []),
        })

    if include_active_lp_pools:
        if lp_scan_plan.get("primary_addresses"):
            groups.append({
                "group_name": "lp_primary_trend",
                "scan_label": "lp_primary_trend",
                "addresses": list(lp_scan_plan.get("primary_addresses") or []),
            })
        if lp_scan_plan.get("extended_addresses"):
            groups.append({
                "group_name": "lp_extended",
                "scan_label": "lp_extended",
                "addresses": list(lp_scan_plan.get("extended_addresses") or []),
            })
        return groups

    if lp_scan_plan.get("primary_scan_used") and lp_scan_plan.get("primary_addresses"):
        groups.append({
            "group_name": "lp_primary_trend",
            "scan_label": "lp_primary_trend",
            "addresses": list(lp_scan_plan.get("primary_addresses") or []),
        })
    if lp_scan_plan.get("extended_scan_used") and lp_scan_plan.get("extended_addresses"):
        groups.append({
            "group_name": "lp_extended",
            "scan_label": "lp_extended",
            "addresses": list(lp_scan_plan.get("extended_addresses") or []),
        })
    return groups


def _build_eth_candidate(tx, block_num: int, touched_watch_addresses: list[str]) -> dict:
    payload = {
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
    payload.update(_listener_runtime_defaults())
    return payload


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
    payload = {
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
    payload.update(_listener_runtime_defaults())
    return payload


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
        "listener_rpc_mode": str(candidate.get("listener_rpc_mode") or ""),
        "listener_block_fetch_mode": str(candidate.get("listener_block_fetch_mode") or ""),
        "listener_block_fetch_reason": str(candidate.get("listener_block_fetch_reason") or ""),
        "listener_block_get_logs_request_count": int(
            candidate.get("listener_block_get_logs_request_count") or 0
        ),
        "listener_block_topic_chunk_count": int(
            candidate.get("listener_block_topic_chunk_count") or 0
        ),
        "listener_block_monitored_address_count": int(
            candidate.get("listener_block_monitored_address_count") or 0
        ),
        "listener_block_lp_secondary_scan_used": bool(
            candidate.get("listener_block_lp_secondary_scan_used")
        ),
        "listener_block_bloom_prefilter_used": bool(
            candidate.get("listener_block_bloom_prefilter_used")
        ),
        "listener_block_bloom_skipped_get_logs_count": int(
            candidate.get("listener_block_bloom_skipped_get_logs_count") or 0
        ),
        "listener_block_bloom_transfer_possible": bool(
            candidate.get("listener_block_bloom_transfer_possible")
            if "listener_block_bloom_transfer_possible" in candidate
            else True
        ),
        "listener_block_bloom_address_possible_count": int(
            candidate.get("listener_block_bloom_address_possible_count") or 0
        ),
        "listener_runtime_adjacent_core_count": int(
            candidate.get("listener_runtime_adjacent_core_count") or 0
        ),
        "listener_runtime_adjacent_secondary_count": int(
            candidate.get("listener_runtime_adjacent_secondary_count") or 0
        ),
        "listener_runtime_adjacent_secondary_scan_used": bool(
            candidate.get("listener_runtime_adjacent_secondary_scan_used")
        ),
        "listener_runtime_adjacent_secondary_skipped_count": int(
            candidate.get("listener_runtime_adjacent_secondary_skipped_count") or 0
        ),
        "listener_block_lp_primary_trend_scan_used": bool(
            candidate.get("listener_block_lp_primary_trend_scan_used")
        ),
        "listener_block_lp_extended_scan_used": bool(
            candidate.get("listener_block_lp_extended_scan_used")
        ),
        "listener_block_lp_primary_trend_pool_count": int(
            candidate.get("listener_block_lp_primary_trend_pool_count") or 0
        ),
        "listener_block_lp_extended_pool_count": int(
            candidate.get("listener_block_lp_extended_pool_count") or 0
        ),
        "listener_block_get_logs_primary_side_count": int(
            candidate.get("listener_block_get_logs_primary_side_count") or 0
        ),
        "listener_block_get_logs_secondary_side_count": int(
            candidate.get("listener_block_get_logs_secondary_side_count") or 0
        ),
        "listener_block_get_logs_secondary_side_skipped_count": int(
            candidate.get("listener_block_get_logs_secondary_side_skipped_count") or 0
        ),
        "listener_block_get_logs_empty_response_count": int(
            candidate.get("listener_block_get_logs_empty_response_count") or 0
        ),
        "low_cu_mode_enabled": bool(candidate.get("low_cu_mode_enabled")),
        "low_cu_mode_lp_secondary_only": bool(
            candidate.get("low_cu_mode_lp_secondary_only")
        ),
        "low_cu_mode_poll_interval_sec": round(
            float(candidate.get("low_cu_mode_poll_interval_sec") or 0.0), 3
        ),
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


def _rpc_get_block_number() -> int | None:
    return rpc_call_with_backoff(
        "listener.block_number",
        lambda: w3.eth.block_number,
        return_none_on_failure=True,
    )


def _rpc_get_block(block_num: int, *, full_transactions: bool):
    return rpc_call_with_backoff(
        f"listener.get_block.{ 'full' if full_transactions else 'header' }",
        lambda: w3.eth.get_block(block_num, full_transactions=full_transactions),
        return_none_on_failure=True,
    )


def _rpc_get_transaction(tx_hash: str):
    return rpc_call_with_backoff(
        "listener.get_transaction",
        lambda: w3.eth.get_transaction(tx_hash),
        return_none_on_failure=True,
    )


async def _fetch_transfer_logs_for_block_for_addresses(
    block_num: int,
    monitored_addresses: set[str],
    *,
    scan_label: str,
    scan_group: str,
    block_bloom: bytes | None = None,
    block_transfer_possible: bool | None = None,
) -> tuple[dict[str, list[dict]], dict]:
    stats = _empty_transfer_scan_stats(scan_label)
    stats["monitored_address_count"] = len(monitored_addresses or [])
    stats["listener_block_bloom_prefilter_used"] = bool(
        LISTENER_BLOCK_BLOOM_PREFILTER_ENABLE and block_bloom
    )
    stats["listener_block_bloom_transfer_possible"] = bool(
        True if block_transfer_possible is None else block_transfer_possible
    )

    if not monitored_addresses:
        return {}, stats

    watch_topics = [_topic_for_address(addr) for addr in sorted(monitored_addresses)]
    grouped: dict[str, list[dict]] = defaultdict(list)
    seen = set()
    primary_side = _scan_group_primary_side(scan_group)
    secondary_side = _scan_group_secondary_side(scan_group)
    query_secondary_side = _scan_group_requires_secondary_side(scan_group)

    for topic_chunk in _chunked(watch_topics, TOPIC_CHUNK_SIZE):
        stats["topic_chunk_count"] = int(stats.get("topic_chunk_count") or 0) + 1
        bloom_decision = _should_issue_get_logs_for_chunk(
            block_bloom=block_bloom,
            topic_chunk=topic_chunk,
            block_transfer_possible=block_transfer_possible,
        )
        stats["listener_block_bloom_prefilter_used"] = bool(
            stats.get("listener_block_bloom_prefilter_used")
            or bloom_decision.get("prefilter_used")
        )
        stats["listener_block_bloom_transfer_possible"] = bool(
            bloom_decision.get("transfer_possible")
            if "transfer_possible" in bloom_decision
            else stats.get("listener_block_bloom_transfer_possible", True)
        )
        stats["listener_block_bloom_address_possible_count"] = int(
            stats.get("listener_block_bloom_address_possible_count") or 0
        ) + int(bloom_decision.get("address_possible_count") or 0)

        if not bool(bloom_decision.get("should_issue", True)):
            skipped_request_count = 1 + (1 if query_secondary_side else 0)
            stats["listener_block_bloom_skipped_get_logs_count"] = int(
                stats.get("listener_block_bloom_skipped_get_logs_count") or 0
            ) + skipped_request_count
            continue

        for side in [primary_side, secondary_side if query_secondary_side else None]:
            if side is None:
                continue
            query_filter = _query_filter_for_side(block_num, topic_chunk, side)
            stats["request_count"] = int(stats.get("request_count") or 0) + 1
            if side == primary_side:
                stats["listener_block_get_logs_primary_side_count"] = int(
                    stats.get("listener_block_get_logs_primary_side_count") or 0
                ) + 1
            else:
                stats["listener_block_get_logs_secondary_side_count"] = int(
                    stats.get("listener_block_get_logs_secondary_side_count") or 0
                ) + 1

            logs = await asyncio.to_thread(
                rpc_call_with_backoff,
                f"listener.get_logs.{scan_label}.{side}",
                lambda query_filter=query_filter: w3.eth.get_logs(query_filter),
                return_none_on_failure=True,
            )
            if logs is None:
                stats["failure_count"] = int(stats.get("failure_count") or 0) + 1
                continue
            if not logs:
                stats["listener_block_get_logs_empty_response_count"] = int(
                    stats.get("listener_block_get_logs_empty_response_count") or 0
                ) + 1
                continue
            _append_grouped_logs(grouped, seen, logs)

        if not query_secondary_side:
            stats["listener_block_get_logs_secondary_side_skipped_count"] = int(
                stats.get("listener_block_get_logs_secondary_side_skipped_count") or 0
            ) + 1

    for tx_hash in grouped:
        grouped[tx_hash].sort(key=lambda item: item["logIndex"])

    return dict(grouped), stats


async def _fetch_transfer_logs_for_scan_groups(
    block_num: int,
    scan_groups: list[dict],
    *,
    block_bloom: bytes | None = None,
    block_transfer_possible: bool | None = None,
) -> tuple[dict[str, list[dict]], dict]:
    grouped: dict[str, list[dict]] = {}
    total_stats = _empty_transfer_scan_stats("block")

    for group in scan_groups:
        group_logs, group_stats = await _fetch_transfer_logs_for_block_for_addresses(
            block_num,
            set(group.get("addresses") or []),
            scan_label=str(group.get("scan_label") or group.get("group_name") or "scan"),
            scan_group=str(group.get("group_name") or group.get("scan_label") or "scan"),
            block_bloom=block_bloom,
            block_transfer_possible=block_transfer_possible,
        )
        grouped = _merge_grouped_logs(grouped, group_logs)
        total_stats = _merge_transfer_scan_stats(total_stats, group_stats)

    return grouped, total_stats


async def _fetch_transfer_logs_for_block(block_num: int):
    scan_groups = [{
        "group_name": "core_watch",
        "scan_label": "core_watch",
        "addresses": sorted(get_monitored_transfer_addresses()),
    }]
    return await _fetch_transfer_logs_for_scan_groups(
        block_num,
        scan_groups,
        block_bloom=None,
        block_transfer_possible=True,
    )


def _merge_grouped_logs(*groups: dict[str, list[dict]]) -> dict[str, list[dict]]:
    merged: dict[str, list[dict]] = defaultdict(list)
    seen = set()
    for group in groups:
        for tx_hash, logs in dict(group or {}).items():
            for log in logs or []:
                log_index = int(log.get("logIndex", 0)) if log.get("logIndex") is not None else 0
                dedup_key = (str(tx_hash or "").lower(), log_index)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                merged[str(tx_hash or "").lower()].append(dict(log))
    for tx_hash in merged:
        merged[tx_hash].sort(key=lambda item: item["logIndex"])
    return dict(merged)


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


def _update_listener_runtime_stats(block_context: dict) -> None:
    QUEUE_STATS.update(_listener_runtime_metadata(block_context))
    QUEUE_STATS.update(get_rpc_stats_snapshot())
    QUEUE_STATS.update(get_token_metadata_stats_snapshot())


async def producer():
    """生产者：拉取新区块并把候选交易放入队列。"""
    print("🚀 启动 Listener...")
    effective_poll_interval = _effective_poll_interval_sec()
    last_block = await asyncio.to_thread(_rpc_get_block_number)
    while last_block is None:
        QUEUE_STATS.update(get_rpc_stats_snapshot())
        await asyncio.sleep(effective_poll_interval)
        last_block = await asyncio.to_thread(_rpc_get_block_number)

    last_runtime_adjacent_secondary_scan_ts = 0.0
    last_lp_primary_trend_scan_ts = 0.0
    last_lp_extended_scan_ts = 0.0

    while True:
        try:
            latest_block = await asyncio.to_thread(_rpc_get_block_number)
            QUEUE_STATS.update(get_rpc_stats_snapshot())
            if latest_block is None:
                await asyncio.sleep(effective_poll_interval)
                continue

            processed_until = last_block

            for block_num in range(last_block + 1, latest_block + 1):
                now_ts = int(time.time())
                runtime_adjacent_plan = _build_runtime_adjacent_scan_plan(
                    now_ts=now_ts,
                    last_secondary_scan_ts=last_runtime_adjacent_secondary_scan_ts,
                )
                lp_scan_plan = _build_lp_scan_plan(
                    last_primary_scan_ts=last_lp_primary_trend_scan_ts,
                    last_extended_scan_ts=last_lp_extended_scan_ts,
                    include_active_lp_pools=bool(LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN),
                )

                block_monitored_watch_addresses = set(WATCH_ADDRESSES) | set(
                    runtime_adjacent_plan.get("core_addresses") or []
                )
                if runtime_adjacent_plan.get("secondary_scan_used"):
                    block_monitored_watch_addresses |= set(
                        runtime_adjacent_plan.get("secondary_addresses") or []
                    )

                scan_groups = _build_transfer_scan_groups(
                    runtime_adjacent_plan=runtime_adjacent_plan,
                    lp_scan_plan=lp_scan_plan,
                    include_active_lp_pools=bool(LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN),
                )

                block_transactions = []
                block_tx_hashes = []
                block_obj = None
                if LOW_CU_MODE:
                    block_obj = await asyncio.to_thread(
                        _rpc_get_block,
                        block_num,
                        full_transactions=False,
                    )
                    if block_obj is None:
                        break
                    block_tx_hashes = [
                        _to_hex(tx_hash).lower()
                        for tx_hash in list(block_obj.transactions or [])
                    ]
                else:
                    block_obj = await asyncio.to_thread(
                        _rpc_get_block,
                        block_num,
                        full_transactions=True,
                    )
                    if block_obj is None:
                        break
                    block_transactions = list(block_obj.transactions or [])
                    block_tx_hashes = [
                        _to_hex(tx.get("hash")).lower()
                        for tx in block_transactions
                    ]

                block_bloom = _extract_block_logs_bloom_bytes(block_obj)
                block_transfer_possible = _block_transfer_bloom_possible(block_bloom)

                tx_transfer_logs, scan_stats = await _fetch_transfer_logs_for_scan_groups(
                    block_num,
                    scan_groups,
                    block_bloom=block_bloom,
                    block_transfer_possible=block_transfer_possible,
                )
                if int(scan_stats.get("failure_count") or 0) > 0:
                    break

                if runtime_adjacent_plan.get("secondary_scan_used"):
                    last_runtime_adjacent_secondary_scan_ts = time.time()
                if (
                    lp_scan_plan.get("primary_scan_used")
                    and not LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN
                ):
                    last_lp_primary_trend_scan_ts = time.time()
                if (
                    lp_scan_plan.get("extended_scan_used")
                    and not LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN
                ):
                    last_lp_extended_scan_ts = time.time()

                token_flow_tx_hashes = set(tx_transfer_logs.keys())

                block_fetch_mode = "full_transactions_block_fetch"
                block_fetch_reason = "standard_mode_native_eth_full_scan"
                if LOW_CU_MODE:
                    block_fetch_mode = "header_only_then_selective_tx_fetch"
                    selective_tx_hashes = [
                        tx_hash
                        for tx_hash in block_tx_hashes
                        if tx_hash and tx_hash not in token_flow_tx_hashes
                    ]
                    if not selective_tx_hashes:
                        block_fetch_reason = "low_cu_skip_native_eth_scan_token_flow_only"
                    elif len(selective_tx_hashes) <= max(int(LISTENER_SELECTIVE_TX_FETCH_MAX_COUNT or 0), 1):
                        block_fetch_reason = "low_cu_selective_native_eth_scan"
                        for tx_hash in selective_tx_hashes:
                            tx = await asyncio.to_thread(_rpc_get_transaction, tx_hash)
                            if tx is not None:
                                block_transactions.append(tx)
                    else:
                        block_fetch_reason = "low_cu_skip_native_eth_scan_high_tx_count"

                block_context = _listener_runtime_metadata(
                    {
                        "listener_rpc_mode": "low_cu" if LOW_CU_MODE else "standard",
                        "listener_block_fetch_mode": block_fetch_mode,
                        "listener_block_fetch_reason": block_fetch_reason,
                        "listener_block_get_logs_request_count": int(
                            scan_stats.get("request_count") or 0
                        ),
                        "listener_block_topic_chunk_count": int(
                            scan_stats.get("topic_chunk_count") or 0
                        ),
                        "listener_block_monitored_address_count": int(
                            scan_stats.get("monitored_address_count") or 0
                        ),
                        "listener_block_lp_secondary_scan_used": bool(
                            lp_scan_plan.get("secondary_scan_used")
                        ),
                        "listener_block_bloom_prefilter_used": bool(
                            scan_stats.get("listener_block_bloom_prefilter_used")
                        ),
                        "listener_block_bloom_skipped_get_logs_count": int(
                            scan_stats.get("listener_block_bloom_skipped_get_logs_count") or 0
                        ),
                        "listener_block_bloom_transfer_possible": bool(block_transfer_possible),
                        "listener_block_bloom_address_possible_count": int(
                            scan_stats.get("listener_block_bloom_address_possible_count") or 0
                        ),
                        "listener_runtime_adjacent_core_count": int(
                            runtime_adjacent_plan.get("core_count") or 0
                        ),
                        "listener_runtime_adjacent_secondary_count": int(
                            runtime_adjacent_plan.get("secondary_count") or 0
                        ),
                        "listener_runtime_adjacent_secondary_scan_used": bool(
                            runtime_adjacent_plan.get("secondary_scan_used")
                        ),
                        "listener_runtime_adjacent_secondary_skipped_count": int(
                            runtime_adjacent_plan.get("secondary_skipped_count") or 0
                        ),
                        "listener_block_lp_primary_trend_scan_used": bool(
                            lp_scan_plan.get("primary_scan_used")
                        ),
                        "listener_block_lp_extended_scan_used": bool(
                            lp_scan_plan.get("extended_scan_used")
                        ),
                        "listener_block_lp_primary_trend_pool_count": int(
                            len(lp_scan_plan.get("primary_addresses") or [])
                        ),
                        "listener_block_lp_extended_pool_count": int(
                            len(lp_scan_plan.get("extended_addresses") or [])
                        ),
                        "listener_block_get_logs_primary_side_count": int(
                            scan_stats.get("listener_block_get_logs_primary_side_count") or 0
                        ),
                        "listener_block_get_logs_secondary_side_count": int(
                            scan_stats.get("listener_block_get_logs_secondary_side_count") or 0
                        ),
                        "listener_block_get_logs_secondary_side_skipped_count": int(
                            scan_stats.get("listener_block_get_logs_secondary_side_skipped_count") or 0
                        ),
                        "listener_block_get_logs_empty_response_count": int(
                            scan_stats.get("listener_block_get_logs_empty_response_count") or 0
                        ),
                        "low_cu_mode_enabled": bool(LOW_CU_MODE),
                        "low_cu_mode_lp_secondary_only": bool(
                            not LISTENER_INCLUDE_ACTIVE_LP_POOLS_IN_MAIN_LOG_SCAN
                            and LP_SECONDARY_SCAN_ENABLE
                        ),
                        "low_cu_mode_poll_interval_sec": _effective_poll_interval_sec(),
                    }
                )
                _update_listener_runtime_stats(block_context)
                print(
                    "新区块: "
                    f"{block_num} tx={len(block_tx_hashes)} "
                    f"rpc_mode={block_context['listener_rpc_mode']} "
                    f"fetch_mode={block_context['listener_block_fetch_mode']} "
                    f"get_logs_req={block_context['listener_block_get_logs_request_count']} "
                    f"bloom_skip={block_context['listener_block_bloom_skipped_get_logs_count']} "
                    f"dir_primary={block_context['listener_block_get_logs_primary_side_count']} "
                    f"dir_secondary={block_context['listener_block_get_logs_secondary_side_count']} "
                    f"dir_skip={block_context['listener_block_get_logs_secondary_side_skipped_count']} "
                    f"empty={block_context['listener_block_get_logs_empty_response_count']} "
                    f"adj_core={block_context['listener_runtime_adjacent_core_count']} "
                    f"adj_secondary={block_context['listener_runtime_adjacent_secondary_count']} "
                    f"lp_primary={int(block_context['listener_block_lp_primary_trend_scan_used'])}/"
                    f"{block_context['listener_block_lp_primary_trend_pool_count']} "
                    f"lp_extended={int(block_context['listener_block_lp_extended_scan_used'])}/"
                    f"{block_context['listener_block_lp_extended_pool_count']}"
                )

                for tx in block_transactions:
                    from_addr = str(tx["from"]).lower()
                    to_addr = str(tx["to"]).lower() if tx.get("to") else None
                    tx_hash_hex = _to_hex(tx.get("hash")).lower()
                    if tx_hash_hex in token_flow_tx_hashes:
                        continue

                    if tx["value"] > 0 and (
                        from_addr in block_monitored_watch_addresses
                        or (to_addr and to_addr in block_monitored_watch_addresses)
                    ):
                        touched_watch_addresses = [
                            addr for addr in [from_addr, to_addr]
                            if addr and addr in block_monitored_watch_addresses
                        ]
                        candidate = _build_eth_candidate(
                            tx,
                            block_num,
                            touched_watch_addresses,
                        )
                        candidate.update(block_context)
                        await _enqueue(candidate)

                for tx_hash, logs in tx_transfer_logs.items():
                    touched_watch_addresses = _extract_touched_watch_addresses(
                        logs,
                        block_monitored_watch_addresses,
                    )
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
                    candidate.update(block_context)

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
                        enriched["monitor_type"] = (
                            "watch_address" if watch_address in WATCH_ADDRESSES else "adjacent_watch"
                        )
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

                processed_until = block_num

            last_block = processed_until

        except Exception as e:
            QUEUE_STATS.update(get_rpc_stats_snapshot())
            print("❌ 获取区块失败:", e)
            await asyncio.sleep(max(effective_poll_interval, 2.0))

        await asyncio.sleep(effective_poll_interval)


async def worker(handle_tx):
    """消费者：从队列中取数据并调用处理函数。"""
    while True:
        tx = await tx_queue.get()
        try:
            await handle_tx(tx)
        except Exception as e:
            print("❌ 处理交易失败:", e)
        tx_queue.task_done()
