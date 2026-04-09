import random
import threading
import time
from typing import Any, Callable

from config import (
    RPC_BACKOFF_BASE_SEC,
    RPC_BACKOFF_MAX_SEC,
    RPC_CIRCUIT_BREAKER_SEC,
    RPC_RETRY_MAX_ATTEMPTS,
)


_RPC_LOCK = threading.Lock()
_RPC_STATS = {
    "rpc_429_count": 0,
    "rpc_backoff_count": 0,
    "rpc_circuit_breaker_open_count": 0,
    "rpc_retry_success_count": 0,
}
_CIRCUIT_OPEN_UNTIL = 0.0
_LAST_LOG_TS_BY_KEY: dict[str, float] = {}


def get_rpc_stats_snapshot() -> dict[str, int]:
    with _RPC_LOCK:
        return dict(_RPC_STATS)


def _throttled_log(key: str, message: str, *, window_sec: float = 10.0) -> None:
    now = time.time()
    with _RPC_LOCK:
        last_ts = float(_LAST_LOG_TS_BY_KEY.get(key) or 0.0)
        if now - last_ts < float(window_sec):
            return
        _LAST_LOG_TS_BY_KEY[key] = now
    print(message)


def _error_text(exc: Exception) -> str:
    return str(exc or "").strip().lower()


def _is_rate_limited_error(exc: Exception) -> bool:
    text = _error_text(exc)
    return (
        "429" in text
        or "too many requests" in text
        or "capacity limit exceeded" in text
        or "capacity exceeded" in text
        or "rate limit" in text
    )


def _is_retryable_rpc_error(exc: Exception) -> bool:
    text = _error_text(exc)
    return (
        _is_rate_limited_error(exc)
        or "timeout" in text
        or "temporarily unavailable" in text
        or "connection aborted" in text
        or "connection reset" in text
        or "max retries exceeded" in text
        or "server error" in text
        or "bad gateway" in text
        or "service unavailable" in text
        or "gateway timeout" in text
    )


def _open_circuit(reason: str) -> None:
    global _CIRCUIT_OPEN_UNTIL
    open_until = time.time() + float(RPC_CIRCUIT_BREAKER_SEC or 30)
    with _RPC_LOCK:
        _CIRCUIT_OPEN_UNTIL = open_until
        _RPC_STATS["rpc_circuit_breaker_open_count"] = int(
            _RPC_STATS.get("rpc_circuit_breaker_open_count") or 0
        ) + 1
    _throttled_log(
        f"rpc_circuit_open:{reason}",
        f"⚠️ RPC 熔断已打开: reason={reason} open_for={int(RPC_CIRCUIT_BREAKER_SEC or 30)}s",
        window_sec=10.0,
    )


def rpc_call_with_backoff(
    operation: str,
    fn: Callable[[], Any],
    *,
    return_none_on_failure: bool = False,
) -> Any:
    global _CIRCUIT_OPEN_UNTIL

    attempt = 0
    had_retry = False
    max_attempts = max(int(RPC_RETRY_MAX_ATTEMPTS or 1), 1)
    base_sec = max(float(RPC_BACKOFF_BASE_SEC or 0.5), 0.1)
    max_sec = max(float(RPC_BACKOFF_MAX_SEC or base_sec), base_sec)

    while attempt < max_attempts:
        now = time.time()
        open_until = float(_CIRCUIT_OPEN_UNTIL or 0.0)
        if open_until > now:
            sleep_sec = min(open_until - now, max_sec)
            _throttled_log(
                f"rpc_circuit_wait:{operation}",
                f"⏸️ RPC 熔断等待: op={operation} sleep={round(sleep_sec, 2)}s",
                window_sec=10.0,
            )
            time.sleep(max(sleep_sec, 0.1))

        try:
            result = fn()
            if had_retry:
                with _RPC_LOCK:
                    _RPC_STATS["rpc_retry_success_count"] = int(
                        _RPC_STATS.get("rpc_retry_success_count") or 0
                    ) + 1
            return result
        except Exception as exc:
            rate_limited = _is_rate_limited_error(exc)
            retryable = _is_retryable_rpc_error(exc)
            if rate_limited:
                with _RPC_LOCK:
                    _RPC_STATS["rpc_429_count"] = int(
                        _RPC_STATS.get("rpc_429_count") or 0
                    ) + 1

            attempt += 1
            if not retryable or attempt >= max_attempts:
                if retryable:
                    _open_circuit(str(exc or operation))
                if return_none_on_failure:
                    _throttled_log(
                        f"rpc_failure:{operation}",
                        f"❌ RPC 调用失败: op={operation} err={exc}",
                        window_sec=10.0,
                    )
                    return None
                raise

            delay = min(base_sec * (2 ** (attempt - 1)) + random.uniform(0.0, base_sec), max_sec)
            had_retry = True
            with _RPC_LOCK:
                _RPC_STATS["rpc_backoff_count"] = int(
                    _RPC_STATS.get("rpc_backoff_count") or 0
                ) + 1
            _throttled_log(
                f"rpc_backoff:{operation}",
                f"⚠️ RPC 重试退避: op={operation} attempt={attempt}/{max_attempts} "
                f"sleep={round(delay, 2)}s err={exc}",
                window_sec=5.0,
            )
            time.sleep(max(delay, 0.1))

    if return_none_on_failure:
        return None
    raise RuntimeError(f"rpc_call_exhausted:{operation}")
