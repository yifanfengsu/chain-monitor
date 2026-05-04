import asyncio
import importlib
import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

from telegram.error import RetryAfter, TimedOut
from telegram.request import HTTPXRequest


ENV_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "CHAT_ID",
    "TELEGRAM_CONNECTION_POOL_SIZE",
    "TELEGRAM_POOL_TIMEOUT",
    "TELEGRAM_CONNECT_TIMEOUT",
    "TELEGRAM_READ_TIMEOUT",
    "TELEGRAM_WRITE_TIMEOUT",
    "TELEGRAM_SEND_CONCURRENCY",
    "TELEGRAM_SEND_MAX_ATTEMPTS",
    "TELEGRAM_NOTIFIER_ALERT_FAILURES",
]


class FakeBot:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    async def send_message(self, **kwargs):
        self.calls.append(kwargs)
        if not self.outcomes:
            return object()
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class NotifierTelegramRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {key: os.environ.get(key) for key in ENV_KEYS}

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        sys.modules.pop("notifier", None)
        sys.modules.pop("app.notifier", None)
        sys.modules.pop("config", None)

    def _fresh_notifier(self, **env):
        for key in ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["TELEGRAM_BOT_TOKEN"] = "123456:unit-test-secret-token"
        os.environ["CHAT_ID"] = "0"
        for key, value in env.items():
            os.environ[key] = str(value)
        sys.modules.pop("notifier", None)
        sys.modules.pop("app.notifier", None)
        sys.modules.pop("config", None)
        return importlib.import_module("notifier")

    def test_bot_uses_explicit_httpx_request_with_defaults(self) -> None:
        notifier = self._fresh_notifier()

        self.assertIsInstance(notifier.bot.request, HTTPXRequest)
        request_config = notifier.bot.request._client_kwargs
        self.assertEqual(16, request_config["limits"].max_connections)
        self.assertEqual(10.0, request_config["timeout"].pool)
        self.assertEqual(3, notifier.TELEGRAM_SEND_CONCURRENCY)
        self.assertEqual(3, notifier._SEND_SEMAPHORE._value)

    def test_request_and_send_concurrency_read_env(self) -> None:
        notifier = self._fresh_notifier(
            TELEGRAM_CONNECTION_POOL_SIZE=24,
            TELEGRAM_POOL_TIMEOUT=7.5,
            TELEGRAM_SEND_CONCURRENCY=5,
        )

        request_config = notifier.bot.request._client_kwargs
        self.assertEqual(24, request_config["limits"].max_connections)
        self.assertEqual(7.5, request_config["timeout"].pool)
        self.assertEqual(5, notifier.TELEGRAM_SEND_CONCURRENCY)
        self.assertEqual(5, notifier._SEND_SEMAPHORE._value)

    def test_send_retries_timed_out_then_returns_false_and_redacts_token(self) -> None:
        asyncio.run(self._assert_timed_out_retry_failure())

    async def _assert_timed_out_retry_failure(self) -> None:
        token = "123456:unit-test-secret-token"
        notifier = self._fresh_notifier(TELEGRAM_BOT_TOKEN=token)
        notifier.bot = FakeBot(
            [
                TimedOut(f"Pool timeout: All connections occupied for {token}"),
                TimedOut(f"Pool timeout: All connections occupied for {token}"),
                TimedOut(f"Pool timeout: All connections occupied for {token}"),
            ]
        )
        sleep_calls = []

        async def fake_sleep(seconds):
            sleep_calls.append(seconds)

        output = io.StringIO()
        with mock.patch.object(notifier.asyncio, "sleep", new=fake_sleep), redirect_stdout(output):
            delivered = await notifier.send("unit test message")

        self.assertFalse(delivered)
        self.assertEqual(3, len(notifier.bot.calls))
        self.assertEqual([1.0, 2.0], sleep_calls)
        self.assertNotIn(token, output.getvalue())
        self.assertIn("[redacted-token]", output.getvalue())
        self.assertEqual(3, notifier.NOTIFIER_RUNTIME_STATS["telegram_pool_timeout_count"])
        self.assertEqual(3, notifier.NOTIFIER_RUNTIME_STATS["telegram_consecutive_failures"])
        self.assertEqual("TimedOut", notifier.NOTIFIER_RUNTIME_STATS["telegram_last_error_type"])
        self.assertEqual(10.0, notifier.bot.calls[0]["pool_timeout"])
        self.assertEqual(20.0, notifier.bot.calls[0]["read_timeout"])

    def test_send_retries_retry_after_then_succeeds(self) -> None:
        asyncio.run(self._assert_retry_after_then_success())

    async def _assert_retry_after_then_success(self) -> None:
        notifier = self._fresh_notifier()
        notifier.bot = FakeBot([RetryAfter(1), object()])
        sleep_calls = []

        async def fake_sleep(seconds):
            sleep_calls.append(seconds)

        output = io.StringIO()
        with mock.patch.object(notifier.asyncio, "sleep", new=fake_sleep), redirect_stdout(output):
            delivered = await notifier.send("unit test message")

        self.assertTrue(delivered)
        self.assertEqual(2, len(notifier.bot.calls))
        self.assertEqual([1.5], sleep_calls)
        self.assertEqual(1, notifier.NOTIFIER_RUNTIME_STATS["telegram_retry_after_count"])
        self.assertEqual(0, notifier.NOTIFIER_RUNTIME_STATS["telegram_consecutive_failures"])
        self.assertIsNotNone(notifier.NOTIFIER_RUNTIME_STATS["telegram_last_success_ts"])

    def test_get_notifier_health_contains_new_fields(self) -> None:
        notifier = self._fresh_notifier()

        health = notifier.get_notifier_health()

        self.assertTrue(
            {
                "attempted",
                "sent_ok",
                "sent_failed",
                "pool_timeout_count",
                "retry_after_count",
                "network_error_count",
                "consecutive_failures",
                "last_success_ts",
                "last_failure_ts",
                "last_error_type",
            }.issubset(health.keys())
        )


if __name__ == "__main__":
    unittest.main()
