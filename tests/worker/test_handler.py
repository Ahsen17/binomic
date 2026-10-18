"""Handler semantics: one test per branch of `WorkerHandler.handle`."""

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from pytest_mock import MockerFixture

from binomic.broker import Message, MessageEntry, RedisBroker
from binomic.worker import WorkerHandler, WorkerPolicy

STREAM_KEY = "binomic:default:stream"
DLQ_STREAM_KEY = "binomic:default:dlq:stream"

Deliver = Callable[..., Coroutine[Any, Any, MessageEntry]]


@pytest.fixture
async def make_handler(
    make_broker: Callable[..., RedisBroker],
    handler_module: Any,
) -> AsyncIterator[Callable[..., Any]]:
    """Build a handler over a fresh fakeredis broker; policy knobs per test.

    The default ``task_timeout`` is deliberately generous: the first Lua
    round-trip in a cold interpreter costs hundreds of ms, and a tight budget
    would race the over-age branch instead of testing the intended one.
    """

    built: list[tuple[WorkerHandler, RedisBroker]] = []

    async def _make(**policy_kwargs: Any) -> Any:

        broker = make_broker(["default"])
        await broker.start()

        fields: dict[str, Any] = {
            "task_timeout": 5.0,
            "max_attempts": 3,
        }
        fields.update(policy_kwargs)
        policy = WorkerPolicy(
            queues=["default"],
            concurrency=2,
            **fields,
        )
        handler = WorkerHandler(
            module_name="handlerapp",
            consumer="test-worker",
            broker=broker,
            worker_policy=policy,
        )
        built.append((handler, broker))

        return SimpleNamespace(handler=handler, broker=broker, policy=policy)

    yield _make

    for handler, broker in built:
        handler.close()
        await broker.close()


async def pending_count(broker: RedisBroker) -> int:

    pending = await broker._ensure_client().xpending(STREAM_KEY, "binomic")
    return int(pending["pending"])


async def dead_letter_count(broker: RedisBroker) -> int:

    entries = await broker._ensure_client().xrange(DLQ_STREAM_KEY, "-", "+")
    assert entries is not None
    return len(cast("list[Any]", entries))


async def stream_entry_ids(broker: RedisBroker) -> list[str]:

    entries = await broker._ensure_client().xrange(STREAM_KEY, "-", "+")
    assert entries is not None

    return [
        entry_id
        for entry_id, _fields in cast("list[tuple[Any, Any]]", entries)
    ]


class TestSuccess:
    async def test_executes_and_acks(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # Execution is proven by the success path's ack; nothing died.
        assert await pending_count(e.broker) == 0
        assert await stream_entry_ids(e.broker) == []
        assert await dead_letter_count(e.broker) == 0


class TestUnknownTask:
    async def test_acks_without_dead_lettering(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ghost"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert await dead_letter_count(e.broker) == 0
        assert await pending_count(e.broker) == 0


class TestRetry:
    async def test_schedules_a_delayed_retry_on_failure(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        set_backoff: Callable[[float], None],
    ) -> None:

        set_backoff(0.05)
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="boom", attempt=1))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # Nothing has been delivered yet: the copy only appears once the
        # scheduled retry fires. The original is still pending.
        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0

        jobs = e.handler._scheduler._scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].func.__name__ == "retry"
        assert jobs[0].args == (entry,)

    async def test_redelivery_carries_incremented_attempt_and_stale_stamp(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        e.handler._backoff = lambda attempt: 0.05
        await e.broker.enqueue("default", Message(task="boom", attempt=1))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        deadline = time.monotonic() + 5
        ids: list[str] = []
        while time.monotonic() < deadline:
            ids = await stream_entry_ids(e.broker)
            if len(ids) == 1 and ids[0] != entry.id:
                break
            await asyncio.sleep(0.02)

        assert len(ids) == 1
        assert ids[0] != entry.id
        assert await pending_count(e.broker) == 0

        raw = await e.broker._ensure_client().hget("binomic:default:hash", ids[0])
        assert raw is not None
        body = json.loads(raw)

        # task_timeout is a total delivery deadline: the copy keeps the
        # original stamp so the age keeps accumulating across retries.
        assert body["message"]["attempt"] == 2
        assert body["enqueued_at"] == entry.enqueued_at


class TestMaxAttempts:
    async def test_failure_at_max_attempts_dead_letters(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="boom", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # attempt + 1 > max_attempts: no retry is scheduled, the message dies
        # with its attempt recorded faithfully (no extra bump in the DLQ).
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0
        assert await stream_entry_ids(e.broker) == []
        # No retry scheduled — the underlying scheduler was never even started.
        assert e.handler._scheduler._scheduler is None


class TestOverAge:
    async def test_skips_execution_and_dead_letters(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")
        entry.enqueued_at = int(time.time()) - 3600

        await e.handler.handle(entry)

        # Dead-lettered without execution, whatever the task would have done:
        # the DLQ copy is the ack-clean message itself.
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0


class TestTimeout:
    async def test_dead_letters_without_retry(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:
        """task_timeout is a total deadline: an overrun execution dies instead
        of retrying (the ref's new semantics). Uses a tight budget: the task
        sleeps two full seconds, far past the half-second one."""

        e = await make_handler(task_timeout=0.5)
        await e.broker.enqueue("default", Message(task="slow"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # A TimeoutError can only come from a task that RAN: execution is
        # implied by the timeout dead-lettering itself.
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0
        assert e.handler._scheduler._scheduler is None


class TestCancellation:
    async def test_propagates_without_ack(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="cancelled"))
        entry = await deliver_entry(e.broker, "default", "c1")

        with pytest.raises(anyio.get_cancelled_exc_class()):
            await e.handler.handle(entry)

        # Left exactly as it was: the message stays pending for reclaim, no
        # dead letter, no retry — the scheduler was never even started.
        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0
        assert e.handler._scheduler._scheduler is None


class TestExitSignals:
    async def test_system_exit_propagates_without_ack(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="quits"))
        entry = await deliver_entry(e.broker, "default", "c1")

        with pytest.raises(SystemExit):
            await e.handler.handle(entry)

        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0


class TestBackoff:
    async def test_doubles_until_the_cap(self, make_handler: Callable[..., Any]) -> None:

        e = await make_handler()
        backoff = e.handler._backoff

        assert [backoff(n) for n in range(1, 8)] == [
            1.5,
            3.0,
            6.0,
            12.0,
            24.0,
            30.0,
            30.0,
        ]


class TestReclaim:
    async def test_passes_policy_derived_parameters(
        self, make_handler: Callable[..., Any], mocker: MockerFixture
    ) -> None:

        e = await make_handler()
        reclaim = mocker.AsyncMock()
        mocker.patch.object(e.broker, "reclaim", reclaim)

        await e.handler.reclaim()

        # min_idle is the task_timeout in ms with a 10s floor.
        reclaim.assert_awaited_once_with(
            consumer="test-worker",
            min_idle_ms=max(int(e.policy.task_timeout * 1000), 10_000),
            count=100,
        )
