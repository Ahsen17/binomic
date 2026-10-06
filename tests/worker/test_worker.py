import logging
import time
from collections.abc import AsyncIterator, Callable

import anyio
import pytest
from pytest_mock import MockerFixture

from binomic.broker import AsyncredisBroker, Entry
from binomic.message import Message
from binomic.task.registry import TaskRegistry, TaskSpec, registry
from binomic.worker import Worker, WorkerPolicy
from binomic.worker.worker import logger as worker_logger


@pytest.fixture
async def worker_broker(
    make_broker: Callable[..., AsyncredisBroker],
) -> AsyncIterator[AsyncredisBroker]:
    """An initialized fakeredis-backed broker handed to the worker under test."""

    instance = make_broker(["default"])
    await instance.initialize()
    yield instance
    await instance.aclose()


@pytest.fixture
def tracked(isolate_registry: TaskRegistry) -> list[str]:
    """Register a task recording its invocations, plus a never-returning task."""

    calls: list[str] = []

    def record(task_id: str) -> None:
        calls.append(task_id)

    async def hang() -> None:
        await anyio.sleep(3600)

    registry.register(TaskSpec(name="record", fn=record))
    registry.register(TaskSpec(name="hang", fn=hang))
    return calls


class TestWorkerRun:
    async def test_executes_task_and_acks(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        tracked: list[str],
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        msg = make_message(queue="default", name="record", args=["job-1"])
        await worker_broker.enqueue(msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        await worker._run(entry)

        assert tracked == ["job-1"]
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_expired_message_is_dropped_without_execution(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        tracked: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        msg = make_message(
            queue="default",
            name="record",
            args=["late-1"],
            enqueued_at=time.time() - 7200,
        )
        await worker_broker.enqueue(msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        with caplog.at_level(logging.WARNING, logger=worker_logger.name):
            await worker._run(entry)

        assert tracked == []
        assert any("has expired" in r.message for r in caplog.records)
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_unknown_task_is_logged_and_acked(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        await worker_broker.enqueue(make_message(queue="default", name="ghost-task"))
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        with caplog.at_level(logging.ERROR, logger=worker_logger.name):
            await worker._run(entry)

        assert any("Failed to process" in r.message for r in caplog.records)
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_unserializable_fields_are_logged_and_acked(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        entry = Entry("default", "1-0", {"id": "bad", "message": "{not-json"})

        with caplog.at_level(logging.ERROR, logger=worker_logger.name):
            await worker._run(entry)

        assert any("Failed to process" in r.message for r in caplog.records)
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_overrun_task_times_out(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        tracked: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        msg = make_message(queue="default", name="hang")
        await worker_broker.enqueue(msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        with caplog.at_level(logging.WARNING, logger=worker_logger.name):
            await worker._run(entry)

        assert any("timed out" in r.message for r in caplog.records)
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_run_without_broker_raises(
        self, make_worker: Callable[..., Worker]
    ) -> None:

        worker = make_worker()
        entry = Entry("default", "1-0", {"id": "x", "message": "{}"})

        with pytest.raises(RuntimeError, match="Broker is not initialized"):
            await worker._run(entry)


class TestWorkerLifecycle:
    def test_worker_carries_identity(self, make_worker: Callable[..., Worker]) -> None:

        worker = make_worker()

        assert worker.name == "test-worker"

    def test_autodiscover_uses_module_name(
        self,
        isolate_registry: TaskRegistry,
    ) -> None:

        Worker(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            consumer="worker-x",
            policy=WorkerPolicy(queues=["q"], concurrency=1),
        )

        assert "example" in registry

    async def test_aclose_signals_termination(
        self, make_worker: Callable[..., Worker]
    ) -> None:

        worker = make_worker()

        await worker.aclose()

        assert worker._terminate.is_set()

    async def test_aclose_is_idempotent(
        self, make_worker: Callable[..., Worker], mocker: MockerFixture
    ) -> None:

        worker = make_worker()
        timers = mocker.Mock()
        timers.cancel_scope.cancel_called = False
        worker._timers = timers

        await worker.aclose()
        await worker.aclose()

        assert timers.cancel_scope.cancel.call_count == 1
