import logging
import sys
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
    """Register tasks recording their invocations, plus a never-returning one."""

    calls: list[str] = []

    def record(task_id: str) -> None:
        calls.append(task_id)

    async def slow() -> None:
        await anyio.sleep(0.05)
        calls.append("slow-done")

    async def hang() -> None:
        await anyio.sleep(3600)

    registry.register(TaskSpec(name="record", queue="default", fn=record))
    registry.register(TaskSpec(name="slow", queue="default", fn=slow))
    registry.register(TaskSpec(name="hang", queue="default", fn=hang))
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
        msg = make_message(name="record", args=["job-1"])
        await worker_broker.enqueue("default", msg)
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
            name="record",
            args=["late-1"],
            enqueued_at=time.time() - 7200,
        )
        await worker_broker.enqueue("default", msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        with caplog.at_level(logging.WARNING, logger=worker_logger.name):
            await worker._run(entry)

        assert tracked == []
        assert any("has expired" in r.message for r in caplog.records)
        assert (await worker_broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_unstamped_message_is_not_expired(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        tracked: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker

        # A message built without `enqueued_at` has an unknown age, not zero age.
        msg = Message(name="record", args=["fresh"])
        await worker_broker.enqueue("default", msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        with caplog.at_level(logging.WARNING, logger=worker_logger.name):
            await worker._run(entry)

        assert tracked == ["fresh"]
        assert not any("has expired" in r.message for r in caplog.records)

    async def test_fresh_async_task_keeps_its_remaining_budget(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        tracked: list[str],
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        msg = make_message(name="slow")
        await worker_broker.enqueue("default", msg)
        entry = (await worker_broker.acquire("test-worker", count=1))[0]

        await worker._run(entry)

        assert tracked == ["slow-done"]

    async def test_unknown_task_is_logged_and_acked(
        self,
        worker_broker: AsyncredisBroker,
        make_worker: Callable[..., Worker],
        make_message: Callable[..., Message],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        worker = make_worker()
        worker._broker = worker_broker
        await worker_broker.enqueue("default", make_message(name="ghost-task"))
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

        # Wall-clock slack well above the timeout: the assertion must observe the
        # timeout branch, not the expiry branch, even on a loaded runner.
        worker = make_worker(
            policy=WorkerPolicy(
                queues=["default"],
                concurrency=2,
                task_timeout=0.5,
            )
        )
        worker._broker = worker_broker
        msg = make_message(name="hang")
        await worker_broker.enqueue("default", msg)
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

    def test_construction_does_not_autodiscover(
        self,
        make_worker: Callable[..., Worker],
        isolate_registry: TaskRegistry,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        # Drop the cached task module so this asserts on the constructor rather
        # than on whatever imported `binomic.tasks` earlier in the process.
        monkeypatch.delitem(sys.modules, "binomic.tasks", raising=False)

        make_worker(module_name="binomic")

        assert "example" not in registry

    async def test_arun_autodiscovers_module(
        self,
        make_worker: Callable[..., Worker],
        mocker: MockerFixture,
    ) -> None:

        discover = mocker.patch("binomic.worker.worker.autodiscover")

        broker = mocker.AsyncMock()
        broker.acquire.return_value = []

        worker = make_worker(module_name="binomic")
        worker._broker = broker
        worker._presence = mocker.AsyncMock()

        discover.assert_not_called()

        with anyio.move_on_after(0.1):
            await worker.arun()

        discover.assert_called_once_with("binomic")
        assert worker._terminate is not None
        assert worker._terminate.is_set()

    async def test_aclose_is_safe_before_arun(
        self, make_worker: Callable[..., Worker]
    ) -> None:

        worker = make_worker()

        await worker.aclose()

        assert worker._terminate is None

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
