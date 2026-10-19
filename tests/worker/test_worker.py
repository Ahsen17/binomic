import time
from collections.abc import Callable
from multiprocessing import get_context
from typing import Any

import anyio

from binomic.broker import Message, RedisBroker
from binomic.worker import Worker, WorkerPolicy
from binomic.worker.presence import SubprocessPresence

SAMPLE_MODULE = "binomic"  # the package holding the shipped sample tasks


class TestWorkerIdentity:
    def test_carries_identity(self, make_worker: Callable[..., Worker]) -> None:

        worker = make_worker(module_name=SAMPLE_MODULE, consumer="test-worker")

        assert worker._consumer == "test-worker"
        assert worker._module_name == SAMPLE_MODULE
        assert worker._broker is None
        assert worker._handler is None


class TestWorkerStart:
    async def test_wires_broker_handler_and_terminate(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:
        """make_broker is present for its fakeredis patching side effect: the
        worker builds its own broker through the real BrokerFactory."""

        worker = make_worker(module_name=SAMPLE_MODULE)
        try:
            await worker.start()

            assert isinstance(worker._broker, RedisBroker)
            assert worker._broker._running
            assert worker._handler is not None
            assert worker._terminate is not None
        finally:
            await worker.close()

    async def test_autodiscovers_the_module(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:
        """The handler owns a registry of its own, filled from module_name."""

        worker = make_worker(module_name=SAMPLE_MODULE)
        try:
            await worker.start()

            handler = worker._handler
            assert handler is not None
            assert "example" in handler._registry
            assert "fail_example" in handler._registry
        finally:
            await worker.close()

    async def test_start_is_idempotent(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:

        worker = make_worker(module_name=SAMPLE_MODULE)
        try:
            await worker.start()
            broker = worker._broker
            handler = worker._handler
            await worker.start()

            assert worker._broker is broker
            assert worker._handler is handler
        finally:
            await worker.close()


class TestWorkerClose:
    async def test_is_safe_before_start(self, make_worker: Callable[..., Worker]) -> None:

        worker = make_worker(module_name=SAMPLE_MODULE)

        await worker.close()  # nothing wired yet; must not raise

    async def test_releases_everything(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:

        worker = make_worker(module_name=SAMPLE_MODULE)
        await worker.start()
        await worker.close()

        assert worker._broker is None
        assert worker._handler is None
        assert worker._terminate is None

    async def test_is_idempotent(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:

        worker = make_worker(module_name=SAMPLE_MODULE)
        await worker.start()

        await worker.close()
        await worker.close()


class TestWorkerHeartbeat:
    async def test_stamps_the_cell_repeatedly(
        self, make_worker: Callable[..., Worker]
    ) -> None:
        """The heartbeat task beats every heartbeat_interval until told to
        stop — the master's whole view of liveness rests on this."""

        cell = get_context("spawn").Value("d", 0.0)
        policy = WorkerPolicy(
            queues=["default"],
            concurrency=1,
            heartbeat_interval=0.05,
        )
        worker = Worker(
            broker_dsn="redis://localhost:6379/0",
            module_name=SAMPLE_MODULE,
            consumer="test-worker",
            policy=policy,
            presence=SubprocessPresence(cell),
        )
        terminate = anyio.Event()

        with anyio.move_on_after(0.25):
            await worker._heartbeat(terminate)

        terminate.set()

        assert cell.value > 0.0
        assert cell.value <= time.time()


class TestWorkerInitHook:
    def test_runs_once_before_the_event_loop(
        self,
        make_worker: Callable[..., Worker],
        mocker: Any,
    ) -> None:

        calls: list[str] = []

        worker = make_worker(
            policy=WorkerPolicy(
                queues=["default"],
                concurrency=1,
                worker_init=lambda: calls.append("init"),
            ),
        )

        def fake_run(func: Any) -> None:

            calls.append("loop")

        mocker.patch("binomic.worker.worker.anyio.run", side_effect=fake_run)

        worker.run()

        assert calls == ["init", "loop"]

    def test_absent_hook_is_a_noop(
        self,
        make_worker: Callable[..., Worker],
        mocker: Any,
    ) -> None:

        worker = make_worker()

        def fake_run(func: Any) -> None: ...

        mocker.patch("binomic.worker.worker.anyio.run", side_effect=fake_run)

        worker.run()


class TestQueueGauges:
    def test_callbacks_render_the_snapshot(
        self,
        make_worker: Callable[..., Worker],
        monkeypatch: Any,
    ) -> None:

        captured: dict[str, Any] = {}

        def fake_gauge(name: str, unit: str, observe: Any) -> None:

            captured[name] = observe

        monkeypatch.setattr(
            "binomic.worker.worker.create_observable_gauge",
            fake_gauge,
        )

        worker = make_worker()
        worker._queue_snapshot.update({"a": (3, 1.5), "b": (0, None)})
        worker._setup_queue_gauges()

        depth = list(captured["binomic.queue.depth"]())
        age = list(captured["binomic.queue.oldest_pending.age"]())

        assert depth == [
            (3.0, {"binomic.queue.name": "a"}),
            (0.0, {"binomic.queue.name": "b"}),
        ]
        assert age == [(1.5, {"binomic.queue.name": "a"})]

    async def test_observation_updates_the_snapshot(
        self,
        fake_broker_backend: None,
        make_worker: Callable[..., Worker],
    ) -> None:

        policy = WorkerPolicy(
            queues=["default"],
            concurrency=1,
            otel_enabled=True,
            reclaim_interval=0.0,
            poll_interval=0.01,
        )
        worker = make_worker(module_name=SAMPLE_MODULE, policy=policy)

        try:
            await worker.start()
            broker = await worker._ensure_broker()
            await broker.enqueue("default", Message(task="example"))

            terminate = worker._ensure_terminate()

            with anyio.move_on_after(0.5):
                await worker._observe_queues(terminate)

            depth, age = worker._queue_snapshot["default"]
            assert depth == 1
            assert age is not None

        finally:
            await worker.close()
