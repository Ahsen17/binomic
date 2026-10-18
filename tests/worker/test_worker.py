import time
from collections.abc import Callable
from multiprocessing import get_context

import anyio

from binomic.broker import RedisBroker
from binomic.worker import Worker, WorkerPolicy
from binomic.worker.presence import SubprocessPresence

SAMPLE_MODULE = "binomic"  # the package holding the shipped sample tasks


class TestWorkerIdentity:
    def test_carries_identity(self, make_worker: Callable[..., Worker]) -> None:

        worker = make_worker(
            module_name=SAMPLE_MODULE, consumer="test-worker"
        )

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
