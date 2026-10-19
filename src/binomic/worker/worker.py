import time
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from binomic.broker import (
    BrokerFactory,
    BrokerLifecycleMixin,
    BrokerTelemetryMixin,
    MessageEntry,
)
from binomic.otel import (
    QUEUE_NAME,
    WORKER_CONSUMER,
    create_observable_gauge,
    create_up_down_counter,
)

from .handler import WorkerHandler

if TYPE_CHECKING:
    from collections.abc import Iterable

    from anyio import Event
    from anyio.abc import TaskGroup

    from binomic.broker import Broker

    from .policies import WorkerPolicy
    from .presence import SubprocessPresence


logger = structlog.stdlib.get_logger(__name__)


class Worker:
    """One consumer process: pull from the broker, execute, ack or requeue.

    ``run`` is the subprocess entry point; ``arun`` builds the broker and the
    handler, then pumps acquired entries through an anyio memory stream to a
    pool of consumers until terminated. Liveness is reported by beating on
    the presence cell the master handed in.
    """

    def __init__(
        self,
        broker_dsn: str,
        module_name: str,
        consumer: str,
        policy: "WorkerPolicy",
        presence: "SubprocessPresence",
        **broker_kwargs: Any,
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._consumer = consumer
        self._policy = policy
        self._presence = presence
        self._broker_kwargs = broker_kwargs

        self._broker: Broker | None = None
        self._timers: TaskGroup | None = None
        self._terminate: Event | None = None
        self._handler: WorkerHandler | None = None
        self._inflight: Any | None = None
        self._queue_snapshot: dict[str, tuple[int, float | None]] = {}

    async def _ensure_broker(self) -> "Broker":

        if self._broker is None:
            self._broker = BrokerFactory(
                dsn=self._broker_dsn,
                queues=self._policy.queues,
                namespace=f"{self._module_name}",
                **self._broker_kwargs,
            ).create()

            if isinstance(self._broker, BrokerLifecycleMixin):
                await self._broker.start()

        return self._broker

    async def _ensure_handler(self) -> "WorkerHandler":

        if self._handler is None:
            self._handler = WorkerHandler(
                module_name=self._module_name,
                consumer=self._consumer,
                broker=await self._ensure_broker(),
                worker_policy=self._policy,
            )

            self._handler.start()

        return self._handler

    def _ensure_terminate(self) -> "Event":

        if self._terminate is None:
            self._terminate = anyio.Event()

        return self._terminate

    async def start(self) -> None:

        await self._ensure_broker()
        await self._ensure_handler()
        self._ensure_terminate()

    async def close(self) -> None:

        if self._terminate is not None:
            if not self._terminate.is_set():
                self._terminate.set()
            self._terminate = None

            await anyio.sleep(1)

        if self._timers is not None:
            if not (scope := self._timers.cancel_scope).cancel_called:
                scope.cancel()
            self._timers = None

        if self._handler is not None:
            self._handler.close()
            self._handler = None

        if self._broker is not None:
            if isinstance(self._broker, BrokerLifecycleMixin):
                await self._broker.close()
            self._broker = None

    async def _heartbeat(self, terminate: "Event") -> None:

        while not terminate.is_set():
            self._presence.heartbeat()

            await anyio.sleep(self._policy.heartbeat_interval)

    def _setup_queue_gauges(self) -> None:

        # Callbacks run on the SDK's collection thread, so they read a copy:
        # iterating the live dict while the loop refreshes it would race.
        def depth() -> "Iterable[tuple[float, dict[str, str]]]":

            return [
                (float(snapshot[0]), {QUEUE_NAME: queue})
                for queue, snapshot in dict(self._queue_snapshot).items()
            ]

        def oldest_age() -> "Iterable[tuple[float, dict[str, str]]]":

            observations: list[tuple[float, dict[str, str]]] = []

            for queue, (_, age) in dict(self._queue_snapshot).items():
                if age is not None:
                    observations.append((age, {QUEUE_NAME: queue}))

            return observations

        create_observable_gauge(
            name="binomic.queue.depth",
            unit="{message}",
            observe=depth,
        )
        create_observable_gauge(
            name="binomic.queue.oldest_pending.age",
            unit="s",
            observe=oldest_age,
        )

    async def _observe_queues(self, terminate: "Event") -> None:

        broker = await self._ensure_broker()

        if not isinstance(broker, BrokerTelemetryMixin):
            return

        last_observed = time.monotonic()

        while not terminate.is_set():
            now = time.monotonic()

            if now - last_observed >= self._policy.reclaim_interval:
                last_observed = now

                for queue in self._policy.queues:
                    try:
                        observed = await broker.observe(queue)

                    except Exception:
                        # A probe hiccup keeps the previous snapshot value.
                        logger.debug(
                            "Queue observation failed",
                            queue=queue,
                            exc_info=True,
                        )

                        continue

                    if observed is not None:
                        self._queue_snapshot[queue] = observed

            await anyio.sleep(self._policy.poll_interval)

    async def arun(self) -> None:  # noqa: C901

        await self.start()

        handler = await self._ensure_handler()
        terminate = self._ensure_terminate()

        observable = isinstance(
            await self._ensure_broker(),
            BrokerTelemetryMixin,
        )

        if self._policy.otel_enabled:
            self._inflight = create_up_down_counter(
                name="binomic.worker.inflight.tasks",
                unit="{task}",
            )

            if observable:
                self._setup_queue_gauges()

        logger.info("Worker started", consumer=self._consumer)

        sender, receiver = anyio.create_memory_object_stream[MessageEntry](
            self._policy.concurrency,
        )

        async def _poll_messages() -> None:
            nonlocal sender, terminate

            while not terminate.is_set():
                entries = await (await self._ensure_broker()).acquire(
                    consumer=self._consumer,
                    count=self._policy.read_count,
                )

                if not entries:
                    await anyio.sleep(self._policy.poll_interval)

                for entry in entries:
                    await sender.send(entry)

        async def _poll_reclaims() -> None:
            nonlocal terminate, handler

            last_reclaimed = time.monotonic()

            while not terminate.is_set():
                now = time.monotonic()
                if now - last_reclaimed >= self._policy.reclaim_interval:
                    last_reclaimed = now
                    await handler.reclaim()

                await anyio.sleep(self._policy.poll_interval)

        async def _worker_consumer() -> None:
            nonlocal terminate, receiver, handler

            while not terminate.is_set():
                try:
                    entry = await receiver.receive()
                except anyio.EndOfStream:
                    break
                except anyio.ClosedResourceError:
                    break

                if (counter := self._inflight) is not None:
                    counter.add(1, attributes={WORKER_CONSUMER: self._consumer})

                try:
                    await handler.handle(entry)

                finally:
                    if (counter := self._inflight) is not None:
                        counter.add(-1, attributes={WORKER_CONSUMER: self._consumer})

        try:
            async with anyio.create_task_group() as timers:
                self._timers = timers
                timers.start_soon(self._heartbeat, terminate)
                timers.start_soon(_poll_messages)
                timers.start_soon(_poll_reclaims)

                if self._policy.otel_enabled and observable:
                    timers.start_soon(self._observe_queues, terminate)

                for _ in range(self._policy.concurrency):
                    timers.start_soon(_worker_consumer)

                await terminate.wait()
                timers.cancel_scope.cancel()

        finally:
            await sender.aclose()
            await receiver.aclose()
            logger.info("Worker stopped", consumer=self._consumer)
            await self.close()

    def run(self) -> None:

        if (init := self._policy.worker_init) is not None:
            init()

        anyio.run(self.arun)
