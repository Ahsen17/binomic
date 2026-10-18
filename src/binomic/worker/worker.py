import time
from typing import TYPE_CHECKING, Any

import anyio

from binomic.broker import (
    BrokerFactory,
    BrokerLifecycleMixin,
    MessageEntry,
)

from .handler import WorkerHandler

if TYPE_CHECKING:
    from anyio import Event
    from anyio.abc import TaskGroup

    from binomic.broker import Broker

    from .policies import WorkerPolicy
    from .presence import SubprocessPresence


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

    async def arun(self) -> None:  # noqa: C901

        await self.start()

        handler = await self._ensure_handler()
        terminate = self._ensure_terminate()

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
                await handler.handle(entry)

        try:
            async with anyio.create_task_group() as timers:
                self._timers = timers
                timers.start_soon(self._heartbeat, terminate)
                timers.start_soon(_poll_messages)
                timers.start_soon(_poll_reclaims)

                for _ in range(self._policy.concurrency):
                    timers.start_soon(_worker_consumer)

                await terminate.wait()
                timers.cancel_scope.cancel()

        finally:
            await sender.aclose()
            await receiver.aclose()
            await self.close()

    def run(self) -> None:

        anyio.run(self.arun)
