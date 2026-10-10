import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Self

import anyio
from anyio import AsyncContextManagerMixin

from binomic.broker import Broker, BrokerFactory
from binomic.message import Message
from binomic.task import TaskScheduler, autodiscover
from binomic.task.registry import registry
from binomic.worker import Master, MasterPolicy, WorkerPolicy

if TYPE_CHECKING:
    from uuid import UUID

    from binomic.config import BinomicConfig


__all__ = (
    "Binomic",
    "BinomicFactory",
)


logger = logging.getLogger(__name__)


class Binomic(AsyncContextManagerMixin):
    """Binomic working client.

    Entering the async context starts the master process group and keeps
    it running until the context exits.
    """

    def __init__(
        self,
        broker_dsn: str,
        redis_dsn: str,
        module_name: str,
        config: "BinomicConfig",
    ) -> None:

        self._broker_dsn = broker_dsn
        self._redis_dsn = redis_dsn
        self._module_name = module_name
        self._config = config

        self._broker: Broker | None = None
        self._scheduler: TaskScheduler | None = None

    async def arun(self) -> None:
        try:
            if self._broker is None:
                self._broker = BrokerFactory(
                    self._broker_dsn,
                    self._config.queues,
                ).create()
                await self._broker.initialize()

            if self._scheduler is None:
                scheduler = TaskScheduler()
                scheduler.start()
                self._scheduler = scheduler

            autodiscover(self._module_name)

        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:

        if self._scheduler is not None:
            self._scheduler.shutdown()
            self._scheduler = None

        if self._broker is not None:
            await self._broker.aclose()
            self._broker = None

    async def _enqueue(self, queue: str, msg: "Message") -> None:

        if self._broker is None:
            raise RuntimeError("Run client `arun` before submitting messages.")

        await self._broker.enqueue(queue, msg)

    async def submit(self, msg: "Message") -> "UUID":
        """Dispatch a message to its task.

        ``direct`` tasks are enqueued immediately and ``delay`` tasks are
        handed to the scheduler for a deferred enqueue. Scheduled tasks
        (``cron`` and ``interval``) cannot be submitted: they are registered
        automatically when the client starts.

        Raises:
            TaskNotFoundError: if no task is registered under the message's name.
            RuntimeError: if the client has not been started with ``arun``.
            ValueError: if the task's mode is neither ``direct`` nor ``delay``.
        """

        spec = registry.get(msg.name)

        match spec.mode:
            case "direct":
                await self._enqueue(spec.queue, msg)

            case "delay":
                if self._scheduler is None:
                    raise RuntimeError("Run client `arun` before submitting messages.")

                self._scheduler.delay(
                    func=self._enqueue,
                    spec=spec,
                    args=(spec.queue, msg),
                )

            case _:
                raise ValueError(
                    "Invalid task mode, `submit` only supports "
                    f"direct and delay tasks. Received `{spec.mode}` task."
                )

        return msg.id

    def _register_interval_cron_tasks(self) -> None:

        if self._scheduler is None:
            raise RuntimeError("Run client `arun` before registering scheduled tasks.")

        scheduler = self._scheduler

        for spec in registry:
            match spec.mode:
                case "interval":
                    scheduler.interval(
                        func=self._enqueue,
                        spec=spec,
                        args=(spec.queue, Message(name=spec.name)),
                    )

                    logger.info("Registered interval task `%s`.", spec.name)

                case "cron":
                    scheduler.cron(
                        func=self._enqueue,
                        spec=spec,
                        args=(spec.queue, Message(name=spec.name)),
                    )

                    logger.info("Registered cron task `%s`.", spec.name)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:

        master = Master(
            broker_dsn=self._broker_dsn,
            redis_dsn=self._redis_dsn,
            module_name=self._module_name,
            policy=MasterPolicy(
                workers=self._config.workers,
                worker=WorkerPolicy(
                    queues=self._config.queues,
                    concurrency=self._config.concurrency,
                    task_timeout=600.0,
                    max_attempts=self._config.max_attempts,
                    read_count=10,
                    poll_interval=0.1,
                    heartbeat_interval=5.0,
                    reclaim_interval=30.0,
                ),
            ),
        )

        try:
            await self.arun()

            # auto register cron and interval tasks
            self._register_interval_cron_tasks()

            async with anyio.create_task_group() as tg:
                tg.start_soon(master.arun)
                yield self
                tg.cancel_scope.cancel()

        finally:
            try:
                await master.aclose()
            finally:
                await self.aclose()


class BinomicFactory:
    """Binomic working client factory."""

    def __init__(
        self,
        broker_dsn: str,
        redis_dsn: str,
        module_name: str,
        config: "BinomicConfig",
    ) -> None:

        self._broker_dsn = broker_dsn
        self._redis_dsn = redis_dsn
        self._module_name = module_name
        self._config = config

        self._binomic: Binomic | None = None

    def create(self) -> "Binomic":
        """Return the client, creating it on the first call."""

        if self._binomic is None:
            self._binomic = Binomic(
                broker_dsn=self._broker_dsn,
                redis_dsn=self._redis_dsn,
                module_name=self._module_name,
                config=self._config,
            )

        return self._binomic
