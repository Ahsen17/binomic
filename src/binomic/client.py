import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Self

import anyio
from anyio import AsyncContextManagerMixin
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from binomic.broker import Broker, BrokerFactory
from binomic.message import Message
from binomic.task import autodiscover
from binomic.task.registry import registry
from binomic.worker import Master, MasterPolicy, WorkerPolicy

if TYPE_CHECKING:
    from uuid import UUID

    from binomic.config import BinomicConfig
    from binomic.task import TaskSpec


__all__ = (
    "Binomic",
    "BinomicFactory",
)


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
        self._scheduler: AsyncIOScheduler | None = None

    async def arun(self) -> None:

        if self._broker is None:
            self._broker = BrokerFactory(
                self._broker_dsn,
                self._config.queues,
            )()

            await self._broker.initialize()

        if self._scheduler is None:
            self._scheduler = AsyncIOScheduler()
            self._scheduler.start()

        autodiscover(self._module_name)

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

        # Stamped here rather than at construction: delay waits for its deadline and
        # cron reuses one Message across firings.
        msg.enqueued_at = time.time()

        await self._broker.enqueue(queue, msg)

    def _delay(self, spec: "TaskSpec", msg: "Message") -> None:

        if spec.mode != "delay" or spec.delay is None:
            raise ValueError("Message is not a delay task.")

        if self._scheduler is None:
            raise RuntimeError("Run client `arun` before submitting messages.")

        self._scheduler.add_job(
            func=self._enqueue,
            trigger="date",
            run_date=datetime.now(UTC) + timedelta(seconds=spec.delay),
            args=(spec.queue, msg),
        )

    def _cron(self, spec: "TaskSpec") -> None:

        if spec.mode != "cron" or spec.cron is None:
            raise ValueError("Message is not a cron task.")

        if self._scheduler is None:
            raise RuntimeError("Run client `arun` before submitting messages.")

        self._scheduler.add_job(
            func=self._enqueue,
            trigger=CronTrigger.from_crontab(spec.cron, timezone=UTC),
            args=(spec.queue, Message(name=spec.name)),
        )

    async def submit(self, msg: "Message") -> "UUID":
        """Enqueue a message to the broker stream."""

        spec = registry.get(msg.name)

        match spec.mode:
            case "direct":
                await self._enqueue(spec.queue, msg)

            case "delay":
                self._delay(spec, msg)

            case _:
                raise ValueError(
                    "Invalid task mode, `submit` only supports "
                    f"direct and delay tasks. Received `{spec.mode}` task."
                )

        return msg.id

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
                ),
            ),
        )

        try:
            await self.arun()

            # auto register cron tasks
            for spec in registry:
                if spec.mode == "cron":
                    self._cron(spec)

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
