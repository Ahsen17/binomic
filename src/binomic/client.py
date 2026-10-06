from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Self

import anyio
from anyio import AsyncContextManagerMixin

from binomic.broker import Broker, BrokerFactory
from binomic.worker import Master, MasterPolicy, WorkerPolicy

if TYPE_CHECKING:
    from binomic.config import BinomicConfig
    from binomic.message import Message


__all__ = (
    "Binomic",
    "BinomicFactory",
)


class Binomic(AsyncContextManagerMixin):
    """Binomic working client."""

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

    async def submit(self, msg: "Message") -> None:

        if self._broker is None:
            self._broker = BrokerFactory(
                self._broker_dsn,
                self._config.queues,
            )()

        await self._broker.enqueue(msg)

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
            async with anyio.create_task_group() as tg:
                tg.start_soon(master.arun)
                yield self
                tg.cancel_scope.cancel()

        finally:
            await master.aclose()


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

        if self._binomic is None:
            self._binomic = Binomic(
                broker_dsn=self._broker_dsn,
                redis_dsn=self._redis_dsn,
                module_name=self._module_name,
                config=self._config,
            )

        return self._binomic
