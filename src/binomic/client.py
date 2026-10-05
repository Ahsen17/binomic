from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from multiprocessing import get_context
from typing import TYPE_CHECKING, Self

import anyio
from anyio import AsyncContextManagerMixin

from binomic.broker import AsyncredisBroker, AsyncredisFactory
from binomic.worker import Master, MasterPolicy

if TYPE_CHECKING:
    from binomic.message import Message


__all__ = ("Binomic",)


class Binomic(AsyncContextManagerMixin):
    """Binomic working client."""

    def __init__(
        self,
        redis_dsn: str,
        queues: list[str],
        workers: int,
        concurrency: int,
    ) -> None:

        self._redis_dsn = redis_dsn
        self._queues = queues
        self._workers = workers
        self._concurrency = concurrency

        self._redis_factory = AsyncredisFactory(redis_dsn)
        self._broker = AsyncredisBroker(
            self._redis_factory.from_pool(),
            queues=queues,
        )
        self._ctx = get_context("fork")

    async def submit(self, msg: "Message") -> None:

        await self._broker.enqueue(msg)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:

        master = Master(
            redis_dsn=self._redis_dsn,
            module_name=__package__,
            policy=MasterPolicy(
                workers=self._workers,
                queues=self._queues,
                concurrency=self._concurrency,
            ),
        )

        try:
            proc = self._ctx.Process(
                target=anyio.run,
                args=(master.arun,),
            )

            proc.start()

            yield self

        finally:
            if proc.is_alive():
                proc.terminate()
                proc.join()
