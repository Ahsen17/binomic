import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Final, cast

import structlog
from redis.asyncio import BlockingConnectionPool
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import ResponseError

from binomic.base import DeserializationError
from binomic.base.constants import APP_NAME
from binomic.message import Message

from .protocols import Broker
from .types import Entry

if TYPE_CHECKING:
    from uuid import UUID

    from .types import Fields


__all__ = ("AsyncredisBroker",)


GROUP_NAMESPACE: Final[str] = APP_NAME


logger = structlog.stdlib.get_logger(__name__)


class AsyncredisBroker(Broker):
    """Broker based on async redis."""

    def __init__(
        self,
        dsn: str,
        queues: Sequence[str],
        **config: Any,
    ) -> None:

        self._dsn = dsn
        self._queues = queues

        self._config = config
        self._group = GROUP_NAMESPACE

        self._client: AsyncRedis | None = None

    def get_stream_key(self, queue: str) -> str:
        """Namespace a queue name into its Redis stream key."""

        return f"{self._group}:{queue}"

    @property
    def client(self) -> "AsyncRedis":

        if self._client is None:
            self._client = AsyncRedis.from_pool(
                BlockingConnectionPool.from_url(
                    url=self._dsn,
                    **self._config,
                ),
            )

        return self._client

    async def initialize(self) -> None:

        for queue in self._queues:
            try:
                await self.client.xgroup_create(
                    self.get_stream_key(queue),
                    groupname=self._group,
                    id="0",
                    mkstream=True,
                )

            except ResponseError as err:
                if "BUSYGROUP" not in str(err):
                    raise

    async def enqueue(self, queue: str, msg: "Message") -> "UUID":

        await self.client.xadd(
            name=self.get_stream_key(queue),
            fields={
                "id": str(msg.id),
                "message": msg.to_json(),
                "enqueued_at": time.time(),
            },
        )

        return msg.id

    async def acquire(self, consumer: str, *, count: int) -> list["Entry"]:

        result = await self.client.xreadgroup(
            groupname=self._group,
            consumername=consumer,
            streams={self.get_stream_key(q): ">" for q in self._queues},
            count=count,
        )

        entries: list[Entry] = []
        if not result:
            return entries

        queue_by_key = {self.get_stream_key(q): q for q in self._queues}

        for key, msgs in cast(
            "list[tuple[str, list[tuple[str, Fields]]]]",
            result.items() if isinstance(result, dict) else result,
        ):
            queue = queue_by_key[key]
            for msg_id, fields in msgs:
                # Redis hands stream fields back as text, while the worker does
                # arithmetic on this one.
                fields["enqueued_at"] = float(fields["enqueued_at"])

                entries.append(Entry(queue, msg_id, fields))

        return entries

    async def ack(self, entry: "Entry") -> int:

        queue, msg_id, _ = entry
        return await self.client.xack(
            self.get_stream_key(queue),
            self._group,
            msg_id,
        )

    async def reclaim(
        self,
        consumer: str,
        *,
        min_idle_ms: int,
        count: int,
    ) -> int:

        reclaimed = 0
        for queue in self._queues:
            try:
                result = await self.client.xautoclaim(
                    name=self.get_stream_key(queue),
                    groupname=self._group,
                    consumername=consumer,
                    min_idle_time=min_idle_ms,
                    start_id="0-0",
                    count=count,
                )

            except ResponseError as err:
                if "NOGROUP" in str(err):
                    continue

                raise

            msgs = result[1] if result else []
            for msg_id, fields in cast(
                "list[tuple[str, Fields]]",
                msgs,
            ):
                try:
                    msg = Message.from_json(fields["message"])

                except DeserializationError as err:
                    # An unreadable payload can never be executed, and raising
                    # here would kill the worker loop it is reclaimed for.
                    logger.warning("Dropping unreadable message %s: %s", msg_id, err)

                    await self.ack(Entry(queue, msg_id, fields))
                    # TODO: send to DLQ
                    continue

                # Re-delivery is a new submission: re-enqueueing stamps a fresh
                # `enqueued_at`, so the reclaimed copy is not dropped as expired
                # the moment it arrives.
                msg.attempt += 1

                await self.enqueue(queue, msg)
                await self.ack(Entry(queue, msg_id, fields))

                reclaimed += 1

        return reclaimed

    async def aclose(self) -> None:

        if self._client is not None:
            await self._client.aclose()
            self._client = None
