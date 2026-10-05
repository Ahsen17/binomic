from collections.abc import Sequence
from typing import TYPE_CHECKING, Final, cast

from redis.exceptions import ResponseError

from binomic.base.constants import APP_NAME
from binomic.message import Message

from .protocols import Broker

if TYPE_CHECKING:
    from uuid import UUID

    from redis.asyncio import Redis as AsyncRedis

    from .types import Entry, Fields


__all__ = ("AsyncredisBroker",)


GROUP_NAMESPACE: Final[str] = APP_NAME


class AsyncredisBroker(Broker):
    """Broker based on async redis."""

    def __init__(
        self,
        client: "AsyncRedis",
        queues: Sequence[str],
        *,
        namespace_group: str = GROUP_NAMESPACE,
    ) -> None:

        self._client = client
        self._queues = queues
        self._group = namespace_group

    def get_stream_key(self, queue: str) -> str:
        """Namespace a queue name into its Redis stream key."""

        return f"{self._group}:{queue}"

    async def initialize(self) -> None:

        for queue in self._queues:
            try:
                await self._client.xgroup_create(
                    self.get_stream_key(queue),
                    groupname=self._group,
                    id="0",
                    mkstream=True,
                )

            except ResponseError as err:
                if "BUSYGROUP" not in str(err):
                    raise

    async def enqueue(self, msg: "Message") -> "UUID":

        await self._client.xadd(
            name=self.get_stream_key(msg.queue),
            fields={
                "id": str(msg.id),
                "message": msg.to_json(),
            },
        )

        return msg.id

    async def fetch(self, consumer: str, *, count: int) -> list["Entry"]:

        result = await self._client.xreadgroup(
            groupname=self._group,
            consumername=consumer,
            streams={self.get_stream_key(q): ">" for q in self._queues},
            count=count,
        )

        entries: list[Entry] = []
        if not result:
            return entries

        pairs = result.items() if isinstance(result, dict) else result
        queue_by_key = {self.get_stream_key(q): q for q in self._queues}

        for key, msgs in pairs:
            queue = queue_by_key[key]
            for msg_id, fields in msgs:
                entries.append(
                    (queue, cast("str", msg_id), cast("Fields", fields)),
                )

        return entries

    async def ack(self, entry: "Entry") -> int:

        queue, msg_id, _ = entry
        return await self._client.xack(
            self.get_stream_key(queue),
            self._group,
            msg_id,
        )

    async def reclaim(
        self,
        consumer: str,
        *,
        min_idle_ms: int,
        count: int = 100,
    ) -> int:

        reclaimed = 0
        for queue in self._queues:
            try:
                result = await self._client.xautoclaim(
                    name=self.get_stream_key(queue),
                    groupname=self._group,
                    consumername=consumer,
                    min_idle_ms=min_idle_ms,
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
                msg = Message.from_json(fields.get("message"))
                await self.enqueue(msg)
                await self.ack((queue, msg_id, fields))

                reclaimed += 1

        return reclaimed
