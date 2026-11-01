import time
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, cast

import msgspec
import structlog
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import ResponseError

from binomic.broker.exceptions import DLQEntryNotFoundError, QueueCapacityError
from binomic.broker.message import DLQRecordEntry, Message, MessageEntry
from binomic.broker.protocols import (
    Broker,
    BrokerDlqCapability,
    BrokerLifecycleCapability,
    BrokerTelemetryCapability,
)

from .schemas import ScriptResponse
from .scripts import (
    _ACK,
    _ACQUIRE,
    _CONTEXT,
    _DEAD,
    _DLQ_PRUNE,
    _ENQUEUE,
    _OBSERVE,
    _RECLAIM,
    _RETRY,
)

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript
    from redis.typing import EncodableT, KeyT


__all__ = ("RedisBroker",)


SCRIPTS: Final[dict[str, str]] = {
    "enqueue": _ENQUEUE,
    "acquire": _ACQUIRE,
    "ack": _ACK,
    "dead": _DEAD,
    "retry": _RETRY,
    "reclaim": _RECLAIM,
    "observe": _OBSERVE,
    "context": _CONTEXT,
    "dlq_prune": _DLQ_PRUNE,
}

_DLQ_LIST_LIMIT_MAX: Final[int] = 200
_DLQ_PRUNE_BATCH: Final[int] = 1000

logger = structlog.stdlib.get_logger(__name__)


class RedisBroker(
    Broker,
    BrokerLifecycleCapability,
    BrokerTelemetryCapability,
    BrokerDlqCapability,
):
    """Redis Streams broker backed by atomic Lua scripts.

    ``namespace`` is both the key prefix (``{namespace}:{queue}:stream``) and
    the consumer group name. Payloads live in a per-queue hash while stream
    entries carry only the message id; dead messages land in ``:dlq:stream``
    plus ``:dlq:hash``.
    ``capacity`` caps every queue's unacknowledged total (the stream length);
    unlimited by default. It stays an explicit constructor parameter, kept
    out of ``**config``: everything there feeds the Redis connection pool, so
    the broker's own options do not compete for that namespace.
    """

    def __init__(
        self,
        dsn: str,
        queues: Sequence[str],
        namespace: str,
        capacity: int | None = None,
        **config: Any,
    ) -> None:

        if capacity is not None and capacity < 0:
            raise ValueError(
                f"`capacity` must be None or a non-negative integer, got {capacity}"
            )

        self._dsn = dsn
        self._queues = queues
        self._group = namespace
        self._capacity = capacity
        self._config = config or {}

        if "decode_responses" not in self._config:
            self._config["decode_responses"] = True

        self._client: Redis | None = None
        self._scripts: dict[str, AsyncScript] = {}

        self._running = False

    @property
    def running(self) -> bool:

        return self._running

    async def start(self) -> None:

        if self._running:
            return

        self._scripts.update(
            {
                name: self._ensure_client().register_script(
                    script,
                )
                for name, script in SCRIPTS.items()
            }
        )

        await self.initialize()

        self._running = True

        logger.info(
            "Broker started",
            namespace=self._group,
            queues=list(self._queues),
        )

    async def close(self) -> None:

        if not self._running:
            return

        if self._client:
            await self._client.aclose(
                close_connection_pool=True,
            )
            self._client = None

        if self._scripts:
            self._scripts.clear()

        self._running = False

        logger.debug(
            "Broker closed",
            namespace=self._group,
            queues=list(self._queues),
        )

    def _ensure_client(self) -> "Redis":

        if self._client is None:
            self._client = Redis.from_pool(
                BlockingConnectionPool.from_url(
                    url=self._dsn,
                    **self._config,
                ),
            )

        return self._client

    async def _invoke_script(
        self,
        script: str,
        keys: Sequence["KeyT"],
        args: Iterable["EncodableT"],
    ) -> Any | None:

        if script not in self._scripts:
            raise KeyError(f"Script '{script}' not found.")

        resp = ScriptResponse.from_json(
            await self._scripts[script](
                client=self._ensure_client(),
                keys=keys,
                args=args,
            )
        )

        if resp.status == "rejected":
            detail = resp.result if isinstance(resp.result, dict) else None

            if detail is None or "current" not in detail or "capacity" not in detail:
                # A rejection without its detail is a broken envelope: treat
                # it as an internal failure rather than fabricating numbers.
                logger.warning(
                    "Rejected envelope without detail",
                    script=script,
                    keys=keys,
                    args=args,
                    result=resp.result,
                )

                return None

            raise QueueCapacityError(
                current=cast("int", detail["current"]),
                capacity=cast("int", detail["capacity"]),
            )

        if resp.status == "error":
            logger.warning(
                "Failed to invoke script",
                script=script,
                keys=keys,
                args=args,
                error=resp.error,
            )

            return None

        return resp.result

    def get_stream_key(self, queue: str) -> str:

        return f"{self._group}:{queue}:stream"

    def get_hash_key(self, queue: str) -> str:

        return f"{self._group}:{queue}:hash"

    def get_dlq_key(self, queue: str) -> str:

        return f"{self._group}:{queue}:dlq"

    def get_dlq_stream_key(self, queue: str) -> str:

        return f"{self._group}:{queue}:dlq:stream"

    def get_dlq_hash_key(self, queue: str) -> str:

        return f"{self._group}:{queue}:dlq:hash"

    async def initialize(self) -> None:

        for queue in self._queues:
            try:
                await self._ensure_client().xgroup_create(
                    name=self.get_stream_key(queue),
                    groupname=self._group,
                    id="0",
                    mkstream=True,
                )

            except ResponseError as err:
                if "BUSYGROUP" not in str(err):
                    raise

    async def enqueue(
        self,
        queue: str,
        message: "Message",
        context: "Mapping[str, str] | None" = None,
    ) -> "MessageEntry | None":
        """Enqueue a message, raising on a full queue.

        Raises ``QueueCapacityError`` when the queue's unacknowledged total
        has reached its effective capacity; internal script failures keep
        degrading to ``None``.
        """

        resp = await self._invoke_script(
            script="enqueue",
            keys=[],
            args=[
                self._group,
                queue,
                message.to_json(),
                time.time(),
                msgspec.json.encode(dict(context)) if context else b"",
                # Empty string encodes "unlimited" for the Lua capacity check.
                "" if self._capacity is None else self._capacity,
            ],
        )

        if resp is None:
            return None

        return MessageEntry.from_dict(cast("dict[str, Any]", resp))

    async def acquire(self, consumer: str, count: int) -> list["MessageEntry"]:

        resp = await self._invoke_script(
            script="acquire",
            keys=[],
            args=[self._group, consumer, count, *self._queues],
        )

        if resp is None:
            return []

        return [
            MessageEntry.from_dict(entry)
            for entry in cast(
                "list[dict[str, Any]]",
                resp,
            )
        ]

    async def ack(self, entry: "MessageEntry") -> int:

        resp = await self._invoke_script(
            script="ack",
            keys=[],
            args=[
                self._group,
                entry.queue,
                entry.id,
                str(entry.message.id),
            ],
        )

        return 0 if resp is None else cast("int", resp)

    async def dead(self, entry: "MessageEntry") -> int:

        resp = await self._invoke_script(
            script="dead",
            keys=[],
            args=[self._group, entry.queue, entry.id],
        )

        return 0 if resp is None else cast("int", resp)

    async def retry(self, entry: "MessageEntry") -> int:

        resp = await self._invoke_script(
            script="retry",
            keys=[],
            args=[
                self._group,
                entry.queue,
                entry.id,
                entry.enqueued_at,
            ],
        )

        return 0 if resp is None else cast("int", resp)

    async def reclaim(
        self,
        consumer: str,
        min_idle_ms: int,
        count: int,
        gc_idle_ms: int,
    ) -> int:

        resp = await self._invoke_script(
            script="reclaim",
            keys=[],
            args=[
                self._group,
                consumer,
                min_idle_ms,
                count,
                time.time(),
                gc_idle_ms,
                *self._queues,
            ],
        )

        return 0 if resp is None else cast("int", resp)

    async def trace_context(
        self,
        queue: str,
        message_id: str,
    ) -> "dict[str, str] | None":

        resp = await self._invoke_script(
            script="context",
            keys=[],
            args=[self._group, queue, message_id],
        )

        if not isinstance(resp, dict):
            return None

        carrier = cast("dict[str, Any]", resp)

        return {
            key: value
            for key, value in carrier.items()
            if isinstance(key, str) and isinstance(value, str)
        }

    async def observe(self, queue: str) -> "tuple[int, float | None] | None":

        resp = await self._invoke_script(
            script="observe",
            keys=[],
            args=[self._group, queue],
        )

        if resp is None:
            return None

        depth = cast("int", resp[0])

        # Lua `false` round-trips as JSON `false` -> Python `False`.
        oldest_ms = cast("float | None", resp[1] or None)

        if oldest_ms is None:
            return depth, None

        return depth, max(0.0, time.time() - oldest_ms / 1000)

    async def dlq_depth(self, queue: str) -> int:

        return await self._ensure_client().xlen(
            name=self.get_dlq_stream_key(queue),
        )

    async def dlq_list(
        self,
        queue: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> "tuple[list[DLQRecordEntry], str | None]":

        if not 1 <= limit <= _DLQ_LIST_LIMIT_MAX:
            raise ValueError(
                f"`limit` must be within [1, {_DLQ_LIST_LIMIT_MAX}], got {limit}"
            )

        client = self._ensure_client()

        # One extra stream entry tells whether a next page exists; the cursor
        # is the last returned entry id (an exclusive bound on the next page),
        # so hash-missing entries are skipped without duplicating or losing
        # stream positions.
        raw = cast(
            "list[tuple[str, Any]]",
            await client.xrange(
                name=self.get_dlq_stream_key(queue),
                min=f"({cursor}" if cursor is not None else "-",
                max="+",
                count=limit + 1,
            ),
        )

        more = len(raw) > limit
        raw = raw[:limit]

        records: list[DLQRecordEntry] = []

        for entry_id, _fields in raw:
            body = await client.hget(self.get_dlq_hash_key(queue), entry_id)

            if body is not None:
                records.append(
                    DLQRecordEntry.from_entry(
                        MessageEntry.from_json(cast("str", body)),
                    ),
                )

        return records, raw[-1][0] if more else None

    async def dlq_read(self, queue: str, entry_id: str) -> "DLQRecordEntry | None":

        body = await self._ensure_client().hget(
            name=self.get_dlq_hash_key(queue),
            key=entry_id,
        )

        if body is None:
            return None

        return DLQRecordEntry.from_entry(MessageEntry.from_json(cast("str", body)))

    async def dlq_replay(self, queue: str, entry_id: str) -> "MessageEntry | None":

        record = await self.dlq_read(queue, entry_id)

        if record is None:
            raise DLQEntryNotFoundError(queue=queue, entry_id=entry_id)

        message = Message(
            task=record.message.task,
            args=record.message.args,
            kwargs=record.message.kwargs,
        )

        entry = await self.enqueue(queue, message)

        if entry is not None:
            logger.info(
                "DLQ message replayed",
                queue=queue,
                entry_id=entry_id,
                message_id=str(entry.message.id),
            )

        return entry

    async def dlq_prune(self, queue: str, retention: float) -> int:

        if retention <= 0:
            raise ValueError(f"`retention` must be positive, got {retention}")

        cutoff_ms = int(time.time() * 1000 - retention * 1000)

        resp = await self._invoke_script(
            script="dlq_prune",
            keys=[],
            args=[self._group, queue, cutoff_ms, _DLQ_PRUNE_BATCH],
        )

        return 0 if resp is None else cast("int", resp)
