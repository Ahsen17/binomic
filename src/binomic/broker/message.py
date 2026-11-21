from collections.abc import Iterable, Mapping
from typing import Any, Self
from uuid import UUID

from msgspec import field
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct

__all__ = (
    "DeadLetterEntry",
    "Message",
    "MessageEntry",
)


class Message(BaseStruct, kw_only=True):
    """The payload submitted to a queue.

    ``task`` names the registered task; the queue comes from that task's
    declaration, never from the message. ``attempt`` starts at 1 and is
    incremented by the broker on retry and reclaim, so it survives
    redelivery because it is serialized with the payload.
    """

    id: UUID = field(default_factory=uuid7)
    task: str
    attempt: int = 1

    args: Iterable[Any] | None = None
    kwargs: Mapping[str, Any] | None = None


class MessageEntry(BaseStruct):
    """A message plus its delivery envelope, as stored by the broker.

    ``id`` is the stream entry id, ``queue`` the owning queue, and
    ``enqueued_at`` the enqueue time in seconds. Retry keeps the original
    stamp (the deadline is measured from first delivery); reclaim rewrites it.
    """

    id: str
    queue: str

    message: Message
    enqueued_at: float


class DeadLetterEntry(MessageEntry):
    """A DLQ record: a message entry plus when it entered the DLQ.

    ``dead_at`` is derived from the DLQ stream entry id, whose prefix is the
    Redis server timestamp in milliseconds.
    """

    dead_at: float

    @classmethod
    def from_entry(cls, entry: MessageEntry) -> Self:
        """Build a DLQ record from ``entry``, deriving ``dead_at`` from its id."""

        return cls(
            id=entry.id,
            queue=entry.queue,
            message=entry.message,
            enqueued_at=entry.enqueued_at,
            dead_at=float(entry.id.split("-")[0]) / 1000,
        )
