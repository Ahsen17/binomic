from collections.abc import Iterable, Mapping
from typing import Any, Self
from uuid import UUID

from msgspec import field
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct

__all__ = (
    "DLQRecordEntry",
    "Message",
    "MessageEntry",
)


class Message(BaseStruct):
    """The unit of work submitted to a queue.

    ``task`` names the registered task to run; ``args``/``kwargs`` carry its
    call arguments. ``attempt`` rides the payload so the retry count survives
    round trips through Redis; ``id`` is a UUIDv7 primary key. The delivery
    timestamp is not carried here: the broker stamps it on the entry when
    the message is enqueued.
    """

    task: str
    attempt: int = 1
    id: UUID = field(default_factory=uuid7)
    args: Iterable[Any] | None = None
    kwargs: Mapping[str, Any] | None = None


class MessageEntry(BaseStruct):
    """The consumer-side envelope of a parked message.

    ``id`` is the Redis stream entry id, ``message`` the payload, and
    ``enqueued_at`` the delivery stamp in (fractional) seconds — the age a
    worker compares against ``task_timeout``.
    """

    id: str
    queue: str
    message: Message
    enqueued_at: float


class DLQRecordEntry(MessageEntry):
    """Read-only view of one DLQ record.

    ``id`` (inherited) is the DLQ stream entry id — the ``:dlq:hash`` key and
    the handle for reads, replays and pruning. ``dead_at`` is the moment the
    message entered the DLQ, derived from the entry id's millisecond stamp
    (Redis server time at ``XADD``) and never stored.
    """

    dead_at: float

    @classmethod
    def from_entry(cls, entry: "MessageEntry") -> Self:
        """Build a record from a DLQ hash envelope read as a MessageEntry."""

        return cls(
            id=entry.id,
            queue=entry.queue,
            message=entry.message,
            enqueued_at=entry.enqueued_at,
            dead_at=float(entry.id.split("-")[0]) / 1000,
        )
