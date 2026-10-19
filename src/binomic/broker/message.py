from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

from msgspec import field
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct

__all__ = (
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
