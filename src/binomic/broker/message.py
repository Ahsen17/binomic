from uuid import UUID

from msgspec import field
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct

__all__ = ("Message",)


class Message(BaseStruct):
    """Message for binomic.

    ``name`` is the registered task name and ``args`` / ``kwargs`` carry the
    task call arguments. The delivery timestamp is not carried here: the broker
    stamps it on the stream entry when the message is enqueued.
    """

    name: str
    id: UUID = field(default_factory=uuid7)
    attempt: int = field(default=1)
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
