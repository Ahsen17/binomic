from typing import Any
from uuid import UUID

from msgspec import field, json
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct, DeserializationError

__all__ = ("Message",)


class Message(BaseStruct):
    """Message for binomic.

    ``name`` is the registered task name and ``args`` / ``kwargs`` carry the
    task call arguments. The delivery timestamp is not carried here: the broker
    stamps it on the stream entry when the message is enqueued.
    """

    name: str
    id: "UUID" = field(default_factory=uuid7)

    attempt: int = field(default=1)
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:

        return json.encode(
            {
                "id": str(self.id),
                "name": self.name,
                "attempt": self.attempt,
                "args": self.args,
                "kwargs": self.kwargs,
            },
            order="sorted",
        ).decode("utf-8")

    @classmethod
    def from_json(cls, json_str: str) -> "Message":

        try:
            data = json.decode(json_str)

            return cls(
                id=UUID(data["id"]),
                name=data["name"],
                attempt=data["attempt"],
                args=data["args"],
                kwargs=data["kwargs"],
            )

        except Exception as exc:
            raise DeserializationError(
                f"Failed to deserialize data structure: {exc}",
            ) from exc
