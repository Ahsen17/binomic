from typing import Any
from uuid import UUID

from msgspec import field, json
from uuid_utils.compat import uuid7

from binomic.base import BaseStruct

__all__ = ("Message",)


class Message(BaseStruct):
    """Message for binomic.

    ``name`` is the registered task name, ``queue`` the target stream and
    ``enqueued_at`` the submission timestamp; ``args`` and ``kwargs`` carry
    the task call arguments.
    """

    name: str
    id: "UUID" = field(default_factory=uuid7)

    enqueued_at: float | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:

        return json.encode(
            {
                "id": str(self.id),
                "name": self.name,
                "enqueued_at": self.enqueued_at,
                "args": self.args,
                "kwargs": self.kwargs,
            },
            order="sorted",
        ).decode("utf-8")

    @classmethod
    def from_json(cls, json_str: str) -> "Message":

        data = json.decode(json_str)

        return cls(
            id=UUID(data["id"]),
            name=data["name"],
            enqueued_at=data["enqueued_at"],
            args=data["args"],
            kwargs=data["kwargs"],
        )
