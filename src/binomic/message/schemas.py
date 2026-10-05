from typing import Any
from uuid import UUID

from msgspec import field, json

from binomic.base import BaseStruct

__all__ = ("Message",)


class Message(BaseStruct):
    """Message for binomic."""

    id: "UUID"
    name: str
    queue: str

    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:

        return json.encode(
            {
                "id": str(self.id),
                "name": self.name,
                "queue": self.queue,
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
            queue=data["queue"],
            args=data["args"],
            kwargs=data["kwargs"],
        )
