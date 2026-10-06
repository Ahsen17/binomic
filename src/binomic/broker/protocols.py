from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from uuid import UUID

    from binomic.message import Message

    from .types import Entry


__all__ = ("Broker",)


class Broker(Protocol):
    """Broker protocol class."""

    async def initialize(self) -> None: ...

    async def enqueue(self, msg: "Message") -> "UUID": ...

    async def acquire(self, consumer: str, *, count: int) -> list["Entry"]: ...

    async def ack(self, entry: "Entry") -> int: ...

    async def reclaim(self, consumer: str, *, min_idle_ms: int, count: int) -> int: ...

    async def aclose(self) -> None: ...
