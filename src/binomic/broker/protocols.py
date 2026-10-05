from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from uuid import UUID

    from binomic.message import Message

    from .types import Entry


__all__ = ("Broker",)


class Broker(Protocol):
    """Nomic broker protocol."""

    async def initialize(self) -> None: ...

    async def enqueue(self, message: "Message") -> "UUID": ...

    async def fetch(self, consumer: str, *, count: int) -> list["Message"]: ...

    async def ack(self, entry: "Entry") -> int: ...

    async def reclaim(self, consumer: str, *, min_idle_ms: int) -> int: ...
