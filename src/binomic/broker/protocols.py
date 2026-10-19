from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .message import Message, MessageEntry

__all__ = (
    "Broker",
    "BrokerLifecycleMixin",
)


@runtime_checkable
class Broker(Protocol):
    """The broker surface the client and workers talk to.

    Every operation is an atomic Lua script; script-level failures degrade to
    empty results instead of raising, so a broker hiccup cannot kill the
    caller's loop.
    """

    async def enqueue(
        self,
        queue: str,
        message: "Message",
    ) -> "MessageEntry | None": ...

    async def acquire(
        self,
        consumer: str,
        count: int,
    ) -> list["MessageEntry"]: ...

    async def ack(self, entry: "MessageEntry") -> int: ...

    async def dead(self, entry: "MessageEntry") -> int: ...

    async def retry(self, entry: "MessageEntry") -> int: ...

    async def reclaim(
        self,
        consumer: str,
        min_idle_ms: int,
        count: int,
        gc_idle_ms: int,
    ) -> int: ...


@runtime_checkable
class BrokerLifecycleMixin(Protocol):
    """The start/stop half of the broker surface.

    Clients and workers bring a broker up through this mixin, so any
    implementation that wires its own connections on ``start`` fits the
    framework regardless of inheritance.
    """

    async def start(self) -> None: ...

    async def close(self) -> None: ...
