from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .message import Message, MessageEntry

__all__ = (
    "Broker",
    "BrokerLifecycleMixin",
    "BrokerTelemetryMixin",
)


@runtime_checkable
class Broker(Protocol):
    """The broker surface the client and workers talk to.

    Every operation is an atomic Lua script; script-level failures degrade to
    empty results instead of raising, so a broker hiccup cannot kill the
    caller's loop. The one expected outcome that is raised is a full queue:
    ``enqueue`` raises ``QueueCapacityError`` instead of degrading.
    """

    async def enqueue(
        self,
        queue: str,
        message: "Message",
        context: "Mapping[str, str] | None" = None,
    ) -> "MessageEntry | None":
        """Deliver a message to a queue.

        ``context`` is an internal telemetry carrier (W3C trace fields) the
        framework fills from the ambient trace context; submitters leave it
        unset. Broker implementations treat it as an opaque string map and
        store it apart from the message envelope.

        Raises ``QueueCapacityError`` when the queue's unacknowledged total
        has reached the capacity configured for it.
        """

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


@runtime_checkable
class BrokerTelemetryMixin(Protocol):
    """Optional observability probes a broker may implement.

    Kept out of ``Broker`` so custom brokers stay valid without them; the
    framework probes this capability and degrades (root spans, no queue
    gauges) when it is absent.
    """

    async def trace_context(
        self,
        queue: str,
        message_id: str,
    ) -> "dict[str, str] | None":
        """Read the delivery trace context stored for a message id.

        ``None`` means no context was stored or the read degraded; telemetry
        then falls back to a root span.
        """

    async def observe(self, queue: str) -> "tuple[int, float | None] | None":
        """Probe queue depth for observability, outside the delivery path.

        Returns ``(depth, oldest entry age in seconds)``; the age is ``None``
        on an empty stream and the whole result ``None`` when the probe
        degrades, so a hiccup never reads as an empty queue.
        """
