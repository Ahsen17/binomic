from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .message import DLQRecordEntry, Message, MessageEntry

__all__ = (
    "Broker",
    "BrokerDlqCapability",
    "BrokerLifecycleCapability",
    "BrokerTelemetryCapability",
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
class BrokerLifecycleCapability(Protocol):
    """The start/stop half of the broker surface.

    Clients and workers bring a broker up through this mixin, so any
    implementation that wires its own connections on ``start`` fits the
    framework regardless of inheritance.
    """

    async def start(self) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class BrokerTelemetryCapability(Protocol):
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


@runtime_checkable
class BrokerDlqCapability(Protocol):
    """Optional DLQ management surface a broker may implement.

    Kept out of ``Broker`` so custom brokers stay valid without it; callers
    (the ``Binomic`` convenience methods and the worker's prune sweep) probe
    for the capability with ``isinstance``. Reads are plain commands whose
    connection errors propagate — management is an explicit operation, not a
    delivery-path concern — while the prune script keeps the ``ScriptResponse``
    degrade contract.
    """

    async def dlq_depth(self, queue: str) -> int:
        """Return how many records the queue's DLQ currently holds."""

        ...

    async def dlq_list(
        self,
        queue: str,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[list["DLQRecordEntry"], str | None]:
        """Page through the DLQ oldest-first.

        Returns ``(records, next_cursor)``; ``next_cursor`` is ``None`` at
        the end of the stream, otherwise feed it back as ``cursor`` (an
        exclusive bound, so pages never repeat or skip). Entries whose hash
        record is missing are skipped, not errors.
        """

        ...

    async def dlq_read(self, queue: str, entry_id: str) -> "DLQRecordEntry | None":
        """Return one full record by DLQ stream entry id, or ``None``."""

        ...

    async def dlq_replay(self, queue: str, entry_id: str) -> "MessageEntry | None":
        """Re-deliver a DLQ record as a brand-new message.

        The replay carries a fresh UUID and ``attempt = 1`` (a full retry
        budget) and targets the record's original queue; the source record is
        kept. Raises ``DLQEntryNotFoundError`` when the id is unknown, and
        ``QueueCapacityError`` out of ``enqueue`` on a full queue; ``None``
        means the enqueue degraded (warning already logged).
        """

        ...

    async def dlq_prune(self, queue: str, retention: float) -> int:
        """Delete records that entered the DLQ more than ``retention`` seconds ago.

        The entry age comes from the DLQ stream entry id's millisecond stamp,
        so the payload is never decoded; at most one bounded batch is deleted
        per call. Returns the number of records deleted; script failures
        degrade to ``0``.
        """

        ...
