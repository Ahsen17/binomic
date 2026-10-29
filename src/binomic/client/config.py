from collections.abc import Callable

from binomic.base import BaseStruct

__all__ = ("BinomicConfig",)


class BinomicConfig(BaseStruct):
    """Binomic configuration.

    ``workers`` is the number of worker subprocesses the master spawns and
    ``concurrency`` the number of tasks each worker executes concurrently;
    every stream in ``queues`` is consumed by all workers.
    ``max_attempts`` is how many times a message may be delivered before it is
    dropped. ``task_timeout`` is a message's total delivery deadline in
    seconds; ``read_count`` and ``poll_interval`` shape each worker's fetch
    loop; ``heartbeat_interval`` and ``reclaim_interval`` set the liveness
    beat and crash-recovery cadence.
    ``capacity`` caps every queue's unacknowledged total (its stream length:
    messages stay in the stream from submission until ack or dead), so
    submissions onto a full queue are rejected immediately; unlimited by
    default. Retry and reclaim re-entries keep the total constant and never
    consume capacity.
    ``worker_init`` runs once per worker subprocess before its event loop
    starts (the hook for installing OpenTelemetry SDK providers), and
    ``otel_enabled`` gates all telemetry — with the default ``False`` binomic
    emits nothing and leaves message envelopes untouched. Structlog logs are
    not affected by the switch.
    """

    queues: list[str]

    workers: int = 1
    concurrency: int = 5

    max_attempts: int = 3

    task_timeout: float = 600.0
    read_count: int = 10
    poll_interval: float = 0.1
    heartbeat_interval: float = 5.0
    reclaim_interval: float = 30.0

    capacity: int | None = None

    worker_init: Callable[[], None] | None = None
    otel_enabled: bool = False

    def __post_init__(self) -> None:

        if self.capacity is not None and self.capacity < 0:
            raise ValueError(
                f"`capacity` must be None or a non-negative integer, got {self.capacity}",
            )
