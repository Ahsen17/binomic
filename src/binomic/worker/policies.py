from collections.abc import Callable

from binomic.base import BaseStruct

__all__ = (
    "MasterPolicy",
    "WorkerPolicy",
)


class WorkerPolicy(BaseStruct):
    """Settings for one worker subprocess.

    ``queues`` are consumed by every worker and ``concurrency`` bounds the
    tasks one worker executes at once. ``task_timeout`` is the total delivery
    deadline from enqueue; ``max_attempts`` is the retry budget before the
    message enters the DLQ. ``read_count`` and ``poll_interval`` shape the
    fetch loop; ``heartbeat_interval`` and ``reclaim_interval`` set the
    liveness beat and crash-recovery cadence. ``dlq_retention`` is the age
    after which the periodic sweep deletes DLQ records (``None`` disables it).

    ``worker_init`` runs once per subprocess, in ``Worker.run`` before the
    event loop starts — the hook where an embedding application installs its
    OpenTelemetry SDK providers. ``otel_enabled`` gates every telemetry call:
    with the default ``False`` binomic emits no spans, metrics and never
    touches the envelope's trace fields.
    """

    queues: list[str]
    concurrency: int

    task_timeout: float = 600.0
    max_attempts: int = 3
    read_count: int = 10
    poll_interval: float = 0.1
    heartbeat_interval: float = 5.0
    reclaim_interval: float = 30.0

    dlq_retention: float | None = 7 * 86400.0

    worker_init: Callable[[], None] | None = None
    otel_enabled: bool = False


class MasterPolicy(BaseStruct):
    """Settings for the master process.

    ``workers`` is how many worker subprocesses to spawn, each configured by
    the same ``worker_policy``.
    """

    workers: int
    worker_policy: WorkerPolicy
