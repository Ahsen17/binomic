from collections.abc import Callable

from binomic.base import BaseStruct

__all__ = (
    "MasterPolicy",
    "WorkerPolicy",
)


class WorkerPolicy(BaseStruct):
    """Bonimic worker policy.

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

    worker_init: Callable[[], None] | None = None
    otel_enabled: bool = False


class MasterPolicy(BaseStruct):
    """Bonimic master policy."""

    workers: int
    worker_policy: WorkerPolicy
