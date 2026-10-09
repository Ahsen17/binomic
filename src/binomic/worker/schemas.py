from binomic.base import BaseStruct

__all__ = (
    "MasterPolicy",
    "WorkerPolicy",
)


class WorkerPolicy(BaseStruct):
    """Binomic worker policy."""

    queues: list[str]
    concurrency: int

    task_timeout: float = 600.0
    max_attempts: int = 3
    read_count: int = 10
    poll_interval: float = 0.1
    heartbeat_interval: float = 5.0
    reclaim_interval: float = 30.0


class MasterPolicy(BaseStruct):
    """Binomic master policy."""

    workers: int
    worker: WorkerPolicy
