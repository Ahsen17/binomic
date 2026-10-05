from binomic.base import BaseStruct

__all__ = (
    "MasterPolicy",
    "WorkerPolicy",
)


class MasterPolicy(BaseStruct):
    """Binomic master policy."""

    workers: int

    # worker general policy
    queues: list[str]
    concurrency: int
    task_timeout: float = 600.0
    read_count: int = 10
    poll_interval: float = 0.1
    heartbeat_interval: float = 5.0


class WorkerPolicy(BaseStruct):
    """Binomic worker policy."""

    consumer: str
    queues: list[str]
    concurrency: int

    task_timeout: float = 600.0
    read_count: int = 10
    poll_interval: float = 0.1
    heartbeat_interval: float = 5.0
