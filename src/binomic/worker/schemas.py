from binomic.base import BaseStruct

__all__ = ("WorkerPolicy",)


class WorkerPolicy(BaseStruct):
    """Binomic worker policy."""

    consumer: str
    concurrency: int

    task_timeout: float = 600.0
    read_count: int = 10
    poll_interval: float = 0.1
    heatbeat_interval: float = 5.0
