from binomic.base import BaseStruct

__all__ = ("BinomicConfig",)


class BinomicConfig(BaseStruct):
    """Binomic configuration.

    ``workers`` is the number of worker subprocesses the master spawns and
    ``concurrency`` the number of tasks each worker executes concurrently;
    every stream in ``queues`` is consumed by all workers.
    ``max_attempts`` is how many times a message may be delivered before it is
    dropped.
    """

    queues: list[str]

    workers: int = 1
    concurrency: int = 5

    max_attempts: int = 3
