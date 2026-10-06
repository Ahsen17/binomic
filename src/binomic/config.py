from binomic.base import BaseStruct

__all__ = ("BinomicConfig",)


class BinomicConfig(BaseStruct):
    """Binomic configuration.

    ``workers`` is the number of worker subprocesses the master spawns and
    ``concurrency`` the number of tasks each worker executes concurrently;
    every stream in ``queues`` is consumed by all workers.
    """

    queues: list[str]
    workers: int = 1
    concurrency: int = 5
