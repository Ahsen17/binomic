from binomic.base import BaseStruct

__all__ = ("BinomicConfig",)


class BinomicConfig(BaseStruct):
    """Binomic configuration."""

    queues: list[str]
    workers: int = 1
    concurrency: int = 5
