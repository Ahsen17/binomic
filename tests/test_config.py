from binomic.base import BaseStruct
from binomic.config import BinomicConfig


class TestBinomicConfig:
    def test_requires_queues(self) -> None:

        config = BinomicConfig(queues=["default"])

        assert config.queues == ["default"]
        assert config.workers == 1
        assert config.concurrency == 5

    def test_accepts_overrides(self) -> None:

        config = BinomicConfig(queues=["a", "b"], workers=3, concurrency=10)

        assert config.workers == 3
        assert config.concurrency == 10

    def test_is_base_struct(self) -> None:

        assert issubclass(BinomicConfig, BaseStruct)
