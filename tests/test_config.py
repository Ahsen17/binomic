import pytest

from binomic.base import BaseStruct
from binomic.client.config import BinomicConfig


class TestBinomicConfig:
    def test_requires_queues(self) -> None:

        config = BinomicConfig(queues=["default"])

        assert config.queues == ["default"]
        assert config.workers == 1
        assert config.concurrency == 5
        assert config.max_attempts == 3

    def test_accepts_overrides(self) -> None:

        config = BinomicConfig(
            queues=["a", "b"],
            workers=3,
            concurrency=10,
            max_attempts=5,
        )

        assert config.workers == 3
        assert config.concurrency == 10
        assert config.max_attempts == 5

    def test_capacity_defaults_to_unlimited(self) -> None:

        assert BinomicConfig(queues=["default"]).capacity is None

    def test_accepts_capacity(self) -> None:

        assert BinomicConfig(queues=["default"], capacity=10).capacity == 10
        assert BinomicConfig(queues=["default"], capacity=0).capacity == 0

    def test_rejects_negative_capacity(self) -> None:

        with pytest.raises(ValueError, match="capacity"):
            BinomicConfig(queues=["default"], capacity=-1)

    def test_is_base_struct(self) -> None:

        assert issubclass(BinomicConfig, BaseStruct)
