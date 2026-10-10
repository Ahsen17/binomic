import pytest

from binomic.broker.handlers import BrokerFactory
from binomic.broker.redis import AsyncredisBroker


class TestBrokerFactory:
    def test_redis_dsn_builds_asyncredis_broker(self) -> None:

        broker = BrokerFactory("redis://localhost:6379/0", ["default"]).create()

        assert isinstance(broker, AsyncredisBroker)

    def test_passes_queues_to_broker(self) -> None:

        broker = BrokerFactory("redis://localhost:6379/0", ["a", "b"]).create()

        assert isinstance(broker, AsyncredisBroker)
        assert broker._queues == ["a", "b"]

    def test_amqp_dsn_is_not_implemented(self) -> None:

        with pytest.raises(NotImplementedError, match="AMQP"):
            BrokerFactory("amqp://localhost:5672", ["default"]).create()

    def test_unknown_scheme_is_rejected(self) -> None:

        with pytest.raises(ValueError, match="Unsupported broker type"):
            BrokerFactory("kafka://localhost:9092", ["default"]).create()
