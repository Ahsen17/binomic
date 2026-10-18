import pytest

from binomic.broker import BrokerFactory, RedisBroker


class TestBrokerFactory:
    def test_redis_dispatch_creates_redis_broker(self) -> None:

        broker = BrokerFactory(
            dsn="redis://localhost:6379/0",
            queues=["default"],
            namespace="binomic",
        ).create()

        assert isinstance(broker, RedisBroker)
        assert broker._group == "binomic"

    def test_config_reaches_the_broker(self) -> None:

        broker = BrokerFactory(
            dsn="redis://localhost:6379/0",
            queues=["default"],
            namespace="ns",
        ).create()

        assert isinstance(broker, RedisBroker)
        assert broker._group == "ns"

    def test_amqp_is_not_implemented(self) -> None:

        with pytest.raises(NotImplementedError, match="AMQP"):
            BrokerFactory(
                dsn="amqp://localhost",
                queues=["default"],
                namespace="binomic",
            ).create()

    def test_unknown_scheme_is_rejected(self) -> None:

        with pytest.raises(ValueError, match="Unsupported broker type"):
            BrokerFactory(
                dsn="kafka://localhost",
                queues=["default"],
                namespace="binomic",
            ).create()
