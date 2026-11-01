from binomic.broker import (
    Broker,
    BrokerLifecycleCapability,
    BrokerTelemetryCapability,
    RedisBroker,
)


class DuckBroker:
    """Satisfies the broker surface by shape alone — no inheritance."""

    async def enqueue(self, queue: object, message: object) -> None: ...

    async def acquire(self, consumer: object, count: object) -> None: ...

    async def ack(self, entry: object) -> None: ...

    async def dead(self, entry: object) -> None: ...

    async def retry(self, entry: object) -> None: ...

    async def reclaim(
        self, consumer: object, min_idle_ms: object, count: object, gc_idle_ms: object
    ) -> None: ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...


class TestProtocols:
    def test_are_runtime_checkable(self) -> None:

        assert isinstance(DuckBroker(), BrokerLifecycleCapability)
        assert isinstance(DuckBroker(), Broker)
        assert not isinstance(DuckBroker(), BrokerTelemetryCapability)
        assert not isinstance(object(), BrokerLifecycleCapability)

    def test_redis_broker_satisfies_both(self) -> None:

        broker = RedisBroker(
            dsn="redis://localhost:6379/0",
            queues=["default"],
            namespace="binomic",
        )

        assert isinstance(broker, Broker)
        assert isinstance(broker, BrokerLifecycleCapability)
        assert isinstance(broker, BrokerTelemetryCapability)
