from binomic.broker.protocols import Broker
from binomic.broker.redis import AsyncredisBroker


class TestBrokerProtocol:
    def test_declares_broker_surface(self) -> None:

        for method in ("initialize", "enqueue", "acquire", "ack", "reclaim", "aclose"):
            assert callable(getattr(Broker, method, None))

    def test_asyncredis_broker_satisfies_protocol(self) -> None:

        for method in ("initialize", "enqueue", "acquire", "ack", "reclaim", "aclose"):
            assert callable(getattr(AsyncredisBroker, method, None))
