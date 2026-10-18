from typing import TYPE_CHECKING, Any

from .redis import RedisBroker

if TYPE_CHECKING:
    from .protocols import Broker

__all__ = ("BrokerFactory",)


class BrokerFactory:
    """A factory class for creating broker instances."""

    def __init__(
        self,
        dsn: str,
        queues: list[str],
        namespace: str,
        **config: Any,
    ) -> None:

        self._dsn = dsn
        self._queues = queues
        self._namespace = namespace
        self._config = config

    def create(self) -> "Broker":

        match self._dsn.split(":")[0]:
            case "redis":
                return RedisBroker(
                    dsn=self._dsn,
                    queues=self._queues,
                    namespace=self._namespace,
                    **self._config,
                )

            case "amqp":
                raise NotImplementedError("AMQP is not supported yet.")

            case _:
                raise ValueError("Unsupported broker type.")
