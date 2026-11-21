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
        capacity: int | None = None,
        **broker_config: Any,
    ) -> None:

        self._dsn = dsn
        self._queues = queues
        self._namespace = namespace
        self._capacity = capacity
        self._config = broker_config

    def create(self) -> "Broker":
        """Build the broker selected by the DSN scheme.

        ``redis``/``rediss`` return a ``RedisBroker``; ``amqp`` is recognized
        but not implemented; any other scheme raises ``ValueError``.
        """

        match self._dsn.split(":")[0]:
            case "redis" | "rediss":
                return RedisBroker(
                    dsn=self._dsn,
                    queues=self._queues,
                    namespace=self._namespace,
                    capacity=self._capacity,
                    **self._config,
                )

            case "amqp":
                raise NotImplementedError("AMQP is not supported yet.")

            case _:
                raise ValueError("Unsupported broker type.")
