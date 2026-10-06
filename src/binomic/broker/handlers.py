from typing import TYPE_CHECKING

from .redis import AsyncredisBroker

if TYPE_CHECKING:
    from .protocols import Broker

__all__ = ("BrokerFactory",)


class BrokerFactory:
    """A factory class for creating broker instances."""

    def __init__(self, dsn: str, queues: list[str]) -> None:

        self._dsn = dsn
        self._queues = queues

    def __call__(self) -> "Broker":

        match self._dsn.split(":")[0]:
            case "redis":
                return AsyncredisBroker(
                    dsn=self._dsn,
                    queues=self._queues,
                    decode_responses=True,
                )

            case "amqp":
                raise NotImplementedError("AMQP is not supported yet.")

            case _:
                raise ValueError("Unsupported broker type.")
