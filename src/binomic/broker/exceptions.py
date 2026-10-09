from binomic.base import BinomicError

__all__ = ("QueueCapacityLimitError",)


class QueueCapacityLimitError(BinomicError):
    """Raised by ``enqueue`` when a queue is at its capacity.

    The limit counts the consumer group's pending work plus its lag. Delivery
    paths treat a full queue as back-pressure rather than as a failure: the
    client and the worker's retry drop the message, while ``reclaim`` leaves it
    pending for a later pass.
    """
