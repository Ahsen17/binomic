from binomic.base import BinomicError

__all__ = ("QueueCapacityError",)


class QueueCapacityError(BinomicError):
    """Exception raised when a submission is rejected on a full queue.

    ``Broker.enqueue`` raises it when the target queue's unacknowledged
    total has reached the effective ``capacity``; ``current`` is the depth
    observed at rejection and ``capacity`` the limit that was applied.
    """

    def __init__(self, current: int, capacity: int) -> None:

        self.current = current
        self.capacity = capacity

        super().__init__(
            f"Queue capacity reached (current={current}, capacity={capacity})"
        )
