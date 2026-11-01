from binomic.base import BinomicError

__all__ = (
    "DLQEntryNotFoundError",
    "QueueCapacityError",
)


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


class DLQEntryNotFoundError(BinomicError):
    """Exception raised when a DLQ entry id has no record to operate on.

    ``Broker.dlq_replay`` raises it when the entry was never in the DLQ or
    has since been pruned; the DLQ keeps no tombstones, so the id is simply
    gone.
    """

    def __init__(self, queue: str, entry_id: str) -> None:

        self.queue = queue
        self.entry_id = entry_id

        super().__init__(f"DLQ entry not found (queue={queue}, entry_id={entry_id})")
