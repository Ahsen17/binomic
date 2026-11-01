from binomic.broker import BrokerDlqCapability, DLQRecordEntry, MessageEntry

__all__ = ("DeadLetters",)


class DeadLetters:
    """Operations on one queue's dead letters.

    Constructed by ``Binomic.dead_letters`` with a broker already resolved
    and probed for the DLQ capability; a thin, stateless facade — the queue
    is fixed at creation and every call delegates to the broker. Cleanup
    runs two ways: the worker's retention sweep (``dlq_retention``) is the
    automatic default, and ``prune`` is the explicit on-demand trigger.
    """

    def __init__(self, broker: BrokerDlqCapability, queue: str) -> None:

        self._broker = broker
        self._queue = queue

    async def depth(self) -> int:
        """Return how many dead letters the queue currently holds."""

        return await self._broker.dlq_depth(self._queue)

    async def list(
        self,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[list[DLQRecordEntry], str | None]:
        """Page through the dead letters oldest-first."""

        return await self._broker.dlq_list(self._queue, limit, cursor)

    async def read(self, entry_id: str) -> DLQRecordEntry | None:
        """Return one full record by stream entry id, or ``None``."""

        return await self._broker.dlq_read(self._queue, entry_id)

    async def replay(self, entry_id: str) -> MessageEntry | None:
        """Re-deliver a dead letter as a brand-new message; the record stays."""

        return await self._broker.dlq_replay(self._queue, entry_id)

    async def prune(self, retention: float) -> int:
        """Delete dead letters older than ``retention`` seconds; return the count.

        The explicit counterpart of the worker's automatic sweep — same
        primitive, invoked on demand. Deletion is irreversible, including
        records never replayed.
        """

        return await self._broker.dlq_prune(self._queue, retention)
