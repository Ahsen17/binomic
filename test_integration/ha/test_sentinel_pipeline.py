"""Script and consumer-group semantics on the Sentinel topology (R4).

Research expectation (task research-redis-ha.md §5, conclusion 1): fully
compatible -- the broker talks to the Sentinel-resolved primary like a
standalone Redis, and script NOSCRIPT reloads are transparent. These cases
pin that expectation against the real topology; the verification report
records the outcomes.
"""

import anyio
import pytest

from binomic.broker import Message
from test_integration.ha.conftest import BrokerFactory

pytestmark = [pytest.mark.integration, pytest.mark.ha]


@pytest.mark.integration
@pytest.mark.ha
class TestSentinelScripts:
    async def test_enqueue_acquire_roundtrip(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])
        first = Message(task="noop")
        second = Message(task="noop")

        assert await broker.enqueue("solo", first) is not None
        assert await broker.enqueue("solo", second) is not None

        entries = await broker.acquire("consumer-1", count=10)

        assert [entry.message.id for entry in entries] == [first.id, second.id]
        assert entries[0].queue == "solo"
        assert isinstance(entries[0].enqueued_at, float)

    async def test_multi_queue_roundtrip(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["multi-a", "multi-b"])
        first = Message(task="noop")
        second = Message(task="noop")

        await broker.enqueue("multi-a", first)
        await broker.enqueue("multi-b", second)

        # One read drains both queues; each entry knows its own queue.
        entries = await broker.acquire("consumer-1", count=10)

        assert {entry.queue for entry in entries} == {"multi-a", "multi-b"}

    async def test_ack_removes_entry(self, make_sentinel_broker: BrokerFactory) -> None:

        broker, probe = await make_sentinel_broker(["solo"])
        entry = await broker.enqueue("solo", Message(task="noop"))
        assert entry is not None
        acquired = await broker.acquire("consumer-1", count=10)

        assert await broker.ack(acquired[0]) == 1

        assert await broker.acquire("consumer-1", count=10) == []
        assert await probe.hlen(broker.get_hash_key("solo")) == 0

    async def test_dead_moves_to_dlq(self, make_sentinel_broker: BrokerFactory) -> None:

        broker, probe = await make_sentinel_broker(["solo"])
        entry = await broker.enqueue("solo", Message(task="noop"))
        assert entry is not None
        acquired = await broker.acquire("consumer-1", count=10)

        assert await broker.dead(acquired[0]) == 1

        assert await probe.xlen(broker.get_dlq_key("solo") + ":stream") == 1
        assert await probe.hlen(broker.get_dlq_key("solo") + ":hash") == 1
        assert await probe.xlen(broker.get_stream_key("solo")) == 0
        assert await probe.hlen(broker.get_hash_key("solo")) == 0

    async def test_retry_increments_attempt_and_keeps_enqueued_at(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])
        entry = await broker.enqueue("solo", Message(task="noop"))
        assert entry is not None
        original = await broker.acquire("consumer-1", count=10)

        assert await broker.retry(original[0]) == 1

        redelivered = await broker.acquire("consumer-2", count=10)
        assert redelivered[0].message.attempt == 2
        assert redelivered[0].enqueued_at == entry.enqueued_at

    async def test_reclaim_reassigns_and_rewrites_enqueued_at(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])
        await broker.enqueue("solo", Message(task="noop"))
        stranded = await broker.acquire("consumer-1", count=10)
        await anyio.sleep(0.05)

        # The stranded PEL entry moves to consumer-2 with a fresh timestamp.
        assert (
            await broker.reclaim(
                "consumer-2", min_idle_ms=1, count=10, gc_idle_ms=3_600_000
            )
            == 1
        )

        redelivered = await broker.acquire("consumer-2", count=10)
        assert redelivered[0].message.attempt == 2
        assert redelivered[0].enqueued_at > stranded[0].enqueued_at

    async def test_observe_reports_depth(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])

        assert await broker.observe("solo") == (0, None)

        await broker.enqueue("solo", Message(task="noop"))
        observed = await broker.observe("solo")
        assert observed is not None
        depth, oldest = observed

        assert depth == 1
        assert oldest is not None and oldest >= 0.0

    async def test_context_roundtrip_and_cleanup(
        self, make_sentinel_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])
        carrier = {
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        }
        entry = await broker.enqueue("solo", Message(task="noop"), context=carrier)
        assert entry is not None

        assert await broker.trace_context("solo", str(entry.message.id)) == carrier

        acquired = await broker.acquire("consumer-1", count=10)
        await broker.ack(acquired[0])

        # Ack sweeps the context: the mapping is gone once the message ends.
        assert await broker.trace_context("solo", str(entry.message.id)) is None

    async def test_group_initialization_is_idempotent(
        self, make_sentinel_broker: BrokerFactory, ha_namespace: str
    ) -> None:

        broker, _ = await make_sentinel_broker(["solo"])

        # A second broker on the same namespace hits BUSYGROUP on start; the
        # framework treats that as success (the group already exists).
        twin = type(broker)(
            dsn="redis://127.0.0.1:6380/0",
            queues=["solo"],
            namespace=ha_namespace,
            decode_responses=True,
        )
        await twin.start()
        await twin.close()

        assert await broker.enqueue("solo", Message(task="noop")) is not None

    async def test_missing_group_reads_empty(
        self, make_sentinel_broker: BrokerFactory, ha_namespace: str
    ) -> None:

        broker, probe = await make_sentinel_broker(["solo"])
        await probe.xgroup_destroy(broker.get_stream_key("solo"), ha_namespace)

        # The acquire script maps NOGROUP to an empty result instead of
        # surfacing the error (docs/api.md contract with the scripts).
        assert await broker.acquire("consumer-1", count=10) == []
