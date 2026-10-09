import logging
import time
from collections.abc import Callable

import pytest

from binomic.broker import AsyncredisBroker, Entry, QueueCapacityLimitError
from binomic.message import Message


class TestStreamKey:
    def test_namespaces_queue_with_app_name(
        self, make_broker: Callable[..., AsyncredisBroker]
    ) -> None:

        assert make_broker(["orders"]).get_stream_key("orders") == "binomic:orders"


class TestClient:
    def test_pool_is_lazy(self, make_broker: Callable[..., AsyncredisBroker]) -> None:

        broker = make_broker(["default"])

        assert broker._client is None

    def test_client_is_cached(self, make_broker: Callable[..., AsyncredisBroker]) -> None:

        broker = make_broker(["default"])
        first = broker.client
        second = broker.client

        assert first is second
        assert broker._client is not None


class TestInitialize:
    async def test_creates_group_per_queue(
        self, make_broker: Callable[..., AsyncredisBroker]
    ) -> None:

        broker = make_broker(["a", "b"])
        await broker.initialize()

        for queue in ("a", "b"):
            groups = await broker.client.xinfo_groups(broker.get_stream_key(queue))
            assert [group["name"] for group in groups] == ["binomic"]

    async def test_is_idempotent_on_busy_group(
        self, make_broker: Callable[..., AsyncredisBroker]
    ) -> None:

        broker = make_broker(["a"])

        await broker.initialize()
        await broker.initialize()

        assert True  # no BUSYGROUP leak means both calls survived


class TestEnqueue:
    async def test_returns_message_id(
        self, broker: AsyncredisBroker, make_message: Callable[..., Message]
    ) -> None:

        msg = make_message()

        assert await broker.enqueue("default", msg) == msg.id

    async def test_persists_message_fields(
        self,
        broker: AsyncredisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(name="deploy")
        await broker.enqueue("default", msg)
        entries = await broker.acquire("consumer-1", count=10)

        assert len(entries) == 1
        assert entries[0].fields["id"] == str(msg.id)
        assert '"name":"deploy"' in entries[0].fields["message"]


class TestQueueCapacity:
    async def test_rejects_an_enqueue_once_the_queue_is_full(
        self,
        make_broker: Callable[..., AsyncredisBroker],
        make_message: Callable[..., Message],
    ) -> None:

        broker = make_broker(["default"], queue_capacity=1)
        await broker.initialize()
        await broker.enqueue("default", make_message())

        with pytest.raises(QueueCapacityLimitError, match="out of capacity"):
            await broker.enqueue("default", make_message())

    async def test_allows_an_enqueue_while_the_queue_has_room(
        self,
        make_broker: Callable[..., AsyncredisBroker],
        make_message: Callable[..., Message],
    ) -> None:

        broker = make_broker(["default"], queue_capacity=3)
        await broker.initialize()

        for _ in range(3):
            await broker.enqueue("default", make_message())

    async def test_has_no_capacity_without_a_group(
        self,
        make_broker: Callable[..., AsyncredisBroker],
        make_message: Callable[..., Message],
    ) -> None:

        broker = make_broker(["default"], queue_capacity=1)
        await broker.initialize()
        await broker.enqueue("default", make_message())
        await broker.client.xgroup_destroy(broker.get_stream_key("default"), "binomic")

        # With no group there is no pending work to count, so nothing holds the
        # queue back.
        await broker.enqueue("default", make_message())


class TestAcquire:
    async def test_returns_empty_without_messages(self, broker: AsyncredisBroker) -> None:

        assert await broker.acquire("consumer-1", count=10) == []

    async def test_reads_across_all_queues(
        self,
        make_broker: Callable[..., AsyncredisBroker],
        make_message: Callable[..., Message],
    ) -> None:

        broker = make_broker(["a", "b"])
        await broker.initialize()
        await broker.enqueue("a", make_message())
        await broker.enqueue("b", make_message())

        entries = await broker.acquire("consumer-1", count=10)

        assert {entry.queue for entry in entries} == {"a", "b"}

    async def test_delivers_undelivered_only_to_repeating_consumer(
        self,
        broker: AsyncredisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        await broker.enqueue("default", make_message())

        first = await broker.acquire("consumer-1", count=10)
        second = await broker.acquire("consumer-1", count=10)

        assert len(first) == 1
        assert second == []


class TestAck:
    async def test_acks_pending_entry(
        self, broker: AsyncredisBroker, make_message: Callable[..., Message]
    ) -> None:

        await broker.enqueue("default", make_message())
        entry = (await broker.acquire("consumer-1", count=10))[0]

        assert await broker.ack(entry) == 1
        assert (await broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

    async def test_unknown_entry_acks_nothing(
        self,
        broker: AsyncredisBroker,
    ) -> None:

        assert (
            await broker.ack(
                Entry(
                    "default",
                    "999-0",
                    {
                        "id": "x",
                        "message": "m",
                        "enqueued_at": time.time(),
                    },
                )
            )
            == 0
        )


class TestReclaim:
    async def test_reclaims_stale_pending_message(
        self, broker: AsyncredisBroker, make_message: Callable[..., Message]
    ) -> None:

        msg = make_message()
        await broker.enqueue("default", msg)
        stale = (await broker.acquire("dead-consumer", count=10))[0]

        reclaimed = await broker.reclaim("consumer-1", min_idle_ms=0, count=10)

        assert reclaimed == 1
        assert (await broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 0

        redelivered = await broker.acquire("consumer-1", count=10)

        assert len(redelivered) == 1
        assert redelivered[0].fields["id"] == stale.fields["id"]

        # A redelivery is another attempt, and the count rides in the payload.
        assert Message.from_json(redelivered[0].fields["message"]).attempt == 2

    async def test_defers_a_reclaim_while_the_queue_is_full(
        self,
        make_broker: Callable[..., AsyncredisBroker],
        make_message: Callable[..., Message],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        broker = make_broker(["default"], queue_capacity=1)
        await broker.initialize()
        await broker.enqueue("default", make_message())
        await broker.acquire("dead-consumer", count=10)

        with caplog.at_level(logging.WARNING, logger="binomic.broker.redis"):
            assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10) == 0

        assert any("Deferring reclaim" in r.message for r in caplog.records)
        # Left pending, so a later pass redelivers it once the queue drains.
        assert (await broker.client.xpending("binomic:default", "binomic"))[
            "pending"
        ] == 1

    async def test_restamps_enqueued_at_on_redelivery(
        self,
        broker: AsyncredisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message()

        # A stale entry written straight to the stream: a fresh stamp is only
        # distinguishable when the original one is old.
        await broker.client.xadd(
            broker.get_stream_key("default"),
            {
                "id": str(msg.id),
                "message": msg.to_json(),
                "enqueued_at": time.time() - 7200,
            },
        )
        await broker.acquire("dead-consumer", count=10)

        await broker.reclaim("consumer-1", min_idle_ms=0, count=10)

        (redelivered,) = await broker.acquire("consumer-1", count=10)

        # Re-delivery is a fresh submission: keeping the old stamp would make the
        # worker drop the message as expired the moment it arrives.
        assert redelivered.fields["enqueued_at"] > time.time() - 60

    async def test_skips_queues_without_group(
        self, make_broker: Callable[..., AsyncredisBroker]
    ) -> None:

        broker = make_broker(["ghost"])

        assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10) == 0

    async def test_reclaims_nothing_when_idle_is_not_met(
        self,
        broker: AsyncredisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        await broker.enqueue("default", make_message())
        await broker.acquire("dead-consumer", count=10)

        assert await broker.reclaim("consumer-1", min_idle_ms=3_600_000, count=10) == 0


class TestAclose:
    async def test_releases_client(
        self, make_broker: Callable[..., AsyncredisBroker]
    ) -> None:

        broker = make_broker(["default"])
        first = broker.client

        await broker.aclose()

        assert broker._client is None
        assert broker.client is not first


class TestMessageToEntryRouting:
    async def test_entry_fields_deserialize_to_message(
        self,
        broker: AsyncredisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(args=[1, 2], kwargs={"deep": True})
        await broker.enqueue("default", msg)
        entry = (await broker.acquire("consumer-1", count=10))[0]

        restored = Message.from_json(entry.fields["message"])

        assert restored == msg
