"""Reclaim pipeline: messages stranded in a dead consumer's PEL are re-delivered."""

import asyncio

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import Message, RedisBroker

QUEUE = "reclaim-pipeline"


@pytest.mark.integration
class TestReclaimPipeline:
    async def test_stranded_message_is_reclaimed_and_redelivered(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        stream_key = f"binomic:{QUEUE}:stream"

        dead_consumer_broker = RedisBroker(
            dsn=broker_dsn,
            queues=[QUEUE],
            namespace="binomic",
            decode_responses=True,
        )
        await dead_consumer_broker.start()
        msg = Message(task="noop")
        await dead_consumer_broker.enqueue(QUEUE, msg)

        stranded = await dead_consumer_broker.acquire("dead-consumer", count=10)

        assert len(stranded) == 1

        successor = RedisBroker(
            dsn=broker_dsn,
            queues=[QUEUE],
            namespace="binomic",
            decode_responses=True,
        )
        await successor.start()
        try:
            reclaimed = await successor.reclaim(
                "successor", min_idle_ms=0, count=10, gc_idle_ms=3_600_000
            )

            assert reclaimed == 1

            pending = await redis_client.xpending(stream_key, "binomic")

            assert pending["pending"] == 0

            redelivered = await successor.acquire("successor", count=10)

            # The redelivered copy carries the payload whole: same message,
            # one attempt richer, and stamped anew by the reclaim.
            assert [entry.message.id for entry in redelivered] == [msg.id]
            assert [entry.message.attempt for entry in redelivered] == [2]
            assert {entry.queue for entry in redelivered} == {QUEUE}
            assert all(entry.enqueued_at > 0 for entry in redelivered)
        finally:
            await successor.close()
            await dead_consumer_broker.close()


@pytest.mark.integration
class TestConsumerSweep:
    """The reclaim pass also cleans consumer-group metadata: instances that
    scaled away leave empty-PEL consumers behind, and Redis never GCs them."""

    async def test_sweeps_an_empty_stale_consumer(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        broker = RedisBroker(
            dsn=broker_dsn,
            queues=[QUEUE],
            namespace="binomic",
            decode_responses=True,
        )
        await broker.start()
        try:
            # A corpse: a consumer registered by a since-dead instance, with
            # nothing pending. A short wait lets its idle clock pass the
            # tiny threshold the sweep below uses.
            await redis_client.xgroup_createconsumer(
                f"binomic:{QUEUE}:stream", "binomic", "corpse"
            )
            await asyncio.sleep(0.05)

            assert (
                await broker.reclaim("successor", min_idle_ms=0, count=10, gc_idle_ms=10)
                == 0
            )

            names = {
                consumer["name"]
                for consumer in await redis_client.xinfo_consumers(
                    f"binomic:{QUEUE}:stream", "binomic"
                )
            }

            assert "corpse" not in names
            assert "successor" in names
        finally:
            await broker.close()

    async def test_spares_a_consumer_with_pending_entries(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        broker = RedisBroker(
            dsn=broker_dsn,
            queues=[QUEUE],
            namespace="binomic",
            decode_responses=True,
        )
        await broker.start()
        try:
            # `busy` holds an unacked message: DELCONSUMER would orphan it,
            # so the sweep must leave the consumer alone no matter how idle.
            await broker.enqueue(QUEUE, Message(task="noop"))
            assert len(await broker.acquire("busy", count=10)) == 1

            await redis_client.xgroup_createconsumer(
                f"binomic:{QUEUE}:stream", "binomic", "corpse"
            )
            await asyncio.sleep(0.05)

            # A huge min_idle_ms keeps the claim arm idle so only the sweep
            # is exercised: nothing is stale enough to reclaim, and `busy`'s
            # pending entry stays exactly where it is.
            assert (
                await broker.reclaim(
                    "successor", min_idle_ms=3_600_000, count=10, gc_idle_ms=10
                )
                == 0
            )

            names = {
                consumer["name"]
                for consumer in await redis_client.xinfo_consumers(
                    f"binomic:{QUEUE}:stream", "binomic"
                )
            }

            assert "busy" in names
            assert "corpse" not in names
        finally:
            await broker.close()
