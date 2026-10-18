"""Multi-queue pipeline: one broker routing, acking and reclaiming across queues.

The unit suite cannot assemble script-side acquires (fakeredis flattens
XREADGROUP's nested reply), so the acquire script's queue loop -- and the
reclaim pass that walks each queue in turn -- is only exercised here, on real
Redis.
"""

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import Message, RedisBroker

QUEUES = ["multi-a", "multi-b"]


@pytest.mark.integration
class TestMultiQueuePipeline:
    async def test_across_queues_roundtrip(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        broker = RedisBroker(
            dsn=broker_dsn,
            queues=QUEUES,
            namespace="binomic",
            decode_responses=True,
        )
        await broker.start()
        try:
            first = Message(task="noop")
            second = Message(task="noop")
            await broker.enqueue("multi-a", first)
            await broker.enqueue("multi-b", second)

            entries = await broker.acquire("consumer-1", count=10)

            # One read drains both queues, each entry knowing its own queue.
            assert {entry.queue for entry in entries} == set(QUEUES)
            by_queue = {entry.queue: entry for entry in entries}
            assert by_queue["multi-a"].message.id == first.id
            assert by_queue["multi-b"].message.id == second.id

            # Acking one queue's entry leaves the other queue's pending.
            await broker.ack(by_queue["multi-a"])
            assert (await redis_client.xpending("binomic:multi-a:stream", "binomic"))[
                "pending"
            ] == 0
            assert (await redis_client.xpending("binomic:multi-b:stream", "binomic"))[
                "pending"
            ] == 1

            # Reclaim walks every queue and hands the stranded one over.
            assert await broker.reclaim("consumer-2", min_idle_ms=0, count=10) == 1
            redelivered = await broker.acquire("consumer-2", count=10)

            assert [entry.message.id for entry in redelivered] == [second.id]
        finally:
            await broker.close()
