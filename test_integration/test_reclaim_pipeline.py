"""Reclaim pipeline: messages stranded in a dead consumer's PEL are re-delivered."""

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
            reclaimed = await successor.reclaim("successor", min_idle_ms=0, count=10)

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
