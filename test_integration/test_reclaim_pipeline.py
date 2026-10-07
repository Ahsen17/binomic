"""Reclaim pipeline: messages stranded in a dead consumer's PEL are re-delivered."""

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import AsyncredisBroker
from binomic.message import Message

QUEUE = "reclaim-pipeline"


@pytest.mark.integration
class TestReclaimPipeline:
    async def test_stranded_message_is_reclaimed_and_redelivered(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        stream_key = f"binomic:{QUEUE}"

        dead_consumer_broker = AsyncredisBroker(
            dsn=broker_dsn,
            queues=[QUEUE],
            decode_responses=True,
        )
        await dead_consumer_broker.initialize()
        msg = Message(name="noop")
        await dead_consumer_broker.enqueue(QUEUE, msg)

        stranded = await dead_consumer_broker.acquire("dead-consumer", count=10)

        assert len(stranded) == 1

        successor = AsyncredisBroker(
            dsn=broker_dsn, queues=[QUEUE], decode_responses=True
        )
        await successor.initialize()
        try:
            reclaimed = await successor.reclaim("successor", min_idle_ms=0, count=10)

            assert reclaimed == 1

            pending = await redis_client.xpending(stream_key, "binomic")

            assert pending["pending"] == 0

            redelivered = await successor.acquire("successor", count=10)

            assert [entry.fields["id"] for entry in redelivered] == [str(msg.id)]
        finally:
            await successor.aclose()
            await dead_consumer_broker.aclose()
