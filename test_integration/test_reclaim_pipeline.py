"""Reclaim pipeline: messages stranded in a dead consumer's PEL are re-delivered."""

import time
from uuid import uuid4

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import AsyncredisBroker
from binomic.message import Message


@pytest.mark.integration
class TestReclaimPipeline:
    async def test_stranded_message_is_reclaimed_and_redelivered(
        self,
        broker_dsn: str,
        real_redis: AsyncRedis,
        track_keys,
    ) -> None:

        queue = f"reclaim-{uuid4()}"
        stream_key = f"binomic:{queue}"
        track_keys(stream_key)

        dead_consumer_broker = AsyncredisBroker(
            dsn=broker_dsn,
            queues=[queue],
            decode_responses=True,
        )
        await dead_consumer_broker.initialize()
        msg = Message(name="noop", queue=queue, enqueued_at=time.time())
        await dead_consumer_broker.enqueue(msg)

        stranded = await dead_consumer_broker.acquire("dead-consumer", count=10)

        assert len(stranded) == 1

        successor = AsyncredisBroker(
            dsn=broker_dsn, queues=[queue], decode_responses=True
        )
        await successor.initialize()
        try:
            reclaimed = await successor.reclaim("successor", min_idle_ms=0, count=10)

            assert reclaimed == 1

            pending = await real_redis.xpending(stream_key, "binomic")

            assert pending["pending"] == 0

            redelivered = await successor.acquire("successor", count=10)

            assert [entry.fields["id"] for entry in redelivered] == [str(msg.id)]
        finally:
            await successor.aclose()
            await dead_consumer_broker.aclose()
