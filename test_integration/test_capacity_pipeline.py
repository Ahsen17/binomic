"""Capacity pipeline: the queue capacity limit, against a real Redis.

The limit is read from the consumer group's own bookkeeping (``pending`` plus
``lag``), so fake clients cannot vouch for it: these tests run against Redis.
"""

from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import AsyncredisBroker, QueueCapacityLimitError
from binomic.client import Binomic
from binomic.config import BinomicConfig
from binomic.message import Message
from test_integration import CAPACITY_QUEUE, E2E_QUEUE

# Long enough for a message to have been picked up, short enough to stay honest.
DROPPED_GRACE: float = 0.5


@pytest.mark.integration
class TestCapacityPipeline:
    async def test_rejects_an_enqueue_once_the_queue_is_full(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        broker = AsyncredisBroker(
            dsn=broker_dsn,
            queues=[CAPACITY_QUEUE],
            queue_capacity=2,
            decode_responses=True,
        )
        await broker.initialize()
        try:
            await broker.enqueue(CAPACITY_QUEUE, Message(name="noop"))
            await broker.enqueue(CAPACITY_QUEUE, Message(name="noop"))

            with pytest.raises(QueueCapacityLimitError):
                await broker.enqueue(CAPACITY_QUEUE, Message(name="noop"))

            assert await redis_client.xlen(f"binomic:{CAPACITY_QUEUE}") == 2
        finally:
            await broker.aclose()

    async def test_submit_drops_a_message_the_queue_cannot_take(
        self,
        broker_dsn: str,
        redis_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        result_key = f"binomic:capacity:{uuid4()}"

        # A zero capacity refuses every enqueue, so the drop is deterministic
        # rather than a race with whatever the worker has already consumed.
        config = BinomicConfig(queues=[E2E_QUEUE], queue_capacity=0)
        client = Binomic(
            broker_dsn=broker_dsn,
            redis_dsn=redis_dsn,
            module_name="test_integration",
            config=config,
        )

        async with client:
            await client.submit(
                Message(name="write_result", args=[redis_dsn, result_key]),
            )
            await anyio.sleep(DROPPED_GRACE)

            # Dropped, not retried: the task never runs and nothing is pending.
            assert not await redis_client.exists(result_key)
            assert await redis_client.xlen(f"binomic:{E2E_QUEUE}") == 0
