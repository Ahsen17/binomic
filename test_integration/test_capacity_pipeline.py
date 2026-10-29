"""Capacity pipeline: the enqueue-side capacity check on real Redis.

The unit suite covers the check through fakeredis-lua; here the same
semantics run on real Redis and through the real acquire script, which
fakeredis cannot assemble -- so the in-flight case (a message sitting in
the consumer group's PEL still counting toward the depth) is only proven
end to end here.
"""

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import Message, QueueCapacityError, RedisBroker


@pytest.mark.integration
class TestCapacityPipeline:
    async def test_rejects_when_full_and_frees_on_ack(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        broker = RedisBroker(
            dsn=broker_dsn,
            queues=["capped"],
            namespace="binomic",
            capacity=1,
            decode_responses=True,
        )
        # Defensive: a previous run killed before its teardown flush would
        # otherwise read as a functional failure on the first enqueue. This
        # must precede ``start()``: flushdb removes the consumer group too,
        # and ``initialize()`` recreates it.
        await redis_client.flushdb()

        await broker.start()
        try:
            await broker.enqueue("capped", Message(task="noop"))

            with pytest.raises(QueueCapacityError) as excinfo:
                await broker.enqueue("capped", Message(task="noop"))

            assert excinfo.value.current == 1
            assert excinfo.value.capacity == 1

            # In-flight counts too: acquire moves nothing out of the stream,
            # so a message sitting in the PEL keeps the depth at 1.
            (entry,) = await broker.acquire("consumer-1", count=10)

            with pytest.raises(QueueCapacityError):
                await broker.enqueue("capped", Message(task="noop"))

            await broker.ack(entry)

            assert await broker.enqueue("capped", Message(task="noop")) is not None
        finally:
            await broker.close()

    async def test_limit_is_global_across_broker_instances(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        brokers = [
            RedisBroker(
                dsn=broker_dsn,
                queues=["shared"],
                namespace="binomic",
                capacity=2,
                decode_responses=True,
            )
            for _ in range(2)
        ]
        # Same as above: flush before ``start()`` so the consumer group is
        # recreated by ``initialize()``.
        await redis_client.flushdb()

        for broker in brokers:
            await broker.start()
        try:
            first, second = brokers

            # Two independent client instances submit into the same stream:
            # the limit applies to their combined depth.
            assert await first.enqueue("shared", Message(task="noop")) is not None
            assert await second.enqueue("shared", Message(task="noop")) is not None

            with pytest.raises(QueueCapacityError) as excinfo:
                await second.enqueue("shared", Message(task="noop"))

            assert excinfo.value.current == 2
            assert excinfo.value.capacity == 2
        finally:
            for broker in brokers:
                await broker.close()
