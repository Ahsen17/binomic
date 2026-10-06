"""End-to-end pipeline: client submits a task, a forked worker executes it."""

import time
from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.client import Binomic
from binomic.config import BinomicConfig
from binomic.message import Message

EXECUTION_TIMEOUT: float = 30.0


@pytest.mark.integration
class TestEndToEndPipeline:
    async def test_task_runs_in_subprocess_and_is_acked(
        self,
        broker_dsn: str,
        redis_dsn: str,
        real_redis: AsyncRedis,
        track_keys,
    ) -> None:

        queue = f"e2e-{uuid4()}"
        result_key = f"binomic:e2e:{uuid4()}"
        track_keys(queue, result_key)

        stream_key = f"binomic:{queue}"
        config = BinomicConfig(queues=[queue], workers=1, concurrency=2)
        client = Binomic(
            broker_dsn=broker_dsn,
            redis_dsn=redis_dsn,
            module_name="test_integration",
            config=config,
        )

        async with client:
            await client.submit(
                Message(
                    name="write_result",
                    queue=queue,
                    enqueued_at=time.time(),
                    args=[redis_dsn, result_key],
                ),
            )

            executed = False
            with anyio.move_on_after(EXECUTION_TIMEOUT):
                while not await real_redis.exists(result_key):
                    await anyio.sleep(0.1)
                executed = True

            assert executed, (
                f"task did not write {result_key} within {EXECUTION_TIMEOUT}s"
            )
            assert await real_redis.get(result_key) == "done"

            pending = await real_redis.xpending(stream_key, "binomic")

            assert pending["pending"] == 0
