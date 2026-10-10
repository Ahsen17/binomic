"""End-to-end pipeline: the client submits a task and a worker executes it."""

from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.client import Binomic
from binomic.config import BinomicConfig
from binomic.message import Message
from test_integration import E2E_QUEUE

EXECUTION_TIMEOUT: float = 30.0


@pytest.mark.integration
class TestEndToEndPipeline:
    async def test_task_runs_in_subprocess_and_is_acked(
        self,
        broker_dsn: str,
        probe_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        result_key = f"binomic:e2e:{uuid4()}"

        config = BinomicConfig(queues=[E2E_QUEUE], workers=1, concurrency=2)
        client = Binomic(
            broker_dsn=broker_dsn,
            module_name="test_integration",
            config=config,
        )

        async with client:
            await client.submit(
                Message(name="write_result", args=[probe_dsn, result_key]),
            )

            executed = False
            with anyio.move_on_after(EXECUTION_TIMEOUT):
                while not await redis_client.exists(result_key):
                    await anyio.sleep(0.1)
                executed = True

            assert executed, (
                f"task did not write {result_key} within {EXECUTION_TIMEOUT}s"
            )
            assert await redis_client.get(result_key) == "done"

            pending = await redis_client.xpending(f"binomic:{E2E_QUEUE}", "binomic")

            assert pending["pending"] == 0
