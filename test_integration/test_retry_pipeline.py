"""Retry pipeline: a failing task is retried until its attempt budget is spent.

The attempt count rides in the message payload, so this also covers the counter
surviving a round trip through Redis.
"""

from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.client import Binomic
from binomic.config import BinomicConfig
from binomic.message import Message
from test_integration import RETRY_QUEUE

MAX_ATTEMPTS: int = 2
EXECUTION_TIMEOUT: float = 30.0

# A window for a retry that should never come.
SETTLE: float = 0.5


@pytest.mark.integration
class TestRetryPipeline:
    async def test_a_failing_task_is_retried_then_dropped(
        self,
        broker_dsn: str,
        probe_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        attempts_key = f"binomic:retry:{uuid4()}"

        config = BinomicConfig(
            queues=[RETRY_QUEUE],
            workers=1,
            concurrency=1,
            max_attempts=MAX_ATTEMPTS,
        )
        client = Binomic(
            broker_dsn=broker_dsn,
            module_name="test_integration",
            config=config,
        )

        async with client:
            await client.submit(
                Message(name="count_then_fail", args=[probe_dsn, attempts_key]),
            )

            with anyio.move_on_after(EXECUTION_TIMEOUT):
                while int(await redis_client.get(attempts_key) or 0) < MAX_ATTEMPTS:
                    await anyio.sleep(0.1)

            assert int(await redis_client.get(attempts_key) or 0) == MAX_ATTEMPTS

            # The budget is spent, so the message is dropped, not redelivered.
            assert (await redis_client.xpending(f"binomic:{RETRY_QUEUE}", "binomic"))[
                "pending"
            ] == 0

            await anyio.sleep(SETTLE)

            assert int(await redis_client.get(attempts_key) or 0) == MAX_ATTEMPTS
