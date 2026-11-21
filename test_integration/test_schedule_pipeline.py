"""Schedule pipeline: the client's scheduler fires real triggers into a worker.

Unlike the unit tests, nothing here calls the job target by hand: the trigger
timing is APScheduler's, and the worker consuming the message runs in its own
subprocess.
"""

from uuid import uuid4

import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.client import Binomic, BinomicConfig
from test_integration.helpers import wait_until
from test_integration.schedule import SCHEDULE_QUEUE
from test_integration.schedule.tasks import DSN_ENV, KEY_ENV

TIMEOUT: float = 30.0
# The client and the workers both derive the namespace from module_name.
STREAM_KEY: str = f"test_integration.schedule:{SCHEDULE_QUEUE}:stream"
GROUP: str = "test_integration.schedule"


def make_client(broker_dsn: str) -> Binomic:
    """A client that discovers the scheduled sample tasks of this pipeline."""

    return Binomic(
        broker_dsn=broker_dsn,
        module_name="test_integration.schedule",
        config=BinomicConfig(queues=[SCHEDULE_QUEUE], workers=1, concurrency=2),
    )


@pytest.mark.integration
class TestSchedulePipeline:
    async def test_interval_task_repeats_and_is_acked(
        self,
        broker_dsn: str,
        probe_dsn: str,
        redis_client: AsyncRedis,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        counter_key = f"binomic:e2e:heartbeat:{uuid4()}"
        monkeypatch.setenv(DSN_ENV, probe_dsn)
        monkeypatch.setenv(KEY_ENV, counter_key)

        async def counted_twice() -> bool:

            return int(await redis_client.get(counter_key) or 0) >= 2

        async def drained() -> bool:

            pending = await redis_client.xpending(STREAM_KEY, GROUP)
            return bool(pending["pending"] == 0)

        async with make_client(broker_dsn):
            # Nothing submits this task: entering the context decides which
            # registered specs get scheduled, and the trigger does the rest.
            # The worker acked whatever it was handed either way, so only the
            # counter the task bumps itself proves it actually ran.
            await wait_until(counted_twice)
            await wait_until(drained)
