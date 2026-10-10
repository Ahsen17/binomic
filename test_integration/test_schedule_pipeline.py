"""Schedule pipeline: the client's scheduler fires real triggers into a worker.

Unlike the unit tests, nothing here calls the job target by hand: the trigger
timing is APScheduler's, and the worker consuming the message runs in its own
subprocess.
"""

import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.client import Binomic
from binomic.config import BinomicConfig
from binomic.message import Message
from test_integration.schedule import DELAY_SECONDS, SCHEDULE_QUEUE
from test_integration.schedule.tasks import DSN_ENV, KEY_ENV

TIMEOUT: float = 30.0
STREAM_KEY: str = f"binomic:{SCHEDULE_QUEUE}"
GROUP: str = "binomic"


async def wait_until(
    predicate: Callable[[], Awaitable[object]],
    *,
    timeout: float = TIMEOUT,
) -> None:
    """Poll `predicate` until its result is truthy, failing when it never is."""

    with anyio.move_on_after(timeout):
        while not await predicate():
            await anyio.sleep(0.1)
        return

    pytest.fail(f"condition not met within {timeout}s")


def make_client(broker_dsn: str) -> Binomic:
    """A client that discovers the scheduled sample tasks of this pipeline."""

    return Binomic(
        broker_dsn=broker_dsn,
        module_name="test_integration.schedule",
        config=BinomicConfig(queues=[SCHEDULE_QUEUE], workers=1, concurrency=2),
    )


@pytest.mark.integration
class TestSchedulePipeline:
    async def test_delayed_task_runs_after_its_delay(
        self,
        broker_dsn: str,
        probe_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:

        result_key = f"binomic:e2e:delayed:{uuid4()}"

        async with make_client(broker_dsn) as client:
            started = time.monotonic()
            await client.submit(
                Message(name="write_delayed_result", args=[probe_dsn, result_key]),
            )

            # Nothing here calls the job: firing it is the scheduler's.
            await wait_until(lambda: redis_client.exists(result_key))
            elapsed = time.monotonic() - started

            # The task may only have run after its delay. A scheduler that
            # ignored the delay would fire on its next tick instead, well under
            # this bound.
            assert elapsed >= DELAY_SECONDS * 2 / 3, f"ran after {elapsed}s"
            assert await redis_client.get(result_key) == "done"
            pending = await redis_client.xpending(STREAM_KEY, GROUP)

            assert pending["pending"] == 0

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
