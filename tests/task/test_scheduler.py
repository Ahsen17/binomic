import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from binomic.task.scheduler import TaskScheduler


def target(*args: Any, **kwargs: Any) -> None:
    """Stand-in job target: accepts whatever the scheduler passes it."""


@pytest.fixture
async def scheduler() -> AsyncIterator[TaskScheduler]:
    """A started scheduler living in the test's event loop."""

    instance = TaskScheduler()
    instance.start()

    yield instance
    instance.close()


class TestLifecycle:
    async def test_starts_not_running(self) -> None:

        instance = TaskScheduler()

        assert not instance.running

    async def test_start_marks_running(self) -> None:

        instance = TaskScheduler()
        instance.start()

        assert instance.running

        instance.close()

    async def test_start_is_idempotent(self) -> None:

        instance = TaskScheduler()
        instance.start()
        underlying = instance._scheduler
        instance.start()

        assert instance._scheduler is underlying
        assert instance.running

        instance.close()

    async def test_close_without_start_is_a_noop(self) -> None:

        instance = TaskScheduler()

        instance.close()

        assert not instance.running

    async def test_close_is_idempotent(self, scheduler: TaskScheduler) -> None:

        assert scheduler.running

        scheduler.close()
        scheduler.close()

        assert not scheduler.running

    async def test_restart_after_close(self) -> None:

        instance = TaskScheduler()
        instance.start()
        instance.close()
        instance.start()

        assert instance.running
        assert instance._scheduler is not None

        instance.close()

    async def test_close_shuts_the_underlying_down(
        self, scheduler: TaskScheduler
    ) -> None:

        underlying = scheduler._scheduler
        assert underlying is not None

        scheduler.close()
        # AsyncIOScheduler defers its shutdown via call_soon_threadsafe: one
        # loop tick later it is stopped.
        await asyncio.sleep(0)

        assert not underlying.running


class TestDelay:
    async def test_registers_a_date_trigger_job(self, scheduler: TaskScheduler) -> None:

        before = datetime.now(UTC)
        scheduler.delay(func=target, delay=30.0, args=(1,), kwargs={"k": 2})

        underlying = scheduler._scheduler
        assert underlying is not None
        jobs = underlying.get_jobs()
        assert len(jobs) == 1

        job = jobs[0]
        assert isinstance(job.trigger, DateTrigger)
        assert job.misfire_grace_time is None  # late fires still run: a
        # dropped retry is a lost retry.
        assert job.args == (1,)
        assert job.kwargs == {"k": 2}

        expected = before + timedelta(seconds=30)
        assert job.next_run_time is not None
        assert abs(job.next_run_time - expected) < timedelta(seconds=5)

    async def test_validation_belongs_to_call_sites_not_the_scheduler(
        self,
    ) -> None:
        """The scheduler is generic: semantics (positive delay etc.) are the
        decorator's and submit's business."""

        instance = TaskScheduler()
        instance.start()

        instance.delay(func=target, delay=-5.0)

        underlying = instance._scheduler
        assert underlying is not None
        assert len(underlying.get_jobs()) == 1

        instance.close()


class TestCron:
    async def test_registers_a_cron_trigger(self, scheduler: TaskScheduler) -> None:

        scheduler.cron(func=target, cron="*/5 * * * *", args=(1,))

        underlying = scheduler._scheduler
        assert underlying is not None
        jobs = underlying.get_jobs()
        assert len(jobs) == 1

        job = jobs[0]
        assert isinstance(job.trigger, CronTrigger)
        assert job.args == (1,)
        assert job.next_run_time is not None

    async def test_cron_honors_the_scheduler_timezone(self) -> None:

        tz = ZoneInfo("Asia/Shanghai")
        instance = TaskScheduler(timezone=tz)
        instance.start()

        instance.cron(func=target, cron="* * * * *")

        underlying = instance._scheduler
        assert underlying is not None
        job = underlying.get_jobs()[0]
        assert isinstance(job.trigger, CronTrigger)
        assert job.trigger.timezone == tz

        instance.close()


class TestInterval:
    async def test_registers_an_interval_trigger(self, scheduler: TaskScheduler) -> None:

        scheduler.interval(func=target, interval=5.0)

        underlying = scheduler._scheduler
        assert underlying is not None
        jobs = underlying.get_jobs()
        assert len(jobs) == 1

        job = jobs[0]
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval == timedelta(seconds=5)


class TestExecution:
    async def test_delayed_job_actually_fires(self) -> None:
        """End-to-end through the real loop: a short delay job runs its
        target — this is the retry-redelivery path's backbone."""

        done: list[Any] = []
        instance = TaskScheduler()
        instance.start()

        instance.delay(func=done.append, delay=0.1, args=("fired",))

        deadline = time.monotonic() + 5
        while not done and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

        assert done == ["fired"]

        instance.close()
