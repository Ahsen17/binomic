from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

if TYPE_CHECKING:
    from .registry import TaskSpec

__all__ = ("TaskScheduler",)


_func = Callable[..., Any | Awaitable[Any]]


class TaskScheduler:
    """Scheduler class for Binomic tasks."""

    def __init__(self, timezone: tzinfo = UTC) -> None:

        self._timezone = timezone

        self._scheduler = AsyncIOScheduler()

    def start(self) -> None:
        """Start the scheduler."""

        self._scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the scheduler."""

        self._scheduler.shutdown()

    def delay(
        self,
        func: _func,
        spec: "TaskSpec",
        *,
        delay: float | None = None,
        args: Sequence[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Delay a task."""

        if delay is None and (spec.mode != "delay" or spec.delay is None):
            raise ValueError("Task is not a delay task or lack `delay` value.")

        if (delay := delay or spec.delay) <= 0:
            raise ValueError("Delay value must be greater than 0.")

        self._scheduler.add_job(
            func=func,
            trigger=DateTrigger(
                run_date=datetime.now(self._timezone) + timedelta(seconds=delay),
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
            # A deferred job is work that must happen: one the scheduler only
            # reaches late still fires, instead of being dropped as a missed run.
            misfire_grace_time=None,
        )

    def interval(
        self,
        func: _func,
        spec: "TaskSpec",
        *,
        args: Sequence[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Schedule a task to run at regular intervals."""

        if spec.mode != "interval" or spec.interval is None:
            raise ValueError("Task is not an interval task or lack `interval` value.")

        if spec.interval <= 0:
            raise ValueError("Interval value must be greater than 0.")

        self._scheduler.add_job(
            func=func,
            trigger=IntervalTrigger(
                seconds=spec.interval,
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
        )

    def cron(
        self,
        func: _func,
        spec: "TaskSpec",
        *,
        args: Sequence[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Schedule a task to run at specific times."""

        if spec.mode != "cron" or spec.cron is None:
            raise ValueError("Task is not a cron task or lack `cron` value.")

        self._scheduler.add_job(
            func=func,
            trigger=CronTrigger.from_crontab(
                spec.cron,
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
        )
