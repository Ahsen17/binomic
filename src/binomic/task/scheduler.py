from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

__all__ = ("TaskScheduler",)


class TaskScheduler:
    """A thin APScheduler wrapper over plain callables.

    It knows nothing about task specs: callers supply the callable, the
    trigger value and the arguments. The client owns one for cron/interval
    delivery; every handler owns one for retry backoff. Times default to UTC.
    """

    def __init__(self, timezone: "tzinfo" = UTC) -> None:

        self._timezone = timezone
        self._scheduler: AsyncIOScheduler | None = None
        self._running = False

    @property
    def running(self) -> bool:
        """Whether ``start`` has been called and ``close`` has not."""

        return self._running

    @property
    def client(self) -> "AsyncIOScheduler":
        """The underlying scheduler, created and started on first access."""

        if self._scheduler is None:
            self._scheduler = AsyncIOScheduler()
            self._scheduler.start()

        return self._scheduler

    def start(self) -> None:
        """Start the scheduler; idempotent while already running."""

        if self._running:
            return

        self.client  # noqa: B018
        self._running = True

    def close(self) -> None:
        """Shut the scheduler down without waiting for running jobs."""

        if not self._running:
            return

        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

        self._running = False

    def delay(
        self,
        func: Callable[..., Any | Awaitable[Any]],
        *,
        delay: float,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Run ``func`` once, ``delay`` seconds from now.

        Misfire grace is disabled, so a late job still runs instead of being
        silently dropped.
        """

        self.client.add_job(
            func=func,
            trigger=DateTrigger(
                run_date=datetime.now(self._timezone) + timedelta(seconds=delay),
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
            misfire_grace_time=None,
        )

    def cron(
        self,
        func: Callable[..., Any | Awaitable[Any]],
        *,
        cron: str,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Run ``func`` on the crontab expression ``cron``."""

        self.client.add_job(
            func=func,
            trigger=CronTrigger.from_crontab(
                expr=cron,
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
        )

    def interval(
        self,
        func: Callable[..., Any | Awaitable[Any]],
        *,
        interval: float,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Run ``func`` every ``interval`` seconds."""

        self.client.add_job(
            func=func,
            trigger=IntervalTrigger(
                seconds=interval,
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
        )
