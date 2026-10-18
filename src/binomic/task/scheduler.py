from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

if TYPE_CHECKING:
    from datetime import tzinfo


__all__ = ("TaskScheduler",)


class TaskScheduler:
    """A thin APScheduler wrapper over generic callables.

    ``delay``/``cron``/``interval`` register plain callables — task semantics
    are validated at the call sites, not here. The client owns one instance
    for delay submissions and cron/interval registrations; each worker handler
    owns another for retry backoff only.
    """

    def __init__(self, timezone: "tzinfo" = UTC) -> None:

        self._timezone = timezone
        self._scheduler: AsyncIOScheduler | None = None

        self._running = False

    @property
    def running(self) -> bool:

        return self._running

    def start(self) -> None:

        if self._running:
            return

        self._ensure_scheduler()

        self._running = True

    def close(self) -> None:

        if not self._running:
            return

        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

        self._running = False

    def _ensure_scheduler(self) -> "AsyncIOScheduler":

        if self._scheduler is None:
            self._scheduler = AsyncIOScheduler()
            self._scheduler.start()

        return self._scheduler

    def delay(
        self,
        func: Callable[..., Any | Awaitable[Any]],
        *,
        delay: float,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:

        self._ensure_scheduler().add_job(
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

        self._ensure_scheduler().add_job(
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

        self._ensure_scheduler().add_job(
            func=func,
            trigger=IntervalTrigger(
                seconds=interval,
                timezone=self._timezone,
            ),
            args=args,
            kwargs=kwargs,
        )
