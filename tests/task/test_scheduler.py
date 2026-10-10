import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import anyio
import anyio.lowlevel
import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from binomic.task.registry import TaskSpec
from binomic.task.scheduler import TaskScheduler


def target(*args: Any, **kwargs: Any) -> None:
    """Stand-in job target: accepts whatever the scheduler passes it."""


# One of `TaskScheduler.delay` / `.interval` / `.cron`, taken unbound so a case
# can carry the method itself rather than an attribute name.
Schedule = Callable[..., None]

SCHEDULES: list[tuple[Schedule, dict[str, Any]]] = [
    (TaskScheduler.delay, {"mode": "delay", "delay": 30.0}),
    (TaskScheduler.interval, {"mode": "interval", "interval": 5.0}),
    (TaskScheduler.cron, {"mode": "cron", "cron": "* * * * *"}),
]


@pytest.fixture
def scheduler() -> TaskScheduler:
    """An unstarted task scheduler: jobs stay pending, so they can be read back."""

    return TaskScheduler()


@pytest.fixture
def make_spec() -> Callable[..., TaskSpec]:
    """Build specs of any mode; the scheduler only reads the schedule fields."""

    def _make(**kwargs: Any) -> TaskSpec:

        return TaskSpec(name="scheduled", queue="default", fn=target, **kwargs)

    return _make


class TestTaskSchedulerGuards:
    @pytest.mark.parametrize(
        ("schedule", "spec_kwargs", "match"),
        [
            pytest.param(
                TaskScheduler.delay,
                {"mode": "interval", "interval": 5.0},
                "not a delay task",
                id="delay-rejects-interval-spec",
            ),
            pytest.param(
                TaskScheduler.delay,
                {"mode": "delay"},
                "not a delay task",
                id="delay-rejects-missing-value",
            ),
            pytest.param(
                TaskScheduler.interval,
                {"mode": "cron", "cron": "* * * * *"},
                "not an interval task",
                id="interval-rejects-cron-spec",
            ),
            pytest.param(
                TaskScheduler.interval,
                {"mode": "interval"},
                "not an interval task",
                id="interval-rejects-missing-value",
            ),
            pytest.param(
                TaskScheduler.cron,
                {"mode": "interval", "interval": 5.0},
                "not a cron task",
                id="cron-rejects-interval-spec",
            ),
            pytest.param(
                TaskScheduler.cron,
                {"mode": "cron"},
                "not a cron task",
                id="cron-rejects-missing-expression",
            ),
        ],
    )
    def test_rejects_a_spec_it_does_not_own(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
        match: str,
    ) -> None:

        with pytest.raises(ValueError, match=match):
            schedule(scheduler, target, make_spec(**spec_kwargs))

        assert scheduler._scheduler.get_jobs() == []

    @pytest.mark.parametrize(
        ("schedule", "spec_kwargs", "match"),
        [
            pytest.param(
                TaskScheduler.delay,
                {"mode": "delay", "delay": 0.0},
                "Delay value must be greater than 0",
                id="delay-zero",
            ),
            pytest.param(
                TaskScheduler.delay,
                {"mode": "delay", "delay": -5.0},
                "Delay value must be greater than 0",
                id="delay-negative",
            ),
            pytest.param(
                TaskScheduler.interval,
                {"mode": "interval", "interval": 0.0},
                "Interval value must be greater than 0",
                id="interval-zero",
            ),
            pytest.param(
                TaskScheduler.interval,
                {"mode": "interval", "interval": -5.0},
                "Interval value must be greater than 0",
                id="interval-negative",
            ),
        ],
    )
    def test_rejects_a_non_positive_schedule_value(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
        match: str,
    ) -> None:

        with pytest.raises(ValueError, match=match):
            schedule(scheduler, target, make_spec(**spec_kwargs))

        assert scheduler._scheduler.get_jobs() == []


class TestTaskSchedulerJobs:
    def test_delay_runs_once_after_its_delay(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
    ) -> None:

        before = datetime.now(UTC)

        scheduler.delay(target, make_spec(mode="delay", delay=30.0))

        (job,) = scheduler._scheduler.get_jobs()
        assert isinstance(job.trigger, DateTrigger)
        # `DateTrigger` carries no `timezone`; the zone lives on the run date.
        assert job.trigger.run_date.tzinfo == UTC
        # Bracketed by the clock around the call, so the assertion stays exact
        # without depending on how long the call itself took.
        assert (
            before + timedelta(seconds=30)
            <= job.trigger.run_date
            <= datetime.now(UTC) + timedelta(seconds=30)
        )

    @pytest.mark.parametrize(
        ("spec_kwargs", "seconds"),
        [
            # A redelivery defers for a spec that is not a delay task at all.
            pytest.param({}, 5.0, id="a-direct-task"),
            pytest.param(
                {"mode": "delay", "delay": 300.0},
                5.0,
                id="overriding-the-declared-delay",
            ),
        ],
    )
    def test_an_explicit_delay_defers_for_that_many_seconds(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
        spec_kwargs: dict[str, Any],
        seconds: float,
    ) -> None:

        before = datetime.now(UTC)

        scheduler.delay(target, make_spec(**spec_kwargs), delay=seconds)

        (job,) = scheduler._scheduler.get_jobs()
        assert (
            before + timedelta(seconds=seconds)
            <= job.trigger.run_date
            <= datetime.now(UTC) + timedelta(seconds=seconds)
        )

    def test_interval_repeats_every_interval(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
    ) -> None:

        scheduler.interval(target, make_spec(mode="interval", interval=5.0))

        (job,) = scheduler._scheduler.get_jobs()
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval == timedelta(seconds=5.0)
        # Compared as objects: `str()` cannot tell the local-time fallback
        # (`ZoneInfo("UTC")`) apart from `datetime.UTC` on a UTC host.
        assert job.trigger.timezone == UTC

    def test_cron_follows_its_crontab(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
    ) -> None:

        scheduler.cron(target, make_spec(mode="cron", cron="*/5 * * * *"))

        (job,) = scheduler._scheduler.get_jobs()
        assert isinstance(job.trigger, CronTrigger)
        assert job.trigger.timezone == UTC
        # `*/5 * * * *` means every 5 minutes: from 00:02 the next fire is 00:05.
        assert job.trigger.get_next_fire_time(
            None, datetime(2026, 1, 1, 0, 2, tzinfo=UTC)
        ) == datetime(2026, 1, 1, 0, 5, tzinfo=UTC)

    @pytest.mark.parametrize(("schedule", "spec_kwargs"), SCHEDULES)
    def test_forwards_the_callable_arguments_and_keywords(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
    ) -> None:

        schedule(
            scheduler,
            target,
            make_spec(**spec_kwargs),
            args=("default", 1),
            kwargs={"retries": 2},
        )

        (job,) = scheduler._scheduler.get_jobs()
        assert job.func is target
        assert job.args == ("default", 1)
        assert job.kwargs == {"retries": 2}

    @pytest.mark.parametrize(("schedule", "spec_kwargs"), SCHEDULES)
    def test_defaults_to_no_arguments(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
    ) -> None:

        schedule(scheduler, target, make_spec(**spec_kwargs))

        (job,) = scheduler._scheduler.get_jobs()
        assert job.args == ()
        assert job.kwargs == {}

    def test_honours_the_configured_timezone(
        self, make_spec: Callable[..., TaskSpec]
    ) -> None:
        """The configured zone reaches the trigger, not the host's local zone."""

        zone = ZoneInfo("America/New_York")
        scheduler = TaskScheduler(timezone=zone)

        scheduler.interval(target, make_spec(mode="interval", interval=5.0))

        (job,) = scheduler._scheduler.get_jobs()
        assert job.trigger.timezone == zone


class TestTaskSchedulerLifecycle:
    async def test_a_delay_the_loop_reaches_late_still_runs(
        self,
        scheduler: TaskScheduler,
        make_spec: Callable[..., TaskSpec],
    ) -> None:
        """A deferred job runs late rather than being discarded as a missed one."""

        fired: list[str] = []

        scheduler.start()
        scheduler.delay(
            lambda: fired.append("late"),
            make_spec(mode="delay", delay=0.05),
        )

        # Blocking the loop is the scenario under test, not an accident: the
        # default grace is one second, so stalling past it is what separates
        # "runs late" from "skipped as missed".
        time.sleep(1.05)  # noqa: ASYNC251

        with anyio.fail_after(1.0):
            while not fired:
                await anyio.sleep(0.01)

        scheduler.shutdown()

        assert fired == ["late"]

    async def test_start_and_shutdown_toggle_the_scheduler(
        self, scheduler: TaskScheduler
    ) -> None:

        scheduler.start()
        assert scheduler._scheduler.running is True

        scheduler.shutdown()
        # `AsyncIOScheduler.shutdown` only hands the teardown to the event loop,
        # so the scheduler is still running until the loop gets a turn.
        await anyio.lowlevel.checkpoint()

        assert scheduler._scheduler.running is False
