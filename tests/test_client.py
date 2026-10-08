from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pytest_mock import MockerFixture

from binomic.broker import AsyncredisBroker
from binomic.client import Binomic, BinomicFactory
from binomic.config import BinomicConfig
from binomic.message import Message
from binomic.task.registry import TaskRegistry, TaskSpec


@pytest.fixture
def config() -> BinomicConfig:

    return BinomicConfig(queues=["default"])


@pytest.fixture
def noop_task(isolate_registry: TaskRegistry) -> TaskSpec:
    """Register the `noop` task that submitted messages resolve to."""

    spec = TaskSpec(name="noop", queue="default", fn=lambda: None)
    isolate_registry.register(spec)
    return spec


@pytest.fixture
async def binomic(
    config: BinomicConfig,
    isolate_registry: TaskRegistry,
) -> AsyncIterator[Binomic]:
    """A client whose `arun` registers tasks into an isolated registry."""

    client = Binomic(
        broker_dsn="redis://localhost:6379/0",
        redis_dsn="redis://localhost:6379/0",
        module_name="binomic",
        config=config,
    )
    yield client
    await client.aclose()


@pytest.fixture
async def running_binomic(
    binomic: Binomic,
    config: BinomicConfig,
    make_broker: Callable[..., AsyncredisBroker],
    mocker: MockerFixture,
) -> AsyncIterator[Binomic]:
    """The `binomic` client once `arun` has started its scheduler.

    The broker the client builds resolves its redis clients through
    ``make_broker``'s fakeredis wiring. ``autodiscover`` is stubbed so the
    registry holds exactly what a test registered: discovering `binomic`
    would also register the sample tasks shipped in `binomic.tasks`.
    """

    make_broker(config.queues)  # for its fakeredis wiring, not the broker it builds
    mocker.patch("binomic.client.autodiscover")
    await binomic.arun()

    yield binomic


@pytest.fixture
def scheduler(running_binomic: Binomic) -> AsyncIOScheduler:
    """The started scheduler of a client that ran."""

    scheduler = running_binomic._scheduler
    assert scheduler is not None

    return scheduler


# Schedule registration is driven through the unbound methods, so cases carry
# the method itself rather than an attribute name: `getattr` by string is untyped.
Schedule = Callable[[Binomic, TaskSpec], None]


class TestBinomicScheduling:
    @pytest.mark.parametrize(
        ("schedule", "spec_kwargs", "match"),
        [
            pytest.param(
                Binomic._interval,
                {"mode": "cron", "cron": "* * * * *"},
                "not an interval task",
                id="interval-rejects-cron-spec",
            ),
            pytest.param(
                Binomic._interval,
                {"mode": "interval"},
                "not an interval task",
                id="interval-rejects-missing-value",
            ),
            pytest.param(
                Binomic._cron,
                {"mode": "interval", "interval": 10.0},
                "not a cron task",
                id="cron-rejects-interval-spec",
            ),
            pytest.param(
                Binomic._cron,
                {"mode": "cron"},
                "not a cron task",
                id="cron-rejects-missing-expression",
            ),
        ],
    )
    async def test_a_schedule_rejects_a_spec_it_does_not_own(
        self,
        binomic: Binomic,
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
        match: str,
    ) -> None:

        spec = TaskSpec(name="scheduled", queue="default", fn=lambda: None, **spec_kwargs)

        with pytest.raises(ValueError, match=match):
            schedule(binomic, spec)

    @pytest.mark.parametrize(
        ("schedule", "spec_kwargs"),
        [
            pytest.param(Binomic._cron, {"mode": "cron", "cron": "* * * * *"}, id="cron"),
            pytest.param(
                Binomic._interval, {"mode": "interval", "interval": 10.0}, id="interval"
            ),
        ],
    )
    async def test_a_schedule_requires_a_client_that_ran(
        self,
        binomic: Binomic,
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
    ) -> None:

        spec = TaskSpec(name="scheduled", queue="default", fn=lambda: None, **spec_kwargs)

        with pytest.raises(RuntimeError, match="Run client `arun`"):
            schedule(binomic, spec)

    async def test_cron_registers_a_crontab_job(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
    ) -> None:

        spec = TaskSpec(
            name="tick",
            queue="default",
            fn=lambda: None,
            mode="cron",
            cron="*/5 * * * *",
        )

        running_binomic._cron(spec)

        (job,) = scheduler.get_jobs()
        assert isinstance(job.trigger, CronTrigger)
        # Compared as objects, not by `str()`: the local-time fallback is a
        # `ZoneInfo("UTC")` on a UTC host, which strings identically.
        assert job.trigger.timezone == UTC
        # `*/5 * * * *` means every 5 minutes: from 00:02 the next fire is 00:05.
        assert job.trigger.get_next_fire_time(
            None, datetime(2026, 1, 1, 0, 2, tzinfo=UTC)
        ) == datetime(2026, 1, 1, 0, 5, tzinfo=UTC)

    async def test_interval_registers_a_repeating_job(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
    ) -> None:

        spec = TaskSpec(
            name="heartbeat",
            queue="default",
            fn=lambda: None,
            mode="interval",
            interval=10.0,
        )

        running_binomic._interval(spec)

        (job,) = scheduler.get_jobs()
        assert isinstance(job.trigger, IntervalTrigger)
        assert job.trigger.interval == timedelta(seconds=10.0)
        assert job.trigger.timezone == UTC

    @pytest.mark.parametrize(
        ("schedule", "spec_kwargs"),
        [
            # A yearly crontab keeps the job from firing again on its own while
            # the test runs; the interval is far longer than the test either.
            pytest.param(
                Binomic._cron,
                {"mode": "cron", "cron": "0 0 1 1 *"},
                id="cron",
            ),
            pytest.param(
                Binomic._interval,
                {"mode": "interval", "interval": 600.0},
                id="interval",
            ),
        ],
    )
    async def test_a_fired_job_enqueues_its_message(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
        schedule: Schedule,
        spec_kwargs: dict[str, Any],
    ) -> None:

        spec = TaskSpec(name="scheduled", queue="default", fn=lambda: None, **spec_kwargs)

        schedule(running_binomic, spec)

        (job,) = scheduler.get_jobs()
        await job.func(*job.args)  # what the scheduler does when the job fires

        broker = running_binomic._broker
        assert broker is not None
        (entry,) = await broker.acquire("tester", count=1)
        msg = Message.from_json(entry.fields["message"])

        assert entry.queue == "default"
        assert msg.name == "scheduled"
        assert msg.enqueued_at is not None

    async def test_a_context_registers_scheduled_tasks_only(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
        isolate_registry: TaskRegistry,
        mocker: MockerFixture,
    ) -> None:

        isolate_registry.register(
            TaskSpec(
                name="tick", queue="cron", fn=lambda: None, mode="cron", cron="* * * * *"
            )
        )
        isolate_registry.register(
            TaskSpec(
                name="heartbeat",
                queue="interval",
                fn=lambda: None,
                mode="interval",
                interval=10.0,
            )
        )
        isolate_registry.register(
            TaskSpec(name="example", queue="default", fn=lambda: None)
        )
        isolate_registry.register(
            TaskSpec(
                name="later", queue="delay", fn=lambda: None, mode="delay", delay=5.0
            )
        )
        master = mocker.Mock()
        master.arun = mocker.AsyncMock()
        master.aclose = mocker.AsyncMock()
        mocker.patch("binomic.client.Master", return_value=master)

        async with running_binomic:
            registered = scheduler.get_jobs()
            jobs = {job.args[0]: job for job in registered}

        # Only the scheduled modes are registered, one job each.
        assert len(registered) == 2
        assert set(jobs) == {"cron", "interval"}
        assert isinstance(jobs["cron"].trigger, CronTrigger)
        assert isinstance(jobs["interval"].trigger, IntervalTrigger)
        assert jobs["cron"].args[1].name == "tick"
        assert jobs["interval"].args[1].name == "heartbeat"


class TestBinomic:
    async def test_submit_rejects_a_client_that_never_ran(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        make_message: Callable[..., Message],
    ) -> None:

        with pytest.raises(RuntimeError, match="Run client `arun`"):
            await binomic.submit(make_message())

    @pytest.mark.parametrize(
        "spec_kwargs",
        [
            pytest.param({"mode": "cron", "cron": "* * * * *"}, id="cron"),
            pytest.param({"mode": "interval", "interval": 10.0}, id="interval"),
        ],
    )
    async def test_submit_rejects_a_scheduled_task(
        self,
        binomic: Binomic,
        isolate_registry: TaskRegistry,
        make_message: Callable[..., Message],
        spec_kwargs: dict[str, Any],
    ) -> None:

        isolate_registry.register(
            TaskSpec(name="scheduled", queue="default", fn=lambda: None, **spec_kwargs)
        )

        with pytest.raises(ValueError, match="only supports direct and delay"):
            await binomic.submit(make_message(name="scheduled"))

    async def test_submit_builds_broker_on_arun(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        mocker: MockerFixture,
        make_message: Callable[..., Message],
    ) -> None:

        broker = mocker.AsyncMock()
        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.return_value = broker
        await binomic.arun()
        msg = make_message()

        assert await binomic.submit(msg) == msg.id

        factory.assert_called_once_with("redis://localhost:6379/0", ["default"])
        broker.enqueue.assert_awaited_once_with("default", msg)

    async def test_submit_reuses_broker_instance(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        mocker: MockerFixture,
        make_message: Callable[..., Message],
    ) -> None:

        broker = mocker.AsyncMock()
        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.return_value = broker
        await binomic.arun()

        await binomic.submit(make_message())
        await binomic.submit(make_message())

        factory.assert_called_once()
        assert broker.enqueue.await_count == 2


class TestBinomicClose:
    async def test_master_failure_still_closes_the_client(
        self,
        binomic: Binomic,
        mocker: MockerFixture,
    ) -> None:

        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.return_value = mocker.AsyncMock()
        master = mocker.Mock()
        master.arun = mocker.AsyncMock()
        master.aclose = mocker.AsyncMock(side_effect=RuntimeError("master boom"))
        mocker.patch("binomic.client.Master", return_value=master)

        with pytest.raises(RuntimeError, match="master boom"):
            async with binomic:
                pass

        # A failing `master.aclose()` must not skip the client's own teardown.
        assert binomic._broker is None
        assert binomic._scheduler is None


class TestBinomicFactory:
    def test_create_returns_singleton(self, config: BinomicConfig) -> None:

        factory = BinomicFactory(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            config=config,
        )

        assert factory.create() is factory.create()

    def test_create_builds_binomic(self, config: BinomicConfig) -> None:

        factory = BinomicFactory(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            config=config,
        )

        client = factory.create()

        assert isinstance(client, Binomic)
