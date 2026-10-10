from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
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
    """The APScheduler that backs the client's task scheduler.

    Reached through ``TaskScheduler``: it exposes no job accessor of its own.
    """

    task_scheduler = running_binomic._scheduler
    assert task_scheduler is not None

    return task_scheduler._scheduler


class TestBinomicScheduling:
    @pytest.mark.parametrize(
        "spec_kwargs",
        [
            # A yearly crontab and a ten-minute interval keep the job from firing
            # again on its own while the test runs.
            pytest.param({"mode": "cron", "cron": "0 0 1 1 *"}, id="cron"),
            pytest.param({"mode": "interval", "interval": 600.0}, id="interval"),
        ],
    )
    async def test_a_registered_job_enqueues_its_message(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
        isolate_registry: TaskRegistry,
        spec_kwargs: dict[str, Any],
    ) -> None:

        isolate_registry.register(
            TaskSpec(name="scheduled", queue="default", fn=lambda: None, **spec_kwargs)
        )

        running_binomic._register_interval_cron_tasks()

        (job,) = scheduler.get_jobs()
        await job.func(*job.args)  # what the scheduler does when the job fires

        broker = running_binomic._broker
        assert broker is not None
        (entry,) = await broker.acquire("tester", count=1)
        msg = Message.from_json(entry.fields["message"])

        assert entry.queue == "default"
        assert msg.name == "scheduled"

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
    async def test_arun_closes_a_broker_when_initialization_fails(
        self,
        binomic: Binomic,
        mocker: MockerFixture,
    ) -> None:

        broker = mocker.AsyncMock()
        broker.initialize.side_effect = RuntimeError("broker boom")
        mocker.patch(
            "binomic.client.BrokerFactory"
        ).return_value.create.return_value = broker

        with pytest.raises(RuntimeError, match="broker boom"):
            await binomic.arun()

        broker.aclose.assert_awaited_once()
        assert binomic._broker is None
        assert binomic._scheduler is None

    async def test_arun_rolls_back_a_broker_when_scheduler_start_fails(
        self,
        binomic: Binomic,
        mocker: MockerFixture,
    ) -> None:

        broker = mocker.AsyncMock()
        mocker.patch(
            "binomic.client.BrokerFactory"
        ).return_value.create.return_value = broker
        scheduler = mocker.patch("binomic.client.TaskScheduler")
        scheduler.return_value.start.side_effect = RuntimeError("scheduler boom")

        with pytest.raises(RuntimeError, match="scheduler boom"):
            await binomic.arun()

        broker.aclose.assert_awaited_once()
        assert binomic._broker is None
        assert binomic._scheduler is None

    async def test_submit_rejects_a_client_that_never_ran(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        make_message: Callable[..., Message],
    ) -> None:

        with pytest.raises(RuntimeError, match="Run client `arun`"):
            await binomic.submit(make_message())

    async def test_submit_rejects_a_delay_task_on_a_client_that_never_ran(
        self,
        binomic: Binomic,
        isolate_registry: TaskRegistry,
        make_message: Callable[..., Message],
    ) -> None:

        isolate_registry.register(
            TaskSpec(
                name="later", queue="default", fn=lambda: None, mode="delay", delay=5.0
            )
        )

        with pytest.raises(RuntimeError, match="Run client `arun`"):
            await binomic.submit(make_message(name="later"))

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

    async def test_submit_schedules_a_delay_task(
        self,
        running_binomic: Binomic,
        scheduler: AsyncIOScheduler,
        isolate_registry: TaskRegistry,
        make_message: Callable[..., Message],
    ) -> None:

        isolate_registry.register(
            TaskSpec(
                name="later", queue="default", fn=lambda: None, mode="delay", delay=30.0
            )
        )
        msg = make_message(name="later")

        assert await running_binomic.submit(msg) == msg.id

        (job,) = scheduler.get_jobs()
        assert isinstance(job.trigger, DateTrigger)
        assert job.args == ("default", msg)

    async def test_submit_builds_broker_on_arun(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        mocker: MockerFixture,
        make_message: Callable[..., Message],
    ) -> None:

        broker = mocker.AsyncMock()
        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.create.return_value = broker
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
        factory.return_value.create.return_value = broker
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
        factory.return_value.create.return_value = mocker.AsyncMock()
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
