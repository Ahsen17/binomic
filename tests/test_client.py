"""Client semantics: registration, submit branches, context lifecycle."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from binomic.broker import Message, RedisBroker
from binomic.client import Binomic, BinomicConfig, BinomicFactory
from binomic.task import TaskNotFoundError

APP_SOURCE = """\
from binomic.task import task


@task("default", mode="direct")
async def alpha() -> None: ...


@task("delayq", mode="delay", delay=5.0)
async def later() -> None: ...


@task("cronq", mode="cron", cron="*/5 * * * *")
async def cronned() -> None: ...


@task("cronq", mode="interval", interval=10.0)
async def ticked() -> None: ...
"""


@pytest.fixture(scope="module")
def client_app(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A task package with one task of every schedulable mode."""

    root = tmp_path_factory.mktemp("clientapp")
    pkg = root / "clientapp"
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "tasks.py").write_text(APP_SOURCE, encoding="utf-8")

    sys.path.insert(0, str(root))
    return root


@pytest.fixture
def config() -> BinomicConfig:

    return BinomicConfig(queues=["default", "delayq", "cronq"])


@pytest.fixture
def make_client(
    client_app: Path,
    config: BinomicConfig,
    fake_broker_backend: None,
) -> Callable[..., Binomic]:
    """Build a client over the sample app with a fakeredis backend."""

    root = str(client_app)
    if root not in sys.path:
        sys.path.insert(0, root)

    def _make(module_name: str = "clientapp", **overrides: Any) -> Binomic:

        return Binomic(
            broker_dsn="redis://localhost:6379/0",
            module_name=module_name,
            config=config,
            **overrides,
        )

    return _make


class TestConstruction:
    def test_autodiscovers_the_module(self, make_client: Callable[..., Binomic]) -> None:

        client = make_client()

        assert {"alpha", "later", "cronned", "ticked"} <= {
            spec.name for spec in client._registry
        }

    def test_unknown_module_fails_construction(
        self, make_client: Callable[..., Binomic]
    ) -> None:

        with pytest.raises(ValueError, match="not found"):
            make_client(module_name="no_such_pkg_xyz")


class TestSubmit:
    async def test_direct_enqueues_immediately(
        self,
        make_client: Callable[..., Binomic],
        make_message: Callable[..., Message],
    ) -> None:

        client = make_client()
        await client.start()

        await client.submit(make_message(task="alpha"))

        broker = client._broker
        assert isinstance(broker, RedisBroker)
        entries = await broker._ensure_client().xrange(
            f"{broker._group}:default:stream", "-", "+"
        )
        assert entries is not None
        assert len(entries) == 1

        await client.close()

    async def test_delay_schedules_instead_of_enqueueing(
        self,
        make_client: Callable[..., Binomic],
        make_message: Callable[..., Message],
    ) -> None:

        client = make_client()
        await client.start()

        await client.submit(make_message(task="later"))

        # No stream entry: the enqueue runs when the scheduled job fires.
        broker = client._broker
        assert isinstance(broker, RedisBroker)
        entries = await broker._ensure_client().xrange(
            f"{broker._group}:default:stream", "-", "+"
        )
        assert entries == []

        scheduler = client._scheduler
        assert scheduler is not None
        underlying = scheduler._scheduler
        assert underlying is not None
        jobs = underlying.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].func.__name__ == "enqueue"
        # (queue, message) pair the job will enqueue with.
        assert jobs[0].args[0] == "delayq"
        assert jobs[0].args[1].task == "later"

        await client.close()

    async def test_unknown_task_raises(
        self,
        make_client: Callable[..., Binomic],
        make_message: Callable[..., Message],
    ) -> None:

        client = make_client()
        await client.start()

        with pytest.raises(TaskNotFoundError):
            await client.submit(make_message(task="ghost"))

        await client.close()

    async def test_scheduled_modes_are_not_submittable(
        self,
        make_client: Callable[..., Binomic],
        make_message: Callable[..., Message],
    ) -> None:

        client = make_client()
        await client.start()

        with pytest.raises(ValueError, match="only supports"):
            await client.submit(make_message(task="cronned"))

        await client.close()


class TestContext:
    async def test_registers_cron_and_interval_jobs(
        self,
        make_client: Callable[..., Binomic],
        mocker: MockerFixture,
    ) -> None:
        """Entering the context registers every cron/interval task as a job
        whose body enqueues the task's message; the Master is assembled but
        mocked out here — real supervision is the integration suite's job."""

        master = mocker.Mock()
        master.start = mocker.AsyncMock()
        master.close = mocker.AsyncMock()
        mocker.patch("binomic.client.client.Master", return_value=master)

        client = make_client()

        async with client:
            scheduler = client._scheduler
            assert scheduler is not None
            underlying = scheduler._scheduler
            assert underlying is not None
            jobs = underlying.get_jobs()
            by_kind = sorted((type(job.trigger).__name__, job.args[0]) for job in jobs)
            assert by_kind == [
                ("CronTrigger", "cronq"),
                ("IntervalTrigger", "cronq"),
            ]

            # Each periodic job carries a prebuilt Message for its task.
            for job in jobs:
                assert isinstance(job.args[1], Message)
                assert job.args[1].task in {"cronned", "ticked"}

        # Torn down: master first, then the client's own resources.
        master.close.assert_awaited_once()
        assert client._broker is None
        assert client._scheduler is None

    async def test_master_failure_propagates_and_still_closes(
        self,
        make_client: Callable[..., Binomic],
        mocker: MockerFixture,
    ) -> None:

        master = mocker.Mock()
        master.start = mocker.AsyncMock(side_effect=RuntimeError("boom"))
        master.close = mocker.AsyncMock()
        mocker.patch("binomic.client.client.Master", return_value=master)

        client = make_client()

        # anyio's task group wraps the child failure in an ExceptionGroup.
        with pytest.raises(BaseExceptionGroup) as exc_info:
            async with client:
                pass

        assert any(
            isinstance(exc, RuntimeError) and "boom" in str(exc)
            for exc in exc_info.value.exceptions
        )
        master.close.assert_awaited_once()
        assert client._broker is None

    async def test_close_is_idempotent(
        self, make_client: Callable[..., Binomic], mocker: MockerFixture
    ) -> None:

        master = mocker.Mock()
        master.start = mocker.AsyncMock()
        master.close = mocker.AsyncMock()
        mocker.patch("binomic.client.client.Master", return_value=master)

        client = make_client()

        async with client:
            pass

        # Context exit already closed everything; a second close is a no-op.
        await client.close()

        assert client._broker is None
        assert client._scheduler is None


class TestStartClose:
    async def test_start_wires_broker_and_scheduler(
        self, make_client: Callable[..., Binomic]
    ) -> None:

        client = make_client()
        await client.start()

        assert isinstance(client._broker, RedisBroker)
        assert client._broker._running
        assert client._scheduler is not None

        await client.close()

    async def test_close_releases_everything(
        self, make_client: Callable[..., Binomic]
    ) -> None:

        client = make_client()
        await client.start()
        await client.close()

        assert client._broker is None
        assert client._scheduler is None


class TestBinomicFactory:
    def test_create_returns_singleton(self, config: BinomicConfig) -> None:

        factory = BinomicFactory(
            broker_dsn="redis://localhost:6379/0",
            module_name="clientapp",
            config=config,
        )

        assert factory.create() is factory.create()

    def test_create_builds_a_binomic(self, config: BinomicConfig) -> None:

        client = BinomicFactory(
            broker_dsn="redis://localhost:6379/0",
            module_name="clientapp",
            config=config,
        ).create()

        assert isinstance(client, Binomic)
        assert client._module_name == "clientapp"
