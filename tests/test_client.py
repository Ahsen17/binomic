from collections.abc import AsyncIterator, Callable

import pytest
from pytest_mock import MockerFixture

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


class TestBinomic:
    async def test_submit_rejects_a_client_that_never_ran(
        self,
        binomic: Binomic,
        noop_task: TaskSpec,
        make_message: Callable[..., Message],
    ) -> None:

        with pytest.raises(RuntimeError, match="Run client `arun`"):
            await binomic.submit(make_message())

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
