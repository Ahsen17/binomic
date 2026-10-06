from collections.abc import Callable

import pytest
from pytest_mock import MockerFixture

from binomic.client import Binomic, BinomicFactory
from binomic.config import BinomicConfig
from binomic.message import Message


@pytest.fixture
def config() -> BinomicConfig:

    return BinomicConfig(queues=["default"])


@pytest.fixture
def binomic(config: BinomicConfig) -> Binomic:

    return Binomic(
        broker_dsn="redis://localhost:6379/0",
        redis_dsn="redis://localhost:6379/0",
        module_name="binomic",
        config=config,
    )


class TestBinomic:
    async def test_submit_creates_broker_lazily(
        self,
        binomic: Binomic,
        mocker: MockerFixture,
        make_message: Callable[..., Message],
    ) -> None:

        broker = mocker.AsyncMock()
        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.return_value = broker

        await binomic.submit(make_message())

        factory.assert_called_once_with("redis://localhost:6379/0", ["default"])
        broker.enqueue.assert_awaited_once()

    async def test_submit_reuses_broker_instance(
        self,
        binomic: Binomic,
        mocker: MockerFixture,
        make_message: Callable[..., Message],
    ) -> None:

        broker = mocker.AsyncMock()
        factory = mocker.patch("binomic.client.BrokerFactory")
        factory.return_value.return_value = broker

        await binomic.submit(make_message())
        await binomic.submit(make_message())

        factory.assert_called_once()
        assert broker.enqueue.await_count == 2


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
