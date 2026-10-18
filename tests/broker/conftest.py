"""Fixtures scoped to broker-module tests."""

from collections.abc import AsyncIterator, Callable

import pytest

from binomic.broker import RedisBroker


@pytest.fixture
async def broker(
    make_broker: Callable[..., RedisBroker],
) -> AsyncIterator[RedisBroker]:
    """An initialized fakeredis-backed broker on a single default queue."""

    instance = make_broker(["default"])
    await instance.start()
    yield instance
    await instance.close()
