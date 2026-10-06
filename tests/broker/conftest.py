"""Fixtures scoped to broker-module tests."""

from collections.abc import AsyncIterator, Callable

import pytest

from binomic.broker import AsyncredisBroker


@pytest.fixture
async def broker(
    make_broker: Callable[..., AsyncredisBroker],
) -> AsyncIterator[AsyncredisBroker]:
    """An initialized fakeredis-backed broker on a single default queue."""

    instance = make_broker(["default"])
    await instance.initialize()
    yield instance
    await instance.aclose()
