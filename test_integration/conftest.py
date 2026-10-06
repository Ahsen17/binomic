"""Integration-test fixtures backed by a real Redis service.

The DSNs come from the application's own environment variables (no test-only
suffix) so the suite runs against whatever service the deployment actually
uses. When neither variable is set the suite falls back to localhost.
"""

import os
from collections.abc import AsyncIterator, Callable

import pytest
from redis.asyncio import Redis as AsyncRedis

BROKER_DSN: str = os.environ.get("BINOMIC_BROKER_DSN", "redis://localhost:6379/0")
REDIS_DSN: str = os.environ.get("BINOMIC_REDIS_DSN", "redis://localhost:6379/0")


@pytest.fixture(scope="session")
def broker_dsn() -> str:

    return BROKER_DSN


@pytest.fixture(scope="session")
def redis_dsn() -> str:

    return REDIS_DSN


@pytest.fixture
async def real_redis(redis_dsn: str) -> AsyncIterator[AsyncRedis]:
    """A real Redis client; skips the suite when the service is unreachable."""

    client = AsyncRedis.from_url(redis_dsn, decode_responses=True)
    try:
        await client.ping()
    except Exception as err:
        pytest.skip(f"Real Redis unavailable at {redis_dsn}: {err}")
    yield client
    await client.aclose()


@pytest.fixture
async def track_keys(real_redis: AsyncRedis) -> AsyncIterator[Callable[..., None]]:
    """Register integration keys for deletion when the test finishes."""

    keys: list[str] = []

    def _track(*new_keys: str) -> None:

        keys.extend(new_keys)

    yield _track
    if keys:
        await real_redis.delete(*keys)
