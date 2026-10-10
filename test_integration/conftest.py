"""Integration-test fixtures backed by a real Redis.

Each test runs against its own logical database, so pipelines never share a
keyspace: they can keep stable queue names, cannot collide with a concurrent
run, and need no per-key cleanup. Database 0 is never handed out -- it may hold
whatever the DSN's target service keeps there.

The DSNs come from the application's own environment variable (no test-only
suffix) so the suite runs against whatever service the deployment actually
uses. When it is unset the suite falls back to localhost.
"""

import itertools
import os
from collections.abc import AsyncIterator
from urllib.parse import SplitResult, urlsplit, urlunsplit

import pytest
from redis.asyncio import Redis as AsyncRedis

BASE_DSN: str = (
    os.environ.get("BINOMIC_REDIS_DSN")
    or os.environ.get("BINOMIC_BROKER_DSN")
    or "redis://localhost:6379/0"
)

# Databases handed out to tests, in turn; index 0 is reserved for the service.
_DATABASES = itertools.cycle(range(1, 16))


def _with_database(dsn: str, index: int) -> str:

    parts: SplitResult = urlsplit(dsn)

    return urlunsplit(parts._replace(path=f"/{index}"))


@pytest.fixture
def db_index() -> int:
    """A database index exclusive to this test."""

    return next(_DATABASES)


@pytest.fixture
def broker_dsn(db_index: int) -> str:

    return _with_database(BASE_DSN, db_index)


@pytest.fixture
def probe_dsn(broker_dsn: str) -> str:
    """The DSN the probe client and the sample tasks reach this test's database on."""

    return broker_dsn


@pytest.fixture
async def redis_client(probe_dsn: str) -> AsyncIterator[AsyncRedis]:
    """A client on this test's database; skips the suite when Redis is down."""

    client = AsyncRedis.from_url(probe_dsn, decode_responses=True)
    try:
        await client.ping()
    except Exception as err:
        await client.aclose()
        pytest.skip(f"Real Redis unavailable at {probe_dsn}: {err}")
    yield client
    await client.flushdb()
    await client.aclose()
