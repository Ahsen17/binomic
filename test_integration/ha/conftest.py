"""Fixtures for the Redis HA topology suites (contract: task docs/api.md).

These suites deliberately bypass the logical-database rotation used by the
rest of test_integration: Redis Cluster exposes a single database. Isolation
comes from a unique namespace prefix per test, and teardown scans and deletes
that prefix. Both topologies run host-networked behind fixed ports (see
assets/), so connectivity is a plain TCP probe -- when the compose
environments are down the suites skip, mirroring the real-Redis contract.
"""

import os
import socket
import uuid
from collections.abc import (
    AsyncIterator,
    Callable,
    Coroutine,
    Sequence,
)
from typing import Any, cast

import pytest
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.sentinel import Sentinel

from binomic.broker import RedisBroker

SENTINEL_PORT_ENV: str = "BINOMIC_HA_SENTINEL_PORT"
CLUSTER_NODE_ENV: str = "BINOMIC_HA_CLUSTER_NODE"
DEFAULT_SENTINEL_PORT: int = 26380
DEFAULT_CLUSTER_NODE: str = "127.0.0.1:7000"
MASTER_NAME: str = "binomic"
MASTER_PORT: int = 6380
CLUSTER_PORT: int = 7000

# Placeholder DSNs: the injection path below never dials them (docs/api.md §4).
_SENTINEL_DSN: str = f"redis://127.0.0.1:{MASTER_PORT}/0"
_CLUSTER_DSN: str = f"redis://127.0.0.1:{CLUSTER_PORT}/0"


def _sentinel_port() -> int:

    return int(os.environ.get(SENTINEL_PORT_ENV, DEFAULT_SENTINEL_PORT))


def _cluster_node() -> tuple[str, int]:

    raw = os.environ.get(CLUSTER_NODE_ENV, DEFAULT_CLUSTER_NODE)
    host, _, port = raw.rpartition(":")

    return (host or "127.0.0.1", int(port))


def _port_open(port: int) -> bool:

    with socket.socket() as sock:
        sock.settimeout(1.0)

        return sock.connect_ex(("127.0.0.1", port)) == 0


class SentinelRedisBroker(RedisBroker):
    """Broker wired to the Sentinel-resolved primary.

    Test-side subclass (task docs/api.md §4): `_ensure_client` injects the
    master client a `Sentinel` instance resolves, so the framework's own
    connection code never runs. The parent's `close` works unchanged --
    `master_for` returns a standard async Redis client.
    """

    def _ensure_client(self) -> AsyncRedis:

        if self._client is None:
            port = _sentinel_port()
            # redis.asyncio.sentinel ships unannotated in redis-py 8.1, so
            # strict mypy flags the constructor; the return types used below
            # (master_for -> Redis) are checked by the runtime contract.
            sentinel = Sentinel(  # type: ignore[no-untyped-call]
                [
                    ("127.0.0.1", port),
                    ("127.0.0.1", port + 1),
                    ("127.0.0.1", port + 2),
                ]
            )
            self._client = sentinel.master_for(MASTER_NAME, decode_responses=True)

        return self._client


class ClusterRedisBroker(RedisBroker):
    """Broker wired to a `RedisCluster` client.

    The parent stores its client as ``Redis | None``; a cluster client is not
    a ``Redis`` subclass, so the assignment goes through ``cast`` -- the
    parent's attribute type is the framework's own declaration, and every
    method invoked on the value (the scripts and `aclose`) exists on both.

    `close` branches on the client type: the async cluster client's
    `aclose()` takes no arguments, so the parent's
    `aclose(close_connection_pool=True)` would raise TypeError.
    """

    def _ensure_client(self) -> AsyncRedis:

        if self._client is None:
            host, port = _cluster_node()
            self._client = cast(
                "AsyncRedis", RedisCluster(host=host, port=port, decode_responses=True)
            )

        return self._client

    async def close(self) -> None:

        if not self._running:
            return

        if self._client:
            await self._client.aclose()
            self._client = None

        if self._scripts:
            self._scripts.clear()

        self._running = False


BrokerFactory = Callable[..., Coroutine[Any, Any, tuple[RedisBroker, AsyncRedis]]]


async def _cleanup_namespace(client: AsyncRedis | RedisCluster, namespace: str) -> None:

    async for key in client.scan_iter(match=f"{namespace}:*", count=100):
        await client.delete(key)


@pytest.fixture
def ha_namespace() -> str:
    """A namespace unique to this test; Cluster has no databases to rotate."""

    return f"ha-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def make_sentinel_broker(ha_namespace: str) -> AsyncIterator[BrokerFactory]:
    """Factory for brokers on the Sentinel topology; skips when it is down."""

    if not _port_open(_sentinel_port()):
        pytest.skip("Sentinel topology unavailable (assets/sentinel compose is down)")

    brokers: list[RedisBroker] = []

    async def _make(
        queues: Sequence[str],
    ) -> tuple[SentinelRedisBroker, AsyncRedis]:

        broker = SentinelRedisBroker(
            dsn=_SENTINEL_DSN,
            queues=queues,
            namespace=ha_namespace,
            decode_responses=True,
        )
        await broker.start()
        brokers.append(broker)

        # The probe dials the master port directly: failover behavior is out
        # of scope, so the static address is stable for the whole suite.
        probe = AsyncRedis(host="127.0.0.1", port=MASTER_PORT, decode_responses=True)

        return broker, probe

    yield _make

    for broker in brokers:
        await broker.close()
    sweeper = AsyncRedis(host="127.0.0.1", port=MASTER_PORT, decode_responses=True)
    try:
        await _cleanup_namespace(sweeper, ha_namespace)
    finally:
        await sweeper.aclose()


@pytest.fixture
async def make_cluster_broker(ha_namespace: str) -> AsyncIterator[BrokerFactory]:
    """Factory for brokers on the Cluster topology; skips when it is down."""

    host, port = _cluster_node()
    if not _port_open(port):
        pytest.skip("Cluster topology unavailable (assets/cluster compose is down)")

    brokers: list[RedisBroker] = []

    async def _make(
        queues: Sequence[str],
    ) -> tuple[ClusterRedisBroker, AsyncRedis]:

        broker = ClusterRedisBroker(
            dsn=_CLUSTER_DSN,
            queues=queues,
            namespace=ha_namespace,
            decode_responses=True,
        )
        await broker.start()
        brokers.append(broker)

        # A probe on the seed node: fine for spot checks, never for routing
        # assumptions (any node may answer for any key).
        probe = AsyncRedis(host=host, port=port, decode_responses=True)

        return broker, probe

    yield _make

    for broker in brokers:
        await broker.close()
    sweeper = RedisCluster(host=host, port=port, decode_responses=True)
    try:
        await _cleanup_namespace(sweeper, ha_namespace)
    finally:
        await sweeper.aclose()


@pytest.fixture
async def cluster_probe() -> AsyncIterator[RedisCluster]:
    """A bare cluster client for the direct probes; skips when it is down."""

    host, port = _cluster_node()
    if not _port_open(port):
        pytest.skip("Cluster topology unavailable (assets/cluster compose is down)")

    client = RedisCluster(host=host, port=port, decode_responses=True)
    yield client

    await _cleanup_namespace(client, "ha-probe")
    await client.aclose()
