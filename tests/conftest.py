"""Global test fixtures shared by every test subtree."""

import time
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from binomic.broker import AsyncredisBroker
from binomic.message import Message
from binomic.task.registry import TaskRegistry, registry

__all__ = ()


@pytest.fixture
def fakeredis_server() -> FakeServer:

    return FakeServer()


@pytest.fixture
async def fake_redis(fakeredis_server: FakeServer) -> AsyncIterator[FakeAsyncRedis]:

    client = FakeAsyncRedis(server=fakeredis_server, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def isolate_registry() -> Iterator[TaskRegistry]:
    """Snapshot and restore the module-level task registry around a test."""

    saved = dict(registry._tasks)
    yield registry
    registry._tasks.clear()
    registry._tasks.update(saved)


@pytest.fixture
def make_message() -> Callable[..., Message]:
    """Build Message with per-test overrides visible in the test body.

    ``enqueued_at`` defaults to the current time; build a ``Message`` directly
    when a test needs an explicitly unstamped one.
    """

    def _make(
        *,
        name: str = "noop",
        enqueued_at: float | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> Message:

        return Message(
            name=name,
            enqueued_at=time.time() if enqueued_at is None else enqueued_at,
            args=args if args is not None else [],
            kwargs=kwargs if kwargs is not None else {},
        )

    return _make


@pytest.fixture
def make_broker(
    monkeypatch: pytest.MonkeyPatch,
    fakeredis_server: FakeServer,
) -> Callable[..., AsyncredisBroker]:
    """Build AsyncredisBroker whose redis clients are backed by fakeredis.

    The broker resolves its client through ``AsyncRedis.from_pool`` inside
    ``binomic.broker.redis``; patching that name routes every connection to a
    FakeAsyncRedis sharing one FakeServer.
    """

    def _make(queues: list[str], **config: Any) -> AsyncredisBroker:

        class FakeClientFactory:
            @staticmethod
            def from_pool(_pool: object) -> FakeAsyncRedis:

                return FakeAsyncRedis(server=fakeredis_server, **config)

        monkeypatch.setattr("binomic.broker.redis.AsyncRedis", FakeClientFactory)
        config.setdefault("decode_responses", True)
        return AsyncredisBroker(dsn="redis://localhost:6379/0", queues=queues, **config)

    return _make
