"""Global test fixtures shared by every test subtree."""

import json
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from typing import Any, cast

import pytest
import structlog
from fakeredis import FakeAsyncRedis, FakeServer

from binomic.broker import Message, MessageEntry, RedisBroker

__all__ = ()


@pytest.fixture(autouse=True, scope="session")
def structlog_stdlib() -> Iterator[None]:
    """Route structlog through stdlib logging so ``caplog`` captures records.

    ``src/`` logs via ``structlog.stdlib.get_logger``, which without
    configuration prints straight to stdout, where caplog cannot see it.
    Recreating the stdlib defaults sends those calls through stdlib logging
    instead; ``log_level=None`` leaves logging's own handlers — owned by
    pytest — untouched.
    """

    saved = structlog.get_config()
    structlog.stdlib.recreate_defaults(log_level=None)
    yield
    structlog.configure(**saved)


@pytest.fixture
def fakeredis_server() -> FakeServer:

    return FakeServer()


@pytest.fixture
async def fake_redis(fakeredis_server: FakeServer) -> AsyncIterator[FakeAsyncRedis]:

    client = FakeAsyncRedis(server=fakeredis_server, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def make_message() -> Callable[..., Message]:
    """Build Message with per-test overrides visible in the test body.

    ``id`` is left to the Message default (uuid7) unless overridden via
    ``**overrides``; ``args``/``kwargs`` stay ``None`` unless given, matching
    the production default for parameterless tasks.
    """

    def _make(
        *,
        task: str = "noop",
        attempt: int = 1,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        **overrides: Any,
    ) -> Message:

        return Message(
            task=task,
            attempt=attempt,
            args=args,
            kwargs=kwargs,
            **overrides,
        )

    return _make


@pytest.fixture
def make_broker(
    monkeypatch: pytest.MonkeyPatch,
    fakeredis_server: FakeServer,
) -> Callable[..., RedisBroker]:
    """Build RedisBroker whose redis clients are backed by fakeredis.

    The broker resolves its client through ``Redis.from_pool`` inside
    ``binomic.broker.redis.broker``; patching that name routes every
    connection to a FakeAsyncRedis sharing one FakeServer. ``register_script``
    works on the fake client, so enqueue/ack/dead/retry exercise the real Lua
    through fakeredis-lua.
    """

    def _make(
        queues: list[str],
        namespace: str = "binomic",
        **config: Any,
    ) -> RedisBroker:

        class FakeClientFactory:
            @staticmethod
            def from_pool(_pool: object) -> FakeAsyncRedis:

                return FakeAsyncRedis(server=fakeredis_server, **config)

        monkeypatch.setattr(
            "binomic.broker.redis.broker.Redis",
            FakeClientFactory,
        )
        config.setdefault("decode_responses", True)
        return RedisBroker(
            dsn="redis://localhost:6379/0",
            queues=queues,
            namespace=namespace,
            **config,
        )

    return _make


@pytest.fixture
def fake_broker_backend(make_broker: Callable[..., RedisBroker]) -> None:
    """Arm the fakeredis patch: real BrokerFactory calls land on a FakeServer.

    ``make_broker`` patches at call time, so a test that only needs the
    backend (the worker builds its own broker) takes this fixture instead.
    """

    make_broker(["default"])


@pytest.fixture
def deliver_entry() -> Callable[..., Coroutine[Any, Any, MessageEntry]]:
    """Hand the next undelivered entry to a consumer, as the broker would.

    fakeredis flattens XREADGROUP's nested reply inside Lua, so the
    script-side ``acquire`` cannot assemble entries there; the Python-side
    read sees the same stream and hash state and builds the entry the real
    acquire returns. Entry delivery on real Redis is covered by the
    integration suite.
    """

    async def _deliver(
        broker: RedisBroker,
        queue: str,
        consumer: str,
    ) -> MessageEntry:

        stream = f"{broker._group}:{queue}:stream"
        result = await broker._ensure_client().xreadgroup(
            broker._group,
            consumer,
            {stream: ">"},
            count=10,
        )

        # redis-py parses the reply into a dict; fakeredis hands back lists.
        delivered = result[stream] if isinstance(result, dict) else result[0][1]
        entry_id, _ = cast(
            "list[tuple[str, dict[str, str]]]",
            delivered,
        )[0]

        raw = await broker._ensure_client().hget(
            f"{broker._group}:{queue}:hash",
            entry_id,
        )
        assert raw is not None
        body = json.loads(raw)

        return MessageEntry.from_dict(
            {
                "id": entry_id,
                "queue": queue,
                "message": body["message"],
                "enqueued_at": body["enqueued_at"],
            }
        )

    return _deliver
