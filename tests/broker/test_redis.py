import json
import logging
import subprocess
import sys
import time
from collections.abc import Callable, Coroutine
from typing import Any, cast

import pytest
from pytest_mock import MockerFixture

from binomic.broker import Message, MessageEntry, RedisBroker
from binomic.broker.redis.broker import SCRIPTS
from binomic.broker.redis.schemas import ScriptResponse
from binomic.broker.redis.scripts import _COMMON

STREAM_KEY = "binomic:default:stream"
HASH_KEY = "binomic:default:hash"
DLQ_STREAM_KEY = "binomic:default:dlq:stream"
DLQ_HASH_KEY = "binomic:default:dlq:hash"

Deliver = Callable[..., Coroutine[Any, Any, MessageEntry]]


async def stored_entry_ids(broker: RedisBroker) -> list[str]:
    """Stream entry ids currently in the queue, in insertion order."""

    entries = await broker._ensure_client().xrange(STREAM_KEY, "-", "+")
    assert entries is not None

    return [entry_id for entry_id, _fields in cast("list[tuple[Any, Any]]", entries)]


class TestScripts:
    def test_all_six_operations_are_registered(self) -> None:

        assert set(SCRIPTS) == {
            "enqueue",
            "acquire",
            "ack",
            "dead",
            "retry",
            "reclaim",
        }

    def test_scripts_embed_the_shared_preamble_once(self) -> None:

        for script in SCRIPTS.values():
            assert script.startswith(_COMMON)
            assert script.count("local function reply_ok") == 1

    def test_scripts_take_no_prebuilt_keys(self) -> None:
        """Key generation lives in the scripts: namespace and queue go in as
        arguments and the key names never cross the Python boundary."""

        for script in SCRIPTS.values():
            assert "KEYS" not in script

    def test_scripts_are_valid_lua(self) -> None:
        """Guards the whole class of f-string brace/trailing-comma slip-ups:
        a script that fails to LOAD takes its whole operation down. Runs in a
        subprocess so an lupa crash cannot take the suite with it."""

        probe = """\
import sys
from lupa import LuaRuntime
from binomic.broker.redis.broker import SCRIPTS
lua = LuaRuntime()
check = lua.eval('function(src, chunk) local fn, err = (load or \
loadstring)(src, chunk) if fn then return nil end return err end')
bad = [f'{name}: {err}' for name, script in SCRIPTS.items()
       if (err := check(script, name)) is not None]
sys.exit('\\n'.join(bad) if bad else 0)
"""
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, (
            f"invalid Lua scripts:\n{result.stdout}{result.stderr}"
        )


class TestLifecycle:
    def test_pool_is_lazy(self, make_broker: Callable[..., RedisBroker]) -> None:

        broker = make_broker(["default"])

        assert broker._client is None

    def test_client_is_cached(self, make_broker: Callable[..., RedisBroker]) -> None:

        broker = make_broker(["default"])
        first = broker._ensure_client()
        second = broker._ensure_client()

        assert first is second
        assert broker._client is not None

    async def test_start_is_idempotent(
        self, make_broker: Callable[..., RedisBroker]
    ) -> None:

        broker = make_broker(["default"])

        await broker.start()
        scripts = dict(broker._scripts)
        await broker.start()

        assert broker._running
        assert broker._scripts == scripts

    async def test_close_releases_everything(
        self, make_broker: Callable[..., RedisBroker]
    ) -> None:

        broker = make_broker(["default"])
        await broker.start()
        await broker.close()

        assert not broker._running
        assert broker._client is None
        assert broker._scripts == {}

    async def test_restarts_after_close(
        self, make_broker: Callable[..., RedisBroker]
    ) -> None:

        broker = make_broker(["default"])
        await broker.start()
        await broker.close()
        await broker.start()

        assert broker._running
        assert set(broker._scripts) == set(SCRIPTS)


class TestInitialize:
    async def test_creates_group_per_queue(
        self, make_broker: Callable[..., RedisBroker]
    ) -> None:

        broker = make_broker(["a", "b"])
        await broker.start()

        for queue in ("a", "b"):
            groups = await broker._ensure_client().xinfo_groups(f"binomic:{queue}:stream")
            assert [group["name"] for group in groups] == ["binomic"]

    async def test_is_idempotent_on_busy_group(
        self, make_broker: Callable[..., RedisBroker]
    ) -> None:

        broker = make_broker(["a"])

        await broker.initialize()
        await broker.initialize()

        assert True  # no BUSYGROUP leak means both calls survived


class TestEnqueue:
    async def test_returns_a_typed_entry(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message()
        entry = await broker.enqueue("default", msg)

        assert entry is not None
        assert "-" in entry.id  # a stream entry id, not the message uuid
        assert entry.queue == "default"
        assert entry.message == msg
        assert entry.enqueued_at == int(entry.enqueued_at)
        assert entry.enqueued_at > time.time() - 60

    async def test_parks_payload_in_hash_and_marks_the_stream(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(task="deploy")
        entry = await broker.enqueue("default", msg)
        assert entry is not None

        entries = await broker._ensure_client().xrange(STREAM_KEY, "-", "+")
        assert entries is not None
        ((entry_id, fields),) = cast("list[tuple[Any, Any]]", entries)

        # The stream entry is only a marker; the payload lives in the hash.
        assert fields == {"id": str(msg.id)}
        assert entry_id == entry.id

        raw = await broker._ensure_client().hget(HASH_KEY, entry_id)
        assert raw is not None
        body = json.loads(raw)

        assert body["id"] == entry_id
        assert body["queue"] == "default"
        assert body["message"] == msg.to_dict()
        assert body["enqueued_at"] == entry.enqueued_at

    async def test_failure_degrades_to_none(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        mocker: MockerFixture,
    ) -> None:

        mocker.patch.object(broker, "_invoke_script", return_value=None)

        assert await broker.enqueue("default", make_message()) is None


class TestAcquire:
    async def test_returns_empty_without_messages(self, broker: RedisBroker) -> None:

        assert await broker.acquire("consumer-1", count=10) == []

    async def test_tolerates_a_missing_group(
        self,
        make_broker: Callable[..., RedisBroker],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        broker = make_broker(["ghost"])
        await broker.start()
        await broker._ensure_client().xgroup_destroy("binomic:ghost:stream", "binomic")

        with caplog.at_level(logging.WARNING, logger="binomic.broker.redis.broker"):
            assert await broker.acquire("consumer-1", count=10) == []

        # Tolerance, not the error path: nothing degraded.
        assert not any(
            "Failed to invoke script" in record.getMessage() for record in caplog.records
        )

    async def test_failure_degrades_to_empty(
        self, broker: RedisBroker, mocker: MockerFixture
    ) -> None:

        mocker.patch.object(broker, "_invoke_script", return_value=None)

        assert await broker.acquire("consumer-1", count=10) == []


class TestAck:
    async def test_acks_pending_entry_and_cleans_up(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        deliver_entry: Deliver,
    ) -> None:

        await broker.enqueue("default", make_message())
        entry = await deliver_entry(broker, "default", "consumer-1")

        assert await broker.ack(entry) == 1
        assert (await broker._ensure_client().xpending(STREAM_KEY, "binomic"))[
            "pending"
        ] == 0
        assert await broker._ensure_client().hget(HASH_KEY, entry.id) is None
        # ack also trims the stream entry (XDEL): no graveyard accumulates.
        assert await stored_entry_ids(broker) == []

    async def test_unknown_entry_acks_nothing(
        self, broker: RedisBroker, make_message: Callable[..., Message]
    ) -> None:

        assert (
            await broker.ack(
                MessageEntry(
                    id="999-0",
                    message=make_message(),
                    enqueued_at=0,
                    queue="default",
                )
            )
            == 0
        )

    async def test_failure_degrades_to_zero(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        mocker: MockerFixture,
    ) -> None:

        mocker.patch.object(broker, "_invoke_script", return_value=None)

        entry = MessageEntry(
            id="1-1",
            message=make_message(),
            enqueued_at=0,
            queue="default",
        )

        assert await broker.ack(entry) == 0


class TestDead:
    async def test_moves_message_to_dlq_faithfully(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(attempt=2)
        entry = await broker.enqueue("default", msg)
        assert entry is not None

        assert await broker.dead(entry) == 1

        # The DLQ copy keeps the attempt as delivered: dead is a terminal
        # state, nothing bumps the counter any more.
        dlq_entries = await broker._ensure_client().xrange(DLQ_STREAM_KEY, "-", "+")
        assert dlq_entries is not None
        ((dlq_id, dlq_fields),) = cast("list[tuple[Any, Any]]", dlq_entries)
        assert dlq_fields == {"id": str(msg.id)}

        raw = await broker._ensure_client().hget(DLQ_HASH_KEY, dlq_id)
        assert raw is not None
        body = json.loads(raw)

        assert body["message"]["attempt"] == 2

        # The original is fully cleaned up: acked, deleted from stream and
        # hash alike.
        assert (await broker._ensure_client().xpending(STREAM_KEY, "binomic"))[
            "pending"
        ] == 0
        assert await stored_entry_ids(broker) == []
        assert await broker._ensure_client().hget(HASH_KEY, entry.id) is None

    async def test_unknown_entry_deads_nothing(
        self, broker: RedisBroker, make_message: Callable[..., Message]
    ) -> None:

        entry = MessageEntry(
            id="999-0",
            message=make_message(),
            enqueued_at=0,
            queue="default",
        )

        assert await broker.dead(entry) == 0


class TestRetry:
    async def test_redelivers_with_incremented_attempt_and_stale_stamp(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(attempt=1)
        entry = await broker.enqueue("default", msg)
        assert entry is not None

        assert await broker.retry(entry) == 1

        # One entry remains in the stream: the redelivered copy. The original
        # is acked and deleted.
        ids = await stored_entry_ids(broker)
        assert len(ids) == 1
        assert ids[0] != entry.id
        assert (await broker._ensure_client().xpending(STREAM_KEY, "binomic"))[
            "pending"
        ] == 0
        assert await broker._ensure_client().hget(HASH_KEY, entry.id) is None

        # task_timeout is a total delivery deadline: the retry keeps the
        # original enqueued_at so the age keeps accumulating across retries.
        raw = await broker._ensure_client().hget(HASH_KEY, ids[0])
        assert raw is not None
        body = json.loads(raw)

        assert body["id"] == ids[0]
        assert body["message"]["attempt"] == 2
        assert body["enqueued_at"] == entry.enqueued_at

    async def test_unknown_entry_retries_nothing(
        self, broker: RedisBroker, make_message: Callable[..., Message]
    ) -> None:

        entry = MessageEntry(
            id="999-0",
            message=make_message(),
            enqueued_at=0,
            queue="default",
        )

        assert await broker.retry(entry) == 0


class TestReclaim:
    async def test_reclaims_stale_pending_message(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        deliver_entry: Deliver,
    ) -> None:

        await broker.enqueue("default", make_message())
        stale = await deliver_entry(broker, "default", "dead-consumer")

        assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10, gc_idle_ms=3_600_000) == 1
        assert (await broker._ensure_client().xpending(STREAM_KEY, "binomic"))[
            "pending"
        ] == 0

        # The copy is a crash recovery: a fresh stream entry whose attempt
        # counts the redelivery and whose stamp is renewed, so the message
        # gets a full fresh timeout window. The original is XDELed — the
        # reclaim leaves no graveyard entries behind.
        ids = await stored_entry_ids(broker)
        assert len(ids) == 1
        assert stale.id not in ids

        new_entry_id = ids[0]
        assert await broker._ensure_client().hget(HASH_KEY, stale.id) is None

        raw = await broker._ensure_client().hget(HASH_KEY, new_entry_id)
        assert raw is not None
        body = json.loads(raw)

        assert body["message"]["attempt"] == 2
        assert body["enqueued_at"] > time.time() - 60

    async def test_drops_an_unreadable_payload(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        deliver_entry: Deliver,
    ) -> None:

        await broker.enqueue("default", make_message())
        stale = await deliver_entry(broker, "default", "dead-consumer")
        await broker._ensure_client().hset(HASH_KEY, stale.id, "{not-json")

        # Unreadable payloads can never execute: dropped with the entry acked,
        # so the reclaim pass is not stuck on them forever.
        assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10, gc_idle_ms=3_600_000) == 0
        assert (await broker._ensure_client().xpending(STREAM_KEY, "binomic"))[
            "pending"
        ] == 0
        assert await broker._ensure_client().hget(HASH_KEY, stale.id) is None

    async def test_skips_queues_without_group(
        self,
        make_broker: Callable[..., RedisBroker],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        broker = make_broker(["ghost"])
        await broker.start()
        await broker._ensure_client().xgroup_destroy("binomic:ghost:stream", "binomic")

        with caplog.at_level(logging.WARNING, logger="binomic.broker.redis.broker"):
            assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10, gc_idle_ms=3_600_000) == 0

        assert not any(
            "Failed to invoke script" in record.getMessage() for record in caplog.records
        )

    async def test_reclaims_nothing_when_idle_is_not_met(
        self,
        broker: RedisBroker,
        make_message: Callable[..., Message],
        deliver_entry: Deliver,
    ) -> None:

        await broker.enqueue("default", make_message())
        await deliver_entry(broker, "default", "dead-consumer")

        assert (
            await broker.reclaim(
                "consumer-1", min_idle_ms=3_600_000, count=10, gc_idle_ms=3_600_000
            )
            == 0
        )

    async def test_failure_degrades_to_zero(
        self, broker: RedisBroker, mocker: MockerFixture
    ) -> None:

        mocker.patch.object(broker, "_invoke_script", return_value=None)

        assert await broker.reclaim("consumer-1", min_idle_ms=0, count=10, gc_idle_ms=3_600_000) == 0


class TestInvokeScript:
    async def test_unknown_script_raises(self, broker: RedisBroker) -> None:

        with pytest.raises(KeyError, match="Script 'ghost' not found"):
            await broker._invoke_script("ghost", keys=[], args=[])

    async def test_ok_envelope_unwraps_the_result(
        self, broker: RedisBroker, mocker: MockerFixture
    ) -> None:

        class FakeScript:
            async def __call__(self, client: Any, keys: Any, args: Any) -> str:

                return ScriptResponse(ok=True, result=7).to_json()

        mocker.patch.object(broker, "_scripts", {"enqueue": FakeScript()})

        assert await broker._invoke_script("enqueue", keys=[], args=[]) == 7

    async def test_error_envelope_degrades_and_logs(
        self,
        broker: RedisBroker,
        mocker: MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        class FakeScript:
            async def __call__(self, client: Any, keys: Any, args: Any) -> str:

                return ScriptResponse(ok=False, error="boom").to_json()

        mocker.patch.object(broker, "_scripts", {"enqueue": FakeScript()})

        with caplog.at_level(logging.WARNING, logger="binomic.broker.redis.broker"):
            assert await broker._invoke_script("enqueue", keys=[], args=[]) is None

        assert any(
            "Failed to invoke script" in record.getMessage() for record in caplog.records
        )
