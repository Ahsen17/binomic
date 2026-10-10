import time

from fakeredis import FakeAsyncRedis
from pytest_mock import MockerFixture

from binomic.worker.presence import (
    ALIVE_PRESENCE_KEY,
    ParentPresence,
    SubprocessPresence,
)


class TestParentPresence:
    async def test_initialize_resets_presence_map(
        self, fake_redis: FakeAsyncRedis
    ) -> None:

        presence = ParentPresence(fake_redis)

        await presence.initialize()

        assert await fake_redis.get(ALIVE_PRESENCE_KEY) == "{}"

    async def test_presence_is_empty_before_heartbeats(
        self,
        fake_redis: FakeAsyncRedis,
    ) -> None:

        presence = ParentPresence(fake_redis)
        await presence.initialize()

        assert await presence.presence() == {}

    async def test_presence_reads_recorded_idents(
        self,
        fake_redis: FakeAsyncRedis,
    ) -> None:

        presence = ParentPresence(fake_redis)
        await presence.initialize()
        await fake_redis.set(ALIVE_PRESENCE_KEY, '{"worker-0": 123.5}')

        assert await presence.presence() == {"worker-0": 123.5}

    async def test_presence_tolerates_missing_key(
        self,
        fake_redis: FakeAsyncRedis,
    ) -> None:

        presence = ParentPresence(fake_redis)

        assert await presence.presence() == {}

    async def test_aclose_closes_redis_client(self, mocker: MockerFixture) -> None:

        client = mocker.AsyncMock()
        presence = ParentPresence(client)

        await presence.aclose()

        client.aclose.assert_awaited_once()


class TestSubprocessPresence:
    async def test_heartbeat_invokes_presence_script(self, mocker: MockerFixture) -> None:

        script = mocker.AsyncMock()
        client = mocker.Mock()
        client.register_script.return_value = script
        presence = SubprocessPresence(client)

        before = time.time()
        await presence.heartbeat("worker-0")

        script.assert_awaited_once()
        call = script.await_args
        assert call.kwargs["keys"] == [ALIVE_PRESENCE_KEY]
        ident, stamp = call.kwargs["args"]
        assert ident == "worker-0"
        assert float(stamp) >= before

    async def test_heartbeat_uses_shared_presence_script(
        self, mocker: MockerFixture
    ) -> None:

        client = mocker.Mock()
        presence = SubprocessPresence(client)

        client.register_script.assert_called_once()
        assert presence._hb_script is client.register_script.return_value

    async def test_aclose_closes_redis_client(self, mocker: MockerFixture) -> None:

        client = mocker.Mock()
        client.register_script.return_value = mocker.AsyncMock()
        client.aclose = mocker.AsyncMock()
        presence = SubprocessPresence(client)

        await presence.aclose()

        client.aclose.assert_awaited_once()
