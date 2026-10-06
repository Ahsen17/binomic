import time
from typing import TYPE_CHECKING, Final, cast

from msgspec import json

from binomic.base.constants import APP_NAME

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis


__all__ = (
    "ParentPresence",
    "SubprocessPresence",
)


ALIVE_PRESENCE_KEY: Final[str] = f"{APP_NAME}:alive:presence"


_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
local p = {}
if raw then
    p = cjson.decode(raw)
end
p[ARGV[1]] = tonumber(ARGV[2])
redis.call('SET', KEYS[1], cjson.encode(p))
return 1
"""


class ParentPresence:
    """Parent presence detection."""

    def __init__(self, client: "AsyncRedis") -> None:

        self._client = client

    async def initialize(self) -> None:

        await self._client.set(ALIVE_PRESENCE_KEY, "{}")

    async def presence(self) -> dict[str, float]:

        return cast(
            "dict[str, float]",
            json.decode(await self._client.get(ALIVE_PRESENCE_KEY) or "{}"),
        )


class SubprocessPresence:
    """Subprocess presence detection."""

    def __init__(self, client: "AsyncRedis") -> None:

        self._client = client
        self._hb_script = client.register_script(_SCRIPT)

    async def heartbeat(self, ident: str) -> None:

        await self._hb_script(
            keys=[ALIVE_PRESENCE_KEY],
            args=[ident, str(time.time())],
        )
