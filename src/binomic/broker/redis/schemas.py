from typing import Any

from binomic.base import BaseStruct

__all__ = ("ScriptResponse",)


class ScriptResponse(BaseStruct):
    """Envelope a Lua script returns.

    Scripts catch their own failures and answer with ``ok=False`` and a
    human-readable ``error`` instead of raising, so the broker logs the cause
    and degrades to an empty result rather than killing its caller's loop.
    """

    ok: bool
    result: Any | None = None
    error: str | None = None
