from typing import Any, Literal

from binomic.base import BaseStruct

__all__ = ("ScriptResponse",)


class ScriptResponse(BaseStruct):
    """Envelope a Lua script returns.

    ``status`` classifies the reply in three states: ``ok`` carries the
    operation result in ``result``; ``rejected`` is an *expected* capacity
    refusal and carries a ``{"current", "capacity"}`` detail in ``result``;
    ``error`` is an internal failure described by ``error``, which the
    broker logs and degrades on instead of killing its caller's loop.
    """

    status: Literal["ok", "rejected", "error"]
    result: Any | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:

        return self.status == "ok"
