import inspect
from collections.abc import Awaitable, Callable
from typing import Literal, cast

from binomic.base import BaseStruct
from binomic.broker.message import MessageEntry

__all__ = (
    "OnDeadHook",
    "OnDeadReason",
    "TaskSpec",
)


OnDeadReason = str
"""Why a message entered the DLQ: ``expired``, ``timeout`` or ``max_attempts``."""

OnDeadHook = Callable[
    [MessageEntry, OnDeadReason, BaseException | None],
    Awaitable[None] | None,
]
"""Notification called after a message is written to the DLQ.

Receives the dead entry, the reason, and the execution exception when there
was one. Sync and async callables are both accepted; exceptions raised by the
hook are logged and ignored.
"""


class TaskSpec[**P, T](BaseStruct):
    """A declared task: its name, queue, callable and scheduling mode.

    ``direct`` tasks run on submission; ``cron`` and ``interval`` tasks run
    periodically and take no arguments. The queue belongs to the declaration,
    not to the message.
    """

    name: str
    queue: str
    func: Callable[P, T]

    mode: Literal["direct", "cron", "interval"]
    cron: str | None = None
    interval: float | None = None

    on_dead_hook: OnDeadHook | None = None

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Invoke the wrapped callable, awaiting it when it is async."""

        res: object = self.func(*args, **kwargs)
        if inspect.isawaitable(res):
            res = await res

        return cast("T", res)
