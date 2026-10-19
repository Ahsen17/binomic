import inspect
from collections.abc import Awaitable, Callable, Iterator
from typing import TYPE_CHECKING, Literal

from binomic.base import BaseStruct

from .exceptions import DuplicateTaskError, TaskNotFoundError

if TYPE_CHECKING:
    from binomic.broker import MessageEntry


__all__ = (
    "OnDeadHook",
    "OnDeadReason",
    "TaskRegistry",
    "TaskSpec",
)


type OnDeadReason = Literal["timeout", "expired", "max_attempts"]
type OnDeadHook = Callable[
    ["MessageEntry", OnDeadReason, BaseException | None],
    Awaitable[None] | None,
]


class TaskSpec[**P, T](BaseStruct):
    """A declared task: its function plus everything needed to deliver to it.

    Calling a spec awaits the function (sync or async) with the given
    arguments. ``mode`` selects the delivery path and which schedule field
    (``delay``/``cron``/``interval``) must be set. ``on_dead`` is an optional
    notification hook the worker fires as ``fn(entry, reason, exc)`` after the
    message is dead-lettered; it may be sync or async, its return value is
    ignored, and its errors are logged and swallowed.
    """

    name: str
    queue: str
    func: Callable[P, T]

    mode: Literal["direct", "delay", "cron", "interval"]
    delay: float | None = None
    cron: str | None = None
    interval: float | None = None

    on_dead: OnDeadHook | None = None

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:

        res = self.func(*args, **kwargs)
        if inspect.isawaitable(res):
            res = await res

        return res


class TaskRegistry:
    """An in-process name-to-spec registry.

    Each client and each worker handler owns one; a name resolves to at most
    one spec, duplicate registrations raise, and misses raise
    TaskNotFoundError.
    """

    def __init__(self) -> None:

        self._specs: dict[str, TaskSpec] = {}

    def register(self, spec: "TaskSpec") -> None:

        if spec.name in self._specs:
            raise DuplicateTaskError(
                f"Task [{spec.name}] already registered",
            )

        self._specs[spec.name] = spec

    def get(self, name: str) -> "TaskSpec":

        if name not in self._specs:
            raise TaskNotFoundError(
                f"Task [{name}] not found",
            )

        return self._specs[name]

    def __iter__(self) -> Iterator["TaskSpec"]:

        return iter(self._specs.values())

    def __contains__(self, name: str) -> bool:

        return name in self._specs

    def __len__(self) -> int:

        return len(self._specs)
