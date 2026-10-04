import inspect
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Literal

from binomic.base import BaseStruct

from .exceptions import DuplicateTaskError, TaskNotFoundError

if TYPE_CHECKING:
    from .wrappers import TaskSpec


__all__ = (
    "TaskRegistry",
    "TaskSpec",
)


class TaskSpec[**P, T](BaseStruct):
    name: str
    fn: Callable[P, T]

    mode: Literal["direct", "delay", "cron"] = "direct"
    delay: float | None = None
    cron: str | None = None

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:

        res = self.fn(*args, **kwargs)
        if inspect.isawaitable(res):
            res = await res

        return res


class TaskRegistry:
    def __init__(self) -> None:

        self._tasks: dict[str, TaskSpec] = {}

    def register(self, spec: "TaskSpec") -> None:

        if spec.name in self._tasks:
            raise DuplicateTaskError(
                f"Task with name '{spec.name}' already exists.",
            )

        self._tasks[spec.name] = spec

    def get(self, name: str) -> "TaskSpec":

        if name not in self._tasks:
            raise TaskNotFoundError(
                f"Task with name '{name}' not found.",
            )

        return self._tasks[name]

    def __iter__(self) -> Iterator["TaskSpec"]:

        return iter(self._tasks.values())

    def __contains__(self, name: str) -> bool:

        return name in self._tasks

    def __len__(self) -> int:

        return len(self._tasks)


registry = TaskRegistry()
