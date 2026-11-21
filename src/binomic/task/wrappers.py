import inspect
from collections.abc import Callable
from typing import Literal, cast, overload

from .types import OnDeadHook, TaskSpec

__all__ = ("task",)


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["direct"] = "direct",
    on_dead: OnDeadHook | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["cron"],
    cron: str,
    on_dead: OnDeadHook | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["interval"],
    interval: float,
    on_dead: OnDeadHook | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


def task[**P, T](
    queue: str,
    *,
    mode: Literal["direct", "cron", "interval"] = "direct",
    cron: str | None = None,
    interval: float | None = None,
    on_dead: OnDeadHook | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]:
    """Declare a task in ``queue`` and return its ``TaskSpec``.

    ``direct`` tasks are submitted by name and execute as soon as a worker
    picks the message. ``cron`` and ``interval`` tasks are periodic: the
    client registers them when its async context starts, so they take no
    arguments and reject ``submit``. ``on_dead`` is an optional notification
    hook, called once after the message enters the DLQ.
    """

    if on_dead is not None and not callable(cast("object", on_dead)):
        raise ValueError(
            "`on_dead` must be callable",
        )

    match str(mode):
        case "direct":
            pass

        case "cron":
            if cron is None:
                raise ValueError(
                    "`cron` must be a valid cron expression",
                )

        case "interval":
            if interval is None or interval <= 0:
                raise ValueError(
                    "`interval` must be a positive number",
                )

        case _:
            raise ValueError(
                f"Unsupported task mode `{mode}`; expected direct, cron or interval",
            )

    def decorator(fn: Callable[P, T]) -> TaskSpec[P, T]:
        if mode in ("cron", "interval"):
            sig = inspect.signature(fn)
            if len(sig.parameters.values()) > 0:
                raise ValueError(
                    "Cron/Interval tasks must not have any parameters",
                )

        return TaskSpec(
            name=fn.__name__,
            queue=queue,
            func=fn,
            mode=mode,
            cron=cron,
            interval=interval,
            on_dead_hook=on_dead,
        )

    return decorator
