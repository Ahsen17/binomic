import importlib
import inspect
import logging
import pkgutil
from collections.abc import Callable
from typing import Literal, overload

from .registry import TaskSpec, registry

logger = logging.getLogger(__name__)

__all__ = (
    "autodiscover",
    "task",
)


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["direct"],
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["delay"],
    delay: float,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["cron"],
    cron: str,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


@overload
def task[**P, T](
    queue: str,
    *,
    mode: Literal["interval"],
    interval: float,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]: ...


def task[**P, T](
    queue: str,
    *,
    mode: Literal["direct", "delay", "cron", "interval"] = "direct",
    delay: float | None = None,
    cron: str | None = None,
    interval: float | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]:
    """Declare a task and register it under the function's lowercase name.

    Mode ``direct`` executes on submit, ``delay`` schedules execution
    ``delay`` seconds after submit, ``cron`` repeats execution on the given
    cron expression, and ``interval`` repeats it every ``interval`` seconds.

    Raises:
        ValueError: if ``delay``, ``cron`` or ``interval`` mode lacks its
            required argument.
    """

    if mode == "delay" and delay is None:
        raise ValueError("delay must be specified for delay mode")
    if mode == "cron" and cron is None:
        raise ValueError("cron must be specified for cron mode")
    if mode == "interval" and interval is None:
        raise ValueError("interval must be specified for interval mode")

    def decorator(fn: Callable[P, T]) -> TaskSpec[P, T]:

        if mode == "cron" or mode == "interval":
            sig = inspect.signature(fn)
            if len(sig.parameters.values()) > 0:
                raise ValueError("Cron/Interval mode requires no arguments.")

        registry.register(
            spec := TaskSpec(
                name=fn.__name__.lower(),
                queue=queue,
                fn=fn,
                mode=mode,
                delay=delay,
                cron=cron,
                interval=interval,
            )
        )
        return spec

    return decorator


def autodiscover(
    package: str,
    *,
    on_error: Literal["warn", "raise"] = "warn",
) -> list[str]:
    """Import every ``tasks`` module inside package to trigger registration.

    Returns the names of the imported modules; a failing module import is
    warned or raised depending on ``on_error``.
    """

    try:
        pkg = importlib.import_module(package)
    except ModuleNotFoundError:
        logger.warning("Package %s not found, skipping task discovery", package)
        return []

    if not hasattr(pkg, "__path__"):
        logger.warning("%s is not a package, cannot recurse", package)
        return []

    imported: list[str] = []

    for module_info in pkgutil.walk_packages(pkg.__path__, prefix=f"{package}."):
        if module_info.name.rsplit(".", 1)[-1] != "tasks":
            continue

        try:
            importlib.import_module(module_info.name)
            imported.append(module_info.name)
        except Exception:
            if on_error == "raise":
                raise
            if on_error == "warn":
                logger.warning(
                    "Failed to import task module %s",
                    module_info.name,
                    exc_info=True,
                )

    return imported
