import importlib
import logging
import pkgutil
from collections.abc import Callable
from typing import Literal

from .registry import TaskSpec, registry

logger = logging.getLogger(__name__)

__all__ = (
    "autodiscover",
    "task",
)


def task[**P, T](
    *,
    mode: Literal["direct", "delay", "cron"] = "direct",
    delay: float | None = None,
    cron: str | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]:
    """Declare a task and register it under the function's lowercase name.

    Mode ``direct`` executes on submit, ``delay`` schedules execution
    ``delay`` seconds after submit, and ``cron`` repeats execution on the
    given cron expression.

    Raises:
        ValueError: if ``delay`` or ``cron`` mode lacks its required argument.
    """

    if mode == "delay" and delay is None:
        raise ValueError("delay must be specified for delay mode")
    if mode == "cron" and cron is None:
        raise ValueError("cron must be specified for cron mode")

    def decorator(fn: Callable[P, T]) -> TaskSpec[P, T]:

        registry.register(
            spec := TaskSpec(
                name=fn.__name__.lower(),
                fn=fn,
                mode=mode,
                delay=delay,
                cron=cron,
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
