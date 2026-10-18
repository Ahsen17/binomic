import contextlib
import importlib
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Literal, overload

from .register import TaskSpec


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
    mode: Literal[
        "direct",
        "delay",
        "cron",
        "interval",
    ] = "direct",
    delay: float | None = None,
    cron: str | None = None,
    interval: float | None = None,
) -> Callable[[Callable[P, T]], TaskSpec[P, T]]:

    match mode:
        case "delay":
            if delay is None or delay <= 0:
                raise ValueError(
                    "`delay` must be a positive number",
                )

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
            delay=delay,
            cron=cron,
            interval=interval,
        )

    return decorator


def autodiscover(package: str) -> list[TaskSpec]:
    """Import every ``tasks`` module under ``package`` and collect its specs.

    A tasks module is a ``tasks.py`` or ``tasks/__init__.py`` at any depth
    below the package. A missing package raises; a module whose import fails
    on a missing import is skipped, and any other error propagates.
    """

    try:
        pkg = importlib.import_module(package)
    except ModuleNotFoundError as err:
        if err.name == package:
            raise ValueError(f"Package `{package}` not found") from err
        raise

    pkg_paths = getattr(pkg, "__path__", None)
    if pkg_paths is None:
        return []

    seen: set[int] = set()
    specs: list[TaskSpec] = []

    for raw_path in pkg_paths:
        pkg_root = Path(raw_path).resolve()
        if not pkg_root.exists():
            continue

        files: list[Path] = []
        files.extend(pkg_root.rglob("tasks.py"))
        files.extend(pkg_root.rglob("tasks/__init__.py"))

        for path in files:
            rel_path = path.relative_to(pkg_root)
            parts = list(rel_path.parts)

            if parts[-1] == "__init__.py":
                module_parts = [package, *parts[:-1]]
            else:
                module_parts = [package, *parts[:-1], path.stem]

            module_name = ".".join(module_parts)

            with contextlib.suppress(ImportError):
                module = importlib.import_module(module_name)

                for obj in vars(module).values():
                    if isinstance(obj, TaskSpec) and id(obj) not in seen:
                        seen.add(id(obj))
                        specs.append(obj)

    return specs
