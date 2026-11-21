import contextlib
import importlib
from pathlib import Path

from .types import TaskSpec

__all__ = ("autodiscover",)


def autodiscover(package: str) -> list["TaskSpec"]:
    """Import every ``tasks`` module under ``package`` and collect its specs.

    A tasks module is a ``tasks.py`` or ``tasks/__init__.py`` at any depth
    below the package. A missing package raises ``ValueError``; an
    ``ImportError`` raised while importing a tasks module skips that module,
    and any other error propagates. Specs are returned, not registered.
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
