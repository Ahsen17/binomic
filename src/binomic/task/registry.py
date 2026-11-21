from collections.abc import Iterator
from typing import TYPE_CHECKING

from .exceptions import DuplicateTaskError, TaskNotFoundError

if TYPE_CHECKING:
    from .types import TaskSpec


__all__ = ("TaskRegistry",)


class TaskRegistry:
    """An in-process name-to-spec map.

    Registries are per-owner, not global: the client and every worker handler
    build their own from ``autodiscover`` and never share one across
    processes.
    """

    def __init__(self) -> None:

        self._specs: dict[str, TaskSpec] = {}

    def register(self, spec: "TaskSpec") -> None:
        """Add ``spec`` under its name.

        Raises ``DuplicateTaskError`` when the name is already taken.
        """

        if spec.name in self._specs:
            raise DuplicateTaskError(
                f"Task [{spec.name}] already registered",
            )

        self._specs[spec.name] = spec

    def get(self, name: str) -> "TaskSpec":
        """Return the spec registered under ``name``.

        Raises ``TaskNotFoundError`` when the name is unknown.
        """

        if name not in self._specs:
            raise TaskNotFoundError(
                f"Task [{name}] not found",
            )

        return self._specs[name]

    def __iter__(self) -> Iterator["TaskSpec"]:
        """Iterate over the registered specs in insertion order."""

        return iter(self._specs.values())

    def __contains__(self, name: str) -> bool:
        """Return whether a spec is registered under ``name``."""

        return name in self._specs

    def __len__(self) -> int:
        """Return how many specs are registered."""

        return len(self._specs)
