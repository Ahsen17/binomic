"""Fixtures scoped to task-module tests."""

from collections.abc import Callable
from typing import Any

import pytest

from binomic.task import TaskRegistry, TaskSpec


@pytest.fixture
def registry() -> TaskRegistry:
    """A fresh per-test registry — the new design has no module-level one."""

    return TaskRegistry()


@pytest.fixture
def register_spec(
    registry: TaskRegistry,
) -> Callable[..., TaskSpec]:
    """Register ad-hoc task specs into the per-test registry."""

    def _register(
        name: str,
        fn: Callable[..., Any],
        queue: str = "default",
        **spec_kwargs: Any,
    ) -> TaskSpec:

        spec = TaskSpec(name=name, queue=queue, func=fn, **spec_kwargs)
        registry.register(spec)
        return spec

    return _register
