"""Fixtures scoped to task-module tests."""

from collections.abc import Callable
from typing import Any

import pytest

from binomic.task.registry import TaskRegistry, TaskSpec


@pytest.fixture
def register_spec(
    isolate_registry: TaskRegistry,
) -> Callable[..., TaskSpec]:
    """Register ad-hoc task specs with automatic teardown via isolate_registry."""

    def _register(
        name: str,
        fn: Callable[..., Any],
        queue: str = "default",
        **spec_kwargs: Any,
    ) -> TaskSpec:

        spec = TaskSpec(name=name, queue=queue, fn=fn, **spec_kwargs)
        isolate_registry.register(spec)
        return spec

    return _register
