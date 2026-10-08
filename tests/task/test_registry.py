from typing import cast

import pytest

from binomic.task.exceptions import DuplicateTaskError, TaskNotFoundError
from binomic.task.registry import TaskRegistry, TaskSpec, registry


def noop() -> None:
    return None


class TestTaskRegistry:
    def test_register_and_get(self, isolate_registry: TaskRegistry) -> None:

        spec = TaskSpec(name="alpha", queue="default", fn=noop)
        isolate_registry.register(spec)

        assert isolate_registry.get("alpha") is spec

    def test_register_duplicate_raises(self, isolate_registry: TaskRegistry) -> None:

        isolate_registry.register(TaskSpec(name="alpha", queue="default", fn=noop))

        with pytest.raises(DuplicateTaskError, match="'alpha' already exists"):
            isolate_registry.register(TaskSpec(name="alpha", queue="default", fn=noop))

    def test_get_missing_raises(self, isolate_registry: TaskRegistry) -> None:

        with pytest.raises(TaskNotFoundError, match="'ghost' not found"):
            isolate_registry.get("ghost")

    def test_iterates_over_specs(self, isolate_registry: TaskRegistry) -> None:

        specs = [
            TaskSpec(name="alpha", queue="default", fn=noop),
            TaskSpec(name="beta", queue="default", fn=noop),
        ]
        for spec in specs:
            isolate_registry.register(spec)

        assert list(isolate_registry) == specs

    def test_contains_by_name(self, isolate_registry: TaskRegistry) -> None:

        isolate_registry.register(TaskSpec(name="alpha", queue="default", fn=noop))

        assert "alpha" in isolate_registry
        assert "ghost" not in isolate_registry

    def test_len_counts_specs(self, isolate_registry: TaskRegistry) -> None:

        assert len(isolate_registry) == 0
        isolate_registry.register(TaskSpec(name="alpha", queue="default", fn=noop))

        assert len(isolate_registry) == 1


class TestTaskSpec:
    async def test_call_invokes_sync_fn(self) -> None:

        spec = TaskSpec(name="alpha", queue="default", fn=lambda a, b: a + b)

        assert await spec(1, b=2) == 3

    async def test_call_awaits_async_fn(self) -> None:

        async def fetch(value: str) -> str:
            return value.upper()

        spec = TaskSpec(name="fetch", queue="default", fn=fetch)

        result = cast("str", await spec("ok"))

        assert result == "OK"

    def test_mode_defaults_to_direct(self) -> None:

        spec = TaskSpec(name="alpha", queue="default", fn=noop)

        assert spec.mode == "direct"
        assert spec.delay is None
        assert spec.cron is None
        assert spec.interval is None


class TestModuleRegistry:
    def test_singleton_registry_is_available(self) -> None:

        assert isinstance(registry, TaskRegistry)
