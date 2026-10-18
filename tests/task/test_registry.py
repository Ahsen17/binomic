from typing import cast

import pytest

from binomic.task import DuplicateTaskError, TaskNotFoundError, TaskRegistry, TaskSpec


def noop() -> None:
    return None


class TestTaskRegistry:
    def test_register_and_get(self, registry: TaskRegistry) -> None:

        spec = TaskSpec(name="alpha", queue="default", func=noop, mode="direct")
        registry.register(spec)

        assert registry.get("alpha") is spec

    def test_register_duplicate_raises(self, registry: TaskRegistry) -> None:

        first = TaskSpec(name="alpha", queue="default", func=noop, mode="direct")
        registry.register(first)

        with pytest.raises(DuplicateTaskError, match="already registered"):
            registry.register(
                TaskSpec(name="alpha", queue="default", func=noop, mode="direct")
            )

    def test_get_missing_raises(self, registry: TaskRegistry) -> None:
        """Regression pin: the hit branch must return the spec — a missing
        return here silently handed every caller `None`."""

        with pytest.raises(TaskNotFoundError, match="not found"):
            registry.get("ghost")

    def test_iterates_over_specs(self, registry: TaskRegistry) -> None:

        specs = [
            TaskSpec(name="alpha", queue="default", func=noop, mode="direct"),
            TaskSpec(name="beta", queue="default", func=noop, mode="direct"),
        ]
        for spec in specs:
            registry.register(spec)

        assert list(registry) == specs

    def test_contains_by_name(self, registry: TaskRegistry) -> None:

        registry.register(
            TaskSpec(name="alpha", queue="default", func=noop, mode="direct")
        )

        assert "alpha" in registry
        assert "ghost" not in registry

    def test_len_counts_specs(self, registry: TaskRegistry) -> None:

        assert len(registry) == 0
        registry.register(
            TaskSpec(name="alpha", queue="default", func=noop, mode="direct")
        )

        assert len(registry) == 1


class TestTaskSpec:
    async def test_call_invokes_sync_fn(self) -> None:

        spec = TaskSpec(
            name="alpha",
            queue="default",
            func=lambda a, b: a + b,
            mode="direct",
        )

        assert await spec(1, b=2) == 3

    async def test_call_awaits_async_fn(self) -> None:

        async def fetch(value: str) -> str:
            return value.upper()

        # Runtime: __call__ awaits async funcs. Typing: with an async func,
        # Callable[P, T] binds T to the coroutine type (a known TaskSpec
        # weakness), so the awaited result is cast to what it really is.
        spec = TaskSpec(name="fetch", queue="default", func=fetch, mode="direct")
        result = cast("str", await spec("ok"))
        assert result == "OK"

    def test_schedule_fields_default_to_none(self) -> None:

        spec = TaskSpec(name="alpha", queue="default", func=noop, mode="direct")

        assert spec.mode == "direct"
        assert spec.delay is None
        assert spec.cron is None
        assert spec.interval is None

    def test_mode_is_required(self) -> None:

        with pytest.raises(TypeError):
            TaskSpec(name="alpha", queue="default", func=noop)  # type: ignore[call-arg]
