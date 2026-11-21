from typing import Any

import pytest

from binomic.task import TaskSpec, task


class TestTaskDecorator:
    def test_produces_a_spec_without_global_registration(self) -> None:
        """The decorator returns a TaskSpec; registration happens wherever the
        caller keeps it (client/handler each own a TaskRegistry)."""

        @task("default", mode="direct")
        def do_work() -> str:
            return "ok"

        assert isinstance(do_work, TaskSpec)
        assert do_work.name == "do_work"  # no lower-casing any more
        assert do_work.queue == "default"
        assert do_work.mode == "direct"
        assert do_work.func() == "ok"

    def test_preserves_casing(self) -> None:

        @task("default", mode="direct")
        def DoWork() -> str:
            return "ok"

        assert DoWork.name == "DoWork"

    async def test_decorated_object_is_callable(self) -> None:

        @task("default", mode="direct")
        def alpha() -> str:
            return "ok"

        assert await alpha() == "ok"

    def test_unsupported_mode_is_rejected(self) -> None:
        """Only the three supported modes are accepted; an untyped caller
        still fails loudly at declaration time."""

        declare: Any = task

        with pytest.raises(ValueError, match="Unsupported task mode"):
            declare("default", mode="delay")

    def test_cron_mode_requires_cron(self) -> None:

        declare: Any = task

        with pytest.raises(ValueError, match="must be a valid cron expression"):
            declare("default", mode="cron")

    def test_cron_mode_accepts_cron(self) -> None:

        @task("default", mode="cron", cron="* * * * *")
        def scheduled() -> None: ...

        assert scheduled.mode == "cron"
        assert scheduled.cron == "* * * * *"

    def test_interval_mode_requires_interval(self) -> None:

        declare: Any = task

        with pytest.raises(ValueError, match="must be a positive number"):
            declare("default", mode="interval")

    def test_interval_mode_accepts_interval(self) -> None:

        @task("default", mode="interval", interval=10.0)
        def heartbeat() -> None: ...

        assert heartbeat.mode == "interval"
        assert heartbeat.interval == 10.0

    def test_parameterized_direct_task_is_accepted(self) -> None:
        """Only cron/interval forbid parameters: parameterized direct tasks
        are the framework's core use case (args ride the Message)."""

        @task("default", mode="direct")
        def work(value: int, label: str = "x") -> int:
            return value

        assert work.func(3) == 3

    def test_cron_mode_rejects_parameterized_fn(self) -> None:

        with pytest.raises(ValueError, match="must not have any parameters"):

            @task("default", mode="cron", cron="* * * * *")
            def scheduled_twice(value: int) -> None: ...

    def test_interval_mode_rejects_parameterized_fn(self) -> None:

        with pytest.raises(ValueError, match="must not have any parameters"):

            @task("default", mode="interval", interval=10.0)
            def heartbeat_twice(value: int) -> None: ...

    def test_on_dead_defaults_to_none(self) -> None:

        @task("default", mode="direct")
        def plain() -> None: ...

        assert plain.on_dead_hook is None

    def test_accepts_on_dead_hook(self) -> None:

        def note(entry: object, reason: object, exc: object) -> None: ...

        @task("default", mode="direct", on_dead=note)
        def tracked() -> None: ...

        assert tracked.on_dead_hook is note

    def test_rejects_non_callable_on_dead(self) -> None:
        """The callable check lives at runtime: declarations built through
        an untyped alias must fail loudly, not just at the type checker."""

        declare: Any = task

        with pytest.raises(ValueError, match="must be callable"):
            declare("default", mode="direct", on_dead=123)
