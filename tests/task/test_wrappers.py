from pathlib import Path
from typing import Any

import pytest

from binomic.task import TaskRegistry, TaskSpec
from binomic.task.wrappers import autodiscover, task


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

    def test_delay_mode_requires_delay(self) -> None:

        declare: Any = task

        with pytest.raises(ValueError, match="must be a positive number"):
            declare("default", mode="delay")

    def test_delay_mode_accepts_delay(self) -> None:

        @task("default", mode="delay", delay=5.0)
        def later() -> None: ...

        assert later.mode == "delay"
        assert later.delay == 5.0

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
        """Only cron/interval forbid parameters: parameterized direct/delay
        tasks are the framework's core use case (args ride the Message)."""

        @task("default", mode="direct")
        def work(value: int, label: str = "x") -> int:
            return value

        assert work.func(3) == 3

    def test_parameterized_delay_task_is_accepted(self) -> None:

        @task("default", mode="delay", delay=1.0)
        def later(value: int) -> None: ...

        assert later.func(1) is None

    def test_cron_mode_rejects_parameterized_fn(self) -> None:

        with pytest.raises(ValueError, match="must not have any parameters"):

            @task("default", mode="cron", cron="* * * * *")
            def scheduled_twice(value: int) -> None: ...

    def test_interval_mode_rejects_parameterized_fn(self) -> None:

        with pytest.raises(ValueError, match="must not have any parameters"):

            @task("default", mode="interval", interval=10.0)
            def heartbeat_twice(value: int) -> None: ...


def make_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    tasks_body: str,
) -> None:

    pkg = tmp_path / name
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "tasks.py").write_text(tasks_body, encoding="utf-8")
    monkeypatch.syspath_prepend(tmp_path)


class TestAutodiscover:
    def test_returns_specs_from_tasks_modules(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        make_package(
            tmp_path,
            monkeypatch,
            "fakeapp",
            "from binomic.task import task\n\n\n"
            '@task("default", mode="direct")\n'
            "def alpha() -> None: ...\n"
            '@task("default", mode="direct")\n'
            "def beta() -> None: ...\n",
        )

        specs = autodiscover("fakeapp")

        assert sorted(spec.name for spec in specs) == ["alpha", "beta"]
        assert all(isinstance(spec, TaskSpec) for spec in specs)

    def test_finds_nested_tasks_modules(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        pkg = tmp_path / "nestedapp"
        (pkg / "sub").mkdir(parents=True)
        (pkg / "__init__.py").touch()
        (pkg / "sub" / "__init__.py").touch()
        (pkg / "sub" / "tasks.py").write_text(
            "from binomic.task import task\n\n\n"
            '@task("default", mode="direct")\n'
            "def deep() -> None: ...\n",
            encoding="utf-8",
        )
        monkeypatch.syspath_prepend(tmp_path)

        specs = autodiscover("nestedapp")

        assert [spec.name for spec in specs] == ["deep"]

    def test_missing_package_raises(self) -> None:

        with pytest.raises(ValueError, match="not found"):
            autodiscover("no_such_pkg_anywhere")

    def test_plain_module_returns_empty(self) -> None:
        """A single module has no ``__path__`` to recurse — silently empty."""

        assert autodiscover("binomic.client") == []

    def test_skips_modules_with_other_names(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        pkg = tmp_path / "quietapp"
        pkg.mkdir()
        (pkg / "__init__.py").touch()
        (pkg / "helpers.py").write_text("VALUE = 1\n", encoding="utf-8")
        monkeypatch.syspath_prepend(tmp_path)

        assert autodiscover("quietapp") == []

    def test_import_error_in_a_tasks_module_is_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One bad module must not take down discovery (fail-soft, matching
        the historical ``on_error="warn"`` spirit)."""

        make_package(
            tmp_path,
            monkeypatch,
            "halfbroken",
            "from binomic.task import task\n"
            "import no_such_dependency_xyz\n\n\n"
            '@task("default", mode="direct")\n'
            "def good() -> None: ...\n",
        )

        assert autodiscover("halfbroken") == []

    def test_non_import_error_still_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Genuinely broken code (syntax errors, raise at import) fails fast —
        only import-resolution failures are skipped."""

        make_package(
            tmp_path,
            monkeypatch,
            "brokenapp",
            "raise RuntimeError('boom')\n",
        )

        with pytest.raises(RuntimeError, match="boom"):
            autodiscover("brokenapp")


class TestDiscoveryDoesNotTouchGlobalState:
    def test_no_registry_is_created_or_filled(self) -> None:
        """Discovery is pure: it returns specs, the caller registers them."""

        registry = TaskRegistry()

        assert len(registry) == 0
