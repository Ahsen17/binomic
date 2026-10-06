import logging
from pathlib import Path

import pytest

from binomic.task.registry import TaskRegistry, TaskSpec, registry
from binomic.task.wrappers import autodiscover, task


class TestTaskDecorator:
    def test_registers_lowercase_name(self, isolate_registry: TaskRegistry) -> None:

        @task()
        def DoWork() -> str:
            return "ok"

        assert "dowork" in isolate_registry
        assert isinstance(DoWork, TaskSpec)
        assert DoWork.name == "dowork"

    async def test_decorated_object_is_callable(
        self, isolate_registry: TaskRegistry
    ) -> None:

        @task()
        def alpha() -> str:
            return "ok"

        assert alpha.mode == "direct"
        assert await alpha() == "ok"

    def test_delay_mode_requires_delay(self) -> None:

        with pytest.raises(ValueError, match="delay must be specified"):
            task(mode="delay")

    def test_cron_mode_requires_cron(self) -> None:

        with pytest.raises(ValueError, match="cron must be specified"):
            task(mode="cron")

    def test_delay_mode_accepts_delay(self, isolate_registry: TaskRegistry) -> None:

        @task(mode="delay", delay=5.0)
        def later() -> None: ...

        registered = registry.get("later")

        assert registered.mode == "delay"
        assert registered.delay == 5.0

    def test_cron_mode_accepts_cron(self, isolate_registry: TaskRegistry) -> None:

        @task(mode="cron", cron="* * * * *")
        def scheduled() -> None: ...

        registered = registry.get("scheduled")

        assert registered.mode == "cron"
        assert registered.cron == "* * * * *"


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
    def test_imports_tasks_modules(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        isolate_registry: TaskRegistry,
    ) -> None:

        make_package(
            tmp_path,
            monkeypatch,
            "fakeapp",
            "from binomic.task import task\n\n\n@task()\ndef alpha() -> None: ...\n",
        )

        assert autodiscover("fakeapp") == ["fakeapp.tasks"]
        assert "alpha" in isolate_registry

    def test_missing_package_warns_and_returns_empty(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        with caplog.at_level(logging.WARNING):
            imported = autodiscover("no_such_pkg_anywhere")

        assert imported == []
        assert any("not found" in r.message for r in caplog.records)

    def test_plain_module_cannot_be_recursed(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        with caplog.at_level(logging.WARNING):
            imported = autodiscover("binomic.client")

        assert imported == []
        assert any("not a package" in r.message for r in caplog.records)

    def test_skips_modules_with_other_names(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        pkg = tmp_path / "quietapp"
        pkg.mkdir()
        (pkg / "__init__.py").touch()
        (pkg / "helpers.py").write_text("VALUE = 1\n", encoding="utf-8")
        monkeypatch.syspath_prepend(tmp_path)

        assert autodiscover("quietapp") == []

    def test_broken_module_warns_by_default(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        make_package(tmp_path, monkeypatch, "brokenapp", "raise RuntimeError('boom')\n")

        with caplog.at_level(logging.WARNING):
            imported = autodiscover("brokenapp")

        assert imported == []
        assert any("Failed to import" in r.message for r in caplog.records)

    def test_broken_module_raises_on_demand(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        make_package(tmp_path, monkeypatch, "brokenapp2", "raise RuntimeError('boom')\n")

        with pytest.raises(RuntimeError, match="boom"):
            autodiscover("brokenapp2", on_error="raise")
