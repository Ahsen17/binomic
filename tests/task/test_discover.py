from pathlib import Path

import pytest

from binomic.task import TaskRegistry, TaskSpec, autodiscover


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


class TestDiscoveryDoesNotTouchRegistries:
    def test_returns_specs_without_filling_a_registry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Discovery is pure: it returns specs, the caller registers them."""

        make_package(
            tmp_path,
            monkeypatch,
            "purityapp",
            "from binomic.task import task\n\n\n"
            '@task("default", mode="direct")\n'
            "def pure() -> None: ...\n",
        )
        registry = TaskRegistry()

        specs = autodiscover("purityapp")

        assert [spec.name for spec in specs] == ["pure"]
        assert len(registry) == 0
