"""Fixtures scoped to worker-module tests."""

import importlib
import sys
from collections.abc import AsyncIterator, Callable
from multiprocessing import get_context
from typing import TYPE_CHECKING, Any

import pytest

from binomic.worker import Master, MasterPolicy, Worker, WorkerHandler, WorkerPolicy
from binomic.worker.presence import SubprocessPresence

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized


@pytest.fixture
def worker_policy() -> WorkerPolicy:

    return WorkerPolicy(queues=["default"], concurrency=2)


@pytest.fixture
def master_policy(worker_policy: WorkerPolicy) -> MasterPolicy:

    return MasterPolicy(workers=1, worker_policy=worker_policy)


@pytest.fixture
def make_heartbeat() -> Callable[[], "Synchronized[float]"]:
    """Build heartbeat cells, the way the master builds one per worker."""

    def _make() -> "Synchronized[float]":

        return get_context("spawn").Value("d", 0.0)

    return _make


@pytest.fixture
def heartbeat(
    make_heartbeat: Callable[[], "Synchronized[float]"],
) -> "Synchronized[float]":
    """A single heartbeat cell, for tests that need only one."""

    return make_heartbeat()


@pytest.fixture
def make_master(master_policy: MasterPolicy) -> Callable[..., Master]:
    """Build a Master without starting any worker."""

    def _make(**overrides: Any) -> Master:

        fields: dict[str, Any] = {
            "broker_dsn": "redis://localhost:6379/0",
            "module_name": "binomic",
            "policy": master_policy,
        }
        fields.update(overrides)

        return Master(**fields)

    return _make


@pytest.fixture
def make_worker(
    worker_policy: WorkerPolicy, heartbeat: "Synchronized[float]"
) -> Callable[..., Worker]:
    """Build a Worker without starting the subprocess."""

    def _make(**overrides: Any) -> Worker:

        fields: dict[str, Any] = {
            "broker_dsn": "redis://localhost:6379/0",
            "module_name": "binomic_no_such_pkg",
            "consumer": "test-worker",
            "policy": worker_policy,
            "presence": SubprocessPresence(heartbeat),
        }
        fields.update(overrides)

        return Worker(**fields)

    return _make


@pytest.fixture
async def make_assembled_worker(
    make_worker: Callable[..., Worker],
) -> AsyncIterator[Callable[..., Worker]]:
    """Track workers built in a test and close every one on teardown.

    The caller drives the lifecycle it needs (`await worker.start()` before
    driving `arun` internals); the fixture only guarantees cleanup.
    """

    workers: list[Worker] = []
    original = make_worker

    def _make(**overrides: Any) -> Worker:

        worker = original(**overrides)
        workers.append(worker)

        return worker

    yield _make

    for worker in workers:
        await worker.close()


@pytest.fixture
def set_backoff(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """Pin the redelivery backoff so a case can observe it without waiting for it."""

    def _set(seconds: float) -> None:

        monkeypatch.setattr(
            WorkerHandler,
            "_backoff",
            lambda self, attempt: seconds,
        )

    return _set


TASKS_SOURCE = """\
import anyio

from binomic.task import task


@task("default", mode="direct")
async def ok() -> None: ...


@task("default", mode="direct")
async def boom() -> None:
    raise ValueError("boom")


@task("default", mode="direct")
async def slow() -> None:
    await anyio.sleep(0.5)


@task("default", mode="direct")
async def cancelled() -> None:
    raise anyio.get_cancelled_exc_class()


@task("default", mode="direct")
async def quits() -> None:
    raise SystemExit(3)
"""


@pytest.fixture(scope="module")
def handler_module(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """A tiny task package the handler autodiscovers.

    Lives in pytest's temp area, so no generated package ever lands in the
    repository and each run starts from a fresh module file.
    """

    root = tmp_path_factory.mktemp("handlerapp_root")
    pkg = root / "handlerapp"
    pkg.mkdir()
    (pkg / "__init__.py").touch()
    (pkg / "tasks.py").write_text(TASKS_SOURCE, encoding="utf-8")
    sys.path.insert(0, str(root))

    try:
        yield importlib.import_module("handlerapp.tasks")
    finally:
        sys.path.remove(str(root))
