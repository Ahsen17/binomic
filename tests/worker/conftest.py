"""Fixtures scoped to worker-module tests."""

from collections.abc import AsyncIterator, Callable
from multiprocessing import get_context
from typing import TYPE_CHECKING, Any

import pytest

from binomic.task import TaskScheduler
from binomic.worker import Master, MasterPolicy, Worker, WorkerPolicy
from binomic.worker.presence import SubprocessPresence

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized


@pytest.fixture
def worker_policy() -> WorkerPolicy:

    return WorkerPolicy(queues=["default"], concurrency=2)


@pytest.fixture
def master_policy(worker_policy: WorkerPolicy) -> MasterPolicy:

    return MasterPolicy(workers=1, worker=worker_policy)


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
    """Build a Worker as `arun` leaves it: its scheduler started and live.

    `_run` refuses to process anything without one, so every case that drives it
    has to arrive assembled. The caller still injects the broker it wants.
    """

    assembled: list[Worker] = []

    def _make(**overrides: Any) -> Worker:

        worker = make_worker(**overrides)
        worker._scheduler = TaskScheduler()
        worker._scheduler.start()
        assembled.append(worker)

        return worker

    yield _make

    for worker in assembled:
        await worker.aclose()


@pytest.fixture
def set_backoff(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """Pin the redelivery backoff so a case can observe it without waiting for it."""

    def _set(seconds: float) -> None:

        monkeypatch.setattr(Worker, "_backoff", lambda self, attempt: seconds)

    return _set
