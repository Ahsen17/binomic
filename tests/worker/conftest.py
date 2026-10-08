"""Fixtures scoped to worker-module tests."""

from collections.abc import Callable
from typing import Any

import pytest

from binomic.worker import Master, MasterPolicy, Worker, WorkerPolicy


@pytest.fixture
def worker_policy() -> WorkerPolicy:

    return WorkerPolicy(queues=["default"], concurrency=2)


@pytest.fixture
def master_policy(worker_policy: WorkerPolicy) -> MasterPolicy:

    return MasterPolicy(workers=1, worker=worker_policy)


@pytest.fixture
def make_master(master_policy: MasterPolicy) -> Callable[..., Master]:
    """Build a Master without starting any worker."""

    def _make(**overrides: Any) -> Master:

        fields: dict[str, Any] = {
            "broker_dsn": "redis://localhost:6379/0",
            "redis_dsn": "redis://localhost:6379/0",
            "module_name": "binomic",
            "policy": master_policy,
        }
        fields.update(overrides)

        return Master(**fields)

    return _make


@pytest.fixture
def make_worker(worker_policy: WorkerPolicy) -> Callable[..., Worker]:
    """Build a Worker without starting the subprocess."""

    def _make(**overrides: Any) -> Worker:

        fields: dict[str, Any] = {
            "broker_dsn": "redis://localhost:6379/0",
            "redis_dsn": "redis://localhost:6379/0",
            "module_name": "binomic_no_such_pkg",
            "consumer": "test-worker",
            "policy": worker_policy,
        }
        fields.update(overrides)

        return Worker(**fields)

    return _make
