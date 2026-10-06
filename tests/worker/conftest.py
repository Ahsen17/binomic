"""Fixtures scoped to worker-module tests."""

from collections.abc import Callable

import pytest

from binomic.worker import MasterPolicy, Worker, WorkerPolicy


@pytest.fixture
def worker_policy() -> WorkerPolicy:

    return WorkerPolicy(queues=["default"], concurrency=2)


@pytest.fixture
def master_policy(worker_policy: WorkerPolicy) -> MasterPolicy:

    return MasterPolicy(workers=1, worker=worker_policy)


@pytest.fixture
def make_worker(worker_policy: WorkerPolicy) -> Callable[..., Worker]:
    """Build a Worker without starting the subprocess."""

    def _make(**kwargs: object) -> Worker:

        return Worker(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic_no_such_pkg",
            consumer="test-worker",
            policy=worker_policy,
            **kwargs,
        )

    return _make
