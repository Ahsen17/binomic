from binomic.base import BaseStruct
from binomic.worker.schemas import MasterPolicy, WorkerPolicy


class TestWorkerPolicy:
    def test_required_fields(self) -> None:

        policy = WorkerPolicy(queues=["a"], concurrency=3)

        assert policy.queues == ["a"]
        assert policy.concurrency == 3

    def test_defaults(self) -> None:

        policy = WorkerPolicy(queues=["a"], concurrency=1)

        assert policy.task_timeout == 600.0
        assert policy.max_attempts == 3
        assert policy.read_count == 10
        assert policy.poll_interval == 0.1
        assert policy.heartbeat_interval == 5.0
        assert policy.reclaim_interval == 30.0


class TestMasterPolicy:
    def test_wraps_worker_policy(self, worker_policy: WorkerPolicy) -> None:

        policy = MasterPolicy(workers=2, worker=worker_policy)

        assert policy.workers == 2
        assert policy.worker is worker_policy

    def test_is_base_struct(self) -> None:

        assert issubclass(MasterPolicy, BaseStruct)
