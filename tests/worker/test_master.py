from pytest_mock import MockerFixture

from binomic.worker import Master, MasterPolicy, Worker


class TestMaster:
    def test_holds_dsn_and_policy(
        self,
        master_policy: MasterPolicy,
    ) -> None:

        master = Master(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            policy=master_policy,
        )

        assert master._policy.workers == 1
        assert master._subprocesses == {}

    def test_stop_proc_terminates_alive_worker(
        self, master_policy: MasterPolicy, mocker: MockerFixture
    ) -> None:

        master = Master(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            policy=master_policy,
        )
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.return_value = True

        master._stop_proc(worker)

        worker.terminate.assert_called_once_with()
        worker.join.assert_called_once_with()

    def test_stop_proc_skips_dead_worker(
        self, master_policy: MasterPolicy, mocker: MockerFixture
    ) -> None:

        master = Master(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            policy=master_policy,
        )
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.return_value = False

        master._stop_proc(worker)

        worker.terminate.assert_not_called()
        worker.join.assert_not_called()

    async def test_aclose_stops_every_subprocess(
        self,
        master_policy: MasterPolicy,
        mocker: MockerFixture,
    ) -> None:

        master = Master(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="binomic",
            policy=master_policy,
        )
        workers = {ident: mocker.Mock(spec=Worker) for ident in ("worker-0", "worker-1")}
        workers["worker-0"].is_alive.return_value = True
        workers["worker-1"].is_alive.return_value = False
        master._subprocesses = workers

        await master.aclose()

        workers["worker-0"].terminate.assert_called_once_with()
        workers["worker-1"].terminate.assert_not_called()
