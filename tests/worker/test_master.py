import time
from collections.abc import Callable

import anyio
import pytest
from pytest_mock import MockerFixture

from binomic.worker import Master, MasterPolicy, Worker, WorkerPolicy
from binomic.worker.master import TERMINATE_TIMEOUT

# A supervision loop that iterates quickly; the default is ten seconds a tick.
BUSY_POLICY = MasterPolicy(
    workers=1,
    worker=WorkerPolicy(queues=["default"], concurrency=2, heartbeat_interval=0.01),
)
LOOP_BUDGET: float = 2.0


class TestMaster:
    def test_holds_dsn_and_policy(self, make_master: Callable[..., Master]) -> None:

        master = make_master()

        assert master._policy.workers == 1
        assert master._subprocesses == {}

    def test_run_proc_builds_and_starts_worker(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master(module_name="app")
        worker_cls = mocker.patch("binomic.worker.master.Worker")

        worker = master._run_proc("worker-0")

        worker_cls.assert_called_once_with(
            broker_dsn="redis://localhost:6379/0",
            redis_dsn="redis://localhost:6379/0",
            module_name="app",
            consumer="worker-0",
            policy=master._policy.worker,
        )
        worker_cls.return_value.start.assert_called_once_with()
        assert worker is worker_cls.return_value

    def test_stop_proc_terminates_alive_worker(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.side_effect = [True, False]

        master._stop_proc(worker)

        worker.terminate.assert_called_once_with()
        worker.join.assert_called_once_with(timeout=TERMINATE_TIMEOUT)
        worker.kill.assert_not_called()

    def test_stop_proc_kills_worker_that_outlives_the_timeout(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.return_value = True

        master._stop_proc(worker)

        worker.kill.assert_called_once_with()

    def test_stop_proc_skips_dead_worker(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.return_value = False

        master._stop_proc(worker)

        worker.terminate.assert_not_called()
        worker.join.assert_not_called()

    async def test_aclose_stops_every_subprocess(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        workers = {ident: mocker.Mock(spec=Worker) for ident in ("worker-0", "worker-1")}
        workers["worker-0"].is_alive.return_value = True
        workers["worker-1"].is_alive.return_value = False
        master._subprocesses = workers

        await master.aclose()

        workers["worker-0"].terminate.assert_called_once_with()
        workers["worker-1"].terminate.assert_not_called()

    async def test_aclose_survives_an_already_cancelled_scope(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Worker)
        worker.is_alive.return_value = True
        master._subprocesses = {"worker-0": worker}

        # Cleanup is shielded: without it `to_thread` aborts at its first
        # checkpoint, inside the cancelled scope, and no worker is stopped.
        with anyio.move_on_after(0):
            await master.aclose()

        worker.terminate.assert_called_once_with()


class TestMasterArun:
    async def test_starts_one_worker_per_policy_and_closes(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        mocker.patch.object(master, "_run_proc", return_value=mocker.Mock(spec=Worker))
        mocker.patch.object(master, "_visor", mocker.AsyncMock())
        closed = mocker.patch.object(master, "aclose", mocker.AsyncMock())

        await master.arun()

        assert set(master._subprocesses) == {"worker-0"}
        closed.assert_awaited_once()

    async def test_starts_worker_per_configured_worker(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
        worker_policy: WorkerPolicy,
    ) -> None:

        master = make_master(policy=MasterPolicy(workers=3, worker=worker_policy))
        started = mocker.patch.object(
            master, "_run_proc", side_effect=lambda _ident: mocker.Mock(spec=Worker)
        )
        mocker.patch.object(master, "_visor", mocker.AsyncMock())
        mocker.patch.object(master, "aclose", mocker.AsyncMock())

        await master.arun()

        assert [call.args[0] for call in started.call_args_list] == [
            "worker-0",
            "worker-1",
            "worker-2",
        ]

    async def test_closes_workers_when_supervision_fails(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        mocker.patch.object(master, "_run_proc", return_value=mocker.Mock(spec=Worker))
        mocker.patch.object(
            master, "_visor", mocker.AsyncMock(side_effect=RuntimeError("visor boom"))
        )
        closed = mocker.patch.object(master, "aclose", mocker.AsyncMock())

        with pytest.raises(RuntimeError, match="visor boom"):
            await master.arun()

        closed.assert_awaited_once()


class TestMasterVisor:
    async def test_restarts_a_worker_that_died(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        dead = mocker.Mock(spec=Worker)
        dead.is_alive.return_value = False
        master._subprocesses = {"worker-0": dead}

        presence = mocker.AsyncMock()
        presence.presence.return_value = {"worker-0": time.time()}
        mocker.patch("binomic.worker.master.ParentPresence", return_value=presence)
        stopped = mocker.patch.object(master, "_stop_proc")
        replacement: Worker = mocker.Mock(spec=Worker)

        with anyio.move_on_after(LOOP_BUDGET) as scope:

            def _restart(_ident: str) -> Worker:

                scope.cancel()

                return replacement

            mocker.patch.object(master, "_run_proc", side_effect=_restart)
            await master._visor()

        stopped.assert_called_once_with(dead)
        assert master._subprocesses["worker-0"] is replacement

    async def test_restarts_a_worker_whose_heartbeat_went_stale(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        silent = mocker.Mock(spec=Worker)
        silent.is_alive.return_value = True
        master._subprocesses = {"worker-0": silent}

        presence = mocker.AsyncMock()
        presence.presence.return_value = {"worker-0": time.time() - 100}
        mocker.patch("binomic.worker.master.ParentPresence", return_value=presence)
        stopped = mocker.patch.object(master, "_stop_proc")
        replacement: Worker = mocker.Mock(spec=Worker)

        with anyio.move_on_after(LOOP_BUDGET) as scope:

            def _restart(_ident: str) -> Worker:

                scope.cancel()

                return replacement

            mocker.patch.object(master, "_run_proc", side_effect=_restart)
            await master._visor()

        stopped.assert_called_once_with(silent)
        assert master._subprocesses["worker-0"] is replacement

    async def test_leaves_a_healthy_worker_alone(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        alive = mocker.Mock(spec=Worker)
        alive.is_alive.return_value = True
        master._subprocesses = {"worker-0": alive}

        presence = mocker.AsyncMock()
        # A heartbeat must stay fresh for the whole loop, not just the first tick.
        presence.presence.side_effect = lambda: {"worker-0": time.time()}
        mocker.patch("binomic.worker.master.ParentPresence", return_value=presence)
        stopped = mocker.patch.object(master, "_stop_proc")
        restarted = mocker.patch.object(master, "_run_proc")

        with anyio.move_on_after(LOOP_BUDGET):
            await master._visor()

        stopped.assert_not_called()
        restarted.assert_not_called()
        assert master._subprocesses["worker-0"] is alive
