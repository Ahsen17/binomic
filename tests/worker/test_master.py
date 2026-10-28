import os
import time
from collections.abc import Callable
from multiprocessing import Process, get_context
from socket import gethostname
from typing import TYPE_CHECKING, Any

import anyio
import pytest
from pytest_mock import MockerFixture

from binomic.otel import telemetry
from binomic.worker import (
    Master,
    MasterPolicy,
    MasterPresence,
    WorkerPolicy,
)
from binomic.worker.master import TERMINATE_TIMEOUT

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized

# A supervision loop that iterates quickly; the default is ten seconds a tick.
BUSY_POLICY = MasterPolicy(
    workers=1,
    worker_policy=WorkerPolicy(
        queues=["default"], concurrency=2, heartbeat_interval=0.01
    ),
)

# The otel-enabled variant, for the supervision telemetry cases.
BUSY_OTEL_POLICY = MasterPolicy(
    workers=1,
    worker_policy=WorkerPolicy(
        queues=["default"], concurrency=2, heartbeat_interval=0.01, otel_enabled=True
    ),
)
LOOP_BUDGET: float = 2.0


def metric_points(metrics_data: Any, name: str) -> list[Any]:

    return [
        point
        for resource in metrics_data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]


@pytest.fixture
def fake_ctx(mocker: MockerFixture) -> Any:
    """A spawn-like context whose Process is a mock: _run_proc runs fully,
    without ever really forking."""

    ctx = mocker.Mock()
    ctx.Value = get_context("spawn").Value
    ctx.Process = mocker.Mock()
    return ctx


class TestMaster:
    def test_holds_the_broker_dsn_and_policy(
        self, make_master: Callable[..., Master]
    ) -> None:

        master = make_master()

        assert master._policy.workers == 1
        assert master._subprocesses == {}

    def test_run_proc_builds_and_starts_a_worker_process(
        self,
        make_master: Callable[..., Master],
        fake_ctx: Any,
        mocker: MockerFixture,
    ) -> None:

        master = make_master(module_name="app")
        worker_cls = mocker.patch("binomic.worker.master.Worker")
        mocker.patch("binomic.worker.master.get_context", return_value=fake_ctx)

        master._run_proc("worker-0")

        worker_cls.assert_called_once_with(
            broker_dsn="redis://localhost:6379/0",
            module_name="app",
            consumer="worker-0",
            policy=master._policy.worker_policy,
            presence=mocker.ANY,
        )
        fake_ctx.Process.assert_called_once_with(
            name="worker-0",
            target=worker_cls.return_value.run,
        )
        fake_ctx.Process.return_value.start.assert_called_once_with()
        assert master._subprocesses["worker-0"] is fake_ctx.Process.return_value

    def test_run_proc_wires_both_sides_to_one_cell(
        self,
        make_master: Callable[..., Master],
        fake_ctx: Any,
        mocker: MockerFixture,
    ) -> None:

        master = make_master()
        mocker.patch("binomic.worker.master.Worker")
        mocker.patch("binomic.worker.master.get_context", return_value=fake_ctx)

        master._run_proc("worker-0")

        assert master._presence.presence() == {"worker-0": 0.0}

        worker_side = master._presence._cells["worker-0"]
        worker_side.value = 42.0

        assert master._presence.presence()["worker-0"] == 42.0

    def test_each_generation_gets_its_own_cell(
        self,
        make_master: Callable[..., Master],
        fake_ctx: Any,
        mocker: MockerFixture,
    ) -> None:

        master = make_master()
        mocker.patch("binomic.worker.master.Worker")
        mocker.patch("binomic.worker.master.get_context", return_value=fake_ctx)

        master._run_proc("worker-0")
        master._presence._cells["worker-0"].value = 42.0

        master._run_proc("worker-0")

        # Handing the successor the old cell would give it its predecessor's
        # last beat, which reads as a worker that just checked in.
        assert master._presence.presence()["worker-0"] == 0.0

    def test_stop_proc_terminates_alive_worker(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Process)
        worker.is_alive.side_effect = [True, False]

        master._stop_proc(worker)

        worker.terminate.assert_called_once_with()
        worker.join.assert_called_once_with(timeout=TERMINATE_TIMEOUT)
        worker.kill.assert_not_called()

    def test_stop_proc_kills_worker_that_outlives_the_timeout(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Process)
        worker.is_alive.return_value = True

        master._stop_proc(worker)

        worker.kill.assert_called_once_with()

    def test_stop_proc_skips_dead_worker(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Process)
        worker.is_alive.return_value = False

        master._stop_proc(worker)

        worker.terminate.assert_not_called()
        worker.join.assert_not_called()

    async def test_close_stops_every_subprocess(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        workers = {ident: mocker.Mock(spec=Process) for ident in ("worker-0", "worker-1")}
        workers["worker-0"].is_alive.return_value = True
        workers["worker-1"].is_alive.return_value = False
        master._subprocesses = workers

        await master.close()

        workers["worker-0"].terminate.assert_called_once_with()
        workers["worker-1"].terminate.assert_not_called()

    async def test_close_survives_an_already_cancelled_scope(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        worker = mocker.Mock(spec=Process)
        worker.is_alive.return_value = True
        master._subprocesses = {"worker-0": worker}

        # Cleanup is shielded: without it `to_thread` aborts at its first
        # checkpoint, inside the cancelled scope, and no worker is stopped.
        with anyio.move_on_after(0):
            await master.close()

        worker.terminate.assert_called_once_with()


class TestMasterStart:
    async def test_starts_one_worker_per_policy_and_closes(
        self,
        make_master: Callable[..., Master],
        fake_ctx: Any,
        mocker: MockerFixture,
    ) -> None:

        master = make_master()
        mocker.patch("binomic.worker.master.Worker")
        mocker.patch("binomic.worker.master.get_context", return_value=fake_ctx)
        mocker.patch.object(master, "_visor", mocker.AsyncMock())
        closed = mocker.patch.object(master, "close", mocker.AsyncMock())

        await master.start()

        assert set(master._subprocesses) == {f"{gethostname()}-{os.getpid()}-consumer-0"}
        closed.assert_awaited_once()

    async def test_starts_worker_per_configured_worker(
        self,
        make_master: Callable[..., Master],
        worker_policy: WorkerPolicy,
        mocker: MockerFixture,
    ) -> None:

        master = make_master(policy=MasterPolicy(workers=3, worker_policy=worker_policy))
        started = mocker.patch.object(
            master,
            "_run_proc",
            side_effect=lambda _ident: mocker.Mock(),
        )
        mocker.patch.object(master, "_visor", mocker.AsyncMock())
        mocker.patch.object(master, "close", mocker.AsyncMock())

        await master.start()

        assert [call.args[0] for call in started.call_args_list] == [
            f"{gethostname()}-{os.getpid()}-consumer-{i}" for i in range(3)
        ]

    async def test_closes_workers_when_supervision_fails(
        self, make_master: Callable[..., Master], mocker: MockerFixture
    ) -> None:

        master = make_master()
        mocker.patch.object(master, "_run_proc", return_value=mocker.Mock(spec=Process))
        mocker.patch.object(
            master, "_visor", mocker.AsyncMock(side_effect=RuntimeError("visor boom"))
        )
        closed = mocker.patch.object(master, "close", mocker.AsyncMock())

        # Supervision failures propagate (no swallowing): a master that cannot
        # start its workers must not fake a healthy run.
        with pytest.raises(RuntimeError, match="visor boom"):
            await master.start()

        closed.assert_awaited_once()


class TestMasterVisor:
    async def test_restarts_a_worker_that_died(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        dead = mocker.Mock(spec=Process)
        dead.is_alive.return_value = False
        master._subprocesses = {"worker-0": dead}

        presence = mocker.Mock(spec=MasterPresence)
        presence.presence.return_value = {"worker-0": time.time()}
        master._presence = presence
        stopped = mocker.patch.object(master, "_stop_proc")
        replacement = mocker.Mock(spec=Process)

        def fake_run(consumer: str) -> None:
            master._subprocesses[consumer] = replacement
            scope.cancel()

        with anyio.move_on_after(LOOP_BUDGET) as scope:
            # Cancels once the replacement is registered: the visor has then
            # finished its restart pass.
            mocker.patch.object(master, "_run_proc", side_effect=fake_run)
            await master._visor()

        stopped.assert_called_once_with(dead)
        assert master._subprocesses["worker-0"] is replacement

    async def test_restarts_a_worker_whose_heartbeat_went_stale(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        silent = mocker.Mock(spec=Process)
        silent.is_alive.return_value = True
        master._subprocesses = {"worker-0": silent}

        presence = mocker.Mock(spec=MasterPresence)
        presence.presence.return_value = {"worker-0": time.time() - 100}
        master._presence = presence
        stopped = mocker.patch.object(master, "_stop_proc")
        replacement = mocker.Mock(spec=Process)

        def fake_run(consumer: str) -> None:
            master._subprocesses[consumer] = replacement
            scope.cancel()

        with anyio.move_on_after(LOOP_BUDGET) as scope:
            mocker.patch.object(master, "_run_proc", side_effect=fake_run)
            await master._visor()

        stopped.assert_called_once_with(silent)
        assert master._subprocesses["worker-0"] is replacement

    async def test_leaves_a_healthy_worker_alone(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
    ) -> None:

        master = make_master(policy=BUSY_POLICY)
        alive = mocker.Mock(spec=Process)
        alive.is_alive.return_value = True
        master._subprocesses = {"worker-0": alive}

        presence = mocker.Mock(spec=MasterPresence)
        # A heartbeat must stay fresh for the whole loop, not just the first tick.
        presence.presence.side_effect = lambda: {"worker-0": time.time()}
        master._presence = presence
        stopped = mocker.patch.object(master, "_stop_proc")
        restarted = mocker.patch.object(master, "_run_proc")

        with anyio.move_on_after(LOOP_BUDGET):
            await master._visor()

        stopped.assert_not_called()
        restarted.assert_not_called()
        assert master._subprocesses["worker-0"] is alive


class TestMasterTelemetry:
    """The supervision surface as OTel metrics: restarts counter + age gauge."""

    def test_disabled_builds_no_instruments(
        self, make_master: Callable[..., Master]
    ) -> None:

        master = make_master()

        assert master._restarts is None

    def test_enabled_requires_the_extra(
        self,
        make_master: Callable[..., Master],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        monkeypatch.setattr(telemetry, "AVAILABLE", False)

        with pytest.raises(RuntimeError, match=r"binomic\[otel\]"):
            make_master(policy=BUSY_OTEL_POLICY)

    async def test_a_stale_worker_counts_a_heartbeat_restart(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
        otel_sdk: Any,
    ) -> None:

        _, metric_reader = otel_sdk
        master = make_master(policy=BUSY_OTEL_POLICY)
        silent = mocker.Mock(spec=Process)
        silent.is_alive.return_value = True
        master._subprocesses = {"consumer-stale": silent}

        presence = mocker.Mock(spec=MasterPresence)
        presence.presence.return_value = {"consumer-stale": time.time() - 100}
        master._presence = presence
        replacement = mocker.Mock(spec=Process)

        def fake_run(consumer: str) -> None:
            master._subprocesses[consumer] = replacement
            scope.cancel()

        with anyio.move_on_after(LOOP_BUDGET) as scope:
            mocker.patch.object(master, "_run_proc", side_effect=fake_run)
            await master._visor()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        (point,) = [
            point
            for point in metric_points(metrics_data, "binomic.worker.restarts")
            if point.attributes["binomic.worker.consumer"] == "consumer-stale"
        ]

        assert point.value == 1
        assert point.attributes["binomic.worker.reason"] == "heartbeat"

    async def test_a_dead_worker_counts_a_dead_restart(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
        otel_sdk: Any,
    ) -> None:

        _, metric_reader = otel_sdk
        master = make_master(policy=BUSY_OTEL_POLICY)
        dead = mocker.Mock(spec=Process)
        dead.is_alive.return_value = False
        master._subprocesses = {"consumer-dead": dead}

        presence = mocker.Mock(spec=MasterPresence)
        # A beat taken at setup can age past the stalled threshold on a loaded
        # CI; a fresh beat on every check pins the reason to the dead arm.
        presence.presence.side_effect = lambda: {"consumer-dead": time.time()}
        master._presence = presence
        replacement = mocker.Mock(spec=Process)

        def fake_run(consumer: str) -> None:
            master._subprocesses[consumer] = replacement
            scope.cancel()

        with anyio.move_on_after(LOOP_BUDGET) as scope:
            mocker.patch.object(master, "_run_proc", side_effect=fake_run)
            await master._visor()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        (point,) = [
            point
            for point in metric_points(metrics_data, "binomic.worker.restarts")
            if point.attributes["binomic.worker.consumer"] == "consumer-dead"
        ]

        assert point.value == 1
        assert point.attributes["binomic.worker.reason"] == "dead"

    async def test_close_terminations_are_not_restarts(
        self,
        make_master: Callable[..., Master],
        mocker: MockerFixture,
        otel_sdk: Any,
    ) -> None:

        _, metric_reader = otel_sdk
        master = make_master(policy=BUSY_OTEL_POLICY)
        worker = mocker.Mock(spec=Process)
        worker.is_alive.return_value = True
        master._subprocesses = {"consumer-close": worker}

        await master.close()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        assert not [
            point
            for point in metric_points(metrics_data, "binomic.worker.restarts")
            if point.attributes["binomic.worker.consumer"] == "consumer-close"
        ]

    def test_the_age_gauge_reports_per_consumer(
        self,
        make_master: Callable[..., Master],
        make_heartbeat: Callable[[], "Synchronized[float]"],
        mocker: MockerFixture,
    ) -> None:

        # An observable gauge is a process-global and the first registration
        # of its name wins, so the wiring and the callback are asserted
        # directly instead of through the shared session meter provider.
        registered: list[dict[str, Any]] = []
        mocker.patch(
            "binomic.worker.master.create_observable_gauge",
            side_effect=lambda **kwargs: registered.append(kwargs),
        )

        master = make_master(policy=BUSY_OTEL_POLICY)

        assert registered != []
        assert registered[0]["name"] == "binomic.worker.heartbeat.age"
        assert registered[0]["unit"] == "s"

        cell = make_heartbeat()
        cell.value = time.time() - 10.0
        master._presence.watch("consumer-age", cell)

        ((age, attributes),) = registered[0]["observe"]()

        assert attributes == {"binomic.worker.consumer": "consumer-age"}
        # The cell was set ten seconds ago; the age reads through the clock.
        assert 10.0 <= age < 60.0
