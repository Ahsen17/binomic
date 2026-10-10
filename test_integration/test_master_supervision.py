"""Supervision pipeline: the master tells a stuck worker from a dead one.

A frozen worker is alive by every process-level measure, so ``is_alive`` alone
cannot catch it -- only a heartbeat that stopped advancing can. That distinction
is the whole reason the heartbeat exists, and nothing short of a real process
can demonstrate it, so this drives it for real: a genuine spawned worker, frozen
with SIGSTOP, and a genuine master that has to notice and replace it.
"""

import os
import signal

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.worker import Master, MasterPolicy, WorkerPolicy
from test_integration import E2E_QUEUE

HEARTBEAT_INTERVAL: float = 1.0

# Generous: the master polls every 2 x heartbeat_interval, then a frozen worker
# queues SIGTERM until it is continued, so the stop path falls through to
# SIGKILL and costs the full terminate timeout before the restart begins.
RESTART_TIMEOUT: float = 40.0


@pytest.mark.integration
@pytest.mark.skipif(os.name != "posix", reason="SIGSTOP/SIGCONT are POSIX signals")
class TestMasterSupervision:
    async def test_a_frozen_worker_is_restarted(
        self,
        broker_dsn: str,
        redis_client: AsyncRedis,
    ) -> None:
        # `redis_client` is unused on purpose: it pins this test's database and
        # skips the suite when no real Redis is reachable, which the master and
        # its workers need just as much as a probe would.

        master = Master(
            broker_dsn=broker_dsn,
            module_name="test_integration",
            policy=MasterPolicy(
                workers=1,
                worker=WorkerPolicy(
                    queues=[E2E_QUEUE],
                    concurrency=1,
                    heartbeat_interval=HEARTBEAT_INTERVAL,
                ),
            ),
        )

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(master.arun)

            # Let the first generation check in, so the freeze lands on a worker
            # that was genuinely healthy rather than on one still starting up.
            # Both the cell and the process are registered in `_run_proc`, so
            # waiting on `_subprocesses` also means the cell is readable.
            with anyio.fail_after(RESTART_TIMEOUT):
                while (
                    "worker-0" not in master._subprocesses
                    or master._presence.presence().get("worker-0", 0.0) == 0.0
                ):
                    await anyio.sleep(0.05)

            original = master._subprocesses["worker-0"]
            original_pid = original.pid
            assert original_pid is not None

            os.kill(original_pid, signal.SIGSTOP)
            await anyio.sleep(0.2)

            # The premise of the case: frozen is not dead.
            assert original.is_alive(), "SIGSTOP should have left the process alive"
            frozen_at = master._presence.presence().get("worker-0", 0.0)

            with anyio.fail_after(RESTART_TIMEOUT):
                while master._subprocesses["worker-0"].pid == original_pid:
                    await anyio.sleep(0.05)

            replacement = master._subprocesses["worker-0"]

            assert replacement.pid != original_pid

            # Beating past the frozen stamp proves both halves at once: the
            # replacement runs, and it got a fresh cell rather than the frozen
            # one, which would read as a worker that had just checked in.
            with anyio.fail_after(RESTART_TIMEOUT):
                while master._presence.presence().get("worker-0", 0.0) <= frozen_at:
                    await anyio.sleep(0.05)

            assert replacement.is_alive()

            tasks.cancel_scope.cancel()
