import logging
import time
from multiprocessing import get_context
from typing import TYPE_CHECKING, Final

import anyio

from .presence import MasterPresence, SubprocessPresence
from .worker import Worker

if TYPE_CHECKING:
    from .schemas import MasterPolicy


__all__ = ("Master",)


logger = logging.getLogger(__name__)


# How long a terminated worker is given to exit.
TERMINATE_TIMEOUT: Final[float] = 5.0


class Master:
    """Binomic processing visor."""

    def __init__(
        self,
        *,
        broker_dsn: str,
        module_name: str,
        policy: "MasterPolicy",
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._policy = policy

        self._subprocesses: dict[str, Worker] = {}
        self._presence = MasterPresence()

    def _run_proc(self, ident: str) -> None:

        # A spawned child only receives picklable data, so the channel is built
        # here and handed over as part of the process object. It is rebuilt with
        # the worker: a cell inherited from the previous generation still holds
        # its last beat, which would read as a worker that just checked in.
        heartbeat = get_context("spawn").Value("d", 0.0)

        worker = Worker(
            broker_dsn=self._broker_dsn,
            module_name=self._module_name,
            consumer=ident,
            policy=self._policy.worker,
            presence=SubprocessPresence(heartbeat),
        )

        worker.start()
        logger.info("Worker [%s] started.", ident)

        # Recorded together, so a worker can never reach the table without the
        # cell the visor reads it through.
        self._presence.watch(ident, heartbeat)
        self._subprocesses[ident] = worker

    def _stop_proc(self, worker: "Worker") -> None:

        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=TERMINATE_TIMEOUT)

            if worker.is_alive():
                logger.warning(
                    "Worker [%s] did not stop within %ss, killing it",
                    worker.name,
                    TERMINATE_TIMEOUT,
                )
                worker.kill()

    async def arun(self) -> None:

        try:
            for i in range(self._policy.workers):
                self._run_proc(f"worker-{i}")

            await self._visor()

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                logger.error("Master process cancelled", exc_info=exc)

            raise

        finally:
            await self.aclose()

    async def _visor(self) -> None:

        while True:
            await anyio.sleep(self._policy.worker.heartbeat_interval * 2)

            now = time.time()
            status = self._presence.presence()

            for ident, worker in self._subprocesses.items():
                if (
                    now - status.get(ident, 0)
                    > self._policy.worker.heartbeat_interval * 3
                    or not worker.is_alive()
                ):
                    await anyio.to_thread.run_sync(self._stop_proc, worker)

                    # restart
                    self._run_proc(ident)
                    logger.warning("Worker [%s] restarted", ident)

    async def aclose(self) -> None:

        # Cleanup must run to completion: inside a cancelled scope `to_thread`
        # aborts before the thread is submitted, stopping no worker at all.
        with anyio.CancelScope(shield=True):
            for proc in self._subprocesses.values():
                await anyio.to_thread.run_sync(self._stop_proc, proc)
