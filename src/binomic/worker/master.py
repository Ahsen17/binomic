import logging
import time
from typing import TYPE_CHECKING, Final

import anyio
from redis.asyncio import Redis as AsyncRedis

from .presence import ParentPresence
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
        redis_dsn: str,
        module_name: str,
        policy: "MasterPolicy",
    ) -> None:

        self._broker_dsn = broker_dsn
        self._redis_dsn = redis_dsn
        self._module_name = module_name
        self._policy = policy

        self._subprocesses: dict[str, Worker] = {}
        self._presence: ParentPresence | None = None

    def _run_proc(self, ident: str) -> "Worker":

        worker = Worker(
            broker_dsn=self._broker_dsn,
            redis_dsn=self._redis_dsn,
            module_name=self._module_name,
            consumer=ident,
            policy=self._policy.worker,
        )

        worker.start()
        logger.info("Worker [%s] started.", ident)

        return worker

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
                ident = f"worker-{i}"

                self._subprocesses[ident] = self._run_proc(ident)

            await self._visor()

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                logger.error("Master process cancelled", exc_info=exc)

            raise

        finally:
            await self.aclose()

    async def _visor(self) -> None:

        if self._presence is None:
            self._presence = ParentPresence(
                AsyncRedis.from_url(self._redis_dsn, decode_responses=True),
            )
            await self._presence.initialize()

        while True:
            await anyio.sleep(self._policy.worker.heartbeat_interval * 2)

            now = time.time()
            status = await self._presence.presence()

            for ident in self._subprocesses:
                if (
                    now - status.get(ident, 0)
                    > self._policy.worker.heartbeat_interval * 3
                    or not self._subprocesses[ident].is_alive()
                ):
                    worker = self._subprocesses[ident]
                    await anyio.to_thread.run_sync(self._stop_proc, worker)

                    # restart
                    self._subprocesses[ident] = self._run_proc(ident)
                    logger.warning("Worker [%s] restarted", ident)

    async def aclose(self) -> None:

        # Cleanup must run to completion: inside a cancelled scope `to_thread`
        # aborts before the thread is submitted, stopping no worker at all.
        with anyio.CancelScope(shield=True):
            for proc in self._subprocesses.values():
                await anyio.to_thread.run_sync(self._stop_proc, proc)
