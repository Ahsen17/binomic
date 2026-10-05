import logging
import time
from typing import TYPE_CHECKING

import anyio

from binomic.broker import AsyncredisFactory

from .presence import ParentPresence
from .schemas import WorkerPolicy
from .worker import Worker

if TYPE_CHECKING:
    from .schemas import MasterPolicy


__all__ = ("Master",)


logger = logging.getLogger(__name__)


class Master:
    """Binomic processing visor."""

    def __init__(
        self,
        *,
        redis_dsn: str,
        module_name: str,
        policy: "MasterPolicy",
    ) -> None:

        self._redis_dsn = redis_dsn
        self._module_name = module_name
        self._policy = policy

        self._redis_factory = AsyncredisFactory(redis_dsn)
        self._subprocesses: dict[str, Worker] = {}
        self._presence: ParentPresence | None = None

    def _run_proc(self, ident: str) -> "Worker":

        worker = Worker(
            redis_dsn=self._redis_dsn,
            module_name=self._module_name,
            policy=WorkerPolicy(
                consumer=ident,
                queues=self._policy.queues,
                concurrency=self._policy.concurrency,
                task_timeout=self._policy.task_timeout,
                read_count=self._policy.read_count,
                poll_interval=self._policy.poll_interval,
                heartbeat_interval=self._policy.heartbeat_interval,
            ),
        )

        worker.start()
        return worker

    def _stop_proc(self, worker: "Worker") -> None:

        if worker.is_alive():
            worker.terminate()
            worker.join()

    async def arun(self) -> None:

        try:
            if self._presence is None:
                self._presence = ParentPresence(self._redis_factory.from_pool())
                await self._presence.initialize()

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

        while True:
            await anyio.sleep(self._policy.heartbeat_interval * 2)

            now = time.time()
            status = await self._presence.presence()

            for ident in self._subprocesses:
                if (
                    now - status.get(ident, 0) > self._policy.heartbeat_interval * 3
                    or not self._subprocesses[ident].is_alive()
                ):
                    worker = self._subprocesses[ident]
                    await anyio.to_thread.run_sync(self._stop_proc, worker)

                    # restart
                    self._subprocesses[ident] = self._run_proc(ident)
                    logger.warning("Worker %s restarted", ident)

    async def aclose(self) -> None:

        for proc in self._subprocesses.values():
            await anyio.to_thread.run_sync(self._stop_proc, proc)
