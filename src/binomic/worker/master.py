import socket
import time
from multiprocessing import get_context
from typing import TYPE_CHECKING, Any, Final, Literal

import anyio
import structlog

from .presence import MasterPresence, SubprocessPresence
from .worker import Worker

if TYPE_CHECKING:
    from multiprocessing.process import BaseProcess

    from .policies import MasterPolicy


TERMINATE_TIMEOUT: Final[float] = 5.0

logger = structlog.stdlib.get_logger(__name__)


class Master:
    """Supervises a fleet of worker subprocesses.

    Every worker beats on a heartbeat cell the master created; the visor
    restarts any worker whose beat stalls or whose process dies, and hands
    the replacement a fresh cell.
    """

    def __init__(
        self,
        broker_dsn: str,
        module_name: str,
        policy: "MasterPolicy",
        method: Literal["spawn"] = "spawn",
        **broker_kwargs: Any,
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._policy = policy
        self._method = method
        self._broker_kwargs = broker_kwargs

        # Consumer names carry the hostname so instances of the same app
        # sharing one Redis stay distinct identities in the consumer group.
        self._hostname = socket.gethostname()

        self._subprocesses: dict[str, BaseProcess] = {}
        self._presence = MasterPresence()

    def _run_proc(self, consumer: str) -> None:

        heartbeat = get_context("spawn").Value("d", 0.0)

        worker = Worker(
            broker_dsn=self._broker_dsn,
            module_name=self._module_name,
            consumer=consumer,
            policy=self._policy.worker_policy,
            presence=SubprocessPresence(heartbeat),
            **self._broker_kwargs,
        )

        match self._method:
            case "spawn" as method:
                ctx = get_context(method)
                process = ctx.Process(
                    name=consumer,
                    target=worker.run,
                )

                process.start()

            case _:
                raise ValueError(
                    f"Unsupported start method: {self._method}",
                )

        self._presence.watch(consumer, heartbeat)
        self._subprocesses[consumer] = process

        logger.info("Worker spawned", consumer=consumer, pid=process.pid)

    def _stop_proc(self, process: "BaseProcess") -> None:

        if process.is_alive():
            process.terminate()
            process.join(timeout=TERMINATE_TIMEOUT)

            if process.is_alive():
                process.kill()

            logger.info(
                "Worker terminated",
                consumer=process.name,
                pid=process.pid,
            )

    async def _visor(self) -> None:

        while True:
            await anyio.sleep(
                self._policy.worker_policy.heartbeat_interval * 2,
            )

            now = time.time()
            status = self._presence.presence()

            for consumer, process in self._subprocesses.items():
                # TODO: no first-beat grace — the presence cell starts at 0.0,
                # so a worker still starting up (spawn + imports + autodiscover
                # + broker connect) is killed and restarted every visor cycle.
                # Intentional for now: a start that slow is itself a fault worth
                # surfacing. Revisit with a first-beat grace window, keeping the
                # is_alive() arm always active.
                stalled = (
                    now - status.get(consumer, 0.0)
                    > self._policy.worker_policy.heartbeat_interval * 3
                )

                if stalled or not process.is_alive():
                    logger.warning(
                        "Worker restarted",
                        consumer=consumer,
                        reason="heartbeat" if stalled else "dead",
                    )

                    await anyio.to_thread.run_sync(self._stop_proc, process)

                    # Restart worker
                    self._run_proc(consumer)

    async def start(self) -> None:

        try:
            for i in range(self._policy.workers):
                self._run_proc(f"{self._hostname}-consumer-{i}")

            await self._visor()

        finally:
            await self.close()

    async def close(self) -> None:

        with anyio.CancelScope(shield=True):
            for process in self._subprocesses.values():
                await anyio.to_thread.run_sync(self._stop_proc, process)
