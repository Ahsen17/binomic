import logging
import time
from typing import TYPE_CHECKING

import anyio
from anyio import Event

from binomic.base import SerializationError
from binomic.message import Message
from binomic.task import TaskNotFoundError

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

    from binomic.broker import Broker, Entry
    from binomic.task import TaskRegistry

    from .schemas import WorkerPolicy

__alL__ = ("Worker",)


logger = logging.getLogger(__name__)


class Worker:
    """Binomic subprocessing worker."""

    def __init__(
        self,
        *,
        broker: "Broker",
        registry: "TaskRegistry",
        policy: "WorkerPolicy",
    ) -> None:

        self._broker = broker
        self._registry = registry
        self._policy = policy

        self._semaphore = anyio.Semaphore(self._policy.concurrency)
        self._timers: TaskGroup | None = None
        self._terminate = Event()

    async def arun(self) -> None:

        async with anyio.create_task_group() as timers:
            self._timers = timers
            timers.start_soon(self._heartbeat)

            try:
                async with anyio.create_task_group() as proc:
                    while not self._terminate.is_set():
                        entires = await self._broker.fetch(
                            consumer=self._policy.consumer,
                            count=self._policy.read_count,
                        )

                        if not entires:
                            await anyio.sleep(self._policy.poll_interval)
                            continue

                        for entry in entires:
                            await self._semaphore.acquire()
                            proc.start_soon(self._run, entry)

            finally:
                if not timers.cancel_scope.cancel_called:
                    timers.cancel_scope.cancel()

    async def _heartbeat(self) -> None:

        while not self._terminate.is_set():
            # TODO: Implement heartbeat logic within broker

            logger.debug("Heartbeat ticked.")
            await anyio.sleep(self._policy.heatbeat_interval)

    async def _run(self, entry: "Entry") -> None:

        try:
            _, _, fields = entry
            msg = Message.from_json(fields.get("message"))
            if time.time() - msg.enqueued_at > self._policy.task_timeout:
                logger.warning("Message %s has expired", fields.get("id"))
                return

            await self._registry.get(msg.name)(*msg.args, **msg.kwargs)

        except (SerializationError, TaskNotFoundError) as err:
            logger.error(
                "Failed to process message %s: %s",
                fields.get("id"),
                str(err),
            )
            return

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise

            logger.error(
                "Failed to process message %s: %s",
                fields.get("id"),
                str(exc),
            )

        finally:
            await self._broker.ack(entry)
            self._semaphore.release()

    async def aclose(self) -> None:

        if not self._terminate.is_set():
            self._terminate.set()

        if self._timers and not self._timers.cancel_scope.cancel_called:
            self._timers.cancel_scope.cancel()
