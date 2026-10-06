import logging
import time
from multiprocessing import Process
from typing import TYPE_CHECKING

import anyio
from redis.asyncio import Redis as AsyncRedis

from binomic.base import SerializationError
from binomic.broker import Broker, BrokerFactory, Entry
from binomic.message import Message
from binomic.task import TaskNotFoundError, autodiscover
from binomic.task.registry import registry

from .presence import SubprocessPresence

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

    from .schemas import WorkerPolicy


__all__ = ("Worker",)


logger = logging.getLogger(__name__)


class Worker(Process):
    """Binomic subprocessing worker."""

    def __init__(
        self,
        *,
        broker_dsn: str,
        redis_dsn: str,
        module_name: str,
        consumer: str,
        policy: "WorkerPolicy",
    ) -> None:

        super().__init__(name=consumer)

        self._broker_dsn = broker_dsn
        self._redis_dsn = redis_dsn
        self._module_name = module_name
        self._consumer = consumer
        self._policy = policy

        self._registry = registry
        self._broker: Broker | None = None

        self._presence: SubprocessPresence | None = None
        self._semaphore = anyio.Semaphore(policy.concurrency)
        self._timers: TaskGroup | None = None
        self._terminate = anyio.Event()

        autodiscover(module_name)

    async def arun(self) -> None:

        if self._broker is None:
            self._broker = BrokerFactory(
                dsn=self._broker_dsn,
                queues=self._policy.queues,
            )()

            await self._broker.initialize()

        try:
            async with anyio.create_task_group() as timers:
                self._timers = timers
                timers.start_soon(self._heartbeat)

                last_reclaim = time.monotonic()

                while not self._terminate.is_set():
                    entries = await self._broker.acquire(
                        consumer=self._consumer,
                        count=self._policy.read_count,
                    )

                    if not entries:
                        await anyio.sleep(self._policy.poll_interval)

                    else:
                        for entry in entries:
                            await self._semaphore.acquire()
                            timers.start_soon(self._run, entry)

                    now = time.monotonic()
                    if now - last_reclaim < self._policy.reclaim_interval:
                        continue

                    last_reclaim = now
                    await self._broker.reclaim(
                        consumer=self._consumer,
                        min_idle_ms=int(self._policy.task_timeout * 1000),
                        count=100,
                    )

        finally:
            await self.aclose()

    async def _heartbeat(self) -> None:

        if self._presence is None:
            self._presence = SubprocessPresence(
                AsyncRedis.from_url(self._redis_dsn, decode_responses=True),
            )

        while not self._terminate.is_set():
            await self._presence.heartbeat(self._consumer)
            logger.info(f"Worker [{self._consumer}] tiktoking: {time.time()}")  # noqa: G004

            await anyio.sleep(self._policy.heartbeat_interval)

    async def _run(self, entry: "Entry") -> None:

        _, _, fields = entry

        if self._broker is None:
            raise RuntimeError("Broker is not initialized.")

        try:
            msg = Message.from_json(fields.get("message"))
            if (leftime := time.time() - msg.enqueued_at) > self._policy.task_timeout:
                logger.warning("Message %s has expired", fields.get("id"))
                return

            with anyio.fail_after(leftime):
                await self._registry.get(msg.name)(*msg.args, **msg.kwargs)

        except (SerializationError, TaskNotFoundError) as err:
            logger.error(
                "Failed to process message %s: %s",
                fields.get("id"),
                str(err),
            )
            return

        except TimeoutError as err:
            logger.warning("Message %s timed out: %s", fields.get("id"), str(err))
            return

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                await self.aclose()

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
            self._timers = None

    def run(self) -> None:

        anyio.run(self.arun)
