import time
from multiprocessing.context import SpawnProcess
from typing import TYPE_CHECKING

import anyio
import structlog

from binomic.base import DeserializationError
from binomic.broker import BrokerFactory, Entry
from binomic.message import Message
from binomic.task import TaskNotFoundError, autodiscover
from binomic.task.registry import registry
from binomic.task.scheduler import TaskScheduler

from .presence import SubprocessPresence

if TYPE_CHECKING:
    from anyio.abc import TaskGroup

    from binomic.broker import Broker

    from .schemas import WorkerPolicy


__all__ = ("Worker",)


logger = structlog.stdlib.get_logger(__name__)


class Worker(SpawnProcess):
    """Binomic subprocessing worker.

    Spawned rather than forked: a forked child inherits the parent's running
    event loop, so ``anyio.run`` cannot start in it.
    """

    def __init__(
        self,
        *,
        broker_dsn: str,
        module_name: str,
        consumer: str,
        policy: "WorkerPolicy",
        presence: SubprocessPresence,
    ) -> None:

        super().__init__(name=consumer)

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._consumer = consumer
        self._policy = policy

        self._broker: Broker | None = None

        # Owned by the master, which frees it: the worker only writes to it.
        self._presence = presence
        self._timers: TaskGroup | None = None

        # A spawned child only receives picklable data, so the AnyIO primitives
        # are created in `arun` instead of here.
        self._semaphore: anyio.Semaphore | None = None
        self._scheduler: TaskScheduler | None = None
        self._terminate: anyio.Event | None = None

    async def arun(self) -> None:

        if self._broker is None:
            self._broker = BrokerFactory(
                dsn=self._broker_dsn,
                queues=self._policy.queues,
            ).create()

            await self._broker.initialize()

        if self._scheduler is None:
            self._scheduler = TaskScheduler()
            self._scheduler.start()

        # Task discovery happens in the process that executes the tasks.
        autodiscover(self._module_name)

        semaphore = anyio.Semaphore(self._policy.concurrency)
        terminate = anyio.Event()
        self._semaphore = semaphore
        self._terminate = terminate

        try:
            async with anyio.create_task_group() as timers:
                self._timers = timers
                timers.start_soon(self._heartbeat, terminate)

                last_reclaim = time.monotonic()

                while not terminate.is_set():
                    entries = await self._broker.acquire(
                        consumer=self._consumer,
                        count=self._policy.read_count,
                    )

                    if not entries:
                        await anyio.sleep(self._policy.poll_interval)

                    else:
                        for entry in entries:
                            await semaphore.acquire()
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

    async def _heartbeat(self, terminate: "anyio.Event") -> None:

        while not terminate.is_set():
            self._presence.heartbeat()
            logger.info(f"Worker [{self._consumer}] tiktoking: {time.time()}")  # noqa: G004

            await anyio.sleep(self._policy.heartbeat_interval)

    def _backoff(self, attempt: int) -> float:

        return float(min(1.5 * (2 ** (attempt - 1)), 30.0))

    async def _run(self, entry: "Entry") -> None:

        queue, _, fields = entry

        # Both are assembled by `arun`. Without the scheduler a redelivery would
        # be dropped silently -- the entry is acked as soon as the retry is
        # scheduled -- so failing loudly beats losing the message.
        if self._broker is None or self._scheduler is None:
            raise RuntimeError("Worker is not initialized.")

        cancelled = False

        try:
            msg = Message.from_json(fields["message"])

            if (
                elapsed := time.time() - fields["enqueued_at"]
            ) > self._policy.task_timeout:
                logger.warning("Message %s has expired.", fields["id"])

            else:
                try:
                    with anyio.fail_after(self._policy.task_timeout - elapsed):
                        await registry.get(msg.name)(*msg.args, **msg.kwargs)

                        return

                except BaseException as err:
                    if isinstance(
                        err,
                        (anyio.get_cancelled_exc_class(), TaskNotFoundError),
                    ):
                        # Cancellation keeps the entry pending and an unregistered
                        # task is not worth retrying: the outer handlers own both.
                        raise

                    logger.error(
                        "Failed to process message %s: %s",
                        fields["id"],
                        str(err),
                    )

            # The invocation did not succeed: retry it until the budget is spent.
            backoff = self._backoff(msg.attempt)
            msg.attempt += 1
            if msg.attempt > self._policy.max_attempts:
                logger.warning(
                    "Failed to process message %s: max attempts reached.",
                    fields["id"],
                )

                # TODO: send to DLQ
                return

            self._scheduler.delay(
                func=self._broker.enqueue,
                spec=registry.get(msg.name),
                delay=backoff,
                args=(queue, msg),
            )

        except TaskNotFoundError as err:
            logger.error(
                "Failed to process message %s: %s",
                fields["id"],
                str(err),
            )

        except DeserializationError as err:
            logger.error(
                "Failed to deserialize message %s: %s",
                fields["id"],
                str(err),
            )

            # TODO: send to DLQ

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                # A cancelled run keeps the message pending for the reclaim pass.
                cancelled = True
                raise

            logger.error(
                "Failed to process message %s: %s",
                fields["id"],
                str(exc),
            )

            # TODO: send to DLQ

        finally:
            if not cancelled:
                await self._broker.ack(entry)

            if self._semaphore is not None:
                self._semaphore.release()

    async def aclose(self) -> None:

        if self._terminate is not None and not self._terminate.is_set():
            self._terminate.set()

        if self._timers and not self._timers.cancel_scope.cancel_called:
            self._timers.cancel_scope.cancel()
            self._timers = None

        if self._scheduler is not None:
            self._scheduler.shutdown()
            self._scheduler = None

    def run(self) -> None:

        anyio.run(self.arun)
