import contextlib
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Self

import anyio
import structlog
from anyio import AsyncContextManagerMixin

from binomic.broker import (
    BrokerDeadLetterCapability,
    BrokerFactory,
    BrokerLifecycleCapability,
    BrokerTelemetryCapability,
    Message,
    QueueCapacityError,
)
from binomic.client.dlq import DeadLetters
from binomic.otel import otel
from binomic.otel.signatures import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    MESSAGING_SYSTEM_VALUE,
    TASK_NAME,
)
from binomic.task import TaskRegistry, TaskScheduler, autodiscover
from binomic.worker import Master, MasterPolicy, WorkerPolicy

if TYPE_CHECKING:
    from binomic.broker import Broker, MessageEntry

    from .config import BinomicConfig


__all__ = (
    "Binomic",
    "BinomicFactory",
)

logger = structlog.stdlib.get_logger(__name__)


class Binomic(AsyncContextManagerMixin):
    """The client tying task discovery, scheduling and submission together.

    Constructing one discovers the task modules of ``module_name`` into a
    private registry. ``start``/``close`` manage the broker and the scheduler;
    the async context manager additionally assembles the ``Master``, registers
    cron/interval tasks and tears everything down on exit.
    ``dead_letters`` resolves the broker and returns a per-queue operations
    handle over its dead-letter surface (inspect and replay) when the broker
    implements the capability.
    """

    def __init__(
        self,
        broker_dsn: str,
        module_name: str,
        config: "BinomicConfig",
        **broker_kwargs: Any,
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._config = config
        self._broker_kwargs = broker_kwargs

        self._broker: Broker | None = None
        self._scheduler: TaskScheduler | None = None
        self._registry = TaskRegistry()

        self._submissions: Any | None = None
        self._rejections: Any | None = None

        if self._config.otel_enabled and otel is not None:
            self._submissions = otel.create_counter(
                name="binomic.task.submissions",
                unit="{task}",
            )

            self._rejections = otel.create_counter(
                name="binomic.task.rejections",
                unit="{task}",
            )

        # Register tasks from module
        for spec in autodiscover(self._module_name):
            self._registry.register(spec)

    async def _ensure_broker(self) -> "Broker":

        if self._broker is None:
            self._broker = BrokerFactory(
                dsn=self._broker_dsn,
                queues=self._config.queues,
                namespace=f"{self._module_name}",
                capacity=self._config.capacity,
                **self._broker_kwargs,
            ).create()

            if isinstance(self._broker, BrokerLifecycleCapability):
                await self._broker.start()

        return self._broker

    def _ensure_scheduler(self) -> "TaskScheduler":

        if self._scheduler is None:
            self._scheduler = TaskScheduler()
            self._scheduler.start()

        return self._scheduler

    async def start(self) -> None:
        """Start the broker and the scheduler; safe to call repeatedly."""

        await self._ensure_broker()
        self._ensure_scheduler()

        logger.info("Client started", module=self._module_name)

    async def close(self) -> None:
        """Stop the scheduler and close the broker; safe to call repeatedly."""

        closed = False

        if self._broker is not None:
            if isinstance(self._broker, BrokerLifecycleCapability):
                await self._broker.close()
            self._broker = None
            closed = True

        if self._scheduler is not None:
            self._scheduler.close()
            self._scheduler = None
            closed = True

        if closed:
            logger.info("Client closed", module=self._module_name)

    def _span_attributes(self, queue: str, message: "Message") -> dict[str, str]:

        return {
            MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
            MESSAGING_OPERATION_NAME: "create",
            MESSAGING_DESTINATION_NAME: queue,
            MESSAGING_MESSAGE_ID: str(message.id),
            TASK_NAME: message.task,
        }

    async def _deliver(
        self,
        queue: str,
        message: "Message",
        carrier: dict[str, str] | None = None,
    ) -> "MessageEntry | None":
        """Enqueue, handing the trace context only to capable brokers.

        Returns the broker's entry, or ``None`` when the enqueue degraded.
        """

        broker = await self._ensure_broker()

        if carrier and isinstance(broker, BrokerTelemetryCapability):
            return await broker.enqueue(
                queue=queue,
                message=message,
                context=carrier,
            )

        return await broker.enqueue(queue=queue, message=message)

    async def _enqueue(self, queue: str, message: "Message") -> "MessageEntry | None":
        """Enqueue with a producer span; shared by all delivery paths.

        Passes through the broker's entry, or ``None`` on degradation.
        """

        if not self._config.otel_enabled or otel is None:
            return await self._deliver(queue, message)

        with otel.start_producer_span(
            queue,
            self._span_attributes(queue, message),
        ):
            return await self._deliver(queue, message, otel.capture_context())

    def _count_rejection(self, task: str, queue: str) -> None:

        if self._rejections is not None:
            self._rejections.add(
                1,
                attributes={
                    TASK_NAME: task,
                    MESSAGING_DESTINATION_NAME: queue,
                },
            )

    async def _deliver_scheduled(self, queue: str, message: "Message") -> None:
        """Deliver inside a cron/interval job, where no caller channel exists.

        Every round goes through ``_enqueue``, so it produces its own producer
        span and trace context. A capacity rejection degrades to a warning and
        a rejection count; nothing is re-enqueued and no exception escapes
        into the job, because the next round comes back on its own.
        """

        try:
            await self._enqueue(queue, message)

        except QueueCapacityError as err:
            self._count_rejection(message.task, queue)

            logger.warning(
                "Task submission rejected: queue capacity reached",
                task=message.task,
                queue=queue,
                message_id=str(message.id),
                current=err.current,
                capacity=err.capacity,
            )

    async def submit(self, message: "Message") -> "MessageEntry | None":
        """Submit a message for delivery according to its task's mode.

        ``direct`` enqueues immediately and returns the broker's
        ``MessageEntry``. ``None`` means the enqueue degraded internally
        (already logged). A full target queue raises ``QueueCapacityError``.
        ``cron`` and ``interval`` tasks are periodic and reject ``submit`` —
        they are registered when the async context starts.
        """

        spec = self._registry.get(message.task)

        if spec.mode != "direct":
            raise ValueError(
                "Invalid task mode, `submit` only supports direct tasks. "
                f"Received `{spec.mode}` task."
            )

        try:
            resp = await self._enqueue(spec.queue, message)

        except QueueCapacityError:
            self._count_rejection(message.task, spec.queue)

            raise

        logger.debug(
            "Task submitted",
            task=message.task,
            queue=spec.queue,
            message_id=str(message.id),
            mode=spec.mode,
        )

        if self._submissions is not None:
            self._submissions.add(
                1,
                attributes={TASK_NAME: message.task},
            )

        return resp

    async def dead_letters(self, queue: str) -> "DeadLetters":
        """Resolve the broker and return its dead-letter operations handle.

        Client-side resolution: lazily creates and starts the broker, probes
        the DLQ capability, then hands the narrowed broker to a ``DeadLetters``
        handle bound to ``queue`` (inspect, replay, and on-demand pruning;
        the worker's retention sweep is the automatic default).
        """

        broker = await self._ensure_broker()

        if not isinstance(broker, BrokerDeadLetterCapability):
            raise NotImplementedError(
                f"Broker {type(broker).__name__} does not implement "
                "BrokerDeadLetterCapability; dead-letter management is unavailable"
            )

        return DeadLetters(broker, queue)

    async def _register_interval_cron_tasks(self) -> None:

        scheduler = self._ensure_scheduler()
        await self._ensure_broker()

        for spec in self._registry:
            match spec.mode:
                case "interval":
                    if spec.interval is None or spec.interval <= 0:
                        raise RuntimeError(
                            "Interval task must supply `interval` argument "
                            "and ensure it's greater than 0",
                        )

                    scheduler.interval(
                        func=self._deliver_scheduled,
                        interval=spec.interval,
                        args=(spec.queue, Message(task=spec.name)),
                    )

                case "cron":
                    if spec.cron is None or spec.cron == "":
                        raise RuntimeError(
                            "Cron task must supply `cron` argument "
                            "and ensure it's not empty",
                        )

                    scheduler.cron(
                        func=self._deliver_scheduled,
                        cron=spec.cron,
                        args=(spec.queue, Message(task=spec.name)),
                    )

                case _:
                    continue

            logger.info(
                "Task schedule registered",
                task=spec.name,
                mode=spec.mode,
                queue=spec.queue,
            )

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:

        master = Master(
            broker_dsn=self._broker_dsn,
            module_name=self._module_name,
            policy=MasterPolicy(
                workers=self._config.workers,
                worker_policy=WorkerPolicy(
                    queues=self._config.queues,
                    concurrency=self._config.concurrency,
                    max_attempts=self._config.max_attempts,
                    task_timeout=self._config.task_timeout,
                    read_count=self._config.read_count,
                    poll_interval=self._config.poll_interval,
                    heartbeat_interval=self._config.heartbeat_interval,
                    reclaim_interval=self._config.reclaim_interval,
                    dlq_retention=self._config.dlq_retention,
                    worker_init=self._config.worker_init,
                    otel_enabled=self._config.otel_enabled,
                ),
            ),
            method="spawn",
            **self._broker_kwargs,
        )

        try:
            await self._register_interval_cron_tasks()

            async with anyio.create_task_group() as proc:
                proc.start_soon(master.start)
                yield self
                proc.cancel_scope.cancel()

        finally:
            try:
                await master.close()

            finally:
                await self.close()


class BinomicFactory:
    """Binomic working client factory."""

    def __init__(
        self,
        broker_dsn: str,
        module_name: str,
        config: "BinomicConfig",
        **broker_config: Any,
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._config = config
        self._broker_config = broker_config

        self._binomic: Binomic | None = None

    def create(self) -> "Binomic":
        """Return the client, creating it on the first call."""

        if self._binomic is None:
            self._binomic = Binomic(
                broker_dsn=self._broker_dsn,
                module_name=self._module_name,
                config=self._config,
                **self._broker_config,
            )

        return self._binomic
