import contextlib
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Self

import anyio
import structlog
from anyio import AsyncContextManagerMixin

from binomic.broker import (
    BrokerFactory,
    BrokerLifecycleMixin,
    BrokerTelemetryMixin,
    Message,
    QueueCapacityError,
)
from binomic.otel import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    MESSAGING_SYSTEM_VALUE,
    TASK_NAME,
    capture_context,
    create_counter,
    ensure_available,
    start_producer_span,
)
from binomic.task import (
    TaskRegistry,
    TaskScheduler,
    autodiscover,
)
from binomic.worker import (
    Master,
    MasterPolicy,
    WorkerPolicy,
)

if TYPE_CHECKING:
    from binomic.broker import Broker

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

        if self._config.otel_enabled:
            ensure_available()

            self._submissions = create_counter(
                name="binomic.task.submissions",
                unit="{task}",
            )

            self._rejections = create_counter(
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

            if isinstance(self._broker, BrokerLifecycleMixin):
                await self._broker.start()

        return self._broker

    def _ensure_scheduler(self) -> "TaskScheduler":

        if self._scheduler is None:
            self._scheduler = TaskScheduler()
            self._scheduler.start()

        return self._scheduler

    async def start(self) -> None:

        await self._ensure_broker()
        self._ensure_scheduler()

        logger.info("Client started", module=self._module_name)

    async def close(self) -> None:

        closed = False

        if self._broker is not None:
            if isinstance(self._broker, BrokerLifecycleMixin):
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

    def _capture(self, queue: str, message: "Message") -> dict[str, str]:
        """Produce the creation span and capture its context for delivery."""

        with start_producer_span(
            queue,
            self._span_attributes(queue, message),
        ):
            return capture_context()

    async def _deliver(
        self,
        queue: str,
        message: "Message",
        carrier: dict[str, str] | None = None,
    ) -> None:
        """Enqueue, handing the trace context only to capable brokers."""

        broker = await self._ensure_broker()

        if carrier and isinstance(broker, BrokerTelemetryMixin):
            await broker.enqueue(
                queue=queue,
                message=message,
                context=carrier,
            )
            return

        await broker.enqueue(queue=queue, message=message)

    async def _enqueue(self, queue: str, message: "Message") -> None:
        """Enqueue with a producer span; shared by all delivery paths."""

        if not self._config.otel_enabled:
            await self._deliver(queue, message)
            return

        with start_producer_span(
            queue,
            self._span_attributes(queue, message),
        ):
            await self._deliver(queue, message, capture_context())

    def _count_rejection(self, task: str, queue: str) -> None:

        if self._rejections is not None:
            self._rejections.add(
                1,
                attributes={
                    TASK_NAME: task,
                    MESSAGING_DESTINATION_NAME: queue,
                },
            )

    async def _deliver_scheduled(
        self,
        queue: str,
        message: "Message",
        carrier: dict[str, str] | None = None,
    ) -> None:
        """Deliver inside a scheduler job, where no caller channel exists.

        A carrier means the producer span and trace context were already
        captured at ``submit`` time (the delay path); without one — the
        cron/interval registrations — the delivery goes through ``_enqueue``
        so each round produces its own span and context, as it always did.
        A capacity rejection there degrades to a warning and a rejection
        count: delay success only ever meant *scheduled* (and the in-memory
        job is non-durable), while cron/interval rounds come back on their
        own. Nothing is re-enqueued and no exception escapes into the job.
        """

        try:
            if carrier:
                await self._deliver(queue, message, carrier)

            else:
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

    async def submit(self, message: "Message") -> None:
        """Submit a message for delivery according to its task's mode.

        ``direct`` enqueues immediately and raises ``QueueCapacityError``
        when the target queue is full; internal enqueue failures keep
        degrading silently. ``delay`` schedules the enqueue on the client
        process's in-memory scheduler, so success only means *scheduled* and
        the job is lost if the process exits before the delay elapses; a
        queue that is full when the delay fires logs and counts the
        rejection and drops that delivery. ``cron`` and ``interval`` tasks
        are periodic and reject ``submit``.
        """

        spec = self._registry.get(message.task)

        match spec.mode:
            case "direct":
                # TODO: internal enqueue failures are still silent —
                # `enqueue` degrades to None on script-level errors and the
                # result goes unchecked here. Capacity rejections are no
                # longer silent: they raise out of `enqueue` (counted and
                # re-raised below). Plan: a shared `_checked_enqueue` (raise
                # on None) for all enqueue-fed paths, and optionally a
                # bounded in-job retry for the scheduled paths.
                try:
                    await self._enqueue(spec.queue, message)

                except QueueCapacityError:
                    self._count_rejection(message.task, spec.queue)
                    raise

            case "delay":
                if spec.delay is None or spec.delay <= 0:
                    raise ValueError(
                        "Delay task must suply `delay` argument "
                        "and ensure it's greater than 0",
                    )

                carrier: dict[str, str] = {}

                if self._config.otel_enabled:
                    carrier = self._capture(spec.queue, message)

                self._ensure_scheduler().delay(
                    func=self._deliver_scheduled,
                    delay=spec.delay,
                    args=(
                        spec.queue,
                        message,
                        carrier or None,
                    ),
                )

            case _:
                raise ValueError(
                    "Invalid task mode, `submit` only supports "
                    f"direct and delay tasks. Received `{spec.mode}` task."
                )

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
        **broker_kwargs: Any,
    ) -> None:

        self._broker_dsn = broker_dsn
        self._module_name = module_name
        self._config = config
        self._broker_kwargs = broker_kwargs

        self._binomic: Binomic | None = None

    def create(self) -> "Binomic":
        """Return the client, creating it on the first call."""

        if self._binomic is None:
            self._binomic = Binomic(
                broker_dsn=self._broker_dsn,
                module_name=self._module_name,
                config=self._config,
                **self._broker_kwargs,
            )

        return self._binomic
