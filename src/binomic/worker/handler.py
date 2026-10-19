import inspect
import time
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from binomic.broker import BrokerTelemetryMixin
from binomic.otel import (
    DEAD_REASON,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    MESSAGING_SYSTEM_VALUE,
    TASK_ATTEMPT,
    TASK_NAME,
    create_counter,
    create_histogram,
    ensure_available,
    mark_span_error,
    start_process_span,
)
from binomic.task import (
    TaskNotFoundError,
    TaskRegistry,
    TaskScheduler,
    autodiscover,
)

if TYPE_CHECKING:
    from binomic.broker import Broker, MessageEntry
    from binomic.task import OnDeadReason

    from .policies import WorkerPolicy


__all__ = ("WorkerHandler",)

logger = structlog.stdlib.get_logger(__name__)


class WorkerHandler:
    def __init__(
        self,
        module_name: str,
        consumer: str,
        broker: "Broker",
        worker_policy: "WorkerPolicy",
    ) -> None:

        self._module_name = module_name
        self._broker = broker
        self._consumer = consumer
        self._scheduler = TaskScheduler()
        self._policy = worker_policy

        self._execution_duration: Any | None = None
        self._delivery_lag: Any | None = None
        self._executions: Any | None = None
        self._acks: Any | None = None
        self._retries: Any | None = None
        self._dead_letters: Any | None = None
        self._reclaims: Any | None = None

        if worker_policy.otel_enabled:
            ensure_available()

            self._execution_duration = create_histogram(
                name="binomic.task.execution.duration",
                unit="s",
            )
            self._delivery_lag = create_histogram(
                name="binomic.task.delivery.lag",
                unit="s",
            )
            self._executions = create_counter(
                name="binomic.task.executions",
                unit="{task}",
            )
            self._acks = create_counter(
                name="binomic.task.acks",
                unit="{task}",
            )
            self._retries = create_counter(
                name="binomic.task.retries",
                unit="{task}",
            )
            self._dead_letters = create_counter(
                name="binomic.task.dead_letters",
                unit="{task}",
            )
            self._reclaims = create_counter(
                name="binomic.task.reclaims",
                unit="{task}",
            )

        self._registry = TaskRegistry()

        # Register tasks from module
        for spec in autodiscover(self._module_name):
            self._registry.register(spec)

    def start(self) -> None:

        self._scheduler.start()

    def close(self) -> None:

        self._scheduler.close()

    def _backoff(self, attempt: int) -> float:

        return float(min(1.5 * (2 ** (max(attempt, 1) - 1)), 30.0))

    def _span_attributes(self, entry: "MessageEntry") -> dict[str, str | int]:

        return {
            MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
            MESSAGING_OPERATION_NAME: "process",
            MESSAGING_DESTINATION_NAME: entry.queue,
            MESSAGING_MESSAGE_ID: str(entry.message.id),
            TASK_NAME: entry.message.task,
            TASK_ATTEMPT: entry.message.attempt,
        }

    def _note_execution(self, entry: "MessageEntry") -> None:

        attributes = {TASK_NAME: entry.message.task}

        if self._executions is not None:
            self._executions.add(1, attributes=attributes)

        if self._delivery_lag is not None:
            self._delivery_lag.record(
                max(0.0, time.time() - entry.enqueued_at),
                attributes=attributes,
            )

    async def on_dead(
        self,
        entry: "MessageEntry",
        reason: "OnDeadReason",
        exc: BaseException | None = None,
    ) -> None:

        await self._broker.dead(entry)

        logger.warning(
            "Task moved to DLQ",
            task=entry.message.task,
            reason=reason,
            attempt=entry.message.attempt,
            message_id=str(entry.message.id),
            entry_id=entry.id,
        )

        if self._dead_letters is not None:
            self._dead_letters.add(
                1,
                attributes={
                    TASK_NAME: entry.message.task,
                    DEAD_REASON: reason,
                },
            )

        # Spec resolution is best-effort: the over-age path enters before
        # the registry lookup, so a missing spec simply skips the hook.
        try:
            hook = self._registry.get(entry.message.task).on_dead
        except TaskNotFoundError:
            return

        if hook is None:
            return

        try:
            res = hook(entry, reason, exc)
            if inspect.isawaitable(res):
                await res

        except Exception:
            # A notification must never break the consume loop.
            logger.warning(
                "on_dead hook failed",
                task=entry.message.task,
                exc_info=True,
            )

    async def on_success(self, entry: "MessageEntry") -> None:

        await self._broker.ack(entry)

        if self._acks is not None:
            self._acks.add(
                1,
                attributes={TASK_NAME: entry.message.task},
            )

    async def on_retry(
        self,
        entry: "MessageEntry",
        exc: BaseException | None = None,
    ) -> None:

        # Task executed failed, retry
        if entry.message.attempt + 1 > self._policy.max_attempts:
            # Task attempted reaches max, send to dlq
            await self.on_dead(entry, "max_attempts", exc)

            return

        delay = self._backoff(entry.message.attempt)

        logger.debug(
            "Task retry scheduled",
            task=entry.message.task,
            attempt=entry.message.attempt,
            message_id=str(entry.message.id),
            entry_id=entry.id,
            delay=delay,
        )

        self._scheduler.delay(
            func=self._broker.retry,
            delay=delay,
            args=(entry,),
        )

        if self._retries is not None:
            self._retries.add(
                1,
                attributes={TASK_NAME: entry.message.task},
            )

    async def _execute(self, entry: "MessageEntry") -> None:

        logger.debug(
            "Task execution started",
            task=entry.message.task,
            attempt=entry.message.attempt,
            message_id=str(entry.message.id),
            entry_id=entry.id,
        )

        attributes = {TASK_NAME: entry.message.task}
        started = 0.0

        try:
            with anyio.fail_after(self._policy.task_timeout):
                func = self._registry.get(entry.message.task)
                started = time.perf_counter()

                try:
                    await func(
                        *(entry.message.args or ()),
                        **(entry.message.kwargs or {}),
                    )

                finally:
                    if (duration := self._execution_duration) is not None:
                        duration.record(
                            time.perf_counter() - started,
                            attributes=attributes,
                        )

            # Task executed successfully
            logger.debug(
                "Task executed",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=str(entry.message.id),
                entry_id=entry.id,
                duration=time.perf_counter() - started,
            )

            await self.on_success(entry)
            return

        except TaskNotFoundError:
            logger.warning(
                "Task not found",
                task=entry.message.task,
                message_id=str(entry.message.id),
                entry_id=entry.id,
            )

            await self.on_success(entry)

        except TimeoutError as exc:
            # Task executed timeout, send to dlq
            logger.warning(
                "Task timed out",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=str(entry.message.id),
                entry_id=entry.id,
            )

            if self._policy.otel_enabled:
                mark_span_error(exc)

            await self.on_dead(entry, "timeout", exc)

        except (SystemExit, KeyboardInterrupt):
            # Exit signals must propagate
            raise

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise

            logger.warning(
                "Task execution failed",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=str(entry.message.id),
                entry_id=entry.id,
                exc_info=True,
            )

            if self._policy.otel_enabled:
                mark_span_error(exc)

            await self.on_retry(entry, exc)

    async def handle(self, entry: "MessageEntry") -> None:

        age = time.time() - entry.enqueued_at

        if age > self._policy.task_timeout:
            # Task waiting til expired, send to dlq
            logger.warning(
                "Task expired",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=str(entry.message.id),
                entry_id=entry.id,
                age=age,
            )

            await self.on_dead(entry, "expired")

            return

        if not self._policy.otel_enabled:
            await self._execute(entry)
            return

        carrier: dict[str, str] | None = None

        if isinstance(self._broker, BrokerTelemetryMixin):
            try:
                carrier = await self._broker.trace_context(
                    entry.queue,
                    str(entry.message.id),
                )

            except Exception:
                logger.debug(
                    "Trace context read failed",
                    task=entry.message.task,
                    message_id=str(entry.message.id),
                    entry_id=entry.id,
                    exc_info=True,
                )

        with start_process_span(
            entry.message.task,
            carrier,
            self._span_attributes(entry),
        ):
            self._note_execution(entry)
            await self._execute(entry)

    async def reclaim(self) -> None:

        count = await self._broker.reclaim(
            consumer=self._consumer,
            min_idle_ms=max(
                int(self._policy.task_timeout * 1000),
                10000,
            ),
            # Consumer metadata hygiene: corpses (empty-PEL consumers left
            # by dead instances) are swept once they sit idle past this.
            gc_idle_ms=max(
                int(self._policy.task_timeout * 1000),
                10000,
                int(self._policy.reclaim_interval * 1000) * 10,
            ),
            count=100,
        )

        if count:
            logger.debug("Messages reclaimed", count=count)

        if self._reclaims is not None:
            self._reclaims.add(count)
