import inspect
import time
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from binomic.broker import BrokerTelemetryCapability
from binomic.otel import otel
from binomic.otel.signatures import (
    DEAD_REASON,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    MESSAGING_SYSTEM_VALUE,
    TASK_ATTEMPT,
    TASK_NAME,
)
from binomic.task import (
    TaskNotFoundError,
    TaskRegistry,
    TaskScheduler,
    autodiscover,
)

if TYPE_CHECKING:
    from binomic.broker import Broker, MessageEntry

    from .policies import WorkerPolicy


__all__ = ("WorkerHandler",)


logger = structlog.stdlib.get_logger(__name__)


class WorkerHandler:
    """Execute one acquired entry and own the retry/backoff decisions.

    A handler autodiscovers ``module_name`` into a private registry, so the
    task functions run from the worker's own imports. ``handle`` enforces the
    delivery deadline, dispatches the task, then acks, dead-letters or
    schedules a retry according to the outcome.
    """

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

        if worker_policy.otel_enabled and otel is not None:
            self._execution_duration = otel.create_histogram(
                name="binomic.task.execution.duration",
                unit="s",
            )
            self._delivery_lag = otel.create_histogram(
                name="binomic.task.delivery.lag",
                unit="s",
            )
            self._executions = otel.create_counter(
                name="binomic.task.executions",
                unit="{task}",
            )
            self._acks = otel.create_counter(
                name="binomic.task.acks",
                unit="{task}",
            )
            self._retries = otel.create_counter(
                name="binomic.task.retries",
                unit="{task}",
            )
            self._dead_letters = otel.create_counter(
                name="binomic.task.dead_letters",
                unit="{task}",
            )
            self._reclaims = otel.create_counter(
                name="binomic.task.reclaims",
                unit="{task}",
            )

        self._registry = TaskRegistry()

        for spec in autodiscover(self._module_name):
            self._registry.register(spec)

    def start(self) -> None:
        """Start the handler's own scheduler, which carries retry backoff."""

        self._scheduler.start()

    def close(self) -> None:
        """Stop the scheduler; pending retry jobs are dropped with it."""

        self._scheduler.close()

    @staticmethod
    def backoff(attempt: int) -> float:
        """Return the delay before the next delivery of ``attempt``.

        The curve starts at 1.5 seconds for the second attempt and doubles
        up to a 30-second ceiling.
        """

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

    async def handle(self, entry: "MessageEntry") -> None:
        """Process one entry: execute, then ack, dead-letter or retry.

        Entries older than ``task_timeout`` go straight to the DLQ without
        running. Execution gets the remaining part of that deadline; a
        timeout dead-letters without retrying, an unknown task is acked, and
        cancellation or process exit propagates without an ack so reclaim can
        recover the message. Other failures schedule a backoff retry, or
        dead-letter once ``max_attempts`` is exhausted.
        """

        age = time.time() - entry.enqueued_at

        if age > self._policy.task_timeout:
            logger.warning(
                "Task expired",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=entry.message.id,
                entry_id=entry.id,
                age=age,
            )

            await self._on_dead(entry, "expired")

            return

        if not self._policy.otel_enabled:
            await self._invoke(
                entry=entry,
                time_margin=self._policy.task_timeout - age,
            )

            return

        carrier: dict[str, str] | None = None

        if isinstance(self._broker, BrokerTelemetryCapability):
            try:
                carrier = await self._broker.trace_context(
                    queue=entry.queue,
                    message_id=str(entry.message.id),
                )

            except Exception:
                logger.debug(
                    "Trace context read failed",
                    task=entry.message.task,
                    message_id=entry.message.id,
                    entry_id=entry.id,
                    exc_info=True,
                )

        if otel is not None:
            with otel.start_process_span(
                task=entry.message.task,
                carrier=carrier,
                attributes=self._span_attributes(entry),
            ):
                self._note_execution(entry)
                await self._invoke(
                    entry=entry,
                    time_margin=self._policy.task_timeout - age,
                )

    async def reclaim(self) -> int:
        """Re-deliver entries whose consumer went silent.

        Entries idle past the task deadline (10-second floor) are claimed and
        requeued; consumer metadata is swept once it has been idle for ten
        reclaim intervals. Returns how many entries were reclaimed.
        """

        count = await self._broker.reclaim(
            consumer=self._consumer,
            min_idle_ms=max(int(self._policy.task_timeout * 1000), 10_000),
            count=100,
            gc_idle_ms=max(
                int(self._policy.task_timeout * 1000),
                10_000,
                int(self._policy.reclaim_interval * 1000) * 10,
            ),
        )

        if count:
            logger.debug("Messages reclaimed", count=count)

        if self._reclaims is not None:
            self._reclaims.add(count)

        return count

    async def _invoke(self, entry: "MessageEntry", time_margin: float) -> None:

        attributes = {TASK_NAME: entry.message.task}
        started = 0.0

        try:
            with anyio.fail_after(time_margin):
                spec = self._registry.get(entry.message.task)
                started = time.perf_counter()

                try:
                    await spec(
                        *(entry.message.args or ()),
                        **(entry.message.kwargs or {}),
                    )

                finally:
                    if (duration := self._execution_duration) is not None:
                        duration.record(
                            time.perf_counter() - started,
                            attributes=attributes,
                        )

            await self._on_success(entry)

        except TaskNotFoundError:
            logger.warning(
                "Task not found",
                task=entry.message.task,
                message_id=entry.message.id,
                entry_id=entry.id,
            )

            await self._on_success(entry)

        except TimeoutError as err:
            logger.warning(
                "Task timeout",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=entry.message.id,
                entry_id=entry.id,
            )

            if self._policy.otel_enabled and otel is not None:
                otel.mark_span_error(err)

            await self._on_dead(entry, "timeout", err)

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise

            logger.warning(
                "Task execution failed",
                task=entry.message.task,
                attempt=entry.message.attempt,
                message_id=entry.message.id,
                entry_id=entry.id,
                exc_info=True,
            )

            if self._policy.otel_enabled and otel is not None:
                otel.mark_span_error(exc)

            await self._on_retry(entry, exc)

    async def _on_success(self, entry: "MessageEntry") -> None:

        await self._broker.ack(entry)

        if self._acks is not None:
            self._acks.add(
                1,
                attributes={TASK_NAME: entry.message.task},
            )

    async def _on_dead(
        self,
        entry: "MessageEntry",
        reason: str,
        exc: BaseException | None = None,
    ) -> None:

        await self._broker.dead(entry)

        logger.warning(
            "Task moved to DLQ",
            task=entry.message.task,
            reason=reason,
            attempt=entry.message.attempt,
            message_id=entry.message.id,
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

        try:
            hook = self._registry.get(entry.message.task).on_dead_hook

        except TaskNotFoundError:
            return

        if hook is None:
            return

        try:
            res = hook(entry, reason, exc)
            if inspect.isawaitable(res):
                await res

        except Exception:
            logger.warning(
                "on_dead hook failed",
                task=entry.message.task,
                exc_info=True,
            )

    async def _on_retry(
        self,
        entry: "MessageEntry",
        exc: BaseException | None = None,
    ) -> None:

        if entry.message.attempt + 1 > self._policy.max_attempts:
            await self._on_dead(entry, "max_attempts", exc)

            return

        self._scheduler.delay(
            func=self._broker.retry,
            delay=self.backoff(entry.message.attempt),
            args=(entry,),
        )

        if self._retries is not None:
            self._retries.add(
                1,
                attributes={TASK_NAME: entry.message.task},
            )
