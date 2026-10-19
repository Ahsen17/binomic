import inspect
import time
from typing import TYPE_CHECKING

import anyio
import structlog

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

    async def on_dead(
        self,
        entry: "MessageEntry",
        reason: "OnDeadReason",
        exc: BaseException | None = None,
    ) -> None:

        await self._broker.dead(entry)

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

        self._scheduler.delay(
            func=self._broker.retry,
            delay=self._backoff(entry.message.attempt),
            args=(entry,),
        )

    async def handle(self, entry: "MessageEntry") -> None:

        if (time.time() - entry.enqueued_at) > self._policy.task_timeout:
            # Task waiting til expired, send to dlq
            await self.on_dead(entry, "expired")

            return

        try:
            with anyio.fail_after(self._policy.task_timeout):
                await self._registry.get(entry.message.task)(
                    *(entry.message.args or ()),
                    **(entry.message.kwargs or {}),
                )

            # Task executed successfully
            await self.on_success(entry)
            return

        except TaskNotFoundError:
            await self.on_success(entry)

        except TimeoutError as exc:
            # Task executed timeout, send to dlq
            await self.on_dead(entry, "timeout", exc)

        except (SystemExit, KeyboardInterrupt):
            # Exit signals must propagate
            raise

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise

            await self.on_retry(entry, exc)

    async def reclaim(self) -> None:

        await self._broker.reclaim(
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
