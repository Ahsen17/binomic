import time
from typing import TYPE_CHECKING

import anyio

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

    async def on_dead(self, entry: "MessageEntry") -> None:

        await self._broker.dead(entry)

        # TODO: notify the task owner via an `on_dead` hook on TaskSpec
        # (`@task(..., on_dead=fn)`), carried to workers through the same
        # autodiscover that populates the registry. Fire it here, after the
        # DLQ write succeeds, as `fn(message, reason, exc)` where reason is
        # "timeout" | "expired" | "max_attempts" (exc is None for expired).
        # Hook errors must be logged and swallowed. Resolve the spec
        # best-effort at this point — the over-age path enters before the
        # registry lookup, and a missing spec (TaskNotFoundError) simply
        # skips the hook.

    async def on_success(self, entry: "MessageEntry") -> None:

        await self._broker.ack(entry)

    async def on_retry(self, entry: "MessageEntry") -> None:

        # Task executed failed, retry
        if entry.message.attempt + 1 > self._policy.max_attempts:
            # Task attempted reaches max, send to dlq
            await self.on_dead(entry)

            return

        self._scheduler.delay(
            func=self._broker.retry,
            delay=self._backoff(entry.message.attempt),
            args=(entry,),
        )

    async def handle(self, entry: "MessageEntry") -> None:

        if (time.time() - entry.enqueued_at) > self._policy.task_timeout:
            # Task waiting til expired, send to dlq
            await self.on_dead(entry)

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

        except TimeoutError:
            # Task executed timeout, send to dlq
            await self.on_dead(entry)

        except (SystemExit, KeyboardInterrupt):
            # Exit signals must propagate
            raise

        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise

            await self.on_retry(entry)

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
