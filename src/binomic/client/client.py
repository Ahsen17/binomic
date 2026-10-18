import contextlib
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Self

import anyio
from anyio import AsyncContextManagerMixin

from binomic.broker import (
    BrokerFactory,
    BrokerLifecycleMixin,
    Message,
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

        # Register tasks from module
        for spec in autodiscover(self._module_name):
            self._registry.register(spec)

    async def _ensure_broker(self) -> "Broker":

        if self._broker is None:
            self._broker = BrokerFactory(
                dsn=self._broker_dsn,
                queues=self._config.queues,
                namespace=f"{self._module_name}",
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

    async def close(self) -> None:

        if self._broker is not None:
            if isinstance(self._broker, BrokerLifecycleMixin):
                await self._broker.close()
            self._broker = None

        if self._scheduler is not None:
            self._scheduler.close()
            self._scheduler = None

    async def submit(self, message: "Message") -> None:

        spec = self._registry.get(message.task)

        match spec.mode:
            case "direct":
                # TODO: enqueue failure is silent — `enqueue` degrades to None
                # on script-level errors and the result goes unchecked here;
                # the delay/cron/interval paths enqueue inside APScheduler
                # jobs with no channel back to the caller. Plan: a shared
                # `_checked_enqueue` (raise on None) for all enqueue-fed
                # paths, a submit docstring contract (delay success only
                # means scheduled), and optionally a bounded in-job retry.
                await (await self._ensure_broker()).enqueue(
                    queue=spec.queue,
                    message=message,
                )

            case "delay":
                if spec.delay is None or spec.delay <= 0:
                    raise ValueError(
                        "Delay task must suply `delay` argument "
                        "and ensure it's greater than 0",
                    )

                self._ensure_scheduler().delay(
                    func=(await self._ensure_broker()).enqueue,
                    delay=spec.delay,
                    args=(
                        spec.queue,
                        message,
                    ),
                )

            case _:
                raise ValueError(
                    "Invalid task mode, `submit` only supports "
                    f"direct and delay tasks. Received `{spec.mode}` task."
                )

    async def _register_interval_cron_tasks(self) -> None:

        scheduler = self._ensure_scheduler()
        broker = await self._ensure_broker()

        for spec in self._registry:
            match spec.mode:
                case "interval":
                    if spec.interval is None or spec.interval <= 0:
                        raise RuntimeError(
                            "Interval task must supply `interval` argument "
                            "and ensure it's greater than 0",
                        )

                    scheduler.interval(
                        func=broker.enqueue,
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
                        func=broker.enqueue,
                        cron=spec.cron,
                        args=(spec.queue, Message(task=spec.name)),
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
