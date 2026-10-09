import time

from binomic.task import task


@task(queue="default", mode="direct")
def example(index: int = 0) -> None:

    print(f"[{index}][DIRECT] Current time: {time.time()}")  # noqa: T201


@task(queue="delay", mode="delay", delay=5)
async def delay_example(index: int = 0) -> None:

    print(f"[{index}][DELAY] Current time: {time.time()}")  # noqa: T201


@task(queue="cron", mode="cron", cron="*/1 * * * *")
async def cron_example() -> None:

    print(f"[None][CRON] Current time: {time.time()}")  # noqa: T201


@task(queue="interval", mode="interval", interval=10.0)
async def interval_example() -> None:

    print(f"[None][INTERVAL] Current time: {time.time()}")  # noqa: T201


@task(queue="fail")
async def fail_example(index: int = 0) -> None:

    raise RuntimeError(f"This task should fail: {index}")
