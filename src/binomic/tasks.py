import time

from binomic.task import task


@task(queue="default", mode="direct")
def example(index: int = 0) -> None:

    print(f"[{index}] Current time: {time.time()}")  # noqa: T201
