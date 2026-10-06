# Quick start

## Installation

```bash
# uv
uv add binomic

# pip
pip install binomic
```

## Declare tasks

Declare tasks in a `tasks.py` module inside your application package (the
worker discovers every `tasks.py` module through `autodiscover`):

```python
# myapp/tasks.py
import time

from binomic.task import task


@task()
def example(index: int = 0) -> None:
    print(f"[{index}] Current time: {time.time()}")


@task(mode="delay", delay=10)  # runs after a 10-second delay
def delayed() -> None: ...


@task(mode="cron", cron="*/5 * * * *")  # runs every 5 minutes
def periodic() -> None: ...
```

## Start and submit

```python
import time

from binomic.client import Binomic, BinomicFactory
from binomic.config import BinomicConfig
from binomic.message import Message
from binomic.task import autodiscover

# Discover and register every tasks.py module inside the myapp package
autodiscover("myapp")

factory = BinomicFactory(
    broker_dsn="redis://localhost:6379/0",
    redis_dsn="redis://localhost:6379/1",
    module_name="myapp",
    config=BinomicConfig(queues=["default"], workers=2, concurrency=5),
)

binomic: Binomic = factory.create()

# Entering the context starts the master (spawning worker subprocesses)
# and exposes the submission entry point
async with binomic:
    await binomic.submit(
        Message(name="example", queue="default", enqueued_at=time.time())
    )
```

- `broker_dsn` points at the Redis used for message delivery (Streams);
- `redis_dsn` points at the Redis used for presence heartbeats and
  supervision coordination;
- in `BinomicConfig`, `workers` is the number of worker subprocesses the
  master spawns and `concurrency` is the number of tasks each worker runs
  concurrently.
