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


@task("default")
def example(index: int = 0) -> None:
    print(f"[{index}] Current time: {time.time()}")


@task("default", mode="delay", delay=10)  # runs after a 10-second delay
def delayed() -> None: ...


@task("default", mode="cron", cron="*/5 * * * *")  # runs every 5 minutes
def periodic() -> None: ...


@task("default", mode="interval", interval=30)  # runs every 30 seconds
def poll() -> None: ...
```

The first positional argument is the **queue name** and must match a queue in
the client's `queues` configuration. Tasks are registered under the
**lowercase form of the function name** (`example`, `delayed`, `periodic`, and
`poll` above), which is how you refer to a task when submitting it.

Functions in the two periodic modes, `cron` and `interval`, **take no
arguments** — a periodic firing has no call arguments to pass, so declaring
one raises `ValueError` outright. See
[Task modes and scheduling](scheduling.md) for the full description of all
four modes.

## Start and submit

```python
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
    await binomic.submit(Message(name="example", args=[1]))
```

- `broker_dsn` points at the Redis used for message delivery (Streams);
- `redis_dsn` points at the Redis used for presence heartbeats and
  supervision coordination;
- in `BinomicConfig`, `workers` is the number of worker subprocesses the
  master spawns and `concurrency` is the number of tasks each worker runs
  concurrently;
- a `Message`'s `name` is the task name and `args` / `kwargs` carry the call
  arguments (the example above calls `example` with `index=1`). Which stream a
  message lands in is decided by the **queue in the task declaration** — a
  message neither needs nor can specify a queue;
- `enqueued_at` needs no manual assignment: the client writes it at the moment
  of **actual delivery**, which is also why delay and periodic tasks see the
  current time on every delivery.
