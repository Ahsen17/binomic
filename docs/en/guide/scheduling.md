# Task modes and scheduling

A task's mode is fixed **at declaration time**, and it decides when the task
runs: immediately on submission, once after a deferred wait, or repeatedly on
a schedule. There are four modes — `direct`, `delay`, `cron`, and `interval`.
See [Architecture](architecture.md) for where they sit in the submission path.

## Declaring tasks

```python
from binomic.task import task


@task("default")
def example(index: int = 0) -> None: ...
```

The first positional argument is the **queue name** and must match a queue in
`BinomicConfig`'s `queues`; `mode` is keyword-only and defaults to `"direct"`.
Tasks are registered under the **lowercase form of the function name**, which
is how you refer to a task when submitting it; names are unique within the
registry, so registering a second task under the same name raises
`DuplicateTaskError`.

| Mode | Extra argument | When it runs | Takes arguments |
|-|-|-|-|
| `direct` | none (default) | immediately on submission | yes |
| `delay` | `delay`: seconds to wait | once, the given number of seconds after submission | yes |
| `cron` | `cron`: cron expression string | repeatedly, on the cron expression | **no** |
| `interval` | `interval`: interval in seconds | repeatedly, every given number of seconds | **no** |

```python
# Run once, 10 seconds later
@task("default", mode="delay", delay=10)
def delayed() -> None: ...


# Run every 5 minutes (cron expression: minute/hour/day/month/weekday)
@task("default", mode="cron", cron="*/5 * * * *")
def periodic() -> None: ...


# Run every 30 seconds
@task("default", mode="interval", interval=30)
def poll() -> None: ...
```

Validation happens immediately at declaration time, raising `ValueError` for
anything invalid:

- a missing required argument: the `delay` / `cron` / `interval` modes each
  require their corresponding argument;
- a periodic task that takes arguments: functions in `cron` and `interval`
  mode **must take no arguments** — a periodic firing has no call arguments to
  pass (see the next section for why).

Delay and interval values only have to be **positive**, and that is checked by
the scheduler at **submit / schedule time**, not at declaration time (which
only checks that the arguments are present) — see "Using the scheduler
directly" below.

```{note}
`cron` and `interval` fire according to the **scheduler's timezone**, which
defaults to **UTC**. Automatic registration of periodic tasks by the client
takes no timezone configuration, so `cron="0 9 * * *"` means **9 AM UTC**, not
9 AM local time. To fire on another timezone, register the task yourself with
`TaskScheduler(timezone=...)` as shown below.
```

## Submitting tasks

`Binomic.submit` hands a `Message` to the client, but it **supports only two
modes**:

- `direct`: the message is written to the task's declared queue immediately;
- `delay`: the message is handed to the scheduler and written to the queue
  once the delay elapses.

Calling `submit` on a `cron` or `interval` task raises `ValueError` — periodic
tasks do not accept one-off submissions; their firings are registered
automatically by the client at startup (see the next section). Which stream a
message lands in is decided by the **queue in the task declaration**; `submit`
does not take a caller-specified queue.

A full queue (one at `queue_capacity`) is different: `submit` raises **nothing**.
The `QueueCapacityLimitError` is swallowed inside the client and logged, the
message is dropped without a retry, and `submit` still returns the message ID.
See [Reliability](reliability.md).

```python
from binomic.message import Message

# `binomic` is built as shown in the quick start
async with binomic:
    await binomic.submit(Message(name="example", args=[1]))
```

## Registering periodic tasks

As the client enters its context (`async with binomic`), it walks every `cron`
/ `interval` task in the registry and registers one periodic job per task:
each firing enqueues a message to that task's queue. This also explains the
constraint from the previous section — a periodic firing's message carries no
call arguments, so a periodic task must be a function that takes none.

## Using the scheduler directly

`TaskScheduler` is public API and can be used independently of the client.

```python
import asyncio

from binomic.task import TaskScheduler, task


@task("default", mode="delay", delay=5)
def reminder_delay() -> None: ...


def send_reminder(user_id: str) -> None:
    print(f"reminder for {user_id}")


async def main() -> None:
    scheduler = TaskScheduler()
    scheduler.start()  # requires a running event loop
    scheduler.delay(func=send_reminder, spec=reminder_delay, args=("u-1",))

    await asyncio.sleep(6)  # wait for the job to come due
    scheduler.shutdown()


asyncio.run(main())
```

Here `reminder_delay` only supplies the timing parameter ("delay 5 seconds") and
the validation; the callable actually invoked is `send_reminder`, passed as
`func`.

- **Requires a running event loop**: `TaskScheduler` is built on APScheduler's
  `AsyncIOScheduler`, which captures the current event loop in place when
  `start()` is called — calling `start()` outside an event loop raises
  `RuntimeError: no running event loop`. The three methods are themselves
  synchronous, but they presuppose a running event loop (the client path
  satisfies this naturally, since it starts the scheduler inside its own loop).
- **Construction**: `TaskScheduler(timezone=UTC)`, with UTC as the default
  timezone; the triggers created by all three methods use that timezone.
- **Where `spec` comes from**: the return value of the `@task(...)` decorator
  is a `TaskSpec`; it supplies the timing parameters (`delay` / `cron` /
  `interval`) and participates in validation.
- **Methods**: `delay(func, spec, *, args=None, kwargs=None)`, `interval(...)`,
  and `cron(...)` share one signature, and all three are **synchronous
  methods**; `func` is the callable to invoke when a job is due, and `args` /
  `kwargs` are passed through to it unchanged.
- **Validation**: a `ValueError` is raised if `spec`'s mode does not match the
  method called, or if the corresponding value is missing (for example
  ``Task is not a delay task or lack `delay` value.``). `delay` and `interval`
  also raise `ValueError` when the value is `<= 0`
  (``... must be greater than 0.``); `cron` has no positivity check — a cron
  expression has no notion of "non-positive".
- **Lifecycle**: jobs only start firing after `start()` is called, and
  `shutdown()` stops them. Both are synchronous methods. Note that calling
  `shutdown()` immediately after scheduling leaves no time for the job to fire
  (the example above waits with `await asyncio.sleep(6)`).
