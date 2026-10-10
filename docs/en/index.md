# Binomic

A lightweight, customizable asynchronous task framework for Python, built on
[AnyIO](https://anyio.readthedocs.io/) and Redis Streams.

Binomic takes tasks declared with decorators, delivers messages through Redis
Streams, executes them in multiprocessing workers, and supervises the whole
fleet with a master process. Message payloads are JSON-serialized with
[msgspec](https://jcristharif.com/msgspec/) and task IDs are UUIDv7.

## Features

- **Declarative tasks**: the `@task()` decorator with `direct` (immediate),
  `delay` (deferred), `cron` (cron expression), and `interval` (fixed interval)
  modes; see [Task modes and scheduling](guide/scheduling.md)
- **Redis Streams broker**: reliable delivery based on consumer groups and the
  PEL, with `xautoclaim` reclaiming messages from lost consumers
- **Retries**: failed or overdue messages are redelivered after a backoff, up to
  `max_attempts` (3 by default); see [Reliability](guide/reliability.md)
- **Multiprocessing workers**: the master spawns worker subprocesses and keeps
  supervising them with presence heartbeats -- carried over **inter-process
  IPC** rather than Redis, so beyond `is_alive()` it also sees a process that is
  up but has stopped beating
- **Type safe**: fully checked with mypy strict and ruff; the task registry is
  built on the generic `TaskSpec[P, T]`
- **Litestar integration**: a built-in `BinomicPlugin` injects the Binomic
  client into Litestar dependencies

Requires Python ≥ 3.12 and Redis ≥ 7.x.

```{toctree}
:hidden:

guide/quickstart
guide/architecture
guide/scheduling
guide/reliability
guide/litestar
api
```
