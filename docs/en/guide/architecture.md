# Architecture

```{mermaid}
flowchart LR
    P[Producer<br>Binomic client] -->|submit: direct| B[AsyncredisBroker<br>Redis Streams]
    P -->|submit: delay| S[TaskScheduler]
    P -.->|registered at startup: cron / interval| S
    S -->|enqueued when due| B
    B -->|consumer group / PEL| W1[Worker 0<br>subprocess]
    B -->|consumer group / PEL| W2[Worker 1<br>subprocess]
    W1 -.->|failure or timeout:<br>redeliver after a backoff, attempt + 1| B
    M[Master<br>supervisor] --> W1
    M --> W2
    M -->|presence heartbeat| R[(Redis)]
```

## Components

- **Producer**: `Binomic.submit` branches on the task's mode — a `direct` task
  is enqueued immediately through the broker that `BrokerFactory` creates; a
  `delay` task is handed to the scheduler for a deferred enqueue; `cron` and
  `interval` tasks **do not go through** `submit` (calling it on them raises
  `ValueError`) and are instead registered as periodic jobs when the client
  starts. Which stream a message lands in comes from the task declaration
  (`TaskSpec.queue`), not from the caller.
- **Scheduler**: `TaskScheduler` creates the matching trigger for each of the
  `delay` / `cron` / `interval` modes, and enqueues through the same path as
  `direct` once a job is due. Each worker holds a `TaskScheduler` of its **own**
  (unrelated to the client's), which carries the backoff of a failed
  redelivery. See [Task modes and scheduling](scheduling.md).
- **Broker**: `AsyncredisBroker` writes messages to Redis Streams; workers
  read them through a consumer group, and messages in the PEL of a lost
  consumer are reclaimed and redelivered by `reclaim` (`xautoclaim`), which
  bumps `attempt`.
- **Master / Worker**: the master spawns worker subprocesses through
  multiprocessing and supervises their liveness; each worker executes tasks
  concurrently with `anyio` inside its process. A task that succeeds is
  `ack`ed; one that fails or overruns is redelivered **after a backoff** while
  `max_attempts` lasts (the wait is deferred through the worker's own
  scheduler) and dropped after that; a cancelled task is **not** `ack`ed,
  leaving its message to `reclaim`. See [Reliability](reliability.md).
