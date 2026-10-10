# Architecture

```{mermaid}
flowchart TD
    P[Producer<br>Binomic client] -->|submit: direct| B[AsyncredisBroker<br>Redis Streams]
    P -->|submit: delay| S[TaskScheduler]
    P -.->|registered at startup: cron / interval| S
    S -->|enqueued when due| B
    B -->|consumer group / PEL| W1[Worker 0<br>subprocess]
    B -->|consumer group / PEL| W2[Worker 1<br>subprocess]
    W1 -.->|failure or timeout:<br>redeliver after a backoff, attempt + 1| B
    M[Master<br>supervisor] -->|restart when dead| W1
    M -->|restart when dead| W2
    W1 -.->|presence heartbeat: IPC| M
    W2 -.->|presence heartbeat: IPC| M
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
  multiprocessing and supervises their liveness. The heartbeat goes over
  **inter-process IPC** rather than Redis: the master creates one shared
  `multiprocessing.Value` per worker and hands it to the child as a `Worker`
  constructor argument, the child stamps a timestamp into it on every interval,
  and the master reads it directly to judge death. That keeps a process which is
  up but has stopped beating distinguishable from one that is gone -- the case
  `is_alive()` alone cannot see. Each worker executes tasks concurrently with
  `anyio` inside its process. A task that succeeds is `ack`ed; one that fails or
  overruns is redelivered **after a backoff** while `max_attempts` lasts (the
  wait is deferred through the worker's own scheduler) and dropped after that; a
  cancelled task is **not** `ack`ed, leaving its message to `reclaim`. See
  [Reliability](reliability.md).
