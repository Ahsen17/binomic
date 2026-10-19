# Reliability

A message that has been sent out can fail, overrun its timeout, or go
stranded. This page covers how Binomic handles each case: **retrying a
failure**, **timeouts and staleness**, and the **dead letter queue (DLQ)**.
See [Architecture](architecture.md) for where they sit in the pipeline.

## Attempts and the deadline

`Message` carries an `attempt` field (default `1`) that is serialized with
the payload, so it travels with the message across processes and across
redeliveries. `task_timeout` is a message's **total delivery deadline**: it
starts at the first enqueue and redelivery does not reset it. Together they
decide every message's fate.

- the client's first delivery: `attempt = 1`;
- a worker whose task fails: the broker-side Lua script bumps `attempt`, and
  the message is redelivered after a backoff;
- `reclaim` picking up a message stranded by a dead worker: `attempt` is
  bumped and the stamp **renewed** (see below).

## Retrying a failure

When a task raises, the worker:

1. logs the failure;
2. if `attempt + 1 > max_attempts` (default `3`), the message goes to the
   **dead letter queue** and the flow ends;
3. otherwise the worker's own scheduler defers an atomic broker re-enqueue by
   a **backoff** — a single Lua call that writes the copy and acks the
   original together. The copy's `attempt` is already bumped, while its
   `enqueued_at` **keeps the original value** (the deadline accumulates
   across retries, see below).

```{note}
Redelivery is **not** immediate: the backoff starts at 1.5 seconds, doubles
each time (3 / 6 / 12 / 24 seconds) and caps at **30 seconds**. The wait is
held by a scheduler the worker owns, and it is scheduled to run even when the
loop reaches it late — a busy event loop never silently skips a retry. The
backoff is currently fixed; if you need a different rhythm, wait inside the
task function itself.
```

## Timeout and staleness (straight to the DLQ)

`task_timeout` bites in two places, both meaning "total delivery deadline":

- **execution timeout**: a task that runs longer than `task_timeout` is
  cancelled — the message goes **straight to the DLQ, with no retry**;
- **staleness**: a message whose `enqueued_at` is older than `task_timeout`
  (it queued too long) is not executed and goes straight to the DLQ.

A failure retry does **not** reset the deadline: the copy keeps the original
`enqueued_at`, so a message whose "queueing + execution + backoff" total
exceeds `task_timeout` is judged stale and DLQ'd on its next delivery. The
one exception is `reclaim` (below).

## The dead letter queue (DLQ)

Three kinds of message end up in the DLQ: execution timeouts, stale
messages, and exhausted `max_attempts`. The DLQ is a pair of keys per queue —
`{namespace}:{queue}:dlq:stream` plus `:dlq:hash`. The copy's `attempt`
matches the last delivery (terminal states do not bump it) and the payload is
preserved whole: inspectable and replayable.

## Dead-letter notification hooks (on_dead)

The DLQ is not a silent end: a task can declare a **dead-letter notification
hook** with `@task(..., on_dead=fn)`. After a message enters the DLQ through
any of the three paths above, the worker calls `fn` once — after the DLQ
write has completed — handing over the message context:

```python
from binomic.broker import MessageEntry
from binomic.task import OnDeadReason, task


def alert(
    entry: MessageEntry,
    reason: OnDeadReason,
    exc: BaseException | None,
) -> None:
    print(f"[{reason}] {entry.message.task} on {entry.queue} is dead")


@task("default", on_dead=alert)
async def charge() -> None: ...
```

The hook contract:

- called with the **positional arguments** `fn(entry, reason, exc)`: `entry`
  is the consumer-side envelope `MessageEntry` (queue, stream entry id,
  payload, and the delivery stamp); `reason` is one of `"timeout"` /
  `"expired"` / `"max_attempts"`; `exc` is the triggering exception — the
  `TimeoutError` instance for an execution timeout, the task's original
  exception at exhausted attempts, and `None` for staleness;
- **no return value**: the hook is a notification about a fact, not control
  flow — return values are ignored; replays from the DLQ stay the consumer's
  business;
- sync and async functions both work;
- **delivery semantics are never affected**: an exception raised by a hook is
  logged (warning) and swallowed, and the consume loop carries on; only
  cancellation (and exit signals) propagate as-is;
- **best-effort resolution**: the hook rides on the task declaration and
  reaches the worker through the same `autodiscover` path; if the message's
  task name is not registered, the hook is skipped and the DLQ write is
  unaffected;
- **hooks must be fast**: they run outside `task_timeout` with no timeout
  budget — a hanging hook pins one concurrency slot.

The type aliases `OnDeadReason` and `OnDeadHook` are exported from
`binomic.task` for annotating hook functions.

## reclaim: crash recovery

When a worker process dies (crash, SIGKILL), nobody acks what sat in its PEL.
Other consumers run `XAUTOCLAIM` every `reclaim_interval`, taking over
messages idle for more than `min_idle_ms` (`task_timeout` in milliseconds,
floored at 10 seconds). Unlike a failure retry, a reclaimed copy is **stamped
anew**: the worker died — a *lost delivery*, not a task failure — so the copy
gets a full fresh deadline window. Otherwise the stranding time alone would
already read as stale, and every reclaimed message would arrive dead on
arrival. `attempt` is bumped as usual.

Beyond the `XAUTOCLAIM` arm, the same script also performs a
**consumer-metadata sweep**: consumer names carry a hostname prefix (so
coexisting instances never collide), and scaling instances up and down leaves
dead consumers behind in the group. Consumers whose PEL is empty and whose
idle time exceeds `gc_idle_ms` (derived from the policy, floored at ten
`reclaim_interval`s) are dropped. A consumer with pending entries is never
touched — `DELCONSUMER` would destroy its unacked entries and strand those
messages for good.

## Neither retried nor dead-lettered

- an unregistered task name (`TaskNotFoundError`): redelivering the same
  payload cannot change the outcome, so the message is acked and dropped;
- a cancelled task, or a worker that received an exit signal (`SystemExit` /
  `KeyboardInterrupt`): the exception propagates as-is, the message is **not
  acked** and stays in the PEL for the next `reclaim` to hand to another
  consumer.

Payload integrity is guarded at the broker: `acquire` / `reclaim` clean up
entries whose body cannot be read (`XACK` + `XDEL`); such corruption does not
count as an attempt.
