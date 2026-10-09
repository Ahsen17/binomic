# Reliability

A message that has been sent out can fail to run, overrun its timeout, or fail to
be enqueued at all. This page covers how Binomic handles the three cases:
**retrying a failure**, **delivery back-pressure**, and the **dead letter queue
(DLQ)** that is not implemented yet. See [Architecture](architecture.md) for
where they sit in the pipeline.

## Attempt count

`Message` carries an `attempt` field (default `1`) that is serialized with the
payload, so it travels with the message across processes and across
redeliveries. Every delivery counts as one attempt:

- the client's first delivery: `attempt = 1`;
- a worker that fails or overruns its timeout: `attempt += 1`, and the message is
  redelivered while the budget lasts;
- `reclaim` bumping a stranded message to a new consumer: `attempt += 1` too.

## Retrying a failure

When a task raises or runs longer than `task_timeout`, the worker:

1. logs the failure;
2. increments `attempt`;
3. enqueues the message again with a **fresh** `enqueued_at` if
   `attempt <= max_attempts` (default `3`) — the new attempt is a new entry in
   the stream;
4. logs `max attempts reached` and drops the message once the budget is spent
   (for now it stops at the log line; see DLQ below).

A message that sat in the queue longer than `task_timeout` (that is, its
`enqueued_at` is too old) takes the same path: it is not executed but
re-enqueued with a new timestamp.

```python
from binomic.config import BinomicConfig

# At most 3 attempts per message (the first one plus 2 redeliveries)
config = BinomicConfig(queues=["default"], max_attempts=3)
```

```{note}
Redelivery is immediate, with no backoff: a message that keeps failing burns
through `max_attempts` at full speed. Wait inside the task function if you need
one.
```

Two kinds of failure are **not** retried — they are logged and `ack`ed (the
message is dropped), because redelivering the same payload cannot change the
outcome:

- a payload that cannot be deserialized;
- a task name that was never registered (`TaskNotFoundError`).

**Cancellation does not ack**: when a worker shuts down or a task is cancelled,
the message stays in the PEL instead of being acknowledged, leaving it for the
next `reclaim` to redeliver to another consumer.

## Delivery back-pressure: the queue capacity limit

`queue_capacity` (default `1000`) caps how much work a queue may hold. The limit
is read from the consumer group's own bookkeeping — `pending` (delivered but
unacknowledged) plus `lag` (not yet delivered) — and `enqueue` raises
`QueueCapacityLimitError` once it is reached:

```python
from binomic.broker import QueueCapacityLimitError
```

What happens on a full queue depends on the caller:

| Caller | When the queue is full |
|-|-|
| client delivery (`submit`) | the error is swallowed inside the client and logged: **`submit` raises nothing and still returns the message ID**, so the caller cannot tell from the return value that the message was dropped — and it is not retried |
| worker redelivery (retry) | logged and dropped the same way, so that retry never happens |
| `reclaim` redelivering a stranded message | logs a warning and leaves the message PENDING (no ack) for the next `reclaim` pass — back-pressure rather than failure |

```{note}
The limit is a *soft* one: the delivery side (the client, and the worker
redelivering a failure) drops the message and logs it, and only `reclaim` keeps
it for a later pass. The check reads the consumer group's bookkeeping, so the
queue must have its consumer group created (that is, the broker initialized);
with no group, there is no capacity limit.
```

## Dead letter queue (not implemented)

Both an exhausted `max_attempts` and the two non-retryable failures above are
currently logged and then dropped; the code carries `TODO: send to DLQ` markers.
Until a DLQ exists, those messages are **unrecoverable** — catch the exception
inside the task function yourself if you need to keep them.
