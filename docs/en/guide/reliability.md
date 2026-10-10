# Reliability

A message that has been sent out can fail to run or overrun its timeout. This
page covers how Binomic handles the two cases: **retrying a failure**, and the
**dead letter queue (DLQ)** that is not implemented yet. See
[Architecture](architecture.md) for where they sit in the pipeline.

## Attempt count

`Message` carries an `attempt` field (default `1`) that is serialized with the
payload, so it travels with the message across processes and across
redeliveries. Every delivery counts as one attempt:

- the client's first delivery: `attempt = 1`;
- a worker that fails or overruns its timeout: `attempt += 1`, and the message is
  redelivered **after a backoff** while the budget lasts;
- `reclaim` bumping a stranded message to a new consumer: `attempt += 1` too.

## Retrying a failure

When a task raises or runs longer than `task_timeout`, the worker:

1. logs the failure;
2. increments `attempt`;
3. redelivers the message **after a backoff** if `attempt <= max_attempts`
   (default `3`) — the new attempt is a new entry in the stream;
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
Redelivery is **not** immediate: the worker defers it by a backoff that starts
at 1.5 seconds, doubles each time (3 / 6 / 12 / 24 seconds) and caps at **30
seconds**. The wait is held by a scheduler the worker owns, which queues the
re-enqueue as a deferred job, and the original is **acked as soon as that job is
scheduled**. Two consequences follow:

- a worker that exits inside the backoff window loses that one redelivery — the
  price of never delivering a message twice;
- the deferred job is scheduled to run even when the loop reaches it late,
  rather than being silently skipped.

The backoff is fixed for now and cannot be configured; wait inside the task
function if you need another pace.
```

Two kinds of failure are **not** retried — they are logged and `ack`ed (the
message is dropped), because redelivering the same payload cannot change the
outcome:

- a payload that cannot be deserialized;
- a task name that was never registered (`TaskNotFoundError`).

**Cancellation does not ack**: when a worker shuts down or a task is cancelled,
the message stays in the PEL instead of being acknowledged, leaving it for the
next `reclaim` to redeliver to another consumer.

## Dead letter queue (not implemented)

Both an exhausted `max_attempts` and the two non-retryable failures above are
currently logged and then dropped; the code carries `TODO: send to DLQ` markers.
Until a DLQ exists, those messages are **unrecoverable** — catch the exception
inside the task function yourself if you need to keep them.
