# Technical debt

This page records capability gaps the framework deliberately accepts: each one
is an adjudicated decision — the mechanism gap is real, but it is not
implemented for now. Every entry states when it was adjudicated, what the gap
is, the reasoning, and what would reopen it; roadmap entries marked
"adjudicated as not built" point here.

## Producer-side rate limiting

**Adjudicated in October 2026: not implemented.**

The roadmap once listed "producer-side rate limiting" (a cap on submissions
per unit of time) as a planned item.

The gap: the delivery rate is orthogonal to the two existing configuration
dimensions — `capacity` bounds the inventory (the unacknowledged total) and
`concurrency` bounds the execution concurrency. When consumers keep up with
producers, the queue depth stays below the `capacity` limit and the capacity
check never fires; the rate a downstream service endures is then bounded only
by `concurrency` divided by the average task duration — the faster the tasks,
the higher that bound, with no framework-level cap.

Why not built:

- no driving scenario: at adjudication time no actual workload or user request
  needed to cap the submission rate;
- industry precedents offer no ready-made semantics to copy: Celery's per-task
  `rate_limit` applies per worker instance and is not global; dramatiq ships
  Redis-backed distributed primitives that users compose inside their tasks;
  arq has no built-in rate limiting; Google Cloud Tasks' queue-level
  `max_dispatches_per_second` (a token bucket) is the closest to a
  producer-side limit, but it delays the excess instead of rejecting it.

The control surface therefore stays two-dimensional — `capacity` (inventory)
and `concurrency` (execution concurrency).

What reopens it: a real scenario — typically tasks calling a rate-limited
external service — means redoing the necessity review, which will also have to
settle the place of enforcement (producer side or worker side), the counting
scope (per process or Redis-global), and the overrun behavior (reject or
delay).
