# Observability

Binomic can emit all three OpenTelemetry (OTel) signals: **traces, metrics and
logs**. Collection is **optional**: the OTel dependency ships in a separate
extra and is disabled by default. Binomic only produces telemetry —
**the SDK, its providers and any exporter are yours to assemble**. The library
registers no exporter and never writes to a collector.

## Install and enable

```bash
# uv
uv add "binomic[otel]"

# pip
pip install "binomic[otel]"
```

```python
from binomic.client import BinomicConfig

config = BinomicConfig(
    queues=["default"],
    otel_enabled=True,
)
```

`otel_enabled` defaults to `False`: Binomic makes no OTel calls, emits no
spans or metrics and writes no trace context to Redis, so behaviour matches a
build without observability. structlog logging is **not** governed by this
switch and always flows as configured.

Enabling it without the extra installed raises an instructive `RuntimeError`
when `Binomic` is constructed (fail-fast) instead of silently degrading.

### Litestar

`BinomicPlugin`'s third argument is a complete `BinomicConfig`, so observability
settings travel with it:

```python
from binomic.client import BinomicConfig
from binomic.plugin.litestar import BinomicPlugin
from myapp.telemetry import init_worker

plugin = BinomicPlugin(
    app_name="myapp",
    broker_dsn="redis://localhost:6379/0",
    config=BinomicConfig(
        queues=["default"],
        otel_enabled=True,
        worker_init=init_worker,
    ),
)
```

The plugin does not initialise the SDK for you: set the provider in the
Litestar process (client side) before startup, and let `worker_init` cover the
worker subprocesses — both as described above.

## Traces

### Application process (client)

Install the global provider **before** constructing `Binomic`. The SDK choice
is yours:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

> A process may set its global provider only once; later calls warn and do
> nothing.

Every `submit` (and every cron/interval trigger) opens a `create {queue}`
PRODUCER span and stores the current W3C trace context (`traceparent`,
`baggage`) in a dedicated Redis record. The message envelope
(`Message` / `MessageEntry`) carries no telemetry fields.

### Worker subprocesses

The master starts workers with **spawn**, so a subprocess does not inherit the
parent's provider. The only place to install the SDK inside a worker is the
`worker_init` hook:

```python
# myapp/telemetry.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


def init_worker() -> None:
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
```

```python
from binomic.client import BinomicConfig
from myapp.telemetry import init_worker

config = BinomicConfig(
    queues=["default"],
    otel_enabled=True,
    worker_init=init_worker,
)
```

- `worker_init` runs **once per worker subprocess**, synchronously, before the
  event loop is created;
- it must be a **module-level function** (spawn pickles callbacks by
  reference), never a lambda or closure;
- pickle transports the `module:qualname` reference, not the code, so that
  module **must be importable inside the worker subprocess** (same
  environment and package layout as the parent);
- if it raises, the subprocess exits and the master's liveness supervision
  restarts it — persistent failure surfaces as a restart loop rather than
  silent loss.

Execution opens a `process {task}` CONSUMER span whose parent is read from the
stored context by message id. Missing context (for example an old message)
degrades to a root span. The consumer links with parent-child semantics — the
single-message, no-ambient-span case, matching Celery and aio-pika defaults.

### Semantic conventions

Span names and attributes follow the shape of the OpenTelemetry messaging
conventions: `messaging.system=binomic`,
`messaging.operation.name=create|process`,
`messaging.destination.name=<queue>`, `messaging.message.id=<uuid>`. Custom
attributes use the `binomic.` prefix. `traceparent`, `tracestate` and
`baggage` all travel with messages.

## Metrics

All instruments live under the `binomic.` namespace and use UCUM units:

| Metric | Type | Unit | Meaning |
|-|-|-|-|
| `binomic.task.execution.duration` | Histogram | `s` | Wall-clock task function duration |
| `binomic.task.delivery.lag` | Histogram | `s` | Total delivery delay: execution start − `enqueued_at` |
| `binomic.task.submissions` | Counter | `{task}` | Submission attempts (silent enqueue degradation is not a failure) |
| `binomic.task.executions` | Counter | `{task}` | Executions, retries included |
| `binomic.task.acks` | Counter | `{task}` | Acks |
| `binomic.task.retries` | Counter | `{task}` | Backoff redeliveries |
| `binomic.task.dead_letters` | Counter | `{task}` | DLQ transfers (with `reason`) |
| `binomic.task.reclaims` | Counter | `{task}` | Reclaimed messages |
| `binomic.worker.inflight.tasks` | UpDownCounter | `{task}` | In-flight tasks (per process) |
| `binomic.queue.depth` | ObservableGauge | `{message}` | Stream length (pending included) |
| `binomic.queue.oldest_pending.age` | ObservableGauge | `s` | Age of the oldest entry |

**About `delivery.lag`**: it is anchored on `enqueued_at`, the message's total
delivery deadline stamp. Retries preserve that stamp, so a retried task
accumulates its earlier waiting time — that is the definition (total delivery
delay), not the delay of one individual delivery.

Explicit histogram buckets are configured by your own View; Binomic does not
impose them. The OTel semconv second-scale bucket set works well as a default.

## Logs

Binomic logs through `structlog.stdlib.get_logger` and deliberately ships **no**
logging configuration (a library must not override its host's setup). Bridging
logs into OTel happens in your application — Python's Logs API is still in
Development, so the mechanics may evolve:

```python
import logging

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler

logger_provider = LoggerProvider()
logging.getLogger().addHandler(
    LoggingHandler(level=logging.INFO, logger_provider=logger_provider),
)
```

With structlog's stdlib `ProcessorFormatter`, the handler above receives both
Binomic's and your application's records. The API reference documents the
gateways that emit them.

## Multiple processes

- A global provider may be installed only once per process; repeated attempts
  warn and do nothing;
- every worker subprocess reports independently, so metrics are per-process
  series and aggregation is the backend's call;
- trace/metric emission goes through the OTel API, whose contract never raises
  into callers; context capture/read failures, span error marking and gauge
  callback failures are swallowed and downgraded to debug logs, never
  interrupting delivery, execution or acks;
- `ensure_available()` is the only gateway that raises on purpose: enabling
  telemetry without the extra fails immediately.
