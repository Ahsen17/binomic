"""The telemetry boundary.

The only place in ``src`` that imports ``opentelemetry``. The dependency is
optional (``binomic[otel]``): this module always imports, and the
context/error gateways degrade to no-ops when the extra is missing. The
``create_*``/``start_*`` gateways require the extra: callers reach them only
after ``ensure_available()`` (run by the client and worker when
``otel_enabled=True``), so a missing extra fails early instead of midway.

Library code talks to the OpenTelemetry *API* through these gateways; the
SDK, its providers and any exporter belong to the application embedding
binomic. With no provider set every call is a no-op proxy.
"""

from typing import TYPE_CHECKING, Any, Final

import structlog

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping


__all__ = (
    "AVAILABLE",
    "DEAD_REASON",
    "MESSAGING_DESTINATION_NAME",
    "MESSAGING_MESSAGE_ID",
    "MESSAGING_OPERATION_NAME",
    "MESSAGING_SYSTEM",
    "MESSAGING_SYSTEM_VALUE",
    "QUEUE_NAME",
    "TASK_ATTEMPT",
    "TASK_NAME",
    "WORKER_CONSUMER",
    "capture_context",
    "create_counter",
    "create_histogram",
    "create_observable_gauge",
    "create_up_down_counter",
    "ensure_available",
    "extract_context",
    "mark_span_error",
    "start_process_span",
    "start_producer_span",
)


logger = structlog.stdlib.get_logger(__name__)

SCOPE_NAME: Final = "binomic"

MESSAGING_SYSTEM: Final = "messaging.system"
MESSAGING_OPERATION_NAME: Final = "messaging.operation.name"
MESSAGING_DESTINATION_NAME: Final = "messaging.destination.name"
MESSAGING_MESSAGE_ID: Final = "messaging.message.id"

MESSAGING_SYSTEM_VALUE: Final = "binomic"

TASK_NAME: Final = "binomic.task.name"
TASK_ATTEMPT: Final = "binomic.task.attempt"
QUEUE_NAME: Final = "binomic.queue.name"
WORKER_CONSUMER: Final = "binomic.worker.consumer"
DEAD_REASON: Final = "binomic.dead.reason"

# The W3C Trace Context fields persisted for delivery.
_TRACE_KEYS: Final[tuple[str, ...]] = ("traceparent", "tracestate", "baggage")

_INSTALL_HINT: Final = (
    "OpenTelemetry is enabled (`otel_enabled=True`) but the optional "
    "`opentelemetry-api` dependency is missing. Install `binomic[otel]`."
)

try:
    import opentelemetry  # noqa: F401

except ImportError:  # pragma: no cover - exercised without the extra
    AVAILABLE = False

else:
    AVAILABLE = True


def ensure_available() -> None:
    """Fail fast when telemetry is enabled without the optional extra."""

    if not AVAILABLE:
        raise RuntimeError(_INSTALL_HINT)


def _tracer() -> Any:

    from opentelemetry import trace

    return trace.get_tracer(SCOPE_NAME)


def _meter() -> Any:

    from opentelemetry import metrics

    return metrics.get_meter(SCOPE_NAME)


def create_counter(name: str, unit: str) -> Any:
    """Create a counter on the global meter."""

    return _meter().create_counter(name=name, unit=unit)


def create_histogram(name: str, unit: str) -> Any:
    """Create a histogram on the global meter."""

    return _meter().create_histogram(name=name, unit=unit)


def create_up_down_counter(name: str, unit: str) -> Any:
    """Create an up/down counter on the global meter."""

    return _meter().create_up_down_counter(name=name, unit=unit)


def create_observable_gauge(
    name: str,
    unit: str,
    observe: "Callable[[], Iterable[tuple[float, dict[str, str]]]]",
) -> None:
    """Register an observable gauge fed by a snapshot-producing callable."""

    from opentelemetry.metrics import Observation

    def callback(options: Any) -> list[Any]:

        return [
            Observation(value, attributes=attributes) for value, attributes in observe()
        ]

    _meter().create_observable_gauge(
        name=name,
        callbacks=[callback],
        unit=unit,
    )


def start_producer_span(
    queue: str,
    attributes: "Mapping[str, str | int]",
) -> Any:
    """Open a producer span for a queue, nested in the current context."""

    from opentelemetry.trace import SpanKind

    return _tracer().start_as_current_span(
        f"create {queue}",
        kind=SpanKind.PRODUCER,
        attributes=attributes,
    )


def start_process_span(
    task: str,
    carrier: "Mapping[str, str] | None",
    attributes: dict[str, str | int],
) -> Any:
    """Open a consumer span for a task, parented to the producer context."""

    from opentelemetry.trace import SpanKind

    return _tracer().start_as_current_span(
        f"process {task}",
        context=extract_context(carrier),
        kind=SpanKind.CONSUMER,
        attributes=attributes,
    )


def capture_context() -> dict[str, str]:
    """Capture the current trace context as carrier fields; never raises."""

    if not AVAILABLE:
        return {}

    from opentelemetry.propagate import inject

    carrier: dict[str, str] = {}

    try:
        inject(carrier)

    except Exception:
        logger.debug("trace context capture failed", exc_info=True)

    return {key: carrier[key] for key in _TRACE_KEYS if carrier.get(key)}


def extract_context(carrier: "Mapping[str, str] | None") -> Any:
    """Build a context from carrier fields; falls back to the current one."""

    if not AVAILABLE:
        return None

    from opentelemetry.context import get_current
    from opentelemetry.propagate import extract

    try:
        return extract(dict(carrier or {}))

    except Exception:
        logger.debug("trace context extract failed", exc_info=True)

        return get_current()


def mark_span_error(exc: BaseException) -> None:
    """Record an exception on the current span; never raises."""

    if not AVAILABLE:
        return

    from opentelemetry.trace import Status, StatusCode, get_current_span

    try:
        span = get_current_span()

        if span.is_recording():
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))

    except Exception:
        logger.debug("span error marking failed", exc_info=True)
