from typing import TYPE_CHECKING, Any, Final

try:
    import opentelemetry  # noqa: F401
    from opentelemetry import metrics, trace
    from opentelemetry.context import get_current
    from opentelemetry.metrics import Observation
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import SpanKind, Status, StatusCode, get_current_span

    if TYPE_CHECKING:
        from opentelemetry.metrics import (
            Counter,
            Histogram,
            ObservableGauge,
            UpDownCounter,
        )

except ImportError:
    AVAILABLE = False

else:
    AVAILABLE = True


from collections.abc import Callable, Iterable, Mapping

import structlog

from .signatures import SCOPE_NAME

_TRACE_KEYS: Final[tuple[str, ...]] = ("traceparent", "tracestate", "baggage")

logger = structlog.stdlib.get_logger(__name__)


class Telemetry:
    """Thin wrapper over the OpenTelemetry tracer and meter.

    Constructing one requires ``opentelemetry-api``; the module-level ``otel``
    instance is ``None`` when the dependency is missing, so callers gate on
    it instead of catching import errors. Spans and instruments are created
    lazily against whatever SDK providers the embedding application installed.
    """

    def __init__(
        self,
        scope: str = SCOPE_NAME,
    ) -> None:

        if not AVAILABLE:
            raise RuntimeError(
                "Opentelemetry is not available, "
                "`opentelemetry-api` dependency is missing. "
                "Install `binomic[otel]` instead.",
            )

        self._scope = scope

        self._tracer = trace.get_tracer(self._scope)
        self._meter = metrics.get_meter(self._scope)

    def create_counter(self, name: str, unit: str) -> "Counter":
        """Create a monotonically increasing counter instrument."""

        return self._meter.create_counter(name=name, unit=unit)

    def create_histogram(self, name: str, unit: str) -> "Histogram":
        """Create a histogram instrument for value distributions."""

        return self._meter.create_histogram(name=name, unit=unit)

    def create_up_down_counter(self, name: str, unit: str) -> "UpDownCounter":
        """Create a counter that can increase and decrease."""

        return self._meter.create_up_down_counter(name=name, unit=unit)

    def create_observable_gauge(
        self,
        name: str,
        unit: str,
        observe: Callable[[], Iterable[tuple[float, dict[str, str]]]],
    ) -> "ObservableGauge":
        """Create a gauge whose value is read from ``observe`` at collection.

        ``observe`` returns ``(value, attributes)`` pairs, so one instrument
        covers every queue or worker the caller tracks.
        """

        def callback(options: Any) -> list[Any]:
            return [
                Observation(value, attributes=attributes)
                for value, attributes in observe()
            ]

        return self._meter.create_observable_gauge(
            name=name,
            callbacks=[callback],
            unit=unit,
        )

    def start_producer_span(
        self,
        queue: str,
        attributes: Mapping[str, str | None],
    ) -> Any:
        """Start the span that represents enqueueing into ``queue``."""

        return self._tracer.start_as_current_span(
            f"create {queue}",
            kind=SpanKind.PRODUCER,
            attributes=attributes,
        )

    def start_process_span(
        self,
        task: str,
        carrier: Mapping[str, str] | None,
        attributes: dict[str, str | int],
    ) -> Any:
        """Start the consumer span for ``task``, continuing ``carrier``.

        A ``None`` or unreadable carrier yields a root span rather than
        breaking execution.
        """

        return self._tracer.start_as_current_span(
            f"process {task}",
            context=self.extract_context(carrier),
            kind=SpanKind.CONSUMER,
            attributes=attributes,
        )

    @staticmethod
    def capture_context() -> dict[str, str]:
        """Serialize the current trace context into a header carrier."""

        carrier: dict[str, str] = {}

        try:
            inject(carrier)

        except Exception:
            logger.debug("trace context capture failed", exc_info=True)

        return {key: carrier[key] for key in _TRACE_KEYS if carrier.get(key)}

    @staticmethod
    def extract_context(carrier: Mapping[str, str] | None) -> Any:
        """Rebuild a trace context from ``carrier``, falling back to current."""

        try:
            return extract(dict(carrier or {}))

        except Exception:
            logger.debug("trace context capture failed", exc_info=True)

            return get_current()

    @staticmethod
    def mark_span_error(exc: BaseException) -> None:
        """Record ``exc`` on the current span and mark it as error."""

        try:
            span = get_current_span()

            if span.is_recording():
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR))

        except Exception:
            logger.debug("span error marking failed", exc_info=True)


otel = Telemetry() if AVAILABLE else None
