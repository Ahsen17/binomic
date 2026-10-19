"""SDK wiring for the telemetry integration test, importable in subprocesses.

Everything here is module-level because spawn pickles the ``worker_init``
callback by reference: lambdas and closures cannot cross the process boundary.
The exporters push telemetry into the test's Redis database, which is how the
parent process observes data produced inside the worker subprocess.
"""

import json
import os
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from redis import Redis

if TYPE_CHECKING:
    from collections.abc import Sequence

    from opentelemetry.sdk.metrics.export import MetricsData
    from opentelemetry.sdk.trace import ReadableSpan


OTEL_SPANS_KEY = "it-otel:spans"
OTEL_METRICS_KEY = "it-otel:metrics"


def _redis() -> Redis:

    return Redis.from_url(os.environ["BINOMIC_IT_OTEL_DSN"], decode_responses=True)


class RedisSpanExporter(SpanExporter):
    def export(self, spans: "Sequence[ReadableSpan]") -> SpanExportResult:

        client = _redis()

        try:
            for span in spans:
                client.rpush(
                    OTEL_SPANS_KEY,
                    json.dumps(
                        {
                            "name": span.name,
                            "kind": span.kind.name,
                            "attributes": dict(span.attributes or {}),
                            "trace_id": format(span.context.trace_id, "032x"),
                            "parent_trace_id": (
                                format(span.parent.trace_id, "032x")
                                if span.parent is not None
                                else None
                            ),
                        },
                    ),
                )

        finally:
            client.close()

        return SpanExportResult.SUCCESS

    def shutdown(self) -> None: ...


class RedisMetricExporter(MetricExporter):
    def export(
        self,
        metrics_data: "MetricsData",
        timeout_millis: float = 30000,
        **kwargs: Any,
    ) -> MetricExportResult:

        client = _redis()

        try:
            names: list[str] = [
                metric.name
                for resource in metrics_data.resource_metrics
                for scope in resource.scope_metrics
                for metric in scope.metrics
            ]
            client.rpush(OTEL_METRICS_KEY, json.dumps(names))

        finally:
            client.close()

        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 30000, **kwargs: Any) -> bool:

        return True

    def shutdown(self, timeout_millis: float = 30000, **kwargs: Any) -> None: ...


def worker_otel_init() -> None:
    """Per-subprocess SDK setup: the hook the test passes as ``worker_init``."""

    spans = TracerProvider()
    spans.add_span_processor(SimpleSpanProcessor(RedisSpanExporter()))
    trace.set_tracer_provider(spans)

    reader = PeriodicExportingMetricReader(
        RedisMetricExporter(),
        export_interval_millis=200,
    )
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))


def read_spans(dsn: str) -> list[dict[str, Any]]:

    client = Redis.from_url(dsn, decode_responses=True)

    try:
        return [json.loads(raw) for raw in client.lrange(OTEL_SPANS_KEY, 0, -1)]

    finally:
        client.close()


def read_metric_names(dsn: str) -> set[str]:

    client = Redis.from_url(dsn, decode_responses=True)

    try:
        names: set[str] = set()

        for raw in client.lrange(OTEL_METRICS_KEY, 0, -1):
            names.update(json.loads(raw))

        return names

    finally:
        client.close()
