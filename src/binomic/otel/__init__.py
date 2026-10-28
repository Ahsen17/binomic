"""OpenTelemetry integration for binomic.

A standalone module so the optional dependency and every trace/metric
gateway live behind one boundary: core code imports ``binomic.otel`` and
never touches ``opentelemetry`` directly.
"""

from .telemetry import (
    AVAILABLE,
    DEAD_REASON,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_SYSTEM,
    MESSAGING_SYSTEM_VALUE,
    QUEUE_NAME,
    RESTART_REASON,
    TASK_ATTEMPT,
    TASK_NAME,
    WORKER_CONSUMER,
    capture_context,
    create_counter,
    create_histogram,
    create_observable_gauge,
    create_up_down_counter,
    ensure_available,
    extract_context,
    mark_span_error,
    start_process_span,
    start_producer_span,
)

__all__ = (
    "AVAILABLE",
    "DEAD_REASON",
    "MESSAGING_DESTINATION_NAME",
    "MESSAGING_MESSAGE_ID",
    "MESSAGING_OPERATION_NAME",
    "MESSAGING_SYSTEM",
    "MESSAGING_SYSTEM_VALUE",
    "QUEUE_NAME",
    "RESTART_REASON",
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
