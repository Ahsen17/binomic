"""Telemetry wrapper semantics: availability, propagation, error marking."""

from typing import Any

import pytest
from opentelemetry import baggage, context, trace

from binomic.otel import telemetry


def active_telemetry() -> telemetry.Telemetry:
    """The module-level instance, asserted available for this suite."""

    instance = telemetry.otel

    assert instance is not None

    return instance


class TestAvailability:
    def test_available_when_the_extra_is_installed(self) -> None:

        assert telemetry.AVAILABLE

    def test_construction_requires_the_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        monkeypatch.setattr(telemetry, "AVAILABLE", False)

        with pytest.raises(RuntimeError, match=r"binomic\[otel\]"):
            telemetry.Telemetry()


class TestPropagation:
    def test_capture_needs_an_active_span(self, otel_sdk: Any) -> None:

        assert active_telemetry().capture_context() == {}

    def test_capture_and_extract_roundtrip(self, otel_sdk: Any) -> None:

        instance = active_telemetry()
        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("parent") as span:
            carrier = instance.capture_context()

            assert "traceparent" in carrier
            assert carrier["traceparent"].startswith("00-")

        extracted = instance.extract_context(carrier)
        remote = trace.get_current_span(extracted)

        assert remote.get_span_context().trace_id == span.get_span_context().trace_id

    def test_baggage_is_captured_too(self, otel_sdk: Any) -> None:

        instance = active_telemetry()
        tracer = trace.get_tracer("test")
        token = context.attach(baggage.set_baggage("tenant", "acme"))

        try:
            with tracer.start_as_current_span("parent"):
                carrier = instance.capture_context()

        finally:
            context.detach(token)

        assert carrier["baggage"] == "tenant=acme"

    def test_malformed_carrier_falls_back_to_current_context(self, otel_sdk: Any) -> None:

        extracted = active_telemetry().extract_context(
            {"traceparent": "not-a-traceparent"}
        )

        assert trace.get_current_span(extracted).get_span_context().is_valid is False


class TestInstruments:
    def test_observable_gauge_callback_builds_observations(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        instance = active_telemetry()
        captured: dict[str, Any] = {}

        def fake_create(
            name: str,
            callbacks: list[Any],
            unit: str,
        ) -> object:

            captured["callback"] = callbacks[0]
            return object()

        monkeypatch.setattr(instance._meter, "create_observable_gauge", fake_create)

        instance.create_observable_gauge(
            "binomic.test.gauge",
            "1",
            lambda: [(3.0, {"queue": "a"})],
        )

        (observation,) = captured["callback"](None)

        assert observation.value == 3.0
        assert observation.attributes == {"queue": "a"}


class TestErrorMarking:
    def test_marks_the_current_span_as_error(self, otel_sdk: Any) -> None:

        span_exporter, _ = otel_sdk
        span_exporter.clear()

        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("failing"):
            active_telemetry().mark_span_error(ValueError("boom"))

        (span,) = span_exporter.get_finished_spans()

        assert span.status.status_code is trace.StatusCode.ERROR
        assert span.events[0].name == "exception"

    def test_noops_outside_a_span(self, otel_sdk: Any) -> None:

        active_telemetry().mark_span_error(ValueError("boom"))
