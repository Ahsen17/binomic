"""Handler semantics: one test per branch of `WorkerHandler.handle`."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from opentelemetry import trace as otel_trace
from opentelemetry.trace import SpanKind, StatusCode
from pytest_mock import MockerFixture

from binomic.broker import Message, MessageEntry, RedisBroker
from binomic.otel import telemetry
from binomic.worker import WorkerHandler, WorkerPolicy

STREAM_KEY = "binomic:default:stream"
DLQ_STREAM_KEY = "binomic:default:dlq:stream"

Deliver = Callable[..., Coroutine[Any, Any, MessageEntry]]


@pytest.fixture
async def make_handler(
    make_broker: Callable[..., RedisBroker],
    handler_module: Any,
) -> AsyncIterator[Callable[..., Any]]:
    """Build a handler over a fresh fakeredis broker; policy knobs per test.

    The default ``task_timeout`` is deliberately generous: the first Lua
    round-trip in a cold interpreter costs hundreds of ms, and a tight budget
    would race the over-age branch instead of testing the intended one.
    """

    built: list[tuple[WorkerHandler, RedisBroker]] = []

    async def _make(**policy_kwargs: Any) -> Any:

        broker = make_broker(["default"])
        await broker.start()

        fields: dict[str, Any] = {
            "task_timeout": 5.0,
            "max_attempts": 3,
        }
        fields.update(policy_kwargs)
        policy = WorkerPolicy(
            queues=["default"],
            concurrency=2,
            **fields,
        )
        handler = WorkerHandler(
            module_name="handlerapp",
            consumer="test-worker",
            broker=broker,
            worker_policy=policy,
        )
        built.append((handler, broker))

        return SimpleNamespace(handler=handler, broker=broker, policy=policy)

    yield _make

    for handler, broker in built:
        handler.close()
        await broker.close()


async def pending_count(broker: RedisBroker) -> int:

    pending = await broker._ensure_client().xpending(STREAM_KEY, "binomic")
    return int(pending["pending"])


async def dead_letter_count(broker: RedisBroker) -> int:

    entries = await broker._ensure_client().xrange(DLQ_STREAM_KEY, "-", "+")
    assert entries is not None
    return len(cast("list[Any]", entries))


async def stream_entry_ids(broker: RedisBroker) -> list[str]:

    entries = await broker._ensure_client().xrange(STREAM_KEY, "-", "+")
    assert entries is not None

    return [entry_id for entry_id, _fields in cast("list[tuple[Any, Any]]", entries)]


class TestSuccess:
    async def test_executes_and_acks(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # Execution is proven by the success path's ack; nothing died.
        assert await pending_count(e.broker) == 0
        assert await stream_entry_ids(e.broker) == []
        assert await dead_letter_count(e.broker) == 0


class TestUnknownTask:
    async def test_acks_without_dead_lettering(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ghost"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert await dead_letter_count(e.broker) == 0
        assert await pending_count(e.broker) == 0


class TestRetry:
    async def test_schedules_a_delayed_retry_on_failure(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        set_backoff: Callable[[float], None],
    ) -> None:

        set_backoff(0.05)
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="boom", attempt=1))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # Nothing has been delivered yet: the copy only appears once the
        # scheduled retry fires. The original is still pending.
        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0

        jobs = e.handler._scheduler._scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].func.__name__ == "retry"
        assert jobs[0].args == (entry,)

    async def test_redelivery_carries_incremented_attempt_and_stale_stamp(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        e.handler._backoff = lambda attempt: 0.05
        await e.broker.enqueue("default", Message(task="boom", attempt=1))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        deadline = time.monotonic() + 5
        ids: list[str] = []
        while time.monotonic() < deadline:
            ids = await stream_entry_ids(e.broker)
            if len(ids) == 1 and ids[0] != entry.id:
                break
            await asyncio.sleep(0.02)

        assert len(ids) == 1
        assert ids[0] != entry.id
        assert await pending_count(e.broker) == 0

        raw = await e.broker._ensure_client().hget("binomic:default:hash", ids[0])
        assert raw is not None
        body = json.loads(raw)

        # task_timeout is a total delivery deadline: the copy keeps the
        # original stamp so the age keeps accumulating across retries.
        assert body["message"]["attempt"] == 2
        assert body["enqueued_at"] == entry.enqueued_at


class TestMaxAttempts:
    async def test_failure_at_max_attempts_dead_letters(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="boom", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # attempt + 1 > max_attempts: no retry is scheduled, the message dies
        # with its attempt recorded faithfully (no extra bump in the DLQ).
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0
        assert await stream_entry_ids(e.broker) == []
        # No retry scheduled — the underlying scheduler was never even started.
        assert e.handler._scheduler._scheduler is None


class TestOverAge:
    async def test_skips_execution_and_dead_letters(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")
        entry.enqueued_at = int(time.time()) - 3600

        await e.handler.handle(entry)

        # Dead-lettered without execution, whatever the task would have done:
        # the DLQ copy is the ack-clean message itself.
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0


class TestTimeout:
    async def test_dead_letters_without_retry(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:
        """task_timeout is a total deadline: an overrun execution dies instead
        of retrying (the ref's new semantics). Uses a tight budget: the task
        sleeps two full seconds, far past the half-second one."""

        e = await make_handler(task_timeout=0.5)
        await e.broker.enqueue("default", Message(task="slow"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        # A TimeoutError can only come from a task that RAN: execution is
        # implied by the timeout dead-lettering itself.
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0
        assert e.handler._scheduler._scheduler is None


class TestCancellation:
    async def test_propagates_without_ack(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="cancelled"))
        entry = await deliver_entry(e.broker, "default", "c1")

        with pytest.raises(anyio.get_cancelled_exc_class()):
            await e.handler.handle(entry)

        # Left exactly as it was: the message stays pending for reclaim, no
        # dead letter, no retry — the scheduler was never even started.
        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0
        assert e.handler._scheduler._scheduler is None


class TestExitSignals:
    async def test_system_exit_propagates_without_ack(
        self, make_handler: Callable[..., Any], deliver_entry: Deliver
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="quits"))
        entry = await deliver_entry(e.broker, "default", "c1")

        with pytest.raises(SystemExit):
            await e.handler.handle(entry)

        assert await pending_count(e.broker) == 1
        assert await dead_letter_count(e.broker) == 0


class TestOnDeadHook:
    """Every DLQ path fires the spec's `on_dead` hook once, after the write."""

    async def test_max_attempts_notifies_with_the_failure(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="dies_hooked", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert await dead_letter_count(e.broker) == 1
        [task, reason, exc] = handler_module.dead_notes[0]
        assert (task, reason) == ("dies_hooked", "max_attempts")
        assert isinstance(exc, ValueError)

    async def test_timeout_notifies_with_the_timeout_error(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler(task_timeout=0.5)
        await e.broker.enqueue("default", Message(task="slow_hooked"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert await dead_letter_count(e.broker) == 1
        [task, reason, exc] = handler_module.dead_notes[0]
        assert (task, reason) == ("slow_hooked", "timeout")
        assert isinstance(exc, TimeoutError)

    async def test_over_age_notifies_without_an_exception(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="aged_hooked"))
        entry = await deliver_entry(e.broker, "default", "c1")
        entry.enqueued_at = int(time.time()) - 3600

        await e.handler.handle(entry)

        # The over-age path resolves the spec after the fact and the hook
        # still fires, with no exception to report.
        assert await dead_letter_count(e.broker) == 1
        assert handler_module.dead_notes == [("aged_hooked", "expired", None)]

    async def test_unknown_task_skips_the_hook(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ghost"))
        entry = await deliver_entry(e.broker, "default", "c1")
        entry.enqueued_at = int(time.time()) - 3600

        await e.handler.handle(entry)

        # The DLQ write still happens; only the hook is skipped.
        assert await dead_letter_count(e.broker) == 1
        assert handler_module.dead_notes == []

    async def test_hook_errors_are_swallowed_and_logged(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="dies_broken_hook", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        with caplog.at_level(logging.WARNING, logger="binomic.worker.handler"):
            await e.handler.handle(entry)

        # The dead letter is written and the hook failure is contained.
        assert await dead_letter_count(e.broker) == 1
        assert any(
            "on_dead hook failed" in record.getMessage() for record in caplog.records
        )

    async def test_cancellation_inside_the_hook_propagates(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:
        """A hook raising the cancel exc must escape: `except Exception`
        guards hook failures, never cancellation."""

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="dies_cancel_hook", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        with pytest.raises(anyio.get_cancelled_exc_class()):
            await e.handler.handle(entry)

        # The DLQ write completed before the hook fired; only the cancel
        # escapes.
        assert await dead_letter_count(e.broker) == 1
        assert await pending_count(e.broker) == 0

    async def test_async_hook_is_awaited(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        handler_module: Any,
    ) -> None:

        handler_module.dead_notes.clear()
        e = await make_handler()
        await e.broker.enqueue("default", Message(task="dies_hooked_async", attempt=3))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        [task, reason, exc] = handler_module.dead_notes[0]
        assert (task, reason) == ("dies_hooked_async", "max_attempts")
        assert isinstance(exc, ValueError)


class TestBackoff:
    async def test_doubles_until_the_cap(self, make_handler: Callable[..., Any]) -> None:

        e = await make_handler()
        backoff = e.handler._backoff

        assert [backoff(n) for n in range(1, 8)] == [
            1.5,
            3.0,
            6.0,
            12.0,
            24.0,
            30.0,
            30.0,
        ]


class TestReclaim:
    async def test_passes_policy_derived_parameters(
        self, make_handler: Callable[..., Any], mocker: MockerFixture
    ) -> None:

        e = await make_handler()
        reclaim = mocker.AsyncMock()
        mocker.patch.object(e.broker, "reclaim", reclaim)

        await e.handler.reclaim()

        # min_idle is the task_timeout in ms with a 10s floor; the consumer
        # sweep threshold adds a floor of ten reclaim intervals.
        reclaim.assert_awaited_once_with(
            consumer="test-worker",
            min_idle_ms=max(int(e.policy.task_timeout * 1000), 10_000),
            gc_idle_ms=max(
                int(e.policy.task_timeout * 1000),
                10_000,
                int(e.policy.reclaim_interval * 1000) * 10,
            ),
            count=100,
        )


class TestTelemetry:
    """The otel_enabled branch: spans, metrics and logs around execution."""

    async def test_disabled_emits_no_process_span(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        otel_sdk: Any,
    ) -> None:

        span_exporter, _ = otel_sdk
        span_exporter.clear()

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert not span_exporter.get_finished_spans()

    async def test_process_span_is_parented_to_the_stored_context(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        otel_sdk: Any,
    ) -> None:

        span_exporter, metric_reader = otel_sdk
        span_exporter.clear()

        with otel_trace.get_tracer("test").start_as_current_span("submit") as submit_span:
            carrier = telemetry.capture_context()

        trace_id = submit_span.get_span_context().trace_id

        e = await make_handler(otel_enabled=True)
        await e.broker.enqueue(
            "default",
            Message(task="ok"),
            context=carrier,
        )
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        (process,) = [
            span
            for span in span_exporter.get_finished_spans()
            if span.name == "process ok"
        ]

        assert process.kind is SpanKind.CONSUMER
        assert process.parent is not None
        assert process.parent.trace_id == trace_id
        assert process.attributes is not None
        assert process.attributes["messaging.operation.name"] == "process"
        assert process.attributes["binomic.task.name"] == "ok"

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        recorded = {
            metric.name: metric.data.data_points
            for resource in metrics_data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }

        executions = recorded.get("binomic.task.executions", [])
        assert any(
            point.attributes.get("binomic.task.name") == "ok" for point in executions
        )
        assert "binomic.task.execution.duration" in recorded
        assert "binomic.task.delivery.lag" in recorded
        assert "binomic.task.acks" in recorded

    async def test_failure_marks_the_span_and_logs(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        otel_sdk: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        span_exporter, _ = otel_sdk
        span_exporter.clear()

        e = await make_handler(otel_enabled=True, max_attempts=1)
        await e.broker.enqueue("default", Message(task="boom"))
        entry = await deliver_entry(e.broker, "default", "c1")

        with caplog.at_level(logging.WARNING, logger="binomic.worker.handler"):
            await e.handler.handle(entry)

        (process,) = [
            span
            for span in span_exporter.get_finished_spans()
            if span.name == "process boom"
        ]

        assert process.status.status_code is StatusCode.ERROR

        messages = [record.getMessage() for record in caplog.records]
        assert any("Task execution failed" in message for message in messages)
        assert any("Task moved to DLQ" in message for message in messages)

    async def test_retry_records_its_counter(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        set_backoff: Callable[[float], None],
        otel_sdk: Any,
    ) -> None:

        set_backoff(60.0)

        e = await make_handler(otel_enabled=True)
        await e.broker.enqueue("default", Message(task="boom"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        metrics_data = otel_sdk[1].get_metrics_data()
        assert metrics_data is not None

        metric_names = {
            metric.name
            for resource in metrics_data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }

        assert "binomic.task.retries" in metric_names

    async def test_expired_entry_logs_before_dlq(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        e = await make_handler()
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")
        entry.enqueued_at = int(time.time()) - 3600

        with caplog.at_level(logging.WARNING, logger="binomic.worker.handler"):
            await e.handler.handle(entry)

        messages = [record.getMessage() for record in caplog.records]
        assert any("Task expired" in message for message in messages)
        assert any("Task moved to DLQ" in message for message in messages)


class TestTelemetryGating:
    """The switch is the single source of truth for telemetry side effects."""

    async def test_disabled_failure_never_touches_spans(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        mocker: Any,
    ) -> None:

        marks: list[BaseException] = []
        mocker.patch(
            "binomic.worker.handler.mark_span_error",
            side_effect=marks.append,
        )

        e = await make_handler(max_attempts=1)
        await e.broker.enqueue("default", Message(task="boom"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        assert marks == []

    async def test_missing_context_falls_back_to_a_root_span(
        self,
        make_handler: Callable[..., Any],
        deliver_entry: Deliver,
        otel_sdk: Any,
    ) -> None:

        span_exporter, _ = otel_sdk
        span_exporter.clear()

        e = await make_handler(otel_enabled=True)
        await e.broker.enqueue("default", Message(task="ok"))
        entry = await deliver_entry(e.broker, "default", "c1")

        await e.handler.handle(entry)

        (process,) = [
            span
            for span in span_exporter.get_finished_spans()
            if span.name == "process ok"
        ]

        assert process.parent is None

    async def test_reclaim_records_its_counter(
        self,
        make_handler: Callable[..., Any],
        mocker: Any,
        otel_sdk: Any,
    ) -> None:

        _, metric_reader = otel_sdk
        e = await make_handler(otel_enabled=True)
        mocker.patch.object(e.broker, "reclaim", return_value=2)

        await e.handler.reclaim()

        metrics_data = metric_reader.get_metrics_data()
        assert metrics_data is not None

        points = [
            point
            for resource in metrics_data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "binomic.task.reclaims"
            for point in metric.data.data_points
        ]

        assert any(point.value >= 2 for point in points)
