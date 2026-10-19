"""Telemetry pipeline: the worker subprocess collects spans and metrics.

The client process enables telemetry and hands the subprocess a
``worker_init`` hook, which installs an SDK that pushes telemetry into the
test's Redis database — the only channel back to the parent process. No
exporter is registered by binomic itself: the hook (application code) owns
the entire SDK setup.
"""

from typing import Any
from uuid import uuid4

import anyio
import pytest
from redis.asyncio import Redis as AsyncRedis

from binomic.broker import Message
from binomic.client import Binomic, BinomicConfig
from test_integration import E2E_QUEUE
from test_integration.otel_support import read_metric_names, read_spans, worker_otel_init

EXECUTION_TIMEOUT: float = 30.0
COLLECTION_TIMEOUT: float = 10.0


@pytest.mark.integration
class TestTelemetryPipeline:
    async def test_worker_subprocess_exports_spans_and_metrics(
        self,
        broker_dsn: str,
        probe_dsn: str,
        redis_client: AsyncRedis,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        monkeypatch.setenv("BINOMIC_IT_OTEL_DSN", broker_dsn)

        result_key = f"binomic:e2e:{uuid4()}"

        config = BinomicConfig(
            queues=[E2E_QUEUE],
            workers=1,
            concurrency=1,
            otel_enabled=True,
            worker_init=worker_otel_init,
        )
        client = Binomic(
            broker_dsn=broker_dsn,
            module_name="test_integration",
            config=config,
        )

        async with client:
            await client.submit(
                Message(task="write_result", args=[probe_dsn, result_key]),
            )

            executed = False
            with anyio.move_on_after(EXECUTION_TIMEOUT):
                while not await redis_client.exists(result_key):
                    await anyio.sleep(0.1)
                executed = True

            assert executed, f"task did not write {result_key}"

            spans: list[dict[str, Any]] = []
            with anyio.move_on_after(COLLECTION_TIMEOUT):
                while not spans:
                    spans = read_spans(broker_dsn)
                    await anyio.sleep(0.1)

            assert spans, "worker subprocess exported no spans"

            metric_names: set[str] = set()
            with anyio.move_on_after(COLLECTION_TIMEOUT):
                while not metric_names:
                    metric_names = read_metric_names(broker_dsn)
                    await anyio.sleep(0.2)

        process_spans = [span for span in spans if span["name"] == "process write_result"]

        assert process_spans, spans
        assert process_spans[0]["kind"] == "CONSUMER"
        assert process_spans[0]["attributes"]["messaging.operation.name"] == "process"
        assert process_spans[0]["attributes"]["messaging.system"] == "binomic"
        assert process_spans[0]["trace_id"]

        assert "binomic.task.executions" in metric_names
        assert "binomic.task.execution.duration" in metric_names
        assert "binomic.task.acks" in metric_names
