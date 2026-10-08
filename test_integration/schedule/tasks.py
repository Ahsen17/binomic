"""Task module discovered by worker subprocesses through autodiscover.

This module is named ``tasks`` so ``autodiscover("test_integration.schedule")``
imports it inside the worker subprocess as well as in the test process.

``autodiscover`` walks packages recursively, so the other integration pipelines
-- which discover ``test_integration`` as a whole -- import this module too and
schedule their own copy of the repeating task. Their queues and assertions are
untouched by that, but the task must stay inert when this pipeline's test did
not set it up, which is what the environment lookups below guard.
"""

import os

import redis

from binomic.task import task
from test_integration.schedule import DELAY_SECONDS, INTERVAL_SECONDS, SCHEDULE_QUEUE

# A repeating task takes no arguments -- the client submits it as a bare
# `Message(name=...)` -- so it reads where to report from the environment the
# spawned worker inherits. Without a recorded side effect nothing can tell an
# executed task from one the worker merely acked (it acks on failure too).
DSN_ENV: str = "BINOMIC_TEST_SCHEDULE_DSN"
KEY_ENV: str = "BINOMIC_TEST_SCHEDULE_KEY"


def _record(key: str, *, dsn: str) -> None:
    """Count an execution, so the test can see the task really ran."""

    client = redis.Redis.from_url(dsn, decode_responses=True)
    try:
        client.incr(key)
    finally:
        client.close()


@task(SCHEDULE_QUEUE, mode="delay", delay=DELAY_SECONDS)
def write_delayed_result(dsn: str, key: str) -> None:

    client = redis.Redis.from_url(dsn, decode_responses=True)
    try:
        client.set(key, "done")
    finally:
        client.close()


@task(SCHEDULE_QUEUE, mode="interval", interval=INTERVAL_SECONDS)
def heartbeat() -> None:

    dsn = os.environ.get(DSN_ENV)
    key = os.environ.get(KEY_ENV)

    if dsn is None or key is None:
        # Scheduled by another pipeline's client, which has no target for us.
        return

    _record(key, dsn=dsn)
