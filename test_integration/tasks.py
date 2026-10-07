"""Task module discovered by worker subprocesses through autodiscover.

This module is named ``tasks`` so ``autodiscover("test_integration")`` imports
it inside the worker subprocess as well as in the test process.
"""

import redis

from binomic.task import task
from test_integration import E2E_QUEUE


@task(E2E_QUEUE, mode="direct")
def write_result(dsn: str, key: str) -> None:

    client = redis.Redis.from_url(dsn, decode_responses=True)
    try:
        client.set(key, "done")
    finally:
        client.close()
