"""Task module discovered by worker subprocesses through autodiscover.

This module is named ``tasks`` so ``autodiscover("test_integration")`` imports
it inside both the parent and the forked worker process.
"""

import redis

from binomic.task import task


@task()
def write_result(dsn: str, key: str) -> None:

    client = redis.Redis.from_url(dsn, decode_responses=True)
    try:
        client.set(key, "done")
    finally:
        client.close()
