"""Integration-test package.

Shared settings live here rather than in ``tasks``: importing the package must
not register tasks, or unit tests that assume an empty registry would see them
during collection.
"""

E2E_QUEUE: str = "e2e-pipeline"
RETRY_QUEUE: str = "retry-pipeline"
