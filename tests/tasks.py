"""Task module for unit tests.

Named ``tasks`` so ``autodiscover("tests")`` finds and imports it; it must not
register anything on package import, only when this module is imported.
"""

from binomic.task import task


@task("default")
def example(index: int = 0) -> None:
    """A parameterized direct task the worker tests can enqueue."""


@task("default")
def fail_example() -> None:
    """A direct task that always fails, for retry paths."""

    raise RuntimeError("fail_example")
