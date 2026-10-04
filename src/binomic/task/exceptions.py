from binomic.base import BinomicError

__all__ = (
    "DuplicateTaskError",
    "TaskNotFoundError",
)


class DuplicateTaskError(BinomicError):
    """Duplicate task error."""


class TaskNotFoundError(BinomicError):
    """Task not found error."""
