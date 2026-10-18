from binomic.base import BinomicError

__all__ = (
    "DuplicateTaskError",
    "TaskNotFoundError",
)


class DuplicateTaskError(BinomicError):
    """Exception raised when a task with the same name already exists."""


class TaskNotFoundError(BinomicError):
    """Exception raised when a task with the given name does not exist."""
