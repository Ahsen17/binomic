from binomic.base import BinomicError
from binomic.task.exceptions import DuplicateTaskError, TaskNotFoundError


class TestExceptionHierarchy:
    def test_duplicate_task_error_is_binomic_error(self) -> None:

        assert issubclass(DuplicateTaskError, BinomicError)

    def test_task_not_found_error_is_binomic_error(self) -> None:

        assert issubclass(TaskNotFoundError, BinomicError)
