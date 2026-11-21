import pytest

from binomic.base import BinomicError
from binomic.broker import DeadLetterEntryNotFoundError, QueueCapacityError
from binomic.task import DuplicateTaskError, TaskNotFoundError


class TestBinomicError:
    def test_is_an_exception(self) -> None:

        assert issubclass(BinomicError, Exception)

    @pytest.mark.parametrize(
        "error",
        [
            DuplicateTaskError,
            TaskNotFoundError,
            QueueCapacityError,
            DeadLetterEntryNotFoundError,
        ],
    )
    def test_domain_errors_share_the_base(self, error: type[BinomicError]) -> None:

        assert issubclass(error, BinomicError)
