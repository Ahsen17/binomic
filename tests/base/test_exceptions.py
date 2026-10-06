import pytest

from binomic.base.exceptions import (
    BinomicError,
    DeserializationError,
    SerializationError,
)


class TestExceptionHierarchy:
    def test_serialization_error_is_binomic_error(self) -> None:

        assert issubclass(SerializationError, BinomicError)
        assert issubclass(SerializationError, Exception)

    def test_deserialization_error_is_binomic_error(self) -> None:

        assert issubclass(DeserializationError, BinomicError)
        assert issubclass(DeserializationError, Exception)

    def test_catchable_through_base_class(self) -> None:

        with pytest.raises(BinomicError):
            raise DeserializationError("bad payload")
