import pytest

from binomic.base.exceptions import (
    BinomicError,
    BuiltinError,
    ConvertError,
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

    def test_builtin_error_is_binomic_error(self) -> None:

        assert issubclass(BuiltinError, BinomicError)
        assert issubclass(BuiltinError, Exception)

    def test_convert_error_is_binomic_error(self) -> None:

        assert issubclass(ConvertError, BinomicError)
        assert issubclass(ConvertError, Exception)

    def test_catchable_through_base_class(self) -> None:

        with pytest.raises(BinomicError):
            raise DeserializationError("bad payload")

        with pytest.raises(BinomicError):
            raise ConvertError("bad dict")

        with pytest.raises(BinomicError):
            raise BuiltinError("bad builtins")
