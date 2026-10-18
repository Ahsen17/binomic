__all__ = (
    "BinomicError",
    "BuiltinError",
    "ConvertError",
    "DeserializationError",
    "SerializationError",
)


class BinomicError(Exception):
    """Base class for binomic exceptions."""


class SerializationError(BinomicError):
    """Exception raised for errors in serialization."""


class DeserializationError(BinomicError):
    """Exception raised for errors in deserialization."""


class BuiltinError(BinomicError):
    """Exception raised for errors in builtins."""


class ConvertError(BinomicError):
    """Exception raised for errors in conversion."""
