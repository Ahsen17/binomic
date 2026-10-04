__all__ = (
    "BinomicError",
    "DeserializationError",
    "SerializationError",
)


class BinomicError(Exception):
    """Base class for binomic exceptions."""


class SerializationError(BinomicError):
    """Exception raised for errors during serialization."""


class DeserializationError(BinomicError):
    """Exception raised for errors during deserialization."""
