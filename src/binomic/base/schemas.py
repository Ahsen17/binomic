from typing import Self

from msgspec import Struct, json

from .exceptions import DeserializationError, SerializationError

__all__ = ("BaseStruct",)


class BaseStruct(Struct):
    """Base data structure."""

    def to_json(self) -> str:
        """Convert the data structure to a JSON string."""

        try:
            return json.encode(self).decode("utf-8")

        except Exception as exc:
            raise SerializationError(
                f"Failed to serialize data structure: {exc}",
            ) from exc

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create a data structure from a JSON string."""

        try:
            return json.decode(json_str, type=cls)

        except Exception as exc:
            raise DeserializationError(
                f"Failed to deserialize data structure: {exc}",
            ) from exc

    def __repr__(self) -> str:
        """Return a string representation of the data structure."""

        return self.to_json()
