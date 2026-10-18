from typing import Any, Self, cast

from msgspec import Struct, convert, json, to_builtins

from .exceptions import (
    BuiltinError,
    ConvertError,
    DeserializationError,
    SerializationError,
)

__all__ = ("BaseStruct",)


class BaseStruct(Struct):
    """Serialization base for all binomic payloads.

    A msgspec Struct with JSON and dict round-trips that wrap failures in
    binomic's own exception hierarchy instead of leaking msgspec errors.
    """

    def to_dict(self) -> dict[str, Any]:

        try:
            return cast(
                "dict[str, Any]",
                to_builtins(self),
            )

        except Exception as exc:
            raise BuiltinError(
                "Failed to convert to dict",
            ) from exc

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> Self:

        try:
            return convert(obj, type=cls)

        except Exception as exc:
            raise ConvertError(
                "Failed to parse from dict",
            ) from exc

    def to_json(self) -> str:

        try:
            return json.encode(self).decode("utf-8")

        except Exception as exc:
            raise SerializationError(
                "Failed to serialize to json",
            ) from exc

    @classmethod
    def from_json(cls, obj: str) -> Self:

        try:
            return json.decode(obj, type=cls)

        except Exception as exc:
            raise DeserializationError(
                "Failed to deserialize from json",
            ) from exc
