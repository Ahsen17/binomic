from typing import Any, Self, cast

from msgspec import Struct, convert, json, to_builtins, toml, yaml

__all__ = ("BaseStruct",)


class BaseStruct(Struct):
    """Base structure for data schemas.

    Adds dict/JSON/TOML/YAML round trips on top of msgspec's ``Struct``.
    Encoding and decoding errors are raised by msgspec unchanged, so callers
    can catch ``TypeError``, ``msgspec.ValidationError`` and
    ``msgspec.DecodeError`` directly.
    """

    def to_dict(
        self,
        includes: set[str] | None = None,
        excludes: set[str] | None = None,
        exclude_none: bool = False,
    ) -> dict[str, Any]:

        kvs = cast("dict[str, Any]", to_builtins(self))
        fields = set(kvs.keys())

        if includes:
            fields &= includes

        if excludes:
            fields -= excludes

        if exclude_none:
            fields = {f for f in fields if kvs[f] is not None}

        return {f: kvs[f] for f in fields}

    def to_jsonb(
        self,
        includes: set[str] | None = None,
        excludes: set[str] | None = None,
        exclude_none: bool = False,
    ) -> bytes:

        return json.encode(
            self.to_dict(
                includes=includes,
                excludes=excludes,
                exclude_none=exclude_none,
            ),
            order="sorted",
        )

    def to_json(
        self,
        includes: set[str] | None = None,
        excludes: set[str] | None = None,
        exclude_none: bool = False,
    ) -> str:

        return self.to_jsonb(
            includes=includes,
            excludes=excludes,
            exclude_none=exclude_none,
        ).decode("utf-8")

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> Self:

        return convert(obj, type=cls)

    @classmethod
    def from_jsonb(cls, obj: bytes, strict: bool = True) -> Self:

        return json.decode(obj.decode("utf-8"), type=cls, strict=strict)

    @classmethod
    def from_json(cls, obj: str, strict: bool = True) -> Self:

        return json.decode(obj, type=cls, strict=strict)

    @classmethod
    def from_toml(cls, obj: str, strict: bool = True) -> Self:

        return toml.decode(obj, type=cls, strict=strict)

    @classmethod
    def from_yaml(cls, obj: str, strict: bool = True) -> Self:

        return yaml.decode(obj, type=cls, strict=strict)
