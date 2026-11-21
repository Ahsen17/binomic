import msgspec
import pytest
from msgspec import Struct

from binomic.base.schemas import BaseStruct


class Payload(BaseStruct):
    name: str
    count: int = 0


class TestBaseStruct:
    def test_is_msgspec_struct(self) -> None:

        assert issubclass(BaseStruct, Struct)

    def test_to_json_encodes_fields(self) -> None:

        assert Payload(name="a", count=2).to_json() == '{"count":2,"name":"a"}'

    def test_from_json_round_trip(self) -> None:

        payload = Payload.from_json('{"name":"a","count":2}')

        assert payload == Payload(name="a", count=2)

    def test_to_dict_round_trip(self) -> None:

        payload = Payload(name="a", count=2)

        assert payload.to_dict() == {"name": "a", "count": 2}
        assert Payload.from_dict(payload.to_dict()) == payload

    def test_to_json_rejects_unsupported_values(self) -> None:

        class Broken(BaseStruct):
            bad: object

        with pytest.raises(TypeError, match="unsupported"):
            Broken(bad=object()).to_json()

    def test_to_dict_rejects_unsupported_values(self) -> None:

        class Broken(BaseStruct):
            bad: object

        with pytest.raises(TypeError, match="unsupported"):
            Broken(bad=object()).to_dict()

    def test_from_json_rejects_malformed_input(self) -> None:

        with pytest.raises(msgspec.DecodeError):
            Payload.from_json("not-json")

    def test_from_json_rejects_schema_mismatch(self) -> None:

        with pytest.raises(msgspec.ValidationError):
            Payload.from_json('{"name":"a","count":"not-an-int"}')

    def test_from_dict_rejects_schema_mismatch(self) -> None:

        with pytest.raises(msgspec.ValidationError):
            Payload.from_dict({"name": "a", "count": "not-an-int"})
