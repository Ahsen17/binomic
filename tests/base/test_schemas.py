import pytest
from msgspec import Struct

from binomic.base.exceptions import DeserializationError, SerializationError
from binomic.base.schemas import BaseStruct


class Payload(BaseStruct):
    name: str
    count: int = 0


class TestBaseStruct:
    def test_is_msgspec_struct(self) -> None:

        assert issubclass(BaseStruct, Struct)

    def test_to_json_encodes_fields(self) -> None:

        assert Payload(name="a", count=2).to_json() == '{"name":"a","count":2}'

    def test_from_json_round_trip(self) -> None:

        payload = Payload.from_json('{"name":"a","count":2}')

        assert payload == Payload(name="a", count=2)

    def test_to_json_wraps_errors(self) -> None:

        class Broken(BaseStruct):
            bad: object

        with pytest.raises(SerializationError, match="Failed to serialize"):
            Broken(bad=object()).to_json()

    def test_from_json_wraps_errors(self) -> None:

        with pytest.raises(DeserializationError, match="Failed to deserialize"):
            Payload.from_json("not-json")

    def test_from_json_rejects_schema_mismatch(self) -> None:

        with pytest.raises(DeserializationError):
            Payload.from_json('{"name":"a","count":"not-an-int"}')

    def test_repr_is_json(self) -> None:

        assert repr(Payload(name="a")) == '{"name":"a","count":0}'
