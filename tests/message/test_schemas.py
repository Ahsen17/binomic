from collections.abc import Callable
from uuid import UUID

from binomic.message import Message


class TestMessageDefaults:
    def test_id_defaults_to_uuid7(self) -> None:

        msg = Message(name="noop")

        assert isinstance(msg.id, UUID)
        assert msg.id.version == 7

    def test_ids_are_unique_per_instance(self) -> None:

        first = Message(name="noop")
        second = Message(name="noop")

        assert first.id != second.id

    def test_args_kwargs_default_empty(self) -> None:

        msg = Message(name="noop")

        assert msg.args == []
        assert msg.kwargs == {}

    def test_attempt_defaults_to_one(self) -> None:

        msg = Message(name="noop")

        assert msg.attempt == 1


class TestMessageSerialization:
    def test_to_json_sorts_keys(self) -> None:

        msg = Message(name="noop", id=UUID(int=0))

        assert msg.to_json().startswith('{"args":')

    def test_round_trip_preserves_fields(
        self,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(attempt=2, args=[1, "two"], kwargs={"key": 3})

        restored = Message.from_json(msg.to_json())

        assert restored == msg

    def test_to_json_contains_core_fields(
        self,
        make_message: Callable[..., Message],
    ) -> None:

        raw = make_message(name="deploy").to_json()

        assert '"id":"' in raw
        assert '"name":"deploy"' in raw
        assert '"attempt":1' in raw
        assert '"args":[]' in raw
        assert '"kwargs":{}' in raw
