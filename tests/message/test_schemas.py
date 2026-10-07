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

    def test_enqueued_at_defaults_to_none(self) -> None:

        msg = Message(name="noop")

        assert msg.enqueued_at is None


class TestMessageSerialization:
    def test_to_json_sorts_keys(self) -> None:

        msg = Message(name="noop", enqueued_at=1.0, id=UUID(int=0))

        assert msg.to_json().startswith('{"args":')

    def test_round_trip_preserves_fields(
        self,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message(args=[1, "two"], kwargs={"key": 3})

        restored = Message.from_json(msg.to_json())

        assert restored == msg

    def test_to_json_contains_core_fields(
        self,
        make_message: Callable[..., Message],
    ) -> None:

        raw = make_message(name="deploy", enqueued_at=1.0).to_json()

        assert '"name":"deploy"' in raw
        assert '"enqueued_at":1.0' in raw

    def test_from_json_accepts_full_payload(
        self,
        make_message: Callable[..., Message],
    ) -> None:

        msg = make_message()

        restored = Message.from_json(msg.to_json())

        assert restored.id == msg.id
        assert restored.name == msg.name
        assert restored.enqueued_at == msg.enqueued_at
