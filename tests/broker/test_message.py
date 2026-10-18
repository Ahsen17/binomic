from collections.abc import Callable
from uuid import UUID

import pytest

from binomic.broker import Message, MessageEntry


class TestMessageDefaults:
    def test_id_defaults_to_uuid7(self) -> None:

        msg = Message(task="noop")

        assert isinstance(msg.id, UUID)
        assert msg.id.version == 7

    def test_ids_are_unique_per_instance(self) -> None:

        first = Message(task="noop")
        second = Message(task="noop")

        assert first.id != second.id

    def test_args_kwargs_default_none(self) -> None:
        """Parameterless tasks are the common case; None unpacks through the
        handler's ``args or ()`` without materializing containers."""

        msg = Message(task="noop")

        assert msg.args is None
        assert msg.kwargs is None

    def test_attempt_defaults_to_one(self) -> None:

        assert Message(task="noop").attempt == 1


class TestMessageSerialization:
    def test_round_trip_preserves_fields(
        self, make_message: Callable[..., Message]
    ) -> None:

        msg = make_message(attempt=2, args=[1, "two"], kwargs={"key": 3})

        assert Message.from_json(msg.to_json()) == msg

    def test_round_trip_preserves_none_containers(self) -> None:

        msg = Message(task="noop")

        restored = Message.from_json(msg.to_json())

        assert restored.args is None
        assert restored.kwargs is None

    def test_to_json_contains_core_fields(
        self, make_message: Callable[..., Message]
    ) -> None:

        raw = make_message(task="deploy").to_json()

        assert '"id":"' in raw
        assert '"task":"deploy"' in raw
        assert '"attempt":1' in raw
        assert '"args":null' in raw
        assert '"kwargs":null' in raw


class TestMessageEntry:
    def test_from_dict_assembles_fields(
        self, make_message: Callable[..., Message]
    ) -> None:

        msg = make_message(task="deploy", args=[1])
        entry = MessageEntry.from_dict(
            {
                "id": "1-1",
                "queue": "default",
                "message": msg.to_dict(),
                "enqueued_at": 1_760_000_000,
            }
        )

        assert entry.id == "1-1"
        assert entry.queue == "default"
        assert entry.message == msg
        assert entry.enqueued_at == 1_760_000_000

    def test_enqueued_at_is_strict_int(self) -> None:
        """A float timestamp (e.g. a stale pre-seconds-era body) must fail
        loudly instead of silently truncating."""

        with pytest.raises(Exception, match="Failed to parse"):
            MessageEntry.from_dict(
                {
                    "id": "1-1",
                    "queue": "default",
                    "message": Message(task="noop").to_dict(),
                    "enqueued_at": 1_760_000_000.5,
                }
            )
