from binomic.broker.types import Entry, Fields


class TestEntry:
    def test_is_named_tuple_with_three_fields(self) -> None:

        entry = Entry("default", "1-0", {"id": "1", "message": "{}"})

        assert entry.queue == "default"
        assert entry.msg_id == "1-0"
        assert entry.fields == {"id": "1", "message": "{}"}

    def test_unpacks_positionally(self) -> None:

        queue, msg_id, fields = Entry("q", "1-1", {"id": "x", "message": "m"})

        assert (queue, msg_id, fields) == ("q", "1-1", {"id": "x", "message": "m"})


class TestFields:
    def test_declares_id_and_message(self) -> None:

        assert set(Fields.__annotations__) == {"id", "message"}
