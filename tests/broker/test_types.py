import time

from binomic.broker.types import Entry


class TestEntry:
    def test_is_named_tuple_with_three_fields(self) -> None:

        enqueued_at = time.time()

        entry = Entry(
            "default",
            "1-0",
            {
                "id": "1",
                "message": "{}",
                "enqueued_at": enqueued_at,
            },
        )

        assert entry.queue == "default"
        assert entry.msg_id == "1-0"
        assert entry.fields == {"id": "1", "message": "{}", "enqueued_at": enqueued_at}

    def test_unpacks_positionally(self) -> None:

        enqueued_at = time.time()

        queue, msg_id, fields = Entry(
            "q",
            "1-1",
            {
                "id": "x",
                "message": "m",
                "enqueued_at": enqueued_at,
            },
        )

        assert (queue, msg_id, fields) == (
            "q",
            "1-1",
            {
                "id": "x",
                "message": "m",
                "enqueued_at": enqueued_at,
            },
        )
