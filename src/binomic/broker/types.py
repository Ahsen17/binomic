from typing import NamedTuple, TypedDict

__all__ = (
    "Entry",
    "Fields",
)


class Fields(TypedDict):
    """Fields in the Entry."""

    id: str
    message: str


class Entry(NamedTuple):
    """Entry in the broker."""

    queue: str
    msg_id: str
    fields: Fields
