"""Polling helpers shared by the integration pipelines."""

from typing import TYPE_CHECKING

import anyio
import pytest

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


TIMEOUT: float = 30.0


async def wait_until(
    predicate: "Callable[[], Awaitable[object]]",
    *,
    timeout: float = TIMEOUT,
) -> None:
    """Poll `predicate` until its result is truthy, failing when it never is.

    Task-side signals (a key the task wrote, a counter it bumped) can land
    before the worker has acked or dead-lettered the message, so assertions
    about the consumer state must wait instead of reading once.
    """

    with anyio.move_on_after(timeout):
        while not await predicate():
            await anyio.sleep(0.1)
        return

    pytest.fail(f"condition not met within {timeout}s")
