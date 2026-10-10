import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized


__all__ = (
    "MasterPresence",
    "SubprocessPresence",
)


class MasterPresence:
    """Master-side view of the liveness of every worker.

    One instance covers the whole worker set, the way the map it replaced did:
    ``presence()`` returns every watched worker's last beat. Comparing that map
    against the clock is how the visor tells a worker that is stuck apart from
    one that is merely idle: ``is_alive`` stays true while a stalled worker
    stops writing.
    """

    def __init__(self) -> None:

        self._cells: dict[str, Synchronized[float]] = {}

    def watch(self, ident: str, cell: "Synchronized[float]") -> None:
        """Take over the cell a worker beats on, replacing any earlier one."""

        self._cells[ident] = cell

    def presence(self) -> dict[str, float]:
        """The last beat of every watched worker.

        Each cell is read through its object, without taking its lock: the lock
        exists for read-modify-write sequences and nothing here does one. Taking
        it would mean waiting on a worker that died -- or was stopped -- while
        holding it, which would wedge the visor for good.
        """

        return {ident: cell.get_obj().value for ident, cell in self._cells.items()}


class SubprocessPresence:
    """Subprocess-side view of a worker's liveness.

    Borrows the cell the master created: the master owns it and frees it, so
    nothing here closes it.
    """

    def __init__(self, heartbeat: "Synchronized[float]") -> None:

        self._heartbeat = heartbeat

    def heartbeat(self) -> None:

        self._heartbeat.value = time.time()
