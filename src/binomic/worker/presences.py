import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized


__all__ = (
    "MasterPresence",
    "SlavePresence",
)


class MasterPresence:
    """The master's view of every worker's heartbeat cell.

    Cells are shared-memory values created by the master and handed to each
    worker. Reads deliberately skip the cell lock: the lock exists for
    read-modify-write, and waiting on it would block the visor forever if a
    worker died while holding it.
    """

    def __init__(self) -> None:

        self._slaves: dict[str, Synchronized[float]] = {}

    def watch(self, ident: str, slave: "Synchronized[float]") -> None:
        """Track ``slave``'s heartbeat cell under ``ident``.

        Re-watching the same ident replaces the cell, so a restarted worker
        starts from a fresh beat instead of inheriting its predecessor's.
        """

        self._slaves[ident] = slave

    def presence(self) -> dict[str, float]:
        """Return each watched worker's last beat, keyed by consumer name."""

        return {ident: slave.get_obj().value for ident, slave in self._slaves.items()}


class SlavePresence:
    """The worker side: write the current time into its heartbeat cell."""

    def __init__(self, heartbeat: "Synchronized[float]") -> None:

        self._heartbeat = heartbeat

    def heartbeat(self) -> None:
        """Stamp the current wall-clock time into the shared cell."""

        self._heartbeat.value = time.time()
