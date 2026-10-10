import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from pytest_mock import MockerFixture

from binomic.worker.presence import MasterPresence, SubprocessPresence

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized


class TestSubprocessPresence:
    def test_heartbeat_stamps_the_current_time(
        self, heartbeat: "Synchronized[float]", mocker: MockerFixture
    ) -> None:

        mocker.patch("binomic.worker.presence.time.time", return_value=1234.5)

        SubprocessPresence(heartbeat).heartbeat()

        assert heartbeat.value == 1234.5

    def test_heartbeat_writes_a_current_timestamp(
        self, heartbeat: "Synchronized[float]"
    ) -> None:

        before = time.time()

        SubprocessPresence(heartbeat).heartbeat()

        assert before <= heartbeat.value <= time.time()


class TestMasterPresence:
    def test_reads_the_cell_of_a_watched_worker(
        self, heartbeat: "Synchronized[float]"
    ) -> None:

        presence = MasterPresence()
        presence.watch("worker-0", heartbeat)

        heartbeat.value = 123.5

        assert presence.presence() == {"worker-0": 123.5}

    def test_reads_zero_before_the_first_beat(
        self, heartbeat: "Synchronized[float]"
    ) -> None:

        presence = MasterPresence()
        presence.watch("worker-0", heartbeat)

        assert presence.presence() == {"worker-0": 0.0}

    def test_sees_what_the_worker_wrote(
        self, heartbeat: "Synchronized[float]", mocker: MockerFixture
    ) -> None:

        mocker.patch("binomic.worker.presence.time.time", return_value=42.0)

        presence = MasterPresence()
        presence.watch("worker-0", heartbeat)
        SubprocessPresence(heartbeat).heartbeat()

        assert presence.presence() == {"worker-0": 42.0}

    def test_covers_every_watched_worker(
        self, make_heartbeat: Callable[[], "Synchronized[float]"]
    ) -> None:
        """One monitor over the whole set: each worker reads off its own cell."""

        presence = MasterPresence()
        presence.watch("worker-0", make_heartbeat())
        presence.watch("worker-1", make_heartbeat())

        assert presence.presence() == {"worker-0": 0.0, "worker-1": 0.0}

    def test_watching_again_replaces_only_that_worker(
        self, make_heartbeat: Callable[[], "Synchronized[float]"]
    ) -> None:
        """A restarted worker's fresh cell supersedes the one it left behind."""

        abandoned = make_heartbeat()
        untouched = make_heartbeat()
        presence = MasterPresence()
        presence.watch("worker-0", abandoned)
        presence.watch("worker-1", untouched)

        abandoned.value = 7.0
        untouched.value = 8.0
        presence.watch("worker-0", make_heartbeat())

        assert presence.presence() == {"worker-0": 0.0, "worker-1": 8.0}
