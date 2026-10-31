"""Script semantics on the Cluster topology (R5).

Research expectation (task research-redis-ha.md §5, conclusion 2): structurally
incompatible. Every script is invoked with ``keys=[]`` and builds its keys
from ARGV, so the cluster client routes each invocation to an arbitrary
primary. A script that touches more than one key then executes on a node that
cannot hold all of them, and the script either aborts (``redis.call`` on a
non-local key raises through the broker) or returns its error envelope (a
``pcall``'ed command), which the broker degrades to its neutral result.

The contract these cases pin is therefore the *brokering* one, not a success
path: a multi-key operation must never complete as if it had succeeded. Which
failure flavor (abort vs degraded neutral result) a given script takes depends
on where the random routing lands, so the cases normalize both into "no
successful outcome" before asserting.
"""

import time

import pytest
from redis.exceptions import ResponseError

from binomic.broker import Message, MessageEntry
from test_integration.ha.conftest import BrokerFactory

pytestmark = [pytest.mark.integration, pytest.mark.ha]

# Distinct keys per invocation cannot land on the primary that holds the
# stream: enqueue touches stream + hash, two different slots, hence two
# different primaries. A single sampling round can still succeed by luck,
# so the case samples enough rounds to make "all lucky" implausible.
ROUNDS = 6


@pytest.mark.integration
@pytest.mark.ha
class TestClusterScripts:
    async def test_enqueue_never_completes(
        self, make_cluster_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_cluster_broker(["solo"])
        outcomes: list[object] = []
        for _ in range(ROUNDS):
            try:
                outcomes.append(await broker.enqueue("solo", Message(task="noop")))
            except ResponseError:
                outcomes.append(None)

        assert all(item is None for item in outcomes), (
            f"enqueue completed successfully in {ROUNDS} cluster rounds: {outcomes}"
        )

    async def test_multi_queue_acquire_never_delivers(
        self, make_cluster_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_cluster_broker(["multi-a", "multi-b"])

        # XREADGROUP inside the acquire script spans two streams on two
        # different primaries; the script always errors and the broker
        # degrades it to an empty read.
        assert await broker.acquire("consumer-1", count=10) == []

    async def test_multi_key_scripts_never_report_progress(
        self, make_cluster_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_cluster_broker(["solo"])
        entry = MessageEntry(
            id="0-1",
            queue="solo",
            message=Message(task="noop"),
            enqueued_at=time.time(),
        )

        # ack/dead/retry each touch hash + stream on different primaries;
        # none of them may report a successful transition (1 / 1).
        acked: list[object] = []
        dead: list[object] = []
        retried: list[object] = []
        for _ in range(ROUNDS):
            for sink, call in (
                (acked, broker.ack(entry)),
                (dead, broker.dead(entry)),
                (retried, broker.retry(entry)),
            ):
                try:
                    sink.append(await call)
                except ResponseError:
                    sink.append(None)

        assert all(item in (0, None) for item in acked + dead + retried), (
            f"a multi-key script reported progress on cluster: "
            f"ack={acked} dead={dead} retry={retried}"
        )

    async def test_single_key_script_routing_is_random(
        self, make_cluster_broker: BrokerFactory
    ) -> None:

        broker, _ = await make_cluster_broker(["solo"])

        # observe touches exactly one key (the stream): it succeeds only
        # when the keys=[] routing happens to land on that key's primary.
        # The random routing itself is the incompatibility under test, so
        # the pinned expectation is "not reliably successful".
        samples = 15
        outcomes: list[object] = []
        for _ in range(samples):
            try:
                outcomes.append(await broker.observe("solo"))
            except ResponseError:
                outcomes.append(None)

        successes = [item for item in outcomes if item is not None]
        assert len(successes) < samples, (
            f"all {samples} single-key observes succeeded; keys=[] routing "
            f"behaved deterministically: {outcomes}"
        )
