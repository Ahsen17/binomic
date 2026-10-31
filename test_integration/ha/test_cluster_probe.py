"""Direct-client probes recording the raw Cluster failure texts (R5 evidence).

These bypass the broker on purpose: they document what Redis Cluster itself
does with the invocation shapes the broker's scripts use, so the reliability
doc can quote the exact constraint instead of a paraphrase. The printed
probe lines are the evidence trail (run pytest with -s to capture them).
"""

import pytest
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisError
from redis.typing import KeyT, StreamIdT

pytestmark = [pytest.mark.integration, pytest.mark.ha]

MULTI_KEY_EVAL = "return redis.call('EXISTS', KEYS[1], KEYS[2])"
NO_KEYS_EVAL = "return redis.call('SET', ARGV[1], 'probe')"


@pytest.mark.integration
@pytest.mark.ha
class TestClusterCrossSlotProbe:
    async def test_multi_key_eval_is_rejected(self, cluster_probe: RedisCluster) -> None:

        # redis-py's cluster middleware rejects the multi-key EVAL client-side
        # (its own wording), before any server-side CROSSSLOT could occur.
        with pytest.raises(RedisError, match="hash to the same slot"):
            await cluster_probe.eval(MULTI_KEY_EVAL, 2, "ha-probe:a", "ha-probe:b")

    async def test_no_keys_eval_hits_non_local_key(
        self, cluster_probe: RedisCluster
    ) -> None:

        # keys=[] routes the script to an arbitrary primary; with three
        # primaries, twelve attempts against distinct keys are overwhelmingly
        # likely to land somewhere the key does not live.
        errors: list[str] = []
        for index in range(12):
            try:
                await cluster_probe.eval(NO_KEYS_EVAL, 0, f"ha-probe:nokeys:{index}")
            except RedisError as err:
                errors.append(str(err))

        assert errors, "all 12 keys=[] EVALs succeeded; routing never hit a non-local key"
        assert any("non local key" in error.lower() for error in errors), (
            f"unexpected cluster error shapes: {errors}"
        )

    async def test_multi_stream_xreadgroup_rejected(
        self, cluster_probe: RedisCluster
    ) -> None:

        # The client rejects multi-stream reads across slots before anything
        # reaches the server -- the acquire script's invocation shape.
        streams: dict[KeyT, StreamIdT] = {
            "ha-probe:stream-1": ">",
            "ha-probe:stream-2": ">",
        }
        with pytest.raises(RedisError) as excinfo:
            await cluster_probe.xreadgroup(
                "probe-group", "probe-consumer", streams=streams, count=1
            )

        assert "slot" in str(excinfo.value).lower()

    async def test_probe_reports_cluster_shape(self, cluster_probe: RedisCluster) -> None:

        # Sanity anchor for the evidence: the probed target really is a
        # 3-primary cluster, so the failures above came from that topology.
        info = await cluster_probe.cluster_info()

        assert info["cluster_state"] == "ok"
        assert int(info["cluster_slots_assigned"]) == 16384
