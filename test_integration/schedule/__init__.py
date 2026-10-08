"""Scheduling-pipeline package.

Shared settings live here rather than in ``tasks`` (same reason as the parent
package): importing them must not register tasks.
"""

SCHEDULE_QUEUE: str = "schedule-pipeline"

# The interval stays short so the suite sees a trigger fire without a long
# wait. The delay is long enough that the test can tell "fired after the delay"
# from "fired on the next scheduler tick", which is what an ignored delay looks
# like from the outside.
DELAY_SECONDS: float = 3.0
INTERVAL_SECONDS: float = 0.5
