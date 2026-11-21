"""Scheduling-pipeline package.

Shared settings live here rather than in ``tasks`` (same reason as the parent
package): importing them must not register tasks.
"""

SCHEDULE_QUEUE: str = "schedule-pipeline"

# The interval stays short so the suite sees a trigger fire without a long wait.
INTERVAL_SECONDS: float = 0.5
