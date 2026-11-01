"""The weather-stream Spark container was OOM-killed at 04:51 and stayed down (exit 137).
Weather stopped landing in the lake ~3 hours ago; seismic is fine and last night's run
was green. Correct fix: restart weather-stream (it resumes from its Kafka checkpoint)."""
from datetime import datetime, timezone

from evals.scenario import Scenario
from evals.world import STATIONS, healthy_world

NOW = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    world = healthy_world(NOW)
    world.lake_lag_minutes["weather"] = 189
    world.station_lag_minutes.update({sid: 190 + i * 7 for i, sid in enumerate(sorted(STATIONS))})
    world.containers["weather-stream"] = {"state": "exited", "status": "Exited (137) 3 hours ago",
                                          "exit_code": 137, "oom_killed": True}
    world.container_logs["weather-stream"] = "\n".join([
        "2026-10-05T04:49:58 INFO MicroBatchExecution: Streaming query made progress: batchId=8812",
        "2026-10-05T04:51:12 WARN TaskMemoryManager: Failed to allocate a page (67108864 bytes), try again.",
        "2026-10-05T04:51:13 ERROR Executor: Exception in task 0.0 in stage 17624.0 (TID 17624)",
        "java.lang.OutOfMemoryError: Java heap space",
        "2026-10-05T04:51:13 ERROR SparkUncaughtExceptionHandler: Uncaught exception in thread "
        "Thread[Executor task launch worker for task 0.0,5,main]",
    ])
    return Scenario(
        id="container_oom",
        question="The weather map hasn't moved in hours. What's going on?",
        world=world,
        expected_category="stale_streaming",
        must_mention=["weather-stream"],
        must_not_mention=["glue"],
        runbook_covered=True,          # runbook_check_freshness_failed.md: restart the stopped container
        fix_options=[[{"action": "restart_container", "container": "weather-stream"}]],
        notes="Only weather is stale and seismic-stream is up, so restarting the producer or Kafka is wrong.",
    )
