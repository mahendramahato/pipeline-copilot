"""sync_to_s3 hit an S3 connect timeout on its first try; Airflow retried after 5
minutes and it succeeded. Data is complete. Correct answer: transient, nothing to do."""
from datetime import datetime, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    world = healthy_world(NOW)
    run = world.dag_runs[0]
    run_id = run["dag_run_id"]
    world.task(run_id, "sync_to_s3").update(try_number=2, duration=17.2)
    run["end_date"] = "2026-10-05T00:38:05Z"       # ~6 minutes later than usual (retry delay)

    # Try 1 failed; try 2 uses the normal successful sync log already in the world
    world.task_logs[(run_id, "sync_to_s3", 1)] = "\n".join([
        "00:30:07 INFO     syncing weather...",
        "00:31:07 ERROR    upload failed: data/lake/weather/date=2026-10-04/part-00003.parquet to "
        "s3://weather-seismic-lake-mahendra/raw/weather/date=2026-10-04/part-00003.parquet "
        'Connect timeout on endpoint URL: "https://weather-seismic-lake-mahendra.s3.us-west-2.amazonaws.com/"',
        "00:31:07 ERROR    Command exited with return code 1",
        "00:31:07 ERROR    Task failed with exception: AirflowException: Bash command failed. "
        "The command returned a non-zero exit code 1.",
        "00:31:07 INFO     Marking task as UP_FOR_RETRY. dag_id=daily_lake_maintenance, task_id=sync_to_s3",
    ])
    return Scenario(
        id="transient_sync_retry",
        question="Last night's pipeline run finished several minutes later than usual. "
                 "Did anything go wrong, and do I need to do anything?",
        world=world,
        expected_category="transient_failure",
        must_mention=["sync_to_s3", "timeout"],
        must_not_mention=["partition"],
        runbook_covered=False,
        notes="Retry recovered; data complete. Over-reacting (backfill, escalation) would be wrong.",
    )
