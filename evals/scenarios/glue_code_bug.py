"""A bad deploy: the Glue script now references a column `temp_c` that doesn't
exist. curate_day failed on both attempts, the DAG run failed, and yesterday is
missing from every curated table."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
GLUE_ERROR = (
    "AWS Glue Job: curate-daily status: FAILED. Run Id: jr_9f2c41d07be3a8c55e1f6d2a4b7c9e0f13a5d8b2 "
    "ErrorMessage: AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function "
    "parameter with name `temp_c` cannot be resolved. Did you mean one of the following? "
    "[`temperature_c`, `station_id`, `observed_at`]"
)


def build() -> Scenario:
    world = healthy_world(NOW)
    run = world.dag_runs[0]
    run_id = run["dag_run_id"]
    yesterday = (NOW.date() - timedelta(days=1)).isoformat()
    run.update(state="failed", end_date="2026-10-05T00:37:40Z")
    world.task(run_id, "curate_day").update(state="failed", try_number=2, duration=48.3)

    for attempt, start, outcome in [(1, "00:30:20", "UP_FOR_RETRY"), (2, "00:36:52", "FAILED")]:
        world.task_logs[(run_id, "curate_day", attempt)] = "\n".join([
            f"{start} INFO     Starting AWS Glue Job: curate-daily with --DATE {yesterday}",
            f"{start} ERROR    {GLUE_ERROR}",
            f"{start} ERROR    Task failed with exception: AirflowException: Error in Glue job curate-daily",
            f"{start} INFO     Marking task as {outcome}. dag_id=daily_lake_maintenance, task_id=curate_day",
        ])

    # The failed run curated nothing: yesterday is missing downstream
    for t in ("curated_weather", "curated_seismic", "curated_weather_daily"):
        world.tables[t] = [r for r in world.tables[t] if r["date"] != yesterday]
    return Scenario(
        id="glue_code_bug",
        question="The nightly pipeline failed. What broke?",
        world=world,
        expected_category="code_bug",
        must_mention=["temp_c"],
        must_not_mention=["partition"],
        runbook_covered=True,          # runbook_glue_job_failed.md lists "script error after a change"
        notes="Table definitions unchanged, so the bad column name points at the script, not the data.",
    )
