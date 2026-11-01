"""Someone manually re-curated 2026-10-02 at 00:29, which overlapped the scheduled 00:30
run. The scheduled curate_day hit ConcurrentRunsExceededException on both tries and
the run failed, so yesterday isn't curated. The manual run has finished since.
Correct fix: clear curate_day in the scheduled run (the clash is over)."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 7, 30, tzinfo=timezone.utc)
ERROR = ("AWS Glue Job: curate-daily status: FAILED. ErrorMessage: ConcurrentRunsExceededException: "
         "Concurrent runs exceeded for curate-daily (max concurrent runs: 1)")


def build() -> Scenario:
    world = healthy_world(NOW)
    run = world.dag_runs[0]
    run_id = run["dag_run_id"]
    yesterday = (NOW.date() - timedelta(days=1)).isoformat()
    run.update(state="failed", end_date="2026-10-05T00:36:31Z")
    world.task(run_id, "curate_day").update(state="failed", try_number=2, duration=2.3)
    for attempt, start, outcome in [(1, "00:30:21", "UP_FOR_RETRY"), (2, "00:36:29", "FAILED")]:
        world.task_logs[(run_id, "curate_day", attempt)] = "\n".join([
            f"{start} INFO     Starting AWS Glue Job: curate-daily with --DATE {yesterday}",
            f"{start} ERROR    {ERROR}",
            f"{start} ERROR    Task failed with exception: AirflowException: Error in Glue job curate-daily",
            f"{start} INFO     Marking task as {outcome}. dag_id=daily_lake_maintenance, task_id=curate_day",
        ])

    # The manual run that caused the clash: started 00:29, done by 00:40
    manual_id = "manual__2026-10-05T00:29:02+00:00"
    world.dag_runs.insert(1, {"dag_run_id": manual_id, "state": "success", "conf": {"date": "2026-10-02"},
                              "queued_at": "2026-10-05T00:29:02Z", "start_date": "2026-10-05T00:29:03Z",
                              "end_date": "2026-10-05T00:40:11Z"})
    world.task_instances[manual_id] = [
        {"task_id": t, "state": "success", "try_number": 1, "duration": d}
        for t, d in [("check_freshness", 1.0), ("target_date", 0.5), ("sync_to_s3", 15.8), ("curate_day", 610.2)]
    ]
    world.task_logs[(manual_id, "curate_day")] = (
        "00:29:20 INFO     Starting AWS Glue Job: curate-daily with --DATE 2026-10-02\n"
        "00:39:31 INFO     AWS Glue Job: curate-daily status: SUCCEEDED.")

    for t in ("curated_weather", "curated_seismic", "curated_weather_daily"):
        world.tables[t] = [r for r in world.tables[t] if r["date"] != yesterday]
    return Scenario(
        id="concurrent_glue_run",
        question="Last night's run is red. Can you figure out why and sort it out?",
        world=world,
        expected_category="transient_failure",
        must_mention=["concurrent"],
        must_not_mention=["temp_c", "partition"],
        runbook_covered=True,          # runbook_glue_job_failed.md: concurrency -> wait, then clear curate_day
        fix_options=[
            [{"action": "clear_task", "run_id": run_id, "task_id": "curate_day"}],
            [{"action": "backfill_date", "date": yesterday}],
        ],
        notes="The clash is over (manual run finished), so a retry now succeeds.",
    )
