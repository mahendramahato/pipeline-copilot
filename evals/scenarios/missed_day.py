"""Three nights ago sync_to_s3 timed out against S3 on both tries, so that run never
curated its day (2026-10-01). Later runs synced the files (s3 sync copies everything)
but each run curates only its own day, so 10-01 stayed a hole in every curated table.
Correct fix: backfill 2026-10-01 (or clear sync_to_s3 in that old run)."""
from datetime import datetime, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)
MISSING = "2026-10-01"
TIMEOUT = ('Connect timeout on endpoint URL: "https://weather-seismic-lake-mahendra.s3.us-west-2.amazonaws.com/"')


def build() -> Scenario:
    world = healthy_world(NOW)
    run_id = "scheduled__2026-10-02T00:30:00+00:00"
    run = next(r for r in world.dag_runs if r["dag_run_id"] == run_id)
    run.update(state="failed", end_date="2026-10-02T00:37:44Z")
    world.task(run_id, "sync_to_s3").update(state="failed", try_number=2, duration=61.0)
    world.task(run_id, "curate_day").update(state="upstream_failed", try_number=0, duration=None)
    for attempt, t in [(1, "00:31"), (2, "00:37")]:
        world.task_logs[(run_id, "sync_to_s3", attempt)] = "\n".join([
            f"{t}:07 INFO     syncing weather...",
            f"{t}:44 ERROR    upload failed: data/lake/weather/date={MISSING}/part-00000.parquet {TIMEOUT}",
            f"{t}:44 ERROR    Command exited with return code 1",
            f"{t}:44 INFO     Marking task as {'UP_FOR_RETRY' if attempt == 1 else 'FAILED'}. "
            "dag_id=daily_lake_maintenance, task_id=sync_to_s3",
        ])
    world.task_logs.pop((run_id, "curate_day"), None)
    for t in ("curated_weather", "curated_seismic", "curated_weather_daily"):
        world.tables[t] = [r for r in world.tables[t] if r["date"] != MISSING]
    return Scenario(
        id="missed_day_backfill",
        question="The daily temperature chart has a hole on October 1st. Why, and can you fix it?",
        world=world,
        expected_category="transient_failure",
        must_mention=["sync_to_s3"],
        must_not_mention=["temp_c"],
        runbook_covered=True,          # runbook_backfill.md
        fix_options=[
            [{"action": "backfill_date", "date": MISSING}],
            [{"action": "clear_task", "run_id": run_id, "task_id": "sync_to_s3"}],
        ],
        notes="Raw has 10-01 (synced by later runs); only curation is missing. The S3 timeout is long gone.",
    )
