"""curate_day has been 'running' for 3.5 hours, polling a Glue run that stays
RUNNING, although the job's configured timeout is 10 minutes. Yesterday isn't curated."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 4, 0, tzinfo=timezone.utc)
GLUE_RUN = "jr_4b1e0c9d27f3a6e85c2d1f0a9b8e7c6d5f4a3b21"


def build() -> Scenario:
    world = healthy_world(NOW)
    run = world.dag_runs[0]
    run_id = run["dag_run_id"]
    yesterday = (NOW.date() - timedelta(days=1)).isoformat()
    run.update(state="running", end_date=None)
    world.task(run_id, "curate_day").update(state="running", duration=None)

    polls = [f"{m // 60:02d}:{m % 60:02d}:21 INFO     Polling for AWS Glue Job curate-daily "
             f"current run state with run_id {GLUE_RUN}: RUNNING"
             for m in range(31, 240, 30)]                    # every 30 min, 00:31 to 03:31
    world.task_logs[(run_id, "curate_day")] = "\n".join(
        [f"00:30:21 INFO     Starting AWS Glue Job: curate-daily with --DATE {yesterday}"] + polls
    )
    for t in ("curated_weather", "curated_seismic", "curated_weather_daily"):
        world.tables[t] = [r for r in world.tables[t] if r["date"] != yesterday]
    return Scenario(
        id="stuck_task",
        question="It's 4am and the dashboard still doesn't show yesterday's data. "
                 "Is the nightly run still going?",
        world=world,
        expected_category="stuck_task",
        must_mention=["curate_day"],
        must_not_mention=["partition"],
        runbook_covered=False,
        notes="Running 3.5h vs a 10-minute job timeout (in the overview doc) means stuck, not slow.",
    )
