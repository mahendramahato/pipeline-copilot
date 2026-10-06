"""NOAA's Denver station (KDEN) started sending readings without a temperature at
midday yesterday: a sensor or feed problem upstream. Other stations and columns are
fine, rows keep arriving, every run is green. NOT schema drift: one station only."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    world = healthy_world(NOW)
    day = (NOW.date() - timedelta(days=1)).isoformat()
    for row in world.tables["raw_weather"]:
        if row["station_id"] == "KDEN" and row["date"] == day and row["observed_at"].hour >= 12:
            row["temperature_c"] = None
            row["z_score"] = None                  # can't score a missing temperature
    world.recurate()
    return Scenario(
        id="null_spike_station",
        question="Denver's temperature chart has had a gap since around midday yesterday. "
                 "Is something wrong with our pipeline?",
        world=world,
        expected_category="bad_upstream_data",
        must_mention=["kden", "null"],
        must_not_mention=["partition"],
        runbook_covered=False,
        notes="Partial NULLs at one station; other stations fine, so it's upstream data, not drift.",
    )
