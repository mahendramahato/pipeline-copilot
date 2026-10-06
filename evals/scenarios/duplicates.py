"""A stream replay re-wrote one day of weather readings to raw (same readings,
later ingested_at). Curation deduplicates, so curated is correct. A good answer
names the duplicates AND says downstream is unaffected."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    world = healthy_world(NOW)
    day = (NOW.date() - timedelta(days=2)).isoformat()      # 2026-10-03
    replayed = [{**r, "ingested_at": r["ingested_at"] + timedelta(hours=2)}
                for r in world.tables["raw_weather"] if r["date"] == day]
    world.tables["raw_weather"] += replayed
    world.recurate()            # dedup keeps the latest per (station_id, observed_at): curated stays clean
    return Scenario(
        id="duplicate_replay",
        question=f"raw_weather has about twice as many rows as normal for {day}. "
                 "Is our weather data corrupted?",
        world=world,
        expected_category="duplicates",
        must_mention=["duplicate"],
        must_not_mention=["partition"],
        runbook_covered=False,
        notes="Raw doubled by replay; curated dedup handled it. Ideal answer: downstream unaffected.",
    )
