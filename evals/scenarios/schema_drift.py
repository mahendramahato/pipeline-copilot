"""USGS renamed the magnitude field. The stream job parses JSON with a fixed schema,
so magnitude silently becomes NULL. Rows keep arriving and every DAG run is green."""
from datetime import datetime, timedelta, timezone

from evals.scenario import Scenario
from evals.world import healthy_world

NOW = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)


def build() -> Scenario:
    world = healthy_world(NOW)

    # --- The one thing that breaks ---
    broken_from = (NOW.date() - timedelta(days=2)).isoformat()      # 2026-10-03
    for row in world.tables["raw_seismic"]:
        if row["date"] >= broken_from:
            row["magnitude"] = None
            row["is_significant"] = None        # derived from magnitude upstream
    world.recurate()                            # the damage flows into curated, as it would for real

    return Scenario(
        id="schema_drift_magnitude",
        question="Earthquake magnitudes have been blank on the dashboard for the last couple "
                 "of days. What's going on?",
        world=world,
        expected_category="schema_drift",
        must_mention=["magnitude", "null"],
        must_not_mention=["partition"],           # the Phase 3-5 bug is a red herring here
        runbook_covered=True,                     # runbook_schema_drift.md covers NULL-from-rename
        notes="Row counts look normal; only a NULL check reveals it.",
    )
