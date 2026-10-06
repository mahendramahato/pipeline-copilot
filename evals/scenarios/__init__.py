"""All eval scenarios, by id. Add each new scenario here."""
from evals.scenarios import schema_drift

SCENARIOS = {
    "schema_drift_magnitude": schema_drift.build,
}

