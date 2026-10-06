"""All eval scenarios, by id. Add each new scenario here."""
from evals.scenarios import (
    duplicates, glue_code_bug, healthy, null_spike, schema_drift, stuck_task, transient_retry,
)

SCENARIOS = {
    "schema_drift_magnitude": schema_drift.build,
    "null_spike_station": null_spike.build,
    "transient_sync_retry": transient_retry.build,
    "duplicate_replay": duplicates.build,
    "glue_code_bug": glue_code_bug.build,
    "stuck_task": stuck_task.build,
    "healthy_control": healthy.build,
}
