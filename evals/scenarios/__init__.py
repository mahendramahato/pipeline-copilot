"""All eval scenarios, by id. Add each new scenario here."""
from evals.scenarios import (
    concurrent_glue_run, container_oom, cpu_saturation, duplicates, glue_code_bug, healthy,
    host_memory_pressure, missed_day, null_spike, schema_drift, stuck_task, transient_retry,
)

SCENARIOS = {
    "schema_drift_magnitude": schema_drift.build,
    "null_spike_station": null_spike.build,
    "transient_sync_retry": transient_retry.build,
    "duplicate_replay": duplicates.build,
    "glue_code_bug": glue_code_bug.build,
    "stuck_task": stuck_task.build,
    # Scenarios where the right answer includes an automated fix
    "container_oom": container_oom.build,
    "concurrent_glue_run": concurrent_glue_run.build,
    "missed_day_backfill": missed_day.build,
    # Resources: the VM ran out of memory / CPU because of ANOTHER app
    "host_memory_pressure": host_memory_pressure.build,
    "cpu_saturation": cpu_saturation.build,
    "healthy_control": healthy.build,
}
