# Runbook: backfilling / re-curating a day

## When
Curated data is missing or wrong for specific dates and the cause is fixed.

## How
- The DAG has a `date` param (yyyy-MM-dd) made for backfills. Trigger one run per day:
  Airflow UI "Trigger DAG w/ config" with {"date": "2026-10-01"}, or
  airflow dags trigger daily_lake_maintenance --conf '{"date": "2026-10-01"}'
- max_active_runs=1: several triggered runs queue and run one at a time.
- Re-curating is safe: the Glue job overwrites only that date's partition.

## Caveats
- A manual run also runs check_freshness. If streaming is currently stale, the run fails
  there before curating; fix streaming first or clear only curate_day.
- The Glue job allows one concurrent run; parallel runs fail with
  ConcurrentRunsExceededException.
- Verify after each run: curated rows for that date > 0 and close to raw.
