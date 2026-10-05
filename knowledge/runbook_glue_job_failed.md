# Runbook: curate_day / Glue job failed

## Symptoms
- curate_day task failed in Airflow; the Glue job run ended FAILED, TIMEOUT or STOPPED.

## Likely causes
- ConcurrentRunsExceededException: another curate-daily run was still active
  (manual run overlapping the scheduled one).
- TIMEOUT: the job exceeded its 10 minute limit (unusually large day, or stuck).
- Script error after a change to curate_job.py (Python exception in the job log).
- IAM / S3 permission error for the Glue role.

## How to check
- curate_day task log: the Glue run id and final state, plus any error message.
- The full error is in the Glue job's CloudWatch logs (not visible to the agent's tools).

## Fix
- Concurrency: wait for the other run, then clear curate_day.
- Script error: fix, re-upload with phase4/create_glue_job.sh's upload step, then backfill.
- After any fix: verify curated rows for the date.
