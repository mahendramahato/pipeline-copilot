# Technology: Apache Airflow, the nightly batch orchestrator

## What is Apache Airflow?
Apache Airflow is a workflow orchestrator. Workflows are written in Python as DAGs (directed
acyclic graphs) of tasks; Airflow schedules them, runs tasks in dependency order, retries
failures, and keeps a history of every run and every task's logs. It does not process data itself:
it tells other systems (scripts, AWS Glue, ...) what to run and when.

## What Airflow does in this pipeline
Airflow 3.3 runs one DAG, daily_lake_maintenance, every night at 00:30 UTC. It turns the
real-time local lake into the curated data lake on AWS:
check_freshness -> sync_to_s3 -> curate_day, and target_date -> curate_day.

## The DAG's tasks
- check_freshness: fails if the newest local Parquet file is older than 3 hours (a streaming job
  has probably stopped). Failing early avoids paying for a Glue run on a broken day.
- sync_to_s3: aws s3 sync from the VM's local lake to s3://weather-seismic-lake-mahendra/raw/.
- target_date: the day to curate, either the date param (manual backfill) or yesterday.
- curate_day: starts the AWS Glue job curate-daily for that date and waits for it to finish.
Settings: 1 retry after 5 minutes, catchup off, max_active_runs 1 (the Glue job allows only one
run at a time).

## How Airflow runs here
Airflow runs in Docker on the Oracle Cloud VM (airflow standalone with a Postgres metadata
database, LocalExecutor). The UI is bound to the VM's localhost and reached through an SSH tunnel.
Pipeline Copilot reads Airflow through its REST API (/api/v2) as the read-only user agent_viewer,
from inside the same Docker network.

## What Airflow can and cannot tell you
A green run means every task reported success, not that the data is right. In 2026 a curated job
"succeeded" for four nights while reading 0 rows. Airflow logs show task failures, retries and
timings; data problems (missing rows, NULLs, duplicates) need checks in Athena.
