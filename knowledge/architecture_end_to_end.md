# Architecture: the end-to-end workflow and how every service connects

## The full workflow, step by step
1. Producer (Python) polls NOAA (15 weather stations) and the USGS past-hour earthquake feed every
   60 seconds and sends new readings to Kafka (topics weather, seismic).
2. Kafka buffers the messages durably (7 days).
3. Spark Structured Streaming (two jobs) reads Kafka every minute, deduplicates, scores weather
   anomalies and flags significant quakes, and writes date-partitioned Parquet to the VM's local lake.
4. Every night at 00:30 UTC, Airflow's DAG daily_lake_maintenance checks freshness, syncs the local
   lake to S3 raw/, and runs the AWS Glue job curate-daily for yesterday.
5. Glue writes compact, deduplicated tables to S3 curated/.
6. Athena queries raw and curated tables through the Glue Data Catalog.
7. The dashboard API (FastAPI + DuckDB) reads the live local lake for "now" and Athena curated
   tables for 7-day history; the React dashboard shows it at weather-seismic.duckdns.org.

## Two speeds: streaming lane and batch lane
Streaming (always on) answers "what is happening now": anomaly flags within about a minute,
in the local lake. Batch (nightly) produces quality data: one compact file per day, the latest
revision of each quake, and daily summaries in S3. So Athena raw tables are up to ~24 hours
behind real time (they update at the 00:30 sync) and curated tables contain up to yesterday.

## Where everything runs
Everything except the AWS services runs in Docker Compose on one Oracle Cloud ARM VM: kafka,
kafka-ui, producer, weather-stream, seismic-stream, airflow (+ postgres), api, web (Caddy +
React), and copilot (Pipeline Copilot, a separate project on the same Docker network). Services
restart automatically after crashes or reboots. AWS hosts S3, Glue, the Glue Data Catalog and Athena.

## How the services connect (network and access)
- Containers talk over the Docker network by name (producer -> kafka:9092, copilot -> airflow:8080).
- Only Caddy (container web) is public: ports 80/443, automatic HTTPS (Let's Encrypt) for
  weather-seismic.duckdns.org and pipeline-copilot.duckdns.org (DuckDNS points both at the VM).
- Admin UIs (Airflow, Kafka UI, Spark UI) are bound to the VM's localhost: SSH tunnel only.
- Each component has its own least-privilege AWS identity (pipeline-vm, dashboard-reader,
  pipeline-copilot-agent).

## Deployment (CI/CD)
Both projects use GitHub Actions: on every push, lint, tests and Docker builds; on push to main
after CI passes, an SSH deploy to the VM (deploy-only key, pinned host key) that pulls, rebuilds
and smoke-tests. The pipeline's deploy also uploads the Glue script to S3 via GitHub OIDC.

## Where Pipeline Copilot fits
Pipeline Copilot watches this pipeline from the outside, read-only: it reads Airflow (runs, task
logs) through its REST API, queries Athena and the Glue catalog, and searches this knowledge base.
It never changes the pipeline; it diagnoses problems and suggests fixes for a human to apply.
