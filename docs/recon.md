# Recon: what the agent can see (Phase 0)

## Airflow
- Version: 3.3.2. REST API at `/api/v2`
- Auth: SimpleAuthManager. POST `/auth/token` → JWT → `Authorization: Bearer <token>`
- Agent user: `agent_viewer` (viewer role). Reads OK, writes → 403 (verified)
- Access: bound to VM 127.0.0.1:8081 (not public). Mac reaches it via SSH tunnel on localhost:8080
- DAGs: `daily_lake_maintenance` (only one)

## AWS
- Region: us-west-2
- Agent identity: IAM user `pipeline-copilot-agent`, profile `pipeline-copilot`
- Policy: `infra/iam/pipeline-copilot-readonly.json`
- Athena workgroup: `pipeline-copilot`, 1 GB per-query scan cutoff
- Write test (`CREATE DATABASE`) → FAILED, access denied (verified)

## Data lake: Glue database `weather_seismic`
| Layer   | Tables |
|---------|--------|
| raw     | raw_weather, raw_seismic |
| curated | curated_weather, curated_seismic, curated_weather_daily |
| alerts  | alerts |

- curated_seismic: 206 rows (2026-10-03)

## Data flow and timing
VM (real-time)                              AWS (daily batch)
producer → kafka → weather-stream ─┐
seismic-stream ─┴► VM local lake (./data/lake)
│
00:30 UTC sync_to_s3 (once a day)
▼
S3 raw/ ──► Glue curate-daily ──► S3 curated/

- **Key fact:** Athena `raw_*` tables can be up to **24h behind**, and that is NORMAL.
  Only the VM's local lake is real-time.
- `curated_*` tables are about **1 day behind** (Glue curates yesterday).

## DAG: daily_lake_maintenance
- Schedule: 00:30 UTC daily, 1 retry after 5 min, max 1 active run
- Optional param `date` (yyyy-MM-dd) for manual backfills; empty = yesterday

| Task | What it does | If it fails, it means |
|------|--------------|-----------------------|
| check_freshness | Fails if the newest local Parquet file is > 3h old | A streaming job (producer/Kafka/Spark) has stopped |
| sync_to_s3 | `aws s3 sync` of the local lake → s3://.../raw/ | S3 upload or credentials problem |
| target_date | Picks the day to curate (yesterday or the param) | Rare |
| curate_day | Runs Glue job `curate-daily` and waits | Glue / ETL problem |

Order: check_freshness → sync_to_s3 → curate_day, and target_date → curate_day

## Raw → curated: Glue job `curate-daily` (phase4/curate_job.py)
One job builds all three curated tables for one day:

| Table | From | Logic |
|-------|------|-------|
| curated_weather | raw_weather | Dedupe: latest ingested_at per (station_id, observed_at) |
| curated_seismic | raw_seismic | Latest USGS revision per event_id (by updated_at) |
| curated_weather_daily | curated_weather | Per station per day: readings, min/max/avg temp, anomalies |

- `alerts` is a VIEW over the raw tables, not a table
- The job prints `weather: X raw -> Y curated | seismic: ...` to CloudWatch

## Containers on the VM
producer, kafka, kafka-ui, weather-stream, seismic-stream, spark (idle),
airflow, airflow-postgres, api, web

## What the agent can and can't see (v1)
| Failure | Seen through |
|---------|--------------|
| Streaming stopped | check_freshness task log (Airflow) ✅ |
| S3 sync failed | sync_to_s3 task log (Airflow) ✅ |
| Glue job failed | curate_day task log (partial). TODO: add glue:GetJobRuns |
| Bad / missing data | Athena row counts, nulls, freshness ✅ |
| Why a container crashed | ❌ Needs container logs → future work (Docker MCP) |
