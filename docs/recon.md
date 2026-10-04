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

## Open questions
- Ingestion (producer → Kafka → Spark) is NOT an Airflow DAG. How does the agent
  detect ingestion failures? (Probably via data checks: freshness, row counts)
- What does `daily_lake_maintenance` do, task by task?
- How does data move from raw → curated? (Glue job? Spark? Inside the DAG?)
