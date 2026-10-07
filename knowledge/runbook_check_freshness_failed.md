# Runbook: streaming job stopped or crashed - check_freshness failed, lake is stale

## Symptoms
- A streaming job stopped, crashed or died (weather-stream or seismic-stream),
  or no new data is arriving / data stopped flowing.
- daily_lake_maintenance fails at check_freshness.
- Log says: "<dataset> lake is stale (N h) - is its streaming job running?"
- sync_to_s3 and curate_day do not run (upstream failed), so raw and curated stop updating.

## Likely causes
- weather-stream or seismic-stream Spark container stopped or crash-looping.
- producer container stopped, or NOAA/USGS API down or rate limiting.
- Kafka container down (both datasets stale at once points here or to the producer).
- VM disk full (writes fail).

## How to check
- get_live_lake_freshness: which feed stopped and when (minutes, not the next nightly run):
  one dataset stale -> its stream job; both stale -> producer or Kafka; one station stale ->
  that NOAA station (upstream), not the pipeline.
- list_containers: is weather-stream / seismic-stream / producer / kafka running, crash-looping
  (restarts climbing) or OOM-killed?
- get_container_logs <container>: the exception or API error that stopped it.

## Fix
- Restart the stopped container (docker compose up -d <service>).
- Once fresh, clear the failed run (or wait for the next 00:30 run). Data is not lost:
  Kafka retains messages and the stream job resumes from its checkpoint.
