# Runbook: check_freshness failed (stale streaming lake)

## Symptoms
- daily_lake_maintenance fails at check_freshness.
- Log says: "<dataset> lake is stale (N h) - is its streaming job running?"
- sync_to_s3 and curate_day do not run (upstream failed), so raw and curated stop updating.

## Likely causes
- weather-stream or seismic-stream Spark container stopped or crash-looping.
- producer container stopped, or NOAA/USGS API down or rate limiting.
- Kafka container down (both datasets stale at once points here or to the producer).
- VM disk full (writes fail).

## How to check
- The log line names the stale dataset: one dataset stale -> its stream job;
  both stale -> producer or Kafka.
- On the VM: docker ps (is the container up?), docker logs <container> --tail 100.
- Container logs are not visible to the agent's tools; a human must check them.

## Fix
- Restart the stopped container (docker compose up -d <service>).
- Once fresh, clear the failed run (or wait for the next 00:30 run). Data is not lost:
  Kafka retains messages and the stream job resumes from its checkpoint.
