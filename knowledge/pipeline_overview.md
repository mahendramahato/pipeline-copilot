# Pipeline overview: weather + seismic lake

## Data flow
Producer polls NOAA (weather) and USGS (earthquakes) every minute and sends to Kafka.
Two Spark Structured Streaming jobs (weather-stream, seismic-stream) run on the VM in
Docker and write Parquet to the VM's LOCAL lake (data/lake/weather, data/lake/seismic),
partitioned by date. Nothing reaches S3 until the nightly sync.

## Nightly DAG: daily_lake_maintenance
Runs 00:30 UTC. Tasks: check_freshness -> sync_to_s3 -> curate_day, and
target_date -> curate_day. 1 automatic retry after 5 min. max_active_runs=1 (extra runs queue).
- check_freshness: fails if the newest local Parquet file is older than 3 hours.
- sync_to_s3: aws s3 sync of the local lake to s3://weather-seismic-lake-mahendra/raw/.
- target_date: the `date` param if given, else the day before the run.
- curate_day: runs Glue job curate-daily for target_date and waits for it.

## Glue job curate-daily
Script phase4/curate_job.py (uploaded to s3://.../scripts/). Glue 5.0, 2 x G.1X workers,
10 minute timeout, 0 retries, one concurrent run allowed.
Reads raw tables through the Glue Data Catalog: spark.table("weather_seismic.raw_weather")
filtered to date = RUN_DATE. Writes curated/weather, curated/seismic, curated/weather_daily
for that one date with dynamic partition overwrite, so re-running a day is safe (idempotent).
Prints "date=... weather: X raw -> Y curated | seismic: X raw -> Y curated" to its
CloudWatch output log.

## Tables (Athena database weather_seismic)
All partitioned by `date` ('YYYY-MM-DD' string).
- raw_weather, raw_seismic: created by a Glue crawler, later switched to partition projection.
  raw_seismic keeps every USGS revision of a quake.
- curated_weather: raw deduplicated (latest ingested_at per station_id + observed_at).
- curated_seismic: latest revision per event_id.
- curated_weather_daily: one row per station per day (readings, min/max/avg temp, anomalies).
- alerts: a VIEW over the raw tables (weather anomalies + significant quakes).

## What normal looks like
- Athena raw tables lag real time by up to ~24h (they only update at the 00:30 sync).
- After a successful run on day D, curated tables contain day D-1.
- Curated row counts are close to raw counts for the same day (dedup removes few rows).
  Curated seismic rows = distinct event_id in raw_seismic for that day.
- Weather: roughly 800 raw rows per full day. Seismic: roughly 250-350 raw rows per full day.

