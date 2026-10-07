# Technology: Apache Spark Structured Streaming, real-time processing and anomaly detection

## What is Apache Spark Structured Streaming?
Apache Spark is a distributed data processing engine (here used through PySpark). Structured
Streaming is its streaming API: a stream is treated as a table that keeps growing, and Spark
processes it in small micro-batches using the same DataFrame/SQL operations as batch code.
Checkpoints record progress (for example Kafka offsets), so a restarted job resumes exactly where
it stopped.

## What Spark does in this pipeline
Two always-on Spark jobs (containers weather-stream and seismic-stream, Spark 4.1) read the Kafka
topics every minute (trigger: 1 minute), clean and score the data, and write Parquet files
partitioned by date to the VM's local lake (data/lake/weather, data/lake/seismic).

## Weather stream: parsing, deduplication and anomaly detection
1. Parse the JSON with a fixed schema; drop rows without station_id.
2. Deduplicate with a 3-hour watermark on (station_id, observed_at), keeping state bounded.
3. Score each reading with a seasonal, robust anomaly detector: compare its temperature with the
   same station at the same time of day (plus or minus 1 hour) over the previous 14 days, using the
   median and MAD (median absolute deviation). Modified z-score = (temp - median) / max(1.4826 x MAD, 1.0 C).
4. Flag is_anomaly when |z| > 3.5. A reading is only scored once it has same-hour history from at
   least 5 previous days; before that z_score is NULL ("calibrating").
This replaced an earlier rolling 24-hour mean/std detector that mixed nights with afternoons and
produced false positives.

## Seismic stream: revisions and significance
USGS revises earthquakes after they happen. The seismic job keeps every revision (a new
updated_at is a new row), deduplicates exact repeats with a 3-hour watermark, flags
is_significant when magnitude >= 4.5, and writes with Spark's exactly-once file sink.
The nightly Glue job later keeps only the latest revision per event_id.

## Fixed schema: why upstream changes become NULLs
The jobs parse JSON with a fixed schema. If NOAA or USGS rename or drop a field, Spark does not
fail: the column silently becomes NULL. That is why schema drift shows up as NULL spikes in the
data, not as errors (see runbook: schema drift).

## Restarts, checkpoints and replay
Checkpoints live in data/checkpoints/. After a crash or VM reboot, Docker restarts the job and it
resumes from the checkpoint, catching up from Kafka (7-day retention). Deleting a lake folder
together with its checkpoint replays everything still in Kafka.
