# Technology: AWS data lake (S3, Glue, Glue Data Catalog, Athena) and Parquet

## What is Amazon S3?
Amazon S3 is object storage: files (objects) in buckets, addressed by key, cheap and durable.
A data lake is a set of files in S3 organized into zones. This pipeline's bucket
weather-seismic-lake-mahendra has: raw/ (lake files as streamed, synced nightly), curated/
(cleaned, one file per day per table), scripts/ (the Glue job script), athena-results/.

## What is Parquet?
Parquet is a columnar file format: values of each column are stored together and compressed, so
queries read only the columns they need. Files are partitioned into folders like date=2026-10-05/,
so a query filtered on date only reads that day's files. Many tiny files are slow and costly,
which is why the nightly job compacts each day into one file.

## What is AWS Glue (ETL job)?
AWS Glue runs Spark jobs serverlessly: no cluster to manage, billed per run. The job
curate-daily (Glue 5.0, 2 G.1X workers, 10-minute timeout, 0 retries) curates one day, given by
--DATE: it reads raw/<table>/date=<DATE>/ directly from S3, keeps the latest row per key
(weather: per station and observation time; seismic: latest revision per event_id), builds
curated_weather_daily (per station per day: readings, min/max/avg temperature, anomalies), and
overwrites only that day's partition, so re-runs and backfills are safe (idempotent). It fails if
raw weather has 0 rows, so an empty day turns the DAG red instead of silently succeeding.

## What is the Glue Data Catalog?
The Glue Data Catalog is a metadata store: database and table definitions (columns, types,
S3 location, partitions). Athena and Glue jobs use it to know what the files mean. Database:
weather_seismic, tables raw_weather, raw_seismic, curated_weather, curated_seismic,
curated_weather_daily, and the view alerts.

## What is Amazon Athena and partition projection?
Athena runs SQL (Trino/Presto) directly on files in S3, billed per data scanned; there is no
database server. Partition projection lets Athena compute a table's date partitions from a rule
(every day from 2026-09-27 to now) instead of reading them from the catalog, so new days are
queryable immediately with no crawler. Important: projection is an Athena-only feature. Spark and
Glue jobs reading through the catalog only see registered partitions; that difference caused
the 4-day silent curation failure in October 2026 (fixed by reading S3 paths directly).

## Cost and access controls
Athena workgroups cap data scanned per query (the dashboard's: 100 MB; Pipeline Copilot's: 1 GB).
Separate least-privilege IAM users: pipeline-vm (upload to raw/, run the Glue job),
dashboard-reader (read curated data, run Athena), pipeline-copilot-agent (read-only, its own
workgroup). CI/CD uploads the Glue script through GitHub OIDC with short-lived credentials.
