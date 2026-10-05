# Runbook: curated data missing or stale

## Symptoms
- curated_* tables have no rows for recent dates, or MAX(date) is older than yesterday.
- Raw tables DO have rows for those dates.
- Airflow curate_day task and the Glue job both report SUCCEEDED.
This is a silent failure: nothing alerts.

## Likely causes
1. The Glue job read 0 input rows. It reads raw tables through the Glue Data Catalog.
   Spark/Glue jobs only see partitions REGISTERED in the catalog; they ignore Athena
   partition projection. If raw tables use projection and new partitions are not
   registered (crawler not re-run, no ALTER TABLE ADD PARTITION), Athena sees new days
   but the Glue job does not, so it processes nothing and still succeeds.
2. The job wrote to a different S3 path than the curated table LOCATION.
3. The job processed a different date than expected (wrong --DATE argument).

## How to check
- Compare raw vs curated counts per date with run_query.
- Check partition projection on the raw tables with get_table_schema.
- Compare with get_registered_partitions on the RAW tables: if registered partitions stop
  before the missing dates, cause 1 is confirmed.
- curate_day log: confirm the Glue run SUCCEEDED and which date target_date returned.
- Glue job CloudWatch output log: "X raw -> Y curated" with X = 0 confirms empty input.

## Fix
- Cause 1: make catalog readers see new partitions. Either register partitions after
  sync_to_s3 (ALTER TABLE ... ADD PARTITION or MSCK REPAIR TABLE), or have the Glue job
  read the S3 path for the date directly instead of the catalog table.
- Then backfill every missing date (see runbook: backfill).
- Prevent recurrence: add a DAG task after curate_day that fails if curated rows for
  target_date = 0 while raw rows > 0.
