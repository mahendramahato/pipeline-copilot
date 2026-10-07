# Runbook: curated data missing or stale

## Symptoms
- curated_* tables have no rows for recent dates, or MAX(date) is older than yesterday.
- Raw tables DO have rows for those dates (or not: see cause 1).
- Usually curate_day FAILED (red DAG). If the DAG is green but curated data stopped
  updating, it is a silent failure: nothing alerts.

## Likely causes
1. The Glue job failed on missing or empty input. Since 2026-10-06 it reads each day
   straight from S3 and fails loudly with "No raw weather rows for <date>" or a
   path-not-found error. Then the problem is upstream: check sync_to_s3 and the
   streaming jobs (see runbook: check_freshness failed).
2. The job wrote to a different S3 path than the curated table LOCATION (job succeeded,
   curated still empty).
3. The job processed a different date than expected (wrong --DATE argument).
4. Seismic only: the job checks for empty WEATHER input, not seismic, so an empty seismic
   day can still "succeed" silently.

## History: the 2026-09-30 silent failure (fixed)
Until 2026-10-06 the job read raw tables through the Glue Data Catalog. Deleting the Glue
crawler on 2026-09-30 stopped partition registration. Athena (partition projection) still
showed the new partitions, but the Glue/Spark job only sees registered partitions, so it
read zero rows and still succeeded for 4 nights while curated data stopped updating.
Fixed by reading S3 paths directly and failing on empty input; 10-01..10-06 backfilled.
Registered catalog partitions are NO LONGER relevant to curation: they stopping at
2026-09-30 is expected, not a problem.

## How to check
- Compare raw vs curated counts per date with run_query.
- curate_day task log: did the Glue run fail (and with what error), or succeed?
- target_date log: which date was curated.
- If raw is missing too, go upstream: sync_to_s3 log and check_freshness.
- Glue job CloudWatch output log: "X raw -> Y curated" (not visible to the agent's tools).

## Fix
- Cause 1: fix the upstream problem (streaming, sync), then backfill (see runbook: backfill).
- Causes 2-3: fix the job's output path or the --DATE argument, then backfill.
- Verify after each backfilled day: curated rows > 0 and close to raw.
