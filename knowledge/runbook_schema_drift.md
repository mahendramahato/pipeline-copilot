# Runbook: schema drift / upstream field changes

## Symptoms
- A column is suddenly NULL for all or most rows from some date onward.
- Row counts look normal, but values are missing (silent failure).
- Or: get_table_versions shows columns added, removed or retyped.

## Likely causes
- Upstream API (NOAA or USGS) renamed or removed a field. The stream jobs parse JSON
  with a FIXED schema, so a renamed field becomes NULL instead of causing an error.
  The Glue table definition does NOT change in this case.
- Someone changed the table definition (crawler run or DDL): visible in table versions.

## How to check
- Null rates per day: SELECT date, COUNT(*), COUNT_IF(col IS NULL) ... GROUP BY date.
  A jump from ~0% to ~100% on one date marks when the upstream change happened.
- get_table_versions for definition changes (only catches catalog changes, not upstream).

## Fix
- Update the stream job's schema to the new field name, restart the stream job.
- Affected days cannot be recovered from raw (the value was dropped at parse time)
  unless Kafka still retains those messages.
