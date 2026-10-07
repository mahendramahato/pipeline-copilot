"""Athena MCP server: read-only data access with a SQL guardrail.

    uv run python -m pipeline_copilot.mcp_servers.athena_server

Never print() here: stdout carries the MCP protocol.
"""
from datetime import datetime, timezone
from functools import cache
import os
from typing import Annotated, Literal

from botocore.exceptions import BotoCoreError, ClientError
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot.athena_client import AthenaClient, AthenaQueryError, QueryResult
from pipeline_copilot.config import load_athena_settings
from pipeline_copilot.sql_guard import UnsafeQueryError, check_query

mcp = FastMCP("athena", log_level="WARNING")
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)


# --- Shared state, created on first use ---
@cache
def _client() -> AthenaClient:
    return AthenaClient(load_athena_settings())


# The guardrail's allow-list comes from Glue itself, so it always matches
# the real tables. Cached for the server's lifetime: restart to pick up new tables.
@cache
def _allowed_tables() -> frozenset[str]:
    return frozenset(t["Name"] for t in _client().list_tables())


# --- AWS errors become text the agent can act on ---
def _aws_error(e: Exception) -> str:
    if isinstance(e, ClientError):
        err = e.response.get("Error", {})
        return f"AWS error {err.get('Code')}: {err.get('Message')}"
    return f"AWS error: {type(e).__name__}: {e}"


def _columns(table: dict) -> dict[str, str]:
    return {c["Name"]: c["Type"] for c in table.get("StorageDescriptor", {}).get("Columns", [])}

# Glue returns timestamps in the machine's local time zone; report everything in UTC
def _utc(value) -> str:
    return value.astimezone(timezone.utc).isoformat() if isinstance(value, datetime) else str(value)


# --- Pure formatters, shared with the eval harness (so fake tools print identically) ---
def _format_schema(table: str, t: dict) -> str:
    cols = "\n".join(f"  {name} {typ}" for name, typ in _columns(t).items())
    parts = [p["Name"] for p in t.get("PartitionKeys", [])]
    projection = {k: v for k, v in t.get("Parameters", {}).items() if k.startswith("projection.")}
    return (
        f"{table} (type={t.get('TableType')}, updated={_utc(t.get('UpdateTime'))})\n"
        f"columns:\n{cols}\n"
        f"partition keys: {parts or 'none'}\n"
        f"partition projection: {projection or 'not enabled'}"
    )


def _format_versions(table: str, versions: list[dict]) -> str:
    versions = sorted(versions, key=lambda v: int(v["VersionId"]), reverse=True)   # newest first
    if len(versions) < 2:
        return f"{table} has only {len(versions)} version(s); nothing to compare."
    lines = []
    for newer, older in zip(versions, versions[1:]):
        new_cols, old_cols = _columns(newer["Table"]), _columns(older["Table"])
        added = sorted(new_cols.keys() - old_cols.keys())
        removed = sorted(old_cols.keys() - new_cols.keys())
        retyped = sorted(c for c in new_cols.keys() & old_cols.keys() if new_cols[c] != old_cols[c])
        change = ", ".join(filter(None, [
            f"added {added}" if added else "",
            f"removed {removed}" if removed else "",
            f"type changed {retyped}" if retyped else "",
        ])) or "no column changes"
        lines.append(
            f"v{newer['VersionId']} (updated {_utc(newer['Table'].get('UpdateTime'))}) "
            f"vs v{older['VersionId']}: {change}"
        )
    return "\n".join(lines)

# --- Query results as a compact pipe-separated table ---
# NULL is spelled out: NULL rates are how this pipeline shows schema drift,
# so an empty cell would hide the exact signal we're looking for.
def _format_result(r: QueryResult) -> str:
    lines = [" | ".join(r.columns)]
    lines += [" | ".join("NULL" if v is None else v for v in row) for row in r.rows]
    note = "; TRUNCATED, more rows exist" if r.truncated else ""
    lines.append(f"({len(r.rows)} rows{note}; scanned {r.bytes_scanned / 1e6:.2f} MB)")
    return "\n".join(lines)


# --- Tools ---

@mcp.tool(annotations=READ_ONLY)
def list_tables() -> str:
    """List the tables and views in the data lake database, with type and partition keys.

    Free (reads the Glue catalog, scans no data). Use this first to see what exists.
    """
    try:
        tables = _client().list_tables()
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    return "\n".join(
        f"{t['Name']} | type={t.get('TableType')} | "
        f"partitioned_by={[p['Name'] for p in t.get('PartitionKeys', [])] or 'none'}"
        for t in tables
    )


@mcp.tool(annotations=READ_ONLY)
def get_table_schema(
    table: Annotated[str, Field(description="Table name from list_tables, e.g. 'raw_seismic'.")],
) -> str:
    """Get a table's columns and types, partition keys and last-updated time.

    Free (Glue catalog). ALWAYS check the schema before writing SQL against a table.
    """
    try:
        t = _client().get_table(table)
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    cols = "\n".join(f"  {name} {typ}" for name, typ in _columns(t).items())
    parts = [p["Name"] for p in t.get("PartitionKeys", [])]
    # Table properties that control how partitions are found (projection.*).
    projection = {k: v for k, v in t.get("Parameters", {}).items() if k.startswith("projection.")}
    return (
        f"{table} (type={t.get('TableType')}, updated={t.get('UpdateTime')})\n"
        f"columns:\n{cols}\n"
        f"partition keys: {parts or 'none'}\n"
        f"partition projection: {projection or 'not enabled'}"
    )
    
    return _format_result(table, t)



@mcp.tool(annotations=READ_ONLY)
def get_table_versions(
    table: Annotated[str, Field(description="Table name from list_tables.")],
    limit: Annotated[int, Field(description="How many recent versions to compare.", ge=2, le=10)] = 5,
) -> str:
    """Show recent versions of a table's definition and which columns changed between them.

    Free (Glue catalog). Detects changes to the table DEFINITION only. If upstream
    data changes but nobody updates the table, versions stay the same; in that case
    look for NULL spikes in the data with run_query instead.
    """
    try:
        versions = _client().get_table_versions(table, limit=limit)
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    versions.sort(key=lambda v: int(v["VersionId"]), reverse=True)   # newest first
    if len(versions) < 2:
        return f"{table} has only {len(versions)} version(s); nothing to compare."
    return _format_versions(table, versions)
    
    lines = []
    for newer, older in zip(versions, versions[1:]):
        new_cols, old_cols = _columns(newer["Table"]), _columns(older["Table"])
        added = sorted(new_cols.keys() - old_cols.keys())
        removed = sorted(old_cols.keys() - new_cols.keys())
        retyped = sorted(c for c in new_cols.keys() & old_cols.keys() if new_cols[c] != old_cols[c])
        change = ", ".join(filter(None, [
            f"added {added}" if added else "",
            f"removed {removed}" if removed else "",
            f"type changed {retyped}" if retyped else "",
        ])) or "no column changes"
        lines.append(
            f"v{newer['VersionId']} (updated {newer['Table'].get('UpdateTime')}) "
            f"vs v{older['VersionId']}: {change}"
        )
    return "\n".join(lines)

@mcp.tool(annotations=READ_ONLY)
def get_registered_partitions(
    table: Annotated[str, Field(description="Table name from list_tables.")],
) -> str:
    """List the partitions registered in the Glue Data Catalog for a table.

    Free (Glue catalog). Shows the partition values the catalog itself knows
    about, which is what readers that go through the catalog will see.
    """
    try:
        partitions = _client().get_partitions(table)
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    values = sorted("/".join(p["Values"]) for p in partitions)
    if not values:
        return f"{table}: no partitions registered in the catalog."
    shown = ", ".join(values) if len(values) <= 31 else f"{', '.join(values[:5])} ... {', '.join(values[-5:])}"
    return f"{table}: {len(values)} registered partitions ({values[0]} to {values[-1]})\n{shown}"


@mcp.tool(annotations=READ_ONLY)
def run_query(
    sql: Annotated[str, Field(description="One Athena (Presto/Trino) SELECT statement.")],
) -> str:
    """Run one read-only SQL SELECT on the data lake and return the rows.

    Costs money (Athena bills per data scanned), so:
    - Check columns with get_table_schema first.
    - Filter on the partition column (date = 'YYYY-MM-DD', a string) whenever possible.
    - Prefer aggregates (COUNT, MIN, MAX, COUNT_IF(col IS NULL)) over SELECT *.
    Only SELECT on this database's tables is allowed. A LIMIT is added automatically.
    """
    try:
        safe_sql = check_query(sql, load_athena_settings().database, _allowed_tables())
    except UnsafeQueryError as e:
        return f"Blocked by SQL guardrail: {e}"
    except (ClientError, BotoCoreError) as e:      # allow-list lookup failed
        return _aws_error(e)

    try:
        result = _client().run_query(safe_sql)
    except AthenaQueryError as e:
        return f"{e}\nSQL that ran: {safe_sql}"
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    return _format_result(result)


GLUE_JOB = os.environ.get("GLUE_JOB_NAME", "curate-daily")


@mcp.tool(annotations=READ_ONLY)
def get_glue_job_runs(
    limit: Annotated[int, Field(description="How many recent runs.", ge=1, le=20)] = 5,
) -> str:
    """Recent runs of the Glue curation job: state, the date it curated (--DATE), start time,
    duration and error message. Use when curate_day failed, ran long, or curated data looks wrong.
    """
    try:
        runs = _client().get_job_runs(GLUE_JOB, limit)
    except (ClientError, BotoCoreError) as e:
        return _aws_error(e)
    if not runs:
        return f"No runs found for Glue job {GLUE_JOB}."
    return "\n".join(
        f"run_id={r['Id']} | state={r['JobRunState']} | date={r.get('Arguments', {}).get('--DATE', '?')} | "
        f"started={_utc(r.get('StartedOn'))} | duration_s={r.get('ExecutionTime')}"
        + (f" | error={r['ErrorMessage'][:300]}" if r.get("ErrorMessage") else "")
        for r in runs
    )


@mcp.tool(annotations=READ_ONLY)
def get_glue_job_log(
    run_id: Annotated[str, Field(description="run_id from get_glue_job_runs (starts with jr_).")],
    stream: Annotated[Literal["output", "error"], Field(description="output = what the job printed; error = errors and stack traces.")] = "output",
) -> str:
    """The CloudWatch log of one Glue job run. The output log includes the job's summary line
    "date=... weather: X raw -> Y curated | seismic: X raw -> Y curated" (X = 0 means empty input).
    """
    try:
        lines = _client().get_job_log(run_id, stream)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return f"No {stream} log for {run_id} (the run may have produced no {stream} output)."
        return _aws_error(e)
    except BotoCoreError as e:
        return _aws_error(e)
    # Lead with the summary lines, then the tail
    summary = [line for line in lines if " raw -> " in line]
    text = "\n".join(line[:300] for line in lines[-80:]) or "(empty log)"
    return ("summary:\n" + "\n".join(summary) + "\n---\n" if summary else "") + text


if __name__ == "__main__":
    mcp.run()
