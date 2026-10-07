"""Ops MCP server: live, read-only views of the pipeline host.

    uv run python -m pipeline_copilot.mcp_servers.ops_server

Holds no secrets: only paths and internal URLs (LAKE_DIR, DOCKER_API_URL, DASHBOARD_*).
Tools whose source isn't configured (e.g. on a laptop) say so instead of failing.
Never print() here: stdout carries the MCP protocol.
"""
import json
from typing import Annotated

import duckdb
import httpx
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot import ops
from pipeline_copilot.sql_guard import UnsafeQueryError

mcp = FastMCP("ops", log_level="WARNING")
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

NO_LAKE = "The live local lake is not available here (LAKE_DIR not set or not mounted)."
NO_DOCKER = "Container access is not configured here (DOCKER_API_URL not set)."


@mcp.tool(annotations=READ_ONLY)
def get_live_lake_freshness() -> str:
    """Is data flowing RIGHT NOW? Reads the local lake the Spark streaming jobs write:
    minutes since each job last wrote a file, minutes each weather station is behind,
    and today's seismic events.

    Use this first for "is the pipeline working / is data current / did a feed stop".
    Athena's raw tables only update at the 00:30 UTC sync; this is minutes-fresh.
    """
    root = ops.lake_dir()
    if root is None:
        return NO_LAKE
    return ops.format_freshness(ops.lake_freshness(root))


@mcp.tool(annotations=READ_ONLY)
def query_live_lake(
    sql: Annotated[str, Field(description=(
        "One DuckDB SELECT over the views lake_weather and lake_seismic (same columns as "
        "raw_weather / raw_seismic, plus `date`). Filter on date, e.g. date >= '2026-10-06'."
    ))],
) -> str:
    """Run one read-only SQL SELECT on the live local lake (today's and recent streaming data,
    before the nightly sync). Free (local DuckDB). A LIMIT is added automatically.
    """
    root = ops.lake_dir()
    if root is None:
        return NO_LAKE
    try:
        return ops.query_lake(root, sql)
    except UnsafeQueryError as e:
        return f"Blocked by SQL guardrail: {e}"
    except duckdb.Error as e:
        return f"Lake unavailable: {str(e).splitlines()[0]}"


@mcp.tool(annotations=READ_ONLY)
def list_containers() -> str:
    """List the pipeline's Docker containers with state, uptime, restart count and exit code.

    Use when data stopped flowing, to see whether the producer, Kafka or a Spark job is down,
    crash-looping (restarts climbing) or was killed for memory (OOM).
    """
    url = ops.docker_url()
    if url is None:
        return NO_DOCKER
    try:
        return ops.format_containers(ops.list_containers(url))
    except httpx.HTTPError as e:
        return f"Docker API unreachable ({type(e).__name__})."


@mcp.tool(annotations=READ_ONLY)
def get_container_logs(
    container: Annotated[str, Field(description="Container name from list_containers, e.g. 'weather-stream'.")],
    tail: Annotated[int, Field(description="How many of the latest lines.", ge=10, le=300)] = 100,
) -> str:
    """Get the latest log lines of one container (stdout + stderr, timestamped, trimmed).

    Use to find WHY a container stopped or misbehaves (exceptions, API errors, crash causes).
    """
    url = ops.docker_url()
    if url is None:
        return NO_DOCKER
    try:
        return ops.container_logs(url, container, tail)
    except httpx.HTTPError as e:
        return f"Docker API unreachable ({type(e).__name__})."


@mcp.tool(annotations=READ_ONLY)
def check_dashboard() -> str:
    """Check the public dashboard: its internal API (/api/health, /api/stations) and the
    public HTTPS site. Reports status codes, response times and station statuses.
    """
    try:
        return ops.format_dashboard(ops.dashboard_health())
    except httpx.HTTPError as e:
        return f"Dashboard check failed ({type(e).__name__})."


@mcp.tool(annotations=READ_ONLY)
def run_health_checks() -> str:
    """Run every deterministic health check at once and return JSON: lake freshness per
    dataset and station, expected containers running, dashboard reachable.
    Each item: {"check", "ok", "detail"}. Fast and free: no model, no AWS.
    """
    try:
        return json.dumps(ops.health_checks())
    except (duckdb.Error, httpx.HTTPError, OSError) as e:
        return json.dumps([{"check": "health checks ran", "ok": False, "detail": f"{type(e).__name__}: {e}"}])


if __name__ == "__main__":
    mcp.run()
