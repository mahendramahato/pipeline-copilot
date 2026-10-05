"""Airflow MCP server: read-only tools over the Airflow 3 REST API.

An MCP client starts this as a subprocess and talks to it over stdio:
    uv run python -m pipeline_copilot.mcp_servers.airflow_server

IMPORTANT: never print() here. stdout carries the MCP protocol messages,
so a stray print corrupts them. Use logging (FastMCP sends it to stderr).
"""
from functools import cache
from typing import Annotated

import httpx
import logging
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot.airflow_client import AirflowClient
from pipeline_copilot.config import load_airflow_settings

# WARNING: only show problems, not every request. The logs still go to
# stderr, so they never interfere with the protocol on stdout.
mcp = FastMCP("airflow", log_level="WARNING")

# httpx logs every request at INFO level; quiet it the same way
logging.getLogger("httpx").setLevel(logging.WARNING)


# --- Log trimming ---
TAIL_LINES = 60
MAX_IMPORTANT_LINES = 30
IMPORTANT_LEVELS = {"warning", "error", "critical"}


def _format_log(records: list[dict]) -> tuple[list[str], list[str]]:
    lines, important = [], []
    for rec in records:
        event = str(rec.get("event", ""))
        if event.startswith(("::group::", "::endgroup::")):
            continue
        ts = str(rec.get("timestamp", ""))[11:19]
        level = str(rec.get("level", "")).lower()
        line = f"{ts} {level.upper():8} {event}".strip()
        if rec.get("error_detail"):
            for err in rec["error_detail"]:
                if isinstance(err, dict):
                    line += f"\n    EXCEPTION {err.get('exc_type')}: {err.get('exc_value')}"
                else:
                    line += f"\n    EXCEPTION {err}"
        lines.append(line)
        if level in IMPORTANT_LEVELS or rec.get("error_detail") or "Traceback" in event:
            important.append(line)
    return lines, important


def _trim_log(records: list[dict]) -> str:
    lines, important = _format_log(records)
    if len(lines) <= TAIL_LINES:
        return "\n".join(lines) or "(log is empty)"
    tail = lines[-TAIL_LINES:]
    earlier_important = [l for l in important if l not in tail][:MAX_IMPORTANT_LINES]
    parts = [f"(log has {len(lines)} lines; showing warnings/errors + last {TAIL_LINES})"]
    if earlier_important:
        parts += ["--- warnings/errors earlier in the log ---", *earlier_important]
    parts += [f"--- last {TAIL_LINES} lines ---", *tail]
    return "\n".join(parts)


def _airflow_error(e: httpx.HTTPError) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        return f"Error {e.response.status_code} from Airflow: {e.response.text[:300]}"
    return (
        f"Cannot reach Airflow ({type(e).__name__}). The SSH tunnel to the VM "
        "may be down, or Airflow may not be running."
    )


# --- The server ---
# The name "airflow" is what MCP clients show for this server.
mcp = FastMCP("airflow")

# Every tool here only reads. Clients may use this hint (e.g. to skip
# approval prompts); the real enforcement is still the Viewer role.
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)


# --- One shared client, created on first use ---
# @cache runs the function once and reuses the result. Lazy creation means
# importing this module (tests, listing tools) doesn't need credentials.
@cache
def _client() -> AirflowClient:
    return AirflowClient(load_airflow_settings())


# --- Tools ---
# The docstring becomes the tool description; Field(description=...)
# becomes each argument's description. Claude sees exactly these words.

@mcp.tool(annotations=READ_ONLY)
def list_dags() -> str:
    """List all Airflow DAGs and whether each is paused.

    Use this first when you don't know the exact dag_id.
    """
    try:
        dags = _client().list_dags()
    except httpx.HTTPError as e:
        return _airflow_error(e)
    return "\n".join(f"{d['dag_id']} (paused={d['is_paused']})" for d in dags) or "No DAGs found."


@mcp.tool(annotations=READ_ONLY)
def get_recent_dag_runs(
    dag_id: Annotated[str, Field(description='Exact DAG id, e.g. "daily_lake_maintenance".')],
    limit: Annotated[int, Field(description="How many runs to return.", ge=1, le=25)] = 5,
) -> str:
    """Get the most recent runs of a DAG, newest first, with state and timing.

    Use this to answer "when did X last succeed/fail" and to find the
    run_id needed by get_task_instances.
    """
    try:
        runs = _client().get_dag_runs(dag_id, limit=limit)
    except httpx.HTTPError as e:
        return _airflow_error(e)
    if not runs:
        return f"No runs found for {dag_id}."
    return "\n".join(
        f"run_id={r['dag_run_id']} | state={r['state']} | "
        f"queued={r.get('queued_at')} | start={r.get('start_date')} | end={r.get('end_date')}"
        for r in runs
    )


@mcp.tool(annotations=READ_ONLY)
def get_task_instances(
    dag_id: Annotated[str, Field(description="Exact DAG id.")],
    run_id: Annotated[str, Field(description="Exact run_id from get_recent_dag_runs.")],
) -> str:
    """Get every task in one DAG run with its state, try number and duration.

    Use this to find which task failed, retried or is still running.
    """
    try:
        tasks = _client().get_task_instances(dag_id, run_id)
    except httpx.HTTPError as e:
        return _airflow_error(e)
    return "\n".join(
        f"task_id={t['task_id']} | state={t['state']} | try_number={t['try_number']} | "
        f"duration_s={t.get('duration')}"
        for t in tasks
    ) or "No tasks found."


@mcp.tool(annotations=READ_ONLY)
def get_task_log(
    dag_id: Annotated[str, Field(description="Exact DAG id.")],
    run_id: Annotated[str, Field(description="Exact run_id from get_recent_dag_runs.")],
    task_id: Annotated[str, Field(description="Task id from get_task_instances.")],
    try_number: Annotated[int, Field(description="Attempt number from get_task_instances (usually the latest).", ge=1)],
) -> str:
    """Get the log of one task attempt, trimmed to errors/warnings and the last lines.

    Use this to find WHY a task failed or behaved unexpectedly.
    """
    try:
        log = _client().get_task_log(dag_id, run_id, task_id, try_number)
    except httpx.HTTPError as e:
        return _airflow_error(e)
    return _trim_log(log.get("content", []))


# --- Entry point ---
# mcp.run() defaults to stdio: read requests from stdin, write replies to stdout.
if __name__ == "__main__":
    mcp.run()
