"""Actions MCP server: the only code that can change the pipeline.

    uv run python -m pipeline_copilot.mcp_servers.actions_server

The model never sees these tools. The graph calls them only after a human approved
the exact action (see graph.py: await_approval -> execute). Layers of protection:
  1. ACTIONS_ENABLED must be true, or every tool refuses.
  2. Each call is re-checked against the catalog (remediation.check_action).
  3. Airflow: a separate user (AIRFLOW_ACTIONS_USERNAME) whose role allows only this DAG.
     Docker: a separate socket proxy (DOCKER_ACTIONS_URL) that allows restarts only.
  4. Every attempt, allowed or not, is appended to an audit log (ACTIONS_AUDIT_LOG).

All results are JSON: {"ok": bool, "detail": str, ...}. Never print() here.
"""
import json
import logging
import os
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
from typing import Annotated

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot import remediation as rem
from pipeline_copilot.airflow_actions_client import AirflowActionsClient
from pipeline_copilot.config import PROJECT_ROOT, actions_enabled, load_airflow_actions_settings

logging.getLogger("httpx").setLevel(logging.WARNING)
mcp = FastMCP("actions", log_level="WARNING")

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
# Changes the pipeline, but each action is safe to repeat (re-running a day overwrites that day)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)


@cache
def _airflow() -> AirflowActionsClient:
    return AirflowActionsClient(load_airflow_actions_settings())


def _audit(entry: dict) -> None:
    path = Path(os.environ.get("ACTIONS_AUDIT_LOG", PROJECT_ROOT / "actions_audit.jsonl"))
    entry = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **entry}
    try:
        with path.open("a") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError as e:
        logging.error("audit log write failed: %s", e)


def _result(ok: bool, detail: str, **extra) -> str:
    return json.dumps({"ok": ok, "detail": detail, **extra})


def _guard(action: dict, thread_id: str) -> str | None:
    """Refuse (and audit the refusal) unless actions are on and the action passes the catalog."""
    if not actions_enabled():
        reason = "actions are disabled (ACTIONS_ENABLED is not true)"
    else:
        reason = rem.check_action(action, datetime.now(timezone.utc).date())
    if reason:
        _audit({"thread_id": thread_id, **action, "ok": False, "detail": f"refused: {reason}"})
        return _result(False, f"Refused: {reason}")
    return None


def _http_error(e: httpx.HTTPError, what: str) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        return f"{what} answered {e.response.status_code}: {e.response.text[:300]}"
    return f"Cannot reach {what} ({type(e).__name__})"


@mcp.tool(annotations=WRITE)
def clear_task(
    run_id: Annotated[str, Field(description="Exact Airflow run_id.")],
    task_id: Annotated[str, Field(description="Task to re-run (its downstream tasks re-run too).")],
    thread_id: Annotated[str, Field(description="Conversation that approved this, for the audit log.")],
) -> str:
    """Re-run one task of daily_lake_maintenance in an existing run. Approved actions only."""
    action = {"action": "clear_task", "run_id": run_id, "task_id": task_id}
    if refused := _guard(action, thread_id):
        return refused
    try:
        out = _airflow().clear_task(rem.DAG_ID, run_id, task_id)
    except Exception as e:  # noqa: BLE001 - settings errors too: report, don't crash the server
        detail = _http_error(e, "Airflow") if isinstance(e, httpx.HTTPError) else f"{type(e).__name__}: {e}"
        _audit({"thread_id": thread_id, **action, "ok": False, "detail": detail})
        return _result(False, detail)
    cleared = [t.get("task_id") for t in out.get("task_instances", [])]
    detail = f"Cleared {', '.join(cleared) or task_id} in {run_id}; Airflow will run them again."
    _audit({"thread_id": thread_id, **action, "ok": True, "detail": detail})
    return _result(True, detail, run_id=run_id)


@mcp.tool(annotations=WRITE)
def trigger_backfill(
    date: Annotated[str, Field(description="Day to re-sync and re-curate, YYYY-MM-DD.")],
    thread_id: Annotated[str, Field(description="Conversation that approved this, for the audit log.")],
) -> str:
    """Trigger a daily_lake_maintenance run for one past day (conf {"date": ...}). Approved actions only."""
    action = {"action": "backfill_date", "date": date}
    if refused := _guard(action, thread_id):
        return refused
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_id = f"manual__copilot_backfill_{date}_{stamp}"
    try:
        _airflow().trigger_run(rem.DAG_ID, run_id, {"date": date},
                               note=f"Pipeline Copilot backfill, approved in {thread_id}")
    except Exception as e:  # noqa: BLE001
        detail = _http_error(e, "Airflow") if isinstance(e, httpx.HTTPError) else f"{type(e).__name__}: {e}"
        _audit({"thread_id": thread_id, **action, "ok": False, "detail": detail})
        return _result(False, detail)
    detail = f"Triggered {run_id} to re-curate {date}."
    _audit({"thread_id": thread_id, **action, "ok": True, "detail": detail, "run_id": run_id})
    return _result(True, detail, run_id=run_id)


@mcp.tool(annotations=WRITE)
def restart_container(
    container: Annotated[str, Field(description="Container name from list_containers.")],
    thread_id: Annotated[str, Field(description="Conversation that approved this, for the audit log.")],
) -> str:
    """Restart one pipeline container through the restart-only Docker proxy. Approved actions only."""
    action = {"action": "restart_container", "container": container}
    if refused := _guard(action, thread_id):
        return refused
    url = os.environ.get("DOCKER_ACTIONS_URL")
    if not url:
        detail = "Container restarts are not configured here (DOCKER_ACTIONS_URL not set)."
        _audit({"thread_id": thread_id, **action, "ok": False, "detail": detail})
        return _result(False, detail)
    try:
        with httpx.Client(base_url=url, timeout=60) as client:
            resp = client.post(f"/containers/{container}/restart", params={"t": "10"})
            resp.raise_for_status()
    except httpx.HTTPError as e:
        detail = _http_error(e, "Docker")
        _audit({"thread_id": thread_id, **action, "ok": False, "detail": detail})
        return _result(False, detail)
    detail = f"Restarted container {container}."
    _audit({"thread_id": thread_id, **action, "ok": True, "detail": detail})
    return _result(True, detail)


@mcp.tool(annotations=READ_ONLY)
def get_dag_run_status(
    run_id: Annotated[str, Field(description="Exact run_id of daily_lake_maintenance.")],
) -> str:
    """State of one run and its tasks as JSON, for checking whether a fix worked:
    {"ok", "run_id", "state", "target_date", "tasks": [{"task_id", "state", "try_number"}]}."""
    try:
        run = _airflow().get_dag_run(rem.DAG_ID, run_id)
        tasks = _airflow().get_task_instances(rem.DAG_ID, run_id)
    except Exception as e:  # noqa: BLE001
        detail = _http_error(e, "Airflow") if isinstance(e, httpx.HTTPError) else f"{type(e).__name__}: {e}"
        return _result(False, detail)
    return json.dumps({
        "ok": True, "run_id": run_id, "state": run.get("state"),
        "target_date": rem.target_date_of(run_id, run.get("conf")),
        "tasks": [{"task_id": t["task_id"], "state": t["state"], "try_number": t.get("try_number")}
                  for t in tasks],
    })


if __name__ == "__main__":
    mcp.run()
