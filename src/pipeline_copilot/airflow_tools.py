"""LangChain tools that let the agent read Airflow. Read-only."""
import httpx
from langchain_core.tools import BaseTool, tool

from pipeline_copilot.airflow_client import AirflowClient

# --- Log trimming limits ---
# Enough context to see what happened before an error, small enough that
# one log doesn't flood Claude's context (and your bill).
TAIL_LINES = 60
MAX_IMPORTANT_LINES = 30
IMPORTANT_LEVELS = {"warning", "error", "critical"}


# --- Turn Airflow 3 structured log records into short text lines ---
# Each record is a dict with ~12 fields; we keep time, level and message.
# "::group::" lines are UI folding markers with no information, so skip them.
def _format_log(records: list[dict]) -> tuple[list[str], list[str]]:
    lines, important = [], []
    for rec in records:
        event = str(rec.get("event", ""))
        if event.startswith(("::group::", "::endgroup::")):
            continue
        ts = str(rec.get("timestamp", ""))[11:19]          # "HH:MM:SS"
        level = str(rec.get("level", "")).lower()
        line = f"{ts} {level.upper():8} {event}".strip()

        # Exceptions: Airflow 3 is expected to attach them as "error_detail".
        # Unverified until we see a real failure, so fall back to the raw value.
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

    # Long log: errors/warnings from anywhere + the last TAIL_LINES lines.
    # Errors often appear early and get buried, so we surface them first.
    tail = lines[-TAIL_LINES:]
    earlier_important = [l for l in important if l not in tail][:MAX_IMPORTANT_LINES]
    parts = [f"(log has {len(lines)} lines; showing warnings/errors + last {TAIL_LINES})"]
    if earlier_important:
        parts += ["--- warnings/errors earlier in the log ---", *earlier_important]
    parts += [f"--- last {TAIL_LINES} lines ---", *tail]
    return "\n".join(parts)


# --- HTTP errors become text the agent can react to ---
def _http_error(e: httpx.HTTPStatusError) -> str:
    return f"Error {e.response.status_code} from Airflow: {e.response.text[:300]}"


# --- Tool factory ---
# Tools are closures over one shared client, instead of a global variable.
# parse_docstring=True turns the "Args:" section into per-argument
# descriptions that Claude sees in the tool schema.
def make_airflow_tools(client: AirflowClient) -> list[BaseTool]:

    @tool(parse_docstring=True)
    def list_dags() -> str:
        """List all Airflow DAGs and whether each is paused.

        Use this first when you don't know the exact dag_id.
        """
        try:
            dags = client.list_dags()
        except httpx.HTTPStatusError as e:
            return _http_error(e)
        return "\n".join(f"{d['dag_id']} (paused={d['is_paused']})" for d in dags) or "No DAGs found."

    @tool(parse_docstring=True)
    def get_recent_dag_runs(dag_id: str, limit: int = 5) -> str:
        """Get the most recent runs of a DAG, newest first, with state and timing.

        Use this to answer "when did X last succeed/fail" and to find the
        run_id needed by get_task_instances.

        Args:
            dag_id: Exact DAG id, e.g. "daily_lake_maintenance".
            limit: How many runs to return (1-25).
        """
        try:
            runs = client.get_dag_runs(dag_id, limit=max(1, min(limit, 25)))
        except httpx.HTTPStatusError as e:
            return _http_error(e)
        if not runs:
            return f"No runs found for {dag_id}."
        return "\n".join(
            f"run_id={r['dag_run_id']} | state={r['state']} | "
            f"queued={r.get('queued_at')} | start={r.get('start_date')} | end={r.get('end_date')}"
            for r in runs
        )

    @tool(parse_docstring=True)
    def get_task_instances(dag_id: str, run_id: str) -> str:
        """Get every task in one DAG run with its state, try number and duration.

        Use this to find which task failed, retried or is still running.

        Args:
            dag_id: Exact DAG id.
            run_id: Exact run_id from get_recent_dag_runs.
        """
        try:
            tasks = client.get_task_instances(dag_id, run_id)
        except httpx.HTTPStatusError as e:
            return _http_error(e)
        return "\n".join(
            f"task_id={t['task_id']} | state={t['state']} | try_number={t['try_number']} | "
            f"duration_s={t.get('duration')}"
            for t in tasks
        ) or "No tasks found."

    @tool(parse_docstring=True)
    def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        """Get the log of one task attempt, trimmed to errors/warnings and the last lines.

        Use this to find WHY a task failed or behaved unexpectedly.

        Args:
            dag_id: Exact DAG id.
            run_id: Exact run_id from get_recent_dag_runs.
            task_id: Task id from get_task_instances.
            try_number: Attempt number from get_task_instances (the latest attempt is usually what you want).
        """
        try:
            log = client.get_task_log(dag_id, run_id, task_id, max(1, try_number))
        except httpx.HTTPStatusError as e:
            return _http_error(e)
        return _trim_log(log.get("content", []))

    return [list_dags, get_recent_dag_runs, get_task_instances, get_task_log]
