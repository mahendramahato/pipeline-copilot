"""Typed results the agent produces."""
from typing import Literal

from pydantic import BaseModel, Field


class Evidence(BaseModel):
    tool: str = Field(description="Name of the tool whose output supports this, e.g. 'run_query'.")
    quote: str = Field(description=(
        "An EXACT substring copied from that tool's output: a value, a row or a log line. "
        "Copy it character for character. Never paraphrase or combine."
    ))
    meaning: str = Field(description="What this evidence shows, in one sentence.")


class ProposedAction(BaseModel):
    """One step of an automated fix, chosen from a fixed catalog (remediation.CATALOG).

    Every argument is present (null when unused) so the schema also works with
    OpenAI's strict mode. Code re-checks every value before a human sees it."""
    action: Literal["clear_task", "backfill_date", "restart_container"]
    run_id: str | None = Field(description="clear_task: the exact Airflow run_id from a tool result. Else null.")
    task_id: str | None = Field(description="clear_task: the task to re-run, e.g. 'curate_day'. Else null.")
    date: str | None = Field(description="backfill_date: the day to re-curate, 'YYYY-MM-DD'. Else null.")
    container: str | None = Field(description="restart_container: the container name from list_containers. Else null.")
    why: str = Field(description="One sentence: why this action fixes the root cause.")


class Diagnosis(BaseModel):
    summary: str = Field(description="One sentence: what is wrong.")
    category: Literal[
        "silent_data_gap", "schema_drift", "bad_upstream_data", "stale_streaming",
        "transient_failure", "duplicates", "code_bug", "stuck_task",
        "config_or_infra", "no_problem_found", "unknown",
    ]
    root_cause: str = Field(description="The underlying cause, as specifically as the evidence allows.")
    evidence: list[Evidence] = Field(description="The facts that support the root cause.")
    impact: str = Field(description="What is affected: tables, dates, row counts, consumers.")
    suggested_fix: str = Field(description="The fix in plain words, including any steps only a human can do.")
    fix_actions: list[ProposedAction] = Field(description=(
        "Automated actions that would fix the root cause, run only after a human approves. "
        "Empty when nothing should be run: no problem, a transient failure that already "
        "recovered, a code or config bug a human must fix first, or the cause is unclear."
    ))
    runbooks_used: list[str] = Field(description="Runbook sources used, e.g. 'runbook_backfill.md'.")
    unverified: list[str] = Field(description="Things that could not be checked with the available tools.")
    confidence: Literal["low", "medium", "high"]
