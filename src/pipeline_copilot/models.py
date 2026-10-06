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
    suggested_fix: str = Field(description="What a human should do. The agent never executes it.")
    runbooks_used: list[str] = Field(description="Runbook sources used, e.g. 'runbook_backfill.md'.")
    unverified: list[str] = Field(description="Things that could not be checked with the available tools.")
    confidence: Literal["low", "medium", "high"]
