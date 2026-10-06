"""Knowledge MCP server: search the team's runbooks and pipeline docs (RAG).

    uv run python -m pipeline_copilot.mcp_servers.knowledge_server

Requires the index: uv run python -m pipeline_copilot.knowledge_base
Never print() here: stdout carries the MCP protocol.
"""
import json
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot import knowledge_base as kb
from pipeline_copilot.config import load_knowledge_settings

mcp = FastMCP("knowledge", log_level="WARNING")
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)


# Writes only to the agent's OWN memory, never to the pipeline. Idempotent: an upsert.
MEMORY_WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)

@mcp.tool(annotations=READ_ONLY)
def search_runbooks(
    query: Annotated[str, Field(description=(
        "The symptom or question in plain words, e.g. "
        "'curated data missing but Glue job succeeded' or 'how to backfill a day'."
    ))],
    k: Annotated[int, Field(description="How many sections to return.", ge=1, le=6)] = 3,
) -> str:
    """Search the team's runbooks and pipeline docs: known failure modes, how to check them, fixes.

    Use this whenever you see a symptom, and BEFORE concluding a root cause. The runbooks
    hold pipeline-specific knowledge that the other tools and general knowledge lack.
    Each result starts with its source (file > section): cite it in your answer.
    """
    try:
        results = kb.search(query, k, load_knowledge_settings())
    except Exception as e:
        return (
            f"Knowledge base unavailable ({type(e).__name__}: {e}). "
            "Build the index with: uv run python -m pipeline_copilot.knowledge_base"
        )
    return "\n\n---\n\n".join(
        f"[source: {r['source']} > {r['section']}]\n{r['text']}" for r in results
    )


@mcp.tool(annotations=READ_ONLY)
def search_past_incidents(
    query: Annotated[str, Field(description="The symptom or suspected problem, e.g. 'curated data missing'.")],
    k: Annotated[int, Field(description="How many past incidents to return.", ge=1, le=5)] = 3,
) -> str:
    """Search incidents diagnosed in earlier investigations: when, category, root cause, fix.

    Use at the start of an incident investigation to answer "has this happened before?".
    A match is a lead, not proof: confirm with current evidence.
    """
    try:
        results = kb.search_incidents(query, k, load_knowledge_settings())
    except Exception as e:
        return f"Incident memory unavailable ({type(e).__name__}: {e})."
    if not results:
        return "No past incidents recorded yet."
    return "\n\n---\n\n".join(
        f"[incident {r['id']} | confidence={r['confidence']} | conversation={r['thread_id']}]\n"
        f"{r['text']}\nFix suggested: {r['fix']}\nOriginal question: {r['question']}"
        for r in results
    )


@mcp.tool(annotations=MEMORY_WRITE)
def record_incident(
    diagnosis_json: Annotated[str, Field(description="A verified Diagnosis as JSON.")],
    question: Annotated[str, Field(description="The user's question that started the investigation.")],
    thread_id: Annotated[str, Field(description="The conversation id.")],
) -> str:
    """Save a verified diagnosis to incident memory.

    Called by the agent's graph after the output guardrail, not by the model.
    """
    incident_id = kb.record_incident(json.loads(diagnosis_json), question, thread_id, load_knowledge_settings())
    return f"saved as incident {incident_id}"



if __name__ == "__main__":
    mcp.run()
