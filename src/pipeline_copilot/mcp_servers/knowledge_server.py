"""Knowledge MCP server: search the team's runbooks and pipeline docs (RAG).

    uv run python -m pipeline_copilot.mcp_servers.knowledge_server

Requires the index: uv run python -m pipeline_copilot.knowledge_base
Never print() here: stdout carries the MCP protocol.
"""
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from pipeline_copilot.config import load_knowledge_settings
from pipeline_copilot.knowledge_base import search

mcp = FastMCP("knowledge", log_level="WARNING")
READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)


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
        results = search(query, k, load_knowledge_settings())
    except Exception as e:
        return (
            f"Knowledge base unavailable ({type(e).__name__}: {e}). "
            "Build the index with: uv run python -m pipeline_copilot.knowledge_base"
        )
    return "\n\n---\n\n".join(
        f"[source: {r['source']} > {r['section']}]\n{r['text']}" for r in results
    )


if __name__ == "__main__":
    mcp.run()
