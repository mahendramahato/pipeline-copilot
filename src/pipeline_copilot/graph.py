"""The agent: Claude + tools wired into a LangGraph loop."""
from datetime import datetime, timezone

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from pipeline_copilot.config import Settings

# --- System prompt ---
# Kept stable (no timestamps or per-request values), so the API can cache
# it across calls. Facts about the pipeline live here for now; in Phase 4
# they move into RAG documents the agent searches.
SYSTEM_PROMPT = """You are Pipeline Copilot, an on-call assistant for a data pipeline.

You investigate using read-only Airflow tools. You cannot change anything.

Pipeline facts:
- Airflow DAG `daily_lake_maintenance` runs daily at 00:30 UTC.
- Tasks: check_freshness -> sync_to_s3 -> curate_day, and target_date -> curate_day.
- check_freshness fails if the streaming lake's newest file is older than 3 hours,
  which means a streaming job (producer, Kafka or Spark) has probably stopped.
- curate_day runs the AWS Glue job `curate-daily`.

How to work:
- Use tools to get facts. Never guess run IDs, states, times or log contents.
- Call get_current_time before reasoning about "today", "last night" or durations.
- Base every claim on tool output and cite the evidence (run_id, task_id, log line).
- If a tool returns an error, adjust and try again (e.g. list_dags to find the right id).
- All times are UTC. Be concise.
"""


# --- Current time as a tool (keeps the system prompt cacheable) ---
@tool
def get_current_time() -> str:
    """Get the current date and time in UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_graph(settings: Settings, tools: list[BaseTool]):
    all_tools = [*tools, get_current_time]

    # --- The model ---
    # bind_tools sends the tool schemas (from your docstrings) with every
    # request, so Claude knows what it can call. The API key is read from
    # ANTHROPIC_API_KEY by the SDK; we never pass it around.
    llm = ChatAnthropic(model=settings.llm_model, max_tokens=16000)
    llm_with_tools = llm.bind_tools(all_tools)

    # --- Node 1: agent ---
    # Reads all messages so far, asks Claude for the next step. Returning
    # {"messages": [response]} APPENDS (MessagesState's reducer), so history
    # is never overwritten.
    def agent(state: MessagesState) -> dict:
        response = llm_with_tools.invoke([SystemMessage(SYSTEM_PROMPT), *state["messages"]])
        return {"messages": [response]}

    # --- Wiring ---
    graph = StateGraph(MessagesState)
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode(all_tools))   # runs whatever tools Claude asked for

    graph.add_edge(START, "agent")
    # tools_condition: tool calls in the last message → "tools", else → END
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")               # results go back to Claude: the loop

    return graph.compile()
