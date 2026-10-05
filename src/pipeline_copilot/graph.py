"""The agent: Claude + tools wired into a LangGraph loop."""
from datetime import datetime, timezone

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, AIMessage
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from pipeline_copilot.config import AgentSettings
from pipeline_copilot.guardrails import make_input_guardrail


# --- System prompt ---
# Kept stable (no timestamps or per-request values), so the API can cache
# it across calls. Facts about the pipeline live here for now; in Phase 4
# they move into RAG documents the agent searches.
SYSTEM_PROMPT = """You are Pipeline Copilot, an on-call assistant for a weather and seismic data pipeline.

You investigate with read-only tools for Airflow (task runs and logs) and Athena
(the data lake). You cannot change anything.

How data flows:
- Streaming jobs on a VM write a local lake continuously.
- Airflow DAG `daily_lake_maintenance` runs daily at 00:30 UTC:
  check_freshness -> sync_to_s3 -> curate_day (and target_date -> curate_day).
  - check_freshness fails if the local lake's newest file is > 3h old
    (a streaming job, producer or Kafka has probably stopped).
  - sync_to_s3 uploads the local lake to S3; this is the ONLY way raw data reaches Athena.
  - curate_day runs Glue job `curate-daily` for ONE day: the day before the run.

Tables (Athena database weather_seismic, partitioned by `date` = 'YYYY-MM-DD' string):
- raw_weather, raw_seismic: everything synced. raw_seismic keeps every USGS revision of a quake.
- curated_weather: raw_weather deduplicated. curated_seismic: latest revision per quake.
- curated_weather_daily: one row per station per day.
- alerts: a view over the raw tables.

What NORMAL looks like:
- Athena raw tables can be up to ~24h behind real time. That alone is NOT an outage.
- After a successful 00:30 run on day D, curated tables should contain day D-1.
- Curated row counts per day should be close to raw counts (dedup removes few rows).

How to work:
- Use tools to get facts. Never guess run IDs, states, times, row counts or log contents.
- Call get_current_time before reasoning about "today", "yesterday" or durations.
- Check a table's schema before querying it. Filter on `date` and prefer aggregates.
- Compare across layers (Airflow state vs raw data vs curated data); problems hide in the gaps.
- Base every claim on tool output and cite the evidence (run_id, task_id, query result).
- If a tool returns an error, adjust and retry. All times are UTC. Be concise.
"""


# --- Current time as a tool (keeps the system prompt cacheable) ---
@tool
def get_current_time() -> str:
    """Get the current date and time in UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_graph(settings: AgentSettings, tools: list[BaseTool]):
    all_tools = [*tools, get_current_time]

    # --- The model ---
    # bind_tools sends the tool schemas (from your docstrings) with every
    # request, so Claude knows what it can call. The API key is read from
    # ANTHROPIC_API_KEY by the SDK; we never pass it around.
    llm = ChatAnthropic(model=settings.llm_model, max_tokens=16000)
    llm_with_tools = llm.bind_tools(all_tools)

    # --- Node 1: agent ---
    # async because the graph now runs with astream(): MCP tools are
    # async-only, and an async graph needs async nodes to await Claude.
    async def agent(state: MessagesState) -> dict:
        response = await llm_with_tools.ainvoke([SystemMessage(SYSTEM_PROMPT), *state["messages"]])
        return {"messages": [response]}

        # --- Route after the guardrail ---
    # The guardrail adds an AIMessage only when it refuses. So if the last
    # message is from the AI, we're done; if it's still the user's question, go on.
    def after_guardrail(state: MessagesState) -> str:
        return END if isinstance(state["messages"][-1], AIMessage) else "agent"

    # --- Wiring ---
    graph = StateGraph(MessagesState)
    graph.add_node("input_guardrail", make_input_guardrail(settings.guardrail_model))
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode(all_tools))

    graph.add_edge(START, "input_guardrail")
    graph.add_conditional_edges("input_guardrail", after_guardrail, ["agent", END])
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")

    return graph.compile()


    graph.add_edge(START, "agent")
    # tools_condition: tool calls in the last message → "tools", else → END
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")               # results go back to Claude: the loop

    return graph.compile()
