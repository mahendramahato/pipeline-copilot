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

You investigate with read-only tools: Airflow (DAG runs, task logs), Athena (the data lake)
and search_runbooks (the team's runbooks and pipeline docs). You cannot change anything.

Core facts (details are in the runbooks):
- DAG `daily_lake_maintenance` runs 00:30 UTC: check_freshness -> sync_to_s3 -> curate_day.
- Athena database weather_seismic: raw_weather, raw_seismic, curated_weather, curated_seismic,
  curated_weather_daily, alerts. Partitioned by `date` ('YYYY-MM-DD' string).
- Normal: Athena raw tables lag up to ~24h. After a successful run on day D, curated has day D-1.

How to work:
- Use tools to get facts. Never guess run IDs, states, times, row counts or log contents.
- Call get_current_time before reasoning about "today", "yesterday" or durations.
- When you see a symptom, search_runbooks for it. Search again BEFORE concluding a root cause,
  and check each likely cause the runbook lists against evidence.
- Evidence beats runbooks: if tool results contradict a runbook, trust the evidence and say so.
- Check a table's schema before querying it. Filter on `date` and prefer aggregates.
- Compare across layers (Airflow state vs raw data vs curated data); problems hide in the gaps.
- Cite evidence for every claim (run_id, task_id, query result) and the runbook source you used.
- If a tool returns an error, adjust and retry. All times are UTC. Be concise.
"""


# --- Current time as a tool (keeps the system prompt cacheable) ---
@tool
def get_current_time() -> str:
    """Get the current date and time in UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_graph(settings: AgentSettings, tools: list[BaseTool], checkpointer=None):
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
    # The checkpointer saves state after every node, keyed by thread_id
    return graph.compile(checkpointer=checkpointer)


    graph.add_edge(START, "agent")
    # tools_condition: tool calls in the last message → "tools", else → END
    graph.add_conditional_edges("agent", tools_condition)
    graph.add_edge("tools", "agent")               # results go back to Claude: the loop
    # The checkpointer saves state after every node, keyed by thread_id
    return graph.compile()
