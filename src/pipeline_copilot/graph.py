"""The agent: an LLM + tools wired into a LangGraph loop."""
import json
from datetime import datetime, timezone

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from pipeline_copilot.config import AgentSettings
from pipeline_copilot.graph_state import AgentState
from pipeline_copilot.guardrails import make_input_guardrail
from pipeline_copilot.llm import chat_model, provider_for
from pipeline_copilot.models import Diagnosis
from pipeline_copilot.output_guardrail import check_grounding

# --- System prompt ---
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
- Conceptual questions (what is Kafka/Spark/Glue, how the pipeline or this assistant works, how
  services connect): answer from search_runbooks, which holds explainers for every technology
  here; no live Airflow/Athena calls needed. Explain how it applies to THIS pipeline and cite.

- For incidents, call search_past_incidents early: has this happened before? A match is a lead, not proof.

"""


# --- Current time as a tool (keeps the system prompt cacheable) ---
@tool
def get_current_time() -> str:
    """Get the current date and time in UTC."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

DIAGNOSIS_PROMPT = """You turn a finished on-call investigation into a structured diagnosis.
Use ONLY what is in the transcript. Every evidence quote must be copied exactly,
character for character, from a TOOL RESULT. If the investigation found no problem,
say so (category no_problem_found). List anything that could not be verified."""


# --- This turn's messages as plain text ---
# Plain text instead of the raw messages: the raw ones carry Opus thinking
# blocks tied to the original conversation, which shouldn't be replayed in a
# different request. Text is also cheaper.
def _turn_transcript(messages: list) -> str:
    start = max(i for i, m in enumerate(messages) if isinstance(m, HumanMessage))
    lines = []
    for m in messages[start:]:
        if isinstance(m, HumanMessage):
            lines.append(f"USER QUESTION: {m.text}")
        elif isinstance(m, AIMessage) and m.tool_calls:
            for call in m.tool_calls:
                lines.append(f"TOOL CALL {call['name']} {json.dumps(call['args'])}")
        elif isinstance(m, ToolMessage):
            lines.append(f"TOOL RESULT [{m.name}]:\n{m.text}")
        elif isinstance(m, AIMessage):
            lines.append(f"AGENT'S FINAL ANSWER:\n{m.text}")
    return "\n\n".join(lines)

# --- (tool_name, output) for every tool result in this turn ---
def _turn_tool_outputs(messages: list) -> list[tuple[str, str]]:
    start = max(i for i, m in enumerate(messages) if isinstance(m, HumanMessage))
    return [(m.name, m.text) for m in messages[start:] if isinstance(m, ToolMessage)]

# Tools only the graph may call. The model never sees them, so it can't write
# to memory on its own; saving is a deterministic step after verification.
GRAPH_ONLY_TOOLS = {"record_incident"}


# MCP tool results can come back as plain text or a list of content blocks
def _as_text(result) -> str:
    if isinstance(result, str):
        return result
    return " ".join(b.get("text", "") for b in result if isinstance(b, dict)) or str(result)


def build_graph(settings: AgentSettings, tools: list[BaseTool], checkpointer=None):
    # get_current_time is added only if the caller didn't supply one: evals pass a
    # fake clock so "yesterday" means the same thing on every run.
    all_tools = [t for t in tools if t.name not in GRAPH_ONLY_TOOLS]
    if not any(t.name == "get_current_time" for t in all_tools):
        all_tools.append(get_current_time)

    recorder = next((t for t in tools if t.name == "record_incident"), None)

    # --- The model (Claude or OpenAI, picked from the model name) ---
    # bind_tools sends the tool schemas (from your docstrings) with every
    # request, so the model knows what it can call. API keys are read from the
    # environment by each provider's SDK; we never pass them around.
    provider = provider_for(settings.llm_model)
    llm = chat_model(settings.llm_model, max_tokens=16000)
    if provider == "anthropic":
        # Prompt caching: every agent call resends the system prompt, the tool schemas
        # and the whole conversation so far. Top-level cache_control tells the API to
        # cache everything up to the last block, so the next call reads that repeated
        # prefix at ~5% of the input price instead of paying full price again.
        # The cache lives ~5 minutes, which covers an investigation's back-to-back calls.
        llm_with_tools = llm.bind_tools(all_tools, cache_control={"type": "ephemeral"})
    else:
        # OpenAI caches repeated prompt prefixes automatically: nothing to switch on
        llm_with_tools = llm.bind_tools(all_tools)

    # --- Node 1: agent ---
    # async because the graph now runs with astream(): MCP tools are
    # async-only, and an async graph needs async nodes to await the model.
    async def agent(state: AgentState) -> dict:
        response = await llm_with_tools.ainvoke([SystemMessage(SYSTEM_PROMPT), *state["messages"]])
        return {"messages": [response]}

    # --- Node: diagnose (incidents only) ---
    # One focused call that extracts a typed Diagnosis from the investigation.
    # method="json_schema": the API constrains the output to the schema directly
    # (Opus 5.5 can't be forced to call a tool, which other methods rely on).
    # On OpenAI, strict=True makes the schema binding exact rather than best-effort.
    diagnoser = chat_model(settings.llm_model, max_tokens=16000).with_structured_output(
        Diagnosis, method="json_schema", **({"strict": True} if provider == "openai" else {})
    )

    async def diagnose(state: AgentState) -> dict:
        diagnosis = await diagnoser.ainvoke([
            SystemMessage(DIAGNOSIS_PROMPT),
            HumanMessage(_turn_transcript(state["messages"])),
        ])
        return {"diagnosis": diagnosis.model_dump()}
    
    # --- Node: verify_diagnosis (output guardrail, deterministic) ---
    # Checks every evidence quote against the real tool outputs of this turn.
    def verify_diagnosis(state: AgentState) -> dict:
        return {"diagnosis": check_grounding(state["diagnosis"], _turn_tool_outputs(state["messages"]))}

    
    # --- Node: remember (incident memory) ---
    # Saves VERIFIED diagnoses only: memory should hold conclusions, not guesses.
    # config gives access to the thread_id, so a past incident links to its conversation.
    async def remember(state: AgentState, config: RunnableConfig) -> dict:
        d = state["diagnosis"]
        if recorder is None:
            note = "not saved: incident memory unavailable"
        elif d["confidence"] == "low" or d["category"] in ("no_problem_found", "unknown"):
            note = f"not saved (confidence={d['confidence']}, category={d['category']})"
        else:
            question = next(m.text for m in reversed(state["messages"]) if isinstance(m, HumanMessage))
            note = _as_text(await recorder.ainvoke({
                "diagnosis_json": json.dumps(d),
                "question": question,
                "thread_id": config["configurable"]["thread_id"],
            }))
        return {"diagnosis": {**d, "memory": note}}

    # --- Route after the agent ---
    # Replaces tools_condition: tool calls → tools; otherwise incidents get
    # diagnosed and plain questions are done.
    def after_agent(state: AgentState) -> str:
        if state["messages"][-1].tool_calls:
            return "tools"
        return "diagnose" if state.get("intent") == "incident" else END

    # --- Route after the guardrail ---
    # The guardrail adds an AIMessage only when it refuses. So if the last
    # message is from the AI, we're done; if it's still the user's question, go on.
    def after_guardrail(state: AgentState) -> str:
        return END if isinstance(state["messages"][-1], AIMessage) else "agent"

    # --- Wiring ---
    graph = StateGraph(AgentState)
    graph.add_node("input_guardrail", make_input_guardrail(settings.guardrail_model))
    graph.add_node("agent", agent)
    graph.add_node("tools", ToolNode(all_tools))
    graph.add_node("diagnose", diagnose)
    graph.add_node("verify_diagnosis", verify_diagnosis)
    graph.add_node("remember", remember)

    graph.add_edge(START, "input_guardrail")
    graph.add_conditional_edges("input_guardrail", after_guardrail, ["agent", END])
    graph.add_conditional_edges("agent", after_agent, ["tools", "diagnose", END])
    graph.add_edge("tools", "agent")
    graph.add_edge("diagnose", "verify_diagnosis")
    graph.add_edge("verify_diagnosis", "remember")
    graph.add_edge("remember", END)

    # The checkpointer saves state after every node, keyed by thread_id
    return graph.compile(checkpointer=checkpointer)

