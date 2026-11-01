"""The agent runtime shared by the terminal chat and the web API.

open_agent() starts the MCP servers, opens conversation memory and builds the
graph. run_turn() runs one question and yields plain event dicts, so every
front end (terminal, browser) shows exactly the same steps.
"""
import sys
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.errors import GraphRecursionError
from langgraph.types import Command

from pipeline_copilot.config import AgentSettings, load_agent_settings
from pipeline_copilot.graph import build_graph

# Graph steps per run: the agent/tools loop plus up to 8 fixed steps
# (triage, diagnose, verify, plan, approval, execute, validate, remember)
MAX_STEPS = 26

# --- Which MCP servers to start, and how ---
# stdio: the agent launches each server as a subprocess and talks over its
# stdin/stdout. sys.executable = this same Python/venv, so the server can
# import pipeline_copilot without going through `uv run` (faster startup).
# The server loads its own credentials from .env: the agent passes no secrets.
MCP_SERVERS = {
    name: {"transport": "stdio", "command": sys.executable,
           "args": ["-m", f"pipeline_copilot.mcp_servers.{name}_server"]}
    # actions: the only server that can change the pipeline; the model never sees its tools
    for name in ("airflow", "athena", "knowledge", "ops", "actions")
}


@dataclass
class Agent:
    graph: object
    tools: list[BaseTool]
    settings: AgentSettings
    checkpointer: AsyncSqliteSaver
    tool_servers: dict[str, str]     # tool name -> MCP server it came from (for display)


def new_thread_id() -> str:
    # Readable and sortable: newest conversations sort last
    return datetime.now(timezone.utc).strftime("chat-%Y%m%d-%H%M%S")


# --- Start everything; stop it all on exit ---
# One persistent session per MCP server, plus the SQLite checkpointer, all held
# open by one AsyncExitStack and closed together (stopping the server processes).
@asynccontextmanager
async def open_agent() -> AsyncIterator[Agent]:
    settings = load_agent_settings()
    client = MultiServerMCPClient(MCP_SERVERS)
    async with AsyncExitStack() as stack:
        tools: list[BaseTool] = []
        tool_servers: dict[str, str] = {}
        for name in MCP_SERVERS:
            session = await stack.enter_async_context(client.session(name))
            loaded = await load_mcp_tools(session)
            tools += loaded
            tool_servers.update({t.name: name for t in loaded})
        checkpointer = await stack.enter_async_context(
            AsyncSqliteSaver.from_conn_string(str(settings.memory_db))
        )
        graph = build_graph(settings, tools, checkpointer)
        yield Agent(graph, tools, settings, checkpointer, tool_servers)


def _config(thread_id: str) -> dict:
    return {"configurable": {"thread_id": thread_id}, "recursion_limit": MAX_STEPS}


# --- Messages → display events (shared by live runs and loaded history) ---
# servers: tool name -> MCP server, so the UI can label each step by its source
def message_events(msg: BaseMessage, servers: dict[str, str]) -> list[dict]:
    if isinstance(msg, AIMessage) and msg.tool_calls:
        return [{"type": "tool_call", "name": c["name"], "args": c["args"],
                 "server": servers.get(c["name"], "agent")} for c in msg.tool_calls]
    if isinstance(msg, ToolMessage):
        return [{"type": "tool_result", "name": msg.name, "chars": len(msg.text)}]
    if isinstance(msg, AIMessage):
        return [{"type": "answer", "text": msg.text}]
    return []


# --- Repair a turn that died halfway ---
# State is saved after every node, so a crash between "Claude asked for a tool"
# and "the tool answered" leaves a dangling tool call in the saved thread, and
# the API rejects that history on the next question. We don't delete anything
# (history is append-only): we append a "cancelled" result for each dangling call.
async def repair_dangling_tool_calls(graph, thread_id: str) -> None:
    config = _config(thread_id)
    state = await graph.aget_state(config)
    messages = state.values.get("messages", [])
    if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls:
        cancelled = [
            ToolMessage(content="Cancelled: this turn was aborted before the tool ran.",
                        tool_call_id=call["id"], name=call["name"])
            for call in messages[-1].tool_calls
        ]
        # as_node="tools": record the update as if the tools node produced it
        await graph.aupdate_state(config, {"messages": cancelled}, as_node="tools")


# --- Graph output → display events ---
# stream_mode=["updates", "custom"]: "updates" after each node, "custom" for the
# progress lines that execute/validate write while they wait on Airflow.
async def _stream(agent: Agent, graph_input, thread_id: str) -> AsyncIterator[dict]:
    try:
        async for mode, chunk in agent.graph.astream(
            graph_input, config=_config(thread_id), stream_mode=["updates", "custom"]
        ):
            if mode == "custom":
                yield chunk
                continue
            for node, change in chunk.items():
                if node == "__interrupt__":
                    # The graph paused at await_approval: a human must decide
                    for intr in change:
                        yield {"type": "approval_required", "request": intr.value}
                    continue
                change = change or {}
                for msg in change.get("messages", []):
                    for event in message_events(msg, agent.tool_servers):
                        yield event
                if node == "triage" and change.get("intent") == "incident":
                    yield {"type": "escalated", "text": "The answer surfaced a problem: diagnosing it."}
                if node == "verify_diagnosis" and change.get("diagnosis"):
                    yield {"type": "diagnosis", "diagnosis": change["diagnosis"]}
                if node == "plan" and change.get("plan"):
                    yield {"type": "plan", "plan": change["plan"]}
                if node == "await_approval" and change.get("approval"):
                    yield {"type": "approval", "approval": change["approval"]}
                if node == "validate" and change.get("remediation"):
                    yield {"type": "remediation", "remediation": change["remediation"]}
                if node == "remember" and change.get("diagnosis"):
                    yield {"type": "memory", "note": change["diagnosis"].get("memory")}
    except GraphRecursionError:
        await repair_dangling_tool_calls(agent.graph, thread_id)
        yield {"type": "error", "message": f"Stopped after {MAX_STEPS} steps without an answer. "
                                           "Try a narrower question."}
    except Exception as e:
        await repair_dangling_tool_calls(agent.graph, thread_id)
        yield {"type": "error", "message": f"{type(e).__name__}: {e}"}


# --- Is this conversation paused, waiting for a fix to be approved? ---
async def pending_approval(agent: Agent, thread_id: str) -> dict | None:
    state = await agent.graph.aget_state(_config(thread_id))
    for task in state.tasks:
        for intr in task.interrupts:
            return intr.value
    return None


# --- Run one question; yield events as each graph node finishes ---
async def run_turn(agent: Agent, thread_id: str, question: str) -> AsyncIterator[dict]:
    # A new question while a fix waits for approval: decline that fix first, so the
    # pending step is closed on the record instead of silently dropped.
    if await pending_approval(agent, thread_id):
        async for event in resume_turn(agent, thread_id, {
            "approved": False, "by": "system", "note": "superseded by a new question"
        }):
            yield event
    # Only the new message: the checkpointer supplies the conversation so far
    async for event in _stream(agent, {"messages": [HumanMessage(question)]}, thread_id):
        yield event


# --- Answer a paused approval: run (or skip) the fix, then validate it ---
async def resume_turn(agent: Agent, thread_id: str, decision: dict) -> AsyncIterator[dict]:
    if not await pending_approval(agent, thread_id):
        yield {"type": "error", "message": "Nothing is waiting for approval in this conversation."}
        return
    async for event in _stream(agent, Command(resume=decision), thread_id):
        yield event


# --- A saved conversation, as turns of display events ---
# Only the latest turn's diagnosis, plan and fix are kept in state (each turn
# replaces them), so they're attached to the last turn.
async def load_thread(agent: Agent, thread_id: str) -> dict:
    state = await agent.graph.aget_state(_config(thread_id))
    turns: list[dict] = []
    for msg in state.values.get("messages", []):
        if isinstance(msg, HumanMessage):
            turns.append({"question": msg.text, "events": []})
        elif turns:
            turns[-1]["events"] += message_events(msg, agent.tool_servers)
    if turns:
        v, events = state.values, turns[-1]["events"]
        if v.get("diagnosis"):
            events.append({"type": "diagnosis", "diagnosis": v["diagnosis"]})
        if v.get("plan"):
            events.append({"type": "plan", "plan": v["plan"]})
        pending = next((i.value for t in state.tasks for i in t.interrupts), None)
        if pending:
            events.append({"type": "approval_required", "request": pending})
        if v.get("approval"):
            events.append({"type": "approval", "approval": v["approval"]})
        if v.get("remediation") and v["remediation"].get("outcome"):
            events.append({"type": "remediation", "remediation": v["remediation"]})
    return {"thread_id": thread_id, "turns": turns}


# --- Saved conversations, newest first, titled by their first question ---
async def list_threads(agent: Agent, limit: int = 30) -> list[dict]:
    async with agent.checkpointer.conn.execute(
        "SELECT DISTINCT thread_id FROM checkpoints ORDER BY thread_id DESC LIMIT ?", (limit,)
    ) as cursor:
        thread_ids = [row[0] async for row in cursor]
    threads = []
    for tid in thread_ids:
        state = await agent.graph.aget_state(_config(tid))
        first = next((m.text for m in state.values.get("messages", []) if isinstance(m, HumanMessage)), "")
        threads.append({"thread_id": tid, "title": first[:80] or "(empty)"})
    return threads
