"""Terminal chat for Pipeline Copilot. Tools come from MCP servers."""
import asyncio
import sys
import argparse

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.errors import GraphRecursionError
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from pipeline_copilot.config import load_agent_settings
from pipeline_copilot.graph import build_graph

from contextlib import AsyncExitStack
from datetime import datetime, timezone

MAX_STEPS = 20

# --- Which MCP servers to start, and how ---
# stdio: the agent launches each server as a subprocess and talks over its
# stdin/stdout. sys.executable = this same Python/venv, so the server can
# import pipeline_copilot without going through `uv run` (faster startup).
# The server loads its own credentials from .env: the agent passes no secrets.
MCP_SERVERS = {
    "airflow": {
        "transport": "stdio",
        "command": sys.executable,
        "args": ["-m", "pipeline_copilot.mcp_servers.airflow_server"],
    },
    "athena": {
        "transport": "stdio",
        "command": sys.executable,
        "args": ["-m", "pipeline_copilot.mcp_servers.athena_server"],
    },
    "knowledge": {
        "transport": "stdio",
        "command": sys.executable,
        "args": ["-m", "pipeline_copilot.mcp_servers.knowledge_server"],
    },

}



def _show(msg: BaseMessage) -> None:
    if isinstance(msg, AIMessage) and msg.tool_calls:
        for call in msg.tool_calls:
            args = ", ".join(f"{k}={v!r}" for k, v in call["args"].items())
            print(f"  → {call['name']}({args})")
    elif isinstance(msg, ToolMessage):
        print(f"  ✓ {msg.name} returned {len(msg.text)} chars")
    elif isinstance(msg, AIMessage):
        print(f"\n{msg.text}\n")

# --- Repair a turn that died halfway ---
# State is saved after every node, so a crash between "Claude asked for a tool"
# and "the tool answered" leaves a dangling tool call in the saved thread, and
# the API rejects that history on the next question. We don't delete anything
# (history is append-only): we append a "cancelled" result for each dangling call.
async def _repair_dangling_tool_calls(graph, config: dict) -> None:
    state = await graph.aget_state(config)
    messages = state.values.get("messages", [])
    if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls:
        cancelled = [
            ToolMessage(
                content="Cancelled: this turn was aborted before the tool ran.",
                tool_call_id=call["id"],
                name=call["name"],
            )
            for call in messages[-1].tool_calls
        ]
        # as_node="tools": record the update as if the tools node produced it
        await graph.aupdate_state(config, {"messages": cancelled}, as_node="tools")

async def chat(thread_id: str) -> None:
    settings = load_agent_settings()
    client = MultiServerMCPClient(MCP_SERVERS)

    # --- One persistent session per server ---
    # AsyncExitStack holds any number of `async with` blocks open at once
    # and closes them all (stopping each server process) when the chat ends.
    async with AsyncExitStack() as stack:
        tools = []
        for name in MCP_SERVERS:
            session = await stack.enter_async_context(client.session(name))
            tools += await load_mcp_tools(session)
        
        # open the sqlite checkpointer for as long as the chat runs
        checkpointer = await stack.enter_async_context(
            AsyncSqliteSaver.from_conn_string(str(settings.memory_db))
        )  
        graph = build_graph(settings, tools, checkpointer)

        # every call names the conversation it belongs to
        config = {"configurable": {"thread_id": thread_id}, "recursion_limit": MAX_STEPS}
        
        print(f"Pipeline Copilot ({settings.llm_model})")
        print(f"Tools from MCP: {', '.join(t.name for t in tools)}")
        print(f"Conversation: {thread_id}  (resume with: uv run pipeline-copilot --thread {thread_id})")
        print("Ask about your pipeline. 'exit' to quit.\n")

        while True:
            # input() blocks; running it in a thread keeps the event loop
            # (and the MCP connection) alive while you type.
            try:
                question = (await asyncio.to_thread(input, "you> ")).strip()
            except EOFError:
                print()
                break
            if question.lower() in {"exit", "quit"}:
                break
            if not question:
                continue

            try:
                # send only the new message; the checkpointer supplies the rest
                async for update in graph.astream(
                    {"messages": [HumanMessage(question)]}, config=config, stream_mode="updates"
                ):
                    for change in update.values():
                        for msg in (change or {}).get("messages", []):
                            _show(msg)
            except GraphRecursionError:
                print(f"  ✗ Stopped after {MAX_STEPS} steps without an answer. Try a narrower question.\n")
                await _repair_dangling_tool_calls(graph, config)
            except Exception as e:
                print(f"  ✗ {type(e).__name__}: {e}\n")
                await _repair_dangling_tool_calls(graph, config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Copilot")
    parser.add_argument("--thread", help="Resume a saved conversation by its id")
    args = parser.parse_args()
    # New conversations get a readable, sortable id
    thread_id = args.thread or datetime.now(timezone.utc).strftime("chat-%Y%m%d-%H%M%S")
    try:
        asyncio.run(chat(thread_id))
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
