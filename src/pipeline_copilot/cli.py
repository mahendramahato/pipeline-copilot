"""Terminal chat for Pipeline Copilot. Tools come from MCP servers."""
import asyncio
import sys

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.errors import GraphRecursionError

from pipeline_copilot.config import load_agent_settings
from pipeline_copilot.graph import build_graph

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


async def chat() -> None:
    settings = load_agent_settings()
    client = MultiServerMCPClient(MCP_SERVERS)

    # --- One persistent MCP session for the whole chat ---
    # The server process starts here and stops when this block exits,
    # instead of a new process (and a new Airflow login) per tool call.
    async with client.session("airflow") as session:
        tools = await load_mcp_tools(session)
        graph = build_graph(settings, tools)

        print(f"Pipeline Copilot ({settings.llm_model})")
        print(f"Tools from MCP: {', '.join(t.name for t in tools)}")
        print("Ask about your pipeline. 'exit' to quit.\n")

        history: list[BaseMessage] = []
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

            turn_start = len(history)
            history.append(HumanMessage(question))
            try:
                async for update in graph.astream(
                    {"messages": history},
                    config={"recursion_limit": MAX_STEPS},
                    stream_mode="updates",
                ):
                    for change in update.values():
                        for msg in change["messages"]:
                            history.append(msg)
                            _show(msg)
            except GraphRecursionError:
                print(f"  ✗ Stopped after {MAX_STEPS} steps without an answer. Try a narrower question.\n")
                del history[turn_start:]
            except Exception as e:
                print(f"  ✗ {type(e).__name__}: {e}\n")
                del history[turn_start:]


def main() -> None:
    # Ctrl+C anywhere: exit quietly instead of printing a traceback
    try:
        asyncio.run(chat())
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
