"""Terminal chat for Pipeline Copilot."""
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langgraph.errors import GraphRecursionError

from pipeline_copilot.airflow_client import AirflowClient
from pipeline_copilot.airflow_tools import make_airflow_tools
from pipeline_copilot.config import load_settings
from pipeline_copilot.graph import build_graph

# Max graph steps per question (each agent call and each tool round is a step).
# Stops a confused agent from looping, and billing you, forever.
MAX_STEPS = 20


# --- Show one message the way a human wants to read it ---
# Claude's replies are lists of blocks (thinking, tool_use, text).
# .tool_calls and .text pull out just the parts worth showing.
def _show(msg: BaseMessage) -> None:
    if isinstance(msg, AIMessage) and msg.tool_calls:
        for call in msg.tool_calls:
            args = ", ".join(f"{k}={v!r}" for k, v in call["args"].items())
            print(f"  → {call['name']}({args})")
    elif isinstance(msg, ToolMessage):
        print(f"  ✓ {msg.name} returned {len(msg.text)} chars")
    elif isinstance(msg, AIMessage):
        print(f"\n{msg.text}\n")


def main() -> None:
    settings = load_settings()
    graph = build_graph(settings, make_airflow_tools(AirflowClient(settings)))

    # Conversation so far. Passed in full on every question, which is what
    # lets follow-ups like "and the run before that?" work.
    history: list[BaseMessage] = []

    print(f"Pipeline Copilot ({settings.llm_model}). Ask about your pipeline. 'exit' to quit.\n")
    while True:
        try:
            question = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):    # Ctrl+D / Ctrl+C
            print()
            break
        if question.lower() in {"exit", "quit"}:
            break
        if not question:
            continue

        # Remember where this turn started, so a failed turn can be undone.
        turn_start = len(history)
        history.append(HumanMessage(question))

        try:
            # stream_mode="updates" yields {node_name: {"messages": [new msgs]}}
            # after each node finishes, so tool calls appear live.
            for update in graph.stream(
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
            # A turn that died halfway can leave a tool call with no result,
            # and the API rejects that on the next question. Drop the whole turn.
            del history[turn_start:]


if __name__ == "__main__":
    main()
