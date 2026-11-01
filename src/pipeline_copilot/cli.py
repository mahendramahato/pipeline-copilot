"""Terminal chat for Pipeline Copilot. The agent itself lives in runtime.py,
shared with the web API, so both show exactly the same steps."""
import argparse
import asyncio

from pipeline_copilot import remediation as rem
from pipeline_copilot.runtime import new_thread_id, open_agent, resume_turn, run_turn


def _show_diagnosis(d: dict) -> None:
    print("  ┌─ DIAGNOSIS " + "─" * 50)
    print(f"  │ {d['summary']}")
    print(f"  │ category={d['category']}  confidence={d['confidence']}")
    print(f"  │ root cause: {d['root_cause']}")
    for e in d["evidence"]:
        mark = "✓" if e.get("grounded") else "✗ UNGROUNDED"
        print(f"  │ {mark} [{e['tool']}]: \"{e['quote']}\"  → {e['meaning']}")
    g = d.get("grounding", {})
    lowered = f" (lowered from {g['confidence_lowered_from']})" if g.get("confidence_lowered_from") else ""
    print(f"  │ grounding: {g.get('checked', 0) - g.get('ungrounded', 0)}/{g.get('checked', 0)} quotes verified{lowered}")
    print(f"  │ impact: {d['impact']}")
    print(f"  │ fix: {d['suggested_fix']}")
    if d["runbooks_used"]:
        print(f"  │ runbooks: {', '.join(d['runbooks_used'])}")
    for u in d["unverified"]:
        print(f"  │ unverified: {u}")
    print("  └" + "─" * 62 + "\n")


def _show_plan(p: dict) -> None:
    for a in p["actions"]:
        print(f"  ⚙ proposed: {rem.describe(a)}  [risk={a['risk']}]  {a['why']}")
    for a in p["rejected"]:
        print(f"  ⊘ not offered: {a['action']} ({a['reason']})")
    if p.get("note"):
        print(f"  ℹ {p['note']}")


def _show(event: dict) -> None:
    match event["type"]:
        case "tool_call":
            args = ", ".join(f"{k}={v!r}" for k, v in event["args"].items())
            print(f"  → {event['name']}({args})")
        case "tool_result":
            print(f"  ✓ {event['name']} returned {event['chars']} chars")
        case "answer":
            print(f"\n{event['text']}\n")
        case "diagnosis":
            _show_diagnosis(event["diagnosis"])
        case "escalated":
            print(f"  ↯ {event['text']}")
        case "plan":
            _show_plan(event["plan"])
        case "progress":
            print(f"    … {event['text']}")
        case "approval":
            a = event["approval"]
            print(f"  {'✓ approved' if a['approved'] else '✗ rejected'} by {a['by']}"
                  + (f" ({a['note']})" if a.get("note") else ""))
        case "remediation":
            r = event["remediation"]
            print(f"\n  FIX OUTCOME: {r['outcome'].upper()}")
            for step in r["results"]:
                print(f"  {'✓' if step['verified'] else '✗'} {rem.describe(step)}: {step['verification']}")
            for c in r.get("still_failing", []):
                print(f"  ! still failing: {c['check']} ({c['detail']})")
            print()
        case "memory":
            if event.get("note"):
                print(f"  memory: {event['note']}\n")
        case "error":
            print(f"  ✗ {event['message']}\n")


async def chat(thread_id: str) -> None:
    async with open_agent() as agent:
        print(f"Pipeline Copilot ({agent.settings.llm_model})")
        print(f"Tools from MCP: {', '.join(t.name for t in agent.tools)}")
        print(f"Conversation: {thread_id}  (resume with: uv run pipeline-copilot --thread {thread_id})")
        print("Ask about your pipeline. 'exit' to quit.\n")

        while True:
            # input() blocks; running it in a thread keeps the event loop
            # (and the MCP connections) alive while you type.
            try:
                question = (await asyncio.to_thread(input, "you> ")).strip()
            except EOFError:
                print()
                break
            if question.lower() in {"exit", "quit"}:
                break
            if question:
                events = run_turn(agent, thread_id, question)
                # A fix may need approval; keep asking until the run finishes
                while events is not None:
                    pending = None
                    async for event in events:
                        _show(event)
                        if event["type"] == "approval_required":
                            pending = event["request"]
                    events = None
                    if pending:
                        answer = (await asyncio.to_thread(input, "  Apply this fix? [y/N] ")).strip().lower()
                        events = resume_turn(agent, thread_id, {"approved": answer in ("y", "yes"), "by": "cli"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Copilot")
    parser.add_argument("--thread", help="Resume a saved conversation by its id")
    args = parser.parse_args()
    try:
        asyncio.run(chat(args.thread or new_thread_id()))
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
