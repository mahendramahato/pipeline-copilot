"""Run the eval suite: each scenario through the REAL graph, with fake tools.

    uv run python -m evals.run                                   # every scenario once
    uv run python -m evals.run --only schema_drift_magnitude     # one scenario
    uv run python -m evals.run --repeat 3                        # each scenario 3 times
"""
import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from langchain_core.messages import HumanMessage

from evals.fake_tools import make_fake_tools
from evals.scenarios import SCENARIOS
from evals.scoring import score
from pipeline_copilot.config import load_agent_settings
from pipeline_copilot.graph import build_graph

RESULTS_DIR = Path(__file__).parent / "results"


async def run_one(build, attempt: int) -> dict:
    s = build()                                             # a fresh, identical world every time
    graph = build_graph(load_agent_settings(), await make_fake_tools(s.world))
    # No checkpointer: each run is isolated. thread_id is still needed by the remember node.
    config = {"configurable": {"thread_id": f"eval-{s.id}-{attempt}"}, "recursion_limit": 30}
    try:
        state = await graph.ainvoke({"messages": [HumanMessage(s.question)]}, config)
    except Exception as e:
        return {"scenario": s.id, "runbook_covered": s.runbook_covered, "correct": False,
                "failure": f"{type(e).__name__}: {e}"}
    return score(s, state)


def _line(r: dict) -> str:
    mark = "✓" if r["correct"] else "✗"
    if "failure" in r:
        return f"{mark} {r['scenario']:28} FAILED: {r['failure']}"
    flags = " ".join(f for f, on in [("ungrounded", not r["grounded"]),
                                     ("CONFIDENTLY-WRONG", r["confidently_wrong"])] if on)
    return (f"{mark} {r['scenario']:28} got={r['category_got']:18} conf={r['confidence']:6} "
            f"calls={r['tool_calls']:2} ${r['cost_usd']:.2f} {flags}")


def _summary(results: list[dict]) -> str:
    def rate(rs):
        return f"{sum(r['correct'] for r in rs)}/{len(rs)}" if rs else "-"
    scored = [r for r in results if "failure" not in r]
    lines = [
        f"Correct: {rate(results)}  (runbook-covered {rate([r for r in results if r['runbook_covered']])}, "
        f"not covered {rate([r for r in results if not r['runbook_covered']])})",
        f"Grounded: {sum(r['grounded'] for r in scored)}/{len(scored)}   "
        f"Confidently wrong: {sum(r['confidently_wrong'] for r in scored)}",
    ]
    if scored:
        lines.append(f"Avg tool calls: {sum(r['tool_calls'] for r in scored) / len(scored):.1f}   "
                     f"Avg agent-loop cost: ${sum(r['cost_usd'] for r in scored) / len(scored):.2f}")
    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Pipeline Copilot evals")
    parser.add_argument("--only", help="run a single scenario by id")
    parser.add_argument("--repeat", type=int, default=1, help="runs per scenario (LLMs vary)")
    args = parser.parse_args()

    selected = {k: v for k, v in SCENARIOS.items() if not args.only or k == args.only}
    if not selected:
        raise SystemExit(f"Unknown scenario. Available: {', '.join(SCENARIOS)}")

    results = []
    for sid, build in selected.items():
        for attempt in range(args.repeat):
            r = await run_one(build, attempt)        # sequential: clearer output, no rate limits
            results.append(r)
            print(_line(r), flush=True)

    summary = _summary(results)
    print("\n" + summary)

    # Save every run: compare before/after a change to know whether it actually helped
    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    (RESULTS_DIR / f"{stamp}.json").write_text(json.dumps(results, indent=2, default=str))
    (RESULTS_DIR / "latest.md").write_text(
        f"# Eval results ({stamp} UTC)\n\n```\n" + "\n".join(_line(r) for r in results)
        + f"\n\n{summary}\n```\n"
    )
    print(f"\nSaved: evals/results/{stamp}.json and evals/results/latest.md")


if __name__ == "__main__":
    asyncio.run(main())
