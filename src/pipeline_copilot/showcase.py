"""Showcase: saved investigations that the public can browse, read-only.

Conversations are exported to showcase/<slug>.json, reviewed by a human, and
committed. The public site only ever reads these files, never the live
conversation database, so nothing is published by accident.

    uv run python -m pipeline_copilot.showcase export <thread_id> <slug> "<title>"
"""
import argparse
import asyncio
import json
import re
from datetime import datetime, timezone

from pipeline_copilot.config import PROJECT_ROOT
from pipeline_copilot.runtime import load_thread, open_agent

SHOWCASE_DIR = PROJECT_ROOT / "showcase"
SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{0,60}$")


def list_showcase() -> list[dict]:
    items = []
    for path in sorted(SHOWCASE_DIR.glob("*.json")):
        data = json.loads(path.read_text())
        items.append({k: data.get(k) for k in ("slug", "title", "summary", "recorded", "order")})
    return sorted(items, key=lambda i: (i.get("order") or 99, i["slug"]))


def get_showcase(slug: str) -> dict | None:
    # The slug pattern also blocks path tricks like "../"
    if not SLUG.match(slug):
        return None
    path = SHOWCASE_DIR / f"{slug}.json"
    return json.loads(path.read_text()) if path.exists() else None


def _summary(turns: list[dict]) -> str:
    for turn in turns:
        for ev in turn["events"]:
            if ev["type"] == "diagnosis":
                return ev["diagnosis"]["summary"]
    answers = [ev["text"] for t in turns for ev in t["events"] if ev["type"] == "answer"]
    return (answers[0][:200] + "…") if answers else ""


async def export(thread_id: str, slug: str, title: str, order: int | None) -> None:
    if not SLUG.match(slug):
        raise SystemExit("slug must be lowercase letters, digits and dashes")
    async with open_agent() as agent:
        data = await load_thread(agent, thread_id)
    if not data["turns"]:
        raise SystemExit(f"No conversation {thread_id}")
    # thread ids are chat-YYYYMMDD-HHMMSS (UTC)
    recorded = datetime.strptime(thread_id[5:20], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
    SHOWCASE_DIR.mkdir(exist_ok=True)
    out = SHOWCASE_DIR / f"{slug}.json"
    out.write_text(json.dumps({
        "slug": slug,
        "title": title,
        "summary": _summary(data["turns"]),
        "recorded": recorded.isoformat(),
        "order": order,
        "turns": data["turns"],
    }, indent=2, default=str))
    print(f"Wrote {out}: {len(data['turns'])} turn(s). Review it before committing: it will be public.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage showcase investigations")
    sub = parser.add_subparsers(dest="cmd", required=True)
    exp = sub.add_parser("export", help="export a saved conversation to showcase/<slug>.json")
    exp.add_argument("thread_id")
    exp.add_argument("slug")
    exp.add_argument("title")
    exp.add_argument("--order", type=int, help="position in the list (1 = first)")
    args = parser.parse_args()
    asyncio.run(export(args.thread_id, args.slug, args.title, args.order))


if __name__ == "__main__":
    main()
