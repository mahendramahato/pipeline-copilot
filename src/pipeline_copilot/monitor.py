"""Health monitor: catches pipeline problems without anyone asking.

Every MONITOR_INTERVAL_MINUTES it runs cheap, deterministic checks through the same MCP
tools the agent uses (so this process never holds credentials either). Only when a check
NEWLY fails does it spend an LLM investigation (capped per day), then sends one alert
with the root cause, impact and fix. Each problem alerts once; recovery is announced.

Enable with MONITOR_ENABLED=true. Optional: ALERT_WEBHOOK_URL (Slack, Discord or ntfy),
MONITOR_INTERVAL_MINUTES (default 30), MONITOR_MAX_INVESTIGATIONS_PER_DAY (default 3),
PUBLIC_URL (link in alerts).
"""
import asyncio
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

import httpx

from pipeline_copilot.graph import _as_text
from pipeline_copilot.runtime import Agent, new_thread_id, run_turn

log = logging.getLogger("pipeline_copilot.monitor")
DAG_ID = "daily_lake_maintenance"


def enabled() -> bool:
    return os.environ.get("MONITOR_ENABLED", "").lower() in ("1", "true", "yes")


@dataclass
class MonitorState:
    enabled: bool = False
    interval_minutes: int = 30
    checked_at: str | None = None
    checks: list[dict] = field(default_factory=list)
    alerted: dict[str, str] = field(default_factory=dict)        # failing check -> first alerted at
    last_investigation: dict | None = None                      # thread_id, issues, at, diagnosis
    investigations: dict[str, int] = field(default_factory=dict)  # UTC day -> count

    def public(self) -> dict:
        return asdict(self)


async def _tool(agent: Agent, name: str, args: dict) -> str:
    tool = next((t for t in agent.tools if t.name == name), None)
    if tool is None:
        raise RuntimeError(f"tool {name} not available")
    return _as_text(await tool.ainvoke(args))


# --- 1. Deterministic checks (free) ---
async def collect_checks(agent: Agent) -> list[dict]:
    checks: list[dict] = []
    try:
        checks += json.loads(await _tool(agent, "run_health_checks", {}))
    except Exception as e:   # noqa: BLE001 - a broken check source is itself a failing check
        checks.append({"check": "ops health checks ran", "ok": False, "detail": f"{type(e).__name__}: {e}"})

    try:
        out = await _tool(agent, "get_recent_dag_runs", {"dag_id": DAG_ID, "limit": 1})
        state = re.search(r"state=(\w+)", out)
        checks.append({"check": f"latest {DAG_ID} run not failed",
                       "ok": bool(state) and state.group(1) != "failed",
                       "detail": out.split(" | queued")[0][:200]})
    except Exception as e:  # noqa: BLE001
        checks.append({"check": "Airflow reachable", "ok": False, "detail": f"{type(e).__name__}: {e}"})

    # After the 00:30 run has had time to finish, yesterday must be curated
    now = datetime.now(timezone.utc)
    if now.hour >= 2:
        yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        try:
            out = await _tool(agent, "run_query", {"sql": (
                "SELECT CAST(max(date) AS VARCHAR) AS latest FROM curated_weather "
                f"WHERE date >= '{week_ago}'")})
            lines = out.splitlines()
            latest = lines[1].strip() if len(lines) > 2 else "unknown"
            checks.append({"check": "curated data includes yesterday", "ok": latest >= yesterday,
                           "detail": f"latest curated date: {latest} (expected {yesterday})"})
        except Exception as e:  # noqa: BLE001
            checks.append({"check": "Athena reachable", "ok": False, "detail": f"{type(e).__name__}: {e}"})
    return checks


# --- 2. Alerts ---
async def send_alert(text: str) -> None:
    url = os.environ.get("ALERT_WEBHOOK_URL")
    log.warning("ALERT: %s", text.replace("\n", " | "))
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if "ntfy" in url:
                await client.post(url, content=text.encode())                  # ntfy: plain text
            else:
                await client.post(url, json={"text": text, "content": text})   # Slack: text, Discord: content
    except httpx.HTTPError as e:
        log.error("alert delivery failed: %s", e)


# --- 3. One monitoring pass ---
async def run_once(agent: Agent, state: MonitorState) -> None:
    now = datetime.now(timezone.utc)
    checks = await collect_checks(agent)
    state.checks, state.checked_at = checks, now.isoformat(timespec="seconds")

    failing = {c["check"]: c for c in checks if not c["ok"]}
    # Forget problems that cleared (and say so), and old alerts after 24 h so a long outage re-alerts daily
    for name in list(state.alerted):
        if name not in failing:
            del state.alerted[name]
            await send_alert(f"✅ Pipeline Copilot: recovered: {name}")
        elif now - datetime.fromisoformat(state.alerted[name]) > timedelta(hours=24):
            del state.alerted[name]
    new = {name: c for name, c in failing.items() if name not in state.alerted}
    if not new:
        return
    for name in new:
        state.alerted[name] = now.isoformat(timespec="seconds")

    issues = "\n".join(f"- {c['check']}: {c['detail']}" for c in failing.values())
    message = f"⚠️ Pipeline Copilot: {len(failing)} check(s) failing\n{issues}"

    # Spend an LLM investigation only within the daily cap
    day = now.strftime("%Y-%m-%d")
    cap = int(os.environ.get("MONITOR_MAX_INVESTIGATIONS_PER_DAY", "3"))
    if state.investigations.get(day, 0) < cap:
        state.investigations = {day: state.investigations.get(day, 0) + 1}
        thread_id = new_thread_id()
        question = ("Automated health check found these failing checks:\n" + issues +
                    "\nInvestigate the root cause and the impact.")
        diagnosis = None
        async for event in run_turn(agent, thread_id, question):
            if event["type"] == "diagnosis":
                diagnosis = event["diagnosis"]
        state.last_investigation = {"thread_id": thread_id, "at": state.checked_at, "issues": list(failing),
                                    "diagnosis": diagnosis}
        if diagnosis and diagnosis.get("category") != "no_problem_found":
            message += (f"\n\nRoot cause: {diagnosis['root_cause']}\nImpact: {diagnosis['impact']}"
                        f"\nSuggested fix: {diagnosis['suggested_fix']}")
        public = os.environ.get("PUBLIC_URL")
        message += f"\n\nFull investigation: {public or 'live mode'} (conversation {thread_id})"
    else:
        message += "\n\n(daily investigation limit reached: checks only)"
    await send_alert(message)


async def monitor_loop(agent: Agent, state: MonitorState) -> None:
    await asyncio.sleep(60)   # let the MCP servers settle after startup
    while True:
        try:
            await run_once(agent, state)
        except Exception:  # noqa: BLE001 - the monitor must never die
            log.exception("monitor pass failed")
        await asyncio.sleep(state.interval_minutes * 60)
