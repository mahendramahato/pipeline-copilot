"""The remediation catalog: the ONLY actions the agent can ever propose, and the
deterministic checks every proposal must pass before a human is asked to approve it.

Shared by the graph (vetting a plan) and the actions MCP server (checking again right
before executing), so both layers enforce exactly the same rules.

Why a catalog instead of free-form commands: a model that can write any command
can do anything. Three small, well-understood actions cover the common incidents,
and each one is safe to repeat (re-curating a day overwrites only that day).
"""
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from pipeline_copilot.ops import DEFAULT_EXPECTED

DAG_ID = "daily_lake_maintenance"
DAG_TASKS = ("check_freshness", "target_date", "sync_to_s3", "curate_day")
MAX_ACTIONS = 3
BACKFILL_MAX_DAYS = 30            # older days need a deliberate, manual backfill
RUN_ID = re.compile(r"^(scheduled|manual|backfill|asset_triggered)__[\w:+.\-]{1,120}$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Never restartable by the agent: its own database and Airflow's metadata DB
NEVER_RESTART = {"airflow-postgres", "copilot", "copilot-docker-proxy", "copilot-actions-proxy"}


@dataclass(frozen=True)
class ActionSpec:
    risk: str            # shown to the approver
    args: tuple          # which ProposedAction fields this action uses
    when: str            # when to propose it (goes into the diagnosis prompt)
    effect: str          # what it does (shown to the approver)


CATALOG = {
    "clear_task": ActionSpec(
        risk="low", args=("run_id", "task_id"),
        when="A task in an existing run failed for a reason that is now gone (retries exhausted on a "
             "timeout, ConcurrentRunsExceededException) or is stuck running far past its timeout.",
        effect="Clears the task and the tasks after it in that run, so Airflow runs them again.",
    ),
    "backfill_date": ActionSpec(
        risk="medium", args=("date",),
        when="Curated data is missing or wrong for a specific past day and the cause is already fixed "
             "(or was transient). One action per day.",
        effect="Triggers a new daily_lake_maintenance run with conf {\"date\": ...}: re-syncs and "
               "re-curates that one day (the Glue job overwrites only that partition).",
    ),
    "restart_container": ActionSpec(
        risk="medium", args=("container",),
        when="A pipeline container exited, crashed or was OOM-killed, so a feed stopped.",
        effect="Restarts that one Docker container.",
    ),
}


def catalog_prompt() -> str:
    """The catalog as text for the diagnosis prompt."""
    lines = [f"- {name}({', '.join(spec.args)}): {spec.when}" for name, spec in CATALOG.items()]
    return "\n".join(lines)


def restartable_containers() -> set[str]:
    names = os.environ.get("EXPECTED_CONTAINERS", DEFAULT_EXPECTED)
    return {n.strip() for n in names.split(",") if n.strip()} - NEVER_RESTART


def check_action(action: dict, today: date) -> str | None:
    """Return why this action is not allowed, or None if it is. Pure and deterministic."""
    name = action.get("action")
    spec = CATALOG.get(name)
    if spec is None:
        return f"unknown action {name!r}"
    missing = [a for a in spec.args if not action.get(a)]
    if missing:
        return f"{name} needs {', '.join(missing)}"
    if name == "clear_task":
        if not RUN_ID.match(action["run_id"]):
            return f"not a valid run_id: {action['run_id']!r}"
        if action["task_id"] not in DAG_TASKS:
            return f"unknown task {action['task_id']!r} (expected one of {', '.join(DAG_TASKS)})"
    elif name == "backfill_date":
        if not DATE.match(action["date"]):
            return f"date must be YYYY-MM-DD, got {action['date']!r}"
        try:
            d = date.fromisoformat(action["date"])
        except ValueError:
            return f"not a real date: {action['date']!r}"
        if d >= today:
            return f"{d} is not a past day (today is {today}); only finished days can be backfilled"
        if d < today - timedelta(days=BACKFILL_MAX_DAYS):
            return f"{d} is more than {BACKFILL_MAX_DAYS} days old; backfill it manually"
    elif name == "restart_container":
        allowed = restartable_containers()
        if action["container"] not in allowed:
            return f"container {action['container']!r} is not restartable (allowed: {', '.join(sorted(allowed))})"
    return None


def _key(action: dict) -> tuple:
    return (action["action"], *(action.get(a) for a in CATALOG[action["action"]].args))


def vet_plan(diagnosis: dict, today: date) -> dict:
    """Decide which proposed actions a human may approve. Returns
    {"actions": [...allowed, each with risk/effect...], "rejected": [{action, reason}], "note": str|None}.

    Rules, in order: no actions for low confidence or "no problem / unknown";
    every action must pass check_action; duplicates dropped; at most MAX_ACTIONS.
    """
    proposed = diagnosis.get("fix_actions") or []
    if not proposed:
        return {"actions": [], "rejected": [], "note": None}
    if diagnosis["category"] in ("no_problem_found", "unknown"):
        return {"actions": [], "rejected": [{**a, "reason": f"category is {diagnosis['category']}"} for a in proposed],
                "note": "No automated fix: the root cause isn't established."}
    if diagnosis["confidence"] == "low":
        return {"actions": [], "rejected": [{**a, "reason": "confidence is low"} for a in proposed],
                "note": "No automated fix offered: confidence is low. Verify the root cause first."}

    allowed, rejected, seen = [], [], set()
    for a in proposed:
        reason = check_action(a, today)
        if reason is None and _key(a) in seen:
            reason = "duplicate"
        if reason is None and len(allowed) >= MAX_ACTIONS:
            reason = f"more than {MAX_ACTIONS} actions"
        if reason:
            rejected.append({**a, "reason": reason})
            continue
        seen.add(_key(a))
        spec = CATALOG[a["action"]]
        allowed.append({"action": a["action"], **{k: a[k] for k in spec.args}, "why": a.get("why", ""),
                        "risk": spec.risk, "effect": spec.effect})
    return {"actions": allowed, "rejected": rejected, "note": None}


def describe(action: dict) -> str:
    """One readable line, e.g. clear_task(curate_day in scheduled__2026-10-05T00:30:00+00:00)."""
    match action["action"]:
        case "clear_task":
            return f"clear_task({action['task_id']} in {action['run_id']})"
        case "backfill_date":
            return f"backfill_date({action['date']})"
        case "restart_container":
            return f"restart_container({action['container']})"
    return action["action"]


def target_date_of(run_id: str, conf: dict | None = None) -> str | None:
    """The day a run curates: conf.date if given, else the day before the scheduled date."""
    if conf and conf.get("date"):
        return conf["date"]
    m = re.search(r"__(\d{4}-\d{2}-\d{2})", run_id)
    if not m:
        return None
    return (datetime.fromisoformat(m.group(1)) - timedelta(days=1)).strftime("%Y-%m-%d")
