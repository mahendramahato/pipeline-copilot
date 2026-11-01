"""The fix loop, end to end, with NO LLM and no network: a scripted fake model drives
the real graph over the eval harness's simulated world. Checks the parts that must
never break: vetting rules, the approval pause, execution, validation, refusals."""
import asyncio
import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from evals.fake_tools import make_fake_tools
from evals.scenarios import SCENARIOS
from pipeline_copilot import graph as graph_module
from pipeline_copilot import guardrails
from pipeline_copilot import remediation as rem
from pipeline_copilot.config import AgentSettings
from pipeline_copilot.models import Diagnosis

TODAY = date(2026, 10, 5)


# ---------------- vetting (pure) ----------------

def _diag(actions, confidence="high", category="stale_streaming"):
    return {"category": category, "confidence": confidence, "fix_actions": actions}


def _act(action, run_id=None, task_id=None, date=None, container=None):
    return {"action": action, "run_id": run_id, "task_id": task_id, "date": date,
            "container": container, "why": "test"}


def test_valid_actions_pass():
    plan = rem.vet_plan(_diag([
        _act("clear_task", run_id="scheduled__2026-10-05T00:30:00+00:00", task_id="curate_day"),
        _act("backfill_date", date="2026-10-01"),
        _act("restart_container", container="weather-stream"),
    ]), TODAY)
    assert [a["action"] for a in plan["actions"]] == ["clear_task", "backfill_date", "restart_container"]
    assert plan["rejected"] == []
    assert all(a["risk"] and a["effect"] for a in plan["actions"])


@pytest.mark.parametrize("action,reason", [
    (_act("clear_task", run_id="scheduled__2026-10-05; rm -rf /", task_id="curate_day"), "not a valid run_id"),
    (_act("clear_task", run_id="scheduled__2026-10-05T00:30:00+00:00", task_id="drop_tables"), "unknown task"),
    (_act("backfill_date", date="2026-10-05"), "not a past day"),
    (_act("backfill_date", date="2026-08-01"), "more than 30 days"),
    (_act("backfill_date", date="2026-02-30"), "not a real date"),
    (_act("restart_container", container="airflow-postgres"), "not restartable"),
    (_act("restart_container", container="copilot"), "not restartable"),
    (_act("backfill_date"), "needs date"),
    ({"action": "run_sql", "why": "x"}, "unknown action"),
])
def test_unsafe_actions_rejected(action, reason):
    plan = rem.vet_plan(_diag([action]), TODAY)
    assert plan["actions"] == []
    assert reason in plan["rejected"][0]["reason"]


def test_low_confidence_or_no_problem_gets_no_actions():
    a = _act("restart_container", container="weather-stream")
    assert rem.vet_plan(_diag([a], confidence="low"), TODAY)["actions"] == []
    assert rem.vet_plan(_diag([a], category="no_problem_found"), TODAY)["actions"] == []


def test_duplicates_and_limit():
    a = _act("backfill_date", date="2026-10-01")
    plan = rem.vet_plan(_diag([a, a] + [_act("backfill_date", date=f"2026-09-{d}") for d in (27, 28, 29)]), TODAY)
    assert len(plan["actions"]) == rem.MAX_ACTIONS
    assert {r["reason"] for r in plan["rejected"]} == {"duplicate", f"more than {rem.MAX_ACTIONS} actions"}


def test_target_date_of():
    assert rem.target_date_of("scheduled__2026-10-05T00:30:00+00:00") == "2026-10-04"
    assert rem.target_date_of("manual__copilot_backfill_x", {"date": "2026-10-01"}) == "2026-10-01"


# ---------------- the actions server refuses unless enabled ----------------

def test_actions_server_refuses_when_disabled(monkeypatch, tmp_path):
    from pipeline_copilot.mcp_servers import actions_server
    monkeypatch.delenv("ACTIONS_ENABLED", raising=False)
    monkeypatch.setenv("ACTIONS_AUDIT_LOG", str(tmp_path / "audit.jsonl"))
    out = json.loads(actions_server.restart_container("weather-stream", "chat-test"))
    assert out["ok"] is False and "disabled" in out["detail"]
    # The refusal is audited too
    entry = json.loads(Path(tmp_path / "audit.jsonl").read_text().splitlines()[0])
    assert entry["ok"] is False and entry["container"] == "weather-stream"


def test_actions_server_rechecks_catalog(monkeypatch, tmp_path):
    from pipeline_copilot.mcp_servers import actions_server
    monkeypatch.setenv("ACTIONS_ENABLED", "true")
    monkeypatch.setenv("ACTIONS_AUDIT_LOG", str(tmp_path / "audit.jsonl"))
    out = json.loads(actions_server.restart_container("airflow-postgres", "chat-test"))
    assert out["ok"] is False and "not restartable" in out["detail"]


# ---------------- the graph, with a scripted model ----------------

class FakeModel:
    """Stands in for every model: tool-calling agent, guardrail, triage and diagnoser."""

    def __init__(self, script):
        self.script = script          # dict of what each role answers

    def bind_tools(self, tools, **kwargs):
        return self

    def with_structured_output(self, schema, **kwargs):
        return FakeStructured(self.script, schema)

    async def ainvoke(self, messages):
        turns = self.script["agent"]
        return turns.pop(0)


class FakeStructured:
    def __init__(self, script, schema):
        self.script, self.schema = script, schema

    async def ainvoke(self, messages):
        if self.schema is Diagnosis:
            return self.script["diagnosis"]
        if self.schema is guardrails.GuardrailVerdict:
            return guardrails.GuardrailVerdict(reason="test", category=self.script["intent"])
        return graph_module.TriageVerdict(reason="test", problem_found=self.script.get("triage", False))


def _oom_diagnosis(actions):
    return Diagnosis(
        summary="weather-stream is down", category="stale_streaming",
        root_cause="weather-stream was OOM-killed and never restarted",
        evidence=[{"tool": "list_containers", "quote": "weather-stream: exited (Exited (137) 3 hours ago)",
                   "meaning": "the container is down"}],
        impact="no weather for 3 hours", suggested_fix="restart weather-stream",
        fix_actions=actions, runbooks_used=[], unverified=[], confidence="high",
    )


RESTART = {"action": "restart_container", "run_id": None, "task_id": None, "date": None,
           "container": "weather-stream", "why": "it was OOM-killed"}


def _build(monkeypatch, script, scenario="container_oom", actions_enabled=True):
    monkeypatch.setattr(graph_module, "chat_model", lambda *a, **k: FakeModel(script))
    monkeypatch.setattr(guardrails, "chat_model", lambda *a, **k: FakeModel(script))
    s = SCENARIOS[scenario]()
    settings = AgentSettings(llm_model="claude-test", guardrail_model="claude-test", memory_db=Path("unused"))
    settings = replace(settings, actions_enabled=actions_enabled, validation_poll_seconds=0,
                       container_settle_seconds=0)
    tools = asyncio.run(make_fake_tools(s.world))
    g = graph_module.build_graph(settings, tools, checkpointer=InMemorySaver())
    return g, s.world


def _agent_turns():
    return [
        AIMessage("", tool_calls=[{"name": "list_containers", "args": {}, "id": "c1"}]),
        AIMessage("weather-stream exited with code 137 (OOM)."),
    ]


CONFIG = {"configurable": {"thread_id": "chat-test"}, "recursion_limit": 30}


def test_fix_waits_for_approval_then_runs_and_is_verified(monkeypatch):
    script = {"intent": "incident", "agent": _agent_turns(), "diagnosis": _oom_diagnosis([RESTART])}
    g, world = _build(monkeypatch, script)

    async def go():
        out = await g.ainvoke({"messages": [("user", "weather map is stuck")]}, CONFIG)
        # Paused: nothing has been executed yet
        assert out["__interrupt__"][0].value["actions"][0]["container"] == "weather-stream"
        assert world.actions == []
        await g.ainvoke(Command(resume={"approved": True, "by": "test"}), CONFIG)
        return (await g.aget_state(CONFIG)).values

    state = asyncio.run(go())
    assert world.actions == [{"action": "restart_container", "container": "weather-stream"}]
    assert state["approval"]["approved"] is True
    assert state["remediation"]["outcome"] == "resolved"
    assert state["remediation"]["results"][0]["verified"] is True
    assert state["remediation"]["still_failing"] == []


def test_rejected_fix_never_runs(monkeypatch):
    script = {"intent": "incident", "agent": _agent_turns(), "diagnosis": _oom_diagnosis([RESTART])}
    g, world = _build(monkeypatch, script)

    async def go():
        await g.ainvoke({"messages": [("user", "weather map is stuck")]}, CONFIG)
        await g.ainvoke(Command(resume={"approved": False, "by": "test"}), CONFIG)
        return (await g.aget_state(CONFIG)).values

    state = asyncio.run(go())
    assert world.actions == []
    assert state["approval"]["approved"] is False
    assert state["remediation"] is None


def test_actions_disabled_shows_plan_without_pausing(monkeypatch):
    script = {"intent": "incident", "agent": _agent_turns(), "diagnosis": _oom_diagnosis([RESTART])}
    g, world = _build(monkeypatch, script, actions_enabled=False)
    out = asyncio.run(g.ainvoke({"messages": [("user", "weather map is stuck")]}, CONFIG))
    assert "__interrupt__" not in out
    assert out["plan"]["executable"] is False and out["plan"]["actions"]
    assert "by hand" in out["plan"]["note"]
    assert world.actions == []


def test_question_that_finds_a_problem_is_escalated(monkeypatch):
    # "Did the stream run OK?" is routed as a question; triage sees the problem and escalates
    script = {"intent": "question", "triage": True, "agent": _agent_turns(),
              "diagnosis": _oom_diagnosis([RESTART])}
    g, _ = _build(monkeypatch, script)
    out = asyncio.run(g.ainvoke({"messages": [("user", "is the weather stream running fine?")]}, CONFIG))
    assert out["intent"] == "incident"
    assert out["diagnosis"]["category"] == "stale_streaming"
    assert "__interrupt__" in out


def test_question_without_problem_stays_a_question(monkeypatch):
    script = {"intent": "question", "triage": False, "agent": _agent_turns(),
              "diagnosis": _oom_diagnosis([])}
    g, _ = _build(monkeypatch, script)
    out = asyncio.run(g.ainvoke({"messages": [("user", "which containers exist?")]}, CONFIG))
    assert out.get("diagnosis") is None


def test_failed_fix_is_reported_not_resolved(monkeypatch):
    # glue_code_bug: the script is broken, so re-running curate_day fails again
    s = SCENARIOS["glue_code_bug"]()
    run_id = s.world.dag_runs[0]["dag_run_id"]
    clear = {"action": "clear_task", "run_id": run_id, "task_id": "curate_day", "date": None,
             "container": None, "why": "retry"}
    diagnosis = Diagnosis(
        summary="curate_day failed", category="code_bug", root_cause="temp_c does not exist",
        evidence=[{"tool": "get_recent_dag_runs", "quote": f"run_id={run_id} | state=failed", "meaning": "red"}],
        impact="yesterday missing", suggested_fix="fix the script", fix_actions=[clear],
        runbooks_used=[], unverified=[], confidence="high",
    )
    turns = [AIMessage("", tool_calls=[{"name": "get_recent_dag_runs",
                                        "args": {"dag_id": rem.DAG_ID, "limit": 1}, "id": "c1"}]),
             AIMessage("curate_day failed")]
    g, world = _build(monkeypatch, {"intent": "incident", "agent": turns, "diagnosis": diagnosis},
                      scenario="glue_code_bug")

    async def go():
        await g.ainvoke({"messages": [("user", "what broke?")]}, CONFIG)
        await g.ainvoke(Command(resume={"approved": True, "by": "test"}), CONFIG)
        return (await g.aget_state(CONFIG)).values

    state = asyncio.run(go())
    assert state["remediation"]["outcome"] == "not_resolved"
    assert "failed" in state["remediation"]["results"][0]["verification"]


def test_runtime_streams_approval_and_outcome(monkeypatch):
    # The API/CLI path: run_turn pauses with approval_required, resume_turn finishes
    from pipeline_copilot.runtime import Agent, pending_approval, resume_turn, run_turn
    script = {"intent": "incident", "agent": _agent_turns(), "diagnosis": _oom_diagnosis([RESTART])}
    g, world = _build(monkeypatch, script)
    agent = Agent(graph=g, tools=[], settings=None, checkpointer=None, tool_servers={})

    async def go():
        first = [e async for e in run_turn(agent, "chat-test", "weather map is stuck")]
        waiting = await pending_approval(agent, "chat-test")
        second = [e async for e in resume_turn(agent, "chat-test", {"approved": True, "by": "test"})]
        return first, waiting, second

    first, waiting, second = asyncio.run(go())
    kinds = [e["type"] for e in first]
    assert kinds.index("diagnosis") < kinds.index("plan") < kinds.index("approval_required")
    assert waiting["actions"][0]["action"] == "restart_container"
    kinds2 = [e["type"] for e in second]
    assert "progress" in kinds2 and "approval" in kinds2
    outcome = next(e for e in second if e["type"] == "remediation")["remediation"]["outcome"]
    assert outcome == "resolved"


def test_new_question_declines_pending_fix(monkeypatch):
    from pipeline_copilot.runtime import Agent, run_turn
    script = {"intent": "incident", "agent": _agent_turns() + _agent_turns(),
              "diagnosis": _oom_diagnosis([RESTART])}
    g, world = _build(monkeypatch, script)
    agent = Agent(graph=g, tools=[], settings=None, checkpointer=None, tool_servers={})

    async def go():
        [e async for e in run_turn(agent, "chat-test", "weather map is stuck")]
        return [e async for e in run_turn(agent, "chat-test", "actually, what about seismic?")]

    events = asyncio.run(go())
    declined = next(e for e in events if e["type"] == "approval")["approval"]
    assert declined["approved"] is False and declined["by"] == "system"
    assert world.actions == []


@pytest.mark.parametrize("sid", sorted(k for k, b in SCENARIOS.items() if b().expects_fix))
def test_expected_fix_resolves_in_simulated_world(monkeypatch, sid):
    # Every scenario's intended fix must actually fix its simulated world, or the
    # eval would mark a correct agent as failing.
    from evals.scoring import plan_matches
    option = SCENARIOS[sid]().fix_options[0]
    actions = [{"run_id": None, "task_id": None, "date": None, "container": None, "why": "t", **m} for m in option]
    now = SCENARIOS[sid]().world.now.isoformat(timespec="seconds")
    diagnosis = Diagnosis(
        summary="s", category="transient_failure", root_cause="r",
        evidence=[{"tool": "get_current_time", "quote": now, "meaning": "time"}],
        impact="i", suggested_fix="f", fix_actions=actions, runbooks_used=[], unverified=[], confidence="high")
    turns = [AIMessage("", tool_calls=[{"name": "get_current_time", "args": {}, "id": "c1"}]), AIMessage("done")]
    g, world = _build(monkeypatch, {"intent": "incident", "agent": turns, "diagnosis": diagnosis}, scenario=sid)

    async def go():
        await g.ainvoke({"messages": [("user", "fix it")]}, CONFIG)
        await g.ainvoke(Command(resume={"approved": True, "by": "test"}), CONFIG)
        return (await g.aget_state(CONFIG)).values

    state = asyncio.run(go())
    assert plan_matches(state["plan"]["actions"], SCENARIOS[sid]().fix_options)
    assert state["remediation"]["outcome"] == "resolved", state["remediation"]
