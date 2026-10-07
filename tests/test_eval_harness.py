"""The eval harness stays in sync with the real agent: every scenario builds, every
real MCP tool has a fake, and fake SQL runs in DuckDB. No LLM, no network."""
import asyncio

import pytest

from evals.fake_tools import make_fake_tools
from evals.scenarios import SCENARIOS


@pytest.mark.parametrize("sid", sorted(SCENARIOS))
def test_scenario_builds_with_fake_tools(sid):
    scenario = SCENARIOS[sid]()
    assert scenario.id == sid
    # make_fake_tools raises if a real MCP tool has no fake implementation
    tools = {t.name for t in asyncio.run(make_fake_tools(scenario.world))}
    assert {"run_query", "get_task_log", "search_runbooks", "get_current_time"} <= tools


def test_fake_run_query_uses_guardrail_and_duckdb():
    world = SCENARIOS["schema_drift_magnitude"]().world
    tools = {t.name: t for t in asyncio.run(make_fake_tools(world))}
    run = lambda sql: asyncio.run(tools["run_query"].ainvoke({"sql": sql}))
    out = run("SELECT date, COUNT_IF(magnitude IS NULL) AS nulls FROM curated_seismic "
              "WHERE date = '2026-10-03' GROUP BY date")
    assert "2026-10-03 | 120" in out                    # the scenario's injected NULLs
    assert run("DROP TABLE raw_seismic").startswith("Blocked by SQL guardrail")
