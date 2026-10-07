"""Tests for the Ops tools' logic and the health monitor. No network, no model, no Docker."""
import asyncio
import json
import os
import struct
from datetime import datetime, timedelta, timezone

import duckdb
import pytest
from langchain_core.tools import StructuredTool

from pipeline_copilot import monitor, ops


# ---------------- Docker log decoding ----------------

def frame(stream: int, text: str) -> bytes:
    payload = text.encode()
    return bytes([stream, 0, 0, 0]) + struct.pack(">I", len(payload)) + payload


def test_demux_joins_stdout_and_stderr_frames():
    raw = frame(1, "2026-10-07T10:00:00.1Z started\n") + frame(2, "2026-10-07T10:00:01.2Z ERROR boom\n")
    assert ops.demux_docker_logs(raw) == "2026-10-07T10:00:00.1Z started\n2026-10-07T10:00:01.2Z ERROR boom\n"


def test_demux_passes_tty_output_through():
    assert ops.demux_docker_logs(b"plain tty output\n") == "plain tty output\n"


# ---------------- the live local lake ----------------

@pytest.fixture()
def lake(tmp_path):
    """A tiny lake laid out like the Spark jobs': <dataset>/date=YYYY-MM-DD/*.parquet."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con = duckdb.connect()
    day = now.strftime("%Y-%m-%d")
    for station, minutes_ago in [("KBOI", 20), ("KDEN", 300)]:     # KDEN is 5 h behind: stale
        out = tmp_path / "weather" / f"date={day}"
        out.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY (SELECT '{station}' AS station_id, TIMESTAMP '{now - timedelta(minutes=minutes_ago)}' "
                    f"AS observed_at, 12.5 AS temperature_c) TO '{out}/{station}.parquet' (FORMAT parquet)")
    out = tmp_path / "seismic" / f"date={day}"
    out.mkdir(parents=True)
    con.execute(f"COPY (SELECT 'us1' AS event_id, TIMESTAMP '{now - timedelta(minutes=5)}' AS ingested_at, "
                f"4.8 AS magnitude) TO '{out}/q.parquet' (FORMAT parquet)")
    # Make the seismic files look 3 hours old: that feed has stopped
    old = (datetime.now() - timedelta(hours=3)).timestamp()
    for path in (tmp_path / "seismic").rglob("*.parquet"):
        os.utime(path, (old, old))
    return tmp_path


def test_freshness_flags_stopped_feed_and_stale_station(lake):
    f = ops.lake_freshness(lake)
    assert f["datasets"]["weather"]["stale"] is False
    assert f["datasets"]["seismic"]["stale"] is True              # no new file for 3 h
    stations = {s["station"]: s for s in f["stations"]}
    assert stations["KBOI"]["stale"] is False
    assert stations["KDEN"]["stale"] is True                       # 300 min behind
    assert f["seismic"]["events_today"] == 1
    assert "STALE" in ops.format_freshness(f)


def test_lake_query_runs_and_is_guarded(lake):
    out = ops.query_lake(lake, "SELECT station_id, temperature_c FROM lake_weather ORDER BY station_id")
    assert "KBOI | 12.5" in out
    with pytest.raises(Exception, match="SELECT"):
        ops.query_lake(lake, "DROP TABLE lake_weather")
    with pytest.raises(Exception, match="Unknown table"):
        ops.query_lake(lake, "SELECT * FROM raw_weather")


# ---------------- the monitor: alert once, recover, cap investigations ----------------

class FakeAgent:
    def __init__(self, checks):
        self.checks = checks

        async def health():
            return json.dumps(self.checks)

        async def runs(dag_id, limit=1):
            return "run_id=scheduled__x | state=success | queued=..."

        async def query(sql):
            return "latest\n2099-01-01\n(1 rows; scanned 0.00 MB)"

        self.tools = [
            StructuredTool.from_function(coroutine=health, name="run_health_checks", description="d"),
            StructuredTool.from_function(coroutine=runs, name="get_recent_dag_runs", description="d"),
            StructuredTool.from_function(coroutine=query, name="run_query", description="d"),
        ]


def test_monitor_alerts_once_then_recovers(monkeypatch):
    alerts, investigations = [], []

    async def fake_alert(text):
        alerts.append(text)

    async def fake_turn(agent, thread_id, question):
        investigations.append(question)
        yield {"type": "diagnosis", "diagnosis": {"category": "stale_streaming", "root_cause": "producer stopped",
                                                  "impact": "no new data", "suggested_fix": "restart producer"}}

    monkeypatch.setattr(monitor, "send_alert", fake_alert)
    monkeypatch.setattr(monitor, "run_turn", fake_turn)
    monkeypatch.setenv("MONITOR_MAX_INVESTIGATIONS_PER_DAY", "1")
    state = monitor.MonitorState(enabled=True)
    agent = FakeAgent([{"check": "lake weather receiving data", "ok": False, "detail": "newest file 200 min ago"}])

    asyncio.run(monitor.run_once(agent, state))           # new problem: investigate + alert
    assert len(alerts) == 1 and "Root cause: producer stopped" in alerts[0]
    assert len(investigations) == 1

    asyncio.run(monitor.run_once(agent, state))           # same problem: stay quiet
    assert len(alerts) == 1

    agent.checks = [{"check": "lake weather receiving data", "ok": True, "detail": "ok"}]
    asyncio.run(monitor.run_once(agent, state))           # cleared: announce recovery
    assert len(alerts) == 2 and "recovered" in alerts[1]

    agent.checks = [{"check": "container producer running", "ok": False, "detail": "exited"}]
    asyncio.run(monitor.run_once(agent, state))           # new problem, but the daily cap (1) is used
    assert len(investigations) == 1 and "limit reached" in alerts[2]
