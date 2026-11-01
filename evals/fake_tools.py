"""Fake tools for evals: the SAME names, descriptions and schemas as the real MCP
tools (copied from the servers at runtime), answering from a simulated World."""
import csv
import hashlib
import json
import tempfile
from datetime import datetime, timedelta

import duckdb
import sqlglot
from langchain_core.tools import BaseTool, StructuredTool

from evals.world import BASE_STATS, DAG_ID, SCHEMAS, STATIONS, TASKS, World, host_for
from pipeline_copilot import ops, resources
from pipeline_copilot import remediation as rem
from pipeline_copilot.athena_client import QueryResult
from pipeline_copilot.config import load_knowledge_settings
from pipeline_copilot.knowledge_base import search
from pipeline_copilot.mcp_servers import actions_server, airflow_server, athena_server, knowledge_server, ops_server

# Container -> the lake dataset it feeds (restarting it makes that feed fresh again)
CONTAINER_FEEDS = {"weather-stream": ("weather",), "seismic-stream": ("seismic",),
                   "producer": ("weather", "seismic"), "kafka": ("weather", "seismic")}
from pipeline_copilot.sql_guard import UnsafeQueryError, check_query

DATABASE = "weather_seismic"
_DUCK_TYPES = {"string": "VARCHAR", "double": "DOUBLE", "bigint": "BIGINT", "int": "INTEGER",
               "boolean": "BOOLEAN", "timestamp": "TIMESTAMP"}
_PROJECTION = {"projection.enabled": "true", "projection.date.type": "date",
               "projection.date.range": "2026-09-27,NOW"}


# --- Load the world's tables into an in-memory DuckDB, under schema weather_seismic ---
def _duckdb(world: World) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA {DATABASE}")
    con.execute(f"SET schema = '{DATABASE}'")      # unqualified table names resolve here
    for table, rows in world.tables.items():
        cols = {**SCHEMAS[table], "date": "string"}
        con.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {_DUCK_TYPES[t]}' for c, t in cols.items())})")
        if rows:
            # Bulk load through a CSV file: ~100x faster than row-by-row INSERTs.
            # Empty fields are read back as NULL.
            with tempfile.NamedTemporaryFile("w", suffix=".csv", newline="") as f:
                writer = csv.writer(f)
                writer.writerows([["" if r.get(c) is None else r[c] for c in cols] for r in rows])
                f.flush()
                con.execute(f"COPY {table} FROM '{f.name}' (HEADER false, NULL '')")
    con.execute("CREATE VIEW lake_weather AS SELECT * FROM raw_weather")
    con.execute("CREATE VIEW lake_seismic AS SELECT * FROM raw_seismic")
    return con


# Athena returns every value as a string; mimic that so the agent sees the same text
def _athena_str(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return str(v)


# A Glue-like table dict, so the REAL formatters can describe simulated tables
def _glue_table(world: World, table: str, version: dict | None = None) -> dict:
    version = version or world.table_versions[table][-1]
    return {
        "TableType": "EXTERNAL_TABLE",
        "UpdateTime": version["updated"],
        "StorageDescriptor": {"Columns": [{"Name": c, "Type": t} for c, t in version["columns"].items()]},
        "PartitionKeys": [{"Name": "date", "Type": "string"}],
        "Parameters": _PROJECTION,
    }


def _impls(world: World) -> dict:
    # The world's tables only change when a fix re-curates a day: reload DuckDB then
    db = {"con": _duckdb(world), "version": world.tables_version}

    def execute(query: str):
        if db["version"] != world.tables_version:
            db["con"], db["version"] = _duckdb(world), world.tables_version
        return db["con"].execute(query)
    not_found = lambda what: f'Error 404 from Airflow: {{"detail":"{what} was not found"}}'

    def list_dags():
        return f"{DAG_ID} (paused=False)"

    def get_recent_dag_runs(dag_id, limit=5):
        if dag_id != DAG_ID:
            return not_found(f"The Dag with ID: `{dag_id}`")
        return "\n".join(
            f"run_id={r['dag_run_id']} | state={r['state']} | queued={r['queued_at']} | "
            f"start={r['start_date']} | end={r['end_date']}"
            for r in world.dag_runs[:limit]
        )

    def get_task_instances(dag_id, run_id):
        tasks = world.task_instances.get(run_id) if dag_id == DAG_ID else None
        if tasks is None:
            return not_found(f"DagRun with run_id: `{run_id}`")
        return "\n".join(
            f"task_id={t['task_id']} | state={t['state']} | try_number={t['try_number']} | "
            f"duration_s={t['duration']}"
            for t in tasks
        )

    def get_task_log(dag_id, run_id, task_id, try_number):
        if dag_id != DAG_ID:
            return not_found(f"The Dag with ID: `{dag_id}`")
        # A scenario can give each attempt its own log; otherwise use the task's default log
        log = world.task_logs.get((run_id, task_id, try_number), world.task_logs.get((run_id, task_id)))
        return log if log is not None else not_found(f"Task log for {task_id} in {run_id}")

    def list_tables():
        return "\n".join(f"{t} | type=EXTERNAL_TABLE | partitioned_by=['date']" for t in world.tables)

    def get_table_schema(table):
        if table not in world.tables:
            return f"AWS error EntityNotFoundException: Table {table} not found."
        return athena_server._format_schema(table, _glue_table(world, table))

    def get_table_versions(table, limit=5):
        if table not in world.tables:
            return f"AWS error EntityNotFoundException: Table {table} not found."
        versions = [{"VersionId": str(v["version"]), "Table": _glue_table(world, table, v)}
                    for v in world.table_versions[table][-limit:]]
        return athena_server._format_versions(table, versions)

    def get_registered_partitions(table):
        values = sorted(world.registered_partitions.get(table, []))
        if not values:
            return f"{table}: no partitions registered in the catalog."
        return f"{table}: {len(values)} registered partitions ({values[0]} to {values[-1]})\n{', '.join(values)}"

    def run_query(sql):
        # The REAL guardrail, with the world's tables as the allow-list
        try:
            safe_sql = check_query(sql, DATABASE, set(world.tables))
        except UnsafeQueryError as e:
            return f"Blocked by SQL guardrail: {e}"
        try:
            duck_sql = sqlglot.transpile(safe_sql, read="athena", write="duckdb")[0]
            cur = execute(duck_sql)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(101)                # real client fetches max_rows=100
        except Exception as e:
            return f"Query FAILED: {type(e).__name__}: {e}\nSQL that ran: {safe_sql}"
        result = QueryResult(
            columns=columns, rows=[[_athena_str(v) for v in r] for r in rows[:100]],
            truncated=len(rows) > 100, bytes_scanned=0, query_id="eval",
        )
        return athena_server._format_result(result)    # the REAL formatter

    def search_runbooks(query, k=3):
        # REAL runbooks: they're part of the agent's environment, not the pipeline's state
        return "\n\n---\n\n".join(
            f"[source: {r['source']} > {r['section']}]\n{r['text']}"
            for r in search(query, k, load_knowledge_settings())
        )

    def search_past_incidents(query, k=3):
        return "No past incidents recorded yet."      # isolation: never read real memory

    def record_incident(diagnosis_json, question, thread_id):
        return "eval run: not saved"                   # isolation: never write real memory

    def get_current_time():
        return world.now.isoformat(timespec="seconds")

    # ---- Ops tools: the live host. Healthy unless the scenario sets world overrides ----
    def _freshness() -> dict:
        datasets = {}
        for name in ("weather", "seismic"):
            lag = world.lake_lag_minutes.get(name, 2)
            datasets[name] = {"newest_file_minutes": lag, "partition": f"date={world.now:%Y-%m-%d}",
                              "stale": lag > ops.STALE_FILE_MINUTES}
        stations = [{"station": sid, "minutes_behind": world.station_lag_minutes.get(sid, 25),
                     "readings_since_yesterday": 120,
                     "stale": world.station_lag_minutes.get(sid, 25) > ops.STALE_STATION_MINUTES}
                    for sid in sorted(STATIONS)]
        return {"now": world.now.isoformat(timespec="seconds"), "datasets": datasets, "stations": stations,
                "seismic": {"events_today": 40, "last_ingest_minutes": world.lake_lag_minutes.get("seismic", 2)}}

    def _containers() -> list[dict]:
        base = [{"name": n, "state": "running", "status": "Up 3 days", "restarts": 0, "exit_code": 0,
                 "oom_killed": False} for n in ops.DEFAULT_EXPECTED.split(",") + ["copilot", "kafka-ui", "spark"]]
        base += [{"name": n, "state": "running", "status": "Up 2 days", "restarts": 0, "exit_code": 0,
                  "oom_killed": False, **extra} for n, extra in world.extra_containers.items()]
        return sorted(({**c, **world.containers.get(c["name"], {})} for c in base), key=lambda c: c["name"])

    # ---- Resources: running containers' stats, a host consistent with them, history ----
    def _snapshot() -> dict:
        stats = []
        for c in _containers():
            if c["state"] != "running":
                continue
            mem, limit, cpu = BASE_STATS.get(c["name"], (150, None, 0.5))
            stats.append({"name": c["name"], "mem_mb": mem, "limit_mb": limit, "cpu_pct": cpu, "pids": 30,
                          **world.container_stats.get(c["name"], {})})
        stats.sort(key=lambda c: -c["mem_mb"])
        return {"at": world.now.isoformat(timespec="seconds"), "host": host_for(stats, world.host_load1),
                "containers": stats}

    def _history() -> list[dict]:
        if world.resource_history is not None:
            return world.resource_history
        snap = _snapshot()
        return [{**snap, "at": (world.now - timedelta(minutes=30 * i)).isoformat(timespec="seconds")}
                for i in range(48, 0, -1)]

    def get_host_resources(as_json=False):
        snap = _snapshot()
        return json.dumps(snap) if as_json else resources.format_snapshot(snap)

    def get_resource_history(hours=24, container=None):
        since = (world.now - timedelta(hours=hours)).isoformat()
        return resources.format_history([r for r in _history() if r["at"] >= since], container)

    def _dashboard() -> dict:
        base = {"api_health": {"ok": True, "status": 200, "ms": 12},
                "api_stations": {"ok": True, "status": 200, "ms": 40, "stations": len(STATIONS),
                                 "by_status": {"normal": len(STATIONS)}},
                "public_site": {"ok": True, "status": 200, "ms": 180}}
        return {k: {**v, **world.dashboard.get(k, {})} for k, v in base.items()}

    def get_live_lake_freshness():
        return ops.format_freshness(_freshness())

    def query_live_lake(sql):
        # Approximation: the live lake = the world's raw tables
        try:
            safe_sql = check_query(sql, "lake", {"lake_weather", "lake_seismic"}, dialect="duckdb")
        except UnsafeQueryError as e:
            return f"Blocked by SQL guardrail: {e}"
        try:
            cur = execute(safe_sql)
            cols, rows = [d[0] for d in cur.description], cur.fetchmany(100)
        except duckdb.Error as e:
            return f"Query FAILED: {str(e).splitlines()[0]}\nSQL that ran: {safe_sql}"
        return "\n".join([" | ".join(cols)] + [" | ".join("NULL" if v is None else str(_athena_str(v)) for v in r)
                                              for r in rows] + [f"({len(rows)} rows; live local lake)"])

    def list_containers():
        return ops.format_containers(_containers())

    def get_container_logs(container, tail=100):
        names = {c["name"] for c in _containers()}
        if container not in names:
            return f"No container named {container!r}. Use list_containers to see the names."
        default = f"{world.now:%Y-%m-%dT%H:%M:%S} INFO running normally"
        return world.container_logs.get(container, default)

    def check_dashboard():
        return ops.format_dashboard(_dashboard())

    def run_health_checks():
        f, checks = _freshness(), []
        for name, d in f["datasets"].items():
            checks.append({"check": f"lake {name} receiving data", "ok": not d["stale"],
                           "detail": f"newest file {d['newest_file_minutes']} min ago"})
        stale = [s["station"] for s in f["stations"] if s["stale"]]
        checks.append({"check": "all weather stations reporting", "ok": not stale,
                       "detail": f"stale: {', '.join(stale)}" if stale else f"{len(f['stations'])} stations current"})
        for c in _containers():
            if c["name"] in ops.DEFAULT_EXPECTED.split(","):
                checks.append({"check": f"container {c['name']} running", "ok": c["state"] == "running",
                               "detail": f"{c['state']} ({c['status']}), restarts={c['restarts']}"})
        for label, r in _dashboard().items():
            checks.append({"check": f"dashboard {label}", "ok": r["ok"], "detail": f"HTTP {r.get('status')}"})
        checks += resources.checks(_snapshot())      # the REAL resource thresholds
        return json.dumps(checks)

    # ---- Glue job runs, derived from each DAG run's curate_day task ----
    def _glue_runs() -> list[dict]:
        runs = []
        for run in world.dag_runs:
            task = next((t for t in world.task_instances.get(run["dag_run_id"], []) if t["task_id"] == "curate_day"), None)
            if task is None:
                continue
            logs = " ".join(v for k, v in world.task_logs.items() if k[0] == run["dag_run_id"] and k[1] == "curate_day")
            error = logs.split("ErrorMessage: ", 1)[1].split("\n")[0] if "ErrorMessage: " in logs else None
            day = rem.target_date_of(run["dag_run_id"], run.get("conf"))   # the day this run curates
            runs.append({"id": "jr_" + hashlib.sha256(run["dag_run_id"].encode()).hexdigest(), "date": day,
                         "state": {"success": "SUCCEEDED", "failed": "FAILED"}.get(task["state"], "RUNNING"),
                         "started": run["start_date"], "duration": task["duration"],
                         "error": error if task["state"] == "failed" else None})
        return runs

    def get_glue_job_runs(limit=5):
        return "\n".join(
            f"run_id={r['id']} | state={r['state']} | date={r['date']} | started={r['started']} | "
            f"duration_s={r['duration']}" + (f" | error={r['error'][:300]}" if r["error"] else "")
            for r in _glue_runs()[:limit]) or "No runs found for Glue job curate-daily."

    def get_glue_job_log(run_id, stream="output"):
        run = next((r for r in _glue_runs() if r["id"] == run_id), None)
        if run is None or run["state"] == "RUNNING":
            return f"No {stream} log for {run_id} (the run may have produced no {stream} output)."
        if run["state"] == "FAILED":
            return run["error"] if stream == "error" else f"No output log for {run_id}."
        count = lambda t: sum(1 for r in world.tables.get(t, []) if r["date"] == run["date"])
        line = (f"date={run['date']} weather: {count('raw_weather')} raw -> {count('curated_weather')} curated | "
                f"seismic: {count('raw_seismic')} raw -> {count('curated_seismic')} curated")
        return f"summary:\n{line}\n---\n{line}"

    # ---- Actions: they CHANGE the world, the same rules as the real server ----
    def _refuse(action: dict) -> str | None:
        reason = rem.check_action(action, world.now.date())
        return json.dumps({"ok": False, "detail": f"Refused: {reason}"}) if reason else None

    def clear_task(run_id, task_id, thread_id):
        action = {"action": "clear_task", "run_id": run_id, "task_id": task_id}
        if refused := _refuse(action):
            return refused
        if run_id not in world.task_instances:
            return json.dumps({"ok": False, "detail": f"Airflow answered 404: DagRun {run_id} not found"})
        world.actions.append(action)
        world.rerun(run_id, task_id, rem.target_date_of(run_id))
        cleared = [t for t in TASKS if TASKS.index(t) >= TASKS.index(task_id)]
        return json.dumps({"ok": True, "detail": f"Cleared {', '.join(cleared)} in {run_id}; "
                                                 "Airflow will run them again.", "run_id": run_id})

    def trigger_backfill(date, thread_id):
        action = {"action": "backfill_date", "date": date}
        if refused := _refuse(action):
            return refused
        world.actions.append(action)
        run_id = f"manual__copilot_backfill_{date}_{world.now:%Y%m%dT%H%M%S}"
        world.dag_runs.insert(0, {"dag_run_id": run_id, "state": "queued", "conf": {"date": date},
                                  "queued_at": world.now.isoformat(), "start_date": None, "end_date": None})
        world.task_instances[run_id] = [{"task_id": t, "state": None, "try_number": 0, "duration": None}
                                        for t in TASKS]
        world.rerun(run_id, TASKS[0], date)
        return json.dumps({"ok": True, "detail": f"Triggered {run_id} to re-curate {date}.", "run_id": run_id})

    def restart_container(container, thread_id):
        action = {"action": "restart_container", "container": container}
        if refused := _refuse(action):
            return refused
        world.actions.append(action)
        old = world.containers.get(container, {})
        world.containers[container] = {"state": "running", "status": "Up 1 minute", "exit_code": 0,
                                       "oom_killed": False, "restarts": old.get("restarts", 0) + 1}
        for feed in CONTAINER_FEEDS.get(container, ()):
            world.lake_lag_minutes.pop(feed, None)
            if feed == "weather":
                world.station_lag_minutes.clear()
        return json.dumps({"ok": True, "detail": f"Restarted container {container}."})

    def get_dag_run_status(run_id):
        run = next((r for r in world.dag_runs if r["dag_run_id"] == run_id), None)
        if run is None:
            return json.dumps({"ok": False, "detail": f"Airflow answered 404: DagRun {run_id} not found"})
        return json.dumps({"ok": True, "run_id": run_id, "state": run["state"],
                           "target_date": rem.target_date_of(run_id, run.get("conf")),
                           "tasks": [{"task_id": t["task_id"], "state": t["state"], "try_number": t["try_number"]}
                                     for t in world.task_instances[run_id]]})

    return {f.__name__: f for f in [
        clear_task, trigger_backfill, restart_container, get_dag_run_status,
        get_host_resources, get_resource_history,
        list_dags, get_recent_dag_runs, get_task_instances, get_task_log,
        list_tables, get_table_schema, get_table_versions, get_registered_partitions, run_query,
        search_runbooks, search_past_incidents, record_incident, get_current_time,
        get_live_lake_freshness, query_live_lake, list_containers, get_container_logs,
        check_dashboard, run_health_checks, get_glue_job_runs, get_glue_job_log,
    ]}


async def make_fake_tools(world: World) -> list[BaseTool]:
    impls = _impls(world)
    # The real tools' names, descriptions and argument schemas, read from the servers
    real = [t for server in (airflow_server, athena_server, knowledge_server, ops_server, actions_server)
            for t in await server.mcp.list_tools()]
    missing = {t.name for t in real} - impls.keys()
    if missing:
        raise RuntimeError(f"No fake implementation for real tools: {missing}")

    def as_tool(name: str, description: str, schema: dict) -> BaseTool:
        async def run(**kwargs):
            return impls[name](**kwargs)
        return StructuredTool(name=name, description=description, args_schema=schema, coroutine=run)

    tools = [as_tool(t.name, t.description or "", t.inputSchema) for t in real]
    tools.append(as_tool("get_current_time", "Get the current date and time in UTC.",
                         {"type": "object", "properties": {}}))
    return tools
