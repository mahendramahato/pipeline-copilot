"""Fake tools for evals: the SAME names, descriptions and schemas as the real MCP
tools (copied from the servers at runtime), answering from a simulated World."""
from datetime import datetime

import duckdb
import sqlglot
from langchain_core.tools import BaseTool, StructuredTool

from evals.world import DAG_ID, SCHEMAS, World
from pipeline_copilot.athena_client import QueryResult
from pipeline_copilot.config import load_knowledge_settings
from pipeline_copilot.knowledge_base import search
from pipeline_copilot.mcp_servers import airflow_server, athena_server, knowledge_server
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
            con.executemany(
                f"INSERT INTO {table} VALUES ({', '.join('?' for _ in cols)})",
                [[r.get(c) for c in cols] for r in rows],
            )
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
    con = _duckdb(world)
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
            cur = con.execute(duck_sql)
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

    return {f.__name__: f for f in [
        list_dags, get_recent_dag_runs, get_task_instances, get_task_log,
        list_tables, get_table_schema, get_table_versions, get_registered_partitions, run_query,
        search_runbooks, search_past_incidents, record_incident, get_current_time,
    ]}


async def make_fake_tools(world: World) -> list[BaseTool]:
    impls = _impls(world)
    # The real tools' names, descriptions and argument schemas, read from the servers
    real = [t for server in (airflow_server, athena_server, knowledge_server)
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
